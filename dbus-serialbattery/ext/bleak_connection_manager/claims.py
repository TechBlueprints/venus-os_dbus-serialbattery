# -*- coding: utf-8 -*-
"""Adapter claims under /run/bt-claims: informal cross-service coordination.

Several BLE services on one host share a set of Bluetooth adapters. This
module implements a file convention any service can follow without linking a
library or coordinating a rollout - a shell script participates with touch,
ls and noclobber. Everything below the line is the whole contract.

The convention (0.2)
--------------------

Directory: /run/bt-claims (tmpfs: a reboot clears every claim, which also
makes hciN-keyed names safe - claims are statements about the current
numbering by live processes, and both die with the boot).

Three claim kinds, visible in the filename:

    hci4.scan                   hard claim: one well-known name per adapter.
                                "I am actively scanning here; use another
                                card." Created with O_EXCL, so two racing
                                claimants cannot both win.
    hci3.use.<owner>[.<qual>]   soft claim: one file per claimant, optionally
                                qualified (this package qualifies by device
                                MAC, one file per connection). "I am using
                                this card, but I can share if you cannot
                                find another." Never blocks, only ranks.
    hci3.link.<k>               link slot: numbered exclusive claim,
                                k < the deployment-configured per-adapter
                                capacity. "One of this card's usable
                                connections is mine." Caps are opt-in
                                deployment config - dongle limits are
                                undocumented and not discoverable - and
                                bound established-link capacity, not
                                connection-attempt pacing.

File content is one line: "<pid> <service> <since-epoch>". The writer
touches its file every HEARTBEAT_INTERVAL seconds; the mtime is the
heartbeat.

A claim is live when the pid in it is alive (kill -0, or /proc/<pid> from
shell) AND the file's mtime is within CLAIM_TTL. A dead process is therefore
detected instantly, and a wedged-but-alive one within the TTL. Anyone may
unlink a file that fails both checks; unlink races are harmless, and a live
writer whose file is wrongly reaped recreates it on its next heartbeat
(exclusive claims - scan and link - recreate with O_EXCL and concede if
another process took the name meanwhile, so cards never ping-pong).

Placement, for a service choosing among its allowed adapters: drop adapters
with a live hard claim held by someone else, sort the rest by live soft-claim
plus link count then by your own preference order, take the first, write your
claim. If every allowed adapter is hard-claimed, use the preferred one anyway
and log: a scanner's claim must never keep a battery off the air.
Coordination is an optimization, not a gate - the same rule covers an
unusable directory.

Soft claims are shareable by design, so two services placing at the same
moment may briefly co-locate; distinct preference orders make that rare, and
it is legal when it happens.

This file is deliberately standalone (stdlib only, no asyncio, no bleak, no
project imports) so other projects can vendor it verbatim.
"""

import logging
import os
import threading
import time

__version__ = "0.2.0"

logger = logging.getLogger(__name__)

CLAIM_DIR = "/run/bt-claims"
HEARTBEAT_INTERVAL = 10.0
CLAIM_TTL = 30.0


def _read_pid(path):
    """The pid recorded in a claim file, or None if unreadable."""
    try:
        with open(path) as f:
            return int(f.read().split()[0])
    except (OSError, ValueError, IndexError):
        return None


def _pid_alive(pid):
    """Whether a pid is running. kill(pid, 0) probes without signaling."""
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def _sanitize(part):
    """Claim-name components must stay within a filename-safe charset."""
    return "".join(c if c.isalnum() or c in "._-" else "-" for c in str(part))


class Claim:
    """A claim this process holds. Release it, or let the reaper find it.

    validity, when set, is a zero-argument callable checked on every
    heartbeat: the moment it returns falsy the manager releases the claim
    instead of touching it. It is the backstop against claims outliving the
    thing they account for (a connection that dropped without its callback
    ever firing, an abandoned scanner) - without it the heartbeat would keep
    such a claim live until process exit.
    """

    def __init__(self, adapter, path, exclusive):
        self.adapter = adapter
        self.path = path
        self.exclusive = exclusive
        self.released = False
        self.validity = None

    def touch(self):
        if self.released:
            return
        try:
            os.utime(self.path, None)
        except OSError:
            # reaped or the directory was cleared: recreate on the beat. An
            # exclusive claim (scan or link slot) recreates exclusively and
            # concedes if another process took the name meanwhile - stealing
            # it back would ping-pong the card between two owners.
            try:
                _write_claim_file(self.path, exclusive=self.exclusive)
            except FileExistsError:
                self.released = True
                logger.warning(f"bt-claims: lost exclusive claim {os.path.basename(self.path)} to another process")
            except OSError:
                pass

    def release(self):
        self.released = True
        try:
            os.unlink(self.path)
        except OSError:
            pass


def _write_claim_file(path, exclusive):
    flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC
    if exclusive:
        flags |= os.O_EXCL
    fd = os.open(path, flags, 0o644)
    try:
        service = os.path.basename(os.readlink("/proc/self/exe")) if os.path.exists("/proc/self/exe") else "python"
        os.write(fd, f"{os.getpid()} {service} {int(time.time())}\n".encode())
    finally:
        os.close(fd)


class ClaimManager:
    """Hold and honor adapter claims for one service.

    Every method degrades rather than raises: an unusable claim directory
    behaves as "no claims anywhere", and the caller proceeds uncoordinated.
    """

    def __init__(self, owner, claim_dir=CLAIM_DIR, ttl=CLAIM_TTL):
        self.owner = _sanitize(owner)
        self.claim_dir = claim_dir
        self.ttl = ttl
        self._held = []
        self._beat = None
        self._lock = threading.Lock()

    # -- liveness ----------------------------------------------------------

    def _is_live(self, path):
        try:
            fresh = (time.time() - os.stat(path).st_mtime) <= self.ttl
        except OSError:
            return False
        pid = _read_pid(path)
        alive = pid is not None and _pid_alive(pid)
        if alive and fresh:
            return True
        if not alive and not fresh:
            # dead by both tests: reap it for everyone
            try:
                os.unlink(path)
                logger.info(f"bt-claims: reaped stale claim {os.path.basename(path)}")
            except OSError:
                pass
        return False

    def claims(self):
        """Live-claim snapshot of the directory, keyed by adapter.

        {adapter: {"hard": path-or-None, "hard_pid": pid-or-None,
                   "soft": count, "soft_owners": [...], "links": count}}

        hard_pid lets a caller distinguish a foreign scanner's hard claim
        from one held by its own process.
        """
        state = {}
        try:
            names = os.listdir(self.claim_dir)
        except OSError:
            return state
        for name in names:
            adapter, sep, rest = name.partition(".")
            if not sep:
                continue
            path = os.path.join(self.claim_dir, name)
            entry = state.setdefault(adapter, {"hard": None, "hard_pid": None, "soft": 0, "soft_owners": [], "links": 0})
            if rest == "scan":
                if self._is_live(path):
                    entry["hard"] = path
                    entry["hard_pid"] = _read_pid(path)
            elif rest.startswith("use."):
                if self._is_live(path):
                    entry["soft"] += 1
                    entry["soft_owners"].append(rest[4:])
            elif rest.startswith("link."):
                if self._is_live(path):
                    entry["links"] += 1
        return state

    def foreign_use(self, adapter):
        """Count of live claims other processes hold on the adapter.

        The gate for disruptive actions: an adapter reset kills every link
        on the card, so a process must not reset one that another live
        process is scanning or connected on. An unusable directory returns
        0 - the caller is already uncoordinated.
        """
        count = 0
        try:
            names = os.listdir(self.claim_dir)
        except OSError:
            return 0
        own = os.getpid()
        prefix = f"{adapter}."
        for name in names:
            if not name.startswith(prefix):
                continue
            path = os.path.join(self.claim_dir, name)
            if not self._is_live(path):
                continue
            pid = _read_pid(path)
            if pid is not None and pid != own:
                count += 1
        return count

    # -- claiming ----------------------------------------------------------

    def _soft_path(self, adapter, qualifier=None):
        name = f"{adapter}.use.{self.owner}"
        if qualifier:
            name += f".{_sanitize(qualifier)}"
        return os.path.join(self.claim_dir, name)

    def claim_soft(self, adapter, qualifier=None):
        """Write this service's soft claim on an adapter. Never fails hard.

        A qualifier makes the claim per-thing rather than per-service (this
        package qualifies by device MAC, one claim per connection), so other
        processes' placement can see how much of the card is in use.
        """
        try:
            os.makedirs(self.claim_dir, exist_ok=True)
            path = self._soft_path(adapter, qualifier)
            _write_claim_file(path, exclusive=False)
            claim = Claim(adapter, path, exclusive=False)
            self._hold(claim)
            return claim
        except OSError as e:
            logger.debug(f"bt-claims: could not claim {adapter}: {repr(e)}")
            return None

    def claim_hard(self, adapter):
        """Take the adapter's single hard claim, or None if someone holds it."""
        path = os.path.join(self.claim_dir, f"{adapter}.scan")
        try:
            os.makedirs(self.claim_dir, exist_ok=True)
            _write_claim_file(path, exclusive=True)
        except FileExistsError:
            if self._is_live(path):
                return None
            # stale: take it over. _is_live may already have reaped the file,
            # so a missing file here is fine; losing the O_EXCL race to
            # another claimant costs us the claim, never correctness.
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
            except OSError:
                return None
            try:
                _write_claim_file(path, exclusive=True)
            except OSError:
                return None
        except OSError as e:
            logger.debug(f"bt-claims: could not hard-claim {adapter}: {repr(e)}")
            return None
        claim = Claim(adapter, path, exclusive=True)
        self._hold(claim)
        return claim

    def claim_slot(self, adapter, cap):
        """Take one of the adapter's cap numbered link slots, or None if all
        are held live.

        The slot files hciN.link.0 .. cap-1 follow the exclusive-claim rules
        of a hard claim: O_EXCL creation, stale takeover, O_EXCL-recreate-and-
        concede on the heartbeat. An unusable claim directory degrades to a
        phantom claim - a truthy no-op - because capacity accounting must
        never gate connections it cannot see.
        """
        try:
            os.makedirs(self.claim_dir, exist_ok=True)
        except OSError as e:
            logger.debug(f"bt-claims: claim directory unusable, links uncoordinated: {repr(e)}")
            return self._phantom(adapter)
        for k in range(cap):
            path = os.path.join(self.claim_dir, f"{adapter}.link.{k}")
            try:
                _write_claim_file(path, exclusive=True)
            except FileExistsError:
                if self._is_live(path):
                    continue
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass
                except OSError:
                    continue
                try:
                    _write_claim_file(path, exclusive=True)
                except FileExistsError:
                    continue
                except OSError as e:
                    logger.debug(f"bt-claims: could not slot-claim {adapter}: {repr(e)}")
                    return self._phantom(adapter)
            except OSError as e:
                logger.debug(f"bt-claims: could not slot-claim {adapter}: {repr(e)}")
                return self._phantom(adapter)
            claim = Claim(adapter, path, exclusive=True)
            self._hold(claim)
            return claim
        return None

    def _phantom(self, adapter):
        """Degraded stand-in for a slot when the directory is unusable: a
        released Claim never heartbeats and unlinks nothing, but is truthy,
        so the caller proceeds uncoordinated instead of treating the outage
        as exhaustion."""
        claim = Claim(adapter, os.path.join(self.claim_dir, f"{adapter}.link.phantom"), exclusive=True)
        claim.released = True
        return claim

    def release(self, claim):
        if claim is None:
            return
        claim.release()
        with self._lock:
            if claim in self._held:
                self._held.remove(claim)

    def release_all(self):
        """Release every claim this manager holds."""
        with self._lock:
            held = list(self._held)
            self._held = []
        for claim in held:
            claim.release()

    # -- placement ---------------------------------------------------------

    def choose(self, adapters):
        """Pick an adapter from a preference-ordered list and claim it soft.

        Adapters hard-claimed by another live process are avoided; among the
        rest, fewer live soft claims plus held link slots wins, preference
        order breaks ties. When everything is hard-claimed, the preferred
        adapter is used anyway: a scanner's claim must never keep this
        service off the air.

        Returns (adapter, Claim-or-None); (None, None) only for an empty list.
        """
        if not adapters:
            return None, None
        state = self.claims()
        open_adapters = [a for a in adapters if not (state.get(a) or {}).get("hard")]
        if not open_adapters:
            logger.info(f"bt-claims: every allowed adapter is hard-claimed, using {adapters[0]} anyway")
            open_adapters = list(adapters)
        ranked = sorted(open_adapters, key=lambda a: ((state.get(a) or {}).get("soft", 0) + (state.get(a) or {}).get("links", 0), adapters.index(a)))
        adapter = ranked[0]
        return adapter, self.claim_soft(adapter)

    # -- heartbeat ---------------------------------------------------------

    def _hold(self, claim):
        with self._lock:
            self._held.append(claim)
            if self._beat is None:
                self._beat = threading.Thread(target=self._heartbeat_loop, name="bt-claims-heartbeat", daemon=True)
                self._beat.start()

    def _beat_once(self):
        with self._lock:
            held = list(self._held)
        for claim in held:
            valid = True
            if claim.validity is not None:
                try:
                    valid = bool(claim.validity())
                except Exception:
                    # never drop a claim on a broken check: a claim wrongly
                    # held is bounded by process life, a claim wrongly
                    # released overcommits the card
                    valid = True
            if valid:
                claim.touch()
            else:
                logger.debug(f"bt-claims: releasing {os.path.basename(claim.path)}: what it accounted for is gone")
                self.release(claim)

    def _heartbeat_loop(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            self._beat_once()
