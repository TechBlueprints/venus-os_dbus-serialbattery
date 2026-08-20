# -*- coding: utf-8 -*-
# Vendored verbatim from https://github.com/TechBlueprints/bt-claims (0.1.0, MIT).
# Do not edit here; change it upstream and re-copy.
"""Adapter claims under /run/bt-claims: informal cross-service coordination.

Several BLE services on one host share a set of Bluetooth adapters. This
module implements a file convention any service can follow without linking a
library or coordinating a rollout - a shell script participates with touch,
ls and noclobber. Everything below the line is the whole contract.

The convention
--------------

Directory: /run/bt-claims (tmpfs: a reboot clears every claim, which also
makes hciN-keyed names safe - claims are statements about the current
numbering by live processes, and both die with the boot).

Two claim levels, visible in the filename:

    hci4.scan                   hard claim: one well-known name per adapter.
                                "I am actively scanning here; use another
                                card." Created with O_EXCL, so two racing
                                claimants cannot both win.
    hci3.use.<owner>            soft claim: one file per claimant.
                                "I am using this card, but I can share if
                                you cannot find another." Never blocks,
                                only ranks.

File content is one line: "<pid> <service> <since-epoch>". The writer
touches its file every HEARTBEAT_INTERVAL seconds; the mtime is the
heartbeat.

A claim is live when the pid in it is alive (kill -0, or /proc/<pid> from
shell) AND the file's mtime is within CLAIM_TTL. A dead process is therefore detected
instantly, and a wedged-but-alive one within the TTL. Anyone may unlink a
file that fails both checks; unlink races are harmless, and a live writer
whose file is wrongly reaped recreates it on its next heartbeat.

Placement, for a service choosing among its allowed adapters: drop adapters
with a live hard claim held by someone else, sort the rest by live soft-claim
count then by your own preference order, take the first, write your claim.
If every allowed adapter is hard-claimed, use the preferred one anyway and
log: a scanner's claim must never keep a battery off the air. Coordination
is an optimization, not a gate - the same rule covers an unusable directory.

Soft claims are shareable by design, so two services placing at the same
moment may briefly co-locate; distinct preference orders make that rare, and
it is legal when it happens.

This file is deliberately standalone (stdlib only, no asyncio, no project
imports) so other projects can vendor it verbatim.
"""

import logging
import os
import threading
import time

__version__ = "0.1.0"

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


class Claim:
    """A claim this process holds. Release it, or let the reaper find it."""

    def __init__(self, adapter, path, hard):
        self.adapter = adapter
        self.path = path
        self.hard = hard
        self.released = False

    def touch(self):
        if self.released:
            return
        try:
            os.utime(self.path, None)
        except OSError:
            # reaped or the directory was cleared: recreate on the beat. A
            # hard claim recreates exclusively and concedes if another
            # process took the name meanwhile - stealing it back would
            # ping-pong the card between two owners.
            try:
                _write_claim_file(self.path, exclusive=self.hard)
            except FileExistsError:
                self.released = True
                logger.warning(f"bt-claims: lost hard claim {os.path.basename(self.path)} to another process")
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
        # owner becomes part of a filename: keep it to a safe charset
        self.owner = "".join(c if c.isalnum() or c in "._-" else "-" for c in owner)
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

    def _claims(self):
        """{adapter: {"hard": live-or-None, "soft": count}} for the directory."""
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
            entry = state.setdefault(adapter, {"hard": None, "soft": 0, "soft_owners": []})
            if rest == "scan":
                if self._is_live(path):
                    entry["hard"] = path
            elif rest.startswith("use."):
                if self._is_live(path):
                    entry["soft"] += 1
                    entry["soft_owners"].append(rest[4:])
        return state

    # -- claiming ----------------------------------------------------------

    def _soft_path(self, adapter):
        return os.path.join(self.claim_dir, f"{adapter}.use.{self.owner}")

    def claim_soft(self, adapter):
        """Write this service's soft claim on an adapter. Never fails hard."""
        try:
            os.makedirs(self.claim_dir, exist_ok=True)
            path = self._soft_path(adapter)
            _write_claim_file(path, exclusive=False)
            claim = Claim(adapter, path, hard=False)
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
        claim = Claim(adapter, path, hard=True)
        self._hold(claim)
        return claim

    def release(self, claim):
        if claim is None:
            return
        claim.release()
        with self._lock:
            if claim in self._held:
                self._held.remove(claim)

    # -- placement ---------------------------------------------------------

    def choose(self, adapters):
        """Pick an adapter from a preference-ordered list and claim it soft.

        Adapters hard-claimed by another live process are avoided; among the
        rest, fewer live soft claims wins, preference order breaks ties. When
        everything is hard-claimed, the preferred adapter is used anyway: a
        scanner's claim must never keep this service off the air.

        Returns (adapter, Claim-or-None); (None, None) only for an empty list.
        """
        if not adapters:
            return None, None
        state = self._claims()
        open_adapters = [a for a in adapters if not (state.get(a) or {}).get("hard")]
        if not open_adapters:
            logger.info(f"bt-claims: every allowed adapter is hard-claimed, using {adapters[0]} anyway")
            open_adapters = list(adapters)
        ranked = sorted(open_adapters, key=lambda a: ((state.get(a) or {"soft": 0})["soft"], adapters.index(a)))
        adapter = ranked[0]
        return adapter, self.claim_soft(adapter)

    # -- heartbeat ---------------------------------------------------------

    def _hold(self, claim):
        with self._lock:
            self._held.append(claim)
            if self._beat is None:
                self._beat = threading.Thread(target=self._heartbeat_loop, name="bt-claims-heartbeat", daemon=True)
                self._beat.start()

    def _heartbeat_loop(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            with self._lock:
                held = list(self._held)
            for claim in held:
                claim.touch()
