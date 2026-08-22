# -*- coding: utf-8 -*-
"""Adapter claims under /run/bt-claims: informal cross-service coordination.

Several BLE services on one host share a set of Bluetooth adapters. This
module implements a file convention any service can follow without linking a
library or coordinating a rollout - a shell script participates with touch,
ls and noclobber. Everything below the line is the whole contract.

The convention (0.4)
--------------------

Directory: /run/bt-claims (tmpfs: a reboot clears every claim - claims are
statements by live processes, and both die with the boot).

Claims are keyed by the ADAPTER'S OWN MAC, colons stripped and uppercased
(0.4). hciN numbering is not stable - a USB reset renumbers a card, and so
does replugging it, both without a reboot - so a claim keyed by number can
come to name a different radio than the one its writer meant. The MAC is
the card. Every API here still accepts hciN as a convenience spelling and
resolves it (sysfs, then hciconfig) at use time, as does a MAC written any
way a human might type it: colons, dashes, dots, spaces, or none, any case.
An adapter whose MAC cannot be read at all - a dead controller reporting
all-zeros - degrades to its hciN name, because coordination must never fail
closed just because a card will not identify itself.

Convention 0.3 and earlier keyed files by hciN. Readers here canonicalize
every filename they find, so a 0.4 process counts a 0.3 process's claims
correctly, and exclusive claims (scan, link slots, drain) check the legacy
name before taking one - a mixed-version fleet cannot double-book a slot.
A 0.3 process cannot see 0.4 names, which is the usual version-skew
degradation: coordination quality, never correctness of the link itself.

Three claim kinds, visible in the filename (shown with an example MAC key
AABBCCDDEEFF; hciN in the 0.3 spelling):

    AABBCCDDEEFF.scan           hard claim: one well-known name per adapter.
                                "I am actively scanning here; use another
                                card." Created with O_EXCL, so two racing
                                claimants cannot both win.
    AABBCCDDEEFF.use.<owner>[.<qual>]
                                soft claim: one file per claimant, optionally
                                qualified (this package qualifies by device
                                MAC, one file per connection). "I am using
                                this card, but I can share if you cannot
                                find another." Never blocks, only ranks.
    AABBCCDDEEFF.link.<k>       link slot: numbered exclusive claim,
                                k < the deployment-configured per-adapter
                                capacity. "One of this card's usable
                                connections is mine." Caps are opt-in
                                deployment config - dongle limits are
                                undocumented and not discoverable - and
                                bound established-link capacity, not
                                connection-attempt pacing.
    AABBCCDDEEFF.drain          drain claim (0.3): one well-known name per
                                adapter, O_EXCL like the scan claim. "This
                                card is about to be reset; place new work
                                elsewhere and, if you can, move your links
                                off it." Held by the process performing the
                                recovery, heartbeated like any claim, and
                                released when the reset - or the abandoned
                                attempt - is over, so a dead resetter's
                                drain is reaped like anything else. Draining
                                steers, never gates: placement ranks a
                                draining card behind every other candidate
                                but still uses it when nothing else is
                                usable, and a claimant that cannot move (its
                                only working card, an operator pin) simply
                                stays - its live claims continue to veto the
                                reset, and the resetter gives up at its
                                deadline rather than pulling a card out from
                                under it.

File content is one line: "<pid> <service> <since-epoch>". This
implementation writes the manager's sanitized owner string as <service>, so
an ls-level debugger can see who holds a card; a bare participant may write
its process name instead. The writer touches its file every
HEARTBEAT_INTERVAL seconds; the mtime is the heartbeat.

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

A claim is a statement about a link, not the link itself, and the two can
drift: the claim can outlive the link (release it - that is what validity
checks are for) or the link can outlive the claim (re-acquire it - the link
exists regardless of what the files say). When judging whether the thing a
claim accounts for still exists, trust what was observed on the link - a
notification arriving, a GATT operation returning - over any cached
connection property; caches of link state go stale precisely in the failure
modes this convention exists to survive (field-validated twice, 2026-08).

This file is deliberately standalone - stdlib only, no asyncio, no bleak,
no imports from anywhere else in this package - so a service that wants
adapter coordination WITHOUT the bleak catcher can copy this one file into
its own tree and import it directly. The docstring above is the whole
specification; a participant that would rather not import anything can
follow it with ls, touch and cat. If you copy this file, record the commit
you took it from, so a convention bump can be traced.
"""

import logging
import os
import re
import subprocess
import threading
import time

__version__ = "0.4.0"

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


# -- adapter identity ------------------------------------------------------
#
# A card's hciN number is not stable: a USB reset renumbers it, and so does
# replugging - both WITHOUT a reboot, which is what the tmpfs-clears-at-boot
# argument used to rest on. The MAC is the card. Claims are therefore keyed
# by MAC (colons stripped, uppercase) and hciN is accepted everywhere as a
# convenience spelling that resolves to one.

BT_SYSFS = "/sys/class/bluetooth"
UNKNOWN_MAC = "00:00:00:00:00:00"
_HCI_RE = re.compile(r"^hci\d+$")
_HEX12_RE = re.compile(r"^[0-9A-F]{12}$")
# separators seen in the wild for the same address: colons, dashes, dots,
# spaces, or nothing at all
_MAC_SEPARATORS = ":-. \t_"

# adapter -> (mac, monotonic). Short-lived because a reset can take a card
# from all-zeros to a real address, long enough that per-connect lookups do
# not spawn hciconfig in a tight loop on kernels with no sysfs address.
_MAC_CACHE_TTL = 30.0
_mac_cache = {}
_mac_cache_lock = threading.Lock()


def _read_sysfs_mac(adapter):
    """The adapter's MAC from sysfs, or None if the attribute is absent.

    Free (no subprocess) where it exists - but Venus OS exposes no address
    attribute for ANY adapter (field 2026-08-22: both Cerbos, all seven
    cards on prod), which is what makes the hciconfig path below the
    normal one rather than the fallback it looks like.
    """
    try:
        with open(os.path.join(BT_SYSFS, str(adapter), "address")) as f:
            mac = f.read().strip().upper()
            return mac or None
    except OSError:
        return None


def _read_hciconfig_table():
    """{hciN: MAC} for every adapter, from ONE hciconfig call.

    Bare hciconfig prints every interface, so the whole table costs one
    process spawn instead of one per card. That matters on the deployment
    this serves: seven adapters, no sysfs address attribute, and identity
    resolved on every placement decision.
    """
    table = {}
    try:
        result = subprocess.run(["hciconfig"], capture_output=True, text=True, timeout=5)
    except Exception:
        return table
    current = None
    for line in result.stdout.splitlines():
        head = re.match(r"^(hci\d+):", line)
        if head:
            current = head.group(1)
            continue
        if current:
            found = re.search(r"BD Address:\s*([0-9A-Fa-f:]{17})", line)
            if found:
                table[current] = found.group(1).upper()
                current = None
    return table


def _read_adapter_mac(adapter):
    """One adapter's MAC. Fills the cache for every OTHER adapter it sees
    on the way, since the hciconfig read costs the same either way."""
    mac = _read_sysfs_mac(adapter)
    if mac:
        return mac
    table = _read_hciconfig_table()
    if table:
        now = time.monotonic()
        with _mac_cache_lock:
            for name, found in table.items():
                if name != adapter:
                    _mac_cache[name] = (found, now)
    return table.get(str(adapter), UNKNOWN_MAC)


def adapter_mac(adapter):
    """The adapter's own MAC (sysfs, then hciconfig), or the all-zeros
    unknown value - which is also what a genuinely failed adapter reports."""
    now = time.monotonic()
    with _mac_cache_lock:
        cached = _mac_cache.get(adapter)
        if cached is not None and now - cached[1] < _MAC_CACHE_TTL:
            return cached[0]
    mac = _read_adapter_mac(adapter)
    with _mac_cache_lock:
        _mac_cache[adapter] = (mac, now)
    return mac


def invalidate_adapter_mac(adapter=None):
    """Drop cached MACs. A reset is exactly when a cached MAC goes stale."""
    with _mac_cache_lock:
        if adapter is None:
            _mac_cache.clear()
        else:
            _mac_cache.pop(adapter, None)


def present_hci_names():
    """Every hciN the kernel currently exposes, sorted by index."""
    try:
        names = [n for n in os.listdir(BT_SYSFS) if _HCI_RE.match(n)]
    except OSError:
        return []
    return sorted(names, key=lambda n: int(n[3:]))


def hci_for(adapter):
    """The hciN name an adapter currently answers to, or None.

    hciN in, the same name back (it IS the current numbering). A MAC in -
    in any spelling - the card carrying it right now, which is the whole
    point: config and claim files name the card, and the number is looked
    up at use time.
    """
    text = str(adapter).strip()
    if _HCI_RE.match(text):
        return text
    key = mac_key(text)
    if key is None:
        return None
    for name in present_hci_names():
        mac = adapter_mac(name)
        if mac != UNKNOWN_MAC and mac.replace(":", "").upper() == key:
            return name
    return None


def mac_key(value):
    """Any spelling of a MAC -> the canonical AABBCCDDEEFF key, else None.

    Deliberately permissive, because this value is typed by humans into
    config files: case is ignored, and the octet separator may be colons,
    dashes, dots, spaces, underscores, or absent entirely. Anything that
    is not twelve hex digits after separator-stripping is not a MAC (an
    hciN name included), and the caller decides what that means.
    """
    text = str(value).strip()
    if not text:
        return None
    for sep in _MAC_SEPARATORS:
        text = text.replace(sep, "")
    text = text.upper()
    return text if _HEX12_RE.match(text) else None


def adapter_key(adapter):
    """The claim-file key for an adapter: its MAC, colons stripped.

    Accepts a MAC or an hciN name. An hciN whose MAC cannot be read (a
    dead controller reporting all-zeros, a kernel with neither sysfs nor
    hciconfig) degrades to the sanitized hciN name - coordination is an
    optimization and must never fail closed just because a card will not
    identify itself.
    """
    key = mac_key(adapter)
    if key is not None:
        return key
    mac = adapter_mac(str(adapter).strip())
    if mac != UNKNOWN_MAC:
        return mac.replace(":", "").upper()
    return _sanitize(adapter)


def legacy_key(adapter):
    """The pre-0.4 (hciN) claim-file key for an adapter, or None.

    Read-side compatibility: a process still running convention 0.3 names
    its claims hciN.*, and this fleet updates one service at a time.
    """
    name = hci_for(adapter)
    if name is None:
        return None
    return name if name != adapter_key(adapter) else None


class Claim:
    """A claim this process holds. Release it, or let the reaper find it.

    validity, when set, is a zero-argument callable checked on every
    heartbeat: the moment it returns falsy the manager releases the claim
    instead of touching it. It is the backstop against claims outliving the
    thing they account for (a connection that dropped without its callback
    ever firing, an abandoned scanner) - without it the heartbeat would keep
    such a claim live until process exit.
    """

    def __init__(self, adapter, path, exclusive, service=None):
        self.adapter = adapter
        self.path = path
        self.exclusive = exclusive
        self.service = service
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
                _write_claim_file(self.path, exclusive=self.exclusive, service=self.service)
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


def _write_claim_file(path, exclusive, service=None):
    flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC
    if exclusive:
        flags |= os.O_EXCL
    fd = os.open(path, flags, 0o644)
    try:
        if service is None:
            # fallback for bare use; managers pass their owner string, which
            # is what makes `ls`-level debugging name the actual holder
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
        # optional zero-argument callable invoked after every heartbeat
        # sweep, on the heartbeat thread. The hook for periodic work that
        # wants the claim cadence without a second timer (this package's
        # bleak catcher watches for drain claims here). Exceptions are
        # swallowed: a broken hook must not stop the heartbeat.
        self.on_beat = None

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
                   "soft": count, "soft_owners": [...], "links": count,
                   "drain": bool, "drain_pid": pid-or-None}}

        hard_pid lets a caller distinguish a foreign scanner's hard claim
        from one held by its own process.
        """
        state = {}
        try:
            names = os.listdir(self.claim_dir)
        except OSError:
            return state
        for name in names:
            prefix, sep, rest = name.partition(".")
            if not sep:
                continue
            # a pre-0.4 process names its files hciN.*; canonicalize so both
            # spellings of the same card land in one entry
            adapter = adapter_key(prefix)
            path = os.path.join(self.claim_dir, name)
            entry = state.setdefault(adapter, {"hard": None, "hard_pid": None, "soft": 0, "soft_owners": [], "links": 0, "drain": False, "drain_pid": None})
            if rest == "scan":
                if self._is_live(path):
                    entry["hard"] = path
                    entry["hard_pid"] = _read_pid(path)
            elif rest == "drain":
                if self._is_live(path):
                    entry["drain"] = True
                    entry["drain_pid"] = _read_pid(path)
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
        wanted = {adapter_key(adapter)}
        legacy = legacy_key(adapter)
        if legacy:
            wanted.add(legacy)
        for name in names:
            prefix, sep, _rest = name.partition(".")
            if not sep or prefix not in wanted:
                continue
            path = os.path.join(self.claim_dir, name)
            if not self._is_live(path):
                continue
            pid = _read_pid(path)
            if pid is not None and pid != own:
                count += 1
        return count

    def own_use(self, adapter):
        """Count of live claims THIS manager holds on the adapter.

        The drain claim itself is excluded: it marks the recovery, it is not
        usage, and counting it would make a resetter wait on itself.
        """
        with self._lock:
            held = list(self._held)
        # keyed off the claim's PATH, not the name it was created with: the
        # path carries the canonical key chosen when the claim was taken,
        # while the caller's spelling ("hci3") goes stale the moment the
        # card renumbers - which is the whole reason for this convention
        wanted = adapter_key(adapter)
        return sum(
            1
            for claim in held
            if os.path.basename(claim.path).partition(".")[0] == wanted
            and not claim.released
            and not claim.path.endswith(".drain")
        )

    def drain_active(self, adapter):
        """Whether a live drain claim exists on the adapter (any process)."""
        path, legacy = self._names(adapter, "drain")
        return self._is_live(path) or (bool(legacy) and self._is_live(legacy))

    # -- claiming ----------------------------------------------------------

    def _names(self, adapter, suffix):
        """(canonical path, legacy path or None) for one claim name.

        The legacy path is what a pre-0.4 process would have created for
        the same card. Exclusive claims consult it before taking a name,
        so a mixed-version fleet cannot double-book one slot.
        """
        canonical = os.path.join(self.claim_dir, f"{adapter_key(adapter)}.{suffix}")
        legacy = legacy_key(adapter)
        return canonical, (os.path.join(self.claim_dir, f"{legacy}.{suffix}") if legacy else None)

    def _legacy_held(self, adapter, suffix):
        """Whether a pre-0.4 process holds this exclusive name right now."""
        _canonical, legacy = self._names(adapter, suffix)
        return bool(legacy) and self._is_live(legacy)

    def _soft_path(self, adapter, qualifier=None):
        name = f"{adapter_key(adapter)}.use.{self.owner}"
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
            _write_claim_file(path, exclusive=False, service=self.owner)
            claim = Claim(adapter, path, exclusive=False, service=self.owner)
            self._hold(claim)
            return claim
        except OSError as e:
            logger.debug(f"bt-claims: could not claim {adapter}: {repr(e)}")
            return None

    def claim_hard(self, adapter):
        """Take the adapter's single hard claim, or None if someone holds it."""
        path, _legacy = self._names(adapter, "scan")
        if self._legacy_held(adapter, "scan"):
            return None  # a pre-0.4 process is scanning this card
        try:
            os.makedirs(self.claim_dir, exist_ok=True)
            _write_claim_file(path, exclusive=True, service=self.owner)
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
                _write_claim_file(path, exclusive=True, service=self.owner)
            except OSError:
                return None
        except OSError as e:
            logger.debug(f"bt-claims: could not hard-claim {adapter}: {repr(e)}")
            return None
        claim = Claim(adapter, path, exclusive=True, service=self.owner)
        self._hold(claim)
        return claim

    def claim_drain(self, adapter):
        """Take the adapter's single drain claim, or None if someone holds it.

        The exclusivity is the coordination: one process performs a recovery
        at a time, and a second would-be resetter backing off on None knows
        the card is already being handled.
        """
        path, _legacy = self._names(adapter, "drain")
        if self._legacy_held(adapter, "drain"):
            return None
        try:
            os.makedirs(self.claim_dir, exist_ok=True)
            _write_claim_file(path, exclusive=True, service=self.owner)
        except FileExistsError:
            if self._is_live(path):
                return None
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
            except OSError:
                return None
            try:
                _write_claim_file(path, exclusive=True, service=self.owner)
            except OSError:
                return None
        except OSError as e:
            logger.debug(f"bt-claims: could not drain-claim {adapter}: {repr(e)}")
            return None
        claim = Claim(adapter, path, exclusive=True, service=self.owner)
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
            path, legacy = self._names(adapter, f"link.{k}")
            if legacy and self._is_live(legacy):
                continue  # a pre-0.4 process holds this slot of this card
            try:
                _write_claim_file(path, exclusive=True, service=self.owner)
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
                    _write_claim_file(path, exclusive=True, service=self.owner)
                except FileExistsError:
                    continue
                except OSError as e:
                    logger.debug(f"bt-claims: could not slot-claim {adapter}: {repr(e)}")
                    return self._phantom(adapter)
            except OSError as e:
                logger.debug(f"bt-claims: could not slot-claim {adapter}: {repr(e)}")
                return self._phantom(adapter)
            claim = Claim(adapter, path, exclusive=True, service=self.owner)
            self._hold(claim)
            return claim
        return None

    def _phantom(self, adapter):
        """Degraded stand-in for a slot when the directory is unusable: a
        released Claim never heartbeats and unlinks nothing, but is truthy,
        so the caller proceeds uncoordinated instead of treating the outage
        as exhaustion."""
        claim = Claim(adapter, os.path.join(self.claim_dir, f"{adapter_key(adapter)}.link.phantom"), exclusive=True)
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
        rest, a card being drained ranks behind every card that is not, then
        fewer live soft claims plus held link slots wins, preference order
        breaks ties. When everything is hard-claimed, the preferred adapter
        is used anyway: a scanner's claim must never keep this service off
        the air, and a drain steers placement but never refuses it.

        Returns (adapter, Claim-or-None); (None, None) only for an empty list.
        """
        if not adapters:
            return None, None
        state = self.claims()
        open_adapters = [a for a in adapters if not (state.get(a) or {}).get("hard")]
        if not open_adapters:
            logger.info(f"bt-claims: every allowed adapter is hard-claimed, using {adapters[0]} anyway")
            open_adapters = list(adapters)
        ranked = sorted(open_adapters, key=lambda a: (bool((state.get(a) or {}).get("drain")), (state.get(a) or {}).get("soft", 0) + (state.get(a) or {}).get("links", 0), adapters.index(a)))
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
                # info, not debug: a validity release is the record of a
                # claim/link divergence - the exact event field diagnosis
                # needs to see (2026-08-22: prod lost a claim invisibly
                # because this line was below the deployed log level)
                logger.info(f"bt-claims: releasing {os.path.basename(claim.path)}: what it accounted for is gone")
                self.release(claim)
        hook = self.on_beat
        if hook is not None:
            try:
                hook()
            except Exception:
                logger.debug("bt-claims: on_beat hook raised", exc_info=True)

    def _heartbeat_loop(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            self._beat_once()
