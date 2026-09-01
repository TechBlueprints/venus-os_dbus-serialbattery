# -*- coding: utf-8 -*-
"""Adapter claims under /run/bt-claims: informal cross-service coordination.

Several BLE services on one host share a set of Bluetooth adapters. This
module implements a file convention any service can follow without linking a
library or coordinating a rollout - a shell script participates with touch,
ls and noclobber. Everything below the line is the whole contract.

The convention (0.5)
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
                                card." A HARDLINK to the holder's own file
                                (0.5, below), so two racing claimants cannot
                                both win and neither can be silently
                                displaced.
    AABBCCDDEEFF.scan.holder.<service>-<pid>-<seq>
                                the holder file the lock above links to.
                                Same inode as the lock; that shared inode IS
                                the proof of ownership.
    AABBCCDDEEFF.scanwait.<service>-<pid>-<seq>
                                queue ticket (0.5): "I want this card's scan
                                claim and I am waiting for it." Claims
                                nothing, gates nothing, blocks no reset -
                                it only publishes a queue so waiters spread
                                themselves over the cards instead of
                                stampeding one. Heartbeated and TTL-reaped
                                like any file here.
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
                                adapter, hardlinked like the scan claim.
                                "This card is about to be reset; place new
                                work elsewhere and, if you can, move your
                                links off it." Held by the process
                                performing the recovery, heartbeated like
                                any claim, and released when the reset - or
                                the abandoned attempt - is over, so a dead
                                resetter's drain is reaped like anything
                                else. Draining is ABSOLUTE from 0.5: nothing
                                NEW starts on a draining card, with no
                                pinned or explicit carve-out. New work waits
                                (scans queue on a scanwait ticket; connects
                                raise the typed out-of-slots error and the
                                retry layer above paces the retry). A drain
                                window is bounded by the resetter's own
                                deadline, and the reset it precedes can only
                                ever fire on a card that emptied
                                VOLUNTARILY: existing work always beats
                                maintenance, and a single live claim of any
                                kind, foreign or our own, refuses the reset
                                at the deadline.

File content is one line: "<pid> <service> <since-epoch> [purpose]". This
implementation writes the manager's sanitized owner string as <service>, so
an ls-level debugger can see who holds a card; a bare participant may write
its process name instead. The writer touches its file every
HEARTBEAT_INTERVAL seconds; the mtime is the heartbeat.

Exclusive locks are hardlinks (0.5)
-----------------------------------

The two exclusive well-known names - .scan and .drain - are held as a
HARDLINK rather than an O_EXCL file, because O_EXCL alone cannot express
"this claim is still mine". Two defects it left open, both reachable in a
fleet where every process reaps every other process's stale files:

- stale takeover was check-unlink-create, and the check and the unlink are
  separate syscalls. A taker acting on a liveness read taken microseconds
  ago could unlink a FRESH claim another process had created in between,
  and the victim never found out.
- the heartbeat's touch() called utime on the well-known NAME without ever
  asking whether that name still referred to its file, so two processes
  could both believe they held one card indefinitely.

The layout closes both:

- Acquire: write MAC.scan.holder.<service>-<pid>-<seq> first, then
  os.link(holder, "MAC.scan"). EEXIST on the link means the race was lost -
  never a partially-created claim.
- Ownership: os.stat(lock).st_ino == os.stat(holder).st_ino. Nothing else
  is proof. Checked on every heartbeat and before every release.
- Heartbeat: touch the HOLDER (the shared inode freshens the lock's mtime
  with it), then verify the inodes still match. A mismatch means the claim
  was provably taken; the holder says so at WARNING, marks itself lost and
  stops - it does NOT steal the name back, because two processes taking
  turns stealing a card is worse than one losing it.
- Stale takeover: os.rename(lock, lock + ".reaping.<pid>-<seq>"), never
  unlink. Rename is atomic and picks exactly one winner; the loser gets
  ENOENT and backs off. The winner re-checks that the file it moved aside
  is the same inode it diagnosed stale, and if it is not - a fresh claimant
  won the name in between - it links the file back under its own name and
  concedes, so the TOCTOU above cannot destroy a live claim.
- Release: verify the inode, unlink the lock (ENOENT is a correct outcome:
  a taker moved it aside first), then unlink the holder.

<seq> is a per-process monotonic counter, and it - not mtime - is the age
authority for the scanwait queue. Venus boxes have no RTC and step their
clock at the first NTP sync, sometimes by years, which would silently
reorder an mtime-ordered queue under its readers.

A 0.4 process reading a 0.5 directory is unaffected: it partitions a
filename at the first dot and matches the remainder exactly ("scan",
"drain") or by prefix ("use.", "link."), and "scan.holder.x",
"scan.reaping.x" and "scanwait.x" match none of those. It sees the .scan
lock, correctly, as a hard claim. Conversely a 0.5 process treats a plain
un-hardlinked MAC.scan - link count 1, no holder sibling - as a valid hard
claim judged the 0.4 way, on content pid and mtime. Both directions work
during the minutes a fleet spends mid-deploy.

A claim is live when the pid in it is alive (kill -0, or /proc/<pid> from
shell) AND the file's mtime is within CLAIM_TTL. A dead process is therefore
detected instantly, and a wedged-but-alive one within the TTL. Anyone may
unlink a file that fails both checks; unlink races are harmless, and a live
writer whose file is wrongly reaped recreates it on its next heartbeat
(exclusive claims - scan and link - recreate with O_EXCL and concede if
another process took the name meanwhile, so cards never ping-pong).

Placement, for a service choosing among its allowed adapters: drop adapters
with a live hard claim held by someone else, drop adapters with a live drain
claim, sort the rest by live soft-claim plus link count then by your own
preference order, take the first, write your claim. If every allowed adapter
is hard-claimed, a CONNECT uses the preferred one anyway and logs - a
scanner's claim must never keep a battery off the air - while a SCAN queues
(see scanwait) rather than starting unclaimed.

That asymmetry is the 0.5 correction, and it was paid for. Before it, a hard
claim was advisory for scans too: "every card is claimed, scan the best one
anyway". On 2026-08-26 that turned a busy fleet into an outage. A scan
started on a card another process in the SAME process tree was already
scanning, BlueZ answered StartDiscovery with InProgress, the wrapper scored
that as a radio failure, three such strikes power-cycled a perfectly healthy
adapter, and every USB reset mass-disappeared the devices on it - which is
the detonation path of the BlueZ gatt-client use-after-free. The coordination
that was skipped as "only an optimization" was the thing keeping the
evidence honest. A hard claim is a GATE for scanning: nothing scans without
holding it. Coordination remains an optimization for CONNECTS, where it
ranks rather than refuses, and an unusable claim directory still degrades to
uncoordinated everywhere.

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

Known limit, worth stating plainly: this convention only ever sees
processes that participate in it. A service that drives a radio some other
way - raw HCI sockets, a C program talking to BlueZ directly, an operator
at bluetoothctl - holds no claims and appears in no occupancy score, so
every score on such a host is structurally blind to that use. This is not
a gap a future adoption closes; it is the boundary of a file-convention
approach. (Concretely, on the deployment this was built for, one service
scans over raw HCI and is permanently invisible here.) Coordination is an
optimization over what it can see, never a guarantee about the radio.

This file is deliberately standalone - stdlib only, no asyncio, no bleak,
no imports from anywhere else in this package - so a service that wants
adapter coordination WITHOUT the bleak catcher can copy this one file into
its own tree and import it directly. The docstring above is the whole
specification; a participant that would rather not import anything can
follow it with ls, touch and cat. If you copy this file, record the commit
you took it from, so a convention bump can be traced.
"""

import itertools
import logging
import os
import re
import subprocess
import threading
import time

__version__ = "0.5.0"

logger = logging.getLogger(__name__)

CLAIM_DIR = "/run/bt-claims"
HEARTBEAT_INTERVAL = 10.0
CLAIM_TTL = 30.0

# Stamped into every holder and scanwait filename, and the age authority for
# the scanwait queue. mtime deliberately is not: a Venus box has no RTC and
# steps its clock at the first NTP sync - sometimes by years - which would
# reorder an mtime-ordered queue under the feet of every process reading it,
# and a queue that reorders is exactly the thrash the queue exists to avoid.
#
# The counter is per process, which is honest about what it can order. Within
# one process it is true arrival order. Across processes it cannot be - two
# processes both start at 0 - so cross-process ties fall through to owner and
# pid, which is stable and deterministic rather than fair. Deterministic is
# the property that matters here: every reader agrees on the order, so no
# waiter is ever woken by a queue that reshuffled itself.
_seq_counter = itertools.count()

# suffix fragments that name the 0.5 bookkeeping files rather than a claim on
# a card. Collected here because three separate readers have to skip them.
_HOLDER_MARK = ".holder."
_REAPING_MARK = ".reaping."
_SCANWAIT_PREFIX = "scanwait."
# the two exclusive well-known names held as a hardlink
_HARDLINKED = (".scan", ".drain")


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


def _unlink_quiet(path):
    """Unlink, ignoring every reason it might not be there. Unlink races on
    files this convention owns are harmless by design."""
    try:
        os.unlink(path)
    except OSError:
        pass


def _default_service():
    """What to write as <service> when a bare participant names none."""
    try:
        if os.path.exists("/proc/self/exe"):
            return os.path.basename(os.readlink("/proc/self/exe"))
    except OSError:
        pass
    return "python"


def _stamp(service):
    """The <service>-<pid>-<seq> tail every 0.5 file is named by."""
    return f"{_sanitize(service or _default_service())}-{os.getpid()}-{next(_seq_counter)}"


def _parse_stamp(tail):
    """A <service>-<pid>-<seq> tail -> (service, pid, seq), or None.

    Split from the RIGHT: the sanitized service string is allowed to contain
    dashes (owners here are "<name>-<pid>"), so only the last two fields are
    positionally reliable.
    """
    parts = str(tail).rsplit("-", 2)
    if len(parts) != 3:
        return None
    try:
        return parts[0], int(parts[1]), int(parts[2])
    except ValueError:
        return None


def _holder_path(lockname, service):
    return f"{lockname}{_HOLDER_MARK}{_stamp(service)}"


def _is_bookkeeping(rest):
    """Whether a filename's post-key remainder names a 0.5 bookkeeping file
    rather than a claim on the card: a holder (the other half of a lock that
    is already counted under its own name), a file moved aside for reaping,
    or a queue ticket that claims nothing."""
    return rest.startswith(_SCANWAIT_PREFIX) or _HOLDER_MARK in rest or _REAPING_MARK in rest


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

# MACs seen on more than one card at once. Counterfeit CSR8510 dongles are
# notorious for shipping a whole batch with one hardcoded address, and this
# deployment runs CSR-based cards (0a12:0001) - if two of them ever report
# the same address, keying claims by it would silently MERGE two physical
# radios into one accounting identity: slots double-booked, occupancy
# halved, a drain on one card draining the other. Cheap to detect while
# reading the table, so it is never keyed on.
_duplicate_macs = set()
_warned_duplicate_macs = set()


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


def _note_duplicate_macs(table):
    """Record any address claimed by more than one present adapter."""
    seen = {}
    for name, mac in table.items():
        if mac == UNKNOWN_MAC:
            continue
        if mac in seen:
            _duplicate_macs.add(mac)
        seen[mac] = name


def _read_adapter_mac(adapter):
    """One adapter's MAC. Fills the cache for every OTHER adapter it sees
    on the way, since the hciconfig read costs the same either way."""
    mac = _read_sysfs_mac(adapter)
    if mac:
        return mac
    table = _read_hciconfig_table()
    if table:
        # checked where the table is CONSUMED, not inside the reader: any
        # path that obtains a table must get the duplicate guard with it
        _note_duplicate_macs(table)
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


def hci_for(adapter, fresh=True):
    """The hciN name an adapter currently answers to, or None.

    hciN in, the same name back (it IS the current numbering). A MAC in -
    in any spelling - the card carrying it RIGHT NOW, resolved against the
    live numbering rather than a cached mapping.

    Fresh by default, and deliberately: naming a card by its MAC is a
    statement that its number may change, so a caller asking this question
    has no use for a stale answer. The hazard is not theoretical - a
    consumer resolving a MAC here to open a RAW HCI SOCKET on the result
    could, within one cache TTL, have programmed a scan onto whatever card
    had since inherited the number.

    Pass fresh=False only where the caller has already refreshed, or where
    staleness is genuinely tolerable, because the refill costs one
    hciconfig call on a host with no sysfs address attribute (about 11ms on
    a Cerbo, against ~19us cached). Both in-tree users do exactly that:
    the catcher refreshes ONCE for a whole configured list rather than per
    entry, and legacy_key is a pre-0.4 compatibility lookup where a stale
    number costs at most a briefly missed legacy claim.
    """
    text = str(adapter).strip()
    if _HCI_RE.match(text):
        return text
    key = mac_key(text)
    if key is None:
        return None
    if fresh:
        # the next adapter_mac below refills the whole table in one call,
        # so this costs one subprocess rather than one per adapter
        invalidate_adapter_mac()
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
        if _formatted(key) in _duplicate_macs:
            _warn_duplicate(key)
        return key
    name = str(adapter).strip()
    mac = adapter_mac(name)
    if mac != UNKNOWN_MAC:
        if mac in _duplicate_macs:
            # two cards answering to one address: the number is wrong as an
            # identity but it is at least unique, which beats merging them
            _warn_duplicate(mac)
            return _sanitize(name)
        return mac.replace(":", "").upper()
    return _sanitize(name)


def _formatted(key):
    return ":".join(key[i : i + 2] for i in range(0, len(key), 2))


def _warn_duplicate(mac):
    if mac in _warned_duplicate_macs:
        return
    _warned_duplicate_macs.add(mac)
    logger.warning(
        f"bt-claims: more than one adapter reports {mac} - counterfeit dongles often share "
        "an address. Claims for those cards fall back to hciN names, which are unstable "
        "across a reset; give them distinct addresses (bdaddr/hcitool) to key on identity."
    )


def legacy_key(adapter):
    """The pre-0.4 (hciN) claim-file key for an adapter, or None.

    Read-side compatibility: a process still running convention 0.3 names
    its claims hciN.*, and this fleet updates one service at a time.
    """
    # fresh=False: a stale number here costs at most a briefly missed
    # pre-0.4 claim, and this runs on every claim-name construction
    name = hci_for(adapter, fresh=False)
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

    It is evaluated ONLY by the owner's own heartbeat thread (_beat_once
    walks this process's held claims), so it cannot catch the owner's own
    death: a dead process beats nothing. A dead owner's claims persist as
    files until their mtime passes the TTL and another process's snapshot
    reaps them, or an acquirer takes the slot over. That path - TTL plus
    on-demand takeover - carries process death alone; validity covers only
    in-process divergence (field 2026-09-02).
    """

    def __init__(self, adapter, path, exclusive, service=None, holder=None, purpose=None, seq=None):
        self.adapter = adapter
        self.path = path
        self.exclusive = exclusive
        self.service = service
        # the holder file this claim's lock is a hardlink to, for the two
        # hardlinked exclusive names; None for everything else (soft claims,
        # link slots, scanwait tickets, and a legacy lock we adopted)
        self.holder = holder
        self.purpose = purpose
        # queue position stamp, for scanwait tickets
        self.seq = seq
        self.released = False
        # set when a heartbeat PROVES the lock is no longer ours: the
        # well-known name exists but points at a different inode. Distinct
        # from released, which is our own decision - lost is somebody else's
        # fact about us, and whatever the claim was backing (a running scan)
        # has to stop.
        self.lost = False
        self.validity = None

    def owns(self):
        """Whether the well-known name still refers to OUR file.

        The shared inode is the only proof there is. Comparing the name, or
        the pid written inside it, answers a nearby question: whether a claim
        exists and looks like ours - not whether it IS ours. A claim that was
        reaped and recreated by another process passes both of those and
        fails this one.
        """
        if self.holder is None:
            return not self.released
        try:
            return os.stat(self.path).st_ino == os.stat(self.holder).st_ino
        except OSError:
            return False

    def _note_lost(self):
        self.lost = True
        self.released = True
        if self.holder is not None:
            _unlink_quiet(self.holder)
        # WARNING, not debug: this is a coordination failure someone has to
        # be able to see in a log after the fact. It should not happen once
        # every participant is on 0.5 - the rename-aside takeover cannot
        # displace a live claim - so an instance of it is a bug report.
        logger.warning(
            f"bt-claims: lost exclusive claim {os.path.basename(self.path)} to another process - "
            "whatever it was backing must stop and re-acquire"
        )

    def _restore(self):
        """Rebuild the lock/holder pair after our holder file went missing.

        Only reachable when something reaped a file it should not have (a
        cleared directory, an over-eager sweep). Re-linking from the lock is
        tried first, because it keeps the inode - and therefore every other
        process's view of who holds the card - completely unchanged.
        """
        holder = _holder_path(self.path, self.service)
        try:
            if _read_pid(self.path) == os.getpid():
                os.link(self.path, holder)
                self.holder = holder
                return True
        except OSError:
            pass
        try:
            _write_holder_file(holder, self.service, self.purpose)
            os.link(holder, self.path)
        except OSError:
            _unlink_quiet(holder)
            return False
        self.holder = holder
        return True

    def touch(self):
        if self.released or self.lost:
            return
        if self.holder is None:
            self._touch_plain()
            return
        # the holder, not the lock: they share an inode, so one utime
        # freshens both, and touching the name we do not own would refresh
        # somebody else's claim for them
        try:
            os.utime(self.holder, None)
        except OSError:
            if not self._restore():
                self._note_lost()
                return
        if not self.owns():
            self._note_lost()

    def _touch_plain(self):
        try:
            os.utime(self.path, None)
        except OSError:
            # reaped or the directory was cleared: recreate on the beat. An
            # exclusive claim (a link slot, or a legacy lock we adopted)
            # recreates exclusively and concedes if another process took the
            # name meanwhile - stealing it back would ping-pong the card
            # between two owners.
            try:
                _write_claim_file(self.path, exclusive=self.exclusive, service=self.service)
            except FileExistsError:
                self.released = True
                logger.warning(f"bt-claims: lost exclusive claim {os.path.basename(self.path)} to another process")
            except OSError:
                pass

    def release(self):
        self.released = True
        if self.holder is None:
            _unlink_quiet(self.path)
            return
        if self.owns():
            # ENOENT here is a correct outcome, not an error: a taker
            # diagnosed us stale and renamed the lock aside first
            _unlink_quiet(self.path)
        _unlink_quiet(self.holder)


def _write_claim_file(path, exclusive, service=None):
    flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC
    if exclusive:
        flags |= os.O_EXCL
    fd = os.open(path, flags, 0o644)
    try:
        if service is None:
            # fallback for bare use; managers pass their owner string, which
            # is what makes `ls`-level debugging name the actual holder
            service = _default_service()
        os.write(fd, f"{os.getpid()} {service} {int(time.time())}\n".encode())
    finally:
        os.close(fd)


def _write_holder_file(path, service, purpose=None):
    """The holder half of a hardlinked exclusive claim.

    O_EXCL on a name that carries our pid and a per-process sequence number,
    so it cannot collide with anything - the exclusivity that decides the
    race is the os.link that follows, not this.
    """
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_EXCL, 0o644)
    try:
        line = f"{os.getpid()} {service or _default_service()} {int(time.time())}"
        if purpose:
            line += f" {_sanitize(purpose)}"
        os.write(fd, (line + "\n").encode())
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
        # optional callable(claim) invoked after every release, on whatever
        # thread released it (the heartbeat's validity sweep releases from
        # the beat thread). It exists so waiters queued on an exclusive name
        # wake the instant it frees instead of on their next poll tick. An
        # optimization only - every waiter still polls, and correctness never
        # depends on this firing. Exceptions swallowed, as for on_beat.
        self.on_release = None

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
            if path.endswith(_HARDLINKED):
                # An exclusive lock is NEVER unlinked on a liveness read.
                # This is the whole 0.5 TOCTOU fix: the stat, the pid read
                # and the unlink are three syscalls, and a claimant that
                # creates a fresh claim between the second and the third
                # would have it destroyed by a reader acting on a diagnosis
                # that was true when it was made. Stale locks are cleared by
                # _reap_stale_lock's atomic rename, at acquire time, by the
                # one process that is about to replace them.
                return False
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
                   "drain": bool, "drain_pid": pid-or-None,
                   "waiters": [(seq, service, pid, name), ...]}}

        hard_pid lets a caller distinguish a foreign scanner's hard claim
        from one held by its own process. waiters is the scanwait queue in
        service order (see _seq_counter for what that order can and cannot
        promise), which is what lets a would-be scanner pick the shortest
        queue instead of stampeding the same card as everyone else.

        The sweep is folded in here rather than run on its own timer: this
        is the one call every participant makes constantly, so the 0.5
        bookkeeping files (orphan holders, abandoned reaping files, stale
        queue tickets) are reaped by whoever is looking anyway.
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
            entry = state.setdefault(adapter, {"hard": None, "hard_pid": None, "soft": 0, "soft_owners": [], "links": 0, "drain": False, "drain_pid": None, "waiters": []})
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
            elif rest.startswith(_SCANWAIT_PREFIX):
                if self._is_live(path):
                    stamp = _parse_stamp(rest[len(_SCANWAIT_PREFIX) :])
                    if stamp is not None:
                        entry["waiters"].append((stamp[2], stamp[0], stamp[1], name))
            elif _REAPING_MARK in rest:
                self._reap_abandoned(path, rest)
            elif _HOLDER_MARK in rest:
                # the lock that links to it is counted under its own name;
                # this call is here purely for its reaping side effect, which
                # clears the holder a crashed process left behind
                self._is_live(path)
        for entry in state.values():
            entry["waiters"].sort()
        return state

    def _reap_abandoned(self, path, rest):
        """Clear a file left renamed-aside by a reaper that died mid-takeover.

        Judged on the pid in the FILENAME, not the content: the content
        belongs to the stale claim that was moved aside, while the name
        carries the pid of the process that moved it. Nothing waits on these
        - the lock name is already free - so this is tidying, not
        coordination.
        """
        stamp = _parse_stamp(rest.rpartition(_REAPING_MARK)[2])
        if stamp is None:
            _unlink_quiet(path)
            return
        if not _pid_alive(stamp[1]):
            _unlink_quiet(path)

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
            prefix, sep, rest = name.partition(".")
            if not sep or prefix not in wanted:
                continue
            if _is_bookkeeping(rest):
                # A holder file is the other half of a lock already counted
                # under its own name, and counting it would double every
                # exclusive claim. A scanwait ticket is not use at all: it
                # claims nothing and must never veto a reset, or a waiter
                # queued on a card the moment it started draining would hold
                # the drain off forever - the waiter is doing exactly what it
                # is supposed to do, which is nothing.
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
        usage, and counting it would make a resetter wait on itself. A
        scanwait ticket is excluded for the mirror-image reason given in
        foreign_use - a waiter is not a user, and one queued on a card as it
        began draining would otherwise deadlock the drain against itself.
        """
        with self._lock:
            held = list(self._held)
        # keyed off the claim's PATH, not the name it was created with: the
        # path carries the canonical key chosen when the claim was taken,
        # while the caller's spelling ("hci3") goes stale the moment the
        # card renumbers - which is the whole reason for this convention
        wanted = adapter_key(adapter)
        count = 0
        for claim in held:
            base = os.path.basename(claim.path)
            prefix, _sep, rest = base.partition(".")
            if prefix != wanted or claim.released:
                continue
            if base.endswith(".drain") or _is_bookkeeping(rest):
                continue
            count += 1
        return count

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

    def _reap_stale_lock(self, lockname):
        """Move a stale exclusive lock aside so it can be replaced.

        Rename, never unlink, and that is the entire point. Rename is atomic
        and picks exactly one winner: a second reaper racing the same file
        gets ENOENT and backs off, where two unlinkers would both "succeed"
        and the second would be destroying whatever the first had created.

        The winner then re-checks that the file it moved aside is the same
        inode it diagnosed stale. If it is not, a fresh claimant won the name
        between the diagnosis and the rename - the exact TOCTOU this replaces
        - so the file is linked back under its own name and we concede.
        os.link cannot clobber, so a third party that has since taken the
        name is left holding it.

        Returns whether the lock name is now clear for us to take.
        """
        try:
            stale_ino = os.stat(lockname).st_ino
        except FileNotFoundError:
            return True  # somebody else already cleared it; go take it
        except OSError:
            return False
        reaping = f"{lockname}{_REAPING_MARK}{_stamp(self.owner)}"
        try:
            os.rename(lockname, reaping)
        except OSError:
            return False
        try:
            moved_ino = os.stat(reaping).st_ino
        except OSError:
            return True
        if moved_ino != stale_ino or self._is_live(reaping):
            try:
                os.link(reaping, lockname)
            except OSError:
                pass
            _unlink_quiet(reaping)
            return False
        self._reap_holders_of(lockname, stale_ino)
        _unlink_quiet(reaping)
        logger.info(f"bt-claims: took over stale exclusive claim {os.path.basename(lockname)}")
        return True

    def _reap_holders_of(self, lockname, ino):
        """Unlink the holder files of a lock we have just reaped, matched by
        INODE rather than by name - the holder's owner string is whatever the
        dead process called itself, and guessing at it would either miss the
        file or delete a live claimant's."""
        base = os.path.basename(lockname) + _HOLDER_MARK
        try:
            names = os.listdir(self.claim_dir)
        except OSError:
            return
        for name in names:
            if not name.startswith(base):
                continue
            path = os.path.join(self.claim_dir, name)
            try:
                if os.stat(path).st_ino == ino:
                    _unlink_quiet(path)
            except OSError:
                continue

    def _acquire_exclusive(self, adapter, suffix, purpose=None):
        """Take one of the hardlinked exclusive names (.scan, .drain).

        Holder file first, then os.link onto the well-known name: EEXIST on
        the link is a lost race and nothing else, and there is no window in
        which a half-created claim is visible under the well-known name.
        """
        lockname, _legacy = self._names(adapter, suffix)
        if self._legacy_held(adapter, suffix):
            return None  # a pre-0.4 process holds this name for this card
        try:
            os.makedirs(self.claim_dir, exist_ok=True)
        except OSError as e:
            logger.debug(f"bt-claims: claim directory unusable for {suffix} on {adapter}: {repr(e)}")
            return None
        # at most one takeover attempt: a second EEXIST after we cleared a
        # stale lock means a live claimant beat us to the free name, and
        # retrying past that is how two processes ping-pong a card
        for _attempt in (0, 1):
            holder = _holder_path(lockname, self.owner)
            try:
                _write_holder_file(holder, self.owner, purpose)
            except OSError as e:
                logger.debug(f"bt-claims: could not write a holder for {adapter}.{suffix}: {repr(e)}")
                return None
            try:
                os.link(holder, lockname)
            except FileExistsError:
                _unlink_quiet(holder)
                if self._is_live(lockname):
                    return None
                if not self._reap_stale_lock(lockname):
                    return None
                continue
            except OSError as e:
                _unlink_quiet(holder)
                logger.debug(f"bt-claims: could not claim {adapter}.{suffix}: {repr(e)}")
                return None
            claim = Claim(adapter, lockname, exclusive=True, service=self.owner, holder=holder, purpose=purpose)
            self._hold(claim)
            return claim
        return None

    def claim_hard(self, adapter, purpose="scan"):
        """Take the adapter's single hard claim, or None if someone holds it.

        From 0.5 this is a GATE, not a hint: a caller that does not get one
        does not scan (see the module docstring for what treating it as
        advisory cost on 2026-08-26).
        """
        return self._acquire_exclusive(adapter, "scan", purpose)

    def claim_drain(self, adapter, purpose="reset"):
        """Take the adapter's single drain claim, or None if someone holds it.

        The exclusivity is the coordination: one process performs a recovery
        at a time, and a second would-be resetter backing off on None knows
        the card is already being handled.
        """
        return self._acquire_exclusive(adapter, "drain", purpose)

    def claim_scanwait(self, adapter):
        """Publish a queue ticket for this adapter's hard scan claim.

        Not a claim on the card: it gates nothing, it is invisible to
        occupancy scoring, and it never counts as use - so it cannot veto a
        reset, and a waiter parked on a card that starts draining does not
        deadlock the drain against itself. All it does is make the queue
        VISIBLE, which is what lets N would-be scanners spread over M cards
        instead of all piling onto the same best-ranked one and waking each
        other up forever.

        The returned Claim carries .seq, its position stamp.
        """
        try:
            os.makedirs(self.claim_dir, exist_ok=True)
        except OSError as e:
            logger.debug(f"bt-claims: cannot queue for {adapter}: {repr(e)}")
            return None
        seq = next(_seq_counter)
        name = f"{adapter_key(adapter)}.{_SCANWAIT_PREFIX}{_sanitize(self.owner)}-{os.getpid()}-{seq}"
        path = os.path.join(self.claim_dir, name)
        try:
            _write_claim_file(path, exclusive=False, service=self.owner)
        except OSError as e:
            logger.debug(f"bt-claims: cannot queue for {adapter}: {repr(e)}")
            return None
        claim = Claim(adapter, path, exclusive=False, service=self.owner, seq=seq)
        self._hold(claim)
        return claim

    def holder_info(self, adapter, suffix="scan"):
        """{"service", "pid", "age"} for the live holder of an exclusive name,
        or None. For naming names in a wait-timeout message: a scan that gave
        up after 30s is only actionable if it says who it was waiting on.
        """
        path, legacy = self._names(adapter, suffix)
        for candidate in (path, legacy):
            if not candidate or not self._is_live(candidate):
                continue
            try:
                with open(candidate) as f:
                    fields = f.read().split()
            except OSError:
                continue
            pid = int(fields[0]) if fields and fields[0].lstrip("-").isdigit() else None
            service = fields[1] if len(fields) > 1 else "unknown"
            age = None
            if len(fields) > 2:
                try:
                    age = max(0.0, time.time() - int(fields[2]))
                except ValueError:
                    age = None
            return {"service": service, "pid": pid, "age": age}
        return None

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
        self._fire_release(claim)

    def release_all(self):
        """Release every claim this manager holds."""
        with self._lock:
            held = list(self._held)
            self._held = []
        for claim in held:
            claim.release()
            self._fire_release(claim)

    def _fire_release(self, claim):
        hook = self.on_release
        if hook is None:
            return
        try:
            hook(claim)
        except Exception:
            logger.debug("bt-claims: on_release hook raised", exc_info=True)

    # -- placement ---------------------------------------------------------

    def choose(self, adapters):
        """Pick an adapter from a preference-ordered list and claim it soft.

        Draining adapters are dropped outright and there is no fallback to
        them: nothing NEW starts on a card someone is emptying (0.5), because
        a drain that new work keeps topping up never ends and the reset it
        precedes then fires - if it ever does - on a card that never actually
        emptied. Adapters hard-claimed by another live process are avoided;
        among the rest, fewer live soft claims plus held link slots wins,
        preference order breaks ties. When everything is hard-claimed the
        preferred adapter is used anyway: a scanner's claim must never keep
        this service off the air. (A SCAN has no such fallback - it queues on
        a scanwait ticket instead. See claim_scanwait.)

        Returns (adapter, Claim-or-None); (None, None) for an empty list, and
        for a list in which every adapter is draining - the caller should
        back off and retry, which is bounded by the drain's own deadline.
        """
        if not adapters:
            return None, None
        state = self.claims()
        usable = [a for a in adapters if not (state.get(a) or {}).get("drain")]
        if not usable:
            logger.info("bt-claims: every allowed adapter is draining; new work waits rather than joining a reset")
            return None, None
        open_adapters = [a for a in usable if not (state.get(a) or {}).get("hard")]
        if not open_adapters:
            logger.info(f"bt-claims: every allowed adapter is hard-claimed, using {usable[0]} anyway")
            open_adapters = list(usable)
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
                if claim.lost:
                    # touch() proved the lock is somebody else's now. Drop it
                    # from the held set so the release hook fires and whatever
                    # was backed by it (a running scan) is told to stop; the
                    # release itself unlinks nothing, because owns() is false.
                    self.release(claim)
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
