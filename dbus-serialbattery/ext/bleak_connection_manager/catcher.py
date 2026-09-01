# -*- coding: utf-8 -*-
"""Process-wide bleak client routing: the bleak catcher.

The same mechanism Home Assistant uses to put habluetooth underneath every
BLE integration: rebind bleak.BleakClient, process wide, to a wrapper that
routes connections through claim-aware adapter selection, per-adapter link
slots and failure-driven adapter rotation. Libraries that build their own
clients pick up the wrapper through their own `from bleak import
BleakClient`, provided install_bleak_catcher() runs before they are
imported. bleak_retry_connector is invariably imported before the catcher
installs, so its internal binding is permanently stale; its module
attributes (BleakClient, BleakClientWithServiceCache) are rebound too, which
covers the dominant establish_connection(BleakClientWithServiceCache, ...)
pattern.

The wrapper ROUTES; it does not retry. Retry semantics belong to whoever
drives the client (bleak-retry-connector above it), exactly as in Home
Assistant. Nesting retry inside the wrapper would multiply every caller's
retry budget.

This module's own name bindings were taken at import time, before any
install, so everything constructed internally uses the original classes and
can never recurse into the wrapper.
"""

import asyncio
import inspect
import logging
import os
import re
import subprocess
import time
import weakref

from bleak import BleakClient as _ORIGINAL_BLEAK_CLIENT
from bleak import BleakScanner as _ORIGINAL_BLEAK_SCANNER
from bleak.exc import BleakDBusError, BleakError

from . import mgmt, recovery, validators
from . import claims as claims
from .claims import CLAIM_DIR, CLAIM_TTL, HEARTBEAT_INTERVAL, ClaimManager

# habluetooth's scanner watchdog thresholds (const.py:96-108), derived
# backwards from BlueZ's 180s device expiry: restart after 90s of silence,
# checking every 30s; past 120s (or having never seen anything) escalate to
# a hardware reset. monotonic is indirected for tests.
SCANNER_WATCHDOG_TIMEOUT = 90.0
SCANNER_WATCHDOG_INTERVAL = 30.0
SCANNER_WATCHDOG_MULTIPLE = SCANNER_WATCHDOG_TIMEOUT + SCANNER_WATCHDOG_INTERVAL
_monotonic = time.monotonic

# RSSI sweeps for scan_to_score placement: habluetooth's active-window
# cadence (const.py:113-158 defaults: 300s interval, 10s duration). A sample
# older than a missed sweep plus the window is stale.
NO_RSSI_VALUE = -127
RSSI_SWEEP_INTERVAL = 300.0
RSSI_SWEEP_DURATION = 10.0
RSSI_STALE_SECONDS = RSSI_SWEEP_INTERVAL * 2 + RSSI_SWEEP_DURATION

# Notifications and completed GATT calls are proof the link is alive
# whatever bleak's connected flag says (field 2026-08-21: a broken D-Bus
# view read disconnected while data flowed). Evidence goes stale on the
# claim convention's own liveness bound, so a silently dead link is swept
# within one TTL - but that floor is only right for a consumer whose
# traffic is FASTER than it.
#
# A consumer whose cadence sits at or above the floor would flicker rather
# than lapse cleanly, which is the kind of thing that gets misfiled as a
# transient for months. The case that prompted this (2026-08-23) is
# power-watchdog: one long-lived notification session, and a driver that
# treats silence up to its own 120s liveness timeout as healthy. Its
# device's nominal ~30s cadence - equal to this floor, so a coin flip
# rather than a margin - is DERIVED FROM A COMMENT IN THAT DRIVER'S
# SOURCE, not measured: the unit was powered off and nobody has observed
# its traffic. Which is the argument for adapting rather than picking a
# bigger constant. The window is the floor, or a multiple of this client's
# own observed interval once it has shown one, capped so a genuinely dead
# link still frees its claims in bounded time - so whatever that device
# actually does, the window is sized from it at runtime and the unverified
# number never becomes load-bearing. Erring toward holding a claim too
# long is the convention's stated preference - a claim wrongly held is
# bounded by process life, a claim wrongly released overcommits the card.
LINK_EVIDENCE_SECONDS = CLAIM_TTL
LINK_EVIDENCE_MULTIPLE = 3.0
LINK_EVIDENCE_MAX = 300.0

# Ceiling on BlueZ's StartDiscovery/StopDiscovery. Both are unbounded
# D-Bus calls, and on a wedged adapter StartDiscovery simply never returns:
# field 2026-08-26, a consumer sat inside one for 2h45m, alive but doing
# nothing, with both its thermostats dead the whole time. The scan claim is
# taken BEFORE that call and its validity is armed AFTER it, so a hang left
# a HARD exclusive claim with no validity check attached - the heartbeat did
# not merely fail to release it, it refreshed it every beat for the whole
# 2h45m, denying that card to every other process on the box. A ceiling
# turns "hung forever, holding a radio hostage" into "failed, scored, and
# rotated off", which is a state the rest of this machinery can act on.
SCAN_OP_TIMEOUT = 30.0

# How long a scan waits for an adapter's hard claim before giving up, and how
# often it re-evaluates while it waits.
#
# There used to be no wait, because there was no gate: with every card
# claimed, a scan simply started on the best-ranked one anyway, on the
# reasoning that coordination is an optimization and must never keep a
# service off the air. On 2026-08-26 that reasoning cost the fleet a day.
# Two scans on one card means BlueZ answers the second StartDiscovery with
# InProgress; the wrapper had no way to tell that apart from a radio that
# would not start, so it scored a strike against a HEALTHY adapter; three
# strikes drained and power-cycled it; and every cycle mass-disappeared the
# devices on that card, which is the detonation path of the BlueZ 5.72
# gatt-client use-after-free. The unclaimed scan was not a graceful
# degradation, it was the input that made the evidence lie.
#
# So the claim is a GATE and the fallthrough is gone. A scan that cannot have
# a card queues for one and is told, in the error, exactly who is holding
# what. 30s is chosen against the things a waiter is actually waiting for: an
# RSSI sweep window is 10s, a drain deadline is 60s but frees the card the
# moment its reset finishes, and bleak-retry-connector's own retry budget sits
# well above this - so a wait long enough to ride out a sweep, and short
# enough that a stuck holder surfaces as a named failure rather than a hang.
SCAN_CLAIM_WAIT = 30.0
# The poll is the floor and the correctness guarantee. The in-process release
# event below is an optimization on top of it: a claim released by another
# PROCESS fires no event here, and neither does one released by a thread the
# event plumbing has not reached, so nothing may depend on an event arriving.
SCAN_CLAIM_POLL = 1.0

# Default ceiling for GATT operations and disconnects that arrive with no
# deadline of their own. This is NOT primarily about protecting the caller:
# an unbounded wait destroys the EVIDENCE. A call that can hang forever
# never becomes an observable failure, so "this adapter is stuck" never
# happens as an event and nothing downstream - scoring, rotation, recovery -
# can act on it. Bounding it is what converts a hang into a fact.
#
# So this wrapper does not pass "forever" through. Where a caller expresses
# a deadline (bleak's connect(timeout=), which bleak already honours) that
# deadline wins and nothing here interferes. Where the API offers no way to
# express one - read_gatt_char, write_gatt_char, start_notify, disconnect -
# a default applies, overridable per-process via install_bleak_catcher's
# gatt_timeout for a consumer whose device is legitimately slower.
GATT_OP_TIMEOUT = 30.0

# Recovery. Detecting a wedged card and rotating off it protects the fleet
# but guarantees the card stays dead: the drain-and-cycle machinery in
# recovery.reset_adapter was reachable only from the scanner watchdog, which
# requires a scanner that STARTED and then went quiet - so a card that hangs
# or refuses at StartDiscovery, or that the kernel has already taken DOWN,
# could never reach it. Worse, scoring a card as bad steers selection away
# from it, so the more evidence accumulated that a card is broken the less
# likely it was ever to be recovered. Recovery was coupled to successful use
# of the thing that was broken.
#
# So accumulated failure now triggers it. After this many scan failures on
# one card, the next one attempts a drain-and-cycle instead of merely
# rotating away - reset_adapter takes the exclusive drain claim, waits for
# other processes' links to migrate off, then rfkill/HCI-bounce/USB-reset.
SCAN_FAILURES_BEFORE_RESET = 3

# ...and this many attempts before declaring the card beyond our reach. A
# physically dead radio must not be power-cycled forever; giving up loudly
# is the signal a human should act on.
MAX_RECOVERY_ATTEMPTS = 3

# Probation, after the drain and before the cycle: how long the emptied
# card gets to show one real advertisement. Prod always has dozens of
# advertisers in range, so a card that reports nothing for this long while
# accepting the scan is the wedge the strikes were about, not a quiet room.
PROBE_SECONDS = 10.0
# ...and how long the one verifying link attempt gets. A scan proves the
# radio hears; only a link proves the card can talk (Clint, 2026-09-02:
# "after the scan don't we need a device to actually connect to verify
# it's healed"). One attempt: it fails, the card is cycled.
PROBE_CONNECT_SECONDS = 15.0

# Operator kill-switch for the drain-and-cycle path. A FILE rather than an
# env var or a config argument on purpose: it must be flippable fleet-wide,
# on a running box, without editing any consumer and WITHOUT RESTARTING
# ANYTHING - restarts re-register D-Bus clients and are themselves part of
# the crash pattern this switch exists around, so needing one to throw it
# would confound the experiment it enables. Checked live on every attempt,
# so touch/rm takes effect within seconds across every process. Under /data
# so it survives a reboot: a switch thrown to stop hardware being cycled
# must not quietly re-arm itself at 3am.
CYCLE_DISABLE_FLAG = "/data/bcm/no-card-cycle"

# DEFAULT IS ON: the flag DISABLES. This reverts the 2026-08-27 inversion,
# and the honest version of why is not "we decided it was fine after all".
#
# What was measured then stands: cycling a card - the USB port reset, or
# even the HCI down/up rung below it - makes every device on that adapter
# disappear at once, and device_disappeared -> device_free is the detonation
# path of the BlueZ 5.72 gatt-client use-after-free. Prod recorded 235
# bluetoothd SIGSEGVs on the day drain-and-cycle deployed, against 0 on each
# of the two preceding days with the same binary and the same consumers.
# Turning cycling off was the right emergency action, and if this box's
# BlueZ still lacks 476ae809a27e and a94f994201a6 (5.84 / 5.86), the flag
# below is how an operator keeps it off.
#
# What changed is not the blast radius, it is WHO CAN BE IN IT. Cycling was
# dangerous because a reset could fire on a card that was still carrying
# other people's links: scans started unclaimed on busy cards, InProgress
# was misread as radio failure, and strikes accumulated against healthy
# adapters until one was power-cycled with live devices on it. Convention
# 0.5 removes each of those in turn. Nothing scans without the card's hard
# claim (R1), so InProgress-as-evidence cannot happen. Nothing NEW starts on
# a draining card, with no carve-out for pins or explicit adapters (R3), so
# a drain actually converges. And the reset refuses outright while ANY claim
# is live, foreign or our own (R2), so a cycle can only ever fire on a card
# that emptied voluntarily - which means the devices that would be
# mass-removed have already gone somewhere else, under their own power.
#
# That is a property of the code, not of anyone's care, and it is the whole
# argument for defaulting on. Against it: a card that is genuinely wedged is
# also a card nobody is using, and steering around it forever guarantees it
# stays dead. Recovery that can never fire is not caution, it is a fleet
# quietly losing radios one at a time.
#
# The evidence gates in front of this are unchanged and still do the work:
# bluetoothd alive first, three strikes SPANNING RECOVERY_STRIKE_SPAN,
# MAX_RECOVERY_ATTEMPTS with proof-clears-attempts, daemon grace stand-down.

# one log line per adapter per armed/disarmed transition, not per attempt
_cycle_suppressed = set()


def card_cycling_disabled():
    """Whether drain-and-cycle is disarmed. False unless the flag exists."""
    try:
        return os.path.exists(CYCLE_DISABLE_FLAG)
    except OSError:
        # os.path.exists swallows its own errors, so this is close to
        # unreachable; if it ever is reached, an unreadable /data must not
        # silently take the fleet's only recovery path away
        return False


# adapter identity -> recovery attempts made; cleared by a successful reset
_recovery_attempts = {}
# adapter identities with a recovery in flight in THIS process (the drain
# claim handles the cross-process case)
_recovering = set()
_recovery_tasks = set()

# When bluetoothd itself is down, every scan on every card fails and the
# per-card failure evidence is meaningless. Monotonic timestamp of the last
# daemon-dead detection; while inside the grace window, card recovery stands
# down so strikes charged to healthy radios during the outage cannot cycle
# them. One daemon restart per window, uncapped across windows: a dead
# daemon takes ALL BLE on the box down, so the alternative to retrying is a
# permanently dark box (2026-08-26 prod: bluetoothd crash-looped, nothing
# restarted it for 8+ minutes, and the misread strikes power-cycled healthy
# cards 14 times).
_daemon_dead_at = None
DAEMON_RESTART_GRACE = 90.0


def _daemon_grace_active():
    return _daemon_dead_at is not None and time.monotonic() - _daemon_dead_at < DAEMON_RESTART_GRACE


# is_bluetoothd_alive walks /proc; on every failed connect of a retry storm
# that is hundreds of file opens a second across the fleet - its own load
# bug. The liveness QUESTION is throttled; the answer's freshness only
# matters at the 90s daemon-restart granularity anyway.
_last_daemon_check = None
DAEMON_CHECK_INTERVAL = 5.0


def _maybe_recover_daemon(adapter):
    """If bluetoothd is gone, schedule its restart and say so.

    Callable from any failure path - scan or connect - because a dead
    daemon fails BOTH, and a consumer that only ever connects (a battery
    driver with a pinned address never scans) would otherwise have no way
    to restart it. Returns whether the daemon was found dead, so callers
    can skip card-blaming gates that reason about evidence a dead daemon
    makes impossible.
    """
    global _last_daemon_check
    now = time.monotonic()
    if _last_daemon_check is not None and now - _last_daemon_check < DAEMON_CHECK_INTERVAL:
        return _daemon_grace_active()
    _last_daemon_check = now
    if recovery.is_bluetoothd_alive():
        return False
    if _daemon_grace_active():
        return True
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return True
    task = loop.create_task(_recover_daemon(adapter))
    _recovery_tasks.add(task)
    task.add_done_callback(_recovery_tasks.discard)
    return True


async def _recover_daemon(adapter):
    """bluetoothd is gone: restart it instead of cycling the card.

    The failures that queued this recovery were the daemon's, not the
    card's, so no card attempt is charged and every card's scan strikes are
    wiped - they are evidence about an outage, not about radios.
    """
    global _daemon_dead_at
    if _daemon_grace_active():
        return False
    _daemon_dead_at = time.monotonic()
    _scan_failures.clear()
    logger.warning(
        f"BLE scan: bluetoothd is not running - the failures charged to {adapter} "
        "are the daemon's, not the card's; restarting bluetoothd instead of cycling the card"
    )
    try:
        ok = bool(await recovery.restart_bluetoothd())
    except Exception:
        logger.exception("BLE scan: bluetoothd restart raised")
        return False
    if ok:
        try:
            await recovery.invalidate_dbus_state()
        except Exception:
            logger.debug("BLE scan: post-restart dbus-state invalidation failed", exc_info=True)
    return ok


def _probe_candidates(adapter, seen):
    """Devices the connect phase may try, best first: configured for THIS
    process (pinned to this card, and inside the allow-set when there is
    one), not currently linked by anyone anywhere, and seen advertising
    during the probe scan. Never a stranger: advertisement-seen is not
    consent to connect, and the probe is bound by THE RULE like any other
    connect."""
    config = _config
    if config is None:
        return []
    card = claims.adapter_key(adapter)
    snapshot = config.claims.claims()
    out = []
    for mac, adapters in config.pins.items():
        if not any(claims.adapter_key(a) == card for a in adapters):
            continue
        if config.allowed_devices is not None and claims.mac_key(mac) not in config.allowed_devices:
            continue
        if _address_key(mac) not in seen:
            continue
        qual = "." + _mac_qualifier(mac)
        # the qualifier is the last dot-segment BY CONSTRUCTION (_soft_path
        # appends it), so an endswith here is exact, not the trailing-
        # segment guess that bit a consumer
        busy = any(o.endswith(qual) for e in snapshot.values() for o in e.get("soft_owners", []))
        if busy:
            continue
        out.append(mac)
    return out


async def _probe_connect(adapter, address):
    client = BLEConnection(
        address, bluez={"adapter": adapter}, timeout=PROBE_CONNECT_SECONDS, _is_retry_client=True
    )
    try:
        await asyncio.wait_for(client.connect(), timeout=PROBE_CONNECT_SECONDS + 5.0)
        return bool(client.is_connected)
    except Exception as e:
        logger.warning(f"BLE scan: probe link to {address} on {adapter} failed: {repr(e)}")
        return False
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


async def _probe_adapter(adapter):
    """Use the drained card for real before cycling it (Clint, 2026-09-02).

    Emptying a card may itself release whatever was wedged - a stuck
    session, a stale discovery, a link the firmware would not let go of -
    so a card that has just drained gets one real use before the hardware
    is touched, in two phases, both judged by traffic. First a scan on
    that card: one real advertisement inside PROBE_SECONDS, or the cycle
    proceeds. Then, because a scan only proves the radio hears, one link:
    a device this process is configured for, pinned to this card, free
    everywhere, and just seen advertising - connected once and dropped.
    If it links, the card is back and its record clears through the
    normal establish path. If it will not, the cycle proceeds. When no
    such device is available to test, the scan verdict stands: a card
    that scans is not cycled for want of a test subject. Runs while our
    own drain claim still steers everyone else off the card.
    """
    seen = set()

    def _saw(device, _advertisement):
        seen.add(_address_key(getattr(device, "address", device)))

    try:
        scanner = BLEScanner(_saw, bluez={"adapter": adapter})
        await scanner.start()
    except Exception as e:
        logger.warning(f"BLE scan: probe of {adapter} after draining could not start a scan: {repr(e)}")
        return False
    heard = False
    try:
        deadline = _monotonic() + PROBE_SECONDS
        while _monotonic() < deadline:
            if scanner._catcher_last_detection != scanner._catcher_start_time:
                heard = True
                break
            await asyncio.sleep(0.25)
    finally:
        try:
            await scanner.stop()
        except Exception:
            pass
    if not heard:
        logger.warning(
            f"BLE scan: {adapter} accepted a scan but reported nothing for {PROBE_SECONDS:.0f}s "
            "after draining - cycling it"
        )
        return False
    candidates = _probe_candidates(adapter, seen)
    if not candidates:
        logger.warning(
            f"BLE scan: {adapter} is advertising again after draining alone; no configured device "
            "is free to test a link on it - accepting the scan, not cycled"
        )
        return True
    address = candidates[0]
    if await _probe_connect(adapter, address):
        logger.warning(f"BLE scan: {adapter} scanned and linked to {address} after draining alone - not cycled")
        return True
    logger.warning(f"BLE scan: {adapter} scans but could not link to {address} after draining - cycling it")
    return False


async def _recover_adapter(adapter):
    """Drain the card and cycle it, with attempt accounting.

    Safe by construction rather than by care, and each word of that is load
    bearing. reset_adapter takes the exclusive drain claim, so only one
    process anywhere attempts a given card. New work cannot land on a
    draining card, from any path, so the drain converges instead of being
    topped up. And the reset refuses outright while ANY claim is live -
    foreign or our own - so a cycle can only ever fire on a card that emptied
    voluntarily, which is precisely the wedged case and never the busy one.
    """
    key_early = claims.adapter_key(adapter)
    if card_cycling_disabled():
        # Absolute, and deliberately ahead of the daemon check: this switch
        # exists to take BCM's hands off the hardware entirely, so it must
        # not be reachable past by any branch below it. Daemon RESTART is
        # left alive - that touches no card and a dark box helps nobody.
        if key_early not in _cycle_suppressed:
            _cycle_suppressed.add(key_early)
            logger.warning(
                f"BLE scan: card cycling is DISARMED ({CYCLE_DISABLE_FLAG} exists) - "
                f"not draining or cycling {adapter}; it will be steered around instead"
            )
        if not recovery.is_bluetoothd_alive():
            return await _recover_daemon(adapter)
        return False
    _cycle_suppressed.discard(key_early)
    if not recovery.is_bluetoothd_alive():
        return await _recover_daemon(adapter)
    if _daemon_grace_active():
        # The daemon died within the window and may still be re-registering;
        # any strikes that survive are outage residue. No attempt charged.
        logger.info(
            f"BLE scan: standing down recovery of {adapter} - bluetoothd "
            f"restarted less than {DAEMON_RESTART_GRACE:.0f}s ago"
        )
        return False
    key = claims.adapter_key(adapter)
    attempt = _recovery_attempts.get(key, 0) + 1
    if attempt > MAX_RECOVERY_ATTEMPTS:
        return False
    _recovery_attempts[key] = attempt
    config = _config
    manager = config.claims if config is not None else None
    logger.warning(
        f"BLE scan: attempting recovery of {adapter} "
        f"(attempt {attempt}/{MAX_RECOVERY_ATTEMPTS}): draining, probing once, then cycling the card"
    )
    try:
        recovered = bool(await recovery.reset_adapter(
            adapter, claims_manager=manager, gone_silent=True, probe=_probe_adapter
        ))
    except Exception:
        logger.exception(f"BLE scan: recovery of {adapter} raised")
        recovered = False
    if recovered:
        # Deliberately NOT clearing _recovery_attempts here: reset_adapter's
        # success means the card re-enumerated with a readable MAC, which is
        # cosmetic - a card can enumerate forever and still never scan. Only
        # PROOF the radio works clears the record (_scan_finished /
        # _connect_finished on success), and that also caps a
        # cosmetically-recovering card at MAX_RECOVERY_ATTEMPTS resets
        # instead of an unbounded USB-reset churn loop (each reset
        # re-enumerates the bus and fails scans box-wide - 2026-08-26 prod
        # load incident).
        logger.warning(f"BLE scan: {adapter} recovered (attempt record kept until the radio proves itself)")
    elif attempt >= MAX_RECOVERY_ATTEMPTS:
        logger.error(
            f"BLE scan: {adapter} did not recover after {attempt} attempts and will not be "
            "retried - it needs physical attention (replug, or a port/power cycle)"
        )
    return recovered


def _schedule_recovery(adapter):
    """Queue a recovery if this card has failed enough to have earned one.

    Fire-and-forget deliberately: the caller is a connect or scan that has
    just failed and is about to raise, and draining a card can take up to a
    minute. Blocking the caller on recovery would turn one failed scan into
    a minute-long stall. By the time the task runs, the failing caller's
    own finally has released its claim, so the drain sees a free card.
    """
    if not adapter:
        return
    if card_cycling_disabled():
        # cheap early-out so a disarmed box does not even spawn the task;
        # _recover_adapter re-checks, since the flag can appear mid-flight
        return
    key = claims.adapter_key(adapter)
    if key in _recovering:
        return
    if not _scan_failures.get(key, 0):
        return
    # The daemon check comes BEFORE every per-card gate, deliberately. The
    # card gates below reason about card evidence - and with bluetoothd
    # dead, card evidence cannot exist: no scan can succeed, so no proof
    # ever clears an attempt record, and a process whose cards have burnt
    # their attempts would lose its only path to the daemon restart
    # (observed live 2026-08-26 15:48: second crash, guard starved, box
    # dark again). One failure is already enough to ask whether the daemon
    # is alive; _recover_daemon's own 90s window bounds the cost.
    if _maybe_recover_daemon(adapter):
        return
    if _scan_failures.get(key, 0) < SCAN_FAILURES_BEFORE_RESET:
        return
    since = _scan_failure_since.get(key)
    if since is None or time.monotonic() - since < RECOVERY_STRIKE_SPAN:
        # Enough strikes, but not enough TIME: a streak this young is what
        # transient churn (mass restart, a neighbour's USB reset) looks
        # like. The failures keep counting; recovery waits until the streak
        # has lasted RECOVERY_STRIKE_SPAN.
        return
    if _recovery_attempts.get(key, 0) >= MAX_RECOVERY_ATTEMPTS:
        return
    config = _config
    if config is not None:
        entry = config.claims.claims().get(key) or {}
        held = entry.get("links", 0) + entry.get("soft", 0)
        if held:
            # A card is cycled only when it is EMPTY. Draining a card that
            # carries links would steer every new placement off a card
            # somebody is using for 60s and then be vetoed anyway - all
            # cost, no reset. Placement already scores this card last for
            # its strikes; that is the whole remedy while it holds links.
            logger.info(
                f"BLE scan: {adapter} has {_scan_failures.get(key, 0)} failed scans but carries "
                f"{held} live claim(s) - not cycled; a card is cycled only when it is empty"
            )
            return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return

    async def _run():
        try:
            await _recover_adapter(adapter)
        finally:
            _recovering.discard(key)

    _recovering.add(key)
    task = loop.create_task(_run())
    _recovery_tasks.add(task)
    task.add_done_callback(_recovery_tasks.discard)

# Ceiling on closing a previous connection's D-Bus connection before
# reconnecting. Bounded because a wedged predecessor must not block the
# reconnect the caller actually asked for.
BACKEND_RETIRE_TIMEOUT = 10.0


# Ceiling on the courtesy disconnect of a link that failed validation: it is
# already known bad, so waiting on BlueZ past this costs the caller's retry
# budget for nothing (v1's DISCONNECT_TIMEOUT).
DISCONNECT_TIMEOUT = 5.0

try:
    import bleak_retry_connector as _brc_module

    _ORIGINAL_BRC_CLIENT = _brc_module.BleakClient
    _ORIGINAL_BRC_CACHE_CLIENT = _brc_module.BleakClientWithServiceCache
except (ImportError, AttributeError):
    _brc_module = None
    _ORIGINAL_BRC_CLIENT = None
    _ORIGINAL_BRC_CACHE_CLIENT = None

logger = logging.getLogger(__name__)


class OutOfConnectionSlotsError(BleakError):
    """Every eligible adapter's configured link capacity is fully claimed.

    The message starts with the literal substring "connection slot", which
    bleak-retry-connector string-matches (OUT_OF_SLOTS_ERRORS) to apply its
    4s out-of-slots backoff on any version, regardless of exception
    identity. It is raised before any backend is constructed and costs file
    stats only: brc draws it from the transient budget, so one
    establish_connection call may absorb it up to ~9 times.
    """


class ScanSlotWaitTimeout(BleakError):
    """No scan-eligible adapter's hard claim came free within SCAN_CLAIM_WAIT.

    Deliberately NOT an out-of-slots error: the message must not start with
    "connection slot", because this is not a connect and brc's out-of-slots
    backoff is not the right pacing for it. The message names every candidate
    and its holder (service, pid, how long it has held) plus this waiter's
    queue position, because a scan that gave up after 30s is only actionable
    if it says who it was waiting on - and a holder that never lets go is a
    bug in that holder, which nobody can find from "scan failed".
    """


class ConnectionValidationError(BleakError):
    """A connect succeeded but the caller's validator rejected the link.

    Raised after the link has been torn down and its claims released, so
    the retry loop above (bleak-retry-connector) sees an ordinary connect
    failure and attempts again - on the next adapter, since the failure is
    scored against the one that produced the unusable link.
    """


def parse_adapter_entries(entries):
    """
    Split configured adapter entries into pinned devices and the shared pool.

    Entries of the form MAC@hciX pin that device to that adapter, with no
    fallback to the shared pool. Repeating a MAC pins it to several adapters
    in order: the first is used for every connection attempt, the rest are
    tried only when it cannot be resolved there. That keeps a battery on a
    known good radio while leaving it somewhere to go if that radio fails,
    without returning it to a pool shared with other devices.

    Plain hciX entries form the pool used by every device that is not pinned.

    An adapter may be named either by its hciN number or by its OWN MAC, in
    any spelling (colons, dashes, dots, spaces or none; any case). The MAC
    is the stable identity - hciN numbering changes under a USB reset or a
    replug - so a MAC entry is kept verbatim and resolved to whatever hciN
    the card answers to at the moment it is used. A device MAC pinned to an
    adapter MAC is written the same way: DEVICE@ADAPTER.

    Returns (pins, pool), with pins keyed by upper case device MAC and each
    value a list of adapters in priority order.
    """
    pins = {}
    pool = []
    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue
        if "@" in entry:
            mac, _, adapter = entry.rpartition("@")
            mac = mac.strip().upper()
            adapter = adapter.strip()
            if mac and adapter:
                adapters = pins.setdefault(mac, [])
                if adapter not in adapters:
                    adapters.append(adapter)
            else:
                logger.warning(f"Ignoring malformed adapter entry '{entry}'")
        else:
            pool.append(entry)
    return pins, pool


def present_adapters():
    """Adapter names the kernel currently exposes, or an empty set for "no
    answer".

    hciN names are not stable identities: a USB reset or reboot renumbers
    them, and an adapter a device is configured for can stop existing while
    its number lives on pointing at different hardware. /sys/class/bluetooth
    is the kernel's view - an adapter can be present there while bluetoothd
    is not serving it; such an adapter fails its connect and consumes a walk
    step, acceptable because the walk is failure-driven. hciconfig is the
    fallback, for parity with the dbus-serialbattery fork's production code.
    """
    try:
        names = {name for name in os.listdir("/sys/class/bluetooth") if name.startswith("hci")}
    except OSError:
        names = set()
    if names:
        return names
    try:
        result = subprocess.run(["hciconfig"], capture_output=True, text=True, timeout=5)
        return set(re.findall(r"^(hci\d+):", result.stdout, re.MULTILINE))
    except Exception:
        return set()


def _address_key(address):
    return str(address).strip().upper()


def _mac_qualifier(address):
    return _address_key(address).replace(":", "")


class BleAdapterRotation:
    """Failure-driven adapter walk state for clients the catcher builds.

    The per-address index advances only when a connect attempt fails or a
    link dies young - a STABLE link's later drop is not a failure, so it
    reconnects on the adapter it was using, but a link that dies within
    QUICK_REDROP_WINDOW of establishing never really connected (field
    2026-08-30, prod RS pack: connected :24/dropped :28, connected :04/
    dropped :06, ping-ponging through an RF storm because each 4-second
    link wiped the record and re-armed the same path). The index never
    resets on success: once a device is talking
    over an adapter there is no reason to re-probe a preferred one that may
    be gone, and the modulo brings the list round to it again if this one
    later fails. State is per address and process wide, because one device's
    client may be rebuilt many times per session (bleak's context managers,
    bleak-retry-connector's attempts) and each rebuild must continue the
    walk, not restart it.
    """

    def __init__(self):
        self._attempt_index = {}

    def index(self, address):
        return self._attempt_index.get(_address_key(address), 0)

    def connect_failed(self, address):
        key = _address_key(address)
        self._attempt_index[key] = self._attempt_index.get(key, 0) + 1


_rotation = BleAdapterRotation()

# (adapter-identity, address) -> consecutive failed connect attempts,
# feeding the placement score; a STABLE link there clears it. Not a bare
# success: habluetooth's clear-on-success model let a 4-second link wipe
# the record during an RF storm and re-arm the same path (field
# 2026-08-30) - so the clear moved from the moment of establish to the
# moment a link proves it can live (_link_ended). Keyed by the card's
# identity rather than its number for the same reason claims are: after a
# renumber, penalties keyed by hciN would follow the NUMBER - a healthy
# card inheriting a bad one's number would inherit its record, and the bad
# card would shed it by moving.
_connect_failures = {}

# A link that dies this soon after establishing never really connected.
# 267/284 of the storm's disconnects were 0x08 supervision timeouts with
# the packs muted by shared external RF - rotation almost never helps
# reachability there, so the PRIMARY lever is the backoff below (damping
# churn); the ledger charge and rotation advance are side effects, kept
# because a card that only ever produces doomed links should still score
# below one that holds them.
QUICK_REDROP_WINDOW = 10.0
# per-address, escalating, capped: 2s, 4s, 8s, 16s, 30s. Applied BEFORE
# claim acquisition, so a backoff never holds a claim while sleeping.
QUICK_REDROP_BACKOFF_BASE = 2.0
QUICK_REDROP_BACKOFF_MAX = 30.0
# address -> consecutive quick drops; cleared by a link surviving the window
_quick_drop_streaks = {}


def _connect_finished(adapter, address, connected):
    if not adapter:
        return
    key = (claims.adapter_key(adapter), _address_key(address))
    if connected:
        # a completed link is proof the RADIO works, so the card-level
        # record clears at establish - recovery attempts AND scan strikes
        # (Clint, 2026-08-26: "if we get a successful connection we should
        # still clear the 3 strikes"). The (adapter, address) connect
        # record does NOT clear here any more: that verdict now waits for
        # the link to survive QUICK_REDROP_WINDOW (_link_ended), because a
        # 4-second link is proof the radio works and NO proof the path to
        # this device does.
        _recovery_attempts.pop(key[0], None)
        _scan_failures.pop(key[0], None)
    else:
        _connect_failures[key] = _connect_failures.get(key, 0) + 1
        # a failed connect is also the moment a connect-only consumer
        # notices a dead daemon - see _maybe_recover_daemon
        _maybe_recover_daemon(key[0])


def _link_ended(adapter, address, lived, ours):
    """Judge a link by how it died, not that it died.

    A link that survived the window proves the path and clears the record,
    whoever ended it. A short link WE ended says nothing about the radio -
    shyion's whole duty cycle is 3-8s connect/read/disconnect polls, and
    charging those would poison a healthy card's placement on every poll
    (`ours` also covers the cache-fault teardown after a failed
    StartNotify re-resolve: that is BlueZ's table, not the radio). Only a
    short link that died on its own charges: ledger, rotation, and the
    streak that prices the next attempt's backoff.
    """
    if not adapter:
        return
    akey = _address_key(address)
    key = (claims.adapter_key(adapter), akey)
    if lived >= QUICK_REDROP_WINDOW:
        _connect_failures.pop(key, None)
        _quick_drop_streaks.pop(akey, None)
    elif ours:
        return
    else:
        _connect_failures[key] = _connect_failures.get(key, 0) + 1
        _rotation.connect_failed(address)
        streak = _quick_drop_streaks.get(akey, 0) + 1
        _quick_drop_streaks[akey] = streak
        logger.info(
            f"BLE [{address}]: link on {adapter} dropped after {lived:.1f}s "
            f"(quick drop {streak} in a row) - charged as a connect failure"
        )


def _hci_sort_key(name):
    match = re.match(r"hci(\d+)$", name)
    return (0, int(match.group(1))) if match else (1, name)


def _device_path_adapter(address_or_ble_device):
    """The adapter baked into a resolved BLEDevice's BlueZ D-Bus path.

    bleak's BlueZ backend connects via device.details["path"] whenever the
    device carries one - the adapter argument is only honored when it has to
    scan - so for a cache-resolved BLEDevice (bleak-retry-connector's
    get_device, scanner-discovered devices) the device's own adapter is the
    truth. Treating it as caller-explicit keeps claims, cap gating and
    conn-param tuning on the adapter the link will actually use, instead of
    on one the backend would silently ignore.
    """
    details = getattr(address_or_ble_device, "details", None)
    if not isinstance(details, dict):
        return None
    match = re.match(r"/org/bluez/(hci\d+)/", str(details.get("path") or ""))
    return match.group(1) if match else None


def _resolve_adapter_entry(entry):
    """A configured adapter entry -> the hciN it names right now, or None.

    hciN entries pass through; a MAC entry is looked up against the cards
    the kernel currently exposes. None means "not present at the moment" -
    a card can be unplugged, or renumbered while we were not looking - and
    callers treat that the way they already treat an absent adapter.
    """
    text = str(entry).strip()
    if not text:
        return None
    key = claims.mac_key(text)
    if key is None:
        return text  # an hciN name (or something the kernel will reject)
    # fresh=False because _resolve_entries has already refreshed once for
    # the whole list - resolving each entry fresh would cost one hciconfig
    # call per configured adapter instead of one per placement
    return claims.hci_for(text, fresh=False)


def _resolve_entries(entries):
    """Configured entries -> present hciN names, order preserved, dropping
    the ones no card answers to. Also records every hciN entry that turned
    out to name a card with a readable MAC, for the config rewrite.

    A MAC entry is resolved AGAINST THE CURRENT NUMBERING, not against a
    cached mapping. Naming a card by its MAC is a statement that its number
    may change, so serving that lookup from a cache reintroduces exactly
    the staleness the MAC was chosen to avoid: for up to a TTL, a pinned
    device could be placed on whatever card has since inherited the number,
    which is the isolation failure pins exist to prevent. The invalidation
    is done once for the whole list rather than per entry, so the refill
    costs ONE hciconfig call however many adapters are configured
    (measured on a Cerbo: ~11ms fresh against ~19us cached), and entries
    already written as hciN pay nothing because there is nothing to
    resolve.
    """
    if any(claims.mac_key(str(entry).strip()) is not None for entry in entries):
        claims.invalidate_adapter_mac()
    resolved = []
    for entry in entries:
        name = _resolve_adapter_entry(entry)
        if name is None:
            continue
        _note_adapter_identity(entry, name)
        if name not in resolved:
            resolved.append(name)
    return resolved


# configured hciN entries observed to carry a real MAC: {entry: mac}. Filled
# on first successful read of each card and drained by the config rewrite.
_observed_identities = {}


def _note_adapter_identity(entry, hci_name):
    text = str(entry).strip()
    if claims.mac_key(text) is not None or text in _observed_identities:
        return
    mac = claims.adapter_mac(hci_name)
    if mac == claims.UNKNOWN_MAC:
        return
    _observed_identities[text] = mac
    config = _config
    if config is not None and config.adapter_config_path:
        rewrite_adapter_config(config.adapter_config_path, {text: mac})


def _entry(snapshot, adapter):
    """One adapter's row from a claims() snapshot, or an empty row.

    The snapshot is keyed by adapter IDENTITY (the card's MAC) while the
    catcher works in hciN, because that is what the kernel and bleak want.
    Every lookup has to cross that boundary, and a raw .get silently
    returns nothing on any host where MACs actually resolve - which is
    every real deployment, and none of the unit tests, which is exactly
    how this survived a green suite.
    """
    return snapshot.get(claims.adapter_key(adapter)) or {}


# A connect can fail for reasons that say nothing about the radio. The one
# seen in the field (2026-08-24, prod) is the system bus refusing a new
# connection once a process reaches dbus's per-user ceiling: bleak opens
# one D-Bus connection per connection session, so at the ceiling EVERY
# connect fails at once, on every adapter, until something frees a slot.
# Scoring those as adapter failures would rank every card as bad
# simultaneously and walk pinned devices off cards that are working
# perfectly, recording a fleet-wide radio problem that does not exist.
_RESOURCE_EXHAUSTION_MARKERS = (
    "limitsexceeded",                       # org.freedesktop.DBus.Error.LimitsExceeded
    "maximum number of active connections",  # its message text
    "too many open files",                   # EMFILE, the fd ceiling behind it
    "no buffer space",
)


def _is_resource_exhaustion(exc):
    """Whether a connect failure is a process/system resource limit rather
    than anything an adapter could be responsible for."""
    if isinstance(exc, OSError) and exc.errno in (23, 24):  # ENFILE, EMFILE
        return True
    text = f"{type(exc).__name__} {exc}".lower()
    return any(marker in text for marker in _RESOURCE_EXHAUSTION_MARKERS)


def _cap_for(config, adapter):
    """The configured link cap for an adapter, whichever way it was keyed.

    Caps may be written against hciN or against the card's MAC; the lookup
    canonicalizes both sides so a renumbered card keeps its cap.
    """
    # accepts the config object or a bare caps mapping
    caps = getattr(config, "link_caps", config)
    if not caps:
        return None
    if adapter in caps:
        return caps[adapter]
    key = claims.adapter_key(adapter)
    for name, cap in caps.items():
        if claims.adapter_key(name) == key:
            return cap
    return None


def _responsive_adapters(candidates):
    """Drop adapters whose sysfs MAC is all-zeros - the kernel's signal for
    a failed or unserved controller (habluetooth's FAILED_ADAPTER_MAC): a
    dead onboard UART controller stays listed in /sys/class/bluetooth
    forever and would otherwise win every tie. Never gates: when nothing
    passes (including hosts with no sysfs at all), the unfiltered list is
    used."""
    live = [a for a in candidates if recovery.adapter_mac(a) != recovery.UNKNOWN_MAC]
    return live or candidates


def _undrained_adapters(candidates, snapshot):
    """Drop adapters with a live drain claim. NO fallback: when every
    candidate is draining, this returns an empty list and the caller waits.

    The "or candidates" fallback that used to be here made a drain a ranking
    penalty rather than a gate, and a ranking penalty is not a thing a reset
    can be built on. Work placed onto a draining card tops the drain back up,
    so either the resetter never sees the card empty (and gives up at its
    deadline, having steered every other process off a card it then did not
    fix) or - worse - it proceeds against claims that arrived after it
    started counting. The reset in this package is only safe because it can
    ONLY fire on a card that emptied voluntarily, and that property is this
    filter having no exceptions: not for pins, not for caller-explicit
    adapters, not for the last card standing.

    A drain window is bounded - the resetter's own deadline, 60s - so the
    cost of waiting it out is bounded too. A scan queues (see
    _acquire_scan_adapter); a connect raises the typed out-of-slots error and
    bleak-retry-connector's 4s backoff paces the retries across the window.
    """
    return [a for a in candidates if not _entry(snapshot, a).get("drain")]


# adapter identity -> consecutive failed scanner starts, feeding scan
# placement; a successful start there clears it. Keyed by identity, not
# number, for the reason above.
_scan_failures = {}

# adapter identity -> monotonic time of the FIRST failure in the current
# streak. Recovery requires the streak to span RECOVERY_STRIKE_SPAN: three
# failures in ten seconds is what a mass service restart looks like (every
# scan contending at once), and a process ten seconds old USB-resetting
# shared hardware on that evidence amplified the 2026-08-26 restart into a
# box-wide load cascade - each reset re-enumerates the bus, fails scans in
# every OTHER process, and feeds their strike counters in turn.
_scan_failure_since = {}
RECOVERY_STRIKE_SPAN = 60.0


def _scan_finished(adapter, started):
    if not adapter:
        return
    key = claims.adapter_key(adapter)
    if started:
        _scan_failures.pop(key, None)
        _scan_failure_since.pop(key, None)
        # traffic proves the radio works, whatever fixed it. Drop the
        # recovery record too, or a card that exhausted its attempts stays
        # locked out of recovery forever - see _recovery_attempts.
        _recovery_attempts.pop(key, None)
    else:
        _scan_failures[key] = _scan_failures.get(key, 0) + 1
        _scan_failure_since.setdefault(key, time.monotonic())


def forget_adapter_failures(adapter):
    """Drop an adapter's accumulated RANKING penalties.

    Called after a successful hardware reset: every penalty in there was
    charged to a card that has since been power-cycled, so it is stale
    evidence about a radio that no longer exists in that state. Leaving it
    is not merely unfair, it is self-reinforcing for scans - the scan
    penalty ranks a card last, and a card ranked last is never selected to
    have the success that would clear it.

    The RECOVERY ATTEMPT record deliberately survives: a reset's "success"
    only proves the card re-enumerated with a readable MAC, and a card can
    enumerate forever without ever scanning again. Clearing attempts here
    made the reset loop unbounded (strike, reset, "success", strike...) -
    each USB reset re-enumerates the bus and fails scans box-wide, which
    is a load cascade during churn (2026-08-26 prod). Attempts clear only
    on proof the radio works: _scan_finished/_connect_finished success.
    """
    key = claims.adapter_key(adapter)
    _scan_failures.pop(key, None)
    _scan_failure_since.pop(key, None)
    for pair in [k for k in _connect_failures if k[0] == key]:
        _connect_failures.pop(pair, None)


# every connected client and started scanner, for the drain watcher; weak
# so the watcher can never keep a wrapper alive
_live_clients = weakref.WeakSet()
_live_scanners = weakref.WeakSet()
_migration_tasks = set()


def _spawn_migration(coro_fn, label):
    # runs inside the wrapper's own event loop (scheduled with
    # call_soon_threadsafe from the heartbeat thread)
    try:
        task = asyncio.get_running_loop().create_task(coro_fn())
    except RuntimeError:
        return
    _migration_tasks.add(task)
    task.add_done_callback(_migration_tasks.discard)


# The most recent event loop any wrapper ran on. The heartbeat thread has
# no loop of its own, and an idle process may hold no live client or
# scanner to borrow one from - which is exactly the process that needs the
# check below.
_last_loop = None


def _remember_loop(loop):
    global _last_loop
    _last_loop = loop


def _heartbeat_loop():
    for obj in list(_live_clients) + list(_live_scanners):
        loop = getattr(obj, "_catcher_loop", None)
        if loop is not None and not loop.is_closed():
            return loop
    loop = _last_loop
    return loop if loop is not None and not loop.is_closed() else None


def _heartbeat_daemon_check():
    """Notice a dead bluetoothd even when nothing is asking for BLE.

    Every other daemon check hangs off a FAILURE - a scan that would not
    start, a connect that would not complete. That makes the guard blind
    on an idle box: with no consumer attempting anything, nothing fails,
    so nothing looks, and the daemon stays dead until someone happens to
    want Bluetooth. Found on dev 2026-08-27 by the sensors-py session,
    whose service was stopped for an experiment: bluetoothd died and
    stayed down with no one to notice.

    The claim heartbeat ticks on a timer whether or not any BLE work is
    happening, which makes it the one place a purely idle process still
    looks at the world. _maybe_recover_daemon is itself throttled, so
    this costs one /proc walk per DAEMON_CHECK_INTERVAL at most.

    A box running NO BCM consumer still has no watchdog - that is
    Victron's supervision gap, not one we can close from here.
    """
    loop = _heartbeat_loop()
    if loop is None:
        return
    try:
        loop.call_soon_threadsafe(_maybe_recover_daemon, None)
    except Exception:
        logger.debug("BLE: heartbeat daemon check raised", exc_info=True)


def _lost_claim_watch():
    """Stop any scanner whose hard claim was proved to be somebody else's.

    The other half of the 0.5 ownership check (claims.Claim.owns). A claim
    marked lost means the well-known name now points at another process's
    file, so this scanner is scanning a card it does not hold - the exact
    state the gate exists to make impossible. It cannot simply carry on: the
    holder is entitled to that radio. Restarting rejoins the queue through
    the normal gate, which is where a scanner that lost a card belongs.

    Runs on the claim heartbeat thread, so the restart is marshalled onto the
    scanner's own loop like every other migration here.
    """
    for scanner in list(_live_scanners):
        claim = scanner._catcher_claim
        if claim is None or not getattr(claim, "lost", False):
            continue
        if scanner._catcher_restarting:
            continue
        loop = scanner._catcher_loop
        if loop is None or loop.is_closed():
            continue
        logger.warning(
            f"BLE scan: the hard claim on {scanner._catcher_adapter} is no longer ours - "
            "stopping this scan and rejoining the queue"
        )
        loop.call_soon_threadsafe(_spawn_migration, scanner._drain_restart, "reclaim")


def _drain_watch():
    """Honor foreign drain claims: move our SCANS off a draining card.

    Runs on the claim heartbeat thread (ClaimManager.on_beat). A running
    scanner on a draining adapter is restarted so selection lands it
    elsewhere. Live links are never touched - see the comment below - so a
    card carrying links simply keeps them, and its live claims keep vetoing
    the reset, which is the safe outcome. Each scanner is kicked at most
    once per adapter per start, so a rescan that lands back on the
    draining card (nothing else worked) is not bounced forever.
    """
    config = _config
    if config is None:
        return
    _heartbeat_daemon_check()
    _lost_claim_watch()
    snapshot = config.claims.claims()
    draining = {a for a, entry in snapshot.items() if entry.get("drain")}
    if not draining:
        return
    present = present_adapters()
    # Live LINKS are never kicked off a draining card (Clint, 2026-09-02: a
    # card is cycled only when it is empty). The version this replaces
    # disconnected connected clients here so the resetter could proceed -
    # a forced mass teardown, which on BlueZ 5.72 is the detonation path of
    # the gatt-client use-after-free and was the storm's rate amplifier.
    # Now the drain claim only steers NEW placements away; a card holding
    # links is not drained at all (_schedule_recovery refuses up front),
    # and if links appear during a drain, reset_adapter's own veto holds.
    # Scanners are still moved: a scan is a session, not a link, and moving
    # it tears down no device.
    for scanner in list(_live_scanners):
        adapter = scanner._catcher_adapter
        if adapter not in draining or scanner._catcher_restarting:
            continue
        if scanner._catcher_drain_kicked == adapter or scanner._catcher_explicit:
            continue
        others = [a for a in sorted(present, key=_hci_sort_key) if a not in draining]
        if not others:
            continue
        loop = scanner._catcher_loop
        if loop is None or loop.is_closed():
            continue
        scanner._catcher_drain_kicked = adapter
        logger.warning(f"BLE scan: {adapter} is draining, moving the scanner")
        loop.call_soon_threadsafe(_spawn_migration, scanner._drain_restart, "rescan")


# addresses already warned about a duplicate claimant, once per process
_warned_duplicate_claimants = set()


def _warn_duplicate_claimant(address):
    """Flag a second live instance of THIS service claiming the same device.

    The field signature of an orphaned driver process (prod 2026-08-22): a
    TERM-immune leftover survives the supervisor's restart and fights the
    new instance for the same battery, a ~8s connect/disconnect flap that
    reads exactly like radio failure and burned 45 minutes of diagnosis.
    The claim files already carry everything needed to name it: same owner
    base, different pid, same MAC qualifier. Warn once per address."""
    config = _config
    if config is None:
        return
    key = _address_key(address)
    if key in _warned_duplicate_claimants:
        return
    mac = _mac_qualifier(address)
    if not mac:
        return
    # the manager's owner is the sanitized "<service>-<pid>"; the base is
    # everything before our own pid suffix
    own = f"-{os.getpid()}"
    full = config.claims.owner
    base = full[: -len(own)] if full.endswith(own) else full
    try:
        names = os.listdir(config.claims.claim_dir)
    except OSError:
        return
    for name in names:
        rest = name.partition(".use.")[2]
        if not rest or not rest.endswith(f".{mac}"):
            continue
        holder = rest[: -len(mac) - 1]
        if not holder.startswith(f"{base}-") or holder == full:
            continue
        if not config.claims._is_live(os.path.join(config.claims.claim_dir, name)):
            continue
        _warned_duplicate_claimants.add(key)
        logger.warning(
            f"BLE [{address}]: another live instance of this service ({holder}) also claims this "
            "device - an orphaned process fighting this one produces a connect/disconnect flap "
            "that looks like radio failure. Check for a leftover pid."
        )
        return


# addresses already warned about bare connect() calls, once per process -
# a reconnect loop hitting this every few seconds would flood the log
_warned_bare_connect_addresses = set()

# addresses already refused by the allow-set, and addresses already warned
# for opting out of StartNotify - both once per process, for the same reason
_warned_refused_addresses = set()
_warned_acquire_notify_addresses = set()


class DeviceNotPermitted(Exception):
    """This process is not configured to talk to that device.

    Deliberately NOT a BleakError: bleak-retry-connector retries every
    BleakError up to max_attempts with backoff, and a policy refusal is
    not a transient condition - it must reach the caller immediately and
    unchanged. THE RULE (fleet root cause, 2026-08-26): a BLE peer set is
    bounded by stored configuration, never by radio range. Advertisement-
    seen is not consent to connect.
    """


def _parse_allowed_devices(entries):
    """Raw config strings -> frozenset of canonical MAC keys, or None.

    None, an empty string, or an empty iterable mean NOT CONFIGURED - no
    gate - by analogy with `adapters =` empty meaning every card. A
    frozenset, so it cannot be clobbered after load: the first silent
    gate bug shipped by a consumer was exactly a set overwritten after
    it was built. Keys are claims.mac_key so any spelling matches - the
    second silent bug was MAC matched as a trailing segment, which fails
    on indexed ids.
    """
    if entries is None:
        return None
    if isinstance(entries, str):
        entries = [e for e in re.split(r"[,\s]+", entries) if e]
    keys = set()
    for raw in entries:
        key = claims.mac_key(raw)
        if key is None:
            logger.warning(f"Ignoring allowed_devices entry '{raw}': not a MAC address")
            continue
        keys.add(key)
    if not keys:
        return None
    return frozenset(keys)


def _refuse_device(address, allowed):
    akey = _address_key(address)
    if akey not in _warned_refused_addresses:
        _warned_refused_addresses.add(akey)
        logger.warning(
            f"BLE [{address}]: connect refused - not in this process's allowed_devices "
            f"({len(allowed)} configured). A peer set is bounded by configuration, not radio range."
        )
    raise DeviceNotPermitted(f"{address} is not in this process's allowed_devices")


class _CatcherConfig:
    def __init__(self, owner, pins, pool, link_caps, claims, tune_conn_params, validate_connection=None):
        self.owner = owner
        self.pins = pins
        self.pool = pool
        self.adapter_config_path = None
        # frozenset of canonical MAC keys this process may connect to, or
        # None for no gate - see _parse_allowed_devices
        self.allowed_devices = None
        # ensure every start_notify in this process uses StartNotify - see
        # install_bleak_catcher(force_start_notify=)
        self.force_start_notify = False
        self.link_caps = link_caps
        self.claims = claims
        self.tune_conn_params = tune_conn_params
        self.gatt_timeout = GATT_OP_TIMEOUT
        self.validate_connection = validate_connection
        self.sweeper = None


class RssiSweeper:
    """Periodic short active scans that feed the placement score with RSSI.

    The configuring driver chooses its placement mode: without a sweeper,
    routing is least-used (occupancy and failures only - no RSSI exists
    because nothing scans); with one, each candidate adapter gets a short
    active window every RSSI_SWEEP_INTERVAL and the score gains its RSSI
    base, habluetooth style. Sweeps take the adapter's hard scan claim for
    the window - so other processes' placement and scans steer around them -
    and skip adapters another live process is already scanning on; the next
    cycle retries. Everything degrades: a sweep that cannot scan just
    leaves the score RSSI-less.
    """

    def __init__(self, config):
        self._config = config
        self._rssi = {}
        self._task = None

    def ensure_running(self):
        if self._task is not None and not self._task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._task = loop.create_task(self._run())

    def stop(self):
        task, self._task = self._task, None
        if task is not None:
            task.cancel()

    def record(self, adapter, address, rssi):
        self._rssi[(adapter, _address_key(address))] = (rssi, _monotonic())

    def rssi_for(self, adapters, address_key):
        """{adapter: rssi} for the fresh samples we hold on this address."""
        now = _monotonic()
        fresh = {}
        for adapter in adapters:
            entry = self._rssi.get((adapter, address_key))
            if entry is not None and now - entry[1] <= RSSI_STALE_SECONDS:
                fresh[adapter] = entry[0]
        return fresh

    async def _run(self):
        try:
            while True:
                for adapter in _scan_candidates(self._config, present_adapters()):
                    await self._sweep_adapter(adapter)
                await asyncio.sleep(RSSI_SWEEP_INTERVAL)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("RSSI sweep loop failed")

    async def _sweep_adapter(self, adapter):
        claim = self._config.claims.claim_hard(adapter)
        if claim is None:
            # another live process is scanning there; its window is not ours
            # to interrupt, and next cycle retries
            return
        scanner = None
        try:

            def _seen(device, advertisement_data):
                rssi = getattr(advertisement_data, "rssi", None)
                if rssi is not None:
                    self.record(adapter, getattr(device, "address", device), rssi)

            # both spellings with a fresh bluez dict, for the same
            # generation split and shared-default poison as BLEScanner.start
            scanner = _ORIGINAL_BLEAK_SCANNER(_seen, adapter=adapter, bluez={"adapter": adapter})
            await scanner.start()
            await asyncio.sleep(RSSI_SWEEP_DURATION)
        except Exception as e:
            logger.debug(f"RSSI sweep on {adapter} failed: {repr(e)}")
        finally:
            if scanner is not None:
                try:
                    await scanner.stop()
                except Exception:
                    pass
            self._config.claims.release(claim)


_config = None


def _out_of_slots_error(address, exhausted, config):
    """The typed exhaustion error, with per-adapter occupancy: when this
    fires on a GX device the occupancy detail is the whole diagnosis."""
    snapshot = config.claims.claims()
    detail = ", ".join(f"{adapter} ({_entry(snapshot, adapter).get('links', cap)}/{cap} links held)" for adapter, cap in exhausted)
    return OutOfConnectionSlotsError(f"connection slot exhausted for {address}: {detail}")


def _all_draining_error(address, adapters, config):
    """The typed error for "every candidate is being emptied for a reset".

    Same class and the same leading "connection slot" substring as real slot
    exhaustion, because it wants exactly the same treatment from the layer
    above: bleak-retry-connector string-matches that substring
    (OUT_OF_SLOTS_ERRORS) and applies its 4s out-of-slots backoff on any
    version. A drain window is bounded by the resetter's own deadline - 60s -
    and brc's paced retries ride that out, so the honest thing to tell it is
    "not now, pace yourself", which is precisely what this error means to it.

    There is no fallback onto a draining card for ANY connect - scored,
    pinned or caller-explicit. A pin is an operator's preference about which
    radio a device should prefer; it is not an instruction to walk into a
    reset in progress, and a carve-out for it would be a carve-out in the one
    property that makes the reset safe (see _undrained_adapters).
    """
    return OutOfConnectionSlotsError(
        f"connection slot unavailable for {address}: every candidate adapter is draining for a "
        f"reset ({', '.join(adapters) or 'none'}) - new work waits rather than joining one"
    )


def _score_order(eligible, address_key, snapshot, config, rssi=None):
    """Candidates best-first, by habluetooth-parity connect scoring.

    habluetooth scores connection paths as RSSI minus penalties
    (base_scanner.py:213-246), the penalties scaled by the spread between
    the two best paths (wrappers.py:586-640). In least-used mode there is
    no RSSI - nothing scans - so the base is 0 and the unit is 1 (their
    effective_rssi_diff floor for equal-RSSI paths); with an RssiSweeper
    feeding rssi, the base is the swept RSSI (NO_RSSI_VALUE when this
    address was never seen on that adapter) and the unit is the real
    spread. The in-progress term is generalized cross-process: live soft
    claims count every in-flight attempt and established link on the card,
    from every process. Failure counts stay local. A capped-full adapter
    sinks to the end rather than dropping out: the claim loop still visits
    it in case a slot freed since the snapshot, and real exhaustion is
    decided by the O_EXCL claim, not the score. Ties break by
    configuration order.
    """
    unit = 1.0
    if rssi:
        known = sorted(rssi.values(), reverse=True)
        if len(known) > 1:
            unit = max(known[0] - known[1], 1.0)
    scored = []
    for index, adapter in enumerate(eligible):
        entry = _entry(snapshot, adapter)
        cap = _cap_for(config, adapter)
        free = (cap - entry.get("links", 0)) if cap else None
        score = float(rssi.get(adapter, NO_RSSI_VALUE)) if rssi else 0.0
        score -= entry.get("soft", 0) * 1.01 * unit
        score -= _connect_failures.get((claims.adapter_key(adapter), address_key), 0) * 0.51 * unit
        if free is not None:
            if free <= 0:
                score -= 100000.0
            elif free == 1:
                score -= 0.76 * unit
        scored.append((-score, index, adapter))
    scored.sort()
    return [adapter for _, _, adapter in scored]


def _acquire_adapter(address):
    """Select an adapter for the next attempt and take its claims.

    Returns (adapter-or-None, claims-held). Pinned devices keep the
    deterministic failure-driven walk over their pin list - a pin is an
    explicit operator preference order, and only a failed connect moves a
    device off it. Unpinned devices are placed by scoring (_score_order)
    over the shared pool, or - with no pool configured - over every adapter
    the kernel currently exposes, so an unconfigured install spreads load by
    default and the pool config acts as an allowlist. Both paths filter by
    kernel presence and foreign live scan claims, each filter falling back
    to the unfiltered list rather than refusing to attempt. An adapter whose
    configured link capacity is fully claimed is passed over WITHOUT a
    failure being recorded: exhaustion says nothing about the radio. Raises
    OutOfConnectionSlotsError only when every eligible adapter is capped and
    full.
    """
    config = _config
    if config is None:
        return None, []
    address_key = _address_key(address)
    pins = config.pins.get(address_key)
    present = present_adapters()
    if pins:
        candidates = _resolve_entries(pins)
    elif config.pool:
        candidates = _resolve_entries(config.pool)
    elif present:
        candidates = sorted(present, key=_hci_sort_key)
    else:
        candidates = []
    if not candidates:
        return None, []
    usable = [a for a in candidates if a in present] if present else candidates
    if not usable:
        # refusing to attempt is worse than trying an adapter that may be gone
        usable = candidates
    usable = _responsive_adapters(usable)
    snapshot = config.claims.claims()
    own_pid = os.getpid()

    def foreign_scan(adapter):
        entry = _entry(snapshot, adapter)
        return bool(entry and entry["hard"] and entry["hard_pid"] != own_pid)

    eligible = [a for a in usable if not foreign_scan(a)]
    if not eligible:
        logger.info(f"BLE [{address}]: every usable adapter is scan-claimed by another process, using them anyway")
        eligible = usable
    drain_free = _undrained_adapters(eligible, snapshot)
    if not drain_free:
        raise _all_draining_error(address, eligible, config)
    eligible = drain_free
    if pins:
        start = _rotation.index(address) % len(eligible)
        ordered = eligible[start:] + eligible[:start]
    else:
        rssi = config.sweeper.rssi_for(eligible, address_key) if config.sweeper is not None else None
        ordered = _score_order(eligible, address_key, snapshot, config, rssi)
    if logger.isEnabledFor(logging.DEBUG):
        occupancy = {a: _entry(snapshot, a) for a in ordered}
        logger.debug(
            f"BLE [{address}]: adapter order {ordered} "
            f"({'pinned walk' if pins else 'scored'}, occupancy {occupancy})"
        )
    exhausted = []
    for adapter in ordered:
        cap = _cap_for(config, adapter)
        if cap:
            slot = config.claims.claim_slot(adapter, cap)
            if slot is None:
                exhausted.append((adapter, cap))
                continue
        else:
            slot = None
        soft = config.claims.claim_soft(adapter, qualifier=_mac_qualifier(address))
        return adapter, [c for c in (slot, soft) if c is not None]
    raise _out_of_slots_error(address, exhausted, config)


def _claim_explicit(adapter, address):
    """Claims for an adapter fixed by the caller - an explicit kwarg, or a
    resolved BLEDevice whose BlueZ path already names its adapter.

    The choice is never overridden. A soft claim is always written so the
    connection counts in every process's occupancy score - most real
    connects arrive as cache-resolved BLEDevices, and leaving them
    invisible would blind least-used placement fleet-wide. A configured
    link cap still gates: the cap is physics, not coordination - the
    connect is doomed when the card is full, and the typed error buys
    correct pacing from bleak-retry-connector.
    """
    config = _config
    if config is None:
        return []
    # ...but a drain still gates, and this is the one place it must, because
    # an explicit adapter is the last path that could still put new work onto
    # a card being emptied. The caller's choice is honoured about WHICH card,
    # never about whether a reset in progress may be walked into.
    if _draining_for_others(_entry(config.claims.claims(), adapter)):
        raise _all_draining_error(address, [adapter], config)
    cap = _cap_for(config, adapter)
    if cap:
        slot = config.claims.claim_slot(adapter, cap)
        if slot is None:
            raise _out_of_slots_error(address, [(adapter, cap)], config)
    else:
        slot = None
    soft = config.claims.claim_soft(adapter, qualifier=_mac_qualifier(address))
    return [c for c in (slot, soft) if c is not None]


class BLEConnection(_ORIGINAL_BLEAK_CLIENT):
    """Drop-in BleakClient that picks its adapter and claims at connect time.

    Construction stores the arguments and builds nothing, because the
    adapter can only be chosen when the connection is actually attempted -
    bleak wires the adapter into its platform backend inside __init__, and
    callers like aiobmsble construct placeholder clients long before they
    connect. connect() runs the real __init__ with the routed adapter merged
    into the bluez args, then delegates; every reconnect on the same
    instance re-runs the selection, which is what lets a retrying caller
    walk on to the next radio after a failure. Every other bleak method
    works via inheritance - they all delegate to self._backend.
    """

    def __init__(self, address_or_ble_device, disconnected_callback=None, services=None, **kwargs):
        # bleak-retry-connector marks every client its establish_connection
        # constructs; habluetooth's wrapper checks the same marker. Without
        # it, connect() below is a single unretried attempt. Popped, never
        # passed down: the real __init__ does not know it.
        self._catcher_is_retry_client = kwargs.pop("_is_retry_client", False)
        # Popped for the same reason: the real __init__ does not know it.
        # establish_connection passes its surplus kwargs straight to the
        # client class, which is how a caller reaches this without ever
        # constructing the client itself.
        self._catcher_validate = kwargs.pop("validate_connection", None)
        self._catcher_args = (address_or_ble_device, disconnected_callback, services)
        self._catcher_kwargs = kwargs
        # str() in case a caller hands us a subclassed str (habluetooth
        # guards the same way)
        self._catcher_address = str(getattr(address_or_ble_device, "address", address_or_ble_device))
        self._catcher_claims = []
        self._catcher_manager = None
        # each connect() call is a generation; a disconnected callback may
        # only release the claims of the generation that created it
        self._catcher_generation = 0
        self._catcher_adapter_used = None
        # True while there is deliberately nothing to account for (never
        # connected, or disconnect() ran): data arriving then must not
        # re-arm claims
        self._catcher_settled = True
        # monotonic stamp of the current link's establish; None when no link
        # has been up this generation. _link_ended consumes and clears it, so
        # a duplicate disconnect event cannot charge the same death twice.
        self._catcher_established_at = None
        # set when the StartNotify cache-recovery path gave up: the teardown
        # that follows is charged to BlueZ's GATT table, not the radio
        self._catcher_cache_fault = False
        self._catcher_last_evidence = None
        # smoothed interval between this client's traffic events, sizing the
        # evidence window to the cadence it actually shows
        self._catcher_traffic_gap = None
        self._catcher_last_rearm = None
        self._catcher_loop = None
        self._catcher_explicit = False
        # the generation whose connect() is in flight right now, or 0. A
        # disconnected event arriving while this is set describes a link
        # that is not up yet, not one that went down.
        self._catcher_connecting = 0
        # the adapter this client was already kicked off during a drain -
        # at most one forced migration per adapter per connect
        self._catcher_drain_kicked = None
        self._backend = None
        # bleak's backend_id property reads this, but the real __init__ that
        # would set it only runs at connect(); seed it so placeholders answer
        self._backend_id = ""
        self._pair_before_connect = False

    def _release_claims(self):
        held, self._catcher_claims = self._catcher_claims, []
        manager = self._catcher_manager
        for claim in held:
            if manager is not None:
                manager.release(claim)
            else:
                claim.release()

    def _warn_bare_connect(self):
        key = _address_key(self._catcher_address)
        if key in _warned_bare_connect_addresses:
            return
        _warned_bare_connect_addresses.add(key)
        logger.warning(
            f"BLE [{self._catcher_address}]: BleakClient.connect() called without bleak-retry-connector. "
            "A bare connect() is a single attempt with no recovery; connect through "
            "bleak_retry_connector.establish_connection() instead."
        )

    def _make_disconnected_callback(self, raw_callback, generation):
        # bleak wraps the raw callable in functools.partial(cb, self) inside
        # the real __init__, so this wrapping must happen on the raw
        # callable: claims are released before the caller hears about the
        # drop, and an unexpected drop frees its link slot. Release is
        # guarded twice (field 2026-08-21: claims vanished while the link
        # lived): only the callback of the CURRENT connect generation may
        # release - a reconnect on this instance leaves the previous
        # backend's late disconnect event closing over the same wrapper,
        # and the claims it would free were never its to account for - and
        # only when the wrapper's own view agrees the link is down. A
        # spurious event with the link still up releases nothing; if the
        # link really is gone, the validity heartbeat sweeps within a beat.
        #
        # Third guard (field 2026-08-23, prod shyion: 4 of 28 transient
        # sessions): a disconnected event can arrive DURING the handshake,
        # before the link has ever been up. Both guards above pass - it is
        # genuinely the current generation, and is_connected is genuinely
        # False because the link is not up YET - so the claims connect()
        # just took were released a second before first traffic re-armed
        # them. "Not up yet" is not "went down", and the difference is not
        # visible in either of the other two tests. While connect() is in
        # flight nothing here releases: if that connect fails, its own
        # finally releases the claims, and if the link dies later the
        # heartbeat sweeps it.
        def _disconnected(client):
            if generation == self._catcher_generation and self._catcher_connecting == generation:
                logger.debug(
                    f"BLE [{self._catcher_address}]: disconnect event during the connect "
                    "handshake, ignored (the link is not up yet, not down)"
                )
                if raw_callback is not None:
                    raw_callback(client)
                return
            if generation == self._catcher_generation and not self.is_connected:
                established = self._catcher_established_at
                if established is not None:
                    self._catcher_established_at = None
                    # settled means the teardown came through this wrapper
                    # (disconnect(), or the failed-validation teardown) -
                    # ours, not the radio's. Cache faults are ours too: the
                    # StartNotify re-resolve path failed on BlueZ's table.
                    _link_ended(
                        self._catcher_adapter_used,
                        self._catcher_address,
                        _monotonic() - established,
                        self._catcher_settled or self._catcher_cache_fault,
                    )
                if any(not c.released for c in self._catcher_claims):
                    # The release reason, on the record: if this line is
                    # followed by a traffic-based re-arm, the property lied
                    # (2026-08-22: prod lost a claim invisibly because this
                    # sat below the deployed level). INFO only for a drop
                    # the radio produced; a teardown the consumer requested
                    # (settled) is routine, its own session-end line already
                    # says why, and at ~2/h it was the loudest line on the
                    # box once the consumers went quiet (2026-09-02).
                    stamp = self._catcher_last_evidence
                    age = "never" if stamp is None else f"{_monotonic() - stamp:.0f}s ago"
                    emit = logger.debug if self._catcher_settled else logger.info
                    emit(
                        f"BLE [{self._catcher_address}]: disconnect event, is_connected False, "
                        f"releasing claims (last link traffic: {age}"
                        f"{', teardown requested' if self._catcher_settled else ''})"
                    )
                self._release_claims()
            if raw_callback is not None:
                raw_callback(client)

        return _disconnected

    def _evidence_window(self):
        """How long this client's silence stays consistent with a live link.

        The floor for a consumer that has not shown its cadence yet, and a
        multiple of its own observed traffic interval once it has.
        """
        gap = self._catcher_traffic_gap
        if not gap:
            return LINK_EVIDENCE_SECONDS
        return min(LINK_EVIDENCE_MAX, max(LINK_EVIDENCE_SECONDS, gap * LINK_EVIDENCE_MULTIPLE))

    def _recent_link_evidence(self):
        stamp = self._catcher_last_evidence
        return stamp is not None and _monotonic() - stamp <= self._evidence_window()

    def _arm_claim_validity(self):
        # Backstop for the release paths: once the connection is up, its
        # claims are validity-checked on every heartbeat, so if the link
        # dies without the disconnected callback ever firing (a torn-down
        # D-Bus watch, an abandoned client object) the slot and soft claim
        # free themselves within a TTL instead of living until process
        # exit. Armed only after a successful connect - a slow in-flight
        # attempt reads as not-connected and must not be swept. Validity is
        # link truth, not wrapper liveness: recent link traffic - a
        # notification or a completed GATT read/write - counts even when
        # is_connected reads False (a broken D-Bus view),
        # and a wrapper collected while its backend still holds the BlueZ
        # link leaves the backend as the link's representative.
        ref = weakref.ref(self)
        backend_ref = weakref.ref(self._backend) if self._backend is not None else (lambda: None)

        def _link_alive():
            client = ref()
            if client is not None:
                return client.is_connected or client._recent_link_evidence()
            backend = backend_ref()
            return backend is not None and bool(getattr(backend, "is_connected", False))

        for claim in self._catcher_claims:
            claim.validity = _link_alive

    def _make_notify_tap(self, callback):
        # every notification is the link's proof of life; the tap mirrors
        # the caller's coroutine-ness because bleak decides sync-vs-async
        # handling by inspecting the callback it is handed
        if inspect.iscoroutinefunction(callback):

            async def _notify(sender, data):
                self._note_link_evidence()
                await callback(sender, data)

            return _notify

        def _notify(sender, data):
            self._note_link_evidence()
            return callback(sender, data)

        return _notify

    def _note_link_evidence(self):
        now = _monotonic()
        previous = self._catcher_last_evidence
        if previous is not None:
            # smoothed rather than last-gap, so one late notification does
            # not swing the window, and one early one does not shrink it
            gap = now - previous
            current = self._catcher_traffic_gap
            self._catcher_traffic_gap = gap if current is None else (current * 0.7 + gap * 0.3)
        self._catcher_last_evidence = now
        if self._catcher_settled or self._backend is None:
            return
        if any(not claim.released for claim in self._catcher_claims):
            return
        now = _monotonic()
        if self._catcher_last_rearm is not None and now - self._catcher_last_rearm < HEARTBEAT_INTERVAL:
            return
        self._catcher_last_rearm = now
        self._rearm_claims()

    def _rearm_claims(self):
        # The recovery for claims lost while the link lived (a spurious
        # release the guards could not see): traffic is flowing - notified
        # or polled - so the connection re-acquires the accounting it held
        # at connect time.
        # The link exists regardless of what the files say - losing the
        # slot race to another process degrades to a soft claim, never to
        # dropping the connection.
        config = _config
        adapter = self._catcher_adapter_used
        if config is None or not adapter:
            return
        claims = []
        cap = _cap_for(config, adapter)
        if cap:
            slot = config.claims.claim_slot(adapter, cap)
            if slot is not None:
                claims.append(slot)
        soft = config.claims.claim_soft(adapter, qualifier=_mac_qualifier(self._catcher_address))
        if soft is not None:
            claims.append(soft)
        if not claims:
            return
        logger.warning(
            f"BLE [{self._catcher_address}]: link on {adapter} is alive (traffic flowing) "
            "but its claims were lost, re-claimed"
        )
        self._catcher_claims = claims
        self._catcher_manager = config.claims
        self._arm_claim_validity()

    async def _run_validation(self, validate, adapter):
        """Run the caller's validator; tear the link down if it says no.

        v1's contract, kept whole: a validator that returns False - or
        raises, which counts as False - fails the connect. The link is
        disconnected first, because a rejected link left up is exactly the
        phantom the caller was validating against, and BlueZ would hold it
        until something else cleared it. The claims and the failure scoring
        are the caller's ordinary connect-failure path, which the raise
        below hands back to.
        """
        try:
            valid = bool(await validate(self))
        except asyncio.CancelledError:
            await self._teardown_rejected()
            raise
        except Exception:
            logger.debug(
                f"BLE [{self._catcher_address}]: validate_connection raised, treating as failed",
                exc_info=True,
            )
            valid = False
        if valid:
            return
        logger.info(
            f"BLE [{self._catcher_address}]: connected on {adapter or 'the caller-chosen adapter'} "
            "but validate_connection rejected the link, tearing it down"
        )
        await self._teardown_rejected()
        raise ConnectionValidationError(f"{self._catcher_address}: connection failed validation")

    async def _teardown_rejected(self):
        # Best effort by design: the link is known bad either way, and the
        # claims it held are released by connect()'s own failure path.
        # Settled first so a straggler notification cannot re-arm claims
        # for a link being taken down.
        self._catcher_settled = True
        try:
            await asyncio.wait_for(_ORIGINAL_BLEAK_CLIENT.disconnect(self), timeout=DISCONNECT_TIMEOUT)
        except Exception:
            logger.debug(
                f"BLE [{self._catcher_address}]: disconnect after failed validation raised",
                exc_info=True,
            )

    async def connect(self, *, validate_connection=None, **kwargs):
        if not self._catcher_is_retry_client:
            self._warn_bare_connect()
        allowed = _config.allowed_devices if _config is not None else None
        if allowed is not None and claims.mac_key(self._catcher_address) not in allowed:
            # before the backoff sleep, before claims, before a backend:
            # construction and init must never open a session, and a
            # refused device must not cost the card anything either
            _refuse_device(self._catcher_address, allowed)
        address_or_ble_device, raw_callback, services = self._catcher_args
        init_kwargs = dict(self._catcher_kwargs)
        streak = _quick_drop_streaks.get(_address_key(self._catcher_address), 0)
        if streak:
            # Damping, the primary lever for RF storms: the packs mute in
            # correlated bursts, the peer is usually advertising again by
            # reconnect time, and rotation rarely changes reachability - so
            # price the retry, do not race it. BEFORE claim acquisition, so
            # the sleep never holds a claim; cancellation lands here first
            # and cancels a sleep, not a half-taken claim set.
            delay = min(QUICK_REDROP_BACKOFF_BASE * (2 ** (streak - 1)), QUICK_REDROP_BACKOFF_MAX)
            logger.info(
                f"BLE [{self._catcher_address}]: {streak} quick drop(s) in a row, "
                f"backing off {delay:.0f}s before reconnecting"
            )
            await asyncio.sleep(delay)
        # a reconnect on this instance must not leak the previous claims,
        # and opens a new generation: stale disconnect events from the
        # backend being replaced lose their power to release
        self._release_claims()
        # a new session measures its own cadence: a gap spanning the previous
        # session would size the window off an idle period, not traffic
        self._catcher_last_evidence = None
        self._catcher_traffic_gap = None
        self._catcher_generation += 1
        generation = self._catcher_generation
        # in flight from here until connect() returns either way: the
        # backend is constructed below and can fire its disconnected
        # callback the moment it exists
        self._catcher_connecting = generation
        # after the generation bump, so the retiring backend's own
        # disconnected callback is already neutered and cannot release the
        # claims this connect is about to take
        await self._retire_previous_backend()
        config = _config
        self._catcher_manager = config.claims if config is not None else None
        if config is not None and config.sweeper is not None:
            # lazily started here because this is the first moment a
            # running event loop is guaranteed
            config.sweeper.ensure_running()
        adapter = None
        requested = init_kwargs.get("adapter") or (init_kwargs.get("bluez") or {}).get("adapter")
        bound = _device_path_adapter(address_or_ble_device)
        if bound and requested and claims.adapter_key(bound) != claims.adapter_key(requested):
            # The device already carries a BlueZ path, and bleak connects
            # via that path whatever adapter it is told - the adapter
            # argument is honoured only when bleak has to scan. So the
            # path names the card the link WILL be on, and the caller's
            # request names a card it will not touch; claiming the request
            # would put every claim, cap and drain decision on the wrong
            # card. Field 2026-09-02, prod RS pack: bleak-retry-connector
            # swaps a caller's BLEDevice for BlueZ's already-CONNECTED copy
            # before building the client (an adopted link, on whatever
            # card it lingered on) and passes the caller's adapter kwarg
            # through unchanged - the claim named hci5, the link was on
            # hci3, and the empty-card rule would have called hci3 free.
            logger.warning(
                f"BLE [{self._catcher_address}]: caller asked for {requested} but the device is "
                f"already bound to {bound} (BlueZ path) - the link will be on {bound}; claiming there"
            )
        explicit = bound or requested
        if explicit:
            self._catcher_claims = _claim_explicit(explicit, self._catcher_address)
        else:
            adapter, self._catcher_claims = _acquire_adapter(self._catcher_address)
        try:
            if adapter:
                bluez = dict(init_kwargs.get("bluez") or {})
                bluez.setdefault("adapter", adapter)
                init_kwargs["bluez"] = bluez
            callback = raw_callback
            if self._catcher_claims or raw_callback is not None:
                callback = self._make_disconnected_callback(raw_callback, generation)
            _ORIGINAL_BLEAK_CLIENT.__init__(
                self,
                address_or_ble_device,
                disconnected_callback=callback,
                services=services,
                **init_kwargs,
            )
        except BaseException:
            # claims must not outlive a client that never got a backend;
            # not a radio failure, so the walk index stays put
            self._catcher_connecting = 0
            self._release_claims()
            raise
        adapter_used = explicit or adapter
        tune = bool(adapter_used and config is not None and config.tune_conn_params)
        if tune:
            # habluetooth's fast-then-medium: pre-seed the kernel so the
            # fast parameters apply to the connection being established
            mgmt.load_fast(adapter_used, self._catcher_address)
        validate = validate_connection or self._catcher_validate
        if validate is None and config is not None:
            validate = config.validate_connection
        connected = False
        try:
            result = await _ORIGINAL_BLEAK_CLIENT.connect(self, **kwargs)
            if validate is not None:
                # inside the try: a link the caller calls unusable is a
                # failed connect, scored and rotated like any other
                await self._run_validation(validate, adapter_used)
            connected = True
        except Exception as e:
            if _is_resource_exhaustion(e):
                # the radio did nothing wrong: the process could not obtain
                # the D-Bus connection bleak opens per session, because the
                # bus refused it. Charging this to the adapter would poison
                # every score on the box at once and walk pinned devices off
                # perfectly good cards, for a cause no adapter can fix.
                logger.error(
                    f"BLE [{self._catcher_address}]: connect failed on a D-Bus resource limit, "
                    f"not a radio fault - not charged to {adapter_used or 'the default adapter'}: {e}"
                )
            else:
                # a failed attempt: penalize this adapter in the score and,
                # for pinned devices, walk to the next pin
                _connect_finished(adapter_used, self._catcher_address, False)
                _rotation.connect_failed(self._catcher_address)
            raise
        finally:
            # no longer in flight, whatever the outcome
            self._catcher_connecting = 0
            if not connected:
                # finally, not except: a cancelled connect (asyncio timeout
                # machinery above us) must release the claims too, and the
                # wrapper must not hold a partially-initialised backend.
                # Cancellation is not a failure - it says nothing about the
                # radio.
                self._release_claims()
                self._backend = None
        _connect_finished(adapter_used, self._catcher_address, True)
        if tune:
            mgmt.load_medium(adapter_used, self._catcher_address)
        self._catcher_adapter_used = adapter_used
        self._catcher_settled = False
        self._catcher_established_at = _monotonic()
        self._catcher_cache_fault = False
        _warn_duplicate_claimant(self._catcher_address)
        self._catcher_explicit = bool(explicit)
        self._catcher_drain_kicked = None
        self._catcher_loop = asyncio.get_running_loop()
        _remember_loop(self._catcher_loop)
        _live_clients.add(self)
        self._arm_claim_validity()
        return result

    @property
    def is_connected(self):
        # queried by callers on never-connected placeholders (aiobmsble does)
        if self._backend is None:
            return False
        return self._backend.is_connected

    @property
    def address(self):
        if self._backend is None:
            return self._catcher_address
        return self._backend.address

    async def start_notify(self, char_specifier, callback, **kwargs):
        config = _config
        if config is not None and config.force_start_notify:
            # Fleet policy, decided at install (force_start_notify) rather
            # than hard-wired: when on, this process uses StartNotify for
            # every subscription - supplied when the caller is silent, so a
            # library default flip cannot reach us; overriding an explicit
            # opt-out, because AcquireNotify is the only path that creates
            # the notify_io BlueZ 5.72 double-frees. Copied, not mutated -
            # the caller's dict is theirs. When off, nothing here runs.
            bluez = kwargs.get("bluez")
            given = bluez if isinstance(bluez, dict) else {}
            if given.get("use_start_notify") is not True:
                if given.get("use_start_notify") is False:
                    akey = _address_key(self._catcher_address)
                    if akey not in _warned_acquire_notify_addresses:
                        _warned_acquire_notify_addresses.add(akey)
                        logger.warning(
                            f"BLE [{self._catcher_address}]: caller asked for AcquireNotify "
                            "(use_start_notify=False) - overridden to StartNotify, fleet policy"
                        )
                kwargs = dict(kwargs)
                kwargs["bluez"] = {**given, "use_start_notify": True}
        if callable(callback):
            callback = self._make_notify_tap(callback)
        try:
            return await self._gatt_traffic(
                _ORIGINAL_BLEAK_CLIENT.start_notify(self, char_specifier, callback, **kwargs)
            )
        except BleakDBusError as exc:
            if exc.dbus_error != "org.freedesktop.DBus.Error.UnknownObject":
                raise
            # Stale GATT object path after a reconnect - BlueZ re-resolved
            # the device but bleak's cached characteristic still points at
            # the old D-Bus path (the known ServicesResolved-lies trait,
            # ~8x/day on prod). This is a CACHE artifact local to this
            # box, so the remedy is re-resolve and retry once on the SAME
            # adapter - migrating the device to another card over it is
            # exactly the churn the field trace showed. Retry by handle
            # when the caller passed a characteristic object: the object
            # is bound to the stale path, but its ATT handle names the
            # same characteristic in the re-read table.
            logger.warning(
                f"BLE [{self._catcher_address}]: StartNotify hit a stale GATT object path "
                f"({exc.dbus_error}) - re-reading the service table and retrying once"
            )
            await validators.refresh_services(self)
            retry_spec = char_specifier
            handle = getattr(char_specifier, "handle", None)
            if handle is not None:
                retry_spec = handle
            try:
                return await self._gatt_traffic(
                    _ORIGINAL_BLEAK_CLIENT.start_notify(self, retry_spec, callback, **kwargs)
                )
            except Exception:
                # a real failure now - but a cache-shaped one. The flag
                # keeps the teardown that follows from being charged to
                # the adapter as a quick redrop (_link_ended).
                self._catcher_cache_fault = True
                raise

    def _close_orphaned_bus(self):
        """Close a per-session D-Bus connection bleak left attached.

        DELIBERATELY SYNCHRONOUS. Every caller runs in a finally, and the
        paths that matter most are the cancelled ones - a cancelled
        disconnect is exactly when bleak leaves the bus attached. An await
        in a finally under cancellation can re-raise at the suspension
        point, and CancelledError is a BaseException, so `except Exception`
        does not catch it: anything sequenced after such an await may never
        run. Closing the socket and nulling the reference are both cheap
        and neither needs to suspend, so neither is placed behind one.
        (The point was made by the sensors-py session, whose own fix is
        synchronous for the same reason.)

        bleak's disconnect() closes the bus in three statements that sit
        AFTER its try/finally, so anything raising inside skips them:

            try:
                if self.is_connected:
                    reply = await self._bus.call(... "Disconnect" ...)
                    assert_reply(reply)          # raises: "Not connected"
                    async with async_timeout(10):
                        await self._disconnecting_event.wait()
            finally:
                self._disconnecting_event = None  # clears only this
            self._bus.disconnect()                # skipped when it raised
            await self._bus.wait_for_disconnect()
            self._bus = None

        Disconnecting from a peer that is already gone is the common case -
        BlueZ answers Disconnect with org.bluez.Error.Failed "Not connected"
        and assert_reply turns that into an exception - and a slow or
        cancelled teardown strands it the same way. _cleanup_all() does not
        recover it: it removes the device watcher and clears the service
        cache but never touches _bus, despite promising to free "all
        otherwise leaked resources". So the connection survives with nothing
        referencing it, against dbus's per-user ceiling of 256.

        Nulling _bus afterwards matters as much as closing it: bleak's own
        `if self._bus is None` guards then see the session as closed, so a
        later disconnect() is a clean no-op rather than a second attempt on
        a dead socket. Everything is getattr-guarded, so a bleak that fixes
        or renames this degrades to a no-op rather than breaking.

        Field 2026-08-24 (cerbo): one wedged thermostat retrying every 900s
        leaked exactly one connection and one fd per attempt, permanently,
        until the bus hit its ceiling and every root process on the box was
        refused a connection.
        """
        backend = self._backend
        bus = getattr(backend, "_bus", None) if backend is not None else None
        if bus is None:
            return False
        try:
            bus.disconnect()
        except Exception:
            pass
        try:
            backend._bus = None
        except Exception:
            pass
        logger.info(
            f"BLE [{self._catcher_address}]: closed a D-Bus connection bleak left attached "
            "after an incomplete disconnect"
        )
        return True

    async def _retire_previous_backend(self):
        """Close the D-Bus connection a previous connect left open.

        bleak's BlueZ client opens its OWN system-bus connection per
        connection session ("Each BLE connection session needs a new D-Bus
        connection to avoid a BlueZ quirk") and closes it in exactly one
        place: disconnect(). What runs when a link drops on its own is
        _cleanup_all(), which removes the device watcher and clears the
        service cache but leaves the bus connected - despite promising in
        its docstring to free "all otherwise leaked resources".

        This wrapper re-runs the real __init__ on every connect, so a
        reconnect that did not pass through disconnect() dropped the
        previous backend on the floor with its bus still attached: one
        leaked system-bus connection per reconnect cycle, against dbus's
        per-user ceiling of 256. Field 2026-08-24: a driver retrying one
        unreachable thermostat reached 148 connections and saturated the
        system bus for every process running as root - no new connection
        for any of them, for eight hours.

        A failed connect does not reach here: its own finally clears the
        backend, and bleak's AsyncExitStack closes the bus on that path
        (only the success path calls stack.pop_all()).
        """
        if self._backend is None:
            return
        try:
            await asyncio.wait_for(_ORIGINAL_BLEAK_CLIENT.disconnect(self), timeout=BACKEND_RETIRE_TIMEOUT)
        except Exception:
            # a wedged predecessor must never block the reconnect that is
            # the caller's actual request
            logger.debug(
                f"BLE [{self._catcher_address}]: retiring the previous backend raised",
                exc_info=True,
            )
        finally:
            # finally, not just after the except: a CancelledError is a
            # BaseException and passes straight through `except Exception`,
            # so cleanup placed after that handler would be skipped on the
            # one path most likely to have left a bus attached. Cancellation
            # still propagates and aborts this connect - it simply does not
            # take a system-bus connection with it.
            self._close_orphaned_bus()
            self._backend = None

    def _op_timeout(self):
        config = _config
        value = getattr(config, "gatt_timeout", GATT_OP_TIMEOUT) if config else GATT_OP_TIMEOUT
        return value if value and value > 0 else None

    async def _gatt_traffic(self, coro):
        # A completed GATT exchange is the link's proof of life exactly as a
        # notification is - and for a polling consumer it is the only proof
        # there will ever be (field 2026-08-22: a thermostat driver that
        # only ever calls read_gatt_char lost its claims to a transient
        # is_connected false negative, and the notification tap could not
        # re-arm them because it never subscribes to anything). Noted after
        # the await, never before: an operation that raised proves nothing.
        timeout = self._op_timeout()
        try:
            result = await asyncio.wait_for(coro, timeout=timeout) if timeout else await coro
        except asyncio.TimeoutError:
            # deliberately NOT scored against the adapter: a GATT call that
            # never answers may be a wedged card OR a peripheral that has
            # stopped talking, and this wrapper cannot tell them apart. A
            # hung StartDiscovery has no peripheral in the path and IS
            # attributable; this is not. Surfacing it as a failure is the
            # whole point - the caller's retry loop can act on an exception
            # and can do nothing at all with a coroutine that never returns.
            logger.error(
                f"BLE [{self._catcher_address}]: GATT operation did not answer within "
                f"{timeout:.0f}s, abandoning it"
            )
            raise
        self._note_link_evidence()
        return result

    # *args/**kwargs throughout: these signatures drift across the bleak
    # versions this package rides on (write_gatt_char's response default
    # went bool -> None), and the wrapper has no reason to know them - it
    # only needs to see that the call returned.
    async def read_gatt_char(self, *args, **kwargs):
        return await self._gatt_traffic(_ORIGINAL_BLEAK_CLIENT.read_gatt_char(self, *args, **kwargs))

    async def write_gatt_char(self, *args, **kwargs):
        return await self._gatt_traffic(_ORIGINAL_BLEAK_CLIENT.write_gatt_char(self, *args, **kwargs))

    async def read_gatt_descriptor(self, *args, **kwargs):
        return await self._gatt_traffic(_ORIGINAL_BLEAK_CLIENT.read_gatt_descriptor(self, *args, **kwargs))

    async def write_gatt_descriptor(self, *args, **kwargs):
        return await self._gatt_traffic(_ORIGINAL_BLEAK_CLIENT.write_gatt_descriptor(self, *args, **kwargs))

    async def disconnect(self):
        # an intentional teardown settles the accounting: a straggler
        # notification or a late read racing it must not re-arm the claims
        self._catcher_settled = True
        if self._backend is None:
            return
        timeout = self._op_timeout()
        try:
            coro = _ORIGINAL_BLEAK_CLIENT.disconnect(self)
            return await asyncio.wait_for(coro, timeout=timeout) if timeout else await coro
        except asyncio.TimeoutError:
            # bleak bounds the event WAITS inside disconnect but not the
            # D-Bus Disconnect call itself, and this wrapper holds a link
            # slot and a soft claim across it - so a hang here strands a
            # shared resource, not just this caller. The same call is
            # already bounded in _retire_previous_backend; it was naked
            # here only because nothing forced the question.
            logger.error(
                f"BLE [{self._catcher_address}]: disconnect did not answer within "
                f"{timeout:.0f}s, releasing its claims anyway"
            )
            raise
        finally:
            # the raising path through bleak's disconnect() skips its own
            # bus teardown; the exception still propagates to the caller,
            # it just no longer takes a system-bus connection with it.
            # Both calls are synchronous so a cancelled disconnect cannot
            # skip them - which is the path that leaks most reliably.
            self._close_orphaned_bus()
            self._release_claims()


class BLEConnectionWithServiceCache(BLEConnection):
    """bleak-retry-connector's BleakClientWithServiceCache surface on top of
    BLEConnection.

    establish_connection's BlueZ GattService1-KeyError path does
    isinstance(client, BleakClientWithServiceCache) -> await
    client.clear_cache(); the module global resolves at call time, so after
    install that check finds this class. The underlying bleak may lack
    clear_cache entirely (vendored 3.x does), hence the hasattr guard -
    a bare inherit would AttributeError on exactly that path.
    """

    def set_cached_services(self, services):
        """No-op back-compat shim, matching bleak-retry-connector's own."""

    async def clear_cache(self, *args, **kwargs):
        """Clear the device's service cache, when the underlying bleak can."""
        if hasattr(super(), "clear_cache"):
            return await super().clear_cache(*args, **kwargs)
        logger.warning("clear_cache not implemented in this version of bleak")
        return False


def _scan_candidates(config, present):
    """Adapters a scan may use: the shared pool, else the union of pinned
    adapters in configuration order (a scan serves discovery for every
    device, so pins only narrow it when they are all there is), else every
    adapter the kernel exposes - like connection placement, an unconfigured
    install uses everything and the config acts as an allowlist."""
    if config.pool:
        return _resolve_entries(config.pool)
    seen = []
    for adapters in config.pins.values():
        for adapter in adapters:
            if adapter not in seen:
                seen.append(adapter)
    if seen:
        return _resolve_entries(seen)
    if present:
        return sorted(present, key=_hci_sort_key)
    return []


def _draining_for_others(entry):
    """A drain claim held by ANOTHER process. Our own drain never steers our
    own recovery probe off the card it is draining - the probe scans, and
    then connects on, that card deliberately, while the drain keeps
    everyone else off it."""
    return bool(entry.get("drain")) and entry.get("drain_pid") != os.getpid()


def _scan_placement(config, snapshot, explicit=None):
    """(every candidate, drain-free candidates best-first) for a scan.

    Candidates are filtered by kernel presence (falling back to the
    unfiltered list - a card the kernel is not admitting to right now is
    still worth attempting) and by responsiveness, then ranked by live
    occupancy plus this card's scan-failure memory, configuration order
    breaking ties. Scan selection has no failure-driven walk, so without the
    failure term a dead-but-listed adapter would win every tie forever; a
    successful start clears the count.

    Draining cards are removed from the ranked list and NOT from the first
    one, because the two answer different questions: what could this scan
    ever use, and what may it start on right now.
    """
    if explicit:
        adapter = str(explicit)
        return [adapter], ([] if _draining_for_others(_entry(snapshot, adapter)) else [adapter])
    present = present_adapters()
    candidates = _scan_candidates(config, present)
    if not candidates:
        return [], []
    usable = [a for a in candidates if a in present] if present else candidates
    if not usable:
        usable = candidates
    usable = _responsive_adapters(usable)

    def rank(adapter):
        entry = _entry(snapshot, adapter)
        occupancy = entry.get("soft", 0) + entry.get("links", 0)
        return (occupancy + _scan_failures.get(claims.adapter_key(adapter), 0), usable.index(adapter))

    return usable, sorted(_undrained_adapters(usable, snapshot), key=rank)


# One asyncio.Event per event loop, set whenever an exclusive claim is
# released anywhere in this process, so a queued scanner wakes on the
# handover instead of on its next poll tick. Per loop because an Event
# belongs to the loop that awaits it, and a process may run several (a test
# suite runs one per asyncio.run). Keyed by the loop object itself, which is
# hashable and lets closed loops be dropped on sight.
_scan_wake_events = {}


def _scan_wake_event():
    for closed in [loop for loop in _scan_wake_events if loop.is_closed()]:
        _scan_wake_events.pop(closed, None)
    loop = asyncio.get_running_loop()
    event = _scan_wake_events.get(loop)
    if event is None:
        event = asyncio.Event()
        _scan_wake_events[loop] = event
    return event


def _wake_scan_waiters(claim):
    """ClaimManager.on_release hook: an exclusive name just freed.

    May run on the heartbeat thread (the validity sweep releases from there),
    so every event is set through call_soon_threadsafe. Drain releases count:
    a card coming out of a drain is exactly the moment waiters excluded from
    it become able to use it again.
    """
    if not str(getattr(claim, "path", "")).endswith((".scan", ".drain")):
        return
    for loop, event in list(_scan_wake_events.items()):
        if loop.is_closed():
            _scan_wake_events.pop(loop, None)
            continue
        try:
            loop.call_soon_threadsafe(event.set)
        except RuntimeError:
            _scan_wake_events.pop(loop, None)


def _waiters(snapshot, adapter):
    return _entry(snapshot, adapter).get("waiters") or []


def _is_our_ticket(waiter, ticket):
    return ticket is not None and waiter[3] == os.path.basename(ticket.path)


def _queue_position(waiters, ticket):
    """How many tickets sit ahead of ours in one adapter's queue, or None if
    ours is not in it (a snapshot older than our write, or a ticket reaped
    from under us) - which callers read as "unknown, assume the back"."""
    if ticket is None:
        return None
    name = os.path.basename(ticket.path)
    for index, waiter in enumerate(waiters):
        if waiter[3] == name:
            return index
    return None


def _queue_target(snapshot, ranked, ticket, queued_on, migrating):
    """Where this waiter should be queued, or None to stay put.

    Unqueued: the candidate with the FEWEST waiters, ties broken by occupancy
    and then configuration order (which is the order `ranked` already
    carries).

    Queued: move only when another candidate's TOTAL waiter count is strictly
    LESS than the number of waiters AHEAD of us here. Strict, and evaluated
    on the poll tick only - never on a wake event - and that pair is the
    whole anti-thrash mechanism. Non-strict would let two waiters swap queues
    forever, each seeing the other's queue as equally good; evaluating it on
    events would let one release trigger a stampede of simultaneous
    migrations, each computed from the same instant's snapshot. With the
    strict form, every move reduces this waiter's own position and no move
    can be reciprocated, so the queues settle.

    A card that has started draining is not in `ranked` at all, which makes
    its queue infinitely expensive: every other candidate becomes a
    migration target, and a waiter whose card is the only one simply stays
    where it is and wakes when the drain releases.
    """
    if not ranked:
        return None
    if ticket is None or queued_on not in ranked:
        if ticket is not None and not migrating:
            # the card we are on started draining, but this is an event
            # wake - migration is tick-only, so leave it for the tick
            return None
        best = min(ranked, key=lambda a: (len(_waiters(snapshot, a)), ranked.index(a)))
        return best if best != queued_on else None
    if not migrating:
        return None
    ahead = _queue_position(_waiters(snapshot, queued_on), ticket)
    if ahead is None:
        ahead = len(_waiters(snapshot, queued_on))
    better = [a for a in ranked if a != queued_on and len(_waiters(snapshot, a)) < ahead]
    if not better:
        return None
    return min(better, key=lambda a: (len(_waiters(snapshot, a)), ranked.index(a)))


def _scan_wait_detail(manager, snapshot, candidates, ticket, queued_on):
    """Who every candidate is held by, for the wait warning and the timeout."""
    parts = []
    for adapter in candidates:
        entry = _entry(snapshot, adapter)
        info = manager.holder_info(adapter)
        if info is not None:
            age = f"{info['age']:.0f}s" if info.get("age") is not None else "unknown age"
            parts.append(f"{adapter} scanned by {info['service']} (pid {info['pid']}, held {age})")
        elif entry.get("drain"):
            parts.append(f"{adapter} draining (pid {entry.get('drain_pid')})")
        else:
            parts.append(f"{adapter} claimed by someone this process cannot name")
    detail = "; ".join(parts) or "no eligible adapter"
    if queued_on is not None:
        waiters = _waiters(snapshot, queued_on)
        ahead = _queue_position(waiters, ticket)
        place = (ahead + 1) if ahead is not None else len(waiters)
        detail += f" - queued {place} of {len(waiters)} on {queued_on}"
    return detail


async def _acquire_scan_adapter(explicit=None):
    """Take a scan-eligible adapter's hard claim, waiting for one if need be.

    Returns (adapter-or-None, Claim-or-None) and raises ScanSlotWaitTimeout
    if no candidate frees within SCAN_CLAIM_WAIT. (None, None) means there is
    nothing to claim at all - no config, and no card the kernel admits to -
    which is the unconfigured passthrough the wrapper has always had: no
    adapter is chosen, so there is no adapter's claim to hold.

    A claim is a gate: this function never returns an adapter without its
    claim. The queue it maintains while waiting (see _queue_target) exists so
    that N waiting scanners spread themselves across M free-ing cards instead
    of all watching the same best-ranked one.
    """
    config = _config
    if config is None:
        return None, None
    manager = config.claims
    event = _scan_wake_event()
    deadline = _monotonic() + SCAN_CLAIM_WAIT
    ticket = None
    queued_on = None
    announced = False
    migrating = False  # the first pass places; only poll ticks may migrate
    try:
        while True:
            snapshot = manager.claims()
            candidates, ranked = _scan_placement(config, snapshot, explicit)
            if not candidates:
                return None, None
            for adapter in ranked:
                entry = _entry(snapshot, adapter)
                if entry.get("hard"):
                    continue
                # free, and either unqueued or queued by nobody but us: a
                # waiter that jumped an existing queue here would make the
                # queue meaningless, and one that refused to take a card
                # holding only its own ticket would deadlock on itself
                if any(not _is_our_ticket(w, ticket) for w in _waiters(snapshot, adapter)):
                    continue
                claim = manager.claim_hard(adapter)
                if claim is not None:
                    return adapter, claim
            if ticket is not None and queued_on in ranked and not _entry(snapshot, queued_on).get("hard"):
                # oldest ticket on a card that just freed: our turn
                if _queue_position(_waiters(snapshot, queued_on), ticket) == 0:
                    claim = manager.claim_hard(queued_on)
                    if claim is not None:
                        return queued_on, claim
            target = _queue_target(snapshot, ranked, ticket, queued_on, migrating)
            if target is not None:
                fresh = manager.claim_scanwait(target)
                if fresh is not None:
                    if ticket is not None:
                        manager.release(ticket)
                    ticket, queued_on = fresh, target
            if not announced:
                announced = True
                logger.warning(
                    f"BLE scan: all {len(candidates)} scan-eligible adapter(s) are held, waiting up to "
                    f"{SCAN_CLAIM_WAIT:.0f}s for one - "
                    f"{_scan_wait_detail(manager, snapshot, candidates, ticket, queued_on)}"
                )
            remaining = deadline - _monotonic()
            if remaining <= 0:
                raise ScanSlotWaitTimeout(
                    f"no scan-eligible adapter came free within {SCAN_CLAIM_WAIT:.0f}s: "
                    f"{_scan_wait_detail(manager, snapshot, candidates, ticket, queued_on)}"
                )
            # per-tick evaluation is silent: one warning at the top, one
            # error at the bottom, nothing in between - a 30s wait polled
            # every second is 30 log lines per scanner otherwise
            migrating = not await _wait_for_release(event, min(SCAN_CLAIM_POLL, remaining))
    finally:
        # every exit path: acquired, timed out, cancelled. A ticket left
        # behind would lengthen a queue on behalf of a scanner that is no
        # longer waiting, and every other waiter would place around a
        # phantom until the TTL reaped it.
        if ticket is not None:
            manager.release(ticket)


async def _wait_for_release(event, timeout):
    """Wait for an in-process claim release, or the poll tick. True if the
    event fired - which is the signal NOT to migrate: migration is a
    poll-tick decision, so that one release cannot start a stampede."""
    try:
        await asyncio.wait_for(event.wait(), timeout)
    except asyncio.TimeoutError:
        return False
    event.clear()
    return True


class BLEScanner(_ORIGINAL_BLEAK_SCANNER):
    """Drop-in BleakScanner that picks its adapter and takes the hard scan
    claim at start().

    Deferred init, like BLEConnection: bleak wires the adapter into its
    platform scanner backend inside __init__, so construction stores the
    arguments and builds nothing, and start() runs the real __init__ with
    the routed adapter merged in - as BOTH a fresh bluez={"adapter": ...}
    dict and the adapter kwarg, because bleak generations disagree on which
    one the BlueZ scanner backend reads (see start()). The hard
    claim (hciN.scan) is held per scan activity: taken at start(), released
    at stop() or on a failed start - so other processes' placement steers
    connections and scans away from a card while it is actually scanning,
    and no longer. An adapter the caller chose explicitly is never
    overridden, though its claim is still taken, best effort.

    Rebinding over bleak.BleakScanner is opt-in
    (install_bleak_catcher(..., wrap_scanner=True)).
    """

    def __init__(self, detection_callback=None, service_uuids=None, scanning_mode="active", **kwargs):
        self._catcher_args = (detection_callback, service_uuids, scanning_mode)
        self._catcher_kwargs = kwargs
        self._catcher_claim = None
        self._catcher_manager = None
        self._catcher_adapter = None
        self._catcher_last_detection = 0.0
        self._catcher_start_time = 0.0
        self._catcher_watchdog = None
        self._catcher_restarting = False
        # True only between a successful start() and stop(); the hard scan
        # claim is valid for exactly this window
        self._catcher_scanning = False
        self._catcher_tasks = set()
        self._catcher_loop = None
        self._catcher_explicit = False
        self._catcher_drain_kicked = None
        self._backend = None
        self._backend_id = ""

    def _release_scan_claim(self):
        claim, self._catcher_claim = self._catcher_claim, None
        if claim is None:
            return
        if self._catcher_manager is not None:
            self._catcher_manager.release(claim)
        else:
            claim.release()

    def _arm_claim_validity(self):
        # The hard claim says "I am scanning here, use another card", so it
        # must be valid for exactly as long as that is true. Two ways it
        # stops being true: the wrapper is collected without anyone calling
        # stop() (an abandoned running scanner its owner can no longer
        # stop), or the scan simply ended. Keying validity on the object's
        # existence alone covered only the first, so a scanner that was
        # started, finished, and is still referenced went on announcing an
        # exclusive scan indefinitely - field 2026-08-25: a single 12s
        # discovery held a shared card's .scan claim for minutes afterwards,
        # steering every other process off a card nobody was scanning on.
        claim = self._catcher_claim
        if claim is None:
            return
        ref = weakref.ref(self)

        def _still_scanning():
            scanner = ref()
            return scanner is not None and scanner._catcher_scanning

        claim.validity = _still_scanning

    def _make_detection_callback(self, raw_callback):
        # every advertisement stamps the liveness clock the watchdog reads -
        # except empty ones: a wedged adapter can keep emitting empty
        # advertisements, so they must not count as signs of life
        # (habluetooth's rule). Async caller callbacks are scheduled, not
        # called, mirroring how habluetooth's scanner wrapper adapts them.
        is_async = raw_callback is not None and inspect.iscoroutinefunction(raw_callback)

        def _detection(device, advertisement_data):
            if (
                getattr(advertisement_data, "local_name", None)
                or getattr(advertisement_data, "manufacturer_data", None)
                or getattr(advertisement_data, "service_data", None)
                or getattr(advertisement_data, "service_uuids", None)
            ):
                if self._catcher_last_detection == self._catcher_start_time:
                    # first real advertisement of this scan session: the card
                    # is demonstrably scanning, so forgive its history. Once
                    # per session, not per advertisement - a busy floor
                    # delivers thousands.
                    _scan_finished(self._catcher_adapter, True)
                self._catcher_last_detection = _monotonic()
            if raw_callback is None:
                return
            if is_async:
                task = asyncio.get_running_loop().create_task(raw_callback(device, advertisement_data))
                self._catcher_tasks.add(task)
                task.add_done_callback(self._catcher_tasks.discard)
            else:
                raw_callback(device, advertisement_data)

        return _detection

    def _cancel_watchdog(self):
        watchdog, self._catcher_watchdog = self._catcher_watchdog, None
        if watchdog is not None:
            watchdog.cancel()

    def _schedule_watchdog(self):
        self._cancel_watchdog()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._catcher_watchdog = loop.call_later(SCANNER_WATCHDOG_INTERVAL, self._watchdog_tick)

    def _quiet_seconds(self):
        return _monotonic() - self._catcher_last_detection

    def _watchdog_tick(self):
        self._catcher_watchdog = None
        if self._backend is None:
            return
        if self._quiet_seconds() > SCANNER_WATCHDOG_TIMEOUT and not self._catcher_restarting:
            task = asyncio.get_running_loop().create_task(self._watchdog_restart())
            self._catcher_tasks.add(task)
            task.add_done_callback(self._catcher_tasks.discard)
        self._schedule_watchdog()

    async def _watchdog_restart(self):
        """habluetooth's two-tier remedy for a scanner gone quiet: restart
        it, and if it never saw anything or stays quiet past the escalation
        threshold, hardware-reset the adapter first - gated on no other
        live process holding claims on the card. The restart re-runs
        selection, so a genuinely dead card can also be walked away from."""
        if self._catcher_restarting:
            return
        self._catcher_restarting = True
        try:
            quiet = self._quiet_seconds()
            never_saw = self._catcher_last_detection == self._catcher_start_time
            adapter = self._catcher_adapter
            logger.warning(
                f"BLE scanner on {adapter or 'the default adapter'} has been quiet for {quiet:.0f}s, restarting"
            )
            # Record the quiet AGAINST THIS CARD before restarting. The
            # restart re-runs selection, but re-running it changes nothing
            # unless something changed its inputs - and a card that cannot
            # scan is otherwise the most attractive candidate there is,
            # holding no claims and carrying no links. Without this the
            # watchdog faithfully migrated discovery back onto the same
            # broken radio, every time.
            _scan_finished(adapter, False)
            await self.stop()
            if adapter and (never_saw or quiet > SCANNER_WATCHDOG_MULTIPLE):
                # through the same accounting, so a card that will not come
                # back is not power-cycled every 90s forever
                await _recover_adapter(adapter)
            await self.start()
        except Exception:
            logger.exception("BLE scanner watchdog restart failed")
        finally:
            self._catcher_restarting = False

    async def start(self):
        detection_callback, service_uuids, scanning_mode = self._catcher_args
        init_kwargs = dict(self._catcher_kwargs)
        # a restart on this instance must not leak the previous claim - and
        # the flag resets WITH it, so the documented invariant ("True only
        # between a successful start() and stop()") holds even when this
        # start fails after a previous successful one
        self._catcher_scanning = False
        self._release_scan_claim()
        config = _config
        self._catcher_manager = config.claims if config is not None else None
        adapter = None
        explicit = init_kwargs.get("adapter") or (init_kwargs.get("bluez") or {}).get("adapter")
        # The gate, and it is ahead of everything: no backend is built and no
        # StartDiscovery is issued until this process holds the card's hard
        # claim. An explicit adapter waits on that ONE card - the caller's
        # choice is never overridden, and "wait for the card you asked for"
        # is the only honest reading of it - and raises the same timeout.
        try:
            if explicit:
                _bound, self._catcher_claim = await _acquire_scan_adapter(explicit=explicit)
            else:
                adapter, self._catcher_claim = await _acquire_scan_adapter()
        except BaseException:
            # A scanner that never got a card has no backend either. This
            # instance may be mid-restart and still carrying the previous
            # start's backend, and leaving it attached would let
            # discovered_devices and the watchdog read a scan that is not
            # running. BaseException, so a cancelled wait clears it too.
            self._backend = None
            raise
        try:
            if adapter:
                # Both spellings, deliberately. Older bleak 3.x BlueZ
                # scanner backends read the adapter kwarg; current bleak
                # reads bluez["adapter"] - and its deprecation shim mutates
                # a SHARED default {} when the kwarg arrives alone (`bluez:
                # BlueZScannerArgs = {}` plus in-place assignment), so the
                # first scanner's adapter poisons every later one in the
                # process. Handing it a fresh bluez dict that already
                # carries the adapter sidesteps the poison on new bleak
                # while the kwarg keeps old backends routed.
                bluez = dict(init_kwargs.get("bluez") or {})
                bluez.setdefault("adapter", adapter)
                init_kwargs["bluez"] = bluez
                init_kwargs.setdefault("adapter", adapter)
            _ORIGINAL_BLEAK_SCANNER.__init__(
                self,
                self._make_detection_callback(detection_callback),
                service_uuids,
                scanning_mode,
                **init_kwargs,
            )
        except BaseException:
            # the claim must not outlive a scanner that never got a backend
            self._release_scan_claim()
            raise
        started = False
        try:
            result = await asyncio.wait_for(
                _ORIGINAL_BLEAK_SCANNER.start(self), timeout=SCAN_OP_TIMEOUT
            )
            started = True
        except asyncio.TimeoutError:
            # a card that accepts StartDiscovery and never answers is wedged,
            # not busy; say so plainly, because the alternative reading -
            # "the scan is still starting" - is what let this run for hours
            logger.error(
                f"BLE scan: {explicit or adapter or 'the default adapter'} did not answer "
                f"StartDiscovery within {SCAN_OP_TIMEOUT:.0f}s, treating as a failed start"
            )
            _scan_finished(explicit or adapter, False)
            _schedule_recovery(explicit or adapter)
            raise
        except Exception:
            # a failed start counts against this adapter in scan placement
            # (cancellation does not - it says nothing about the radio)
            _scan_finished(explicit or adapter, False)
            _schedule_recovery(explicit or adapter)
            raise
        finally:
            if not started:
                # finally, not except: a cancelled start must release the
                # scan claim too, and drop the partially-initialised backend
                self._release_scan_claim()
                self._backend = None
        # deliberately NOT clearing this adapter's failure memory here: a
        # start that returns success answers "did the card accept the scan
        # command", which is not "is this card scanning". A wedged card
        # accepts it and then reports nothing, so clearing on start alone
        # let a broken radio look freshly healthy at every selection. The
        # first real advertisement clears it instead - traffic is the truth,
        # exactly as it is for links (field 2026-08-26: two cards scan-
        # wedged on prod, each rating BEST because a card that cannot scan
        # holds no claims, carries no links and had its failures wiped).
        self._catcher_scanning = True
        self._catcher_adapter = explicit or adapter
        self._catcher_explicit = bool(explicit)
        self._catcher_drain_kicked = None
        self._catcher_loop = asyncio.get_running_loop()
        _remember_loop(self._catcher_loop)
        _live_scanners.add(self)
        now = _monotonic()
        self._catcher_start_time = now
        self._catcher_last_detection = now
        self._schedule_watchdog()
        self._arm_claim_validity()
        return result

    async def _drain_restart(self):
        # the drain watcher's migration: stop and start, nothing more - the
        # restart re-runs selection, which steers off the draining card
        if self._catcher_restarting:
            return
        self._catcher_restarting = True
        try:
            await self.stop()
            await self.start()
        except Exception:
            logger.exception("BLE scan: drain migration failed")
        finally:
            self._catcher_restarting = False

    async def stop(self):
        self._cancel_watchdog()
        if self._backend is None:
            # nothing to stop, but a claim may still be held - releasing it
            # is the whole point of being asked to stop
            self._catcher_scanning = False
            self._release_scan_claim()
            return
        try:
            return await asyncio.wait_for(
                _ORIGINAL_BLEAK_SCANNER.stop(self), timeout=SCAN_OP_TIMEOUT
            )
        except asyncio.TimeoutError:
            # the same hazard on the way out, and worse placed: the watchdog
            # stops before it restarts, so an unbounded stop would wedge the
            # very machinery meant to recover from a wedged card
            logger.error(
                f"BLE scan: {self._catcher_adapter or 'the default adapter'} did not answer "
                f"StopDiscovery within {SCAN_OP_TIMEOUT:.0f}s, abandoning the scan"
            )
            raise
        except BleakDBusError as exc:
            if exc.dbus_error != "org.bluez.Error.InProgress":
                raise
            # Swallowed, counted, logged (Clint, 2026-08-26). InProgress on
            # a STOP is the kernel saying it REJECTED the stop because it
            # was not scanning - the scan this stop was for is already
            # over, and bluetoothd detached our session before replying
            # (5.72 stop_discovery_complete runs discovery_remove before
            # checking status; a stop refused mid-start is counter-stopped
            # by bluetoothd's own no-clients-left branch within one MGMT
            # round-trip). Every path ends with the radio not scanning, so
            # the stop's INTENT is satisfied and raising would only destroy
            # the caller's find/teardown - bleak#958 is this exact trace,
            # closed "a BlueZ bug"; bleak already swallows NotReady here.
            # Counted for RANKING, and no longer as evidence for recovery.
            # With the 0.5 gate in place an InProgress here should be
            # impossible: it means two discovery sessions overlapped on one
            # card, and nothing reaches StartDiscovery now without holding
            # that card's exclusive claim. So seeing one is a report that a
            # scan escaped the claim convention - a bug in this package, or a
            # participant not following it - and the one thing it is NOT is
            # evidence that the radio is broken. Reading it that way is what
            # power-cycled healthy adapters through 2026-08-26.
            #
            # The strike stays because ranking off a card that keeps doing
            # this costs nothing and helps; the _schedule_recovery call is
            # gone, because charging a card for someone else's protocol
            # violation is how the fleet melted. Not retried: bleak's stop()
            # nulls its _stop closure before invoking it, so a second call is
            # a silent no-op.
            adapter = self._catcher_adapter
            logger.warning(
                f"BLE scan: StopDiscovery on {adapter or 'the default adapter'} answered "
                f"InProgress - the kernel had already stopped scanning and BlueZ had detached "
                f"the session. With the scan claim gating every start this should be "
                f"unreachable: a scan escaped the claim convention. Treating the scan as "
                f"stopped and ranking this card down, but NOT counting it as radio evidence"
            )
            _scan_finished(adapter, False)
            return None
        finally:
            # cleared HERE, not at entry: while bleak's stop is in flight
            # the scan claim is still deliberately held, and a flag cleared
            # before the await opens a window where the heartbeat's validity
            # check sees a "finished" scan and sweep-releases a claim that
            # stop() is about to release anyway - same outcome, but logged
            # as a divergence ("what it accounted for is gone") when nothing
            # diverged. finally, so cancellation clears it too.
            self._catcher_scanning = False
            self._release_scan_claim()

    @property
    def discovered_devices(self):
        # queried on never-started placeholders; delegate once real
        if self._backend is None:
            return []
        prop = getattr(_ORIGINAL_BLEAK_SCANNER, "discovered_devices", None)
        if isinstance(prop, property):
            return prop.fget(self)
        return self._backend.discovered_devices


def install_bleak_catcher(owner, adapters=(), link_caps=None, claim_dir=CLAIM_DIR, wrap_scanner=False, tune_conn_params=True, scan_to_score=False, validate_connection=None, adapter_config_path=None, gatt_timeout=GATT_OP_TIMEOUT, allowed_devices=None, force_start_notify=None):
    """Route every bleak client in this process through the catcher.

    Must run before consumer libraries are imported: they capture `from
    bleak import BleakClient` at import time. A consumer that imported
    BleakClientWithServiceCache before install and passes the stale original
    as client_class silently skips cache-clear - degradation, not breakage.

    owner names this process's claims; the pid is appended to disambiguate
    restart races (the old process's claims awaiting reap while the new one
    starts). adapters are raw config strings, verbatim ("MAC@hciX" pins,
    plain "hciX" pools - see parse_adapter_entries). An adapter may be named
    by hciN or by its own MAC in any spelling; the MAC is the stable
    identity, since hciN numbering changes under a USB reset or a replug,
    and a MAC entry is resolved to the current number at use time. Pass
    adapter_config_path to have the FIRST successful read of an hciN entry
    rewrite that entry in the consumer's config file to the MAC it proved
    to be, with a comment recording the substitution - the number stops
    being load-bearing without anyone having to hand-edit anything.
    link_caps maps adapter name (either spelling) to its established-link
    capacity; caps are opt-in, an uncapped adapter is never slot-gated. wrap_scanner additionally rebinds
    bleak.BleakScanner to the adapter-bound, hard-claiming BLEScanner -
    opt-in because it changes which adapter unrelated code scans on.
    tune_conn_params loads habluetooth's fast-then-medium connection
    parameters over the mgmt socket around each connect; it degrades to a
    no-op wherever the mgmt channel is unavailable (non-Linux, no
    NET_ADMIN). scan_to_score is the driver's placement-mode choice: False
    routes unpinned devices least-used (occupancy and failures only), True
    additionally runs periodic short RSSI sweeps per adapter (RssiSweeper)
    so the score gains its habluetooth RSSI base - a driver that scans can
    place by signal, one that will not scan still spreads load.
    validate_connection is the process-wide post-connect validator, `async
    (client) -> bool`: a link it rejects is torn down and raised as a
    connect failure for the retry loop above (see the validators module).
    gatt_timeout bounds operations whose API offers no way to express a
    deadline - read/write_gatt_char, start_notify, disconnect - because
    this wrapper does not pass "forever" through: an unbounded wait never
    becomes an observable failure, so a stuck adapter never surfaces as an
    event anything downstream can act on. Raise it for a device that is
    legitimately slower, or pass None to restore unbounded waits. A
    caller-supplied deadline is never overridden - connect(timeout=) is
    bleak's own and is left alone.
    It is the fallback, applying to every routed connect that does not
    carry its own `validate_connection=` client kwarg - which is how a
    driver validates connections made deep inside a library it does not
    call directly. allowed_devices, when given, is the closed set of device
    MACs this process may connect to (any spelling, comma-separated string
    or iterable); a connect to anything else raises DeviceNotPermitted
    before a claim is taken or a backend built. Omit it for no gate.
    force_start_notify makes every start_notify in this process use BlueZ
    StartNotify: supplied when the caller is silent, overriding an explicit
    opt-out (with a warning). AcquireNotify is the only path that creates
    the notify_io BlueZ 5.72 double-frees, and bleak's own default is
    changing to AcquireNotify, so this is the fleet's guard against both a
    library flip and a call site in code nobody here controls. None (the
    default) reads BCM_FORCE_START_NOTIFY (true/false) from the environment
    - the shim sets it to true, so the deploy decides fleet-wide without
    touching any consumer; pass True or False here to decide for this
    process alone.
    False changes nothing: bleak's default and every caller's choice stand.
    Idempotent.
    """
    global _config
    import bleak

    pins, pool = parse_adapter_entries(adapters)
    caps = {}
    for adapter, cap in (link_caps or {}).items():
        try:
            cap = int(cap)
        except (TypeError, ValueError):
            cap = 0
        if cap > 0:
            caps[str(adapter).strip()] = cap
        else:
            logger.warning(f"Ignoring link cap for '{adapter}': not a positive integer")
    if _config is not None:
        if _config.sweeper is not None:
            _config.sweeper.stop()
        _config.claims.release_all()
    _config = _CatcherConfig(
        owner=owner,
        pins=pins,
        pool=pool,
        link_caps=caps,
        claims=ClaimManager(owner=f"{owner}-{os.getpid()}", claim_dir=claim_dir),
        tune_conn_params=tune_conn_params,
        validate_connection=validate_connection,
    )
    _config.claims.on_beat = _drain_watch
    _config.claims.on_release = _wake_scan_waiters
    _config.adapter_config_path = adapter_config_path
    if force_start_notify is None:
        # fleet-wide from the environment: the /data/bcm/python3 shim exports
        # BCM_FORCE_START_NOTIFY, so the deploy decides for every consumer it
        # launches and no consumer's source has to know
        # true/false (case-insensitive); 1/0 and yes/no are tolerated
        force_start_notify = os.environ.get("BCM_FORCE_START_NOTIFY", "").strip().lower() in ("true", "1", "yes", "on")
    _config.force_start_notify = bool(force_start_notify)
    if _config.force_start_notify:
        logger.info("bleak catcher: StartNotify is forced for every start_notify in this process")
    _config.allowed_devices = _parse_allowed_devices(allowed_devices)
    if _config.allowed_devices is not None:
        logger.info(
            f"bleak catcher: allowed_devices in force - {len(_config.allowed_devices)} device(s); "
            "every other connect in this process is refused"
        )
    _config.gatt_timeout = gatt_timeout
    if scan_to_score:
        _config.sweeper = RssiSweeper(_config)
    bleak.BleakClient = BLEConnection
    bleak.BleakScanner = BLEScanner if wrap_scanner else _ORIGINAL_BLEAK_SCANNER
    if _brc_module is not None:
        _brc_module.BleakClient = BLEConnection
        _brc_module.BleakClientWithServiceCache = BLEConnectionWithServiceCache
    logger.info("bleak catcher installed: BLE clients are routed through claim-aware adapter selection")


def uninstall_bleak_catcher():
    """Restore the original classes and release every held claim."""
    global _config
    import bleak

    bleak.BleakClient = _ORIGINAL_BLEAK_CLIENT
    bleak.BleakScanner = _ORIGINAL_BLEAK_SCANNER
    if _brc_module is not None:
        _brc_module.BleakClient = _ORIGINAL_BRC_CLIENT
        _brc_module.BleakClientWithServiceCache = _ORIGINAL_BRC_CACHE_CLIENT
    if _config is not None:
        if _config.sweeper is not None:
            _config.sweeper.stop()
        _config.claims.release_all()
        _config = None


def rewrite_adapter_config(path, mapping):
    """Rewrite hciN adapter names in a config file to the MACs they proved
    to be, leaving a comment recording what happened.

    Line-oriented and format-agnostic on purpose: it substitutes the hciN
    token wherever it appears in a value and inserts a comment above that
    line, which is what INI, conf and shell-style files all understand. A
    line already commented out is left alone. Best effort in every failure
    mode - a config that cannot be read or written is not worth breaking a
    connection over, and the resolution itself is unaffected.

        # bcm: hci3 was detected as AA:BB:CC:DD:EE:FF and rewritten
        adapters = AA:BB:CC:DD:EE:FF,hci5
    """
    if not path or not mapping:
        return False
    try:
        with open(path) as f:
            lines = f.readlines()
    except OSError as e:
        logger.debug(f"adapter config rewrite: cannot read {path}: {repr(e)}")
        return False
    out = []
    changed = False
    for line in lines:
        stripped = line.strip()
        replaced = line
        hits = []
        if stripped and not stripped.startswith(("#", ";")):
            for entry, mac in mapping.items():
                # word-boundary so hci1 never matches inside hci10
                pattern = rf"(?<![0-9A-Za-z]){re.escape(entry)}(?![0-9A-Za-z])"
                if re.search(pattern, replaced):
                    replaced = re.sub(pattern, mac, replaced)
                    hits.append((entry, mac))
        if hits:
            indent = line[: len(line) - len(line.lstrip())]
            for entry, mac in hits:
                out.append(f"{indent}# bcm: {entry} was detected as {mac} and rewritten\n")
            changed = True
        out.append(replaced)
    if not changed:
        return False
    try:
        tmp = f"{path}.bcm-tmp"
        with open(tmp, "w") as f:
            f.writelines(out)
        os.replace(tmp, path)
    except OSError as e:
        logger.debug(f"adapter config rewrite: cannot write {path}: {repr(e)}")
        return False
    for entry, mac in mapping.items():
        logger.warning(f"adapter config: {entry} was detected as {mac}, rewritten in {path}")
    return True
