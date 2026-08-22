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
from bleak.exc import BleakError

from . import mgmt, recovery
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

# Notifications are proof the link is alive whatever bleak's connected flag
# says (field 2026-08-21: a broken D-Bus view read disconnected while data
# flowed). Evidence goes stale on the same bound the claim convention uses
# for its own liveness, so a silently dead link is swept within one TTL.
LINK_EVIDENCE_SECONDS = CLAIM_TTL

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
    Returns (pins, pool), with pins keyed by upper case MAC address and each
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

    The per-address index advances only when a connect attempt fails - a
    disconnect is not a failure, so a dropped link reconnects on the adapter
    it was using - and never resets on success: once a device is talking
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

# (adapter, address) -> consecutive failed connect attempts, feeding the
# placement score; a success there clears it (habluetooth's model)
_connect_failures = {}


def _connect_finished(adapter, address, connected):
    if not adapter:
        return
    key = (adapter, _address_key(address))
    if connected:
        _connect_failures.pop(key, None)
    else:
        _connect_failures[key] = _connect_failures.get(key, 0) + 1


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


def _responsive_adapters(candidates):
    """Drop adapters whose sysfs MAC is all-zeros - the kernel's signal for
    a failed or unserved controller (habluetooth's FAILED_ADAPTER_MAC): a
    dead onboard UART controller stays listed in /sys/class/bluetooth
    forever and would otherwise win every tie. Never gates: when nothing
    passes (including hosts with no sysfs at all), the unfiltered list is
    used."""
    live = [a for a in candidates if recovery.adapter_mac(a) != recovery.UNKNOWN_MAC]
    return live or candidates


# adapter -> consecutive failed scanner starts, feeding scan placement;
# a successful start there clears it
_scan_failures = {}


def _scan_finished(adapter, started):
    if not adapter:
        return
    if started:
        _scan_failures.pop(adapter, None)
    else:
        _scan_failures[adapter] = _scan_failures.get(adapter, 0) + 1


# addresses already warned about bare connect() calls, once per process -
# a reconnect loop hitting this every few seconds would flood the log
_warned_bare_connect_addresses = set()


class _CatcherConfig:
    def __init__(self, owner, pins, pool, link_caps, claims, tune_conn_params):
        self.owner = owner
        self.pins = pins
        self.pool = pool
        self.link_caps = link_caps
        self.claims = claims
        self.tune_conn_params = tune_conn_params
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
    detail = ", ".join(f"{adapter} ({(snapshot.get(adapter) or {}).get('links', cap)}/{cap} links held)" for adapter, cap in exhausted)
    return OutOfConnectionSlotsError(f"connection slot exhausted for {address}: {detail}")


def _score_order(eligible, address_key, snapshot, caps, rssi=None):
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
        entry = snapshot.get(adapter) or {}
        cap = caps.get(adapter)
        free = (cap - entry.get("links", 0)) if cap else None
        score = float(rssi.get(adapter, NO_RSSI_VALUE)) if rssi else 0.0
        score -= entry.get("soft", 0) * 1.01 * unit
        score -= _connect_failures.get((adapter, address_key), 0) * 0.51 * unit
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
        candidates = list(pins)
    elif config.pool:
        candidates = list(config.pool)
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
        entry = snapshot.get(adapter)
        return bool(entry and entry["hard"] and entry["hard_pid"] != own_pid)

    eligible = [a for a in usable if not foreign_scan(a)]
    if not eligible:
        logger.info(f"BLE [{address}]: every usable adapter is scan-claimed by another process, using them anyway")
        eligible = usable
    if pins:
        start = _rotation.index(address) % len(eligible)
        ordered = eligible[start:] + eligible[:start]
    else:
        rssi = config.sweeper.rssi_for(eligible, address_key) if config.sweeper is not None else None
        ordered = _score_order(eligible, address_key, snapshot, config.link_caps, rssi)
    if logger.isEnabledFor(logging.DEBUG):
        occupancy = {a: (snapshot.get(a) or {}) for a in ordered}
        logger.debug(
            f"BLE [{address}]: adapter order {ordered} "
            f"({'pinned walk' if pins else 'scored'}, occupancy {occupancy})"
        )
    exhausted = []
    for adapter in ordered:
        cap = config.link_caps.get(adapter)
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
    cap = config.link_caps.get(adapter)
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
        self._catcher_last_evidence = None
        self._catcher_last_rearm = None
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
        def _disconnected(client):
            if generation == self._catcher_generation and not self.is_connected:
                self._release_claims()
            if raw_callback is not None:
                raw_callback(client)

        return _disconnected

    def _recent_link_evidence(self):
        stamp = self._catcher_last_evidence
        return stamp is not None and _monotonic() - stamp <= LINK_EVIDENCE_SECONDS

    def _arm_claim_validity(self):
        # Backstop for the release paths: once the connection is up, its
        # claims are validity-checked on every heartbeat, so if the link
        # dies without the disconnected callback ever firing (a torn-down
        # D-Bus watch, an abandoned client object) the slot and soft claim
        # free themselves within a TTL instead of living until process
        # exit. Armed only after a successful connect - a slow in-flight
        # attempt reads as not-connected and must not be swept. Validity is
        # link truth, not wrapper liveness: recent notification traffic
        # counts even when is_connected reads False (a broken D-Bus view),
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
        self._catcher_last_evidence = _monotonic()
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
        # release the guards could not see): data is flowing, so the
        # connection re-acquires the accounting it held at connect time.
        # The link exists regardless of what the files say - losing the
        # slot race to another process degrades to a soft claim, never to
        # dropping the connection.
        config = _config
        adapter = self._catcher_adapter_used
        if config is None or not adapter:
            return
        claims = []
        cap = config.link_caps.get(adapter)
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
            f"BLE [{self._catcher_address}]: link on {adapter} is alive (notifications flowing) "
            "but its claims were lost, re-claimed"
        )
        self._catcher_claims = claims
        self._catcher_manager = config.claims
        self._arm_claim_validity()

    async def connect(self, **kwargs):
        if not self._catcher_is_retry_client:
            self._warn_bare_connect()
        address_or_ble_device, raw_callback, services = self._catcher_args
        init_kwargs = dict(self._catcher_kwargs)
        # a reconnect on this instance must not leak the previous claims,
        # and opens a new generation: stale disconnect events from the
        # backend being replaced lose their power to release
        self._release_claims()
        self._catcher_generation += 1
        generation = self._catcher_generation
        config = _config
        self._catcher_manager = config.claims if config is not None else None
        if config is not None and config.sweeper is not None:
            # lazily started here because this is the first moment a
            # running event loop is guaranteed
            config.sweeper.ensure_running()
        adapter = None
        explicit = (
            init_kwargs.get("adapter")
            or (init_kwargs.get("bluez") or {}).get("adapter")
            or _device_path_adapter(address_or_ble_device)
        )
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
            self._release_claims()
            raise
        adapter_used = explicit or adapter
        tune = bool(adapter_used and config is not None and config.tune_conn_params)
        if tune:
            # habluetooth's fast-then-medium: pre-seed the kernel so the
            # fast parameters apply to the connection being established
            mgmt.load_fast(adapter_used, self._catcher_address)
        connected = False
        try:
            result = await _ORIGINAL_BLEAK_CLIENT.connect(self, **kwargs)
            connected = True
        except Exception:
            # a failed attempt: penalize this adapter in the score and, for
            # pinned devices, walk to the next pin
            _connect_finished(adapter_used, self._catcher_address, False)
            _rotation.connect_failed(self._catcher_address)
            raise
        finally:
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
        if callable(callback):
            callback = self._make_notify_tap(callback)
        return await _ORIGINAL_BLEAK_CLIENT.start_notify(self, char_specifier, callback, **kwargs)

    async def disconnect(self):
        # an intentional teardown settles the accounting: a straggler
        # notification racing it must not re-arm the claims
        self._catcher_settled = True
        if self._backend is None:
            return
        try:
            return await _ORIGINAL_BLEAK_CLIENT.disconnect(self)
        finally:
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
        return list(config.pool)
    seen = []
    for adapters in config.pins.values():
        for adapter in adapters:
            if adapter not in seen:
                seen.append(adapter)
    if seen:
        return seen
    if present:
        return sorted(present, key=_hci_sort_key)
    return []


def _acquire_scan_adapter():
    """Choose an adapter for a scan and take its hard claim.

    Returns (adapter-or-None, Claim-or-None). Candidates are filtered by
    kernel presence (falling back to the unfiltered list), ranked by live
    occupancy - fewest soft claims plus held link slots first, configuration
    order breaking ties - and the best claimable card wins; claim_hard
    itself skips cards another live process is scanning on. When every card
    is claimed, the best-ranked one is scanned anyway, unclaimed:
    coordination is an optimization, never a gate.
    """
    config = _config
    if config is None:
        return None, None
    present = present_adapters()
    candidates = _scan_candidates(config, present)
    if not candidates:
        return None, None
    usable = [a for a in candidates if a in present] if present else candidates
    if not usable:
        # refusing to scan is worse than scanning an adapter that may be gone
        usable = candidates
    usable = _responsive_adapters(usable)
    snapshot = config.claims.claims()

    def rank(adapter):
        # occupancy first, like connect scoring - but scans also carry
        # start-failure memory: scan selection has no failure-driven walk,
        # so without it a dead-but-listed adapter would win every tie
        # forever (a successful start clears the count)
        entry = snapshot.get(adapter) or {}
        occupancy = entry.get("soft", 0) + entry.get("links", 0)
        return (occupancy + _scan_failures.get(adapter, 0), usable.index(adapter))

    ranked = sorted(usable, key=rank)
    for adapter in ranked:
        claim = config.claims.claim_hard(adapter)
        if claim is not None:
            return adapter, claim
    logger.info(f"bt-claims: every adapter is scan-claimed, scanning on {ranked[0]} unclaimed")
    return ranked[0], None


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
        self._catcher_tasks = set()
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
        # an abandoned running scanner can never be stopped by its owner:
        # when the wrapper is collected, the scan claim frees itself on the
        # next heartbeat rather than marking the card scanning forever
        claim = self._catcher_claim
        if claim is None:
            return
        ref = weakref.ref(self)
        claim.validity = lambda: ref() is not None

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
            await self.stop()
            if adapter and (never_saw or quiet > SCANNER_WATCHDOG_MULTIPLE):
                config = _config
                manager = config.claims if config is not None else None
                await recovery.reset_adapter(adapter, claims_manager=manager, gone_silent=True)
            await self.start()
        except Exception:
            logger.exception("BLE scanner watchdog restart failed")
        finally:
            self._catcher_restarting = False

    async def start(self):
        detection_callback, service_uuids, scanning_mode = self._catcher_args
        init_kwargs = dict(self._catcher_kwargs)
        # a restart on this instance must not leak the previous claim
        self._release_scan_claim()
        config = _config
        self._catcher_manager = config.claims if config is not None else None
        adapter = None
        explicit = init_kwargs.get("adapter") or (init_kwargs.get("bluez") or {}).get("adapter")
        if explicit:
            if config is not None:
                self._catcher_claim = config.claims.claim_hard(explicit)
        else:
            adapter, self._catcher_claim = _acquire_scan_adapter()
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
            result = await _ORIGINAL_BLEAK_SCANNER.start(self)
            started = True
        except Exception:
            # a failed start counts against this adapter in scan placement
            # (cancellation does not - it says nothing about the radio)
            _scan_finished(explicit or adapter, False)
            raise
        finally:
            if not started:
                # finally, not except: a cancelled start must release the
                # scan claim too, and drop the partially-initialised backend
                self._release_scan_claim()
                self._backend = None
        _scan_finished(explicit or adapter, True)
        self._catcher_adapter = explicit or adapter
        now = _monotonic()
        self._catcher_start_time = now
        self._catcher_last_detection = now
        self._schedule_watchdog()
        self._arm_claim_validity()
        return result

    async def stop(self):
        self._cancel_watchdog()
        if self._backend is None:
            return
        try:
            return await _ORIGINAL_BLEAK_SCANNER.stop(self)
        finally:
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


def install_bleak_catcher(owner, adapters=(), link_caps=None, claim_dir=CLAIM_DIR, wrap_scanner=False, tune_conn_params=True, scan_to_score=False):
    """Route every bleak client in this process through the catcher.

    Must run before consumer libraries are imported: they capture `from
    bleak import BleakClient` at import time. A consumer that imported
    BleakClientWithServiceCache before install and passes the stale original
    as client_class silently skips cache-clear - degradation, not breakage.

    owner names this process's claims; the pid is appended to disambiguate
    restart races (the old process's claims awaiting reap while the new one
    starts). adapters are raw config strings, verbatim ("MAC@hciX" pins,
    plain "hciX" pools - see parse_adapter_entries). link_caps maps adapter
    name to its established-link capacity; caps are opt-in, an uncapped
    adapter is never slot-gated. wrap_scanner additionally rebinds
    bleak.BleakScanner to the adapter-bound, hard-claiming BLEScanner -
    opt-in because it changes which adapter unrelated code scans on.
    tune_conn_params loads habluetooth's fast-then-medium connection
    parameters over the mgmt socket around each connect; it degrades to a
    no-op wherever the mgmt channel is unavailable (non-Linux, no
    NET_ADMIN). scan_to_score is the driver's placement-mode choice: False
    routes unpinned devices least-used (occupancy and failures only), True
    additionally runs periodic short RSSI sweeps per adapter (RssiSweeper)
    so the score gains its habluetooth RSSI base - a driver that scans can
    place by signal, one that will not scan still spreads load. Idempotent.
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
    )
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
