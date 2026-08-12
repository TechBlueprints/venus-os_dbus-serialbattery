"""Managed BLE scanning with adapter rotation, locking, and InProgress retry.

This is the scan counterpart to :mod:`bleak_connection_manager.connection`.
It wraps ``BleakScanner`` with:

- **Per-adapter scan lock** — in-process ``asyncio.Lock`` so only one
  scan per adapter runs at a time within this process (bleak shares one
  D-Bus client, and a same-client double ``StartDiscovery`` returns
  ``InProgress``; cross-process scans are merged by BlueZ >= 5.50 and
  need no coordination).
- **Adapter rotation** — if the scan lock on the current adapter can't be
  acquired quickly, or if BlueZ returns ``InProgress``, automatically
  rotate to the next adapter.
- **Hard timeout** — wraps every ``BleakScanner`` call in
  ``asyncio.wait_for()`` to guard against Stuck State 16 (scanner hangs
  ignoring its own timeout parameter).
- **Retry with backoff** — transient scan failures get retried across
  adapters before giving up.

Each workaround is independently removable.  When upstream or BlueZ
fixes the root cause, the corresponding code path can be deleted.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.exc import BleakError

from .adapters import discover_adapters, pick_adapter
from .bluez import (
    _power_cycle_adapter_with_cooldown,
    ensure_adapter_scan_ready,
    ensure_adapters_up,
    get_connected_devices,
    try_stop_discovery,
)
from .const import IS_LINUX, AdapterScanState, ScanLockConfig
from .diagnostics import StuckState, clear_stuck_state, diagnose_stuck_state
from .scan_lock import acquire_scan_lock, release_scan_lock

_LOGGER = logging.getLogger(__name__)

# Time to wait after clearing a phantom connection to let the
# peripheral's supervision timer expire and start advertising again.
# The clear itself includes a power cycle with its own wait, so this
# additional wait is just for the peripheral side.
_PHANTOM_CLEAR_WAIT = 3.0

# Extra seconds added to the BleakScanner timeout to form the hard
# asyncio.wait_for timeout.  This gives BleakScanner a chance to
# return normally before the hard timeout fires.
_HARD_TIMEOUT_BUFFER = 5.0

# How long to try acquiring the scan lock on a single adapter before
# giving up and rotating.  Short — we'd rather try another adapter
# than wait a long time for one.
_LOCK_ATTEMPT_TIMEOUT = 2.0

# Brief pause between retries on different adapters.
_RETRY_BACKOFF = 0.25


def _is_inprogress(exc: BaseException) -> bool:
    """Check if an exception is an InProgress error."""
    err_str = str(exc).lower()
    return "inprogress" in err_str or "in progress" in err_str


def _is_passive_scan(**scanner_kwargs: Any) -> bool:
    """Check if the caller requested passive scanning.

    Passive scanning uses BlueZ AdvertisementMonitor1 instead of
    StartDiscovery, so it cannot cause InProgress contention and
    does not need the scan lock or adapter health checks.
    """
    return str(scanner_kwargs.get("scanning_mode", "")).lower() == "passive"


def _is_service_unknown(exc: BaseException) -> bool:
    """Check if an exception is a D-Bus ServiceUnknown error (bluetoothd not running)."""
    from .dbus_bus import is_service_unknown_error
    return is_service_unknown_error(exc)


async def _try_recover_adapter(
    adapter: str,
    effective_adapters: list[str],
    reason: str,
) -> None:
    """Attempt non-destructive recovery of a stuck adapter.

    Uses the tiered strategy:
    - **Tier 1**: Try ``StopDiscovery`` from our bus (zero-cost).
    - **Tier 2**: If other adapters exist, just log and let the
      scan loop rotate.  Do NOT power-cycle.
    - Power-cycling (Tier 3) is NOT done here — it is deferred
      to ``_last_resort_power_cycle`` after all adapters are
      exhausted.

    Parameters
    ----------
    adapter:
        The adapter that is stuck.
    effective_adapters:
        All available adapters (used to decide whether rotation
        is possible).
    reason:
        Human-readable description of why recovery is needed
        (for log messages).
    """
    if not IS_LINUX:
        return

    # Tier 1: try StopDiscovery
    cleared = await try_stop_discovery(adapter)
    if cleared:
        return

    # Tier 2: if we have other adapters, rotation handles it
    if len(effective_adapters) > 1:
        _LOGGER.info(
            "%s: %s — will rotate to another adapter "
            "(avoiding power-cycle to preserve connections)",
            adapter,
            reason,
        )
    else:
        _LOGGER.warning(
            "%s: %s — single adapter, no rotation possible",
            adapter,
            reason,
        )


async def _last_resort_power_cycle(
    stuck_adapters: set[str],
    effective_adapters: list[str],
) -> str | None:
    """Power-cycle the best candidate adapter as a last resort.

    Called when all adapters are stuck and no scan can proceed.
    Picks the adapter with the fewest active BLE connections to
    minimize disruption.

    Returns the adapter name that was power-cycled, or ``None``
    if none could be cycled (cooldown, errors, etc.).
    """
    if not IS_LINUX or not stuck_adapters:
        return None

    # Score each stuck adapter: prefer the one with fewest connections
    best_adapter: str | None = None
    best_count: int | None = None

    for adapter in effective_adapters:
        if adapter not in stuck_adapters:
            continue
        connected = await get_connected_devices(adapter)
        count = len(connected)
        if best_count is None or count < best_count:
            best_adapter = adapter
            best_count = count

    if best_adapter is None:
        return None

    _LOGGER.warning(
        "All adapters stuck — power-cycling %s as last resort "
        "(%d active connection(s))",
        best_adapter,
        best_count,
    )
    if await _power_cycle_adapter_with_cooldown(best_adapter):
        return best_adapter
    return None


async def _diagnose_inprogress(adapter: str, address: str) -> None:
    """Log BlueZ adapter state when InProgress occurs.

    Queries the adapter's ``Discovering`` property and connected device
    count via D-Bus to help identify *why* the adapter returned
    InProgress.  Possible causes:

    - ``Discovering: true`` — another process or a previous scan left
      the adapter in discovery mode.
    - ``Discovering: false`` — BlueZ has stale internal state from a
      previous scan that was never properly cleaned up.
    - Connected device count can reveal if the adapter is saturated.

    This is diagnostic-only — it never modifies state.  Uses the shared
    bus from :mod:`.dbus_bus` with raw ``bus.call()`` — no proxy objects,
    no introspection, no fire-and-forget ``AddMatch``.
    """
    if not IS_LINUX:
        return
    try:
        from dbus_fast import Message, MessageType

        from .dbus_bus import get_bus

        bus = await get_bus()
        adapter_path = f"/org/bluez/{adapter}"

        discovering_reply = await bus.call(
            Message(
                destination="org.bluez",
                path=adapter_path,
                interface="org.freedesktop.DBus.Properties",
                member="Get",
                signature="ss",
                body=["org.bluez.Adapter1", "Discovering"],
            )
        )
        powered_reply = await bus.call(
            Message(
                destination="org.bluez",
                path=adapter_path,
                interface="org.freedesktop.DBus.Properties",
                member="Get",
                signature="ss",
                body=["org.bluez.Adapter1", "Powered"],
            )
        )

        if (
            discovering_reply.message_type == MessageType.ERROR
            or powered_reply.message_type == MessageType.ERROR
        ):
            _LOGGER.debug(
                "%s: D-Bus error querying adapter %s state",
                address, adapter,
            )
            return

        disc_val = discovering_reply.body[0]
        if hasattr(disc_val, "value"):
            disc_val = disc_val.value
        pow_val = powered_reply.body[0]
        if hasattr(pow_val, "value"):
            pow_val = pow_val.value

        _LOGGER.warning(
            "%s: InProgress on %s — adapter state: "
            "Powered=%s, Discovering=%s. "
            "If Discovering=False, BlueZ has stale internal scan state "
            "from a previous failed scan (likely our own code or vesmart). "
            "If Discovering=True, another process holds a discovery session.",
            address, adapter, pow_val, disc_val,
        )
    except Exception:
        _LOGGER.debug(
            "%s: Could not query %s adapter state for InProgress diagnosis",
            address, adapter, exc_info=True,
        )



async def _find_in_bluez_cache(address: str) -> BLEDevice | None:
    """Check if BlueZ already has a cached device entry from another scanner.

    Uses the shared bus from :mod:`.dbus_bus` with raw ``bus.call()`` —
    no proxy objects, no introspection, no fire-and-forget ``AddMatch``.
    """
    try:
        from dbus_fast import Message, MessageType

        from .dbus_bus import get_bus
    except ImportError:
        return None

    addr_path_suffix = address.upper().replace(":", "_")
    bus = await get_bus()

    reply = await bus.call(
        Message(
            destination="org.bluez",
            path="/",
            interface="org.freedesktop.DBus.ObjectManager",
            member="GetManagedObjects",
        )
    )
    if reply.message_type == MessageType.ERROR:
        return None

    objects = reply.body[0]
    for path, interfaces in objects.items():
        if not path.endswith(addr_path_suffix):
            continue
        dev_props = interfaces.get("org.bluez.Device1", {})
        if not dev_props:
            continue
        dev_addr = dev_props.get("Address", {})
        if hasattr(dev_addr, "value"):
            dev_addr = dev_addr.value
        dev_name = dev_props.get("Name", {})
        if hasattr(dev_name, "value"):
            dev_name = dev_name.value
        dev_rssi = dev_props.get("RSSI", {})
        if hasattr(dev_rssi, "value"):
            dev_rssi = dev_rssi.value
        if isinstance(dev_addr, str) and dev_addr.upper() == address.upper():
            return BLEDevice(
                address=dev_addr,
                name=dev_name if isinstance(dev_name, str) else None,
                rssi=dev_rssi if isinstance(dev_rssi, int) else 0,
                details={"path": path},
            )

    return None


async def _find_all_in_bluez_cache() -> list[BLEDevice]:
    """Return all ``Device1`` entries from the BlueZ D-Bus cache.

    Used as a fallback for ``discover()`` when an external raw HCI scan
    is active — the scan continuously populates the cache via kernel
    mgmt events, so the cache contents are reasonably fresh.
    """
    try:
        from dbus_fast import Message, MessageType

        from .dbus_bus import get_bus
    except ImportError:
        return []

    bus = await get_bus()
    reply = await bus.call(
        Message(
            destination="org.bluez",
            path="/",
            interface="org.freedesktop.DBus.ObjectManager",
            member="GetManagedObjects",
        )
    )
    if reply.message_type == MessageType.ERROR:
        return []

    devices: list[BLEDevice] = []
    objects = reply.body[0]
    for path, interfaces in objects.items():
        dev_props = interfaces.get("org.bluez.Device1")
        if dev_props is None:
            continue
        dev_addr = dev_props.get("Address")
        if hasattr(dev_addr, "value"):
            dev_addr = dev_addr.value
        if not isinstance(dev_addr, str):
            continue
        dev_name = dev_props.get("Name")
        if hasattr(dev_name, "value"):
            dev_name = dev_name.value
        dev_rssi = dev_props.get("RSSI")
        if hasattr(dev_rssi, "value"):
            dev_rssi = dev_rssi.value
        devices.append(
            BLEDevice(
                address=dev_addr,
                name=dev_name if isinstance(dev_name, str) else None,
                rssi=dev_rssi if isinstance(dev_rssi, int) else 0,
                details={"path": path},
            )
        )
    return devices


# How often to re-check the BlueZ cache while waiting for another
# process's scan to populate it.
_CACHE_POLL_INTERVAL = 0.5


async def _poll_cache_while_locked(
    address: str,
    wait_seconds: float,
) -> BLEDevice | None:
    """Poll the BlueZ D-Bus cache while another scan holds the scan lock.

    When the scan lock is busy, we know another process is running
    ``StartDiscovery``.  Its results will appear in the shared BlueZ
    D-Bus object tree.  Rather than wait for the lock and scan again
    ourselves, we poll the cache — if the other process finds our
    device, we can return it without ever scanning.

    Returns the cached ``BLEDevice`` if found, or ``None`` after
    *wait_seconds* expires.
    """
    elapsed = 0.0
    while elapsed < wait_seconds:
        await asyncio.sleep(_CACHE_POLL_INTERVAL)
        elapsed += _CACHE_POLL_INTERVAL
        try:
            cached = await _find_in_bluez_cache(address)
            if cached is not None:
                return cached
        except Exception:
            pass  # Cache lookup failed — keep polling
    return None


async def find_device(
    address: str,
    *,
    timeout: float = 10.0,
    max_attempts: int = 3,
    adapters: list[str] | None = None,
    scan_lock_config: ScanLockConfig | None = None,
    **scanner_kwargs: Any,
) -> BLEDevice | None:
    """Find a BLE device by address with cache-first strategy.

    **Cache-first approach** (avoids ``StartDiscovery`` when possible):

    1. Check the BlueZ D-Bus cache first.  If another process (or a
       previous scan by this process) already discovered the device,
       its ``Device1`` entry persists in BlueZ's object tree and we
       return it immediately — **zero scanning overhead**.

    2. If the cache misses, try to acquire the scan lock.  If the lock
       is busy (another process is actively scanning), poll the cache
       while waiting — that other process's scan results will appear
       in the shared BlueZ cache.

    3. Only if the cache remains empty *and* we hold the scan lock do
       we actually call ``BleakScanner.find_device_by_address()``
       (which triggers ``StartDiscovery``).

    This eliminates the vast majority of ``StartDiscovery`` calls and
    ``InProgress`` errors in multi-process environments.

    Parameters
    ----------
    address:
        The BLE device MAC address to find.
    timeout:
        Per-attempt scan timeout in seconds.  Each attempt on each
        adapter gets this much time.
    max_attempts:
        Maximum total attempts across all adapters.
    adapters:
        List of adapters to rotate through.  If ``None``, auto-discovered.
    scan_lock_config:
        Cross-process scan lock configuration.  If ``None`` or
        ``enabled=False``, no locking is performed.
    **scanner_kwargs:
        Additional keyword arguments passed to
        ``BleakScanner.find_device_by_address()`` (e.g. ``scanning_mode``).

    Returns
    -------
    BLEDevice | None
        The found device, or ``None`` if not found after all attempts.
    """
    # Ensure BlueZ is available before any BLE operations.  On Venus OS
    # our service may start before bluetoothd registers on D-Bus.
    if IS_LINUX:
        from .dbus_bus import wait_for_bluez
        if not await wait_for_bluez():
            _LOGGER.warning(
                "%s: BlueZ not available — skipping scan",
                address,
            )
            return None

    if adapters is None and IS_LINUX:
        adapters = discover_adapters()
    effective_adapters = adapters or ["hci0"]

    # ── Self-heal: bring up any adapters left DOWN ─────────────────
    if IS_LINUX:
        await ensure_adapters_up(effective_adapters)

    # ── Step 1: Cache-first — check BlueZ D-Bus cache ─────────────
    #
    # If any process has previously scanned and found this device,
    # BlueZ keeps a Device1 object in its cache.  We can return it
    # directly without triggering StartDiscovery.
    if IS_LINUX:
        try:
            cached = await _find_in_bluez_cache(address)
            if cached is not None:
                _LOGGER.debug(
                    "%s: Found in BlueZ cache — scan avoided",
                    address,
                )
                return cached
        except Exception:
            _LOGGER.debug(
                "%s: BlueZ cache lookup failed, will scan",
                address,
                exc_info=True,
            )

    # ── Step 2: Pre-scan phantom check ─────────────────────────────
    #
    # If BlueZ has a stale "Connected" entry for this device but HCI
    # shows no actual connection, clear it.  Without this, the
    # peripheral thinks it's still connected and won't advertise,
    # making all scan attempts fail.
    if IS_LINUX:
        try:
            primary_adapter = effective_adapters[0]
            state = await diagnose_stuck_state(
                address, primary_adapter, adapters=effective_adapters,
            )
            if state in (
                StuckState.PHANTOM_NO_HANDLE,
                StuckState.INACTIVE_CONNECTION,
                StuckState.ORPHAN_HCI_HANDLE,
            ):
                _LOGGER.info(
                    "%s: Pre-scan detected %s, clearing before scan",
                    address,
                    state.value,
                )
                await clear_stuck_state(
                    address, primary_adapter, state,
                    adapters=effective_adapters,
                )
                # Give the peripheral time to notice the link is gone
                # and start advertising again (supervision timeout).
                await asyncio.sleep(_PHANTOM_CLEAR_WAIT)
        except Exception:
            _LOGGER.debug(
                "%s: Pre-scan phantom check failed, proceeding",
                address,
                exc_info=True,
            )

    # ── Step 3: Scan with lock coordination ────────────────────────
    #
    # If the scan lock is busy, another scan in this process (or an
    # external client's merged discovery) is running.  Poll
    # the BlueZ cache while we wait — their scan results will show
    # up in the shared cache.  Only scan ourselves if the cache
    # stays empty and we acquire the lock.
    #
    # Passive scanning (AdvertisementMonitor1) does not call
    # StartDiscovery, so it cannot cause InProgress contention.
    # Skip the scan lock and adapter health check entirely.

    passive = _is_passive_scan(**scanner_kwargs)
    hard_timeout = timeout + _HARD_TIMEOUT_BUFFER
    last_error: Exception | None = None
    stuck_adapters: dict[str, AdapterScanState] = {}

    for attempt in range(1, max_attempts + 1):
        adapter = pick_adapter(effective_adapters, attempt)

        # --- Try to acquire the scan lock for this adapter ---
        fd: asyncio.Lock | None = None
        if not passive and scan_lock_config is not None and scan_lock_config.enabled:
            # Try non-blocking first (timeout=0)
            instant_cfg = ScanLockConfig(
                enabled=True,
                lock_dir=scan_lock_config.lock_dir,
                lock_template=scan_lock_config.lock_template,
                lock_timeout=0.0,
            )
            fd = await acquire_scan_lock(instant_cfg, adapter)

            if fd is None:
                # Lock is busy — another scan in this process is
                # running on this adapter.  Poll the BlueZ cache while
                # waiting; its results appear in the shared cache.
                _LOGGER.debug(
                    "%s: Scan lock busy on %s, polling cache (attempt %d/%d)",
                    address,
                    adapter,
                    attempt,
                    max_attempts,
                )
                cached = await _poll_cache_while_locked(
                    address, _LOCK_ATTEMPT_TIMEOUT,
                )
                if cached is not None:
                    _LOGGER.debug(
                        "%s: Found in cache while another scan ran",
                        address,
                    )
                    return cached

                # Cache still empty — try the lock one more time with
                # a short timeout before rotating.
                short_cfg = ScanLockConfig(
                    enabled=True,
                    lock_dir=scan_lock_config.lock_dir,
                    lock_template=scan_lock_config.lock_template,
                    lock_timeout=min(
                        _LOCK_ATTEMPT_TIMEOUT,
                        scan_lock_config.lock_timeout,
                    ),
                )
                fd = await acquire_scan_lock(short_cfg, adapter)
                if fd is None and len(effective_adapters) > 1:
                    _LOGGER.debug(
                        "%s: Scan lock still busy on %s, rotating",
                        address,
                        adapter,
                    )
                    continue

        # ── Pre-scan adapter health check ──────────────────────────
        if IS_LINUX and not passive:
            try:
                scan_state = await ensure_adapter_scan_ready(adapter)
                if scan_state == AdapterScanState.EXTERNAL_SCAN:
                    _LOGGER.info(
                        "%s: External scan detected on %s, polling "
                        "cache instead of scanning (attempt %d/%d)",
                        address,
                        adapter,
                        attempt,
                        max_attempts,
                    )
                    release_scan_lock(fd)
                    fd = None
                    cached = await _poll_cache_while_locked(
                        address, timeout,
                    )
                    if cached is not None:
                        _LOGGER.info(
                            "%s: Found in cache via external scan on %s",
                            address,
                            adapter,
                        )
                        return cached
                    continue
                elif scan_state == AdapterScanState.STUCK:
                    _LOGGER.warning(
                        "%s: Adapter %s stuck (orphaned session), "
                        "rotating (attempt %d/%d)",
                        address,
                        adapter,
                        attempt,
                        max_attempts,
                    )
                    stuck_adapters[adapter] = AdapterScanState.STUCK
                    release_scan_lock(fd)
                    fd = None
                    continue
            except Exception:
                _LOGGER.debug(
                    "%s: Pre-scan health check failed on %s, proceeding",
                    address,
                    adapter,
                    exc_info=True,
                )
            except BaseException:
                release_scan_lock(fd)
                raise

        try:
            _LOGGER.debug(
                "%s: Scanning on %s (attempt %d/%d, timeout=%.1f s)",
                address,
                adapter,
                attempt,
                max_attempts,
                timeout,
            )

            kwargs: dict[str, Any] = dict(scanner_kwargs)
            if IS_LINUX:
                kwargs.setdefault("adapter", adapter)

            device = await asyncio.wait_for(
                BleakScanner.find_device_by_address(
                    address,
                    timeout=timeout,
                    **kwargs,
                ),
                timeout=hard_timeout,
            )

            if device is not None:
                _LOGGER.debug(
                    "%s: Found on %s (attempt %d/%d)",
                    address,
                    adapter,
                    attempt,
                    max_attempts,
                )
                return device

            _LOGGER.debug(
                "%s: Not found on %s (attempt %d/%d)",
                address,
                adapter,
                attempt,
                max_attempts,
            )

        except (BleakError, asyncio.TimeoutError) as exc:
            last_error = exc
            if _is_inprogress(exc):
                _LOGGER.warning(
                    "%s: InProgress on %s, rotating (attempt %d/%d)",
                    address,
                    adapter,
                    attempt,
                    max_attempts,
                )
                await _diagnose_inprogress(adapter, address)
                stuck_adapters[adapter] = AdapterScanState.STUCK
                await _try_recover_adapter(
                    adapter, effective_adapters,
                    "InProgress during scan",
                )
            elif isinstance(exc, asyncio.TimeoutError):
                _LOGGER.warning(
                    "%s: Scanner hard timeout on %s after %.0f s "
                    "(Stuck State 16), rotating (attempt %d/%d)",
                    address,
                    adapter,
                    hard_timeout,
                    attempt,
                    max_attempts,
                )
                stuck_adapters[adapter] = AdapterScanState.STUCK
                await _try_recover_adapter(
                    adapter, effective_adapters,
                    "hard timeout (Stuck State 16)",
                )
            elif IS_LINUX and _is_service_unknown(exc):
                # bluetoothd died at runtime — clear the ready flag and
                # wait for it to restart before the next attempt.
                from .dbus_bus import mark_bluez_gone, wait_for_bluez
                mark_bluez_gone()
                _LOGGER.warning(
                    "%s: org.bluez gone on %s (bluetoothd crashed?), "
                    "waiting for restart (attempt %d/%d)",
                    address,
                    adapter,
                    attempt,
                    max_attempts,
                )
                await wait_for_bluez(timeout=30.0)
            else:
                _LOGGER.debug(
                    "%s: Scan error on %s: %s (attempt %d/%d)",
                    address,
                    adapter,
                    exc,
                    attempt,
                    max_attempts,
                )

        finally:
            release_scan_lock(fd)

        if attempt < max_attempts:
            await asyncio.sleep(_RETRY_BACKOFF)

    # ── Last resort: all attempts exhausted ────────────────────────
    #
    # Only power-cycle adapters that are STUCK (not EXTERNAL_SCAN —
    # power-cycling is futile when an external raw HCI scanner will
    # re-corrupt the state immediately).
    truly_stuck = {
        a for a, state in stuck_adapters.items()
        if state == AdapterScanState.STUCK
    }
    if IS_LINUX and truly_stuck:
        await _last_resort_power_cycle(truly_stuck, effective_adapters)

    # Check BlueZ cache — another process's scan or the power-cycle
    # may have populated it.
    if IS_LINUX and last_error is not None and _is_inprogress(last_error):
        _LOGGER.info(
            "%s: All scans failed with InProgress, checking BlueZ cache",
            address,
        )
        try:
            cached = await _find_in_bluez_cache(address)
            if cached is not None:
                _LOGGER.info(
                    "%s: Found in BlueZ cache (bypassed InProgress)",
                    address,
                )
                return cached
        except Exception:
            _LOGGER.debug(
                "%s: BlueZ cache lookup failed", address, exc_info=True,
            )

    if last_error is not None:
        _LOGGER.warning(
            "%s: All %d scan attempts failed, last error: %s",
            address,
            max_attempts,
            last_error,
        )
    return None


async def discover(
    *,
    timeout: float = 5.0,
    max_attempts: int = 2,
    adapters: list[str] | None = None,
    scan_lock_config: ScanLockConfig | None = None,
    **scanner_kwargs: Any,
) -> list[BLEDevice]:
    """Discover BLE devices with automatic adapter rotation.

    Drop-in replacement for ``BleakScanner.discover()`` with scan lock,
    adapter rotation, and InProgress retry built in.

    Parameters
    ----------
    timeout:
        Per-attempt scan duration in seconds.
    max_attempts:
        Maximum total attempts across all adapters.
    adapters:
        List of adapters to rotate through.  If ``None``, auto-discovered.
    scan_lock_config:
        Cross-process scan lock configuration.  If ``None`` or
        ``enabled=False``, no locking is performed.
    **scanner_kwargs:
        Additional keyword arguments passed to
        ``BleakScanner.discover()`` (e.g. ``scanning_mode``).

    Returns
    -------
    list[BLEDevice]
        List of discovered devices.  May be empty if all attempts fail.
    """
    # Ensure BlueZ is available before any BLE operations.
    if IS_LINUX:
        from .dbus_bus import wait_for_bluez
        if not await wait_for_bluez():
            _LOGGER.warning("BlueZ not available — skipping discover")
            return []

    if adapters is None and IS_LINUX:
        adapters = discover_adapters()
    effective_adapters = adapters or ["hci0"]

    # Self-heal: bring up any adapters left DOWN.
    if IS_LINUX:
        await ensure_adapters_up(effective_adapters)

    passive = _is_passive_scan(**scanner_kwargs)
    hard_timeout = timeout + _HARD_TIMEOUT_BUFFER
    stuck_adapters: dict[str, AdapterScanState] = {}

    for attempt in range(1, max_attempts + 1):
        adapter = pick_adapter(effective_adapters, attempt)

        fd: asyncio.Lock | None = None
        if not passive and scan_lock_config is not None and scan_lock_config.enabled:
            lock_cfg_for_attempt = ScanLockConfig(
                enabled=True,
                lock_dir=scan_lock_config.lock_dir,
                lock_template=scan_lock_config.lock_template,
                lock_timeout=min(_LOCK_ATTEMPT_TIMEOUT, scan_lock_config.lock_timeout),
            )
            fd = await acquire_scan_lock(lock_cfg_for_attempt, adapter)
            if fd is None and len(effective_adapters) > 1:
                _LOGGER.debug(
                    "Scan lock busy on %s, rotating (attempt %d/%d)",
                    adapter,
                    attempt,
                    max_attempts,
                )
                continue

        # Pre-scan adapter health check
        if IS_LINUX and not passive:
            try:
                scan_state = await ensure_adapter_scan_ready(adapter)
                if scan_state == AdapterScanState.EXTERNAL_SCAN:
                    _LOGGER.info(
                        "External scan detected on %s, returning "
                        "cached devices (attempt %d/%d)",
                        adapter,
                        attempt,
                        max_attempts,
                    )
                    release_scan_lock(fd)
                    fd = None
                    try:
                        devices = await _find_all_in_bluez_cache()
                        _LOGGER.info(
                            "Returning %d devices from BlueZ cache "
                            "(external scan on %s)",
                            len(devices),
                            adapter,
                        )
                        return devices
                    except Exception:
                        _LOGGER.debug(
                            "Cache fallback failed on %s",
                            adapter,
                            exc_info=True,
                        )
                    continue
                elif scan_state == AdapterScanState.STUCK:
                    _LOGGER.warning(
                        "Adapter %s stuck (orphaned session), "
                        "rotating (attempt %d/%d)",
                        adapter,
                        attempt,
                        max_attempts,
                    )
                    stuck_adapters[adapter] = AdapterScanState.STUCK
                    release_scan_lock(fd)
                    fd = None
                    continue
            except Exception:
                _LOGGER.debug(
                    "Pre-scan health check failed on %s, proceeding",
                    adapter,
                    exc_info=True,
                )
            except BaseException:
                release_scan_lock(fd)
                raise

        try:
            _LOGGER.debug(
                "Discovering on %s (attempt %d/%d, timeout=%.1f s)",
                adapter,
                attempt,
                max_attempts,
                timeout,
            )

            kwargs: dict[str, Any] = dict(scanner_kwargs)
            if IS_LINUX:
                kwargs.setdefault("adapter", adapter)

            devices = await asyncio.wait_for(
                BleakScanner.discover(
                    timeout=timeout,
                    **kwargs,
                ),
                timeout=hard_timeout,
            )

            _LOGGER.debug(
                "Discovered %d devices on %s (attempt %d/%d)",
                len(devices),
                adapter,
                attempt,
                max_attempts,
            )
            return list(devices)

        except (BleakError, asyncio.TimeoutError) as exc:
            if _is_inprogress(exc):
                _LOGGER.warning(
                    "InProgress on %s, rotating (attempt %d/%d)",
                    adapter,
                    attempt,
                    max_attempts,
                )
                await _diagnose_inprogress(adapter, "discover")
                stuck_adapters[adapter] = AdapterScanState.STUCK
                await _try_recover_adapter(
                    adapter, effective_adapters,
                    "InProgress during discover",
                )
            elif isinstance(exc, asyncio.TimeoutError):
                _LOGGER.warning(
                    "Scanner hard timeout on %s after %.0f s "
                    "(Stuck State 16), rotating (attempt %d/%d)",
                    adapter,
                    hard_timeout,
                    attempt,
                    max_attempts,
                )
                stuck_adapters[adapter] = AdapterScanState.STUCK
                await _try_recover_adapter(
                    adapter, effective_adapters,
                    "hard timeout (Stuck State 16)",
                )
            elif IS_LINUX and _is_service_unknown(exc):
                from .dbus_bus import mark_bluez_gone, wait_for_bluez
                mark_bluez_gone()
                _LOGGER.warning(
                    "org.bluez gone on %s (bluetoothd crashed?), "
                    "waiting for restart (attempt %d/%d)",
                    adapter,
                    attempt,
                    max_attempts,
                )
                await wait_for_bluez(timeout=30.0)
            else:
                _LOGGER.debug(
                    "Scan error on %s: %s (attempt %d/%d)",
                    adapter,
                    exc,
                    attempt,
                    max_attempts,
                )

        finally:
            release_scan_lock(fd)

        if attempt < max_attempts:
            await asyncio.sleep(_RETRY_BACKOFF)

    # Only power-cycle STUCK adapters (not EXTERNAL_SCAN).
    truly_stuck = {
        a for a, state in stuck_adapters.items()
        if state == AdapterScanState.STUCK
    }
    if IS_LINUX and truly_stuck:
        await _last_resort_power_cycle(truly_stuck, effective_adapters)

    return []
