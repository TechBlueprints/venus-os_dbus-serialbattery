"""BlueZ D-Bus utilities for connection state inspection.

Provides functions to detect phantom/inactive BLE connections and
construct BlueZ D-Bus object paths.  Uses the shared ``MessageBus``
from :mod:`.dbus_bus` and raw ``bus.call()`` for all queries — no
proxy objects, no introspection, no fire-and-forget ``AddMatch``.

These functions fill a gap: ``bleak-retry-connector`` does not expose
connection state inspection, and ``bleak`` itself does not provide
phantom detection.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from .const import IS_LINUX, AdapterScanState

_LOGGER = logging.getLogger(__name__)

# D-Bus constants
_BLUEZ_SERVICE = "org.bluez"
_DEVICE_INTERFACE = "org.bluez.Device1"
_ADAPTER_INTERFACE = "org.bluez.Adapter1"
_PROPERTIES_INTERFACE = "org.freedesktop.DBus.Properties"
_OBJECT_MANAGER_INTERFACE = "org.freedesktop.DBus.ObjectManager"


def address_to_bluez_path(address: str, adapter: str = "hci0") -> str:
    """Convert a BLE address + adapter to a BlueZ D-Bus object path.

    Example::

        >>> address_to_bluez_path("AA:BB:CC:DD:EE:FF", "hci0")
        '/org/bluez/hci0/dev_AA_BB_CC_DD_EE_FF'
    """
    dev_part = f"dev_{address.upper().replace(':', '_')}"
    return f"/org/bluez/{adapter}/{dev_part}"


async def _get_device_properties(
    address: str, adapter: str = "hci0"
) -> dict[str, Any] | None:
    """Fetch D-Bus properties for a BLE device.

    Returns a dict of property name → value, or ``None`` if the device
    is not found on D-Bus.
    """
    if not IS_LINUX:
        return None

    try:
        from dbus_fast import Message, MessageType

        from .dbus_bus import get_bus
    except ImportError:
        _LOGGER.debug("dbus-fast not available, cannot query D-Bus")
        return None

    path = address_to_bluez_path(address, adapter)

    try:
        bus = await get_bus()
        reply = await bus.call(
            Message(
                destination=_BLUEZ_SERVICE,
                path=path,
                interface=_PROPERTIES_INTERFACE,
                member="GetAll",
                signature="s",
                body=[_DEVICE_INTERFACE],
            )
        )
        if reply.message_type == MessageType.ERROR:
            return None
        return {k: v.value for k, v in reply.body[0].items()}
    except Exception:
        _LOGGER.debug(
            "Failed to get D-Bus properties for %s on %s",
            address,
            adapter,
            exc_info=True,
        )
        return None


async def is_inactive_connection(
    address: str, adapter: str = "hci0"
) -> bool:
    """Check whether a device has an inactive BLE connection.

    An inactive connection is one where BlueZ D-Bus reports
    ``Connected=True`` but ``ServicesResolved`` is not ``True``.
    This indicates GATT discovery never completed — typically because
    the HCI transport died after the initial connection event.

    Parameters
    ----------
    address:
        The BLE device MAC address (e.g. ``"AA:BB:CC:DD:EE:FF"``).
    adapter:
        The adapter name (e.g. ``"hci0"``).

    Returns ``True`` if the connection is inactive (phantom).
    """
    if not IS_LINUX:
        return False

    props = await _get_device_properties(address, adapter)
    if props is None:
        return False

    connected = props.get("Connected", False)
    if not connected:
        return False

    services_resolved = props.get("ServicesResolved", False)
    if services_resolved is not True:
        _LOGGER.debug(
            "%s: Inactive connection detected — Connected but "
            "ServicesResolved is not True",
            address,
        )
        return True

    return False


async def remove_device(address: str, adapter: str = "hci0") -> bool:
    """Remove a device from BlueZ via the adapter's RemoveDevice method.

    This is the D-Bus equivalent of ``bluetoothctl remove <address>``.
    Clears all cached state including GATT services.

    Returns ``True`` if the device was removed successfully.
    """
    if not IS_LINUX:
        return False

    try:
        from dbus_fast import Message, MessageType

        from .dbus_bus import get_bus
    except ImportError:
        return False

    adapter_path = f"/org/bluez/{adapter}"
    device_path = address_to_bluez_path(address, adapter)

    try:
        bus = await get_bus()
        reply = await bus.call(
            Message(
                destination=_BLUEZ_SERVICE,
                path=adapter_path,
                interface=_ADAPTER_INTERFACE,
                member="RemoveDevice",
                signature="o",
                body=[device_path],
            )
        )
        if reply.message_type == MessageType.ERROR:
            _LOGGER.debug(
                "RemoveDevice D-Bus error for %s: %s",
                address,
                reply.body[0] if reply.body else "unknown",
            )
            return False
        _LOGGER.debug("Removed device %s from %s", address, adapter)
        return True
    except Exception:
        _LOGGER.debug(
            "Failed to remove %s from %s",
            address,
            adapter,
            exc_info=True,
        )
        return False


async def disconnect_device(address: str, adapter: str = "hci0") -> bool:
    """Disconnect a device via D-Bus.

    Returns ``True`` if the disconnect was initiated successfully.
    """
    if not IS_LINUX:
        return False

    try:
        from dbus_fast import Message, MessageType

        from .dbus_bus import get_bus
    except ImportError:
        return False

    path = address_to_bluez_path(address, adapter)

    try:
        bus = await get_bus()
        reply = await bus.call(
            Message(
                destination=_BLUEZ_SERVICE,
                path=path,
                interface=_DEVICE_INTERFACE,
                member="Disconnect",
            )
        )
        if reply.message_type == MessageType.ERROR:
            _LOGGER.debug(
                "Disconnect D-Bus error for %s: %s",
                address,
                reply.body[0] if reply.body else "unknown",
            )
            return False
        _LOGGER.debug("Disconnected %s on %s via D-Bus", address, adapter)
        return True
    except Exception:
        _LOGGER.debug(
            "Failed to disconnect %s on %s",
            address,
            adapter,
            exc_info=True,
        )
        return False


async def verified_disconnect(
    address: str,
    adapter: str = "hci0",
    timeout: float = 5.0,
    poll_interval: float = 0.5,
) -> bool:
    """Disconnect a device and verify the D-Bus ``Connected`` property is ``False``.

    Unlike :func:`disconnect_device`, this function polls the D-Bus
    ``Connected`` property after initiating the disconnect to confirm
    the device is truly disconnected.  If ``Connected`` remains ``True``
    after *timeout* seconds, it escalates to :func:`remove_device` to
    force-clear the stale BlueZ state.

    This addresses Stuck State 8 (disconnect hang) without using
    ``hcitool`` — purely via D-Bus.

    Parameters
    ----------
    address:
        The BLE device MAC address.
    adapter:
        The adapter name (e.g. ``"hci0"``).
    timeout:
        Maximum seconds to wait for ``Connected`` to become ``False``.
    poll_interval:
        Seconds between D-Bus property polls.

    Returns ``True`` if the device is confirmed disconnected, ``False``
    if the disconnect could not be verified (but cleanup was attempted).
    """
    if not IS_LINUX:
        return True

    # Step 1: Initiate disconnect via D-Bus
    await disconnect_device(address, adapter)

    # Step 2: Poll D-Bus Connected property
    elapsed = 0.0
    while elapsed < timeout:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

        props = await _get_device_properties(address, adapter)
        if props is None:
            # Device no longer on D-Bus — fully disconnected
            return True
        if not props.get("Connected", False):
            _LOGGER.debug(
                "%s: Verified disconnected on %s after %.1f s",
                address,
                adapter,
                elapsed,
            )
            return True

    # Step 3: Still connected after timeout — escalate to remove
    _LOGGER.warning(
        "%s: Still Connected=True on %s after %.1f s disconnect timeout, "
        "escalating to remove_device",
        address,
        adapter,
        timeout,
    )
    await remove_device(address, adapter)
    return False


# ── Adapter health check for scan readiness ────────────────────────
#
# Pre-scan detection and repair of stale BlueZ adapter state that
# causes ``InProgress`` errors on ``StartDiscovery``.

# Per-adapter cooldown tracking for power-cycle operations.
# Prevents rapid cascading power-cycles when multiple processes
# detect stale state within a short window.
_POWER_CYCLE_COOLDOWN = 30.0
_last_power_cycle: dict[str, float] = {}

# Settle time after power-cycling an adapter.  The adapter goes
# through power-off → power-on → BlueZ re-initialization.
_POWER_CYCLE_SETTLE = 2.0


async def get_adapter_discovering(adapter: str) -> bool | None:
    """Query the ``Discovering`` property of a BlueZ adapter.

    Returns ``True`` if the adapter is actively discovering,
    ``False`` if not, or ``None`` if the query failed.
    """
    if not IS_LINUX:
        return None

    try:
        from dbus_fast import Message, MessageType

        from .dbus_bus import get_bus
    except ImportError:
        return None

    adapter_path = f"/org/bluez/{adapter}"

    try:
        bus = await get_bus()
        reply = await bus.call(
            Message(
                destination=_BLUEZ_SERVICE,
                path=adapter_path,
                interface=_PROPERTIES_INTERFACE,
                member="Get",
                signature="ss",
                body=[_ADAPTER_INTERFACE, "Discovering"],
            )
        )
        if reply.message_type == MessageType.ERROR:
            return None
        val = reply.body[0]
        if hasattr(val, "value"):
            val = val.value
        return bool(val)
    except Exception:
        _LOGGER.debug(
            "Failed to query Discovering on %s", adapter, exc_info=True,
        )
        return None


async def _get_adapter_powered(adapter: str) -> bool | None:
    """Read the ``Powered`` property of a BlueZ adapter.

    Returns ``True``/``False`` or ``None`` on error.
    """
    if not IS_LINUX:
        return None
    try:
        from dbus_fast import Message, MessageType

        from .dbus_bus import get_bus
    except ImportError:
        return None

    try:
        bus = await get_bus()
        reply = await bus.call(
            Message(
                destination=_BLUEZ_SERVICE,
                path=f"/org/bluez/{adapter}",
                interface=_PROPERTIES_INTERFACE,
                member="Get",
                signature="ss",
                body=[_ADAPTER_INTERFACE, "Powered"],
            )
        )
        if reply.message_type == MessageType.ERROR:
            return None
        val = reply.body[0]
        if hasattr(val, "value"):
            val = val.value
        return bool(val)
    except Exception:
        _LOGGER.debug(
            "Failed to read Powered on %s", adapter, exc_info=True,
        )
        return None


async def _set_adapter_powered(adapter: str, powered: bool) -> bool:
    """Set the ``Powered`` property via D-Bus.

    Returns ``True`` if the call succeeded, ``False`` on error.
    """
    if not IS_LINUX:
        return False
    try:
        from dbus_fast import Message, MessageType, Variant

        from .dbus_bus import get_bus
    except ImportError:
        return False

    try:
        bus = await get_bus()
        reply = await bus.call(
            Message(
                destination=_BLUEZ_SERVICE,
                path=f"/org/bluez/{adapter}",
                interface=_PROPERTIES_INTERFACE,
                member="Set",
                signature="ssv",
                body=[_ADAPTER_INTERFACE, "Powered", Variant("b", powered)],
            )
        )
        if reply.message_type == MessageType.ERROR:
            _LOGGER.warning(
                "%s: Failed to set Powered=%s: %s",
                adapter,
                powered,
                reply.body[0] if reply.body else "unknown",
            )
            return False
        return True
    except Exception:
        _LOGGER.debug(
            "Failed to set Powered=%s on %s", powered, adapter, exc_info=True,
        )
        return False


def _hciconfig_up(adapter: str) -> bool:
    """Bring an adapter up via ``hciconfig`` as a subprocess fallback.

    This bypasses BlueZ's D-Bus path and talks directly to the kernel,
    which succeeds even when the D-Bus ``Powered=True`` fails (e.g.
    after an HCI_Reset timeout).
    """
    import subprocess

    try:
        result = subprocess.run(
            ["hciconfig", adapter, "up"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            _LOGGER.info(
                "%s: Brought adapter up via hciconfig fallback", adapter,
            )
            return True
        _LOGGER.warning(
            "%s: hciconfig up failed (rc=%d): %s",
            adapter,
            result.returncode,
            result.stderr.strip(),
        )
        return False
    except Exception:
        _LOGGER.warning(
            "%s: hciconfig up subprocess failed", adapter, exc_info=True,
        )
        return False


async def power_cycle_adapter(adapter: str) -> bool:
    """Power-cycle a BlueZ adapter via D-Bus ``Powered`` property.

    Sets ``Powered=False``, waits briefly, then ``Powered=True``.
    This drops ALL connections and discovery sessions on the adapter,
    clearing any stale BlueZ state.

    After setting ``Powered=True``, verifies the adapter actually
    came back up.  If D-Bus power-on fails (e.g. HCI_Reset timeout
    left the controller in a bad state), retries once via D-Bus
    then falls back to ``hciconfig <adapter> up``.

    Returns ``True`` if the adapter is powered on when this function
    returns, ``False`` if all recovery attempts failed.
    """
    if not IS_LINUX:
        return False

    # ── Step 1: Power off ──────────────────────────────────────────
    if not await _set_adapter_powered(adapter, False):
        return False

    await asyncio.sleep(0.5)

    # ── Step 2: Power on with verification ─────────────────────────
    await _set_adapter_powered(adapter, True)

    await asyncio.sleep(0.5)
    powered = await _get_adapter_powered(adapter)
    if powered is True:
        _LOGGER.info("%s: Power-cycled adapter via D-Bus", adapter)
        await asyncio.sleep(_POWER_CYCLE_SETTLE)
        return True

    # ── Step 3: D-Bus retry ────────────────────────────────────────
    _LOGGER.warning(
        "%s: Adapter still DOWN after Powered=True, retrying D-Bus", adapter,
    )
    await asyncio.sleep(1.0)
    await _set_adapter_powered(adapter, True)
    await asyncio.sleep(0.5)
    powered = await _get_adapter_powered(adapter)
    if powered is True:
        _LOGGER.info("%s: Power-cycled adapter via D-Bus (retry succeeded)", adapter)
        await asyncio.sleep(_POWER_CYCLE_SETTLE)
        return True

    # ── Step 4: hciconfig fallback ─────────────────────────────────
    _LOGGER.warning(
        "%s: D-Bus Powered=True failed twice, falling back to hciconfig",
        adapter,
    )
    await asyncio.to_thread(_hciconfig_up, adapter)
    await asyncio.sleep(1.0)
    powered = await _get_adapter_powered(adapter)
    if powered is True:
        _LOGGER.info(
            "%s: Adapter recovered via hciconfig fallback", adapter,
        )
        await asyncio.sleep(_POWER_CYCLE_SETTLE)
        return True

    _LOGGER.error(
        "%s: Adapter LEFT DOWN after all recovery attempts — "
        "manual intervention may be required",
        adapter,
    )
    return False


async def ensure_bluetoothd() -> bool:
    """Self-heal: restart ``bluetoothd`` if it has crashed.

    Lightweight check called at the start of scan operations (via
    :func:`ensure_adapters_up`) to detect a dead ``bluetoothd`` between
    scan cycles.  Uses a pure ``/proc`` scan — no D-Bus round-trip —
    so cost is negligible when ``bluetoothd`` is healthy.

    If ``bluetoothd`` is dead, attempts to restart it and waits up to
    15 seconds for it to register on D-Bus.

    Returns ``True`` if ``bluetoothd`` is running when the function
    returns (whether it was already running or freshly started).
    """
    if not IS_LINUX:
        return True

    from .recovery import is_bluetoothd_alive, restart_bluetoothd

    if is_bluetoothd_alive():
        return True

    _LOGGER.warning("bluetoothd is not running — attempting restart")

    if not await restart_bluetoothd():
        _LOGGER.error("Failed to restart bluetoothd")
        return False

    from .dbus_bus import wait_for_bluez

    if await wait_for_bluez(timeout=15.0):
        _LOGGER.info("bluetoothd restarted and BlueZ is back on D-Bus")
        return True

    _LOGGER.error("bluetoothd restarted but BlueZ did not appear on D-Bus")
    return False


async def ensure_adapters_up(adapters: list[str]) -> None:
    """Self-heal: bring up any adapters that are currently DOWN.

    Called at the start of scan operations to recover from failed
    power-cycles or other conditions that left an adapter powered off.
    Also verifies that ``bluetoothd`` is running and restarts it if needed.
    Tries D-Bus first, then falls back to ``hciconfig`` if needed.
    """
    if not IS_LINUX:
        return

    await ensure_bluetoothd()

    for adapter in adapters:
        powered = await _get_adapter_powered(adapter)
        if powered is False:
            _LOGGER.warning(
                "%s: Adapter is DOWN, attempting self-heal", adapter,
            )
            if await _set_adapter_powered(adapter, True):
                await asyncio.sleep(0.5)
                powered = await _get_adapter_powered(adapter)
            if powered is not True:
                _LOGGER.warning(
                    "%s: D-Bus power-on failed, trying hciconfig", adapter,
                )
                await asyncio.to_thread(_hciconfig_up, adapter)
                await asyncio.sleep(1.0)
                powered = await _get_adapter_powered(adapter)
            if powered is True:
                _LOGGER.info("%s: Self-healed — adapter is back UP", adapter)
            else:
                _LOGGER.error(
                    "%s: Self-heal FAILED — adapter remains DOWN", adapter,
                )


async def _power_cycle_adapter_with_cooldown(adapter: str) -> bool:
    """Power-cycle an adapter, respecting a per-adapter cooldown.

    If the adapter was power-cycled less than ``_POWER_CYCLE_COOLDOWN``
    seconds ago, skip the cycle and return ``False``.  This prevents
    rapid cascading cycles when multiple processes detect stale state
    within a short window.

    Before cycling, queries D-Bus for active connections on the adapter
    and logs a WARNING listing affected devices.  The caller is
    responsible for deciding that power-cycling is necessary (e.g. no
    other adapter available); this function always proceeds if the
    cooldown allows.

    Returns ``True`` if the power-cycle ran, ``False`` if skipped or
    failed.
    """
    now = time.monotonic()
    last = _last_power_cycle.get(adapter, 0.0)
    if (now - last) < _POWER_CYCLE_COOLDOWN:
        _LOGGER.debug(
            "%s: Power-cycle cooldown active (%.0fs remaining)",
            adapter,
            _POWER_CYCLE_COOLDOWN - (now - last),
        )
        return False

    connected = await get_connected_devices(adapter)
    if connected:
        _LOGGER.warning(
            "%s: Power-cycling adapter with %d active connection(s) "
            "that will be dropped: %s",
            adapter,
            len(connected),
            ", ".join(connected),
        )
    else:
        _LOGGER.debug("%s: No active connections, safe to power-cycle", adapter)

    result = await power_cycle_adapter(adapter)
    if result:
        _last_power_cycle[adapter] = time.monotonic()
    return result


async def try_stop_discovery(adapter: str) -> bool:
    """Attempt to clear a discovery session from our shared D-Bus bus.

    BlueZ ties each discovery session to the D-Bus connection that
    called ``StartDiscovery``.  If *our* bus started a session that
    was never stopped (e.g. ``BleakScanner`` cancelled mid-scan),
    calling ``StopDiscovery`` from the same bus will clear it.

    If the orphaned session belongs to a *different* D-Bus connection,
    this call will fail harmlessly with an error.

    Returns ``True`` if ``StopDiscovery`` succeeded (session cleared),
    ``False`` otherwise.
    """
    if not IS_LINUX:
        return False

    try:
        from dbus_fast import Message, MessageType

        from .dbus_bus import get_bus
    except ImportError:
        return False

    adapter_path = f"/org/bluez/{adapter}"

    try:
        bus = await get_bus()
        reply = await bus.call(
            Message(
                destination=_BLUEZ_SERVICE,
                path=adapter_path,
                interface=_ADAPTER_INTERFACE,
                member="StopDiscovery",
            )
        )
        if reply.message_type == MessageType.ERROR:
            _LOGGER.debug(
                "%s: StopDiscovery from our bus failed (expected if "
                "session belongs to another connection): %s",
                adapter,
                reply.body[0] if reply.body else "unknown",
            )
            return False
        _LOGGER.info(
            "%s: StopDiscovery from our bus succeeded — "
            "cleared orphaned session without power-cycle",
            adapter,
        )
        return True
    except Exception:
        _LOGGER.debug(
            "%s: StopDiscovery call failed", adapter, exc_info=True,
        )
        return False


async def get_connected_devices(adapter: str) -> list[str]:
    """Return MAC addresses of devices connected on *adapter*.

    Queries BlueZ ``GetManagedObjects`` and filters for
    ``org.bluez.Device1`` entries under this adapter where
    ``Connected=True``.

    Returns an empty list on error or non-Linux platforms.
    """
    if not IS_LINUX:
        return []

    try:
        from dbus_fast import Message, MessageType

        from .dbus_bus import get_bus
    except ImportError:
        return []

    adapter_prefix = f"/org/bluez/{adapter}/"

    try:
        bus = await get_bus()
        reply = await bus.call(
            Message(
                destination=_BLUEZ_SERVICE,
                path="/",
                interface=_OBJECT_MANAGER_INTERFACE,
                member="GetManagedObjects",
            )
        )
        if reply.message_type == MessageType.ERROR:
            return []

        connected: list[str] = []
        objects = reply.body[0]
        for path, interfaces in objects.items():
            if not path.startswith(adapter_prefix):
                continue
            dev_props = interfaces.get(_DEVICE_INTERFACE)
            if dev_props is None:
                continue
            conn_val = dev_props.get("Connected")
            if conn_val is not None and hasattr(conn_val, "value"):
                conn_val = conn_val.value
            if conn_val is True:
                addr_val = dev_props.get("Address")
                if addr_val is not None and hasattr(addr_val, "value"):
                    addr_val = addr_val.value
                if isinstance(addr_val, str):
                    connected.append(addr_val)
        return connected
    except Exception:
        _LOGGER.debug(
            "%s: Failed to enumerate connected devices",
            adapter,
            exc_info=True,
        )
        return []


async def _probe_start_discovery(adapter: str) -> bool | None:
    """Probe whether ``StartDiscovery`` works on an adapter.

    Calls ``StartDiscovery`` on the shared D-Bus bus.  If it succeeds,
    immediately calls ``StopDiscovery`` to clean up.

    Returns:
    - ``True`` if the adapter is clean (StartDiscovery succeeded).
    - ``False`` if ``InProgress`` was returned (stale internal state).
    - ``None`` on other errors (e.g. adapter not ready).

    Must be called **while holding the scan lock** to prevent
    interference from other processes.
    """
    if not IS_LINUX:
        return None

    try:
        from dbus_fast import Message, MessageType

        from .dbus_bus import get_bus
    except ImportError:
        return None

    adapter_path = f"/org/bluez/{adapter}"

    try:
        bus = await get_bus()

        reply = await bus.call(
            Message(
                destination=_BLUEZ_SERVICE,
                path=adapter_path,
                interface=_ADAPTER_INTERFACE,
                member="StartDiscovery",
            )
        )
        if reply.message_type == MessageType.ERROR:
            error_body = reply.body[0] if reply.body else ""
            error_name = getattr(reply, "error_name", "") or ""
            if "InProgress" in error_name or "InProgress" in str(error_body):
                return False
            _LOGGER.debug(
                "%s: StartDiscovery probe error: %s (%s)",
                adapter,
                error_name,
                error_body,
            )
            return None

        # Probe succeeded — adapter is clean.  Stop our test session.
        stop_reply = await bus.call(
            Message(
                destination=_BLUEZ_SERVICE,
                path=adapter_path,
                interface=_ADAPTER_INTERFACE,
                member="StopDiscovery",
            )
        )
        if stop_reply.message_type == MessageType.ERROR:
            _LOGGER.debug(
                "%s: StopDiscovery after probe failed (non-critical)",
                adapter,
            )
        return True

    except Exception:
        _LOGGER.debug(
            "%s: StartDiscovery probe failed", adapter, exc_info=True,
        )
        return None


async def ensure_adapter_scan_ready(adapter: str) -> AdapterScanState:
    """Pre-scan health check: detect and repair stale adapter state.

    Uses a tiered recovery strategy that avoids power-cycling
    whenever possible:

    **Tier 1** — Try ``StopDiscovery`` from our shared D-Bus bus.
    If the orphaned session belongs to *our* bus, this clears it
    without disrupting any connections.

    **Tier 2** — If Tier 1 fails, return a non-READY state so the
    scanner can adapt.  The caller is responsible for deciding
    whether to cache-poll, rotate, or escalate.

    This function never power-cycles the adapter directly.  That
    decision is deferred to the scanner loop which has visibility
    into whether alternative adapters are available and whether
    the stuck adapter has active connections.

    Returns
    -------
    AdapterScanState
        ``READY`` if the adapter can scan normally.
        ``STUCK`` if ``Discovering=True`` and can't be cleared
        (orphaned BlueZ session — rotation/power-cycle may help).
        ``EXTERNAL_SCAN`` if ``Discovering=False`` but
        ``StartDiscovery`` returns ``InProgress`` (external raw
        HCI scan corruption — cache polling is the correct
        response, power-cycling is futile).
    """
    if not IS_LINUX:
        return AdapterScanState.READY

    # ── Check 1: Orphaned Discovering=True ─────────────────────────
    discovering = await get_adapter_discovering(adapter)

    if discovering is True:
        _LOGGER.warning(
            "%s: Discovering=True with scan lock held — "
            "orphaned scan session, attempting StopDiscovery",
            adapter,
        )
        await try_stop_discovery(adapter)

        discovering = await get_adapter_discovering(adapter)
        if discovering is True:
            _LOGGER.warning(
                "%s: Still Discovering=True after StopDiscovery — "
                "session belongs to another D-Bus connection",
                adapter,
            )
            return AdapterScanState.STUCK
        return AdapterScanState.READY

    # ── Check 2: Hidden stale InProgress ───────────────────────────
    probe_ok = await _probe_start_discovery(adapter)

    if probe_ok is False:
        _LOGGER.warning(
            "%s: Discovering=False but StartDiscovery returns InProgress "
            "— likely external raw HCI scan corruption",
            adapter,
        )
        await try_stop_discovery(adapter)

        probe_ok = await _probe_start_discovery(adapter)
        if probe_ok is False:
            _LOGGER.warning(
                "%s: Still InProgress after StopDiscovery — "
                "external scan active, will use cache polling",
                adapter,
            )
            return AdapterScanState.EXTERNAL_SCAN

    return AdapterScanState.READY
