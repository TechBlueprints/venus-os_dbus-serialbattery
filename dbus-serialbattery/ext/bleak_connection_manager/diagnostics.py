"""Stuck-state diagnosis and targeted recovery for BLE connections.

Provides :func:`diagnose_stuck_state` to determine why a BLE connection
is stuck, and :func:`clear_stuck_state` to apply the minimal targeted
fix for each diagnosis.

Diagnosis uses two layers:

1. **D-Bus** (via ``bluez`` module) — BlueZ's cached view of the world.
2. **HCI** (via ``hci`` module) — the kernel's ground-truth connection
   list, queried through ``hcitool``.

Cross-referencing these two layers is the only reliable way to detect
phantom connections and stale handles.  If ``hcitool`` is not available,
HCI-dependent diagnostics (phantom detection, orphan handle detection)
are skipped and the module trusts D-Bus alone.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import subprocess
from enum import Enum
from pathlib import Path

from .bluez import _get_device_properties, disconnect_device, remove_device
from .const import IS_LINUX
from .hci import disconnect_by_address, find_connection_by_address, hci_available

_LOGGER = logging.getLogger(__name__)

_BLUEZ_LIB_PATHS = [
    Path("/data/var/lib/bluetooth"),  # Venus OS
    Path("/var/lib/bluetooth"),       # Standard Linux
]


def _find_bluez_lib() -> Path | None:
    """Find the BlueZ persistent storage directory."""
    for p in _BLUEZ_LIB_PATHS:
        if p.is_dir():
            return p
    return None


async def _delete_bluez_cache(address: str, adapter: str = "hci0") -> bool:
    """Delete the BlueZ persistent cache for a device, bypassing D-Bus.

    When ``RemoveDevice`` hangs (phantom connections where BlueZ
    internally tries to ``Disconnect()`` first), this function removes
    the on-disk device directory that BlueZ persists GATT attributes
    and connection info in.  After deletion, ``hciconfig down/up`` is
    used to force BlueZ to drop the in-memory phantom state.

    This is a last-resort cleanup path for phantom connections.
    """
    if not IS_LINUX:
        return False

    bluez_lib = _find_bluez_lib()
    if bluez_lib is None:
        _LOGGER.debug("BlueZ lib directory not found")
        return False

    # Resolve adapter BD_ADDR from hciconfig
    hciconfig = shutil.which("hciconfig")
    if hciconfig is None:
        _LOGGER.debug("hciconfig not found, cannot delete BlueZ cache")
        return False

    try:
        result = await asyncio.to_thread(
            subprocess.run,
            [hciconfig, adapter],
            capture_output=True,
            text=True,
            timeout=5.0,
        )
        adapter_addr: str | None = None
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith("BD Address:"):
                adapter_addr = line.split()[2].strip()
                break
        if not adapter_addr:
            _LOGGER.debug("Could not determine BD address for %s", adapter)
            return False
    except (OSError, subprocess.TimeoutExpired):
        return False

    # Build path using colons (BlueZ uses colons in directory names)
    dev_addr = address.upper()
    device_dir = bluez_lib / adapter_addr / dev_addr

    if not device_dir.is_dir():
        # Also check the cache subdirectory
        cache_entry = bluez_lib / adapter_addr / "cache" / dev_addr
        if cache_entry.is_file():
            try:
                cache_entry.unlink()
                _LOGGER.info(
                    "%s: Deleted BlueZ cache entry %s", address, cache_entry,
                )
            except OSError as exc:
                _LOGGER.debug(
                    "%s: Could not delete cache entry: %s", address, exc,
                )
        else:
            _LOGGER.debug(
                "%s: No BlueZ device dir at %s", address, device_dir,
            )
            # No on-disk state to delete — still try power-cycle to
            # clear the in-memory phantom.

    else:
        try:
            shutil.rmtree(device_dir)
            _LOGGER.info(
                "%s: Deleted BlueZ device dir %s", address, device_dir,
            )
        except OSError as exc:
            _LOGGER.warning(
                "%s: Failed to delete BlueZ device dir: %s", address, exc,
            )
            return False

    # Force BlueZ to re-read by power-cycling the specific adapter.
    # When the adapter goes down, BlueZ drops all device objects for it.
    # When it comes back up, BlueZ re-reads the storage — but the
    # phantom's directory is gone, so it won't be reloaded.
    try:
        await asyncio.to_thread(
            subprocess.run,
            [hciconfig, adapter, "down"],
            capture_output=True,
            timeout=5.0,
        )
        await asyncio.sleep(0.5)
        await asyncio.to_thread(
            subprocess.run,
            [hciconfig, adapter, "up"],
            capture_output=True,
            timeout=5.0,
        )
        _LOGGER.info(
            "%s: Power-cycled %s to clear phantom from BlueZ",
            address,
            adapter,
        )
        # Wait for BlueZ to fully re-initialize the adapter.  The
        # adapter goes through power-off → power-on → ready, which
        # takes several seconds on real hardware.
        await asyncio.sleep(5.0)
    except (OSError, subprocess.TimeoutExpired):
        _LOGGER.debug("%s: hciconfig down/up failed for %s", address, adapter)

    return True


class StuckState(Enum):
    """Diagnosis of why a BLE connection is stuck.

    Each value maps to a specific recovery action in
    :func:`clear_stuck_state`.
    """

    NOT_STUCK = "not_stuck"
    INACTIVE_CONNECTION = "inactive_connection"
    STALE_CACHE = "stale_cache"
    ORPHAN_HCI_HANDLE = "orphan_hci_handle"
    PHANTOM_NO_HANDLE = "phantom_no_handle"


async def diagnose_stuck_state(
    address: str,
    adapter: str,
    adapters: list[str] | None = None,
) -> StuckState:
    """Determine what kind of stuck state a device is in.

    Cross-references D-Bus properties with HCI connection state:

    - **PHANTOM_NO_HANDLE**: D-Bus ``Connected=True`` but no HCI handle
      exists on any adapter.  This is the classic phantom — BlueZ
      believes the device is connected but there is no radio link.
      (Stuck State 1)
    - **INACTIVE_CONNECTION**: D-Bus ``Connected=True``,
      ``ServicesResolved`` is not ``True``, and an HCI handle exists.
      The radio link is up but GATT never resolved — dead handle.
      (Stuck State 2)
    - **ORPHAN_HCI_HANDLE**: D-Bus does NOT show the device as connected,
      but an HCI handle exists.  The service that created the connection
      died without cleaning up, and the peripheral is stuck in connected
      mode (won't advertise).  (Stuck State 20)
    - **STALE_CACHE**: Device exists on D-Bus but is not connected and
      has no HCI handle.  Stale BlueZ cache that may cause
      ``InProgress`` errors.  (Stuck State 13)
    - **NOT_STUCK**: Device appears healthy or is not present.

    Parameters
    ----------
    address:
        The BLE device MAC address.
    adapter:
        The primary adapter name (e.g. ``"hci0"``).
    adapters:
        All available adapters.  If provided, HCI connections are
        checked across all of them.  If ``None``, only *adapter* is
        checked.
    """
    if not IS_LINUX:
        return StuckState.NOT_STUCK

    # Layer 1: Check HCI (ground truth)
    # If HCI is not functional on this platform (e.g. "bind(): bad family"
    # on some kernels), we cannot cross-reference D-Bus with HCI, so we
    # must not diagnose phantom/orphan states — they would be false
    # positives that remove devices BlueZ is actively connecting to.
    search_adapters = adapters if adapters else [adapter]
    # hcitool subprocess calls block for up to 5 s each — keep them off
    # the event loop thread or every other connection in this process
    # stalls while we diagnose.
    _hci_works = await asyncio.to_thread(
        lambda: any(hci_available(a) for a in search_adapters)
    )

    hci_conn = None
    if _hci_works:
        hci_conn = await asyncio.to_thread(
            find_connection_by_address, address, adapters=search_adapters
        )

    # Layer 2: Check D-Bus (BlueZ's cached view)
    props = await _get_device_properties(address, adapter)

    dbus_connected = False
    dbus_exists = props is not None
    if props is not None:
        dbus_connected = props.get("Connected", False)

    # Cross-reference the two layers
    if dbus_connected:
        if not _hci_works:
            # HCI unavailable — we cannot tell phantom from real.
            # Trust D-Bus and assume the connection is healthy.
            _LOGGER.debug(
                "%s: D-Bus Connected=True, HCI unavailable — "
                "skipping phantom detection",
                address,
            )
            return StuckState.NOT_STUCK

        services_resolved = props.get("ServicesResolved", False)  # type: ignore[union-attr]
        if hci_conn is None:
            # D-Bus says connected, HCI says no handle → PHANTOM.
            # This is true regardless of ServicesResolved — BlueZ can
            # cache both Connected=True and ServicesResolved=True from
            # a previous session even after the radio link is dead.
            return StuckState.PHANTOM_NO_HANDLE
        if services_resolved is not True:
            # D-Bus connected + HCI handle exists + GATT not resolved
            return StuckState.INACTIVE_CONNECTION
        # Both layers agree: connected with services resolved → healthy
        return StuckState.NOT_STUCK

    # D-Bus does NOT show connected
    if _hci_works and hci_conn is not None:
        # D-Bus says not connected (or not present), but HCI handle
        # exists → orphan handle from a crashed service.  The peripheral
        # is stuck in connected mode and won't advertise.
        return StuckState.ORPHAN_HCI_HANDLE

    if dbus_exists:
        # Device in D-Bus cache but not connected, no HCI handle
        return StuckState.STALE_CACHE

    # Not in D-Bus, no HCI handle → clean state
    return StuckState.NOT_STUCK


async def clear_stuck_state(
    address: str,
    adapter: str,
    state: StuckState,
    adapters: list[str] | None = None,
) -> bool:
    """Apply the targeted fix for a diagnosed stuck state.

    - **PHANTOM_NO_HANDLE**: Disconnect + remove via D-Bus (clears the
      stale BlueZ state since there's no HCI handle to clear).
    - **INACTIVE_CONNECTION**: Disconnect via D-Bus (which sends HCI
      Disconnect since a handle exists), then remove.
    - **ORPHAN_HCI_HANDLE**: Disconnect at the HCI level to free the
      peripheral, then remove from D-Bus.  This is the fix for
      Stuck State 20 -- D-Bus ``RemoveDevice`` alone does NOT clear
      HCI handles.
    - **STALE_CACHE**: Remove the device from BlueZ cache.

    Returns ``True`` if the cleanup action was executed.
    """
    if state == StuckState.NOT_STUCK:
        return True

    if not IS_LINUX:
        return False

    if state == StuckState.PHANTOM_NO_HANDLE:
        _LOGGER.info(
            "%s: Clearing phantom connection (D-Bus Connected=True "
            "but no HCI handle)",
            address,
        )
        # Skip disconnect_device — there is no real HCI link, so BlueZ's
        # Disconnect() will block indefinitely waiting for a link teardown
        # that can never happen.
        #
        # Strategy: Try RemoveDevice with a short timeout.  If that also
        # hangs (BlueZ internally calls Disconnect before Remove for
        # "Connected" devices), fall back to deleting the BlueZ persistent
        # cache directory for this device.  The device will be re-discovered
        # on the next scan as if new.
        cleared = False
        try:
            cleared = await asyncio.wait_for(
                remove_device(address, adapter), timeout=5.0,
            )
        except asyncio.TimeoutError:
            _LOGGER.debug(
                "%s: remove_device timed out (expected for phantom)",
                address,
            )

        if not cleared:
            cleared = await _delete_bluez_cache(address, adapter)

        if cleared:
            _LOGGER.info("%s: Phantom cleared successfully", address)
        else:
            _LOGGER.warning(
                "%s: Could not clear phantom — scan may fail",
                address,
            )
        return True

    if state == StuckState.INACTIVE_CONNECTION:
        _LOGGER.info(
            "%s: Clearing inactive connection (Connected but "
            "ServicesResolved is not True)",
            address,
        )
        try:
            await asyncio.wait_for(
                disconnect_device(address, adapter), timeout=10.0,
            )
        except asyncio.TimeoutError:
            _LOGGER.warning(
                "%s: disconnect_device timed out after 10 s", address,
            )
        try:
            await asyncio.wait_for(
                remove_device(address, adapter), timeout=10.0,
            )
        except asyncio.TimeoutError:
            _LOGGER.warning(
                "%s: remove_device timed out after 10 s", address,
            )
        return True

    if state == StuckState.ORPHAN_HCI_HANDLE:
        _LOGGER.info(
            "%s: Clearing orphan HCI handle (peripheral stuck in "
            "connected mode, won't advertise)",
            address,
        )
        search_adapters = adapters if adapters else [adapter]
        await asyncio.to_thread(
            disconnect_by_address, address, adapters=search_adapters
        )
        await remove_device(address, adapter)
        return True

    if state == StuckState.STALE_CACHE:
        _LOGGER.info(
            "%s: Removing stale BlueZ cache entry on %s",
            address,
            adapter,
        )
        await remove_device(address, adapter)
        return True

    return False
