"""Constants and configuration dataclasses for bleak-connection-manager."""

from __future__ import annotations

import platform
from dataclasses import dataclass
from enum import Enum

IS_LINUX = platform.system() == "Linux"


class AdapterScanState(Enum):
    """Result of pre-scan adapter health check.

    Returned by :func:`~bleak_connection_manager.bluez.ensure_adapter_scan_ready`
    to tell the scanner how to proceed.
    """

    READY = "ready"
    """Adapter can scan normally via ``StartDiscovery``."""

    EXTERNAL_SCAN = "external_scan"
    """An external process (e.g. ``dbus-ble-sensors``) is running raw HCI
    scans that corrupt BlueZ's internal discovery state.  Pattern:
    ``Discovering=False`` but ``StartDiscovery`` returns ``InProgress``.

    The scanner should fall back to BlueZ cache polling — the external
    scan continuously populates the cache via kernel mgmt events.
    Power-cycling is futile because the external scanner re-corrupts
    the state immediately."""

    STUCK = "stuck"
    """BlueZ-level orphaned discovery session (``Discovering=True`` that
    cannot be cleared via ``StopDiscovery``).  The scanner should rotate
    to another adapter and may escalate to power-cycling as a last resort."""

# Thread-level safety timer timeout (seconds).  Must be less than
# BLEAK_SAFETY_TIMEOUT (60 s in bleak-retry-connector) so the asyncio
# timeout remains the primary mechanism and the thread timer is only
# a fallback for a stuck event loop.
THREAD_SAFETY_TIMEOUT = 45.0

# How long to wait for a disconnect to complete before giving up.
DISCONNECT_TIMEOUT = 5.0

# Default number of outer retry attempts.
DEFAULT_MAX_ATTEMPTS = 4


@dataclass
class LockConfig:
    """Configuration for cross-process BLE serialization locks.

    On multi-service systems (e.g. Venus OS / Cerbo GX) several processes
    may compete for the same BLE adapter, causing ``InProgress`` errors
    on ~40% of connection attempts.  Slot-based file locking limits
    concurrent connection attempts per adapter across all processes.

    All services sharing adapters on the same host **must** use the same
    *lock_dir* and *lock_template* to coordinate.

    Parameters
    ----------
    enabled:
        Whether cross-process locking is active.
    lock_dir:
        Directory for lock files.  Defaults to ``/run`` — cleared on
        reboot so stale locks cannot survive reboots.
    lock_template:
        Template with ``{adapter}`` and ``{slot}`` placeholders.
    lock_timeout:
        Maximum seconds to wait for slot acquisition.  If exceeded, the
        connection attempt proceeds without a slot (graceful
        degradation).
    max_slots:
        Maximum concurrent connection attempts allowed per adapter.
        Each slot is a separate lock file.  ``1`` gives strict
        serialization (old behavior).  ``2``-``3`` is typical for a
        single adapter on a Cerbo GX.  Higher values suit systems
        with multiple USB adapters.  ``flock`` is crash-safe — if a
        process dies, the kernel releases its slot automatically.
    """

    enabled: bool = False
    lock_dir: str = "/run"
    lock_template: str = "bleak-cm-{adapter}-slot-{slot}.lock"
    lock_timeout: float = 15.0
    max_slots: int = 2

    def path_for_slot(self, adapter: str | None, slot: int) -> str:
        """Return the full lock file path for a given adapter slot."""
        name = adapter or "default"
        filename = self.lock_template.format(adapter=name, slot=slot)
        return f"{self.lock_dir}/{filename}"

    def path_for_adapter(self, adapter: str | None) -> str:
        """Return the lock file path for slot 0 (backwards compatibility)."""
        return self.path_for_slot(adapter, 0)


@dataclass
class ScanLockConfig:
    """Configuration for per-adapter scan serialization.

    Modern BlueZ (>= 5.50) merges discovery sessions across D-Bus
    clients, so scans by *different processes* on the same adapter do
    not conflict (verified on Venus OS v3.73 / BlueZ 5.72).  The real
    ``org.bluez.Error.InProgress`` hazard is two concurrent scans in
    the **same process**: they share bleak's D-Bus connection, and the
    second ``StartDiscovery`` from the same client fails.

    This config therefore controls an in-process per-adapter
    ``asyncio.Lock`` (see :mod:`bleak_connection_manager.scan_lock`).
    It was previously a cross-process file lock built on the incorrect
    premise that BlueZ allows only one scan per adapter system-wide.

    Parameters
    ----------
    enabled:
        Whether per-adapter scan serialization is active.
    lock_dir:
        Deprecated — retained for API compatibility, no longer used
        (locking is in-process; no lock files are created).
    lock_template:
        Deprecated — retained for API compatibility, no longer used.
    lock_timeout:
        Maximum seconds to wait for scan lock acquisition.  If exceeded,
        the scan proceeds without holding the lock (graceful degradation)
        so the caller can still attempt the scan and handle the
        ``InProgress`` error itself.
    """

    enabled: bool = False
    lock_dir: str = "/run"
    lock_template: str = "bleak-cm-{adapter}-scan.lock"
    lock_timeout: float = 30.0

    def path_for_adapter(self, adapter: str | None) -> str:
        """Return the full lock file path for a given adapter."""
        name = adapter or "default"
        filename = self.lock_template.format(adapter=name)
        return f"{self.lock_dir}/{filename}"
