"""bleak-connection-manager: Robust BLE connection lifecycle manager.

Wraps bleak-retry-connector with production-hardened workarounds for
real-world BlueZ failure modes.
"""

from __future__ import annotations

__version__ = "0.1.0"

from .adapters import discover_adapters, make_device_for_adapter, pick_adapter
from .bluez import (
    address_to_bluez_path,
    disconnect_device,
    is_inactive_connection,
    remove_device,
    verified_disconnect,
)
from .connection import establish_connection
from .const import (
    DEFAULT_MAX_ATTEMPTS,
    DISCONNECT_TIMEOUT,
    IS_LINUX,
    THREAD_SAFETY_TIMEOUT,
    LockConfig,
    ScanLockConfig,
)
from .diagnostics import StuckState, clear_stuck_state, diagnose_stuck_state
from .hci import (
    HCI_DISCONNECT_REASONS,
    HciConnection,
    cancel_le_connect,
    disconnect_by_address,
    disconnect_handle,
    disconnect_reason_str,
    find_connection_by_address,
    get_connections,
    hci_available,
)
from .lock import acquire_lock, acquire_slot, release_lock, release_slot
from .scan_lock import ScanLock, acquire_scan_lock, release_scan_lock
from .scanner import discover as managed_discover
from .scanner import find_device as managed_find_device
from .recovery import (
    PROFILE_BATTERY,
    PROFILE_ON_DEMAND,
    PROFILE_SENSOR,
    EscalationAction,
    EscalationConfig,
    EscalationPolicy,
    invalidate_dbus_state,
    is_bluetoothd_alive,
    reset_adapter,
)
from .dbus_bus import is_service_unknown_error, mark_bluez_gone, wait_for_bluez
from .validators import validate_char_exists, validate_gatt_services, validate_read_char
from .watchdog import ConnectionWatchdog

__all__ = [
    # Core connection function
    "establish_connection",
    # Watchdog
    "ConnectionWatchdog",
    # Lock / slot-based concurrency control (connections)
    "LockConfig",
    "acquire_slot",
    "release_slot",
    "acquire_lock",  # backwards-compatible alias for acquire_slot
    "release_lock",  # backwards-compatible alias for release_slot
    # Lock / scan concurrency control
    "ScanLockConfig",
    "ScanLock",
    "acquire_scan_lock",
    "release_scan_lock",
    # Adapters
    "discover_adapters",
    "make_device_for_adapter",
    "pick_adapter",
    # BlueZ utilities
    "address_to_bluez_path",
    "disconnect_device",
    "is_inactive_connection",
    "remove_device",
    "verified_disconnect",
    # Diagnostics
    "StuckState",
    "clear_stuck_state",
    "diagnose_stuck_state",
    # HCI layer (connection state via hcitool)
    "HciConnection",
    "HCI_DISCONNECT_REASONS",
    "disconnect_reason_str",
    "hci_available",
    "get_connections",
    "find_connection_by_address",
    "disconnect_handle",
    "disconnect_by_address",
    "cancel_le_connect",
    # Recovery / escalation
    "is_bluetoothd_alive",
    "is_service_unknown_error",
    "mark_bluez_gone",
    "wait_for_bluez",
    "EscalationAction",
    "EscalationConfig",
    "EscalationPolicy",
    "PROFILE_BATTERY",
    "PROFILE_ON_DEMAND",
    "PROFILE_SENSOR",
    "invalidate_dbus_state",
    "reset_adapter",
    # Validators
    "validate_char_exists",
    "validate_gatt_services",
    "validate_read_char",
    # Managed scanning (lock + rotation + retry)
    "managed_find_device",
    "managed_discover",
    # Constants
    "DEFAULT_MAX_ATTEMPTS",
    "DISCONNECT_TIMEOUT",
    "IS_LINUX",
    "THREAD_SAFETY_TIMEOUT",
]
