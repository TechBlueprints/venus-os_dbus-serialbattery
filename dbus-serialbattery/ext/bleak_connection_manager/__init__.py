"""bleak-connection-manager: Robust BLE connection lifecycle manager.

Wraps bleak-retry-connector with production-hardened workarounds for
real-world BlueZ failure modes.

Vendored subset - see README.md for provenance and for what was removed
from the upstream project.
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
from .scanner import discover as managed_discover
from .scanner import find_device as managed_find_device
from .recovery import (
    PROFILE_BATTERY,
    PROFILE_ON_DEMAND,
    EscalationAction,
    EscalationConfig,
    EscalationPolicy,
    is_bluetoothd_alive,
)
from .dbus_bus import is_service_unknown_error, mark_bluez_gone, wait_for_bluez

__all__ = [
    # Core connection function
    "establish_connection",
    # Lock / slot-based concurrency control (connections)
    "LockConfig",
    "acquire_slot",
    "release_slot",
    "acquire_lock",  # backwards-compatible alias for acquire_slot
    "release_lock",  # backwards-compatible alias for release_slot
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
    # Managed scanning (cache-first + rotation + retry)
    "managed_find_device",
    "managed_discover",
    # Constants
    "DEFAULT_MAX_ATTEMPTS",
    "DISCONNECT_TIMEOUT",
    "IS_LINUX",
    "THREAD_SAFETY_TIMEOUT",
]
