"""Adapter enumeration, rotation, and scoring for multi-adapter BLE systems.

Wraps ``bluetooth-adapters`` for enumeration and adds both round-robin
rotation and score-based selection logic for distributing connection
attempts across adapters.

When multiple USB BLE adapters are available, rotating between them
avoids saturating a single adapter and works around per-adapter
connection limits in BlueZ.

Score-based selection (inspired by habluetooth's
``_score_connection_paths``) improves on blind round-robin by
considering free connection slots, recent failure history, and
in-progress connections when choosing which adapter to try next.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from bleak.backends.device import BLEDevice

from .const import IS_LINUX

if TYPE_CHECKING:
    from .const import LockConfig
    from .recovery import EscalationPolicy

_LOGGER = logging.getLogger(__name__)

# ── Scoring constants ──────────────────────────────────────────────
#
# Inspired by habluetooth's _score_connection_paths().
# A higher score means a better adapter to try.

_BASE_SCORE = 100.0
_PENALTY_PER_FAILURE = 15.0
_PENALTY_PER_IN_PROGRESS = 20.0
_PENALTY_NO_FREE_SLOTS = 200.0  # effectively disqualifies
_PENALTY_LAST_SLOT = 10.0


def discover_adapters() -> list[str]:
    """Discover available BLE adapters on the system.

    Uses ``bluetooth-adapters`` when available, falls back to
    ``/sys/class/bluetooth/`` enumeration.

    Returns a sorted list of adapter names (e.g. ``["hci0", "hci1"]``).
    Returns ``["hci0"]`` as a safe default if no adapters are found.
    """
    if not IS_LINUX:
        return ["hci0"]

    # Try bluetooth-adapters first (more reliable, handles USB adapters)
    try:
        from bluetooth_adapters import get_adapters_from_hci

        adapters_from_hci = get_adapters_from_hci()
        if adapters_from_hci:
            names = sorted(a["name"] for a in adapters_from_hci.values())
            if names:
                _LOGGER.debug("Discovered adapters via bluetooth-adapters: %s", names)
                return names
    except Exception:
        _LOGGER.debug(
            "bluetooth-adapters enumeration failed, trying /sys",
            exc_info=True,
        )

    # Fallback: /sys/class/bluetooth/
    try:
        import pathlib

        bt_path = pathlib.Path("/sys/class/bluetooth")
        if bt_path.exists():
            adapters = sorted(
                d.name for d in bt_path.iterdir() if d.name.startswith("hci")
            )
            if adapters:
                _LOGGER.debug("Discovered adapters via /sys: %s", adapters)
                return adapters
    except Exception:
        _LOGGER.debug("Failed to enumerate /sys/class/bluetooth", exc_info=True)

    return ["hci0"]


def pick_adapter(
    adapters: list[str],
    attempt: int,
) -> str:
    """Pick an adapter for the current attempt using round-robin.

    Parameters
    ----------
    adapters:
        List of available adapter names (e.g. ``["hci0", "hci1"]``).
    attempt:
        The current attempt number (1-based).

    Returns the adapter to use for this attempt.
    """
    if not adapters:
        return "hci0"
    idx = (attempt - 1) % len(adapters)
    return adapters[idx]


def score_adapter(
    adapter: str,
    *,
    escalation_policy: EscalationPolicy | None = None,
    lock_config: LockConfig | None = None,
    in_progress: dict[str, int] | None = None,
) -> float:
    """Score an adapter for connection suitability (higher = better).

    Factors considered (inspired by habluetooth's scoring):

    - **Free connection slots**: No free slots → heavy penalty.
      Last slot free → small penalty.
    - **Failure history**: Each consecutive failure on this adapter
      incurs a penalty.
    - **In-progress connections**: Each ongoing connection attempt
      on this adapter incurs a penalty (avoids piling onto a busy
      adapter).

    Parameters
    ----------
    adapter:
        The adapter name (e.g. ``"hci0"``).
    escalation_policy:
        If provided, failure count is read from here.
    lock_config:
        If provided and enabled, free slot count is probed.
    in_progress:
        Dict mapping adapter → number of connection attempts
        currently in flight.  If ``None``, no in-progress penalty.
    """
    score = _BASE_SCORE

    # --- Failure penalty ---
    if escalation_policy is not None:
        failures = escalation_policy.failure_count(adapter)
        score -= _PENALTY_PER_FAILURE * failures

    # --- In-progress penalty ---
    if in_progress is not None:
        count = in_progress.get(adapter, 0)
        score -= _PENALTY_PER_IN_PROGRESS * count

    # --- Slot availability penalty ---
    if lock_config is not None and lock_config.enabled:
        from .lock import probe_free_slots

        free = probe_free_slots(lock_config, adapter)
        if free == 0:
            score -= _PENALTY_NO_FREE_SLOTS
        elif free == 1:
            score -= _PENALTY_LAST_SLOT

    return score


def select_best_adapter(
    adapters: list[str],
    *,
    escalation_policy: EscalationPolicy | None = None,
    lock_config: LockConfig | None = None,
    in_progress: dict[str, int] | None = None,
) -> str:
    """Select the best adapter from a list using score-based ranking.

    Falls back to the first adapter if all scores are equal.

    Parameters are the same as :func:`score_adapter`.
    """
    if not adapters:
        return "hci0"
    if len(adapters) == 1:
        return adapters[0]

    best_adapter = adapters[0]
    best_score = float("-inf")

    for adapter in adapters:
        s = score_adapter(
            adapter,
            escalation_policy=escalation_policy,
            lock_config=lock_config,
            in_progress=in_progress,
        )
        _LOGGER.debug("Adapter %s score: %.1f", adapter, s)
        if s > best_score:
            best_score = s
            best_adapter = adapter

    return best_adapter


def make_device_for_adapter(
    device: BLEDevice,
    adapter: str,
) -> BLEDevice:
    """Create a BLEDevice targeting a specific adapter.

    Constructs a new ``BLEDevice`` with the D-Bus path pointing to the
    chosen adapter, so that ``bleak-retry-connector`` connects through
    it.

    Parameters
    ----------
    device:
        The original BLEDevice (from scanning).
    adapter:
        The adapter to target (e.g. ``"hci0"``).

    Returns a new BLEDevice with the adapter-specific path.
    """
    from .bluez import address_to_bluez_path

    path = address_to_bluez_path(device.address, adapter)
    details: dict[str, Any] = {"path": path}

    if isinstance(device.details, dict):
        # Preserve any extra details from the original device
        details.update(
            {k: v for k, v in device.details.items() if k != "path"}
        )

    return BLEDevice(
        device.address,
        device.name,
        details,
    )
