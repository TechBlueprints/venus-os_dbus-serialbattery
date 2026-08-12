"""Adapter recovery and escalation chain for BLE connection failures.

Provides a configurable escalation policy that tracks consecutive
failures per adapter and recommends increasingly aggressive recovery
actions.  The policy respects caller configuration — it never suggests
an action the caller has disabled.

Escalation levels (least to most disruptive)::

    1. RETRY          — simple backoff retry
    2. DIAGNOSE       — diagnose stuck state + targeted fix
    3. CLEAR_BLUEZ    — clear InProgress-dominant stale BlueZ state
    4. ROTATE_ADAPTER — switch to a different adapter
    5. RESET_ADAPTER  — power cycle adapter (disrupts ALL connections)

Adapter reset delegates to ``bluetooth-auto-recovery`` which handles
MGMT socket power cycle, USB reset, and rfkill — much more robust
than a simple ``hciconfig down/up``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from enum import Enum

from .const import IS_LINUX

_LOGGER = logging.getLogger(__name__)


class EscalationAction(str, Enum):
    """Actions the escalation policy can recommend."""

    RETRY = "retry"
    DIAGNOSE = "diagnose"
    CLEAR_BLUEZ = "clear_bluez"
    ROTATE_ADAPTER = "rotate"
    RESET_ADAPTER = "reset"


# Ordered from least to most disruptive
_LEVELS = list(EscalationAction)


@dataclass
class EscalationConfig:
    """Configuration for the recovery escalation chain.

    Each escalation level can be individually enabled or disabled.
    Thresholds control when each level triggers.

    Parameters
    ----------
    diagnose_and_fix:
        Enable stuck-state diagnosis + targeted fix.
    clear_bluez_on_inprogress_dominance:
        Enable BlueZ state cleanup when ``InProgress`` errors dominate.
    rotate_adapter:
        Enable adapter rotation on failure.  Requires multiple adapters.
    reset_adapter:
        Enable adapter reset as last resort.  **WARNING:** disrupts ALL
        connections on the adapter.  Default ``False``.
    rotate_after:
        Consecutive failures before rotating adapter.
    clear_after:
        Consecutive ``InProgress`` failures before BlueZ cleanup.
    reset_after:
        Consecutive failures before adapter reset.
    reset_cooldown:
        Minimum seconds between adapter resets.
    max_escalation:
        Hard ceiling on escalation level.
    """

    diagnose_and_fix: bool = True
    clear_bluez_on_inprogress_dominance: bool = True
    rotate_adapter: bool = True
    reset_adapter: bool = False
    rotate_after: int = 2
    clear_after: int = 4
    reset_after: int = 6
    reset_cooldown: float = 300.0
    max_escalation: EscalationAction = EscalationAction.RESET_ADAPTER


# Pre-built profiles for common service types
PROFILE_BATTERY = EscalationConfig(
    reset_adapter=True,
    reset_after=6,
    reset_cooldown=300.0,
)

PROFILE_SENSOR = EscalationConfig(
    reset_adapter=False,
    max_escalation=EscalationAction.ROTATE_ADAPTER,
)

PROFILE_ON_DEMAND = EscalationConfig(
    clear_bluez_on_inprogress_dominance=False,
    reset_adapter=False,
    rotate_after=1,
    max_escalation=EscalationAction.ROTATE_ADAPTER,
)


class EscalationPolicy:
    """Track consecutive failures per adapter and decide escalation level.

    The policy respects the caller's :class:`EscalationConfig` — it will
    never suggest an action the caller has disabled.

    Example::

        config = EscalationConfig(reset_adapter=False)
        policy = EscalationPolicy(["hci0", "hci1"], config=config)

        action = policy.on_failure("hci0")
        # action will never be RESET_ADAPTER because config disabled it

        policy.on_success("hci0")  # resets failure counter
    """

    def __init__(
        self,
        adapters: list[str],
        config: EscalationConfig | None = None,
    ) -> None:
        self._config = config or EscalationConfig()
        self._adapters = adapters
        self._max_level_idx = _LEVELS.index(self._config.max_escalation)
        self._failures: dict[str, int] = {a: 0 for a in adapters}
        self._last_reset: dict[str, float] = {a: 0.0 for a in adapters}

    @property
    def config(self) -> EscalationConfig:
        """Return the current escalation configuration."""
        return self._config

    def on_failure(self, adapter: str) -> EscalationAction:
        """Record a failure and return the next escalation action.

        The returned action will never exceed *max_escalation* or
        suggest a disabled level.
        """
        self._failures[adapter] = self._failures.get(adapter, 0) + 1
        count = self._failures[adapter]

        if (
            count >= self._config.reset_after
            and self._is_level_enabled(EscalationAction.RESET_ADAPTER)
            and self._can_reset(adapter)
        ):
            return EscalationAction.RESET_ADAPTER

        if count >= self._config.clear_after and self._is_level_enabled(
            EscalationAction.CLEAR_BLUEZ
        ):
            return EscalationAction.CLEAR_BLUEZ

        if count >= self._config.rotate_after and self._is_level_enabled(
            EscalationAction.ROTATE_ADAPTER
        ):
            return EscalationAction.ROTATE_ADAPTER

        if count >= 1 and self._is_level_enabled(EscalationAction.DIAGNOSE):
            return EscalationAction.DIAGNOSE

        return EscalationAction.RETRY

    def on_success(self, adapter: str) -> None:
        """Record a success — resets the failure counter for *adapter*."""
        self._failures[adapter] = 0

    def failure_count(self, adapter: str) -> int:
        """Return the current consecutive failure count for *adapter*.

        Used by adapter scoring to penalize adapters with recent failures.
        """
        return self._failures.get(adapter, 0)

    def record_reset(self, adapter: str) -> None:
        """Record that an adapter reset was performed."""
        self._last_reset[adapter] = time.monotonic()
        self._failures[adapter] = 0

    def _is_level_enabled(self, level: EscalationAction) -> bool:
        """Check if a given escalation level is enabled in config."""
        if _LEVELS.index(level) > self._max_level_idx:
            return False
        level_config_map = {
            EscalationAction.DIAGNOSE: self._config.diagnose_and_fix,
            EscalationAction.CLEAR_BLUEZ: (
                self._config.clear_bluez_on_inprogress_dominance
            ),
            EscalationAction.ROTATE_ADAPTER: self._config.rotate_adapter,
            EscalationAction.RESET_ADAPTER: self._config.reset_adapter,
        }
        return level_config_map.get(level, True)

    def _can_reset(self, adapter: str) -> bool:
        """Check if enough time has passed since the last reset."""
        last = self._last_reset.get(adapter, 0.0)
        return (time.monotonic() - last) >= self._config.reset_cooldown


def is_bluetoothd_alive() -> bool:
    """Check whether ``bluetoothd`` is running.

    Scans ``/proc`` for a process whose ``comm`` is ``bluetoothd``.
    This avoids shelling out to ``pidof`` or ``pgrep``.

    Returns ``True`` if at least one ``bluetoothd`` process is found,
    ``False`` if not found or if ``/proc`` is unavailable (non-Linux).
    """
    if not IS_LINUX:
        return True  # assume OK on non-Linux

    proc_dir = "/proc"
    try:
        for entry in os.listdir(proc_dir):
            if not entry.isdigit():
                continue
            comm_path = os.path.join(proc_dir, entry, "comm")
            if not os.path.exists(comm_path):
                continue
            try:
                with open(comm_path) as f:
                    name = f.read().strip()
                if name == "bluetoothd":
                    return True
            except (OSError, PermissionError):
                continue
    except OSError:
        _LOGGER.debug("Cannot read /proc to check bluetoothd status")

    return False


async def restart_bluetoothd(
    init_script: str = "/etc/init.d/bluetooth",
    timeout: float = 5.0,
) -> bool:
    """Restart ``bluetoothd`` via the init script if it is not running.

    On Venus OS, ``bluetoothd`` is managed by ``/etc/init.d/bluetooth``
    (a SysV init script) with no crash supervision.  If it segfaults or
    is killed, it stays dead until someone manually starts it.

    This function:

    1. Checks ``is_bluetoothd_alive()`` — if already running, returns
       ``True`` immediately (no-op).
    2. Runs ``<init_script> start`` via subprocess.
    3. Waits up to *timeout* seconds for it to complete.
    4. Verifies ``bluetoothd`` is running with a second ``/proc`` check.

    Returns ``True`` if ``bluetoothd`` is running when the function
    returns (whether it was already running or freshly started).
    """
    if not IS_LINUX:
        return True

    if is_bluetoothd_alive():
        return True

    if not os.path.isfile(init_script):
        _LOGGER.error(
            "Cannot restart bluetoothd: init script %s not found",
            init_script,
        )
        return False

    _LOGGER.warning(
        "bluetoothd is not running — attempting restart via %s",
        init_script,
    )

    try:
        proc = await asyncio.create_subprocess_exec(
            init_script, "start",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout,
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            _LOGGER.error(
                "bluetoothd restart timed out after %.0fs", timeout,
            )
            return False

        if proc.returncode != 0:
            _LOGGER.error(
                "bluetoothd restart failed (exit %d): %s",
                proc.returncode,
                (stderr or stdout or b"").decode(errors="replace").strip(),
            )
            return False

        await asyncio.sleep(0.5)

        if is_bluetoothd_alive():
            _LOGGER.info("bluetoothd restarted successfully")
            return True

        _LOGGER.error(
            "bluetoothd init script exited 0 but process not found in /proc"
        )
        return False

    except Exception:
        _LOGGER.exception("Failed to restart bluetoothd")
        return False


async def invalidate_dbus_state() -> None:
    """Tear down and invalidate all cached D-Bus state after an adapter reset.

    An adapter reset (MGMT power cycle) causes ``bluetoothd`` to rebuild
    its D-Bus object tree.  Any in-process D-Bus state cached before the
    reset becomes stale:

    * **Bleak's BlueZManager** singleton caches device paths, adapter
      paths, and GATT service maps from ``GetManagedObjects`` and D-Bus
      signals.  After a reset, these paths no longer exist in BlueZ.
      The manager's ``async_init()`` skips re-initialization because
      ``self._bus.connected`` is still ``True``, so the stale cache
      persists indefinitely.

    * **BCM's shared bus** (``dbus_bus.py``) may have pending state
      tied to the old adapter configuration.

    This function forces both to tear down so they rebuild fresh state
    on next use.
    """
    # 1. Invalidate BCM's own shared bus
    from .dbus_bus import close_bus
    await close_bus()
    _LOGGER.debug("Closed BCM shared D-Bus bus")

    # 2. Invalidate Bleak's BlueZManager singleton
    try:
        from bleak.backends.bluezdbus.manager import _global_instances
    except ImportError:
        _LOGGER.debug("Bleak bluezdbus manager not available, skipping")
        return

    loop = asyncio.get_running_loop()
    manager = _global_instances.get(loop)
    if manager is None:
        _LOGGER.debug("No BlueZManager instance for current event loop")
        return

    bus = getattr(manager, "_bus", None)
    if bus is not None:
        try:
            bus.disconnect()
        except Exception:
            pass
        manager._bus = None
        _LOGGER.info(
            "Invalidated BlueZManager D-Bus bus — will rebuild on next use"
        )
    else:
        _LOGGER.debug("BlueZManager has no active bus")


async def reset_adapter(adapter: str, mac: str = "") -> bool:
    """Reset a BLE adapter using bluetooth-auto-recovery.

    Delegates to ``bluetooth_auto_recovery.recover_adapter()`` which
    handles MGMT socket power cycle, USB reset, and rfkill unblock.
    This is the same approach used by Home Assistant's habluetooth.

    After the reset:

    1. Verifies that ``bluetoothd`` is still alive (restarts it if
       it crashed during the reset — "Stuck State 11").
    2. Invalidates all cached D-Bus state (BlueZManager singleton and
       BCM's shared bus) so that the next connection attempt rebuilds
       from ``GetManagedObjects`` instead of using stale cached paths.

    Parameters
    ----------
    adapter:
        The adapter name (e.g. ``"hci0"``).
    mac:
        The adapter MAC address.  If empty, recovery proceeds with
        a best-effort adapter lookup.

    Returns ``True`` if the reset appeared successful and ``bluetoothd``
    is still alive.
    """
    if not IS_LINUX:
        return False

    try:
        from bluetooth_auto_recovery import recover_adapter as _recover

        hci_num = int(adapter.removeprefix("hci"))
        result = await _recover(hci_num, mac or "00:00:00:00:00:00")
        if result:
            _LOGGER.info(
                "Adapter %s reset successfully via bluetooth-auto-recovery",
                adapter,
            )
        else:
            _LOGGER.warning("Adapter %s reset returned failure", adapter)
            return False

        # Brief settle time for bluetoothd to stabilize
        await asyncio.sleep(1.0)

        # Verify bluetoothd survived the reset (Stuck State 11)
        if not is_bluetoothd_alive():
            _LOGGER.warning(
                "bluetoothd died after resetting %s (Stuck State 11) "
                "— attempting auto-restart",
                adapter,
            )
            if await restart_bluetoothd():
                await invalidate_dbus_state()
                return True
            _LOGGER.critical(
                "bluetoothd could not be restarted after resetting %s",
                adapter,
            )
            return False

        # Invalidate cached D-Bus state — the adapter reset caused
        # bluetoothd to rebuild its object tree, so any cached device
        # paths, adapter properties, or GATT maps are now stale.
        await invalidate_dbus_state()

        return True

    except ImportError:
        _LOGGER.warning(
            "bluetooth-auto-recovery not installed, cannot reset %s",
            adapter,
        )
        return False
    except Exception:
        _LOGGER.exception("Failed to reset adapter %s", adapter)
        return False
