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

Upstream also has a fifth rung that power-cycles the adapter through
``bluetooth-auto-recovery``.  That dependency is not vendored here, so
the rung is omitted rather than advertised and always failing.
"""

from __future__ import annotations

import asyncio
import logging
import os
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
    rotate_after:
        Consecutive failures before rotating adapter.
    clear_after:
        Consecutive ``InProgress`` failures before BlueZ cleanup.
    max_escalation:
        Hard ceiling on escalation level.
    """

    diagnose_and_fix: bool = True
    clear_bluez_on_inprogress_dominance: bool = True
    rotate_adapter: bool = True
    rotate_after: int = 2
    clear_after: int = 4
    max_escalation: EscalationAction = EscalationAction.ROTATE_ADAPTER


# Pre-built profiles for common service types
# A long-lived connection (a BMS) can afford the full ladder: diagnose,
# clear stale BlueZ state, then rotate adapters.
PROFILE_BATTERY = EscalationConfig()

# A connect-on-demand consumer wants to move on quickly: rotate after a
# single failure and never spend time clearing BlueZ state.
PROFILE_ON_DEMAND = EscalationConfig(
    clear_bluez_on_inprogress_dominance=False,
    rotate_after=1,
)


class EscalationPolicy:
    """Track consecutive failures per adapter and decide escalation level.

    The policy respects the caller's :class:`EscalationConfig` — it will
    never suggest an action the caller has disabled.

    Example::

        config = EscalationConfig(rotate_adapter=False)
        policy = EscalationPolicy(["hci0", "hci1"], config=config)

        action = policy.on_failure("hci0")
        # action will never be ROTATE_ADAPTER because config disabled it

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
        }
        return level_config_map.get(level, True)


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
