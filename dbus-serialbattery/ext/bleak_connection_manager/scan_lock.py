"""Per-adapter scan serialization (in-process).

**Premise correction (2026-08-09, verified on Venus OS v3.73 / BlueZ
5.72):** this module originally implemented a cross-process
``fcntl.flock`` lock on the premise that BlueZ allows only one
``StartDiscovery`` per adapter and returns ``InProgress`` to every
other *process*.  That premise is false on modern BlueZ (>= 5.50):
discovery sessions are per-D-Bus-client and merged — a second
process's ``StartDiscovery`` joins the running discovery and succeeds
(verified live on the target Cerbo with two concurrent processes).

The real ``InProgress`` hazard is *same-client*: two concurrent scans
in one process share bleak's D-Bus connection, and the second
``StartDiscovery`` from the same client fails with ``InProgress``
(bleak 2.x does not refcount active scans).  That is an in-process
problem, so this module now provides a per-adapter ``asyncio.Lock``.
The old file locks also serialized scans *across* services that BlueZ
would happily merge, adding latency for nothing.

The public API is unchanged (``acquire_scan_lock`` /
``release_scan_lock`` / ``ScanLock``); only the handle type changed
from a file descriptor to the held ``asyncio.Lock``.

**Graceful degradation:**

If the lock cannot be acquired within the configured timeout, the
caller proceeds without the lock so a wedged scan cannot deadlock the
BLE subsystem; the worst case is a same-client ``InProgress`` the
retry loop already handles.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .const import ScanLockConfig

_LOGGER = logging.getLogger(__name__)

# Per-adapter locks, recreated if the event loop changes (asyncio
# primitives are bound to the loop they were created on).
_locks: dict[str, asyncio.Lock] = {}
_locks_loop: asyncio.AbstractEventLoop | None = None


def _get_lock(adapter: str) -> asyncio.Lock:
    global _locks, _locks_loop
    loop = asyncio.get_running_loop()
    if _locks_loop is not loop:
        _locks = {}
        _locks_loop = loop
    lock = _locks.get(adapter)
    if lock is None:
        lock = asyncio.Lock()
        _locks[adapter] = lock
    return lock


async def acquire_scan_lock(
    config: ScanLockConfig,
    adapter: str | None,
) -> asyncio.Lock | None:
    """Acquire the per-adapter scan lock.

    Waits up to *config.lock_timeout* seconds.  A timeout of ``0``
    is a non-blocking attempt.

    Returns
    -------
    asyncio.Lock | None
        The held lock (pass to :func:`release_scan_lock`), or ``None``
        if locking is disabled or the lock could not be acquired in
        time.
    """
    if not config.enabled:
        return None

    lock = _get_lock(adapter or "hci0")

    if config.lock_timeout <= 0:
        # Non-blocking attempt.  Lock.acquire() on an uncontended lock
        # completes without yielding, so this cannot race in-loop.
        if lock.locked():
            return None
        await lock.acquire()
        return lock

    try:
        await asyncio.wait_for(lock.acquire(), timeout=config.lock_timeout)
    except asyncio.TimeoutError:
        _LOGGER.warning(
            "Timed out waiting for scan lock on %s after %.1f s "
            "— scan lock not acquired",
            adapter,
            config.lock_timeout,
        )
        return None
    return lock


def release_scan_lock(lock: asyncio.Lock | None) -> None:
    """Release a previously acquired scan lock.

    Safe to call with ``None`` (no-op).
    """
    if lock is None:
        return
    try:
        lock.release()
    except RuntimeError:
        _LOGGER.debug("Scan lock already released", exc_info=True)


class ScanLock:
    """Async context manager for per-adapter scan locking.

    Usage::

        async with ScanLock(config, "hci0"):
            device = await BleakScanner.find_device_by_address(addr, ...)

    If the lock cannot be acquired, the context manager still enters
    (graceful degradation) but logs a warning.

    Parameters
    ----------
    config:
        Scan lock configuration.
    adapter:
        Adapter name (e.g. ``"hci0"``).
    """

    __slots__ = ("_config", "_adapter", "_lock")

    def __init__(self, config: ScanLockConfig, adapter: str | None = "hci0") -> None:
        self._config = config
        self._adapter = adapter
        self._lock: asyncio.Lock | None = None

    async def __aenter__(self) -> "ScanLock":
        self._lock = await acquire_scan_lock(self._config, self._adapter)
        return self

    async def __aexit__(self, *exc: object) -> None:
        release_scan_lock(self._lock)
        self._lock = None

    @property
    def acquired(self) -> bool:
        """Whether the scan lock is currently held."""
        return self._lock is not None
