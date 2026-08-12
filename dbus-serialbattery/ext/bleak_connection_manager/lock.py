"""Cross-process slot-based file locking for BLE adapter serialization.

On multi-service systems several processes may compete for the same BLE
adapter, causing ``InProgress`` errors.  This module provides slot-based
``fcntl.flock`` helpers that limit concurrent BLE operations per adapter
without blocking the asyncio event loop.

**How it works:**

Each adapter gets *N* lock files (slot 0 … slot N-1).  To acquire a
slot, we try ``flock(LOCK_NB)`` on each slot file in order.  The first
one that succeeds grants us a slot.  If all slots are held, we sleep
and retry until the timeout expires.

**Why this can't deadlock:**

- Every ``flock`` call is non-blocking (``LOCK_NB``).  If a slot isn't
  free, we don't hold anything while waiting — we release, sleep, retry.
- No two-resource dependency = no deadlock possible.
- ``flock`` is kernel-managed: if a process crashes, the kernel
  releases all its locks automatically.  No stale counters.

**Backwards compatibility:**

``max_slots=1`` gives strict one-at-a-time serialization identical to
the original binary lock behavior.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .const import LockConfig

try:
    import fcntl

    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False

_LOGGER = logging.getLogger(__name__)

_SLOT_RETRY_INTERVAL = 0.15


async def acquire_slot(
    lock_config: LockConfig,
    adapter: str | None,
) -> int | None:
    """Acquire one of N exclusive slots for the given adapter.

    Tries each slot file (0 … max_slots-1) with ``flock(LOCK_NB)``.
    The first slot that can be locked is returned as an open file
    descriptor.

    If no slot is available within *lock_config.lock_timeout*, returns
    ``None`` — the caller should proceed without a slot (graceful
    degradation).

    Parameters
    ----------
    lock_config:
        Lock configuration including slot count and timeout.
    adapter:
        Adapter name (e.g. ``"hci0"``).  Used to derive lock paths.

    Returns
    -------
    int | None
        An open file descriptor holding the slot lock, or ``None``
        if acquisition timed out or locking is unavailable.
    """
    if not _HAS_FCNTL or not lock_config.enabled:
        return None

    elapsed = 0.0

    while True:
        # Try each slot in order
        for slot_idx in range(lock_config.max_slots):
            slot_path = lock_config.path_for_slot(adapter, slot_idx)
            try:
                fd = os.open(slot_path, os.O_CREAT | os.O_RDWR, 0o666)
            except OSError:
                _LOGGER.debug(
                    "Failed to open slot file %s, skipping",
                    slot_path,
                    exc_info=True,
                )
                continue

            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                _LOGGER.debug(
                    "Acquired BLE slot %d/%d for %s (%s)",
                    slot_idx,
                    lock_config.max_slots,
                    adapter,
                    slot_path,
                )
                return fd
            except OSError:
                # Slot is held by another process — close and try next
                os.close(fd)

        # All slots busy — wait and retry
        elapsed += _SLOT_RETRY_INTERVAL
        if elapsed >= lock_config.lock_timeout:
            _LOGGER.warning(
                "Timed out waiting for a BLE slot on %s after %.1f s "
                "(%d slots all held) — proceeding without slot",
                adapter,
                elapsed,
                lock_config.max_slots,
            )
            return None

        await asyncio.sleep(_SLOT_RETRY_INTERVAL)


def release_slot(fd: int | None) -> None:
    """Release a previously acquired slot lock.

    Safe to call with ``None`` (no-op).
    """
    if fd is None:
        return
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    except OSError:
        _LOGGER.debug("Failed to release BLE slot", exc_info=True)


def probe_free_slots(lock_config: LockConfig, adapter: str | None) -> int:
    """Count how many connection slots are currently free for *adapter*.

    Tries each slot file with ``flock(LOCK_NB)``.  Slots that can be
    locked are immediately unlocked and counted as free.  Slots held by
    other processes are counted as busy.

    This is a **non-blocking snapshot** — the counts may change by the
    time the caller acts on them.  Use this for scoring / prioritization,
    not for hard guarantees.

    Returns the number of free slots (0 … max_slots).
    """
    if not _HAS_FCNTL or not lock_config.enabled:
        return lock_config.max_slots  # assume all free when locking disabled

    free = 0
    for slot_idx in range(lock_config.max_slots):
        slot_path = lock_config.path_for_slot(adapter, slot_idx)
        try:
            fd = os.open(slot_path, os.O_CREAT | os.O_RDWR, 0o666)
        except OSError:
            continue

        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Got the lock — slot is free.  Release immediately.
            fcntl.flock(fd, fcntl.LOCK_UN)
            free += 1
        except OSError:
            pass  # Slot held by another process
        finally:
            os.close(fd)

    return free


# Backwards-compatible aliases
acquire_lock = acquire_slot
release_lock = release_slot
