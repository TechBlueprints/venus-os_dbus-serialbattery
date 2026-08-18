# -*- coding: utf-8 -*-
"""Cross-process BLE adapter slots: placement and mutual exclusion in one act.

Several BLE services on one host share a set of Bluetooth adapters. Two
problems follow, and this module solves both with a single primitive:

  1. Mutual exclusion: an adapter can only carry so much at once, and two
     processes connecting through it simultaneously produce
     org.bluez.Error.InProgress failures.
  2. Placement: a service choosing an adapter should prefer one that no other
     service is using, without a read-then-decide race.

Each adapter gets max_slots lock files. Taking a slot is flock(LOCK_NB) on the
first free file; choosing an adapter is walking a preference list and keeping
the first slot that acquires. Selection and acquisition are one atomic act, so
two processes choosing at the same moment cannot land on the same slot: the
kernel serializes them. Reading system state first and deciding afterwards
(e.g. from BlueZ Connected properties) is a race, and stale state after a
restart makes it choose wrong; this is why flock, not observation.

Crash safety comes from the kernel: flock is released when the holding process
dies, however it dies. The lock files themselves are empty markers and are
never deleted; a file with no flock on it IS a free slot.

File convention, shared with bleak_connection_manager's LockConfig so services
using either implementation coordinate:

    /run/bleak-cm-{adapter}-slot-{N}.lock      N in 0..max_slots-1

Hold semantics are the caller's choice and interoperate freely:
  - attempt-hold: acquire before a connect attempt, release after it. Limits
    concurrent attempts per adapter (bleak_connection_manager's behavior).
  - lifetime-hold: acquire before connecting, keep while the connection lives,
    release on disconnect. Additionally makes the adapter's steady-state
    occupancy visible to every other participant's placement walk.

This file is deliberately standalone (stdlib only, no asyncio, no project
imports) so other projects can vendor it verbatim.
"""

import fcntl
import logging
import os

logger = logging.getLogger(__name__)

DEFAULT_LOCK_DIR = "/run"
DEFAULT_TEMPLATE = "bleak-cm-{adapter}-slot-{slot}.lock"
DEFAULT_MAX_SLOTS = 2


class AdapterSlot:
    """A held slot on one adapter. Release it, or let process death do it."""

    def __init__(self, adapter, index, fd):
        self.adapter = adapter
        self.index = index
        self._fd = fd

    def release(self):
        """Free the slot. Safe to call more than once."""
        if self._fd is None:
            return
        fd, self._fd = self._fd, None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass

    @property
    def held(self):
        return self._fd is not None

    def __repr__(self):
        state = "held" if self.held else "released"
        return f"<AdapterSlot {self.adapter}#{self.index} {state}>"


class AdapterSlotManager:
    """Acquire slots on adapters shared with other BLE services on this host.

    Every method degrades rather than blocks or raises: an unwritable lock
    directory or a missing fcntl behaves as "no slot obtainable", and the
    caller proceeds unlocked. Coordination is an optimization; refusing to
    connect because a lock file could not be created would invert that.
    """

    def __init__(self, lock_dir=DEFAULT_LOCK_DIR, template=DEFAULT_TEMPLATE, max_slots=DEFAULT_MAX_SLOTS):
        self.lock_dir = lock_dir
        self.template = template
        self.max_slots = max(1, int(max_slots))

    def _path(self, adapter, slot):
        return os.path.join(self.lock_dir, self.template.format(adapter=adapter or "default", slot=slot))

    def try_acquire(self, adapter):
        """One free slot on this adapter, or None. Never blocks."""
        for index in range(self.max_slots):
            path = self._path(adapter, index)
            fd = None
            try:
                fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return AdapterSlot(adapter, index, fd)
            except OSError:
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
        return None

    def acquire_first(self, adapters):
        """Walk a preference list, keep the first slot that acquires.

        Returns (adapter, AdapterSlot), or (None, None) when every listed
        adapter is fully occupied or the directory is unusable. Selection and
        acquisition are one atomic act per adapter: there is no moment where
        an adapter has been chosen but not yet claimed.
        """
        for adapter in adapters:
            slot = self.try_acquire(adapter)
            if slot is not None:
                return adapter, slot
        return None, None

    def occupancy(self, adapter):
        """(held, max_slots) for an adapter, by probing without keeping.

        Diagnostic only. For placement use acquire_first(), which cannot race;
        a count read here is stale the moment it returns.
        """
        held = 0
        for index in range(self.max_slots):
            probe = self.try_acquire_index(adapter, index)
            if probe is None:
                held += 1
            else:
                probe.release()
        return held, self.max_slots

    def try_acquire_index(self, adapter, index):
        """One specific slot, or None. Building block for occupancy()."""
        path = self._path(adapter, index)
        fd = None
        try:
            fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return AdapterSlot(adapter, index, fd)
        except OSError:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            return None
