"""Shared D-Bus system bus for BlueZ diagnostic queries.

Provides a single long-lived ``MessageBus`` connection to the system
D-Bus daemon, reused across all BCM diagnostic operations (device
property queries, remove, disconnect, adapter state inspection, etc.).

Why a shared bus?
-----------------

Previously, each diagnostic function created its own temporary
``MessageBus``, used it for one query, and immediately disconnected.
This caused two problems:

1. **Spurious errors**: ``dbus-fast``'s ``get_proxy_object()`` triggers
   ``_init_high_level_client()`` which fire-and-forgets an ``AddMatch``
   for ``NameOwnerChanged``.  If the bus disconnects before the reply
   arrives, the callback logs ``add match request failed`` at ERROR
   level.

2. **Unnecessary overhead**: Each temporary bus creates a Unix socket,
   authenticates, and tears down.  For the shyion service polling 7
   devices per hour, that was 14+ bus connections created and destroyed
   per poll cycle.

The shared bus eliminates both issues: the ``AddMatch`` (if it occurs)
succeeds because the bus stays alive, and all queries reuse one
connection.

All functions use raw ``bus.call(Message(...))`` instead of proxy
objects, which avoids triggering ``_init_high_level_client()`` entirely
and also skips the unnecessary ``bus.introspect()`` round-trip.

Thread / async safety
---------------------

The bus is lazily created on first use and auto-reconnects if the
connection drops.  All access goes through :func:`get_bus`, which is
safe to call from any coroutine in the same event loop.  There is no
cross-thread sharing.
"""

from __future__ import annotations

import asyncio
import logging
import time

from .const import IS_LINUX

_LOGGER = logging.getLogger(__name__)

_bus: object | None = None  # dbus_fast.aio.MessageBus, typed loosely to avoid import on non-Linux
_bus_loop: object | None = None  # The event loop the bus was created on
_bluez_ready = False  # Set True once we've confirmed org.bluez is on D-Bus

# Rate-limit: after a confirmed wait_for_bluez() failure (bluetoothd dead
# and restart unsuccessful), don't re-poll for this many seconds.  Prevents
# rapid cycling that leaks D-Bus socket FDs in bleak/dbus_fast.
_BLUEZ_FAILURE_COOLDOWN = 60.0
_last_bluez_failure: float = 0.0


def is_service_unknown_error(exc: BaseException) -> bool:
    """Return True if *exc* is a D-Bus ServiceUnknown error (org.bluez not running)."""
    msg = str(exc)
    return "ServiceUnknown" in msg or "not provided by any .service files" in msg


def mark_bluez_gone() -> None:
    """Clear the BlueZ-ready flag so the next scan re-waits for bluetoothd.

    Call this whenever a BLE operation fails with a ServiceUnknown error —
    it means bluetoothd crashed at runtime and must come back before any
    BlueZ operation can succeed.
    """
    global _bluez_ready
    if _bluez_ready:
        _LOGGER.warning(
            "org.bluez is no longer on D-Bus (bluetoothd crashed?); "
            "future scans will wait for it to restart"
        )
        _bluez_ready = False


async def get_bus():
    """Get the shared system D-Bus connection, creating or reconnecting as needed.

    Returns a connected ``dbus_fast.aio.MessageBus`` instance.

    If the running event loop differs from the one the bus was created
    on, the old bus is discarded and a fresh one is created.  This
    prevents ``Future attached to a different loop`` RuntimeErrors when
    the caller's event loop changes (e.g. ``asyncio.run()`` creates a
    new loop between calls).

    Raises ``ImportError`` if ``dbus-fast`` is not available, or
    ``RuntimeError`` on non-Linux platforms.
    """
    global _bus, _bus_loop

    if not IS_LINUX:
        raise RuntimeError("Shared D-Bus bus is only available on Linux")

    from dbus_fast.aio import MessageBus
    from dbus_fast.constants import BusType

    current_loop = asyncio.get_running_loop()

    if _bus is not None:
        if _bus_loop is not current_loop:
            _LOGGER.debug(
                "Shared D-Bus bus was created on a different event loop, "
                "reconnecting on current loop"
            )
            try:
                _bus.disconnect()
            except Exception:
                pass
            _bus = None
        elif _bus.connected:
            return _bus
        else:
            _LOGGER.debug("Shared D-Bus bus disconnected, reconnecting")
            try:
                _bus.disconnect()
            except Exception:
                pass

    _bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
    _bus_loop = current_loop
    _LOGGER.debug("Shared D-Bus bus connected")
    return _bus


async def _ping_bluez() -> bool:
    """Single fast D-Bus check for ``org.bluez`` availability.

    Returns ``True`` if BlueZ responded (any non-ServiceUnknown reply).
    """
    from dbus_fast import Message, MessageType

    try:
        bus = await get_bus()
        reply = await bus.call(
            Message(
                destination="org.bluez",
                path="/org/bluez",
                interface="org.freedesktop.DBus.Properties",
                member="GetAll",
                signature="s",
                body=["org.bluez.AgentManager1"],
            )
        )
        if reply.message_type != MessageType.ERROR:
            return True
        error_name = reply.error_name or ""
        if "ServiceUnknown" in error_name or "UnknownObject" in error_name:
            return False
        return True
    except Exception:
        return False


async def _poll_bluez(timeout: float, poll_interval: float) -> bool:
    """Poll D-Bus for ``org.bluez`` up to *timeout* seconds.

    Returns ``True`` as soon as BlueZ responds, ``False`` on timeout.
    """
    elapsed = 0.0
    while elapsed < timeout:
        if await _ping_bluez():
            if elapsed > 0:
                _LOGGER.info("BlueZ ready on D-Bus after %.1fs", elapsed)
            else:
                _LOGGER.debug("BlueZ ready on D-Bus")
            return True

        _LOGGER.debug(
            "Waiting for BlueZ on D-Bus (%.1fs / %.0fs)...",
            elapsed, timeout,
        )
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

        # Reconnect bus if it dropped while we were waiting
        await get_bus()

    return False


async def wait_for_bluez(
    timeout: float = 30.0,
    poll_interval: float = 1.0,
) -> bool:
    """Wait until ``org.bluez`` is available on the system D-Bus.

    On Venus OS, BLE services may start before ``bluetoothd`` has
    registered on D-Bus.  Any BlueZ operation attempted before
    ``org.bluez`` is available fails with::

        org.freedesktop.DBus.Error.ServiceUnknown:
        The name org.bluez was not provided by any .service files

    This function polls D-Bus until ``org.bluez`` is reachable or
    *timeout* seconds have elapsed.

    If the initial poll times out, the function attempts to restart
    ``bluetoothd`` via ``/etc/init.d/bluetooth start`` and polls for
    an additional 10 seconds.

    The ``_bluez_ready`` cache is validated with a single D-Bus ping
    on every call, so a crashed ``bluetoothd`` is detected even when
    the cache says BlueZ was previously healthy.

    Returns ``True`` if BlueZ became available, ``False`` on timeout.
    """
    global _bluez_ready, _last_bluez_failure

    if not IS_LINUX:
        return True

    # Rate-limit: if we recently confirmed BlueZ is dead and couldn't
    # recover, don't burn CPU and FDs re-polling.  Return False
    # immediately so the caller backs off.
    if _last_bluez_failure > 0:
        since_failure = time.monotonic() - _last_bluez_failure
        if since_failure < _BLUEZ_FAILURE_COOLDOWN:
            _LOGGER.debug(
                "BlueZ failure cooldown: %.0fs remaining",
                _BLUEZ_FAILURE_COOLDOWN - since_failure,
            )
            return False

    # Validate the cache: if we previously marked BlueZ as ready,
    # do a quick ping to confirm it's still alive.  This catches
    # bluetoothd crashes that happened since the last successful check.
    if _bluez_ready:
        if await _ping_bluez():
            return True
        _LOGGER.warning(
            "BlueZ was previously available but is no longer responding "
            "on D-Bus — bluetoothd may have crashed"
        )
        _bluez_ready = False

    # Normal poll loop (shortened to 5s — the full 30s poll is wasteful
    # for a crashed daemon, and the restart below is the real fix)
    if await _poll_bluez(min(timeout, 5.0), poll_interval):
        _bluez_ready = True
        _last_bluez_failure = 0.0
        return True

    # Poll exhausted — attempt recovery
    _LOGGER.warning(
        "BlueZ not on D-Bus — attempting recovery",
    )

    from .recovery import restart_bluetoothd

    # Stop dbus-ble-sensors first if present — its raw HCI scanning
    # is the primary cause of bluetoothd crashes on Venus OS.
    await _stop_raw_hci_scanner()

    if await restart_bluetoothd():
        if await _poll_bluez(10.0, poll_interval):
            _bluez_ready = True
            _last_bluez_failure = 0.0
            # Re-enable dbus-ble-sensors now that bluetoothd is healthy
            await _start_raw_hci_scanner()
            return True
        _LOGGER.error(
            "bluetoothd restarted but BlueZ still not on D-Bus after 10s"
        )
    else:
        _LOGGER.error(
            "Failed to restart bluetoothd — BLE operations will fail"
        )

    # Re-enable dbus-ble-sensors even on failure (don't leave it stopped)
    await _start_raw_hci_scanner()

    _last_bluez_failure = time.monotonic()
    return False


async def _stop_raw_hci_scanner() -> None:
    """Stop ``dbus-ble-sensors`` if it exists.

    On Venus OS, this Victron service uses raw HCI sockets for BLE
    scanning, bypassing BlueZ entirely.  This corrupts BlueZ's
    internal state and eventually crashes ``bluetoothd``.  Stopping it
    before restarting ``bluetoothd`` prevents the new daemon from being
    immediately killed again.

    No-op if the service doesn't exist or ``svc`` is unavailable.
    """
    svc_dir = "/service/dbus-ble-sensors"
    try:
        import os
        if not os.path.exists(svc_dir):
            return
        proc = await asyncio.create_subprocess_exec(
            "svc", "-d", svc_dir,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=3.0)
        _LOGGER.info(
            "Stopped dbus-ble-sensors before bluetoothd restart"
        )
        await asyncio.sleep(0.5)
    except Exception:
        _LOGGER.debug(
            "Could not stop dbus-ble-sensors (may not exist)",
            exc_info=True,
        )


async def _start_raw_hci_scanner() -> None:
    """Re-enable ``dbus-ble-sensors`` if it exists."""
    svc_dir = "/service/dbus-ble-sensors"
    try:
        import os
        if not os.path.exists(svc_dir):
            return
        proc = await asyncio.create_subprocess_exec(
            "svc", "-u", svc_dir,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=3.0)
        _LOGGER.info("Re-enabled dbus-ble-sensors after bluetoothd restart")
    except Exception:
        _LOGGER.debug(
            "Could not re-enable dbus-ble-sensors",
            exc_info=True,
        )


async def close_bus() -> None:
    """Disconnect the shared bus if it's open.

    Safe to call even if no bus was ever created.
    """
    global _bus, _bus_loop
    if _bus is not None:
        try:
            _bus.disconnect()
        except Exception:
            pass
        _bus = None
        _bus_loop = None
