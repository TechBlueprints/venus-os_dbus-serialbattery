"""Connection watchdog for monitoring BLE notification activity.

Detects "zombie" connections where BlueZ still reports Connected=True
but no notifications are being received — the radio link is effectively
dead without a disconnect callback ever firing.

The caller must specify the expected timeout because only the caller
knows the device's notification cadence.  There is no sensible default
— a battery BMS sends every 1-5 s while a temperature sensor may send
every 60 s.  Making the timeout explicit forces the caller to think
about what "dead" means for their specific device.

Usage::

    watchdog = ConnectionWatchdog(
        timeout=30.0,
        on_timeout=my_reconnect_callback,
    )
    watchdog.start()

    # In your notification callback:
    watchdog.notify_activity()

    # When done:
    watchdog.stop()

When *client* and *device* are provided, the watchdog automatically
tears down the connection at the BlueZ level before invoking the
callback.  This ensures the next ``establish_connection()`` call
starts fresh instead of adopting stale state.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import re
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from .bluez import remove_device, verified_disconnect
from .const import DISCONNECT_TIMEOUT

if TYPE_CHECKING:
    from bleak import BleakClient
    from bleak.backends.device import BLEDevice

_LOGGER = logging.getLogger(__name__)


def _adapter_from_device(device: BLEDevice | None) -> str | None:
    """Extract the adapter name from a BLEDevice's BlueZ D-Bus path.

    bleak's BlueZ backend stores the object path in ``device.details``
    (e.g. ``/org/bluez/hci1/dev_AA_BB_...``).  Returns ``None`` when the
    device has no path (non-BlueZ backend, fabricated device).
    """
    details = getattr(device, "details", None)
    if not isinstance(details, dict):
        return None
    match = re.match(r"/org/bluez/(hci\d+)/", details.get("path") or "")
    return match.group(1) if match else None


class ConnectionWatchdog:
    """Monitor a BLE connection for notification activity.

    Tracks the time since the last :meth:`notify_activity` call.
    When the timeout is exceeded the optional *on_timeout* callback
    is invoked so the caller can trigger reconnection or cleanup.

    When *client* and *device* are both provided, the watchdog
    performs BlueZ-level cleanup before invoking the callback:

    1. ``client.disconnect()`` with a 5 s timeout (prevents hang
       on phantom connections).
    2. ``remove_device()`` via D-Bus to clear BlueZ cache.
    3. The *on_timeout* callback, where the caller can reconnect.

    Firing semantics: the watchdog is **one-shot on success** — after
    the cleanup and a callback invocation that does not raise, the
    monitoring task exits, and a new watchdog must be created for the
    next connection.  If the callback *raises*, the watchdog re-arms
    and retries after another full timeout instead of dying silently;
    the most recent failure is exposed as :attr:`last_callback_error`.

    .. warning::
        The cleanup calls ``remove_device()``, which deletes the BlueZ
        device object.  The caller's ``BleakClient`` never receives a
        property-change signal for that deletion, so its cached
        ``is_connected`` may remain ``True`` indefinitely.  Do not use
        ``client.is_connected`` as a loop condition to detect this
        watchdog firing — drive reconnection from the *on_timeout*
        callback (or from your own data-freshness check).

    Parameters
    ----------
    timeout:
        Seconds of inactivity before the watchdog fires.  Required —
        there is no default because only the caller knows the device's
        expected notification cadence.
    on_timeout:
        Callback invoked when the timeout expires.  May be a plain
        callable or a coroutine function; a returned awaitable is
        awaited.
    client:
        The connected ``BleakClient``.
    device:
        The ``BLEDevice`` for the connection.
    adapter:
        The adapter the connection lives on (e.g. ``"hci1"``).  If
        ``None``, derived from the device's BlueZ D-Bus path, falling
        back to ``hci0``.  Cleanup must target the right adapter or
        the D-Bus calls silently miss the stale device object.
    """

    def __init__(
        self,
        timeout: float,
        on_timeout: Callable[[], Awaitable[None]] | None = None,
        client: BleakClient | None = None,
        device: BLEDevice | None = None,
        adapter: str | None = None,
    ) -> None:
        self._timeout = timeout
        self._on_timeout = on_timeout
        self._client = client
        self._device = device
        self._adapter = adapter
        self._last_activity: float = 0.0
        self._task: asyncio.Task[None] | None = None
        self._started = False
        self._last_callback_error: BaseException | None = None

    @property
    def is_running(self) -> bool:
        """Return whether the watchdog is actively monitoring."""
        return self._started and self._task is not None and not self._task.done()

    @property
    def last_activity(self) -> float:
        """Return the monotonic timestamp of the last activity."""
        return self._last_activity

    @property
    def last_callback_error(self) -> BaseException | None:
        """Return the exception from the most recent failed *on_timeout* call.

        ``None`` if the callback has not failed (or succeeded on a
        retry).  While this is non-``None`` the watchdog is re-arming
        and retrying the callback after each timeout period.
        """
        return self._last_callback_error

    def notify_activity(self) -> None:
        """Record that a notification or other activity was received.

        Call this from your BLE notification callback to reset the
        watchdog timer.
        """
        self._last_activity = time.monotonic()

    def start(self) -> None:
        """Start the watchdog monitoring loop.

        Records the current time as the initial activity timestamp and
        creates an asyncio task for the monitoring loop.  Calling
        ``start()`` on an already-running watchdog is a no-op.
        """
        if self._started:
            return
        self._last_activity = time.monotonic()
        self._started = True
        self._task = asyncio.ensure_future(self._monitor())

    def stop(self) -> None:
        """Stop the watchdog.

        Cancels the monitoring task.  Safe to call multiple times or
        before ``start()``.
        """
        self._started = False
        if self._task is not None:
            self._task.cancel()
            self._task = None

    async def _cleanup_connection(self) -> None:
        """Disconnect the client and verify via D-Bus, then clear cache.

        Called when *client* and *device* were provided and the
        inactivity timeout has fired.

        Uses a two-step approach:

        1. ``client.disconnect()`` with a timeout (prevents phantom hang).
        2. ``verified_disconnect()`` polls D-Bus ``Connected`` property
           to confirm the device is truly disconnected.  If still
           connected, escalates to ``remove_device()`` automatically.
        3. Final ``remove_device()`` to clear BlueZ cache for a fresh
           reconnect.
        """
        if self._client is None or self._device is None:
            return
        address = self._device.address
        adapter = self._adapter or _adapter_from_device(self._device) or "hci0"

        # Step 1: disconnect with timeout (prevents phantom hang)
        try:
            await asyncio.wait_for(
                self._client.disconnect(), timeout=DISCONNECT_TIMEOUT
            )
        except asyncio.TimeoutError:
            _LOGGER.debug(
                "ConnectionWatchdog: disconnect timed out for %s,"
                " proceeding to verified disconnect",
                address,
            )
        except Exception:
            _LOGGER.debug(
                "ConnectionWatchdog: disconnect failed for %s,"
                " proceeding to verified disconnect",
                address,
                exc_info=True,
            )

        # Step 2: verify D-Bus agrees the device is disconnected;
        # if still Connected=True, escalates to remove_device internally
        try:
            await verified_disconnect(
                address, adapter, timeout=DISCONNECT_TIMEOUT
            )
        except Exception:
            _LOGGER.debug(
                "ConnectionWatchdog: verified_disconnect failed for %s",
                address,
                exc_info=True,
            )

        # Step 3: remove device from BlueZ so next connect starts fresh
        try:
            await remove_device(address, adapter)
        except Exception:
            _LOGGER.debug(
                "ConnectionWatchdog: remove_device failed for %s",
                address,
                exc_info=True,
            )

    async def _monitor(self) -> None:
        """Internal monitoring loop.

        Wakes up periodically and checks whether the inactivity timeout
        has been exceeded.  Uses a check interval of half the timeout
        (clamped to 1–30 s) so the actual fire time is at most one
        interval late.
        """
        check_interval = min(self._timeout / 2, 30.0)
        try:
            while self._started:
                await asyncio.sleep(check_interval)
                elapsed = time.monotonic() - self._last_activity
                if elapsed < self._timeout:
                    continue

                _LOGGER.warning(
                    "ConnectionWatchdog: no activity for %.1f s (timeout %.1f s)",
                    elapsed,
                    self._timeout,
                )

                # Cleanup is repeated on each retry after a callback
                # failure — the bluez helpers tolerate an already-
                # removed device, and stale state may have reappeared.
                if self._client is not None and self._device is not None:
                    await self._cleanup_connection()

                if self._on_timeout is not None:
                    try:
                        # Accept both plain callables and coroutine
                        # functions — a sync callback returns None,
                        # which must not be awaited.
                        result = self._on_timeout()
                        if inspect.isawaitable(result):
                            await result
                    except Exception as exc:
                        # A failed recovery hook must not end monitoring:
                        # the connection is still down.  Re-arm and keep
                        # retrying (and nagging the log) until the
                        # callback succeeds or stop() is called.
                        self._last_callback_error = exc
                        self._last_activity = time.monotonic()
                        _LOGGER.exception(
                            "ConnectionWatchdog: on_timeout callback failed"
                            " — re-arming, retrying in %.1f s",
                            self._timeout,
                        )
                        continue
                    self._last_callback_error = None
                break
        except asyncio.CancelledError:
            pass
        finally:
            self._started = False
