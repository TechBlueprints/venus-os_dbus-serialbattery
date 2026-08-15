"""Outer retry loop wrapping bleak-retry-connector.

This is the core of ``bleak-connection-manager``.  It calls
``bleak_retry_connector.establish_connection(max_attempts=1)`` inside
its own retry loop, adding all the BlueZ workarounds between attempts:

- Per-device in-process lock (pre-attempt)
- Cross-process slot-based adapter locking (per-attempt)
- Phantom/inactive connection cleanup (pre-attempt)
- Adapter rotation (per-attempt)
- InProgress classification (post-failure)
- Stuck-state diagnosis + targeted fix (post-failure)
- Escalation chain (post-failure)
- Clear stale BlueZ state (post-failure)
- Post-connect validation (post-success)
- Thread-level safety timer (per-attempt)

Each workaround is independently removable.  When upstream or BlueZ
fixes the root cause, the corresponding code path can be deleted.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import weakref
from collections.abc import Awaitable, Callable
from typing import Any

from bleak import BleakClient
from bleak.backends.device import BLEDevice
from bleak.exc import BleakError

from .adapters import discover_adapters, make_device_for_adapter, pick_adapter
from .bluez import (
    disconnect_device,
    is_inactive_connection,
    remove_device,
    verified_disconnect,
)
from .const import (
    DEFAULT_MAX_ATTEMPTS,
    DISCONNECT_TIMEOUT,
    IS_LINUX,
    THREAD_SAFETY_TIMEOUT,
    LockConfig,
)
from .diagnostics import StuckState, clear_stuck_state, diagnose_stuck_state
from .hci import cancel_le_connect as _hci_cancel_le_connect
from .lock import acquire_slot, release_slot
from .recovery import (
    EscalationAction,
    EscalationConfig,
    EscalationPolicy,
)

_LOGGER = logging.getLogger(__name__)

# ── Per-device in-process lock ─────────────────────────────────────
#
# Prevents two coroutines within the same process from racing to
# connect to the same BLE device simultaneously.  This is a common
# source of InProgress errors when a watchdog reconnect fires while
# a scheduled reconnect is already underway.
#
# Uses a WeakValueDictionary so locks for devices that are no longer
# referenced are garbage-collected automatically.

_device_locks: weakref.WeakValueDictionary[str, asyncio.Lock] = (
    weakref.WeakValueDictionary()
)
_device_locks_guard = asyncio.Lock()


async def _get_device_lock(address: str) -> asyncio.Lock:
    """Get or create an asyncio.Lock for a specific BLE device address.

    Thread-safe within the asyncio event loop.  Locks are weakly
    referenced — once all callers release their reference, the lock
    is garbage-collected.
    """
    addr = address.upper()
    lock = _device_locks.get(addr)
    if lock is not None:
        return lock

    async with _device_locks_guard:
        # Double-check after acquiring guard
        lock = _device_locks.get(addr)
        if lock is not None:
            return lock
        lock = asyncio.Lock()
        _device_locks[addr] = lock
        return lock


# Upstream exceptions we catch to decide retry behavior
try:
    from bleak_retry_connector import (
        BleakAbortedError,
        BleakNotFoundError,
        establish_connection as _brc_establish_connection,
    )
except ImportError as _exc:
    raise ImportError(
        "bleak-retry-connector is required: pip install bleak-retry-connector"
    ) from _exc

# Also import clear_cache and close_stale_connections from upstream
try:
    from bleak_retry_connector import clear_cache as _brc_clear_cache
except ImportError:
    _brc_clear_cache = None  # type: ignore[assignment]

try:
    from bleak_retry_connector import close_stale_connections as _brc_close_stale
except ImportError:
    _brc_close_stale = None  # type: ignore[assignment]


async def _safe_validate(
    validate_connection: Callable[[BleakClient], Awaitable[bool]],
    client: BleakClient,
) -> bool:
    """Run the validation callback, catching all exceptions."""
    try:
        return await validate_connection(client)
    except Exception:
        _LOGGER.debug(
            "validate_connection raised an exception, treating as failed",
            exc_info=True,
        )
        return False


async def _clear_inactive_connections(
    device: BLEDevice, adapter: str, adapters: list[str] | None = None,
) -> None:
    """Clear phantom/inactive connections before a connection attempt.

    Checks both the D-Bus layer and HCI layer:
    1. D-Bus inactive connection check (Connected=True, ServicesResolved!=True)
    2. Orphan HCI handle check (HCI handle exists, D-Bus disagrees)
    """
    try:
        inactive = await is_inactive_connection(device.address, adapter)
        if inactive:
            _LOGGER.info(
                "%s: Clearing inactive connection on %s before attempt",
                device.address,
                adapter,
            )
            await disconnect_device(device.address, adapter)
            await remove_device(device.address, adapter)
            await asyncio.sleep(0.5)
            return
    except Exception:
        _LOGGER.debug(
            "Failed to clear inactive connections for %s",
            device.address,
            exc_info=True,
        )

    # Full cross-layer diagnostics to catch orphan HCI handles
    # that D-Bus doesn't know about (Stuck State 20)
    try:
        search_adapters = adapters if adapters else [adapter]
        state = await diagnose_stuck_state(
            device.address, adapter, adapters=search_adapters
        )
        # Only clear actual connection anomalies pre-attempt.
        # STALE_CACHE (device in D-Bus, not connected, no HCI handle)
        # is the normal state of a freshly-scanned device — clearing it
        # here would invalidate the BLEDevice reference and cause the
        # connection attempt to fail with "device disappeared".
        # STALE_CACHE is handled post-failure instead.
        if state not in (StuckState.NOT_STUCK, StuckState.STALE_CACHE):
            _LOGGER.info(
                "%s: Pre-attempt diagnostics found %s, clearing",
                device.address,
                state.value,
            )
            await clear_stuck_state(
                device.address, adapter, state, adapters=search_adapters
            )
            await asyncio.sleep(0.5)
    except Exception:
        _LOGGER.debug(
            "Failed cross-layer diagnostics for %s",
            device.address,
            exc_info=True,
        )


async def _clear_stale_state(device: BLEDevice) -> None:
    """Clear stale BlueZ state using upstream utility if available."""
    if _brc_clear_cache is not None:
        try:
            await _brc_clear_cache(device.address)
        except Exception:
            _LOGGER.debug(
                "clear_cache failed for %s",
                device.address,
                exc_info=True,
            )
    else:
        await remove_device(device.address)


async def _handle_inprogress(
    device: BLEDevice, adapter: str = "hci0"
) -> None:
    """Handle an InProgress error by cancelling at HCI and clearing D-Bus.

    Order of operations:
    1. Cancel pending LE Create Connection at HCI level (State 3 fix)
    2. Close stale connections via upstream utility
    3. Clear stale D-Bus cache
    """
    _LOGGER.debug(
        "%s: InProgress error on %s — cancelling LE connect and clearing state",
        device.address,
        adapter,
    )
    # Step 1: Cancel at HCI level — this is safe even if no LE connect
    # is pending (controller returns Command Disallowed, which we ignore)
    try:
        await asyncio.to_thread(_hci_cancel_le_connect, adapter)
    except Exception:
        _LOGGER.debug(
            "HCI LE cancel failed for %s on %s",
            device.address,
            adapter,
            exc_info=True,
        )

    # Step 2: Close stale connections via upstream
    if _brc_close_stale is not None:
        try:
            await _brc_close_stale(device)
        except Exception:
            _LOGGER.debug(
                "close_stale_connections failed for %s",
                device.address,
                exc_info=True,
            )

    # Step 3: Clear D-Bus cache
    await _clear_stale_state(device)


async def _execute_escalation(
    action: EscalationAction,
    adapter: str,
    device: BLEDevice,
    escalation_policy: EscalationPolicy | None,
    adapters: list[str] | None = None,
) -> None:
    """Execute the escalation action recommended by the policy."""
    if action == EscalationAction.RETRY:
        return

    if action == EscalationAction.DIAGNOSE:
        state = await diagnose_stuck_state(
            device.address, adapter, adapters=adapters
        )
        # Only clear actual connection anomalies, not STALE_CACHE.
        # STALE_CACHE is the normal state of a freshly-scanned device;
        # removing it invalidates the BLEDevice reference.
        if state not in (StuckState.NOT_STUCK, StuckState.STALE_CACHE):
            await clear_stuck_state(
                device.address, adapter, state, adapters=adapters
            )
        return

    if action == EscalationAction.CLEAR_BLUEZ:
        await _clear_stale_state(device)
        return

    if action == EscalationAction.ROTATE_ADAPTER:
        # Rotation is handled by pick_adapter in the main loop
        return


def _create_safety_timer(
    timeout: float,
    loop: asyncio.AbstractEventLoop,
    connect_task: asyncio.Task[Any],
) -> threading.Timer:
    """Create a thread-level safety timer that cancels a stuck task.

    This runs on a separate thread so it can fire even when the asyncio
    event loop is blocked.  It's the last resort for Stuck State 6
    (blocked asyncio event loop).
    """

    def _on_timer_expired() -> None:
        if not connect_task.done():
            _LOGGER.warning(
                "Thread safety timer expired after %.0f s — "
                "cancelling stuck connection task",
                timeout,
            )
            loop.call_soon_threadsafe(connect_task.cancel)

    timer = threading.Timer(timeout, _on_timer_expired)
    timer.daemon = True
    return timer


async def establish_connection(
    client_class: type[BleakClient],
    device: BLEDevice,
    name: str | None = None,
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    adapters: list[str] | None = None,
    close_inactive_connections: bool = False,
    safety_timer: bool = False,
    try_direct_first: bool = False,
    validate_connection: Callable[[BleakClient], Awaitable[bool]] | None = None,
    lock_config: LockConfig | None = None,
    escalation_policy: EscalationPolicy | None = None,
    overall_timeout: float | None = None,
    **kwargs: Any,
) -> BleakClient:
    """Establish a BLE connection with full lifecycle management.

    Drop-in replacement for ``bleak_retry_connector.establish_connection()``
    with additional BlueZ workarounds.

    Parameters
    ----------
    client_class:
        The BleakClient class (or subclass) to use.
    device:
        The BLE device to connect to.
    name:
        Device name for logging.
    max_attempts:
        Maximum number of outer retry attempts.
    adapters:
        List of adapters to rotate through.  If ``None``, auto-discovered.
        Pass a single-element list to pin to one adapter.
    close_inactive_connections:
        If ``True``, detect and clear phantom connections before each
        attempt.
    safety_timer:
        If ``True``, start a thread-level timer that cancels the
        connection attempt if the asyncio event loop appears blocked.
    try_direct_first:
        If ``True``, the first attempt uses ``use_services_cache=True``
        to try a direct connection from the BlueZ cache without scanning.
        This is 2-5 s faster than scan + connect (10-15 s) for devices
        that are already in the BlueZ cache.  Subsequent attempts revert
        to the normal scan + connect flow.  Useful for connect-on-demand
        services where latency matters.
    validate_connection:
        Optional async callback ``(client) -> bool`` called after a
        successful connection.  If it returns ``False`` (or raises),
        the connection is torn down and the next attempt starts.
    lock_config:
        Cross-process lock configuration.  If ``None`` or
        ``enabled=False``, no locking is performed.
    escalation_policy:
        Failure escalation policy.  If ``None``, a default policy
        (diagnose -> clear BlueZ state -> rotate adapter) is used.
    overall_timeout:
        Hard ceiling (in seconds) for the entire connection process
        including all retry attempts and escalation.  If ``None``
        (the default), there is no overall timeout — only per-attempt
        timeouts apply.  Recommended values: 240 s for critical BMS
        connections, 300 s for sensors/switches.  When the timeout
        fires, an ``asyncio.TimeoutError`` is raised.
    **kwargs:
        Additional keyword arguments passed through to
        ``bleak_retry_connector.establish_connection()``.

    Returns
    -------
    BleakClient
        A connected BleakClient instance.

    Raises
    ------
    BleakAbortedError
        If all attempts are exhausted.
    BleakNotFoundError
        If the device cannot be found on any attempt.
    """
    display_name = name or device.name or device.address

    async def _do_establish() -> BleakClient:
        # Auto-discover adapters if not provided
        nonlocal adapters
        if adapters is None and IS_LINUX:
            adapters = discover_adapters()

        effective_adapters = adapters or ["hci0"]
        loop = asyncio.get_running_loop()
        last_error: Exception | None = None

        # Per-device in-process lock — prevents two coroutines from
        # racing to connect to the same BLE device simultaneously
        device_lock = await _get_device_lock(device.address)

        async with device_lock:
            for attempt in range(1, max_attempts + 1):
                adapter = pick_adapter(effective_adapters, attempt)

                # --- Pre-attempt: phantom detection ---
                if close_inactive_connections and IS_LINUX:
                    await _clear_inactive_connections(
                        device, adapter, adapters=effective_adapters
                    )

                # --- Adapter rotation: create device for this adapter ---
                if len(effective_adapters) > 1:
                    attempt_device = make_device_for_adapter(device, adapter)
                else:
                    attempt_device = device

                # --- Cross-process slot-based lock ---
                fd: int | None = None
                if lock_config is not None:
                    fd = await acquire_slot(lock_config, adapter)

                timer: threading.Timer | None = None
                try:
                    # --- Single attempt via upstream bleak-retry-connector ---
                    attempt_kwargs = dict(kwargs)
                    if try_direct_first and attempt == 1:
                        attempt_kwargs.setdefault("use_services_cache", True)
                        _LOGGER.debug(
                            "%s: Attempt 1 — trying direct connect (cache path)",
                            display_name,
                        )

                    connect_coro = _brc_establish_connection(
                        client_class,
                        attempt_device,
                        display_name,
                        max_attempts=1,
                        **attempt_kwargs,
                    )

                    if safety_timer:
                        connect_task = asyncio.ensure_future(connect_coro)
                        timer = _create_safety_timer(
                            THREAD_SAFETY_TIMEOUT, loop, connect_task
                        )
                        timer.start()
                        try:
                            client = await connect_task
                        finally:
                            timer.cancel()
                            timer = None
                    else:
                        client = await connect_coro

                except BleakNotFoundError:
                    # Must precede the BleakError clause below
                    # (BleakNotFoundError subclasses BleakError) so the
                    # documented not-found contract raises instead of
                    # retrying through the escalation chain.
                    raise

                except (BleakError, BleakAbortedError, asyncio.TimeoutError, EOFError, BrokenPipeError, asyncio.CancelledError) as exc:
                    if isinstance(exc, asyncio.CancelledError):
                        # Only a cancel aimed at the inner connect task
                        # (safety timer, or a CancelledError leaked from
                        # bleak internals) is retryable.  A cancel of
                        # *this* task — caller shutdown, or wait_for
                        # enforcing overall_timeout — must propagate, or
                        # the retry loop keeps the task alive against
                        # the caller's will.  Task.cancelling() requires
                        # Python 3.11; on older versions propagate every
                        # cancel (conservative).
                        current = asyncio.current_task()
                        cancelling = getattr(current, "cancelling", None)
                        if cancelling is None or cancelling() > 0:
                            raise
                    last_error = exc
                    _LOGGER.debug(
                        "%s: Attempt %d/%d on %s failed: %s",
                        display_name,
                        attempt,
                        max_attempts,
                        adapter,
                        exc,
                    )

                    # --- InProgress classification ---
                    if "InProgress" in str(exc):
                        await _handle_inprogress(device, adapter)
                    elif attempt == max_attempts:
                        await _clear_stale_state(device)

                    # --- Escalation ---
                    if escalation_policy is not None:
                        action = escalation_policy.on_failure(adapter)
                        await _execute_escalation(
                            action, adapter, device, escalation_policy,
                            adapters=effective_adapters,
                        )

                    # Brief backoff before retry
                    if attempt < max_attempts:
                        await asyncio.sleep(0.25)

                    continue

                finally:
                    if timer is not None:
                        timer.cancel()
                    release_slot(fd)

                # --- Post-connect validation ---
                if validate_connection is not None:
                    valid = await _safe_validate(validate_connection, client)

                    if not valid and IS_LINUX:
                        # Some BLE devices (especially Telink-based chips)
                        # report ServicesResolved before all GATT services
                        # are registered on D-Bus.  The Generic Attribute
                        # service appears first, then vendor-specific
                        # services arrive 2-10s later via InterfacesAdded
                        # or Service Changed indication.
                        #
                        # Wait with escalating delays and re-read the GATT
                        # service table from BlueZ each time.
                        cumulative = 0.0
                        for retry_wait in (2.0, 4.0, 8.0):
                            if not client.is_connected:
                                _LOGGER.info(
                                    "%s: BLE link dropped during GATT retry "
                                    "wait, aborting re-validation",
                                    display_name,
                                )
                                break

                            cumulative += retry_wait
                            _LOGGER.info(
                                "%s: Attempt %d/%d on %s — validation failed, "
                                "waiting %.0fs for late GATT services "
                                "(%.0fs cumulative)",
                                display_name,
                                attempt,
                                max_attempts,
                                adapter,
                                retry_wait,
                                cumulative,
                            )
                            await asyncio.sleep(retry_wait)

                            # Force bleak to re-read services from BlueZ.
                            # Clear the backend's cached service collection
                            # (not the wrapper's @property) so _get_services()
                            # rebuilds it from BlueZ's current D-Bus state.
                            try:
                                client._backend.services = None  # type: ignore[union-attr]
                                await client._backend._get_services(  # type: ignore[union-attr]
                                    dangerous_use_bleak_cache=False,
                                )
                            except Exception:
                                _LOGGER.warning(
                                    "%s: _get_services() raised during GATT "
                                    "re-read at %.0fs — will retry",
                                    display_name,
                                    cumulative,
                                    exc_info=True,
                                )
                                continue

                            valid = await _safe_validate(
                                validate_connection, client,
                            )
                            if valid:
                                _LOGGER.info(
                                    "%s: Late GATT services appeared after "
                                    "%.0fs — validation passed",
                                    display_name,
                                    cumulative,
                                )
                                break

                    if not valid:
                        _LOGGER.info(
                            "%s: Attempt %d/%d on %s — validation failed "
                            "after GATT retry, tearing down",
                            display_name,
                            attempt,
                            max_attempts,
                            adapter,
                        )
                        try:
                            await asyncio.wait_for(
                                client.disconnect(), timeout=DISCONNECT_TIMEOUT
                            )
                        except Exception:
                            _LOGGER.debug(
                                "Disconnect after failed validation raised",
                                exc_info=True,
                            )

                        if IS_LINUX:
                            await verified_disconnect(
                                device.address,
                                adapter,
                                timeout=DISCONNECT_TIMEOUT,
                            )

                        await _clear_stale_state(device)
                        if attempt < max_attempts:
                            await asyncio.sleep(0.25)
                        continue

                # --- Success ---
                if escalation_policy is not None:
                    escalation_policy.on_success(adapter)

                _LOGGER.debug(
                    "%s: Connected on attempt %d/%d via %s",
                    display_name,
                    attempt,
                    max_attempts,
                    adapter,
                )
                return client

        # All attempts exhausted
        raise BleakAbortedError(
            f"{display_name}: Failed to connect after {max_attempts} attempts"
            + (f": {last_error}" if last_error else "")
        )

    # Apply overall_timeout if specified
    if overall_timeout is not None:
        _LOGGER.debug(
            "%s: overall_timeout=%.0fs", display_name, overall_timeout,
        )
        return await asyncio.wait_for(_do_establish(), timeout=overall_timeout)
    return await _do_establish()
