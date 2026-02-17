"""BLE connection utilities for dbus-serialbattery BMS drivers.

Provides :class:`Syncron_Ble`, a synchronous-friendly BLE connection wrapper
that maintains a persistent background connection with automatic reconnection
and a notification watchdog.

Internally uses bleak-connection-manager (BCM) for all BLE operations:
  - :func:`managed_find_device` for device discovery with scan locking
  - :func:`establish_connection` with escalation policy for robust connects
  - :class:`ConnectionWatchdog` to detect zombie connections (4-minute timeout)

The external API is unchanged — callers use ``send_data()`` from synchronous
code and read queued notifications from ``_notification_queue``.
"""

import asyncio
import logging
import os
import subprocess
import sys
import threading
import time as _time
from collections import deque

from bleak import BleakClient

from utils import (
    logger,
    BLUETOOTH_FORCE_RESET_BLE_STACK,
    BLUETOOTH_DIRECT_CONNECT,
    BLUETOOTH_PREFERRED_ADAPTER,
)

# BCM imports — available because dbus-serialbattery.py adds ext/bleak-connection-manager/src to sys.path
try:
    from bleak_connection_manager import (
        ConnectionWatchdog,
        EscalationPolicy,
        LockConfig,
        ScanLockConfig,
        establish_connection,
        managed_find_device,
        validate_gatt_services,
        PROFILE_BATTERY,
    )
    from bleak_connection_manager.adapters import discover_adapters
    from bleak_connection_manager.recovery import reset_adapter as _bcm_reset_adapter
    _HAS_BCM = True
except ImportError:
    _HAS_BCM = False
    logger.warning("bleak-connection-manager not available, falling back to basic BLE")

_bcm_logger = logging.getLogger("bleak_connection_manager")
_bcm_logger.setLevel(logging.DEBUG)

# Shared BCM lock configs — all serialbattery BLE instances share the same locks
# to prevent scan/connect contention across battery processes
_lock_config = LockConfig(enabled=True) if _HAS_BCM else None
_scan_lock_config = ScanLockConfig(enabled=True) if _HAS_BCM else None


class Syncron_Ble:
    """Synchronous BLE connection wrapper for serialbattery BMS drivers.

    Maintains a persistent background connection with automatic reconnection
    using bleak-connection-manager.  The external API is synchronous — callers
    use :meth:`send_data` to write commands and read responses.  BLE
    notifications are queued in :attr:`_notification_queue`.

    Parameters
    ----------
    address:
        The BLE MAC address of the BMS device.
    read_characteristic:
        The UUID of the BLE characteristic that sends notifications.
    write_characteristic:
        The UUID of the BLE characteristic to write commands to.
    """

    ble_async_thread_ready = threading.Event()
    ble_connection_ready = threading.Event()
    ble_async_thread_event_loop = False
    client = False
    address = None
    response_event = False
    response_data = False
    main_thread = False
    connected = False

    write_characteristic = None
    read_characteristic = None
    _first_connect = True

    # Watchdog timeout: 90s.  If no BLE notifications arrive for this long,
    # the connection is assumed dead and will be torn down and re-established.
    # The HumsiENK BMS sends battery_info every ~60s, so 90s gives one
    # missed cycle before we act.
    _notification_watchdog_timeout = 90

    # Hard stale timeout: if the connection appears "connected" but no
    # actual BMS data frames have been received for this long, force a
    # full teardown including adapter reset.  Catches the pathological
    # case where scan-fail + direct-connect loops keep cycling without
    # ever getting real data.
    _hard_stale_timeout = 300  # 5 minutes

    def __init__(self, address, read_characteristic, write_characteristic):
        self.write_characteristic = write_characteristic
        self.read_characteristic = read_characteristic
        self.address = address
        self._notification_queue = deque()
        self._last_notification_time = _time.time()

        # Start a new thread that will run the async BLE connection loop
        self.main_thread = threading.current_thread()
        self._ble_thread = threading.Thread(
            name="BMS_ble_%s" % address[-5:].replace(":", ""),
            target=self.initiate_ble_thread_main,
            daemon=True,
        )
        self._ble_thread.start()

        thread_start_ok = self.ble_async_thread_ready.wait(2)
        if not thread_start_ok:
            logger.error("BLE async thread took too long to start for %s", address)

        # Wait up to 30s for the first connection.  The daemon thread
        # keeps retrying in background, so the process stays alive and
        # serves cached data until the connection succeeds.
        connected_ok = self.ble_connection_ready.wait(30)
        if not connected_ok:
            logger.warning(
                "BLE initial connection to %s not ready in 30s — "
                "daemon thread will keep retrying in background",
                self.address,
            )
            self.connected = False
        else:
            try:
                self.connected = bool(self.client and self.client.is_connected)
            except Exception:
                self.connected = False

    def initiate_ble_thread_main(self):
        try:
            asyncio.run(self.async_main(self.address))
        except Exception as e:
            logger.error("BLE daemon thread crashed: %s", repr(e))
        finally:
            logger.error("BLE daemon thread exited for %s", self.address)

    async def async_main(self, address):
        self.ble_async_thread_event_loop = asyncio.get_event_loop()
        self.ble_async_thread_ready.set()

        # Discover adapters once for the escalation policy
        if _HAS_BCM:
            self._adapters = discover_adapters()
            logger.info("BLE [%s] discovered adapters: %s", address, self._adapters)
        else:
            self._adapters = ["hci0"]

        consecutive_failures = 0
        # Track when last actual notification data was received (for hard stale timeout)
        self._last_data_time = _time.time()
        total_reconnect_cycles = 0
        while self.main_thread.is_alive():
            total_reconnect_cycles += 1
            stale_seconds = _time.time() - self._last_data_time

            # ── Hard stale timeout: if we've been cycling through connect
            # attempts for too long without getting real data, force an
            # adapter reset to clear any stuck BlueZ/HCI state.
            if (
                consecutive_failures >= 3
                and stale_seconds > self._hard_stale_timeout
                and _HAS_BCM
            ):
                logger.error(
                    "BLE [%s] HARD STALE TIMEOUT: %d consecutive failures, "
                    "no data for %.0fs — forcing adapter reset",
                    address, consecutive_failures, stale_seconds,
                )
                for adapter in self._adapters:
                    try:
                        success = await _bcm_reset_adapter(adapter)
                        logger.info(
                            "BLE [%s] adapter %s reset %s",
                            address, adapter,
                            "succeeded" if success else "FAILED",
                        )
                    except Exception as e:
                        logger.error(
                            "BLE [%s] adapter %s reset error: %s",
                            address, adapter, repr(e),
                        )
                consecutive_failures = 0
                await asyncio.sleep(5.0)
                continue

            logger.info(
                "BLE [%s] connect cycle %d (failures=%d, stale=%.0fs)",
                address, total_reconnect_cycles, consecutive_failures,
                stale_seconds,
            )

            try:
                result = await self.connect_to_bms(self.address)
            except Exception as e:
                logger.error("BLE connect_to_bms raised: %s", repr(e))
                result = False

            if result is False:
                consecutive_failures += 1
                delay = min(0.5 * (2 ** (consecutive_failures - 1)), 8.0)
                logger.warning(
                    "BLE [%s] connection attempt failed (%d consecutive, "
                    "stale=%.0fs), retry in %.1fs",
                    self.address, consecutive_failures, stale_seconds, delay,
                )
                try:
                    await asyncio.sleep(delay)
                except Exception:
                    pass
                if consecutive_failures >= 5:
                    consecutive_failures = 0
            else:
                consecutive_failures = 0
                # Brief pause before reconnecting to let BlueZ clean up
                await asyncio.sleep(2.0)

    def client_disconnected(self, client):
        logger.warning(
            "BLE device %s disconnected (was connected=%s, thread_alive=%s)",
            self.address,
            getattr(self, 'connected', '?'),
            self._ble_thread.is_alive() if hasattr(self, '_ble_thread') else '?',
        )
        self.connected = False

    async def connect_to_bms(self, address):
        """Connect to BMS using bleak-connection-manager.

        Uses BCM's managed_find_device for discovery with scan locking,
        establish_connection with PROFILE_BATTERY escalation for robust
        connection, and ConnectionWatchdog for zombie detection.
        """
        if not _HAS_BCM:
            logger.error("BLE: bleak-connection-manager not available")
            return False

        adapters = getattr(self, '_adapters', None) or discover_adapters()
        escalation = EscalationPolicy(adapters, config=PROFILE_BATTERY)

        # Step 1: Find the device via managed scan
        logger.info("BLE [%s] scanning...", address)
        device = None
        try:
            device = await managed_find_device(
                address,
                timeout=15.0,
                max_attempts=3,
                scan_lock_config=_scan_lock_config,
            )
        except Exception as e:
            logger.warning("BLE [%s] scan failed: %s", address, repr(e))

        # Step 2: Establish connection via BCM
        # If scan found the device, connect normally.
        # If scan failed (InProgress, device not advertising, etc.),
        # fall back to direct connect using BlueZ cached device info.
        direct_fallback = device is None
        if direct_fallback:
            logger.info(
                "BLE [%s] scan failed, trying direct connect (cached device info)",
                address,
            )
            from bleak.backends.device import BLEDevice

            device = BLEDevice(address=address, name=None, rssi=0, details={})

        logger.info("BLE [%s] connecting%s...", address,
                     " (direct fallback)" if direct_fallback else "")
        try:
            self.client = await establish_connection(
                BleakClient,
                device,
                "serialbattery %s" % address,
                disconnected_callback=self.client_disconnected,
                max_attempts=5,
                close_inactive_connections=True,
                try_direct_first=direct_fallback or BLUETOOTH_DIRECT_CONNECT,
                validate_connection=validate_gatt_services,
                lock_config=_lock_config,
                escalation_policy=escalation,
                overall_timeout=240.0,
                timeout=15.0,
            )
        except asyncio.TimeoutError:
            logger.error("BLE [%s] establish_connection timed out (240s)", address)
            self.ble_connection_ready.set()
            self.connected = False
            return False
        except Exception as e:
            logger.warning("BLE [%s] connection failed: %s", address, repr(e))
            self.ble_connection_ready.set()
            self.connected = False
            return False

        logger.info(
            "BLE [%s] connected (MTU: %d)",
            address, self.client.mtu_size,
        )

        # Step 3: Start notifications
        try:
            await asyncio.wait_for(
                self.client.start_notify(
                    self.read_characteristic,
                    self.notify_read_callback,
                ),
                timeout=5.0,
            )
        except Exception as e:
            logger.warning(
                "BLE [%s] start_notify failed: %s", address, repr(e)
            )
            # If "Not connected" → the BlueZ cache was stale.  Remove the
            # device from BlueZ so the next attempt does a fresh connect
            # instead of reusing the dead cache entry.
            if "Not connected" in str(e):
                logger.info(
                    "BLE [%s] clearing stale BlueZ cache entry", address,
                )
                try:
                    from bleak_connection_manager.bluez import (
                        disconnect_device,
                        remove_device,
                    )
                    await disconnect_device(address, "hci0")
                    await remove_device(address, "hci0")
                except Exception:
                    pass
            else:
                try:
                    await asyncio.wait_for(self.client.disconnect(), timeout=5.0)
                except Exception:
                    pass
            self.ble_connection_ready.set()
            self.connected = False
            return False

        # Step 4: Mark connected
        self._first_connect = False
        self.ble_connection_ready.set()
        self.connected = True
        self._last_notification_time = _time.time()

        logger.info(
            "BLE [%s] fully connected, starting watchdog (timeout=%ds)",
            address, self._notification_watchdog_timeout,
        )

        # Step 5: Connection maintenance loop with watchdog
        #
        # The watchdog is NOT fed from raw BLE notifications.  Instead,
        # the BMS driver calls feed_watchdog() after it successfully
        # parses a valid, checksum-verified frame.  This ensures the
        # watchdog fires when the BMS stops sending *real data*, not
        # just when the BLE transport goes quiet.
        async def _on_watchdog_timeout():
            logger.error(
                "BLE [%s] WATCHDOG FIRED: no valid BMS frames for %ds "
                "— forcing disconnect+reconnect",
                address, self._notification_watchdog_timeout,
            )

        watchdog = ConnectionWatchdog(
            timeout=float(self._notification_watchdog_timeout),
            on_timeout=_on_watchdog_timeout,
            client=self.client,
        )
        watchdog.start()
        self._watchdog = watchdog

        connection_start = _time.time()
        last_maintenance_log = 0.0
        try:
            while self.client.is_connected and self.main_thread.is_alive():
                await asyncio.sleep(0.5)

                # Periodic maintenance status log (every 5 min)
                now = _time.time()
                silence = now - self._last_notification_time
                if now - last_maintenance_log > 300:
                    uptime = now - connection_start
                    logger.info(
                        "BLE [%s] connection alive %.0fs, last notification %.0fs ago, "
                        "watchdog_running=%s",
                        address, uptime, silence, watchdog.is_running,
                    )
                    last_maintenance_log = now

                # Check if watchdog fired (it stops itself after timeout)
                if not watchdog.is_running:
                    logger.warning(
                        "BLE [%s] watchdog expired after %.0fs silence, "
                        "breaking connection loop (connection was up %.0fs)",
                        address, silence, now - connection_start,
                    )
                    break
        finally:
            watchdog.stop()
            try:
                await asyncio.wait_for(self.client.disconnect(), timeout=5.0)
            except (asyncio.TimeoutError, Exception):
                pass
            self.connected = False
            uptime = _time.time() - connection_start
            logger.info(
                "BLE [%s] disconnected after %.0fs connection",
                address, uptime,
            )

    # ── Watchdog feeding ────────────────────────────────────────────

    def feed_watchdog(self):
        """Signal that a valid BMS frame was received.

        Called by the BMS driver (e.g. HumsiENK_Ble) after it
        successfully parses a checksum-verified frame.  This is the
        ONLY way the connection watchdog gets fed — raw BLE
        notifications are not sufficient.
        """
        if hasattr(self, '_watchdog') and self._watchdog is not None:
            self._watchdog.notify_activity()

    # ── Notification handling ─────────────────────────────────────────

    def notify_read_callback(self, sender, data: bytearray):
        """Handle incoming BLE notification.

        Queues the data for consumption by the synchronous caller and
        updates the notification timestamp for watchdog tracking.
        Also updates _last_data_time used by the hard stale timeout
        in async_main.
        """
        try:
            self._notification_queue.append(bytes(data))
            now = _time.time()
            self._last_notification_time = now
            # Also update the async_main stale tracker so the hard stale
            # timeout knows we're getting real BLE data
            self._last_data_time = now
        except Exception:
            pass
        self.response_data = data
        try:
            if self.response_event and hasattr(self.response_event, "set"):
                self.response_event.set()
        except Exception:
            pass

    # ── Synchronous command interface ─────────────────────────────────

    async def ble_thread_send_com(self, command):
        self.response_event = asyncio.Event()
        self.response_data = False
        await self.client.write_gatt_char(self.write_characteristic, command, True)
        await asyncio.wait_for(self.response_event.wait(), timeout=1)
        self.response_event = False
        return self.response_data

    async def send_coroutine_to_ble_thread_and_wait_for_result(self, coroutine):
        bt_task = asyncio.run_coroutine_threadsafe(coroutine, self.ble_async_thread_event_loop)
        result = await asyncio.wait_for(asyncio.wrap_future(bt_task), timeout=1.5)
        return result

    def send_data(self, data):
        """Send a command to the BMS and wait for the response.

        This is the primary synchronous interface for BMS drivers.
        Schedules the BLE write on the daemon thread's event loop and
        blocks the calling thread until the response arrives (up to 2s).

        Returns the response data on success, or ``False`` on failure.
        """
        if not self._ble_thread.is_alive():
            logger.error("BLE: daemon thread is dead, send_data returning False")
            return False
        if not self.connected:
            logger.debug("BLE: send_data skipped — not connected")
            return False

        future = asyncio.run_coroutine_threadsafe(
            self.ble_thread_send_com(data), self.ble_async_thread_event_loop
        )
        try:
            return future.result(timeout=2.0)
        except Exception as e:
            logger.warning("BLE: send_data failed: %s", e)
            return False


def restart_ble_hardware_and_bluez_driver():
    if not BLUETOOTH_FORCE_RESET_BLE_STACK:
        return

    logger.info("*** Restarting BLE hardware and Bluez driver ***")

    # list bluetooth controllers
    result = subprocess.run(["hciconfig"], capture_output=True, text=True)
    logger.info("hciconfig exit code: %d", result.returncode)
    logger.info("hciconfig output: %s", result.stdout)

    # bluetoothctl list
    result = subprocess.run(["bluetoothctl", "list"], capture_output=True, text=True)
    logger.info("bluetoothctl list exit code: %d", result.returncode)
    logger.info("bluetoothctl list output: %s", result.stdout)

    # stop will not work, if service/bluetooth driver is stuck
    result = subprocess.run(["/etc/init.d/bluetooth", "stop"], capture_output=True, text=True)
    logger.info("bluetooth stop exit code: %d", result.returncode)
    logger.info("bluetooth stop output: %s", result.stdout)

    # process kill is needed, since the service/bluetooth driver is probably freezed
    result = subprocess.run(["pkill", "-f", "bluetoothd"], capture_output=True, text=True)
    logger.info("pkill exit code: %d", result.returncode)
    logger.info("pkill output: %s", result.stdout)

    # rfkill block bluetooth
    result = subprocess.run(["rfkill", "block", "bluetooth"], capture_output=True, text=True)
    logger.info("rfkill block exit code: %d", result.returncode)
    logger.info("rfkill block output: %s", result.stdout)

    # kill hdciattach
    result = subprocess.run(["pkill", "-f", "hciattach"], capture_output=True, text=True)
    logger.info("pkill hciattach exit code: %d", result.returncode)
    logger.info("pkill hciattach output: %s", result.stdout)
    from time import sleep
    sleep(0.5)

    # kill hci_uart
    result = subprocess.run(["rmmod", "hci_uart"], capture_output=True, text=True)
    logger.info("rmmod hci_uart exit code: %d", result.returncode)
    logger.info("rmmod hci_uart output: %s", result.stdout)

    # kill btbcm
    result = subprocess.run(["rmmod", "btbcm"], capture_output=True, text=True)
    logger.info("rmmod btbcm exit code: %d", result.returncode)
    logger.info("rmmod btbcm output: %s", result.stdout)

    # load hci_uart
    result = subprocess.run(["modprobe", "hci_uart"], capture_output=True, text=True)
    logger.info("modprobe hci_uart exit code: %d", result.returncode)
    logger.info("modprobe hci_uart output: %s", result.stdout)

    # load btbcm
    result = subprocess.run(["modprobe", "btbcm"], capture_output=True, text=True)
    logger.info("modprobe btbcm exit code: %d", result.returncode)
    logger.info("modprobe btbcm output: %s", result.stdout)

    sleep(2)

    result = subprocess.run(["rfkill", "unblock", "bluetooth"], capture_output=True, text=True)
    logger.info("rfkill unblock exit code: %d", result.returncode)
    logger.info("rfkill unblock output: %s", result.stdout)

    result = subprocess.run(["/etc/init.d/bluetooth", "start"], capture_output=True, text=True)
    logger.info("bluetooth start exit code: %d", result.returncode)
    logger.info("bluetooth start output: %s", result.stdout)

    logger.info("System Bluetooth daemon should have been restarted")
    logger.info("Exit driver for clean restart")

    sys.exit(1)
