import threading
import time as _time
import asyncio
import subprocess
import sys
import fcntl
from collections import deque
from bleak import BleakClient
from time import sleep
from utils import (
    logger,
    BLUETOOTH_FORCE_RESET_BLE_STACK,
    BLUETOOTH_DIRECT_CONNECT,
    BLUETOOTH_PREFERRED_ADAPTER,
)

# Cross-process lock file for serializing BLE scan/connect across service instances.
# BleakClient(address) triggers an implicit BleakScanner.discover() during connect(),
# and BlueZ returns "InProgress" if two processes scan simultaneously.
_BLE_CONNECT_LOCK_PATH = "/tmp/dbus-serialbattery-ble-connect.lock"


# Class that enables synchronous writing and reading to a bluetooh device
class Syncron_Ble:

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
    # Watchdog: force-disconnect if no BLE notifications for this many seconds.
    # Catches "zombie connections" where BlueZ thinks it's connected but the
    # radio link has silently died (no disconnect callback fires).
    _notification_watchdog_timeout = 180  # 3 minutes
    # Adapter reset escalation: after this many consecutive full connect_to_bms
    # failures, reset the BLE adapter as a last resort to clear deeply-stuck
    # phantom BlueZ states (Connected=yes with no HCI handle).
    _consecutive_connect_failures = 0
    _adapter_reset_threshold = 3  # full cycles before escalation
    _last_adapter_reset_time = 0.0
    _adapter_reset_cooldown = 300.0  # min 5 minutes between resets

    def __init__(self, address, read_characteristic, write_characteristic):
        """
        address: the address of the bluetooth device to read and write to
        read_characteristic: the id of bluetooth LE characteristic that will send a
        notification when there is new data to read.
        write_characteristic: the id of the bluetooth LE characteristic that the class writes messages to
        """

        self.write_characteristic = write_characteristic
        self.read_characteristic = read_characteristic
        self.address = address
        self._notification_queue = deque()
        self._last_notification_time = _time.time()

        # Start a new thread that will run bleak the async bluetooth LE library
        self.main_thread = threading.current_thread()
        self._ble_thread = threading.Thread(name="BMS_bluetooth_async_thread", target=self.initiate_ble_thread_main, daemon=True)
        self._ble_thread.start()

        thread_start_ok = self.ble_async_thread_ready.wait(2)
        if not thread_start_ok:
            logger.error("bluetooh LE thread took to long to start")

        # Wait up to 30 s for the first connection.  If it doesn't happen
        # in time, log a warning but do NOT mark the handle as failed.
        # The daemon thread keeps retrying in the background, so the
        # process stays alive and serves cached / default data until the
        # connection succeeds.  This prevents the destructive crash-loop
        # where each killed process leaves BlueZ in a dirty state.
        connected_ok = self.ble_connection_ready.wait(30)
        if not connected_ok:
            logger.warning(
                f"BLE initial connection to {self.address} not ready in 30 s — "
                f"daemon thread will keep retrying in background"
            )
            # Mark as not-yet-connected; daemon thread will flip this
            # to True as soon as it succeeds.
            self.connected = False
        else:
            # Mark connected only if client exists and is connected
            try:
                self.connected = bool(self.client and self.client.is_connected)
            except Exception:
                self.connected = False

    def initiate_ble_thread_main(self):
        try:
            asyncio.run(self.async_main(self.address))
        except Exception as e:
            logger.error(f"BLE daemon thread crashed: {repr(e)}")
        finally:
            logger.error(f"BLE daemon thread exited for {self.address}")

    async def async_main(self, address):
        self.ble_async_thread_event_loop = asyncio.get_event_loop()
        self.ble_async_thread_ready.set()

        # try to connect over and over if the connection fails
        consecutive_failures = 0
        while self.main_thread.is_alive():
            try:
                result = await self.connect_to_bms(self.address)
            except Exception as e:
                logger.error(f"BLE connect_to_bms raised unhandled exception: {repr(e)}")
                result = False
            if result is False:
                consecutive_failures += 1
                # exponential backoff: 0.5s, 1s, 2s, 4s, 8s (cap at 8s)
                delay = min(0.5 * (2 ** (consecutive_failures - 1)), 8.0)
                try:
                    await asyncio.sleep(delay)
                except Exception:
                    pass
                if consecutive_failures >= 5:
                    consecutive_failures = 0
            else:
                # reset failure counter after any successful session
                consecutive_failures = 0
                # Brief pause before reconnecting to let BlueZ clean up.
                # Too short (1s) causes unstable reconnections; too long
                # (3s+) wastes data time.  2s is a good compromise.
                await asyncio.sleep(2.0)

    def client_disconnected(self, client):
        logger.error(f"bluetooh device with address: {self.address} disconnected")

    async def connect_to_bms(self, address):
        def _list_adapters():
            names = []
            try:
                import os
                base = "/sys/class/bluetooth"
                if os.path.isdir(base):
                    for name in os.listdir(base):
                        if name.startswith("hci"):
                            names.append(name)
            except Exception:
                pass
            if not names:
                try:
                    result = subprocess.run(["hciconfig"], capture_output=True, text=True)
                    for line in (result.stdout or "").splitlines():
                        line = line.strip()
                        if line.startswith("hci") and ":" in line:
                            names.append(line.split(":", 1)[0])
                except Exception:
                    pass
            # de-dup and sort
            return sorted(list(dict.fromkeys(names)))

        if BLUETOOTH_DIRECT_CONNECT:
            # Parse BLUETOOTH_PREFERRED_ADAPTER as a comma-separated list
            # e.g. "hci1, hci0" → rotate between both on InProgress retries
            configured = [
                a.strip().lower()
                for a in BLUETOOTH_PREFERRED_ADAPTER.split(",")
                if a.strip() and a.strip().lower() not in ("auto", "default")
            ] if BLUETOOTH_PREFERRED_ADAPTER else []

            if configured:
                adapters_to_rotate = configured
            else:
                adapters_to_rotate = _list_adapters() or ["hci0"]
        else:
            adapters_to_rotate = [None]

        # Acquire a cross-process file lock to serialize BLE scanning/connecting.
        # This prevents BlueZ "InProgress" errors when multiple battery services
        # attempt to scan simultaneously via bleak's implicit discover().
        # CRITICAL: must use LOCK_NB (non-blocking) with async sleep between
        # attempts.  A blocking LOCK_EX freezes the entire asyncio event loop,
        # making the daemon thread permanently unresponsive if the other process
        # holds the lock for a long time (e.g. during a hanging connect()).
        lock_fd = None
        lock_acquired = False
        try:
            lock_fd = open(_BLE_CONNECT_LOCK_PATH, "w")
            logger.info(f"BLE connect lock: waiting to acquire for {address}")
            lock_timeout = _time.time() + 15.0  # max 15s waiting for lock
            while _time.time() < lock_timeout:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    lock_acquired = True
                    logger.info(f"BLE connect lock: acquired for {address}")
                    break
                except (IOError, OSError):
                    # Lock held by other process — yield to event loop and retry
                    await asyncio.sleep(0.5)
            if not lock_acquired:
                logger.warning(
                    f"BLE connect lock: timed out after 15s for {address}, "
                    f"proceeding without lock"
                )
        except Exception as e:
            logger.warning(f"BLE connect lock: could not acquire ({repr(e)}), proceeding without lock")

        # Wall-clock deadline — must finish before main thread timeout (20s)
        deadline = _time.time() + 18.0
        max_retries = 8
        connected = False
        inprogress_failures = 0  # track how many attempts failed with InProgress

        try:
            # Clean up any stale BlueZ state before every connect attempt.
            # Previously gated by _first_connect, but phantom "Connected"
            # states persist across watchdog-triggered reconnects too:
            # BlueZ reports Connected=yes with no HCI handle, causing
            # BleakClient.connect() to "succeed" instantly on a dead link.
            for _adapter in adapters_to_rotate:
                    if _adapter is None:
                        continue
                    # Cancel any pending LE Create Connection left by a
                    # killed process.  Without this, BlueZ may be stuck
                    # waiting for a connection that will never complete,
                    # causing the next connect() to hang indefinitely.
                    try:
                        subprocess.run(
                            ["hcitool", "-i", _adapter, "cmd", "0x08", "0x000E"],
                            capture_output=True, timeout=3,
                        )
                    except Exception:
                        pass
                    # Drop any stale LE connection handles.
                    try:
                        _result = subprocess.run(
                            ["hcitool", "-i", _adapter, "con"],
                            capture_output=True, text=True, timeout=3,
                        )
                        for _line in (_result.stdout or "").splitlines():
                            if address.upper() in _line.upper():
                                _parts = _line.split("handle ")
                                if len(_parts) > 1:
                                    _handle = _parts[1].split()[0]
                                    subprocess.run(
                                        ["hcitool", "-i", _adapter, "ledc", _handle],
                                        timeout=3,
                                    )
                                    logger.info(f"BLE: dropped stale handle {_handle} on {_adapter} for {address}")
                                    await asyncio.sleep(0.5)
                    except Exception:
                        pass

            # Clear phantom BlueZ "Connected" state via D-Bus disconnect.
            # BlueZ can report Connected=yes with no real HCI handle after
            # a radio link dies silently.  hcitool cleanup above handles
            # actual HCI handles, but the D-Bus property needs a separate
            # Disconnect call to clear.
            try:
                subprocess.run(
                    ["bluetoothctl", "disconnect", address],
                    capture_output=True, text=True, timeout=5,
                )
            except Exception:
                pass

            logger.info(f"BLE connect: entering retry loop for {address} (deadline in {deadline - _time.time():.0f}s)")
            for attempt in range(1, max_retries + 1):
                remaining = deadline - _time.time()
                if remaining <= 1.0:
                    logger.warning(f"BLE connect deadline reached for {address}")
                    break

                # Rotate through adapters on successive attempts
                adapter = adapters_to_rotate[(attempt - 1) % len(adapters_to_rotate)]
                adapter_name = adapter or "default"
                logger.info(f"BLE connect: attempt {attempt}/{max_retries} on {adapter_name} for {address} ({remaining:.0f}s left)")

                # Cap each connect attempt so a hanging BlueZ doesn't
                # consume the entire deadline budget.
                connect_timeout = min(10.0, max(remaining - 1.0, 3.0))

                self.client = BleakClient(
                    address,
                    disconnected_callback=self.client_disconnected,
                    adapter=adapter,
                    timeout=connect_timeout,
                ) if adapter else BleakClient(
                    address,
                    disconnected_callback=self.client_disconnected,
                    timeout=connect_timeout,
                )

                try:
                    # Wrap the ENTIRE connection setup (connect + GATT check
                    # + start_notify) in a single hard timeout.  On BlueZ,
                    # any of these calls can hang indefinitely despite the
                    # BleakClient timeout parameter being set.
                    async with asyncio.timeout(connect_timeout):
                        await self.client.connect()

                        # Verify GATT service discovery completed
                        if not bool(self.client.services):
                            logger.error(f"BLE GATT services not resolved on {adapter_name} for {address}")
                            try:
                                await self.client.disconnect()
                            except Exception:
                                pass
                            await asyncio.sleep(0.3)
                            continue

                        # Request larger MTU (the HumsiENK app requests 150).
                        # Larger MTU avoids ATT fragmentation for multi-byte responses.
                        try:
                            mtu = self.client.mtu_size
                            if mtu and mtu < 150:
                                logger.debug(f"BLE negotiated MTU={mtu} for {address}")
                        except Exception:
                            pass

                        await self.client.start_notify(self.read_characteristic, self.notify_read_callback)
                        await asyncio.sleep(0.2)

                    logger.info(f"BLE connected to {address} on {adapter_name} (attempt {attempt})")
                    self._first_connect = False
                    connected = True
                    break
                except Exception as e:
                    error_str = repr(e)
                    try:
                        await self.client.disconnect()
                    except Exception:
                        pass

                    if "InProgress" in error_str and attempt < max_retries:
                        inprogress_failures += 1
                        delay = min(0.75 + attempt * 0.25, 3.0)
                        logger.info(f"BLE {adapter_name} busy (InProgress), retry in {delay:.1f}s ({attempt}/{max_retries})")
                        await asyncio.sleep(delay)
                        continue
                    elif "InProgress" in error_str:
                        inprogress_failures += 1
                    else:
                        logger.error(f"BLE connect failed on {adapter_name} ({attempt}/{max_retries}): {error_str}")
                        await asyncio.sleep(0.3)
                        continue

            if not connected:
                # Auto-recovery: if most attempts failed with "InProgress",
                # BlueZ has a stale connecting/connected state for this device.
                # Clear it by cancelling pending HCI connections and removing
                # the device so it can be re-discovered cleanly.
                if inprogress_failures >= max_retries // 2:
                    logger.warning(
                        f"BLE auto-recovery: {inprogress_failures}/{max_retries} attempts "
                        f"failed with InProgress for {address} — clearing BlueZ state"
                    )
                    for _adapter in adapters_to_rotate:
                        if _adapter is None:
                            continue
                        try:
                            subprocess.run(
                                ["hcitool", "-i", _adapter, "cmd", "0x08", "0x000E"],
                                capture_output=True, timeout=3,
                            )
                        except Exception:
                            pass
                    # Remove device from BlueZ to clear stale connection state.
                    # It will be re-discovered via BLE advertisements automatically.
                    try:
                        subprocess.run(
                            ["bluetoothctl", "remove", address],
                            capture_output=True, text=True, timeout=5,
                        )
                        logger.info(f"BLE auto-recovery: removed {address} from BlueZ, waiting for re-discovery")
                        await asyncio.sleep(3.0)
                    except Exception as e:
                        logger.warning(f"BLE auto-recovery: bluetoothctl remove failed: {repr(e)}")

                # Track consecutive full-cycle failures for adapter reset escalation
                self._consecutive_connect_failures += 1
                logger.warning(
                    f"BLE connect failed for {address} "
                    f"({self._consecutive_connect_failures}/{self._adapter_reset_threshold} "
                    f"consecutive failures before adapter reset)"
                )

                # Last resort: reset the BLE adapter to clear deeply-stuck
                # phantom states where BlueZ reports Connected=yes but there
                # is no HCI handle.  bluetoothctl disconnect/remove and D-Bus
                # RemoveDevice all hang in this state — only an adapter
                # down/up clears it.
                if (
                    self._consecutive_connect_failures >= self._adapter_reset_threshold
                    and (_time.time() - self._last_adapter_reset_time) > self._adapter_reset_cooldown
                ):
                    # Find which adapter(s) to reset — only reset adapters
                    # that were actually tried, not all adapters.
                    for _adapter in adapters_to_rotate:
                        if _adapter is None:
                            continue
                        logger.error(
                            f"BLE ADAPTER RESET: resetting {_adapter} after "
                            f"{self._consecutive_connect_failures} consecutive failures "
                            f"for {address}"
                        )
                        try:
                            subprocess.run(
                                ["hciconfig", _adapter, "down"],
                                capture_output=True, timeout=5,
                            )
                            await asyncio.sleep(1.0)
                            subprocess.run(
                                ["hciconfig", _adapter, "up"],
                                capture_output=True, timeout=5,
                            )
                            logger.info(f"BLE ADAPTER RESET: {_adapter} reset complete")
                        except Exception as e:
                            logger.error(f"BLE ADAPTER RESET: failed for {_adapter}: {repr(e)}")

                    # An adapter down/up can crash bluetoothd.  Verify it's
                    # still running and restart if needed.
                    try:
                        _check = subprocess.run(
                            ["/etc/init.d/bluetooth", "status"],
                            capture_output=True, text=True, timeout=5,
                        )
                        if "is stopped" in (_check.stdout or ""):
                            logger.error("BLE ADAPTER RESET: bluetoothd crashed, restarting")
                            subprocess.run(
                                ["/etc/init.d/bluetooth", "restart"],
                                capture_output=True, timeout=10,
                            )
                            await asyncio.sleep(3.0)
                    except Exception as e:
                        logger.warning(f"BLE ADAPTER RESET: bluetooth status check failed: {repr(e)}")

                    self._last_adapter_reset_time = _time.time()
                    self._consecutive_connect_failures = 0
                    # Give BlueZ time to re-initialize the adapter
                    await asyncio.sleep(3.0)

                self.ble_connection_ready.set()
                self.connected = False
                return False
        finally:
            # Release the lock so the next service instance can connect
            if lock_fd:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    lock_fd.close()
                    logger.info(f"BLE connect lock: released for {address}")
                except Exception:
                    pass

        try:
            self.ble_connection_ready.set()
            self.connected = True
            self._consecutive_connect_failures = 0  # reset escalation counter on success
            self._last_notification_time = _time.time()  # reset watchdog on connect
            while self.client.is_connected and self.main_thread.is_alive():
                await asyncio.sleep(0.5)
                # Watchdog: if no BLE notifications for 3 minutes, the radio
                # link is likely dead even though BlueZ still says "connected".
                # Force a disconnect so the outer loop can reconnect.
                silence = _time.time() - self._last_notification_time
                if silence > self._notification_watchdog_timeout:
                    logger.warning(
                        f"BLE watchdog: no notifications for {silence:.0f}s "
                        f"from {self.address}, forcing reconnect"
                    )
                    try:
                        await self.client.disconnect()
                    except Exception:
                        pass
                    break
        finally:
            try:
                await self.client.disconnect()
            except Exception:
                pass
            self.connected = False

    # saves response and tells the command sender that the response has arived
    def notify_read_callback(self, sender, data: bytearray):
        # Append to queue to avoid races and packet drops
        try:
            self._notification_queue.append(bytes(data))
            self._last_notification_time = _time.time()
        except Exception:
            pass
        self.response_data = data
        try:
            # Only signal if an Event was set up for a waiter
            if self.response_event and hasattr(self.response_event, "set"):
                self.response_event.set()
        except Exception:
            # Ignore if no waiter is present
            pass

    async def ble_thread_send_com(self, command):
        self.response_event = asyncio.Event()
        self.response_data = False
        await self.client.write_gatt_char(self.write_characteristic, command, True)
        await asyncio.wait_for(self.response_event.wait(), timeout=1)  # Wait for the response notification
        self.response_event = False
        return self.response_data

    async def send_coroutine_to_ble_thread_and_wait_for_result(self, coroutine):
        bt_task = asyncio.run_coroutine_threadsafe(coroutine, self.ble_async_thread_event_loop)
        result = await asyncio.wait_for(asyncio.wrap_future(bt_task), timeout=1.5)
        return result

    def send_data(self, data):
        # Guard: if the daemon thread has died, fail fast instead of blocking
        # on a dead event loop for the full 2s timeout.
        if not self._ble_thread.is_alive():
            logger.error("BLE: daemon thread is dead, send_data returning False")
            return False
        # Guard: if not connected, fail fast — avoids the 2s timeout and
        # "Service Discovery has not been performed yet" warnings.
        if not self.connected:
            logger.debug("BLE: send_data skipped — not connected")
            return False
        # Schedule the BLE write on the daemon thread's event loop and wait
        # synchronously for the result.  Avoids asyncio.run() which would
        # create a throwaway event loop and block the calling thread during
        # its cleanup phase.
        future = asyncio.run_coroutine_threadsafe(
            self.ble_thread_send_com(data), self.ble_async_thread_event_loop
        )
        try:
            return future.result(timeout=2.0)
        except Exception as e:
            logger.warning(f"BLE: send_data failed: {e}")
            return False


def restart_ble_hardware_and_bluez_driver():
    if not BLUETOOTH_FORCE_RESET_BLE_STACK:
        return

    logger.info("*** Restarting BLE hardware and Bluez driver ***")

    # list bluetooth controllers
    result = subprocess.run(["hciconfig"], capture_output=True, text=True)
    logger.info(f"hciconfig exit code: {result.returncode}")
    logger.info(f"hciconfig output: {result.stdout}")

    # bluetoothctl list
    result = subprocess.run(["bluetoothctl", "list"], capture_output=True, text=True)
    logger.info(f"bluetoothctl list exit code: {result.returncode}")
    logger.info(f"bluetoothctl list output: {result.stdout}")

    # stop will not work, if service/bluetooth driver is stuck
    result = subprocess.run(["/etc/init.d/bluetooth", "stop"], capture_output=True, text=True)
    logger.info(f"bluetooth stop exit code: {result.returncode}")
    logger.info(f"bluetooth stop output: {result.stdout}")

    # process kill is needed, since the service/bluetooth driver is probably freezed
    result = subprocess.run(["pkill", "-f", "bluetoothd"], capture_output=True, text=True)
    logger.info(f"pkill exit code: {result.returncode}")
    logger.info(f"pkill output: {result.stdout}")

    # rfkill block bluetooth
    result = subprocess.run(["rfkill", "block", "bluetooth"], capture_output=True, text=True)
    logger.info(f"rfkill block exit code: {result.returncode}")
    logger.info(f"rfkill block output: {result.stdout}")

    # kill hdciattach
    result = subprocess.run(["pkill", "-f", "hciattach"], capture_output=True, text=True)
    logger.info(f"pkill hciattach exit code: {result.returncode}")
    logger.info(f"pkill hciattach output: {result.stdout}")
    sleep(0.5)

    # kill hci_uart
    result = subprocess.run(["rmmod", "hci_uart"], capture_output=True, text=True)
    logger.info(f"rmmod hci_uart exit code: {result.returncode}")
    logger.info(f"rmmod hci_uart output: {result.stdout}")

    # kill btbcm
    result = subprocess.run(["rmmod", "btbcm"], capture_output=True, text=True)
    logger.info(f"rmmod btbcm exit code: {result.returncode}")
    logger.info(f"rmmod btbcm output: {result.stdout}")

    # load hci_uart
    result = subprocess.run(["modprobe", "hci_uart"], capture_output=True, text=True)
    logger.info(f"modprobe hci_uart exit code: {result.returncode}")
    logger.info(f"modprobe hci_uart output: {result.stdout}")

    # load btbcm
    result = subprocess.run(["modprobe", "btbcm"], capture_output=True, text=True)
    logger.info(f"modprobe btbcm exit code: {result.returncode}")
    logger.info(f"modprobe btbcm output: {result.stdout}")

    sleep(2)

    result = subprocess.run(["rfkill", "unblock", "bluetooth"], capture_output=True, text=True)
    logger.info(f"rfkill unblock exit code: {result.returncode}")
    logger.info(f"rfkill unblock output: {result.stdout}")

    result = subprocess.run(["/etc/init.d/bluetooth", "start"], capture_output=True, text=True)
    logger.info(f"bluetooth start exit code: {result.returncode}")
    logger.info(f"bluetooth start output: {result.stdout}")

    logger.info("System Bluetooth daemon should have been restarted")
    logger.info("Exit driver for clean restart")

    sys.exit(1)
