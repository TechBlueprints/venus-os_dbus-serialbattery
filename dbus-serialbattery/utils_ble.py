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
        self._first_connect = True  # hcitool cleanup only on first connect

        # Start a new thread that will run bleak the async bluetooth LE library
        self.main_thread = threading.current_thread()
        ble_async_thread = threading.Thread(name="BMS_bluetooth_async_thread", target=self.initiate_ble_thread_main, daemon=True)
        ble_async_thread.start()

        thread_start_ok = self.ble_async_thread_ready.wait(2)
        connected_ok = self.ble_connection_ready.wait(60)
        if not thread_start_ok:
            logger.error("bluetooh LE thread took to long to start")
        if not connected_ok:
            logger.error(f"bluetooh LE connection to address: {self.address} took to long to inititate")
            self.connected = False
        else:
            # Mark connected only if client exists and is connected
            try:
                self.connected = bool(self.client and self.client.is_connected)
            except Exception:
                self.connected = False

    def initiate_ble_thread_main(self):
        asyncio.run(self.async_main(self.address))

    async def async_main(self, address):
        self.ble_async_thread_event_loop = asyncio.get_event_loop()
        self.ble_async_thread_ready.set()

        # try to connect over and over if the connection fails
        consecutive_failures = 0
        while self.main_thread.is_alive():
            result = await self.connect_to_bms(self.address)
            if result is False:
                consecutive_failures += 1
                # exponential backoff: 1s, 2s, 4s, 8s, 15s, 15s, ... (cap at 15s)
                delay = min(1.0 * (2 ** (consecutive_failures - 1)), 15.0)
                logger.info(f"BLE: connect failed ({consecutive_failures} in a row), retrying in {delay:.0f}s")
                try:
                    await asyncio.sleep(delay)
                except Exception:
                    pass
            else:
                # reset failure counter after any successful session (even if later disconnected)
                consecutive_failures = 0
                # After a connection session ends (device disconnected), wait before
                # reconnecting.  This gives BlueZ time to clean up the old handle and
                # avoids confusing the BMS with rapid reconnect cycles.
                logger.info(f"BLE: connection session ended for {address}, waiting 3s before reconnect")
                await asyncio.sleep(3.0)

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
                adapters_to_rotate = _list_adapters() or [None]
        else:
            # rely on BlueZ default unless configured otherwise
            adapters_to_rotate = [None]

        # Acquire a cross-process file lock to serialize BLE scanning/connecting.
        # This prevents BlueZ "InProgress" errors when multiple battery services
        # attempt to scan simultaneously via bleak's implicit discover().
        lock_fd = None
        try:
            lock_fd = open(_BLE_CONNECT_LOCK_PATH, "w")
            logger.info(f"BLE connect lock: waiting to acquire for {address}")
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            logger.info(f"BLE connect lock: acquired for {address}")
        except Exception as e:
            logger.warning(f"BLE connect lock: could not acquire ({repr(e)}), proceeding without lock")

        try:
            # Drop any stale BlueZ connection to this address left over from a
            # previous process that was killed (e.g. by runit `svc -t`).  Without
            # this, the BMS device refuses a new connection because BlueZ still
            # holds the old handle, and all connect attempts time out.
            # Only do this on the FIRST connect — on auto-reconnects after a normal
            # disconnect, BlueZ cleans up itself and this would interfere.
            if self._first_connect:
                for _adapter in (adapters_to_rotate if adapters_to_rotate != [None] else []):
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

            connected = False
            # Wall-clock deadline: the main thread waits 60s for ble_connection_ready.
            # We MUST finish (success or failure) before that, otherwise the process
            # exits while this daemon thread is still mid-connection, leaving BlueZ
            # with a stale half-open handle that causes InProgress errors on restart.
            # Long timeout needed because dbus-ble-sensors scans on all adapters,
            # causing persistent InProgress errors; we need to outlast scan cycles.
            deadline = _time.time() + 55.0  # 5s margin before 60s main-thread timeout
            max_retries = 15
            for attempt in range(1, max_retries + 1):
                remaining = deadline - _time.time()
                if remaining <= 1.0:
                    logger.warning("BLE connect: wall-clock deadline reached, no time for more attempts")
                    break
                adapter = adapters_to_rotate[(attempt - 1) % len(adapters_to_rotate)]
                adapter_name = adapter or "default"
                self.client = BleakClient(address, disconnected_callback=self.client_disconnected, adapter=adapter) if adapter else BleakClient(address, disconnected_callback=self.client_disconnected)
                # Per-attempt timeout: shrink to fit remaining budget (leave 1s for cleanup)
                attempt_timeout = min(8.0, remaining - 1.0)
                if attempt_timeout < 1.0:
                    logger.warning("BLE connect: insufficient time remaining, giving up")
                    break
                try:
                    await asyncio.wait_for(self.client.connect(), timeout=attempt_timeout)
                    # Verify GATT service discovery completed — without this,
                    # every write/notify call fails with "Service Discovery has
                    # not been performed yet".  Some Bleak/BlueZ combos return
                    # from connect() before services are resolved.
                    if not self.client.services:
                        logger.warning(f"BLE connect on {adapter_name}: GATT services empty after connect, resolving explicitly...")
                        svcs = await asyncio.wait_for(self.client.get_services(), timeout=10.0)
                        if not svcs:
                            raise Exception("GATT service discovery returned no services")
                        logger.info(f"BLE connect on {adapter_name}: resolved GATT services OK")
                    await self.client.start_notify(self.read_characteristic, self.notify_read_callback)
                    await asyncio.sleep(0.2)
                    connected = True
                    logger.info(f"BLE connected successfully on {adapter_name}")
                    break
                except asyncio.TimeoutError:
                    logger.warning(f"BLE connect attempt {attempt}/{max_retries} on {adapter_name} timed out after {attempt_timeout:.0f}s")
                    try:
                        await self.client.disconnect()
                    except Exception:
                        pass
                    await asyncio.sleep(0.3)
                    continue
                except Exception as e:
                    error_str = repr(e)
                    logger.error(f"Failed to connect on {adapter_name} (attempt {attempt}/{max_retries}): {error_str}")
                    try:
                        await self.client.disconnect()
                    except Exception:
                        pass
                    if "InProgress" in error_str and attempt < max_retries:
                        delay = min(1.0 + attempt * 0.5, 5.0)
                        logger.info(f"BLE adapter {adapter_name} busy (InProgress), retrying in {delay:.1f}s (attempt {attempt}/{max_retries})...")
                        await asyncio.sleep(delay)
                        continue
                    if attempt < max_retries:
                        await asyncio.sleep(0.3)
                        continue
                    break
            if not connected:
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
            self._first_connect = False  # subsequent reconnects skip hcitool cleanup
            while self.client.is_connected and self.main_thread.is_alive():
                await asyncio.sleep(0.1)
        finally:
            try:
                await self.client.disconnect()
            except Exception:
                pass
            self.connected = False

    # saves response and tells the command sender that the response has arived
    def notify_read_callback(self, sender, data: bytearray):
        logger.debug(f"BLE RX notification: {len(data)} bytes from {sender}")
        # Append to queue to avoid races and packet drops
        try:
            self._notification_queue.append(bytes(data))
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
        logger.debug(f"BLE TX: {len(command)} bytes -> {self.write_characteristic}")
        await self.client.write_gatt_char(self.write_characteristic, command, True)
        await asyncio.wait_for(self.response_event.wait(), timeout=1)  # Wait for the response notification
        self.response_event = False
        return self.response_data

    def send_data(self, data):
        """Send a command and wait for the BMS response.

        Uses run_coroutine_threadsafe + Future.result() instead of asyncio.run()
        to avoid blocking the GLib main loop during asyncio cleanup when the
        daemon thread's write_gatt_char hangs on a stale connection.
        """
        try:
            future = asyncio.run_coroutine_threadsafe(
                self.ble_thread_send_com(data),
                self.ble_async_thread_event_loop,
            )
            return future.result(timeout=2.0)
        except Exception:
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
