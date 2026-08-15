import threading
import asyncio
import subprocess
import sys
import time
from bleak import BleakClient
from time import sleep
from utils import (
    logger,
    BLUETOOTH_CONNECTION_BACKEND,
    BLUETOOTH_FORCE_RESET_BLE_STACK,
    capture_raw_data,
)


# Outer deadlines for the connection backend. Generous on purpose: they are a
# last resort against a permanently parked await, not a connection timeout.
BLE_ESTABLISH_TIMEOUT = 300.0
BLE_RELEASE_TIMEOUT = 30.0


class BleConnectionBackend:
    """
    Interface for establishing and releasing BLE connections.

    Separates how a connection is established and torn down (the backend) from
    how Syncron_Ble supervises it and exchanges data with the BMS drivers, so
    alternative connection strategies can be plugged in without touching the
    drivers.
    """

    def create_client(self, address, disconnected_callback):
        """
        Create the BleakClient for the given address, or None if the backend
        creates its own client during establish().
        """
        raise NotImplementedError

    async def establish(self, client, address, notify_char, notify_callback):
        """
        Connect and start notifications. Returns the connected client
        (may differ from the one passed in). Raises on failure.
        """
        raise NotImplementedError

    async def release(self, client):
        """Disconnect the client."""
        raise NotImplementedError


class BleakBackend(BleConnectionBackend):
    """
    Default backend: connects directly with bleak, matching the historical
    behavior of this driver.
    """

    def create_client(self, address, disconnected_callback):
        return BleakClient(address, disconnected_callback=disconnected_callback)

    async def establish(self, client, address, notify_char, notify_callback):
        logger.info("initiating BLE connection to: " + address)
        await client.connect()
        logger.info("connected to bluetooh device" + address)
        await client.start_notify(notify_char, notify_callback)
        return client

    async def release(self, client):
        await client.disconnect()


# Available connection backends, selected by class name via BLUETOOTH_CONNECTION_BACKEND
supported_ble_backends = [BleakBackend]


def get_ble_backend(name=None):
    """Return the connection backend selected by BLUETOOTH_CONNECTION_BACKEND."""
    name = BLUETOOTH_CONNECTION_BACKEND if name is None else name
    for backend in supported_ble_backends:
        if backend.__name__ == name:
            return backend()
    logger.warning(f"Unknown BLUETOOTH_CONNECTION_BACKEND '{name}', using 'BleakBackend'")
    return BleakBackend()


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
        self.backend = get_ble_backend()
        # Only the BLE thread of the current generation keeps running; see
        # rebuild_ble_thread()
        self._ble_thread_generation = 0

        # Start a new thread that will run bleak the async bluetooth LE library
        self.main_thread = threading.current_thread()
        ble_async_thread = threading.Thread(name="BMS_bluetooth_async_thread", target=self.initiate_ble_thread_main, daemon=True)
        ble_async_thread.start()

        thread_start_ok = self.ble_async_thread_ready.wait(2)
        connected_ok = self.ble_connection_ready.wait(10)
        if not thread_start_ok:
            logger.error("bluetooh LE thread took to long to start")
        if not connected_ok:
            logger.error(f"bluetooh LE connection to address: {self.address} took to long to inititate")
        else:
            self.connected = True

    def initiate_ble_thread_main(self, generation=0):
        asyncio.run(self.async_main(self.address, generation))

    def rebuild_ble_thread(self):
        """Abandon a wedged BLE thread and start a fresh one in-process.

        The remedy for a deadlocked reconnect loop used to be exiting the
        whole process, which takes the dbus service and the in-RAM state
        down with it and makes the inverter raise 'BMS connection lost'.
        Rebuilding just the BLE thread keeps everything the rest of the
        system depends on alive. The old thread, if merely slow rather than
        hung, exits at its next loop iteration via the generation check; a
        truly hung one is abandoned (it is a daemon thread).
        """
        try:
            self._ble_thread_generation += 1
            generation = self._ble_thread_generation
            self.ble_async_thread_ready = threading.Event()
            self.ble_connection_ready = threading.Event()
            self.ble_async_thread_event_loop = False
            self.connected = False
            self.backend = get_ble_backend()  # fresh backend state
            ble_async_thread = threading.Thread(
                name=f"BMS_bluetooth_async_thread_gen{generation}",
                target=self.initiate_ble_thread_main,
                args=(generation,),
                daemon=True,
            )
            ble_async_thread.start()
            started = self.ble_async_thread_ready.wait(5)
            logger.error(f"BLE thread rebuild for {self.address}: generation {generation} {'started' if started else 'FAILED TO START'}")
            return started
        except Exception as e:
            logger.error(f"BLE thread rebuild for {self.address} failed: {repr(e)}")
            return False

    async def async_main(self, address, generation=0):
        self.ble_async_thread_event_loop = asyncio.get_event_loop()
        self.ble_async_thread_ready.set()

        # Space out connection attempts: 1s, 3s, then steady 6s. The first
        # retry stays instant-ish for ordinary blips; the 6s cruise stops
        # the continuous hammering that produced 220-attempt recovery
        # storms and wedged adapter discovery state. A session that held
        # for over a minute resets the ramp.
        backoff = [1, 3, 6]
        failures = 0
        while self.main_thread.is_alive() and generation == self._ble_thread_generation:
            attempt_started = time.time()
            await self.connect_to_bms(self.address)
            if time.time() - attempt_started > 60.0:
                failures = 0
            else:
                failures = min(failures + 1, len(backoff) - 1)
            await asyncio.sleep(backoff[failures])

    def client_disconnected(self, client):
        logger.error(f"bluetooh device with address: {self.address} disconnected")

    async def connect_to_bms(self, address):
        self.client = self.backend.create_client(address, self.client_disconnected)
        try:
            # Belt-and-braces deadline: a backend's own timeouts should always
            # fire first, but no single stalled await may park the reconnect
            # loop permanently. One unguarded D-Bus await once silenced
            # reconnection for four hours without a single log line.
            self.client = await asyncio.wait_for(
                self.backend.establish(self.client, address, self.read_characteristic, self.notify_read_callback),
                timeout=BLE_ESTABLISH_TIMEOUT,
            )

        except Exception as e:
            logger.error("Failed when trying to connect", e)
            return False
        finally:
            self.ble_connection_ready.set()
            if self.client:
                while self.client.is_connected and self.main_thread.is_alive():
                    await asyncio.sleep(0.1)
                try:
                    await asyncio.wait_for(self.backend.release(self.client), timeout=BLE_RELEASE_TIMEOUT)
                except Exception as e:
                    # a disconnect that never completes must not prevent the
                    # next connection attempt
                    logger.warning(f"BLE [{address}] disconnect did not complete: {repr(e)}")

    # saves response and tells the command sender that the response has arived
    def notify_read_callback(self, sender, data: bytearray):
        capture_raw_data(self.address, "rx", data)
        self.response_data = data
        self.response_event.set()

    async def ble_thread_send_com(self, command):
        self.response_event = asyncio.Event()
        self.response_data = False
        capture_raw_data(self.address, "tx", command)
        await self.client.write_gatt_char(self.write_characteristic, command, True)
        await asyncio.wait_for(self.response_event.wait(), timeout=1)  # Wait for the response notification
        self.response_event = False
        return self.response_data

    def send_data(self, data):
        # Schedule the write on the BLE thread's existing event loop and wait
        # for the result directly. The previous implementation wrapped this in
        # asyncio.run(), constructing and tearing down a whole event loop for
        # every command sent — measurable CPU overhead on GX hardware for
        # drivers that poll several commands every few seconds.
        future = asyncio.run_coroutine_threadsafe(self.ble_thread_send_com(data), self.ble_async_thread_event_loop)
        try:
            return future.result(timeout=1.5)
        except Exception:
            future.cancel()
            raise


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
