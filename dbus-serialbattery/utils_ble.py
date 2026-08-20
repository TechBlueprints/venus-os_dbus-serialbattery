import threading
import asyncio
import os
import subprocess
import sys
import time
from bleak import BleakClient, BleakScanner
from bleak.exc import BleakError
from time import sleep
from utils import (
    logger,
    BLUETOOTH_ADAPTERS,
    BLUETOOTH_CONNECTION_BACKEND,
    BLUETOOTH_FORCE_RESET_BLE_STACK,
    capture_raw_data,
)


def parse_adapter_entries(entries):
    """
    Split BLUETOOTH_ADAPTERS into pinned devices and the shared pool.

    Entries of the form MAC@hciX pin that device to that adapter, with no
    fallback to the shared pool. Repeating a MAC pins it to several adapters
    in order: the first is used for every connection attempt, the rest are
    tried only when it cannot be resolved there. That keeps a battery on a
    known good radio while leaving it somewhere to go if that radio fails,
    without returning it to a pool shared with other devices.

    Plain hciX entries form the pool used by every device that is not pinned.
    Returns (pins, pool), with pins keyed by upper case MAC address and each
    value a list of adapters in priority order.
    """
    pins = {}
    pool = []
    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue
        if "@" in entry:
            mac, _, adapter = entry.rpartition("@")
            mac = mac.strip().upper()
            adapter = adapter.strip()
            if mac and adapter:
                adapters = pins.setdefault(mac, [])
                if adapter not in adapters:
                    adapters.append(adapter)
            else:
                logger.warning(f"Ignoring malformed BLUETOOTH_ADAPTERS entry '{entry}'")
        else:
            pool.append(entry)
    return pins, pool


BLUETOOTH_ADAPTER_PINS, BLUETOOTH_ADAPTER_POOL = parse_adapter_entries(BLUETOOTH_ADAPTERS)


def adapters_for(address):
    """Adapters pinned to this device in priority order, or None if unpinned."""
    pins = BLUETOOTH_ADAPTER_PINS.get(str(address).strip().upper())
    return list(pins) if pins else None


def bluez_present_adapters():
    """
    Adapter names BlueZ currently exposes, or an empty set for "no answer".

    hciN names are not stable identities: a USB reset or reboot renumbers
    them, and an adapter a battery is configured for can stop existing while
    its number lives on pointing at different hardware. Selection therefore
    asks BlueZ what exists right now rather than trusting the config's names.

    A private connection, closed again before returning: dbus.SystemBus()
    hands out a cached shared connection, and creating that before the driver
    installs its main loop breaks every later signal receiver on it.
    """
    present = set()
    bus = None
    try:
        import dbus

        bus = dbus.SystemBus(private=True)
        manager = dbus.Interface(bus.get_object("org.bluez", "/"), "org.freedesktop.DBus.ObjectManager")
        for path, interfaces in manager.GetManagedObjects().items():
            parts = str(path).split("/")
            if len(parts) >= 4 and "org.bluez.Adapter1" in interfaces:
                present.add(parts[3])
    except Exception as e:
        logger.debug(f"BlueZ adapter state unavailable, using configured order: {repr(e)}")
        return set()
    finally:
        if bus is not None:
            try:
                bus.close()
            except Exception:
                pass
    return present


def adapters_in_attempt_order(address, present=None):
    """
    Adapters to try for this battery, best first.

    A battery uses its own configured adapters, or the shared pool if it has
    none; either list is walked by index, advancing only after a failed
    connection attempt. Adapters BlueZ does not currently expose are dropped:
    a battery whose radio was renumbered away by a USB reset must reach its
    next adapter rather than keep asking for a name that no longer resolves.
    If filtering would leave nothing, the configured list is returned
    unfiltered - refusing to attempt a connection is worse than trying an
    adapter that may not be there.
    """
    adapters = adapters_for(address) or list(BLUETOOTH_ADAPTER_POOL)
    if not adapters:
        return []
    if present is None:
        present = bluez_present_adapters()
    usable = [a for a in adapters if a in present] if present else list(adapters)
    return usable if usable else list(adapters)


# Hold flag: while the flag file for a device exists, the reconnect loop makes
# no connection attempts for that device at all, giving a degraded BMS radio
# extended quiet. The driver, its dbus service and its published data stay up,
# which is the whole point - killing the driver process instead makes DVCC see
# the service disappear and raises alarms across the bank.
# A flag whose content is "auto" was written by an automatic recovery path and
# expires by itself; any other content is an operator hold and persists until
# the file is removed.
BLE_HOLD_FLAG_DIR = "/data/tmp"
BLE_HOLD_FLAG_PREFIX = "ble-hold-"
BLE_HOLD_AUTO_EXPIRY = 1200.0
BLE_HOLD_POLL_INTERVAL = 5


def ble_hold_flag_path(address):
    """Path of the hold flag file for the given device address."""
    return os.path.join(BLE_HOLD_FLAG_DIR, BLE_HOLD_FLAG_PREFIX + str(address).replace(":", "").lower())


# Outer deadlines for the connection backend. Generous on purpose: they are a
# last resort against a permanently parked await, not a connection timeout.
BLE_ESTABLISH_TIMEOUT = 300.0
BLE_RELEASE_TIMEOUT = 30.0


# bleak-retry-connector lives in the ext folder, which dbus-serialbattery.py
# adds to sys.path; it is already vendored for the aiobmsble drivers, so this
# backend costs no new vendoring. Guard the import so utils_ble can be
# imported without it, and alias establish_connection: managed backends
# stacked on this branch import a function of the same name from their own
# library further down the module, and an unaliased import would be silently
# shadowed into calling the wrong one.
try:
    from bleak_retry_connector import (
        close_stale_connections,
        establish_connection as retry_establish_connection,
        get_device,
        get_device_by_adapter,
    )

    HAS_BLEAK_RETRY_CONNECTOR = True
except ImportError:
    HAS_BLEAK_RETRY_CONNECTOR = False


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

    If BLUETOOTH_ADAPTERS is set, connections are made only via the listed
    adapters. A device with its own MAC@hciX entries uses those and never the
    shared pool; every other device uses the pool. Either list is walked in
    order, moving on only after a failed attempt, so a dropped link reconnects
    on the adapter it was using, and adapters BlueZ does not currently expose
    are skipped. An empty list uses the system default adapter.
    """

    def __init__(self):
        self.adapter_index = 0
        self.current_adapter = None

    def _select_adapter(self, address):
        """
        Adapter for the next attempt, or None when nothing is configured.

        The order is recomputed from live BlueZ state each time, so an adapter
        that has gone away is accounted for without the driver having to
        remember anything. The index only advances when an attempt fails, so a
        dropped link reconnects on the adapter it was already using and only a
        failed connect moves on to the next one. It is never reset on success:
        once a battery is talking over an adapter there is no reason to
        re-probe a preferred one that may be gone, and the modulo brings the
        list round to it again if this one later fails.
        """
        adapters = adapters_in_attempt_order(address)
        if not adapters:
            return None
        return adapters[self.adapter_index % len(adapters)]

    def create_client(self, address, disconnected_callback):
        kwargs = {}
        self.current_adapter = self._select_adapter(address)
        if self.current_adapter:
            kwargs["adapter"] = self.current_adapter
        return BleakClient(address, disconnected_callback=disconnected_callback, **kwargs)

    async def establish(self, client, address, notify_char, notify_callback):
        try:
            return await self._establish(client, address, notify_char, notify_callback)
        except Exception:
            # a failed attempt, so the next one goes out on the next adapter
            self.adapter_index += 1
            raise

    async def _establish(self, client, address, notify_char, notify_callback):
        logger.info("initiating BLE connection to: " + address + (f" (adapter {self.current_adapter})" if self.current_adapter else ""))
        await client.connect()
        logger.info("connected to bluetooh device" + address)
        await client.start_notify(notify_char, notify_callback)
        return client

    async def release(self, client):
        await client.disconnect()


class BleakRetryBackend(BleConnectionBackend):
    """
    Backend based on bleak-retry-connector, which is vendored in the ext
    folder and also used by the aiobmsble drivers. establish_connection()
    retries with error-classified backoff and cleans up stale BlueZ state,
    and the device is resolved from the BlueZ cache first, scanning only when
    that misses - so a reconnect is a single cheap connect call rather than a
    scan, and an idle system runs no scanners at all.

    BLUETOOTH_ADAPTERS is honored the same way as in BleakBackend: a battery
    walks its own adapters, or the shared pool, moving on only after a failed
    attempt.
    """

    def __init__(self):
        self.adapter_index = 0
        self.current_adapter = None
        self.disconnected_callback = None

    def _select_adapter(self, address):
        """Same selection contract as BleakBackend: live order, failure-driven index."""
        adapters = adapters_in_attempt_order(address)
        if not adapters:
            return None
        return adapters[self.adapter_index % len(adapters)]

    def create_client(self, address, disconnected_callback):
        # establish_connection() creates the client itself
        self.disconnected_callback = disconnected_callback
        self.current_adapter = self._select_adapter(address)
        return None

    async def establish(self, client, address, notify_char, notify_callback):
        try:
            return await self._establish(client, address, notify_char, notify_callback)
        except Exception:
            # a failed attempt, so the next one goes out on the next adapter
            self.adapter_index += 1
            raise

    async def _establish(self, client, address, notify_char, notify_callback):
        logger.info("initiating BLE connection to: " + address + (f" (adapter {self.current_adapter})" if self.current_adapter else ""))
        device = await self._resolve_device(address)
        if device is None:
            raise BleakError(f"bluetooth device {address} not found" + (f" on adapter {self.current_adapter}" if self.current_adapter else ""))
        await close_stale_connections(device)
        kwargs = {"adapter": self.current_adapter} if self.current_adapter else {}
        client = await retry_establish_connection(BleakClient, device, address, disconnected_callback=self.disconnected_callback, **kwargs)
        logger.info("connected to bluetooth device " + address)
        await client.start_notify(notify_char, notify_callback)
        return client

    async def _resolve_device(self, address):
        """BLEDevice for the address from the BlueZ cache, scanning as fallback.

        With an adapter selected, both the cache lookup and the scan are bound
        to that adapter, so a battery can never resolve to a path on an
        adapter it is not configured for.
        """
        if self.current_adapter:
            device = await get_device_by_adapter(address, self.current_adapter)
        else:
            device = await get_device(address)
        if device is None:
            logger.info(f"bluetooth device {address} not in BlueZ cache, scanning")
            kwargs = {"adapter": self.current_adapter} if self.current_adapter else {}
            device = await BleakScanner.find_device_by_address(address, timeout=10.0, **kwargs)
        return device

    async def release(self, client):
        await client.disconnect()


# Available connection backends, selected by class name via BLUETOOTH_CONNECTION_BACKEND
supported_ble_backends = [BleakBackend]
if HAS_BLEAK_RETRY_CONNECTOR:
    supported_ble_backends.append(BleakRetryBackend)


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
        hold_flag = ble_hold_flag_path(self.address)
        holding = False
        while self.main_thread.is_alive() and generation == self._ble_thread_generation:
            if os.path.exists(hold_flag):
                try:
                    with open(hold_flag) as f:
                        automatic = f.read().strip() == "auto"
                    if automatic and time.time() - os.path.getmtime(hold_flag) > BLE_HOLD_AUTO_EXPIRY:
                        os.remove(hold_flag)
                        logger.info(f"BLE hold for {self.address} auto-expired, resuming connection attempts")
                        continue
                except Exception as e:
                    if not holding:
                        logger.warning(f"BLE hold flag {hold_flag} could not be read: {repr(e)}")
                if not holding:
                    holding = True
                    logger.warning(f"BLE hold flag {hold_flag} present, pausing connection attempts for {self.address}")
                await asyncio.sleep(BLE_HOLD_POLL_INTERVAL)
                continue
            if holding:
                holding = False
                logger.info(f"BLE hold for {self.address} released, resuming connection attempts")
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
