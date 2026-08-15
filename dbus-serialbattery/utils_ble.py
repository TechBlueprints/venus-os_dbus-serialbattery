import threading
import asyncio
import os
import time
import subprocess
import sys
from bleak import BleakClient
from bleak.exc import BleakCharacteristicNotFoundError
from time import sleep
from utils import (
    logger,
    BLUETOOTH_ADAPTERS,
    BLUETOOTH_CONNECTION_BACKEND,
    BLUETOOTH_FORCE_RESET_BLE_STACK,
    BLUETOOTH_SUPERVISION_TIMEOUT,
    capture_raw_data,
)


def _parse_adapter_pins():
    """
    BLUETOOTH_ADAPTERS entries of the form MAC@hciX pin that device to exactly
    that adapter (no rotation, no fallback to other adapters); plain hciX
    entries form the shared pool for unpinned devices. Returns (pins, pool).
    """
    pins = {}
    pool = []
    for entry in BLUETOOTH_ADAPTERS:
        if "@" in entry:
            mac, _, adapter = entry.rpartition("@")
            if mac and adapter:
                pins[mac.strip().upper()] = adapter.strip()
        else:
            pool.append(entry)
    return pins, pool


BLUETOOTH_ADAPTER_PINS, BLUETOOTH_ADAPTER_POOL = _parse_adapter_pins()


def adapters_for(address):
    """Pinned adapter (as a single-element list) for this device, or None."""
    pin = BLUETOOTH_ADAPTER_PINS.get(str(address).strip().upper())
    return [pin] if pin else None


def apply_supervision_timeout(address, adapters=None):
    """
    Raise the LE link supervision timeout of a just-established connection via
    an HCI connection parameter update (central-initiated, no peripheral
    consent needed). The kernel builds every LE Create Connection with its
    420 ms default on this platform - even with the hci debugfs override set -
    and 420 ms means any sub-second radio noise burst kills the link. A longer
    deadline lets the link ride bursts out; a genuinely absent BMS is still
    detected by the driver's own polling. No-op when
    BLUETOOTH_SUPERVISION_TIMEOUT is 0.
    """
    if not BLUETOOTH_SUPERVISION_TIMEOUT:
        return
    mac = str(address).upper()
    for adapter in adapters or BLUETOOTH_ADAPTERS or ["hci0"]:
        try:
            con = subprocess.run(["hcitool", "-i", adapter, "con"], capture_output=True, text=True, timeout=5)
            for line in con.stdout.splitlines():
                if mac in line.upper() and " handle " in line:
                    handle = line.split(" handle ")[1].split()[0]
                    upd = subprocess.run(
                        ["hcitool", "-i", adapter, "lecup", "--handle", handle, "--min", "24", "--max", "40", "--latency", "0", "--timeout", str(BLUETOOTH_SUPERVISION_TIMEOUT)],
                        capture_output=True,
                        text=True,
                        timeout=5,
                    )
                    if upd.returncode == 0:
                        logger.info(f"BLE [{address}] supervision timeout set to {BLUETOOTH_SUPERVISION_TIMEOUT * 10} ms (handle {handle} on {adapter})")
                    else:
                        logger.warning(f"BLE [{address}] supervision timeout update failed on {adapter}: {upd.stderr.strip() or upd.stdout.strip()}")
                    return
        except Exception as e:
            logger.warning(f"BLE [{address}] supervision timeout update errored on {adapter}: {repr(e)}")
    logger.warning(f"BLE [{address}] supervision timeout update skipped: no live handle found")


class BleConnectionBackend:
    """
    Interface for establishing and releasing BLE connections.

    Separates how a connection is established/torn down (the backend) from how
    Syncron_Ble supervises it and exchanges data with the BMS drivers. This allows
    alternative connection strategies to be plugged in without touching the drivers.
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
    adapters, rotating to the next entry after a failed attempt. An empty
    list uses the system default adapter.
    """

    def __init__(self):
        self.adapter_index = 0
        self.current_adapter = None

    def create_client(self, address, disconnected_callback):
        kwargs = {}
        pinned = adapters_for(address)
        if pinned:
            self.current_adapter = pinned[0]
            kwargs["adapter"] = self.current_adapter
        elif BLUETOOTH_ADAPTER_POOL:
            self.current_adapter = BLUETOOTH_ADAPTER_POOL[self.adapter_index % len(BLUETOOTH_ADAPTER_POOL)]
            kwargs["adapter"] = self.current_adapter
        return BleakClient(address, disconnected_callback=disconnected_callback, **kwargs)

    async def establish(self, client, address, notify_char, notify_callback):
        logger.info("initiating BLE connection to: " + address + (f" (adapter {self.current_adapter})" if self.current_adapter else ""))
        try:
            await client.connect()
            logger.info("connected to bluetooh device" + address)
            # On some devices GATT characteristics become available only after connect()
            # has already returned, so the first start_notify() can raise
            # BleakCharacteristicNotFoundError for a characteristic that does exist.
            # Re-run service discovery and retry before giving up.
            for attempt in range(3):
                try:
                    await client.start_notify(notify_char, notify_callback)
                    break
                except BleakCharacteristicNotFoundError:
                    if attempt == 2:
                        raise
                    logger.warning(f"characteristic {notify_char} not found yet, re-running service discovery")
                    # bleak has no public API to re-run service discovery on a connected
                    # client; clear the cached services so _get_services() fetches again
                    client._backend.services = None
                    await client._backend._get_services()
                    await asyncio.sleep(0.5)
            adapters = [self.current_adapter] if self.current_adapter else None
            await asyncio.get_event_loop().run_in_executor(None, apply_supervision_timeout, address, adapters)
        except Exception:
            # rotate to the next configured adapter for the next attempt
            if BLUETOOTH_ADAPTERS:
                self.adapter_index += 1
            raise
        return client

    async def release(self, client):
        await client.disconnect()


try:
    from bleak_connection_manager import (
        PROFILE_BATTERY,
        EscalationPolicy,
        establish_connection,
    )
    from bleak_connection_manager.adapters import discover_adapters
    from bleak_connection_manager.scanner import find_device as bcm_find_device

    _HAS_BCM = True
except Exception as e:  # pragma: no cover - import guard
    _HAS_BCM = False
    _BCM_IMPORT_ERROR = e


class BCMBackend(BleConnectionBackend):
    """
    bleak-connection-manager backend: managed connection lifecycle for hosts
    where the direct bleak path is unreliable (GX devices with several BLE
    services fighting over adapters).

    Adds over BleakBackend:
      - cache-first device resolution (no StartDiscovery when the device is
        already in the BlueZ cache; scan-lock coordination when it isn't)
      - connect retries with per-adapter failure tracking and escalation
        (PROFILE_BATTERY policy)
      - adapter selection from BLUETOOTH_ADAPTERS, or auto-discovery of all
        adapters when unset
      - phantom-connection cleanup before attempts
    """

    def __init__(self):
        if not _HAS_BCM:
            logger.error(f"BCMBackend selected but bleak_connection_manager is not importable: {_BCM_IMPORT_ERROR}")
            raise ImportError(_BCM_IMPORT_ERROR)
        # Half-connect oscillation breaker: ConnectDevice can succeed at the
        # HCI level while the subsequent establish fails ("device
        # disappeared"), and each attempt re-occupies the peripheral so it
        # never advertises long enough for scan-based resolution to repair
        # the handoff. Observed three times in production, wedging a battery
        # for 10-26 minutes each time. After 3 consecutive failures, skip
        # the ConnectDevice-first path, pause so the device can advertise,
        # and resolve by scan instead.
        self._handoff_fails = 0

    def _adapters(self, address=None):
        pinned = adapters_for(address) if address else None
        if pinned:
            # hard pin: this device may use exactly this adapter, nothing else
            return pinned
        adapters = list(BLUETOOTH_ADAPTER_POOL) if BLUETOOTH_ADAPTER_POOL else None
        if not adapters:
            try:
                adapters = discover_adapters()
            except Exception as e:
                logger.warning(f"BLE: adapter discovery failed ({e}), using system default")
                return None
        # Deterministic per-device spread: rotate each device's preference
        # order by a stable hash of its address so multiple BMS instances
        # distribute across the allowed adapters instead of all competing
        # for (and sharing the fate of) the first one. Preference order
        # only — every allowed adapter is still tried on failure.
        if address and adapters and len(adapters) > 1:
            offset = sum(ord(c) for c in str(address)) % len(adapters)
            adapters = adapters[offset:] + adapters[:offset]
        return adapters

    def create_client(self, address, disconnected_callback):
        # BCM creates and returns its own client during establish()
        self._disconnected_callback = disconnected_callback
        return None

    async def _connect_device_no_scan(self, address, adapter):
        """Create + connect the device object on a specific adapter via
        BlueZ's experimental Adapter1.ConnectDevice (bluetoothd -E).

        Returns the device object path, or None on failure.  Unlike a
        discovery scan, this does not need the adapter's scan slot, so it
        works while other services hold scanning on the adapter.
        """
        try:
            from dbus_fast import Message, Variant
            from dbus_fast.aio import MessageBus
            from dbus_fast.constants import BusType, MessageType

            # the bus connect itself must be guarded too: any unguarded await
            # in this path can park the BLE daemon coroutine forever if D-Bus
            # stalls (observed in production: reconnect loop silent for hours
            # while the fallback sensor made the outage invisible)
            bus = await asyncio.wait_for(MessageBus(bus_type=BusType.SYSTEM).connect(), timeout=10.0)
            try:
                reply = await asyncio.wait_for(
                    bus.call(
                        Message(
                            destination="org.bluez",
                            path=f"/org/bluez/{adapter}",
                            interface="org.bluez.Adapter1",
                            member="ConnectDevice",
                            signature="a{sv}",
                            body=[{"Address": Variant("s", address), "AddressType": Variant("s", "public")}],
                        )
                    ),
                    timeout=30.0,
                )
                if reply.message_type == MessageType.METHOD_RETURN:
                    return reply.body[0] if reply.body else f"/org/bluez/{adapter}/dev_" + address.replace(":", "_").upper()
                error_name = getattr(reply, "error_name", "")
                if "AlreadyExists" in error_name:
                    # device object already present on this adapter — usable
                    return f"/org/bluez/{adapter}/dev_" + address.replace(":", "_").upper()
                logger.debug(f"BLE [{address}] ConnectDevice on {adapter} failed: {error_name}")
                return None
            finally:
                bus.disconnect()
        except Exception as e:
            logger.debug(f"BLE [{address}] ConnectDevice on {adapter} error: {repr(e)}")
            return None

    async def establish(self, client, address, notify_char, notify_callback):
        adapters = self._adapters(address)
        escalation = EscalationPolicy(adapters or [], config=PROFILE_BATTERY)

        # Resolution order:
        #   1. ConnectDevice on the PREFERRED adapter (no cache, no scan
        #      slot needed). This makes the per-device adapter preference
        #      actually win: cache-first resolution otherwise pins the
        #      device to whichever adapter last scanned it (observed: a
        #      sibling instance's scans re-cache the device on the shared
        #      adapter within seconds, defeating any migration).
        #   2. Cache-first find across allowed adapters.
        #   3. ConnectDevice on the remaining allowed adapters.
        # BLUETOOTH_ADAPTERS remains a hard allow-list throughout.
        device = None
        connect_adapters = adapters
        if self._handoff_fails >= 3:
            logger.warning(f"BLE [{address}] {self._handoff_fails} consecutive half-connects — pausing 10s, then scan-resolving")
            await asyncio.sleep(10.0)
        if adapters and self._handoff_fails < 3:
            path = await self._connect_device_no_scan(address, adapters[0])
            if path:
                from bleak.backends.device import BLEDevice

                logger.info(f"BLE [{address}] connected on preferred adapter {adapters[0]} via ConnectDevice")
                device = BLEDevice(address=address, name=None, details={"path": path, "props": {}})
                connect_adapters = [adapters[0]]
        try:
            if device is None:
                device = await bcm_find_device(address, timeout=15.0, max_attempts=2, adapters=adapters)
        except Exception as e:
            logger.warning(f"BLE [{address}] managed scan failed: {repr(e)}")

        if device is not None:
            # Pin the connection to the adapter holding the device object.
            # The cache-first resolution can return an object cached on a
            # NON-allowed adapter (e.g. left over from an earlier connection
            # there) — reject and remove those so they stop shadowing the
            # allowed adapters, and fall through to ConnectDevice instead.
            try:
                found_adapter = device.details["path"].split("/")[3]
                if adapters and found_adapter not in adapters:
                    logger.info(f"BLE [{address}] cached on disallowed {found_adapter} — removing and using ConnectDevice on allowed adapters")
                    try:
                        from bleak_connection_manager.bluez import remove_device

                        # timeout-guarded: an unguarded remove_device await parked
                        # the reconnect loop for 4 hours in production when its
                        # D-Bus reply never arrived
                        await asyncio.wait_for(remove_device(address, found_adapter), timeout=15.0)
                    except Exception:
                        pass
                    device = None
                else:
                    connect_adapters = [found_adapter]
            except Exception:
                pass
        if device is None:
            for adapter in adapters or ["hci0"]:
                path = await self._connect_device_no_scan(address, adapter)
                if path:
                    from bleak.backends.device import BLEDevice

                    logger.info(f"BLE [{address}] created device on {adapter} via ConnectDevice (no scan)")
                    device = BLEDevice(address=address, name=None, details={"path": path, "props": {}})
                    connect_adapters = [adapter]
                    break
            if device is None:
                self._handoff_fails += 1
                raise Exception(f"device not resolvable on allowed adapters {adapters} (scan + ConnectDevice both failed)")

        try:
            client = await establish_connection(
            BleakClient,
            device,
            f"serialbattery {address}",
            disconnected_callback=self._disconnected_callback,
            max_attempts=5,
            adapters=connect_adapters,
            close_inactive_connections=True,
            escalation_policy=escalation,
                overall_timeout=240.0,
                timeout=15.0,
            )
        except Exception:
            self._handoff_fails += 1
            raise
        logger.info(f"BLE [{address}] connected via BCM")

        try:
            await asyncio.wait_for(client.start_notify(notify_char, notify_callback), timeout=10.0)
        except Exception as e:
            logger.warning(f"BLE [{address}] start_notify failed: {repr(e)}")
            # A stale BlueZ cache entry can produce a client that claims to
            # be connected but has no live link. Clear it so the next
            # attempt performs a fresh connect.
            if "Not connected" in str(e):
                try:
                    from bleak_connection_manager.bluez import remove_device

                    for adap in adapters or ["hci0"]:
                        await remove_device(address, adap)
                    logger.info(f"BLE [{address}] cleared stale BlueZ cache entry")
                except Exception:
                    pass
            try:
                await client.disconnect()
            except Exception:
                pass
            self._handoff_fails += 1
            raise
        await asyncio.get_event_loop().run_in_executor(None, apply_supervision_timeout, address, connect_adapters or adapters)
        self._handoff_fails = 0
        return client

    async def release(self, client):
        await client.disconnect()


# Available connection backends, selected by class name via BLUETOOTH_CONNECTION_BACKEND
supported_ble_backends = [BleakBackend, BCMBackend]


def get_ble_backend():
    """Return the connection backend selected by BLUETOOTH_CONNECTION_BACKEND."""
    for backend in supported_ble_backends:
        if backend.__name__ == BLUETOOTH_CONNECTION_BACKEND:
            return backend()
    logger.warning(f"Unknown BLUETOOTH_CONNECTION_BACKEND '{BLUETOOTH_CONNECTION_BACKEND}', using 'BleakBackend'")
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

    def initiate_ble_thread_main(self):
        asyncio.run(self.async_main(self.address))

    async def async_main(self, address):
        self.ble_async_thread_event_loop = asyncio.get_event_loop()
        self.ble_async_thread_ready.set()

        # Space out connection attempts: 1s, 3s, then steady 6s. The first
        # retry stays instant-ish for ordinary blips; the 6s cruise stops
        # the continuous hammering that produced 220-attempt recovery
        # storms and wedged adapter discovery state. A session that held
        # for over a minute resets the ramp.
        backoff = [1, 3, 6]
        failures = 0
        # Operator hold: while this flag file exists, make no connection
        # attempts at all but keep the process (and its dbus service +
        # fallback serving) alive. Gives a degraded BMS radio extended
        # quiet without the bank-wide alarms that killing the driver
        # process causes (DVCC sees the service vanish).
        hold_flag = "/data/tmp/ble-hold-" + self.address.replace(":", "").lower()
        while self.main_thread.is_alive():
            if os.path.exists(hold_flag):
                # keep the liveness heartbeat advancing: a hold is deliberate
                # quiet, not parked machinery - without this the supervision
                # layer restarts the driver 10 minutes into every hold
                self.last_connect_cycle = time.time()
                await asyncio.sleep(5)
                continue
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
        # reconnect-liveness heartbeat, see supervision check in dbushelper
        self.last_connect_cycle = time.time()
        self.client = self.backend.create_client(address, self.client_disconnected)
        try:
            # outer belt-and-braces timeout: the backend's internal timeouts
            # should always fire first, but no single stalled await may ever
            # park the reconnect daemon loop permanently (observed in
            # production: one unguarded D-Bus await silenced reconnection for
            # hours while the fallback sensor made the outage invisible)
            self.client = await asyncio.wait_for(
                self.backend.establish(self.client, address, self.read_characteristic, self.notify_read_callback), timeout=300.0
            )

        except Exception as e:
            logger.error("Failed when trying to connect", e)
            return False
        finally:
            self.ble_connection_ready.set()
            if self.client:
                while self.client.is_connected and self.main_thread.is_alive():
                    await asyncio.sleep(0.1)
                await self.backend.release(self.client)

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
