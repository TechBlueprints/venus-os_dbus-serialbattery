# -*- coding: utf-8 -*-

import asyncio
import atexit
import threading
import sys
import time
import json
import os
from typing import Optional, Union
import re
import subprocess
from struct import unpack_from

from utils import logger, USE_PORT_AS_UNIQUE_ID
import utils
from bms.lltjbd import LltJbdProtection, LltJbd, checksum


class LltJbd_FastBle(LltJbd):
    BATTERYTYPE = "LLT/JBD BLE (fast)"

    def __init__(self, port: Optional[str], baud: Optional[int], address: str):
        super(LltJbd_FastBle, self).__init__(port, -1, address)

        self.address = address
        self.protection = LltJbdProtection()
        self.type = self.BATTERYTYPE
        self.main_thread = threading.current_thread()
        self.run = True
        # Provide sane defaults so setup_vedbus can initialize paths immediately
        self.cell_count = 4
        try:
            logger.info("Init of LltJbd_FastBle at " + address)
        except Exception:
            pass

        # local debug file logging to bypass svlogd issues
        self._dbg_path = "/tmp/fastble.log"

        # runtime state
        self.bt_thread = threading.Thread(name="LltJbd_FastBle_Loop", target=self.background_loop, daemon=True)
        self.bt_loop: Optional[asyncio.AbstractEventLoop] = None
        self.response_queue: Optional[asyncio.Queue] = None
        self.ready_event: Optional[asyncio.Event] = None

        # BlueZ D-Bus GATT via python-dbus fallback
        self.dbus_bus = None
        self.dbus_dev_path: Optional[str] = None
        self.dbus_rx_char_path: Optional[str] = None
        self.dbus_tx_char_path: Optional[str] = None
        self._dbus_rx_queue: list[bytes] = []
        self._dbus_signal_attached = False
        self._glib_loop = None
        self._glib_thread = None
        # dbus-fast state
        self._fast_bus = None
        self._fast_signal_attached = False
        self._dbus_rx_extra_path: Optional[str] = None
        self._alt_rx_char_path: Optional[str] = None
        self._alt_tx_char_path: Optional[str] = None
        self._notif_started: bool = False
        self._prefer_alt_once: bool = True
        self._send_lock: Optional[asyncio.Lock] = None
        self._gen_ok: bool = False
        self._poll_count: int = 0
        self._rx_buffer = bytearray()
        self._assembled_frames = []
        # Gate publishing to D-Bus until we have verified frames on the wire
        self._frames_verified: int = 0
        # Relax initial publish gate to speed up UI population
        self._publish_gate_frames: int = 1
        self._last_good_time = 0.0
        # Bootstrap extra cell reads during first few polls
        self._bootstrap_polls_remaining: int = 6
        # Track consecutive general read failures to trigger app-like probes
        self._gen_fail_count: int = 0
        # App-like cadence: throttle general requests by time
        self._last_gen_send_ts: float = 0.0
        self._gen_period_s: float = 1.5
        # Alternate dd03 and dd04 strictly (single outstanding like app queue)
        self._next_poll_gen: bool = True
        # Single-family stickiness: prefer FF00; switch to FFE* only after repeated dd03 misses
        self._active_family: str = "ff"  # 'ff' or 'ffe'
        self._dd03_miss_count: int = 0
        self._dd03_switch_threshold: int = 1
        self._handshake_quiet_until: float = 0.0
        # BMS firmware bug workaround: detect repeated chunks
        self._last_rx_chunk: bytes = b''
        self._rx_chunk_repeat_count: int = 0
        self._force_incomplete_frame_extraction: bool = False
        # dd03 probing matrix (family x write type) with learned working mode
        self._dd03_modes_to_try = [("ff", "request"), ("ff", "command"), ("ffe", "request"), ("ffe", "command")]
        self._dd03_mode_idx: int = 0
        self._dd03_working_mode = None  # tuple[str family, str write_type]
        # Reconnection state
        self._consecutive_errors: int = 0
        self._last_reconnect_attempt: float = 0.0
        self._reconnect_cooldown: float = 30.0  # Wait 30s between reconnection attempts
        self._is_connected: bool = False
        self._last_successful_data_time: float = time.time()  # Track last successful data reception
        # State persistence for graceful disconnection handling
        self._state_file: str = f"/tmp/serialbattery_state_{self.address.replace(':', '_').lower()}.json"
        self._last_good_state: dict = {}
        self._last_state_save: float = 0.0
        self._state_save_interval: float = 5.0  # Save state every 5 seconds
        self._cached_data_max_age: float = 600.0  # 10 minutes
        self._load_cached_state()  # Load previous state on init

    def _load_cached_state(self):
        """Load previously saved battery state from disk"""
        try:
            if os.path.exists(self._state_file):
                with open(self._state_file, 'r') as f:
                    self._last_good_state = json.load(f)
                    save_time = self._last_good_state.get('timestamp', 0)
                    age = time.time() - save_time
                    if age < self._cached_data_max_age:
                        logger.info(f"Loaded cached state from {age:.0f}s ago")
                        # Restore key values
                        if 'voltage' in self._last_good_state:
                            self.voltage = self._last_good_state['voltage']
                        if 'current' in self._last_good_state:
                            self.current = self._last_good_state['current']
                        if 'soc' in self._last_good_state:
                            self.soc = self._last_good_state['soc']
                        if 'capacity' in self._last_good_state:
                            self.capacity = self._last_good_state['capacity']
                        if 'cells' in self._last_good_state and len(self._last_good_state['cells']) > 0:
                            # Restore cell voltages
                            for idx, cell_v in enumerate(self._last_good_state['cells']):
                                if idx < len(self.cells):
                                    self.cells[idx].voltage = cell_v
                    else:
                        logger.info(f"Cached state too old ({age:.0f}s), ignoring")
        except Exception as e:
            logger.debug(f"Could not load cached state: {e}")

    def _save_cached_state(self):
        """Save current battery state to disk for recovery after restart"""
        now = time.time()
        # Rate limit saves
        if now - self._last_state_save < self._state_save_interval:
            return
        
        try:
            state = {
                'timestamp': now,
                'voltage': self.voltage if self.voltage is not None else 0.0,
                'current': self.current if self.current is not None else 0.0,
                'soc': self.soc if self.soc is not None else 50.0,
                'capacity': self.capacity if self.capacity is not None else 0.0,
                'cells': [c.voltage for c in self.cells if c.voltage is not None],
            }
            with open(self._state_file, 'w') as f:
                json.dump(state, f)
            self._last_good_state = state
            self._last_state_save = now
        except Exception as e:
            logger.debug(f"Could not save state: {e}")

    def _use_cached_data(self) -> bool:
        """
        Return True if we should use cached data (disconnected but cache is fresh).
        Also restores cached values to self.
        """
        if not self._last_good_state or 'timestamp' not in self._last_good_state:
            return False
        
        age = time.time() - self._last_good_state.get('timestamp', 0)
        if age > self._cached_data_max_age:
            return False
        
        # Restore cached values
        try:
            if 'voltage' in self._last_good_state:
                self.voltage = self._last_good_state['voltage']
            if 'current' in self._last_good_state:
                self.current = self._last_good_state['current']
            if 'soc' in self._last_good_state:
                self.soc = self._last_good_state['soc']
            if 'capacity' in self._last_good_state:
                self.capacity = self._last_good_state['capacity']
            if 'cells' in self._last_good_state and len(self._last_good_state['cells']) > 0:
                for idx, cell_v in enumerate(self._last_good_state['cells']):
                    if idx < len(self.cells):
                        self.cells[idx].voltage = cell_v
        except Exception as e:
            logger.debug(f"Error restoring cached data: {e}")
        
        return True

    # Override validate_packet to accept both BLE (2-byte length) and UART (1-byte length with status) formats
    def validate_packet(self, data):
        if data is False:
            try:
                logger.info("|  validate_packet: data is False")
            except Exception:
                pass
            return False
        try:
            if len(data) < 7:
                try:
                    logger.info(f"|  validate_packet: len={len(data)} < 7")
                except Exception:
                    pass
                return False
            if data[0] != 0xDD or data[-1] != 0x77:
                try:
                    logger.info(f"|  validate_packet: bad header/footer {data[0]:02x}...{data[-1]:02x}")
                except Exception:
                    pass
                return False
            # Prefer 2-byte length used by BLE replies: DD, type, len_hi, len_lo, ... payload ..., chk(2), 77
            try:
                plen2 = (data[2] << 8) | data[3]
                total2 = plen2 + 7
                if total2 == len(data):
                    chk_sum, end = unpack_from(">HB", data, plen2 + 4)
                    if end != 0x77:
                        try:
                            logger.info(f"|  validate_packet: bad end byte {end:02x}")
                        except Exception:
                            pass
                        return False
                    # BLE replies: checksum covers [len_hi, len_lo, payload]
                    # sum(cmd + length + payload), then two's complement
                    # For 2-byte length: data[2]=len_hi, data[3]=len_lo, data[4:4+plen2]=payload
                    calc_chk = checksum(data[2:-3])
                    if chk_sum != calc_chk:
                        try:
                            logger.info(f"|  validate_packet: checksum mismatch {chk_sum:04x} != {calc_chk:04x}")
                        except Exception:
                            pass
                        return False
                    try:
                        logger.info(f"|  validate_packet: SUCCESS, returning payload {len(data[4:4+plen2])} bytes")
                    except Exception:
                        pass
                    return data[4 : 4 + plen2]
            except Exception as e:
                try:
                    logger.info(f"|  validate_packet: 2-byte length exception: {e}")
                except Exception:
                    pass
                pass
            # Fallback to 1-byte length with status (legacy UART layout): DD, type, status, len, payload..., chk(2), 77
            try:
                _, _, status, plen1 = unpack_from("BBBB", data)
                total1 = plen1 + 7
                if status == 0x00 and total1 == len(data):
                    chk_sum, end = unpack_from(">HB", data, plen1 + 4)
                    if end != 0x77:
                        return False
                    if chk_sum != checksum(data[2:-3]):
                        return False
                    return data[4 : 4 + plen1]
            except Exception:
                pass
            return False
        except Exception:
            return False

    @property
    def unique_identifier(self) -> str:
        """
        Provide a stable unique identifier for BLE instances.
        When configured, prefer the port plus BLE address to avoid collisions.
        """
        if USE_PORT_AS_UNIQUE_ID:
            return self.port + ("__" + self.address.replace(":", "").lower() if self.address is not None else "")
        else:
            return self.address.replace(":", "").lower()

    def _dbg(self, text: str) -> None:
        try:
            with open(self._dbg_path, "a") as f:
                f.write(f"{time.strftime('%H:%M:%S')} {text}\n")
        except Exception:
            pass

    def _process_rx_chunk(self, data: bytes) -> None:
        try:
            if not data:
                return
            # Ignore BlueZ/GATT write acknowledgments (FF AA ...), which are not part of JBD frames
            try:
                if len(data) >= 2 and data[0] == 0xFF and data[1] == 0xAA:
                    try:
                        logger.info("|  fast: asm ignore FFAA ack chunk")
                    except Exception:
                        pass
                    return
            except Exception:
                pass
            
            buf = self._rx_buffer
            
            # Detect if BMS is repeating the same chunk
            # This can happen due to: 1) Multiple signal handlers, 2) BMS re-broadcasting same state
            # Only add chunk if buffer doesn't already end with this exact data
            is_duplicate = False
            if data == self._last_rx_chunk:
                self._rx_chunk_repeat_count += 1
                # Check if buffer already ends with this chunk - if so, it's a true duplicate
                if len(buf) >= len(data) and buf[-len(data):] == data:
                    is_duplicate = True
                    try:
                        logger.info(f"|  fast: Skipping duplicate chunk {len(data)} bytes (repeat={self._rx_chunk_repeat_count})")
                    except Exception:
                        pass
            else:
                self._last_rx_chunk = data
                self._rx_chunk_repeat_count = 1
            
            # Only add chunk if it's not a duplicate
            if not is_duplicate:
                buf.extend(data)
                try:
                    logger.info(f"|  fast: RX chunk {len(data)} bytes, buffer now {len(buf)} bytes (repeat={self._rx_chunk_repeat_count})")
                except Exception:
                    pass
            
            # Continue with frame assembly
            while True:
                if not buf:
                    return
                # Drop any leading bytes until a 0xDD header
                if buf[0] != 0xDD:
                    try:
                        first = buf.index(0xDD)
                        if first > 0:
                            try:
                                logger.info(f"|  fast: resync: found 0xDD at {first}, discarding {first} bytes")
                            except Exception:
                                pass
                            del buf[:first]
                        else:
                            del buf[:1]
                        continue
                    except ValueError:
                        buf.clear()
                        return

                # Need at least 5 bytes to know expected length (DD, type, len_hi, len_lo, ...)
                if len(buf) < 5:
                    return

                # Scan for any valid frame starting at a header within the buffer
                found_any = False
                i = 0
                end_limit = max(0, len(buf) - 4)
                while i < end_limit:
                    if buf[i] != 0xDD:
                        i += 1
                        continue
                    # We have a header at i; ensure length bytes available
                    if i + 4 > len(buf):
                        break
                    # JBD replies use 2-byte length: len = (len_hi << 8) | len_lo
                    plen_hi = buf[i + 2]
                    plen_lo = buf[i + 3]
                    plen = (plen_hi << 8) | plen_lo
                    need = plen + 7
                    
                    # If BMS is repeating chunks and we have partial data, clear buffer and move on
                    if self._force_incomplete_frame_extraction and len(buf) >= i + 5 and buf[i] == 0xDD:
                        # BMS firmware bug: it keeps resending first 20 bytes instead of continuation
                        partial_len = len(buf) - i
                        try:
                            logger.info(f"|  fast: BMS firmware bug: dd03 incomplete ({partial_len}/{need} bytes), clearing buffer")
                        except Exception:
                            pass
                        # Clear buffer to avoid infinite loop
                        buf.clear()
                        self._force_incomplete_frame_extraction = False
                        self._rx_chunk_repeat_count = 0
                        self._last_rx_chunk = b''
                        return
                    
                    # If not enough bytes yet, wait
                    if i + need > len(buf):
                        break
                    # Check end marker
                    if buf[i + need - 1] == 0x77:
                        frame = bytes(buf[i : i + need])
                        self._assembled_frames.append(frame)
                        # Count verified frames to gate first D-Bus publish
                        try:
                            self._frames_verified += 1
                            self._last_good_time = time.time()
                        except Exception:
                            pass
                        try:
                            logger.info(f"|  fast: ✓ assembled frame {len(frame)} bytes HEX={frame.hex()}")
                        except Exception:
                            pass
                        # Remove everything up to end of this frame
                        del buf[: i + need]
                        # Reset repeat detection
                        self._rx_chunk_repeat_count = 0
                        self._last_rx_chunk = b''
                        self._force_incomplete_frame_extraction = False
                        found_any = True
                        # After removal, restart outer while to process remaining frames from new start
                        break
                    else:
                        # Header but bad tail at computed end; try next possible header inside window
                        i += 1
                        continue
                if found_any:
                    continue
                # If nothing assembled and buffer grows too large, aggressively clean
                if len(buf) > 512:
                    try:
                        # Try to find the last DD header
                        last = len(buf) - 1 - buf[::-1].index(0xDD)
                        if last > 0:
                            try:
                                logger.info(f"|  fast: buffer overflow ({len(buf)} bytes), keeping from last DD at -{len(buf)-last}")
                            except Exception:
                                pass
                            del buf[:last]
                        else:
                            # No DD found in reverse scan, clear entire buffer
                            try:
                                logger.info(f"|  fast: buffer overflow ({len(buf)} bytes), no DD found, clearing")
                            except Exception:
                                pass
                            buf.clear()
                    except ValueError:
                        # No DD header found at all, clear buffer
                        try:
                            logger.info(f"|  fast: buffer overflow ({len(buf)} bytes), no DD found, clearing")
                        except Exception:
                            pass
                        buf.clear()
                return
        except Exception:
            return

    def connection_name(self) -> str:
        return "BLE " + self.address

    def custom_name(self) -> str:
        try:
            return "SerialBattery(" + self.type + ") " + self.address[-5:]
        except Exception:
            return "SerialBattery(" + self.type + ") " + self.address[-5:]

    def on_disconnect(self):
        logger.info("BLE (fast) disconnected")

    def validate_data(self) -> bool:
        return True

    # Ensure SoC calc is always available to the D-Bus layer
    def set_calculated_data(self) -> None:
        try:
            super().set_calculated_data()
        except Exception:
            pass
        try:
            if getattr(self, "soc_calc", None) is None and getattr(self, "soc", None) is not None:
                self.soc_calc = float(self.soc)
        except Exception:
            pass

    # removed duplicate early test_connection definition (actual implementation appears later)

    async def bt_main_loop(self):
        logger.info("|- Try to connect (fast) to LltJbd at " + self.address)
        self._dbg("bt_main_loop start")

        def _pydbus_connect_device(adapter_opt: str, target_addr: str) -> bool:
            try:
                from dbus.mainloop.glib import DBusGMainLoop
                DBusGMainLoop(set_as_default=True)
                import dbus
                bus = dbus.SystemBus()
                dev_path = f"/org/bluez/{adapter_opt}/dev_" + target_addr.replace(":", "_")
                try:
                    dev_obj = bus.get_object('org.bluez', dev_path)
                except Exception:
                    return False
                props_iface = dbus.Interface(dev_obj, 'org.freedesktop.DBus.Properties')
                dev_iface = dbus.Interface(dev_obj, 'org.bluez.Device1')
                try:
                    connected = bool(props_iface.Get('org.bluez.Device1', 'Connected'))
                except Exception:
                    connected = False
                if not connected:
                    # Try D-Bus Connect first
                    try:
                        dev_iface.Connect()
                        self._dbg("Device1.Connect called")
                    except Exception as ex:
                        self._dbg(f"Device1.Connect failed: {ex}, trying bluetoothctl")
                        # Fallback to bluetoothctl
                        try:
                            result = subprocess.run(
                                ["bluetoothctl", "connect", self.address],
                                capture_output=True,
                                text=True,
                                timeout=10
                            )
                            self._dbg(f"bluetoothctl connect result: {result.returncode}")
                        except Exception as ex2:
                            self._dbg(f"bluetoothctl connect failed: {ex2}")
                deadline = time.time() + 6.0
                while time.time() < deadline:
                    try:
                        connected = bool(props_iface.Get('org.bluez.Device1', 'Connected'))
                        resolved = bool(props_iface.Get('org.bluez.Device1', 'ServicesResolved'))
                        if connected and resolved:
                            self.dbus_bus = bus
                            self.dbus_dev_path = str(dev_path)
                            self._dbg("pydbus connect success and services resolved")
                            return True
                    except Exception:
                        pass
                    time.sleep(0.2)
                return False
            except Exception:
                return False

        # Strategy: prefer async dbus-fast adoption; fallback to python-dbus adopt without GLib main loop.
        for ad in ("hci0", "hci1"):
            # Try dbus-fast adoption first
            try:
                self._dbg(f"bt: fast adopt attempt on {ad}")
            except Exception:
                pass
            try:
                if await self._fast_try_adopt_connected_device(ad, self.address):
                    logger.info("|- Adopted BlueZ connection via dbus-fast GATT")
                    self.bt_loop = asyncio.get_event_loop()
                    self.response_queue = asyncio.Queue()
                    if self.ready_event is not None:
                        self.ready_event.set()
                    # Optional: perform handshake after adoption
                    try:
                        await self._maybe_do_handshake()
                    except Exception:
                        pass
                    # Keep connection alive; let the main D-Bus poll drive read_gen_data/read_cell_data
                    while self.run and self.main_thread.is_alive():
                        try:
                            await asyncio.sleep(0.2)
                        except Exception:
                            pass
                    return
            except Exception:
                pass

            # Fallback: python-dbus adoption (no GLib main loop)
            try:
                try:
                    self._dbg(f"bt: adopt attempt on {ad}")
                except Exception:
                    pass
                if self._dbus_try_adopt_connected_device(ad, self.address):
                    logger.info("|- Adopted BlueZ connection via python-dbus GATT (fast)")
                    # For python-dbus, don't set bt_loop - we use synchronous calls with temporary loops
                    # self.bt_loop = asyncio.get_event_loop()
                    # self.response_queue = asyncio.Queue()
                    if self.ready_event is not None:
                        self.ready_event.set()
                    # Optional: perform handshake after adoption
                    try:
                        await self._maybe_do_handshake()
                    except Exception:
                        pass
                    while self.run and self.main_thread.is_alive():
                        try:
                            await asyncio.sleep(0.2)
                        except Exception:
                            pass
                    return
            except Exception:
                pass

            try:
                if _pydbus_connect_device(ad, self.address):
                    if await self._fast_try_adopt_connected_device(ad, self.address):
                        logger.info("|- Connected and adopted via dbus-fast")
                    elif self._dbus_try_adopt_connected_device(ad, self.address):
                        logger.info("|- Connected and adopted via python-dbus (fast)")
                    else:
                        raise Exception("adopt failed after connect")
                    self.bt_loop = asyncio.get_event_loop()
                    self.response_queue = asyncio.Queue()
                    if self.ready_event is not None:
                        self.ready_event.set()
                    # Optional: perform handshake after adoption
                    try:
                        await self._maybe_do_handshake()
                    except Exception:
                        pass
                    while self.run and self.main_thread.is_alive():
                        try:
                            await asyncio.sleep(0.2)
                        except Exception:
                            pass
                    return
            except Exception:
                pass

        # Nothing to do if not connected at BlueZ level; sleep briefly and return to allow retries
        self._dbg("no adoption; sleeping")
        await asyncio.sleep(1.0)

    def background_loop(self):
        while self.run and self.main_thread.is_alive():
            asyncio.run(self.bt_main_loop())

    def refresh_data(self) -> bool:
        """
        Prefer general data on every poll; read cells opportunistically every ~10 polls.
        Return True if general or cell data succeeded so D-Bus keeps publishing.
        """
        try:
            logger.info("refresh_data called")
        except Exception:
            pass
        
        # Check if no successful data for 60 seconds and attempt reconnection
        now = time.time()
        if now - self._last_successful_data_time > 60.0:
            try:
                logger.warning(f"LltJbd_FastBle: No data for 60s, attempting reconnection...")
                self._attempt_reconnection()
            except Exception as e:
                try:
                    logger.warning(f"Reconnection attempt raised exception: {e}")
                except Exception:
                    pass
        
        # Check for connection loss and attempt reconnection based on error count
        if self._consecutive_errors >= 5:  # After 5 consecutive errors, try to reconnect
            try:
                reconnect_attempted = self._attempt_reconnection()
                if reconnect_attempted:
                    # After reconnection attempt, give it a moment to stabilize
                    time.sleep(2.0)
            except Exception as e:
                try:
                    logger.warning(f"Reconnection attempt raised exception: {e}")
                except Exception:
                    pass
        
        # Respect a short quiet window after handshake like the app
        try:
            if self._handshake_quiet_until and time.time() < self._handshake_quiet_until:
                return True
        except Exception:
            pass
        # Prefer cell-first during bootstrap; then strictly alternate dd03 and dd04 (single in-flight)
        ok_cells = False
        need_cells_first = (self._bootstrap_polls_remaining > 0) or (self.get_min_cell_voltage() is None) or (self.get_max_cell_voltage() is None)
        ok_gen = False
        if need_cells_first:
            ok_cells = bool(self.read_cell_data())
            self._next_poll_gen = True
        else:
            # Alternate: dd03 -> dd04 -> dd03 ... honoring cadence for dd03
            if self._next_poll_gen:
                now = time.time()
                attempt_gen = (now - self._last_gen_send_ts) >= getattr(self, "_gen_period_s", 1.5)
                if attempt_gen:
                    ok_gen = bool(self.read_gen_data())
                    self._last_gen_send_ts = now
                else:
                    # If not time yet for dd03, try cells instead this cycle
                    ok_cells = bool(self.read_cell_data())
            else:
                ok_cells = bool(self.read_cell_data())
            # Toggle after a successful transaction
            if ok_gen or ok_cells:
                try:
                    self._next_poll_gen = not self._next_poll_gen
                except Exception:
                    self._next_poll_gen = False
        # As a fallback, opportunistically try a general read if not yet successful
        try:
            if not ok_gen:
                ok_gen = bool(self.read_gen_data())
        except Exception:
            pass
        if ok_gen:
            self._gen_fail_count = 0
        else:
            try:
                self._gen_fail_count += 1
            except Exception:
                self._gen_fail_count = 1
        ok_any = ok_gen or ok_cells
        if ok_any:
            try:
                self._last_good_time = time.time()
                self._last_successful_data_time = time.time()  # Update successful data timestamp
                # Save state on successful data refresh
                self._save_cached_state()
            except Exception:
                pass
        else:
            # No fresh data - check if we can use cached data
            try:
                if self._use_cached_data():
                    age = time.time() - self._last_good_state.get('timestamp', 0)
                    logger.info(f"Using cached data ({age:.0f}s old) during disconnection")
                    ok_any = True  # Treat cached data as valid to prevent cable fault
            except Exception as e:
                logger.debug(f"Failed to use cached data: {e}")
        
        # Gate first publishes until a minimum number of verified frames were seen/logged
        try:
            if self._frames_verified < self._publish_gate_frames and ok_any:
                try:
                    logger.info(
                        f"|  fast: publish gate active: frames_seen={self._frames_verified}/{self._publish_gate_frames}"
                    )
                except Exception:
                    pass
                # Do not block online state; allow ok_any to return True so UI does not show cable fault
        except Exception:
            pass
        if ok_any:
            try:
                # Ensure soc_calc has a sane value for publishing even when SOC_CALCULATION is disabled
                if getattr(self, "soc_calc", None) is None and getattr(self, "soc", None) is not None:
                    self.soc_calc = float(self.soc)
                # No opportunistic extra reads; sequencing is handled above
                # Derive pack voltage from sum of cells if general failed
                try:
                    if (not ok_gen) and ok_cells and getattr(self, "cells", None):
                        vsum = 0.0
                        valid = 0
                        for c in self.cells:
                            try:
                                v = getattr(c, "voltage", None)
                                if v is not None:
                                    vsum += float(v)
                                    valid += 1
                            except Exception:
                                pass
                        if valid > 0 and vsum > 0:
                            self.voltage = round(vsum, 2)
                except Exception:
                    pass
                # Ensure hardware info fetched once; on repeated general failures, probe hardware before retrying general
                if getattr(self, "_hw_read_done", False) is False:
                    try:
                        if self.read_hardware_data():
                            self._hw_read_done = True
                    except Exception:
                        self._hw_read_done = True
                else:
                    try:
                        # Every 3rd failed general cycle, issue a hardware probe (mirrors app behavior) then a quick general retry
                        if not ok_gen and (self._gen_fail_count % 3 == 2):
                            _ = self.read_hardware_data()
                            _ = self.read_gen_data()
                            if _:
                                self._gen_fail_count = 0
                    except Exception:
                        pass
            except Exception:
                pass
        else:
            # General read failed; still try to seed cells and bootstrap
            try:
                if self._bootstrap_polls_remaining > 0:
                    self._bootstrap_polls_remaining -= 1
                    ok_cells = bool(self.read_cell_data()) or ok_cells
                else:
                    # Keep trying cells on failures to get min/max quickly
                    ok_cells = bool(self.read_cell_data()) or ok_cells
            except Exception:
                pass
        # If the last successful read was recent, keep reporting online to avoid UI flap
        try:
            if not ok_any and self._last_good_time and (time.time() - self._last_good_time) < 120:
                return True
        except Exception:
            pass
        return ok_any

    async def async_test_connection(self):
        self.ready_event = asyncio.Event()
        if not self.bt_thread.is_alive():
            self.bt_thread.start()

            def shutdown_ble_atexit(thread):
                self.run = False
                thread.join()

            atexit.register(shutdown_ble_atexit, self.bt_thread)
        try:
            return await asyncio.wait_for(self.ready_event.wait(), timeout=60)
        except asyncio.TimeoutError:
            logger.error(">>> ERROR: Unable to connect with BLE device (fast)")
            return False

    def test_connection(self):
        try:
            if not self.address:
                return False
            if getattr(self, 'ready_event', None) is None:
                self.ready_event = asyncio.Event()
            if not self.bt_thread.is_alive():
                self.bt_thread.start()

                def shutdown_ble_atexit(thread):
                    self.run = False
                    thread.join()

                atexit.register(shutdown_ble_atexit, self.bt_thread)
            
            # Wait for adoption to complete
            # Unlike nordicnus_ble which uses synchronous Syncron_Ble, we use async notifications
            # so we can't easily wait for data in test_connection(). Just verify adoption succeeded.
            import time
            deadline = time.time() + 15.0
            
            while time.time() < deadline:
                # Check if dbus-fast path is ready
                if self._fast_bus and self.dbus_rx_char_path and self.dbus_tx_char_path:
                    try:
                        logger.info(f"|  test_connection: dbus-fast ready, RX={self.dbus_rx_char_path} TX={self.dbus_tx_char_path}")
                        self._is_connected = True  # Mark as initially connected
                    except Exception:
                        pass
                    return True
                # Check if python-dbus path is ready
                if self.dbus_bus and self.dbus_rx_char_path and self.dbus_tx_char_path and self._dbus_signal_attached:
                    try:
                        logger.info(f"|  test_connection: python-dbus ready, RX={self.dbus_rx_char_path} TX={self.dbus_tx_char_path}")
                        # Give GLib loop a moment to start dispatching signals
                        time.sleep(1.0)
                        logger.info(f"|  test_connection: GLib loop should be running now")
                        self._is_connected = True  # Mark as initially connected
                    except Exception:
                        pass
                    return True
                
                time.sleep(0.5)
            
            try:
                logger.error(f"|  test_connection: timeout waiting for adoption")
            except Exception:
                pass
            return False
        except Exception:
            import sys as _sys
            _t, _o, _tb = _sys.exc_info()
            file = _tb.tb_frame.f_code.co_filename
            line = _tb.tb_lineno
            logger.error(f"Exception occurred: {repr(_o)} of type {_t} in {file} line #{line}")
            return False

    async def send_command(self, command, swap_paths: bool = False) -> Union[bytearray, bool]:
        try:
            # For python-dbus (synchronous mode), write and return False immediately
            # to avoid blocking the GLib main loop. Rely on scavenging in read_serial_data_llt.
            if self.dbus_bus and not self._fast_bus:
                import dbus
                try:
                    tx_path = self.dbus_tx_char_path
                    tx_obj = self.dbus_bus.get_object('org.bluez', tx_path)
                    tx_iface = dbus.Interface(tx_obj, 'org.bluez.GattCharacteristic1')
                    arr = dbus.Array([dbus.Byte(b) for b in bytes(command)], signature='y')
                    opts = dbus.Dictionary({'type': dbus.String('command')}, signature='sv')
                    tx_iface.WriteValue(arr, opts)
                    try:
                        logger.info(f"|  fast: WriteValue (py-dbus quick) SUCCESS")
                        # Reset error counter on successful write
                        self._consecutive_errors = 0
                        self._is_connected = True
                    except Exception:
                        pass
                except Exception as e:
                    try:
                        err_str = str(e)
                        logger.info(f"|  fast: WriteValue (py-dbus quick) FAILED: {e}")
                        # If InProgress, BlueZ is busy (likely scanning), wait for it to finish
                        # Treat it the same as if we were running a scan ourselves
                        if "org.bluez.Error.InProgress" in err_str or "Operation already in progress" in err_str:
                            try:
                                logger.info("|  fast: BlueZ InProgress detected, waiting for scan to complete (4s)...")
                                time.sleep(4.0)  # Wait for scan/operation to finish (3s typical scan + 1s settle)
                                # Reset error counter after waiting - the scan may have refreshed the device
                                self._consecutive_errors = 0
                            except Exception:
                                pass
                        # Track consecutive connection errors
                        elif "Not connected" in err_str or "org.bluez.Error.Failed" in err_str:
                            self._consecutive_errors += 1
                            self._is_connected = False
                        else:
                            # Reset on other errors (not connection-related)
                            self._consecutive_errors = 0
                    except Exception:
                        pass
                # Return False to trigger scavenging in read_serial_data_llt
                return False
            
            if self._send_lock is None:
                self._send_lock = asyncio.Lock()
            async with self._send_lock:
                # Do not clear RX queue; allow assembler to complete multi-chunk frames
                self._dbg("send_command: tx request queued")

                # Prefer dbus-fast path
                if self._fast_bus is not None and self.dbus_rx_char_path and self.dbus_tx_char_path:

                    write_msg = None
                    # Choose TX path based on active family stickiness (prefer alt for dd03 if available)
                    chosen_tx_path = None
                    try:
                        # Peek register if possible
                        reg_hint = None
                        try:
                            if isinstance(command, (bytes, bytearray)) and len(command) >= 3 and command[0] == 0xDD:
                                reg_hint = command[2]
                        except Exception:
                            reg_hint = None
                        if reg_hint == 0x03 and self._alt_tx_char_path:
                            chosen_tx_path = self._alt_tx_char_path
                        elif self._active_family == "ff" and self.dbus_tx_char_path:
                            chosen_tx_path = self.dbus_tx_char_path
                        elif self._active_family == "ffe" and self._alt_tx_char_path:
                            chosen_tx_path = self._alt_tx_char_path
                        elif self.dbus_tx_char_path:
                            chosen_tx_path = self.dbus_tx_char_path
                        elif self._alt_tx_char_path:
                            chosen_tx_path = self._alt_tx_char_path
                    except Exception:
                        chosen_tx_path = self.dbus_tx_char_path or self._alt_tx_char_path
                    try:
                        from dbus_fast import Message, Variant
                        try:
                            logger.info(f"|  fast: WriteValue to TX {chosen_tx_path}, {len(bytes(command))} bytes")
                        except Exception:
                            pass
                        # BlueZ WriteValue signature: ay a{sv}
                        # For dd03, use probing matrix (family x write type); dd04 default request
                        try:
                            is_dd = isinstance(command, (bytes, bytearray)) and len(command) >= 3 and command[0] == 0xDD
                            reg = command[2] if is_dd else None
                            if reg == 0x03:
                                mode = self._dd03_working_mode or self._dd03_modes_to_try[self._dd03_mode_idx % len(self._dd03_modes_to_try)]
                                fam, wtyp = mode
                                write_type = wtyp
                                # If family from mode mismatches chosen path, override chosen path accordingly
                                try:
                                    if fam == "ff" and self.dbus_tx_char_path:
                                        chosen_tx_path = self.dbus_tx_char_path
                                    elif fam == "ffe" and self._alt_tx_char_path:
                                        chosen_tx_path = self._alt_tx_char_path
                                except Exception:
                                    pass
                            elif reg == 0x04:
                                write_type = "command"
                            else:
                                write_type = "command"
                        except Exception:
                            write_type = "command"
                        write_msg = Message(
                            destination="org.bluez",
                            path=chosen_tx_path,
                            interface="org.bluez.GattCharacteristic1",
                            member="WriteValue",
                            signature="aya{sv}",
                            body=[bytes(command), {"type": Variant("s", write_type)}],
                        )
                        await self._fast_bus.call(write_msg)
                        self._dbg(f"WriteValue (dbus-fast) {len(bytes(command))} bytes")
                    except Exception:
                        # fall back to python-dbus below if dbus-fast write failed
                        write_msg = None

                    # Wait for assembled frame and prefer reply matching requested register
                    # Shorten dd04 window to avoid blocking alternation
                    dd = isinstance(command, (bytes, bytearray)) and len(command) >= 3 and command[0] == 0xDD
                    deadline = time.time() + (4.0 if (dd and command[2] == 0x03) else 3.0)
                    frame = None
                    expected_reg = None
                    try:
                        if isinstance(command, (bytes, bytearray)) and len(command) >= 3 and command[0] == 0xDD:
                            expected_reg = command[2]
                    except Exception:
                        expected_reg = None
                    last_resend = time.time()
                    # App parity: avoid aggressive bursts; rely on a quiet window and occasional resend
                    quiet_until = 0.0

                    # Stickiness: resend only on the chosen family TX path; for dd03 do a one-time alternate write early
                    resend_tx_paths = []
                    if chosen_tx_path:
                        resend_tx_paths.append(chosen_tx_path)
                    # One-time early alternate family write for dd03 (if both exist)
                    dd03_alt_sent = False
                    resend_idx = 0
                    while time.time() < deadline and frame is None:
                        # Drain queued chunks and assemble
                        try:
                            while self._dbus_rx_queue:
                                chunk = self._dbus_rx_queue.pop(0)
                                self._process_rx_chunk(chunk)
                        except Exception:
                            pass
                        # Conservative resend cadence:
                        #  - general (0x03): ~1.5s
                        #  - cells   (0x04): ~0.4s
                        try:
                            resend_due = False
                            now = time.time()
                            if now >= quiet_until:
                                if expected_reg == 0x03 and (now - last_resend) >= 1.5:
                                    resend_due = True
                                elif expected_reg == 0x04 and (now - last_resend) >= 0.5:
                                    resend_due = True
                            # For dd03, opportunistically send once to the other family early (without waiting full resend cadence)
                            if expected_reg == 0x03 and (not dd03_alt_sent) and self._alt_tx_char_path and chosen_tx_path and self._alt_tx_char_path != chosen_tx_path and (now - last_resend) >= 0.25:
                                from dbus_fast import Message, Variant
                                resend_msg = Message(
                                    destination="org.bluez",
                                    path=self._alt_tx_char_path,
                                    interface="org.bluez.GattCharacteristic1",
                                    member="WriteValue",
                                    signature="aya{sv}",
                                    body=[bytes(command), {"type": Variant("s", "request")}],
                                )
                                await self._fast_bus.call(resend_msg)
                                dd03_alt_sent = True
                                last_resend = now
                            if resend_due and resend_tx_paths:
                                from dbus_fast import Message, Variant
                                tx_path_try = resend_tx_paths[resend_idx % len(resend_tx_paths)]
                                resend_idx += 1
                                resend_msg = Message(
                                    destination="org.bluez",
                                    path=tx_path_try,
                                    interface="org.bluez.GattCharacteristic1",
                                    member="WriteValue",
                                    signature="aya{sv}",
                                    body=[bytes(command), {"type": Variant("s", "command")}],
                                )
                                await self._fast_bus.call(resend_msg)
                                last_resend = now
                        except Exception:
                            pass
                        if self._assembled_frames:
                            try:
                                if expected_reg is None:
                                    frame = self._assembled_frames.pop(0)
                                else:
                                    picked = None
                                    for idx, fr in enumerate(self._assembled_frames):
                                        if len(fr) >= 4 and fr[0] == 0xDD and fr[1] == expected_reg:
                                            picked = idx
                                            break
                                    if picked is not None:
                                        frame = self._assembled_frames.pop(picked)
                                    else:
                                        frame = None
                                if frame is not None:
                                    break
                            except Exception:
                                frame = None
                        await asyncio.sleep(0.05)
                    if frame is None:
                        try:
                            logger.info("|  fast: no response before timeout")
                        except Exception:
                            pass
                        self._dbg("timeout waiting for rx (dbus-fast)")
                        # Try to restart notifications on alternate RX then retry once for dd03
                        try:
                            if expected_reg == 0x03:
                                self._restart_notifications_for_dd03()
                                await asyncio.sleep(0.1)
                                # Force re-send on alternate tx if available
                                if self._alt_tx_char_path:
                                    from dbus_fast import Message, Variant
                                    resend_msg = Message(
                                        destination="org.bluez",
                                        path=self._alt_tx_char_path,
                                        interface="org.bluez.GattCharacteristic1",
                                        member="WriteValue",
                                        signature="aya{sv}",
                                        body=[bytes(command), {"type": Variant("s", "request")}],
                                    )
                                    await self._fast_bus.call(resend_msg)
                                    # Give a short window to arrive
                                    await asyncio.sleep(0.25)
                                    # Drain queue once
                                    try:
                                        while self._dbus_rx_queue:
                                            chunk = self._dbus_rx_queue.pop(0)
                                            self._process_rx_chunk(chunk)
                                    except Exception:
                                        pass
                                    if self._assembled_frames:
                                        try:
                                            picked = None
                                            for idx, fr in enumerate(self._assembled_frames):
                                                if len(fr) >= 4 and fr[0] == 0xDD and fr[1] == expected_reg:
                                                    picked = idx
                                                    break
                                            if picked is not None:
                                                frame = self._assembled_frames.pop(picked)
                                                logger.info("|  fast: dd03 arrived after notify restart (dbus-fast)")
                                                return bytearray(frame)
                                        except Exception:
                                            pass
                        except Exception:
                            pass
                        # Advance dd03 probing mode on misses
                        try:
                            if expected_reg == 0x03 and self._dd03_working_mode is None:
                                self._dd03_mode_idx = (self._dd03_mode_idx + 1) % len(self._dd03_modes_to_try)
                                logger.info(f"|  fast: dd03 probe advance -> {self._dd03_modes_to_try[self._dd03_mode_idx]}")
                        except Exception:
                            pass
                        # Track dd03 misses and consider switching active family
                        try:
                            if expected_reg == 0x03:
                                self._dd03_miss_count += 1
                                if self._dd03_miss_count >= self._dd03_switch_threshold:
                                    if self._active_family == "ff" and self._alt_tx_char_path:
                                        self._active_family = "ffe"
                                        self._dd03_miss_count = 0
                                        logger.info("|  fast: switching active family to FFE* after dd03 misses")
                                    elif self._active_family == "ffe" and self.dbus_tx_char_path:
                                        self._active_family = "ff"
                                        self._dd03_miss_count = 0
                                        logger.info("|  fast: switching active family to FF00 after dd03 misses")
                        except Exception:
                            pass
                        # One retry: resend once and try alternate paths if available
                        try:
                            if self._alt_rx_char_path and self._alt_tx_char_path:
                                self._prefer_alt_once = True
                            # simple short wait and retry same command
                            await asyncio.sleep(0.05)
                            return await self.send_command(command, swap_paths=False)
                        except Exception:
                            return False
                    # Reset dd03 miss counter on success and learn working mode
                    try:
                        if expected_reg == 0x03:
                            self._dd03_miss_count = 0
                            # Learn working mode
                            try:
                                # Determine which mode we used
                                fam_used = "ffe" if (chosen_tx_path and self._alt_tx_char_path and chosen_tx_path == self._alt_tx_char_path) else "ff"
                                wtyp_used = write_type if isinstance(write_type, str) else "request"
                                self._dd03_working_mode = (fam_used, wtyp_used)
                                logger.info(f"|  fast: dd03 working mode learned -> {self._dd03_working_mode}")
                            except Exception:
                                pass
                    except Exception:
                        pass
                    try:
                        logger.info(f"|  fast: RX {len(frame)} bytes")
                    except Exception:
                        pass
                    self._dbg(f"RX (dbus-fast) {len(frame)} bytes")
                    return bytearray(frame)

                # Fallback: python-dbus
                if self.dbus_bus is None or not (self.dbus_rx_char_path and self.dbus_tx_char_path):
                    logger.error(">>> ERROR: No BlueZ D-Bus GATT connection - returning")
                    return False

                import dbus
                # Use discovered RX/TX when available; prefer active family stickiness, allow swap
                base = self.dbus_dev_path or ""
                # For dd03, use probing matrix (family preference and write type)
                try:
                    reg_hint = None
                    if isinstance(command, (bytes, bytearray)) and len(command) >= 3 and command[0] == 0xDD:
                        reg_hint = command[2]
                except Exception:
                    reg_hint = None
                if reg_hint == 0x03:
                    mode = self._dd03_working_mode or self._dd03_modes_to_try[self._dd03_mode_idx % len(self._dd03_modes_to_try)]
                    fam = mode[0]
                    if fam == "ffe" and self._alt_rx_char_path and self._alt_tx_char_path:
                        rx_default = self._alt_rx_char_path
                        tx_default = self._alt_tx_char_path
                    else:
                        rx_default = self.dbus_rx_char_path or (base + "/service000f/char0010")
                        tx_default = self.dbus_tx_char_path or (base + "/service000f/char0014")
                elif self._active_family == "ffe" and self._alt_rx_char_path and self._alt_tx_char_path:
                    rx_default = self._alt_rx_char_path
                    tx_default = self._alt_tx_char_path
                else:
                    rx_default = self.dbus_rx_char_path or (base + "/service000f/char0010")
                    tx_default = self.dbus_tx_char_path or (base + "/service000f/char0014")
                rx_path = tx_default if swap_paths else rx_default
                tx_path = rx_default if swap_paths else tx_default
                # If no response later, send_command will try swap_paths and can also try alternates
                rx_obj = self.dbus_bus.get_object('org.bluez', rx_path)
                rx_iface = dbus.Interface(rx_obj, 'org.bluez.GattCharacteristic1')
                # Notifications are started at adopt time; do not reissue each command
                tx_obj = self.dbus_bus.get_object('org.bluez', tx_path)
                tx_iface = dbus.Interface(tx_obj, 'org.bluez.GattCharacteristic1')
                arr = dbus.Array([dbus.Byte(b) for b in bytes(command)], signature='y')
                try:
                    logger.info(f"|  fast: WriteValue (py-dbus) to TX {tx_path}, {len(arr)} bytes HEX={bytes(command).hex()}")
                except Exception:
                    pass
                if self._prefer_alt_once:
                    self._prefer_alt_once = False
                # Use write-without-response for all commands (matching Android app behavior)
                try:
                    write_type = 'command'  # Always use write-without-response
                    opts = dbus.Dictionary({'type': dbus.String(write_type)}, signature='sv')
                    try:
                        logger.info(f"|  fast: WriteValue (py-dbus) calling with type={write_type}")
                    except Exception:
                        pass
                    tx_iface.WriteValue(arr, opts)
                    try:
                        logger.info(f"|  fast: WriteValue (py-dbus) returned SUCCESS")
                    except Exception:
                        pass
                except Exception as e:
                    try:
                        logger.info(f"|  fast: WriteValue (py-dbus) EXCEPTION: {e}")
                    except Exception:
                        pass
                try:
                    self._dbg(f"WriteValue (py-dbus) {len(arr)} bytes to {tx_path}")
                except Exception:
                    pass

                # Allow shorter window for cells to avoid blocking alternation
                dd = isinstance(command, (bytes, bytearray)) and len(command) >= 3 and command[0] == 0xDD
                deadline = time.time() + (5.0 if (dd and command[2] == 0x03) else 3.0)
                frame = None
                expected_reg = None
                try:
                    if isinstance(command, (bytes, bytearray)) and len(command) >= 3 and command[0] == 0xDD:
                        expected_reg = command[2]
                except Exception:
                    expected_reg = None
                last_read_bytes = b""
                last_resend = time.time()
                quiet_until = 0.0
                # Stickiness: resend only on chosen tx_path; for dd03, one-time alternate write early
                resend_tx_paths = [tx_path]
                dd03_alt_sent = False
                resend_idx = 0
                while time.time() < deadline and frame is None:
                    # Drain queued chunks
                    try:
                        while self._dbus_rx_queue:
                            chunk = self._dbus_rx_queue.pop(0)
                            self._process_rx_chunk(chunk)
                    except Exception:
                        pass
                    # Conservative resend cadence (same as above)
                    try:
                        now = time.time()
                        resend_due = False
                        if now >= quiet_until:
                            # Do not resend 0x03; allow limited 0x04 resend
                            if expected_reg == 0x04 and (now - last_resend) >= 0.5:
                                resend_due = True
                        # For dd03, do a one-time write to alt tx early and a mid-loop ReadValue probe
                        if expected_reg == 0x03 and (not dd03_alt_sent) and self._alt_tx_char_path and self._alt_tx_char_path != tx_path and (now - last_resend) >= 0.25:
                            opts = dbus.Dictionary({'type': dbus.String('request')}, signature='sv')
                            tx_obj2 = self.dbus_bus.get_object('org.bluez', self._alt_tx_char_path)
                            tx_iface2 = dbus.Interface(tx_obj2, 'org.bluez.GattCharacteristic1')
                            tx_iface2.WriteValue(arr, opts)
                            dd03_alt_sent = True
                            last_resend = now
                            try:
                                # mid-loop ReadValue probe to prime cache
                                rv = rx_iface.ReadValue(dbus.Dictionary({}, signature='sv'))
                                _ = rv  # ignore content; handler will process if valid
                            except Exception:
                                pass
                        if resend_due and resend_tx_paths:
                            opts = dbus.Dictionary({'type': dbus.String('command')}, signature='sv')
                            try_path = resend_tx_paths[resend_idx % len(resend_tx_paths)]
                            resend_idx += 1
                            # write on chosen path (includes RX path as last resort)
                            tx_obj2 = self.dbus_bus.get_object('org.bluez', try_path)
                            tx_iface2 = dbus.Interface(tx_obj2, 'org.bluez.GattCharacteristic1')
                            try:
                                logger.info(f"|  fast: resend (py-dbus) to {try_path} HEX={bytes(command).hex()}")
                            except Exception:
                                pass
                            tx_iface2.WriteValue(arr, opts)
                            last_resend = now
                    except Exception:
                        pass
                    if self._assembled_frames:
                        try:
                            if expected_reg is None:
                                frame = self._assembled_frames.pop(0)
                            else:
                                picked = None
                                for idx, fr in enumerate(self._assembled_frames):
                                    if len(fr) >= 4 and fr[0] == 0xDD and fr[1] == expected_reg:
                                        picked = idx
                                        break
                                if picked is not None:
                                    frame = self._assembled_frames.pop(picked)
                                else:
                                    frame = None
                            if frame is not None:
                                break
                        except Exception:
                            frame = None
                    # Do not poll ReadValue; rely on notifications like the app
                    await asyncio.sleep(0.05)
                if frame is None:
                    try:
                        logger.info("|  fast: no response before timeout (py-dbus)")
                    except Exception:
                        pass
                    # Attempt direct read to prime cache
                    try:
                        rv = rx_iface.ReadValue(dbus.Dictionary({}, signature='sv'))
                        data = None
                        try:
                            data = bytes(bytearray(rv))
                        except Exception:
                            data = None
                        if data and data != last_read_bytes:
                            last_read_bytes = data
                            self._process_rx_chunk(data)
                    except Exception:
                        pass
                    # second chance: drain queue then check
                    try:
                        while self._dbus_rx_queue:
                            chunk = self._dbus_rx_queue.pop(0)
                            self._process_rx_chunk(chunk)
                    except Exception:
                        pass
                    if self._assembled_frames:
                        try:
                            frame = self._assembled_frames.pop(0)
                        except Exception:
                            frame = None
                # Do not switch to alternate characteristics during assembly

                if frame is None:
                    self._dbg("timeout waiting for rx (py-dbus)")
                    # Try to restart notifications on alternate RX then retry once for dd03
                    try:
                        if expected_reg == 0x03:
                            self._restart_notifications_for_dd03()
                            await asyncio.sleep(0.1)
                            # Force re-send on alternate tx if available
                            if self._alt_tx_char_path:
                                opts = dbus.Dictionary({'type': dbus.String('request')}, signature='sv')
                                tx_obj2 = self.dbus_bus.get_object('org.bluez', self._alt_tx_char_path)
                                tx_iface2 = dbus.Interface(tx_obj2, 'org.bluez.GattCharacteristic1')
                                tx_iface2.WriteValue(arr, opts)
                                await asyncio.sleep(0.25)
                                # Drain queue once
                                try:
                                    while self._dbus_rx_queue:
                                        chunk = self._dbus_rx_queue.pop(0)
                                        self._process_rx_chunk(chunk)
                                except Exception:
                                    pass
                                if self._assembled_frames:
                                    try:
                                        picked = None
                                        for idx, fr in enumerate(self._assembled_frames):
                                            if len(fr) >= 4 and fr[0] == 0xDD and fr[1] == expected_reg:
                                                picked = idx
                                                break
                                        if picked is not None:
                                            frame = self._assembled_frames.pop(picked)
                                            logger.info("|  fast: dd03 arrived after notify restart (py-dbus)")
                                            return bytearray(frame)
                                    except Exception:
                                        pass
                    except Exception:
                        pass
                    # Advance dd03 probing mode on misses
                    try:
                        if expected_reg == 0x03 and self._dd03_working_mode is None:
                            self._dd03_mode_idx = (self._dd03_mode_idx + 1) % len(self._dd03_modes_to_try)
                            logger.info(f"|  fast: dd03 probe advance (py-dbus) -> {self._dd03_modes_to_try[self._dd03_mode_idx]}")
                        
                    except Exception:
                        pass
                    # Track dd03 misses and consider switching active family
                    try:
                        if expected_reg == 0x03:
                            self._dd03_miss_count += 1
                            if self._dd03_miss_count >= self._dd03_switch_threshold:
                                if self._active_family == "ff" and self._alt_tx_char_path:
                                    self._active_family = "ffe"
                                    self._dd03_miss_count = 0
                                    logger.info("|  fast: switching active family to FFE* after dd03 misses (py-dbus)")
                                elif self._active_family == "ffe" and self.dbus_tx_char_path:
                                    self._active_family = "ff"
                                    self._dd03_miss_count = 0
                                    logger.info("|  fast: switching active family to FF00 after dd03 misses (py-dbus)")
                    except Exception:
                        pass
                    # Retry: try swapping RX/TX first, then try alternate family once if available
                    try:
                        if not swap_paths:
                            return await self.send_command(command, swap_paths=True)
                    except Exception:
                        pass
                    try:
                        if self._alt_rx_char_path and self._alt_tx_char_path:
                            self._prefer_alt_once = True
                            return await self.send_command(command, swap_paths=False)
                    except Exception:
                        pass
                    return False
                # Reset dd03 miss counter on success and learn working mode
                try:
                    if expected_reg == 0x03:
                        self._dd03_miss_count = 0
                        try:
                            fam_used = "ffe" if (tx_path and self._alt_tx_char_path and tx_path == self._alt_tx_char_path) else "ff"
                            # Determine write_type used above
                            mode = (fam_used, write_type if isinstance(write_type, str) else 'request')
                            self._dd03_working_mode = mode
                            logger.info(f"|  fast: dd03 working mode learned (py-dbus) -> {self._dd03_working_mode}")
                        except Exception:
                            pass
                except Exception:
                    pass
                try:
                    logger.info(f"|  fast: RX (py-dbus) {len(frame)} bytes HEX={frame.hex()}")
                except Exception:
                    pass
                try:
                    self._dbg(f"RX (py-dbus) {len(frame)} bytes hex={frame.hex()}")
                except Exception:
                    pass
                return bytearray(frame)
        except Exception:
            return False

    def _build_fa_command(self, start_addr: int, count: int) -> bytes:
        """
        Build a 0xFA parameter read command .
        Args:
            start_addr: Starting address (2 bytes)
            count: Number of bytes to read (1 byte)
        Returns:
            Complete frame: DD A5 FA 03 [addr_hi] [addr_lo] [count] [checksum_hi] [checksum_lo] 77
        """
        # Payload: [addr_hi, addr_lo, count]
        addr_hi = (start_addr >> 8) & 0xFF
        addr_lo = start_addr & 0xFF
        payload = bytes([addr_hi, addr_lo, count])
        
        # Checksum: sum of (cmd + length + payload), then take 2's complement (invert + 1)
        cmd = 0xFA
        length = len(payload)  # 3
        checksum_sum = cmd + length
        for b in payload:
            checksum_sum += b
        checksum_sum &= 0xFFFF  # Keep as 16-bit
        checksum_twos = ((~checksum_sum) + 1) & 0xFFFF  # Two's complement
        checksum_hi = (checksum_twos >> 8) & 0xFF
        checksum_lo = checksum_twos & 0xFF
        
        # Build frame: DD A5 FA [len] [payload] [checksum] 77
        frame = bytes([0xDD, 0xA5, cmd, length]) + payload + bytes([checksum_hi, checksum_lo, 0x77])
        return frame

    async def _send_fa_init_commands(self) -> None:
        """
        Send 0xFA initialization commands as observed in HCI capture.
        NOTE: BMS sends spontaneous notifications continuously, so we don't need to wait
        for responses. Just send the init commands and let the regular polling handle the rest.
        """
        try:
            logger.info("|  init: Sending 0xFA init commands (non-blocking, BMS sends spontaneous data)")
            
            # 0xFA command 1: Read 4 bytes starting at address 0x0075 (117)
            fa_cmd_1 = self._build_fa_command(0x0075, 4)
            try:
                logger.info(f"|  init: Sending FA cmd 1 (addr=0x0075, count=4): {fa_cmd_1.hex()}")
                # Fire and forget - don't wait for response
                asyncio.create_task(self.send_command(fa_cmd_1))
            except Exception as e:
                logger.warning(f"|  init: FA cmd 1 failed: {e}")
            
            logger.info("|  init: 0xFA initialization complete (non-blocking)")
        except Exception as e:
            logger.warning(f"|  init: 0xFA initialization failed: {e}")

    async def _maybe_do_handshake(self) -> None:
        # HCI capture shows Android app sends 0xFA commands, but BMS sends spontaneous data
        # continuously, so we don't need any initialization - just let regular polling handle it
        try:
            logger.info("|  init: Skipping init sequence (BMS sends spontaneous data)")
        except Exception:
            pass
        return

    def read_serial_data_llt(self, command):
        import asyncio
        try:
            # For python-dbus (synchronous), call send_command directly
            # For dbus-fast (async), schedule on bt_loop
            is_dd = isinstance(command, (bytes, bytearray)) and len(command) >= 3 and command[0] == 0xDD
            reg = command[2] if is_dd else None
            
            if is_dd and reg in (0x04, 0x03):
                # python-dbus uses synchronous calls, dbus-fast uses async
                if self.bt_loop:
                    # dbus-fast async path
                    try:
                        fut1 = asyncio.run_coroutine_threadsafe(self.send_command(command, swap_paths=False), self.bt_loop)
                        data = fut1.result(timeout=12)
                    except Exception:
                        data = None
                else:
                    # python-dbus synchronous path - send_command writes and returns False immediately
                    # to avoid blocking GLib loop. We scavenge frames after a delay.
                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            data = loop.run_until_complete(self.send_command(command, swap_paths=False))
                        finally:
                            loop.close()
                            asyncio.set_event_loop(None)
                        # Give GLib loop time to process notifications
                        import time
                        time.sleep(0.5)
                    except Exception:
                        data = None
                
                # If send_command timed out but assembler has a matching frame, scavenge it
                if not data and self._assembled_frames:
                    try:
                        picked = None
                        for idx, fr in enumerate(self._assembled_frames):
                            if len(fr) >= 4 and fr[0] == 0xDD and fr[1] == reg:
                                picked = idx
                                break
                        if picked is not None:
                            scav = self._assembled_frames.pop(picked)
                            data = bytearray(scav)
                    except Exception:
                        pass
                
                return self.validate_packet(data)
            else:
                fut = asyncio.run_coroutine_threadsafe(self.async_read_serial_data_llt(command), self.bt_loop)
                data = fut.result(timeout=15)
                return self.validate_packet(data)
        except Exception:
            return False

    async def async_read_serial_data_llt(self, command):
        try:
            # If we're inside the same event loop, await directly to avoid threadsafe deadlocks
            try:
                if asyncio.get_event_loop() is self.bt_loop:
                    data = await asyncio.wait_for(self.send_command(command), 12)
                    return data
            except Exception:
                pass
            bt_task = asyncio.run_coroutine_threadsafe(self.send_command(command), self.bt_loop)
            result = await asyncio.wait_for(asyncio.wrap_future(bt_task), 15)
            return result
        except Exception:
            return False

    def _attempt_reconnection(self) -> bool:
        """
        Attempt to reconnect to the BMS when connection is lost.
        Returns True if reconnection attempt was made, False if skipped (cooldown).
        """
        now = time.time()
        
        # Check cooldown period
        if now - self._last_reconnect_attempt < self._reconnect_cooldown:
            return False
        
        self._last_reconnect_attempt = now
        
        try:
            logger.warning(f"Connection lost ({self._consecutive_errors} consecutive errors), attempting reconnection...")
        except Exception:
            pass
        
        # Brief scan to refresh BlueZ cache before reconnecting
        # (unless we just handled an InProgress error, which means scan is already happening)
        try:
            import subprocess
            logger.info("LltJbd_FastBle: Running brief scan to refresh device cache...")
            # Start scan in background, wait 3 seconds, then stop it
            scan_proc = subprocess.Popen(['bluetoothctl', 'scan', 'on'], 
                                        stdout=subprocess.DEVNULL, 
                                        stderr=subprocess.DEVNULL)
            time.sleep(3.0)
            scan_proc.terminate()
            try:
                scan_proc.wait(timeout=1.0)
            except Exception:
                scan_proc.kill()
        except Exception as e:
            logger.debug(f"LltJbd_FastBle: Scan attempt: {e}")
        
        # Clear previous state
        self._notif_started = False
        self._dbus_signal_attached = False
        self._fast_signal_attached = False
        
        # Attempt to re-adopt the device
        dev_short = self.address.replace(":", "").lower()
        adapter = "hci0"
        success = self._dbus_try_adopt_connected_device(adapter, self.address)
        
        if success:
            try:
                logger.info("Reconnection successful!")
                self._consecutive_errors = 0
                self._is_connected = True
            except Exception:
                pass
        else:
            try:
                logger.warning("Reconnection attempt failed, will retry after cooldown")
            except Exception:
                pass
        
        return True

    # ---------------------- python-dbus GATT helpers ----------------------
    def _dbus_try_adopt_connected_device(self, adapter: str, target_addr: str) -> bool:
        try:
            from dbus.mainloop.glib import DBusGMainLoop
            DBusGMainLoop(set_as_default=True)
            import dbus
            self.dbus_bus = dbus.SystemBus()
            dev_path = f"/org/bluez/{adapter}/dev_" + target_addr.replace(":", "_")
            try:
                self._dbg(f"adopt: enter {adapter} dev={dev_path}")
            except Exception:
                pass
            
            # Check if device object exists (might have been removed after failed connection)
            try:
                dev = self.dbus_bus.get_object('org.bluez', dev_path)
                props = dbus.Interface(dev, 'org.freedesktop.DBus.Properties')
            except Exception as ex:
                try:
                    self._dbg(f"adopt: device not in cache: {ex}")
                except Exception:
                    pass
                return False  # Device not discovered yet, will retry after scan
            
            # Check connection status
            try:
                connected = bool(props.Get('org.bluez.Device1', 'Connected'))
                resolved = bool(props.Get('org.bluez.Device1', 'ServicesResolved'))
            except Exception as ex:
                try:
                    self._dbg(f"adopt: can't get device properties: {ex}")
                except Exception:
                    pass
                return False
            
            # If not connected, try to connect
            if not connected:
                try:
                    self._dbg(f"adopt: calling Device1.Connect()")
                    dev_iface = dbus.Interface(dev, 'org.bluez.Device1')
                    dev_iface.Connect()
                    self._dbg(f"adopt: Device1.Connect() completed")
                except Exception as ex:
                    try:
                        self._dbg(f"adopt: Device1.Connect() failed: {ex}, trying bluetoothctl")
                    except Exception:
                        pass
                    # Fallback to bluetoothctl
                    try:
                        import subprocess
                        result = subprocess.run(
                            ["bluetoothctl", "connect", target_addr],
                            capture_output=True,
                            text=True,
                            timeout=10
                        )
                        self._dbg(f"adopt: bluetoothctl connect result: {result.returncode}")
                    except Exception as ex2:
                        self._dbg(f"adopt: bluetoothctl connect failed: {ex2}")
                
                # Wait for connection and services to resolve
                import time
                deadline = time.time() + 10.0
                while time.time() < deadline:
                    try:
                        connected = bool(props.Get('org.bluez.Device1', 'Connected'))
                        resolved = bool(props.Get('org.bluez.Device1', 'ServicesResolved'))
                        if connected and resolved:
                            self._dbg(f"adopt: connected and resolved!")
                            break
                    except Exception:
                        pass
                    time.sleep(0.5)
            
            if not (connected and resolved):
                try:
                    self._dbg(f"adopt: not connected/resolved: connected={connected} resolved={resolved}")
                except Exception:
                    pass
                return False

            objmgr = dbus.Interface(self.dbus_bus.get_object('org.bluez', '/'), 'org.freedesktop.DBus.ObjectManager')
            managed = objmgr.GetManagedObjects()
            try:
                # Instrument: log discovered characteristics under device
                for path, ifaces in managed.items():
                    p = str(path)
                    if not p.startswith(dev_path + "/"):
                        continue
                    ch = ifaces.get('org.bluez.GattCharacteristic1')
                    if not ch:
                        continue
                    uuid = str(ch.get('UUID') or '').lower()
                    flags = [str(f) for f in (ch.get('Flags') or [])]
                    try:
                        logger.info(f"|  adopt: char {p} uuid={uuid} flags={flags}")
                    except Exception:
                        pass
            except Exception:
                pass
            rx_uuid_opts = ["0000ff01-0000-1000-8000-00805f9b34fb", "0000ffe1-0000-1000-8000-00805f9b34fb"]
            tx_uuid_opts = ["0000ff02-0000-1000-8000-00805f9b34fb", "0000ffe2-0000-1000-8000-00805f9b34fb"]
            rx_path = None
            tx_path = None
            notify_candidates = []
            write_candidates = []
            # Gather notify/write candidates across FF00/FFE0
            for path, ifaces in managed.items():
                p = str(path)
                if not p.startswith(dev_path + "/"):
                    continue
                ch = ifaces.get('org.bluez.GattCharacteristic1')
                if not ch:
                    continue
                uuid = str(ch.get('UUID') or '').lower()
                flags = [str(f) for f in (ch.get('Flags') or [])]
                if (uuid.startswith("0000ff") or uuid.startswith("0000ffe")):
                    if "notify" in flags:
                        notify_candidates.append((uuid, p))
                    if ("write-without-response" in flags or "write" in flags):
                        write_candidates.append((uuid, p))
            # Prefer FF00 over FFE*
            def pick(cands, prefs):
                for pref in prefs:
                    for u, pth in cands:
                        if u.startswith(pref):
                            return pth
                return cands[0][1] if cands else None
            # Prefer FF00 family first (ff01 notify, ff02 write), then FFE* fallback
            rx_path = pick(notify_candidates, ("0000ff01", "0000ffe1"))
            tx_path = pick(write_candidates, ("0000ff02", "0000ffe2"))
            # Determine alternates from other family based on known char ids if present
            alt_rx = None
            alt_tx = None
            try:
                if rx_path and tx_path:
                    if rx_path.endswith("/char0010") and tx_path.endswith("/char0014"):
                        # likely FF00; alt is FFE1/FFE2 if discovered
                        for u, pth in notify_candidates:
                            if u.startswith("0000ffe1"):
                                alt_rx = pth
                                break
                        for u, pth in write_candidates:
                            if u.startswith("0000ffe2"):
                                alt_tx = pth
                                break
                    else:
                        for u, pth in notify_candidates:
                            if u.startswith("0000ff01"):
                                alt_rx = pth
                                break
                        for u, pth in write_candidates:
                            if u.startswith("0000ff02"):
                                alt_tx = pth
                                break
            except Exception:
                pass
            # Fallback: by UUIDs if flags scan did not find both
            if not (rx_path and tx_path):
                for path, ifaces in managed.items():
                    p = str(path)
                    if not p.startswith(dev_path + "/"):
                        continue
                    ch = ifaces.get('org.bluez.GattCharacteristic1')
                    if not ch:
                        continue
                    uuid = str(ch.get('UUID') or '')
                    if (not rx_path) and uuid.lower() in rx_uuid_opts:
                        rx_path = p
                    if (not tx_path) and uuid.lower() in tx_uuid_opts:
                        tx_path = p
                    if rx_path and tx_path:
                        break
            try:
                self._dbg(f"adopt: Connected={connected} Resolved={resolved} on {adapter}")
            except Exception:
                pass
            if not (rx_path and tx_path):
                # Fallback: known JBD FF00 family characteristic paths
                base = dev_path + "/service000f"
                try:
                    test_rx = base + "/char0010"
                    test_tx = base + "/char0014"
                    _ = self.dbus_bus.get_object('org.bluez', test_rx)
                    _ = self.dbus_bus.get_object('org.bluez', test_tx)
                    rx_path, tx_path = test_rx, test_tx
                except Exception:
                    return False
            try:
                self._dbg(f"adopt: RX={rx_path} TX={tx_path}")
            except Exception:
                pass
            try:
                logger.info(f"|  adopt: RX={rx_path} TX={tx_path}")
            except Exception:
                pass
            self.dbus_dev_path = dev_path
            self.dbus_rx_char_path = rx_path
            self.dbus_tx_char_path = tx_path
            self._alt_rx_char_path = alt_rx
            self._alt_tx_char_path = alt_tx

            if not self._dbus_signal_attached:
                # Remove any old signal handlers first to prevent duplicates from reconnections
                try:
                    self.dbus_bus.remove_signal_receiver(
                        None,  # Remove all handlers
                        dbus_interface='org.freedesktop.DBus.Properties',
                        signal_name='PropertiesChanged',
                        path=self.dbus_rx_char_path,
                        arg0='org.bluez.GattCharacteristic1',
                    )
                except Exception:
                    pass  # No handlers to remove, that's OK
                
                def on_props_changed(interface, changed, invalidated):
                    try:
                        if interface == 'org.bluez.GattCharacteristic1' and 'Value' in changed:
                            raw = bytes(bytearray(changed['Value']))
                            self._dbus_rx_queue.append(raw)
                            try:
                                self._process_rx_chunk(raw)
                            except Exception:
                                pass
                            try:
                                logger.info(f"|  fast: py-dbus signal RX chunk {len(raw)} bytes HEX={raw.hex()}")
                            except Exception:
                                pass
                    except Exception:
                        pass

                self.dbus_bus.add_signal_receiver(
                    on_props_changed,
                    dbus_interface='org.freedesktop.DBus.Properties',
                    signal_name='PropertiesChanged',
                    path=self.dbus_rx_char_path,
                    arg0='org.bluez.GattCharacteristic1',
                )
                # Also listen on alternate RX path if present (some units send dd 03 there)
                # Subscribe to notifications on alternate RX (FFE1) if present
                # DISABLED: Subscribing to both FF01 and FFE1 causes duplicate notifications
                # if self._alt_rx_char_path:
                #     try:
                #         self.dbus_bus.add_signal_receiver(
                #             on_props_changed,
                #             dbus_interface='org.freedesktop.DBus.Properties',
                #             signal_name='PropertiesChanged',
                #             path=self._alt_rx_char_path,
                #             arg0='org.bluez.GattCharacteristic1',
                #         )
                #         self._dbus_rx_extra_path = self._alt_rx_char_path
                #     except Exception:
                #         self._dbus_rx_extra_path = None
                # else:
                #     self._dbus_rx_extra_path = None
                self._dbus_rx_extra_path = None
                self._dbus_signal_attached = True
                try:
                    self._dbg(f"adopt: signal attached on {self.dbus_rx_char_path}")
                except Exception:
                    pass

                # DON'T create our own GLib main loop - dbus-serialbattery.py creates one
                # Having multiple GLib loops causes crashes
                # Signals will dispatch once the main loop starts (after test_connection returns)
                try:
                    self._dbg("adopt: relying on main GLib loop for signal dispatch")
                except Exception:
                    pass

            rx_obj = self.dbus_bus.get_object('org.bluez', self.dbus_rx_char_path)
            rx_iface = dbus.Interface(rx_obj, 'org.bluez.GattCharacteristic1')
            try:
                rx_iface.StartNotify()
                try:
                    notifying = False
                    try:
                        props_i = dbus.Interface(rx_obj, 'org.freedesktop.DBus.Properties')
                        notifying = bool(props_i.Get('org.bluez.GattCharacteristic1', 'Notifying'))
                    except Exception:
                        notifying = False
                    logger.info(f"|  adopt: StartNotify primary OK notify={notifying}")
                    self._dbg("adopt: StartNotify OK")
                except Exception:
                    pass
            except Exception:
                try:
                    logger.info("|  adopt: StartNotify primary failed")
                except Exception:
                    pass
            # StartNotify on alternate RX if present (some firmwares notify on FFE1)
            # DISABLED: Enabling both FF01 and FFE1 causes duplicate notifications, corrupting frame assembly
            # Explicitly stop notifications on alternate to prevent duplicates from previous runs
            try:
                if self._alt_rx_char_path:
                    rx_obj2 = self.dbus_bus.get_object('org.bluez', self._alt_rx_char_path)
                    rx_iface2 = dbus.Interface(rx_obj2, 'org.bluez.GattCharacteristic1')
                    try:
                        rx_iface2.StopNotify()
                        logger.info("|  adopt: StopNotify on alternate RX to prevent duplicates")
                    except Exception:
                        pass  # Might not be started, that's OK
            except Exception:
                pass
            return True
        except Exception as ex:
            try:
                self._dbg(f"adopt: exception: {ex}")
                import traceback
                self._dbg(f"adopt: traceback: {traceback.format_exc()}")
            except Exception:
                pass
            return False

    # ---------------------- dbus-fast GATT helpers ----------------------
    async def _fast_try_adopt_connected_device(self, adapter: str, target_addr: str) -> bool:
        try:
            try:
                from dbus_fast.aio import MessageBus
                from dbus_fast import Message, MessageType, Variant
            except Exception:
                return False
            # Connect to system bus
            self._fast_bus = await MessageBus(bus_type=MessageBus.TYPE_SYSTEM).connect()
            dev_path = f"/org/bluez/{adapter}/dev_" + target_addr.replace(":", "_")

            # Check Connected and ServicesResolved
            get_connected = Message(
                destination="org.bluez",
                path=dev_path,
                interface="org.freedesktop.DBus.Properties",
                member="Get",
                signature="ss",
                body=["org.bluez.Device1", "Connected"],
            )
            r1 = await self._fast_bus.call(get_connected)
            if r1.message_type != MessageType.METHOD_RETURN:
                return False
            connected = bool(r1.body[0].value) if hasattr(r1.body[0], 'value') else bool(r1.body[0])

            get_resolved = Message(
                destination="org.bluez",
                path=dev_path,
                interface="org.freedesktop.DBus.Properties",
                member="Get",
                signature="ss",
                body=["org.bluez.Device1", "ServicesResolved"],
            )
            r2 = await self._fast_bus.call(get_resolved)
            if r2.message_type != MessageType.METHOD_RETURN:
                return False
            resolved = bool(r2.body[0].value) if hasattr(r2.body[0], 'value') else bool(r2.body[0])
            try:
                logger.info(f"|  fast: {dev_path} Connected={connected} Resolved={resolved}")
            except Exception:
                pass
            if not (connected and resolved):
                return False

            # Find RX/TX under device path
            get_managed = Message(
                destination="org.bluez",
                path="/",
                interface="org.freedesktop.DBus.ObjectManager",
                member="GetManagedObjects",
            )
            rep = await self._fast_bus.call(get_managed)
            if rep.message_type != MessageType.METHOD_RETURN:
                return False
            managed = rep.body[0]
            # Collect candidates and prefer FF00 family (ff01 notify, ff02 write), with FFE* as alternates
            notify_candidates = []  # list of (uuid, path)
            write_candidates = []   # list of (uuid, path)
            for path, ifaces in managed.items():
                p = str(path)
                if not p.startswith(dev_path + "/"):
                    continue
                ch = ifaces.get("org.bluez.GattCharacteristic1")
                if not ch:
                    continue
                uuid_v = ch.get("UUID")
                uuid_s = str(uuid_v.value if hasattr(uuid_v, 'value') else uuid_v).lower()
                flags_v = ch.get("Flags")
                # dbus-fast returns Variants; normalize to list[str]
                if hasattr(flags_v, 'value'):
                    flags_list = [str(f) for f in (flags_v.value or [])]
                else:
                    flags_list = [str(f) for f in (flags_v or [])]
                try:
                    logger.info(f"|  fast adopt: char {p} uuid={uuid_s} flags={flags_list}")
                except Exception:
                    pass
                if uuid_s in {"0000ff01-0000-1000-8000-00805f9b34fb", "0000ffe1-0000-1000-8000-00805f9b34fb"}:
                    if "notify" in flags_list:
                        notify_candidates.append((uuid_s, p))
                if uuid_s in {"0000ff02-0000-1000-8000-00805f9b34fb", "0000ffe2-0000-1000-8000-00805f9b34fb"}:
                    if ("write-without-response" in flags_list) or ("write" in flags_list):
                        write_candidates.append((uuid_s, p))

            def pick_primary(cands, family_first_prefix: str, family_alt_prefix: str):
                primary = None
                alt = None
                for u, pth in cands:
                    if u.startswith(family_first_prefix):
                        primary = pth if primary is None else primary
                for u, pth in cands:
                    if u.startswith(family_alt_prefix):
                        alt = pth if alt is None else alt
                # If no primary found, swap
                if primary is None and alt is not None:
                    primary, alt = alt, None
                return primary, alt

            rx_path, alt_rx = pick_primary(notify_candidates, "0000ff01", "0000ffe1")
            tx_path, alt_tx = pick_primary(write_candidates, "0000ff02", "0000ffe2")
            if not (rx_path and tx_path):
                return False
            if not (rx_path and tx_path):
                return False

            self.dbus_dev_path = dev_path
            self.dbus_rx_char_path = rx_path
            self.dbus_tx_char_path = tx_path
            # Save alternates if discovered
            self._alt_rx_char_path = alt_rx
            self._alt_tx_char_path = alt_tx
            # Prefer alternate family if available (matches certain JBD firmwares)
            try:
                if self._alt_rx_char_path and self._alt_tx_char_path:
                    self._active_family = "ffe"
            except Exception:
                pass
            try:
                logger.info(f"|  fast: RX={rx_path} TX={tx_path}")
            except Exception:
                pass
            # Prefer alternate family if available
            try:
                if self._alt_rx_char_path and self._alt_tx_char_path:
                    self._active_family = "ffe"
            except Exception:
                pass

            # Attach signal handler once
            if not self._fast_signal_attached:
                def on_msg(msg):
                    try:
                        if (
                            msg.message_type == MessageType.SIGNAL and
                            (msg.path == self.dbus_rx_char_path or (self._alt_rx_char_path and msg.path == self._alt_rx_char_path)) and
                            msg.interface == "org.freedesktop.DBus.Properties" and
                            msg.member == "PropertiesChanged"
                        ):
                            changed = msg.body[1]
                            if "Value" in changed:
                                val = changed["Value"].value if hasattr(changed["Value"], 'value') else changed["Value"]
                                try:
                                    data = bytes(val)
                                except Exception:
                                    data = bytes(bytearray(val))
                                self._dbus_rx_queue.append(data)
                                try:
                                    self._process_rx_chunk(data)
                                except Exception:
                                    pass
                                try:
                                    logger.info(f"|  fast: signal RX chunk {len(data)} bytes")
                                except Exception:
                                    pass
                    except Exception:
                        pass
                self._fast_bus.add_message_handler(on_msg)
                self._fast_signal_attached = True

            # Start notifications on primary RX
            start_msg = Message(
                destination="org.bluez",
                path=self.dbus_rx_char_path,
                interface="org.bluez.GattCharacteristic1",
                member="StartNotify",
            )
            await self._fast_bus.call(start_msg)
            try:
                logger.info("|  fast: StartNotify primary OK (dbus-fast)")
            except Exception:
                pass
            # Start notifications on alternate RX if present
            try:
                if self._alt_rx_char_path:
                    start_msg2 = Message(
                        destination="org.bluez",
                        path=self._alt_rx_char_path,
                        interface="org.bluez.GattCharacteristic1",
                        member="StartNotify",
                    )
                    await self._fast_bus.call(start_msg2)
                    try:
                        logger.info("|  fast: StartNotify alt OK (dbus-fast)")
                    except Exception:
                        pass
            except Exception:
                pass
            return True
        except Exception:
            return False

    def _restart_notifications_for_dd03(self) -> None:
        try:
            # Prefer python-dbus when available
            if self.dbus_bus is not None and self.dbus_rx_char_path:
                import dbus
                try:
                    # Stop on primary
                    rx_obj = self.dbus_bus.get_object('org.bluez', self.dbus_rx_char_path)
                    rx_iface = dbus.Interface(rx_obj, 'org.bluez.GattCharacteristic1')
                    try:
                        rx_iface.StopNotify()
                    except Exception:
                        pass
                except Exception:
                    pass
                # Start on alternate if present; otherwise restart primary
                try:
                    if self._alt_rx_char_path:
                        rx_obj2 = self.dbus_bus.get_object('org.bluez', self._alt_rx_char_path)
                        rx_iface2 = dbus.Interface(rx_obj2, 'org.bluez.GattCharacteristic1')
                        rx_iface2.StartNotify()
                        try:
                            logger.info("|  dd03: restarted notify on alternate RX (py-dbus)")
                        except Exception:
                            pass
                    else:
                        try:
                            rx_iface.StartNotify()
                            logger.info("|  dd03: restarted notify on primary RX (py-dbus)")
                        except Exception:
                            pass
                except Exception:
                    pass
                return
            # dbus-fast path
            if self._fast_bus is not None and self.dbus_rx_char_path:
                try:
                    from dbus_fast import Message
                    # Stop primary notify
                    stop_msg = Message(
                        destination="org.bluez",
                        path=self.dbus_rx_char_path,
                        interface="org.bluez.GattCharacteristic1",
                        member="StopNotify",
                    )
                    try:
                        fut = asyncio.run_coroutine_threadsafe(self._fast_bus.call(stop_msg), self.bt_loop)
                        fut.result(timeout=1)
                    except Exception:
                        pass
                    # Start alternate if present, otherwise primary
                    start_path = self._alt_rx_char_path or self.dbus_rx_char_path
                    start_msg = Message(
                        destination="org.bluez",
                        path=start_path,
                        interface="org.bluez.GattCharacteristic1",
                        member="StartNotify",
                    )
                    try:
                        fut2 = asyncio.run_coroutine_threadsafe(self._fast_bus.call(start_msg), self.bt_loop)
                        fut2.result(timeout=1)
                        try:
                            which = "alternate" if (self._alt_rx_char_path and start_path == self._alt_rx_char_path) else "primary"
                            logger.info(f"|  dd03: restarted notify on {which} RX (dbus-fast)")
                        except Exception:
                            pass
                    except Exception:
                        pass
                except Exception:
                    pass
                return
        except Exception:
            pass


if __name__ == "__main__":
    bat = LltJbd_FastBle("Foo", -1, sys.argv[1])
    if not bat.test_connection():
        logger.error(">>> ERROR: Unable to connect (fast)")
    else:
        bat.refresh_data()

