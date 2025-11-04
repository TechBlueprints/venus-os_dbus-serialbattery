# -*- coding: utf-8 -*-

from battery import Battery, Cell
import time
import json
import os
from utils_ble import Syncron_Ble
from utils import logger, BATTERY_CAPACITY, MAX_BATTERY_CHARGE_CURRENT, MAX_BATTERY_DISCHARGE_CURRENT


class NordicNus_Ble(Battery):
    BATTERYTYPE = "Nordic NUS BLE"

    # Nordic UART Service UUIDs
    NUS_RX_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"  # notifications from device
    NUS_TX_UUID = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"  # writes to device

    def __init__(self, port, baud, address):
        super(NordicNus_Ble, self).__init__(port, baud, address)
        self.type = self.BATTERYTYPE
        self.address = address
        self.poll_interval = 1000
        self.ble_handle = None
        self._nus_buf = bytearray()
        self._frame_min_len = 140
        self._last_frame_time = 0.0
        self._last_trigger_time = 0.0
        self._last_heartbeat_log = 0.0
        self._last_update_log_time = 0.0
        # Wake-up trigger backoff tracking
        self._wake_trigger_attempt = 0  # Track which wake-up attempt we're on
        self._wake_trigger_next_time = 0.0  # When to send the next wake-up trigger
        # Periodic reconnection to prevent nRF52 buffer buildup and deadlock
        self._connection_start_time = 0.0  # Track when current connection started
        self._planned_reconnect_interval = 300.0  # Reconnect every 5 minutes (300s)
        # Safe defaults until parsed from BMS (avoid div-by-zero in D-Bus publishing)
        # Start with 4 cells for this pack; we'll auto-detect real count from frames
        self.cell_count = 4
        self.capacity = BATTERY_CAPACITY if BATTERY_CAPACITY is not None else 0
        # Seed sane max currents so base control logic doesn't collapse to zero
        self.max_battery_charge_current = MAX_BATTERY_CHARGE_CURRENT
        self.max_battery_discharge_current = MAX_BATTERY_DISCHARGE_CURRENT
        # Pre-initialize cells and basic values to prevent internal calculation errors
        if len(self.cells) == 0:
            for _ in range(self.cell_count):
                self.cells.append(Cell(False))
        for c in range(len(self.cells)):
            self.cells[c].voltage = 3.2
        self.voltage = round(self.cell_count * 3.2, 2)
        self.current = 0.0
        self.soc = 50
        # Default to allowed until proven otherwise (BMS doesn't expose FET bits here)
        self.charge_fet = True
        self.discharge_fet = True
        # Optional extras
        self.heater_on = None
        # State persistence for graceful disconnection handling
        self._state_file = f"/tmp/serialbattery_state_{self.address.replace(':', '_').lower()}.json"
        self._last_good_state = {}
        self._last_state_save = 0.0
        self._state_save_interval = 5.0  # Save state every 5 seconds
        self._cached_data_max_age = 600.0  # 10 minutes
        self._last_reconnect_attempt = 0.0  # Track last reconnection attempt
        self._reconnect_cooldown = 120.0  # Start with 2 minutes between reconnection attempts (was 30s)
        self._reconnecting_in_progress = False  # Flag to prevent Bleak operations during scan/reconnect
        self._reconnect_failure_count = 0  # Track consecutive failures for exponential backoff
        self._max_reconnect_attempts = 3  # Max consecutive attempts before longer backoff
        self._load_cached_state()  # Load previous state on init
        logger.info("Init of NordicNus_Ble at " + address)

    def connection_name(self) -> str:
        return "BLE " + self.address

    def custom_name(self) -> str:
        return "SerialBattery(" + self.type + ") " + self.address[-5:]

    @property
    def unique_identifier(self) -> str:
        return self.address

    def _load_cached_state(self):
        """Load previously saved battery state from disk"""
        try:
            if os.path.exists(self._state_file):
                with open(self._state_file, 'r') as f:
                    self._last_good_state = json.load(f)
                    save_time = self._last_good_state.get('timestamp', 0)
                    age = time.time() - save_time
                    if age < self._cached_data_max_age:
                        logger.info(f"NordicNus: Loaded cached state from {age:.0f}s ago")
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
                        logger.info(f"NordicNus: Cached state too old ({age:.0f}s), ignoring")
        except Exception as e:
            logger.debug(f"NordicNus: Could not load cached state: {e}")

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
            logger.debug(f"NordicNus: Could not save state: {e}")

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
            logger.debug(f"NordicNus: Error restoring cached data: {e}")
        
        return True


    def test_connection(self):
        try:
            self.ble_handle = Syncron_Ble(self.address, read_characteristic=self.NUS_RX_UUID, write_characteristic=self.NUS_TX_UUID)
            ok = bool(self.ble_handle and self.ble_handle.connected)
            # Send initial trigger to start notifications, per app behavior
            if ok:
                # Mark connection start time for planned reconnections
                self._connection_start_time = time.time()
                try:
                    # Send initial trigger to start notifications
                    self._send_trigger()
                except Exception:
                    pass
                # Detect cell count from the first complete frame before D-Bus setup
                try:
                    scratch = bytearray()
                    deadline = time.time() + 2.0
                    while time.time() < deadline:
                        payload = self._pop_next_notification(timeout=0.2)
                        if payload and isinstance(payload, (bytes, bytearray)):
                            scratch.extend(payload)
                            start_idx = scratch.find(b":")
                            if start_idx == -1:
                                if len(scratch) > 256:
                                    scratch.clear()
                                continue
                            end_idx = scratch.find(b"~", start_idx + 1)
                            if end_idx == -1:
                                continue
                            frame = scratch[start_idx:end_idx + 1]
                            s = bytes(frame).decode(errors="ignore").replace("\r", "").replace("\n", "")
                            if not s.startswith(":") or not s.endswith("~"):
                                break
                            if len(s) < 90:
                                break
                            cell_hex = s[25:89]
                            vals = []
                            try:
                                for i in range(0, 64, 4):
                                    hi = int(cell_hex[i:i+2], 16)
                                    lo = int(cell_hex[i+2:i+4], 16)
                                    vals.append((hi << 8) | lo)
                            except Exception:
                                vals = []
                            if len(vals) == 16:
                                # Count contiguous non-zero cells from start; clamp to [4,16]
                                detected = 0
                                for mv in vals:
                                    if mv > 0:
                                        detected += 1
                                    else:
                                        break
                                if detected < 4:
                                    detected = 4
                                if detected > 16:
                                    detected = 16
                                if self.cell_count != detected:
                                    self.cell_count = detected
                                    if len(self.cells) < detected:
                                        for _ in range(detected - len(self.cells)):
                                            self.cells.append(Cell(False))
                                    elif len(self.cells) > detected:
                                        self.cells = self.cells[:detected]
                                break
                except Exception:
                    pass
            return ok
        except Exception as e:
            logger.error(f"NordicNus_Ble: connection error: {e}")
            return False

    def get_settings(self):
        # Ensure critical fields are initialized (keep conservative defaults)
        if getattr(self, "cell_count", None) in (None, 0):
            self.cell_count = 4
        # init cells if empty to avoid dbus crashes; will be resized on first real frame
        if len(self.cells) == 0:
            for _ in range(self.cell_count):
                self.cells.append(Cell(False))
        # Initialize placeholder voltages to avoid None in internal calculations
        for c in range(len(self.cells)):
            if getattr(self.cells[c], "voltage", None) is None:
                self.cells[c].voltage = 0.0
        if getattr(self, "capacity", None) is None:
            self.capacity = BATTERY_CAPACITY if BATTERY_CAPACITY is not None else 0
        if getattr(self, "max_battery_charge_current", None) is None:
            self.max_battery_charge_current = MAX_BATTERY_CHARGE_CURRENT
        if getattr(self, "max_battery_discharge_current", None) is None:
            self.max_battery_discharge_current = MAX_BATTERY_DISCHARGE_CURRENT
        return True

    def refresh_data(self):
        # If reconnection is in progress, wait the same time as a scan would take, then continue
        if self._reconnecting_in_progress:
            try:
                logger.debug("NordicNus: Reconnection in progress, waiting 4s (scan duration)...")
                time.sleep(4.0)  # Wait same duration as scan (3s scan + 1s settle)
            except Exception:
                pass
            # Continue with normal refresh after waiting - don't skip the cycle
        
        # Collect notifications and parse framed ASCII hex between ':' and '~' (length ~140)
        data_refreshed = False
        try:
            now_tick = time.time()
            if (now_tick - self._last_heartbeat_log) > 10.0:
                logger.debug("NUS refresh tick")
                self._last_heartbeat_log = now_tick
            
            # Planned reconnection every 5 minutes to prevent nRF52 buffer buildup
            # The nRF52 BLE chip can deadlock if buffers fill or connection stays open too long
            # This is a clean, controlled disconnect/reconnect cycle (not an error recovery)
            if self.ble_handle and self._connection_start_time > 0:
                time_connected = now_tick - self._connection_start_time
                if time_connected >= self._planned_reconnect_interval:
                    try:
                        logger.info(f"NordicNus: Planned reconnection after {int(time_connected)}s to prevent buffer buildup...")
                        # Clean disconnect
                        try:
                            if hasattr(self.ble_handle, 'disconnect'):
                                self.ble_handle.disconnect()
                        except Exception:
                            pass
                        self.ble_handle = None
                        
                        # Wait a few seconds to let nRF52 flush buffers
                        time.sleep(5.0)
                        
                        # Reconnect
                        from utils_ble import Syncron_Ble
                        try:
                            self.ble_handle = Syncron_Ble(self.address, read_characteristic=self.NUS_RX_UUID, write_characteristic=self.NUS_TX_UUID)
                            if self.ble_handle and self.ble_handle.connected:
                                logger.info(f"NordicNus: Planned reconnection successful!")
                                self._connection_start_time = time.time()  # Reset connection timer
                                self._last_frame_time = time.time()
                                self._last_trigger_time = time.time()
                                # Send initial trigger after reconnection
                                try:
                                    self._send_trigger()
                                except Exception:
                                    pass
                            else:
                                logger.warning(f"NordicNus: Planned reconnection failed, will retry in 5 minutes")
                                self._connection_start_time = time.time()  # Reset timer even on failure
                        except Exception as conn_err:
                            logger.warning(f"NordicNus: Planned reconnection exception: {conn_err}")
                            self._connection_start_time = time.time()  # Reset timer even on failure
                    except Exception as e:
                        logger.warning(f"NordicNus: Planned reconnection outer exception: {e}")
                        self._connection_start_time = time.time()  # Reset timer even on failure
            
            # Emergency reconnection only if we truly haven't received ANY data for 9 minutes
            # This is different from the planned reconnection above - this is error recovery
            # Mirror the app's behavior: NEVER disconnect under normal circumstances
            # The app does NOT have reconnection logic - it just sends triggers indefinitely
            # However, if we truly haven't received ANY data for 9 minutes (540s),
            # disconnect and reconnect as a last resort (keeps us inside the 10-minute cached data window)
            if self.ble_handle and (now_tick - self._last_frame_time) > 540.0:
                try:
                    logger.warning(f"NordicNus: No data for 9 minutes, attempting full reconnection as last resort...")
                    # Close old handle
                    try:
                        if hasattr(self.ble_handle, 'disconnect'):
                            self.ble_handle.disconnect()
                    except Exception:
                        pass
                    self.ble_handle = None
                    
                    # Brief scan to refresh BlueZ cache before reconnecting
                    try:
                        import subprocess
                        logger.info("NordicNus: Running brief scan to refresh device cache...")
                        scan_proc = subprocess.Popen(['bluetoothctl', 'scan', 'on'], 
                                                    stdout=subprocess.DEVNULL, 
                                                    stderr=subprocess.DEVNULL)
                        time.sleep(3.0)
                        scan_proc.terminate()
                        try:
                            scan_proc.wait(timeout=1.0)
                        except Exception:
                            scan_proc.kill()
                        time.sleep(1.0)
                    except Exception as e:
                        logger.debug(f"NordicNus: Scan attempt: {e}")
                    
                    # Reconnect
                    from utils_ble import Syncron_Ble
                    try:
                        self.ble_handle = Syncron_Ble(self.address, read_characteristic=self.NUS_RX_UUID, write_characteristic=self.NUS_TX_UUID)
                        if self.ble_handle and self.ble_handle.connected:
                            logger.warning(f"NordicNus: Reconnection successful after 9-minute timeout!")
                            self._last_frame_time = time.time()
                            self._last_trigger_time = time.time()
                            # Send initial trigger after reconnection
                            try:
                                self._send_trigger()
                            except Exception:
                                pass
                        else:
                            logger.warning(f"NordicNus: Reconnection failed, will retry in 9 minutes")
                    except Exception as conn_err:
                        logger.warning(f"NordicNus: Reconnection exception: {conn_err}")
                except Exception as e:
                    logger.warning(f"NordicNus: Reconnection outer exception: {e}")
            
            
            if self.ble_handle:
                # Get one notification chunk; assemble frames locally
                chunk = self._pop_next_notification(timeout=0.3)
                if chunk and isinstance(chunk, (bytes, bytearray)):
                    logger.debug(f"NUS got chunk: {len(chunk)} bytes")
                    self._nus_buf.extend(chunk)
                    # Process as many complete frames as present using resync & strict-length (140 bytes)
                    while True:
                        # If multiple headers present, keep only the latest ':' to resync
                        last_start = self._nus_buf.rfind(b":")
                        if last_start == -1:
                            if len(self._nus_buf) > 512:
                                self._nus_buf.clear()
                            break
                        if last_start > 0:
                            # discard everything before the latest header
                            del self._nus_buf[:last_start]
                        # Now search for footer after the (now at index 0) header
                        end_idx = self._nus_buf.find(b"~", 1)
                        if end_idx == -1:
                            # wait for more bytes
                            break
                        frame = self._nus_buf[: end_idx + 1]
                        # Enforce exact ASCII frame length (app uses 140 bytes including ':' and '~')
                        if len(frame) != 140:
                            # drop this invalid frame and continue scanning
                            del self._nus_buf[: end_idx + 1]
                            continue
                        # consume frame and parse
                        del self._nus_buf[: end_idx + 1]
                        logger.debug(f"parsing frame len={len(frame)} ascii={frame[:40]!r}")
                        self._parse_and_update(frame)
                        self._last_frame_time = time.time()
                        data_refreshed = True
                # Send trigger every 10 seconds to keep BMS awake
                # We're a persistent service (not an interactive app), so we can poll less frequently
                # The app uses 2 seconds for real-time UI updates, but 10 seconds is sufficient for monitoring
                now = time.time()
                if (now - self._last_trigger_time) >= 10.0:
                    try:
                        logger.debug("TX trigger :000250000E03~")
                        self._send_trigger()
                        self._last_trigger_time = now
                    except Exception as e:
                        logger.warning(f"NordicNus: Failed to send trigger: {e}")
            
            # Save state on successful data refresh
            if data_refreshed:
                self._save_cached_state()
            else:
                # No fresh data - check if we can use cached data
                if self._use_cached_data():
                    age = time.time() - self._last_good_state.get('timestamp', 0)
                    logger.info(f"NordicNus: Using cached data ({age:.0f}s old) during disconnection")
            
            # Provide placeholder values to avoid empty readings
            if getattr(self, "voltage", None) is None:
                self.voltage = 0.0
            if getattr(self, "current", None) is None:
                self.current = 0.0
            if getattr(self, "soc", None) is None:
                self.soc = 50
            # Ensure cells exist and have numeric voltages to prevent internal calc errors
            if len(self.cells) == 0:
                for _ in range(self.cell_count):
                    self.cells.append(Cell(False))
            for c in range(len(self.cells)):
                if getattr(self.cells[c], "voltage", None) is None:
                    self.cells[c].voltage = 0.0
            if getattr(self, "charge_fet", None) is None:
                self.charge_fet = True
            if getattr(self, "discharge_fet", None) is None:
                self.discharge_fet = True
        except Exception:
            # Keep driver alive on parsing issues
            pass
        return True

    def _parse_and_update(self, frame_bytes: bytearray):
        try:
            s = bytes(frame_bytes).decode(errors="ignore")
        except Exception:
            return

        # Enforce expected header/footer
        if not s.startswith(":") or "~" not in s:
            return

        # Helper to convert hex substrings into byte values
        def hex2int(h: str) -> int:
            try:
                return int(h, 16)
            except Exception:
                return 0

        def hex2bytes(hs: str):
            if hs is None or len(hs) % 2 != 0:
                return []
            out = []
            for i in range(0, len(hs), 2):
                out.append(hex2int(hs[i : i + 2]))
            return out

        # Map fields
        try:
            # Some frames may include CR/LF; strip them
            s = s.replace("\r", "").replace("\n", "")
            # Ensure we operate on the inner ASCII hex between ':' and '~'
            if s.startswith(":") and s.endswith("~"):
                pass
            else:
                start = s.find(":")
                end = s.rfind("~")
                if start != -1 and end != -1 and end > start:
                    s = s[start:end+1]
                else:
                    return
            # Enforce minimal, valid frame length before parsing fixed offsets
            if len(s) < self._frame_min_len:
                return

            # Helper for hex validation
            def is_hex_str(text: str) -> bool:
                try:
                    int(text, 16)
                    return True
                except Exception:
                    return False

            # Cells: str[25:89] -> 32 bytes -> up to 16 cell values (big-endian)
            cell_hex = s[25:89]
            # Validate exact length and hex-only for cell segment (32 bytes => 64 hex chars)
            if len(cell_hex) != 64 or not is_hex_str(cell_hex):
                return
            cell_bytes = hex2bytes(cell_hex)
            if len(cell_bytes) >= 32:
                # Parse all 16 registers
                full_values_mv = []
                for i in range(0, 32, 2):
                    v = (cell_bytes[i] << 8) + cell_bytes[i + 1]
                    full_values_mv.append(v)
                # Determine effective cells if not yet sized from test_connection
                if self.cell_count is None or self.cell_count < 1:
                    contiguous = 0
                    for mv in full_values_mv:
                        if mv > 0:
                            contiguous += 1
                        else:
                            break
                    self.cell_count = max(4, min(16, contiguous))
                # Ensure cells list sized
                if len(self.cells) < self.cell_count:
                    for _ in range(self.cell_count - len(self.cells)):
                        self.cells.append(Cell(False))
                elif len(self.cells) > self.cell_count:
                    self.cells = self.cells[: self.cell_count]
                # Choose a plausible consecutive window of cell_count values
                def is_plausible(mv: int) -> bool:
                    return 1500 <= mv <= 4500
                start_index = None
                for start in range(0, 16 - self.cell_count + 1):
                    window = full_values_mv[start : start + self.cell_count]
                    if all(is_plausible(v) for v in window):
                        start_index = start
                        break
                if start_index is None:
                    # No plausible window found; skip updating on this frame
                    return
                pack_mv = 0
                for idx in range(self.cell_count):
                    mv = full_values_mv[start_index + idx]
                    self.cells[idx].voltage = mv / 1000.0
                    pack_mv += mv
                # Validate pack voltage range for typical 4S or higher packs to avoid spikes
                pack_v = pack_mv / 1000.0
                if self.cell_count == 4 and not (8.0 <= pack_v <= 16.8):
                    return
                self.voltage = pack_v

            # Currents: str[89:97] -> 4 bytes (charge, discharge) in units of 10 mA
            cur_hex = s[89:97]
            if len(cur_hex) != 8 or not is_hex_str(cur_hex):
                cur_hex = ""
            cb = hex2bytes(cur_hex)
            if len(cb) >= 4:
                chg_ma = ((cb[0] << 8) + cb[1]) * 10
                dsg_ma = ((cb[2] << 8) + cb[3]) * 10
                net_ma = chg_ma - dsg_ma
                self.current = net_ma / 1000.0

            # Temperature: str[97:99] one byte, value - 40, clamp ≤ 120
            t_hex = s[97:99]
            if len(t_hex) != 2 or not is_hex_str(t_hex):
                return
            tem_c = hex2int(t_hex) - 40
            if tem_c > 120:
                tem_c = 120
            self.to_temperature(1, float(tem_c))

            # SoC: str[123:125]
            soc_hex = s[123:125]
            if len(soc_hex) != 2 or not is_hex_str(soc_hex):
                soc_hex = ""
            val_soc = hex2int(soc_hex)
            if 0 <= val_soc <= 100:
                self.soc = val_soc
                # Debug: log when we see 0% to investigate parsing
                if val_soc == 0:
                    try:
                        logger.warning(f"NordicNus: Parsed SOC=0% from frame, soc_hex='{soc_hex}', voltage={self.voltage}V, frame_snippet={s[120:130]}")
                    except Exception:
                        pass

            # Status code (2 bytes): str[105:109] -> keep as uppercase hex for reference
            try:
                status_hex = s[105:109].upper()
                if len(status_hex) == 4 and is_hex_str(status_hex):
                    self.bms_status_code = status_hex
            except Exception:
                pass

            # Heater flag + AddElectric + Cycle Count (5 bytes): str[109:119]
            # byte0: heater flag (non-zero => heater on)
            # byte1-2: AddElectric raw units (value * 10)
            # byte3-4: cycle count (big-endian)
            extra_hex = s[109:119]
            if len(extra_hex) == 10 and is_hex_str(extra_hex):
                eb = hex2bytes(extra_hex)
                if len(eb) >= 5:
                    try:
                        self.heater_on = bool(eb[0] != 0)
                        add_electric_raw = ((eb[1] << 8) + eb[2]) * 10
                        # Map to lifetime total Ah drawn (assume 10 mAh units -> Ah)
                        try:
                            self.history.total_ah_drawn = float(add_electric_raw) / 1000.0
                        except Exception:
                            pass
                        cycles = (eb[3] << 8) + eb[4]
                        # Publish cycles via history when available
                        try:
                            self.history.charge_cycles = int(cycles)
                        except Exception:
                            pass
                    except Exception:
                        pass

            # Capacity: str[133:137] -> 2 bytes, divide by 10 -> Ah
            cap_hex = s[133:137]
            if len(cap_hex) != 4 or not is_hex_str(cap_hex):
                cap_hex = ""
            cap_b = hex2bytes(cap_hex)
            if len(cap_b) >= 2:
                cap_ah = ((cap_b[0] << 8) + cap_b[1]) / 10.0
                # Cap to 1000 Ah as app does
                if cap_ah > 1000:
                    cap_ah = 1000
                self.capacity = float(cap_ah)

            # Keep FETs allowed unless explicit status bits are available (not provided by this BMS)
            now_ts = time.time()
            if (now_ts - self._last_update_log_time) > 12.0:
                logger.info(f"updated: V={self.voltage}V I={self.current}A SoC={self.soc}% cells={self.cell_count}")
                self._last_update_log_time = now_ts

        except Exception:
            # Ignore malformed frame
            return

    def _send_trigger(self):
        # App sends ASCII ":000250000E03~" to TX to request/keep data flowing
        if self.ble_handle:
            payloads = [b":000250000E03~", b":000250000E03~\r\n"]
            for p in payloads:
                self.ble_handle.send_data(p)

    def _pop_next_notification(self, timeout: float = 0.3):
        # Pop a single notification chunk from the BLE notify queue, waiting up to timeout
        if not self.ble_handle:
            return None
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if getattr(self.ble_handle, "_notification_queue", None) and len(self.ble_handle._notification_queue) > 0:
                    return self.ble_handle._notification_queue.popleft()
            except Exception:
                return None
            time.sleep(0.02)
        # Final attempt after timeout
        try:
            if getattr(self.ble_handle, "_notification_queue", None) and len(self.ble_handle._notification_queue) > 0:
                return self.ble_handle._notification_queue.popleft()
        except Exception:
            pass
        return None



