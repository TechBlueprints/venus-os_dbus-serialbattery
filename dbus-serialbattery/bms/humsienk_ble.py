# -*- coding: utf-8 -*-

from battery import Battery, Cell
import time
import json
import os
from utils_ble import Syncron_Ble
from utils import logger, BATTERY_CAPACITY, MAX_BATTERY_CHARGE_CURRENT, MAX_BATTERY_DISCHARGE_CURRENT


class HumsiENK_Ble(Battery):
    BATTERYTYPE = "HumsiENK BLE"

    # BLE Service and Characteristic UUIDs
    # Note: RX/TX from OUR perspective (app/driver), not device perspective
    BLE_SERVICE_UUID = "00000001-0000-1000-8000-00805f9b34fb"
    BLE_RX_UUID = "00000003-0000-1000-8000-00805f9b34fb"  # We receive notifications here (notify property)
    BLE_TX_UUID = "00000002-0000-1000-8000-00805f9b34fb"  # We write commands here (write property)

    # Protocol command constants
    CMD_HANDSHAKE = 0x00          # Initial handshake/connection
    CMD_STATUS = 0x20             # Operating status (FETs, temps, runtime)
    CMD_BATTERY_INFO = 0x21       # Battery info (voltage, current, SOC, SOH, capacity, cycles)
    CMD_CELL_VOLTAGES = 0x22      # Individual cell voltages
    CMD_FET_CHARGE = 0x50         # Charge FET control (data: [0]=off, [1]=on)
    CMD_FET_DISCHARGE = 0x51      # Discharge FET control (data: [0]=off, [1]=on)
    CMD_BALANCE = 0x52            # Balance control (data: [0]=off, [1]=on)
    CMD_CLEAR_STATUS = 0x53       # Clear error status
    CMD_CONFIG = 0x58             # Configuration parameters
    CMD_VERSION = 0xF5            # Firmware version (ASCII string)
    
    # Protocol framing bytes
    FRAME_START = 0xAA            # Command frame start byte
    FRAME_RESPONSE = 0xAA         # Response frame start byte (same as command, confirmed from live device)

    def __init__(self, port, baud, address):
        super(HumsiENK_Ble, self).__init__(port, baud, address)
        self.type = self.BATTERYTYPE
        self.address = address
        self.poll_interval = 1000
        self.ble_handle = None
        self._rx_buffer = bytearray()  # Receive buffer for assembling frames
        self._min_response_len = 5  # Minimum: [0xAA, CMD, LEN, CHK_LO, CHK_HI] = 5 bytes (zero data)
        self._last_frame_time = 0.0
        self._last_trigger_time = 0.0
        self._last_battery_info_time = 0.0
        self._last_cell_voltages_time = 0.0
        self._last_heartbeat_log = 0.0
        self._last_update_log_time = 0.0
        # Wake-up trigger backoff tracking
        self._wake_trigger_attempt = 0  # Track which wake-up attempt we're on
        self._wake_trigger_next_time = 0.0  # When to send the next wake-up trigger
        self._connection_start_time = 0.0  # Track when current connection started
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
        # Safe cell defaults until BMS reports real data; extra cells get trimmed
        # once the real cell count is detected from frames 0x22 or 0x58
        for c in range(len(self.cells)):
            self.cells[c].voltage = 3.4
        self.voltage = round(self.cell_count * 3.4, 2)  # 13.6V — avoids low-battery warnings on startup
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
        logger.info("Init of HumsiENK_Ble at " + address)

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
                        logger.info(f"HumsiENK: Loaded cached state from {age:.0f}s ago")
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
                        logger.info(f"HumsiENK: Cached state too old ({age:.0f}s), ignoring")
        except Exception as e:
            logger.debug(f"HumsiENK: Could not load cached state: {e}")

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
            logger.debug(f"HumsiENK: Could not save state: {e}")

    def _build_command(self, command: int, data: list = None) -> bytes:
        """
        Build a binary command packet according to protocol spec.
        
        Format: [0xAA, CMD, LEN, DATA..., CHK_LO, CHK_HI]
        - Start byte: 0xAA
        - Command: 1 byte
        - Length: 1 byte - length of DATA only (not including header/checksum)
        - Data: 0-N bytes (optional)
        - Checksum: 16-bit LE sum of bytes from CMD through end of DATA
        
        Examples:
            _build_command(0x21, []) → [0xAA, 0x21, 0x00, 0x21, 0x00]
            _build_command(0x50, [0x01]) → [0xAA, 0x50, 0x01, 0x01, 0x52, 0x00]
        """
        if data is None:
            data = []
        
        # Build payload: CMD, LEN, DATA...
        payload = [command, len(data) & 0xFF] + data
        
        # Checksum: 16-bit LE sum of payload bytes (CMD through end of DATA)
        crc = sum(payload) & 0xFFFF
        
        return bytes([self.FRAME_START] + payload + [crc & 0xFF, (crc >> 8) & 0xFF])

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
            logger.debug(f"HumsiENK: Error restoring cached data: {e}")
        
        return True


    def test_connection(self):
        try:
            self.ble_handle = Syncron_Ble(self.address, read_characteristic=self.BLE_RX_UUID, write_characteristic=self.BLE_TX_UUID)
            ok = bool(self.ble_handle and self.ble_handle.connected)
            # Send initial trigger to start notifications, per app behavior
            if ok:
                # Mark connection start time for planned reconnections
                self._connection_start_time = time.time()
                # Initialize frame time so the 9-minute emergency reconnect doesn't fire immediately
                self._last_frame_time = time.time()
                try:
                    # Send initial trigger to start notifications
                    self._send_trigger()
                except Exception:
                    pass
                
                # Send handshake command (0x00) and wait for response
                try:
                    handshake_cmd = self._build_command(self.CMD_HANDSHAKE, [])
                    self.ble_handle.send_data(handshake_cmd)
                    time.sleep(0.5)
                except Exception as e:
                    logger.debug(f"HumsiENK: Handshake send failed: {e}")
                
                # Send cell voltage command (0x22) to detect cell count
                try:
                    cell_cmd = self._build_command(self.CMD_CELL_VOLTAGES, [])
                    self.ble_handle.send_data(cell_cmd)
                    logger.debug("HumsiENK: Sent cell voltage request for detection")
                except Exception as e:
                    logger.debug(f"HumsiENK: Cell voltage command failed: {e}")
                
                # Collect responses for up to 3 seconds to detect cell count
                deadline = time.time() + 3.0
                detected_cells = None
                while time.time() < deadline:
                    chunk = self._pop_next_notification(timeout=0.3)
                    if chunk and isinstance(chunk, (bytes, bytearray)):
                        self._rx_buffer.extend(chunk)
                        
                        # Try to find and parse a 0x22 response
                        start_idx = self._rx_buffer.find(self.FRAME_RESPONSE)
                        if start_idx >= 0 and len(self._rx_buffer) >= start_idx + 5:
                            cmd = self._rx_buffer[start_idx + 1]
                            if cmd == self.CMD_CELL_VOLTAGES:
                                # Parse length (1 byte)
                                data_len = self._rx_buffer[start_idx + 2]
                                total_len = 3 + data_len + 2  # header(3) + data + checksum(2)
                                
                                if len(self._rx_buffer) >= start_idx + total_len:
                                    # Extract frame
                                    frame = bytes(self._rx_buffer[start_idx:start_idx + total_len])
                                    data = frame[3:3+data_len]
                                    
                                    # Count cells
                                    cell_count_detected = 0
                                    for i in range(0, min(len(data), 48), 2):
                                        if i + 1 < len(data):
                                            cell_mv = int.from_bytes(data[i:i+2], byteorder='little', signed=False)
                                            if 1000 <= cell_mv <= 5000:
                                                cell_count_detected += 1
                                            elif cell_count_detected > 0:
                                                break
                                    
                                    if cell_count_detected >= 4:
                                        detected_cells = cell_count_detected
                                        logger.info(f"HumsiENK: Detected {detected_cells} cells during connection")
                                        break
                    
                    time.sleep(0.1)
                
                # Update cell count if detected
                if detected_cells is not None:
                    self.cell_count = detected_cells
                    # Resize cells list
                    if len(self.cells) < self.cell_count:
                        for _ in range(self.cell_count - len(self.cells)):
                            self.cells.append(Cell(False))
                    elif len(self.cells) > self.cell_count:
                        self.cells = self.cells[:self.cell_count]
                
                # Clear buffer after detection
                self._rx_buffer.clear()
                
            return ok
        except Exception as e:
            logger.error(f"HumsiENK_Ble: connection error: {e}")
            return False

    def get_settings(self):
        # Ensure critical fields are initialized (keep conservative defaults)
        if getattr(self, "cell_count", None) in (None, 0):
            self.cell_count = 4
        # init cells if empty to avoid dbus crashes; will be resized on first real frame
        if len(self.cells) == 0:
            for _ in range(self.cell_count):
                self.cells.append(Cell(False))
        # Safe cell defaults for any newly added cells (trimmed once real count is known)
        for c in range(len(self.cells)):
            if getattr(self.cells[c], "voltage", None) is None:
                self.cells[c].voltage = 3.4
        if getattr(self, "capacity", None) is None:
            self.capacity = BATTERY_CAPACITY if BATTERY_CAPACITY is not None else 0
        if getattr(self, "max_battery_charge_current", None) is None:
            self.max_battery_charge_current = MAX_BATTERY_CHARGE_CURRENT
        if getattr(self, "max_battery_discharge_current", None) is None:
            self.max_battery_discharge_current = MAX_BATTERY_DISCHARGE_CURRENT
        
        # Request configuration data from BMS (protection limits, capacity, etc.)
        # This is called once at startup to get BMS settings
        if self.ble_handle and self.ble_handle.connected:
            try:
                logger.debug("HumsiENK: Requesting configuration data (0x58)...")
                config_cmd = self._build_command(self.CMD_CONFIG, [])
                self.ble_handle.send_data(config_cmd)
                
                # Wait for and process response
                time.sleep(0.5)
                
                # Try to get the config response
                for _ in range(10):  # Try up to 10 times (3 seconds total)
                    chunk = self._pop_next_notification(timeout=0.3)
                    if chunk and isinstance(chunk, (bytes, bytearray)):
                        self._rx_buffer.extend(chunk)
                        
                        # Look for config response (0x58)
                        start_idx = self._rx_buffer.find(self.FRAME_RESPONSE)
                        if start_idx >= 0 and len(self._rx_buffer) >= start_idx + 5:
                            cmd = self._rx_buffer[start_idx + 1]
                            if cmd == self.CMD_CONFIG:
                                # Parse length (1 byte)
                                data_len = self._rx_buffer[start_idx + 2]
                                total_len = 3 + data_len + 2  # header(3) + data + checksum(2)
                                
                                if len(self._rx_buffer) >= start_idx + total_len:
                                    # Extract and parse frame
                                    frame = bytes(self._rx_buffer[start_idx:start_idx + total_len])
                                    self._parse_and_update(frame)
                                    # Clear buffer after successful parse
                                    self._rx_buffer.clear()
                                    logger.info("HumsiENK: Configuration loaded successfully")
                                    break
                
                # Request firmware version (0xF5)
                logger.debug("HumsiENK: Requesting firmware version (0xF5)...")
                version_cmd = self._build_command(self.CMD_VERSION, [])
                self.ble_handle.send_data(version_cmd)
                
                # Wait for and process response
                time.sleep(0.5)
                
                # Try to get the version response
                for _ in range(10):  # Try up to 10 times (3 seconds total)
                    chunk = self._pop_next_notification(timeout=0.3)
                    if chunk and isinstance(chunk, (bytes, bytearray)):
                        self._rx_buffer.extend(chunk)
                        
                        # Look for version response (0xF5)
                        start_idx = self._rx_buffer.find(self.FRAME_RESPONSE)
                        if start_idx >= 0 and len(self._rx_buffer) >= start_idx + 5:
                            cmd = self._rx_buffer[start_idx + 1]
                            if cmd == self.CMD_VERSION:
                                # Parse length (1 byte)
                                data_len = self._rx_buffer[start_idx + 2]
                                total_len = 3 + data_len + 2  # header(3) + data + checksum(2)
                                
                                if len(self._rx_buffer) >= start_idx + total_len:
                                    # Extract and parse frame
                                    frame = bytes(self._rx_buffer[start_idx:start_idx + total_len])
                                    self._parse_and_update(frame)
                                    # Clear buffer after successful parse
                                    self._rx_buffer.clear()
                                    logger.info("HumsiENK: Firmware version loaded successfully")
                                    break
                
            except Exception as e:
                logger.warning(f"HumsiENK: Failed to request configuration: {e}")
        
        return True

    def refresh_data(self):
        # If reconnection is in progress, wait the same time as a scan would take, then continue
        if self._reconnecting_in_progress:
            try:
                logger.debug("HumsiENK: Reconnection in progress, waiting 4s (scan duration)...")
                time.sleep(4.0)  # Wait same duration as scan (3s scan + 1s settle)
            except Exception:
                pass
            # Continue with normal refresh after waiting - don't skip the cycle
        
        # Collect binary response frames and parse commands 0x21, 0x22, 0x20
        data_refreshed = False
        try:
            now_tick = time.time()
            if (now_tick - self._last_heartbeat_log) > 10.0:
                logger.debug("HumsiENK refresh tick")
                self._last_heartbeat_log = now_tick
            
            # Emergency reconnection only if we truly haven't received ANY data for 9 minutes
            # This is error recovery, not a routine maintenance cycle
            # Mirror the app's behavior: NEVER disconnect under normal circumstances
            # The app does NOT have reconnection logic - it just sends triggers indefinitely
            # However, if we truly haven't received ANY data for 9 minutes (540s),
            # disconnect and reconnect as a last resort (keeps us inside the 10-minute cached data window)
            if self.ble_handle and (now_tick - self._last_frame_time) > 540.0:
                try:
                    logger.warning(f"HumsiENK: No data for 9 minutes, attempting full reconnection as last resort...")
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
                        logger.info("HumsiENK: Running brief scan to refresh device cache...")
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
                        logger.debug(f"HumsiENK: Scan attempt: {e}")
                    
                    # Reconnect
                    from utils_ble import Syncron_Ble
                    try:
                        self.ble_handle = Syncron_Ble(self.address, read_characteristic=self.BLE_RX_UUID, write_characteristic=self.BLE_TX_UUID)
                        if self.ble_handle and self.ble_handle.connected:
                            logger.warning(f"HumsiENK: Reconnection successful after 9-minute timeout!")
                            self._last_frame_time = time.time()
                            self._last_trigger_time = time.time()
                            self._last_battery_info_time = time.time()
                            self._last_cell_voltages_time = time.time()
                            # Send initial trigger after reconnection
                            try:
                                self._send_trigger()
                            except Exception:
                                pass
                        else:
                            logger.warning(f"HumsiENK: Reconnection failed, will retry in 9 minutes")
                    except Exception as conn_err:
                        logger.warning(f"HumsiENK: Reconnection exception: {conn_err}")
                except Exception as e:
                    logger.warning(f"HumsiENK: Reconnection outer exception: {e}")
            
            
            if self.ble_handle:
                # Get one notification chunk; assemble frames locally
                chunk = self._pop_next_notification(timeout=0.3)
                if chunk and isinstance(chunk, (bytes, bytearray)):
                    logger.debug(f"HumsiENK got chunk: {len(chunk)} bytes")
                    
                    # Buffer overflow protection: if buffer is already too large and has no valid frame start,
                    # clear it before adding new data
                    if len(self._rx_buffer) > 512:
                        # Since FRAME_RESPONSE is 0xAA which is common, check for a valid CMD byte too
                        has_valid_frame = False
                        search_pos = 0
                        while True:
                            idx = self._rx_buffer.find(self.FRAME_RESPONSE, search_pos)
                            if idx == -1 or idx + 3 > len(self._rx_buffer):
                                break
                            # Check if byte after 0xAA looks like a valid command
                            cmd_byte = self._rx_buffer[idx + 1]
                            if cmd_byte in (0x00, 0x20, 0x21, 0x22, 0x58, 0xF5):
                                has_valid_frame = True
                                break
                            search_pos = idx + 1
                        if not has_valid_frame:
                            logger.debug(f"HumsiENK: Clearing oversized buffer ({len(self._rx_buffer)} bytes) with no valid frame")
                            self._rx_buffer.clear()
                    
                    self._rx_buffer.extend(chunk)
                    
                    # Process as many complete binary frames as present
                    # Frame format: [0xAA, CMD, LEN, DATA..., CHK_LO, CHK_HI]
                    while True:
                        # Need at least 5 bytes: [0xAA, CMD, LEN] + 2-byte checksum
                        if len(self._rx_buffer) < 5:
                            break
                        
                        # Look for frame start byte 0xAA
                        start_idx = self._rx_buffer.find(self.FRAME_RESPONSE)
                        if start_idx == -1:
                            # No frame start found, clear old junk if buffer too large
                            if len(self._rx_buffer) > 512:
                                self._rx_buffer.clear()
                            break
                        
                        # Discard bytes before frame start
                        if start_idx > 0:
                            del self._rx_buffer[:start_idx]
                        
                        # Now buffer starts with 0xAA
                        # Need at least 5 bytes: [0xAA, CMD, LEN, CHK_LO, CHK_HI]
                        if len(self._rx_buffer) < 5:
                            break
                        
                        # Parse length field (1 byte at position 2)
                        data_len = self._rx_buffer[2]
                        
                        # Sanity check: reasonable data length (0-255 bytes)
                        if data_len > 200:
                            # Invalid length, discard this start byte and resync
                            logger.debug(f"HumsiENK: Invalid data_len={data_len}, resyncing")
                            del self._rx_buffer[0:1]
                            continue
                        
                        # Calculate total frame length: header(3) + data + checksum(2)
                        total_len = 3 + data_len + 2
                        
                        # Wait for complete frame
                        if len(self._rx_buffer) < total_len:
                            break
                        
                        # Extract complete frame
                        frame = bytes(self._rx_buffer[:total_len])
                        
                        # Validate checksum: 16-bit LE sum of bytes from CMD to end of DATA
                        # (excludes start byte 0xAA and the checksum itself)
                        payload = frame[1:-2]  # CMD through end of data
                        calculated_checksum = sum(payload) & 0xFFFF
                        received_checksum = frame[-2] | (frame[-1] << 8)  # Little-endian
                        
                        if calculated_checksum != received_checksum:
                            logger.warning(f"HumsiENK: Checksum mismatch! Calculated: {calculated_checksum:04X}, Received: {received_checksum:04X}")
                            # Discard this start byte and try to resync
                            del self._rx_buffer[0:1]
                            continue
                        
                        # Valid frame - consume it and parse
                        del self._rx_buffer[:total_len]
                        logger.debug(f"HumsiENK: Valid frame received, cmd={frame[1]:02X}, len={data_len}")
                        self._parse_and_update(frame)
                        self._last_frame_time = time.time()
                        data_refreshed = True
                # Send trigger every 10 seconds to keep BMS awake
                # We're a persistent service (not an interactive app), so we can poll less frequently
                # The app uses 2 seconds for real-time UI updates, but 10 seconds is sufficient for monitoring
                now = time.time()
                # Send periodic commands to keep connection alive and get fresh data
                now = time.time()
                
                # Status command (0x20) every 2 seconds - keep-alive
                if (now - self._last_trigger_time) >= 2.0:
                    try:
                        logger.debug("TX CMD_STATUS (0x20) keep-alive")
                        cmd = self._build_command(self.CMD_STATUS, [])
                        self.ble_handle.send_data(cmd)
                        self._last_trigger_time = now
                    except Exception as e:
                        logger.warning(f"HumsiENK: Failed to send status command: {e}")
                
                # Battery info command (0x21) every 10 seconds
                if (now - self._last_battery_info_time) >= 10.0:
                    try:
                        logger.debug("TX CMD_BATTERY_INFO (0x21)")
                        cmd = self._build_command(self.CMD_BATTERY_INFO, [])
                        self.ble_handle.send_data(cmd)
                        self._last_battery_info_time = now
                    except Exception as e:
                        logger.warning(f"HumsiENK: Failed to send battery info command: {e}")
                
                # Cell voltages command (0x22) every 30 seconds
                if (now - self._last_cell_voltages_time) >= 30.0:
                    try:
                        logger.debug("TX CMD_CELL_VOLTAGES (0x22)")
                        cmd = self._build_command(self.CMD_CELL_VOLTAGES, [])
                        self.ble_handle.send_data(cmd)
                        self._last_cell_voltages_time = now
                    except Exception as e:
                        logger.warning(f"HumsiENK: Failed to send cell voltages command: {e}")
            
            # Save state on successful data refresh
            if data_refreshed:
                self._save_cached_state()
            else:
                # No fresh data this poll cycle — normal between BLE command/response rounds.
                # Use last-known-good values so dbus always has something to report.
                if self._use_cached_data():
                    age = time.time() - self._last_good_state.get('timestamp', 0)
                    msg = f"HumsiENK: No new data this cycle, using cached values ({age:.0f}s old)"
                    if age >= 60:
                        logger.error(msg)
                    elif age >= 30:
                        logger.warning(msg)
                    elif age >= 15:
                        logger.info(msg)
                    else:
                        logger.debug(msg)
            
            # Provide placeholder values to avoid empty readings (13.6V avoids low-battery warnings)
            if getattr(self, "voltage", None) is None:
                self.voltage = round(self.cell_count * 3.4, 2)
            if getattr(self, "current", None) is None:
                self.current = 0.0
            if getattr(self, "soc", None) is None:
                self.soc = 50
            # Ensure cells exist and have numeric voltages to prevent internal calc errors
            if len(self.cells) == 0:
                for _ in range(self.cell_count):
                    self.cells.append(Cell(False))
            # Safe cell defaults for any newly added cells (trimmed once real count is known)
            for c in range(len(self.cells)):
                if getattr(self.cells[c], "voltage", None) is None:
                    self.cells[c].voltage = 3.4
            if getattr(self, "charge_fet", None) is None:
                self.charge_fet = True
            if getattr(self, "discharge_fet", None) is None:
                self.discharge_fet = True
        except Exception:
            # Keep driver alive on parsing issues
            pass
        return True

    def _parse_and_update(self, frame: bytes):
        """
        Parse binary response frame and update battery state.
        
        Frame format: [0xAA, CMD, LEN, DATA..., CHK_LO, CHK_HI]
        - Header: 3 bytes (start byte, command, data length)
        - Data: LEN bytes
        - Checksum: 2 bytes LE (sum of bytes from CMD to end of data)
        """
        try:
            if len(frame) < 5:
                return
            
            cmd = frame[1]
            data_len = frame[2]
            data = frame[3:3+data_len]
            
            logger.debug(f"HumsiENK: Parsing response CMD={cmd:02X}, data_len={data_len}")
            
            # Route to appropriate parser based on command
            if cmd == self.CMD_BATTERY_INFO:
                self._parse_battery_info(data)
            elif cmd == self.CMD_CELL_VOLTAGES:
                self._parse_cell_voltages(data)
            elif cmd == self.CMD_STATUS:
                self._parse_status(data)
            elif cmd == self.CMD_CONFIG:
                self._parse_config(data)
            elif cmd == self.CMD_VERSION:
                self._parse_version(data)
            else:
                logger.debug(f"HumsiENK: Unknown command response {cmd:02X}")
                
        except Exception as e:
            logger.warning(f"HumsiENK: Parse error: {e}")
    
    def _parse_battery_info(self, data: bytes):
        """
        Parse command 0x21 response (Battery Info) - 26 bytes.
        
        Field layout (confirmed via live device testing):
            vol (4 bytes): Battery voltage in mV (millivolts)
            ele (4 bytes): Current in mA (milliamps), signed 32-bit
            SOC (1 byte): State of Charge %
            SOH (1 byte): State of Health %
            capacity (4 bytes): Remaining capacity in mAh
            allCapacity (4 bytes): Total capacity in mAh
            circulate (2 bytes): Charge cycle count
            t1 (1 byte): Cell temperature 1 in °C (direct)
            t2 (1 byte): Cell temperature 2 in °C (direct)
            t3 (1 byte): Cell temperature 3 in °C (direct)
            t4 (1 byte): Cell temperature 4 in °C (direct)
            MOS (1 byte): MOSFET temperature in °C (direct)
            environment (1 byte): Environment temperature in °C (direct)
        
        Note: Charge/discharge FET status is NOT in this response.
        FET status comes from the 0x20 status command (operation_status bits 7/23).
        """
        try:
            if len(data) < 26:
                logger.warning(f"HumsiENK: Battery info data too short: {len(data)} bytes")
                return
            
            idx = 0
            
            # Voltage (4 bytes, millivolts → divide by 1000 for V)
            voltage_raw = int.from_bytes(data[idx:idx+4], byteorder='little', signed=False)
            self.voltage = voltage_raw / 1000.0
            idx += 4
            
            # Current (4 bytes, milliamps signed → divide by 1000 for A)
            # Signed 32-bit: values > 2^31 are negative (two's complement)
            current_raw = int.from_bytes(data[idx:idx+4], byteorder='little', signed=True)
            self.current = current_raw / 1000.0
            idx += 4
            
            # SOC (1 byte, %)
            self.soc = data[idx]
            idx += 1
            
            # SOH (1 byte, %)
            self.soh = data[idx] if 0 <= data[idx] <= 100 else None
            idx += 1
            
            # Remaining capacity (4 bytes, mAh → divide by 1000 for Ah)
            # Raw value in mAh; divide by 1000 for Ah
            capacity_raw = int.from_bytes(data[idx:idx+4], byteorder='little', signed=False)
            self.capacity_remain = capacity_raw / 1000.0
            idx += 4
            
            # Total capacity (4 bytes, mAh → divide by 1000 for Ah)
            # Raw value in mAh; divide by 1000 for Ah
            total_capacity_raw = int.from_bytes(data[idx:idx+4], byteorder='little', signed=False)
            if total_capacity_raw > 0:
                self.capacity = total_capacity_raw / 1000.0
            idx += 4
            
            # Cycles (2 bytes)
            self.cycles = int.from_bytes(data[idx:idx+2], byteorder='little', signed=False)
            idx += 2
            
            # Temperatures (6 × 1 byte, direct °C, signed byte)
            # Fields: t1, t2, t3, t4, MOS, environment (each 1 byte)
            # Signed byte: values > 127 are negative (for sub-zero temps)
            def _signed_byte(b):
                return b if b < 128 else b - 256
            
            if idx < len(data):
                self.temperature_1 = float(_signed_byte(data[idx]))
                idx += 1
            if idx < len(data):
                self.temperature_2 = float(_signed_byte(data[idx]))
                idx += 1
            if idx < len(data):
                self.temperature_3 = float(_signed_byte(data[idx]))
                idx += 1
            if idx < len(data):
                self.temperature_4 = float(_signed_byte(data[idx]))
                idx += 1
            if idx < len(data):
                self.temperature_mos = float(_signed_byte(data[idx]))
                idx += 1
            if idx < len(data):
                # Environment temp — not mapped to D-Bus (base class supports 1-4 + MOS)
                # but stored for diagnostics / future use
                self.temperature_env = float(_signed_byte(data[idx]))
                idx += 1
            
            logger.debug(f"HumsiENK: Battery info: {self.voltage:.2f}V, {self.current:.2f}A, {self.soc}%, "
                        f"{self.capacity_remain:.1f}/{self.capacity:.1f}Ah, {self.cycles} cycles")
            
        except Exception as e:
            logger.warning(f"HumsiENK: Error parsing battery info: {e}")
    
    def _parse_cell_voltages(self, data: bytes):
        """
        Parse command 0x22 response (Cell Voltages) - up to 48 bytes.
        
        Each cell voltage is 2 bytes (little-endian):
        - 24 cell slots maximum
        - Value in millivolts (e.g., 3300 = 3.300V)
        - Non-zero values indicate active cells
        """
        try:
            if len(data) < 2:
                return
            
            # Determine number of cells from non-zero values
            cell_voltages_mv = []
            for i in range(0, min(len(data), 48), 2):
                if i + 1 < len(data):
                    cell_mv = int.from_bytes(data[i:i+2], byteorder='little', signed=False)
                    cell_voltages_mv.append(cell_mv)
            
            # Count non-zero cells (assuming they're contiguous from start)
            detected_cell_count = 0
            for mv in cell_voltages_mv:
                if mv > 0 and 1000 <= mv <= 5000:  # Reasonable LiFePO4/Li-ion range
                    detected_cell_count += 1
                elif detected_cell_count > 0:
                    # Stop at first zero after non-zero cells
                    break
            
            # Update cell count if different (auto-detection)
            if detected_cell_count > 0 and detected_cell_count != self.cell_count:
                logger.info(f"HumsiENK: Detected {detected_cell_count} cells (was {self.cell_count})")
                self.cell_count = detected_cell_count
            
            # Ensure cells list is properly sized
            if len(self.cells) < self.cell_count:
                for _ in range(self.cell_count - len(self.cells)):
                    self.cells.append(Cell(False))
            elif len(self.cells) > self.cell_count:
                self.cells = self.cells[:self.cell_count]
            
            # Update cell voltages
            for idx in range(min(self.cell_count, len(cell_voltages_mv))):
                mv = cell_voltages_mv[idx]
                if 1000 <= mv <= 5000:  # Sanity check
                    self.cells[idx].voltage = mv / 1000.0
            
            # Note: pack voltage is reported directly by the BMS in 0x21 and is more
            # accurate than summing individual cell readings (which accumulate rounding errors).
            # Do NOT overwrite self.voltage here.
            
            logger.debug(f"HumsiENK: Cell voltages: {[c.voltage for c in self.cells[:min(4, self.cell_count)]]}")
            
        except Exception as e:
            logger.warning(f"HumsiENK: Error parsing cell voltages: {e}")
    
    def _parse_status(self, data: bytes):
        """
        Parse command 0x20 response (Operating Status) - 14 bytes.
        
        Data map:
            Bytes 0-3: Runtime (days:2, hours:1, minutes:1)
            Bytes 4-7: Operation status flags (32-bit) - See alarm bit mapping below
            Bytes 8-10: Cell balance status (24-bit bitmap)
            Bytes 11-13: Cell disconnect status (24-bit bitmap)
        
        Operation Status Alarm/Protection Bits (confirmed via live device testing):
        
        CHARGE SECTION (Bits 0-15):
          Bit 0:  Charge overcurrent protection
          Bit 1:  Charge over-temperature protection
          Bit 2:  Charge under-temperature protection
          Bit 4:  Pack overvoltage protection
          Bit 7:  Charge FET status (1=on, 0=off)
          Bit 8:  Charge overcurrent warning
          Bit 9:  Charge over-temperature warning
          Bit 10: Charge under-temperature warning
          Bit 12: Pack overvoltage warning
          Bit 15: Balance active (1=yes, 0=no)
        
        DISCHARGE SECTION (Bits 16-31):
          Bit 16: Discharge overcurrent protection
          Bit 17: Discharge over-temperature protection
          Bit 18: Discharge under-temperature protection
          Bit 20: Short circuit protection
          Bit 21: Pack undervoltage protection
          Bit 22: (Unknown alarm)
          Bit 23: Discharge FET status (1=on, 0=off)
          Bit 24: Discharge overcurrent warning
          Bit 25: Discharge over-temperature warning
          Bit 26: Discharge under-temperature warning
          Bit 28: Pack undervoltage warning
          Bit 29: MOS over-temperature warning
          Bit 30: MOS over-temperature protection
        """
        try:
            if len(data) < 14:
                logger.debug(f"HumsiENK: Status data too short: {len(data)} bytes")
                return
            
            # Runtime (bytes 0-3)
            runtime_days = int.from_bytes(data[0:2], byteorder='little', signed=False)
            runtime_hours = data[2]
            runtime_minutes = data[3]
            runtime_seconds = ((runtime_days * 24 + runtime_hours) * 60 + runtime_minutes) * 60
            self.runtime_seconds = runtime_seconds
            
            # Operation status (bytes 4-7) - 32-bit field
            operation_status = int.from_bytes(data[4:8], byteorder='little', signed=False)
            
            # FET Status (bits 7, 23)
            self.charge_fet = bool(operation_status & (1 << 7))
            self.discharge_fet = bool(operation_status & (1 << 23))
            self.balance_active = bool(operation_status & (1 << 15))
            
            # Parse alarm/protection bits
            # Victron uses: 0=OK, 1=Warning, 2=Alarm/Protection
            
            # High Voltage (Overvoltage)
            if operation_status & (1 << 4):  # Bit 4: Pack OVP protection
                self.protection.high_voltage = 2
            elif operation_status & (1 << 12):  # Bit 12: Pack OVP warning
                self.protection.high_voltage = 1
            else:
                self.protection.high_voltage = 0
            
            # Low Voltage (Undervoltage)
            if operation_status & (1 << 21):  # Bit 21: Pack UVP protection
                self.protection.low_voltage = 2
            elif operation_status & (1 << 28):  # Bit 28: Pack UVP warning
                self.protection.low_voltage = 1
            else:
                self.protection.low_voltage = 0
            
            # High Charge Current
            if operation_status & (1 << 0):  # Bit 0: Charge OCP protection
                self.protection.high_charge_current = 2
            elif operation_status & (1 << 8):  # Bit 8: Charge OCP warning
                self.protection.high_charge_current = 1
            else:
                self.protection.high_charge_current = 0
            
            # High Discharge Current
            if operation_status & (1 << 16):  # Bit 16: Discharge OCP protection
                self.protection.high_discharge_current = 2
            elif operation_status & (1 << 24):  # Bit 24: Discharge OCP warning
                self.protection.high_discharge_current = 1
            else:
                self.protection.high_discharge_current = 0
            
            # High Charge Temperature
            if operation_status & (1 << 1):  # Bit 1: Charge high temp protection
                self.protection.high_charge_temp = 2
            elif operation_status & (1 << 9):  # Bit 9: Charge high temp warning
                self.protection.high_charge_temp = 1
            else:
                self.protection.high_charge_temp = 0
            
            # Low Charge Temperature
            if operation_status & (1 << 2):  # Bit 2: Charge low temp protection
                self.protection.low_charge_temp = 2
            elif operation_status & (1 << 10):  # Bit 10: Charge low temp warning
                self.protection.low_charge_temp = 1
            else:
                self.protection.low_charge_temp = 0
            
            # High Discharge Temperature
            if operation_status & (1 << 17):  # Bit 17: Discharge high temp protection
                self.protection.high_discharge_temp = 2
            elif operation_status & (1 << 25):  # Bit 25: Discharge high temp warning
                self.protection.high_discharge_temp = 1
            else:
                self.protection.high_discharge_temp = 0
            
            # Low Discharge Temperature
            if operation_status & (1 << 18):  # Bit 18: Discharge low temp protection
                self.protection.low_discharge_temp = 2
            elif operation_status & (1 << 26):  # Bit 26: Discharge low temp warning
                self.protection.low_discharge_temp = 1
            else:
                self.protection.low_discharge_temp = 0
            
            # Short Circuit (no warning level, only protection)
            if operation_status & (1 << 20):  # Bit 20: Short circuit protection
                self.protection.short_circuit = 2
            else:
                self.protection.short_circuit = 0
            
            # MOS Over-Temperature (additional protection)
            if operation_status & (1 << 30):  # Bit 30: MOS over-temp protection
                self.protection.internal_failure = 2  # Use internal_failure for MOS protection
            elif operation_status & (1 << 29):  # Bit 29: MOS over-temp warning
                self.protection.internal_failure = 1
            else:
                self.protection.internal_failure = 0
            
            # Log active alarms
            active_alarms = []
            if self.protection.high_voltage == 2:
                active_alarms.append("OVP")
            if self.protection.low_voltage == 2:
                active_alarms.append("UVP")
            if self.protection.high_charge_current == 2:
                active_alarms.append("Charge-OCP")
            if self.protection.high_discharge_current == 2:
                active_alarms.append("Discharge-OCP")
            if self.protection.high_charge_temp == 2:
                active_alarms.append("Charge-OTP")
            if self.protection.low_charge_temp == 2:
                active_alarms.append("Charge-UTP")
            if self.protection.high_discharge_temp == 2:
                active_alarms.append("Discharge-OTP")
            if self.protection.low_discharge_temp == 2:
                active_alarms.append("Discharge-UTP")
            if self.protection.short_circuit == 2:
                active_alarms.append("Short-Circuit")
            if self.protection.internal_failure == 2:
                active_alarms.append("MOS-OTP")
            
            if active_alarms:
                logger.warning(f"HumsiENK: Active protections: {', '.join(active_alarms)}")
            
            # Cell balance status (bytes 8-10) - 24-bit bitmap
            if len(data) >= 11:
                cell_balance = int.from_bytes(data[8:11], 'little', signed=False)
                
                # Update each cell's balance status
                for i in range(min(self.cell_count, 24)):
                    if i < len(self.cells):
                        self.cells[i].balance = bool((cell_balance >> i) & 1)
            
            # Cell disconnect status (bytes 11-13) - 24-bit bitmap
            # This indicates if any cells are physically disconnected (faulty connections, broken wires)
            # Critical safety monitoring: detect physically disconnected cells.
            if len(data) >= 14:
                cell_disconnect = int.from_bytes(data[11:14], 'little', signed=False)
                
                # Check if any cells are disconnected
                if cell_disconnect != 0:
                    disconnected_cells = []
                    for i in range(min(self.cell_count, 24)):
                        if (cell_disconnect >> i) & 1:
                            disconnected_cells.append(i + 1)  # 1-indexed for user display
                    
                    if disconnected_cells:
                        logger.error(f"HumsiENK: CRITICAL - Cells physically disconnected: {disconnected_cells}")
                        # Set critical alarm - this is a serious hardware issue
                        # Use cell_imbalance as it's the closest match for cell-level problems
                        self.protection.cell_imbalance = 2
                        # Also log as internal failure since this shouldn't happen in normal operation
                        if self.protection.internal_failure < 2:  # Don't downgrade existing MOS temp alarm
                            self.protection.internal_failure = max(self.protection.internal_failure, 1)
            
            logger.debug(f"HumsiENK: Status: CHG_FET={self.charge_fet}, DSG_FET={self.discharge_fet}, BAL={self.balance_active}, runtime={runtime_seconds}s")
            
        except Exception as e:
            logger.warning(f"HumsiENK: Error parsing status: {e}")
    
    def _parse_config(self, data: bytes):
        """
        Parse command 0x58 response (Configuration) - 44 bytes.
        
        Configuration parameters (all 2-byte little-endian values):
            battery_count (cells in series)
            battery_capacity (rated capacity in 0.01Ah)
            overvoltage_protection (OVP threshold in mV)
            overvoltage_recovery (OVP recovery in mV)
            overvoltage_delay (OVP delay in seconds)
            undervoltage_protection (UVP threshold in mV)
            undervoltage_recovery (UVP recovery in mV)
            undervoltage_delay (UVP delay in seconds)
            charge_overcurrent_protection (charge OCP in 0.1A)
            charge_overcurrent_delay (charge OCP delay in seconds)
            discharge_overcurrent1_protection (discharge OCP level 1 in 0.1A)
            discharge_overcurrent1_delay (discharge OCP1 delay in seconds)
            discharge_overcurrent2_protection (discharge OCP level 2 in 0.1A)
            discharge_overcurrent2_delay (discharge OCP2 delay in seconds)
            charge_high_temp_protection (charge high temp in °C)
            charge_high_temp_recovery (charge high temp recovery)
            charge_low_temp_protection (charge low temp in °C, signed)
            charge_low_temp_recovery (charge low temp recovery)
            discharge_high_temp_protection (discharge high temp in °C)
            discharge_high_temp_recovery (discharge high temp recovery)
            discharge_low_temp_protection (discharge low temp in °C, signed)
            discharge_low_temp_recovery (discharge low temp recovery)
        """
        try:
            if len(data) < 44:
                logger.warning(f"HumsiENK: Config data too short: {len(data)} bytes")
                return
            
            idx = 0
            
            # Battery configuration
            battery_count = int.from_bytes(data[idx:idx+2], 'little', signed=False)
            idx += 2
            
            battery_capacity_raw = int.from_bytes(data[idx:idx+2], 'little', signed=False)
            battery_capacity = battery_capacity_raw / 100.0  # Convert from 0.01Ah to Ah
            idx += 2
            
            # Voltage protection limits (mV)
            cell_ovp_mv = int.from_bytes(data[idx:idx+2], 'little', signed=False)
            cell_ovp = cell_ovp_mv / 1000.0  # Convert to volts
            idx += 2
            
            cell_ovp_recovery_mv = int.from_bytes(data[idx:idx+2], 'little', signed=False)
            idx += 2
            
            ovp_delay = int.from_bytes(data[idx:idx+2], 'little', signed=False)
            idx += 2
            
            cell_uvp_mv = int.from_bytes(data[idx:idx+2], 'little', signed=False)
            cell_uvp = cell_uvp_mv / 1000.0  # Convert to volts
            idx += 2
            
            cell_uvp_recovery_mv = int.from_bytes(data[idx:idx+2], 'little', signed=False)
            idx += 2
            
            uvp_delay = int.from_bytes(data[idx:idx+2], 'little', signed=False)
            idx += 2
            
            # Current protection limits (0.1A units)
            charge_ocp_raw = int.from_bytes(data[idx:idx+2], 'little', signed=False)
            charge_ocp = charge_ocp_raw / 10.0  # Convert to amps
            idx += 2
            
            charge_ocp_delay = int.from_bytes(data[idx:idx+2], 'little', signed=False)
            idx += 2
            
            discharge_ocp1_raw = int.from_bytes(data[idx:idx+2], 'little', signed=False)
            discharge_ocp1 = discharge_ocp1_raw / 10.0  # Convert to amps
            idx += 2
            
            discharge_ocp1_delay = int.from_bytes(data[idx:idx+2], 'little', signed=False)
            idx += 2
            
            discharge_ocp2_raw = int.from_bytes(data[idx:idx+2], 'little', signed=False)
            discharge_ocp2 = discharge_ocp2_raw / 10.0  # Convert to amps
            idx += 2
            
            discharge_ocp2_delay = int.from_bytes(data[idx:idx+2], 'little', signed=False)
            idx += 2
            
            # Temperature protection limits (raw values in deciKelvin)
            # Temperature conversion: (raw_deciKelvin - 2731) / 10 → °C
            def _dk_to_c(raw):
                return (raw - 2731) / 10.0
            
            charge_high_temp = _dk_to_c(int.from_bytes(data[idx:idx+2], 'little', signed=False))
            idx += 2
            
            charge_high_temp_recovery = _dk_to_c(int.from_bytes(data[idx:idx+2], 'little', signed=False))
            idx += 2
            
            charge_low_temp = _dk_to_c(int.from_bytes(data[idx:idx+2], 'little', signed=False))
            idx += 2
            
            charge_low_temp_recovery = _dk_to_c(int.from_bytes(data[idx:idx+2], 'little', signed=False))
            idx += 2
            
            discharge_high_temp = _dk_to_c(int.from_bytes(data[idx:idx+2], 'little', signed=False))
            idx += 2
            
            discharge_high_temp_recovery = _dk_to_c(int.from_bytes(data[idx:idx+2], 'little', signed=False))
            idx += 2
            
            discharge_low_temp = _dk_to_c(int.from_bytes(data[idx:idx+2], 'little', signed=False))
            idx += 2
            
            discharge_low_temp_recovery = _dk_to_c(int.from_bytes(data[idx:idx+2], 'little', signed=False))
            idx += 2
            
            # Set battery parameters for Victron to use
            if battery_count > 0 and battery_count <= 24:
                # Update cell count if different (but don't override auto-detection if already set correctly)
                if self.cell_count != battery_count:
                    logger.info(f"HumsiENK: Config reports {battery_count} cells (current: {self.cell_count})")
            
            if battery_capacity > 0:
                if self.capacity != battery_capacity:
                    logger.info(f"HumsiENK: Config capacity: {battery_capacity}Ah (current: {self.capacity}Ah)")
                    # Only update if not already set from config.ini
                    if self.capacity == 0 or BATTERY_CAPACITY is None:
                        self.capacity = battery_capacity
            
            # Set voltage limits for Victron
            if cell_ovp > 0 and battery_count > 0:
                self.max_battery_voltage = cell_ovp * battery_count
                logger.debug(f"HumsiENK: Max pack voltage: {self.max_battery_voltage}V ({cell_ovp}V/cell)")
            
            if cell_uvp > 0 and battery_count > 0:
                self.min_battery_voltage = cell_uvp * battery_count
                logger.debug(f"HumsiENK: Min pack voltage: {self.min_battery_voltage}V ({cell_uvp}V/cell)")
            
            # Set current limits for Victron
            if charge_ocp > 0:
                # Only override if not set in config.ini
                if MAX_BATTERY_CHARGE_CURRENT is None or self.max_battery_charge_current == MAX_BATTERY_CHARGE_CURRENT:
                    self.max_battery_charge_current = charge_ocp
                    logger.debug(f"HumsiENK: Max charge current: {charge_ocp}A")
            
            if discharge_ocp1 > 0:
                # Use discharge_ocp1 as the primary discharge limit
                # Only override if not set in config.ini
                if MAX_BATTERY_DISCHARGE_CURRENT is None or self.max_battery_discharge_current == MAX_BATTERY_DISCHARGE_CURRENT:
                    self.max_battery_discharge_current = discharge_ocp1
                    logger.debug(f"HumsiENK: Max discharge current: {discharge_ocp1}A")
            
            logger.info(f"HumsiENK: Configuration loaded - {battery_count}S {battery_capacity}Ah, "
                       f"OVP={cell_ovp}V UVP={cell_uvp}V, "
                       f"Charge={charge_ocp}A Discharge={discharge_ocp1}A, "
                       f"ChgTemp={charge_low_temp}~{charge_high_temp}°C "
                       f"DsgTemp={discharge_low_temp}~{discharge_high_temp}°C")
            
        except Exception as e:
            logger.warning(f"HumsiENK: Error parsing config: {e}")
    
    def _parse_version(self, data: bytes):
        """
        Parse command 0xF5 response (Firmware Version).
        
        Returns ASCII string representation of firmware version.
        """
        try:
            if len(data) == 0:
                logger.warning("HumsiENK: Version data is empty")
                return
            
            # Convert bytes to ASCII string
            version_str = ""
            for byte in data:
                # Only include printable ASCII characters (32-126)
                if 32 <= byte <= 126:
                    version_str += chr(byte)
            
            if version_str:
                self.hardware_version = f"HumsiENK v{version_str}"
                logger.info(f"HumsiENK: Firmware version: {version_str}")
            else:
                logger.warning(f"HumsiENK: Unable to decode version from data: {data.hex()}")
                
        except Exception as e:
            logger.warning(f"HumsiENK: Error parsing version: {e}")

    def _send_trigger(self):
        """Send status command (0x20) to keep data flowing and maintain connection."""
        if self.ble_handle:
            cmd = self._build_command(self.CMD_STATUS, [])
            self.ble_handle.send_data(cmd)

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



