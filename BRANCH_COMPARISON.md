# Comparison: ble-advertisements Branch vs Upstream

## Files That Are Pure Additions (Keep All)

### 1. **utils_ble_discovery.py** (140 lines)
- ✅ NEW file for BLE router integration
- ✅ Self-contained, graceful fallback
- **Action**: KEEP

### 2. **BLE_INTEGRATION.md** (117 lines)
- ✅ NEW documentation file
- **Action**: KEEP

### 3. **grenergy_ble.py** (2,232 lines, renamed from lltjbd_fastble.py)
- ✅ NEW BLE driver for Grenergy devices
- ✅ Direct BlueZ D-Bus implementation
- **Action**: KEEP (after renaming complete)

### 4. **nordicnus_ble.py** (668 lines)
- ✅ NEW Nordic UART Service BLE driver
- **Action**: KEEP

### 5. **jbd_ble_probe.py** & **jbd_fast_probe.py** (155 lines total)
- ✅ NEW testing/probing tools
- **Action**: KEEP

## Files With Mixed Changes (Review Line by Line)

### 6. **config.default.ini**

**Changes to KEEP:**
```ini
# NEW JBD BLE handshake settings (for Grenergy driver)
JBD_BLE_HANDSHAKE_ENABLE = False
JBD_BLE_HANDSHAKE_CLEAN_PSW = False
JBD_BLE_HANDSHAKE_ENABLE_FFAA = False
JBD_BLE_HANDSHAKE_PASSWORD = 123456

# NEW setting for verbose D-Bus logging
LOG_DBUS_UPDATES = False

# Updated BMS type list to include NordicNus_Ble
Available Bluetooth BMS: ..., NordicNus_Ble
```

**Action**: KEEP ALL

---

### 7. **utils.py**

**Changes to KEEP:**
```python
# Default value for BATTERY_CELL_DATA_FORMAT
BATTERY_CELL_DATA_FORMAT: int = get_int_from_config("DEFAULT", "BATTERY_CELL_DATA_FORMAT", 3)

# NEW setting
LOG_DBUS_UPDATES: bool = get_bool_from_config("DEFAULT", "LOG_DBUS_UPDATES")

# NEW JBD BLE handshake settings
JBD_BLE_HANDSHAKE_ENABLE: bool = ...
JBD_BLE_HANDSHAKE_CLEAN_PSW: bool = ...
JBD_BLE_HANDSHAKE_ENABLE_FFAA: bool = ...
JBD_BLE_HANDSHAKE_PASSWORD: str = ...
```

**Action**: KEEP ALL

---

### 8. **utils_ble.py**

**Changes to KEEP (All Improvements):**
- ✅ Added `_notification_queue` (deque) to prevent packet drops
- ✅ Added exponential backoff reconnection logic
- ✅ Improved error handling and connection state tracking
- ✅ Better `connected` state management
- ✅ Graceful exception handling in callbacks

**Action**: KEEP ALL (these are quality improvements)

---

### 9. **dbus-serialbattery.py**

**Changes to REMOVE (Temporary Debug Code):**
```python
# REMOVE: Debug logging to /tmp/fastble-main.log
try:
    with open('/tmp/fastble-main.log','a') as f:
        f.write(...)
except Exception:
    pass
```
**Lines to remove**: ~40 lines of debug logging scattered throughout

**Changes to KEEP:**
```python
# KEEP: BLE discovery integration (10 lines)
try:
    from utils_ble_discovery import get_discovery_helper
    discovery_helper = get_discovery_helper()
    if discovery_helper.is_available():
        discovery_helper.register_mac_addresses([ble_address])
except Exception as e:
    logger.debug(f"BLE discovery helper not available: {e}")

# KEEP: Support for new BLE drivers
elif port == "Grenergy_Ble":
    from bms.grenergy_ble import Grenergy_Ble

elif port == "NordicNus_Ble":
    from bms.nordicnus_ble import NordicNus_Ble

# KEEP: Async connection handling for Grenergy_Ble
if port in ("LltJbd_Ble", "Grenergy_Ble"):
    # async connection logic

# KEEP: Updated BMS type list
"Supported Bluetooth BMS types (CASE SENSITIVE!): ..., Grenergy_Ble, NordicNus_Ble"

# KEEP: Changed endswith check
if port.endswith("Ble"):  # was "_Ble"
```

**Action**: 
- REMOVE: ~40 lines of `/tmp/fastble-main.log` debug code
- KEEP: ~56 lines of actual functionality

---

## Summary

### Lines to Remove:
- **dbus-serialbattery.py**: ~40 lines of debug logging

### Lines to Keep:
- **New files**: ~3,300 lines (drivers, tools, docs, helper)
- **Improvements**: ~150 lines (utils_ble.py enhancements, config, utils)
- **Integration**: ~10 lines (BLE discovery helper)
- **Feature support**: ~46 lines (new driver imports, async handling)

### Total Clean Addition:
- **~3,500 lines** of new functionality
- **~40 lines** of temporary debug code to remove

### Recommendation:
1. Remove the `/tmp/fastble-main.log` debug logging
2. Keep everything else - it's all valuable additions
3. The branch is clean and ready for use after removing debug code

