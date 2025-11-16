# BLE Drivers Analysis: Manufacturer ID Requirements

## Summary

**None of the BLE BMS drivers in dbus-serialbattery require manufacturer ID registration with the BLE router.**

All BLE BMS drivers connect **directly via MAC address** using GATT services, not by parsing manufacturer data from advertisements.

## BLE Driver Connection Methods

### 1. **JKBMS_Ble** (`jkbms_ble.py`)
- **Connection**: Direct BleakClient via `Jkbms_Brn` helper
- **Method**: GATT connection to known service UUIDs
- **Manufacturer Data**: No parsing
- **Router Registration**: MAC address only (for range detection)

### 2. **Kilovault_Ble** (`kilovault_ble.py`)
- **Service UUID**: `FFE0`
- **Notify UUID**: `FFE4`
- **Name UUID**: `FFE6`
- **Connection**: Direct BleakClient connection
- **Manufacturer Data**: No parsing
- **Router Registration**: MAC address only (for range detection)

### 3. **LiTime_Ble** (`litime_ble.py`)
- **Connection**: Uses `Syncron_Ble` helper (utils_ble.py)
- **Method**: Direct GATT connection via BleakClient
- **Manufacturer Data**: No parsing
- **Router Registration**: MAC address only (for range detection)

### 4. **LltJbd_Ble** (`lltjbd_ble.py`)
- **Service UUID**: `0000ff00-0000-1000-8000-00805f9b34fb`
- **TX UUID**: `0000ff02-0000-1000-8000-00805f9b34fb`
- **RX UUID**: `0000ff01-0000-1000-8000-00805f9b34fb`
- **Connection**: Direct BleakClient connection
- **Manufacturer Data**: No parsing
- **Router Registration**: MAC address only (for range detection)

### 5. **Grenergy_Ble** (`grenergy_ble.py`)
- **Parent**: Extends `LltJbd` (same protocol)
- **Service UUID**: Same as LltJbd (`0000ff00-...`)
- **Connection**: Direct BlueZ D-Bus GATT + fallback to Bleak
- **Manufacturer Data**: No parsing
- **Router Registration**: MAC address only (for range detection)

### 6. **NordicNus_Ble** (`nordicnus_ble.py`)
- **RX UUID**: `6e400003-b5a3-f393-e0a9-e50e24dcca9e` (Nordic UART Service)
- **TX UUID**: `6e400002-b5a3-f393-e0a9-e50e24dcca9e`
- **Connection**: Direct BleakClient to Nordic UART Service
- **Manufacturer Data**: No parsing
- **Router Registration**: MAC address only (for range detection)

## Comparison with Other Services

### Services That DO Need Manufacturer ID Registration:

#### **dbus-victron-orion-tr**
- **Manufacturer ID**: `0x02E1` (Victron)
- **Why**: Parses voltage, current, temperature from manufacturer data
- **Registration**: 
  ```python
  manufacturer_id=0x02E1
  ```

#### **victron-seelevel-python**
- **Manufacturer IDs**: 
  - `305` (0x0131) - Cypress BTP3
  - `3264` (0x0CC0) - SeeLevel BTP7
- **Why**: Parses tank levels, battery voltage, temperatures from manufacturer data
- **Registration**:
  ```python
  manufacturer_ids=[305, 3264]
  ```

### Services That DON'T Need Manufacturer ID Registration:

#### **venus-os_dbus-serialbattery (all BLE drivers)**
- **Manufacturer ID**: None required
- **Why**: All drivers connect directly via MAC address using GATT services
- **Registration**: 
  ```python
  register_mac_addresses([ble_address])  # MAC only
  ```

## BLE Router Integration Purpose

For dbus-serialbattery BLE drivers, the BLE router integration serves **one purpose only**:

**Fast Range Detection**: Check if a device MAC is in Bluetooth range before attempting a Bleak connection.

```python
# Check if device is in range before connecting
if discovery_helper.check_mac_in_range(ble_address):
    # Attempt Bleak connection
    client = BleakClient(ble_address)
    await client.connect()
```

This avoids long timeouts when attempting to connect to a device that's not present.

## Why Not Manufacturer ID Registration?

**Current Architecture**: dbus-serialbattery requires the user to **manually provide the MAC address** via command line:

```bash
# Example service invocation
/opt/victronenergy/dbus-serialbattery/dbus-serialbattery.py Grenergy_Ble F0:C6:DC:C8:74:7A
```

**No Automatic Discovery**: There is no automatic device discovery feature. The user must:
1. Find the BMS MAC address (via `bluetoothctl` scan or similar)
2. Configure the service with that specific MAC address
3. The service then connects **directly to that MAC**

**Therefore**: Manufacturer ID registration is not needed because:
- The service already knows the exact MAC address to connect to
- No scanning for "any JKBMS device" or "any LltJbd device"
- The BLE router only needs to monitor that specific MAC for range detection

## Future Enhancement: Automatic Discovery

**If** automatic discovery were added in the future, then manufacturer IDs would be useful:

1. Register manufacturer IDs for common BMS brands:
   - JKBMS: Unknown manufacturer ID (would need to scan and identify)
   - LltJbd/Grenergy: Unknown manufacturer ID (Chinese OEM manufacturers)
   - Kilovault: Unknown manufacturer ID
   - LiTime: Unknown manufacturer ID

2. The BLE router would notify when devices with those manufacturer IDs appear

3. The service could auto-create instances for discovered devices

**However**, this would require:
- Identifying the manufacturer IDs used by each BMS brand (currently unknown)
- Implementing discovery logic in dbus-serialbattery
- Auto-configuration of new device instances
- This is **not currently implemented** and may not be needed given the manual configuration workflow

## Configuration Summary

| Project | Service Type | Needs Mfg ID? | Registration |
|---------|-------------|---------------|--------------|
| dbus-victron-orion-tr | Advertisement Parser | ✅ Yes | `0x02E1` |
| victron-seelevel-python | Advertisement Parser | ✅ Yes | `305`, `3264` |
| venus-os_dbus-serialbattery | GATT Connection | ❌ No | MAC address only |

## Current Implementation Status

✅ **Correctly Implemented**

The current `utils_ble_discovery.py` in dbus-serialbattery only registers MAC addresses:

```python
# In dbus-serialbattery.py
discovery_helper.register_mac_addresses([ble_address])
```

**No manufacturer ID registration is needed or implemented**, which is correct for this use case.

## Conclusion

- All 6 BLE BMS drivers use **direct GATT connections** via service UUIDs
- None parse **manufacturer data** from advertisements
- Only **MAC address registration** is needed (for range detection)
- Current implementation is **correct and complete**

