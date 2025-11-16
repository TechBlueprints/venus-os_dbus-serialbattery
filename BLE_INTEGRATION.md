# BLE Discovery Integration

## Overview

This branch integrates `dbus-serialbattery` with the `dbus-ble-advertisements` router service for improved BLE device discovery.

## What This Integration Does

The `dbus-ble-advertisements` router provides a centralized BLE scanning service that:
1. Continuously monitors BLE advertisements from all devices in range
2. Routes advertisement data to interested services via D-Bus
3. Reduces Bluetooth stack load by eliminating redundant scanning

### Integration Benefits

When `dbus-ble-advertisements` is installed and running, `dbus-serialbattery` will:
- **Register BLE BMS MAC addresses** with the router for discovery assistance
- **Faster device detection** - the router already knows which devices are in range
- **Reduced BLE stack load** - centralized scanning instead of per-service scanning
- **Seamless fallback** - if the router isn't available, standard Bleak scanning is used
- **Simplified configuration** - no need for `BLUETOOTH_DIRECT_CONNECT` or `BLUETOOTH_PREFERRED_ADAPTER` settings

## How It Works

1. When a BLE BMS service starts, it checks if `dbus-ble-advertisements` is available
2. If available, it registers its MAC address with the router
3. The router notifies when the device is seen, speeding up connection attempts
4. The BMS driver still uses Bleak to establish the actual connection and communicate

**Note**: The router only helps with *discovery*. The actual BLE connection and communication still uses the standard Bleak library, as BMS devices require bidirectional command/response communication.

## Installation

### With dbus-ble-advertisements (Recommended)

1. Install `dbus-ble-advertisements` first:
   ```bash
   git clone https://github.com/TechBlueprints/dbus-ble-advertisements.git /data/apps/dbus-ble-advertisements
   cd /data/apps/dbus-ble-advertisements
   ./install.sh
   ./enable.sh
   ```

2. Install `dbus-serialbattery` (this branch):
   ```bash
   # Install as normal, the BLE router integration is automatic
   ```

### Without dbus-ble-advertisements (Standalone)

This branch works perfectly fine without `dbus-ble-advertisements`. It will automatically detect that the router is not available and fall back to standard Bleak scanning.

## Technical Details

### Files Modified

- **`utils_ble_discovery.py`**: New module providing BLE router integration
  - `BLEDiscoveryHelper` class for router communication
  - Automatic router availability detection
  - MAC address registration

- **`dbus-serialbattery.py`**: Updated BLE initialization
  - Calls discovery helper when BLE BMS is started
  - Registers MAC address with router if available
  - Graceful fallback if router is not present

- **`config.default.ini`**: Removed obsolete BLE configuration
  - Removed `BLUETOOTH_DIRECT_CONNECT` (no longer needed with router)
  - Removed `BLUETOOTH_PREFERRED_ADAPTER` (no longer needed with router)

- **`utils.py`**: Removed obsolete configuration imports
  - Removed `BLUETOOTH_DIRECT_CONNECT`
  - Removed `BLUETOOTH_PREFERRED_ADAPTER`

- **`utils_ble.py`**: Simplified connection logic
  - Removed adapter selection code
  - Now uses BlueZ default adapter (handled by router)
  - Cleaner, more reliable connection process

### D-Bus Interface

The integration uses the following D-Bus service:
- **Service**: `com.victronenergy.switch.ble_router`
- **Path**: `/ble_advertisements`

MAC addresses are registered by creating paths like:
- `/ble_advertisements/serialbattery/mac/{mac_without_colons}`

## Supported BMS Types

This integration works with all BLE BMS types supported by `dbus-serialbattery`:
- Jkbms_Ble
- Kilovault_Ble
- LiTime_Ble
- LltJbd_Ble
- LltJbd_FastBle
- NordicNus_Ble

## Limitations

- The router helps with *discovery only*, not communication
- BMS devices don't typically advertise manufacturer IDs, only service UUIDs
- The actual BLE connection still requires Bleak/BlueZ (this is by design)

## Future Enhancements

Potential future improvements:
- Service UUID registration (in addition to MAC addresses)
- Manufacturer ID detection for BMS vendors that use it
- Connection status sharing between multiple BMS instances

## Compatibility

- **Venus OS**: v2.90 or newer (same as base `dbus-serialbattery` requirement)
- **dbus-ble-advertisements**: Any version
- **Backward compatible**: Works with or without the router installed

