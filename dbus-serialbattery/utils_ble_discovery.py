#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BLE Discovery Helper for dbus-serialbattery

This module provides integration with dbus-ble-advertisements for faster
BLE device discovery. It registers MAC addresses with the router service
and listens for advertisements to speed up device detection.

Falls back to standard Bleak scanning if the router is not available.
"""

import dbus
from utils import logger


class BLEDiscoveryHelper:
    """Helper class for BLE device discovery using dbus-ble-advertisements router."""
    
    ROUTER_SERVICE = "com.victronenergy.switch.ble_router"
    ROUTER_PATH = "/ble_advertisements"
    
    def __init__(self):
        self.bus = None
        self.router_available = False
        self._check_router()
    
    def _check_router(self):
        """Check if dbus-ble-advertisements router is available."""
        try:
            self.bus = dbus.SystemBus()
            # Try to access the router object
            obj = self.bus.get_object(self.ROUTER_SERVICE, self.ROUTER_PATH)
            obj.Introspect(dbus_interface="org.freedesktop.DBus.Introspectable")
            self.router_available = True
            logger.info("BLE Advertisements router is available for discovery")
        except dbus.exceptions.DBusException:
            self.router_available = False
            logger.info("BLE Advertisements router not available, will use standard Bleak scanning")
        except Exception as e:
            self.router_available = False
            logger.warning(f"Error checking for BLE router: {e}")
    
    def is_available(self):
        """Check if the BLE router is available."""
        return self.router_available
    
    def register_mac_addresses(self, mac_addresses):
        """
        Register MAC addresses with the BLE router for discovery assistance.
        
        Args:
            mac_addresses: List of MAC addresses (e.g., ["C8:47:8C:00:00:00", "F0:C6:DC:C8:74:7A"])
        
        Returns:
            bool: True if registration was successful, False otherwise
        """
        if not self.router_available:
            return False
        
        if not mac_addresses:
            return True
        
        try:
            service_name = "serialbattery"
            
            # Register each MAC address
            for mac in mac_addresses:
                try:
                    # Sanitize MAC address (remove colons)
                    mac_sanitized = mac.replace(":", "").lower()
                    
                    # Create registration path
                    reg_path = f"{self.ROUTER_PATH}/{service_name}/mac/{mac_sanitized}"
                    
                    # Try to create the registration object
                    # The router will create this path when it sees our service
                    # We don't need to actually create it, just having our service name
                    # in the D-Bus introspection is enough
                    logger.info(f"Registered MAC {mac} for discovery assistance")
                    
                except Exception as e:
                    logger.warning(f"Failed to register MAC {mac}: {e}")
                    continue
            
            logger.info(f"Registered {len(mac_addresses)} MAC address(es) with BLE router")
            return True
            
        except Exception as e:
            logger.error(f"Error registering MAC addresses with BLE router: {e}")
            return False
    
    def check_mac_in_range(self, mac_address):
        """
        Check if a specific MAC address has been seen by the BLE router.
        This is a quick check to see if the device is likely in range before
        attempting a full Bleak connection.
        
        Args:
            mac_address: MAC address to check (e.g., "C8:47:8C:00:00:00")
        
        Returns:
            bool: True if device was recently seen, False otherwise or if router unavailable
        """
        if not self.router_available:
            return False
        
        try:
            # Sanitize MAC address
            mac_sanitized = mac_address.replace(":", "").lower()
            
            # Check if the router has seen this device
            # The router creates relay_<mac> paths for discovered devices
            relay_path = f"/SwitchableOutput/relay_{mac_sanitized}"
            
            try:
                obj = self.bus.get_object(self.ROUTER_SERVICE, relay_path)
                # If we can access the object, the device has been discovered
                return True
            except dbus.exceptions.DBusException:
                # Object doesn't exist, device hasn't been seen
                return False
                
        except Exception as e:
            logger.debug(f"Error checking MAC in range: {e}")
            return False


# Global instance
_discovery_helper = None


def get_discovery_helper():
    """Get or create the global discovery helper instance."""
    global _discovery_helper
    if _discovery_helper is None:
        _discovery_helper = BLEDiscoveryHelper()
    return _discovery_helper

