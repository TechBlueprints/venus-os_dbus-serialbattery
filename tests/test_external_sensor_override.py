# -*- coding: utf-8 -*-
"""Tests for the external sensor override (EXTERNAL_SENSOR_DBUS_PATH_*) and fallback sensor (FALLBACK_SENSOR_DBUS_*)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery", "ext", "velib_python"))

import utils  # noqa: E402
from battery import Battery  # noqa: E402


class _FakeDbusItem:
    def __init__(self, value):
        self._value = value

    def get_value(self):
        return self._value


# concrete subclass so the abstract Battery can be instantiated without a BMS
_ConcreteBattery = type("_ConcreteBattery", (Battery,), {name: lambda self, *a, **kw: None for name in Battery.__abstractmethods__})


def _make_battery():
    battery = _ConcreteBattery.__new__(_ConcreteBattery)
    battery.voltage = 26.0
    battery.voltage_calc = None
    battery.dbus_external_objects = None
    battery.dbus_fallback_objects = None
    battery.online = True
    return battery


class TestGetVoltage:
    def test_no_external_sensor_returns_bms_voltage(self):
        battery = _make_battery()
        assert battery.get_voltage() == 26.0

    def test_external_sensor_overrides_bms_voltage(self):
        battery = _make_battery()
        battery.dbus_external_objects = {"Voltage": _FakeDbusItem(26.437)}
        assert battery.get_voltage() == 26.437

    def test_external_sensor_none_value_falls_back_to_bms(self):
        battery = _make_battery()
        battery.dbus_external_objects = {"Voltage": _FakeDbusItem(None)}
        assert battery.get_voltage() == 26.0

    def test_external_objects_without_voltage_key_falls_back_to_bms(self):
        battery = _make_battery()
        battery.dbus_external_objects = {"Current": _FakeDbusItem(-0.2)}
        assert battery.get_voltage() == 26.0

    def test_external_sensor_value_is_rounded(self):
        battery = _make_battery()
        battery.dbus_external_objects = {"Voltage": _FakeDbusItem(26.43759)}
        assert battery.get_voltage() == 26.438

    def test_no_external_sensor_and_no_bms_voltage_returns_none(self):
        battery = _make_battery()
        battery.voltage = None
        assert battery.get_voltage() is None


class TestFallbackSensor:
    def test_fallback_not_used_while_bms_online(self):
        battery = _make_battery()
        battery.dbus_fallback_objects = {"Voltage": _FakeDbusItem(25.5)}
        assert battery.get_voltage() == 26.0

    def test_fallback_used_while_bms_offline(self):
        battery = _make_battery()
        battery.online = False
        battery.dbus_fallback_objects = {"Voltage": _FakeDbusItem(25.5)}
        assert battery.get_voltage() == 25.5

    def test_stale_bms_value_served_when_no_fallback_path(self):
        battery = _make_battery()
        battery.online = False
        battery.dbus_fallback_objects = {"Current": _FakeDbusItem(-0.2)}
        assert battery.get_voltage() == 26.0

    def test_fallback_none_value_falls_back_to_stale_bms_value(self):
        battery = _make_battery()
        battery.online = False
        battery.dbus_fallback_objects = {"Voltage": _FakeDbusItem(None)}
        assert battery.get_voltage() == 26.0

    def test_external_override_wins_over_fallback(self):
        battery = _make_battery()
        battery.online = False
        battery.dbus_external_objects = {"Voltage": _FakeDbusItem(26.437)}
        battery.dbus_fallback_objects = {"Voltage": _FakeDbusItem(25.5)}
        assert battery.get_voltage() == 26.437

    def test_fallback_temperature_used_while_bms_offline(self):
        battery = _make_battery()
        battery.online = False
        battery.temperature_1 = 20.0
        battery.temperature_2 = 22.0
        battery.temperature_3 = None
        battery.temperature_4 = None
        battery.dbus_fallback_objects = {"Temperature": _FakeDbusItem(18.73)}
        assert battery.get_temperature() == 18.7

    def test_get_value_from_fallback_sensor_requires_offline(self):
        battery = _make_battery()
        battery.dbus_fallback_objects = {"Voltage": _FakeDbusItem(25.5)}
        assert battery.get_value_from_fallback_sensor("Voltage") is None
        battery.online = False
        assert battery.get_value_from_fallback_sensor("Voltage") == 25.5


class TestGetFallbackSensorDevice:
    def _resolve(self, config_value, port="/ble_5320b7d7f9e7"):
        battery = _make_battery()
        battery.port = port
        saved = utils.FALLBACK_SENSOR_DBUS_DEVICE
        utils.FALLBACK_SENSOR_DBUS_DEVICE = config_value
        try:
            return battery.get_fallback_sensor_device()
        finally:
            utils.FALLBACK_SENSOR_DBUS_DEVICE = saved

    def test_not_configured_returns_none(self):
        assert self._resolve(None) is None

    def test_single_service_applies_to_any_battery(self):
        assert self._resolve("com.victronenergy.battery.ttyS2") == "com.victronenergy.battery.ttyS2"

    def test_mapping_resolves_matching_battery(self):
        mapping = "ble_5320b7d7f9e7:com.victronenergy.battery.ttyS5, ble_ab807254e0b4:com.victronenergy.battery.ttyS6"
        assert self._resolve(mapping, port="/ble_5320b7d7f9e7") == "com.victronenergy.battery.ttyS5"
        assert self._resolve(mapping, port="/ble_ab807254e0b4") == "com.victronenergy.battery.ttyS6"

    def test_mapping_without_match_returns_none(self):
        mapping = "ble_5320b7d7f9e7:com.victronenergy.battery.ttyS5"
        assert self._resolve(mapping, port="/ble_ab807254e0b4") is None


class TestGetTemperature:
    def _make_battery_with_temperatures(self):
        battery = _make_battery()
        battery.temperature_1 = 20.0
        battery.temperature_2 = 22.0
        battery.temperature_3 = None
        battery.temperature_4 = None
        return battery

    def test_no_external_sensor_returns_bms_temperature_average(self):
        battery = self._make_battery_with_temperatures()
        assert battery.get_temperature() == 21.0

    def test_external_sensor_overrides_bms_temperature(self):
        battery = self._make_battery_with_temperatures()
        battery.dbus_external_objects = {"Temperature": _FakeDbusItem(18.73)}
        assert battery.get_temperature() == 18.7

    def test_external_sensor_none_value_falls_back_to_bms(self):
        battery = self._make_battery_with_temperatures()
        battery.dbus_external_objects = {"Temperature": _FakeDbusItem(None)}
        assert battery.get_temperature() == 21.0

    def test_external_objects_without_temperature_key_falls_back_to_bms(self):
        battery = self._make_battery_with_temperatures()
        battery.dbus_external_objects = {"Voltage": _FakeDbusItem(26.4)}
        assert battery.get_temperature() == 21.0


class TestGetPower:
    def test_power_uses_calculated_voltage(self):
        battery = _make_battery()
        battery.voltage_calc = 26.437
        battery.current_calc = -0.2
        battery.power_calc = None
        battery.power_calc_last_time = None
        assert battery.get_power() == 26.437 * -0.2

    def test_power_none_when_voltage_calc_missing(self):
        battery = _make_battery()
        battery.voltage_calc = None
        battery.current_calc = -0.2
        battery.power_calc = None
        battery.power_calc_last_time = None
        assert battery.get_power() is None
