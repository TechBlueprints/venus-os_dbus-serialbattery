# -*- coding: utf-8 -*-
"""Tests for the external voltage sensor override (EXTERNAL_SENSOR_DBUS_PATH_VOLTAGE)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery", "ext", "velib_python"))

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
