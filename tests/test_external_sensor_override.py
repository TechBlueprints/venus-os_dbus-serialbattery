# -*- coding: utf-8 -*-
"""Tests for the external sensor override (EXTERNAL_SENSOR_DBUS_PATH_*) and fallback sensor (FALLBACK_SENSOR_DBUS_*)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery", "ext", "velib_python"))

import utils  # noqa: E402
from battery import Battery  # noqa: E402
from dbushelper import DbusHelper  # noqa: E402


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


class TestStaleServingEligible:
    def _make_helper(self, monkeypatch, fallback_objects, minutes=480.0, block_on_disconnect=False, cell_voltages_good=None):
        monkeypatch.setattr(utils, "FALLBACK_SERVE_STALE_MINUTES", minutes)
        monkeypatch.setattr(utils, "BLOCK_ON_DISCONNECT", block_on_disconnect)
        helper = DbusHelper.__new__(DbusHelper)
        helper.battery = _make_battery()
        helper.battery.dbus_fallback_objects = fallback_objects
        helper.cell_voltages_good = cell_voltages_good
        return helper

    def test_eligible_with_fallback_sensor(self, monkeypatch):
        helper = self._make_helper(monkeypatch, {"Voltage": _FakeDbusItem(25.5)})
        assert helper.stale_serving_eligible() is True

    def test_eligible_regardless_of_cell_voltages(self, monkeypatch):
        # the BMS's own cell-level protection acts independently of this driver,
        # so cells outside the BLOCK_ON_DISCONNECT band must not block stale serving
        helper = self._make_helper(monkeypatch, {"Voltage": _FakeDbusItem(25.5)}, cell_voltages_good=False)
        assert helper.stale_serving_eligible() is True

    def test_not_eligible_without_fallback_sensor(self, monkeypatch):
        helper = self._make_helper(monkeypatch, None)
        assert helper.stale_serving_eligible() is False

    def test_not_eligible_when_disabled(self, monkeypatch):
        helper = self._make_helper(monkeypatch, {"Voltage": _FakeDbusItem(25.5)}, minutes=0)
        assert helper.stale_serving_eligible() is False

    def test_not_eligible_with_block_on_disconnect(self, monkeypatch):
        helper = self._make_helper(monkeypatch, {"Voltage": _FakeDbusItem(25.5)}, block_on_disconnect=True)
        assert helper.stale_serving_eligible() is False


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


class TestNeverOnlineStaleEngagement:
    """Regression for the never-online path (production incident 2026-08-13).

    A driver that only got data during init and lost BLE before its first
    successful main-loop cycle still has ``battery.online = None``. Stale
    serving must engage on eligibility alone, mark the battery offline, and
    the fallback getter must actually serve — an earlier half-fix engaged
    stale serving but left ``online = None``, so the fallback refused to
    serve, ``/Dc/0/Temperature`` published None, and dbus-aggregate-batteries
    crashed downstream on ``Temperature += None``.
    """

    class _FakeLoop:
        def __init__(self):
            self.quit_called = False

        def quit(self):
            self.quit_called = True

    def _make_helper(self, monkeypatch):
        from time import time as now

        monkeypatch.setattr(utils, "FALLBACK_SERVE_STALE_MINUTES", 480.0)
        monkeypatch.setattr(utils, "BLOCK_ON_DISCONNECT", False)
        monkeypatch.setattr(utils, "FALLBACK_BMS_CABLE_WARN_MINUTES", 480.0)
        monkeypatch.setattr(utils, "EXTERNAL_SENSOR_DBUS_DEVICE", None)
        monkeypatch.setattr(utils, "FALLBACK_SENSOR_DBUS_DEVICE", None)

        battery = _make_battery()
        battery.online = None  # never had a successful main-loop cycle
        battery.port = "/ble_test"
        battery.cells = []
        battery.current = None
        battery.soc = None
        battery.voltage_calc = None
        battery.current_calc = None
        battery.power_calc = None
        battery.power_calc_last_time = None
        battery.soc_calc = None
        battery.temperature_1 = None
        battery.temperature_2 = None
        battery.temperature_3 = None
        battery.temperature_4 = None
        battery.dbus_fallback_objects = {
            "Voltage": _FakeDbusItem(26.1),
            "Temperature": _FakeDbusItem(28.0),
        }
        battery.refresh_data = lambda: False
        battery.last_refresh_duration = 0.0

        helper = DbusHelper.__new__(DbusHelper)
        helper.battery = battery
        helper.stale_serving = False
        helper.stale_clock_start = None
        helper.stale_concluded_at = None
        helper.cell_voltages_good = None
        helper.disconnect_threshold = None
        helper.bms_cable_alarm = 0
        helper.error = {
            "count": 3,
            "timestamp_first": int(now()) - 30,
            "timestamp_last": int(now()),
            "cleared": True,
        }
        helper.publish_dbus = lambda: None
        helper.telemetry_upload = lambda: None
        return helper

    def test_stale_engages_marks_offline_and_serves_fallback(self, monkeypatch):
        helper = self._make_helper(monkeypatch)
        loop = self._FakeLoop()

        helper.publish_battery(loop)

        # engagement must not depend on the battery ever having been online
        assert helper.stale_serving is True
        # the engagement must mark the battery offline so the fallback serves
        assert helper.battery.online is False
        # the aggregate's contract: a fallback-configured battery ALWAYS
        # publishes temperature while stale-serving
        assert helper.battery.get_value_from_fallback_sensor("Temperature") == 28.0
        assert helper.battery.get_temperature() == 28.0
        assert helper.battery.get_voltage() == 26.1
        # no exit while the fallback is serving
        assert loop.quit_called is False
