# -*- coding: utf-8 -*-
"""Tests for the external sensor override (EXTERNAL_SENSOR_DBUS_PATH_*) and fallback sensor (FALLBACK_SENSOR_DBUS_*)."""

import os
import sys

import pytest

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

    def test_held_bms_value_served_when_no_fallback_path(self):
        battery = _make_battery()
        battery.online = False
        battery.dbus_fallback_objects = {"Current": _FakeDbusItem(-0.2)}
        assert battery.get_voltage() == 26.0

    def test_fallback_none_value_falls_back_to_held_bms_value(self):
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

    def test_fallback_soc_used_while_bms_offline(self, monkeypatch):
        monkeypatch.setattr(utils, "SOC_CALCULATION", False)
        battery = _make_battery()
        battery.soc = 97.0
        battery.online = False
        battery.dbus_fallback_objects = {"Soc": _FakeDbusItem(93.4)}
        assert battery.get_soc() == 93.4

    def test_held_bms_soc_served_when_no_fallback_soc_path(self, monkeypatch):
        monkeypatch.setattr(utils, "SOC_CALCULATION", False)
        battery = _make_battery()
        battery.soc = 97.0
        battery.online = False
        battery.dbus_fallback_objects = {"Voltage": _FakeDbusItem(25.5)}
        assert battery.get_soc() == 97.0

    def test_capacity_remain_derived_from_soc_while_offline(self, monkeypatch):
        monkeypatch.setattr(utils, "SOC_CALCULATION", False)
        battery = _make_battery()
        battery.online = False
        battery.capacity = 300.0
        battery.soc_calc = 93.4
        battery.capacity_remain = 291.0  # frozen BMS value must not win while offline
        assert battery.get_capacity_remain() == 300.0 * 93.4 / 100

    def test_capacity_remain_uses_bms_value_while_online(self, monkeypatch):
        monkeypatch.setattr(utils, "SOC_CALCULATION", False)
        battery = _make_battery()
        battery.capacity = 300.0
        battery.soc_calc = 97.0
        battery.capacity_remain = 291.0
        assert battery.get_capacity_remain() == 291.0


class _FakeCell:
    def __init__(self, voltage):
        self.voltage = voltage


class TestFallbackCellProjection:
    def _make_helper(self, cell_voltages):
        helper = DbusHelper.__new__(DbusHelper)
        helper.battery = _make_battery()
        helper.battery.cell_count = len(cell_voltages)
        helper.battery.cells = [_FakeCell(v) for v in cell_voltages]
        helper.fallback_cell_snapshot = None
        helper.fallback_charge_blocked = False
        helper.fallback_discharge_blocked = False
        return helper

    def test_snapshot_captures_cells(self):
        helper = self._make_helper([3.30, 3.31, 3.29, 3.35])
        helper.fallback_take_cell_snapshot()
        assert helper.fallback_cell_snapshot == [3.30, 3.31, 3.29, 3.35]

    def test_snapshot_none_when_cell_unread(self):
        helper = self._make_helper([3.30, None, 3.29, 3.35])
        helper.fallback_take_cell_snapshot()
        assert helper.fallback_cell_snapshot is None

    def test_projection_distributes_delta_evenly(self):
        helper = self._make_helper([3.30, 3.31, 3.29, 3.35])
        helper.fallback_take_cell_snapshot()
        # snapshot sum 13.25, live shunt 13.05 -> delta -0.05 per cell
        projected = helper.fallback_project_cells(13.05)
        assert projected == pytest.approx([3.25, 3.26, 3.24, 3.30])

    def test_projection_preserves_cell_ordering(self):
        helper = self._make_helper([3.30, 3.31, 3.29, 3.35])
        helper.fallback_take_cell_snapshot()
        projected = helper.fallback_project_cells(12.85)
        assert projected.index(max(projected)) == 3
        assert projected.index(min(projected)) == 2

    def test_projection_none_without_shunt_voltage(self):
        helper = self._make_helper([3.30, 3.31, 3.29, 3.35])
        helper.fallback_take_cell_snapshot()
        assert helper.fallback_project_cells(None) is None

    def test_projection_rejects_implausible_delta(self):
        # one shunt across a series string (or wrong pairing): voltage ~2x the pack
        helper = self._make_helper([3.30, 3.31, 3.29, 3.35])
        helper.fallback_take_cell_snapshot()
        assert helper.fallback_project_cells(26.5) is None


class TestFallbackBandFlags:
    def _make_helper(self, monkeypatch, zone_min=2.70, zone_max=3.55):
        # the safe zone is its own dedicated config (FALLBACK_SAFE_CELL_VOLTAGE_MIN/MAX),
        # set from the battery manufacturer's spec — deliberately not shared with any
        # other voltage option
        monkeypatch.setattr(utils, "FALLBACK_SAFE_CELL_VOLTAGE_MIN", zone_min)
        monkeypatch.setattr(utils, "FALLBACK_SAFE_CELL_VOLTAGE_MAX", zone_max)
        helper = DbusHelper.__new__(DbusHelper)
        helper.battery = _make_battery()
        helper.fallback_charge_blocked = False
        helper.fallback_discharge_blocked = False
        return helper

    def test_zone_unset_blocks_charging_keeps_discharging(self, monkeypatch):
        helper = self._make_helper(monkeypatch, zone_min=0, zone_max=0)
        helper.fallback_update_band_flags(3.20, 3.35)
        assert helper.fallback_charge_blocked is True
        assert helper.fallback_discharge_blocked is False

    def test_inside_zone_allows_both(self, monkeypatch):
        helper = self._make_helper(monkeypatch)
        helper.fallback_update_band_flags(3.20, 3.35)
        assert helper.fallback_charge_blocked is False
        assert helper.fallback_discharge_blocked is False

    def test_above_zone_blocks_charging_only(self, monkeypatch):
        helper = self._make_helper(monkeypatch)
        helper.fallback_update_band_flags(3.40, 3.56)
        assert helper.fallback_charge_blocked is True
        assert helper.fallback_discharge_blocked is False

    def test_below_zone_blocks_discharging_only(self, monkeypatch):
        helper = self._make_helper(monkeypatch)
        helper.fallback_update_band_flags(2.69, 3.10)
        assert helper.fallback_charge_blocked is False
        assert helper.fallback_discharge_blocked is True

    def test_charge_reallow_requires_hysteresis_margin(self, monkeypatch):
        helper = self._make_helper(monkeypatch)
        helper.fallback_update_band_flags(3.40, 3.56)
        assert helper.fallback_charge_blocked is True
        # back under the edge but within the margin: stays blocked
        helper.fallback_update_band_flags(3.40, 3.52)
        assert helper.fallback_charge_blocked is True
        # inside by more than the margin: re-allowed
        helper.fallback_update_band_flags(3.40, 3.49)
        assert helper.fallback_charge_blocked is False

    def test_discharge_reallow_requires_hysteresis_margin(self, monkeypatch):
        helper = self._make_helper(monkeypatch)
        helper.fallback_update_band_flags(2.69, 3.10)
        assert helper.fallback_discharge_blocked is True
        helper.fallback_update_band_flags(2.73, 3.10)
        assert helper.fallback_discharge_blocked is True
        helper.fallback_update_band_flags(2.76, 3.10)
        assert helper.fallback_discharge_blocked is False

    def test_spread_wider_than_band_blocks_both(self, monkeypatch):
        helper = self._make_helper(monkeypatch)
        helper.fallback_update_band_flags(2.65, 3.60)
        assert helper.fallback_charge_blocked is True
        assert helper.fallback_discharge_blocked is True


class TestUtilsImportTimeConfig:
    """Import utils.py for real against a config file, so import-time validation
    and clamping runs — the class of bug the monkeypatch pattern structurally
    misses (production incident: config values silently clamped + error #119)."""

    def _import_utils_with_config(self, tmp_path, config_ini):
        import json
        import shutil
        import subprocess

        src = os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery")
        shutil.copy(os.path.join(src, "utils.py"), tmp_path / "utils.py")
        shutil.copy(os.path.join(src, "config.default.ini"), tmp_path / "config.default.ini")
        (tmp_path / "config.ini").write_text(config_ini)
        # minimal serial stub so utils imports without pyserial
        (tmp_path / "serial.py").write_text("class Serial:\n    pass\n\nclass SerialException(IOError):\n    pass\n\nEIGHTBITS = 8\nPARITY_NONE = 'N'\nSTOPBITS_ONE = 1\n")
        result = subprocess.run(
            [sys.executable, "-c", "import utils, json; print(json.dumps(utils.errors_in_config))"],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, f"utils import failed: {result.stderr}"
        return json.loads(result.stdout.strip().splitlines()[-1])

    def test_fallback_config_loads_without_config_errors(self, tmp_path):
        errors = self._import_utils_with_config(
            tmp_path,
            "[DEFAULT]\n"
            "FALLBACK_SENSOR_DBUS_DEVICE = ble_test:com.victronenergy.battery.ttyS5\n"
            "FALLBACK_SENSOR_DBUS_PATH_VOLTAGE = /Dc/0/Voltage\n"
            "FALLBACK_SENSOR_DBUS_PATH_CURRENT = /Dc/0/Current\n"
            "FALLBACK_SENSOR_DBUS_PATH_TEMPERATURE = /Dc/0/Temperature\n"
            "FALLBACK_SENSOR_DBUS_PATH_SOC = /Soc\n"
            "FALLBACK_STOP_MINUTES = 480\n"
            "FALLBACK_SAFE_CELL_VOLTAGE_MIN = 2.70\n"
            "FALLBACK_SAFE_CELL_VOLTAGE_MAX = 3.55\n",
        )
        assert errors == []

    def test_timeout_minutes_without_device_reports_config_error(self, tmp_path):
        errors = self._import_utils_with_config(
            tmp_path,
            "[DEFAULT]\nFALLBACK_STOP_MINUTES = 480\n",
        )
        assert any("FALLBACK_STOP_MINUTES" in error for error in errors)

    def test_safe_zone_min_without_max_reports_config_error(self, tmp_path):
        errors = self._import_utils_with_config(
            tmp_path,
            "[DEFAULT]\nFALLBACK_SAFE_CELL_VOLTAGE_MIN = 2.70\n",
        )
        assert any("FALLBACK_SAFE_CELL_VOLTAGE" in error for error in errors)


class TestFallbackModeEligible:
    def _make_helper(self, monkeypatch, fallback_objects, minutes=480.0, block_on_disconnect=False, cell_voltages_good=None):
        monkeypatch.setattr(utils, "FALLBACK_STOP_MINUTES", minutes)
        monkeypatch.setattr(utils, "BLOCK_ON_DISCONNECT", block_on_disconnect)
        helper = DbusHelper.__new__(DbusHelper)
        helper.battery = _make_battery()
        helper.battery.dbus_fallback_objects = fallback_objects
        helper.cell_voltages_good = cell_voltages_good
        return helper

    def test_eligible_with_fallback_sensor(self, monkeypatch):
        helper = self._make_helper(monkeypatch, {"Voltage": _FakeDbusItem(25.5)})
        assert helper.fallback_mode_eligible() is True

    def test_eligible_regardless_of_cell_voltages(self, monkeypatch):
        # the BMS's own cell-level protection acts independently of this driver,
        # so cells outside the BLOCK_ON_DISCONNECT band must not block fallback mode
        helper = self._make_helper(monkeypatch, {"Voltage": _FakeDbusItem(25.5)}, cell_voltages_good=False)
        assert helper.fallback_mode_eligible() is True

    def test_not_eligible_without_fallback_sensor(self, monkeypatch):
        helper = self._make_helper(monkeypatch, None)
        assert helper.fallback_mode_eligible() is False

    def test_eligible_with_stop_minutes_zero(self, monkeypatch):
        # configuring the fallback device is the opt-in;
        # FALLBACK_STOP_MINUTES = 0 means hold forever, not disabled
        helper = self._make_helper(monkeypatch, {"Voltage": _FakeDbusItem(25.5)}, minutes=0)
        assert helper.fallback_mode_eligible() is True

    def test_not_eligible_with_block_on_disconnect(self, monkeypatch):
        helper = self._make_helper(monkeypatch, {"Voltage": _FakeDbusItem(25.5)}, block_on_disconnect=True)
        assert helper.fallback_mode_eligible() is False


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


class TestNeverOnlineFallbackEngagement:
    """Regression for the never-online path (production incident 2026-08-13).

    A driver that only got data during init and lost BLE before its first
    successful main-loop cycle still has ``battery.online = None``. Fallback
    mode must engage on eligibility alone, mark the battery offline, and
    the fallback getter must actually serve — an earlier half-fix engaged
    fallback mode but left ``online = None``, so the fallback refused to
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

        monkeypatch.setattr(utils, "FALLBACK_STOP_MINUTES", 480.0)
        monkeypatch.setattr(utils, "BLOCK_ON_DISCONNECT", False)
        monkeypatch.setattr(utils, "FALLBACK_BMS_CABLE_WARN_MINUTES", 480.0)
        monkeypatch.setattr(utils, "EXTERNAL_SENSOR_DBUS_DEVICE", None)
        monkeypatch.setattr(utils, "FALLBACK_SENSOR_DBUS_DEVICE", None)

        battery = _make_battery()
        battery.online = None  # never had a successful main-loop cycle
        battery.port = "/ble_test"
        battery.cells = []
        battery.cell_count = None
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
        helper.fallback_mode = False
        helper.fallback_last_alive = None
        helper.fallback_concluded_at = None
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

    def test_fallback_engages_marks_offline_and_serves(self, monkeypatch):
        helper = self._make_helper(monkeypatch)
        loop = self._FakeLoop()

        helper.publish_battery(loop)

        # engagement must not depend on the battery ever having been online
        assert helper.fallback_mode is True
        # the engagement must mark the battery offline so the fallback serves
        assert helper.battery.online is False
        # the aggregate's contract: a fallback-configured battery ALWAYS
        # publishes temperature while in fallback mode
        assert helper.battery.get_value_from_fallback_sensor("Temperature") == 28.0
        assert helper.battery.get_temperature() == 28.0
        assert helper.battery.get_voltage() == 26.1
        # no exit while the fallback is serving
        assert loop.quit_called is False
        # BmsCable stays quiet inside the warn window while the fallback covers
        assert helper.bms_cable_alarm == 0

    def test_both_instruments_dark_alarms_immediately(self, monkeypatch):
        # BMS down AND fallback not delivering: a real fault — no warn-window
        # grace, BmsCable goes straight to alarm (2)
        helper = self._make_helper(monkeypatch)
        helper.battery.dbus_fallback_objects = {
            "Voltage": _FakeDbusItem(None),
            "Temperature": _FakeDbusItem(None),
        }
        loop = self._FakeLoop()

        helper.publish_battery(loop)

        assert helper.fallback_mode is True  # engaged (device present) but dark
        assert helper.bms_cable_alarm == 2
        # dark grace not yet expired: no exit, values not reset yet
        assert loop.quit_called is False
