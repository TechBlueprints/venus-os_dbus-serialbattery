# -*- coding: utf-8 -*-
"""Tests for the bulk voltage ramp (BULK_VOLTAGE_RAMP_ENABLE) in battery.py."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery"))

import utils  # noqa: E402
from battery import Battery, Cell  # noqa: E402


class _DummyBattery(Battery):
    """Minimal concrete battery so the abstract base class can be instantiated."""

    def test_connection(self) -> bool:
        return True

    def get_settings(self) -> bool:
        return True

    def refresh_data(self) -> bool:
        return True


def _make_battery(cell_count: int = 4, cell_voltage: float = 3.30, soc: float = 50.0) -> _DummyBattery:
    battery = _DummyBattery("test", 9600, "0")
    battery.cell_count = cell_count
    battery.cells = [Cell(False) for _ in range(cell_count)]
    for cell in battery.cells:
        cell.voltage = cell_voltage
    battery.soc = soc
    battery.soc_calc = soc
    battery.current = 20.0
    battery.current_calc = 20.0
    battery.min_battery_voltage = round(utils.MIN_CELL_VOLTAGE * cell_count, 2)
    battery.max_battery_voltage = round(utils.MAX_CELL_VOLTAGE * cell_count, 2)
    return battery


@pytest.fixture
def ramp_config(monkeypatch):
    """Clint's example: 4 cells, 14.0 V target, 14.8 V max bulk at 60 %, target at 95 %."""
    monkeypatch.setattr(utils, "MIN_CELL_VOLTAGE", 2.9)
    monkeypatch.setattr(utils, "MAX_CELL_VOLTAGE", 3.5)
    monkeypatch.setattr(utils, "FLOAT_CELL_VOLTAGE", 3.375)
    monkeypatch.setattr(utils, "BULK_VOLTAGE_RAMP_ENABLE", True)
    monkeypatch.setattr(utils, "BULK_CELL_VOLTAGE_MAX", 3.7)
    monkeypatch.setattr(utils, "BULK_CELL_VOLTAGE_MIN", 3.5)
    monkeypatch.setattr(utils, "BULK_VOLTAGE_RAMP_SOC_START", 60)
    monkeypatch.setattr(utils, "BULK_VOLTAGE_RAMP_SOC_END", 95)
    # Keep the surrounding CVL logic in its default, deterministic shape
    monkeypatch.setattr(utils, "CVCM_ENABLE", True)
    monkeypatch.setattr(utils, "CVL_CONTROLLER_MODE", 1)
    monkeypatch.setattr(utils, "CVL_RECOVERY_HOLD_SEC", 60)
    monkeypatch.setattr(utils, "CVL_RECOVERY_RATE_V_PER_SEC", 0.001)
    monkeypatch.setattr(utils, "SWITCH_TO_FLOAT_WAIT_FOR_SEC", 900)
    monkeypatch.setattr(utils, "SWITCH_TO_FLOAT_CELL_VOLTAGE_DIFF", 0.010)
    monkeypatch.setattr(utils, "SWITCH_TO_FLOAT_CELL_VOLTAGE_DEVIATION", 0.003)
    monkeypatch.setattr(utils, "SWITCH_TO_BULK_SOC_THRESHOLD", 80)
    monkeypatch.setattr(utils, "SWITCH_TO_BULK_CELL_VOLTAGE_DIFF", 0.080)
    monkeypatch.setattr(utils, "SOC_RESET_AFTER_DAYS", False)
    monkeypatch.setattr(utils, "CHARGE_MODE", 1)
    monkeypatch.setattr(utils, "GUI_PARAMETERS_SHOW_ADDITIONAL_INFO", False)


# =============================================================================
# get_bulk_ramp_voltage()
# =============================================================================


class TestGetBulkRampVoltage:
    @pytest.mark.parametrize(
        "soc,expected",
        [
            (0, 14.8),
            (60, 14.8),
            (70, 14.571),
            (80, 14.343),
            (90, 14.114),
            (95, 14.0),
            (100, 14.0),
        ],
    )
    def test_linear_between_start_and_end(self, ramp_config, soc, expected):
        battery = _make_battery(soc=soc)
        assert battery.get_bulk_ramp_voltage() == pytest.approx(expected, abs=0.001)

    def test_disabled_returns_none(self, ramp_config, monkeypatch):
        monkeypatch.setattr(utils, "BULK_VOLTAGE_RAMP_ENABLE", False)
        assert _make_battery(soc=50).get_bulk_ramp_voltage() is None

    def test_unknown_soc_returns_none(self, ramp_config):
        battery = _make_battery(soc=50)
        battery.soc_calc = None
        assert battery.get_bulk_ramp_voltage() is None

    def test_unknown_cell_count_returns_none(self, ramp_config):
        battery = _make_battery(soc=50)
        battery.cell_count = None
        assert battery.get_bulk_ramp_voltage() is None

    def test_misconfigured_soc_window_falls_back_to_step(self, ramp_config, monkeypatch):
        monkeypatch.setattr(utils, "BULK_VOLTAGE_RAMP_SOC_START", 95)
        monkeypatch.setattr(utils, "BULK_VOLTAGE_RAMP_SOC_END", 60)
        assert _make_battery(soc=50).get_bulk_ramp_voltage() == pytest.approx(14.8)
        assert _make_battery(soc=70).get_bulk_ramp_voltage() == pytest.approx(14.0)


# =============================================================================
# manage_charge_voltage_limit() integration
# =============================================================================


class TestManageChargeVoltageLimitWithRamp:
    def test_bulk_uses_ramp_voltage(self, ramp_config):
        battery = _make_battery(soc=70)
        battery.manage_charge_voltage_limit()
        assert battery.control_voltage == pytest.approx(14.571, abs=0.001)
        assert battery.charge_mode.startswith("Bulk (Ramp)")
        assert "OVP" not in battery.charge_mode

    def test_bulk_ramp_falls_with_soc_without_ovp_label(self, ramp_config):
        battery = _make_battery(soc=70)
        battery.manage_charge_voltage_limit()
        first = battery.control_voltage
        battery.soc_calc = 80
        battery.manage_charge_voltage_limit()
        assert battery.control_voltage < first
        assert battery.control_voltage == pytest.approx(14.343, abs=0.001)
        assert "OVP" not in battery.charge_mode
        assert battery.control_voltage_last_limit_time is None

    def test_bulk_at_end_soc_is_plain_bulk_at_max_voltage(self, ramp_config):
        battery = _make_battery(soc=96)
        battery.manage_charge_voltage_limit()
        assert battery.control_voltage == pytest.approx(14.0)
        assert battery.charge_mode.startswith("Bulk │")

    def test_ramp_disabled_keeps_max_battery_voltage(self, ramp_config, monkeypatch):
        monkeypatch.setattr(utils, "BULK_VOLTAGE_RAMP_ENABLE", False)
        battery = _make_battery(soc=70)
        battery.manage_charge_voltage_limit()
        assert battery.control_voltage == pytest.approx(14.0)
        assert battery.charge_mode.startswith("Bulk │")

    def test_absorption_ignores_ramp(self, ramp_config):
        # Pack at the target voltage with balanced cells: timer starts, mode is Absorption
        battery = _make_battery(cell_voltage=3.5, soc=70)
        battery.manage_charge_voltage_limit()
        assert battery.charge_mode.startswith("Absorption")
        assert "Ramp" not in battery.charge_mode
        assert battery.control_voltage == pytest.approx(14.0)

    def test_cell_overvoltage_still_clamps_below_max_battery_voltage(self, ramp_config):
        # One cell above MAX_CELL_VOLTAGE while the pack SoC says 70 %: the P-controller must win
        battery = _make_battery(cell_voltage=3.40, soc=70)
        battery.cells[0].voltage = 3.60
        # cells unbalanced -> stays in bulk (no float timer)
        battery.manage_charge_voltage_limit()
        assert battery.control_voltage <= battery.max_battery_voltage
        assert "Cell OVP" in battery.charge_mode

    def test_ramp_never_below_max_battery_voltage(self, ramp_config, monkeypatch):
        # A BULK_CELL_VOLTAGE_MIN below MAX_CELL_VOLTAGE is flagged at startup and ignored here
        monkeypatch.setattr(utils, "BULK_CELL_VOLTAGE_MIN", 3.4)
        battery = _make_battery(soc=94)
        battery.manage_charge_voltage_limit()
        assert battery.control_voltage >= battery.max_battery_voltage
