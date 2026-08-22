# -*- coding: utf-8 -*-
"""Tests for the BLE connection manager wiring in utils_ble_manager.

The module under test deliberately imports neither bleak nor the vendored
bleak_connection_manager at module scope (the whole point is installing the
catcher before anything imports bleak), so these tests run without either:
the vendored package is replaced by a stub in sys.modules and the config
flags are patched on the utils module the wiring reads at call time.
"""

import os
import sys
import types

import pytest

DRIVER_DIR = os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery")
sys.path.insert(0, DRIVER_DIR)

import utils  # noqa: E402
import utils_ble_manager  # noqa: E402


class TestParseLinkCaps:
    def test_parses_entries(self):
        assert utils_ble_manager.parse_link_caps(["hci0:5", " hci1 : 7 "]) == {"hci0": 5, "hci1": 7}

    def test_empty_and_blank_entries_are_skipped(self):
        assert utils_ble_manager.parse_link_caps([]) == {}
        assert utils_ble_manager.parse_link_caps(["", "  "]) == {}

    def test_malformed_entries_are_skipped_not_guessed(self):
        # no colon, non-integer, non-positive, missing adapter: a wrong cap
        # silently gates connections, so none of these may survive
        assert utils_ble_manager.parse_link_caps(["hci0"]) == {}
        assert utils_ble_manager.parse_link_caps(["hci0:many"]) == {}
        assert utils_ble_manager.parse_link_caps(["hci0:0", "hci1:-3"]) == {}
        assert utils_ble_manager.parse_link_caps([":5"]) == {}

    def test_malformed_entry_does_not_poison_the_rest(self):
        assert utils_ble_manager.parse_link_caps(["hci0:zz", "hci1:4"]) == {"hci1": 4}

    def test_last_duplicate_wins(self):
        assert utils_ble_manager.parse_link_caps(["hci0:5", "hci0:3"]) == {"hci0": 3}

    def test_adapter_macs_survive_the_split(self):
        # a MAC is full of colons: the split has to be on the last one, or
        # the adapter comes out as "00:1A:7D:DA:71" with a cap of 13
        assert utils_ble_manager.parse_link_caps(["00:1A:7D:DA:71:13:5"]) == {"00:1A:7D:DA:71:13": 5}
        assert utils_ble_manager.parse_link_caps(["00:1A:7D:DA:71:13:5", "hci4:7"]) == {
            "00:1A:7D:DA:71:13": 5,
            "hci4": 7,
        }

    def test_a_mac_without_a_cap_is_still_rejected(self):
        assert utils_ble_manager.parse_link_caps(["00:1A:7D:DA:71:13:x"]) == {}


class TestInstallBleConnectionManager:
    @pytest.fixture
    def catcher_stub(self, monkeypatch):
        """A stand-in vendored package that records the install call."""
        calls = []

        def install_bleak_catcher(owner, **kwargs):
            calls.append((owner, kwargs))

        module = types.ModuleType("bleak_connection_manager")
        module.install_bleak_catcher = install_bleak_catcher
        monkeypatch.setitem(sys.modules, "bleak_connection_manager", module)

        validators = types.ModuleType("bleak_connection_manager.validators")

        def validate_gatt_services(client):
            raise NotImplementedError

        validators.validate_gatt_services = validate_gatt_services
        validators.tolerate_late_gatt = lambda v: ("late-gatt-wrapped", v)
        module.validators = validators
        monkeypatch.setitem(sys.modules, "bleak_connection_manager.validators", validators)
        return calls

    def test_disabled_by_default_installs_nothing(self, monkeypatch, catcher_stub):
        monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER", False)
        assert utils_ble_manager.install_ble_connection_manager("C8:47:8C:00:00:00") is False
        assert catcher_stub == []

    def test_installs_with_config_handed_over_verbatim(self, monkeypatch, catcher_stub):
        monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER", True)
        monkeypatch.setattr(utils, "BLUETOOTH_ADAPTERS", ["C8:47:8C:00:00:00@hci1", "hci2"])
        monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER_LINK_CAPS", ["hci1:5"])
        monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER_WRAP_SCANNER", True)

        assert utils_ble_manager.install_ble_connection_manager("C8:47:8C:00:00:00") is True

        assert len(catcher_stub) == 1
        owner, kwargs = catcher_stub[0]
        # the owner names this battery's claims: service plus MAC, no colons
        assert owner == "dbus-serialbattery.c8478c000000"
        # BLUETOOTH_ADAPTERS entries pass through verbatim - the library
        # parses the same MAC@hciX / hciX forms itself
        assert kwargs["adapters"] == ["C8:47:8C:00:00:00@hci1", "hci2"]
        assert kwargs["link_caps"] == {"hci1": 5}
        assert kwargs["wrap_scanner"] is True
        # validation is its own opt-in; not requested here
        assert kwargs["validate_connection"] is None

    def test_validation_opt_in_passes_wrapped_gatt_validator(self, monkeypatch, catcher_stub):
        monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER", True)
        monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER_VALIDATION", True)

        assert utils_ble_manager.install_ble_connection_manager("C8:47:8C:00:00:00") is True

        _, kwargs = catcher_stub[0]
        wrapped, inner = kwargs["validate_connection"]
        assert wrapped == "late-gatt-wrapped"
        assert inner.__name__ == "validate_gatt_services"

    def test_failed_install_is_swallowed(self, monkeypatch):
        monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER", True)

        def explode(owner, **kwargs):
            raise RuntimeError("no /run/bt-claims on this platform")

        module = types.ModuleType("bleak_connection_manager")
        module.install_bleak_catcher = explode
        monkeypatch.setitem(sys.modules, "bleak_connection_manager", module)

        # coordination is an optimization: the driver must still start
        assert utils_ble_manager.install_ble_connection_manager("C8:47:8C:00:00:00") is False

    def test_missing_vendored_package_is_swallowed(self, monkeypatch):
        monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER", True)
        monkeypatch.setitem(sys.modules, "bleak_connection_manager", None)
        assert utils_ble_manager.install_ble_connection_manager("C8:47:8C:00:00:00") is False
