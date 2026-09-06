# -*- coding: utf-8 -*-
"""install_ble_connection_manager(): the four startup lines, verbatim.

These strings are the fleet contract for every consumer of the shared
bleak-connection-manager (CONSUMER_MIGRATION.md names this driver's wording as
the reference) and they are watched by substring on the boxes. Four cases, one
line each, distinct by the operator action they call for: loaded (none),
no shared install (none: normal on every upstream box), DIR empty while the
manager is on (fix the config), shared install present but unusable (fix the
install). A change here must reach the watch's owner first.
"""

import logging
import os
import sys
import types

import pytest

DRIVER_DIR = os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery")
sys.path.insert(0, DRIVER_DIR)

import ble_stack  # noqa: E402
import utils  # noqa: E402
import utils_ble_manager  # noqa: E402


@pytest.fixture
def manager_on(monkeypatch):
    monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER", True, raising=False)
    monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER_DIR", "/data/bcm", raising=False)
    monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER_VALIDATION", False, raising=False)
    monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER_LINK_CAPS", [], raising=False)
    monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER_WRAP_SCANNER", False, raising=False)
    monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER_FORCE_START_NOTIFY", True, raising=False)
    monkeypatch.setattr(utils, "BLUETOOTH_ADAPTERS", [], raising=False)
    monkeypatch.delitem(sys.modules, "bleak_connection_manager", raising=False)
    monkeypatch.setattr(ble_stack, "shared_failure", None)


def _fake_bcm(monkeypatch, calls, accepts_policy=True):
    mod = types.ModuleType("bleak_connection_manager")
    mod.__file__ = "/data/bcm/src/bleak_connection_manager/__init__.py"
    if accepts_policy:

        def install_bleak_catcher(owner, adapters=(), link_caps=None, wrap_scanner=False, validate_connection=None, force_start_notify=None):
            calls.append((owner, {"force_start_notify": force_start_notify}))

    else:  # an install from before 2026-09-02 (BCM 159536a): no such parameter

        def install_bleak_catcher(owner, adapters=(), link_caps=None, wrap_scanner=False, validate_connection=None):
            calls.append((owner, {}))

    mod.install_bleak_catcher = install_bleak_catcher
    monkeypatch.setitem(sys.modules, "bleak_connection_manager", mod)


def test_loaded_is_reported_once_at_info_with_the_folder(manager_on, monkeypatch, caplog):
    calls = []
    _fake_bcm(monkeypatch, calls)
    with caplog.at_level(logging.INFO, logger="SerialBattery"):
        assert utils_ble_manager.install_ble_connection_manager("AB:80:72:54:E0:B4") is True
    lines = [r for r in caplog.records if "BLE coordination" in r.getMessage()]
    assert [(r.levelname, r.getMessage()) for r in lines] == [
        ("INFO", "BLE coordination: bleak_connection_manager loaded from /data/bcm/src/bleak_connection_manager")
    ]
    assert calls[0][0] == "dbus-serialbattery.ab807254e0b4"
    assert calls[0][1]["force_start_notify"] is True


def test_an_install_that_predates_the_policy_parameter_keeps_the_catcher_and_says_so(manager_on, monkeypatch, caplog):
    calls = []
    _fake_bcm(monkeypatch, calls, accepts_policy=False)
    monkeypatch.delenv("BCM_FORCE_START_NOTIFY", raising=False)
    with caplog.at_level(logging.INFO, logger="SerialBattery"):
        assert utils_ble_manager.install_ble_connection_manager("AB:80:72:54:E0:B4") is True, "a TypeError here would have lost the catcher"
    assert calls == [("dbus-serialbattery.ab807254e0b4", {})]
    assert os.environ["BCM_FORCE_START_NOTIFY"] == "true"
    assert [(r.levelname, r.getMessage()) for r in caplog.records if "BLE coordination" in r.getMessage()] == [
        (
            "WARNING",
            "BLE coordination: shared install at /data/bcm predates the force_start_notify parameter; "
            "StartNotify policy passed through the legacy BCM_FORCE_START_NOTIFY environment",
        ),
        ("INFO", "BLE coordination: bleak_connection_manager loaded from /data/bcm/src/bleak_connection_manager"),
    ]


def test_no_shared_install_is_a_warning_not_a_fault(manager_on, caplog):
    with caplog.at_level(logging.INFO, logger="SerialBattery"):
        assert utils_ble_manager.install_ble_connection_manager("AB:80:72:54:E0:B4") is False
    assert [(r.levelname, r.getMessage()) for r in caplog.records if "BLE coordination" in r.getMessage()] == [
        ("WARNING", "BLE coordination: no shared install at /data/bcm; running uncoordinated, no claims, no adapter routing, no card recovery")
    ]


def test_manager_on_with_an_empty_dir_names_the_misconfiguration(manager_on, monkeypatch, caplog):
    monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER_DIR", "")
    with caplog.at_level(logging.INFO, logger="SerialBattery"):
        assert utils_ble_manager.install_ble_connection_manager("AB:80:72:54:E0:B4") is False
    assert [(r.levelname, r.getMessage()) for r in caplog.records if "BLE coordination" in r.getMessage()] == [
        (
            "WARNING",
            "BLE coordination: BLUETOOTH_CONNECTION_MANAGER is on but BLUETOOTH_CONNECTION_MANAGER_DIR is empty; "
            "running uncoordinated, no claims, no adapter routing, no card recovery",
        )
    ]


def test_a_broken_shared_install_is_an_error_that_says_why(manager_on, monkeypatch, caplog):
    monkeypatch.setattr(ble_stack, "shared_failure", "RuntimeError('half-installed')")
    with caplog.at_level(logging.INFO, logger="SerialBattery"):
        assert utils_ble_manager.install_ble_connection_manager("AB:80:72:54:E0:B4") is False
    assert [(r.levelname, r.getMessage()) for r in caplog.records if "BLE coordination" in r.getMessage()] == [
        (
            "ERROR",
            "BLE coordination: shared install at /data/bcm is present but unusable, running uncoordinated: RuntimeError('half-installed')",
        )
    ]


def test_manager_off_says_nothing(manager_on, monkeypatch, caplog):
    monkeypatch.setattr(utils, "BLUETOOTH_CONNECTION_MANAGER", False)
    with caplog.at_level(logging.DEBUG, logger="SerialBattery"):
        assert utils_ble_manager.install_ble_connection_manager("AB:80:72:54:E0:B4") is False
    assert not [r for r in caplog.records if "BLE coordination" in r.getMessage()]
