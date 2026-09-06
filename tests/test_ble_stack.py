# -*- coding: utf-8 -*-
"""ble_stack decides where the BLE stack comes from, before anything imports bleak.

These tests never import bleak. They exercise the decision and the sys.path
arrangement only, which is the whole of what this module does.
"""

import importlib
import os
import re
import sys

import pytest

DRIVER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery"))
if DRIVER_DIR not in sys.path:
    sys.path.insert(0, DRIVER_DIR)

import ble_stack  # noqa: E402


@pytest.fixture(autouse=True)
def _fresh():
    """Each test gets an undecided module and its own sys.path/sys.modules."""
    path_before = list(sys.path)
    modules_before = dict(sys.modules)
    ble_stack._decided = None
    ble_stack.shared_failure = None
    sys.modules.pop("bleak_connection_manager", None)
    yield
    sys.path[:] = path_before
    for name in set(sys.modules) - set(modules_before):
        del sys.modules[name]
    sys.modules.update(modules_before)
    ble_stack._decided = None
    ble_stack.shared_failure = None


def _make_shared(tmp_path, body="VERSION = 'shared'\n"):
    """A minimal box install: <dir>/src/bleak_connection_manager/__init__.py."""
    pkg = tmp_path / "src" / "bleak_connection_manager"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text(body)
    return str(tmp_path)


def test_an_empty_setting_never_looks_and_uses_the_vendored_stack():
    assert ble_stack.ensure_ble_stack("") == "vendored"
    assert ble_stack.VENDORED_BLE_DIR in sys.path
    assert ble_stack.shared_failure is None


def test_a_configured_folder_that_does_not_exist_falls_back_quietly(tmp_path):
    assert ble_stack.ensure_ble_stack(str(tmp_path / "nope")) == "vendored"
    assert ble_stack.VENDORED_BLE_DIR in sys.path
    # absent is not a failure: nothing was present to fail
    assert ble_stack.shared_failure is None


def test_a_shared_install_is_used_and_its_roots_come_first(tmp_path):
    shared = _make_shared(tmp_path)
    assert ble_stack.ensure_ble_stack(shared) == "shared"
    assert "bleak_connection_manager" in sys.modules
    # the shared roots must precede the vendored stack, or ext/ble would win
    assert ble_stack.VENDORED_BLE_DIR not in sys.path
    assert sys.path[0] == os.path.join(shared, "src")


def test_a_present_but_broken_install_is_withdrawn_completely(tmp_path):
    shared = _make_shared(tmp_path, body="raise RuntimeError('boom')\n")
    path_before = list(sys.path)

    assert ble_stack.ensure_ble_stack(shared) == "vendored"

    assert "RuntimeError" in ble_stack.shared_failure
    assert "boom" in ble_stack.shared_failure
    # no half-imported shared stack left behind, and none of its roots linger
    assert "bleak_connection_manager" not in sys.modules
    for root in ble_stack.shared_roots(shared):
        assert root not in sys.path
    assert sys.path[: len(path_before)] != []  # vendored dir was added, not the shared roots
    assert ble_stack.VENDORED_BLE_DIR in sys.path


def test_an_already_importable_manager_is_left_alone(tmp_path):
    """A shim on PYTHONPATH, or the suite's own stub, owns the arrangement."""
    sys.modules["bleak_connection_manager"] = object()
    assert ble_stack.ensure_ble_stack(_make_shared(tmp_path)) == "shared"
    assert ble_stack.VENDORED_BLE_DIR not in sys.path


def test_the_decision_is_made_once(tmp_path):
    assert ble_stack.ensure_ble_stack("") == "vendored"
    # a later call with a real install must not change a process mid-flight
    assert ble_stack.ensure_ble_stack(_make_shared(tmp_path)) == "vendored"


def test_the_vendored_stack_actually_contains_the_four_packages():
    for name in ("bleak", "bleak_retry_connector", "bluetooth_adapters", "aiooui"):
        assert os.path.isdir(os.path.join(ble_stack.VENDORED_BLE_DIR, name)), name


def test_no_vendored_connection_manager_remains():
    """Clint's ruling: present-only, never vendored."""
    assert not os.path.exists(os.path.join(DRIVER_DIR, "ext", "bleak_connection_manager"))


# --- the ordering control -------------------------------------------------
#
# The catcher rebinds bleak.BleakClient process-wide, and a module that has
# already run `from bleak import BleakClient` keeps its original binding. So
# the install has to happen ABOVE every BLE import in dbus-serialbattery.py.
# Getting this backwards is silent: imports still succeed, this suite still
# passes, and the catcher simply never binds. Hence a source-order assertion.

DRIVER_MAIN = os.path.join(DRIVER_DIR, "dbus-serialbattery.py")


def _line_numbers(pattern):
    with open(DRIVER_MAIN, encoding="utf-8") as handle:
        return [i for i, line in enumerate(handle, 1) if re.search(pattern, line)]


def test_the_ble_stack_is_arranged_before_any_ble_import():
    arranged = _line_numbers(r"\bensure_ble_stack\(")
    ble_imports = _line_numbers(r"^\s*from (utils_ble|bms\.[a-z0-9_]*_ble|bms\.generic_aiobmsble) import")
    assert arranged, "ensure_ble_stack is never called"
    assert ble_imports, "no BLE imports found - has the file been restructured?"
    assert min(arranged) < min(ble_imports), (
        f"ensure_ble_stack first called at line {min(arranged)}, "
        f"but a BLE module is imported at line {min(ble_imports)}"
    )


def test_the_connection_manager_is_installed_before_any_ble_import():
    installed = _line_numbers(r"^\s*install_ble_connection_manager\(")
    ble_imports = _line_numbers(r"^\s*from (utils_ble|bms\.[a-z0-9_]*_ble|bms\.generic_aiobmsble) import")
    assert installed, "install_ble_connection_manager is never called"
    assert min(installed) < min(ble_imports), (
        f"catcher installed at line {min(installed)}, "
        f"but a BLE module is imported at line {min(ble_imports)}"
    )


def test_every_ble_import_site_is_preceded_by_its_own_install():
    """Both sys.argv[2] branches must arrange and install, not just the first."""
    installed = _line_numbers(r"^\s*install_ble_connection_manager\(")
    ble_imports = _line_numbers(r"^\s*from (utils_ble|bms\.[a-z0-9_]*_ble|bms\.generic_aiobmsble) import")
    for line in ble_imports:
        assert any(i < line for i in installed), f"BLE import at line {line} has no install above it"
