# -*- coding: utf-8 -*-
"""Tests for the pure logic in utils_ble: connection backend selection.

utils_ble imports bleak, which is not installed on the machines this suite
runs on (and is Linux/BlueZ specific in practice). A minimal module stub is
registered before the import so the non-BLE logic can be exercised for real.
Everything that actually talks to a radio is left untested here.
"""

import asyncio
import configparser
import importlib.util
import os
import sys
import types

import pytest

DRIVER_DIR = os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery")
CONFIG_DEFAULT = os.path.join(DRIVER_DIR, "config.default.ini")
sys.path.insert(0, DRIVER_DIR)

if "bleak" not in sys.modules:
    sys.modules["bleak"] = types.SimpleNamespace(BleakClient=object)
    _bleak_error = type("BleakError", (Exception,), {})
    sys.modules["bleak"].exc = types.SimpleNamespace(
        BleakError=_bleak_error,
        BleakCharacteristicNotFoundError=type("BleakCharacteristicNotFoundError", (_bleak_error,), {}),
    )
    sys.modules["bleak.exc"] = sys.modules["bleak"].exc


def _load_utils_ble():
    """Load the real utils_ble under a private module name.

    tests/bms/test_litime_ble.py registers a stub under "utils_ble" in
    sys.modules and is collected first, so a plain import would pick up that
    stub. Load the module from disk under a different name instead, leaving
    sys.modules["utils_ble"] alone in both directions.
    """
    spec = importlib.util.spec_from_file_location("utils_ble_under_test", os.path.join(DRIVER_DIR, "utils_ble.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


utils_ble = _load_utils_ble()


def _config_default():
    parser = configparser.ConfigParser()
    with open(CONFIG_DEFAULT) as f:
        parser.read_file(f)
    return parser["DEFAULT"]


def test_backend_lookup_returns_requested_backend():
    backend = utils_ble.get_ble_backend("BleakBackend")
    assert isinstance(backend, utils_ble.BleakBackend)


def test_backend_lookup_falls_back_to_bleak_for_unknown_name():
    backend = utils_ble.get_ble_backend("NoSuchBackend")
    assert isinstance(backend, utils_ble.BleakBackend)


def test_every_supported_backend_is_selectable_by_its_class_name():
    """The registry is keyed by class name, so every entry must resolve to itself."""
    for cls in utils_ble.supported_ble_backends:
        assert type(utils_ble.get_ble_backend(cls.__name__)) is cls


def test_config_default_backend_name_resolves_without_falling_back():
    """The shipped default must name a real backend, not silently fall back."""
    configured = _config_default()["BLUETOOTH_CONNECTION_BACKEND"].strip()
    assert configured in [cls.__name__ for cls in utils_ble.supported_ble_backends]
    assert type(utils_ble.get_ble_backend(configured)).__name__ == configured


def test_plain_entries_form_the_pool_and_pin_nothing():
    pins, pool = utils_ble.parse_adapter_entries(["hci1", "hci2"])
    assert pins == {}
    assert pool == ["hci1", "hci2"]


def test_pool_order_is_preserved():
    """Rotation walks the pool in configured order, so order must survive parsing."""
    _, pool = utils_ble.parse_adapter_entries(["hci2", "hci0", "hci1"])
    assert pool == ["hci2", "hci0", "hci1"]


def test_mac_at_adapter_entries_pin_and_stay_out_of_the_pool():
    pins, pool = utils_ble.parse_adapter_entries(["C8:47:8C:00:00:00@hci1", "C8:47:8C:00:00:11@hci2"])
    assert pins == {"C8:47:8C:00:00:00": ["hci1"], "C8:47:8C:00:00:11": ["hci2"]}
    # a pinned MAC is not an adapter name and must never be handed to bleak
    assert pool == []


def test_pins_and_pool_can_be_mixed():
    pins, pool = utils_ble.parse_adapter_entries(["hci0", "C8:47:8C:00:00:00@hci1"])
    assert pins == {"C8:47:8C:00:00:00": ["hci1"]}
    assert pool == ["hci0"]


def test_entries_are_whitespace_and_case_normalized():
    pins, pool = utils_ble.parse_adapter_entries([" c8:47:8c:00:00:00 @ hci1 ", " hci0 "])
    assert pins == {"C8:47:8C:00:00:00": ["hci1"]}
    assert pool == ["hci0"]


def test_malformed_entries_are_dropped_rather_than_pinned():
    pins, pool = utils_ble.parse_adapter_entries(["@hci1", "C8:47:8C:00:00:00@", "", "  ", "hci3"])
    assert pins == {}
    assert pool == ["hci3"]


def test_empty_config_pins_and_pools_nothing():
    pins, pool = utils_ble.parse_adapter_entries([])
    assert pins == {}
    assert pool == []


def test_config_default_adapters_is_empty_so_the_default_adapter_is_used():
    assert _config_default()["BLUETOOTH_ADAPTERS"].strip() == ""
    pins, pool = utils_ble.parse_adapter_entries([])
    assert not pins and not pool


def test_adapters_for_matches_a_pinned_device_regardless_of_case():
    original = utils_ble.BLUETOOTH_ADAPTER_PINS
    utils_ble.BLUETOOTH_ADAPTER_PINS = {"C8:47:8C:00:00:00": ["hci1"]}
    try:
        assert utils_ble.adapters_for("c8:47:8c:00:00:00") == ["hci1"]
        assert utils_ble.adapters_for("C8:47:8C:00:00:00") == ["hci1"]
        # an unpinned device falls through to the shared pool
        assert utils_ble.adapters_for("C8:47:8C:00:00:11") is None
    finally:
        utils_ble.BLUETOOTH_ADAPTER_PINS = original


def test_hold_flag_path_normalizes_the_mac_address():
    """One battery, one flag file — regardless of how the MAC was written."""
    lower = utils_ble.ble_hold_flag_path("c8:47:8c:00:00:00")
    upper = utils_ble.ble_hold_flag_path("C8:47:8C:00:00:00")
    assert lower == upper
    assert os.path.basename(lower) == "ble-hold-c8478c000000"
    assert os.path.dirname(lower) == utils_ble.BLE_HOLD_FLAG_DIR


def test_hold_flag_paths_differ_per_device():
    assert utils_ble.ble_hold_flag_path("C8:47:8C:00:00:00") != utils_ble.ble_hold_flag_path("C8:47:8C:00:00:11")


def test_backends_implement_the_connection_interface():
    """Every backend must be usable through the seam Syncron_Ble drives."""
    for cls in utils_ble.supported_ble_backends:
        assert issubclass(cls, utils_ble.BleConnectionBackend)
        for method in ("create_client", "establish", "release"):
            assert getattr(cls, method) is not getattr(utils_ble.BleConnectionBackend, method)


def test_a_mac_repeated_pins_several_adapters_in_priority_order():
    # first entry is the primary, the rest are only tried if it cannot resolve
    pins, pool = utils_ble.parse_adapter_entries(["AA:BB@hci4", "CC:DD@hci5", "AA:BB@hci2"])

    assert pins == {"AA:BB": ["hci4", "hci2"], "CC:DD": ["hci5"]}
    assert pool == []


def test_a_repeated_pin_to_the_same_adapter_is_not_duplicated():
    pins, _ = utils_ble.parse_adapter_entries(["AA:BB@hci4", "AA:BB@hci4"])

    assert pins == {"AA:BB": ["hci4"]}


# ------------------------------------------------- late GATT resolution
#
# These drive BleakBackend.establish, which is the single start_notify call
# site on this branch and the one HumsiENK reaches through the backend seam.
# They fail if the rediscovery wiring is removed, which is the point: a test
# that only asserted the helper exists would still pass against a bare
# start_notify.


class _LateGattClient:
    """A client whose GATT tree is incomplete for the first N subscribes."""

    def __init__(self, missing_for, rebuild_fixes=True):
        self.missing_for = missing_for
        self.rebuild_fixes = rebuild_fixes
        self.subscribe_attempts = 0
        self.rebuilds = 0
        self.connected = False
        # what rediscover_services reaches for
        self._backend = types.SimpleNamespace(services=object(), _get_services=self._get_services)

    async def connect(self):
        self.connected = True

    async def start_notify(self, char, callback):
        self.subscribe_attempts += 1
        if self.subscribe_attempts <= self.missing_for:
            raise sys.modules["bleak.exc"].BleakCharacteristicNotFoundError(char)

    async def _get_services(self):
        self.rebuilds += 1
        if self.rebuild_fixes:
            self._backend.services = object()


def _establish(client):
    backend = utils_ble.BleakBackend()
    return asyncio.run(backend.establish(client, "AA:BB:CC:DD:EE:FF", "char-uuid", lambda *a: None))


def test_a_characteristic_missing_from_a_late_tree_is_recovered_not_reconnected():
    """The endless-reconnect bug: BlueZ says resolved, the tree is not."""
    client = _LateGattClient(missing_for=1)

    _establish(client)

    assert client.subscribe_attempts == 2
    assert client.rebuilds == 1


def test_recovery_is_attempted_more_than_once_before_giving_up():
    client = _LateGattClient(missing_for=2)

    _establish(client)

    assert client.subscribe_attempts == 3
    assert client.rebuilds == 2


def test_a_tree_that_never_resolves_still_raises_rather_than_looping():
    client = _LateGattClient(missing_for=99)

    with pytest.raises(sys.modules["bleak.exc"].BleakCharacteristicNotFoundError):
        _establish(client)

    assert client.subscribe_attempts == utils_ble.GATT_REDISCOVERY_ATTEMPTS
    # the last attempt raises without sleeping, so N attempts spend N-1 rebuilds
    assert client.rebuilds == utils_ble.GATT_REDISCOVERY_ATTEMPTS - 1


def test_a_bleak_without_the_private_rebuild_hook_fails_loudly():
    """Never silently stop rediscovering: that restores the endless reconnect."""
    client = _LateGattClient(missing_for=1)
    client._backend = types.SimpleNamespace()

    with pytest.raises(sys.modules["bleak.exc"].BleakError) as raised:
        _establish(client)

    # the loud failure, not the characteristic error a bare start_notify
    # would have let through unchanged
    assert not isinstance(raised.value, sys.modules["bleak.exc"].BleakCharacteristicNotFoundError)
    assert "no way to rebuild it" in str(raised.value)


def test_a_tree_that_is_already_complete_is_not_rebuilt():
    """The common case pays nothing: no rebuild, no settle, one subscribe."""
    client = _LateGattClient(missing_for=0)

    _establish(client)

    assert client.subscribe_attempts == 1
    assert client.rebuilds == 0
