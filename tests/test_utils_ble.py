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
import time
import types

import pytest

DRIVER_DIR = os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery")
CONFIG_DEFAULT = os.path.join(DRIVER_DIR, "config.default.ini")
sys.path.insert(0, DRIVER_DIR)
# The driver puts ext on sys.path before it imports utils_ble, which is what
# makes the vendored habluetooth discoverable there. Do the same here, so the
# backend registry under test is the one the driver builds. Only find_spec()
# runs against it; habluetooth itself is never imported by this suite.
sys.path.insert(1, os.path.join(DRIVER_DIR, "ext"))

if "bleak" not in sys.modules:
    _bleak_exc = types.ModuleType("bleak.exc")
    _bleak_exc.BleakCharacteristicNotFoundError = type("BleakCharacteristicNotFoundError", (Exception,), {})
    _bleak_exc.BleakError = type("BleakError", (Exception,), {})
    _bleak = types.ModuleType("bleak")
    _bleak.BleakClient = type("BleakClient", (), {"__init__": lambda self, *a, **kw: None})
    _bleak.BleakScanner = object
    _bleak.exc = _bleak_exc
    sys.modules["bleak"] = _bleak
    sys.modules["bleak.exc"] = _bleak_exc


if "bleak_retry_connector" not in sys.modules:
    # utils_ble only needs these four names; stubbing the module keeps
    # BleakRetryBackend in supported_ble_backends so the generic backend
    # tests below cover it as well.
    async def _not_under_test(*args, **kwargs):
        raise NotImplementedError

    _brc = types.ModuleType("bleak_retry_connector")
    _brc.close_stale_connections = _not_under_test
    _brc.establish_connection = _not_under_test
    _brc.get_device = _not_under_test
    _brc.get_device_by_adapter = _not_under_test
    sys.modules["bleak_retry_connector"] = _brc


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
    """The registry is keyed by class name, so every entry must resolve to itself.

    A backend whose optional dependencies are missing on this machine degrades
    to BleakBackend rather than raising - see the fallback test below. That is
    the case for BCMBackend here, which needs a real bleak and BlueZ.
    """
    for cls in utils_ble.supported_ble_backends:
        resolved = utils_ble.get_ble_backend(cls.__name__)
        assert type(resolved) is cls or type(resolved) is utils_ble.BleakBackend


def test_bcm_backend_is_registered_and_reachable_by_name():
    """BCMBackend must be selectable by config, whether or not it loads here."""
    assert utils_ble.BCMBackend in utils_ble.supported_ble_backends
    assert issubclass(utils_ble.BCMBackend, utils_ble.BleConnectionBackend)


def test_an_unloadable_backend_degrades_to_bleak_instead_of_killing_the_driver():
    """A backend whose dependency is missing must not take the driver down.

    BCMBackend raises ImportError when bleak_connection_manager is not
    importable; the selector has to survive that, because it runs inside
    Syncron_Ble.__init__ on a GX device where a raised ImportError means no
    dbus service at all.
    """

    class UnloadableBackend(utils_ble.BleConnectionBackend):
        def __init__(self):
            raise ImportError("dependency missing")

    utils_ble.supported_ble_backends.append(UnloadableBackend)
    try:
        assert type(utils_ble.get_ble_backend("UnloadableBackend")) is utils_ble.BleakBackend
    finally:
        utils_ble.supported_ble_backends.remove(UnloadableBackend)


def test_bcm_backend_constructs_when_its_dependency_is_importable():
    """Where bleak_connection_manager does import, selection must return it."""
    if not utils_ble._HAS_BCM:
        import pytest

        pytest.skip("bleak_connection_manager not importable in this environment")
    assert type(utils_ble.get_ble_backend("BCMBackend")) is utils_ble.BCMBackend


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


def test_mac_at_adapter_entries_stay_out_of_the_default_pool():
    pins, pool = utils_ble.parse_adapter_entries(["C8:47:8C:00:00:00@hci1", "C8:47:8C:00:00:11@hci2"])
    assert pins == {"C8:47:8C:00:00:00": ["hci1"], "C8:47:8C:00:00:11": ["hci2"]}
    # a pinned MAC is not an adapter name and must never be handed to bleak
    assert pool == []


def test_per_battery_adapters_and_the_pool_can_be_mixed():
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
    original = utils_ble.BLUETOOTH_DEVICE_ADAPTERS
    utils_ble.BLUETOOTH_DEVICE_ADAPTERS = {"C8:47:8C:00:00:00": ["hci1"]}
    try:
        assert utils_ble.adapters_for("c8:47:8c:00:00:00") == ["hci1"]
        assert utils_ble.adapters_for("C8:47:8C:00:00:00") == ["hci1"]
        # an unpinned device falls through to the shared pool
        assert utils_ble.adapters_for("C8:47:8C:00:00:11") is None
    finally:
        utils_ble.BLUETOOTH_DEVICE_ADAPTERS = original


def test_hold_flag_path_normalizes_the_mac_address():
    """One battery, one flag file — regardless of how the MAC was written."""
    lower = utils_ble.ble_hold_flag_path("c8:47:8c:00:00:00")
    upper = utils_ble.ble_hold_flag_path("C8:47:8C:00:00:00")
    assert lower == upper
    assert os.path.basename(lower) == "ble-hold-c8478c000000"
    assert os.path.dirname(lower) == utils_ble.BLE_HOLD_FLAG_DIR


def test_hold_flag_paths_differ_per_device():
    assert utils_ble.ble_hold_flag_path("C8:47:8C:00:00:00") != utils_ble.ble_hold_flag_path("C8:47:8C:00:00:11")


def _bcm():
    """A BCMBackend with its dependency check bypassed.

    Only the pure adapter-selection logic is exercised through it; nothing
    here touches bleak_connection_manager or a radio.
    """
    return object.__new__(utils_ble.BCMBackend)


def test_bcm_adapter_selection_honors_a_pin_and_ignores_the_pool():
    original_pins = utils_ble.BLUETOOTH_DEVICE_ADAPTERS
    original_pool = utils_ble.BLUETOOTH_ADAPTER_POOL
    utils_ble.BLUETOOTH_DEVICE_ADAPTERS = {"C8:47:8C:00:00:00": ["hci1"]}
    utils_ble.BLUETOOTH_ADAPTER_POOL = ["hci0", "hci2"]
    try:
        # a pinned battery may use exactly one adapter, never the pool
        assert _bcm()._adapters("C8:47:8C:00:00:00") == ["hci1"]
    finally:
        utils_ble.BLUETOOTH_DEVICE_ADAPTERS = original_pins
        utils_ble.BLUETOOTH_ADAPTER_POOL = original_pool


def test_bcm_adapter_selection_spreads_unpinned_devices_across_the_pool():
    """Preference order is rotated per device, but stays a permutation of the pool."""
    original_pins = utils_ble.BLUETOOTH_DEVICE_ADAPTERS
    original_pool = utils_ble.BLUETOOTH_ADAPTER_POOL
    utils_ble.BLUETOOTH_DEVICE_ADAPTERS = {}
    utils_ble.BLUETOOTH_ADAPTER_POOL = ["hci0", "hci1", "hci2"]
    try:
        backend = _bcm()
        orders = {addr: backend._adapters(addr) for addr in ("C8:47:8C:00:00:00", "C8:47:8C:00:00:01", "C8:47:8C:00:00:02")}
        for order in orders.values():
            # every allowed adapter is still tried, only the preference moves
            assert sorted(order) == ["hci0", "hci1", "hci2"]
        # the rotation is by address, so different devices lead with different adapters
        assert len({tuple(order) for order in orders.values()}) == 3
        # and it is stable: the same address always yields the same order
        assert backend._adapters("C8:47:8C:00:00:00") == orders["C8:47:8C:00:00:00"]
    finally:
        utils_ble.BLUETOOTH_DEVICE_ADAPTERS = original_pins
        utils_ble.BLUETOOTH_ADAPTER_POOL = original_pool


def test_bluez_device_path_is_built_the_way_bluez_names_objects():
    assert utils_ble._bluez_device_path("hci1", "c8:47:8c:00:00:00") == "/org/bluez/hci1/dev_C8_47_8C_00_00_00"


def test_adapter_of_recovers_the_adapter_a_resolved_device_lives_under():
    """The allow-list check depends on this, so a wrong answer connects via a banned adapter."""

    class FakeDevice:
        details = {"path": "/org/bluez/hci2/dev_C8_47_8C_00_00_00"}

    assert utils_ble._adapter_of(FakeDevice()) == "hci2"


def test_adapter_of_returns_none_when_the_path_is_not_a_bluez_device_path():
    class NoPath:
        details = {}

    assert utils_ble._adapter_of(NoPath()) is None
    assert utils_ble._adapter_of(object()) is None


def test_breaker_only_trips_after_consecutive_half_connects():
    backend = _bcm()
    backend._handoff_fails = 0
    for _ in range(utils_ble.BLE_HANDOFF_BREAKER_THRESHOLD - 1):
        assert not backend._breaker_tripped()
        backend._handoff_fails += 1
    assert not backend._breaker_tripped()
    backend._handoff_fails += 1
    assert backend._breaker_tripped()


def test_a_successful_handoff_clears_the_breaker_count():
    """The count is of *consecutive* failures - one good session resets it."""
    backend = _bcm()
    backend._handoff_fails = utils_ble.BLE_HANDOFF_BREAKER_THRESHOLD + 2
    assert backend._breaker_tripped()
    backend._handoff_fails = 0
    assert not backend._breaker_tripped()


def _hold_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(utils_ble, "BLE_HOLD_FLAG_DIR", str(tmp_path / "tmp"))
    return utils_ble


def test_auto_hold_is_written_only_once_the_engagement_threshold_is_reached(tmp_path, monkeypatch):
    _hold_dir(tmp_path, monkeypatch)
    backend = _bcm()
    backend._breaker_times = []
    address = "C8:47:8C:00:00:00"
    flag = utils_ble.ble_hold_flag_path(address)

    for engagement in range(1, utils_ble.BLE_AUTO_HOLD_THRESHOLD):
        assert backend._engage_breaker(address, now=1000.0 + engagement) is False
        assert not os.path.exists(flag), f"held after only {engagement} engagements"

    assert backend._engage_breaker(address, now=1000.0 + utils_ble.BLE_AUTO_HOLD_THRESHOLD) is True
    assert os.path.exists(flag)


def test_engagements_older_than_the_window_do_not_count_towards_a_storm(tmp_path, monkeypatch):
    """A slow trickle of engagements is not the failure this is meant to catch."""
    _hold_dir(tmp_path, monkeypatch)
    backend = _bcm()
    backend._breaker_times = []
    address = "C8:47:8C:00:00:00"
    spacing = utils_ble.BLE_AUTO_HOLD_WINDOW  # each engagement ages the previous one out
    for engagement in range(utils_ble.BLE_AUTO_HOLD_THRESHOLD * 2):
        assert backend._engage_breaker(address, now=1000.0 + engagement * spacing) is False
    assert not os.path.exists(utils_ble.ble_hold_flag_path(address))


def test_a_written_hold_starts_a_fresh_window(tmp_path, monkeypatch):
    """Once held, earlier engagements must not immediately re-trigger a hold."""
    _hold_dir(tmp_path, monkeypatch)
    backend = _bcm()
    backend._breaker_times = []
    address = "C8:47:8C:00:00:00"
    for engagement in range(1, utils_ble.BLE_AUTO_HOLD_THRESHOLD + 1):
        held = backend._engage_breaker(address, now=1000.0 + engagement)
    assert held is True
    assert backend._breaker_times == []
    assert backend._engage_breaker(address, now=1000.0 + utils_ble.BLE_AUTO_HOLD_THRESHOLD + 1) is False


def test_auto_hold_writes_a_self_expiring_flag_the_reconnect_loop_recognizes(tmp_path, monkeypatch):
    _hold_dir(tmp_path, monkeypatch)
    address = "C8:47:8C:00:00:00"
    assert utils_ble.write_ble_auto_hold(address) is True

    flag = utils_ble.ble_hold_flag_path(address)
    assert os.path.dirname(flag) == utils_ble.BLE_HOLD_FLAG_DIR
    with open(flag) as f:
        assert f.read().strip() == utils_ble.BLE_HOLD_AUTO_MARKER

    # a hold that has just been written must hold, not expire immediately
    assert utils_ble.ble_hold_expired(flag) is False
    # and it must release itself once it has aged past the expiry
    assert utils_ble.ble_hold_expired(flag, now=time.time() + utils_ble.BLE_HOLD_AUTO_EXPIRY + 1) is True


def test_an_operator_hold_never_expires_by_itself(tmp_path, monkeypatch):
    """Only automatic holds self-release; a hand-written one persists until removed."""
    _hold_dir(tmp_path, monkeypatch)
    address = "C8:47:8C:00:00:00"
    flag = utils_ble.ble_hold_flag_path(address)
    os.makedirs(utils_ble.BLE_HOLD_FLAG_DIR, exist_ok=True)
    with open(flag, "w") as f:
        f.write("held by clint, radio is sick")

    assert utils_ble.ble_hold_expired(flag) is False
    assert utils_ble.ble_hold_expired(flag, now=time.time() + utils_ble.BLE_HOLD_AUTO_EXPIRY * 100) is False


def test_an_empty_hold_flag_is_treated_as_an_operator_hold(tmp_path, monkeypatch):
    _hold_dir(tmp_path, monkeypatch)
    flag = utils_ble.ble_hold_flag_path("C8:47:8C:00:00:00")
    os.makedirs(utils_ble.BLE_HOLD_FLAG_DIR, exist_ok=True)
    open(flag, "w").close()
    assert utils_ble.ble_hold_expired(flag, now=time.time() + utils_ble.BLE_HOLD_AUTO_EXPIRY * 100) is False


def test_an_unwritable_hold_directory_is_reported_rather_than_raised(tmp_path, monkeypatch):
    """The auto-hold is a best effort; failing to write it must not kill the attempt."""
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("")
    monkeypatch.setattr(utils_ble, "BLE_HOLD_FLAG_DIR", str(blocker / "tmp"))
    assert utils_ble.write_ble_auto_hold("C8:47:8C:00:00:00") is False


def test_backends_implement_the_connection_interface():
    """Every backend must be usable through the seam Syncron_Ble drives."""
    for cls in utils_ble.supported_ble_backends:
        assert issubclass(cls, utils_ble.BleConnectionBackend)
        for method in ("create_client", "establish", "release"):
            assert getattr(cls, method) is not getattr(utils_ble.BleConnectionBackend, method)


def test_a_mac_repeated_gives_a_battery_several_adapters_in_order():
    # first entry is the primary, the rest are only tried if it cannot resolve
    pins, pool = utils_ble.parse_adapter_entries(["AA:BB@hci4", "CC:DD@hci5", "AA:BB@hci2"])

    assert pins == {"AA:BB": ["hci4", "hci2"], "CC:DD": ["hci5"]}
    assert pool == []


def test_a_repeated_adapter_for_one_battery_is_not_duplicated():
    pins, _ = utils_ble.parse_adapter_entries(["AA:BB@hci4", "AA:BB@hci4"])

    assert pins == {"AA:BB": ["hci4"]}


# ── post-connect validation ──────────────────────────────────────────────
#
# The manager calls this after connecting and re-reads GATT services when it
# answers False, so what it reports decides whether a half-resolved client is
# used or torn down. The manager's own retry ladder needs a real radio and is
# not exercised here.


class _FakeServices:
    def __init__(self, characteristics):
        self._characteristics = characteristics

    def get_characteristic(self, uuid):
        return self._characteristics.get(uuid)


class _FakeClient:
    def __init__(self, services):
        self.services = services


NOTIFY_UUID = "00000003-0000-1000-8000-00805f9b34fb"


def test_a_resolved_notify_characteristic_validates():
    client = _FakeClient(_FakeServices({NOTIFY_UUID: object()}))

    assert utils_ble.notify_characteristic_present(client, NOTIFY_UUID) is True


def test_an_unresolved_notify_characteristic_does_not_validate():
    # GATT discovery finished for other characteristics but not this one
    client = _FakeClient(_FakeServices({"0000ffff-0000-1000-8000-00805f9b34fb": object()}))

    assert utils_ble.notify_characteristic_present(client, NOTIFY_UUID) is False


def test_services_not_populated_at_all_does_not_validate():
    # connect() returned before any service discovery completed
    assert utils_ble.notify_characteristic_present(_FakeClient(None), NOTIFY_UUID) is False


def test_a_client_that_raises_on_lookup_does_not_validate():
    class _Raising:
        def get_characteristic(self, uuid):
            raise RuntimeError("not connected")

    assert utils_ble.notify_characteristic_present(_FakeClient(_Raising()), NOTIFY_UUID) is False


def test_habluetooth_backend_is_available_from_the_vendored_copy():
    """ext/habluetooth is what makes the backend selectable at all."""
    assert utils_ble.HAS_HABLUETOOTH
    assert utils_ble.HaBluetoothBackend in utils_ble.supported_ble_backends


def test_habluetooth_backend_defers_client_creation_to_establish():
    backend = utils_ble.get_ble_backend("HaBluetoothBackend")
    sentinel = object()
    assert backend.create_client("C8:47:8C:00:00:00", sentinel) is None
    assert backend.disconnected_callback is sentinel


def test_habluetooth_backend_scans_only_the_batterys_own_adapters():
    original_pins = utils_ble.BLUETOOTH_DEVICE_ADAPTERS
    original_pool = utils_ble.BLUETOOTH_ADAPTER_POOL
    utils_ble.BLUETOOTH_DEVICE_ADAPTERS = {"C8:47:8C:00:00:00": ["hci1", "hci3"]}
    utils_ble.BLUETOOTH_ADAPTER_POOL = ["hci2"]
    try:
        backend = utils_ble.get_ble_backend("HaBluetoothBackend")
        available = {"hci0": {}, "hci1": {}, "hci2": {}, "hci3": {}}
        # a pinned battery never scans on anything but its own adapters
        assert backend._adapters_to_scan("c8:47:8c:00:00:00", available) == ["hci1", "hci3"]
        # an unpinned one uses the pool, not every adapter present
        assert backend._adapters_to_scan("C8:47:8C:00:00:11", available) == ["hci2"]
    finally:
        utils_ble.BLUETOOTH_DEVICE_ADAPTERS = original_pins
        utils_ble.BLUETOOTH_ADAPTER_POOL = original_pool


def test_habluetooth_backend_scans_every_adapter_when_unconfigured():
    original_pins = utils_ble.BLUETOOTH_DEVICE_ADAPTERS
    original_pool = utils_ble.BLUETOOTH_ADAPTER_POOL
    utils_ble.BLUETOOTH_DEVICE_ADAPTERS = {}
    utils_ble.BLUETOOTH_ADAPTER_POOL = []
    try:
        backend = utils_ble.get_ble_backend("HaBluetoothBackend")
        assert backend._adapters_to_scan("C8:47:8C:00:00:11", {"hci0": {}, "hci1": {}}) == ["hci0", "hci1"]
    finally:
        utils_ble.BLUETOOTH_DEVICE_ADAPTERS = original_pins
        utils_ble.BLUETOOTH_ADAPTER_POOL = original_pool


def test_bleak_retry_backend_defers_client_creation_to_establish():
    backend = utils_ble.get_ble_backend("BleakRetryBackend")
    sentinel = object()
    assert backend.create_client("C8:47:8C:00:00:00", sentinel) is None
    assert backend.disconnected_callback is sentinel


def test_bleak_retry_backend_selects_the_batterys_first_adapter():
    original_pins = utils_ble.BLUETOOTH_DEVICE_ADAPTERS
    original_pool = utils_ble.BLUETOOTH_ADAPTER_POOL
    utils_ble.BLUETOOTH_DEVICE_ADAPTERS = {"C8:47:8C:00:00:00": ["hci1", "hci4"]}
    utils_ble.BLUETOOTH_ADAPTER_POOL = ["hci2", "hci3"]
    try:
        backend = utils_ble.get_ble_backend("BleakRetryBackend")
        backend.create_client("c8:47:8c:00:00:00", None)
        # the first pin is the one connections go out on
        assert backend.current_adapter == "hci1"

        backend = utils_ble.get_ble_backend("BleakRetryBackend")
        backend.create_client("C8:47:8C:00:00:11", None)
        assert backend.current_adapter == "hci2"
    finally:
        utils_ble.BLUETOOTH_DEVICE_ADAPTERS = original_pins
        utils_ble.BLUETOOTH_ADAPTER_POOL = original_pool


def test_bleak_retry_backend_rotates_the_pool_after_a_failed_attempt():
    original_pins = utils_ble.BLUETOOTH_DEVICE_ADAPTERS
    original_pool = utils_ble.BLUETOOTH_ADAPTER_POOL
    utils_ble.BLUETOOTH_DEVICE_ADAPTERS = {}
    utils_ble.BLUETOOTH_ADAPTER_POOL = ["hci2", "hci3"]
    try:
        backend = utils_ble.get_ble_backend("BleakRetryBackend")
        backend.create_client("C8:47:8C:00:00:11", None)
        assert backend.current_adapter == "hci2"
        # the stubbed bleak_retry_connector raises NotImplementedError, which
        # counts as a failed attempt and must advance the pool index
        with pytest.raises(NotImplementedError):
            asyncio.run(backend.establish(None, "C8:47:8C:00:00:11", "char", None))
        backend.create_client("C8:47:8C:00:00:11", None)
        assert backend.current_adapter == "hci3"
    finally:
        utils_ble.BLUETOOTH_DEVICE_ADAPTERS = original_pins
        utils_ble.BLUETOOTH_ADAPTER_POOL = original_pool


def test_the_retry_connector_establish_is_not_shadowed_by_the_managers():
    """
    bleak_connection_manager and habluetooth both bring an establish_connection
    of their own, and bleak_connection_manager's is imported later in the module
    than the retry connector's. An unaliased import would leave BleakRetryBackend
    silently calling BCM's function with the wrong signature, which no other test
    here would catch because none of them reach the connect path.
    """
    assert utils_ble.retry_establish_connection is sys.modules["bleak_retry_connector"].establish_connection


def _configure(devices, pool):
    utils_ble.BLUETOOTH_DEVICE_ADAPTERS = devices
    utils_ble.BLUETOOTH_ADAPTER_POOL = pool


@pytest.mark.parametrize("backend_name", ["BleakBackend", "BleakRetryBackend"])
def test_a_battery_advances_to_its_next_adapter_after_a_failed_attempt(backend_name):
    """
    The reason multi-pin exists: the preferred radio can vanish (USB renumbering
    after a reset), and the battery has to reach its second pin or the driver
    blocks charging for a bank that is perfectly healthy.
    """
    original_pins, original_pool = utils_ble.BLUETOOTH_DEVICE_ADAPTERS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["hci5", "hci6"]}, [])
    try:
        backend = utils_ble.get_ble_backend(backend_name)
        assert backend._select_adapter("C8:47:8C:00:00:00") == "hci5"
        backend.adapter_index += 1
        assert backend._select_adapter("C8:47:8C:00:00:00") == "hci6"
        # and round again, so a pin that comes back is reachable
        backend.adapter_index += 1
        assert backend._select_adapter("C8:47:8C:00:00:00") == "hci5"
    finally:
        _configure(original_pins, original_pool)


@pytest.mark.parametrize("backend_name", ["BleakBackend", "BleakRetryBackend"])
def test_a_failed_connect_is_what_advances_the_adapter(backend_name):
    original_pins, original_pool = utils_ble.BLUETOOTH_DEVICE_ADAPTERS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["hci5", "hci6"]}, [])
    try:
        backend = utils_ble.get_ble_backend(backend_name)
        backend.create_client("C8:47:8C:00:00:00", None)
        assert backend.current_adapter == "hci5"
        # the stubs raise, which is a failed attempt
        with pytest.raises(Exception):
            asyncio.run(backend.establish(None, "C8:47:8C:00:00:00", "char", None))
        backend.create_client("C8:47:8C:00:00:00", None)
        assert backend.current_adapter == "hci6"
    finally:
        _configure(original_pins, original_pool)


@pytest.mark.parametrize("backend_name", ["BleakBackend", "BleakRetryBackend"])
def test_a_dropped_link_reconnects_on_the_same_adapter(backend_name):
    """
    A disconnect is not a failed attempt. The reconnect loop calls create_client
    again without establish() having raised, and that must not move the battery
    off a radio that is working - only a failed connect does.
    """
    original_pins, original_pool = utils_ble.BLUETOOTH_DEVICE_ADAPTERS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["hci5", "hci6"]}, [])
    try:
        backend = utils_ble.get_ble_backend(backend_name)
        backend.create_client("C8:47:8C:00:00:00", None)
        assert backend.current_adapter == "hci5"
        for _ in range(5):
            backend.create_client("C8:47:8C:00:00:00", None)
            assert backend.current_adapter == "hci5"
    finally:
        _configure(original_pins, original_pool)


@pytest.mark.parametrize("backend_name", ["BleakBackend", "BleakRetryBackend"])
def test_a_battery_with_its_own_adapters_never_uses_the_default_pool(backend_name):
    original_pins, original_pool = utils_ble.BLUETOOTH_DEVICE_ADAPTERS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["hci5"]}, ["hci0", "hci1"])
    try:
        backend = utils_ble.get_ble_backend(backend_name)
        # exhausting the single pin wraps back onto itself, never onto the pool
        for i in range(4):
            backend.adapter_index = i
            assert backend._select_adapter("C8:47:8C:00:00:00") == "hci5"
    finally:
        _configure(original_pins, original_pool)


# ---------------------------------------------------------------------------
# /run/bt-claims adapter claims (bt_claims.py)


def _manager(tmp_path, owner="svc-a"):
    from bt_claims import ClaimManager

    return ClaimManager(owner=owner, claim_dir=str(tmp_path))


def _age(path, seconds):
    old = time.time() - seconds
    os.utime(path, (old, old))


def test_a_hard_claim_is_exclusive_and_a_racing_claimant_loses(tmp_path):
    a = _manager(tmp_path, "scanner-a")
    b = _manager(tmp_path, "scanner-b")
    claim = a.claim_hard("hci4")
    assert claim is not None
    assert b.claim_hard("hci4") is None
    a.release(claim)
    reclaimed = b.claim_hard("hci4")
    assert reclaimed is not None
    b.release(reclaimed)


def test_a_stale_hard_claim_is_reaped_and_taken(tmp_path):
    """A dead scanner must not hold its card forever: dead pid + old mtime = free."""
    a = _manager(tmp_path)
    path = os.path.join(str(tmp_path), "hci4.scan")
    with open(path, "w") as f:
        f.write("99999999 dead-scanner 0\n")
    _age(path, 3600)
    claim = a.claim_hard("hci4")
    assert claim is not None
    a.release(claim)


def test_a_crashed_holders_claim_is_dead_immediately_not_after_the_ttl(tmp_path):
    """
    The pid check is what makes crash detection instant: a dead process with
    a still-fresh heartbeat file must not hold its card for the TTL tail.
    """
    b = _manager(tmp_path, "scanner-b")
    path = os.path.join(str(tmp_path), "hci4.scan")
    with open(path, "w") as f:
        f.write("99999999 crashed-scanner 0\n")  # dead pid, fresh mtime
    taken = b.claim_hard("hci4")
    assert taken is not None
    b.release(taken)


def test_a_wedged_but_alive_holder_loses_its_claim_after_the_ttl(tmp_path):
    """
    Liveness needs BOTH a running pid and a fresh heartbeat. A hung scanner
    that stops beating must not hold its card forever; the TTL is the bound
    on how long a wedge can monopolize an adapter.
    """
    a = _manager(tmp_path, "scanner-a")
    b = _manager(tmp_path, "scanner-b")
    claim = a.claim_hard("hci4")
    _age(claim.path, 3600)  # pid alive, heartbeat long overdue
    taken = b.claim_hard("hci4")
    assert taken is not None
    b.release(taken)


def test_placement_avoids_a_hard_claimed_adapter(tmp_path):
    scanner = _manager(tmp_path, "scanner")
    battery = _manager(tmp_path, "battery")
    hard = scanner.claim_hard("hci1")
    adapter, claim = battery.choose(["hci1", "hci2"])
    try:
        assert adapter == "hci2"
    finally:
        battery.release(claim)
        scanner.release(hard)


def test_placement_prefers_the_less_claimed_adapter(tmp_path):
    other = _manager(tmp_path, "other-service")
    battery = _manager(tmp_path, "battery")
    theirs = other.claim_soft("hci1")
    adapter, claim = battery.choose(["hci1", "hci2"])
    try:
        assert adapter == "hci2"
    finally:
        battery.release(claim)
        other.release(theirs)


def test_soft_claims_share_when_there_is_no_alternative(tmp_path):
    """Soft means soft: a fully-claimed world ranks, it never refuses."""
    other = _manager(tmp_path, "other-service")
    battery = _manager(tmp_path, "battery")
    held = [other.claim_soft("hci1"), other.claim_soft("hci2")]
    adapter, claim = battery.choose(["hci1", "hci2"])
    try:
        assert adapter in ("hci1", "hci2")
        assert claim is not None
    finally:
        battery.release(claim)
        for h in held:
            other.release(h)


def test_a_hard_claim_never_keeps_a_battery_off_the_air(tmp_path):
    scanner = _manager(tmp_path, "scanner")
    battery = _manager(tmp_path, "battery")
    hard = scanner.claim_hard("hci1")
    adapter, claim = battery.choose(["hci1"])
    try:
        assert adapter == "hci1"
    finally:
        battery.release(claim)
        scanner.release(hard)


def test_an_unusable_claim_directory_degrades_to_uncoordinated(tmp_path):
    from bt_claims import ClaimManager

    m = ClaimManager(owner="battery", claim_dir="/proc/definitely/not/writable")
    adapter, claim = m.choose(["hci1", "hci2"])
    assert adapter == "hci1"
    assert claim is None


def test_claim_files_carry_pid_service_and_since(tmp_path):
    m = _manager(tmp_path, "svc")
    claim = m.claim_soft("hci1")
    try:
        with open(claim.path) as f:
            pid, service, since = f.read().split()
        assert int(pid) == os.getpid()
        assert int(since) > 0
    finally:
        m.release(claim)


def test_backend_reuses_its_claim_so_a_drop_reconnects_on_the_same_adapter(tmp_path):
    original_devs = utils_ble.BLUETOOTH_DEVICE_ADAPTERS
    utils_ble.BLUETOOTH_DEVICE_ADAPTERS = {"C8:47:8C:00:00:00": ["hci1", "hci2"]}
    backend = utils_ble.get_ble_backend("BleakRetryBackend")
    backend._claims = _manager(tmp_path, "battery")
    try:
        backend.create_client("C8:47:8C:00:00:00", None)
        assert backend.current_adapter == "hci1"
        assert backend._claim is not None and backend._claim.adapter == "hci1"
        backend.create_client("C8:47:8C:00:00:00", None)
        assert backend.current_adapter == "hci1"
    finally:
        backend._release_claim()
        utils_ble.BLUETOOTH_DEVICE_ADAPTERS = original_devs


def test_backend_releases_its_claim_on_a_failed_connect_before_rotating(tmp_path):
    original_devs = utils_ble.BLUETOOTH_DEVICE_ADAPTERS
    utils_ble.BLUETOOTH_DEVICE_ADAPTERS = {"C8:47:8C:00:00:00": ["hci1", "hci2"]}
    backend = utils_ble.get_ble_backend("BleakRetryBackend")
    backend._claims = _manager(tmp_path, "battery")
    try:
        backend.create_client("C8:47:8C:00:00:00", None)
        assert backend.current_adapter == "hci1"
        with pytest.raises(Exception):
            asyncio.run(backend.establish(None, "C8:47:8C:00:00:00", "char", None))
        assert backend._claim is None
        assert not os.path.exists(os.path.join(str(tmp_path), "hci1.use.battery"))
        backend.create_client("C8:47:8C:00:00:00", None)
        assert backend.current_adapter == "hci2"
    finally:
        backend._release_claim()
        utils_ble.BLUETOOTH_DEVICE_ADAPTERS = original_devs


def _claiming_backend(tmp_path, adapters):
    utils_ble.BLUETOOTH_DEVICE_ADAPTERS = {"C8:47:8C:00:00:00": adapters}
    backend = utils_ble.get_ble_backend("BleakRetryBackend")
    backend._claims = _manager(tmp_path, "battery")
    return backend


def test_the_fallback_scan_holds_the_hard_claim_for_its_duration(tmp_path, monkeypatch):
    """
    A scan is a scan, however brief. The ten-second cache-miss fallback must be
    visible to other services' placement while it runs, and gone the moment it
    ends - and a hard claim someone else holds must not block the scan.
    """
    original_devs = utils_ble.BLUETOOTH_DEVICE_ADAPTERS
    backend = _claiming_backend(tmp_path, ["hci1"])
    hard_path = os.path.join(str(tmp_path), "hci1.scan")
    seen = {}

    async def fake_get_device_by_adapter(address, adapter):
        return None

    class FakeScanner:
        @staticmethod
        async def find_device_by_address(address, timeout, **kwargs):
            seen["held_during_scan"] = os.path.exists(hard_path)
            return object()

    monkeypatch.setattr(utils_ble, "get_device_by_adapter", fake_get_device_by_adapter)
    monkeypatch.setattr(utils_ble, "BleakScanner", FakeScanner)
    try:
        backend.create_client("C8:47:8C:00:00:00", None)
        asyncio.run(backend._resolve_device("C8:47:8C:00:00:00"))
        assert seen["held_during_scan"] is True
        assert not os.path.exists(hard_path)  # released with the scan
    finally:
        backend._release_claim()
        utils_ble.BLUETOOTH_DEVICE_ADAPTERS = original_devs


def test_a_foreign_hard_claim_does_not_block_the_fallback_scan(tmp_path, monkeypatch):
    original_devs = utils_ble.BLUETOOTH_DEVICE_ADAPTERS
    backend = _claiming_backend(tmp_path, ["hci1"])
    scanner = _manager(tmp_path, "someone-else")
    foreign = scanner.claim_hard("hci1")
    ran = {}

    async def fake_get_device_by_adapter(address, adapter):
        return None

    class FakeScanner:
        @staticmethod
        async def find_device_by_address(address, timeout, **kwargs):
            ran["scanned"] = True
            return object()

    monkeypatch.setattr(utils_ble, "get_device_by_adapter", fake_get_device_by_adapter)
    monkeypatch.setattr(utils_ble, "BleakScanner", FakeScanner)
    try:
        backend.create_client("C8:47:8C:00:00:00", None)
        asyncio.run(backend._resolve_device("C8:47:8C:00:00:00"))
        assert ran.get("scanned") is True
        # and their claim survived: not released, file still present
        assert not foreign.released
        assert os.path.exists(foreign.path)
    finally:
        backend._release_claim()
        scanner.release(foreign)
        utils_ble.BLUETOOTH_DEVICE_ADAPTERS = original_devs


def test_an_adapter_mid_scan_is_not_picked_while_a_quiet_one_exists():
    """
    Scanning is the single point of contention: BlueZ's Discovering flag is
    the system's own record of it, and it covers services that follow no
    convention of ours. A card mid-scan is placement's last resort, never its
    first choice.
    """
    original = utils_ble.BLUETOOTH_DEVICE_ADAPTERS
    utils_ble.BLUETOOTH_DEVICE_ADAPTERS = {"C8:47:8C:00:00:00": ["hci1", "hci2"]}
    try:
        order = utils_ble.adapters_in_attempt_order("C8:47:8C:00:00:00", present={"hci1", "hci2"}, discovering={"hci1"})
        assert order == ["hci2"]
    finally:
        utils_ble.BLUETOOTH_DEVICE_ADAPTERS = original


def test_a_scan_on_every_adapter_gates_nothing():
    original = utils_ble.BLUETOOTH_DEVICE_ADAPTERS
    utils_ble.BLUETOOTH_DEVICE_ADAPTERS = {"C8:47:8C:00:00:00": ["hci1", "hci2"]}
    try:
        order = utils_ble.adapters_in_attempt_order("C8:47:8C:00:00:00", present={"hci1", "hci2"}, discovering={"hci1", "hci2"})
        assert order == ["hci1", "hci2"]
    finally:
        utils_ble.BLUETOOTH_DEVICE_ADAPTERS = original


# ---------------------------------------------------------------------------
# The abandoned-generation reaper.
#
# rebuild_ble_thread() abandons an event loop whose BlueZ manager bus stays
# pinned in bleak's module-level dict with live match rules; dbus-daemon then
# queues every BlueZ signal to a socket nobody reads (measured at ~44 MB/h of
# daemon growth on a Cerbo GX). These tests assert the reaper's side effects
# — the dict entry removed, the socket actually closed, the sibling entry
# untouched — rather than exception types, per the recovery-helper standard:
# a reaper that does nothing raises nothing.
# ---------------------------------------------------------------------------


@pytest.fixture
def bluez_manager_stub():
    """Install bleak.backends.bluezdbus.manager as a stub carrying the dict.

    Guarded and restored: other test files stub bleak differently and
    collection order decides who wins (see the module docstring), so this
    fixture saves whatever is there and puts it back.
    """
    saved = {name: sys.modules.get(name) for name in ("bleak.backends", "bleak.backends.bluezdbus", "bleak.backends.bluezdbus.manager")}
    backends = sys.modules.get("bleak.backends") or types.ModuleType("bleak.backends")
    bluezdbus = sys.modules.get("bleak.backends.bluezdbus") or types.ModuleType("bleak.backends.bluezdbus")
    manager = types.ModuleType("bleak.backends.bluezdbus.manager")
    manager._global_instances = {}
    bluezdbus.manager = manager
    backends.bluezdbus = bluezdbus
    sys.modules["bleak"].backends = backends
    sys.modules["bleak.backends"] = backends
    sys.modules["bleak.backends.bluezdbus"] = bluezdbus
    sys.modules["bleak.backends.bluezdbus.manager"] = manager
    yield manager
    for name, mod in saved.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod


def _fake_manager_with_socket():
    import socket as socket_module

    left, right = socket_module.socketpair()
    manager = types.SimpleNamespace(_bus=types.SimpleNamespace(_sock=left))
    return manager, left, right


def test_a_reaped_generation_loses_its_manager_and_its_socket_is_closed(bluez_manager_stub):
    abandoned_loop = object()
    live_loop = object()
    abandoned_mgr, abandoned_sock, _peer_a = _fake_manager_with_socket()
    live_mgr, live_sock, _peer_b = _fake_manager_with_socket()
    bluez_manager_stub._global_instances[abandoned_loop] = abandoned_mgr
    bluez_manager_stub._global_instances[live_loop] = live_mgr

    utils_ble._reap_abandoned_ble_generation(None, abandoned_loop, "AA:BB:CC:DD:EE:FF", 0)

    # the abandoned entry is gone and ONLY that entry: a reaper degenerated
    # into dict.clear() fails on the count and on the sibling
    assert abandoned_loop not in bluez_manager_stub._global_instances
    assert len(bluez_manager_stub._global_instances) == 1
    assert bluez_manager_stub._global_instances[live_loop] is live_mgr
    # the socket is actually closed, not merely dereferenced — fileno() is -1
    # after close(); a reaper that only pops the dict fails here
    assert abandoned_sock.fileno() == -1
    assert live_sock.fileno() != -1
    live_sock.close()
    _peer_a.close()
    _peer_b.close()


def test_the_reaper_tolerates_an_already_reaped_generation(bluez_manager_stub):
    live_loop = object()
    live_mgr, live_sock, _peer = _fake_manager_with_socket()
    bluez_manager_stub._global_instances[live_loop] = live_mgr

    # bleak's own closed-loop sweep may win the race; reaping a loop with no
    # entry must be a no-op, not an error, and must not touch the survivor
    utils_ble._reap_abandoned_ble_generation(None, object(), "AA:BB:CC:DD:EE:FF", 1)

    assert bluez_manager_stub._global_instances == {live_loop: live_mgr}
    assert live_sock.fileno() != -1
    live_sock.close()
    _peer.close()


def test_the_reaper_waits_for_the_old_thread_before_touching_its_state(bluez_manager_stub):
    joins = []
    old_thread = types.SimpleNamespace(join=lambda timeout: joins.append(timeout))
    abandoned_loop = object()
    mgr, sock, _peer = _fake_manager_with_socket()
    bluez_manager_stub._global_instances[abandoned_loop] = mgr

    utils_ble._reap_abandoned_ble_generation(old_thread, abandoned_loop, "AA:BB:CC:DD:EE:FF", 0)

    # joined exactly once, with the bounded timeout — an unbounded join would
    # hang the reaper forever on a truly wedged generation
    assert joins == [utils_ble.BLE_GENERATION_REAP_TIMEOUT]
    assert sock.fileno() == -1
    _peer.close()


def test_a_rebuild_hands_the_reaper_the_abandoned_generation_not_the_new_one(monkeypatch):
    reaped = []
    monkeypatch.setattr(utils_ble, "_reap_abandoned_ble_generation", lambda *args: reaped.append(args))

    sb = object.__new__(utils_ble.Syncron_Ble)
    sb.address = "AA:BB:CC:DD:EE:FF"
    sb._ble_thread_generation = 0
    old_thread = object()
    old_loop = object()
    sb._ble_async_thread = old_thread
    sb.ble_async_thread_event_loop = old_loop

    def fake_thread_main(generation=0):
        sb.ble_async_thread_ready.set()

    sb.initiate_ble_thread_main = fake_thread_main

    assert sb.rebuild_ble_thread() is True

    # the reaper got the OLD generation's thread and loop, captured before
    # the rebuild overwrote them — capturing after the reset hands it False
    # and reaps nothing, which is exactly the defect this wiring fixes
    assert reaped == [(old_thread, old_loop, "AA:BB:CC:DD:EE:FF", 0)]
    # and the NEW thread handle was stored for the next generation's reaper
    assert sb._ble_async_thread is not old_thread
    assert sb._ble_async_thread.name == "BMS_bluetooth_async_thread_gen1"
