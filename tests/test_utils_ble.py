# -*- coding: utf-8 -*-
"""Tests for the pure logic in utils_ble: connection backend selection.

utils_ble imports bleak, which is not installed on the machines this suite
runs on (and is Linux/BlueZ specific in practice). A minimal module stub is
registered before the import so the non-BLE logic can be exercised for real.
Everything that actually talks to a radio is left untested here.
"""

import configparser
import importlib.util
import os
import sys
import time
import types

DRIVER_DIR = os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery")
CONFIG_DEFAULT = os.path.join(DRIVER_DIR, "config.default.ini")
sys.path.insert(0, DRIVER_DIR)

if "bleak" not in sys.modules:
    sys.modules["bleak"] = types.SimpleNamespace(BleakClient=type("BleakClient", (), {"__init__": lambda self, *a, **kw: None}))
    sys.modules["bleak"].BleakScanner = object
    sys.modules["bleak"].exc = types.SimpleNamespace(BleakError=type("BleakError", (Exception,), {}))
    sys.modules["bleak.exc"] = sys.modules["bleak"].exc
if "bleak_retry_connector" not in sys.modules:
    # utils_ble only needs these four names; stubbing keeps BleakRetryBackend
    # in supported_ble_backends so the generic backend tests cover it too.
    async def _not_under_test(*args, **kwargs):
        raise NotImplementedError

    sys.modules["bleak_retry_connector"] = types.SimpleNamespace(
        close_stale_connections=_not_under_test,
        establish_connection=_not_under_test,
        get_device=_not_under_test,
        get_device_by_adapter=_not_under_test,
    )


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


def _bcm():
    """A BCMBackend with its dependency check bypassed.

    Only the pure adapter-selection logic is exercised through it; nothing
    here touches bleak_connection_manager or a radio.
    """
    return object.__new__(utils_ble.BCMBackend)


def test_bcm_adapter_selection_honors_a_pin_and_ignores_the_pool():
    original_pins = utils_ble.BLUETOOTH_ADAPTER_PINS
    original_pool = utils_ble.BLUETOOTH_ADAPTER_POOL
    utils_ble.BLUETOOTH_ADAPTER_PINS = {"C8:47:8C:00:00:00": ["hci1"]}
    utils_ble.BLUETOOTH_ADAPTER_POOL = ["hci0", "hci2"]
    try:
        # a pinned battery may use exactly one adapter, never the pool
        assert _bcm()._adapters("C8:47:8C:00:00:00") == ["hci1"]
    finally:
        utils_ble.BLUETOOTH_ADAPTER_PINS = original_pins
        utils_ble.BLUETOOTH_ADAPTER_POOL = original_pool


def test_bcm_adapter_selection_spreads_unpinned_devices_across_the_pool():
    """Preference order is rotated per device, but stays a permutation of the pool."""
    original_pins = utils_ble.BLUETOOTH_ADAPTER_PINS
    original_pool = utils_ble.BLUETOOTH_ADAPTER_POOL
    utils_ble.BLUETOOTH_ADAPTER_PINS = {}
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
        utils_ble.BLUETOOTH_ADAPTER_PINS = original_pins
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


def test_a_mac_repeated_pins_several_adapters_in_priority_order():
    # first entry is the primary, the rest are only tried if it cannot resolve
    pins, pool = utils_ble.parse_adapter_entries(["AA:BB@hci4", "CC:DD@hci5", "AA:BB@hci2"])

    assert pins == {"AA:BB": ["hci4", "hci2"], "CC:DD": ["hci5"]}
    assert pool == []


def test_a_repeated_pin_to_the_same_adapter_is_not_duplicated():
    pins, _ = utils_ble.parse_adapter_entries(["AA:BB@hci4", "AA:BB@hci4"])

    assert pins == {"AA:BB": ["hci4"]}


def _configure(devices, pool):
    utils_ble.BLUETOOTH_ADAPTER_PINS = devices
    utils_ble.BLUETOOTH_ADAPTER_POOL = pool


def test_an_adapter_bluez_does_not_expose_is_skipped():
    """
    The failure this exists for: a USB reset renumbers the radios, the adapter
    a battery is configured for stops existing, and asking BlueZ for it by
    name fails forever. The battery has to reach its next adapter instead.
    """
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["hci5", "hci6"]}, [])
    try:
        assert utils_ble.adapters_in_attempt_order("C8:47:8C:00:00:00", present={"hci6"}) == ["hci6"]
    finally:
        _configure(original_pins, original_pool)


def test_a_battery_is_never_stranded_when_bluez_knows_none_of_its_adapters():
    """
    An empty or odd answer from BlueZ must degrade to the configured order,
    not to an empty list - refusing to attempt a connection is worse than
    trying an adapter that may not be there.
    """
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["hci5", "hci6"]}, [])
    try:
        assert utils_ble.adapters_in_attempt_order("C8:47:8C:00:00:00", present=set()) == ["hci5", "hci6"]
        assert utils_ble.adapters_in_attempt_order("C8:47:8C:00:00:00", present={"hci9"}) == ["hci5", "hci6"]
    finally:
        _configure(original_pins, original_pool)


def test_a_battery_advances_to_its_next_adapter_after_a_failed_attempt():
    """
    Why multi-adapter entries exist: the preferred radio can vanish, and the
    battery has to reach its second one or the driver blocks charging for a
    bank that is perfectly healthy.
    """
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["hci5", "hci6"]}, [])
    try:
        backend = utils_ble.get_ble_backend("BleakBackend")
        monkey = lambda address, present=None: ["hci5", "hci6"]  # noqa: E731
        original = utils_ble.adapters_in_attempt_order
        utils_ble.adapters_in_attempt_order = monkey
        try:
            assert backend._select_adapter("C8:47:8C:00:00:00") == "hci5"
            backend.adapter_index += 1
            assert backend._select_adapter("C8:47:8C:00:00:00") == "hci6"
            # and round again, so a radio that comes back is reachable
            backend.adapter_index += 1
            assert backend._select_adapter("C8:47:8C:00:00:00") == "hci5"
        finally:
            utils_ble.adapters_in_attempt_order = original
    finally:
        _configure(original_pins, original_pool)


def test_a_failed_connect_is_what_advances_the_adapter():
    import asyncio

    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["hci5", "hci6"]}, [])
    original = utils_ble.adapters_in_attempt_order
    utils_ble.adapters_in_attempt_order = lambda address, present=None: ["hci5", "hci6"]
    try:
        backend = utils_ble.get_ble_backend("BleakBackend")
        backend.create_client("C8:47:8C:00:00:00", None)
        assert backend.current_adapter == "hci5"
        try:
            asyncio.run(backend.establish(None, "C8:47:8C:00:00:00", "char", None))
        except Exception:
            pass
        backend.create_client("C8:47:8C:00:00:00", None)
        assert backend.current_adapter == "hci6"
    finally:
        utils_ble.adapters_in_attempt_order = original
        _configure(original_pins, original_pool)


def test_a_dropped_link_reconnects_on_the_same_adapter():
    """
    A disconnect is not a failed attempt: the reconnect loop calls
    create_client again without establish() having raised, and that must not
    move the battery off a radio that is working.
    """
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["hci5", "hci6"]}, [])
    original = utils_ble.adapters_in_attempt_order
    utils_ble.adapters_in_attempt_order = lambda address, present=None: ["hci5", "hci6"]
    try:
        backend = utils_ble.get_ble_backend("BleakBackend")
        for _ in range(5):
            backend.create_client("C8:47:8C:00:00:00", None)
            assert backend.current_adapter == "hci5"
    finally:
        utils_ble.adapters_in_attempt_order = original
        _configure(original_pins, original_pool)


def test_a_battery_with_its_own_adapters_never_uses_the_default_pool():
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["hci5"]}, ["hci0", "hci1"])
    try:
        assert utils_ble.adapters_in_attempt_order("C8:47:8C:00:00:00", present={"hci5", "hci0", "hci1"}) == ["hci5"]
    finally:
        _configure(original_pins, original_pool)


def test_bluez_state_is_unavailable_rather_than_raising_without_dbus():
    """utils_ble must stay importable and usable where python-dbus is absent."""
    assert utils_ble.bluez_present_adapters() == set()


def test_bleak_retry_backend_registers_when_the_connector_is_importable():
    assert utils_ble.HAS_BLEAK_RETRY_CONNECTOR
    assert utils_ble.BleakRetryBackend in utils_ble.supported_ble_backends


def test_bleak_retry_backend_defers_client_creation_to_establish():
    backend = utils_ble.get_ble_backend("BleakRetryBackend")
    sentinel = object()
    assert backend.create_client("C8:47:8C:00:00:00", sentinel) is None
    assert backend.disconnected_callback is sentinel


def test_bleak_retry_backend_rotates_after_a_failed_attempt():
    import asyncio

    original = utils_ble.adapters_in_attempt_order
    utils_ble.adapters_in_attempt_order = lambda address, present=None: ["hci5", "hci6"]
    try:
        backend = utils_ble.get_ble_backend("BleakRetryBackend")
        backend.create_client("C8:47:8C:00:00:00", None)
        assert backend.current_adapter == "hci5"
        try:
            asyncio.run(backend.establish(None, "C8:47:8C:00:00:00", "char", None))
        except Exception:
            pass
        backend.create_client("C8:47:8C:00:00:00", None)
        assert backend.current_adapter == "hci6"
    finally:
        utils_ble.adapters_in_attempt_order = original


def test_the_retry_connector_establish_is_aliased_against_shadowing():
    """
    Managed backends stacked on this branch import establish_connection from
    their own library later in the module; the alias is what keeps this
    backend calling the right one, and no other test reaches the connect path.
    """
    assert utils_ble.retry_establish_connection is sys.modules["bleak_retry_connector"].establish_connection


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
