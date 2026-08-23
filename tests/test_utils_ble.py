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


# --------- adapter pinning by MAC ---------
#
# hciN numbering is assigned in probe order: a reboot or USB reset can renumber
# the dongles, silently re-pointing every pin at different hardware while the
# batteries still connect and nothing looks wrong. An adapter's MAC does not
# move, so configuration may name that instead and be resolved against live
# BlueZ state.

ADAPTERS = {"hci3": "00:1A:7D:DA:71:13", "hci4": "00:1A:7D:DA:71:14"}


def test_a_mac_entry_resolves_to_the_adapters_current_name():
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["00:1A:7D:DA:71:14"]}, [])
    try:
        assert utils_ble.adapters_in_attempt_order("C8:47:8C:00:00:00", present=ADAPTERS) == ["hci4"]
    finally:
        _configure(original_pins, original_pool)


def test_a_mac_pin_follows_its_adapter_across_renumbering():
    """The whole point: the same config resolves to whatever number the
    dongle currently answers to."""
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["00:1A:7D:DA:71:13"]}, [])
    try:
        before = utils_ble.adapters_in_attempt_order("C8:47:8C:00:00:00", present={"hci3": "00:1A:7D:DA:71:13"})
        after = utils_ble.adapters_in_attempt_order("C8:47:8C:00:00:00", present={"hci0": "00:1A:7D:DA:71:13"})
        assert before == ["hci3"]
        assert after == ["hci0"]
    finally:
        _configure(original_pins, original_pool)


def test_mac_matching_ignores_case():
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["00:1a:7d:da:71:13"]}, [])
    try:
        assert utils_ble.adapters_in_attempt_order("C8:47:8C:00:00:00", present=ADAPTERS) == ["hci3"]
    finally:
        _configure(original_pins, original_pool)


def test_hci_and_mac_entries_mix_and_keep_their_order():
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["00:1A:7D:DA:71:14", "hci3"]}, [])
    try:
        assert utils_ble.adapters_in_attempt_order("C8:47:8C:00:00:00", present=ADAPTERS) == ["hci4", "hci3"]
    finally:
        _configure(original_pins, original_pool)


def test_a_mac_whose_adapter_is_gone_is_dropped():
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["00:1A:7D:DA:71:99", "hci3"]}, [])
    try:
        assert utils_ble.adapters_in_attempt_order("C8:47:8C:00:00:00", present=ADAPTERS) == ["hci3"]
    finally:
        _configure(original_pins, original_pool)


def test_only_unresolvable_macs_degrade_to_the_default_adapter_not_to_garbage():
    """
    A MAC is not a name bleak can use. Where an unresolvable hciN list is
    handed back unfiltered (better to try than to refuse), an unresolvable MAC
    list must come back empty so the caller falls back to the system default
    adapter instead of passing a MAC into the connect.
    """
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["00:1A:7D:DA:71:99"]}, [])
    try:
        assert utils_ble.adapters_in_attempt_order("C8:47:8C:00:00:00", present=ADAPTERS) == []
    finally:
        _configure(original_pins, original_pool)


def test_the_pool_accepts_macs_too():
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({}, ["00:1A:7D:DA:71:14"])
    try:
        assert utils_ble.adapters_in_attempt_order("C8:47:8C:00:00:11", present=ADAPTERS) == ["hci4"]
    finally:
        _configure(original_pins, original_pool)


def test_is_adapter_mac_distinguishes_the_two_forms():
    assert utils_ble.is_adapter_mac("00:1A:7D:DA:71:13")
    assert utils_ble.is_adapter_mac(" 00:1a:7d:da:71:13 ")
    assert not utils_ble.is_adapter_mac("hci0")
    assert not utils_ble.is_adapter_mac("00:1A:7D:DA:71")
    assert not utils_ble.is_adapter_mac("")


# --------- writing adapter MACs back to the config ---------


def _write_config(tmp_path, body):
    p = tmp_path / "config.ini"
    p.write_text(body)
    return str(p)


def test_hci_names_are_rewritten_to_macs_with_a_comment(tmp_path):
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({"C8:47:8C:00:00:00": ["hci3"]}, [])
    cfg = _write_config(tmp_path, "[DEFAULT]\nBLUETOOTH_ADAPTERS = C8:47:8C:00:00:00@hci3\n")
    try:
        assert utils_ble.pin_adapters_by_mac(cfg, adapters=ADAPTERS) is True
        text = open(cfg).read()
        assert "C8:47:8C:00:00:00@00:1A:7D:DA:71:13" in text
        assert "hci3 was detected as 00:1A:7D:DA:71:13" in text
        # the comment must be a comment, above the line it explains
        lines = text.splitlines()
        note = next(i for i, ln in enumerate(lines) if "was detected as" in ln)
        assert lines[note].lstrip().startswith(";")
        assert lines[note + 1].startswith("BLUETOOTH_ADAPTERS")
    finally:
        _configure(original_pins, original_pool)


def test_rewriting_leaves_every_other_line_alone(tmp_path):
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({}, ["hci3"])
    body = "[DEFAULT]\n; a comment about hci3 that must not change\nMAX_BATTERY_CHARGE_CURRENT = 50.0\nBLUETOOTH_ADAPTERS = hci3\n"
    cfg = _write_config(tmp_path, body)
    try:
        utils_ble.pin_adapters_by_mac(cfg, adapters=ADAPTERS)
        text = open(cfg).read()
        assert "; a comment about hci3 that must not change" in text
        assert "MAX_BATTERY_CHARGE_CURRENT = 50.0" in text
    finally:
        _configure(original_pins, original_pool)


def test_hci1_is_not_matched_inside_hci10(tmp_path):
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({}, ["hci1"])
    cfg = _write_config(tmp_path, "[DEFAULT]\nBLUETOOTH_ADAPTERS = hci10, hci1\n")
    try:
        utils_ble.pin_adapters_by_mac(cfg, adapters={"hci1": "00:1A:7D:DA:71:13", "hci10": "00:1A:7D:DA:71:99"})
        text = open(cfg).read()
        assert "hci10, 00:1A:7D:DA:71:13" in text
    finally:
        _configure(original_pins, original_pool)


def test_a_dead_controller_is_never_written_back(tmp_path):
    """All-zeros is the kernel's answer for a card it cannot talk to. Pinning
    a battery to that would be worse than leaving the name in place."""
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({}, ["hci9"])
    cfg = _write_config(tmp_path, "[DEFAULT]\nBLUETOOTH_ADAPTERS = hci9\n")
    try:
        assert utils_ble.pin_adapters_by_mac(cfg, adapters={"hci9": "00:00:00:00:00:00"}) is False
        assert "hci9" in open(cfg).read()
    finally:
        _configure(original_pins, original_pool)


def test_entries_already_written_as_macs_are_left_alone(tmp_path):
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({}, ["00:1A:7D:DA:71:13"])
    cfg = _write_config(tmp_path, "[DEFAULT]\nBLUETOOTH_ADAPTERS = 00:1A:7D:DA:71:13\n")
    try:
        assert utils_ble.pin_adapters_by_mac(cfg, adapters=ADAPTERS) is False
        assert "was detected as" not in open(cfg).read()
    finally:
        _configure(original_pins, original_pool)


def test_a_commented_out_line_is_not_rewritten(tmp_path):
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({}, ["hci3"])
    cfg = _write_config(tmp_path, "[DEFAULT]\n; BLUETOOTH_ADAPTERS = hci3\nBLUETOOTH_ADAPTERS = hci3\n")
    try:
        utils_ble.pin_adapters_by_mac(cfg, adapters=ADAPTERS)
        lines = open(cfg).read().splitlines()
        assert "; BLUETOOTH_ADAPTERS = hci3" in lines
    finally:
        _configure(original_pins, original_pool)


def test_an_unwritable_config_is_not_worth_failing_over(tmp_path):
    original_pins, original_pool = utils_ble.BLUETOOTH_ADAPTER_PINS, utils_ble.BLUETOOTH_ADAPTER_POOL
    _configure({}, ["hci3"])
    try:
        assert utils_ble.pin_adapters_by_mac(str(tmp_path / "nope.ini"), adapters=ADAPTERS) is False
    finally:
        _configure(original_pins, original_pool)
