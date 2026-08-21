# -*- coding: utf-8 -*-
"""Tests for the bleak catcher: process-wide client routing, habluetooth style.

A rich bleak stub mimics the vendored bleak 3.x contract the wrapper relies
on: __init__ builds the platform backend eagerly (which is why the wrapper
must defer it), connect()/disconnect()/is_connected all delegate to
self._backend. The stub is swapped into sys.modules only while this module's
private utils_ble copy is loaded and while each test runs, so the minimal
stubs other test files share are never disturbed.
"""

import asyncio
import contextlib
import importlib.util
import os
import sys
import types

import pytest

DRIVER_DIR = os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery")
sys.path.insert(0, DRIVER_DIR)

ADDRESS = "C8:47:8C:00:00:00"

# scripts the rich stub consumes, reset per test
CONNECT_RESULTS = []
RECORDED_INITS = []


class _RichBackend:
    def __init__(self, adapter):
        self.adapter = adapter
        self.is_connected = False

    async def connect(self, pair, **kwargs):
        result = CONNECT_RESULTS.pop(0) if CONNECT_RESULTS else True
        if isinstance(result, Exception):
            raise result
        self.is_connected = True

    async def disconnect(self):
        self.is_connected = False


class RichBleakClient:
    """Just like bleak 3.x: the backend is wired up inside __init__."""

    def __init__(self, address_or_ble_device, disconnected_callback=None, services=None, *, timeout=30, pair=False, bluez=None, backend=None, **kwargs):
        adapter = (bluez or {}).get("adapter")
        RECORDED_INITS.append(
            {"address": getattr(address_or_ble_device, "address", address_or_ble_device), "adapter": adapter, "services": services, "extra": sorted(kwargs)}
        )
        self._backend = _RichBackend(adapter)
        self._pair_before_connect = pair

    async def connect(self, **kwargs):
        await self._backend.connect(self._pair_before_connect, **kwargs)

    async def disconnect(self):
        await self._backend.disconnect()

    @property
    def is_connected(self):
        return self._backend.is_connected


def _make_rich_bleak():
    exc = types.ModuleType("bleak.exc")
    exc.BleakError = type("BleakError", (Exception,), {})
    exc.BleakDeviceNotFoundError = type("BleakDeviceNotFoundError", (Exception,), {})
    bleak = types.ModuleType("bleak")
    bleak.BleakClient = RichBleakClient
    bleak.BleakScanner = object
    bleak.exc = exc
    return bleak, exc


RICH_BLEAK, RICH_BLEAK_EXC = _make_rich_bleak()


@contextlib.contextmanager
def _rich_bleak_installed():
    saved = {name: sys.modules.get(name) for name in ("bleak", "bleak.exc")}
    sys.modules["bleak"] = RICH_BLEAK
    sys.modules["bleak.exc"] = RICH_BLEAK_EXC
    try:
        yield
    finally:
        for name, module in saved.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


def _load_utils_ble():
    """Load the real utils_ble bound to the rich stub, under a private name."""
    with _rich_bleak_installed():
        spec = importlib.util.spec_from_file_location("utils_ble_catcher_under_test", os.path.join(DRIVER_DIR, "utils_ble.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


utils_ble = _load_utils_ble()


@pytest.fixture
def catcher(monkeypatch):
    """Rich bleak in sys.modules, fresh router state, uninstalled on exit."""
    CONNECT_RESULTS.clear()
    RECORDED_INITS.clear()
    utils_ble._warned_unretried_addresses.clear()
    monkeypatch.setattr(utils_ble, "ble_adapter_router", utils_ble.BleAdapterRouter())
    with _rich_bleak_installed():
        yield utils_ble
        utils_ble.uninstall_bleak_catcher()


def test_install_rebinds_bleak_and_uninstall_restores(catcher):
    assert sys.modules["bleak"].BleakClient is RichBleakClient
    catcher.install_bleak_catcher()
    assert sys.modules["bleak"].BleakClient is catcher.RoutedBleakClient
    catcher.install_bleak_catcher()  # idempotent
    assert sys.modules["bleak"].BleakClient is catcher.RoutedBleakClient
    catcher.uninstall_bleak_catcher()
    assert sys.modules["bleak"].BleakClient is RichBleakClient


def test_a_late_importer_gets_the_wrapper_like_aiobmsble_does(catcher):
    """
    aiobmsble's BaseBMS does `from bleak import BleakClient` when it is first
    imported and builds a placeholder client immediately. After install, that
    binding must be the wrapper, and the placeholder must be inert: nothing
    constructed, not connected, but still answering is_connected and address.
    """
    catcher.install_bleak_catcher()
    BleakClient = sys.modules["bleak"].BleakClient  # what a late import binds
    device = types.SimpleNamespace(address=ADDRESS)
    placeholder = BleakClient(device, disconnected_callback=None, services=["fff0", "180a"])

    assert isinstance(placeholder, catcher.RoutedBleakClient)
    assert RECORDED_INITS == []  # nothing built before connect
    assert placeholder.is_connected is False
    assert placeholder.address == ADDRESS
    asyncio.run(placeholder.disconnect())  # noop, must not raise


def test_the_seam_keeps_the_original_class_and_cannot_recurse(catcher):
    catcher.install_bleak_catcher()
    # utils_ble's own binding was captured at import time, before install
    assert catcher.BleakClient is RichBleakClient
    assert catcher._ORIGINAL_BLEAK_CLIENT is RichBleakClient
    backend = catcher.get_ble_backend("BleakBackend")
    client = backend.create_client(ADDRESS, None)
    assert type(client) is RichBleakClient


def test_connect_routes_the_selected_adapter_into_bluez_args(catcher, monkeypatch):
    monkeypatch.setattr(catcher, "adapters_in_attempt_order", lambda address, present=None: ["hci7"])
    catcher.install_bleak_catcher()
    client = sys.modules["bleak"].BleakClient(ADDRESS)

    asyncio.run(client.connect())

    assert [r["adapter"] for r in RECORDED_INITS] == ["hci7"]
    assert client.is_connected is True


def test_a_failed_connect_walks_to_the_next_adapter_on_the_same_instance(catcher, monkeypatch):
    """
    bleak-retry-connector retries connect() on one client instance; each
    attempt must re-run the selection so the walk continues mid-retry.
    """
    monkeypatch.setattr(catcher, "adapters_in_attempt_order", lambda address, present=None: ["hci5", "hci6"])
    catcher.install_bleak_catcher()
    client = sys.modules["bleak"].BleakClient(ADDRESS)
    CONNECT_RESULTS.append(RuntimeError("le-connection-abort-by-local"))

    with pytest.raises(RuntimeError):
        asyncio.run(client.connect())
    asyncio.run(client.connect())

    assert [r["adapter"] for r in RECORDED_INITS] == ["hci5", "hci6"]
    assert client.is_connected is True


def test_a_dropped_link_reconnects_on_the_same_adapter(catcher, monkeypatch):
    """A disconnect is not a failed attempt and must not advance the walk."""
    monkeypatch.setattr(catcher, "adapters_in_attempt_order", lambda address, present=None: ["hci5", "hci6"])
    catcher.install_bleak_catcher()
    client = sys.modules["bleak"].BleakClient(ADDRESS)

    asyncio.run(client.connect())
    asyncio.run(client.disconnect())
    asyncio.run(client.connect())

    assert [r["adapter"] for r in RECORDED_INITS] == ["hci5", "hci5"]


def test_an_adapter_chosen_by_the_caller_is_never_overridden(catcher, monkeypatch):
    monkeypatch.setattr(catcher, "adapters_in_attempt_order", lambda address, present=None: ["hci5"])
    catcher.install_bleak_catcher()
    client = sys.modules["bleak"].BleakClient(ADDRESS, bluez={"adapter": "hci9"})

    asyncio.run(client.connect())

    assert [r["adapter"] for r in RECORDED_INITS] == ["hci9"]


def test_without_configured_adapters_the_wrapper_is_a_passthrough(catcher, monkeypatch):
    monkeypatch.setattr(catcher, "adapters_in_attempt_order", lambda address, present=None: [])
    catcher.install_bleak_catcher()
    client = sys.modules["bleak"].BleakClient(ADDRESS, services=["fff0"])

    asyncio.run(client.connect())

    assert RECORDED_INITS == [{"address": ADDRESS, "adapter": None, "services": ["fff0"], "extra": []}]
    assert client.is_connected is True


UNRETRIED_WARNING = "called without bleak-retry-connector"


def test_a_client_built_by_establish_connection_connects_without_the_warning(catcher, caplog):
    """
    bleak-retry-connector marks the clients it constructs with
    _is_retry_client=True (our vendored copy does too); the marker must
    silence the warning and must not leak into the real client's kwargs.
    """
    catcher.install_bleak_catcher()
    client = sys.modules["bleak"].BleakClient(ADDRESS, _is_retry_client=True)

    asyncio.run(client.connect())

    assert UNRETRIED_WARNING not in caplog.text
    assert RECORDED_INITS[0]["extra"] == []  # marker was popped, not passed down


def test_a_bare_connect_warns_that_it_has_no_retry(catcher, caplog):
    catcher.install_bleak_catcher()
    client = sys.modules["bleak"].BleakClient(ADDRESS)

    asyncio.run(client.connect())

    assert UNRETRIED_WARNING in caplog.text
    assert ADDRESS in caplog.text


def test_the_bare_connect_warning_fires_once_per_address(catcher, caplog):
    """A reconnect loop must not repeat this into the GX log every pass."""
    catcher.install_bleak_catcher()
    client = sys.modules["bleak"].BleakClient(ADDRESS)

    asyncio.run(client.connect())
    asyncio.run(client.disconnect())
    asyncio.run(client.connect())
    other = sys.modules["bleak"].BleakClient(ADDRESS)
    asyncio.run(other.connect())

    assert caplog.text.count(UNRETRIED_WARNING) == 1


def test_router_state_is_shared_across_client_instances(catcher, monkeypatch):
    """
    A caller that builds a fresh client per attempt (the old drivers, some
    libraries) must still continue the walk instead of restarting it.
    """
    monkeypatch.setattr(catcher, "adapters_in_attempt_order", lambda address, present=None: ["hci5", "hci6"])
    catcher.install_bleak_catcher()

    first = sys.modules["bleak"].BleakClient(ADDRESS)
    CONNECT_RESULTS.append(RuntimeError("boom"))
    with pytest.raises(RuntimeError):
        asyncio.run(first.connect())

    second = sys.modules["bleak"].BleakClient(ADDRESS)
    asyncio.run(second.connect())

    assert [r["adapter"] for r in RECORDED_INITS] == ["hci5", "hci6"]
