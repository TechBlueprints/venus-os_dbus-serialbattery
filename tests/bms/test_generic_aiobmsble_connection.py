import sys

import pytest

# generic_aiobmsble annotates function signatures with PEP 604 unions, and so
# does upstream: `def _run_coro(self, coro, timeout: float | None = None)`.
# Signature annotations are evaluated at def time, so the module cannot be
# imported at all before 3.10 - a syntax check does not catch this. CI runs
# 3.12, where the tests below execute rather than skip.
_needs_pep604 = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="generic_aiobmsble cannot be imported before Python 3.10 (PEP 604 annotations in signatures)",
)


# --------- the refresh path must not start a discovery ---------
#
# Field failure, dev-cerbo 2026-08-23: with the client lost and another
# service already scanning the adapter, a bare find_device_by_address on
# every poll failed with org.bluez.Error.InProgress, blocked the caller for
# the whole coroutine timeout, and starved the GLib main thread so the
# battery service could not answer D-Bus at all.


def test_refresh_resolves_cache_first_and_does_not_scan():
    import ast
    import os

    src = os.path.join(os.path.dirname(__file__), "..", "..", "dbus-serialbattery", "bms", "generic_aiobmsble.py")
    tree = ast.parse(open(src).read())

    scanning_calls = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if isinstance(f, ast.Attribute) and f.attr == "find_device_by_address":
            scanning_calls.append(node.lineno)

    resolver = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == "_resolve_device")
    inside = [ln for ln in scanning_calls if resolver.lineno <= ln <= (resolver.end_lineno or resolver.lineno)]

    # every discovery in this module must live inside _resolve_device, which
    # only reaches it after the BlueZ cache has missed
    assert scanning_calls, "expected the cache-miss fallback to still exist"
    assert scanning_calls == inside, f"find_device_by_address called outside _resolve_device at lines {sorted(set(scanning_calls) - set(inside))}"


def test_refresh_never_blocks_the_main_thread_on_the_bms():
    """refresh_data runs on the GLib main thread, which also answers D-Bus.

    Waiting there for a BMS coroutine stops the driver serving anything:
    on dev-cerbo an unreachable pack blocked it for 10 s out of every 10 s,
    the battery service stopped answering /Soc and /Mgmt/Connection while
    still registered, and the fallback never got a turn. The poll must
    schedule and harvest, never wait.
    """
    import ast
    import os

    src = os.path.join(os.path.dirname(__file__), "..", "..", "dbus-serialbattery", "bms", "generic_aiobmsble.py")
    tree = ast.parse(open(src).read())

    refresh = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "refresh_data")
    called = {n.func.attr for n in ast.walk(refresh) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert "_poll_update" in called, "refresh_data must drive the update through the non-blocking poller"
    assert "_run_coro" not in called, "refresh_data must not call the blocking runner"

    # and the poller itself must never wait on the future
    poller = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_poll_update")
    for n in ast.walk(poller):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "result":
            # result(0) is a harvest of an already-finished future, not a wait
            assert n.args and isinstance(n.args[0], ast.Constant) and n.args[0].value == 0, "harvest must use result(0); anything else waits"


# --------- an unreachable device must be paced, not hammered ---------
#
# Field failure, dev-cerbo 2026-09-18: a USB dongle was swapped while a
# battery was pinned to the old card's MAC. refresh_data polls at 1 Hz and
# every poll ran a full establish_connection (four BlueZ attempts of its
# own), for about 18 hours. The condition could not clear until a card or
# the config changed, so the retries bought nothing, cost load, and at two
# log lines per second evicted their own onset from the retained log.


def _load_driver():
    import importlib
    import os
    import sys

    import types

    root = os.path.join(os.path.dirname(__file__), "..", "..", "dbus-serialbattery")
    if root not in sys.path:
        sys.path.insert(0, root)

    # Stub the BLE libraries rather than importing the vendored copies: they
    # use match statements, so on Python 3.9 a real import is a SyntaxError.
    # Same approach as tests/conftest.py, and only ever additive - an already
    # imported real module wins.
    def _stub(name, **attrs):
        if name not in sys.modules:
            mod = types.ModuleType(name)
            for k, v in attrs.items():
                setattr(mod, k, v)
            sys.modules[name] = mod
        return sys.modules[name]

    _stub("bleak", BleakScanner=object)
    _stub("bleak.backends")
    _stub("bleak.backends.device", BLEDevice=object)
    _stub("bleak.exc", BleakError=type("BleakError", (Exception,), {}))
    _stub("aiobmsble", BMSInfo=dict, BMSSample=dict, TempSensor=object)

    return importlib.import_module("bms.generic_aiobmsble")


def _paced_instance(mod):
    """A bare instance carrying only the reconnect-pacing state."""
    # The REAL methods, borrowed onto a stand-in that has no __del__: a bare
    # Generic_AioBmsBle would run the background-loop teardown on collection
    # and bury the assertions in unrelated log noise. The functions under test
    # are the production ones either way.
    driver = mod.Generic_AioBmsBle
    stand_in = type(
        "PacedStandIn",
        (),
        {
            "_reconnect_on_hold": driver._reconnect_on_hold,
            "_note_connect_failure": driver._note_connect_failure,
            "_note_connect_success": driver._note_connect_success,
        },
    )
    obj = stand_in()
    obj.address = "A4:C1:38:33:41:24"
    obj._connect_failures = 0
    obj._reconnect_hold_until = 0.0
    obj._reconnect_warned = False
    return obj


@_needs_pep604
def test_unreachable_device_is_paced_but_never_abandoned():
    mod = _load_driver()
    bms = _paced_instance(mod)

    # a first miss must NOT impose a wait: a pack that slept through one
    # advertising window has to recover on the very next poll
    bms._note_connect_failure("device not found")
    assert not bms._reconnect_on_hold(), "a single miss must not pace the next attempt"

    # a sustained outage must reach the longest step
    for _ in range(len(mod.RECONNECT_BACKOFF_SECONDS) + 3):
        bms._note_connect_failure("device not found")
    assert bms._reconnect_on_hold(), "a sustained outage must pace the next attempt"

    # ...and must still RETRY once the wait elapses. This is the assertion that
    # separates "paced" from "gave up" - a driver that stops trying would also
    # pass a test that only checked the hammering had stopped.
    import time as _t

    bms._reconnect_hold_until = _t.monotonic() - 0.01
    assert not bms._reconnect_on_hold(), "pacing must expire so the device is retried, not abandoned"


@_needs_pep604
def test_sustained_outage_warns_once_and_resets_on_recovery():
    mod = _load_driver()
    bms = _paced_instance(mod)

    warnings = []
    real_logger = mod.logger

    class _Capture:
        def warning(self, msg, *a):
            warnings.append(msg % a if a else msg)

        def debug(self, msg, *a):
            pass

        def error(self, msg, *a):
            pass

        def info(self, msg, *a):
            pass

    mod.logger = _Capture()
    try:
        for _ in range(60):  # a full minute of 1 Hz polling
            bms._note_connect_failure("device not found")
        assert len(warnings) == 1, f"one warning per outage, got {len(warnings)}: {warnings}"

        bms._note_connect_success()
        assert len(warnings) == 2, "recovery must be reported too"
        assert bms._connect_failures == 0 and not bms._reconnect_on_hold(), "success must clear the pacing state"

        # a later outage warns again - the once-per-outage latch must reset
        for _ in range(60):
            bms._note_connect_failure("device not found")
        assert len(warnings) == 3, "a second outage must warn again"
    finally:
        mod.logger = real_logger
