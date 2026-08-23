

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
    called = {
        n.func.attr
        for n in ast.walk(refresh)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
    }
    assert "_poll_update" in called, "refresh_data must drive the update through the non-blocking poller"
    assert "_run_coro" not in called, "refresh_data must not call the blocking runner"

    # and the poller itself must never wait on the future
    poller = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_poll_update")
    for n in ast.walk(poller):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "result":
            # result(0) is a harvest of an already-finished future, not a wait
            assert n.args and isinstance(n.args[0], ast.Constant) and n.args[0].value == 0, "harvest must use result(0); anything else waits"
