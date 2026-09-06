# -*- coding: utf-8 -*-
"""Decide, once per process, where this driver's BLE stack comes from.

Two sources, never both. A box install of bleak-connection-manager (the
shared install, with its own bleak and bleak-retry-connector) is used when
it is present; otherwise this repo's own copies under ext/ble are used and
the driver runs plain bleak with no coordination at all.

This has to run before ANY module imports bleak: bleak is bound at import
time, and a module that already holds a binding never sees a later change.
That is also why this module imports neither bleak nor utils_ble.

The BLE packages live in ext/ble rather than the flat ext/ for one reason:
dbus-serialbattery.py inserts ext/ at sys.path position 1, which is ahead
of PYTHONPATH, so a flat vendored bleak would shadow a box-installed one no
matter how the process was started.
"""

import importlib
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
VENDORED_BLE_DIR = os.path.join(_HERE, "ext", "ble")

# repr() of what went wrong when a shared install was PRESENT but unusable.
# None means either "no shared install" or "the shared install worked" - the
# caller distinguishes those by the return value, not by this.
shared_failure = None

_decided = None


def current():
    """The decision this process made: "shared", "vendored", or None if undecided.

    Deliberately not a log call: this module runs before utils is
    necessarily usable and must stay importable with nothing but stdlib.
    The caller owns reporting.
    """
    return _decided


def shared_roots(shared_dir):
    """The import roots a box install exposes, in the order it exposes them."""
    ext = os.path.join(shared_dir, "ext")
    return [
        os.path.join(shared_dir, "src"),
        ext,
        os.path.join(ext, "upstream", "bleak"),
        os.path.join(ext, "upstream", "bleak-retry-connector", "src"),
    ]


def _use_vendored():
    global _decided
    if VENDORED_BLE_DIR not in sys.path:
        sys.path.insert(1, VENDORED_BLE_DIR)
    _decided = "vendored"
    return _decided


def ensure_ble_stack(shared_dir):
    """
    Put exactly one BLE stack on sys.path and return which one.

    "shared"   - the box install at shared_dir is in use
    "vendored" - ext/ble is in use, plain bleak, no connection manager

    An empty shared_dir means "never look", which is the upstream default:
    a box that has no shared install should not pay for a lookup or be told
    about one it never asked for.

    Idempotent: the first call decides, later calls report that decision.
    """
    global shared_failure, _decided

    if _decided is not None:
        return _decided

    # Already importable - a shim on PYTHONPATH, or a test stub. Whoever put
    # it there owns the arrangement; do not add paths underneath it.
    if "bleak_connection_manager" in sys.modules:
        _decided = "shared"
        return _decided

    if not shared_dir:
        return _use_vendored()

    if not os.path.isdir(os.path.join(shared_dir, "src", "bleak_connection_manager")):
        return _use_vendored()

    # Present. Claim it by importing it HERE, before bleak exists in this
    # process, so the shared bleak/brc win every later import.
    before_path = list(sys.path)
    before_modules = set(sys.modules)
    for root in reversed(shared_roots(shared_dir)):
        if root not in sys.path:
            sys.path.insert(0, root)
    try:
        importlib.import_module("bleak_connection_manager")
    except BaseException as e:
        # Present but unusable. Withdraw completely rather than run half of
        # it: leaving a partially imported shared stack behind would let some
        # later import resolve against it and the rest against ext/ble.
        shared_failure = repr(e)
        for name in set(sys.modules) - before_modules:
            del sys.modules[name]
        sys.path[:] = before_path
        return _use_vendored()

    _decided = "shared"
    return _decided
