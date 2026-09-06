# -*- coding: utf-8 -*-
"""Opt-in wiring for the shared bleak-connection-manager (the bleak catcher).

Kept separate from utils_ble on purpose: the catcher rebinds
bleak.BleakClient process wide, and a module only picks the wrapper up
through its own `from bleak import BleakClient` if the install already
happened when it was imported. utils_ble and the BMS modules import bleak at
module scope, so dbus-serialbattery.py calls install_ble_connection_manager()
before importing any of them - which is why this module must not import
bleak, utils_ble or a BMS module at module scope itself.
"""

import os
import sys

import utils
from utils import logger


def parse_link_caps(entries):
    """
    Split BLUETOOTH_CONNECTION_MANAGER_LINK_CAPS into {adapter: capacity}.

    Entries have the form ADAPTER:N with N a positive integer, the
    established-link capacity of that adapter, and ADAPTER an hciX name or
    the adapter's own MAC (the same identities BLUETOOTH_ADAPTERS accepts).
    The split is on the LAST colon, because a MAC is full of them.
    Malformed entries are logged and skipped rather than guessed at: a wrong
    cap silently gates connections.
    """
    caps = {}
    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue
        adapter, sep, cap = entry.rpartition(":")
        adapter = adapter.strip()
        try:
            cap_value = int(cap.strip()) if sep else 0
        except ValueError:
            cap_value = 0
        if not adapter or cap_value <= 0:
            logger.warning(f"Ignoring malformed BLUETOOTH_CONNECTION_MANAGER_LINK_CAPS entry '{entry}'")
            continue
        caps[adapter] = cap_value
    return caps


def install_ble_connection_manager(address):
    """
    Install the bleak catcher for this battery's process, when enabled.

    Returns True when the catcher was installed. BLUETOOTH_ADAPTERS entries
    are handed over verbatim - the library understands the same MAC@hciX and
    plain hciX forms, with the same failure-driven walk contract for pinned
    devices - so one config key drives both the catcher and the plain
    backends identically.

    A failed install is logged and swallowed: the catcher is coordination,
    and connecting uncoordinated beats not connecting at all.
    """
    import ble_stack

    shared_dir = utils.BLUETOOTH_CONNECTION_MANAGER_DIR

    if not utils.BLUETOOTH_CONNECTION_MANAGER:
        # Silent. A box that never asked for coordination has nothing to
        # report, and "loaded from" is what the fleet's log watch reads as
        # coordination ACTIVE - which it would not be with the catcher off.
        return False

    if ble_stack.current() == "shared":
        # The PACKAGE directory, not the configured folder: it proves which
        # tree actually served the import, which is the whole question when a
        # box has both a shared install and this repo's ext/ble copies.
        loaded = sys.modules.get("bleak_connection_manager")
        package_dir = os.path.dirname(getattr(loaded, "__file__", "") or "") or shared_dir
        logger.info(f"BLE coordination: bleak_connection_manager loaded from {package_dir}")

    if not shared_dir:
        logger.warning(
            "BLE coordination: BLUETOOTH_CONNECTION_MANAGER is on but "
            "BLUETOOTH_CONNECTION_MANAGER_DIR is empty; running uncoordinated, no claims, no adapter routing, no card recovery"
        )
        return False
    if ble_stack.shared_failure:
        logger.error(f"BLE coordination: shared install at {shared_dir} is present but unusable, running uncoordinated: {ble_stack.shared_failure}")
        return False
    if ble_stack.current() != "shared":
        logger.warning(f"BLE coordination: no shared install at {shared_dir}; running uncoordinated, no claims, no adapter routing, no card recovery")
        return False

    try:
        from bleak_connection_manager import install_bleak_catcher

        validator = None
        if utils.BLUETOOTH_CONNECTION_MANAGER_VALIDATION:
            # weakest built-in validator, wrapped for chips that register
            # their vendor services after ServicesResolved: an empty GATT
            # table is a phantom link, and rejecting it here makes
            # bleak-retry-connector retry on the next radio instead of
            # handing the driver a client that fails on first read
            from bleak_connection_manager.validators import tolerate_late_gatt, validate_gatt_services

            validator = tolerate_late_gatt(validate_gatt_services)

        install_bleak_catcher(
            f"dbus-serialbattery.{str(address).strip().lower().replace(':', '')}",
            adapters=utils.BLUETOOTH_ADAPTERS,
            link_caps=parse_link_caps(utils.BLUETOOTH_CONNECTION_MANAGER_LINK_CAPS),
            wrap_scanner=utils.BLUETOOTH_CONNECTION_MANAGER_WRAP_SCANNER,
            validate_connection=validator,
        )
        return True
    except ImportError as e:
        # The shared tree could not give us the module or the validators:
        # that IS the install being unusable.
        logger.error(f"BLE coordination: shared install at {shared_dir} is present but unusable, running uncoordinated: {repr(e)}")
        return False
    except Exception as e:
        # The install imported fine and the catcher refused to install -
        # a bad kwarg, a validator that raised, a bug in the catcher. Saying
        # "the install is unusable" here would send an operator to replace a
        # shared tree that is not the problem.
        logger.error(f"BLE coordination: catcher would not install from {shared_dir}, running uncoordinated: {repr(e)}")
        return False
