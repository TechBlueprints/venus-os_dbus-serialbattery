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

import inspect
import os
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
    if not utils.BLUETOOTH_CONNECTION_MANAGER:
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

        # fleet policy: BlueZ StartNotify, never AcquireNotify (the BlueZ 5.72
        # notify_io double-free); a consumer-side key, default True, instead of
        # the retired shim's BCM_FORCE_START_NOTIFY environment. The shared
        # install is whatever the box has: one that predates the parameter
        # would raise TypeError and lose the catcher entirely, so the policy
        # is passed the way that install understands it.
        policy = {}
        params = inspect.signature(install_bleak_catcher).parameters
        if "force_start_notify" in params or any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
            policy["force_start_notify"] = utils.BLUETOOTH_CONNECTION_MANAGER_FORCE_START_NOTIFY
        else:
            os.environ["BCM_FORCE_START_NOTIFY"] = "true" if utils.BLUETOOTH_CONNECTION_MANAGER_FORCE_START_NOTIFY else "false"
            logger.warning(
                f"BLE coordination: shared install at {utils.BLUETOOTH_CONNECTION_MANAGER_DIR} predates the force_start_notify "
                "parameter; StartNotify policy passed through the legacy BCM_FORCE_START_NOTIFY environment"
            )

        install_bleak_catcher(
            f"dbus-serialbattery.{str(address).strip().lower().replace(':', '')}",
            adapters=utils.BLUETOOTH_ADAPTERS,
            link_caps=parse_link_caps(utils.BLUETOOTH_CONNECTION_MANAGER_LINK_CAPS),
            wrap_scanner=utils.BLUETOOTH_CONNECTION_MANAGER_WRAP_SCANNER,
            validate_connection=validator,
            **policy,
        )
        import bleak_connection_manager as _bcm

        logger.info(f"BLE coordination: bleak_connection_manager loaded from {os.path.dirname(getattr(_bcm, '__file__', '?'))}")
        return True
    except ImportError:
        import ble_stack

        if ble_stack.shared_failure:
            logger.error(
                f"BLE coordination: shared install at {utils.BLUETOOTH_CONNECTION_MANAGER_DIR} is present but unusable, "
                f"running uncoordinated: {ble_stack.shared_failure}"
            )
        elif not utils.BLUETOOTH_CONNECTION_MANAGER_DIR:
            # Misconfiguration: the manager is wanted but told never to look for it.
            logger.warning(
                "BLE coordination: BLUETOOTH_CONNECTION_MANAGER is on but BLUETOOTH_CONNECTION_MANAGER_DIR is empty; "
                "running uncoordinated, no claims, no adapter routing, no card recovery"
            )
        else:
            # The normal state on any box without the shared install: not a fault.
            logger.warning(
                f"BLE coordination: no shared install at {utils.BLUETOOTH_CONNECTION_MANAGER_DIR}; "
                "running uncoordinated, no claims, no adapter routing, no card recovery"
            )
        return False
    except Exception as e:
        # The install imported fine and the catcher refused to install - a bad
        # kwarg, a validator that raised, a bug in the catcher. Saying "the
        # install is unusable" here would send an operator to replace a shared
        # tree that is not the problem. (Handler and wording from
        # feat/bcm-v2-backend 6aa3ca9.)
        logger.error(f"BLE coordination: catcher would not install from {utils.BLUETOOTH_CONNECTION_MANAGER_DIR}, running uncoordinated: {repr(e)}")
        return False
