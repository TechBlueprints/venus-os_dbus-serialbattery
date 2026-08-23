# -*- coding: utf-8 -*-
"""Claims-gated adapter recovery.

habluetooth escalates a persistently quiet scanner to a hardware reset
(rfkill unblock, power cycle, USB port reset for USB adapters - a reset
can renumber the hci device). In a multi-process deployment a reset is a
shared-radio hazard: every link on the card dies, including other
processes' - so this module performs the same escalation but coordinates
it through the claims convention: a drain claim asks every claimant that
can move to migrate off the card, the reset waits for the card to empty,
and gives up at its deadline rather than pulling it out from under a
holder that cannot move (its only working card, an operator pin).

The reset primitive itself is native and stdlib-only: sysfs rfkill
unblock (/dev/rfkill fallback), an HCIDEVDOWN/HCIDEVUP bounce over a raw
AF_BLUETOOTH socket, and - for a card that has gone silent - USBDEVFS_RESET
on the adapter's USB device node. When the optional bluetooth-auto-recovery
package (the "recovery" extra) is importable it is preferred, for its
mgmt-socket powered handling and post-reset adapter re-find; it is NOT
vendorable into ext/ (its rfkill path hard-imports GPLv3 PyRIC) and can
never be present on a Cerbo (Venus has no usable pip), so on Venus the
native sequence is always the one that runs.

After any successful reset the BlueZ side is repaired too: bluetoothd is
restarted if the reset killed it (v1's Stuck State 11), and bleak's cached
D-Bus manager state is invalidated so the next connect rebuilds from
GetManagedObjects instead of stale object paths.
"""

import asyncio
import logging
import os
import re
import socket
import struct
import subprocess
import time

from . import claims, mgmt

logger = logging.getLogger(__name__)

try:
    from bluetooth_auto_recovery import recover_adapter

    HAS_AUTO_RECOVERY = True
except ImportError:
    recover_adapter = None
    HAS_AUTO_RECOVERY = False

# adapter identity lives in claims.py: it is the module that must name a
# card stably, it is stdlib-only, and one implementation beats two
UNKNOWN_MAC = claims.UNKNOWN_MAC
adapter_mac = claims.adapter_mac

# module-level so tests can point them at a fake tree
BT_SYSFS = "/sys/class/bluetooth"
RFKILL_SYSFS = "/sys/class/rfkill"
RFKILL_DEV = "/dev/rfkill"
USB_SYSFS = "/sys/bus/usb/devices"
USB_DEVFS = "/dev/bus/usb"
PROC = "/proc"

# linux/rfkill.h: struct rfkill_event {u32 idx; u8 type, op, soft, hard}
_RFKILL_EVENT = "IBBBB"
_RFKILL_TYPE_BLUETOOTH = 2
_RFKILL_OP_CHANGE_ALL = 3
# linux/hci.h ioctls, and _IO('U', 20) from linux/usbdevice_fs.h
HCIDEVUP = 0x400448C9
HCIDEVDOWN = 0x400448CA
USBDEVFS_RESET = 0x5514
BTPROTO_HCI = 1

# how long a coordinated reset waits for the card to drain, and how often
# it re-checks; consumers notice the drain on their claim heartbeat (10s),
# then their disconnect must round-trip a reconnect elsewhere
DRAIN_TIMEOUT = 60.0
_DRAIN_POLL = 2.0

def _rfkill_unblock(adapter):
    """Clear soft rfkill blocks on bluetooth switches, best effort.

    sysfs first: every type=="bluetooth" rfkill with soft==1 gets a "0"
    written back (the per-hci rfkill is named after the adapter on BlueZ,
    but a platform switch like "bt_default" blocks it just as hard, so all
    bluetooth switches are cleared, matching what an operator's `rfkill
    unblock bluetooth` does). Falls back to one RFKILL_OP_CHANGE_ALL event
    on /dev/rfkill. Returns whether any block was found and cleared.
    """
    cleared = False
    try:
        entries = os.listdir(RFKILL_SYSFS)
    except OSError:
        entries = []
    for entry in entries:
        base = os.path.join(RFKILL_SYSFS, entry)
        try:
            with open(os.path.join(base, "type")) as f:
                if f.read().strip() != "bluetooth":
                    continue
            with open(os.path.join(base, "soft")) as f:
                if f.read().strip() != "1":
                    continue
            with open(os.path.join(base, "soft"), "w") as f:
                f.write("0")
            logger.warning(f"bt-recovery: cleared rfkill soft block on {entry} for {adapter}")
            cleared = True
        except OSError:
            continue
    if cleared or entries:
        return cleared
    # no sysfs view: one change-all event for the bluetooth type
    try:
        with open(RFKILL_DEV, "wb") as f:
            f.write(struct.pack(_RFKILL_EVENT, 0, _RFKILL_TYPE_BLUETOOTH, _RFKILL_OP_CHANGE_ALL, 0, 0))
        return True
    except OSError:
        return False


def _bounce_interface_hciconfig(dev_id):
    """The subprocess fallback: Venus OS ships a Python built without
    bluetooth socket support (no socket.AF_BLUETOOTH - field 2026-08-22,
    both Cerbos), so on the flagship platform the ioctl path cannot even
    open its socket. hciconfig is present there and is literally the
    runbook remedy this function automates."""
    adapter = f"hci{dev_id}"
    try:
        subprocess.run(["hciconfig", adapter, "down"], capture_output=True, timeout=10)
        time.sleep(0.5)
        result = subprocess.run(["hciconfig", adapter, "up"], capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            logger.warning(f"bt-recovery: hciconfig {adapter} up failed: {detail or result.returncode}")
            return False
        return True
    except Exception as e:
        logger.warning(f"bt-recovery: hciconfig bounce of {adapter} failed: {repr(e)}")
        return False


def _bounce_interface(dev_id):
    """hciconfig down/up: HCIDEVDOWN then HCIDEVUP over a raw AF_BLUETOOTH
    socket - opened via mgmt.open_bt_socket, whose ctypes path covers the
    Venus Python built without Bluetooth support - falling back to the
    hciconfig subprocess when no socket can be had at all. Blocking
    (caller runs it in an executor). Returns whether the up succeeded -
    down failing is fine (the interface may already be down, which is why
    it is being reset)."""
    try:
        sock = mgmt.open_bt_socket()
    except OSError as e:
        logger.debug(f"bt-recovery: cannot open a bluetooth socket ({repr(e)}), using hciconfig")
        return _bounce_interface_hciconfig(dev_id)
    from fcntl import ioctl

    try:
        try:
            ioctl(sock.fileno(), HCIDEVDOWN, dev_id)
        except OSError as e:
            logger.debug(f"bt-recovery: HCIDEVDOWN hci{dev_id}: {repr(e)}")
        time.sleep(0.5)
        try:
            ioctl(sock.fileno(), HCIDEVUP, dev_id)
        except OSError as e:
            # EALREADY: something else raced the interface back up - fine
            if e.errno != 114:
                logger.warning(f"bt-recovery: HCIDEVUP hci{dev_id} failed: {repr(e)}")
                return False
        return True
    finally:
        sock.close()


def _usb_reset(adapter):
    """USBDEVFS_RESET the adapter's USB device, or None if it is not USB.

    The sysfs device link for a USB hci names the interface (e.g.
    "1-1.2:1.0"); its parent directory ("1-1.2") carries busnum/devnum,
    which locate the devfs node to ioctl. A UART/platform controller has no
    such shape and returns None - not False: not-USB is not a failure.
    Blocking (caller runs it in an executor).
    """
    try:
        link = os.readlink(os.path.join(BT_SYSFS, adapter, "device"))
    except OSError:
        return None
    interface = os.path.basename(link)
    if ":" not in interface or "-" not in interface:
        return None
    parent = interface.split(":")[0]
    try:
        with open(os.path.join(USB_SYSFS, parent, "busnum")) as f:
            busnum = int(f.read().strip())
        with open(os.path.join(USB_SYSFS, parent, "devnum")) as f:
            devnum = int(f.read().strip())
    except (OSError, ValueError):
        return None
    from fcntl import ioctl

    node = os.path.join(USB_DEVFS, f"{busnum:03d}", f"{devnum:03d}")
    try:
        fd = os.open(node, os.O_WRONLY)
    except OSError as e:
        logger.warning(f"bt-recovery: cannot open {node} for USB reset of {adapter}: {repr(e)}")
        return False
    try:
        ioctl(fd, USBDEVFS_RESET, 0)
        logger.warning(f"bt-recovery: USB port reset issued for {adapter} ({node})")
        return True
    except OSError as e:
        logger.warning(f"bt-recovery: USB reset of {adapter} failed: {repr(e)}")
        return False
    finally:
        os.close(fd)


async def _native_recover(dev_id, adapter, gone_silent):
    """The stdlib reset sequence: rfkill unblock, interface bounce, and -
    for a card that has gone silent - a USB port reset when the card is
    USB. Success is judged by the card answering afterwards: its MAC reads
    as something other than all-zeros (the kernel's failed-adapter value),
    checked with the cache invalidated because a reset is exactly the
    moment a cached MAC goes stale."""
    loop = asyncio.get_running_loop()
    _rfkill_unblock(adapter)
    bounced = await loop.run_in_executor(None, _bounce_interface, dev_id)
    if gone_silent:
        usb = await loop.run_in_executor(None, _usb_reset, adapter)
        if usb:
            # the reset re-enumerates the device; give the kernel and
            # bluetoothd a moment before judging
            await asyncio.sleep(3.5)
        elif usb is None and not bounced:
            return False  # not USB and the bounce failed: nothing worked
    elif not bounced:
        return False
    claims.invalidate_adapter_mac(adapter)
    alive = claims.adapter_mac(adapter) != UNKNOWN_MAC
    if not alive:
        logger.warning(f"bt-recovery: {adapter} still reports no MAC after reset")
    return alive


def is_bluetoothd_alive():
    """Whether a bluetoothd process exists, by /proc comm scan (BusyBox-safe:
    no pidof/pgrep). No /proc (non-Linux) reads as alive."""
    try:
        entries = os.listdir(PROC)
    except OSError:
        return True
    found_proc = False
    for entry in entries:
        if not entry.isdigit():
            continue
        found_proc = True
        try:
            with open(os.path.join(PROC, entry, "comm")) as f:
                if f.read().strip() == "bluetoothd":
                    return True
        except OSError:
            continue
    return not found_proc


async def restart_bluetoothd(init_script="/etc/init.d/bluetooth", timeout=10.0):
    """Start bluetoothd via its init script if it is not running.

    Venus OS runs bluetoothd from a SysV script with no crash supervision
    (v1's Stuck State 11: a reset can take bluetoothd down with it, and it
    stays down). Returns whether bluetoothd is running on exit.
    """
    if is_bluetoothd_alive():
        return True
    if not os.path.isfile(init_script):
        logger.error(f"bt-recovery: bluetoothd is not running and {init_script} does not exist")
        return False
    logger.warning(f"bt-recovery: bluetoothd is not running, starting it via {init_script}")
    try:
        proc = await asyncio.create_subprocess_exec(
            init_script, "start", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        try:
            await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            logger.error(f"bt-recovery: bluetoothd restart timed out after {timeout:.0f}s")
            return False
    except Exception as e:
        logger.error(f"bt-recovery: bluetoothd restart failed: {repr(e)}")
        return False
    await asyncio.sleep(0.5)
    return is_bluetoothd_alive()


async def invalidate_dbus_state():
    """Best-effort teardown of bleak's cached BlueZ D-Bus state.

    A reset makes bluetoothd rebuild its object tree, so device paths and
    adapter properties bleak cached beforehand are stale - and bleak skips
    re-initialization while its bus still reads connected. Disconnecting
    the per-loop global manager's bus forces the next connect to rebuild
    from GetManagedObjects. Reaches into bleak internals by necessity;
    every step degrades silently because the internals drift by version.
    """
    try:
        from bleak.backends.bluezdbus.manager import _global_instances
    except Exception:
        return
    try:
        manager = _global_instances.get(asyncio.get_running_loop())
    except Exception:
        return
    if manager is None:
        return
    bus = getattr(manager, "_bus", None)
    if bus is not None:
        try:
            bus.disconnect()
        except Exception:
            pass
        try:
            manager._bus = None
        except Exception:
            pass
        logger.info("bt-recovery: invalidated bleak's cached BlueZ manager bus")


async def _drain_and_wait(adapter, claims_manager, drain_timeout):
    """Take the drain claim and wait for the card to empty.

    Returns (drain-claim-or-None, ok). ok False means back off entirely:
    either another process is already draining the card, or holders that
    cannot move are still on it at the deadline. Foreign claims are the
    veto; our own remaining claims are only waited on (our own links get
    killed by our own choice - the wait gives them time to migrate)."""
    drain = claims_manager.claim_drain(adapter)
    if drain is None:
        logger.warning(f"bt-recovery: not resetting {adapter}: another process is already draining it")
        return None, False
    deadline = time.monotonic() + max(0.0, drain_timeout)
    logged = False
    while True:
        foreign = claims_manager.foreign_use(adapter)
        own = claims_manager.own_use(adapter)
        if foreign == 0 and own == 0:
            return drain, True
        if time.monotonic() >= deadline:
            if foreign:
                holders = (claims_manager.claims().get(adapter) or {}).get("soft_owners", [])
                logger.warning(
                    f"bt-recovery: not resetting {adapter}: {foreign} live foreign claim(s) "
                    f"remain after {drain_timeout:.0f}s drain ({holders or 'unnamed'})"
                )
                claims_manager.release(drain)
                return None, False
            # only our own remain: they could not migrate, and we are the
            # one asking - proceed, as an uncoordinated reset always did
            logger.warning(
                f"bt-recovery: {own} of our own claim(s) on {adapter} could not migrate, resetting anyway"
            )
            return drain, True
        if not logged:
            logger.warning(
                f"bt-recovery: draining {adapter} before reset "
                f"({foreign} foreign / {own} own live claim(s))"
            )
            logged = True
        await asyncio.sleep(_DRAIN_POLL)


async def reset_adapter(adapter, claims_manager=None, force=False, gone_silent=False, drain_timeout=DRAIN_TIMEOUT):
    """Hardware-reset an adapter, coordinated through the claims convention.

    With a claims_manager, a drain claim is taken first: placement steers
    new work away, claimants that can move migrate off the card, and the
    reset waits (up to drain_timeout) for the card to empty. Foreign claims
    still on it at the deadline veto the reset - a holder that cannot move
    keeps its card. drain_timeout=0 restores the old semantics: an
    immediate foreign-claims gate with no waiting. force skips every gate -
    for an operator who knows the card is dead.

    gone_silent escalates the recovery (power cycle, then USB reset for USB
    adapters), matching habluetooth's watchdog flag. The reset itself
    prefers bluetooth-auto-recovery when installed and falls back to the
    stdlib-native sequence; after a success, bluetoothd is restarted if the
    reset killed it and bleak's cached D-Bus state is invalidated.
    Returns True when a reset was performed and reported success.
    """
    match = re.match(r"hci(\d+)$", str(adapter))
    if not match:
        logger.warning(f"bt-recovery: cannot reset '{adapter}': not an hciN adapter name")
        return False
    drain = None
    if claims_manager is not None and not force:
        if drain_timeout > 0:
            drain, ok = await _drain_and_wait(adapter, claims_manager, drain_timeout)
            if not ok:
                return False
        else:
            foreign = claims_manager.foreign_use(adapter)
            if foreign:
                logger.warning(
                    f"bt-recovery: not resetting {adapter}: {foreign} live claim(s) held by other processes"
                )
                return False
    elif claims_manager is not None:
        # forced: the drain claim is still worth holding while we reset so
        # other processes do not place new work onto a card mid-reset
        drain = claims_manager.claim_drain(adapter)
    try:
        logger.warning(f"bt-recovery: resetting {adapter}" + (" (gone silent)" if gone_silent else ""))
        try:
            if HAS_AUTO_RECOVERY:
                ok = bool(await recover_adapter(int(match.group(1)), adapter_mac(adapter), gone_silent))
            else:
                ok = await _native_recover(int(match.group(1)), adapter, gone_silent)
        except Exception as e:
            logger.warning(f"bt-recovery: reset of {adapter} failed: {repr(e)}")
            return False
        claims.invalidate_adapter_mac(adapter)
        if ok:
            # the card was just power-cycled: its accumulated failure record
            # describes a radio that no longer exists in that state, and for
            # scans it is self-reinforcing (ranked last, so never selected to
            # earn the success that would clear it)
            try:
                from .catcher import forget_adapter_failures

                forget_adapter_failures(adapter)
            except Exception:
                pass
            if not await restart_bluetoothd():
                logger.error(f"bt-recovery: {adapter} reset but bluetoothd could not be revived")
                return False
            await invalidate_dbus_state()
        return ok
    finally:
        if drain is not None:
            claims_manager.release(drain)
