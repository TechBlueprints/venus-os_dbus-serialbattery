# -*- coding: utf-8 -*-
"""Claims-gated adapter recovery.

habluetooth escalates a persistently quiet scanner to a hardware reset via
bluetooth-auto-recovery (rfkill unblock, power cycle, USB port reset for
USB adapters - a reset can renumber the hci device). In a multi-process
deployment a reset is a shared-radio hazard: every link on the card dies,
including other processes' - so this module performs the same escalation
but refuses to reset an adapter that another live process holds claims on.
bluetooth-auto-recovery is an optional dependency (the "recovery" extra);
without it reset_adapter degrades to a logged no-op.
"""

import logging
import re
import subprocess
import time

logger = logging.getLogger(__name__)

try:
    from bluetooth_auto_recovery import recover_adapter

    HAS_AUTO_RECOVERY = True
except ImportError:
    recover_adapter = None
    HAS_AUTO_RECOVERY = False

UNKNOWN_MAC = "00:00:00:00:00:00"

# adapter -> (mac, monotonic); short-lived because a reset can take a card
# from all-zeros to a real address, and long enough that selection (which
# asks per candidate per connect) does not spawn hciconfig in a tight loop
# on kernels with no sysfs address attribute
_MAC_CACHE_TTL = 30.0
_mac_cache = {}


def _read_adapter_mac(adapter):
    # sysfs first - but some kernels (Venus OS Cerbos among them) expose no
    # address attribute under /sys/class/bluetooth/hciN/ at all, so fall
    # back to parsing hciconfig, which does report BD Address there
    try:
        with open(f"/sys/class/bluetooth/{adapter}/address") as f:
            mac = f.read().strip().upper()
            if mac:
                return mac
    except OSError:
        pass
    try:
        result = subprocess.run(["hciconfig", str(adapter)], capture_output=True, text=True, timeout=5)
        match = re.search(r"BD Address:\s*([0-9A-Fa-f:]{17})", result.stdout)
        if match:
            return match.group(1).upper()
    except Exception:
        pass
    return UNKNOWN_MAC


def adapter_mac(adapter):
    """The adapter's own MAC (sysfs, then hciconfig), or the all-zeros
    unknown value - which is also what a genuinely failed adapter reports."""
    now = time.monotonic()
    cached = _mac_cache.get(adapter)
    if cached is not None and now - cached[1] < _MAC_CACHE_TTL:
        return cached[0]
    mac = _read_adapter_mac(adapter)
    _mac_cache[adapter] = (mac, now)
    return mac


async def reset_adapter(adapter, claims_manager=None, force=False, gone_silent=False):
    """Hardware-reset an adapter, unless other live processes are using it.

    gone_silent escalates the recovery (power cycle, then USB reset for USB
    adapters), matching habluetooth's watchdog flag. force skips the
    foreign-claims gate - for an operator who knows the card is dead.
    Returns True when a reset was performed and reported success.
    """
    match = re.match(r"hci(\d+)$", str(adapter))
    if not match:
        logger.warning(f"bt-recovery: cannot reset '{adapter}': not an hciN adapter name")
        return False
    if claims_manager is not None and not force:
        foreign = claims_manager.foreign_use(adapter)
        if foreign:
            logger.warning(
                f"bt-recovery: not resetting {adapter}: {foreign} live claim(s) held by other processes"
            )
            return False
    if not HAS_AUTO_RECOVERY:
        logger.warning(f"bt-recovery: cannot reset {adapter}: bluetooth-auto-recovery is not installed")
        return False
    logger.warning(f"bt-recovery: resetting {adapter}" + (" (gone silent)" if gone_silent else ""))
    try:
        return bool(await recover_adapter(int(match.group(1)), adapter_mac(adapter), gone_silent))
    except Exception as e:
        logger.warning(f"bt-recovery: reset of {adapter} failed: {repr(e)}")
        return False
