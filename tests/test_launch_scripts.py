# -*- coding: utf-8 -*-
"""The generated run scripts and the pkill patterns that manage them.

Field 2026-09-02: a run script that trapped TERM, forwarded it once and then
`wait`ed fell through when the signal interrupted `wait`, so a python that
survived that single TERM was reparented to init holding the BLE link and the
D-Bus name while supervise respawned a successor - a duplicate-instance loop.
The fix is `exec`: python replaces the shell, supervise signals python.

The process cmdline stays "python /data/apps/..." - the shared BLE stack is
sourced from its folder by the driver itself, not by an interpreter shim - so
the `pkill -f "python .*/dbus-serialbattery.py ..."` patterns in enable,
disable and restart must keep matching that exact form. `pkill -f` takes POSIX
EXTENDED regex; these tests run every pattern through grep -E, the flavour
pkill uses, against the rendered cmdline.
"""

import os
import re
import subprocess

DRIVER_DIR = os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery")
CMDLINE = "python /data/apps/dbus-serialbattery/dbus-serialbattery.py HumsiENK_Ble AB:80:72:54:E0:B4"


def _pkill_patterns():
    pats = []
    for name in ("enable.sh", "disable.sh", "restart.sh"):
        with open(os.path.join(DRIVER_DIR, name)) as f:
            for line in f:
                m = re.search(r'pkill -f "(python[^"]*dbus-serialbattery\.py[^"]*)"', line)
                if m:
                    pats.append((name, m.group(1)))
    return pats


def _grep_e(pattern, text):
    return subprocess.run(["grep", "-qE", pattern], input=text.encode(), check=False).returncode == 0


def test_every_python_pkill_pattern_matches_the_rendered_cmdline_under_ere():
    pats = _pkill_patterns()
    assert len(pats) == 10, [p for _, p in pats]
    for name, pat in pats:
        generic = pat
        for tail in (" .*_Ble", " /dev/tty.*", " can.*", " vcan.*", " vecan.*", " mqtt.*"):
            generic = generic.replace(tail, " .*")
        assert _grep_e(generic, CMDLINE), f"{name}: {pat!r} does not match the rendered cmdline under grep -E"


def test_generator_execs_python_at_every_site_and_leaves_no_trap_wait_shim():
    with open(os.path.join(DRIVER_DIR, "enable.sh")) as f:
        src = f.read()
    assert src.count('echo "exec python /data/apps/dbus-serialbattery/dbus-serialbattery.py') == 3
    for residue in ("trap 'kill -TERM", 'echo "wait \\$PID"', "EXIT_STATUS", 'dbus-serialbattery.py $2 $3 &"', "BCM_PY", "/data/bcm/python3"):
        assert residue not in src, residue
