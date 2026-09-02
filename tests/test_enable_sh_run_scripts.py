# -*- coding: utf-8 -*-
"""Tests for the run scripts that enable.sh generates for the daemontools services.

The run script's process is the one supervise watches. If it forks python into
the background and waits, a TERM that the shell forwards but python outlives
leaves the shell exiting, supervise respawning, and the old python reparented to
init while still holding its D-Bus name. Using exec makes python the run process
itself, so there is nothing to fall out of sync.

The generator is shell, so these tests run the emitting block and read what it
writes, rather than matching the source that writes it.
"""

import os
import re
import subprocess

import pytest

ENABLE_SH = os.path.join(os.path.dirname(__file__), "..", "dbus-serialbattery", "enable.sh")

# redirect target -> positional arguments the block is emitted with
SERVICES = {
    "/service/dbus-blebattery.$1/run": ["0", "Jkbms_Ble", "C8:47:8C:E4:9F:2A"],
    "/service/dbus-canbattery.$1/run": ["can0"],
    "/service/dbus-mqttbattery/run": ["mqtt-battery"],
}


def _extract_block(target):
    """Return the shell group command that writes the run script for `target`.

    The generator emits each run script as `{ ... } > "<target>"`. Dropping the
    redirect leaves a group command that writes the same bytes to stdout.
    """
    with open(ENABLE_SH, encoding="utf-8") as enable_file:
        lines = enable_file.read().splitlines()

    closing = f'}} > "{target}"'
    end = next(i for i, line in enumerate(lines) if line.strip() == closing)
    start = next(i for i in range(end, -1, -1) if lines[i].strip() == "{")

    return "\n".join(lines[start:end] + ["}"])


def _render(target):
    """Run the emitting block and return the run script it produces."""
    result = subprocess.run(
        ["sh", "-s"] + SERVICES[target],
        input=_extract_block(target),
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


@pytest.mark.parametrize("target", sorted(SERVICES))
def test_run_script_execs_python(target):
    """python must replace the shell, so it becomes the process supervise watches."""
    rendered = _render(target)

    assert re.search(r"^exec python .*dbus-serialbattery\.py", rendered, re.MULTILINE)


@pytest.mark.parametrize("target", sorted(SERVICES))
def test_run_script_does_not_background_python(target):
    """A trailing & is what creates the second process this change removes."""
    rendered = _render(target)

    python_lines = [line for line in rendered.splitlines() if "dbus-serialbattery.py" in line]
    assert python_lines
    for line in python_lines:
        assert not line.rstrip().endswith("&")


@pytest.mark.parametrize("target", sorted(SERVICES))
@pytest.mark.parametrize("shim", ["trap ", "PID=$!", "wait $PID", "EXIT_STATUS"])
def test_run_script_has_no_signal_forwarding_shim(target, shim):
    """No part of the fork-and-wait shim may survive; each piece is a failure mode."""
    assert shim not in _render(target)


@pytest.mark.parametrize("target", sorted(SERVICES))
def test_run_script_keeps_stderr_redirect(target):
    """`exec 2>&1` is the redirect-only form and must not be dropped with the rest."""
    rendered = _render(target)

    assert "exec 2>&1" in rendered


def test_ble_run_script_keeps_the_disconnect_preamble():
    """The preamble runs before python and survives the exec, so it must stay."""
    rendered = _render("/service/dbus-blebattery.$1/run")

    lines = rendered.splitlines()
    disconnect = next(i for i, line in enumerate(lines) if line.startswith("bluetoothctl disconnect"))
    python = next(i for i, line in enumerate(lines) if "dbus-serialbattery.py" in line)
    assert disconnect < python


def test_run_script_is_a_valid_shell_script():
    """A generated script that does not parse would fail only at service start."""
    for target in SERVICES:
        rendered = _render(target)
        assert rendered.startswith("#!/bin/sh")
        subprocess.run(["sh", "-n"], input=rendered, text=True, check=True)
