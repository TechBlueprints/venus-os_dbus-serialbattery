"""HCI connection inspection and control via ``hcitool``.

Provides ground-truth BLE connection state by querying the Linux HCI
layer through the ``hcitool`` command-line utility (part of BlueZ).
This fills a critical gap: D-Bus (BlueZ layer 2) can disagree with
HCI (layer 1), and detecting that mismatch is the only way to find
phantom connections and stale handles.

At import time the module probes for ``hcitool`` on ``$PATH``.  If it
is not found, all functions gracefully degrade — returning empty results
or ``False`` — and phantom/orphan detection in the diagnostics module
is skipped.

Key functions:

- :func:`get_connections` — list active HCI connections on an adapter
  (equivalent to ``hcitool con``).
- :func:`disconnect_handle` — disconnect a specific HCI handle
  (equivalent to ``hcitool ledc <handle>``).
- :func:`cancel_le_connect` — cancel a pending LE Create Connection
  (equivalent to ``hcitool cmd 0x08 0x000E``).
- :func:`find_connection_by_address` — find a connection by BLE MAC.
- :func:`hci_available` — check if ``hcitool`` is present and usable.

**Requires Linux and ``hcitool`` on PATH** (typically from the
``bluez`` package).  All functions degrade gracefully when these
conditions are not met.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
from dataclasses import dataclass

from .const import IS_LINUX

_LOGGER = logging.getLogger(__name__)

# ── hcitool availability probe ─────────────────────────────────────

_HCITOOL: str | None = shutil.which("hcitool") if IS_LINUX else None

if IS_LINUX and _HCITOOL is None:
    _LOGGER.debug("hcitool not found on PATH; HCI diagnostics disabled")


# ── HCI disconnect reason codes ───────────────────────────────────
# From Bluetooth Core Spec Vol 1 Part F (Error Codes).

HCI_DISCONNECT_REASONS: dict[int, str] = {
    0x05: "Authentication Failure",
    0x06: "PIN or Key Missing",
    0x07: "Memory Capacity Exceeded",
    0x08: "Connection Timeout",
    0x09: "Connection Limit Exceeded",
    0x0A: "Synchronous Connection Limit Exceeded",
    0x0C: "Command Disallowed",
    0x0D: "Rejected Limited Resources",
    0x0E: "Rejected Security Reasons",
    0x0F: "Rejected Unacceptable BD_ADDR",
    0x10: "Connection Accept Timeout Exceeded",
    0x13: "Remote User Terminated Connection",
    0x14: "Remote Device Terminated Low Resources",
    0x15: "Remote Device Terminated Power Off",
    0x16: "Connection Terminated by Local Host",
    0x1A: "Unsupported Remote Feature",
    0x1F: "Unspecified Error",
    0x22: "LMP/LL Response Timeout",
    0x28: "Instant Passed",
    0x2A: "Different Transaction Collision",
    0x3B: "Unacceptable Connection Parameters",
    0x3E: "Connection Failed to be Established",
}


def disconnect_reason_str(reason: int) -> str:
    """Return a human-readable string for an HCI disconnect reason code."""
    name = HCI_DISCONNECT_REASONS.get(reason)
    if name:
        return f"{name} (0x{reason:02X})"
    return f"Unknown (0x{reason:02X})"


# ── Public data model ──────────────────────────────────────────────


@dataclass(frozen=True)
class HciConnection:
    """A single active HCI connection on an adapter.

    Attributes
    ----------
    handle:
        The HCI connection handle (1-4095).
    address:
        The remote device's Bluetooth address (e.g. ``"AA:BB:CC:DD:EE:FF"``).
    link_type:
        The link type (``0x80`` for LE, ``0x01`` for ACL, etc.).
    link_type_name:
        Human-readable link type (``"LE"``, ``"ACL"``, etc.).
    outgoing:
        ``True`` if this host initiated the connection (central role).
    state:
        HCI connection state code.
    link_mode:
        HCI link mode flags (0 when unavailable).
    adapter:
        The adapter name (e.g. ``"hci0"``).
    """

    handle: int
    address: str
    link_type: int
    link_type_name: str
    outgoing: bool
    state: int
    link_mode: int
    adapter: str


# ── hcitool output parsing ─────────────────────────────────────────

# Example output of `hcitool -i hci0 con`:
#   Connections:
#   	< LE AA:BB:CC:DD:EE:FF handle 64 state 1 lm MASTER
#   	> ACL 11:22:33:44:55:66 handle 42 state 1 lm SLAVE
#
# Fields: direction(</>)  type  address  "handle"  N  "state"  N  "lm"  mode

_CON_RE = re.compile(
    r"^\s*([<>])\s+"                  # direction
    r"(\w+)\s+"                       # link type (LE, ACL, SCO, eSCO)
    r"([0-9A-Fa-f:]{17})\s+"         # BD_ADDR
    r"handle\s+(\d+)\s+"             # handle
    r"state\s+(\d+)"                  # state
    r"(?:\s+lm\s+(\S+))?",           # link mode (optional)
    re.MULTILINE,
)

_LINK_TYPE_MAP: dict[str, int] = {
    "LE": 0x80,
    "ACL": 0x01,
    "SCO": 0x00,
    "eSCO": 0x02,
}


def _parse_hcitool_con(output: str, adapter: str) -> list[HciConnection]:
    """Parse the output of ``hcitool con`` into HciConnection objects."""
    connections: list[HciConnection] = []
    for m in _CON_RE.finditer(output):
        direction, link_name, address, handle_s, state_s, lm = m.groups()
        link_type = _LINK_TYPE_MAP.get(link_name, 0xFF)
        connections.append(
            HciConnection(
                handle=int(handle_s),
                address=address.upper(),
                link_type=link_type,
                link_type_name=link_name,
                outgoing=(direction == "<"),
                state=int(state_s),
                link_mode=0,
                adapter=adapter,
            )
        )
    return connections


# ── Core functions ─────────────────────────────────────────────────


def hci_available(adapter: str = "hci0") -> bool:
    """Test whether ``hcitool`` is usable for HCI queries.

    Returns ``True`` if ``hcitool`` is on PATH and can query the
    given adapter.  A ``False`` result means :func:`get_connections`
    will always return ``[]``, so callers should not rely on HCI for
    diagnostics.
    """
    if _HCITOOL is None:
        return False
    try:
        result = subprocess.run(
            [_HCITOOL, "-i", adapter, "con"],
            capture_output=True,
            timeout=5.0,
        )
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def get_connections(adapter: str = "hci0") -> list[HciConnection]:
    """List active HCI connections on an adapter.

    Equivalent to ``hcitool -i <adapter> con``.

    Returns a list of :class:`HciConnection` objects, or an empty list
    if ``hcitool`` is not available or the query fails.
    """
    if _HCITOOL is None:
        return []

    try:
        result = subprocess.run(
            [_HCITOOL, "-i", adapter, "con"],
            capture_output=True,
            text=True,
            timeout=5.0,
        )
        if result.returncode != 0:
            _LOGGER.debug(
                "hcitool con failed on %s: %s",
                adapter,
                result.stderr.strip(),
            )
            return []
        return _parse_hcitool_con(result.stdout, adapter)
    except subprocess.TimeoutExpired:
        _LOGGER.debug("hcitool con timed out on %s", adapter)
        return []
    except OSError as ex:
        _LOGGER.debug("hcitool con failed on %s: %s", adapter, ex)
        return []


def find_connection_by_address(
    address: str,
    adapter: str | None = None,
    adapters: list[str] | None = None,
) -> HciConnection | None:
    """Find an active HCI connection by BLE MAC address.

    Searches the specified adapter(s) for a connection matching the
    given address.

    Parameters
    ----------
    address:
        The BLE MAC address to search for (case-insensitive).
    adapter:
        A single adapter to check.
    adapters:
        Multiple adapters to check.

    Returns the matching :class:`HciConnection`, or ``None`` if not
    found.
    """
    target = address.upper()

    if adapter is not None:
        search_adapters = [adapter]
    elif adapters is not None:
        search_adapters = adapters
    else:
        search_adapters = ["hci0"]

    for adap in search_adapters:
        for conn in get_connections(adap):
            if conn.address.upper() == target:
                return conn

    return None


def disconnect_handle(
    adapter: str,
    handle: int,
    reason: int = 0x13,
) -> bool:
    """Disconnect a specific HCI connection handle.

    Equivalent to ``hcitool -i <adapter> ledc <handle>``.

    Returns ``True`` if the command was sent successfully.
    """
    if _HCITOOL is None:
        return False

    try:
        result = subprocess.run(
            [_HCITOOL, "-i", adapter, "ledc", str(handle)],
            capture_output=True,
            text=True,
            timeout=5.0,
        )
        if result.returncode == 0:
            _LOGGER.info(
                "Sent HCI disconnect for handle %d on %s", handle, adapter,
            )
            return True
        _LOGGER.debug(
            "hcitool ledc failed on %s handle %d: %s",
            adapter,
            handle,
            result.stderr.strip(),
        )
        return False
    except (OSError, subprocess.TimeoutExpired) as ex:
        _LOGGER.debug(
            "hcitool ledc failed on %s handle %d: %s", adapter, handle, ex,
        )
        return False


def cancel_le_connect(adapter: str = "hci0") -> bool:
    """Cancel a pending LE Create Connection on an adapter.

    Equivalent to ``hcitool -i <adapter> cmd 0x08 0x000E``
    (HCI LE Create Connection Cancel).

    Safe to call even if no connection is pending — the controller
    returns ``Command Disallowed`` which we silently ignore.

    Returns ``True`` if the command was sent successfully.
    """
    if _HCITOOL is None:
        return False

    try:
        result = subprocess.run(
            [_HCITOOL, "-i", adapter, "cmd", "0x08", "0x000E"],
            capture_output=True,
            text=True,
            timeout=5.0,
        )
        if result.returncode == 0:
            _LOGGER.info("Sent LE Create Connection Cancel on %s", adapter)
            return True
        _LOGGER.debug(
            "hcitool cmd cancel failed on %s: %s",
            adapter,
            result.stderr.strip(),
        )
        return False
    except (OSError, subprocess.TimeoutExpired) as ex:
        _LOGGER.debug(
            "hcitool cmd cancel failed on %s: %s", adapter, ex,
        )
        return False


def disconnect_by_address(
    address: str,
    adapter: str | None = None,
    adapters: list[str] | None = None,
) -> bool:
    """Find and disconnect an HCI connection by BLE MAC address.

    Combines :func:`find_connection_by_address` and
    :func:`disconnect_handle` into a single call.

    Returns ``True`` if a connection was found and the disconnect
    command was sent, ``False`` otherwise.
    """
    conn = find_connection_by_address(
        address, adapter=adapter, adapters=adapters,
    )
    if conn is None:
        return False

    _LOGGER.info(
        "Found stale HCI connection for %s on %s (handle=%d, type=%s), "
        "sending disconnect",
        conn.address,
        conn.adapter,
        conn.handle,
        conn.link_type_name,
    )
    return disconnect_handle(conn.adapter, conn.handle)
