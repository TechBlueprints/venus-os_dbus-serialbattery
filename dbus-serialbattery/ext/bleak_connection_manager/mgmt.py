# -*- coding: utf-8 -*-
"""BlueZ management-socket connection parameters.

habluetooth pre-seeds the kernel's per-device connection parameters over
the raw BlueZ management socket (MGMT_OP_LOAD_CONN_PARAM), FAST before a
connect so the parameters apply to the connection being established -
snappy connect and service discovery - and MEDIUM after it succeeds, for
steady-state stability. The supervision timeout is the knob deciding how
long a link survives radio silence before the kernel declares it dead.

This is the minimal slice of that machinery: open the mgmt control channel
(a raw AF_BLUETOOTH/BTPROTO_HCI socket bound to HCI_CHANNEL_CONTROL - the
bind needs ctypes because CPython's socket module cannot express the
channel), and fire LOAD_CONN_PARAM packets without waiting for replies.
Everything degrades: no AF_BLUETOOTH (non-Linux, or a Python built without
Bluetooth), no NET_ADMIN (bind refused), or any send error just turns the
module off for the process. We do not know a peripheral's address type, so
every load carries both the LE-public and LE-random entry - the kernel
matches whichever applies and ignores the other.
"""

import ctypes
import logging
import re
import socket
import struct
import threading

logger = logging.getLogger(__name__)

AF_BLUETOOTH = 31
BTPROTO_HCI = 1
HCI_DEV_NONE = 0xFFFF
HCI_CHANNEL_CONTROL = 3

MGMT_OP_LOAD_CONN_PARAM = 0x0035

BDADDR_LE_PUBLIC = 0x01
BDADDR_LE_RANDOM = 0x02

# habluetooth's parameter sets (const.py:190-206). Intervals are in 1.25ms
# units, supervision timeouts in 10ms units.
FAST_CONN_PARAMS = (0x06, 0x06, 0, 1000)  # 7.5ms interval, 10s supervision
MEDIUM_CONN_PARAMS = (0x07, 0x09, 0, 800)  # 8.75-11.25ms, 8s supervision


class _SockaddrHci(ctypes.Structure):
    _fields_ = [
        ("hci_family", ctypes.c_ushort),
        ("hci_dev", ctypes.c_ushort),
        ("hci_channel", ctypes.c_ushort),
    ]


_lock = threading.Lock()
_sock = None
_available = None


def _pack_bdaddr(address):
    """BLE addresses go over mgmt in reversed byte order."""
    return bytes(reversed(bytes.fromhex(str(address).replace(":", ""))))


def _conn_param_packet(hci_index, address, params):
    """One LOAD_CONN_PARAM packet: header + count + an entry per address
    type (struct mgmt_cp_load_conn_param, BlueZ mgmt-api.txt)."""
    min_interval, max_interval, latency, timeout = params
    bdaddr = _pack_bdaddr(address)
    entries = b"".join(
        struct.pack("<6sBHHHH", bdaddr, addr_type, min_interval, max_interval, latency, timeout)
        for addr_type in (BDADDR_LE_PUBLIC, BDADDR_LE_RANDOM)
    )
    payload = struct.pack("<H", 2) + entries
    return struct.pack("<HHH", MGMT_OP_LOAD_CONN_PARAM, hci_index, len(payload)) + payload


def _open_control_socket():
    """The mgmt control channel, or None: CPython's socket module cannot
    set hci_channel, so the bind goes through libc with a hand-built
    sockaddr_hci."""
    if not hasattr(socket, "AF_BLUETOOTH"):
        return None
    sock = socket.socket(socket.AF_BLUETOOTH, socket.SOCK_RAW, BTPROTO_HCI)
    sock.setblocking(False)
    libc = ctypes.CDLL(None, use_errno=True)
    addr = _SockaddrHci(AF_BLUETOOTH, HCI_DEV_NONE, HCI_CHANNEL_CONTROL)
    if libc.bind(sock.fileno(), ctypes.byref(addr), ctypes.sizeof(addr)) != 0:
        errno = ctypes.get_errno()
        sock.close()
        raise OSError(errno, f"mgmt control bind failed (errno {errno})")
    return sock


def available():
    """Whether the mgmt channel is usable, probed once per process."""
    global _available, _sock
    with _lock:
        if _available is None:
            try:
                _sock = _open_control_socket()
                _available = _sock is not None
            except OSError as e:
                # no NET_ADMIN, no Bluetooth support: run without tuning
                logger.debug(f"mgmt channel unavailable, connection parameters not tuned: {repr(e)}")
                _available = False
        return _available


def load_conn_params(adapter, address, params):
    """Fire a LOAD_CONN_PARAM at the kernel, best-effort.

    Never raises and never waits for the reply; any failure turns tuning
    off for the process rather than perturbing the connect path it serves.
    """
    global _available
    match = re.match(r"hci(\d+)$", str(adapter))
    if not match or not available():
        return False
    packet = _conn_param_packet(int(match.group(1)), address, params)
    with _lock:
        sock = _sock
        if sock is None:
            return False
        try:
            # drain unread mgmt events so the buffer cannot fill up
            while True:
                try:
                    if not sock.recv(1024):
                        break
                except (BlockingIOError, InterruptedError):
                    break
            sock.send(packet)
            return True
        except OSError as e:
            logger.debug(f"mgmt LOAD_CONN_PARAM failed, disabling tuning: {repr(e)}")
            _available = False
            return False


def load_fast(adapter, address):
    """Pre-connect parameters: minimum interval for connect + discovery."""
    return load_conn_params(adapter, address, FAST_CONN_PARAMS)


def load_medium(adapter, address):
    """Post-connect parameters: relaxed interval for steady state."""
    return load_conn_params(adapter, address, MEDIUM_CONN_PARAMS)
