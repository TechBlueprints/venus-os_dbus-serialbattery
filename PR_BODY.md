# Rework the BLE connection layer: pluggable backend, reconnect reliability, adapter pinning

## Why

`utils_ble.Syncron_Ble` is the shared BLE plumbing behind the Bluetooth BMS
drivers. It built a `BleakClient`, called `connect()` and `start_notify()`
inline, and retried every second forever if anything went wrong. That is fine
when the radio behaves. It is not fine on a GX device where several BLE
services share a handful of adapters.

This has been running on a Cerbo GX MK2 with two Bluetooth BMS batteries on
Sena UD100 adapters, each battery pinned to its own adapter, through an extended
period of BLE link instability. Every change below exists because a specific
failure was observed there.

The commits are sequenced edits to the same class, so they ship together rather
than as separate PRs.

## What changes

### 1. A connection-backend seam (`354c01a`)

`BleConnectionBackend` defines three operations (`create_client()`,
`establish()` and `release()`) and `BleakBackend` is a verbatim extraction of
the existing bleak path. `Syncron_Ble` now only supervises the link and exchanges
data; how the link comes up is the backend's business.
`BLUETOOTH_CONNECTION_BACKEND` selects the backend by class name and defaults
to `BleakBackend`, so this commit changes no behaviour.

### 2. Reliability fixes on that seam

Each has its own commit, with the failure that motivated it in the message.

**Retry `start_notify` after re-running service discovery** (`db0b9ff`).
`BleakClient.connect()` can return before all GATT characteristics are
resolved, so the immediately following `start_notify()` raises
`BleakCharacteristicNotFoundError` for a characteristic that does exist.
`Syncron_Ble` treated that as a failed connection and reconnected into the same
race, producing an endless reconnect loop against a perfectly reachable battery.

**Drop the per-command event loop in `send_data()`** (`4d36649`). Every BMS
command was wrapped in `asyncio.run()`, constructing and tearing down a whole
event loop just to schedule a coroutine on the BLE thread's existing loop.
Drivers that poll several commands every few seconds paid that continuously.
Measured on the Cerbo: sustained driver CPU 4.7% → 2.7%, together with a
driver-side fix.

**Pace reconnect attempts with a 1 s / 3 s / 6 s ramp** (`39ac4e9`). The flat
1 s retry turned a real outage into continuous hammering: production logs show
single recovery episodes of 220 connect attempts, which wedged the adapter's
discovery state and made recovery take longer than the outage. A session that
held for more than a minute resets the ramp.

**In-process BLE thread rebuild with a generation guard** (`3ff72ff`). The only
remedy for a deadlocked reconnect loop was exiting the process, which takes the
dbus service and all in-RAM battery state with it, makes the inverter raise
`BMS connection lost`, and leaves BlueZ dirty, so repeated restarts degrade the
stack instead of repairing it. `rebuild_ble_thread()` starts a fresh BLE thread
with a new generation number; the old thread retires itself at its next loop
iteration, or is abandoned as the daemon thread it already is.

**Deadlines on backend `establish` / `release`** (`fe7bc54`). Everything the
loop does between iterations is one await into the backend, and BlueZ is
reached over D-Bus. A reply that never arrives parks the loop forever with no
exception, no retry, no log line. This happened: a battery stopped reconnecting
for four hours and the only evidence was the absence of log output. Both calls
are now wrapped in `asyncio.wait_for` with deliberately generous deadlines
(300 s and 30 s). They are a last resort behind the backend's own timeouts,
not a connection timeout.

**Hold-flag support** (`e418e5b`). Some BMS radios get into a degraded state
that only extended quiet cures: each attempt re-occupies the peripheral, so the
harder the driver tries the longer the battery stays unreachable. Granting that
quiet previously meant stopping the driver, which removes its dbus service and
makes DVCC raise alarms across the bank. While `/data/tmp/ble-hold-<mac>`
exists, the loop for that device makes no connection attempts at all while the
driver and its dbus service stay up. A flag containing `auto` is written by an
automatic recovery path and expires by itself after 20 minutes; anything else
is an operator hold and persists until removed.

### 3. Adapter selection and pinning (`29a20b5`)

bleak connects via the system default adapter. On a GX device with several BLE
services that goes wrong two ways: the pre-connect scan cannot start while
another service holds the adapter, producing `org.bluez.Error.InProgress`
storms; and a misbehaving controller drops every LE link on its adapter at once.
Kernel logs show vendor-opcode errors on one adapter coinciding with the
simultaneous loss of every BLE BMS connected through it.

`BLUETOOTH_ADAPTERS` lists the adapters allowed for BLE BMS connections:

```ini
; shared pool: unpinned devices use these in order,
; rotating to the next entry after a failed attempt
BLUETOOTH_ADAPTERS = hci1, hci2

; or one battery per adapter, no rotation and no fallback
BLUETOOTH_ADAPTERS = C8:47:8C:00:00:00@hci1, C8:47:8C:00:00:11@hci2
```

An entry of the form `MAC@hciX` pins that battery to exactly that adapter, so
one failing controller can no longer take the other batteries down with it.
Plain `hciX` entries form the shared pool. An empty list (the default) keeps
the previous default-adapter behaviour. Note that the pool is built from the
plain entries only, because a pinned MAC is not an adapter name and must never
reach bleak.

Both options are documented in `config.default.ini`.

## Not included

A managed connection backend, with cache-first device resolution, per-adapter
escalation and phantom-connection cleanup, is built on this seam and comes as a
separate PR that depends on this one. This PR ships only the seam and the direct
bleak backend, so it can be reviewed on its own.

An LE link supervision-timeout override was tried and dismissed. Measured
side by side, the long-timeout arm churned roughly 3.5 times worse than the
kernel default, so none of it is here.

## Testing

Live: a Cerbo GX MK2 with two Bluetooth BMS batteries, each pinned to its own
Sena UD100 adapter. Every fix above is the response to a failure observed on
that system.

Automated: `tests/test_utils_ble.py` covers what can be exercised honestly
without a radio: the `BLUETOOTH_ADAPTERS` parser (pool versus pinned entries,
`MAC@hciX` splitting, whitespace and case handling, malformed entries, order
preservation), pinned-device lookup, hold-flag path derivation, and the backend
selection plumbing including the check that the shipped `config.default.ini`
default resolves to a real backend instead of silently falling back. `bleak` is
not installed on non-Venus machines, so the test module registers a minimal
module stub and loads `utils_ble` under a private name.

Deliberately untested: the connect/notify/disconnect paths themselves. Testing
them without hardware would mean asserting that mocks were called in a
particular order, which would pin the implementation without proving anything
about behaviour on real BlueZ.

Verification: `py_compile`, `flake8 --max-line-length=160` and `black --check`
clean at every commit; the full `tests/` run at every commit produces exactly
the same set of failing node IDs as `upstream/master` (46 pre-existing
failures: 18 in `tests/test_utils.py`, 28 in `tests/bms/test_lltjbd_up16s.py`).
