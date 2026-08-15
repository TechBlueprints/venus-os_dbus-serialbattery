# Opt-in managed BLE connection backend (`BCMBackend`)

Adds a second connection backend behind the `BLUETOOTH_CONNECTION_BACKEND` seam,
built on a vendored `bleak_connection_manager`. It is **opt-in and the default is
unchanged**: without `BLUETOOTH_CONNECTION_BACKEND = BCMBackend` in `config.ini`,
nothing here is imported and the connection path is byte-for-byte the same as
before.

## Why you would want it

`BleakBackend` connects straight through bleak: scan, connect, `start_notify`.
That is fine when the driver has an adapter to itself. It is not fine on a GX
device where several BLE services share one or two adapters. There, the failure
modes are not "the connection dropped" but:

* `org.bluez.Error.InProgress`, because another service holds the adapter's
  discovery session and our scan cannot start;
* phantom connections, where BlueZ reports `Connected=True` with no live link and the
  slot stays occupied;
* stale cache entries that pin a battery to whichever adapter last scanned it,
  quietly defeating a `MAC@hciX` pin;
* connects that stall inside D-Bus and never return.

`BCMBackend` handles those:

* **Resolution without scanning.** `Adapter1.ConnectDevice` on the preferred
  adapter first, then a cache-first find, then `ConnectDevice` on the remaining
  allowed adapters. `ConnectDevice` does not need the adapter's scan slot, so it
  works while another service is scanning.
* **Retries with escalation.** Per-adapter failure tracking, then diagnose stuck
  state, clear stale BlueZ state, rotate adapter.
* **Phantom and inactive connection cleanup** before each attempt.
* **Per-device adapter handling.** `BLUETOOTH_ADAPTERS` is a hard allow-list. A
  device found cached on a disallowed adapter is *evicted* rather than connected
  through. This is what makes per-battery pinning actually stick when a sibling
  instance keeps re-caching the device on a shared adapter within seconds.
  Unpinned devices get a stable per-address rotation of the pool so several
  batteries spread out instead of all crowding the first adapter.

Every D-Bus await in the new paths carries a deadline. An unguarded one parks the
reconnect loop with no log line at all.

Production context for all of this: a Cerbo GX MK2 with two HumsiENK BLE
batteries on Sena UD100 adapters, each battery pinned to its own adapter.

`BCMBackend` needs `bluetoothd -E` for the no-scan connect path; without it, it
falls back to scanning. If the backend cannot be constructed at all on a given
system, `get_ble_backend()` logs an error and returns `BleakBackend`, because that
call happens inside `Syncron_Ble.__init__`, and letting an `ImportError` escape there
means no dbus service rather than a degraded one.

## Two loop-side resilience features

**Half-connect oscillation breaker.** `ConnectDevice` can succeed at the HCI level
while the handoff to a usable notification channel fails, and each attempt
re-occupies the peripheral, so it never advertises long enough for scan-based
resolution to repair the handoff. Left alone this oscillates. Observed three times
in production, wedging one battery for 10 to 26 minutes each time. After 3
consecutive half-connects the backend stops connecting blind: it pauses 10 s so the
device can advertise, and resolves by scan instead of `ConnectDevice`. A session
that reaches `start_notify` clears the count.

**Auto-hold writer.** When the breaker engages 10 times inside 5 minutes, the short
pauses are demonstrably not curing it. The BMS radio is in a degraded state that
only extended quiet clears. Twice in production a *manual* hold flag ended an
identical episode. This automates that playbook: write the per-device hold flag
(the reader is already on the BLE-connection-layer branch), which stands the
reconnect loop down for 20 minutes. The flag is written as an automatic hold, so it
releases itself; an operator-written hold still persists until removed. The driver,
its dbus service and its published data stay up throughout, which is the whole
point. Killing the process instead makes DVCC see the service vanish and raises
alarms across the bank.

One small refactor came with this: the reader's auto-expiry decision moved out of
`Syncron_Ble.async_main` into `ble_hold_expired()`, so writer and reader agree on
the marker and the expiry by construction instead of via two copies of the same
literal, and so both sides are testable without spinning up a BLE thread. Behavior
is unchanged.

## Vendoring: licence and provenance

`dbus-serialbattery/ext/bleak_connection_manager/` is vendored from
<https://github.com/TechBlueprints/bleak-connection-manager> v0.1.0 at commit
`8723b82`, Apache-2.0. `LICENSE` and `README.md` in that directory carry the
licence text and the full provenance/modification record, laid out the way
`ext/velib_python` and `ext/venus-os_overlay-fs` already are.

Vendoring rather than depending: Venus OS has no package manager for Python
dependencies on the target, and this repository already vendors its entire BLE
stack (`ext/bleak`, `ext/bleak_retry_connector`, `ext/bluetooth_adapters`,
`ext/aiobmsble`). The library wraps `bleak-retry-connector` (MIT, Bluetooth Devices
Authors), which is itself already vendored at `ext/bleak_retry_connector`. Both
licences are permissive.

`ext/` is excluded from this repository's flake8 and black configuration, so the
remaining upstream files were not reformatted.

## What was pruned from the vendored library, and why

This is a subset of upstream, not a verbatim copy. Everything below had **no
consumer in this driver and no configuration path that could give it one**:

| Removed | Lines | Why |
|---|---|---|
| `validators.py` | 222 | Post-connect GATT validators. Never passed as `establish_connection(validate_connection=...)`. |
| `watchdog.py` | 305 | `ConnectionWatchdog` notification-silence monitor. Never instantiated, `Syncron_Ble`'s own reconnect loop already supervises liveness. |
| scan-lock subsystem | 154 + `ScanLockConfig` in `const.py` + ~30 call sites in `scanner.py` | Every path was gated on `scan_lock_config is not None and scan_lock_config.enabled`, and no caller ever passed a `scan_lock_config`. Unreachable code. |
| `RESET_ADAPTER` escalation rung | enum member, `reset_adapter`/`reset_after`/`reset_cooldown` config fields, `reset_adapter()`, `invalidate_dbus_state()`, the branch in `connection.py` | The rung delegates to `bluetooth-auto-recovery`, which is not vendored, so it could only ever return failure. The ladder now tops out at `ROTATE_ADAPTER`, which the driver can actually perform. `PROFILE_SENSOR` went with it, without the reset rung it was an exact duplicate of the default `EscalationConfig()`. |

Two follow-on renames: `find_device()` and `discover()` lost their
`scan_lock_config` parameter, and `_poll_cache_while_locked()` became
`_poll_bluez_cache()` because its one remaining caller is the external-scan
fallback, not a lock-busy wait.

`grep` confirms nothing references any of it, and the package still imports
cleanly with all 41 declared exports resolving.

There is no supervision-timeout code here: `BLUETOOTH_SUPERVISION_TIMEOUT` /
`apply_supervision_timeout()` is a dismissed feature and no call, import or
reference to it survives.

## Dependency and merge order

This PR is **stacked on #511** and must merge after it. It depends on that
branch for:

* the `BleConnectionBackend` seam and `BLUETOOTH_CONNECTION_BACKEND` selection;
* `parse_adapter_entries()` / `adapters_for()` and the `BLUETOOTH_ADAPTERS`
  allow-list;
* the per-device hold flag **reader** in the reconnect loop, since this PR only
  adds the writer;
* the outer establish/release deadlines and the 1 s / 3 s / 6 s reconnect ramp.

Merge order: #511, then this PR.

## Testing

`python3 -m py_compile`, `python3 -m flake8 --max-line-length=160` and
`python3 -m black --check` are clean across the repository (`ext/` is excluded from
both linters by the repo's existing config, matching how the other vendored trees
are treated).

Full `tests/` run compared against a baseline taken at the base branch
`feat/ble-connection-layer`, by sorted FAILED/ERROR node-ID list:

| | base branch | this branch |
|---|---|---|
| failed | 18 | 18 |
| errors | 28 | 28 |
| passed | 91 | 107 |
| skipped | 0 | 1 |

The 46 failing/erroring node IDs are **identical sets** (18 in `tests/test_utils.py`,
28 in `tests/bms/test_lltjbd_up16s.py`), all pre-existing, none introduced here.
The one skip is the BCM-construction test, which skips where
`bleak_connection_manager` is not importable (no BlueZ on the CI/dev machine).

New tests in `tests/test_utils_ble.py` cover, without hardware or a real bleak:

* **Backend registration and selection**: `BCMBackend` is registered and reachable
  by name; a backend whose dependency is missing degrades to `BleakBackend` instead
  of raising; where the library *does* import, selection returns a real
  `BCMBackend`.
* **Per-device adapter selection**: a pin wins outright and the pool is ignored;
  unpinned devices get a per-address rotation that is stable and is still a
  permutation of the whole pool (preference order only, no adapter is dropped).
* **Breaker counting and threshold**: trips only at the configured count, and the
  count is of *consecutive* failures.
* **Auto-hold writer**: the flag is written only once the engagement threshold is
  reached inside the window; a slow trickle of engagements ages out and never
  holds; a written hold starts a fresh window; the flag lands at the expected path
  with the expected marker content; expiry round-trips in both directions; an
  operator hold never self-expires; an empty flag counts as an operator hold; an
  unwritable flag directory is reported rather than raised. All using a `tmp_path`
  flag directory.
* **BlueZ path helpers** the allow-list check depends on.

Deliberately **not** tested: anything that needs a radio, a live BlueZ or D-Bus.
`_connect_device_no_scan()`, `_resolve_device()`, `establish()`, `release()` and the
vendored library's own internals. Driving those through mocks would only assert
that the mocks were called, which proves nothing about behavior on a Cerbo. Those
paths were exercised on the production Cerbo GX MK2 instead.
