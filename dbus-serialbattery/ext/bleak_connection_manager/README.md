# bleak_connection_manager (vendored)

The "bleak catcher": a per-process BLE connection layer injected underneath
every `from bleak import BleakClient` via module rebinding — the same
mechanism habluetooth uses to sit underneath every Home Assistant BLE
integration. Installed (opt-in) by `utils_ble_manager.py` when
`BLUETOOTH_CONNECTION_MANAGER = True`; not imported at all otherwise.

## Provenance

| | |
|---|---|
| Upstream project | <https://github.com/TechBlueprints/bleak-connection-manager> |
| Upstream version | 2.0.0.dev0 |
| Vendored commit | `bc31d9a` (2026-08-21, `main`) |
| Upstream path | `src/bleak_connection_manager/` |
| Licence | Apache License 2.0 — see `LICENSE` |

This is the v2 rewrite of the library. The v1 codebase (the BlueZ
connection-lifecycle manager wrapped around `establish_connection`, briefly
proposed for this driver as `BCMBackend` in upstream PR #512 and withdrawn)
lives on the upstream `v1-main` branch; the two share no code.

Not vendored from upstream: its test suite, its own `ext/` (`bt_claims.py`,
the bt-claims reference library that `claims.py` reimplements, and a copy of
dbus-fast for consumers whose system copy is too old for current bleak -
this repository vendors its own BLE dependency chain under `ext/` already).

## Contents

The complete `src/bleak_connection_manager/` package, unmodified:

* **`claims.py`** — the bt-claims file convention under `/run/bt-claims`
  (stdlib only, no bleak): heartbeated claim files coordinating adapter use
  across processes. Kinds: `hciN.scan` (hard, exclusive),
  `hciN.use.<owner>[.<qualifier>]` (soft, ranks placement),
  `hciN.link.<k>` (numbered exclusive link slots).
* **`catcher.py`** — the process-wide rebinding layer: `BLEConnection`
  (drop-in `BleakClient` that picks its adapter and takes claims at
  `connect()`), `BLEScanner` (adapter-bound, hard-claiming, with
  habluetooth's silence watchdog; opt-in), habluetooth-parity connect
  scoring for unpinned devices, per-adapter link slots and
  `OutOfConnectionSlotsError`.
* **`mgmt.py`** — habluetooth's fast-then-medium connection parameters
  loaded over the BlueZ management socket; degrades to a no-op without
  `AF_BLUETOOTH`/NET_ADMIN.
* **`recovery.py`** — claims-gated adapter hardware reset. Depends on the
  optional `bluetooth-auto-recovery` package, which is **not** vendored
  here, so a reset degrades to a logged no-op; the gating and the scanner
  watchdog's restart tier still work.

The library wraps whoever drives the client — it routes, it never retries.
Retry semantics stay with
[bleak-retry-connector](https://github.com/Bluetooth-Devices/bleak-retry-connector),
vendored separately at `ext/bleak_retry_connector/` and already used by the
aiobmsble drivers and the `BleakRetryBackend` connection backend.
