# bleak_connection_manager (vendored)

Managed BLE connection lifecycle for Linux/BlueZ. Used by the optional
`BCMBackend` connection backend in `dbus-serialbattery/utils_ble.py`, selected
with `BLUETOOTH_CONNECTION_BACKEND = BCMBackend`. Not imported at all when the
default `BleakBackend` is used.

## Provenance

| | |
|---|---|
| Upstream project | <https://github.com/TechBlueprints/bleak-connection-manager> |
| Upstream version | 0.1.0 |
| Vendored commit | `8723b82fab95e06bcece21990782ec8cd5fc7fea` (2026-08-13) |
| Upstream path | `src/bleak_connection_manager/` |
| Licence | Apache License 2.0 — see `LICENSE` |

The library wraps
[bleak-retry-connector](https://github.com/Bluetooth-Devices/bleak-retry-connector)
(MIT, Bluetooth Devices Authors), which is itself vendored in this repository at
`dbus-serialbattery/ext/bleak_retry_connector/`. It adds BlueZ workarounds
around `bleak_retry_connector.establish_connection(max_attempts=1)`: cache-first
device resolution, phantom/inactive connection cleanup, adapter rotation,
stuck-state diagnosis and a failure escalation chain.

Both licences are permissive and compatible with this repository's licence.
The Apache-2.0 text above covers the vendored files; the MIT notice for the
bleak-retry-connector lineage is carried with that package's own vendored copy.

## Changes made when vendoring

This is a **subset** of the upstream package, not a verbatim copy. Nothing was
added; the following was removed because it has no consumer in this driver and
no configuration path that could give it one:

* **`validators.py`** (222 lines) — post-connect GATT validators. Never passed
  to `establish_connection(validate_connection=...)` from this driver.
* **`watchdog.py`** (305 lines) — `ConnectionWatchdog` notification-silence
  monitor. `Syncron_Ble` already supervises liveness through its own reconnect
  loop, so the watchdog was never instantiated.
* **The scan-lock subsystem** — `scan_lock.py` (154 lines), `ScanLockConfig` in
  `const.py`, and its call sites in `scanner.py`. Every lock path was gated on
  `scan_lock_config is not None and scan_lock_config.enabled`, and the
  `scan_lock_config` argument was never passed by any caller, so the code was
  unreachable. The `scan_lock_config` parameter was removed from
  `find_device()` and `discover()`; `_poll_cache_while_locked()` was renamed to
  `_poll_bluez_cache()`, since its remaining caller is the external-scan
  fallback rather than a lock-busy wait.
* **The `RESET_ADAPTER` escalation rung** — the enum member, the
  `reset_adapter` / `reset_after` / `reset_cooldown` config fields, the
  `reset_adapter()` and `invalidate_dbus_state()` functions, and the branch in
  `connection.py`. The rung delegates to `bluetooth-auto-recovery`, which is not
  vendored here, so it could only ever return failure. With it gone the ladder
  tops out at `ROTATE_ADAPTER`, which the driver can actually perform.
  `PROFILE_SENSOR` was dropped along with it — without the reset rung it was an
  exact duplicate of the default `EscalationConfig()`.

The tree is otherwise untouched: files carry the upstream formatting, and
`dbus-serialbattery/ext/` is excluded from this repository's flake8 and black
configuration, so no reformatting was applied.

To re-vendor a newer upstream release, copy `src/bleak_connection_manager/*.py`
over this directory and re-apply the removals above.
