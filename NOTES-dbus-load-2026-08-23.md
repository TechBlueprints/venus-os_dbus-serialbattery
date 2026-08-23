# Prod Cerbo D-Bus load incident, 2026-08-23 — reference material

**This is evidence about the box, not about this PR.** It is deliberately NOT part of
PR #510's description or diff. See "How #510 should cite this" at the bottom.

## Provenance

Measured by the `running monitor` session on the prod Airstream Cerbo, 2026-08-23,
except the block marked *independent*, measured by the `serialbattery-bcmv2`
integration session. Where the two overlap they agree. This session (the #510 fork)
measured none of it and must not present it in the first person.

## Before — 18:45–19:10 UTC

| Metric | Value | |
| --- | --- | --- |
| load average | 7.39 / 6.73 / 5.70 | |
| total bus traffic | 77 msg/s | **low — not the constraint** |
| new D-Bus connections | ~1.40 /sec | the actual cost |
| AddMatch / RemoveMatch | 15 / 15 per 10 s | |
| ListNames | 21 per 10 s | |
| dbus-daemon CPU | 16.3 % | top consumer, 30 s window |
| bluetoothd CPU | 15.3 % | |
| dbus-ble-sensors-py CPU | 9.2 % | |
| serialbattery drivers CPU | 6.2 % and 6.1 % | |
| CPU idle | ~60 %, almost no D-state | |
| run queue | ~3.3 runnable/sweep on 2 cores, ~2400 ctx switches/s | |

## After — 19:50 UTC

| Metric | Value | |
| --- | --- | --- |
| load average | 1.78 / 3.56 / 4.46 | |
| new D-Bus connections | 0.13 /sec | ~11x lower |
| dbus-daemon CPU | 10.6 % | |
| bluetoothd CPU | 5.2 % | ~3x lower |

## Independent cross-check — 19:48 UTC (serialbattery-bcmv2)

| Metric | Value |
| --- | --- |
| load average | 2.07 / 4.04 / 4.67 |
| dbus-daemon CPU | 4 % |
| bluetoothd | absent from top 5 |

## Method — connection churn

`dbus-monitor` on `NameOwnerChanged`, counting distinct `:1.N` connection ids in a
fixed window. Cross-checked against the cumulative connection count so the figure is
not a sampling artifact:

- 19,166 connections at 79,200 s uptime → 0.242/sec lifetime average
- 22,929 connections at 84,513 s uptime
- over that 5,313 s interval: 0.708/sec average, against 0.13/sec instantaneous after

The churn genuinely decayed. It was not a window effect.

## Why it mattered

`/etc/watchdog.conf` on prod sets `max-load-5 = 10` and `max-load-15 = 6`. A trip is a
**hard reset of the GX device**. The 15-minute figure reached **5.85** during the
incident — 0.15 from a hard reset of an occupied RV's power system.

## The caveat that must travel with these numbers

Three changes landed in the same window with no clean control between them:

1. sensors-py traceback rate-limit (3c32d1e)
2. the dbus-off-the-BLE-thread use-after-free fix
3. two 10 Hz supervision-spin removals

**Nobody can attribute the recovery**, and all three sessions have declined to claim
it. Note further that a crash-looping driver reconnecting BLE would itself produce
sequential short-lived connections, so the crash fix has a plausible mechanism for a
large share of the churn collapse.

## How #510 should cite this

The load incident was **connection setup churn**, not message volume — SASL auth on a
new connection costs far more than routing a signal, and 77 msg/s of total bus traffic
was never the binding constraint.

> Connection churn was the binding constraint that night, and this PR does not
> address it.

#510 reduces bus **work** (fewer signals per publish cycle, fewer redundant
republishes, fewer introspection walks for ESS settings). That is worth doing on its
own terms and is measured in messages. It is not measured load relief, and no
before/after isolating it exists.

**Do not** reconcile the "~57 msg / 30 s → ~16 / 30 s" figure in the PR body with the
77 msg/s total here. They were taken in different windows on different trees; pairing
them manufactures a before/after nobody measured.

## Unrelated correction worth carrying

The log-volume figure of **206 MiB/day** that circulated earlier is **wrong** — its own
intermediates did not reconcile. Measured value: **703 B/s = 57 MiB/day**.

The method error is instructive: a two-sample stat of `current` misses everything
written into rotated files, and a larger second sample does not rule out a rotation,
because `current` restarts at zero.
