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

## Correction — no reboot inside the attribution window (added 2026-08-24)

`running monitor` described the churn collapse as arriving "alongside three other
changes **plus a reboot**". The reboot is real but falls ~2h45m *outside* the window,
and the record's own data proves it:

- Incident 8, "UNATTRIBUTED HARD RESET at 22:34:21 UTC"
- the monitor's health check at 22:32:11 recorded `UPTIME_S=94234` (~26 h)

The before/after measurements were at **79,200 s** and **84,513 s** uptime — the same
counter, increasing monotonically across the interval, which is only possible if the box
did not restart between them. 94,234 s at 22:32 continues the same unbroken count. So:
before 18:45–19:10, after 19:50, hard reset at 22:34, well after both.

**Why this matters.** A reboot inside the window would be the most likely single
explanation for a churn collapse and would swamp the other three candidates — a reader
would reasonably conclude the recovery was just a restart and that none of the fixes
earned anything. The caveat above stays **three-way**. It does not become four-way.

The uptime counter is a good instrument here precisely because it is independent of
everything else being measured — the same property that made the cumulative-connection
cross-check the right way to rule out a sampling artifact on the churn figure.

## Refinement — the "before" state already contained one of the three fixes

From `running monitor`, 2026-08-24, with the arithmetic re-checked here independently.

**Two different windows are conflated by the labels above.** "18:45–19:10" belongs to the
load/CPU rows. The cumulative-connection cross-check — the evidence that churn genuinely
decayed rather than being a sampling artifact — spans a different interval, recovered from
the uptime counter:

| Sample | uptime | wall clock |
| --- | --- | --- |
| cross-check start | 79,200 s | 2026-08-23 18:21:37 UTC |
| cross-check end / after | 84,513 s | 19:50:10 UTC |
| health check | 94,234 s | 22:32:11 UTC |
| hard reset (Incident 8) | — | 22:34:21 UTC, **2.74 h after** the after-sample |

Differences check out: 84,513 − 79,200 = 5,313 s = 1h 28m 33s, and 94,234 − 84,513 =
9,721 s = 2h 42m 1s. The chain self-validates — the after-sample reconstructs to 19:50:10
against a recorded 19:50, which is only possible on an unbroken counter.

**The consequence.** The dbus-off-the-BLE-thread use-after-free fix deployed at **18:01:35**,
with the old process confirmed dead at 18:02:24. So:

- the cross-check window opens at 18:21:37 — **20 minutes after** that fix went live
- the load/CPU before-sample at 18:45 is **43m 25s** post-restart

Every "before" number in this file was therefore measured with the use-after-free fix
already running, and the box was still at load 7.39 with churn ~1.40/sec at that point.
**A change already in effect during the "before" state cannot explain the before/after
delta.**

**This does NOT narrow attribution to two.** Establishing that would need deploy timestamps
for 3c32d1e and for the supervision-spin removal, which nobody has, and settling time after
a driver restart is a real effect nobody has bounded. It is a lead with the arithmetic
attached, recorded while the window is still reconstructible. The caveat above stays
**three-way**, with an open question about whether one of the three was already spent.
