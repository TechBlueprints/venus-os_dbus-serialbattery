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

## Deploy log findings (running monitor, 2026-08-24)

Process starts recovered from prod's pre-reboot blebattery startup banners.

    pack .0 (RS)  17:18:57  17:32:21  17:49:09  18:04:20  19:40:40  21:13:22
    pack .1 (CS)  16:43:59  17:25:33  17:29:00  17:39:12  18:04:46  19:40:42  21:13:45

### The lead above holds for BOTH packs — not staggered

The deploy restart is 18:04:20 / 18:04:46, **26 seconds apart**. One event. Both processes
were live 17 minutes before the cross-check window opens (18:21:37) and ~40 minutes before
the load/CPU before-sample. So the "before" state contained the use-after-free fix on both
packs; the argument does not fall back to a single pack.

**Figure correction:** the fix landed at 18:01:35 but the drivers did not come up until
18:04:20/18:04:46. The 18:45 sample is **40m 40s** post-restart, not 43m 25s. Use the
process starts, not the deploy timestamp.

### CONFOUND: both drivers restarted 9.5 minutes before the after-sample

19:40:40 and 19:40:42 are **9m 30s / 9m 28s** before the after-sample at 19:50:10.

- **The cumulative cross-check is UNAFFECTED.** 19,166 → 22,929 is a system-wide D-Bus
  counter that does not reset when a client restarts. The 0.708/sec window average stands.
  This is precisely why that was the right instrument.
- **The instantaneous rows ARE confounded.** "0.13/sec instantaneous", dbus-daemon 10.6 %
  and bluetoothd 5.2 % were all sampled 9.5 minutes after both drivers restarted. Not
  necessarily wrong — but they have an alternative explanation the cumulative figure does
  not, and the table above presents them side by side as if equally robust. They are not.

### Rates, not counts — and they invert the obvious reading

Raw counts suggested a large win (RS 345 → 53 reconnects). The windows are 32.79 h and
4.56 h, so per hour: 345/32.79 = **10.5/h** before, 53/4.56 = **11.6/h** after.

|  | recon/h | no-data-180s/h | fallback-entries/h |
| --- | --- | --- | --- |
| RS before | 10.5 | 4.0 | 2.6 |
| RS after | 11.6 | 0.2 | 5.9 |
| CS before | 4.8 | 0.5 | 1.1 |
| CS after | 11.2 | 0.2 | 5.3 |

Reconnection rate did **not** improve — CS more than doubled. What improved is
"no data for 180 s on an open link" (20x on RS) and fallback entries (2–4x on both).

Two readings, neither chosen: the link got worse, **or** the driver stopped sitting on dead
links for 180 s and now hands off to fallback quickly — which produces exactly this
signature (fewer long stalls, more fallback transitions, unchanged reconnects) and is more
consistent with what the fix did.

Either way it corroborates the lead from an independent instrument: **the use-after-free fix
did not reduce BLE churn** — which load 7.39 at 18:45 already said.

### The 22:34 reboot did something much larger, on a different metric

15 h post-reboot: **2 reconnects** (1 per pack), **0 no-data-180s**. Confirmed not a rotation artifact — newest rotated file is the 22:35:09 reboot
rotation and `current` has not rotated since.

This does **not** resurrect the reboot as a candidate in the 18:21–19:50 window; the uptime
counter rules that out and the correction above stands. But any future load comparison
spanning 22:34 is comparing two different machines.

**RESOLVED — the true figure is 171x.** Both the originally reported ~165x and the ~340x
derived from it here were wrong, from the same mixed-denominator error. The source count
was `reconnects=1` *per pack* and was written up as "1 across BOTH packs", which is 2.
Recomputed with both packs combined throughout:

| | window | reconnects | rate |
| --- | --- | --- | --- |
| pre-reboot | 18:04 → 22:35, 4.56 h | 53 + 51 = 104 | 22.81/h |
| post-reboot | 22:35:45 → 13:36:34, 15.01 h | 1 + 1 = 2 | 0.1332/h |

Ratio **171x**. Verified here: 104/4.56 = 22.807, 2/15.01 = 0.13324, 22.807/0.13324 = 171.2.

Worth keeping the trail: 165x was wrong by construction but landed within 4 % of the right
answer, so comparing the *number* would never have exposed it — only re-deriving it from the
stated rates did, and that produced 340x, wrong in the other direction because it inherited
the bad count. Two wrong figures bracketing the right one.
