# Measurement topology on D-Bus

This document describes the `/Measurement/*` paths that this driver publishes,
and the vocabulary they belong to. It is an interface contract: other services
on the bus read these paths, so the names, types and value formats below are
what consumers depend on.

## The problem it solves

A physical battery is often measured twice. The BMS reports pack voltage,
current and state of charge over its own link; a Victron SmartShunt wired at
the same terminals reports the same quantities independently. Both appear on
D-Bus as `com.victronenergy.battery.*` services.

Anything that sums battery services — an aggregator, a DC-system calculator —
cannot tell from the values alone that these two services describe one battery
rather than two. It counts the pack twice. On a live system this produced a DC
load reading of about 20.5 A against a true 13.5 A, which fed into DVCC
compensation and set the bank oscillating.

Consumers cannot infer the relationship, because nothing in the published
values expresses it. So the services declare it.

## Where the declarations live

On each service, under `/Measurement/` in its own object tree, beside
`/Dc/0/Voltage` and everything else it publishes. There is no central
registry and no separate topology service. A consumer discovers the graph by
walking the services already on the bus and reading these paths.

A service declares only what it can speak for. Victron's own shunt services
know nothing of this vocabulary and cannot declare anything, so the driver
that knows about a pairing declares it on their behalf.

## Schema

All values are strings. Which paths a service publishes depends on its `Kind`.

### `/Measurement/Kind`

Required for participation. One of:

| Value | Meaning |
| --- | --- |
| `direct` | This service publishes measurements of a physical device it observes. |
| `derived` | This service computes its values from other services. |

A service that publishes no `Kind` is not part of the graph, and consumers
must fall back to whatever behaviour they used before.

### `/Measurement/PhysicalDevice`

Published by `direct` services. A stable identifier for the physical device
being measured. Two services publishing the same value are measuring the
same physical thing.

**The value is opaque.** Consumers use it only as a grouping key and must
not parse it. This driver publishes `battery:<bms_id>`, for example
`battery:53_20_B7_D7_F9_E7`, but the format is a convention, not part of
the contract.

The identifier must be stable across restarts of the publishing service, or
groups will fragment.

### `/Measurement/PeerServices`

Published by `direct` services that know of other services measuring their
own physical device. A **comma-separated list** of D-Bus service names:

```
com.victronenergy.battery.ttyS5
com.victronenergy.battery.ttyS5,com.victronenergy.battery.ttyS9
```

Omitted entirely when there are no known peers, rather than published empty —
the peer set is fixed at registration, so a present-but-empty value would
falsely suggest a value still to come.

### `/Measurement/LineAuthority`

Published by `direct` services that can say which service should be treated
as the truth for line voltage and current on their physical device. A single
D-Bus service name.

Where a shunt is wired at the battery terminals it is normally the authority:
it measures the line directly, where a BMS may report a filtered or
deadbanded value.

### `/Measurement/TracksServices`

Published by `derived` services. A comma-separated list of the service names
whose values this service aggregates.

This driver never publishes it — a `direct` service has nothing to track. It
is documented here because it is part of the same vocabulary, and consumers
read it from aggregators.

## Worked example

Two BLE batteries, each with its own SmartShunt, plus an aggregator:

```
com.victronenergy.battery.ble_5320b7d7f9e7      (this driver)
  /Measurement/Kind            = direct
  /Measurement/PhysicalDevice  = battery:53_20_B7_D7_F9_E7
  /Measurement/PeerServices    = com.victronenergy.battery.ttyS5
  /Measurement/LineAuthority   = com.victronenergy.battery.ttyS5

com.victronenergy.battery.ble_ab807254e0b4      (this driver)
  /Measurement/Kind            = direct
  /Measurement/PhysicalDevice  = battery:AB_80_72_54_E0_B4
  /Measurement/PeerServices    = com.victronenergy.battery.ttyS6
  /Measurement/LineAuthority   = com.victronenergy.battery.ttyS6

com.victronenergy.battery.ttyS5                 (SmartShunt, declares nothing)
com.victronenergy.battery.ttyS6                 (SmartShunt, declares nothing)

com.victronenergy.battery.aggregate             (aggregator)
  /Measurement/Kind            = derived
  /Measurement/TracksServices  = com.victronenergy.battery.ble_5320b7d7f9e7,
                                 com.victronenergy.battery.ble_ab807254e0b4
```

Five battery services are on the bus; two physical batteries exist. The
declarations are what let a consumer tell the difference.

## How a consumer resolves the graph

For each service on the bus:

1. Read `/Measurement/Kind`. If absent, the service is not participating.
2. If `derived`, mark it claimed and skip it — this is what prevents an
   aggregator being summed alongside the constituents it already includes.
3. If `direct`, read `/Measurement/PhysicalDevice`. Group services by that
   value, adding everyone named in `/Measurement/PeerServices` to the same
   group and marking them claimed.
4. Take `/Measurement/LineAuthority`, when present, as the group's preferred
   source for line voltage and current.

The result is one representative per physical device. Services that declared
nothing and were never named as a peer are left to whatever fallback the
consumer chooses.

## Notes for implementers

- **Absent is meaningful.** Optional paths are omitted rather than published
  as `None`. Consumers must treat an absent path as "not declared", never as
  an error.
- **Registration-time values.** These paths are declared when the service is
  registered and do not change during its lifetime. A consumer may cache them,
  and should not treat a transient read failure as a change in topology.
- **Lists are comma-separated strings**, not D-Bus arrays, for the sake of
  services and tools that read paths as simple text. Consumers should strip
  whitespace around entries and ignore empty ones.
- **Naming.** The paths intentionally describe measurement relationships
  rather than any particular feature, so that other pairing sources can
  publish the same vocabulary.
