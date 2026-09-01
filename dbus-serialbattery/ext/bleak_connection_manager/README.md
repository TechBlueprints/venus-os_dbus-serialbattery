# bleak-connection-manager (v2)

An embeddable library that does for any [bleak](https://github.com/hbldh/bleak)
consumer what habluetooth does for Home Assistant: a per-process connection
layer injected underneath every `from bleak import BleakClient` via module
rebinding, so consumers (dbus-serialbattery drivers, vendored aiobmsble,
anything) transparently connect through claim-aware adapter selection,
per-adapter link slots, and failure-driven adapter rotation.

The v1 codebase (the BlueZ connection-lifecycle manager) lives on the
[`v1-main`](https://github.com/TechBlueprints/bleak-connection-manager/tree/v1-main)
branch with its full history; target v1 hotfixes there.

## Quick start

```python
from bleak_connection_manager import install_bleak_catcher

# BEFORE importing any library that does `from bleak import BleakClient` -
# they capture the binding at import time.
install_bleak_catcher(
    "dbus-blebattery.0",
    adapters=["C8:47:8C:11:22:33@hci0", "C8:47:8C:11:22:33@hci1", "hci2"],
    link_caps={"hci0": 5},          # opt-in; uncapped adapters are never gated
    wrap_scanner=True,              # opt-in; also route BleakScanner (see below)
)

import my_bms_library  # its BleakClient is now the routed wrapper
```

Every client the process builds afterwards routes through the catcher. The
wrapper **routes, it never retries** — retry semantics belong to whoever
drives the client (bleak-retry-connector, typically). A bare
`BleakClient.connect()` without bleak-retry-connector logs a warning, once
per address per process.

## Adapter configuration

Adapters may be named by `hciN` **or by the adapter's own MAC**, in any
spelling — colons, dashes, dots, spaces or none, any case — anywhere an
adapter is configured (`adapters=`, `link_caps=` keys, and the right-hand
side of a `DEVICE@ADAPTER` pin). The MAC is the stable identity: `hciN`
numbering changes under a USB reset or a replug, so a MAC entry is resolved
to whatever number the card answers to at the moment it is used, and a card
that renumbers keeps its pins, caps and claims.

**Plain `hciN` entries are accepted forever** — the vast majority of
configuration written by users everywhere spells adapters that way, and
nothing here deprecates it.

Rewriting the config is **opt-in and off by default**, because the file
belongs to the consumer and other code may parse it. In particular, a
driver that keeps its own adapter parser for a non-catcher fallback path
(the rollback other code depends on) must be taught to translate MACs
*before* the rewrite is enabled, or that fallback breaks silently the
first time it reads a rewritten file. Enable it only when the whole
consumer understands MACs.

Pass `adapter_config_path=` to `install_bleak_catcher` and the first
successful read of an `hciN` entry rewrites that entry in your config file
to the MAC it proved to be, leaving a comment above the line:

```ini
# bcm: hci3 was detected as AA:BB:CC:DD:EE:FF and rewritten
adapters = AA:BB:CC:DD:EE:FF,hci5
```

The rewrite is line-oriented and format-agnostic (INI, conf, shell-style
all work), respects token boundaries (`hci1` never matches inside `hci10`),
skips commented-out lines, and is best effort — a config that cannot be
written is never worth breaking a connection over.

Entries are raw strings, passed verbatim:

- `MAC@hciX` pins that device to that adapter; repeating the MAC gives an
  ordered preference list. A pinned device **never** falls back to the pool.
- Plain `hciX` entries form the shared pool for unpinned devices.

With **no adapters configured at all**, every adapter the kernel exposes is
a candidate (numeric hci order) — the pool config acts as an allowlist, and
an unconfigured install still spreads load by default.

Selection re-runs on every `connect()` call. **Pinned devices** walk their
pin list failure-driven: the per-address index advances only after a failed
connect attempt — a disconnect is not a failure (a dropped link reconnects
on the adapter it was using), and the index never resets on success; the
modulo wrap brings a recovered preferred radio back naturally. **Unpinned
devices** are placed by habluetooth-parity connect scoring: penalties for
live occupancy (soft claims from *any* process — the cross-process
generalization of habluetooth's in-progress term), per-adapter failure
counts for this address (cleared by a success there), and a last-slot
penalty; capped-full adapters rank last. Ties break by configuration order,
and all scoring state is process-wide, so fresh-client-per-attempt callers
continue where they left off.

**The configuring driver picks its placement mode.** By default
(`scan_to_score=False`) nothing scans, so the score has no RSSI base —
routing is least-used. With `scan_to_score=True` the catcher runs periodic
short active sweeps per candidate adapter (habluetooth's active-window
cadence: 10s every 300s, each sweep holding that adapter's `hciN.scan`
claim and skipping cards another live process is scanning on), and the
score gains its RSSI base with penalties charged in units of the spread
between the two best paths, exactly as habluetooth scores connection paths.

Configured adapters are filtered against what the kernel currently exposes
(`/sys/class/bluetooth`, `hciconfig` fallback), against adapters whose sysfs
MAC is all-zeros (a dead or unserved controller stays listed in sysfs
forever), and against adapters another live process is actively scanning on
(a foreign `hciN.scan` claim). Every filter falls back to the unfiltered
list rather than refusing to attempt — coordination is an optimization,
never a gate. An adapter the caller chose explicitly (`bluez={"adapter":
...}`) is never overridden.

**Resolved BLEDevices route themselves.** bleak's BlueZ backend connects
via `device.details["path"]` whenever the device carries one — the adapter
argument is only honored when it has to scan. So adapter *selection* applies
to plain-address connects; a cache-resolved `BLEDevice` (bleak-retry-
connector's `get_device`, scanner-discovered devices) already names its
adapter in its D-Bus path, and the catcher treats that as caller-explicit:
claims, cap gating, and connection-parameter tuning land on the adapter the
link will actually use. Explicit connects (both kinds) always write the soft
claim, so they still count in every process's occupancy score.

## Link slots

`link_caps` bounds **established-link capacity** per adapter (dongle limits
like the CSR8510's are undocumented, so caps are deployment config, not
discovery — bleak-retry-connector's field experience suggests ~5 for CSR
adapters and ~7 for Broadcom as starting points). A capped adapter's connections each hold a numbered exclusive
`hciN.link.<k>` claim file; when all slots are held live, selection moves to
the next eligible adapter **without** advancing the failure index. When every
eligible adapter is full, `connect()` raises `OutOfConnectionSlotsError`
(a `BleakError` whose message starts with the literal `"connection slot"`,
which bleak-retry-connector string-matches into its 4-second out-of-slots
backoff on any version), with per-adapter occupancy in the message, e.g.
`hci0 (5/5 links held)`. Slots release on `disconnect()`, on an unexpected
drop (via the disconnected callback), and on a failed connect.

## Scanning (opt-in)

With `wrap_scanner=True`, `bleak.BleakScanner` is also rebound, to an
adapter-bound, hard-claiming scanner. At `start()` it ranks the shared pool
(or, with no pool, the union of pinned adapters) by live occupancy — fewest
soft claims plus held link slots first — skips cards another live process is
scanning on, and takes the winner's exclusive `hciN.scan` claim, held per
scan activity: released at `stop()` or on a failed start. When every card is
claimed it scans on the best-ranked one anyway, unclaimed. A caller-chosen
adapter is never overridden (its claim is still taken, best effort). Opt-in
because it changes which adapter unrelated code scans on.

A wrapped scanner also carries habluetooth's watchdog: checked every 30s,
quiet past 90s → restart (which re-runs selection, so a dead card is walked
away from), quiet past 120s or never-saw-anything → a **drain-coordinated
hardware reset**. Empty advertisements don't count as signs of life.

A reset kills every link on the card, so it is never sprung on the card's
other users. The resetter takes the adapter's exclusive `hciN.drain` claim
(convention 0.3): placement in every participating process steers new work
elsewhere, and each process's drain watcher — run on its claim heartbeat —
disconnects its own clients on the card *when they have somewhere else to
go*, letting their retry loops reconnect on another adapter. A holder that
cannot move (its only working card, an operator pin, a caller-chosen
explicit adapter) stays put, and its live claims veto the reset: the
resetter waits up to `drain_timeout` (default 60s) for the card to empty
and gives up rather than pulling it out from under a holder. `force` skips
every gate, for an operator who knows the card is dead.

The reset primitive itself is native and stdlib-only — rfkill unblock,
`HCIDEVDOWN`/`HCIDEVUP` bounce over a raw `AF_BLUETOOTH` socket, and
`USBDEVFS_RESET` for a USB card that has gone silent — so no extra install
is needed. When `bluetooth-auto-recovery` (the optional `recovery` extra)
is importable it is preferred, for its mgmt-socket powered handling and
its post-USB-reset re-find of an adapter that renumbered. **It can never
be present on a Cerbo**: Venus OS has no usable pip and the package is
not vendorable (its rfkill path hard-imports GPLv3 PyRIC), so on Venus
the native sequence is always the one that runs — the preference only
changes behavior in environments that already ship the library (a Home
Assistant host, a dev box). One quirk to know there: on a non-USB adapter
with `gone_silent` the library reports failure even after a successful
power cycle; the native path judges by whether the card answers instead. After a
successful reset, `bluetoothd` is restarted if the reset killed it and
bleak's cached D-Bus manager state is invalidated so the next connect
rebuilds from `GetManagedObjects`. `reset_adapter(adapter,
claims_manager=None, force=False, gone_silent=False,
drain_timeout=DRAIN_TIMEOUT)` is exported for consumers' own recovery
paths; `drain_timeout=0` is the old immediate foreign-claims gate.

## Connection parameters

By default (`tune_conn_params=True`) each routed connect pre-seeds the
kernel with habluetooth's FAST parameters (7.5ms interval, 10s supervision
timeout) over the BlueZ management socket so they apply to the connection
being established, then relaxes to MEDIUM (8.75–11.25ms, 8s) once it's up.

**Venus OS note** (field, 2026-08-22, both Cerbos): the platform Python
is built without bluetooth socket support — no `socket.AF_BLUETOOTH`.
The kernel supports the family fine, so `mgmt.open_bt_socket()` falls
back to making the socket(2) syscall through libc and wrapping the fd
(btsocket's technique, MIT — the reason bluetooth-auto-recovery would
have worked there). The mgmt channel and the reset's ioctl bounce both
ride this opener; `hciconfig down/up` remains the bounce's last resort,
`adapter_mac` keeps its hciconfig fallback, and where no fallback is
wired up a feature just doesn't happen — documented, never silently
assumed working. Deployment note: the first release carrying this opener
is also the first on which conn-param tuning actually functions on Venus
— sequence its rollout deliberately.
Degrades silently to a no-op wherever the mgmt channel is unavailable
(non-Linux, Python without `AF_BLUETOOTH`, no NET_ADMIN).

## Post-connect validation (optional)

A connect that returns success is not always a usable link: GATT discovery
can come back empty, the characteristic the caller needs can be missing, a
phantom connection can read as connected until the first real read fails.
v1 answered this with a `validate_connection` callback, and it is here
unchanged in meaning — `async (client) -> bool`, run once the link is up,
and **a rejection is a connection failure**: the link is disconnected, its
claims are released, the adapter takes the failure in the placement score
and the pin walk, and `ConnectionValidationError` (a `BleakError`) is
raised so the retry loop above — bleak-retry-connector — attempts again,
on the next radio. A validator that raises counts as a rejection.

Two ways in, per client and process-wide:

```python
from bleak_connection_manager import validate_char_exists

# per client: establish_connection passes surplus kwargs to the client class
client = await establish_connection(
    BleakClientWithServiceCache, device, name,
    validate_connection=validate_char_exists("6e400003-b5a3-f393-e0a9-e50e24dcca9e"),
)

# process-wide fallback: validates connections made deep inside a library
# the driver never calls directly (a vendored BMS library, say)
install_bleak_catcher("dbus-blebattery.0", validate_connection=validate_gatt_services)
```

The client kwarg wins over the installed default; `client.connect(
validate_connection=...)` wins over both. With neither, nothing is
validated and connects behave exactly as before.

The built-ins live in `bleak_connection_manager.validators` (stdlib only,
duck-typed on the client — the module imports without bleak), weakest to
strongest: `validate_gatt_services` (service table non-empty),
`validate_char_exists(uuid)`, `validate_read_char(uuid, timeout=5.0)`
(reads the characteristic, so a phantom link or dead HCI handle fails
here). Any `async (client) -> bool` of your own works just as well.

**Chips that register GATT late** (Telink-based ones notably) announce
ServicesResolved with only the Generic Attribute service present and add
the vendor services seconds later. v1 waited 2s/4s/8s for them, re-reading
the service table from BlueZ between tries, around *every* validator. The
catcher itself never retries, so here that wait is explicit — wrap the
validator in it where you want v1's behaviour:

```python
from bleak_connection_manager import tolerate_late_gatt, validate_char_exists

validate_connection=tolerate_late_gatt(validate_char_exists(UUID))   # 2s, 4s, 8s
```

It gives up early if the link drops while waiting, and `refresh_services(
client)` — the cache-bypassing GATT re-read it uses — is exported for
validators that want to do their own waiting. Validation runs inside
`connect()`, so whatever it spends comes out of bleak-retry-connector's
60-second per-attempt safety timeout — budget slow validators accordingly.

## The claims layer

Coordination uses the bt-claims file convention under `/run/bt-claims`
(tmpfs): heartbeated `<pid> <service> <since>` files, live = pid alive AND
mtime fresh, anyone may reap a file failing both. Files are keyed by the
**adapter's own MAC** (colons stripped, uppercase — convention 0.4), because
hciN numbering changes under a USB reset or a replug without a reboot, and a
claim keyed by number can come to name a different radio than its writer
meant. Kinds: `<MAC>.scan` (hard, exclusive), `<MAC>.use.<owner>[.<qualifier>]`
(soft, ranks placement), `<MAC>.link.<k>` (numbered exclusive slots),
`<MAC>.drain` (exclusive: "this card is about to be reset — place elsewhere,
and move your links off it if you can"). Readers canonicalize pre-0.4 `hciN.*`
files, and exclusive claims check the legacy name before taking one, so a
fleet mid-upgrade cannot double-book a slot.

A connection's claims are tied to the truth of its link, not to the
wrapper object: once connected they are re-checked on every heartbeat and
released if the link is gone, so a torn-down D-Bus watch or an abandoned
client frees its slot within a TTL instead of at process exit. Link truth
is *observed traffic* — a notification arriving, or a `read_gatt_char` /
`write_gatt_char` returning — which outvotes a `is_connected` that reads
False on a broken D-Bus view. How long silence stays consistent with a
live link **adapts to each connection's own cadence**: the floor is the
claim TTL, but a client whose traffic interval approaches or exceeds that
gets a window sized from what it actually does (capped, so a genuinely
dead link still frees its claims in bounded time). A fixed floor would
flicker for exactly the consumers that need it most — a device notifying
every 30 seconds against a 30 second floor is a coin flip, not a margin. The same signal re-acquires claims that were
lost while the link lived, so a polling consumer that never subscribes to
notifications recovers on its next poll.

**Known limit.** The convention only sees processes that participate in
it. A service driving a radio another way — raw HCI sockets, a C program
talking to BlueZ directly, someone at `bluetoothctl` — holds no claims and
appears in no occupancy score, so scores on such a host are structurally
blind to that use. That is the boundary of a file-convention approach, not
something a future migration fixes.

`bleak_connection_manager.claims` is deliberately standalone — stdlib only,
no bleak, vendorable verbatim — so services that only want adapter
coordination can use it (or copy it) without any of the bleak machinery:

```python
from bleak_connection_manager.claims import ClaimManager

manager = ClaimManager(owner="my-scanner")
claim = manager.claim_hard("hci0")     # exclusive scan claim, or None
adapter, soft = manager.choose(["hci0", "hci1"])  # ranked placement
```

**Using the claims layer on its own.** This repository is the single home
of the convention and its reference implementation — there is no separate
library to track. [`claims.py`](src/bleak_connection_manager/claims.py) is
one stdlib-only file with no imports from anywhere else in this package
(no bleak, no asyncio, no project imports), and the module docstring is
the complete specification. Any service that wants adapter coordination
without adopting the bleak catcher can copy that one file into its own
tree and import it directly, or participate at the file level with nothing
but `ls`, `touch` and `cat` — a shell script is a legitimate participant.
Copies should record the commit they came from, so a convention bump can
be traced.

## Threads this package adds to your process

Installing the catcher adds **one thread** to a consumer that may not have
had one: the bt-claims heartbeat, which touches its claim files every
`HEARTBEAT_INTERVAL` and runs the drain watcher. That matters on platforms
where D-Bus bindings are thread-affine — `dbus-python` connections and the
`SettingsDevice`/`VeDbusService` objects built on them are *transparent* to
thread affinity, not safe for it: hand one a connection created on another
thread and nothing raises, it simply misbehaves later, often as a crash
inside libdbus far from the cause.

The contract this package holds itself to, and the one a consumer should
hold too:

- The heartbeat thread **only reads cached state** — `is_connected` on
  bleak's BlueZ backend is a plain attribute read, not a bus call — and
  **never touches D-Bus**.
- Every action it decides on is marshalled back to the owning loop with
  `loop.call_soon_threadsafe(...)`. The drain watcher never calls
  `disconnect()` or `start()`/`stop()` itself; it schedules them.

Two consequences worth knowing as a consumer:

1. **A drain can disconnect your client at a moment you did not choose**
   (see the claims layer). Your `disconnected_callback` therefore fires on
   bleak's thread in response to something *this package* initiated, not
   only in response to your own calls. If that callback touches a
   thread-affine binding, marshal it — `GLib.idle_add`, or your own loop —
   exactly as you would for any other bleak callback.
2. **Safety here is a property of what marshals a call, not of what the
   call constructs.** A site that builds a bus at runtime is fine if
   everything reaching it is marshalled onto the owning thread, and unsafe
   the moment something is not. Auditing construction sites alone will miss
   it; audit the call graphs that reach them. Consider making such sites
   assert their thread rather than trusting the graph to stay that way —
   a named error at the call site beats a segfault inside libdbus later.

## Deployment: the shared install (Venus OS)

Vendoring this stack per consumer means every fix is N re-vendor passes,
and the claims convention pays for version skew (an old consumer ignores
drain claims it has never heard of). The supported alternative: the
shared install IS a git checkout of this repository at `/data/bcm` — the
one directory that survives a Venus firmware update — with bleak and
bleak-retry-connector as submodules pinned to the field-validated
upstream releases (v3.0.2 / v4.6.0) and `dbus_fast`, `bluetooth_adapters`
and `aiooui` vendored in `ext/`. Each consumer's installer converges it:

```sh
BCM_DIR=/data/bcm
if [ -d "$BCM_DIR/.git" ]; then
    git -C "$BCM_DIR" fetch -q origin && git -C "$BCM_DIR" merge -q --ff-only origin/main
else
    git clone -q https://github.com/TechBlueprints/bleak-connection-manager "$BCM_DIR"
fi
"$BCM_DIR/install.sh"
```

`--ff-only` means a stale consumer can never move the fleet backwards.
`install.sh` initializes submodules, smoke-imports the whole stack, and
writes the interpreter shim `/data/bcm/python3` — the ONE place the
shared path is written down. A consumer's runit script uses it with a
standalone fallback, so public repos keep working from a bare clone:

```sh
BCM_PY=/data/bcm/python3
[ -x "$BCM_PY" ] || BCM_PY=python3
exec "$BCM_PY" main.py
```

Rollback: `git -C /data/bcm checkout <hash> && /data/bcm/install.sh`.
Canary: a second clone at a pinned hash plus `BCM_ROOT=<that-clone>` in
one service's run script; the rest of the fleet rides the main checkout.

### Autowire (opt-in): every Python bleak consumer, no opt-in needed

`install.sh --autowire` plants a `.pth` plus `bcm_autowire.py` in the
system site-packages, handling Venus's read-only rootfs itself
(remount-rw, plant, remount-ro; a boot needing no changes never
remounts). The plant is replanted at boot via `/data/rc.local`, since
firmware updates erase the rootfs — and the replant logs to
`/data/bcm/autowire-replant.log`, because a silent failure at the
post-update boot is exactly the failure the replant exists to prevent.
Every successful wire also appends one line to
`/data/bcm/autowire-events.log` — timestamp, pid, full argv, cwd, owner,
and whether the process brought its own bleak or was served the shared
stack (size-capped at 1 MB) — so the box can answer "what BLE software
has ever run here" after the processes are gone. Together the three
files make `/data/bcm` self-describing: what's planted, what's been
wired, who's been on the radios. From then on, ANY Python process on
the box that imports bleak gets the catcher installed over it before the
importer can capture `bleak.BleakClient` — community drivers that have
never heard of BCM participate in slotting, latching and drains. A
process with its own vendored bleak keeps it (the hook wraps whatever
bleak the process brought and never overrides its choices); a process
with none is served the shared checkout's. Fleet defaults come from
`/data/bcm/autowire.conf` (JSON, keys as `install_bleak_catcher`); a
consumer's own explicit `install_bleak_catcher` call supersedes the
autowired one — and a process that imports `bleak_connection_manager`
itself is never autowired at all: the finder stands down the moment the
package appears in `sys.modules` (a deliberate consumer installs
explicitly, with its own owner and config), and the `/data/bcm/python3`
shim exports the kill switch for the same reason. Kill switch:
`BCM_AUTOWIRE=0` in the environment.

Scope honesty: this covers Python+bleak only — C programs talking to
BlueZ directly (Victron's `dbus-ble-sensors`) and `bluetoothctl` remain
outside the claims convention either way. And because a `.pth` runs at
the startup of every Python interpreter on the machine, `bcm_autowire.py`
is built to never raise: enable autowire only after the shared checkout
itself has soaked on the fleet.

## Public API

| Name | Purpose |
| --- | --- |
| `install_bleak_catcher(owner, adapters=(), link_caps=None, claim_dir="/run/bt-claims", wrap_scanner=False, tune_conn_params=True, scan_to_score=False, validate_connection=None)` | Rebind `bleak.BleakClient` and, when importable, `bleak_retry_connector.BleakClient` / `.BleakClientWithServiceCache`; with `wrap_scanner=True`, also `bleak.BleakScanner`. Idempotent. The pid is appended to `owner` in claim files. |
| `reset_adapter(adapter, claims_manager=None, force=False, gone_silent=False, drain_timeout=DRAIN_TIMEOUT)` | Drain-coordinated hardware reset, stdlib-native (the `recovery` extra is optional and preferred when installed). Takes the `hciN.drain` claim, waits for holders to migrate, refuses if foreign claims remain at the deadline; `drain_timeout=0` is an immediate foreign-claims gate. |
| `uninstall_bleak_catcher()` | Restore the originals and release all held claims. |
| `BLEConnection` | The wrapper client (subclass of `BleakClient`, deferred init, routes at `connect()`). |
| `BLEConnectionWithServiceCache` | Adds bleak-retry-connector's service-cache surface (`set_cached_services` no-op, guarded `clear_cache`). |
| `BLEScanner` | The adapter-bound, hard-claiming scanner (deferred init, routes and claims at `start()`). |
| `OutOfConnectionSlotsError` | `BleakError` subclass raised when every eligible adapter's cap is fully claimed. |
| `ConnectionValidationError` | `BleakError` subclass raised when `validate_connection` rejects a link (after it has been torn down). |
| `validate_gatt_services`, `validate_char_exists(uuid)`, `validate_read_char(uuid, timeout=5.0)` | Built-in post-connect validators (`bleak_connection_manager.validators`). |
| `tolerate_late_gatt(validator, waits=(2.0, 4.0, 8.0))`, `refresh_services(client)` | Wrap a validator to wait out late-registering GATT services; the cache-bypassing service re-read it uses. |

## Tests

```bash
python -m pytest tests/
```

No bleak install and no hardware needed: the suites run against an in-memory
bleak 3.x-shaped stub and a tmpdir claim directory.
