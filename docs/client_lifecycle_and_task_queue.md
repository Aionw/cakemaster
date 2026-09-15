# Client lifecycle and TaskQueue integration design

## Conclusions and implementation order

`ClientTaskQueue` only handles asynchronous task delivery to a known client. It
does not determine whether a client exists, is still alive, or represents an old
process reconnecting, nor does it clean up resources after a client timeout.
Unified client lifecycle management must therefore come before Mooncake
`FetchTasks`, offload heartbeat, and promotion heartbeat integration.

Recommended implementation order:

1. Implement a synchronous, deterministically testable `ClientRegistry` for
   sessions, states, TTL, and timeout detection, without dependencies on Tokio,
   RPC, `SegmentPool`, or TaskQueue.
2. Implement a synchronous `ClientManager` in the core crate to coordinate
   registration/remount, `Ping`, write fencing, timeout cleanup, and per-client
   mount slots; the server only supplies explicit ticks.
3. Create a mailbox when a client is successfully activated, and close it when
   the client enters draining/expired state.
4. Once the lifecycle is stable, implement the task ledger, `FetchTasks`, and
   typed task lanes.

Phase one does not implement a generic task state machine or turn every RPC
with a `client_id` into a task. Put, Mount, capacity reports, and completion RPCs
remain synchronous domain operations; TaskQueue carries only asynchronous work
that the Master assigns to Clients.

## Background and current gaps

`ClientId` is currently a value object containing two `u64` values, used mainly
as a segment owner and write owner. `SegmentPool` can validate ownership, but no
global component answers these questions:

- Has the client completed mounting and become ready to receive work?
- When was its last valid heartbeat, and when should it time out?
- In what order should its queue, segments, pending writes, and tasks be cleaned up?
- Can cleanup from an old session affect a new session with the same `ClientId`?
- Must clients remount after a Master restart or HA leader switch?

The existing `ClientTaskQueue<T>` is a bounded per-client Tokio channel. Master
producers hold a cloneable `ClientTaskTx<T>`, and the fetch RPC handler holds the
unique `ClientTaskRx<T>`. It provides FIFO ordering, wakeups, asynchronous
backpressure, and cancellation safety while waiting, but deliberately excludes
a client registry, task completion state, and persistence.

Current C++ Mooncake semantics provide a compatibility baseline:

- `Ping(client_id)` returns a view version and `OK/NEED_REMOUNT`;
- `ReMountSegment` handles initial connections and remounts after heartbeat TTL expiry;
- `FetchTasks(client_id, batch_size)` retrieves tasks for a client, while
  `MarkTaskToComplete` uses a separate completion path;
- offload and promotion heartbeats also retrieve per-client pending work.

Upstream references:

- [`MasterService::Ping`](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store/src/master_service.cpp#L5499-L5518)
- [`FetchTasks`/`MarkTaskToComplete`](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store/src/master_service.cpp#L9391-L9413)
- [`ClientTaskManager`](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store/include/task_manager.h#L182-L233)

## Design goals

The lifecycle layer must enforce these invariants:

1. Only clients that successfully register or remount are `Active`.
2. `Ping` from an unknown client returns only `NEED_REMOUNT`; it must not
   implicitly create a registry entry or TaskQueue, preventing arbitrary IDs
   from consuming Master memory.
3. A `ClientId` has at most one active session at any time.
4. Heartbeats can extend only the current active session, never revive draining
   or expired sessions.
5. Timeout handling fences the session out of the serving path before cleaning
   up resources asynchronously.
6. Every cleanup operation carries a session generation; old-session cleanup
   must not delete a new session's mailbox or state.
7. The same `ClientId` cannot establish a new session until cleanup finishes.
   Segments currently record only the owner `ClientId`, not a session generation;
   early remounts would let old cleanup delete new resources.
8. `ClientRegistry` never executes `.await` or calls `SegmentPool`, RPC,
   TaskQueue, or task completion under its locks.
9. Liveness is not restored after a Master restart or leader switch. The new
   process starts with an empty registry; all clients first receive
   `NEED_REMOUNT` and must remount to prove that their resources remain valid.

Version one assumes that the C++ client generates a new `ClientId` on each
process start. The wire carries only `client_id`, so an old process and a new
process reusing its ID cannot be strongly authenticated as distinct. Server-side
generations isolate internal asynchronous cleanup but cannot prevent two
processes from using the same ID concurrently. Explicit ID reuse would require
a session token on the wire or sessions bound to RPC connections.

## Identity and session

`ClientId` is a stable caller-provided identity; `ClientSession` is the internal
incarnation allocated by the Master for a successful activation:

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ClientSession {
    client_id: ClientId,
    generation: u64,
}
```

Generations are allocated monotonically across the Master and never reused
within a process lifetime. Internal mailboxes, cleanup events, and future task
ledger assignments use `ClientSession`, not bare `ClientId`. An RPC adapter
receiving a bare `client_id` must first resolve its current active session
through the registry.

Phase one continues to use `cakemaster::segment::ClientId` to avoid migrating all
owner APIs merely for module naming. Once the lifecycle implementation is
stable, the definition can move to `cakemaster::client::ClientId`, with a
compatibility re-export in the `segment` façade; this refactoring must not block
lifecycle functionality.

## State machine

The registry holds only entries establishing a serving relationship or awaiting
cleanup. A fully removed client is represented by absence from the map.

| State | Meaning | Ping behavior | Accepts new work | Remount allowed |
| --- | --- | --- | --- | --- |
| Absent | Not in the registry | Returns `NEED_REMOUNT` | No | Yes |
| Active | Mount succeeded and TTL is valid | Refreshes deadline, returns `OK` | Yes | Idempotent retries can reuse the current session |
| Draining | Explicit unmount or Master shutdown | Returns `NEED_REMOUNT` | No | No, until cleanup finishes |
| Expired | TTL expired; forced cleanup in progress | Returns `NEED_REMOUNT` | No | No, until cleanup finishes |

State transitions:

```text
                      successful register/remount
             ┌─────────────────────────────────────┐
             │                                     ▼
          Absent                                Active
             ▲                                  │    │
             │                   graceful close │    │ TTL reached
             │                                  ▼    ▼
             └──── cleanup finished ─────── Draining Expired
```

`Draining` and `Expired` have different business cleanup behavior but identical
fencing rules: neither can return to `Active` through a heartbeat. Cleanup
removes the entry; a subsequent remount creates a higher-generation session.

`MountSegment` dynamically adds one segment for an active client. For an absent
client, it explicitly runs an initial `attach_quiesced → activate session →
reactivate` transaction, rolling back the new attachment/session on any failure.
After TTL expiry enters `Expired`, cleanup must still finish before
`MountSegment` or `ReMountSegment` can create a higher generation. Ordinary
`Ping` and object RPCs never implicitly register clients.

## Time and TTL

The lifecycle uses server-side monotonic time, not wall-clock time. The core
uses explicit ticks to support deterministic tests without sleeps:

```rust
pub struct ClientTick(u64);

pub struct ClientLifecycleConfig {
    ttl_ticks: u64,
    max_clients: usize,
    cleanup_scan_budget: usize,
}
```

Configuration fields remain private and are changed through validated
constructors and consuming `with_*` builders, matching existing core conventions.

Successful activation and valid `Ping` update `expires_at` to
`max(old_expires_at, now + ttl)`, so out-of-order old heartbeats cannot shorten
the deadline. Successful remount and mount lifecycle RPCs may also count as
heartbeats. Put/Get, task fetch, and completion do not refresh liveness by
default, so business traffic cannot hide a failed heartbeat loop.

Expiry consistently means `now >= expires_at`. Configuration construction
rejects `ttl_ticks == 0`, `max_clients == 0`, and `cleanup_scan_budget == 0`.

### Deadline index

Version one uses `BinaryHeap<Reverse<DeadlineRecord>>`, without appending a new
record for every heartbeat:

1. Insert one deadline record when a session activates.
2. Heartbeats update only the entry's `expires_at`.
3. When a heap record becomes due, reread the entry. If a heartbeat extended the
   deadline, reinsert only the latest deadline; otherwise mark it `Expired`.

The heap normally stays near one record per client rather than growing with
heartbeat count. Records carry generations; records for deleted entries or
mismatched generations are discarded.

`claim_due_cleanups(now, budget)` processes at most `budget` due/rescheduled
records and returns sessions requiring cleanup.
`ClientManager::run_cleanup_step` calls it with the configured
`cleanup_scan_budget`. A server timer can later drive the manager periodically,
then be optimized to wake at `next_deadline()`. Core behavior is independent of
the timer implementation.

## ClientRegistry and public Manager API

`ClientRegistry` is a synchronous, thread-safe state machine. Methods return
state facts or exclusive cleanup claims without external side effects. Ordinary
RPCs, timers, and benchmarks use `ClientManager` rather than holding the registry
directly, preventing state transitions that omit segment or pending-write cleanup.

```rust
pub enum ClientState {
    Active,
    Draining,
    Expired,
}

pub enum ActivateOutcome {
    Activated(ClientSession),
    AlreadyActive(ClientSession),
}

pub enum HeartbeatOutcome {
    Alive(ClientSession),
    NeedRemount,
}

pub enum CleanupReason {
    GracefulUnmount,
    HeartbeatExpired,
    ServerShutdown,
}

pub struct ClientCleanup {
    // Exclusive cleanup claim for a fenced session.
    // Dropping without finishing returns it to the maintenance retry queue.
}

impl ClientRegistry {
    pub fn activate(
        &self,
        client_id: ClientId,
        now: ClientTick,
    ) -> Result<ActivateOutcome, ClientLifecycleError>;

    pub fn heartbeat(
        &self,
        client_id: ClientId,
        now: ClientTick,
    ) -> HeartbeatOutcome;

    pub fn active_session(
        &self,
        client_id: ClientId,
    ) -> Result<ClientSession, ClientLifecycleError>;

    pub fn begin_drain(
        &self,
        session: ClientSession,
        reason: CleanupReason,
    ) -> Result<Option<ClientCleanup>, ClientLifecycleError>;

    pub fn claim_due_cleanups(
        &self,
        now: ClientTick,
        budget: usize,
    ) -> Vec<ClientCleanup>;

}

impl ClientCleanup {
    pub fn finish(self) -> Result<(), ClientLifecycleError>;
}
```

Principal errors include `NilClientId`, `CapacityExceeded`, `CleanupInProgress`,
`ClientNotActive`, and `StaleSession`. These operations must be idempotent:

- Repeated activation of an active client returns `AlreadyActive` without a new generation.
- A session has at most one cleanup claim at a time.
- Dropping an unfinished claim automatically returns it to the retry queue, so
  future cancellation cannot lose cleanup work.
- Only the claim itself can `finish`, preventing an old generation from crossing
  the cleanup boundary and affecting a new session.

The registry exposes copied snapshots/outcomes, not lock guards. Version one
can use a single `parking_lot::Mutex<Inner>` to keep the map, generation
allocator, and deadline heap atomic. Heartbeats are typically far less frequent
than object RPCs; prioritize correctness, then use benchmarks to decide whether
to shard.

The public `ClientManager` API exposes only composition and business entry
points: `new/with_config`, `heartbeat`, `remount`, `write_admission`,
`write_owner`, `drain_sessions`, and `run_cleanup_step`. It does not expose the
registry, segment pool, mount slots, cleanup claims, or fencing guards.
`PendingWriteRevoker` is only an opaque capability supplied during construction;
callers cannot execute catalog cleanup directly.

## Core Manager and server boundary

Resource transactions belong to the core; clocks and asynchronous driving
belong to the server:

```text
cakemaster::client::ClientManager
├── ClientRegistry                       # synchronous state machine
├── mount_slots[ClientId]                # per-client resource transaction gate
├── Arc<SegmentPool>                     # segment lifecycle
└── PendingWriteRevoker                  # opaque pending-write revocation capability

cakemaster::server
├── ObjectCatalogRpcService              # wire conversion and error mapping
├── MasterClock + view version           # server/HA concern
├── MasterReconciler                     # explicitly started bounded timed reconciliation
└── ClientTaskHub                        # future integration
```

Each mount slot uses a synchronous `parking_lot::Mutex` to serialize only
ownership-changing operations: register/remount, mount/unmount, and cleanup.
There is no I/O or `.await` here. `Ping` performs only a short registry update
without taking a mount slot; object RPCs also run without a global client lock.

### Registration or remount

RPC handler sequence:

1. Obtain the mount slot for the `ClientId`.
2. Lock its mutex.
3. Verify that the registry entry is `Absent` or an idempotently handled `Active`.
4. Validate and attach the complete segment list in a quiesced state; roll back
   newly attached segments on failure.
5. Atomically reactivate this segment set so resources become accepting first.
6. Call `registry.activate`, after which write admission can acquire the session fence.
7. Record the session's complete segment set and return RPC success.

There is no network I/O between steps 4 and 7. For a new session, the manager
makes mounting and registry activation a rollback-capable transaction: an RPC
failure must not leave some segments permanently attached to the pool.
`registry.activate` must follow the last fallible resource step, preventing
object RPCs from observing a partially activated session whose segments are not
ready.

### Ping

`Ping` calls `registry.heartbeat(client_id, now)`:

- `Alive` maps to C++ `ClientStatus::OK`;
- `NeedRemount` maps to `ClientStatus::NEED_REMOUNT`;
- the response also carries the current Master view version.

Unknown `client_id` values allocate no slot, entry, or queue. To prevent
attackers from creating large numbers of per-client mutexes through Ping alone,
mount slots are created only on register/remount paths, or use temporary gates
that are not retained.

### TTL expiry

Expiry must fence before cleanup:

1. `claim_due_cleanups` atomically changes `Active` to `Expired` under the registry lock.
2. It also invalidates the session's internal write-admission guard, blocking
   writes that have been claimed but not staged.
3. The manager locks the same mount slot mutex, waiting for any remount in progress.
4. The cleanup claim retains exclusive ownership of the generation; early
   returns or unwinding automatically retry through Drop.
5. The manager scans the catalog's existing pending queue once for this batch
   of sessions and proactively revokes writes still in PENDING state.
6. The coordinator submits expired clients in a batch to
   `SegmentPool::invalidate_owners`, invalidating and detaching their segments
   under one lock and rebuilding the placement snapshot once.
7. Pending writes on invalidated segments need not wait for write timeout.
   Published-object reads immediately filter stale replicas; catalog maintenance
   prunes and releases them through a bounded liveness pass. Get/Exist remain
   visible while any live replica survives. Metadata retires only after the
   final replica becomes invalid; external handles may still defer physical
   release through RAII.
8. Cleanup of processing tasks, mailboxes, and LocalSSD workflows will extend
   the manager boundary when those subsystems are integrated.
9. Remove empty, unused mount slots, then call `cleanup.finish()`.

Every step must be idempotent for the same session. Any failure retains the
`Expired` entry for retry; deleting the registry entry first must never leave
orphan resources available for allocation.

`ClientManager::run_cleanup_step` implements steps 1–7 and 9 as a synchronous
single-round entry point. The server's `MasterReconciler` explicitly schedules
client cleanup, Graceful unmount, and object maintenance every 100ms by default.
RPCs that add or advance a Graceful deadline wake it precisely through a shared
`Notify` to recompute the timer, while the composition root still owns startup,
shutdown, and join. The task ledger and LocalSSD workflows are not integrated.
Cleanup completion means logical resource invalidation, not that all external
object handles have been released or physical reclamation has finished.

### Two ownership dimensions of pending writes

Segment cleanup and writer cleanup are distinct:

- If a replica's segment belongs to a departed client, its incarnation becomes
  invalid immediately, PutEnd fails, and the liveness pass retires pending
  metadata early.
- If a departed session initiated a write whose replicas all reside on healthy
  clients' segments, cleanup proactively revokes it by the full
  `(ClientId, generation)` rather than waiting for pending timeout.

Version one stores the pure `(ClientId, generation)` identity in a copyable
`WriteOwner`. Only `WriteAdmission` carries the internal session guard during
start/stage; catalog nodes hold no guard, and normal writes maintain no second
owner index. Staging first enters the catalog's existing pending timeout queue,
then rechecks the guard. After registry fencing, batch cleanup scans that queue
once under the collector consumer gate. A racing write is therefore either
seen by cleanup or revoked by the staging side. Normal publication needs no
extra global lock or index insertion/removal. Cleanup revocation and PutEnd
compete through the existing object lifecycle CAS; the first success determines
the final state. Full `(ClientId, generation)` matching also prevents old
sessions from affecting new ones.

### Explicit unmount and shutdown

Ordinary single-segment `UnmountSegment` does not put the client into
`Draining`: it validates the current session, slot, and owner, then immediately
runs `quiesce → remove`. Other segments and the session remain usable.
`GracefulUnmountSegment` likewise leaves the client session open. It quiesces
immediately and stores the earliest deadline under `(ClientSession, SegmentId)`.
The deadline holds only a weak incarnation token and validates both session
generation and segment incarnation when due, preventing old jobs from deleting
new mounts that reuse the same ID. Ordinary unmount and client expiry cancel
pending jobs.

Only whole-client or server graceful shutdown enters `Draining`, with reason
`GracefulUnmount` or `ServerShutdown`. No new work is accepted afterward;
bounded operations already inside synchronous handlers may finish before the
cleanup worker completes teardown. Segment deadlines are driven by the
explicitly running `MasterReconciler`, not independent workers started by RPC
handlers.

## TaskQueue integration

Add `ClientTaskHub` after the lifecycle layer is stable:

```rust
pub struct ClientMailbox {
    session: ClientSession,
    // Initially use typed lanes with different return types for C++ RPC compatibility.
    replication: ClientTaskQueue<DispatchToken>,
    offload: ClientTaskQueue<DispatchToken>,
    promotion: ClientTaskQueue<DispatchToken>,
    control: ClientTaskQueue<DispatchToken>,
}
```

Typed lanes are required for wire compatibility: `FetchTasks`,
`OffloadObjectHeartbeat`, `PromotionObjectHeartbeat`, and `PollRemoveAll` return
different types, so their four handlers cannot compete for one heterogeneous
FIFO. Lane consolidation can be considered only after introducing a unified
tagged-union `FetchClientTasks`.

The hub must follow these rules:

- Only `Activated(ClientSession)` may create a mailbox; no lazy creation on
  first enqueue or fetch.
- Producers resolve an active session through the registry, clone `Tx`, release
  registry/hub locks, and then call `send().await`.
- Fetch handlers use the unique `Rx`; concurrent fetches for the same client and
  lane must be serialized or explicitly rejected.
- `Draining/Expired` closes the mailbox for exactly that generation; old cleanup
  must not close a new session's mailbox.
- Channels are not the source of task truth. A `DispatchToken` discarded on
  closure must be reconstructible, failed, or reassigned from TaskLedger; the
  mpsc buffer must not be snapshotted.
- Completion RPCs remain separate and validate the assigned session/task attempt.

Existing `recv_many` waits for at least one item, whereas C++ `FetchTasks` may
return an empty batch immediately. Before RPC integration, add `try_recv_many`
or bounded `recv_many_timeout`; producers also need `try_send`/`send_timeout` to
avoid waiting indefinitely under domain locks or inside RPC handlers when a
queue is full.

## Master restart and HA

Client liveness, Tokio channels, and unconfirmed network connections are not
snapshotted. A new Master process or leader uses a new view version and starts
with empty `ClientRegistry`/`ClientTaskHub` instances:

1. The client's next `Ping` receives `NEED_REMOUNT`.
2. The client resubmits its complete segment descriptions.
3. The Master checks restored segment state against the remount request.
4. Successful validation establishes a new `ClientSession` and publishes its mailbox.
5. Still-executable tasks in the persistent TaskLedger are dispatched again.

Remote resources restored from a snapshot must not enter accepting placement
snapshots before their client remounts. View version belongs to the server/HA
runtime, not individual client entries.

## Observability

Provide at least these gauges/counters:

- active, draining, expired, and cleanup-retry client counts;
- activation, idempotent activation, heartbeat, unknown heartbeat, and expiry counts;
- client entry capacity rejections;
- latency from expiry to mailbox closure, segment quiesce, and cleanup completion;
- stale-generation cleanup/finish counts;
- per-lane depth, full/closed sends, and fetch batch sizes.

Logs must include `client_id`, generation, old state, new state, and cleanup
reason. Do not log every normal heartbeat at INFO, which amplifies logs in large
clusters; reserve INFO/WARN for transitions and anomalies.

## Test plan

### Core unit tests

- Unknown Ping returns `NeedRemount` without changing registry length.
- Activation creates a session; repeated activation preserves its generation.
- Heartbeats extend deadlines; out-of-order ticks never shorten them.
- Old heap deadlines reschedule using the entry's latest deadline rather than
  incorrectly expiring the client.
- `now == expires_at` enters Expired; later heartbeats cannot revive it.
- Activation with the same ID before cleanup finishes returns `CleanupInProgress`.
- Finish from an old generation cannot delete the current entry.
- Cleanup budgets are respected, leaving remaining due clients for the next round.
- max_clients, nil IDs, and invalid configuration produce explicit errors.

### Manager concurrency and resource tests

- Concurrent Pings lose no updates and keep deadlines monotonic.
- Remount and timeout cleanup serialize through the same client slot.
- Activated creates a mailbox only once; Expired immediately closes its generation.
- Old cleanup interleaved with a new session cannot close or delete the new mailbox.
- Server shutdown moves all active clients to Draining and waits for bounded cleanup.
- Handler cancellation/mount failure leaves neither an Active entry nor partial new mounts.

### C++ wire cross-layer tests

- Unknown client: `Ping -> NEED_REMOUNT`.
- Successful remount: `Ping -> OK`.
- TTL expiry: `Ping -> NEED_REMOUNT`, task fetch rejected.
- Remount after cleanup: a new server generation is established.
- Clients must remount after the Master view version changes.

### Client exit-storm performance tests

`client_cleanup_benchmark` continuously runs 50:50 put/get on the direct path and
uses real `ClientManager::drain_sessions` calls to remove clients in batches
mid-traffic. Defaults are 8 workers, 1K clients, 4 published objects per client,
and 2048 healthy hot keys, taking the median sample of 5 rounds.
`--pending-per-client` can also preload PENDING writes initiated by those
sessions. Healthy traffic uses `ClientManager::write_admission` too, so the
benchmark does not bypass session fencing. 10K clients form a manual stress
configuration; 64K clients test only cleanup scalability, avoiding RPC codec
costs in catalog/placement lock contention measurements.

```bash
cargo run --release --bin client_cleanup_benchmark -- \
  --workers=8 --operations=50000 --clients=16 \
  --objects-per-client=4 --pending-per-client=4 \
  --hot-objects=2048 --rounds=5
```

In the Memory-only version each client segment owns an allocator, so
preallocated metadata grows with segment count. The normal stress configuration
therefore defaults to 16 exiting clients; do not directly reuse the historical
shared-CXL-arena version's 10K setting. Earlier CXL performance data does not
represent the current implementation. The 64K cleanup-only test mounts no
exiting segments; this low-memory configuration tests registry/slot/claim
scalability:

```bash
cargo run --release --bin client_cleanup_benchmark -- \
  --workers=1 --operations=0 --clients=64000 \
  --objects-per-client=0 --hot-objects=0 --rounds=3
```

Output includes baseline and exit-storm throughput, put/get p50/p99/p99.9,
cleanup/settle duration, invalidated metadata counts, and healthy-request errors.
Acceptance uses relative measurements from the same machine and round: liveness
checks without exits must degrade throughput/p99 by no more than 10%; during a
10K storm, throughput must drop by no more than 25%, p99 must stay within 2x
baseline, and logical cleanup must finish within 2 seconds. Healthy objects must
have zero errors. The 64K cleanup-only target is completion within 10 seconds
without superlinear growth.

## Proposed file layout

Expected phase-one additions:

```text
src/client.rs                              # client domain façade
src/client/lifecycle.rs                    # registry, state machine, deadline heap
src/client/manager.rs                      # remount, write fence, batch resource cleanup
src/client/config.rs                       # TTL/capacity configuration
src/client/error.rs                        # lifecycle errors/outcomes
tests/client_lifecycle.rs                  # deterministic core tests
tests/client_manager.rs                    # cross-resource manager tests
tests/client_lifecycle_rpc.rs
```

Phase two adds TaskLedger, `ClientTaskHub`, and Mooncake task RPC adapters.
Existing `src/server/client_task_queue.rs` remains the lowest-level channel
primitive, without registry, segment cleanup, or task persistence logic.

## Phase-one acceptance criteria

Client lifecycle management is complete only when all these conditions hold:

1. Core state-machine and TTL tests require no wall-clock sleeps.
2. Unknown Ping allocates no persistent state.
3. Timeout fences the client before any resource cleanup.
4. The same ID cannot establish a new session before cleanup finishes.
5. All cleanup validates generations; unfinished claims automatically reenter
   the retry queue.
6. Master restart explicitly requires remount and does not restore old liveness.
7. Server/RPC code uses only `ClientManager`, without access to the registry,
   slots, or cleanup claims.
8. No await or call to another domain manager runs under the client registry lock.

With these constraints in place, TaskQueue integration will not determine client
liveness, and queue closure, RPC cancellation, or old-session cleanup will not
corrupt segment/object domain state.
