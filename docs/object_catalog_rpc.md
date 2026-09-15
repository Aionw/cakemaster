# ObjectCatalog Mooncake RPC design

This interface connects Mooncake `WrappedMasterService` metadata RPCs to the
existing domain layer without duplicating catalog, placement, or transaction
rules in handlers. The implementation provides client lifecycle RPCs `Ping`,
`MountSegment`, `ReMountSegment`, `UnmountSegment`, and
`GracefulUnmountSegment`; single-key `ExistKey` and `GetReplicaList`; and
`BatchExistKey` and `BatchGetReplicaList`. `PutStart/End/Revoke` and their batch
variants are covered, as are `UpsertStart/End/Revoke` and their batch variants.
Deletion covers `Remove`, `BatchRemove`, `RemoveByRegex`, and `RemoveAll`.
`ServiceReady` and `GetStorageConfig` provide the version handshake and
nonpersistent configuration required for upstream Client initialization.

## Layers and synchronous/asynchronous boundary

```text
async WrappedMasterService handler
        ├── ServiceReady / GetStorageConfig → fixed wire-compatible configuration
        ├── Ping / segment lifecycle → ClientManager → ClientRegistry + SegmentPool
        └── object RPC → ObjectManager / TenantObjectManager
                         ├── ObjectCatalog: key lifecycle, owner, lease, pin, reclamation state
                         └── ReplicaAllocator: placement and SegmentPool reservations
```

The generated RPC trait uses `async fn`, allowing Tokio/coro_rpc drivers to
schedule network entry points directly. `ObjectManager` deliberately remains
synchronous: it accesses only concurrent in-memory structures and the local
`SegmentPool`, has no I/O to await, and bounds each maintenance step by a budget.
If placement later needs a remote scheduler, introduce asynchrony at the
placement/coordinator boundary rather than making the entire in-memory catalog
state machine asynchronous.

`ObjectCatalogRpcService` holds a shared `MasterClock`. Object handlers validate
wire requests and normalize domain inputs, then obtain a monotonic tick, execute
one bounded maintenance step, resolve the batch tenant once, call domain batch
APIs, and map results. RPCs and background controllers must clone the same clock
to avoid passing `CatalogTick` values with different time origins to one manager.
Each key succeeds or fails independently within a batch; only connection/codec
failures return transport-level `RpcFailure`.

All client/segment lifecycle RPCs use the same core `ClientManager`. `Ping`
refreshes only existing sessions' heartbeats; unknown clients receive
`NEED_REMOUNT`. Segment lifecycle operations convert wire segments into
`SegmentSpec`, perform attach/reactivate and session activation under a
per-client lock, and roll back new resource changes on failure. The active
session is published only after all segments reactivate, so object writes never
observe a partially mounted state. `MountSegment` also lets absent clients
atomically establish their first session and active clients add segments
dynamically. Ordinary `UnmountSegment` immediately quiesces/removes the target
segment while retaining the client session and other segments.
`GracefulUnmountSegment` quiesces immediately, preserves existing replica
liveness during the grace window, and removes only at expiry; it neither waits
for allocations to reach zero nor migrates data. After removal, reads return only
live replicas. Catalog maintenance prunes stale replicas from published objects
in place and releases capacity and tenant quota together. Objects remain visible
while at least one replica survives; the entire object retires only after its
last replica becomes invalid. Pruned replicas are not automatically replenished.

Routine object maintenance is driven by `MasterReconciler`, constructed from the
service by the composition root, rather than preceding every RPC batch. Write
finish/revoke/remove batches add only one bounded post-operation step sized to
the batch width to promptly drain their candidates; reads do not perform
maintenance. Every 100ms by default, the reconciler runs client cleanup, due
Graceful unmounts, and bounded object maintenance in sequence. Topology, deadline,
and memory-pressure notifications trigger it immediately. Steps that make
physical progress explicitly yield and reschedule another round rather than
combining multiple steps into an uninterruptible loop. The production
`ObjectManager` also enables 90%/80% Memory high/low watermarks. Within the same
collector gate, each step reclaims first, then computes its Memory byte target
from deduplicated physical capacity/used values. Allocation failure still allows
the request thread to attempt one bounded collection. The RPC service and
reconciler share deadline/topology `Notify` instances, while the controller has
a separate memory-pressure `Notify`, so Graceful deadlines are not quantized to
100ms. The reconciler uses `MissedTickBehavior::Skip` and must be explicitly run,
stopped, and joined by the caller. Synchronous domain managers and individual
handlers never implicitly start background tasks.

`ServiceReady` returns handshake version `2.0.0` for the pinned upstream
baseline. This string is centralized in `cakemaster::MOONCAKE_STORE_VERSION` and
matches upstream `MasterClient::Connect()`'s strict equality check.
`GetStorageConfig` always returns `fsdir=""`, `enable_disk_eviction=false`, and
`quota_bytes=0`, indicating that no storage backend should be created. Upstream
falls back to legacy `GetFsdir` only if this RPC fails, so a successful empty
configuration is sufficient to initialize a nonpersistent Client. The
compatibility fallback `GetFsdir` remains unimplemented.

The service type is `ObjectCatalogRpcService<B>`, with `ObjectManager` as its
default backend. A private `ObjectBatchBackend` trait unifies the batch interface:
the single backend's request tenant is `()`, while the multi backend's is
`ResolvedTenant`. Generic static dispatch adds no per-batch virtual calls. Its
default `execute_batch` unifies maintenance, tenant resolution, and per-item error
expansion; the implementations differ only in actual domain calls. The two
concrete managers remain explicit core safety boundaries. Typed accessors expose
a raw `ObjectManager` only for single-tenant services; multi-tenant services can
access only `TenantObjectManager`.

## Production composition and lifecycle

`MooncakeServerConfig::build` is the production composition root, constructing a
single-tenant, process-local in-memory backend by default. It constructs each
piece of state once, in this order:

```text
SegmentPool
    └── Arc<ObjectManager>
          ├── MemoryEvictionController + pressure Notify
          └── ObjectCatalogRpcService + MasterClock + ClientManager + deadline Notify
                ├── WrappedMasterServiceServer
                └── MasterReconciler (derived from the same service)
```

Before binding, `MooncakeServerComposition` exposes read-only accessors so tests
can verify pool/manager `Arc` identity, clock epoch, and client-manager state
identity. After a successful bind, `BoundMooncakeServer::run_until` polls the RPC
server and reconciler with structured concurrency. External shutdown or early
exit of either participant broadcasts a stop, then fully awaits the other.
The coro_rpc server cancels and joins all connection tasks itself. The
reconciler prioritizes shutdown over intervals/deadlines, so shutdown does not
run an extra maintenance step or Graceful deadline.

The production binary is `cakemaster`:

```bash
cargo run --release -- \
  --listen 127.0.0.1:50051
```

`--listen` requires an explicit socket address and defaults conservatively to
loopback `127.0.0.1:50051`. `--max-allocator-nodes-per-segment` defaults to 128K
and controls the number of preallocated range metadata nodes per direct-memory
allocator. Size it for peak concurrent slices plus fragmentation headroom; it
does not change the segment's declared byte capacity.
`--expected-objects` defaults to 64K and is an initial object-index capacity hint,
not a hard count limit. It should cover peak indexed objects, including empty
slots retained during the grace period. Undersizing permits concurrent hash-table
growth on the request path, causing isolated tail-latency spikes.
`--object-collection-budget-per-step` defaults to 256 and controls both candidate
scan and retired-object reclaim limits per background reconcile step. Raising it
can speed watermark convergence but also increase collector occupancy per step.
Tune against both success rate and RPC tail latency under the target load;
larger is not always better. The allocation-failure request path retains an
independent 64/64 budget unaffected by this setting.
Per-request access logs are disabled by default. `--access-log` enables info-level
logging of source, route, sequence, result, request/response sizes, and duration.
Unix handles both Ctrl-C and SIGTERM; other Tokio-supported platforms handle
Ctrl-C. Defaults retain the core's validated configuration: 64K expected objects,
64K clients, 10s client TTL, 10s object lease, 30s pending timeout, 1GiB retired-byte
ceiling, internally fixed 90%/80% Memory watermarks, and a 100ms reconcile interval.
See [`memory_eviction.md`](memory_eviction.md) for full accounting, bounded failure
retry, and diagnostics semantics. Configuration and metadata are in-memory only
and are not restored after restart. No segments are premounted; clients register
Memory capacity through Mount/ReMount RPCs.

This entry point deploys only the current `WrappedMasterService` subset listed
here, without HA, persistence, TLS, HTTP metadata, NoF/LocalSSD workflows, or a
multi-tenant policy connector. The integrated `ServiceReady` and empty
`GetStorageConfig` are registered by the same generated
`WrappedMasterServiceServer`; no extra bootstrap server is required.

Pure request validation, including Vec lengths, wire configuration, replica
selectors, and checksums, occurs before `execute_batch`; invalid requests trigger
neither maintenance nor tenant lookup. Per-item checksum validation in `put_end`
first separates valid items, passes only valid keys to the backend, then merges
results by original index. Backend callbacks therefore contain only the
corresponding domain batch calls.

## ObjectManager responsibilities and behavior

`ObjectManager` coordinates put/get/exists in the domain layer. It owns an
`ObjectCatalog`, a `ReplicaAllocator`, and an optional production Memory eviction
controller. The catalog maintains owners, per-key transactions, timeout
candidates, and committed versions; the manager does not duplicate transaction
tables or deadline heaps. Controller thresholds come from composition
configuration. The manager only samples usage, sets byte debt, and coordinates
bounded collection.

Each catalog slot has two orthogonal states: an immutable committed version
stored in `ArcSwapOption` for lock-free reader loads, and an active transaction
protected by a slot-local mutex. Active transactions have only `Claimed` and
`Staged` phases and never alter the committed pointer. Visibility is therefore
no longer represented by combined
`Claimed/Pending/Published/Updating/Retiring` states, and there is no rollback
pointer. Retired versions belong to a separate physical reclamation queue. The
controller decides only when to set global byte debt; second-chance, lease,
pin, tenant scope, segment invalidation, and RAII rules remain in the
catalog/replica layer.

`start_put` sequence:

1. Validate object size, allocation size, replica count, and replica class.
2. Atomically claim the key in the catalog; only one claimant can succeed for a
   key at a time.
3. Have `ReplicaAllocator` reserve space according to the placement plan.
4. Convert reservations into a catalog-owned `ReplicaSet` and stage the claim
   as a pending version.
5. The catalog slot retains only the pure `WriteOwner` identity,
   `TransactionId`, replicas, pin, and timeout deadline. The `WriteAdmission`
   fence used for start/stage is released when the claim leaves; the manager
   discards the temporary ticket and returns writable descriptors to RPC.

Any intermediate failure rolls back through claim/reservation RAII drops.
Partial all-or-nothing placement reservations are also released before returning
an error.

`finish_put` looks up the current active transaction by key, checks the client
owner and requested replica selector, then commits atomically. Commit metadata
always uses `checksum=None`. Repeated finish with the same transaction and
metadata succeeds idempotently; a different owner returns `ILLEGAL_CLIENT`, and
a class mismatch or invalidated write transaction returns `INVALID_WRITE`.

`revoke_put` performs the same owner/class checks, then revokes the active
transaction. Reservations follow the catalog record into reclamation and are
eventually returned to the allocator. Revoke cannot delete committed objects.

`start_upsert` is an insert for missing keys. It always requests fresh allocation,
regardless of size changes, and records the current committed version as the
transaction base. The old version remains readable throughout the transaction.
After all fallible validation and quota transitions, `finish_put` atomically
switches the committed pointer once. The old version enters deferred reclamation
using the lease deadline from its last reader refresh; allocation and quota are
returned only after all local handles are released. `revoke_put`, pending
timeout, and client-session fencing discard only the candidate, leaving the old
committed version and pin metadata unchanged. An existing active transaction
causes a conflict; upstream UpsertStart's immediate preemption of an old
PROCESSING writer is not implemented.

The current Mooncake wire carries no transaction ID. The manager can resolve an
active transaction only by `key + owner`, so once the same client session starts
a new transaction on a key, a delayed End from an old request cannot be
distinguished from the new transaction. This is an explicit limitation of
keeping the IDL unchanged.

Pin changes commit with the write transaction. `ENABLE` uses the requested TTL,
defaulting to 30 minutes with a 24-hour per-request maximum. TTL with
`PRESERVE`/`DISABLE` is rejected; `ENABLE + 0` means no soft pin after commit.
Deadlines start at the commit tick of `finish_put_at`. Upsert soft-pin actions
take effect only at End; Revoke, timeout, and session fencing preserve the old
deadline. `PRESERVE` inherits an unexpired deadline, `ENABLE` recalculates from
the commit tick, and `DISABLE` clears it. Ordinary Put fixes hard pin at creation;
replacement preserves the old hard pin and allows requests to promote it from
false to true.

`remove` deletes only published objects. Ordinary deletion respects leases and
hard pins; `force=true` bypasses both. Pending objects or objects undergoing
upsert return `REPLICA_IS_NOT_READY`; missing objects return `OBJECT_NOT_FOUND`.
Batch removal returns per-item results. Regex/all removal counts only actual
successful deletions and skips protected or incomplete objects. The wire has no
dedicated hard-pin error, so ordinary deletion of hard-pinned objects reuses
`OBJECT_HAS_LEASE`. Replication tasks are not modeled, so force does not bypass
those not-yet-existing states.

`get` returns only complete committed versions: pending inserts return
`REPLICA_IS_NOT_READY`, while pending upserts return the old committed version.
Readers refresh that version's lease before validating the committed pointer,
retrying if the pointer changed. The result therefore linearizes either before
or after commit. `exists` uses the same visibility and lease semantics but
returns only bool. `maintenance(now, budget)` uses the catalog's single bounded
collector for expired soft pins, pending writes, eviction, physical reclamation,
and empty slots. The soft-pin queue scans at most `max_candidates` registrations
per step. Old generations and stale entries whose deadlines were refreshed
invalidate automatically through weak nodes and deadline CAS, without full-table
scans. No call scans indefinitely. Diagnostic snapshots also expose candidate
queue depths to reveal cleanup throughput falling behind writes.

## ReplicaAllocator responsibilities

`ReplicaAllocator` only converts a `PlacementRequest` into a set of
reservation-owning results:

- Obtain a lock-free snapshot for the requested `ReplicaClass` from `SegmentPool`.
- Filter non-accepting, undersized, excluded, or disallowed-kind segments.
- Sort by preferred name, then free ratio, largest contiguous free range, and
  stable segment ID.
- Deduplicate by `Segment`, `Resource`, or `Owner` failure domain.
- Call `SegmentPool::reserve` for each candidate, tolerating `OutOfSpace` or
  `NotAccepting` caused by stale snapshots and trying the next candidate.
- Release all partial results if `AllOrNothing` cannot meet the count;
  `BestEffort` returns at least one successfully allocated replica.

It does not handle key deduplication, write ownership, pending/published states,
leases, eviction selection, RPC error codes, or descriptor wire formats. Those
belong to `ObjectManager`, `ObjectCatalog`, the pressure controller, and the RPC
adapter.

## Current Mooncake compatibility subset

| Mooncake input | Current behavior |
| --- | --- |
| `replica_num > 0, nof_replica_num == 0` | Memory; best-effort as in C++, with at least one successful replica |
| `nof_replica_num > 0` or nonempty `preferred_nof_segments` | `INVALID_PARAMS` |
| Memory and NoF requested together | `INVALID_PARAMS` |
| preferred Memory segment | Converted to placement preferred names |
| soft pin `PRESERVE/ENABLE/DISABLE` | Fully transactional; TTL only with `ENABLE`, default 30 minutes, maximum 24 hours, 0 means no pin |
| hard pin | Stored in metadata; eviction always skips it, ordinary Remove rejects it, force Remove can delete it |
| same-node, host/group | `INVALID_PARAMS`, avoiding silent fallback |
| Disk/LocalDisk selector | `INVALID_PARAMS` |
| `ObjectMeta.object_checksum=Some(...)` | `INVALID_PARAMS` |
| Get/BatchGet checksum | Always returns `None` |
| `ServiceReady` | Returns pinned-baseline handshake version `2.0.0`, satisfying upstream `MasterClient::Connect()`'s strict version check |
| `GetStorageConfig` | Empty `fsdir`, disk eviction disabled, quota 0; no persistent backend initialized |
| `GetFsdir` | Unimplemented; only a legacy Client compatibility fallback when `GetStorageConfig` fails |
| `Ping` | Returns view version; `OK` for activated sessions, otherwise `NEED_REMOUNT` |
| `MountSegment` | Atomically establishes absent clients' sessions; dynamically adds for active clients; identical configuration is idempotent, conflicts return `SEGMENT_ALREADY_EXISTS` |
| `ReMountSegment` | Atomic activation and idempotent remount of Memory segments; CXL, NoF, and conflicting configuration return errors |
| `UnmountSegment` | Immediately detaches one segment; absence succeeds idempotently, client session stays active |
| `GracefulUnmountSegment` | Immediately stops new allocations, detaches at grace deadline; absence returns `SEGMENT_NOT_FOUND`; requires an explicitly running `MasterReconciler` |
| `UpsertStart/End/Revoke` + batch | Missing keys behave as put; always fresh allocation; old committed version readable while pending; end switches atomically, revoke/timeout/session-fence preserves the old version |
| `Remove` / `BatchRemove` | Ordinary mode respects leases and hard pins; force bypasses both; pending/upserting objects reject deletion |
| `RemoveByRegex` / `RemoveAll` | Deletes all currently removable matching objects and returns success count; multi-tenant `RemoveAll` with an empty tenant covers all tenants |
| tenant id (single constructor) | Ignored and mapped to `NamespaceId::DEFAULT` |
| tenant id (multi constructor) | Mapped to isolated namespaces; unknown tenants and quota excess use existing tenant error codes |

`ObjectDataType::KVCACHE` and `TENSOR` retain their corresponding `ObjectKind`;
other types currently map to `General`. Empty keys, zero lengths, and mismatched
batch key/length counts produce explicit per-item errors.

## Remaining design work

The RPC adapter is already a thin layer. Tenant quota accounts only for Memory
and uses scoped filters for targeted reclamation on existing generation queues;
overall physical watermark control remains with the deployment-side controller.
Mixed Memory+NoF replicas require extending an object plan from one class to
multiple class subplans with atomic rollback. Groups and checksums are explicitly
unsupported, not hidden behind RPC placeholders. Pins cover the in-memory
lifecycle but lack snapshot/oplog recovery and dedicated metrics. See
`docs/tenant_quota.md` for complete tenant constraints.

## Known edge-case differences from current C++ Mooncake

Behavior was checked against Mooncake commit
[`07422af7d81eb905fb8054c0f8f87bea243343f7`](https://github.com/kvcache-ai/Mooncake/tree/07422af7d81eb905fb8054c0f8f87bea243343f7)
(the `extern/yalantinglibs` submodule had local changes that did not affect
Master pin logic). Observable commit/rollback, Upsert preserve/enable/disable,
hard-pin eviction protection, and TTL validation semantics are aligned without
copying C++ internals. Rust uses monotonic `CatalogTick`, atomic deadlines, and
the existing bounded collector; C++ uses the system clock, a deadline index, and
metadata shards. Known differences remain:

- When soft-pin eviction is allowed, C++ uses global two-phase selection:
  unpinned objects first, then soft-pinned objects if still necessary. Rust
  permits those candidates within bounded generation queues and cannot
  guarantee strict lower priority for soft pins across all objects.
- This implementation intentionally protects non-force Remove with hard pins
  and lets force bypass both leases and hard pins. Current C++ `Remove` checks
  only leases, with hard pins mainly protecting eviction; this is a deliberate
  safety enhancement.
- C++ may commit pending pin actions for mixed Memory/NoF when the first eligible
  replica completes, without refreshing TTL on later replica End calls. Rust
  supports only one replica class per object and commits the whole object in one
  End, so this partial-End edge case does not yet apply.
- Rust Upsert always requests fresh allocation, including same-size updates;
  C++ may reuse the original allocation. This deliberate per-key MVCC difference
  keeps the old version readable while pending.
- Pin metadata is in-memory only and is not restored after service restart;
  upstream soft-pin key metrics are also not integrated.

Implementation entry points:

- `src/object/manager.rs`: domain coordinator;
- `src/object/tenant/mod.rs`: public tenant models and module exports;
- `src/object/tenant/manager.rs`: tenant-safe object façade;
- `src/object/tenant/registry.rs`: ID/namespace resolution, registration, lifecycle;
- `src/object/tenant/quota.rs`: quota admission, accounting, RAII tokens;
- `src/segment/placement.rs`: placement and reservations;
- `src/server/rpc/mod.rs`: service and RPC handlers;
- `src/server/rpc/backend.rs`: static backend contract and common batch flow;
- `src/server/rpc/single_tenant.rs`, `multi_tenant.rs`: the two domain backend adapters;
- `src/server/rpc/request.rs`, `response.rs`: wire request normalization and response mapping;
- `src/client/manager.rs`: client remount, session fencing, resource cleanup coordination;
- `tests/rpc.rs`, `client_lifecycle_rpc.rs`: real-TCP cross-layer tests.
