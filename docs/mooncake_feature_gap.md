# Feature gaps between Cakemaster and upstream C++ Mooncake Store

## Comparison baseline and scope

This document pins the following source baselines so the tables remain meaningful
as `main` changes:

- Upstream: [kvcache-ai/Mooncake `5c0724d22e7f04513a3453c8b6642a5a21b80b47`](https://github.com/kvcache-ai/Mooncake/tree/5c0724d22e7f04513a3453c8b6642a5a21b80b47), committed 2026-08-11;
- This repository: the commit containing this document, updated 2026-08-16;
- Historical wire baseline `8c6095c06e20848506cbf91ef4a714924e7b03b1` is mentioned
  only as the migration source; current IDL and golden vectors match `5c0724d` above.

The comparison covers upstream `mooncake-store` Master, Store Client, and their
directly related data plane. Standalone Transfer Engine, P2P Store, Mooncake
EP/PG, schedulers, and vLLM/SGLang integrations are not themselves counted as
missing Cakemaster features. The Transfer Engine functionality that Store Client
uses for Put/Get is included because it is an essential data plane for a complete
Mooncake Store.

Status definitions:

- **Implemented**: callable entry points, complete state transitions, and tests
  exist in the current code.
- **Partially implemented**: only domain models, low-level primitives, older
  contracts, or benchmark compositions exist; upstream end-to-end behavior is
  not yet available.
- **Not implemented**: no corresponding business state machine or runtime exists
  in the current source.
- **Scope undecided**: a Master-only replacement can reuse upstream Clients
  rather than rewriting them here; a complete Store must implement these features.

Design documents are not implementations. For example,
[client_lifecycle_and_task_queue.md](client_lifecycle_and_task_queue.md) has led
to a synchronous `ClientRegistry`, core `ClientManager`, single-round resource
cleanup coordinator, and production composition that explicitly runs/stops/joins
`MasterReconciler`, but there is still no `TaskLedger` or `ClientTaskHub`.

## Conclusions

Cakemaster currently consists of a high-concurrency metadata/placement core and
single/batch RPC adapters compatible with the pinned Mooncake wire, plus a
process-local production binary that deploys these routes. Incomplete routing
and storage capabilities mean it cannot fully replace `mooncake_master`, much
less the complete Mooncake Store.

- Upstream actually registers **60** coro_rpc routes in `RegisterRpcService`;
  this repository's contract and adapter provide **27**, leaving **33** missing.
- The workspace's main `cakemaster` binary composes these 27 routes; the old
  `DemoService` entry point has been removed.
- All 27 match the pinned latest upstream wire. `BatchPutStart`
  `SoftPinAction`/TTL schema drift has been fixed, and
  `ServiceReady`/`GetStorageConfig` suffice for nonpersistent Client initialization.
- Existing `ObjectCatalog`, `SegmentPool`, and Memory tenant quota are reusable,
  but client lifecycle, complete object APIs, tiered-storage tasks, HA/recovery,
  the data plane, and operations capabilities remain incomplete.

Treat “replaceable basic Memory Master” and “full Mooncake Store parity” as
separate milestones. Passing selected route interoperability tests does not
establish complete compatibility.

## Existing foundations

| Capability | Current implementation | Boundary |
| --- | --- | --- |
| Object metadata | `ObjectCatalog` and `ObjectManager` provide per-key transactions, lock-free committed-version reads, commit/abort, atomic upsert, remove, get/exists, leases, soft/hard pins, transaction timeouts, proactive revocation by client session, bounded reclamation, and replica-level pruning after segment invalidation | No automatic replica repair/replenishment or complete upstream API; checksum, group, upsert preemption/busy-refcnt semantics are not integrated; pins lack persistent recovery |
| Segment/placement | `ClientManager` connects `Ping`, Memory `MountSegment`/`ReMountSegment`, immediate/Graceful unmount, session TTL fencing, and batch cleanup to `SegmentPool`; production explicitly runs and joins `MasterReconciler` with 100ms periodic maintenance and Graceful deadline wakeups | No NoF lifecycle RPCs, liveness probes, or real I/O |
| Placement | Preferred segments, free-capacity ordering, replica failure domains, and RAII rollback | Not upstream's five configurable policies; no mixed Memory+NoF or host-local placement |
| Tenant | `TenantObjectManager` provides namespace isolation, Memory charging, quota admission, RAII accounting, and targeted reclamation | No upstream policy connector, HTTP admin, persistence, or startup recovery |
| Mooncake RPC | `ServiceReady`, persistence-disabled `GetStorageConfig`, `Ping`, four Memory segment lifecycle routes, single/batch exists/get/put, six upsert routes, and four remove routes, composed by `cakemaster` | `GetFsdir` compatibility fallback is unimplemented; production remains an in-memory single-tenant subset, not a complete upstream Master |
| RPC runtime | coro_rpc v0/struct_pack compatibility over TCP, multiplexing, attachments, timeouts, cancellation, streaming framing | No upstream optional RDMA RPC socket, leader-aware client pool, or Store API |
| Task/client primitives | `ClientRegistry` is encapsulated behind core `ClientManager`, with generation-fenced cleanup; bounded per-client `ClientTaskQueue<T>` also exists | No authoritative task table, retries, recovery, task/LocalSSD cleanup hooks, or task RPCs |

See [object_catalog_rpc.md](object_catalog_rpc.md),
[segment_pool_backends.md](segment_pool_backends.md), and
[tenant_quota.md](tenant_quota.md) for implementation details.

## Latest upstream contract drift already addressed

Upstream `5c0724d` changed `ReplicateConfig` from:

```cpp
bool with_soft_pin;
```

to:

```cpp
SoftPinAction soft_pin_action;
std::optional<uint64_t> soft_pin_ttl_ms;
```

See upstream [`replica.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/replica.h#L63-L126).
This repository's [mooncake_master.thrift](../idl/mooncake_master.thrift) now
declares `SoftPinAction: u8` and optional TTL, with updated `BatchPutStart`
struct_pack type literals/hashes. Golden tests pin C++ yalantinglibs-generated
metadata and representative request bytes.

The adapter accepts and executes `PRESERVE/ENABLE/DISABLE`, optional TTL, and
hard pins. The domain layer stores deadlines relative to the commit tick and
validates the 30-minute default and 24-hour maximum. Only the latest schema is
supported; `BatchPutStart` from old `8c6095c` clients is explicitly rejected by
type hash rather than ambiguously decoded using two schemas.

## Detailed feature gaps

### 1. Complete object APIs and semantics

Status: **Partially implemented; P0 gap for a basic Memory Master**.

Twenty object-related handlers currently exist. These upstream capabilities are
not yet available as compatible public services:

- `GetReplicaListByRegex`;
- `BatchReplicaClear` and `BatchQueryIp`;
- checksum storage, return values, data-plane validation, and snapshot/oplog
  compatibility; current RPCs explicitly reject nonempty checksums and get
  always returns `None`;
- pin snapshot/oplog recovery, dedicated metrics, and exact C++ global two-phase
  priority when soft-pin eviction is allowed;
- same-shard routing, group lease refresh, and best-effort group eviction for
  optional object groups;
- simultaneous Memory and NoF replicas for one object; request conversion
  currently allows only Memory;
- object commit and selection for Disk/LocalDisk replicas;
- `prefer_alloc_in_same_node`, `host_id`, and full `ObjectDataType` behavior;
  all types except KVCACHE and TENSOR currently collapse to `General`;
- upstream's separate `put_start_discard_timeout` and `put_start_release_timeout`.
  There is currently only one pending timeout, with nonequivalent preemption
  and delayed-release behavior;
- upstream `ReplicaID` is a globally increasing `uint64_t`; the current domain
  `ReplicaId` is a per-object `u32` ordinal starting at 1. Widening it to u64 on
  the wire does not provide equivalent uniqueness or ID space.

Upsert supports insertion of missing keys, fresh-allocation version replacement
at every size, atomic switching at end, and preservation of the old committed
version on revoke/timeout/session-fence. Upstream immediate preemption of an
existing PROCESSING writer and replica busy-refcnt checks are not implemented.
Remove supports single/batch/regex/all, leases, hard pins, and force; force
bypasses both leases and hard pins. Replication tasks are not modeled.

Upstream references: [`rpc_service.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/rpc_service.h),
[`Master/Store design`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/docs/source/design/mooncake-store.md).

### 2. Placement, eviction, and background maintenance

Status: **Partially implemented; P0/P1 gap for a basic Memory Master**.

Upstream offers five policies: `random`, `free_ratio_first`,
`ssd_free_ratio_first`, `cxl`, and `local_first`. Currently there is only
`FreeCapacityPolicy`, which scans a snapshot and sorts by preferred name, free
ratio, and largest contiguous free range:

- No default random or best-of-N sampling behavior.
- No integration with SSD free ratios.
- The shared CXL capacity model has been removed, and upstream's `cxl` strategy
  is absent.
- No host-aware `local_first`.
- No deployment configuration for policy selection.

The catalog provides second-chance-style bounded reclamation, and production
composition integrates a Memory high/low watermark controller. It computes the
byte debt needed to return to low from deduplicated physical capacity/used,
distinguishes live/retired/actual RAII reclaim, and supports allocation-failure
wakeups, bounded synchronous collection, and limited retries.
`MasterReconciler` unifies running, stopping, and joining. Remaining differences
from upstream include:

- Separate NoF high/low watermarks.
- Complete candidate filtering for groups, busy replicas, and incomplete writes.
- Count-Min Sketch promotion admission and global two-phase soft-pin priority.
- Targeted cleanup and capacity recomputation after client/segment failures.

The basic Memory feedback loop is therefore available in the production runtime,
but is not equivalent to the full Mooncake eviction policy.

Upstream references: [`mooncake-store.md#AllocationStrategy`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/docs/source/design/mooncake-store.md#allocationstrategy),
[`master_config.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/master_config.h#L24-L148).

### 3. Client lifecycle and dynamic segment control plane

Status: **Basic Memory runtime and five lifecycle RPCs implemented; complete
control plane not implemented; P0 gap for a basic Master**.

End-to-end gaps and current progress include:

- `MountNoFSegment`, `ReMountNoFSegment`, and `UnmountNoFSegment` are missing.
- Production composition explicitly starts and joins `MasterReconciler`;
  single-round session fencing, batch segment cleanup, pending-write revocation
  by session, Graceful deadlines, and safe rejoining are implemented.
- Cleanup ordering for objects, segments, tasks, offload queues, and metadata
  service registrations after client timeout is incomplete.
- NoF heartbeat probes, timeouts, consecutive-failure thresholds, and automatic
  removal are missing.
- `GetAllNoFSegments`, `GetNoFSegmentsByName`, `QuerySegmentStatus*`, and `GetFsdir`
  are missing. `GetFsdir` is only the legacy compatibility fallback after
  `GetStorageConfig` failure; the current successful empty `GetStorageConfig`
  response already suffices for nonpersistent Client initialization.
- Graceful drain and segment drain jobs are missing.

`SegmentPool::attach/quiesce/reactivate/remove/invalidate_owners` provides the
low-level capabilities for these workflows. `ClientManager` implements atomic
activation and rollback for Memory `MountSegment`/`ReMountSegment`,
immediate/Graceful single-segment removal, and batch pending-write revocation
plus logical segment/object invalidation after timeout. Task and LocalSSD
workflows are not integrated. See
[client_lifecycle_and_task_queue.md](client_lifecycle_and_task_queue.md) for
detailed constraints.

### 4. SSD/NoF/DFS tiered storage

Status: **LocalSSD/NoF models and capacity primitives removed; workflows and I/O
not implemented**.

Only the Memory domain implementation remains; unintegrated storage primitives
are no longer maintained. Upstream capabilities still missing include:

- `MountLocalDiskSegment`, SSD capacity heartbeats, and per-client offload queues;
- eager offload and `offload_on_evict` memory -> SSD policies;
- `OffloadObjectHeartbeat`, `NotifyOffloadSuccess`, and `PollRemoveAll`;
- LOCAL_DISK descriptors in object metadata, remote reads, and buffer TTL/GC;
- SSD-only hit promotion, admission thresholds, allocation, and success/failure notifications;
- `EvictDiskReplica`/batch, disk high/low watermarks, FIFO/LRU;
- bucket, file-per-key, and offset-allocator local storage backends, restart
  scan/recovery, and POSIX/io_uring I/O;
- legacy DFS persistence and distributed storage/HF3FS/3FS adapters;
- real NoF SSD namespace management, liveness probes, and data transfer;
  no NoF allocator is currently provided.

Upstream references: [`SSD Offload design`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/docs/source/design/ssd-offload.md),
[`storage_backend.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/storage_backend.h).

### 5. Copy/Move, asynchronous tasks, and drain jobs

Status: **Not implemented; only channel primitives exist**.

Upstream includes:

- user-facing `CreateCopyTask`, `CreateMoveTask`, and `QueryTask`;
- client polling through `FetchTasks` and `MarkTaskToComplete`;
- three-phase metadata transactions `CopyStart/End/Revoke` and `MoveStart/End/Revoke`;
- pending/processing/finished states, limits, timeouts, retry attempts, and client assignment;
- segment drain job create/query/cancel, concurrency, progress, and failure statistics;
- authoritative task state that can be restored or reconstructed after HA failover/leader switches.

Existing `ClientTaskQueue<T>` stores no authoritative task state and provides no
mailbox generation, status, retry, or completion validation. It is not equivalent
to upstream TaskManager.

Upstream references: [`task_manager.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/task_manager.h),
[`transfer_task.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/transfer_task.h).

### 6. Multi-tenant parity

Status: **Core admission/accounting implemented; configuration, administration,
and recovery not implemented**.

The implementation provides Memory quota, but not a deployable equivalent of
upstream's complete tenant feature:

- No `enable_multi_tenants` startup mode or production composition.
- No file/etcd YAML policy connector or connector-first atomic policy updates.
- No HTTP list/get/upsert/delete admin API.
- No quota Prometheus metrics.
- No usage/effective-quota reconstruction from connector policy after snapshot restore.
- No HA active-only admin fencing.
- LocalSSD quota, group accounting, and upstream orphan-tenant recovery rules
  are not implemented.

This repository retains only Memory quota. Wire/admin differences from upstream
need explicit versioning and documentation.

Upstream references: [`multi-tenancy.md`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/docs/source/deployment/multi-tenancy.md),
[`tenant_quota_policy_store.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/tenant_quota_policy_store.h).

### 7. HA, OpLog, snapshots, and recovery

Status: **Not implemented**.

Missing upstream capabilities include:

- etcd, Redis, and optional K8s Lease leader election;
- master runtime state, leader discovery, view versions, and client switch/remount;
- Primary/Standby supervisors and active-only RPC/HTTP serving;
- etcd ordered batch OpLog, strict standby sequence application, gap/retry/catch-up;
- snapshot fork/COW and object/segment/allocator/checksum metadata codecs;
- local/S3 snapshot object stores, embedded/Redis snapshot catalogs, retention, and restore;
- promotion after snapshot bootstrap + OpLog catch-up;
- explicit recovery rules for tenant policy/usage, client liveness, soft pins,
  tasks, and other state.

Without these capabilities, a Master process restart loses all metadata, and
multiple instances cannot safely serve together.

Upstream references: [`master_service_supervisor.cpp`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/ha/leadership/master_service_supervisor.cpp),
[`ha/oplog`](https://github.com/kvcache-ai/Mooncake/tree/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/ha/oplog),
[`ha/snapshot`](https://github.com/kvcache-ai/Mooncake/tree/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/ha/snapshot).

### 8. Store Client and real data plane

Status: **Not implemented; scope undecided**.

This repository's `RpcClient` is a generic coro_rpc client, not a Mooncake Store
Client. A complete Store still needs:

- high-level `Put/Get/BatchPut/BatchGet/Upsert/Remove/Query`;
- local buffer registration, slice/stripe planning, parallel transfer, and replica selection;
- Transfer Engine paths for TCP/RDMA/CXL, multi-NIC, GPUDirect, and automatic failover;
- CPU/CUDA/HIP/Ascend/Sunrise buffer/device support and pinned host memory;
- checksum computation, writing, and post-Get verification, plus lease
  revalidation after transfer completion;
- local memcpy, local hot cache, and same-node optimizations;
- embedded real clients, dummy-real clients, standalone services, and
  UDS/shared-memory forwarding;
- C/C++, Python, Go, and upstream Rust Store APIs/bindings.

If Cakemaster targets only Master compatibility, explicitly reuse upstream
C++/Python Clients and use them for cross-language acceptance; this section then
requires no rewrite here. If the goal is an independent complete Store,
everything in this section is necessary.

Upstream references: [`client_service.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/client_service.h),
[`real_client.cpp`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/real_client.cpp).

### 9. Production server, administration, and observability

Status: **Basic production runtime implemented; full administration and
observability not implemented**.

`cakemaster --listen ADDRESS` constructs and shares `SegmentPool`, the in-memory
`ObjectManager`, `MasterClock`, `ObjectCatalogRpcService`, and `MasterReconciler`.
It supports ephemeral-port binding, per-direct-memory-allocator metadata node
budgets through `--max-allocator-nodes-per-segment`, an object-index capacity
hint through `--expected-objects`, background candidate/reclaim budgets through
`--object-collection-budget-per-step`, Ctrl-C/SIGTERM shutdown, and joining server
connections and the reconciler before exit. Upstream's production master also
provides:

- JSON/YAML/gflags configuration and complete argument validation;
- separate RPC/HTTP listen addresses, thread counts, connection timeouts, TCP_NODELAY;
- 21 HTTP methods/routes, including metrics, health, role/leader/HA status,
  key/segment queries, drain, tenant quota, and remove-all;
- Prometheus metrics, summaries, and cache/tenant/SSD/HA/task metrics;
- a KV event publisher and `/kv_events/status`;
- a built-in HTTP metadata server and metadata cleanup after client timeout;
- complete readiness, cross-component graceful shutdown, more background-worker
  lifecycle management, and failure propagation.

Fixed segments and eviction threads in benchmark binaries cannot replace
production composition/configuration; they remain performance tools. The
production binary registers the integrated `ServiceReady` and empty
`GetStorageConfig`, bringing the current route count to 27.

Upstream references: [`master.cpp`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/master.cpp),
[`MasterAdminServer routes`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/master_admin_service.cpp#L1190-L1291),
[`http_metadata_server.cpp`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/http_metadata_server.cpp).

## coro_rpc route gaps

See upstream's actual registry in [`RegisterRpcService`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/rpc_service.cpp#L1637-L1775).
Grouped by function:

| Group | Upstream routes | Current contract/adapter | Conclusion |
| --- | ---: | ---: | --- |
| Objects, metadata, and queries | 23 | 20 | 3 missing |
| Segments, client lifecycle, and configuration | 15 | 7 | 8 missing |
| LocalSSD offload/promotion | 11 | 0 | All missing |
| Copy/Move and asynchronous tasks | 11 | 0 | All missing |
| **Total** | **60** | **27** | **33 routes missing; full latest-wire compatibility is 27/60** |

### Twenty-seven existing routes

```text
Ping
MountSegment
ReMountSegment
UnmountSegment
GracefulUnmountSegment
ExistKey
GetReplicaList
BatchExistKey
BatchGetReplicaList
PutStart
PutEnd
PutRevoke
BatchPutStart
BatchPutEnd
BatchPutRevoke
UpsertStart
UpsertEnd
UpsertRevoke
BatchUpsertStart
BatchUpsertEnd
BatchUpsertRevoke
Remove
RemoveByRegex
RemoveAll
BatchRemove
GetStorageConfig
ServiceReady
```

### The 33 missing routes

Objects and metadata (3):

```text
BatchQueryIp
BatchReplicaClear
GetReplicaListByRegex
```

Segments, client lifecycle, and configuration (8):

```text
MountNoFSegment
ReMountNoFSegment
UnmountNoFSegment
GetAllNoFSegments
GetNoFSegmentsByName
GetFsdir
QuerySegmentStatus
QuerySegmentStatusById
```

LocalSSD offload/promotion (11):

```text
MountLocalDiskSegment
OffloadObjectHeartbeat
ReportSsdCapacity
NotifyOffloadSuccess
PromotionObjectHeartbeat
PromotionAllocStart
NotifyPromotionSuccess
NotifyPromotionFailure
EvictDiskReplica
BatchEvictDiskReplica
PollRemoveAll
```

Copy/Move and asynchronous tasks (11):

```text
CopyStart
CopyEnd
CopyRevoke
MoveStart
MoveEnd
MoveRevoke
CreateCopyTask
CreateMoveTask
QueryTask
FetchTasks
MarkTaskToComplete
```

Counts include only actually registered routes, not `WrappedMasterService`
methods used solely as HTTP admin delegates without coro_rpc registration.
Tenant quota administration and drain jobs, for example, use HTTP and should be
tracked separately under administration.

## Recommended implementation order

### M0: Restore the latest wire baseline (wire migration complete)

- `ReplicateConfig`, golden vectors, and bidirectional C++ interop peers are updated.
- Only the latest schema is explicitly supported; old `8c6095c` `BatchPutStart`
  is not supported alongside it.
- CI still needs to pin/check the upstream commit and routinely run latest
  C++ -> Rust and Rust -> latest C++ tests.

### M1: Replaceable basic Memory Master

- Production server composition/configuration (basic in-memory version complete).
- `ClientManager` is integrated into production composition and runs TTL cleanup;
  NoF Mount/ReMount/Unmount remains to be implemented.
- Single-key put and remove/upsert routes have basic in-memory semantics;
  query and administration object APIs remain incomplete.
- Add checksum, group, mixed replicas, pin persistence, and both pending-timeout semantics.
- Extend NoF watermarks, external metrics exporters, group eviction, and global
  soft-pin priority.
- Complete upstream C++ Client E2E: mount -> put -> get metadata -> remove -> remount.

### M2: Dynamic operations and tiered storage

- Graceful unmount/drain and NoF heartbeats.
- TaskLedger, copy/move, and client task runtime.
- LocalSSD offload/promotion, disk eviction, and storage backends.
- Tenant connector/admin/persistence.
- HTTP admin, production health/readiness lifecycle, and KV events.

### M3: Reliability and complete product capabilities

- Snapshot/restore.
- Leader election, OpLog, standby catch-up/promotion.
- If building a complete Store, implement the Client/Transfer Engine data plane
  and multilingual bindings.

“Implemented” at each milestone should require cross-process E2E with real
upstream C++ peers, not merely Rust domain unit tests, generated code, or
successful stateless benchmark responses.

## Update checklist

When the upstream baseline changes, update this document in this order:

1. Record the new Mooncake commit and date.
2. Diff `rpc_service.cpp::RegisterRpcService` and recount routes.
3. Diff all wire structs in `replica.h`, `rpc_types.h`, `segment.h`, and `task_manager.h`.
4. Regenerate type literal/hash/bytes golden vectors.
5. Check `master_config.h`, admin HTTP routes, HA/snapshots, and Store Client public APIs.
6. Change entries from “Partially implemented” to “Implemented” only after entry
   points, state machines, and E2E are all complete.
