# Production memory watermark eviction

The production `MooncakeServerComposition` enables a hysteretic memory
watermark controller by default. Its defaults are a `0.90` high watermark and
a `0.80` low watermark. The production binary intentionally keeps these policy
details out of its CLI; in-process compositions and benchmarks can replace them
through `MooncakeServerConfig::with_memory_eviction`.

Configuration is rejected unless both ratios are finite, lie strictly inside
`(0, 1)`, and remain `low < high` after conversion to integer
parts-per-million. Allocation-failure work is also configured as a finite
`CollectBudget`; retries are capped at three and default to one.

## Capacity and debt units

All watermark decisions and reclaim targets use physical reservation bytes,
not object count or logical object size. For the current mounted Memory/CXL
resources:

```text
high_bytes = floor(total_capacity_bytes * high_ratio)
low_bytes  = floor(total_capacity_bytes * low_ratio)
watermark_debt = max(physical_used_bytes - low_bytes, 0)
```

`SegmentPool::space_for(ReplicaClass::Memory)` counts a shared CXL arena once.
It includes both accepting and quiesced segments because quiescing placement
does not release their allocations. This prevents graceful-unmount state from
looking like an abrupt capacity loss. Attach, remove, and remount changes are
resampled on every bounded controller step, so capacity growth cancels an old
watermark target before another object is retired.

The catalog persists only explicit/admin reclaim debt. Watermark and
allocation-failure pressure remain controller-owned, are recomputed for each
collector step, and are restricted to Memory/CXL candidates. This prevents a
late concurrent sample from restoring stale debt and prevents Memory pressure
from retiring NoF objects. Retirement and physical reclaim are intentionally
different accounting events:

- retiring an object removes it from `live_bytes` and adds it to
  `retired_bytes`, but its reservation remains in physical `used_bytes`;
- retired Memory bytes already in flight cover the same watermark target,
  preventing a later periodic sample from retiring duplicate replacements;
- allocation-failure pressure is separate and may retire another candidate
  when an old reader blocks physical release, because the failed allocation
  needs reusable capacity rather than eventual watermark convergence;
- debt falls only when RAII reclamation actually releases the replica
  reservations;
- a pinned retired node remains used and indebted until its final `ObjectRead`
  drops, after which a later bounded step reclaims it.

Crossing strictly above high starts a cycle. The collector gate owns the whole
step: it first attempts pending RAII reclamation, then samples physical usage,
updates hysteresis, derives a local Memory target, retires candidates, and
publishes the final sample before releasing the gate. Each step performs only
its configured candidate and reclaim budgets. The cycle remains active through
the hysteresis band and stops only when physical usage reaches low (or the
Memory capacity disappears).
Existing second-chance recency, object leases, tenant-scoped reclaim,
segment-invalidation pruning, and reservation RAII remain the candidate and
release authority.

## Allocation failures and lifecycle

A Memory allocation failure publishes a controller-owned physical-byte
shortfall before waking the production `MasterReconciler`, then attempts one
bounded collector step in the caller. `BestEffort` requests reclaim only one
replica; `AllOrNothing` uses the allocator's actual missing replica count. A
retry is made only when that step observed Memory reclaim or a larger free
region, and never more than the configured finite retry limit. A successful
retry cancels its own pressure generation without erasing newer concurrent
failures. NoF allocation failures do not invoke the Memory controller. There
is no unbounded map scan or busy loop in the request path.

The controller does not spawn a detached worker. Periodic, topology, deadline,
and pressure wakeups are owned by `MasterReconciler`, which is started alongside
the RPC server by `BoundMooncakeServer::run_until`. A step that makes physical
progress cooperatively yields and reschedules another bounded step; normal RPC
reads no longer run catalog maintenance, while write completion/revoke/remove
batches perform one opportunistic bounded step to drain the queue entries they
produce. The shared shutdown path stops and joins both the listener and
reconciler, so no eviction work survives server shutdown.

`ObjectManager::memory_eviction_stats` exposes current capacity, used/live/
retired bytes, thresholds, maximum sampled physical usage, explicit,
allocation, and watermark debt, active state, trigger and step counts, retirement/reclaim
totals, allocation failures/retries, busy steps, and wakeups. Active
reconciliation also emits structured debug records under
`cakemaster::server::eviction`.

The RPC pressure benchmark server is built from the same production
composition and prints these diagnostics when it shuts down. The 1:1:1
`BatchPut`/`BatchGet`/`BatchExists` run recorded in
[`object_catalog_mooncake_benchmark.md`](object_catalog_mooncake_benchmark.md)
includes both the current MVCC reimplementation and the pre-MVCC PR #19
implementation. The current three-run sample crossed the high watermark in
every run, sustained the configured 900 logical batch QPS, and completed every
PUT without a handle failure.

Soft- and hard-pin enforcement remains catalog-owned. Group eviction, NoF
watermarks, and disk eviction are not implemented by this controller.
