# Production memory watermark eviction

The production `MooncakeServerComposition` enables a hysteretic memory
watermark controller by default. Its defaults are a `0.90` high watermark and
a `0.80` low watermark; both can be replaced through
`MooncakeServerConfig::with_memory_eviction` or the production binary flags:

```bash
cargo run --release -- \
  --eviction-high-watermark 0.90 \
  --eviction-low-watermark 0.80
```

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

The catalog keeps explicit/admin reclaim debt separate from watermark debt and
uses their maximum as the coalesced global target. Retirement and physical
reclaim are intentionally different accounting events:

- retiring an object removes it from `live_bytes` and adds it to
  `retired_bytes`, but its reservation remains in physical `used_bytes`;
- retired bytes already in flight cover the same amount of debt, preventing a
  later sample from retiring duplicate replacements;
- debt falls only when RAII reclamation actually releases the replica
  reservations;
- a pinned retired node remains used and indebted until its final `ObjectRead`
  drops, after which a later bounded step reclaims it.

Crossing strictly above high starts a cycle. Each step refreshes the absolute
target from current physical usage and performs only its configured candidate
and reclaim budgets. The cycle remains active through the hysteresis band and
stops only when physical usage reaches low (or the Memory capacity disappears).
Existing second-chance recency, object leases, tenant-scoped reclaim,
segment-invalidation pruning, and reservation RAII remain the candidate and
release authority.

## Allocation failures and lifecycle

A Memory allocation failure coalesces a physical-byte reclaim request, wakes
the production `MasterReconciler`, and attempts one bounded collector step in
the caller. A retry is made only when that step observed physical reclaim or a
larger free region, and never more than the configured finite retry limit. NoF
allocation failures do not invoke the Memory controller. There is no unbounded
map scan or busy loop in the request path.

The controller does not spawn a detached worker. Its periodic steps and wakeup
future are owned by `MasterReconciler`, which is started alongside the RPC
server by `BoundMooncakeServer::run_until`. The shared shutdown path stops and
joins both the listener and reconciler, so no eviction work survives server
shutdown.

`ObjectManager::memory_eviction_stats` exposes current capacity, used/live/
retired bytes, thresholds, maximum sampled physical usage, explicit and
watermark debt, active state, trigger and step counts, retirement/reclaim
totals, allocation failures/retries, busy steps, and wakeups. Active
reconciliation also emits structured debug records under
`cakemaster::server::eviction`.

The RPC pressure benchmark server is built from the same production
composition and prints these diagnostics when it shuts down. A current 1:1:1
`BatchPut`/`BatchGet`/`BatchExists` run that crosses high and settles at low is
recorded in [`object_catalog_mooncake_benchmark.md`](object_catalog_mooncake_benchmark.md).

Soft pin, hard pin, group eviction, NoF watermarks, and disk eviction are not
implemented by this controller.
