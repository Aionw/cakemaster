# Tenant isolation and quota design

Tenant support is an optional domain layer above `ObjectManager`. The default
configuration remains single-tenant and preserves the original hot path; a
multi-tenant service must be constructed explicitly with registered policies.
Group placement/accounting is not part of this design.

## Public boundary

The core value and policy types are:

- `TenantId`: validated external identity with an opaque representation;
- `TenantResourceClass::{Memory, Nof}`: independent accounting domains;
- `TenantQuotaLimits` and `TenantPolicy`: requested policy;
- `TenantConfig::{Single, Multi { initial_policies }}`;
- `ResolvedTenant`: opaque manager- and generation-bound batch handle;
- `TenantObjectManager`: quota-safe façade for object operations and tenant
  administration;
- `TenantSnapshot` and `TenantQuotaSnapshot`: diagnostics without exposing
  mutable internals.

`TenantObjectManager::resolve_tenant` is intended to run once for an RPC batch.
The batch APIs validate that handle once, then use its internal namespace
directly. `catalog()` is available for diagnostics and reclaim controls, but the
façade does not expose a raw `ObjectManager` that could bypass quota admission.

In `TenantConfig::Single`, every external tenant ID resolves to
`NamespaceId::DEFAULT` and no quota atomics are touched. In
`TenantConfig::Multi`, only registered IDs resolve, and each tenant owns a
private non-default namespace. Re-registering a deleted tenant advances its
generation, so stale `ResolvedTenant` values cannot be reused.

## Quota semantics

Quota is tracked independently for Memory and NoF. LocalSSD is deliberately
outside v1. An object is charged by logical object bytes multiplied by its
actual direct replica count; allocator rounding and shared segment capacity do
not inflate the tenant charge.

Requested quota is policy. Effective quota is recomputed from the capacity of
currently accepting `SegmentPool` resources:

1. Shared physical resource IDs, such as multiple CXL mounts of one arena, are
   counted once.
2. If total requested quota is no greater than capacity, every tenant receives
   its request.
3. Otherwise, capacity is divided proportionally using `u128` arithmetic.
   Remaining bytes are assigned in stable `TenantId` order.

Segment changes publish a cheap capacity epoch. Normal maintenance reads one
atomic; it only takes the pool snapshot lock and rewrites effective quotas when
that epoch changes. Tenant policy updates are serialized and rare, while
resolution reads an immutable `ArcSwap` directory snapshot without a lock.

Admission uses a policy epoch plus atomic compare-and-swap. A batch performs
its initial demand reservation with at most one CAS per resource class, then
splits that reservation into per-object guards. Best-effort placement initially reserves
one replica and grows the reservation after placement if more replicas were
actually allocated. A failed growth releases the physical reservations before
anything is staged.

## Object accounting lifecycle

`TenantQuotaCharge` is the only tenant-specific state stored in an object
record. It follows the existing catalog lifecycle:

```text
QuotaReservationGuard
        │ stage (ownership transfer)
        ▼
Reserved ── publish ──▶ Committed ── retire ──▶ Retiring
   │                        │                       │
   └ revoke/timeout ───────▶ Released ◀── physical reclaim/drop
```

- reserve increments `demand` and `reserved`;
- publish moves bytes from `reserved` to `used` without changing `demand`;
- revoke or pending timeout removes `reserved` and `demand`;
- retire keeps `used`/`demand` charged and records the retiring subset;
- physical reclaim removes `used`, `retiring`, and `demand`.

Explicit transitions maintain precise snapshots. Before stage,
`QuotaReservationGuard::drop` rolls back an untransferred reservation; after
stage, the owning catalog node's `Drop` finalizes its compact one-`Arc`
`TenantQuotaCharge`. This covers error paths and catalog shutdown without
making every charge carry duplicate class/size fields. Already-admitted pending
writes are not cancelled when policy or capacity shrinks.

## Scoped reclamation

If `demand - retiring` exceeds effective quota, maintenance submits a
`ReclaimFilter::Scope { namespace, replica_class }` debt. The collector scans the
existing global young/protected queues with its normal bounded budget; v1 does
not add per-tenant queues. Non-matching candidates are returned to the same
generation, and matching objects still obey second-chance and lease rules.
Accounting remains charged until the final object handle is gone and physical
replicas are released.

Deleting a tenant first closes admission and succeeds only when both class
demands are zero. Revoked pending records no longer make the tenant logically
non-empty while their physical reservations finish asynchronous cleanup.

## RPC behavior and scope

`ObjectCatalogRpcService::new` is the compatible single-tenant constructor and
produces the default `ObjectCatalogRpcService<ObjectManager>` specialization.
`ObjectCatalogRpcService::with_tenants` produces the
`ObjectCatalogRpcService<TenantObjectManager>` specialization and enables
tenant resolution and quota. A private batch-backend trait gives both
specializations one statically dispatched RPC implementation without exposing
a quota-bypassing common core interface.
Invalid or unknown tenant IDs produce one error per batch item. Existing IDL
codes are used:

- `INVALID_PARAMS` for an empty/invalid ID;
- `TENANT_NOT_REGISTERED` for an unknown, deleted, or stale tenant;
- `TENANT_QUOTA_EXCEEDED` for admission failure.

The trusted `tenant_id` field is an isolation key, not authentication. There is
no tenant HTTP/RPC administration endpoint or policy persistence in v1; Rust
domain callers use `upsert_tenant`, `delete_tenant`, `tenant_snapshot`, and
`list_tenants`. Mixed-class objects, group semantics, and LocalSSD quota remain
unsupported.

Performance regressions are checked for the raw path, the quota-free
`TenantConfig::Single` façade, one hot tenant, and four tenants at batch sizes
333 and 1. The façade row separates API/batch costs from quota accounting. Put
modes use identical key streams and rotate execution order each round so hash
distribution, frequency drift, and thermal drift are not consistently charged
to one mode.

The direct-path benchmark accepts `put_items`, `lookup_items`, and `rounds`:

```bash
taskset -c 2 cargo run --release -p cakemaster-server \
  --bin tenant_quota_benchmark -- 250000 1000000 11
```

The optional `breakdown` mode reports total, start, and finish nanoseconds per
item for batch put:

```bash
taskset -c 2 cargo run --release -p cakemaster-server \
  --bin tenant_quota_benchmark -- 250000 1 7 breakdown
```

On a Ryzen 7 9700X pinned to CPU 2, the median deltas from three independent
processes (11 within-process rounds, 250K puts, 1M lookups) were:

| batch | operation | multi-1 vs raw | multi-4 vs raw |
|---:|---|---:|---:|
| 333 | get | -0.56% | -0.56% |
| 333 | exists | +0.25% | +0.25% |
| 333 | put | -2.23% | -1.46% |
| 333 | put p99 | -0.55% | +0.04% |
| 1 | get | -1.94% | -2.33% |
| 1 | exists | -7.27% | -7.63% |
| 1 | put | -0.17% | -0.39% |

Batch 1 intentionally pays one tenant-generation check per object. The normal
batch path amortizes that check and the initial quota CAS across the batch.
Samply attribution showed the remaining put cost is dominated by the required
per-object `Arc` ownership of the RAII charge; publish accounting itself is
about 3–5 ns per object.

It pre-resolves tenant handles so the result isolates generation validation,
quota admission, accounting, and object operations. RPC tenant string lookup is
performed once per batch and should be measured separately at the wire level.
