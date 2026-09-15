# Memory SegmentPool

The current implementation supports only Memory segments. Shared CXL arenas,
NoF namespace allocators, LocalSSD capacity reporting, and offload permits/leases
have been removed; no dispatch layer is retained for unintegrated backends.
Mooncake IDL variant ordering and type hashes remain unchanged. `cxl` / `nvmeof`
mounts, NoF replica requests, and disk selectors return `INVALID_PARAMS` at the
RPC boundary rather than falling back to Memory.

## Structure

```text
Catalog
├── segments[id] ──> SegmentEntry(spec, state, allocator, lifetime)
├── segments_by_owner
└── accepting snapshot: Arc<[SegmentHandle]>

SegmentHandle ── reserve ──> Reservation ──> ReplicaLease ──> Object
```

- `SegmentSpec::memory` stores identity, a memory range with a nonzero base, and
  transport. `region()` / `transport()` return values or references directly,
  no longer `Option`.
- Each segment owns a `ByteAllocator`. Address ranges cannot overlap for the
  same owner + transport; different owners may use identical virtual addresses.
  Mount validation also rejects `cxl` / `nvmeof` disguised as custom protocols.
- `SegmentPool::reserve` takes a `SegmentHandle` directly and checks pool identity
  and current state; `DirectCandidate` and other capability wrappers are no longer needed.
- Placement retains preferred names, excluded segments, free-capacity ordering,
  and Segment / Owner failure domains. Kind filtering and the Resource domain,
  which was equivalent to Segment, have been removed.
- `ReservationDescriptor` / `ReservationDescriptorRef` are aliases for range
  descriptors, no longer wrappers around a Memory/NoF enum. `ReplicaLease` holds
  a reservation directly; `DirectReplica` remains its type alias.
- The catalog maintains one accepting snapshot sorted by segment ID, with no
  per-backend index, shared resource registry, or offload snapshot.

## Lifecycle and statistics

`Accepting` allows allocations, `Quiesced` leaves placement but remains readable,
and `Removed` immediately invalidates that mount's reservations logically.
Old snapshots and new mounts with the same ID cannot bypass the incarnation
fence. Ordinary handles do not prevent unmounting; issued reservations retain
the allocator and lease, returning physical resources only after the final RAII
owner releases them.

`capacity_for(ReplicaClass::Memory)` sums accepting capacity.
`space_for(ReplicaClass::Memory)` includes both accepting and quiesced capacity
to avoid artificial watermark spikes during graceful unmount. Each segment's
capacity is counted once, without shared-resource deduplication.
`active_allocations` counts reservations still holding capacity, not object readers.

Future backends should first implement real lifecycles and RPC/I/O workflows,
then introduce the necessary abstractions, rather than adding backend variants
that cannot be used end to end.
