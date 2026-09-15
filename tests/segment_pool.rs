use cakemaster::segment::error::{
    AttachError, ParseTransportProtocolError, ReserveError, SegmentStateError,
};
use cakemaster::segment::placement::{
    AllocationSpec, FailureDomain, FulfillmentPolicy, PlacementConstraints, PlacementError,
    PlacementRequest, ReplicaAllocator, ReplicaPolicy,
};
use cakemaster::segment::stats::SegmentState;
use cakemaster::segment::{
    AttachOutcome, ClientId, MemoryRegion, ReplicaClass, SegmentId, SegmentIdentity, SegmentPool,
    SegmentPoolConfig, SegmentSpec, TransportEndpoint, TransportProtocol,
};
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const OWNER: ClientId = ClientId::new(7, 11);
const OTHER_OWNER: ClientId = ClientId::new(7, 12);
const CAPACITY: u64 = 8 * 1024 * 1024;

fn pool() -> SegmentPool {
    SegmentPool::with_config(SegmentPoolConfig::new(4096)).unwrap()
}

fn spec(index: u64, name: &str) -> SegmentSpec {
    owned_spec(index, name, OWNER)
}

fn owned_spec(index: u64, name: &str, owner: ClientId) -> SegmentSpec {
    SegmentSpec::memory(
        SegmentIdentity::new(SegmentId::new(1, index), owner, name),
        MemoryRegion::new(0x1_0000_0000 + index * (CAPACITY * 2), CAPACITY),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
    )
}

#[test]
fn protocol_is_typed_in_core_and_extensible_at_the_wire_boundary() {
    assert_eq!("tcp".parse(), Ok(TransportProtocol::Tcp));
    assert_eq!("rdma".parse(), Ok(TransportProtocol::Rdma));
    assert_eq!("cxl".parse(), Ok(TransportProtocol::Cxl));
    assert_eq!("nvmeof".parse(), Ok(TransportProtocol::NvmeOf));

    let custom: TransportProtocol = "sunrise_link".parse().unwrap();
    assert_eq!(custom.as_str(), "sunrise_link");
    assert!("TCP".parse::<TransportProtocol>().is_err());
    assert!("".parse::<TransportProtocol>().is_err());

    let invalid_protocol = Arc::<str>::from("TCP");
    let invalid_spec = SegmentSpec::memory(
        SegmentIdentity::new(SegmentId::new(1, 1), OWNER, "invalid-protocol"),
        MemoryRegion::new(0x1000, 4096),
        TransportEndpoint::new(
            TransportProtocol::Custom(invalid_protocol.clone()),
            "endpoint",
        ),
    );
    assert_eq!(
        pool().attach(invalid_spec).unwrap_err(),
        AttachError::InvalidTransportProtocol {
            protocol: invalid_protocol,
            source: ParseTransportProtocolError,
        }
    );

    let cxl_as_memory = SegmentSpec::memory(
        SegmentIdentity::new(SegmentId::new(1, 2), OWNER, "misclassified-cxl"),
        MemoryRegion::new(0x2000, 4096),
        TransportEndpoint::new(TransportProtocol::Cxl, "cxl-client"),
    );
    assert_eq!(
        pool().attach(cxl_as_memory).unwrap_err(),
        AttachError::IncompatibleTransportProtocol {
            protocol: TransportProtocol::Cxl,
        }
    );
}

#[test]
fn requests_and_segments_expose_semantic_groups() {
    let segment = spec(1, "memory-a");
    assert_eq!(segment.identity().id(), SegmentId::new(1, 1));
    assert_eq!(segment.identity().owner(), OWNER);
    assert_eq!(segment.region().size(), CAPACITY);

    let pool = pool();
    let candidate = pool.attach(segment.clone()).unwrap().segment().clone();
    assert_eq!(candidate.replica_class(), ReplicaClass::Memory);
    assert_eq!(pool.snapshot().replica_class(), ReplicaClass::Memory);

    let excluded = SegmentId::new(9, 9);
    let request = PlacementRequest::new(
        AllocationSpec::new(4096),
        ReplicaPolicy::new(3).across(FailureDomain::Owner),
    )
    .constrained_by(
        PlacementConstraints::default()
            .with_preferred_names(["memory-a"])
            .excluding([excluded]),
    );

    assert_eq!(request.allocation().bytes(), 4096);
    assert_eq!(request.replicas().count(), 3);
    assert_eq!(request.replicas().failure_domain(), FailureDomain::Owner);
    assert_eq!(request.replica_class(), ReplicaClass::Memory);
    assert_eq!(request.fulfillment(), FulfillmentPolicy::AllOrNothing);
    assert_eq!(
        request.constraints().preferred_names()[0].as_ref(),
        "memory-a"
    );
    assert!(
        request
            .constraints()
            .excluded_segments()
            .contains(&excluded)
    );
}

#[test]
fn attach_is_idempotent_but_rejects_conflicts_and_overlaps() {
    let pool = pool();
    let first = spec(1, "memory-a");

    assert!(matches!(
        pool.attach(first.clone()).unwrap(),
        AttachOutcome::Attached(_)
    ));
    let generation = pool.snapshot().generation();
    assert!(matches!(
        pool.attach(first.clone()).unwrap(),
        AttachOutcome::AlreadyAttached(_)
    ));
    assert_eq!(pool.snapshot().generation(), generation);

    let conflict = SegmentSpec::memory(
        SegmentIdentity::new(first.identity().id(), OWNER, "different-name"),
        first.region(),
        first.transport().clone(),
    );
    assert_eq!(
        pool.attach(conflict).unwrap_err(),
        AttachError::ConflictingSegmentId(first.identity().id())
    );

    let first_region = first.region();
    let overlap = SegmentSpec::memory(
        SegmentIdentity::new(SegmentId::new(1, 2), OWNER, "overlap"),
        MemoryRegion::new(first_region.base() + 4096, first_region.size()),
        first.transport().clone(),
    );
    assert_eq!(
        pool.attach(overlap).unwrap_err(),
        AttachError::OverlappingAddressRange {
            existing: first.identity().id()
        }
    );
    assert_eq!(pool.len(), 1);
}

#[test]
fn quiesced_attach_stays_hidden_until_atomic_batch_reactivation() {
    let pool = pool();
    let first = spec(1, "memory-a");
    let first_id = first.identity().id();
    let second = owned_spec(2, "memory-b", OTHER_OWNER);
    let second_id = second.identity().id();

    assert!(matches!(
        pool.attach_quiesced(first).unwrap(),
        AttachOutcome::Attached(_)
    ));
    pool.attach_quiesced(second).unwrap();
    assert_eq!(pool.stats(first_id).unwrap().state, SegmentState::Quiesced);
    assert_eq!(pool.stats(second_id).unwrap().state, SegmentState::Quiesced);
    assert!(pool.snapshot().is_empty());

    assert_eq!(
        pool.reactivate_many(OWNER, &[first_id, second_id]),
        Err(SegmentStateError::OwnerMismatch {
            segment: second_id,
            expected: OTHER_OWNER,
            actual: OWNER,
        })
    );
    assert_eq!(pool.stats(first_id).unwrap().state, SegmentState::Quiesced);
    assert_eq!(pool.stats(second_id).unwrap().state, SegmentState::Quiesced);
    assert!(pool.snapshot().is_empty());

    pool.reactivate_many(OWNER, &[first_id]).unwrap();
    assert_eq!(pool.stats(first_id).unwrap().state, SegmentState::Accepting);
    assert_eq!(pool.stats(second_id).unwrap().state, SegmentState::Quiesced);
    assert_eq!(pool.snapshot().len(), 1);
    assert_eq!(pool.snapshot().candidates()[0].id(), first_id);
}

#[test]
fn owner_invalidation_is_intent_level_idempotent_and_does_not_wait_for_allocations() {
    let pool = pool();
    let first = spec(1, "memory-a");
    let first_id = first.identity().id();
    let second = spec(2, "memory-b");
    let second_id = second.identity().id();
    let foreign = owned_spec(3, "memory-c", OTHER_OWNER);
    let foreign_id = foreign.identity().id();

    let first_handle = pool.attach(first).unwrap().segment().clone();
    let second_handle = pool.attach(second).unwrap().segment().clone();
    let foreign_handle = pool.attach(foreign).unwrap().segment().clone();
    let first_reservation = pool.reserve_on(first_id, 2048).unwrap();
    let reservation = pool.reserve_on(second_id, 4096).unwrap();
    assert!(first_reservation.is_live());
    assert!(reservation.is_live());

    assert_eq!(pool.invalidate_owner(OWNER), 2);
    assert_eq!(first_handle.stats().state, SegmentState::Removed);
    assert_eq!(second_handle.stats().state, SegmentState::Removed);
    assert_eq!(foreign_handle.stats().state, SegmentState::Accepting);
    assert!(!first_reservation.is_live());
    assert!(!reservation.is_live());
    assert!(pool.segment(first_id).is_none());
    assert!(pool.segment(second_id).is_none());
    assert!(pool.segment(foreign_id).is_some());
    assert_eq!(pool.len(), 1);
    assert_eq!(pool.invalidate_owner(OWNER), 0);

    drop(first_reservation);
    drop(reservation);
    assert_eq!(first_handle.stats().usage.active_allocations, 0);
    assert_eq!(second_handle.stats().usage.active_allocations, 0);
}

#[test]
fn batch_owner_invalidation_deduplicates_owners_and_updates_placement_once() {
    let pool = pool();
    let first = spec(1, "memory-a");
    let first_id = first.identity().id();
    let second = owned_spec(2, "memory-b", OTHER_OWNER);
    let second_id = second.identity().id();
    pool.attach(first).unwrap();
    pool.attach(second).unwrap();
    let generation = pool.snapshot().generation();

    assert_eq!(pool.invalidate_owners([OWNER, OWNER, OTHER_OWNER]), 2);
    assert!(pool.segment(first_id).is_none());
    assert!(pool.segment(second_id).is_none());
    assert!(pool.snapshot().generation() > generation);
    assert_eq!(pool.invalidate_owners([OWNER, OTHER_OWNER]), 0);
}

#[test]
fn reservation_owns_the_range_and_releases_it_on_drop() {
    let pool = pool();
    let candidate = pool.attach(spec(1, "memory-a")).unwrap();
    let candidate = candidate.segment().clone();

    let first = pool.reserve(&candidate, 1024).unwrap();
    let second = pool.reserve(&candidate, 512).unwrap();
    assert_eq!(first.offset(), 0);
    assert_eq!(second.offset(), 1024);
    assert_eq!(first.descriptor().region().size(), 1024);
    assert_eq!(
        first.descriptor().transport().protocol(),
        &TransportProtocol::Tcp
    );
    assert_eq!(first.descriptor().transport().endpoint(), "127.0.0.1:12345");
    assert_eq!(first.owned_descriptor(), first.descriptor().to_owned());

    let stats = candidate.stats();
    assert_eq!(stats.space.used_bytes, 1536);
    assert_eq!(stats.usage.active_allocations, 2);

    drop(first);
    drop(second);
    let stats = candidate.stats();
    assert_eq!(stats.space.used_bytes, 0);
    assert_eq!(stats.space.available_bytes, stats.space.capacity_bytes);
    assert_eq!(stats.usage.active_allocations, 0);

    let whole_segment = pool.reserve(&candidate, CAPACITY).unwrap();
    assert_eq!(whole_segment.offset(), 0);
}

#[test]
fn reservations_observe_pool_shutdown_as_incarnation_invalidation() {
    let (reservation, candidate) = {
        let pool = pool();
        let candidate = pool.attach(spec(1, "memory-a")).unwrap().segment().clone();
        let reservation = pool.reserve(&candidate, 4096).unwrap();
        assert!(reservation.is_live());
        (reservation, candidate)
    };

    assert!(!reservation.is_live());
    assert_eq!(candidate.stats().state, SegmentState::Removed);
}

#[test]
fn remove_invalidates_mount_incarnation_without_waiting_for_resource_handles() {
    let pool = pool();
    let segment = spec(1, "memory-a");
    let id = segment.identity().id();
    let candidate = pool.attach(segment).unwrap().segment().clone();
    let reservation = pool.reserve(&candidate, 4096).unwrap();
    assert!(reservation.is_live());
    assert_eq!(candidate.stats().state, SegmentState::Accepting);

    assert_eq!(
        pool.remove(OWNER, id),
        Err(SegmentStateError::StillAccepting(id))
    );
    pool.quiesce(OWNER, id).unwrap();
    assert_eq!(candidate.stats().state, SegmentState::Quiesced);
    assert!(pool.snapshot().is_empty());
    assert_eq!(
        pool.reserve(&candidate, 64).unwrap_err(),
        ReserveError::NotAccepting(id)
    );
    pool.remove(OWNER, id).unwrap();
    assert!(!reservation.is_live());
    assert_eq!(candidate.stats().state, SegmentState::Removed);
    assert_eq!(candidate.stats().usage.active_allocations, 1);
    assert!(pool.segment(id).is_none());
    assert_eq!(
        pool.reserve(&candidate, 64).unwrap_err(),
        ReserveError::NotAccepting(id)
    );

    // A new mount gets an independent incarnation immediately. The old
    // reservation may retain its allocator storage, but it remains fenced.
    let remount_id = SegmentId::new(1, 2);
    let remount_spec = SegmentSpec::memory(
        SegmentIdentity::new(remount_id, OWNER, "memory-b"),
        MemoryRegion::new(0x1_0000_0000 + CAPACITY * 2, CAPACITY),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
    );
    let remounted = pool.attach(remount_spec).unwrap().segment().clone();
    let fresh = pool.reserve(&remounted, 64).unwrap();
    assert!(fresh.is_live());
    assert_eq!(fresh.offset(), 0);
    assert!(!reservation.is_live());
    assert!(pool.segment(remount_id).is_some());

    drop(reservation);
    assert_eq!(candidate.stats().usage.active_allocations, 0);
}

#[test]
fn exact_placement_enforces_failure_domains_and_rolls_back_partial_work() {
    let same_owner_pool = Arc::new(pool());
    same_owner_pool.attach(spec(1, "memory-a")).unwrap();
    same_owner_pool.attach(spec(2, "memory-b")).unwrap();
    let allocator = ReplicaAllocator::new(same_owner_pool.clone());
    let exact = PlacementRequest::new(
        AllocationSpec::new(4096),
        ReplicaPolicy::new(2).across(FailureDomain::Owner),
    );

    assert_eq!(
        allocator.reserve(&exact).unwrap_err(),
        PlacementError::InsufficientReplicas {
            requested: 2,
            allocated: 1
        }
    );
    for candidate in same_owner_pool.snapshot().iter() {
        assert_eq!(candidate.stats().usage.active_allocations, 0);
    }

    same_owner_pool
        .attach(owned_spec(3, "memory-c", OTHER_OWNER))
        .unwrap();
    let reservations = allocator.reserve(&exact).unwrap();
    assert_eq!(reservations.len(), 2);
    let owners: std::collections::HashSet<_> = reservations
        .iter()
        .map(|reservation| {
            same_owner_pool
                .segment(reservation.segment_id())
                .unwrap()
                .spec()
                .identity()
                .owner()
        })
        .collect();
    assert_eq!(owners.len(), 2);

    let best_effort = PlacementRequest::new(
        AllocationSpec::new(CAPACITY),
        ReplicaPolicy::new(4).across(FailureDomain::Owner),
    )
    .with_fulfillment(FulfillmentPolicy::BestEffort);
    assert!(allocator.reserve(&best_effort).unwrap().len() < 4);
}

#[test]
fn candidates_cannot_cross_pool_boundaries() {
    let first_pool = pool();
    let second_pool = pool();
    let candidate = first_pool.attach(spec(1, "memory-a")).unwrap();
    assert_eq!(
        second_pool
            .reserve(&candidate.segment().clone(), 64)
            .unwrap_err(),
        ReserveError::ForeignCandidate
    );
}

#[test]
fn high_concurrency_reserve_drop_preserves_allocator_invariants() {
    let pool = Arc::new(pool());
    let segment_count = 8_u64;
    for index in 0..segment_count {
        pool.attach(spec(index + 1, "memory")).unwrap();
    }
    let snapshot = pool.snapshot();
    let threads = thread::available_parallelism()
        .map_or(8, usize::from)
        .clamp(4, 16);
    let operations_per_thread = 10_000;
    let barrier = Arc::new(Barrier::new(threads));
    let started = Instant::now();

    thread::scope(|scope| {
        for worker in 0..threads {
            let pool = pool.clone();
            let snapshot = snapshot.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                barrier.wait();
                for operation in 0..operations_per_thread {
                    let candidate = &snapshot.candidates()[(worker + operation) % snapshot.len()];
                    let size = 64_u64 << (operation & 5);
                    let reservation = pool.reserve(candidate, size).unwrap();
                    black_box(reservation.descriptor().region().base());
                    drop(reservation);
                }
            });
        }
    });

    let operations = threads * operations_per_thread;
    let elapsed = started.elapsed();
    eprintln!(
        "segment pool stress: {operations} operations in {elapsed:?} ({:.0} ops/s)",
        operations as f64 / elapsed.as_secs_f64()
    );
    for candidate in snapshot.iter() {
        let stats = candidate.stats();
        assert_eq!(stats.space.used_bytes, 0);
        assert_eq!(stats.usage.active_allocations, 0);
        assert_eq!(stats.space.available_bytes, stats.space.capacity_bytes);
    }
}

#[test]
fn accepting_memory_capacity_tracks_state_without_changing_physical_space() {
    let pool = pool();
    pool.attach(spec(1, "memory-a")).unwrap();
    pool.attach(spec(2, "memory-b")).unwrap();
    let initial = pool.snapshot();
    assert_eq!(
        pool.capacity_for(ReplicaClass::Memory).capacity_bytes(),
        2 * CAPACITY
    );
    pool.quiesce(OWNER, SegmentId::new(1, 1)).unwrap();
    assert_ne!(initial.generation(), pool.snapshot().generation());
    assert_eq!(
        pool.capacity_for(ReplicaClass::Memory).capacity_bytes(),
        CAPACITY
    );
    assert_eq!(
        pool.space_for(ReplicaClass::Memory).capacity_bytes,
        2 * CAPACITY
    );
    pool.reactivate(OWNER, SegmentId::new(1, 1)).unwrap();
    assert_eq!(
        pool.capacity_for(ReplicaClass::Memory).capacity_bytes(),
        2 * CAPACITY
    );
}

#[test]
fn memory_mount_rejects_storage_protocols_including_custom_spellings() {
    for protocol in [
        TransportProtocol::Cxl,
        TransportProtocol::NvmeOf,
        TransportProtocol::Custom(Arc::from("cxl")),
        TransportProtocol::Custom(Arc::from("nvmeof")),
    ] {
        let pool = pool();
        let memory = SegmentSpec::memory(
            spec(1, "memory").identity().clone(),
            MemoryRegion::new(0x1000, 4096),
            TransportEndpoint::new(protocol.clone(), "endpoint"),
        );
        assert_eq!(
            pool.attach(memory).unwrap_err(),
            AttachError::IncompatibleTransportProtocol { protocol }
        );
        assert!(pool.is_empty());
    }
}
