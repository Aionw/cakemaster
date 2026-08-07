use cakemaster::segment::{
    AllocationSpec, AttachError, AttachOutcome, ClientId, FailureDomain, FulfillmentPolicy,
    LifecycleError, MemoryRegion, MemorySegmentSpec, PlacementConstraints, PlacementError,
    PlacementRequest, ReplicaAllocator, ReplicaPolicy, ReserveError, SegmentId, SegmentIdentity,
    SegmentPool, SegmentPoolConfig, SegmentState, SegmentTopology, TransportEndpoint,
    TransportProtocol,
};
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const OWNER: ClientId = ClientId::new(7, 11);
const CAPACITY: u64 = 8 * 1024 * 1024;

fn pool() -> SegmentPool {
    SegmentPool::with_config(SegmentPoolConfig::new(4096)).unwrap()
}

fn spec(index: u64, name: &str, host: &str) -> MemorySegmentSpec {
    MemorySegmentSpec::new(
        SegmentIdentity::new(SegmentId::new(1, index), OWNER, name),
        MemoryRegion::new(0x1_0000_0000 + index * (CAPACITY * 2), CAPACITY),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
    )
    .with_topology(SegmentTopology::on_host(host))
}

#[test]
fn protocol_is_typed_in_core_and_extensible_at_the_wire_boundary() {
    assert_eq!("tcp".parse(), Ok(TransportProtocol::Tcp));
    assert_eq!("rdma".parse(), Ok(TransportProtocol::Rdma));
    assert_eq!("cxl".parse(), Ok(TransportProtocol::Cxl));

    let custom: TransportProtocol = "sunrise_link".parse().unwrap();
    assert_eq!(custom.as_str(), "sunrise_link");
    assert!("TCP".parse::<TransportProtocol>().is_err());
    assert!("".parse::<TransportProtocol>().is_err());
}

#[test]
fn requests_and_segments_expose_semantic_groups() {
    let segment = spec(1, "memory-a", "host-a");
    assert_eq!(segment.identity().id(), SegmentId::new(1, 1));
    assert_eq!(segment.identity().owner(), OWNER);
    assert_eq!(segment.region().size(), CAPACITY);
    assert_eq!(segment.topology().host_id(), Some("host-a"));

    let excluded = SegmentId::new(9, 9);
    let request = PlacementRequest::new(
        AllocationSpec::new(4096),
        ReplicaPolicy::new(3).across(FailureDomain::Host),
    )
    .constrained_by(
        PlacementConstraints::default()
            .with_preferred_names(["memory-a"])
            .excluding([excluded]),
    );

    assert_eq!(request.allocation().bytes(), 4096);
    assert_eq!(request.replicas().count(), 3);
    assert_eq!(request.replicas().failure_domain(), FailureDomain::Host);
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
    let first = spec(1, "memory-a", "host-a");

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

    let conflict = MemorySegmentSpec::new(
        SegmentIdentity::new(first.identity().id(), OWNER, "different-name"),
        first.region(),
        first.transport().clone(),
    );
    assert_eq!(
        pool.attach(conflict).unwrap_err(),
        AttachError::ConflictingSegmentId(first.identity().id())
    );

    let overlap = MemorySegmentSpec::new(
        SegmentIdentity::new(SegmentId::new(1, 2), OWNER, "overlap"),
        MemoryRegion::new(first.region().base() + 4096, first.region().size()),
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
fn reservation_owns_the_range_and_releases_it_on_drop() {
    let pool = pool();
    let candidate = pool.attach(spec(1, "memory-a", "host-a")).unwrap();
    let candidate = candidate.candidate();

    let first = pool.reserve(candidate, 1024).unwrap();
    let second = pool.reserve(candidate, 512).unwrap();
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
    assert_eq!(stats.reservations.live, 2);

    drop(first);
    drop(second);
    let stats = candidate.stats();
    assert_eq!(stats.space.used_bytes, 0);
    assert_eq!(stats.space.available_bytes, stats.space.capacity_bytes);
    assert_eq!(stats.reservations.live, 0);

    let whole_segment = pool.reserve(candidate, CAPACITY).unwrap();
    assert_eq!(whole_segment.offset(), 0);
}

#[test]
fn quiesce_invalidates_stale_candidates_and_remove_waits_for_handles() {
    let pool = pool();
    let segment = spec(1, "memory-a", "host-a");
    let id = segment.identity().id();
    let candidate = pool.attach(segment).unwrap().candidate().clone();
    let reservation = pool.reserve(&candidate, 4096).unwrap();
    assert_eq!(candidate.stats().state, SegmentState::Accepting);

    assert_eq!(
        pool.remove(OWNER, id),
        Err(LifecycleError::StillAccepting(id))
    );
    pool.quiesce(OWNER, id).unwrap();
    assert_eq!(candidate.stats().state, SegmentState::Quiesced);
    assert!(pool.snapshot().is_empty());
    assert_eq!(
        pool.reserve(&candidate, 64).unwrap_err(),
        ReserveError::NotAccepting(id)
    );
    assert_eq!(
        pool.remove(OWNER, id),
        Err(LifecycleError::Busy {
            segment: id,
            live_allocations: 1
        })
    );

    drop(reservation);
    pool.remove(OWNER, id).unwrap();
    assert_eq!(candidate.stats().state, SegmentState::Removed);
    assert!(pool.candidate(id).is_none());
    assert_eq!(
        pool.reserve(&candidate, 64).unwrap_err(),
        ReserveError::NotAccepting(id)
    );
}

#[test]
fn exact_placement_enforces_failure_domains_and_rolls_back_partial_work() {
    let same_host_pool = Arc::new(pool());
    same_host_pool
        .attach(spec(1, "memory-a", "host-a"))
        .unwrap();
    same_host_pool
        .attach(spec(2, "memory-b", "host-a"))
        .unwrap();
    let allocator = ReplicaAllocator::new(same_host_pool.clone());
    let exact = PlacementRequest::new(
        AllocationSpec::new(4096),
        ReplicaPolicy::new(2).across(FailureDomain::Host),
    );

    assert_eq!(
        allocator.reserve(&exact).unwrap_err(),
        PlacementError::InsufficientReplicas {
            requested: 2,
            allocated: 1
        }
    );
    for candidate in same_host_pool.snapshot().iter() {
        assert_eq!(candidate.stats().reservations.live, 0);
    }

    same_host_pool
        .attach(spec(3, "memory-c", "host-b"))
        .unwrap();
    let reservations = allocator.reserve(&exact).unwrap();
    assert_eq!(reservations.len(), 2);
    let hosts: std::collections::HashSet<_> = reservations
        .iter()
        .map(|reservation| {
            same_host_pool
                .candidate(reservation.segment_id())
                .unwrap()
                .spec()
                .topology()
                .host_id()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(hosts.len(), 2);

    let best_effort = PlacementRequest::new(
        AllocationSpec::new(CAPACITY),
        ReplicaPolicy::new(4).across(FailureDomain::Host),
    )
    .with_fulfillment(FulfillmentPolicy::BestEffort);
    assert!(allocator.reserve(&best_effort).unwrap().len() < 4);
}

#[test]
fn candidates_cannot_cross_pool_boundaries() {
    let first_pool = pool();
    let second_pool = pool();
    let candidate = first_pool.attach(spec(1, "memory-a", "host-a")).unwrap();
    assert_eq!(
        second_pool.reserve(candidate.candidate(), 64).unwrap_err(),
        ReserveError::ForeignCandidate
    );
}

#[test]
fn high_concurrency_reserve_drop_preserves_allocator_invariants() {
    let pool = Arc::new(pool());
    let segment_count = 8_u64;
    for index in 0..segment_count {
        pool.attach(spec(index + 1, "memory", &format!("host-{index}")))
            .unwrap();
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
        assert_eq!(stats.reservations.live, 0);
        assert_eq!(stats.space.available_bytes, stats.space.capacity_bytes);
    }
}
