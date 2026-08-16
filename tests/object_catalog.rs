use cakemaster::object::error::{
    LookupError, ObjectCatalogConfigError, PublishError, PutError, RemoveError, StageError,
};
use cakemaster::object::reclamation::{CatalogTick, CollectBudget};
use cakemaster::object::{
    DirectReplica, LocalSsdReplica, NamespaceId, ObjectCatalog, ObjectCatalogConfig, ObjectCommit,
    ObjectContent, ObjectIdentity, ObjectLookup, ReplicaId, ReplicaLease, ReplicaSet,
    WriteAdmission, WriteOwner,
};
use cakemaster::segment::placement::{
    AllocationSpec, PlacementRequest, ReplicaAllocator, ReplicaPolicy,
};
use cakemaster::segment::{
    ClientId, MemoryRegion, ReplicaClass, SegmentId, SegmentIdentity, SegmentPool,
    SegmentPoolConfig, SegmentSpec, TransportEndpoint, TransportProtocol,
};
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;

const OWNER: ClientId = ClientId::new(17, 23);
const SEGMENT_ID: SegmentId = SegmentId::new(9, 1);

fn pool(capacity: u64, allocator_nodes: u32) -> Arc<SegmentPool> {
    let pool = Arc::new(SegmentPool::with_config(SegmentPoolConfig::new(allocator_nodes)).unwrap());
    pool.attach(SegmentSpec::memory(
        SegmentIdentity::new(SEGMENT_ID, OWNER, "catalog-memory"),
        MemoryRegion::new(0x2_0000_0000, capacity),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
    ))
    .unwrap();
    pool
}

fn identity(key: impl Into<Arc<str>>) -> ObjectIdentity {
    ObjectIdentity::new(NamespaceId::DEFAULT, key)
}

fn owner() -> WriteOwner {
    WriteOwner::new(OWNER)
}

fn admission() -> WriteAdmission {
    WriteAdmission::unmanaged(OWNER)
}

fn replica(pool: &SegmentPool, bytes: u64) -> ReplicaSet {
    ReplicaSet::one(ReplicaLease::Direct(DirectReplica::new(
        ReplicaId::new(1),
        pool.reserve_on(SEGMENT_ID, bytes).unwrap(),
    )))
}

#[test]
fn rejects_inconsistent_lease_configuration_with_context() {
    assert_eq!(
        ObjectCatalog::with_config(ObjectCatalogConfig::new(16).with_lease(10, 11)).err(),
        Some(ObjectCatalogConfigError::LeaseRefreshExceedsTtl {
            lease_ttl_ticks: 10,
            lease_refresh_ticks: 11,
        })
    );
}

#[test]
fn replica_set_preserves_inline_and_multiple_replica_views() {
    let pool = pool(1 << 20, 64);
    let inline = replica(&pool, 1024);
    assert_eq!(inline.len(), 1);
    assert_eq!(inline.reserved_bytes(), 1024);
    assert_eq!(inline.replicas()[0].id(), ReplicaId::new(1));
    drop(inline);

    let multiple = ReplicaSet::new([
        ReplicaLease::Direct(DirectReplica::new(
            ReplicaId::new(1),
            pool.reserve_on(SEGMENT_ID, 1024).unwrap(),
        )),
        ReplicaLease::Direct(DirectReplica::new(
            ReplicaId::new(2),
            pool.reserve_on(SEGMENT_ID, 2048).unwrap(),
        )),
    ]);
    assert_eq!(multiple.len(), 2);
    assert_eq!(multiple.reserved_bytes(), 3072);
    assert_eq!(multiple.replicas()[1].id(), ReplicaId::new(2));
    drop(multiple);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 0);
}

#[test]
fn replica_set_preserves_nof_reservations_as_nof_replicas() {
    let pool = pool(1 << 20, 64);
    let nof_id = SegmentId::new(9, 2);
    pool.attach(SegmentSpec::nof(
        SegmentIdentity::new(nof_id, OWNER, "catalog-nof"),
        MemoryRegion::new(0, 1 << 20),
        "nvme://10.0.0.1/nqn.1",
    ))
    .unwrap();

    let reservations = ReplicaAllocator::new(pool.clone())
        .reserve(
            &PlacementRequest::new(AllocationSpec::new(4096), ReplicaPolicy::new(1))
                .for_replica_class(ReplicaClass::Nof),
        )
        .unwrap();
    let replicas = ReplicaSet::from_reservations(reservations);
    let nof = replicas.replicas()[0].nof().unwrap();
    assert_eq!(nof.segment_id(), nof_id);
    assert_eq!(nof.descriptor().region().base(), 0);
    drop(replicas);
    assert_eq!(pool.stats(nof_id).unwrap().usage.active_allocations, 0);
}

#[test]
fn pending_object_reclamation_releases_local_ssd_capacity() {
    let pool = pool(1 << 20, 64);
    let local_id = SegmentId::new(9, 3);
    let candidate = pool
        .attach(SegmentSpec::local_ssd(
            SegmentIdentity::new(local_id, OWNER, "catalog-local-ssd"),
            true,
        ))
        .unwrap()
        .offload_target()
        .expect("LocalSSD attachment must expose an offload target");
    pool.report_local_ssd_capacity(OWNER, local_id, 1 << 20)
        .unwrap();
    let lease = pool
        .admit_offload(&candidate, 4096)
        .unwrap()
        .commit("file://catalog-local/object")
        .unwrap();
    let replicas = ReplicaSet::one(ReplicaLease::LocalSsd(LocalSsdReplica::new(
        ReplicaId::new(1),
        lease,
    )));
    let local_replica = replicas.replicas()[0].local_ssd().unwrap();
    assert_eq!(local_replica.segment_id(), local_id);
    assert_eq!(local_replica.capacity_bytes(), 4096);
    assert_eq!(
        local_replica.descriptor().transport_endpoint(),
        "file://catalog-local/object"
    );

    let catalog = ObjectCatalog::with_config(
        ObjectCatalogConfig::new(16)
            .with_pending_timeout(1)
            .with_empty_slot_grace(1),
    )
    .unwrap();
    let ticket = catalog
        .claim_put(
            identity("local-ssd-pending"),
            admission(),
            CatalogTick::ZERO,
        )
        .unwrap()
        .stage(ObjectContent::new(4096), replicas)
        .unwrap();
    drop(ticket);
    assert_eq!(candidate.local_ssd_stats().unwrap().committed_bytes, 4096);

    let report = catalog.collect_step(CatalogTick::new(1), CollectBudget::new(8, 8, 0));
    assert_eq!(report.expired_pending, 1);
    assert_eq!(report.reclaimed_objects, 1);
    assert_eq!(candidate.local_ssd_stats().unwrap().committed_bytes, 0);
    assert_eq!(candidate.stats().usage.active_allocations, 0);
}

#[test]
fn dropped_claim_reopens_the_stable_slot() {
    let catalog = ObjectCatalog::with_config(
        ObjectCatalogConfig::new(16)
            .with_empty_slot_grace(2)
            .with_pending_timeout(10),
    )
    .unwrap();

    let first = catalog
        .claim_put(identity("same-key"), admission(), CatalogTick::new(1))
        .unwrap();
    assert!(matches!(
        catalog.claim_put(identity("same-key"), admission(), CatalogTick::new(1)),
        Err(PutError::WriteInProgress)
    ));
    let first_id = first.id();
    drop(first);

    let second = catalog
        .claim_put(identity("same-key"), admission(), CatalogTick::new(2))
        .unwrap();
    assert!(second.id().generation() > first_id.generation());
    drop(second);

    let report = catalog.collect_step(CatalogTick::new(4), CollectBudget::new(0, 0, 8));
    assert_eq!(report.removed_empty_slots, 1);
    assert_eq!(catalog.stats().slots, 0);
}

#[test]
fn publish_lookup_remove_and_reclaim_preserve_lease_and_ownership() {
    let pool = pool(1 << 20, 4096);
    let catalog = ObjectCatalog::with_config(
        ObjectCatalogConfig::new(16)
            .with_lease(10, 5)
            .with_empty_slot_grace(2),
    )
    .unwrap();
    let object = identity("published");
    let ticket = catalog
        .claim_put(object.clone(), admission(), CatalogTick::ZERO)
        .unwrap()
        .stage(ObjectContent::new(4096), replica(&pool, 4096))
        .unwrap();

    assert!(matches!(
        catalog.get(object.as_lookup(), CatalogTick::ZERO),
        Err(LookupError::NotReady)
    ));
    let published = catalog
        .publish(&ticket, ObjectCommit::new(Some(0xfeed_beef)))
        .unwrap();
    drop(
        catalog
            .publish(&ticket, ObjectCommit::new(Some(0xfeed_beef)))
            .unwrap(),
    );
    assert!(matches!(
        catalog.publish(&ticket, ObjectCommit::new(Some(7))),
        Err(PublishError::CommitConflict)
    ));
    assert_eq!(published.identity(), &object);
    assert_eq!(published.content().logical_bytes(), 4096);
    assert_eq!(published.commit().checksum(), Some(0xfeed_beef));
    assert_eq!(published.owner(), owner());

    let read = catalog
        .get(object.as_lookup(), CatalogTick::new(1))
        .unwrap();
    assert_eq!(read.lease_expires_at(), CatalogTick::new(11));
    assert_eq!(read.object().identity(), &object);
    assert_eq!(
        catalog.remove(object.as_lookup(), CatalogTick::new(1)),
        Err(RemoveError::Leased {
            expires_at: CatalogTick::new(11)
        })
    );

    drop(read);
    drop(published);
    drop(ticket);
    catalog
        .remove(object.as_lookup(), CatalogTick::new(11))
        .unwrap();
    let report = catalog.collect_step(CatalogTick::new(11), CollectBudget::new(0, 8, 0));
    assert_eq!(report.reclaimed_objects, 1);
    assert_eq!(report.reclaimed_bytes, 4096);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 0);
    assert_eq!(catalog.stats().published_objects, 0);
    assert_eq!(catalog.stats().retired_bytes, 0);

    let report = catalog.collect_step(CatalogTick::new(13), CollectBudget::new(0, 0, 8));
    assert_eq!(report.removed_empty_slots, 1);
    assert_eq!(catalog.stats().slots, 0);
}

#[test]
fn pending_timeout_reclaims_memory_without_a_batch_pause() {
    let pool = pool(1 << 20, 4096);
    let catalog = ObjectCatalog::with_config(
        ObjectCatalogConfig::new(16)
            .with_pending_timeout(5)
            .with_empty_slot_grace(1),
    )
    .unwrap();
    let object = identity("abandoned-write");
    let ticket = catalog
        .claim_put(object.clone(), admission(), CatalogTick::ZERO)
        .unwrap()
        .stage(ObjectContent::new(1024), replica(&pool, 1024))
        .unwrap();
    drop(ticket);

    let report = catalog.collect_step(CatalogTick::new(5), CollectBudget::new(8, 8, 0));
    assert_eq!(report.expired_pending, 1);
    assert_eq!(report.reclaimed_objects, 1);
    assert_eq!(catalog.stats().pending_objects, 0);
    assert_eq!(catalog.stats().retired_bytes, 0);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 0);
    assert!(matches!(
        catalog.get(object.as_lookup(), CatalogTick::new(5)),
        Err(LookupError::NotFound)
    ));
}

#[test]
fn pending_expiration_has_no_future_deadline_head_of_line_blocking() {
    let pool = pool(1 << 20, 4096);
    let catalog =
        ObjectCatalog::with_config(ObjectCatalogConfig::new(16).with_pending_timeout(10)).unwrap();
    let future = catalog
        .claim_put(identity("future"), admission(), CatalogTick::new(100))
        .unwrap()
        .stage(ObjectContent::new(1024), replica(&pool, 1024))
        .unwrap();
    let expired = catalog
        .claim_put(identity("expired"), admission(), CatalogTick::ZERO)
        .unwrap()
        .stage(ObjectContent::new(1024), replica(&pool, 1024))
        .unwrap();
    drop(future);
    drop(expired);

    let report = catalog.collect_step(CatalogTick::new(10), CollectBudget::new(2, 2, 0));
    assert_eq!(report.expired_pending, 1);
    assert_eq!(report.reclaimed_objects, 1);
    assert_eq!(catalog.stats().pending_objects, 1);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 1);

    drop(catalog);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 0);
}

#[test]
fn invalid_staging_rolls_back_claim_and_reservation_by_raii() {
    let pool = pool(1 << 20, 4096);
    let catalog = ObjectCatalog::new();
    let object = identity("undersized");
    let result = catalog
        .claim_put(object.clone(), admission(), CatalogTick::ZERO)
        .unwrap()
        .stage(ObjectContent::new(4096), replica(&pool, 1024));

    assert!(matches!(
        result,
        Err(StageError::ReplicaTooSmall {
            replica,
            required_bytes: 4096,
            capacity_bytes: 1024,
        }) if replica == ReplicaId::new(1)
    ));
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 0);
    assert_eq!(catalog.stats().claims, 0);
    assert!(
        catalog
            .claim_put(object, admission(), CatalogTick::new(1))
            .is_ok()
    );
}

#[test]
fn invalidated_segment_replicas_cannot_be_staged_or_published() {
    let pool = pool(1 << 20, 4096);
    let catalog = ObjectCatalog::new();
    let publish_object = identity("invalidate-before-publish");
    let stage_object = identity("invalidate-before-stage");

    let ticket = catalog
        .claim_put(publish_object, admission(), CatalogTick::ZERO)
        .unwrap()
        .stage(ObjectContent::new(1024), replica(&pool, 1024))
        .unwrap();
    let claim = catalog
        .claim_put(stage_object, admission(), CatalogTick::ZERO)
        .unwrap();
    let unstaged_replicas = replica(&pool, 1024);

    pool.quiesce(OWNER, SEGMENT_ID).unwrap();
    pool.remove(OWNER, SEGMENT_ID).unwrap();

    assert!(matches!(
        catalog.publish(&ticket, ObjectCommit::new(None)),
        Err(PublishError::ReplicasInvalidated)
    ));
    assert!(matches!(
        claim.stage(ObjectContent::new(1024), unstaged_replicas),
        Err(StageError::ReplicaInvalidated { replica }) if replica == ReplicaId::new(1)
    ));
}

#[test]
fn reclamation_promotes_recent_objects_and_defers_pinned_resources() {
    let pool = pool(1 << 20, 4096);
    let catalog = ObjectCatalog::with_config(
        ObjectCatalogConfig::new(16)
            .with_lease(5, 2)
            .with_empty_slot_grace(1),
    )
    .unwrap();
    let hot = identity("hot");
    let cold = identity("cold");

    let hot_ticket = catalog
        .claim_put(hot.clone(), admission(), CatalogTick::ZERO)
        .unwrap()
        .stage(ObjectContent::new(4096), replica(&pool, 4096))
        .unwrap();
    let hot_handle = catalog
        .publish(&hot_ticket, ObjectCommit::default())
        .unwrap();
    let cold_ticket = catalog
        .claim_put(cold.clone(), admission(), CatalogTick::ZERO)
        .unwrap()
        .stage(ObjectContent::new(4096), replica(&pool, 4096))
        .unwrap();
    let cold_handle = catalog
        .publish(&cold_ticket, ObjectCommit::default())
        .unwrap();

    let pinned_hot = catalog.get(hot.as_lookup(), CatalogTick::ZERO).unwrap();
    drop(hot_ticket);
    drop(hot_handle);
    drop(cold_ticket);
    drop(cold_handle);

    catalog.request_reclaim(4096);
    let report = catalog.collect_step(CatalogTick::new(5), CollectBudget::new(8, 8, 0));
    assert_eq!(report.scanned_candidates, 2);
    assert_eq!(report.retired_objects, 1);
    assert_eq!(report.reclaimed_objects, 1);
    assert_eq!(catalog.stats().published_objects, 1);
    assert!(matches!(
        catalog.get(cold.as_lookup(), CatalogTick::new(5)),
        Err(LookupError::NotFound)
    ));

    // The read handle is an epoch pin: retirement detaches the object after
    // its lease expires, but reclamation cannot return the reservation yet.
    catalog.request_reclaim(4096);
    let report = catalog.collect_step(CatalogTick::new(10), CollectBudget::new(8, 8, 0));
    assert_eq!(report.retired_objects, 1);
    assert_eq!(report.reclaimed_objects, 0);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 1);
    assert_eq!(catalog.stats().retired_bytes, 4096);

    drop(pinned_hot);
    let report = catalog.collect_step(CatalogTick::new(11), CollectBudget::new(8, 8, 0));
    assert_eq!(report.reclaimed_objects, 1);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 0);
}

#[test]
fn high_concurrency_put_get_and_incremental_collection_leave_no_resources() {
    let pool = pool(32 << 20, 32 * 1024);
    let catalog = Arc::new(
        ObjectCatalog::with_config(
            ObjectCatalogConfig::new(16 * 1024)
                .with_lease(8, 4)
                .with_pending_timeout(10_000)
                .with_empty_slot_grace(4),
        )
        .unwrap(),
    );
    let workers = thread::available_parallelism()
        .map_or(4, usize::from)
        .clamp(4, 8);
    let operations_per_worker = 2_000;
    let barrier = Arc::new(Barrier::new(workers + 2));

    thread::scope(|scope| {
        for worker in 0..workers {
            let catalog = catalog.clone();
            let pool = pool.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                barrier.wait();
                for operation in 0..operations_per_worker {
                    let sequence = worker * operations_per_worker + operation;
                    if operation & 1 == 0 {
                        let key = format!("object-{sequence}");
                        let ticket = catalog
                            .claim_put(
                                identity(key),
                                admission(),
                                CatalogTick::new(operation as u64),
                            )
                            .unwrap()
                            .stage(ObjectContent::new(64), replica(&pool, 64))
                            .unwrap();
                        let handle = catalog
                            .publish(&ticket, ObjectCommit::default())
                            .expect("the ticket has one serialized publisher");
                        black_box(handle.identity());
                    } else {
                        let prior = sequence - 1;
                        let key = format!("object-{prior}");
                        let _ = black_box(catalog.get(
                            ObjectLookup::new(NamespaceId::DEFAULT, &key),
                            CatalogTick::new(operation as u64),
                        ));
                    }
                }
            });
        }

        let catalog = catalog.clone();
        let collector_barrier = barrier.clone();
        scope.spawn(move || {
            collector_barrier.wait();
            for tick in 0..operations_per_worker {
                catalog.request_reclaim(16 * 1024);
                black_box(
                    catalog
                        .collect_step(CatalogTick::new(tick as u64), CollectBudget::new(16, 16, 4)),
                );
            }
        });

        barrier.wait();
    });

    assert_eq!(catalog.stats().claims, 0);
    assert_eq!(catalog.stats().pending_objects, 0);
    catalog.request_reclaim(u64::MAX);
    for _ in 0..4 {
        catalog.collect_step(
            CatalogTick::new(u64::MAX),
            CollectBudget::new(usize::MAX, usize::MAX, usize::MAX),
        );
    }
    let stats = catalog.stats();
    assert_eq!(stats.published_objects, 0);
    assert_eq!(stats.live_bytes, 0);
    assert_eq!(stats.retired_bytes, 0);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 0);
}
