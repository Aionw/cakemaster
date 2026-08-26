use cakemaster::object::error::{LookupError, ObjectManagerError, ObjectRemoveError};
use cakemaster::object::reclamation::{CatalogTick, CollectBudget};
use cakemaster::object::{
    NamespaceId, ObjectCatalogConfig, ObjectContent, ObjectIdentity, ObjectKind, ObjectManager,
    ObjectPinRequest, ObjectPutPlan, ReplicaSelector, SoftPinAction, WriteAdmission, WriteMode,
    WriteOwner,
};
use cakemaster::segment::placement::{
    AllocationSpec, FulfillmentPolicy, PlacementRequest, ReplicaPolicy,
};
use cakemaster::segment::{
    ClientId, CxlArenaId, CxlArenaSpec, MemoryRegion, ReplicaClass, ReservationDescriptor,
    SegmentId, SegmentIdentity, SegmentPool, SegmentSpec, TransportEndpoint, TransportProtocol,
};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::Duration;

const OWNER: ClientId = ClientId::new(7, 11);
const OTHER_OWNER: ClientId = ClientId::new(7, 12);
const MEMORY_ID: SegmentId = SegmentId::new(1, 1);
const SECOND_MEMORY_ID: SegmentId = SegmentId::new(1, 2);
const FIRST_CXL_ID: SegmentId = SegmentId::new(3, 1);
const SECOND_CXL_ID: SegmentId = SegmentId::new(3, 2);
const NOF_ID: SegmentId = SegmentId::new(2, 1);
const CAPACITY: u64 = 1 << 20;

fn pool(memory: bool, nof: bool) -> Arc<SegmentPool> {
    let pool = Arc::new(SegmentPool::new());
    if memory {
        pool.attach(SegmentSpec::memory(
            SegmentIdentity::new(MEMORY_ID, OWNER, "memory-a"),
            MemoryRegion::new(0x1_0000_0000, CAPACITY),
            TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12000"),
        ))
        .unwrap();
    }
    if nof {
        pool.attach(SegmentSpec::nof(
            SegmentIdentity::new(NOF_ID, OWNER, "nof-a"),
            MemoryRegion::new(0, CAPACITY),
            "nvme://127.0.0.1/nqn.1",
        ))
        .unwrap();
    }
    pool
}

fn replicated_memory_pool() -> Arc<SegmentPool> {
    let pool = pool(true, false);
    pool.attach(SegmentSpec::memory(
        SegmentIdentity::new(SECOND_MEMORY_ID, OWNER, "memory-b"),
        MemoryRegion::new(0x2_0000_0000, CAPACITY),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12001"),
    ))
    .unwrap();
    pool
}

fn replicated_cxl_pool() -> Arc<SegmentPool> {
    let pool = Arc::new(SegmentPool::new());
    let arena = CxlArenaSpec::new(CxlArenaId::new("shared-object-arena"), CAPACITY);
    pool.attach(SegmentSpec::cxl(
        SegmentIdentity::new(FIRST_CXL_ID, OWNER, "cxl-a"),
        arena.clone(),
    ))
    .unwrap();
    pool.attach(SegmentSpec::cxl(
        SegmentIdentity::new(SECOND_CXL_ID, OWNER, "cxl-b"),
        arena,
    ))
    .unwrap();
    pool
}

fn identity(key: &str) -> ObjectIdentity {
    ObjectIdentity::new(NamespaceId::DEFAULT, key)
}

fn owner(client: ClientId) -> WriteOwner {
    WriteOwner::new(client)
}

fn admission(client: ClientId) -> WriteAdmission {
    WriteAdmission::unmanaged(client)
}

fn plan(
    bytes: u64,
    replicas: usize,
    replica_class: ReplicaClass,
    fulfillment: FulfillmentPolicy,
) -> ObjectPutPlan {
    ObjectPutPlan::new(
        ObjectContent::new(bytes).with_kind(ObjectKind::Tensor),
        PlacementRequest::new(AllocationSpec::new(bytes), ReplicaPolicy::new(replicas))
            .for_replica_class(replica_class)
            .with_fulfillment(fulfillment),
    )
}

fn memory_plan(bytes: u64) -> ObjectPutPlan {
    plan(
        bytes,
        1,
        ReplicaClass::Memory,
        FulfillmentPolicy::AllOrNothing,
    )
}

fn put_memory(manager: &ObjectManager, object: &ObjectIdentity, writer: ClientId, bytes: u64) {
    manager
        .start_put(
            object.clone(),
            admission(writer),
            memory_plan(bytes),
            CatalogTick::ZERO,
        )
        .unwrap();
    manager
        .finish_put(object, owner(writer), ReplicaSelector::All)
        .unwrap();
}

#[test]
fn manager_owns_the_complete_pending_to_published_lifecycle() {
    let pool = pool(true, false);
    let manager =
        ObjectManager::with_config(pool.clone(), ObjectCatalogConfig::new(32).with_lease(10, 5))
            .unwrap();
    let object = identity("tensor");
    let started = manager
        .start_put(
            object.clone(),
            admission(OWNER),
            plan(4096, 1, ReplicaClass::Memory, FulfillmentPolicy::BestEffort),
            CatalogTick::ZERO,
        )
        .unwrap();

    assert_eq!(started.replica_class(), ReplicaClass::Memory);
    assert_eq!(started.replicas().len(), 1);
    assert_eq!(started.replicas()[0].id().get(), 1);
    match started.replicas()[0].descriptor() {
        ReservationDescriptor::Memory(descriptor) => {
            assert_eq!(descriptor.region(), MemoryRegion::new(0x1_0000_0000, 4096));
            assert_eq!(descriptor.transport().endpoint(), "127.0.0.1:12000");
        }
        descriptor => panic!("unexpected descriptor: {descriptor:?}"),
    }
    assert!(matches!(
        manager.get(object.as_lookup(), CatalogTick::ZERO),
        Err(LookupError::NotReady)
    ));
    assert_eq!(
        manager.finish_put(&object, owner(OTHER_OWNER), ReplicaSelector::All),
        Err(ObjectManagerError::IllegalOwner)
    );
    assert_eq!(
        manager.finish_put(
            &object,
            owner(OWNER),
            ReplicaSelector::Class(ReplicaClass::Nof),
        ),
        Err(ObjectManagerError::ReplicaClassMismatch {
            requested: ReplicaClass::Nof,
            actual: ReplicaClass::Memory,
        })
    );

    manager
        .finish_put(
            &object,
            owner(OWNER),
            ReplicaSelector::Class(ReplicaClass::Memory),
        )
        .unwrap();
    let read = manager
        .get(object.as_lookup(), CatalogTick::new(1))
        .unwrap();
    assert_eq!(read.object().content().kind(), ObjectKind::Tensor);
    assert_eq!(read.object().commit().checksum(), None);
    assert_eq!(read.lease_expires_at(), CatalogTick::new(11));

    manager
        .finish_put(&object, owner(OWNER), ReplicaSelector::All)
        .unwrap();
    assert_eq!(
        manager.revoke_put(
            &object,
            owner(OWNER),
            ReplicaSelector::All,
            CatalogTick::new(2),
        ),
        Err(ObjectManagerError::InvalidWrite)
    );
    assert_eq!(pool.stats(MEMORY_ID).unwrap().usage.active_allocations, 1);
}

#[test]
fn same_size_upsert_uses_a_fresh_version_and_keeps_the_old_version_readable() {
    let pool = pool(true, false);
    let manager = ObjectManager::new(pool.clone());
    let object = identity("in-place-upsert");
    let original = manager
        .start_put(
            object.clone(),
            admission(OWNER),
            plan(
                4096,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::ZERO,
        )
        .unwrap();
    let original_descriptor = original.replicas()[0].descriptor().clone();
    manager
        .finish_put(&object, owner(OWNER), ReplicaSelector::All)
        .unwrap();
    let original_version = manager
        .get(object.as_lookup(), CatalogTick::ZERO)
        .unwrap()
        .object()
        .version_id();

    let replacement = manager
        .start_upsert(
            object.clone(),
            admission(OTHER_OWNER),
            plan(
                4096,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::new(1),
        )
        .unwrap();
    assert_ne!(replacement.replicas()[0].descriptor(), &original_descriptor);
    assert_eq!(pool.stats(MEMORY_ID).unwrap().usage.active_allocations, 2);
    let pending_read = manager
        .get(object.as_lookup(), CatalogTick::new(1))
        .unwrap();
    assert_eq!(pending_read.object().version_id(), original_version);
    assert_eq!(pending_read.object().owner(), owner(OWNER));
    drop(pending_read);
    assert_eq!(
        manager.finish_put(&object, owner(OWNER), ReplicaSelector::All),
        Err(ObjectManagerError::IllegalOwner)
    );
    manager
        .finish_put(&object, owner(OTHER_OWNER), ReplicaSelector::All)
        .unwrap();
    let committed = manager
        .get(object.as_lookup(), CatalogTick::new(2))
        .unwrap();
    assert_ne!(committed.object().version_id(), original_version);
    assert_eq!(committed.object().owner(), owner(OTHER_OWNER));
}

#[test]
fn upsert_revoke_and_failed_reallocation_restore_the_published_version() {
    let pool = Arc::new(SegmentPool::new());
    pool.attach(SegmentSpec::memory(
        SegmentIdentity::new(MEMORY_ID, OWNER, "memory-a"),
        MemoryRegion::new(0x1_0000_0000, 12 * 1024),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12000"),
    ))
    .unwrap();
    let manager = ObjectManager::new(pool.clone());
    let object = identity("rollback-upsert");
    manager
        .start_put(
            object.clone(),
            admission(OWNER),
            plan(
                4096,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::ZERO,
        )
        .unwrap();
    manager
        .finish_put(&object, owner(OWNER), ReplicaSelector::All)
        .unwrap();

    manager
        .start_upsert(
            object.clone(),
            admission(OTHER_OWNER),
            plan(
                8192,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::new(1),
        )
        .unwrap();
    let pending_read = manager
        .get(object.as_lookup(), CatalogTick::new(1))
        .unwrap();
    assert_eq!(pending_read.object().content().logical_bytes(), 4096);
    assert_eq!(pending_read.object().owner(), owner(OWNER));
    drop(pending_read);
    manager
        .revoke_put(
            &object,
            owner(OTHER_OWNER),
            ReplicaSelector::All,
            CatalogTick::new(2),
        )
        .unwrap();
    let restored = manager
        .get(object.as_lookup(), CatalogTick::new(2))
        .unwrap();
    assert_eq!(restored.object().content().logical_bytes(), 4096);
    assert_eq!(restored.object().owner(), owner(OWNER));
    drop(restored);
    let _ = manager.maintenance(CatalogTick::new(2), CollectBudget::new(0, 8, 0));
    assert_eq!(pool.stats(MEMORY_ID).unwrap().usage.active_allocations, 1);

    assert_eq!(
        manager.start_upsert(
            object.clone(),
            admission(OTHER_OWNER),
            plan(
                16 * 1024,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::new(3),
        ),
        Err(ObjectManagerError::NoAvailableReplicas)
    );
    assert_eq!(
        manager
            .get(object.as_lookup(), CatalogTick::new(3))
            .unwrap()
            .object()
            .content()
            .logical_bytes(),
        4096
    );
}

#[test]
fn size_changing_upsert_commits_new_generation_and_reclaims_old_allocation() {
    let pool = pool(true, false);
    let manager =
        ObjectManager::with_config(pool.clone(), ObjectCatalogConfig::new(32).with_lease(10, 5))
            .unwrap();
    let object = identity("resized-upsert");
    put_memory(&manager, &object, OWNER, 4096);
    let old_reader = manager
        .get(object.as_lookup(), CatalogTick::new(1))
        .unwrap();
    let old_version = old_reader.object().version_id();
    assert_eq!(old_reader.lease_expires_at(), CatalogTick::new(11));
    manager
        .start_upsert(
            object.clone(),
            admission(OTHER_OWNER),
            memory_plan(8192),
            CatalogTick::new(2),
        )
        .unwrap();
    manager
        .finish_put_at(
            &object,
            owner(OTHER_OWNER),
            ReplicaSelector::All,
            CatalogTick::new(2),
        )
        .unwrap();
    assert_eq!(pool.stats(MEMORY_ID).unwrap().usage.active_allocations, 2);
    let current = manager
        .get(object.as_lookup(), CatalogTick::new(2))
        .unwrap();
    assert_eq!(current.object().content().logical_bytes(), 8192);
    assert_ne!(current.object().version_id(), old_version);
    drop(current);

    let report = manager.maintenance(CatalogTick::new(11), CollectBudget::new(0, 8, 0));
    assert_eq!(report.catalog.reclaimed_objects, 0);
    drop(old_reader);
    let report = manager.maintenance(CatalogTick::new(11), CollectBudget::new(0, 8, 0));
    assert_eq!(report.catalog.reclaimed_objects, 1);
    assert_eq!(pool.stats(MEMORY_ID).unwrap().usage.active_allocations, 1);
}

#[test]
fn get_racing_upsert_observes_complete_versions() {
    let manager = Arc::new(ObjectManager::new(pool(true, false)));
    let object = identity("concurrent-upsert");
    put_memory(&manager, &object, OWNER, 4096);
    let old_version = manager
        .get(object.as_lookup(), CatalogTick::ZERO)
        .unwrap()
        .object()
        .version_id();
    manager
        .start_upsert(
            object.clone(),
            admission(OTHER_OWNER),
            memory_plan(8192),
            CatalogTick::new(1),
        )
        .unwrap();

    let start = Arc::new(Barrier::new(2));
    thread::scope(|scope| {
        let reader_manager = manager.clone();
        let reader_object = object.clone();
        let reader_start = start.clone();
        scope.spawn(move || {
            reader_start.wait();
            for _ in 0..256 {
                let read = reader_manager
                    .get(reader_object.as_lookup(), CatalogTick::new(1))
                    .unwrap();
                let handle = read.object();
                match handle.content().logical_bytes() {
                    4096 => {
                        assert_eq!(handle.version_id(), old_version);
                        assert_eq!(handle.owner(), owner(OWNER));
                    }
                    8192 => {
                        assert_ne!(handle.version_id(), old_version);
                        assert_eq!(handle.owner(), owner(OTHER_OWNER));
                    }
                    bytes => panic!("observed incomplete object with {bytes} bytes"),
                }
                assert_eq!(handle.replicas().len(), 1);
            }
        });

        start.wait();
        manager
            .finish_put(&object, owner(OTHER_OWNER), ReplicaSelector::All)
            .unwrap();
    });

    let current = manager
        .get(object.as_lookup(), CatalogTick::new(2))
        .unwrap();
    assert_eq!(current.object().content().logical_bytes(), 8192);
    assert_ne!(current.object().version_id(), old_version);
}

#[test]
fn remove_honors_lease_unless_forced() {
    let pool = pool(true, false);
    let manager =
        ObjectManager::with_config(pool, ObjectCatalogConfig::new(32).with_lease(10, 5)).unwrap();
    let object = identity("remove-me");
    manager
        .start_put(
            object.clone(),
            admission(OWNER),
            plan(
                4096,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::ZERO,
        )
        .unwrap();
    manager
        .finish_put(&object, owner(OWNER), ReplicaSelector::All)
        .unwrap();
    drop(
        manager
            .get(object.as_lookup(), CatalogTick::new(1))
            .unwrap(),
    );

    assert_eq!(
        manager.remove(object.as_lookup(), CatalogTick::new(2), false),
        Err(ObjectRemoveError::Leased {
            expires_at: CatalogTick::new(11)
        })
    );
    manager
        .remove(object.as_lookup(), CatalogTick::new(2), true)
        .unwrap();
    assert!(matches!(
        manager.get(object.as_lookup(), CatalogTick::new(2)),
        Err(LookupError::NotFound)
    ));
}

#[test]
fn stale_upsert_timeout_is_fenced_by_transaction_id() {
    let pool = pool(true, false);
    let manager =
        ObjectManager::with_config(pool, ObjectCatalogConfig::new(32).with_pending_timeout(2))
            .unwrap();
    let object = identity("upsert-timeout");
    manager
        .start_put(
            object.clone(),
            admission(OWNER),
            plan(
                4096,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::ZERO,
        )
        .unwrap();
    manager
        .finish_put(&object, owner(OWNER), ReplicaSelector::All)
        .unwrap();

    manager
        .start_upsert(
            object.clone(),
            admission(OTHER_OWNER),
            plan(
                4096,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::new(1),
        )
        .unwrap();
    manager
        .finish_put(&object, owner(OTHER_OWNER), ReplicaSelector::All)
        .unwrap();
    manager
        .start_upsert(
            object.clone(),
            admission(OWNER),
            plan(
                4096,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::new(2),
        )
        .unwrap();

    let report = manager.maintenance(CatalogTick::new(3), CollectBudget::new(8, 8, 0));
    assert_eq!(report.expired_writes, 0);
    assert_eq!(
        manager
            .get(object.as_lookup(), CatalogTick::new(3))
            .unwrap()
            .object()
            .owner(),
        owner(OTHER_OWNER)
    );
    let report = manager.maintenance(CatalogTick::new(4), CollectBudget::new(8, 8, 0));
    assert_eq!(report.expired_writes, 1);
    assert_eq!(
        manager
            .get(object.as_lookup(), CatalogTick::new(4))
            .unwrap()
            .object()
            .owner(),
        owner(OTHER_OWNER)
    );
}

#[test]
fn published_objects_become_invisible_and_are_retired_after_segment_invalidation() {
    let pool = pool(true, false);
    let segment = pool.segment(MEMORY_ID).unwrap();
    let manager = ObjectManager::new(pool.clone());
    let object = identity("segment-backed");

    manager
        .start_put(
            object.clone(),
            admission(OWNER),
            plan(
                4096,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::ZERO,
        )
        .unwrap();
    manager
        .finish_put(&object, owner(OWNER), ReplicaSelector::All)
        .unwrap();
    let read = manager
        .get(object.as_lookup(), CatalogTick::new(1))
        .unwrap();
    assert!(read.is_live());

    assert_eq!(pool.invalidate_owner(OWNER), 1);
    assert!(pool.segment(MEMORY_ID).is_none());
    assert!(!read.is_live());

    assert!(matches!(
        manager.get(object.as_lookup(), CatalogTick::new(2)),
        Err(cakemaster::object::error::LookupError::NotFound)
    ));
    assert!(!manager.exists(object.as_lookup(), CatalogTick::new(2)));
    assert_eq!(manager.catalog().stats().published_objects, 1);

    let report = manager.maintenance(CatalogTick::new(2), CollectBudget::new(8, 8, 0));
    assert_eq!(report.catalog.invalidated_published, 1);
    assert_eq!(manager.catalog().stats().published_objects, 0);
    drop(read);
    let report = manager.maintenance(CatalogTick::new(3), CollectBudget::new(0, 8, 0));
    assert_eq!(report.catalog.reclaimed_objects, 1);
    drop(manager);
    assert_eq!(segment.stats().usage.active_allocations, 0);
}

#[test]
fn dropping_an_upsert_claim_rechecks_an_invalidated_base() {
    let pool = pool(true, false);
    let manager = ObjectManager::new(pool.clone());
    let object = identity("invalidated-during-claim");
    put_memory(&manager, &object, OWNER, 4096);

    let claim = manager
        .catalog()
        .begin_write(
            object.clone(),
            admission(OTHER_OWNER),
            WriteMode::Upsert,
            ObjectPinRequest::default(),
            CatalogTick::new(1),
        )
        .unwrap();
    assert_eq!(pool.invalidate_owner(OWNER), 1);

    let report = manager.maintenance(CatalogTick::new(1), CollectBudget::new(8, 8, 0));
    assert_eq!(report.catalog.invalidated_published, 0);
    assert_eq!(manager.catalog().stats().published_objects, 1);

    drop(claim);
    let report = manager.maintenance(CatalogTick::new(2), CollectBudget::new(8, 8, 0));
    assert_eq!(report.catalog.invalidated_published, 1);
    assert_eq!(manager.catalog().stats().published_objects, 0);
}

#[test]
fn segment_invalidation_prunes_only_stale_published_replicas() {
    let pool = replicated_memory_pool();
    let first_segment = pool.segment(MEMORY_ID).unwrap();
    let second_segment = pool.segment(SECOND_MEMORY_ID).unwrap();
    let manager = ObjectManager::new(pool.clone());
    let object = identity("replicated-segment-backed");

    let started = manager
        .start_put(
            object.clone(),
            admission(OWNER),
            plan(
                4096,
                2,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::ZERO,
        )
        .unwrap();
    assert_eq!(started.replicas().len(), 2);
    manager
        .finish_put(&object, owner(OWNER), ReplicaSelector::All)
        .unwrap();
    let original = manager
        .get(object.as_lookup(), CatalogTick::new(1))
        .unwrap();
    assert_eq!(original.object().replicas().len(), 2);
    assert_eq!(manager.catalog().stats().live_bytes, 8192);

    pool.quiesce(OWNER, MEMORY_ID).unwrap();
    pool.remove(OWNER, MEMORY_ID).unwrap();

    assert!(original.is_live());
    let surviving = manager
        .get(object.as_lookup(), CatalogTick::new(2))
        .unwrap();
    {
        let replicas = surviving.object().replicas();
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas.first().unwrap().segment_id(), SECOND_MEMORY_ID);
    }
    assert!(manager.exists(object.as_lookup(), CatalogTick::new(2)));
    assert_eq!(manager.catalog().stats().published_objects, 1);
    assert_eq!(manager.catalog().stats().live_bytes, 8192);

    let report = manager.maintenance(CatalogTick::new(2), CollectBudget::new(8, 8, 0));
    assert_eq!(report.catalog.pruned_objects, 1);
    assert_eq!(report.catalog.pruned_replicas, 1);
    assert_eq!(report.catalog.pruned_replica_bytes, 4096);
    assert_eq!(report.catalog.invalidated_published, 0);
    assert_eq!(manager.catalog().stats().published_objects, 1);
    assert_eq!(manager.catalog().stats().live_bytes, 4096);
    assert_eq!(first_segment.stats().usage.active_allocations, 0);
    assert_eq!(second_segment.stats().usage.active_allocations, 1);

    pool.quiesce(OWNER, SECOND_MEMORY_ID).unwrap();
    pool.remove(OWNER, SECOND_MEMORY_ID).unwrap();
    assert!(matches!(
        manager.get(object.as_lookup(), CatalogTick::new(3)),
        Err(LookupError::NotFound)
    ));
    let report = manager.maintenance(CatalogTick::new(3), CollectBudget::new(8, 8, 0));
    assert_eq!(report.catalog.invalidated_published, 1);
    assert_eq!(report.catalog.retired_objects, 1);
    assert_eq!(report.catalog.retired_bytes, 4096);
    assert_eq!(manager.catalog().stats().published_objects, 0);

    drop(original);
    drop(surviving);
    let report = manager.maintenance(CatalogTick::new(4), CollectBudget::new(0, 8, 0));
    assert_eq!(report.catalog.reclaimed_objects, 1);
    assert_eq!(report.catalog.reclaimed_bytes, 4096);
    assert_eq!(second_segment.stats().usage.active_allocations, 0);
}

#[test]
fn pruning_waits_for_live_replica_views_before_releasing_resources() {
    let pool = replicated_memory_pool();
    let first_segment = pool.segment(MEMORY_ID).unwrap();
    let manager = Arc::new(ObjectManager::new(pool.clone()));
    let object = identity("replica-view-fence");
    manager
        .start_put(
            object.clone(),
            admission(OWNER),
            plan(
                4096,
                2,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::ZERO,
        )
        .unwrap();
    manager
        .finish_put(&object, owner(OWNER), ReplicaSelector::All)
        .unwrap();
    let read = manager.get(object.as_lookup(), CatalogTick::ZERO).unwrap();
    let replicas = read.object().replicas();

    pool.quiesce(OWNER, MEMORY_ID).unwrap();
    pool.remove(OWNER, MEMORY_ID).unwrap();
    assert_eq!(replicas.len(), 1);

    let barrier = Arc::new(Barrier::new(2));
    let worker_barrier = barrier.clone();
    let worker_manager = manager.clone();
    let (completed_tx, completed_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        worker_barrier.wait();
        let report = worker_manager.maintenance(CatalogTick::new(1), CollectBudget::new(8, 8, 0));
        completed_tx.send(report).unwrap();
    });
    barrier.wait();
    assert!(
        completed_rx
            .recv_timeout(Duration::from_millis(20))
            .is_err()
    );
    assert_eq!(first_segment.stats().usage.active_allocations, 1);

    drop(replicas);
    let report = completed_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(report.catalog.pruned_replicas, 1);
    worker.join().unwrap();
    assert_eq!(first_segment.stats().usage.active_allocations, 0);
    assert!(read.is_live());
}

#[test]
fn cxl_segment_invalidation_keeps_the_surviving_replica() {
    let pool = replicated_cxl_pool();
    let first_segment = pool.segment(FIRST_CXL_ID).unwrap();
    let second_segment = pool.segment(SECOND_CXL_ID).unwrap();
    let manager = ObjectManager::new(pool.clone());
    let object = identity("cxl-replicated");
    manager
        .start_put(
            object.clone(),
            admission(OWNER),
            plan(
                4096,
                2,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::ZERO,
        )
        .unwrap();
    manager
        .finish_put(&object, owner(OWNER), ReplicaSelector::All)
        .unwrap();

    pool.quiesce(OWNER, FIRST_CXL_ID).unwrap();
    pool.remove(OWNER, FIRST_CXL_ID).unwrap();
    let read = manager
        .get(object.as_lookup(), CatalogTick::new(1))
        .unwrap();
    assert_eq!(read.object().replicas().len(), 1);
    assert_eq!(
        read.object().replicas().first().unwrap().segment_id(),
        SECOND_CXL_ID
    );

    let report = manager.maintenance(CatalogTick::new(1), CollectBudget::new(8, 8, 0));
    assert_eq!(report.catalog.pruned_replicas, 1);
    assert_eq!(first_segment.stats().usage.active_allocations, 0);
    assert_eq!(second_segment.stats().usage.active_allocations, 1);
    assert_eq!(manager.catalog().stats().live_bytes, 4096);
}

#[test]
fn bounded_liveness_scan_does_not_skip_objects_promoted_between_generations() {
    let pool = pool(true, false);
    let manager = ObjectManager::new(pool.clone());
    let first = identity("recent-before-invalidation");
    let second = identity("unscanned-before-invalidation");
    for object in [&first, &second] {
        manager
            .start_put(
                object.clone(),
                admission(OWNER),
                plan(1, 1, ReplicaClass::Memory, FulfillmentPolicy::AllOrNothing),
                CatalogTick::ZERO,
            )
            .unwrap();
        manager
            .finish_put(object, owner(OWNER), ReplicaSelector::All)
            .unwrap();
    }
    let _recent = manager.get(first.as_lookup(), CatalogTick::ZERO).unwrap();
    pool.invalidate_owner(OWNER);

    let first_step = manager.maintenance(CatalogTick::new(1), CollectBudget::new(1, 0, 0));
    assert_eq!(first_step.catalog.scanned_candidates, 1);
    assert_eq!(manager.catalog().stats().liveness_scan_remaining, 1);
    let second_step = manager.maintenance(CatalogTick::new(2), CollectBudget::new(1, 0, 0));

    assert_eq!(second_step.catalog.scanned_candidates, 1);
    assert_eq!(manager.catalog().stats().published_objects, 0);
    assert_eq!(manager.catalog().stats().liveness_scan_remaining, 0);
}

#[test]
fn invalidated_pending_put_cannot_publish_but_can_be_revoked() {
    let pool = pool(true, false);
    let manager = ObjectManager::new(pool.clone());
    let object = identity("pending-on-dead-segment");

    manager
        .start_put(
            object.clone(),
            admission(OWNER),
            plan(
                4096,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::ZERO,
        )
        .unwrap();
    assert_eq!(pool.invalidate_owner(OWNER), 1);
    assert_eq!(
        manager.finish_put(&object, owner(OWNER), ReplicaSelector::All),
        Err(ObjectManagerError::NoAvailableReplicas)
    );
    manager
        .revoke_put(
            &object,
            owner(OWNER),
            ReplicaSelector::All,
            CatalogTick::new(1),
        )
        .unwrap();

    let report = manager.maintenance(CatalogTick::new(1), CollectBudget::new(0, 1, 0));
    assert_eq!(report.catalog.reclaimed_objects, 1);
}

#[test]
fn invalidated_pending_put_is_retired_before_its_timeout() {
    let pool = pool(true, false);
    let manager = ObjectManager::with_config(
        pool.clone(),
        ObjectCatalogConfig::new(32).with_pending_timeout(10_000),
    )
    .unwrap();
    let object = identity("pending-cleanup-on-dead-segment");
    manager
        .start_put(
            object.clone(),
            admission(OWNER),
            plan(
                4096,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            ),
            CatalogTick::ZERO,
        )
        .unwrap();

    assert_eq!(pool.invalidate_owner(OWNER), 1);
    let report = manager.maintenance(CatalogTick::new(1), CollectBudget::new(8, 8, 0));
    assert_eq!(report.expired_writes, 0);
    assert_eq!(report.catalog.invalidated_pending, 1);
    assert_eq!(report.catalog.expired_pending, 0);
    assert_eq!(report.catalog.reclaimed_objects, 1);
    assert_eq!(manager.catalog().stats().pending_objects, 0);
}

#[test]
fn segment_invalidation_racing_maintenance_still_retires_published_objects() {
    use std::sync::{Arc, Barrier};

    for round in 0..128 {
        let pool = pool(true, false);
        let manager = Arc::new(ObjectManager::new(pool.clone()));
        let object =
            ObjectIdentity::new(NamespaceId::DEFAULT, format!("invalidation-race-{round}"));
        manager
            .start_put(
                object.clone(),
                admission(OWNER),
                plan(
                    4096,
                    1,
                    ReplicaClass::Memory,
                    FulfillmentPolicy::AllOrNothing,
                ),
                CatalogTick::ZERO,
            )
            .unwrap();
        manager
            .finish_put(&object, owner(OWNER), ReplicaSelector::All)
            .unwrap();

        let barrier = Arc::new(Barrier::new(2));
        std::thread::scope(|scope| {
            let manager = manager.clone();
            let worker_barrier = barrier.clone();
            scope.spawn(move || {
                worker_barrier.wait();
                for tick in 1..=8 {
                    manager.maintenance(CatalogTick::new(tick), CollectBudget::new(8, 8, 0));
                }
            });
            barrier.wait();
            pool.invalidate_owner(OWNER);
        });

        for tick in 9..=16 {
            manager.maintenance(CatalogTick::new(tick), CollectBudget::new(8, 8, 0));
        }
        assert_eq!(manager.catalog().stats().published_objects, 0);
    }
}

#[test]
fn manager_revoke_and_timeout_release_reservations_for_reuse() {
    let pool = pool(true, false);
    let manager = ObjectManager::with_config(
        pool.clone(),
        ObjectCatalogConfig::new(32)
            .with_pending_timeout(5)
            .with_empty_slot_grace(1),
    )
    .unwrap();
    let object = identity("retry");
    let put_plan = plan(
        CAPACITY,
        1,
        ReplicaClass::Memory,
        FulfillmentPolicy::AllOrNothing,
    );

    manager
        .start_put(
            object.clone(),
            admission(OWNER),
            put_plan.clone(),
            CatalogTick::ZERO,
        )
        .unwrap();
    assert_eq!(pool.stats(MEMORY_ID).unwrap().usage.active_allocations, 1);
    assert_eq!(
        manager
            .maintenance(CatalogTick::new(4), CollectBudget::new(8, 8, 8))
            .expired_writes,
        0
    );
    let report = manager.maintenance(CatalogTick::new(5), CollectBudget::new(8, 8, 8));
    assert_eq!(report.expired_writes, 1);
    assert_eq!(report.catalog.reclaimed_objects, 1);
    assert_eq!(pool.stats(MEMORY_ID).unwrap().usage.active_allocations, 0);
    assert_eq!(
        manager.finish_put(&object, owner(OWNER), ReplicaSelector::All),
        Err(ObjectManagerError::NotFound)
    );

    manager
        .start_put(
            object.clone(),
            admission(OWNER),
            put_plan,
            CatalogTick::new(6),
        )
        .unwrap();
    manager
        .revoke_put(
            &object,
            owner(OWNER),
            ReplicaSelector::All,
            CatalogTick::new(6),
        )
        .unwrap();
    let report = manager.maintenance(CatalogTick::new(6), CollectBudget::new(8, 8, 8));
    assert_eq!(report.catalog.reclaimed_objects, 1);
    assert_eq!(pool.stats(MEMORY_ID).unwrap().usage.active_allocations, 0);
}

#[test]
fn completed_writes_leave_no_timeout_backlog_after_one_bounded_step() {
    let pool = pool(true, false);
    let manager = ObjectManager::with_config(
        pool,
        ObjectCatalogConfig::new(32).with_pending_timeout(1_000),
    )
    .unwrap();

    for index in 0..8 {
        let object = identity(&format!("completed-{index}"));
        manager
            .start_put(
                object.clone(),
                admission(OWNER),
                plan(1, 1, ReplicaClass::Memory, FulfillmentPolicy::AllOrNothing),
                CatalogTick::ZERO,
            )
            .unwrap();
        manager
            .finish_put(&object, owner(OWNER), ReplicaSelector::All)
            .unwrap();
    }

    assert_eq!(manager.catalog().stats().pending_candidates, 8);
    let report = manager.maintenance(CatalogTick::new(1), CollectBudget::new(8, 0, 0));
    assert_eq!(report.expired_writes, 0);
    assert_eq!(manager.catalog().stats().pending_candidates, 0);
}

#[test]
fn allocator_policy_is_selected_by_the_normalized_plan() {
    let memory_pool = pool(true, false);
    let memory_manager = ObjectManager::new(memory_pool);
    let started = memory_manager
        .start_put(
            identity("best-effort"),
            admission(OWNER),
            plan(4096, 2, ReplicaClass::Memory, FulfillmentPolicy::BestEffort),
            CatalogTick::ZERO,
        )
        .unwrap();
    assert_eq!(started.replicas().len(), 1);

    let nof_pool = pool(false, true);
    let nof_manager = ObjectManager::new(nof_pool.clone());
    assert_eq!(
        nof_manager.start_put(
            identity("all-or-nothing"),
            admission(OWNER),
            plan(4096, 2, ReplicaClass::Nof, FulfillmentPolicy::AllOrNothing,),
            CatalogTick::ZERO,
        ),
        Err(ObjectManagerError::NoAvailableReplicas)
    );
    assert_eq!(nof_pool.stats(NOF_ID).unwrap().usage.active_allocations, 0);
}

#[test]
fn concurrent_start_for_one_key_has_one_winner() {
    let manager = Arc::new(ObjectManager::new(pool(true, false)));
    let barrier = Arc::new(Barrier::new(9));
    let mut workers = Vec::new();
    for _ in 0..8 {
        let manager = manager.clone();
        let barrier = barrier.clone();
        workers.push(thread::spawn(move || {
            barrier.wait();
            manager.start_put(
                identity("one-winner"),
                admission(OWNER),
                plan(
                    4096,
                    1,
                    ReplicaClass::Memory,
                    FulfillmentPolicy::AllOrNothing,
                ),
                CatalogTick::ZERO,
            )
        }));
    }
    barrier.wait();
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert!(
        results
            .iter()
            .filter(|result| result.is_err())
            .all(|result| { result.as_ref().unwrap_err() == &ObjectManagerError::AlreadyExists })
    );
}

#[test]
fn pin_requests_validate_and_commit_at_finish_time() {
    let manager = ObjectManager::with_config(
        pool(true, false),
        ObjectCatalogConfig::new(16).with_soft_pin_ttl(50, 100),
    )
    .unwrap();
    let invalid_ttl = plan(
        1024,
        1,
        ReplicaClass::Memory,
        FulfillmentPolicy::AllOrNothing,
    )
    .with_pins(ObjectPinRequest::new(
        SoftPinAction::Preserve,
        Some(10),
        false,
    ));
    assert_eq!(
        manager.start_put(
            identity("ttl-requires-enable"),
            admission(OWNER),
            invalid_ttl,
            CatalogTick::ZERO,
        ),
        Err(ObjectManagerError::InvalidPlan)
    );

    let over_limit = plan(
        1024,
        1,
        ReplicaClass::Memory,
        FulfillmentPolicy::AllOrNothing,
    )
    .with_pins(ObjectPinRequest::new(
        SoftPinAction::Enable,
        Some(101),
        false,
    ));
    assert_eq!(
        manager.start_put(
            identity("ttl-over-limit"),
            admission(OWNER),
            over_limit,
            CatalogTick::ZERO,
        ),
        Err(ObjectManagerError::InvalidPlan)
    );

    let object = identity("default-ttl");
    manager
        .start_put(
            object.clone(),
            admission(OWNER),
            plan(
                1024,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            )
            .with_pins(ObjectPinRequest::new(SoftPinAction::Enable, None, false)),
            CatalogTick::ZERO,
        )
        .unwrap();
    manager
        .finish_put_at(
            &object,
            owner(OWNER),
            ReplicaSelector::All,
            CatalogTick::new(10),
        )
        .unwrap();
    let read = manager
        .get(object.as_lookup(), CatalogTick::new(10))
        .unwrap();
    assert_eq!(
        read.object().soft_pin_expires_at(),
        Some(CatalogTick::new(60))
    );
    assert!(read.object().is_soft_pinned(CatalogTick::new(59)));
    assert!(!read.object().is_soft_pinned(CatalogTick::new(60)));

    let zero = identity("zero-ttl");
    manager
        .start_put(
            zero.clone(),
            admission(OWNER),
            plan(
                1024,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            )
            .with_pins(ObjectPinRequest::new(SoftPinAction::Enable, Some(0), false)),
            CatalogTick::new(20),
        )
        .unwrap();
    manager
        .finish_put_at(
            &zero,
            owner(OWNER),
            ReplicaSelector::All,
            CatalogTick::new(21),
        )
        .unwrap();
    assert_eq!(
        manager
            .get(zero.as_lookup(), CatalogTick::new(21))
            .unwrap()
            .object()
            .soft_pin_expires_at(),
        None
    );
}

#[test]
fn upsert_pin_changes_commit_atomically_and_rollback_preserves_metadata() {
    let manager = ObjectManager::with_config(
        pool(true, false),
        ObjectCatalogConfig::new(16)
            .with_lease(1, 0)
            .with_pending_timeout(5)
            .with_soft_pin_ttl(100, 1_000),
    )
    .unwrap();
    let object = identity("pin-upsert");
    let make_plan = |bytes, action, ttl, hard| {
        plan(
            bytes,
            1,
            ReplicaClass::Memory,
            FulfillmentPolicy::AllOrNothing,
        )
        .with_pins(ObjectPinRequest::new(action, ttl, hard))
    };

    manager
        .start_put(
            object.clone(),
            admission(OWNER),
            make_plan(4096, SoftPinAction::Enable, Some(100), false),
            CatalogTick::new(1),
        )
        .unwrap();
    manager
        .finish_put_at(
            &object,
            owner(OWNER),
            ReplicaSelector::All,
            CatalogTick::new(10),
        )
        .unwrap();

    // Every upsert creates a replacement version. Hard pin is monotonic and
    // PRESERVE keeps the original committed soft-pin deadline.
    manager
        .start_upsert(
            object.clone(),
            admission(OWNER),
            make_plan(4096, SoftPinAction::Preserve, None, true),
            CatalogTick::new(20),
        )
        .unwrap();
    manager
        .finish_put_at(
            &object,
            owner(OWNER),
            ReplicaSelector::All,
            CatalogTick::new(20),
        )
        .unwrap();
    let read = manager
        .get(object.as_lookup(), CatalogTick::new(20))
        .unwrap();
    assert_eq!(
        read.object().soft_pin_expires_at(),
        Some(CatalogTick::new(110))
    );
    assert!(read.object().is_hard_pinned());
    drop(read);

    // Revoke and timeout both discard the pending soft-pin change.
    manager
        .start_upsert(
            object.clone(),
            admission(OWNER),
            make_plan(4096, SoftPinAction::Disable, None, false),
            CatalogTick::new(30),
        )
        .unwrap();
    manager
        .revoke_put(
            &object,
            owner(OWNER),
            ReplicaSelector::All,
            CatalogTick::new(31),
        )
        .unwrap();
    assert_eq!(
        manager
            .get(object.as_lookup(), CatalogTick::new(31))
            .unwrap()
            .object()
            .soft_pin_expires_at(),
        Some(CatalogTick::new(110))
    );
    manager
        .start_upsert(
            object.clone(),
            admission(OWNER),
            make_plan(4096, SoftPinAction::Disable, None, false),
            CatalogTick::new(40),
        )
        .unwrap();
    let maintenance = manager.maintenance(CatalogTick::new(46), CollectBudget::new(8, 0, 0));
    assert_eq!(maintenance.expired_writes, 1);
    assert_eq!(
        manager
            .get(object.as_lookup(), CatalogTick::new(46))
            .unwrap()
            .object()
            .soft_pin_expires_at(),
        Some(CatalogTick::new(110))
    );

    manager
        .start_upsert(
            object.clone(),
            admission(OWNER),
            make_plan(4096, SoftPinAction::Disable, None, false),
            CatalogTick::new(47),
        )
        .unwrap();
    manager
        .finish_put_at(
            &object,
            owner(OWNER),
            ReplicaSelector::All,
            CatalogTick::new(48),
        )
        .unwrap();
    assert_eq!(
        manager
            .get(object.as_lookup(), CatalogTick::new(48))
            .unwrap()
            .object()
            .soft_pin_expires_at(),
        None
    );
    manager
        .start_upsert(
            object.clone(),
            admission(OWNER),
            make_plan(4096, SoftPinAction::Enable, Some(100), false),
            CatalogTick::new(49),
        )
        .unwrap();
    manager
        .finish_put_at(
            &object,
            owner(OWNER),
            ReplicaSelector::All,
            CatalogTick::new(50),
        )
        .unwrap();

    // A size-changing upsert may add a hard pin and carries the committed
    // soft deadline into the replacement generation.
    manager
        .start_upsert(
            object.clone(),
            admission(OWNER),
            make_plan(8192, SoftPinAction::Preserve, None, true),
            CatalogTick::new(60),
        )
        .unwrap();
    manager
        .finish_put_at(
            &object,
            owner(OWNER),
            ReplicaSelector::All,
            CatalogTick::new(70),
        )
        .unwrap();
    let read = manager
        .get(object.as_lookup(), CatalogTick::new(70))
        .unwrap();
    assert!(read.object().is_hard_pinned());
    assert_eq!(
        read.object().soft_pin_expires_at(),
        Some(CatalogTick::new(150))
    );
    drop(read);

    assert_eq!(
        manager.remove(object.as_lookup(), CatalogTick::new(200), false),
        Err(ObjectRemoveError::HardPinned)
    );
    manager
        .remove(object.as_lookup(), CatalogTick::new(200), true)
        .unwrap();
}

#[test]
fn eviction_obeys_hard_and_soft_pin_policy_and_expiry_scan_is_bounded() {
    let protected = ObjectManager::with_config(
        pool(true, false),
        ObjectCatalogConfig::new(16)
            .with_lease(1, 0)
            .with_soft_pin_ttl(100, 100)
            .with_soft_pin_eviction(false),
    )
    .unwrap();
    for index in 0..3 {
        let object = identity(&format!("soft-{index}"));
        protected
            .start_put(
                object.clone(),
                admission(OWNER),
                plan(
                    1024,
                    1,
                    ReplicaClass::Memory,
                    FulfillmentPolicy::AllOrNothing,
                )
                .with_pins(ObjectPinRequest::new(
                    SoftPinAction::Enable,
                    Some(100),
                    false,
                )),
                CatalogTick::ZERO,
            )
            .unwrap();
        protected
            .finish_put_at(
                &object,
                owner(OWNER),
                ReplicaSelector::All,
                CatalogTick::ZERO,
            )
            .unwrap();
    }
    protected.catalog().request_reclaim(1024);
    let active = protected.maintenance(CatalogTick::new(10), CollectBudget::new(8, 8, 0));
    assert_eq!(active.catalog.retired_objects, 0);

    let first_expiry = protected.maintenance(CatalogTick::new(101), CollectBudget::new(1, 8, 0));
    assert_eq!(first_expiry.catalog.scanned_soft_pins, 1);
    assert_eq!(first_expiry.catalog.expired_soft_pins, 1);
    assert!(protected.catalog().stats().soft_pin_candidates <= 2);

    let permissive = ObjectManager::with_config(
        pool(true, false),
        ObjectCatalogConfig::new(8)
            .with_lease(1, 0)
            .with_soft_pin_ttl(100, 100)
            .with_soft_pin_eviction(true),
    )
    .unwrap();
    let soft = identity("evict-active-soft-pin");
    permissive
        .start_put(
            soft.clone(),
            admission(OWNER),
            plan(
                1024,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            )
            .with_pins(ObjectPinRequest::new(
                SoftPinAction::Enable,
                Some(100),
                false,
            )),
            CatalogTick::ZERO,
        )
        .unwrap();
    permissive
        .finish_put_at(&soft, owner(OWNER), ReplicaSelector::All, CatalogTick::ZERO)
        .unwrap();
    permissive.catalog().request_reclaim(1024);
    let evicted = permissive.maintenance(CatalogTick::new(10), CollectBudget::new(8, 8, 0));
    assert_eq!(evicted.catalog.retired_objects, 1);

    let hard_manager = ObjectManager::with_config(
        pool(true, false),
        ObjectCatalogConfig::new(8).with_lease(1, 0),
    )
    .unwrap();
    let hard = identity("never-evict-hard-pin");
    hard_manager
        .start_put(
            hard.clone(),
            admission(OWNER),
            plan(
                1024,
                1,
                ReplicaClass::Memory,
                FulfillmentPolicy::AllOrNothing,
            )
            .with_pins(ObjectPinRequest::new(SoftPinAction::Preserve, None, true)),
            CatalogTick::ZERO,
        )
        .unwrap();
    hard_manager
        .finish_put_at(&hard, owner(OWNER), ReplicaSelector::All, CatalogTick::ZERO)
        .unwrap();
    hard_manager.catalog().request_reclaim(1024);
    let skipped = hard_manager.maintenance(CatalogTick::new(1_000), CollectBudget::new(8, 8, 0));
    assert_eq!(skipped.catalog.retired_objects, 0);
    assert!(hard_manager.exists(hard.as_lookup(), CatalogTick::new(1_000)));
}
