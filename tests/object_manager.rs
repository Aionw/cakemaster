use cakemaster::object::error::{LookupError, ObjectManagerError};
use cakemaster::object::reclamation::{CatalogTick, CollectBudget};
use cakemaster::object::{
    NamespaceId, ObjectCatalogConfig, ObjectContent, ObjectIdentity, ObjectKind, ObjectManager,
    ObjectPutPlan, ReplicaSelector, WriteOwner,
};
use cakemaster::segment::placement::{
    AllocationSpec, FulfillmentPolicy, PlacementRequest, ReplicaPolicy,
};
use cakemaster::segment::{
    ClientId, MemoryRegion, ReplicaClass, ReservationDescriptor, SegmentId, SegmentIdentity,
    SegmentPool, SegmentSpec, TransportEndpoint, TransportProtocol,
};
use std::sync::{Arc, Barrier};
use std::thread;

const OWNER: ClientId = ClientId::new(7, 11);
const OTHER_OWNER: ClientId = ClientId::new(7, 12);
const MEMORY_ID: SegmentId = SegmentId::new(1, 1);
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

fn identity(key: &str) -> ObjectIdentity {
    ObjectIdentity::new(NamespaceId::DEFAULT, key)
}

fn owner(client: ClientId) -> WriteOwner {
    WriteOwner::new(client)
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
            owner(OWNER),
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
            owner(OWNER),
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
        .start_put(object.clone(), owner(OWNER), put_plan, CatalogTick::new(6))
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
                owner(OWNER),
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
            owner(OWNER),
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
            owner(OWNER),
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
                owner(OWNER),
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
