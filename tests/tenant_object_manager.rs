use cakemaster::object::reclamation::{CatalogTick, CollectBudget};
use cakemaster::object::{
    ObjectCatalogConfig, ObjectContent, ObjectKind, ObjectPutPlan, ReplicaSelector,
    TenantAdminError, TenantConfig, TenantId, TenantObjectError, TenantObjectManager, TenantPolicy,
    TenantPutRequest, TenantQuotaLimits, WriteAdmission, WriteOwner,
};
use cakemaster::segment::placement::{
    AllocationSpec, FulfillmentPolicy, PlacementRequest, ReplicaPolicy,
};
use cakemaster::segment::{
    ClientId, MemoryRegion, ReplicaClass, SegmentId, SegmentIdentity, SegmentPool, SegmentSpec,
    TransportEndpoint, TransportProtocol,
};
use std::sync::Arc;

const OWNER: ClientId = ClientId::new(31, 41);
const MEMORY_ID: SegmentId = SegmentId::new(10, 1);
const SECOND_MEMORY_ID: SegmentId = SegmentId::new(10, 2);
const NOF_ID: SegmentId = SegmentId::new(20, 1);
const CAPACITY: u64 = 1 << 20;

fn tenant_id(value: &str) -> TenantId {
    TenantId::try_from(value).unwrap()
}

fn policy(memory_bytes: u64, nof_bytes: u64) -> TenantPolicy {
    TenantPolicy::new(TenantQuotaLimits::new(memory_bytes, nof_bytes))
}

fn pool() -> Arc<SegmentPool> {
    let pool = Arc::new(SegmentPool::new());
    pool.attach(SegmentSpec::memory(
        SegmentIdentity::new(MEMORY_ID, OWNER, "memory"),
        MemoryRegion::new(0x3_0000_0000, CAPACITY),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
    ))
    .unwrap();
    pool
}

fn manager(a_quota: u64, b_quota: u64) -> TenantObjectManager {
    TenantObjectManager::new(
        pool(),
        TenantConfig::multi(vec![
            (tenant_id("a"), policy(a_quota, 0)),
            (tenant_id("b"), policy(b_quota, 0)),
        ]),
    )
    .unwrap()
}

fn plan(bytes: u64, replicas: usize) -> ObjectPutPlan {
    plan_for(
        bytes,
        replicas,
        ReplicaClass::Memory,
        FulfillmentPolicy::BestEffort,
    )
}

fn plan_for(
    bytes: u64,
    replicas: usize,
    class: ReplicaClass,
    fulfillment: FulfillmentPolicy,
) -> ObjectPutPlan {
    ObjectPutPlan::new(
        ObjectContent::new(bytes).with_kind(ObjectKind::Tensor),
        PlacementRequest::new(AllocationSpec::new(bytes), ReplicaPolicy::new(replicas))
            .for_replica_class(class)
            .with_fulfillment(fulfillment),
    )
}

fn owner() -> WriteOwner {
    WriteOwner::new(OWNER)
}

fn admission() -> WriteAdmission {
    WriteAdmission::unmanaged(OWNER)
}

#[test]
fn tenants_isolate_keys_and_charge_actual_logical_replica_bytes() {
    let manager = manager(4096, 8192);
    let a = manager.resolve_tenant(&tenant_id("a")).unwrap();
    let b = manager.resolve_tenant(&tenant_id("b")).unwrap();

    let a_started = manager
        .start_put(
            &a,
            "same-key",
            admission(),
            plan(4096, 2),
            CatalogTick::ZERO,
        )
        .unwrap();
    // Best effort placement found one physical target, so quota follows the
    // actual replica count rather than the requested count.
    assert_eq!(a_started.replicas().len(), 1);
    manager
        .finish_put(&a, "same-key", owner(), ReplicaSelector::All)
        .unwrap();
    manager
        .start_put(
            &b,
            "same-key",
            admission(),
            plan(4096, 1),
            CatalogTick::ZERO,
        )
        .unwrap();
    manager
        .finish_put(&b, "same-key", owner(), ReplicaSelector::All)
        .unwrap();

    assert!(manager.exists(&a, "same-key", CatalogTick::ZERO).unwrap());
    assert!(manager.exists(&b, "same-key", CatalogTick::ZERO).unwrap());
    let a_snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(a_snapshot.memory.used_bytes, 4096);
    assert_eq!(a_snapshot.memory.demand_bytes, 4096);

    assert_eq!(
        manager.start_put(
            &a,
            "over-quota",
            admission(),
            plan(8192, 1),
            CatalogTick::ZERO,
        ),
        Err(TenantObjectError::TenantQuotaExceeded {
            class: cakemaster::object::TenantResourceClass::Memory,
            requested_bytes: 8192,
            demand_bytes: 4096,
            effective_bytes: 4096,
        })
    );
}

#[test]
fn pruning_one_replica_releases_only_its_committed_tenant_charge() {
    let pool = pool();
    pool.attach(SegmentSpec::memory(
        SegmentIdentity::new(SECOND_MEMORY_ID, OWNER, "memory-2"),
        MemoryRegion::new(0x4_0000_0000, CAPACITY),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12346"),
    ))
    .unwrap();
    let manager = TenantObjectManager::new(
        pool.clone(),
        TenantConfig::multi(vec![(tenant_id("a"), policy(8192, 0))]),
    )
    .unwrap();
    let tenant = manager.resolve_tenant(&tenant_id("a")).unwrap();

    let started = manager
        .start_put(
            &tenant,
            "replicated",
            admission(),
            plan_for(
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
        .finish_put(&tenant, "replicated", owner(), ReplicaSelector::All)
        .unwrap();
    let snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(snapshot.memory.used_bytes, 8192);
    assert_eq!(snapshot.memory.demand_bytes, 8192);

    pool.quiesce(OWNER, MEMORY_ID).unwrap();
    pool.remove(OWNER, MEMORY_ID).unwrap();
    assert!(
        manager
            .exists(&tenant, "replicated", CatalogTick::new(1))
            .unwrap()
    );
    let report = manager.maintenance(CatalogTick::new(1), CollectBudget::new(8, 8, 0));
    assert_eq!(report.catalog.pruned_replicas, 1);
    let snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(snapshot.memory.used_bytes, 4096);
    assert_eq!(snapshot.memory.demand_bytes, 4096);
    assert_eq!(snapshot.memory.retiring_bytes, 0);

    pool.quiesce(OWNER, SECOND_MEMORY_ID).unwrap();
    pool.remove(OWNER, SECOND_MEMORY_ID).unwrap();
    assert!(
        !manager
            .exists(&tenant, "replicated", CatalogTick::new(2))
            .unwrap()
    );
    let report = manager.maintenance(CatalogTick::new(2), CollectBudget::new(8, 8, 0));
    assert_eq!(report.catalog.invalidated_published, 1);
    let snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(snapshot.memory.used_bytes, 0);
    assert_eq!(snapshot.memory.demand_bytes, 0);
    assert_eq!(snapshot.memory.retiring_bytes, 0);
}

#[test]
fn revoke_returns_reserved_quota_and_unknown_tenants_are_rejected() {
    let manager = manager(4096, 4096);
    let a = manager.resolve_tenant(&tenant_id("a")).unwrap();
    assert!(matches!(
        manager.resolve_tenant(&tenant_id("missing")),
        Err(TenantObjectError::TenantNotRegistered)
    ));
    manager
        .start_put(&a, "pending", admission(), plan(4096, 1), CatalogTick::ZERO)
        .unwrap();
    assert_eq!(
        manager
            .tenant_snapshot(&tenant_id("a"))
            .unwrap()
            .memory
            .reserved_bytes,
        4096
    );
    manager
        .revoke_put(
            &a,
            "pending",
            owner(),
            ReplicaSelector::All,
            CatalogTick::ZERO,
        )
        .unwrap();
    manager.maintenance(CatalogTick::ZERO, CollectBudget::new(8, 8, 8));
    let snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(snapshot.memory.demand_bytes, 0);
    assert_eq!(snapshot.memory.reserved_bytes, 0);
}

#[test]
fn tenant_upsert_reuses_charge_and_accounts_replacement_until_reclaim() {
    let manager = manager(12 * 1024, 4096);
    let a = manager.resolve_tenant(&tenant_id("a")).unwrap();
    manager
        .start_put(&a, "key", admission(), plan(4096, 1), CatalogTick::ZERO)
        .unwrap();
    manager
        .finish_put(&a, "key", owner(), ReplicaSelector::All)
        .unwrap();

    manager
        .start_upsert(&a, "key", admission(), plan(4096, 1), CatalogTick::new(1))
        .unwrap();
    let snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(snapshot.memory.demand_bytes, 4096);
    assert_eq!(snapshot.memory.reserved_bytes, 0);
    manager
        .revoke_put(
            &a,
            "key",
            owner(),
            ReplicaSelector::All,
            CatalogTick::new(1),
        )
        .unwrap();

    manager
        .start_upsert(&a, "key", admission(), plan(8192, 1), CatalogTick::new(2))
        .unwrap();
    let snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(snapshot.memory.used_bytes, 4096);
    assert_eq!(snapshot.memory.reserved_bytes, 8192);
    assert_eq!(snapshot.memory.demand_bytes, 12 * 1024);
    manager
        .finish_put(&a, "key", owner(), ReplicaSelector::All)
        .unwrap();
    let snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(snapshot.memory.used_bytes, 12 * 1024);
    assert_eq!(snapshot.memory.retiring_bytes, 4096);
    manager.maintenance(CatalogTick::new(3), CollectBudget::new(0, 8, 0));
    let snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(snapshot.memory.used_bytes, 8192);
    assert_eq!(snapshot.memory.retiring_bytes, 0);
    assert_eq!(snapshot.memory.demand_bytes, 8192);
}

#[test]
fn pending_timeout_returns_reserved_quota() {
    let manager = TenantObjectManager::with_config(
        pool(),
        ObjectCatalogConfig::default().with_pending_timeout(1),
        TenantConfig::multi(vec![(tenant_id("a"), policy(4096, 0))]),
    )
    .unwrap();
    let a = manager.resolve_tenant(&tenant_id("a")).unwrap();
    manager
        .start_put(&a, "expires", admission(), plan(4096, 1), CatalogTick::ZERO)
        .unwrap();

    let report = manager.maintenance(CatalogTick::new(1), CollectBudget::new(8, 8, 8));
    assert_eq!(report.expired_writes, 1);
    let snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(snapshot.memory.demand_bytes, 0);
    assert_eq!(snapshot.memory.reserved_bytes, 0);
}

#[test]
fn deleted_tenant_handles_stay_stale_after_reregistration() {
    let manager = manager(4096, 4096);
    let old = manager.resolve_tenant(&tenant_id("a")).unwrap();
    manager.delete_tenant(&tenant_id("a")).unwrap();
    let current = manager
        .upsert_tenant(tenant_id("a"), policy(4096, 0))
        .unwrap();

    assert_eq!(old.namespace(), current.namespace());
    assert_eq!(
        manager.exists(&old, "key", CatalogTick::ZERO),
        Err(TenantObjectError::InvalidTenantHandle)
    );
    assert!(!manager.exists(&current, "key", CatalogTick::ZERO).unwrap());
    assert_eq!(
        manager.tenant_snapshot(&tenant_id("a")).unwrap().generation,
        2
    );
}

#[test]
fn resolved_tenant_handles_cannot_cross_manager_boundaries() {
    let first = manager(4096, 4096);
    let second = manager(4096, 4096);
    let tenant = first.resolve_tenant(&tenant_id("a")).unwrap();

    assert_eq!(
        second.exists(&tenant, "key", CatalogTick::ZERO),
        Err(TenantObjectError::InvalidTenantHandle)
    );
    assert_eq!(
        second.start_put(
            &tenant,
            "key",
            admission(),
            plan(4096, 1),
            CatalogTick::ZERO,
        ),
        Err(TenantObjectError::InvalidTenantHandle)
    );
}

#[test]
fn retiring_quota_is_released_only_after_the_last_read_handle() {
    let manager = manager(4096, 4096);
    let a = manager.resolve_tenant(&tenant_id("a")).unwrap();
    manager
        .start_put(&a, "held", admission(), plan(4096, 1), CatalogTick::ZERO)
        .unwrap();
    manager
        .finish_put(&a, "held", owner(), ReplicaSelector::All)
        .unwrap();
    let read = manager.get(&a, "held", CatalogTick::ZERO).unwrap();
    let lease_expires_at = read.lease_expires_at();

    manager.upsert_tenant(tenant_id("a"), policy(0, 0)).unwrap();
    // First chance clears the recent bit; second chance retires the object.
    manager.maintenance(lease_expires_at, CollectBudget::new(8, 8, 8));
    let report = manager.maintenance(lease_expires_at, CollectBudget::new(8, 8, 8));
    assert_eq!(report.catalog.scoped_retired_objects, 1);
    assert_eq!(report.catalog.reclaimed_objects, 0);
    let snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(snapshot.memory.demand_bytes, 4096);
    assert_eq!(snapshot.memory.retiring_bytes, 4096);

    drop(read);
    let report = manager.maintenance(
        lease_expires_at.saturating_add(1),
        CollectBudget::new(8, 8, 8),
    );
    assert_eq!(report.catalog.reclaimed_objects, 1);
    let snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(snapshot.memory.demand_bytes, 0);
    assert_eq!(snapshot.memory.used_bytes, 0);
    assert_eq!(snapshot.memory.retiring_bytes, 0);
}

#[test]
fn quota_shrink_reclaims_only_the_target_tenant_and_does_not_cancel_pending_puts() {
    let manager = manager(8192, 8192);
    let a = manager.resolve_tenant(&tenant_id("a")).unwrap();
    let b = manager.resolve_tenant(&tenant_id("b")).unwrap();
    for (tenant, key) in [(&a, "published-a"), (&b, "published-b")] {
        manager
            .start_put(tenant, key, admission(), plan(4096, 1), CatalogTick::ZERO)
            .unwrap();
        manager
            .finish_put(tenant, key, owner(), ReplicaSelector::All)
            .unwrap();
    }
    manager
        .start_put(
            &a,
            "pending-a",
            admission(),
            plan(4096, 1),
            CatalogTick::ZERO,
        )
        .unwrap();

    manager.upsert_tenant(tenant_id("a"), policy(0, 0)).unwrap();
    let report = manager.maintenance(CatalogTick::ZERO, CollectBudget::new(16, 16, 16));
    assert_eq!(report.catalog.scoped_retired_objects, 1);
    assert!(
        !manager
            .exists(&a, "published-a", CatalogTick::ZERO)
            .unwrap()
    );
    assert!(
        manager
            .exists(&b, "published-b", CatalogTick::ZERO)
            .unwrap()
    );

    // Shrink does not cancel an already-admitted pending write.
    manager
        .finish_put(&a, "pending-a", owner(), ReplicaSelector::All)
        .unwrap();
    let report = manager.maintenance(CatalogTick::ZERO, CollectBudget::new(16, 16, 16));
    assert_eq!(report.catalog.scoped_retired_objects, 1);
    assert_eq!(
        manager
            .tenant_snapshot(&tenant_id("a"))
            .unwrap()
            .memory
            .demand_bytes,
        0
    );
    assert_eq!(
        manager.delete_tenant(&tenant_id("b")),
        Err(TenantAdminError::TenantNotEmpty)
    );
    manager.delete_tenant(&tenant_id("a")).unwrap();
    assert!(matches!(
        manager.resolve_tenant(&tenant_id("a")),
        Err(TenantObjectError::TenantNotRegistered)
    ));
}

#[test]
fn oversubscribed_requested_quotas_are_scaled_deterministically() {
    let manager = manager(CAPACITY, CAPACITY);
    assert_eq!(
        manager
            .list_tenants()
            .into_iter()
            .map(|tenant| tenant.id)
            .collect::<Vec<_>>(),
        vec![tenant_id("a"), tenant_id("b")]
    );
    let a = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    let b = manager.tenant_snapshot(&tenant_id("b")).unwrap();
    assert_eq!(a.memory.requested_bytes, CAPACITY);
    assert_eq!(b.memory.requested_bytes, CAPACITY);
    assert_eq!(a.memory.effective_bytes, CAPACITY / 2);
    assert_eq!(b.memory.effective_bytes, CAPACITY / 2);
}

#[test]
fn single_mode_collapses_external_tenant_ids_without_quota() {
    let manager = TenantObjectManager::new(pool(), TenantConfig::Single).unwrap();
    let a = manager.resolve_tenant(&tenant_id("a")).unwrap();
    let b = manager.resolve_tenant(&tenant_id("b")).unwrap();
    manager
        .start_put(&a, "shared", admission(), plan(4096, 1), CatalogTick::ZERO)
        .unwrap();
    manager
        .finish_put(&b, "shared", owner(), ReplicaSelector::All)
        .unwrap();
    assert!(manager.exists(&a, "shared", CatalogTick::ZERO).unwrap());
    assert!(manager.exists(&b, "shared", CatalogTick::ZERO).unwrap());
}

#[test]
fn batch_admission_is_atomic_per_resource_class() {
    let manager = manager(8192, 8192);
    let a = manager.resolve_tenant(&tenant_id("a")).unwrap();
    let requests = vec![
        TenantPutRequest::new("one", plan(4096, 1)),
        TenantPutRequest::new("two", plan(4096, 1)),
    ];
    let results = manager.start_put_batch(&a, admission(), requests, CatalogTick::ZERO);
    assert!(results.iter().all(Result::is_ok));
    let snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(snapshot.memory.reserved_bytes, 8192);

    let rejected = manager.start_put_batch(
        &a,
        admission(),
        vec![
            TenantPutRequest::new("three", plan(1, 1)),
            TenantPutRequest::new("four", plan(1, 1)),
        ],
        CatalogTick::ZERO,
    );
    assert!(
        rejected
            .iter()
            .all(|result| matches!(result, Err(TenantObjectError::TenantQuotaExceeded { .. })))
    );
}

#[test]
fn accepting_capacity_changes_recompute_effective_quota_and_trigger_reclaim() {
    let pool = pool();
    let manager = TenantObjectManager::new(
        pool.clone(),
        TenantConfig::multi(vec![(tenant_id("a"), policy(CAPACITY, 0))]),
    )
    .unwrap();
    let a = manager.resolve_tenant(&tenant_id("a")).unwrap();
    manager
        .start_put(&a, "object", admission(), plan(4096, 1), CatalogTick::ZERO)
        .unwrap();
    manager
        .finish_put(&a, "object", owner(), ReplicaSelector::All)
        .unwrap();

    pool.quiesce(OWNER, MEMORY_ID).unwrap();
    let report = manager.maintenance(CatalogTick::ZERO, CollectBudget::new(8, 8, 8));
    assert_eq!(report.catalog.scoped_retired_objects, 1);
    let snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(snapshot.memory.effective_bytes, 0);
    assert_eq!(snapshot.memory.demand_bytes, 0);

    pool.reactivate(OWNER, MEMORY_ID).unwrap();
    manager.maintenance(CatalogTick::ZERO, CollectBudget::new(8, 8, 8));
    assert_eq!(
        manager
            .tenant_snapshot(&tenant_id("a"))
            .unwrap()
            .memory
            .effective_bytes,
        CAPACITY
    );
}

#[test]
fn best_effort_actual_replica_growth_rechecks_quota_and_rolls_back_placement() {
    let pool = pool();
    pool.attach(SegmentSpec::memory(
        SegmentIdentity::new(SECOND_MEMORY_ID, OWNER, "memory-2"),
        MemoryRegion::new(0x4_0000_0000, CAPACITY),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12346"),
    ))
    .unwrap();
    let manager = TenantObjectManager::new(
        pool.clone(),
        TenantConfig::multi(vec![(tenant_id("a"), policy(4096, 0))]),
    )
    .unwrap();
    let a = manager.resolve_tenant(&tenant_id("a")).unwrap();

    assert!(matches!(
        manager.start_put(
            &a,
            "two-replicas",
            admission(),
            plan(4096, 2),
            CatalogTick::ZERO
        ),
        Err(TenantObjectError::TenantQuotaExceeded { .. })
    ));
    assert_eq!(
        manager
            .tenant_snapshot(&tenant_id("a"))
            .unwrap()
            .memory
            .demand_bytes,
        0
    );
    assert_eq!(pool.stats(MEMORY_ID).unwrap().usage.active_allocations, 0);
    assert_eq!(
        pool.stats(SECOND_MEMORY_ID)
            .unwrap()
            .usage
            .active_allocations,
        0
    );
}

#[test]
fn memory_and_nof_quotas_are_admitted_and_accounted_independently() {
    let pool = pool();
    pool.attach(SegmentSpec::nof(
        SegmentIdentity::new(NOF_ID, OWNER, "nof"),
        MemoryRegion::new(0, CAPACITY),
        "nvme://127.0.0.1/nqn.tenant",
    ))
    .unwrap();
    let manager = TenantObjectManager::new(
        pool,
        TenantConfig::multi(vec![(tenant_id("a"), policy(4096, 8192))]),
    )
    .unwrap();
    let a = manager.resolve_tenant(&tenant_id("a")).unwrap();
    manager
        .start_put(&a, "memory", admission(), plan(4096, 1), CatalogTick::ZERO)
        .unwrap();
    manager
        .finish_put(&a, "memory", owner(), ReplicaSelector::All)
        .unwrap();
    manager
        .start_put(
            &a,
            "nof",
            admission(),
            plan_for(8192, 1, ReplicaClass::Nof, FulfillmentPolicy::AllOrNothing),
            CatalogTick::ZERO,
        )
        .unwrap();
    manager
        .finish_put(&a, "nof", owner(), ReplicaSelector::All)
        .unwrap();

    let snapshot = manager.tenant_snapshot(&tenant_id("a")).unwrap();
    assert_eq!(snapshot.memory.used_bytes, 4096);
    assert_eq!(snapshot.nof.used_bytes, 8192);
    assert!(matches!(
        manager.start_put(
            &a,
            "memory-over",
            admission(),
            plan(1, 1),
            CatalogTick::ZERO,
        ),
        Err(TenantObjectError::TenantQuotaExceeded {
            class: cakemaster::object::TenantResourceClass::Memory,
            ..
        })
    ));
    assert!(matches!(
        manager.start_put(
            &a,
            "nof-over",
            admission(),
            plan_for(1, 1, ReplicaClass::Nof, FulfillmentPolicy::AllOrNothing,),
            CatalogTick::ZERO,
        ),
        Err(TenantObjectError::TenantQuotaExceeded {
            class: cakemaster::object::TenantResourceClass::Nof,
            ..
        })
    ));
}
