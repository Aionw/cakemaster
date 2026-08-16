use cakemaster::object::error::ObjectManagerError;
use cakemaster::object::reclamation::{CatalogTick, CollectBudget};
use cakemaster::object::{
    MemoryEvictionConfig, MemoryEvictionConfigError, NamespaceId, ObjectCatalogConfig,
    ObjectContent, ObjectIdentity, ObjectManager, ObjectPutPlan, ReplicaSelector, WriteAdmission,
    WriteOwner,
};
use cakemaster::segment::placement::{AllocationSpec, PlacementRequest, ReplicaPolicy};
use cakemaster::segment::{
    ClientId, CxlArenaId, CxlArenaSpec, MemoryRegion, ReplicaClass, SegmentId, SegmentIdentity,
    SegmentPool, SegmentSpec, TransportEndpoint, TransportProtocol,
};
use cakemaster::server::{MasterClock, MasterReconcileConfig, ObjectCatalogRpcService};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tokio::sync::oneshot;

const OWNER: ClientId = ClientId::new(101, 103);
const BYTES: u64 = 4096;

fn segment(index: u64, capacity: u64) -> SegmentSpec {
    SegmentSpec::memory(
        SegmentIdentity::new(
            SegmentId::new(107, index),
            OWNER,
            format!("watermark-{index}"),
        ),
        MemoryRegion::new(0x7_0000_0000 + index * (capacity + BYTES), capacity),
        TransportEndpoint::new(
            TransportProtocol::Tcp,
            format!("127.0.0.1:{}", 12_000 + index),
        ),
    )
}

fn manager(capacity: u64, eviction: MemoryEvictionConfig) -> Arc<ObjectManager> {
    let pool = Arc::new(SegmentPool::new());
    pool.attach(segment(1, capacity)).unwrap();
    Arc::new(
        ObjectManager::with_eviction_config(
            pool,
            ObjectCatalogConfig::new(128).with_lease(1, 0),
            eviction,
        )
        .unwrap(),
    )
}

fn identity(index: usize) -> ObjectIdentity {
    ObjectIdentity::new(NamespaceId::DEFAULT, format!("watermark-object-{index}"))
}

fn plan(bytes: u64) -> ObjectPutPlan {
    ObjectPutPlan::new(
        ObjectContent::new(bytes),
        PlacementRequest::new(AllocationSpec::new(bytes), ReplicaPolicy::new(1)),
    )
}

fn nof_plan(bytes: u64) -> ObjectPutPlan {
    ObjectPutPlan::new(
        ObjectContent::new(bytes),
        PlacementRequest::new(AllocationSpec::new(bytes), ReplicaPolicy::new(1))
            .for_replica_class(ReplicaClass::Nof),
    )
}

fn publish(manager: &ObjectManager, index: usize, now: CatalogTick) {
    let identity = identity(index);
    manager
        .start_put(
            identity.clone(),
            WriteAdmission::unmanaged(OWNER),
            plan(BYTES),
            now,
        )
        .unwrap();
    manager
        .finish_put(&identity, WriteOwner::new(OWNER), ReplicaSelector::All)
        .unwrap();
}

#[test]
fn watermarks_and_failure_limits_are_strictly_validated() {
    assert_eq!(
        MemoryEvictionConfig::new(f64::NAN, 0.5),
        Err(MemoryEvictionConfigError::InvalidHighWatermark)
    );
    assert_eq!(
        MemoryEvictionConfig::new(0.9, f64::INFINITY),
        Err(MemoryEvictionConfigError::InvalidLowWatermark)
    );
    assert_eq!(
        MemoryEvictionConfig::new(0.8, 0.8),
        Err(MemoryEvictionConfigError::LowNotBelowHigh)
    );
    assert_eq!(
        MemoryEvictionConfig::new(0.500_000_1, 0.5),
        Err(MemoryEvictionConfigError::LowNotBelowHigh),
        "ratios that collapse to one integer threshold must be rejected"
    );

    let config = MemoryEvictionConfig::new(0.9, 0.8).unwrap();
    assert_eq!(config.high_watermark_ratio(), 0.9);
    assert_eq!(config.low_watermark_ratio(), 0.8);
    assert_eq!(
        config.with_allocation_failure_policy(CollectBudget::new(0, 1, 0), 1),
        Err(MemoryEvictionConfigError::ZeroFailureCandidates)
    );
    assert_eq!(
        config.with_allocation_failure_policy(CollectBudget::new(1, 0, 0), 1),
        Err(MemoryEvictionConfigError::ZeroFailureReclaims)
    );
    assert_eq!(
        config.with_allocation_failure_policy(CollectBudget::new(1, 1, 0), 4),
        Err(MemoryEvictionConfigError::TooManyAllocationRetries)
    );
}

#[test]
fn aggregate_space_deduplicates_cxl_and_keeps_quiesced_capacity() {
    let pool = SegmentPool::new();
    let arena = CxlArenaSpec::new(CxlArenaId::new("watermark-shared"), 8 * BYTES);
    let first = SegmentId::new(109, 1);
    let second = SegmentId::new(109, 2);
    pool.attach(SegmentSpec::cxl(
        SegmentIdentity::new(first, OWNER, "cxl-first"),
        arena.clone(),
    ))
    .unwrap();
    pool.attach(SegmentSpec::cxl(
        SegmentIdentity::new(second, OWNER, "cxl-second"),
        arena,
    ))
    .unwrap();
    let reservation = pool.reserve_on(first, BYTES).unwrap();

    let space = pool.space_for(ReplicaClass::Memory);
    assert_eq!(space.capacity_bytes, 8 * BYTES);
    assert_eq!(space.used_bytes, BYTES);
    pool.quiesce(OWNER, first).unwrap();
    pool.quiesce(OWNER, second).unwrap();
    let quiesced = pool.space_for(ReplicaClass::Memory);
    assert_eq!(quiesced.capacity_bytes, space.capacity_bytes);
    assert_eq!(quiesced.used_bytes, space.used_bytes);
    assert_eq!(pool.capacity_for(ReplicaClass::Memory).capacity_bytes(), 0);
    drop(reservation);
}

#[test]
fn active_cycle_runs_in_bounded_steps_until_physical_usage_reaches_low() {
    let eviction = MemoryEvictionConfig::new(0.80, 0.50).unwrap();
    let manager = manager(10 * BYTES, eviction);
    for index in 0..9 {
        publish(&manager, index, CatalogTick::ZERO);
    }

    let first = manager.maintenance(CatalogTick::new(2), CollectBudget::new(2, 2, 0));
    assert_eq!(first.catalog.retired_objects, 2);
    assert_eq!(first.catalog.reclaimed_objects, 2);
    let first_stats = first.memory_eviction.unwrap();
    assert!(first_stats.active);
    assert_eq!(first_stats.capacity_bytes, 10 * BYTES);
    assert_eq!(first_stats.used_bytes, 7 * BYTES);
    assert_eq!(first_stats.maximum_used_bytes, 9 * BYTES);
    assert_eq!(first_stats.maximum_used_ratio_ppm, 900_000);
    assert_eq!(first_stats.low_watermark_bytes, 5 * BYTES);
    assert_eq!(first_stats.watermark_reclaim_debt_bytes, 2 * BYTES);

    let second = manager.maintenance(CatalogTick::new(3), CollectBudget::new(2, 2, 0));
    assert_eq!(second.catalog.reclaimed_objects, 2);
    let final_stats = manager.memory_eviction_stats().unwrap();
    assert!(!final_stats.active);
    assert_eq!(final_stats.used_bytes, 5 * BYTES);
    assert_eq!(manager.catalog().stats().watermark_reclaim_debt, 0);
    assert_eq!(manager.catalog().stats().published_objects, 5);
}

#[test]
fn retired_pin_remains_used_and_covers_inflight_debt_without_duplicate_eviction() {
    let eviction = MemoryEvictionConfig::new(0.75, 0.25).unwrap();
    let manager = manager(BYTES, eviction);
    publish(&manager, 0, CatalogTick::ZERO);
    let pinned = manager
        .get(identity(0).as_lookup(), CatalogTick::ZERO)
        .unwrap();

    let first = manager.maintenance(CatalogTick::new(1), CollectBudget::new(8, 8, 0));
    assert_eq!(
        first.catalog.retired_objects, 0,
        "second chance is preserved"
    );
    let second = manager.maintenance(CatalogTick::new(2), CollectBudget::new(8, 8, 0));
    assert_eq!(second.catalog.retired_objects, 1);
    assert_eq!(second.catalog.reclaimed_objects, 0);
    let pinned_stats = manager.memory_eviction_stats().unwrap();
    assert!(pinned_stats.active);
    assert_eq!(pinned_stats.used_bytes, BYTES);
    assert_eq!(pinned_stats.live_bytes, 0);
    assert_eq!(pinned_stats.retired_bytes, BYTES);
    assert_eq!(pinned_stats.watermark_reclaim_debt_bytes, 3 * BYTES / 4);

    let third = manager.maintenance(CatalogTick::new(3), CollectBudget::new(8, 8, 0));
    assert_eq!(third.catalog.retired_objects, 0);
    assert_eq!(manager.catalog().stats().retired_candidates, 1);
    drop(pinned);

    let released = manager.maintenance(CatalogTick::new(4), CollectBudget::new(8, 8, 0));
    assert_eq!(released.catalog.reclaimed_objects, 1);
    assert_eq!(manager.memory_eviction_stats().unwrap().used_bytes, 0);
    assert_eq!(manager.catalog().stats().reclaim_debt, 0);
}

#[test]
fn capacity_growth_cancels_stale_watermark_debt_before_retiring_objects() {
    let eviction = MemoryEvictionConfig::new(0.70, 0.50).unwrap();
    let manager = manager(8 * BYTES, eviction);
    for index in 0..6 {
        publish(&manager, index, CatalogTick::ZERO);
    }
    let armed = manager.maintenance(CatalogTick::new(2), CollectBudget::new(0, 0, 0));
    assert!(armed.memory_eviction.unwrap().active);
    assert_eq!(manager.catalog().stats().watermark_reclaim_debt, 2 * BYTES);

    manager.pool().attach(segment(2, 8 * BYTES)).unwrap();
    let grown = manager.maintenance(CatalogTick::new(3), CollectBudget::new(32, 32, 0));
    assert_eq!(grown.catalog.retired_objects, 0);
    assert_eq!(manager.catalog().stats().published_objects, 6);
    assert_eq!(manager.catalog().stats().watermark_reclaim_debt, 0);
    let stats = manager.memory_eviction_stats().unwrap();
    assert!(!stats.active);
    assert_eq!(stats.capacity_bytes, 16 * BYTES);
    assert_eq!(stats.used_bytes, 6 * BYTES);
}

#[test]
fn allocation_failure_runs_one_bounded_collection_and_retries_after_progress() {
    let eviction = MemoryEvictionConfig::new(0.90, 0.60)
        .unwrap()
        .with_allocation_failure_policy(CollectBudget::new(1, 1, 0), 1)
        .unwrap();
    let manager = manager(3 * BYTES, eviction);
    for index in 0..3 {
        publish(&manager, index, CatalogTick::ZERO);
    }

    let started = manager
        .start_put(
            identity(3),
            WriteAdmission::unmanaged(OWNER),
            plan(BYTES),
            CatalogTick::new(2),
        )
        .unwrap();
    assert_eq!(started.replicas().len(), 1);
    let stats = manager.memory_eviction_stats().unwrap();
    assert_eq!(stats.allocation_failures, 1);
    assert_eq!(stats.allocation_retries, 1);
    assert_eq!(stats.allocation_retry_successes, 1);
    assert_eq!(stats.reclaimed_objects, 1);
    assert_eq!(manager.catalog().stats().pending_objects, 1);
    assert_eq!(manager.catalog().stats().published_objects, 2);
}

#[test]
fn allocation_failure_does_not_retry_without_observed_reclaim_progress() {
    let eviction = MemoryEvictionConfig::new(0.90, 0.60)
        .unwrap()
        .with_allocation_failure_policy(CollectBudget::new(1, 1, 0), 1)
        .unwrap();
    let manager = manager(BYTES, eviction);
    publish(&manager, 0, CatalogTick::ZERO);
    let leased = manager
        .get(identity(0).as_lookup(), CatalogTick::ZERO)
        .unwrap();

    assert_eq!(
        manager.start_put(
            identity(1),
            WriteAdmission::unmanaged(OWNER),
            plan(BYTES),
            CatalogTick::ZERO,
        ),
        Err(ObjectManagerError::NoAvailableReplicas)
    );
    let stats = manager.memory_eviction_stats().unwrap();
    assert_eq!(stats.allocation_failures, 1);
    assert_eq!(stats.allocation_retries, 0);
    assert_eq!(stats.controller_steps, 1);
    drop(leased);
}

#[test]
fn nof_allocation_failure_does_not_issue_memory_reclaim_debt() {
    let manager = manager(BYTES, MemoryEvictionConfig::default());
    assert_eq!(
        manager.start_put(
            identity(0),
            WriteAdmission::unmanaged(OWNER),
            nof_plan(BYTES),
            CatalogTick::ZERO,
        ),
        Err(ObjectManagerError::NoAvailableReplicas)
    );
    assert_eq!(manager.catalog().stats().reclaim_debt, 0);
    let stats = manager.memory_eviction_stats().unwrap();
    assert_eq!(stats.allocation_failures, 0);
    assert_eq!(stats.controller_steps, 0);
}

#[tokio::test(start_paused = true)]
async fn allocation_failure_wakes_the_owned_reconciler_and_shutdown_joins_it() {
    let eviction = MemoryEvictionConfig::new(0.90, 0.60)
        .unwrap()
        .with_allocation_failure_policy(CollectBudget::new(1, 1, 0), 0)
        .unwrap();
    let manager = manager(BYTES, eviction);
    publish(&manager, 0, CatalogTick::ZERO);
    let leased = manager
        .get(identity(0).as_lookup(), CatalogTick::ZERO)
        .unwrap();
    let clock = MasterClock::new();
    let service = ObjectCatalogRpcService::new_with_clock(manager.clone(), clock);
    let reconciler = service.reconciler(
        MasterReconcileConfig::new(Duration::from_secs(60), CollectBudget::new(1, 1, 0)).unwrap(),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(reconciler.run_until(async {
        let _ = shutdown_rx.await;
    }));
    tokio::task::yield_now().await;

    assert_eq!(
        manager.start_put(
            identity(1),
            WriteAdmission::unmanaged(OWNER),
            plan(BYTES),
            CatalogTick::ZERO,
        ),
        Err(ObjectManagerError::NoAvailableReplicas)
    );
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert!(
        manager.memory_eviction_stats().unwrap().controller_steps >= 2,
        "one synchronous failure step must be followed by the notified reconciler step"
    );

    shutdown_tx.send(()).unwrap();
    task.await.unwrap();
    let stopped_steps = manager.memory_eviction_stats().unwrap().controller_steps;
    tokio::time::advance(Duration::from_secs(120)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        manager.memory_eviction_stats().unwrap().controller_steps,
        stopped_steps
    );
    drop(leased);
}

#[test]
fn concurrent_allocators_and_bounded_collection_converge_without_debt_oscillation() {
    let eviction = MemoryEvictionConfig::new(0.75, 0.50)
        .unwrap()
        .with_allocation_failure_policy(CollectBudget::new(4, 4, 0), 1)
        .unwrap();
    let manager = manager(64 * BYTES, eviction);
    for index in 0..56 {
        publish(&manager, index, CatalogTick::ZERO);
    }

    let workers = 4;
    let start = Arc::new(Barrier::new(workers + 2));
    thread::scope(|scope| {
        for worker in 0..workers {
            let manager = manager.clone();
            let start = start.clone();
            scope.spawn(move || {
                start.wait();
                for sequence in 0..64 {
                    let index = 1_000 + worker * 64 + sequence;
                    let identity = identity(index);
                    if manager
                        .start_put(
                            identity.clone(),
                            WriteAdmission::unmanaged(OWNER),
                            plan(BYTES),
                            CatalogTick::new(10 + sequence as u64),
                        )
                        .is_ok()
                    {
                        manager
                            .finish_put(&identity, WriteOwner::new(OWNER), ReplicaSelector::All)
                            .unwrap();
                    }
                }
            });
        }
        let collector = manager.clone();
        let collector_start = start.clone();
        scope.spawn(move || {
            collector_start.wait();
            for tick in 10..266 {
                let _ = collector.maintenance(CatalogTick::new(tick), CollectBudget::new(4, 4, 0));
                thread::yield_now();
            }
        });
        start.wait();
    });

    for tick in 300..600 {
        let stats = manager.catalog().stats();
        let usage = manager.pool().space_for(ReplicaClass::Memory);
        if stats.reclaim_debt == 0 && stats.retired_bytes == 0 && usage.used_bytes <= 32 * BYTES {
            break;
        }
        let _ = manager.maintenance(CatalogTick::new(tick), CollectBudget::new(16, 16, 0));
    }

    let catalog = manager.catalog().stats();
    let usage = manager.pool().space_for(ReplicaClass::Memory);
    let eviction = manager.memory_eviction_stats().unwrap();
    assert!(eviction.trigger_events > 0);
    assert!(eviction.controller_steps > 0);
    assert_eq!(catalog.pending_objects, 0);
    assert_eq!(catalog.retired_bytes, 0);
    assert_eq!(catalog.reclaim_debt, 0);
    assert!(usage.used_bytes <= eviction.low_watermark_bytes);
    assert_eq!(usage.used_bytes, catalog.live_bytes);
}
