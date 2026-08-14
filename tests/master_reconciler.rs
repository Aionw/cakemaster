use cakemaster::client::{ClientId, ClientLifecycleConfig, ClientTick};
use cakemaster::mooncake::{Uuid, WrappedMasterService};
use cakemaster::object::error::LookupError;
use cakemaster::object::reclamation::CollectBudget;
use cakemaster::object::{
    NamespaceId, ObjectContent, ObjectIdentity, ObjectManager, ObjectPutPlan, ReplicaSelector,
    TenantConfig, TenantId, TenantObjectManager,
};
use cakemaster::segment::placement::{AllocationSpec, PlacementRequest, ReplicaPolicy};
use cakemaster::segment::{
    MemoryRegion, SegmentId, SegmentIdentity, SegmentPool, SegmentSpec, TransportEndpoint,
    TransportProtocol,
};
use cakemaster::server::{
    DEFAULT_OBJECT_COLLECTION_BUDGET, DEFAULT_RECONCILE_INTERVAL, MasterClock,
    MasterReconcileConfig, MasterReconcileConfigError, ObjectCatalogRpcService,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Barrier, oneshot};

const CLIENT: ClientId = ClientId::new(11, 17);
const SEGMENT_BYTES: u64 = 1 << 20;

fn segment(client: ClientId, index: u64) -> SegmentSpec {
    SegmentSpec::memory(
        SegmentIdentity::new(SegmentId::new(31, index), client, format!("memory-{index}")),
        MemoryRegion::new(0x4_0000_0000 + index * SEGMENT_BYTES * 2, SEGMENT_BYTES),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
    )
}

fn identity(key: &str) -> ObjectIdentity {
    ObjectIdentity::new(NamespaceId::DEFAULT, key)
}

fn plan(bytes: u64) -> ObjectPutPlan {
    ObjectPutPlan::new(
        ObjectContent::new(bytes),
        PlacementRequest::new(AllocationSpec::new(bytes), ReplicaPolicy::new(1)),
    )
}

fn single_service(
    ttl_millis: u64,
    cleanup_scan_budget: usize,
) -> (
    Arc<SegmentPool>,
    Arc<ObjectManager>,
    ObjectCatalogRpcService,
) {
    let pool = Arc::new(SegmentPool::new());
    let manager = Arc::new(ObjectManager::new(pool.clone()));
    let service = ObjectCatalogRpcService::new_with_client_config(
        manager.clone(),
        ClientLifecycleConfig::new(64)
            .with_ttl(ttl_millis)
            .with_cleanup_scan_budget(cleanup_scan_budget),
        MasterClock::new(),
        1,
    )
    .unwrap();
    (pool, manager, service)
}

#[test]
fn reconcile_config_validates_interval_and_preserves_budget() {
    let default = MasterReconcileConfig::default();
    assert_eq!(default.interval(), DEFAULT_RECONCILE_INTERVAL);
    assert_eq!(default.object_budget(), DEFAULT_OBJECT_COLLECTION_BUDGET);

    let budget = CollectBudget::new(7, 5, 3);
    let config = MasterReconcileConfig::new(Duration::from_secs(2), budget).unwrap();
    assert_eq!(config.interval(), Duration::from_secs(2));
    assert_eq!(config.object_budget(), budget);
    assert_eq!(
        MasterReconcileConfig::new(Duration::ZERO, budget),
        Err(MasterReconcileConfigError::ZeroInterval)
    );
}

#[tokio::test(start_paused = true)]
async fn one_step_closes_client_segment_and_object_lifecycles() {
    let (pool, manager, service) = single_service(10, 16);
    let clients = service.client_manager().clone();
    clients
        .remount(
            CLIENT,
            vec![segment(CLIENT, 1)],
            service.clock().client_now(),
        )
        .unwrap();

    let admission = clients.write_admission(CLIENT).unwrap();
    let pending = identity("pending");
    manager
        .start_put(
            pending.clone(),
            admission.clone(),
            plan(4096),
            service.clock().now(),
        )
        .unwrap();
    let published = identity("published");
    manager
        .start_put(
            published.clone(),
            admission,
            plan(4096),
            service.clock().now(),
        )
        .unwrap();
    manager
        .finish_put(
            &published,
            clients.write_owner(CLIENT).unwrap(),
            ReplicaSelector::All,
        )
        .unwrap();

    tokio::time::advance(Duration::from_millis(10)).await;
    let report = service
        .reconciler(MasterReconcileConfig::default())
        .reconcile_once();
    let cleanup = report.client_cleanup.unwrap();
    assert_eq!(cleanup.completed_sessions, 1);
    assert_eq!(cleanup.revoked_pending_writes, 1);
    assert_eq!(cleanup.invalidated_segments, 1);
    assert_eq!(report.object_collection.catalog.invalidated_published, 1);
    assert!(pool.is_empty());
    assert!(matches!(
        manager.get(pending.as_lookup(), service.clock().now()),
        Err(LookupError::NotFound)
    ));
    assert!(matches!(
        manager.get(published.as_lookup(), service.clock().now()),
        Err(LookupError::NotFound)
    ));
}

#[tokio::test(start_paused = true)]
async fn object_collection_budget_bounds_each_reconcile_step() {
    let (pool, manager, service) = single_service(10, 16);
    let clients = service.client_manager().clone();
    clients
        .remount(
            CLIENT,
            vec![segment(CLIENT, 1)],
            service.clock().client_now(),
        )
        .unwrap();
    let admission = clients.write_admission(CLIENT).unwrap();
    let owner = clients.write_owner(CLIENT).unwrap();
    for index in 0..3 {
        let object = identity(&format!("published-{index}"));
        manager
            .start_put(
                object.clone(),
                admission.clone(),
                plan(4096),
                service.clock().now(),
            )
            .unwrap();
        manager
            .finish_put(&object, owner, ReplicaSelector::All)
            .unwrap();
    }

    let config =
        MasterReconcileConfig::new(Duration::from_millis(100), CollectBudget::new(1, 0, 0))
            .unwrap();
    let reconciler = service.reconciler(config);
    tokio::time::advance(Duration::from_millis(10)).await;

    let first = reconciler.reconcile_once();
    assert_eq!(first.client_cleanup.unwrap().invalidated_segments, 1);
    assert_eq!(first.object_collection.catalog.invalidated_published, 1);
    assert_eq!(manager.catalog().stats().published_objects, 2);

    assert_eq!(
        reconciler
            .reconcile_once()
            .object_collection
            .catalog
            .invalidated_published,
        1
    );
    assert_eq!(manager.catalog().stats().published_objects, 1);
    assert_eq!(
        reconciler
            .reconcile_once()
            .object_collection
            .catalog
            .invalidated_published,
        1
    );
    assert_eq!(manager.catalog().stats().published_objects, 0);
    assert!(pool.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_and_background_collection_converge_without_duplicate_retirement() {
    let (pool, manager, service) = single_service(1, 64);
    let clients = service.client_manager().clone();
    clients
        .remount(
            CLIENT,
            vec![segment(CLIENT, 1)],
            service.clock().client_now(),
        )
        .unwrap();
    let admission = clients.write_admission(CLIENT).unwrap();
    let owner = clients.write_owner(CLIENT).unwrap();
    let keys: Vec<_> = (0..512).map(|index| format!("race-{index}")).collect();
    for key in &keys {
        let object = identity(key);
        manager
            .start_put(
                object.clone(),
                admission.clone(),
                plan(1024),
                service.clock().now(),
            )
            .unwrap();
        manager
            .finish_put(&object, owner, ReplicaSelector::All)
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(2)).await;

    let reconciler = service.reconciler(
        MasterReconcileConfig::new(DEFAULT_RECONCILE_INTERVAL, CollectBudget::new(1, 1, 1))
            .unwrap(),
    );
    let service = Arc::new(service);
    let start = Arc::new(Barrier::new(3));
    let reconcile_task = {
        let reconciler = reconciler.clone();
        let start = start.clone();
        tokio::spawn(async move {
            start.wait().await;
            for _ in 0..512 {
                let _ = reconciler.reconcile_once();
                tokio::task::yield_now().await;
            }
        })
    };
    let rpc_task = {
        let service = service.clone();
        let start = start.clone();
        tokio::spawn(async move {
            start.wait().await;
            for _ in 0..16 {
                service
                    .batch_exist_key(keys.clone(), String::new())
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        })
    };
    start.wait().await;
    reconcile_task.await.unwrap();
    rpc_task.await.unwrap();

    for _ in 0..512 {
        if manager.catalog().stats().published_objects == 0 {
            break;
        }
        let _ = reconciler.reconcile_once();
    }
    assert_eq!(manager.catalog().stats().published_objects, 0);
    assert_eq!(manager.catalog().stats().retired_candidates, 0);
    assert_eq!(manager.catalog().stats().retired_bytes, 0);
    assert!(pool.is_empty());
}

#[tokio::test(start_paused = true)]
async fn periodic_reconcile_waits_for_the_first_tick_and_skips_missed_ticks() {
    let (pool, _manager, service) = single_service(1, 1);
    let clients = service.client_manager().clone();
    for index in 1..=3 {
        let client = ClientId::new(41, index);
        clients
            .remount(
                client,
                vec![segment(client, index)],
                service.clock().client_now(),
            )
            .unwrap();
    }

    let reconciler = service.reconciler(MasterReconcileConfig::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(reconciler.run_until(async {
        let _ = shutdown_rx.await;
    }));
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_millis(99)).await;
    tokio::task::yield_now().await;
    assert_eq!(pool.len(), 3);

    // Advancing across several historical deadlines produces only one step:
    // MissedTickBehavior::Skip schedules the next tick from the current time.
    tokio::time::advance(Duration::from_millis(401)).await;
    tokio::task::yield_now().await;
    assert_eq!(pool.len(), 2);

    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;
    assert_eq!(pool.len(), 1);

    shutdown_tx.send(()).unwrap();
    task.await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        pool.len(),
        1,
        "shutdown must not run a final reconcile step"
    );
}

#[tokio::test(start_paused = true)]
async fn graceful_deadline_wakes_before_the_periodic_interval() {
    let (pool, _manager, service) = single_service(10_000, 16);
    let spec = segment(CLIENT, 1);
    let id = spec.identity().id();
    service
        .client_manager()
        .mount_segment(CLIENT, spec, service.clock().client_now())
        .unwrap();
    let reconciler = service.reconciler(
        MasterReconcileConfig::new(Duration::from_secs(1), DEFAULT_OBJECT_COLLECTION_BUDGET)
            .unwrap(),
    );
    let service = Arc::new(service);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(reconciler.run_until(async {
        let _ = shutdown_rx.await;
    }));
    tokio::task::yield_now().await;

    assert_eq!(
        service
            .graceful_unmount_segment(
                Uuid {
                    high: id.high(),
                    low: id.low(),
                },
                Uuid {
                    high: CLIENT.high(),
                    low: CLIENT.low(),
                },
                37,
            )
            .await
            .unwrap(),
        Ok(())
    );
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(36)).await;
    tokio::task::yield_now().await;
    assert!(pool.segment(id).is_some());

    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert!(pool.segment(id).is_none());

    let after_shutdown = segment(CLIENT, 2);
    let after_shutdown_id = after_shutdown.identity().id();
    service
        .client_manager()
        .mount_segment(CLIENT, after_shutdown, service.clock().client_now())
        .unwrap();
    service
        .graceful_unmount_segment(
            Uuid {
                high: after_shutdown_id.high(),
                low: after_shutdown_id.low(),
            },
            Uuid {
                high: CLIENT.high(),
                low: CLIENT.low(),
            },
            50,
        )
        .await
        .unwrap()
        .unwrap();
    shutdown_tx.send(()).unwrap();
    task.await.unwrap();
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(pool.segment(after_shutdown_id).is_some());
}

#[tokio::test(start_paused = true)]
async fn client_expiry_cancels_same_step_graceful_work_before_object_collection() {
    let (pool, manager, service) = single_service(10, 16);
    let spec = segment(CLIENT, 1);
    let id = spec.identity().id();
    service
        .client_manager()
        .mount_segment(CLIENT, spec, service.clock().client_now())
        .unwrap();
    let object = identity("expiry-wins");
    manager
        .start_put(
            object.clone(),
            service.client_manager().write_admission(CLIENT).unwrap(),
            plan(4096),
            service.clock().now(),
        )
        .unwrap();
    manager
        .finish_put(
            &object,
            service.client_manager().write_owner(CLIENT).unwrap(),
            ReplicaSelector::All,
        )
        .unwrap();
    service
        .client_manager()
        .schedule_graceful_unmount(CLIENT, id, ClientTick::new(10))
        .unwrap();

    tokio::time::advance(Duration::from_millis(10)).await;
    let report = service
        .reconciler(MasterReconcileConfig::default())
        .reconcile_once();
    assert_eq!(report.client_cleanup.unwrap().completed_sessions, 1);
    assert_eq!(report.graceful_unmount.completed, 0);
    assert_eq!(report.graceful_unmount.stale_or_cancelled, 1);
    assert_eq!(report.graceful_unmount.retried, 0);
    assert_eq!(report.object_collection.catalog.invalidated_published, 1);
    assert!(pool.is_empty());
}

#[tokio::test(start_paused = true)]
async fn tenant_service_builds_a_reconciler_for_its_backend() {
    let pool = Arc::new(SegmentPool::new());
    let manager = Arc::new(TenantObjectManager::new(pool.clone(), TenantConfig::Single).unwrap());
    let service =
        ObjectCatalogRpcService::with_tenants_and_clock(manager.clone(), MasterClock::new());
    let clients = service.client_manager().clone();
    clients
        .remount(
            CLIENT,
            vec![segment(CLIENT, 1)],
            service.clock().client_now(),
        )
        .unwrap();
    let tenant = manager
        .resolve_tenant(&TenantId::try_from("tenant-a").unwrap())
        .unwrap();
    manager
        .start_put(
            &tenant,
            "tenant-key",
            clients.write_admission(CLIENT).unwrap(),
            plan(4096),
            service.clock().now(),
        )
        .unwrap();
    manager
        .finish_put(
            &tenant,
            "tenant-key",
            clients.write_owner(CLIENT).unwrap(),
            ReplicaSelector::All,
        )
        .unwrap();
    tokio::time::advance(Duration::from_millis(10_000)).await;

    let report = service
        .reconciler(MasterReconcileConfig::default())
        .reconcile_once();
    assert_eq!(report.client_cleanup.unwrap().completed_sessions, 1);
    assert_eq!(report.object_collection.catalog.invalidated_published, 1);
    assert!(!report.object_collection.catalog.busy);
    assert_eq!(manager.catalog().stats().published_objects, 0);
    assert!(pool.is_empty());
}
