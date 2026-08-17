use cakemaster::client::ClientId;
use cakemaster::mooncake::{
    ClientStatus, ObjectDataType, ObjectMeta, ReplicaStatus, ReplicaType, ReplicateConfig, Segment,
    SoftPinAction, Uuid, WrappedMasterService, WrappedMasterServiceClient,
};
use cakemaster::object::ObjectCatalogConfig;
use cakemaster::segment::{
    MemoryRegion, SegmentId, SegmentIdentity, SegmentSpec, TransportEndpoint, TransportProtocol,
};
use cakemaster::server::{
    DEFAULT_OBJECT_COLLECTION_BUDGET, MasterReconcileConfig, MooncakeServerConfig,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

const CLIENT: ClientId = ClientId::new(71, 73);
const SEGMENT: SegmentId = SegmentId::new(79, 83);

fn client_id() -> Uuid {
    Uuid {
        high: CLIENT.high(),
        low: CLIENT.low(),
    }
}

fn wire_segment() -> Segment {
    Segment {
        id: Uuid {
            high: SEGMENT.high(),
            low: SEGMENT.low(),
        },
        name: "production-memory".to_owned(),
        base: 0x5_0000_0000,
        size: 1 << 20,
        te_endpoint: "127.0.0.1:12345".to_owned(),
        protocol: "tcp".to_owned(),
        host_id: "host-production".to_owned(),
    }
}

fn core_segment() -> SegmentSpec {
    SegmentSpec::memory(
        SegmentIdentity::new(SEGMENT, CLIENT, "production-memory"),
        MemoryRegion::new(0x5_0000_0000, 1 << 20),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
    )
}

fn replicate_config() -> ReplicateConfig {
    ReplicateConfig {
        replica_num: 1,
        nof_replica_num: 0,
        soft_pin_action: SoftPinAction::Preserve,
        soft_pin_ttl_ms: None,
        with_hard_pin: false,
        preferred_segments: Vec::new(),
        preferred_segment: String::new(),
        preferred_nof_segments: Vec::new(),
        prefer_alloc_in_same_node: false,
        data_type: ObjectDataType::Kvcache,
        host_id: String::new(),
        group_ids: None,
    }
}

#[test]
fn production_composition_shares_managers_clock_and_reconciler_state() {
    let config = MooncakeServerConfig::default().with_listen_addr("127.0.0.1:0".parse().unwrap());
    let composition = config.build().unwrap();

    assert_eq!(composition.config(), config);
    assert!(Arc::ptr_eq(
        composition.pool(),
        composition.manager().pool()
    ));
    assert!(Arc::ptr_eq(
        composition.manager(),
        composition.service().manager()
    ));
    assert!(
        composition
            .clock()
            .shares_origin_with(composition.service().clock())
    );
    assert!(
        composition
            .clock()
            .shares_origin_with(composition.reconciler().clock())
    );
    assert!(
        composition
            .service()
            .client_manager()
            .shares_state_with(composition.reconciler().client_manager())
    );
    assert_eq!(composition.reconciler().config(), config.reconcile());
    assert_eq!(
        composition.config().memory_eviction(),
        cakemaster::object::MemoryEvictionConfig::default()
    );
    assert!(composition.manager().memory_eviction_stats().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cakemaster_handles_ping_segment_and_object_rpcs_then_joins() {
    let config = MooncakeServerConfig::default()
        .with_listen_addr("127.0.0.1:0".parse().unwrap())
        .with_object_catalog(ObjectCatalogConfig::new(64).with_lease(10_000, 5_000));
    let bound = config.build().unwrap().bind().await.unwrap();
    let address = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(bound.run_until(async {
        let _ = shutdown_rx.await;
    }));

    let client = WrappedMasterServiceClient::connect(address).await.unwrap();
    let id = client_id();
    let ping = client.ping(id.clone()).await.unwrap().unwrap();
    assert_eq!(ping.client_status, ClientStatus::NeedRemount);
    assert_eq!(
        client
            .mount_segment(wire_segment(), id.clone())
            .await
            .unwrap(),
        Ok(())
    );
    assert_eq!(
        client
            .ping(id.clone())
            .await
            .unwrap()
            .unwrap()
            .client_status,
        ClientStatus::Ok
    );

    let started = client
        .batch_put_start(
            id.clone(),
            vec!["production-key".to_owned()],
            vec![4096],
            replicate_config(),
            "ignored-by-single-tenant".to_owned(),
        )
        .await
        .unwrap();
    let replicas = started[0].as_ref().unwrap();
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].status, ReplicaStatus::Processing);
    assert_eq!(
        client
            .batch_put_end(
                id,
                vec![ObjectMeta {
                    key: "production-key".to_owned(),
                    object_checksum: None,
                }],
                ReplicaType::Memory,
                String::new(),
            )
            .await
            .unwrap(),
        vec![Ok(())]
    );
    assert_eq!(
        client
            .exist_key("production-key".to_owned(), String::new())
            .await
            .unwrap(),
        Ok(true)
    );
    let get = client
        .get_replica_list("production-key".to_owned(), String::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(get.replicas.len(), 1);
    assert_eq!(get.replicas[0].status, ReplicaStatus::Complete);

    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("server and reconciler must join after shutdown")
        .unwrap()
        .unwrap();
    assert!(WrappedMasterServiceClient::connect(address).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn shutdown_does_not_run_a_late_graceful_deadline() {
    let reconcile =
        MasterReconcileConfig::new(Duration::from_secs(60), DEFAULT_OBJECT_COLLECTION_BUDGET)
            .unwrap();
    let config = MooncakeServerConfig::default()
        .with_listen_addr("127.0.0.1:0".parse().unwrap())
        .with_reconcile(reconcile);
    let composition = config.build().unwrap();
    let pool = composition.pool().clone();
    composition
        .service()
        .client_manager()
        .mount_segment(CLIENT, core_segment(), composition.clock().client_now())
        .unwrap();
    assert_eq!(
        composition
            .service()
            .graceful_unmount_segment(
                Uuid {
                    high: SEGMENT.high(),
                    low: SEGMENT.low(),
                },
                client_id(),
                50,
            )
            .await
            .unwrap(),
        Ok(())
    );

    let bound = composition.bind().await.unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(bound.run_until(async {
        let _ = shutdown_rx.await;
    }));
    tokio::task::yield_now().await;
    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();

    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;
    assert!(
        pool.segment(SEGMENT).is_some(),
        "shutdown must preempt and permanently stop graceful deadline work"
    );
}
