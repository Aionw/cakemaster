use cakemaster::client::{ClientId, ClientLifecycleConfig, ClientTick};
use cakemaster::mooncake::{
    ClientStatus, ErrorCode, ObjectDataType, ObjectMeta, ReplicaType, ReplicateConfig, Segment,
    SoftPinAction, Uuid, WrappedMasterServiceClient, WrappedMasterServiceServer,
};
use cakemaster::object::ObjectManager;
use cakemaster::segment::stats::SegmentState;
use cakemaster::segment::{SegmentId, SegmentPool};
use cakemaster::server::{MasterClock, MasterReconcileConfig, ObjectCatalogRpcService};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

const CLIENT: ClientId = ClientId::new(11, 12);
const SEGMENT: SegmentId = SegmentId::new(13, 14);
const SECOND_SEGMENT: SegmentId = SegmentId::new(13, 15);

fn wire_segment() -> Segment {
    Segment {
        id: Uuid {
            high: SEGMENT.high(),
            low: SEGMENT.low(),
        },
        name: "rpc-memory".to_owned(),
        base: 0x2_0000_0000,
        size: 1 << 20,
        te_endpoint: "127.0.0.1:12345".to_owned(),
        protocol: "tcp".to_owned(),
        host_id: "host-a".to_owned(),
    }
}

fn second_wire_segment() -> Segment {
    Segment {
        id: Uuid {
            high: SECOND_SEGMENT.high(),
            low: SECOND_SEGMENT.low(),
        },
        name: "rpc-memory-b".to_owned(),
        base: 0x3_0000_0000,
        size: 1 << 20,
        te_endpoint: "127.0.0.1:12346".to_owned(),
        protocol: "tcp".to_owned(),
        host_id: "host-b".to_owned(),
    }
}

fn replicated_memory_config() -> ReplicateConfig {
    ReplicateConfig {
        replica_num: 2,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_remount_and_expiry_follow_client_session_state() {
    let pool = Arc::new(SegmentPool::new());
    let manager = Arc::new(ObjectManager::new(pool.clone()));
    let service = ObjectCatalogRpcService::new_with_client_config(
        manager,
        ClientLifecycleConfig::new(16)
            .with_ttl(10_000)
            .with_cleanup_scan_budget(16),
        MasterClock::new(),
        29,
    )
    .unwrap();
    let clients = service.client_manager().clone();
    let server = WrappedMasterServiceServer::new(service)
        .into_rpc_server()
        .unwrap();
    let bound = server.bind("127.0.0.1:0").await.unwrap();
    let address = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(bound.run_until(async {
        let _ = shutdown_rx.await;
    }));
    let client = WrappedMasterServiceClient::connect(address).await.unwrap();
    let client_id = Uuid {
        high: CLIENT.high(),
        low: CLIENT.low(),
    };

    let ping = client.ping(client_id.clone()).await.unwrap().unwrap();
    assert_eq!(ping.view_version_id, 29);
    assert_eq!(ping.client_status, ClientStatus::NeedRemount);

    assert_eq!(
        client
            .re_mount_segment(vec![wire_segment()], client_id.clone())
            .await
            .unwrap(),
        Ok(())
    );
    let mounted = pool.segment(SEGMENT).unwrap();
    assert_eq!(mounted.spec().identity().host_id(), "host-a");
    let ping = client.ping(client_id.clone()).await.unwrap().unwrap();
    assert_eq!(ping.client_status, ClientStatus::Ok);

    clients.heartbeat(CLIENT, ClientTick::new(u64::MAX));
    let ping = client.ping(client_id.clone()).await.unwrap().unwrap();
    assert_eq!(ping.client_status, ClientStatus::NeedRemount);
    assert_eq!(
        client
            .re_mount_segment(vec![wire_segment()], client_id)
            .await
            .unwrap(),
        Err(ErrorCode::UnavailableInCurrentStatus)
    );

    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_mount_unmount_and_graceful_unmount_follow_segment_lifecycle() {
    let pool = Arc::new(SegmentPool::new());
    let manager = Arc::new(ObjectManager::new(pool.clone()));
    let service = ObjectCatalogRpcService::new(manager);
    let reconciler = service.reconciler(MasterReconcileConfig::default());
    let server = WrappedMasterServiceServer::new(service)
        .into_rpc_server()
        .unwrap();
    let bound = server.bind("127.0.0.1:0").await.unwrap();
    let address = bound.local_addr().unwrap();
    let (server_shutdown_tx, server_shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(bound.run_until(async {
        let _ = server_shutdown_rx.await;
    }));
    let (reconcile_shutdown_tx, reconcile_shutdown_rx) = oneshot::channel();
    let reconcile_task = tokio::spawn(reconciler.run_until(async {
        let _ = reconcile_shutdown_rx.await;
    }));
    let client = WrappedMasterServiceClient::connect(address).await.unwrap();
    let client_id = Uuid {
        high: CLIENT.high(),
        low: CLIENT.low(),
    };
    let segment_id = Uuid {
        high: SEGMENT.high(),
        low: SEGMENT.low(),
    };

    assert_eq!(
        client
            .mount_segment(wire_segment(), client_id.clone())
            .await
            .unwrap(),
        Ok(())
    );
    assert_eq!(
        client
            .mount_segment(wire_segment(), client_id.clone())
            .await
            .unwrap(),
        Ok(())
    );
    let mut conflicting = wire_segment();
    conflicting.size += 1;
    assert_eq!(
        client
            .mount_segment(conflicting, client_id.clone())
            .await
            .unwrap(),
        Err(ErrorCode::SegmentAlreadyExists)
    );
    assert_eq!(
        client
            .graceful_unmount_segment(Uuid { high: 99, low: 99 }, client_id.clone(), 10,)
            .await
            .unwrap(),
        Err(ErrorCode::SegmentNotFound)
    );
    let wrong_client = Uuid { high: 44, low: 55 };
    assert_eq!(
        client
            .unmount_segment(segment_id.clone(), wrong_client.clone())
            .await
            .unwrap(),
        Err(ErrorCode::InvalidParams)
    );
    assert_eq!(
        client
            .graceful_unmount_segment(segment_id.clone(), wrong_client, 10)
            .await
            .unwrap(),
        Err(ErrorCode::InvalidParams)
    );

    assert_eq!(
        client
            .graceful_unmount_segment(segment_id.clone(), client_id.clone(), 100)
            .await
            .unwrap(),
        Ok(())
    );
    assert_eq!(
        pool.segment(SEGMENT).unwrap().stats().state,
        SegmentState::Quiesced
    );
    assert_eq!(
        client
            .mount_segment(wire_segment(), client_id.clone())
            .await
            .unwrap(),
        Err(ErrorCode::UnavailableInCurrentStatus)
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.segment(SEGMENT).is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    assert_eq!(
        client
            .mount_segment(wire_segment(), client_id.clone())
            .await
            .unwrap(),
        Ok(())
    );
    assert_eq!(
        client
            .unmount_segment(segment_id.clone(), client_id.clone())
            .await
            .unwrap(),
        Ok(())
    );
    assert_eq!(
        client.unmount_segment(segment_id, client_id).await.unwrap(),
        Ok(())
    );
    assert!(pool.is_empty());

    server_shutdown_tx.send(()).unwrap();
    reconcile_shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();
    reconcile_task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_unmount_prunes_only_the_target_replica() {
    let pool = Arc::new(SegmentPool::new());
    let manager = Arc::new(ObjectManager::new(pool.clone()));
    let service = ObjectCatalogRpcService::new(manager);
    let reconciler = service.reconciler(MasterReconcileConfig::default());
    let server = WrappedMasterServiceServer::new(service)
        .into_rpc_server()
        .unwrap();
    let bound = server.bind("127.0.0.1:0").await.unwrap();
    let address = bound.local_addr().unwrap();
    let (server_shutdown_tx, server_shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(bound.run_until(async {
        let _ = server_shutdown_rx.await;
    }));
    let (reconcile_shutdown_tx, reconcile_shutdown_rx) = oneshot::channel();
    let reconcile_task = tokio::spawn(reconciler.run_until(async {
        let _ = reconcile_shutdown_rx.await;
    }));
    let client = WrappedMasterServiceClient::connect(address).await.unwrap();
    let client_id = Uuid {
        high: CLIENT.high(),
        low: CLIENT.low(),
    };
    let first_id = Uuid {
        high: SEGMENT.high(),
        low: SEGMENT.low(),
    };
    let second_id = Uuid {
        high: SECOND_SEGMENT.high(),
        low: SECOND_SEGMENT.low(),
    };

    assert_eq!(
        client
            .mount_segment(wire_segment(), client_id.clone())
            .await
            .unwrap(),
        Ok(())
    );
    assert_eq!(
        client
            .mount_segment(second_wire_segment(), client_id.clone())
            .await
            .unwrap(),
        Ok(())
    );
    let first_segment = pool.segment(SEGMENT).unwrap();
    let started = client
        .batch_put_start(
            client_id.clone(),
            vec!["replicated".to_owned()],
            vec![4096],
            replicated_memory_config(),
            String::new(),
        )
        .await
        .unwrap();
    assert_eq!(started[0].as_ref().unwrap().len(), 2);
    assert_eq!(
        client
            .batch_put_end(
                client_id.clone(),
                vec![ObjectMeta {
                    key: "replicated".to_owned(),
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
            .get_replica_list("replicated".to_owned(), String::new())
            .await
            .unwrap()
            .unwrap()
            .replicas
            .len(),
        2
    );

    assert_eq!(
        client
            .unmount_segment(first_id, client_id.clone())
            .await
            .unwrap(),
        Ok(())
    );
    assert_eq!(
        client
            .exist_key("replicated".to_owned(), String::new())
            .await
            .unwrap(),
        Ok(true)
    );
    let surviving = client
        .get_replica_list("replicated".to_owned(), String::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(surviving.replicas.len(), 1);
    assert_eq!(first_segment.stats().usage.active_allocations, 0);

    assert_eq!(
        client
            .graceful_unmount_segment(second_id, client_id, 100)
            .await
            .unwrap(),
        Ok(())
    );
    assert_eq!(
        client
            .get_replica_list("replicated".to_owned(), String::new())
            .await
            .unwrap()
            .unwrap()
            .replicas
            .len(),
        1
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.segment(SECOND_SEGMENT).is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        client
            .get_replica_list("replicated".to_owned(), String::new())
            .await
            .unwrap(),
        Err(ErrorCode::ObjectNotFound)
    );

    server_shutdown_tx.send(()).unwrap();
    reconcile_shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();
    reconcile_task.await.unwrap();
}
