use cakemaster::client::{ClientId, ClientLifecycleConfig, ClientTick};
use cakemaster::object::ObjectManager;
use cakemaster::segment::stats::SegmentState;
use cakemaster::segment::{SegmentId, SegmentPool};
use cakemaster_proto::mooncake::{
    ClientStatus, ErrorCode, Segment, Uuid, WrappedMasterServiceClient, WrappedMasterServiceServer,
};
use cakemaster_server::{MasterClock, MasterReconcileConfig, ObjectCatalogRpcService};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

const CLIENT: ClientId = ClientId::new(11, 12);
const SEGMENT: SegmentId = SegmentId::new(13, 14);

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
