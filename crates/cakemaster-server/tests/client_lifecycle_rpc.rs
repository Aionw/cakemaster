use cakemaster::client::{ClientId, ClientLifecycleConfig, ClientTick};
use cakemaster::object::ObjectManager;
use cakemaster::segment::{SegmentId, SegmentPool};
use cakemaster_proto::mooncake::{
    ClientStatus, ErrorCode, Segment, Uuid, WrappedMasterServiceClient, WrappedMasterServiceServer,
};
use cakemaster_server::{MasterClock, ObjectCatalogRpcService};
use std::sync::Arc;
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
