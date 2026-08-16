//! End-to-end tests for the Mooncake RPC adapter.

use cakemaster::MOONCAKE_STORE_VERSION;
use cakemaster::client::ClientTick;
use cakemaster::mooncake::{
    DescriptorVariant, ErrorCode, ObjectDataType, ObjectMeta, ReplicaStatus, ReplicaType,
    ReplicateConfig, SoftPinAction, Uuid, WrappedMasterService, WrappedMasterServiceClient,
    WrappedMasterServiceServer,
};
use cakemaster::object::{
    ObjectCatalogConfig, ObjectManager, TenantConfig, TenantId, TenantObjectManager, TenantPolicy,
    TenantQuotaLimits,
};
use cakemaster::segment::{
    ClientId, MemoryRegion, SegmentId, SegmentIdentity, SegmentPool, SegmentSpec,
    TransportEndpoint, TransportProtocol,
};
use cakemaster::server::ObjectCatalogRpcService;
use std::sync::Arc;
use tokio::sync::oneshot;

const OWNER: ClientId = ClientId::new(17, 23);
const MEMORY_ID: SegmentId = SegmentId::new(1, 1);
const NOF_ID: SegmentId = SegmentId::new(2, 1);

fn pool() -> Arc<SegmentPool> {
    let pool = Arc::new(SegmentPool::new());
    pool.attach(SegmentSpec::memory(
        SegmentIdentity::new(MEMORY_ID, OWNER, "memory-a"),
        MemoryRegion::new(0x2_0000_0000, 1 << 20),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
    ))
    .unwrap();
    pool.attach(SegmentSpec::nof(
        SegmentIdentity::new(NOF_ID, OWNER, "nof-a"),
        MemoryRegion::new(0, 1 << 20),
        "nvme://127.0.0.1/nqn.1",
    ))
    .unwrap();
    pool
}

fn config(replica_num: u64, nof_replica_num: u64) -> ReplicateConfig {
    ReplicateConfig {
        replica_num,
        nof_replica_num,
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
async fn bootstrap_rpcs_support_memory_only_client_initialization() {
    let manager = Arc::new(ObjectManager::new(Arc::new(SegmentPool::new())));
    let server = WrappedMasterServiceServer::new(ObjectCatalogRpcService::new(manager))
        .into_rpc_server()
        .unwrap();
    let bound = server.bind("127.0.0.1:0").await.unwrap();
    let address = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(bound.run_until(async {
        let _ = shutdown_rx.await;
    }));
    let client = WrappedMasterServiceClient::connect(address).await.unwrap();

    assert_eq!(
        client.service_ready().await.unwrap(),
        Ok(MOONCAKE_STORE_VERSION.to_owned())
    );
    let storage_config = client.get_storage_config().await.unwrap().unwrap();
    assert_eq!(storage_config.fsdir, "");
    assert!(!storage_config.enable_disk_eviction);
    assert_eq!(storage_config.quota_bytes, 0);

    drop(client);
    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generated_mooncake_rpc_drives_the_real_object_manager() {
    let pool = pool();
    let manager = Arc::new(
        ObjectManager::with_config(
            pool.clone(),
            ObjectCatalogConfig::new(64).with_lease(10_000, 5_000),
        )
        .unwrap(),
    );
    let service = ObjectCatalogRpcService::new(manager.clone());
    service
        .client_manager()
        .remount(OWNER, Vec::new(), ClientTick::ZERO)
        .unwrap();
    service
        .client_manager()
        .remount(ClientId::new(17, 24), Vec::new(), ClientTick::ZERO)
        .unwrap();
    assert!(Arc::ptr_eq(service.manager(), &manager));
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
    let writer = Uuid { high: 17, low: 23 };
    let other_writer = Uuid { high: 17, low: 24 };

    assert_eq!(
        client
            .exist_key("missing".to_owned(), "tenant-a".to_owned())
            .await
            .unwrap(),
        Ok(false)
    );
    assert_eq!(
        client
            .get_replica_list("missing".to_owned(), "tenant-a".to_owned())
            .await
            .unwrap(),
        Err(ErrorCode::ObjectNotFound)
    );

    let started = client
        .batch_put_start(
            writer.clone(),
            vec!["memory-key".to_owned()],
            vec![4096],
            config(1, 0),
            "tenant-a".to_owned(),
        )
        .await
        .unwrap();
    let replicas = started[0].as_ref().unwrap();
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].id, 1);
    assert_eq!(replicas[0].status, ReplicaStatus::Processing);
    let DescriptorVariant::Memory(descriptor) = &replicas[0].descriptor_variant else {
        panic!("expected a memory descriptor");
    };
    assert_eq!(descriptor.buffer_descriptor.size, 4096);
    assert_eq!(descriptor.buffer_descriptor.buffer_address, 0x2_0000_0000);
    assert_eq!(descriptor.buffer_descriptor.protocol, "tcp");
    assert_eq!(
        descriptor.buffer_descriptor.transport_endpoint,
        "127.0.0.1:12345"
    );

    assert_eq!(
        client
            .batch_exist_key(vec!["memory-key".to_owned()], "tenant-b".to_owned())
            .await
            .unwrap(),
        vec![Ok(false)]
    );
    assert_eq!(
        client
            .exist_key("memory-key".to_owned(), "tenant-b".to_owned())
            .await
            .unwrap(),
        Ok(false)
    );
    assert_eq!(
        client
            .batch_get_replica_list(vec!["memory-key".to_owned()], "ignored".to_owned())
            .await
            .unwrap(),
        vec![Err(ErrorCode::ReplicaIsNotReady)]
    );
    assert_eq!(
        client
            .get_replica_list("memory-key".to_owned(), "ignored".to_owned())
            .await
            .unwrap(),
        Err(ErrorCode::ReplicaIsNotReady)
    );
    assert_eq!(
        client
            .batch_put_end(
                writer.clone(),
                vec![ObjectMeta {
                    key: "memory-key".to_owned(),
                    object_checksum: Some(7),
                }],
                ReplicaType::Memory,
                "ignored".to_owned(),
            )
            .await
            .unwrap(),
        vec![Err(ErrorCode::InvalidParams)]
    );
    assert_eq!(
        client
            .batch_put_end(
                other_writer,
                vec![ObjectMeta {
                    key: "memory-key".to_owned(),
                    object_checksum: None,
                }],
                ReplicaType::Memory,
                "ignored".to_owned(),
            )
            .await
            .unwrap(),
        vec![Err(ErrorCode::IllegalClient)]
    );

    let metadata = ObjectMeta {
        key: "memory-key".to_owned(),
        object_checksum: None,
    };
    assert_eq!(
        client
            .batch_put_end(
                writer.clone(),
                vec![metadata.clone()],
                ReplicaType::Memory,
                "tenant-a".to_owned(),
            )
            .await
            .unwrap(),
        vec![Ok(())]
    );
    assert_eq!(
        client
            .batch_put_end(
                writer.clone(),
                vec![metadata],
                ReplicaType::All,
                "tenant-b".to_owned(),
            )
            .await
            .unwrap(),
        vec![Ok(())]
    );
    assert_eq!(
        client
            .batch_exist_key(vec!["memory-key".to_owned()], "tenant-b".to_owned())
            .await
            .unwrap(),
        vec![Ok(true)]
    );
    assert_eq!(
        client
            .exist_key("memory-key".to_owned(), "tenant-b".to_owned())
            .await
            .unwrap(),
        Ok(true)
    );
    let single_get = client
        .get_replica_list("memory-key".to_owned(), "tenant-b".to_owned())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(single_get.object_checksum, None);
    assert!(single_get.lease_ttl_ms > 0 && single_get.lease_ttl_ms <= 10_000);
    assert_eq!(single_get.replicas[0].status, ReplicaStatus::Complete);
    let get = client
        .batch_get_replica_list(vec!["memory-key".to_owned()], "tenant-b".to_owned())
        .await
        .unwrap();
    let get = get[0].as_ref().unwrap();
    assert_eq!(get.object_checksum, None);
    assert!(get.lease_ttl_ms > 0 && get.lease_ttl_ms <= 10_000);
    assert_eq!(get.replicas[0].status, ReplicaStatus::Complete);

    assert_eq!(
        client
            .batch_put_start(
                writer.clone(),
                vec!["memory-key".to_owned()],
                vec![4096],
                config(1, 0),
                "ignored".to_owned(),
            )
            .await
            .unwrap(),
        vec![Err(ErrorCode::ObjectAlreadyExists)]
    );
    assert_eq!(
        client
            .batch_put_revoke(
                writer.clone(),
                vec!["memory-key".to_owned()],
                ReplicaType::All,
                "ignored".to_owned(),
            )
            .await
            .unwrap(),
        vec![Err(ErrorCode::InvalidWrite)]
    );

    let nof = client
        .batch_put_start(
            writer.clone(),
            vec!["nof-key".to_owned()],
            vec![8192],
            config(0, 1),
            "ignored".to_owned(),
        )
        .await
        .unwrap();
    let DescriptorVariant::NofSsd(descriptor) = &nof[0].as_ref().unwrap()[0].descriptor_variant
    else {
        panic!("expected a NoF descriptor");
    };
    assert_eq!(descriptor.buffer_descriptor.protocol, "nvmeof");
    assert_eq!(descriptor.buffer_descriptor.buffer_address, 0);
    assert_eq!(
        client
            .batch_put_revoke(
                writer.clone(),
                vec!["nof-key".to_owned()],
                ReplicaType::NofSsd,
                "ignored".to_owned(),
            )
            .await
            .unwrap(),
        vec![Ok(())]
    );

    assert_eq!(
        client
            .batch_put_start(
                writer.clone(),
                vec!["mixed".to_owned()],
                vec![4096],
                config(1, 1),
                "ignored".to_owned(),
            )
            .await
            .unwrap(),
        vec![Err(ErrorCode::InvalidParams)]
    );
    let mut unsupported = config(1, 0);
    unsupported.soft_pin_action = SoftPinAction::Enable;
    assert_eq!(
        client
            .batch_put_start(
                writer.clone(),
                vec!["pinned".to_owned()],
                vec![4096],
                unsupported,
                "ignored".to_owned(),
            )
            .await
            .unwrap(),
        vec![Err(ErrorCode::InvalidParams)]
    );
    assert_eq!(
        client
            .batch_put_start(
                writer.clone(),
                vec!["a".to_owned(), "b".to_owned()],
                vec![4096],
                config(1, 0),
                "ignored".to_owned(),
            )
            .await
            .unwrap(),
        vec![Err(ErrorCode::InvalidParams), Err(ErrorCode::InvalidParams)]
    );
    assert_eq!(
        client
            .batch_put_revoke(
                writer,
                vec!["missing".to_owned()],
                ReplicaType::Disk,
                "ignored".to_owned(),
            )
            .await
            .unwrap(),
        vec![Err(ErrorCode::InvalidParams)]
    );

    drop(client);
    let _ = shutdown_tx.send(());
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn multi_tenant_rpc_resolves_once_per_batch_and_maps_tenant_errors() {
    let tenant_a = TenantId::try_from("tenant-a").unwrap();
    let tenant_b = TenantId::try_from("tenant-b").unwrap();
    let policy = TenantPolicy::new(TenantQuotaLimits::new(4096, 0));
    let manager = Arc::new(
        TenantObjectManager::new(
            pool(),
            TenantConfig::multi(vec![(tenant_a.clone(), policy), (tenant_b.clone(), policy)]),
        )
        .unwrap(),
    );
    let service = ObjectCatalogRpcService::with_tenants(manager.clone());
    service
        .client_manager()
        .remount(OWNER, Vec::new(), ClientTick::ZERO)
        .unwrap();
    assert!(Arc::ptr_eq(service.tenant_manager(), &manager));
    let writer = Uuid { high: 17, low: 23 };

    assert_eq!(
        service
            .exist_key("key".to_owned(), "missing".to_owned())
            .await
            .unwrap(),
        Err(ErrorCode::TenantNotRegistered)
    );
    assert_eq!(
        service
            .get_replica_list("key".to_owned(), "missing".to_owned())
            .await
            .unwrap(),
        Err(ErrorCode::TenantNotRegistered)
    );

    assert_eq!(
        service
            .batch_exist_key(vec!["key".to_owned()], "missing".to_owned())
            .await
            .unwrap(),
        vec![Err(ErrorCode::TenantNotRegistered)]
    );
    assert_eq!(
        service
            .batch_exist_key(vec!["key".to_owned()], String::new())
            .await
            .unwrap(),
        vec![Err(ErrorCode::InvalidParams)]
    );
    assert_eq!(
        service
            .batch_put_start(
                writer.clone(),
                vec!["a".to_owned(), "b".to_owned()],
                vec![4096],
                config(1, 0),
                "missing".to_owned(),
            )
            .await
            .unwrap(),
        vec![Err(ErrorCode::InvalidParams), Err(ErrorCode::InvalidParams)]
    );
    let mut unsupported = config(1, 0);
    unsupported.soft_pin_action = SoftPinAction::Enable;
    assert_eq!(
        service
            .batch_put_start(
                writer.clone(),
                vec!["unsupported".to_owned()],
                vec![4096],
                unsupported,
                "missing".to_owned(),
            )
            .await
            .unwrap(),
        vec![Err(ErrorCode::InvalidParams)]
    );
    assert_eq!(
        service
            .batch_put_end(
                writer.clone(),
                vec![
                    ObjectMeta {
                        key: "bad-checksum".to_owned(),
                        object_checksum: Some(7),
                    },
                    ObjectMeta {
                        key: "valid".to_owned(),
                        object_checksum: None,
                    },
                ],
                ReplicaType::Memory,
                "missing".to_owned(),
            )
            .await
            .unwrap(),
        vec![
            Err(ErrorCode::InvalidParams),
            Err(ErrorCode::TenantNotRegistered)
        ]
    );
    assert_eq!(
        service
            .batch_put_revoke(
                writer.clone(),
                vec!["key".to_owned()],
                ReplicaType::Disk,
                "missing".to_owned(),
            )
            .await
            .unwrap(),
        vec![Err(ErrorCode::InvalidParams)]
    );

    for tenant in [tenant_a.as_str(), tenant_b.as_str()] {
        let started = service
            .batch_put_start(
                writer.clone(),
                vec!["same-key".to_owned()],
                vec![4096],
                config(1, 0),
                tenant.to_owned(),
            )
            .await
            .unwrap();
        assert!(started[0].is_ok());
        assert_eq!(
            service
                .batch_put_end(
                    writer.clone(),
                    vec![ObjectMeta {
                        key: "same-key".to_owned(),
                        object_checksum: None,
                    }],
                    ReplicaType::Memory,
                    tenant.to_owned(),
                )
                .await
                .unwrap(),
            vec![Ok(())]
        );
    }
    assert_eq!(
        service
            .batch_exist_key(vec!["same-key".to_owned()], tenant_b.to_string())
            .await
            .unwrap(),
        vec![Ok(true)]
    );
    assert_eq!(
        service
            .exist_key("same-key".to_owned(), tenant_b.to_string())
            .await
            .unwrap(),
        Ok(true)
    );
    assert!(
        service
            .get_replica_list("same-key".to_owned(), tenant_b.to_string())
            .await
            .unwrap()
            .is_ok()
    );
    assert_eq!(
        service
            .batch_put_start(
                writer,
                vec!["over-quota".to_owned()],
                vec![1],
                config(1, 0),
                tenant_a.to_string(),
            )
            .await
            .unwrap(),
        vec![Err(ErrorCode::TenantQuotaExceeded)]
    );
}
