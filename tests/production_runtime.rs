use cakemaster::client::ClientId;
use cakemaster::mooncake::{
    ClientStatus, DescriptorVariant, ErrorCode, ObjectDataType, ObjectMeta, ReplicaStatus,
    ReplicaType, ReplicateConfig, Segment, SoftPinAction, Uuid, WrappedMasterService,
    WrappedMasterServiceClient,
};
use cakemaster::object::{NamespaceId, ObjectCatalogConfig};
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

fn metadata_owner(tenant: &str, key: &str, shard_count: usize) -> usize {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in tenant
        .bytes()
        .chain([0xff])
        .chain(NamespaceId::DEFAULT.get().to_le_bytes())
        .chain([0xfe])
        .chain(key.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as usize) % shard_count
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sharded_backend_keeps_key_ownership_and_restores_batch_order() {
    let config = MooncakeServerConfig::default()
        .with_metadata_shards(4)
        .with_object_catalog(ObjectCatalogConfig::new(256));
    let composition = config.build().unwrap();
    assert_eq!(composition.manager().shard_count(), 4);
    assert_eq!(
        composition
            .manager()
            .shard_stats()
            .into_iter()
            .map(|shard| shard.segment_pool_instance)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        4,
        "metadata shards must own distinct segment pools"
    );
    composition
        .service()
        .client_manager()
        .remount(
            CLIENT,
            vec![core_segment()],
            composition.clock().client_now(),
        )
        .unwrap();

    let tenant = "tenant-a".to_owned();
    let keys = (0..64)
        .map(|index| format!("sharded-key-{index}"))
        .collect::<Vec<_>>();
    let started = composition
        .service()
        .batch_put_start(
            client_id(),
            keys.clone(),
            vec![1024; keys.len()],
            replicate_config(),
            tenant.clone(),
        )
        .await
        .unwrap();
    assert!(started.iter().all(Result::is_ok));

    let arena_size = (1_u64 << 20) / 4;
    let mut arenas = std::collections::HashSet::new();
    for replicas in &started {
        let descriptor = &replicas.as_ref().unwrap()[0];
        let buffer = match &descriptor.descriptor_variant {
            DescriptorVariant::Memory(memory) => &memory.buffer_descriptor,
            other => panic!("unexpected descriptor: {other:?}"),
        };
        arenas.insert((buffer.buffer_address - 0x5_0000_0000) / arena_size);
    }
    assert_eq!(
        arenas.len(),
        4,
        "stable owners must allocate from local arenas"
    );

    let metas = keys
        .iter()
        .map(|key| ObjectMeta {
            key: key.clone(),
            object_checksum: None,
        })
        .collect();
    assert!(
        composition
            .service()
            .batch_put_end(client_id(), metas, ReplicaType::Memory, tenant.clone(),)
            .await
            .unwrap()
            .iter()
            .all(Result::is_ok)
    );

    let probes = vec![
        keys[41].clone(),
        "missing-a".to_owned(),
        keys[3].clone(),
        "missing-b".to_owned(),
        keys[62].clone(),
    ];
    assert_eq!(
        composition
            .service()
            .batch_exist_key(probes, tenant.clone())
            .await
            .unwrap(),
        vec![Ok(true), Ok(false), Ok(true), Ok(false), Ok(true)]
    );
    assert_eq!(
        composition
            .service()
            .batch_remove(
                vec![keys[19].clone(), "missing-c".to_owned(), keys[7].clone()],
                true,
                tenant,
            )
            .await
            .unwrap(),
        vec![Ok(()), Err(ErrorCode::ObjectNotFound), Ok(())]
    );
    assert_eq!(composition.manager().catalog_stats().published_objects, 62);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sharded_backend_borrows_a_completely_empty_allocator_extent() {
    let config = MooncakeServerConfig::default()
        .with_metadata_shards(2)
        .with_object_catalog(ObjectCatalogConfig::new(16));
    let composition = config.build().unwrap();
    composition
        .service()
        .client_manager()
        .remount(
            CLIENT,
            vec![core_segment()],
            composition.clock().client_now(),
        )
        .unwrap();

    let tenant = "capacity-tenant".to_owned();
    let keys = (0..100)
        .map(|index| format!("capacity-key-{index}"))
        .filter(|key| metadata_owner(&tenant, key, 2) == 0)
        .take(2)
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), 2);

    let first = composition
        .service()
        .batch_put_start(
            client_id(),
            vec![keys[0].clone()],
            vec![1 << 19],
            replicate_config(),
            tenant.clone(),
        )
        .await
        .unwrap();
    let second = composition
        .service()
        .batch_put_start(
            client_id(),
            vec![keys[1].clone()],
            vec![1 << 18],
            replicate_config(),
            tenant.clone(),
        )
        .await
        .unwrap();

    let first_address = match &first[0].as_ref().unwrap()[0].descriptor_variant {
        DescriptorVariant::Memory(memory) => memory.buffer_descriptor.buffer_address,
        other => panic!("unexpected descriptor: {other:?}"),
    };
    let second_address = match &second[0].as_ref().unwrap()[0].descriptor_variant {
        DescriptorVariant::Memory(memory) => memory.buffer_descriptor.buffer_address,
        other => panic!("unexpected descriptor: {other:?}"),
    };
    assert_eq!(first_address, 0x5_0000_0000);
    assert_eq!(second_address, 0x5_0008_0000);

    assert_eq!(
        composition
            .service()
            .batch_put_revoke(client_id(), keys, ReplicaType::Memory, tenant)
            .await
            .unwrap(),
        vec![Ok(()), Ok(())]
    );
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
