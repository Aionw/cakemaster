use cakemaster::api::{
    DemoService, DemoServiceClient, DemoServiceServer, DescriptorVariant, DiskDescriptor,
    ErrorCode, LocalDiskDescriptor, MemoryDescriptor, NoFDescriptor, ReplicaDescriptor, User,
    UserId,
};
use coro_rpc::{RemoteError, RequestContext, RpcError, RpcFailure, RpcResponse};
use tokio::sync::oneshot;

struct TestService;

impl DemoService for TestService {
    async fn echo(&self, value: String) -> Result<String, RpcFailure> {
        Ok(value)
    }

    async fn add(&self, left: i32, right: i32) -> Result<i32, RpcFailure> {
        Ok(left + right)
    }

    async fn echo_error(&self, error: ErrorCode) -> Result<ErrorCode, RpcFailure> {
        Ok(error)
    }

    async fn echo_descriptor(
        &self,
        descriptor: ReplicaDescriptor,
    ) -> Result<ReplicaDescriptor, RpcFailure> {
        Ok(descriptor)
    }

    async fn ping(&self) -> Result<String, RpcFailure> {
        Ok("pong".to_owned())
    }

    async fn fail(&self) -> Result<(), RpcFailure> {
        Err(RpcFailure::new(1001, "generated service error"))
    }

    async fn attachment_echo(
        &self,
        context: RequestContext,
    ) -> Result<RpcResponse<()>, RpcFailure> {
        Ok(RpcResponse::with_attachment((), context.attachment))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generated_client_and_server_share_one_contract() {
    let server = DemoServiceServer::new(TestService)
        .into_rpc_server()
        .unwrap();
    let bound = server.bind("127.0.0.1:0").await.unwrap();
    let address = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(bound.run_until(async {
        let _ = shutdown_rx.await;
    }));

    let client = DemoServiceClient::connect(address).await.unwrap();
    assert_eq!(client.echo("hello".to_owned()).await.unwrap(), "hello");
    assert_eq!(client.add(20, 22).await.unwrap(), 42);
    assert_eq!(
        client.echo_error(ErrorCode::ObjectNotFound).await.unwrap(),
        ErrorCode::ObjectNotFound
    );
    let descriptor = ReplicaDescriptor {
        id: 7,
        descriptor_variant: DescriptorVariant::Disk(DiskDescriptor {
            path: "/tmp/cake".to_owned(),
            object_size: 4096,
        }),
        status: 3,
    };
    assert_eq!(
        client.echo_descriptor(descriptor.clone()).await.unwrap(),
        descriptor
    );
    assert_eq!(client.ping().await.unwrap(), "pong");
    assert_eq!(
        client.fail().await.unwrap_err(),
        RpcError::Remote(RemoteError {
            code: 1001,
            message: "generated service error".to_owned(),
        })
    );
    let reply = client
        .attachment_echo(b"generated attachment".to_vec())
        .await
        .unwrap();
    assert_eq!(reply.attachment.as_ref(), b"generated attachment");

    drop(client);
    let _ = shutdown_tx.send(());
    server_task.await.unwrap().unwrap();
}

#[test]
fn generated_thrift_models_use_struct_pack_wire_types() {
    let user = User {
        id: UserId::from(42),
        name: "Alice".to_owned(),
        tags: Some(vec!["admin".to_owned()]),
        counters: Some([("calls".to_owned(), 7)].into_iter().collect()),
        levels: Some([3_i16, 5_i16].into_iter().collect()),
    };
    let encoded = coro_rpc::struct_pack::serialize(&user).unwrap();
    let decoded = coro_rpc::struct_pack::deserialize::<User>(&encoded).unwrap();
    assert_eq!(decoded, user);

    let enum_encoded = coro_rpc::struct_pack::serialize(&ErrorCode::ObjectNotFound).unwrap();
    assert_eq!(
        enum_encoded,
        coro_rpc::struct_pack::serialize(&-704_i32).unwrap()
    );
    assert_eq!(
        coro_rpc::struct_pack::deserialize::<ErrorCode>(&enum_encoded).unwrap(),
        ErrorCode::ObjectNotFound
    );

    let batch_expected = vec![Ok(true), Err(ErrorCode::ObjectNotFound)];
    let batch_encoded = coro_rpc::struct_pack::serialize(&batch_expected).unwrap();
    assert_eq!(
        coro_rpc::struct_pack::deserialize::<Vec<Result<bool, ErrorCode>>>(&batch_encoded).unwrap(),
        batch_expected
    );

    let variants = [
        DescriptorVariant::Memory(MemoryDescriptor { address: 11 }),
        DescriptorVariant::NofSsd(NoFDescriptor { address: 22 }),
        DescriptorVariant::Disk(DiskDescriptor {
            path: "/disk/cake".to_owned(),
            object_size: 33,
        }),
        DescriptorVariant::LocalDisk(LocalDiskDescriptor {
            client_id_high: 44,
            client_id_low: 55,
            transport_endpoint: "tcp://host:1234".to_owned(),
        }),
    ];
    for (expected_index, variant) in variants.into_iter().enumerate() {
        let encoded = coro_rpc::struct_pack::serialize(&variant).unwrap();
        assert_eq!(encoded[4], expected_index as u8);
        assert_eq!(
            coro_rpc::struct_pack::deserialize::<DescriptorVariant>(&encoded).unwrap(),
            variant
        );
    }
}

#[test]
fn generated_enum_rejects_unknown_discriminants() {
    let encoded = coro_rpc::struct_pack::serialize(&12345_i32).unwrap();
    assert_eq!(
        coro_rpc::struct_pack::deserialize::<ErrorCode>(&encoded).unwrap_err(),
        coro_rpc::StructPackError::InvalidEnumDiscriminant {
            name: "ErrorCode",
            value: 12345,
        }
    );
}

#[test]
fn generated_union_rejects_unknown_variant_indices() {
    let mut encoded =
        coro_rpc::struct_pack::serialize(&DescriptorVariant::Memory(MemoryDescriptor {
            address: 11,
        }))
        .unwrap();
    assert_eq!(encoded[4], 0);
    encoded[4] = 4;
    assert_eq!(
        coro_rpc::struct_pack::deserialize::<DescriptorVariant>(&encoded).unwrap_err(),
        coro_rpc::StructPackError::InvalidVariantIndex {
            name: "DescriptorVariant",
            index: 4,
            alternatives: 4,
        }
    );
}
