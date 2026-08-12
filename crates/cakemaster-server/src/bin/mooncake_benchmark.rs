use std::error::Error;
use std::time::Instant;

use cakemaster_proto::mooncake::{
    BufferDescriptor, ClientStatus, DescriptorVariant, ExpectedBool,
    ExpectedGetReplicaListResponse, ExpectedPingResponse, ExpectedReplicaDescriptors, ExpectedVoid,
    GetReplicaListResponse, MemoryDescriptor, ObjectDataType, ObjectMeta, PingResponse,
    ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, Segment, SoftPinAction, Uuid,
    WrappedMasterService, WrappedMasterServiceServer,
};
use coro_rpc::struct_pack::{serialize, type_hash, type_literal};
use coro_rpc::{ClientConfig, RpcClient, RpcError, RpcFailure, RpcMethod, StructPack, function_id};
use futures_util::future::join_all;
use tokio::runtime::Builder;

const EXIST_KEY: &str = "mooncake::WrappedMasterService::ExistKey";
const GET_REPLICA_LIST: &str = "mooncake::WrappedMasterService::GetReplicaList";
const BATCH_EXIST_KEY: &str = "mooncake::WrappedMasterService::BatchExistKey";
const BATCH_GET_REPLICA_LIST: &str = "mooncake::WrappedMasterService::BatchGetReplicaList";
const BATCH_PUT_START: &str = "mooncake::WrappedMasterService::BatchPutStart";
const BATCH_PUT_END: &str = "mooncake::WrappedMasterService::BatchPutEnd";
const BATCH_PUT_REVOKE: &str = "mooncake::WrappedMasterService::BatchPutRevoke";

type SingleKeyRequest = (String, String);
type SingleExistResponse = ExpectedBool;
type SingleGetResponse = ExpectedGetReplicaListResponse;
type BatchKeyRequest = (Vec<String>, String);
type BatchExistResponse = Vec<ExpectedBool>;
type BatchGetResponse = Vec<ExpectedGetReplicaListResponse>;
type BatchPutStartRequest = (Uuid, Vec<String>, Vec<u64>, ReplicateConfig, String);
type BatchPutStartResponse = Vec<ExpectedReplicaDescriptors>;
type BatchPutEndRequest = (Uuid, Vec<ObjectMeta>, ReplicaType, String);
type BatchPutRevokeRequest = (Uuid, Vec<String>, ReplicaType, String);
type BatchVoidResponse = Vec<ExpectedVoid>;

#[derive(Clone, Copy)]
enum Operation {
    SingleExists,
    SingleGet,
    Exists,
    Get,
    PutStart,
    PutEnd,
    PutRevoke,
}

impl Operation {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "single-exists" => Ok(Self::SingleExists),
            "single-get" => Ok(Self::SingleGet),
            "exists" | "batch-exists" => Ok(Self::Exists),
            "get" | "batch-get" => Ok(Self::Get),
            "put-start" => Ok(Self::PutStart),
            "put-end" => Ok(Self::PutEnd),
            "put-revoke" => Ok(Self::PutRevoke),
            _ => Err(format!(
                "unknown operation {value:?}; expected single-exists|single-get|exists|get|put-start|put-end|put-revoke"
            )),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::SingleExists => "single-exists",
            Self::SingleGet => "single-get",
            Self::Exists => "exists",
            Self::Get => "get",
            Self::PutStart => "put-start",
            Self::PutEnd => "put-end",
            Self::PutRevoke => "put-revoke",
        }
    }

    const fn item_count(self, batch_size: usize) -> usize {
        match self {
            Self::SingleExists | Self::SingleGet => 1,
            Self::Exists | Self::Get | Self::PutStart | Self::PutEnd | Self::PutRevoke => {
                batch_size
            }
        }
    }
}

struct BenchmarkMasterService;

impl WrappedMasterService for BenchmarkMasterService {
    async fn ping(&self, _client_id: Uuid) -> Result<ExpectedPingResponse, RpcFailure> {
        Ok(Ok(PingResponse {
            view_version_id: 1,
            client_status: ClientStatus::Ok,
        }))
    }

    async fn re_mount_segment(
        &self,
        _segments: Vec<Segment>,
        _client_id: Uuid,
    ) -> Result<ExpectedVoid, RpcFailure> {
        Ok(Ok(()))
    }

    async fn exist_key(
        &self,
        _key: String,
        _tenant_id: String,
    ) -> Result<ExpectedBool, RpcFailure> {
        Ok(Ok(false))
    }

    async fn get_replica_list(
        &self,
        _key: String,
        _tenant_id: String,
    ) -> Result<ExpectedGetReplicaListResponse, RpcFailure> {
        Ok(Ok(GetReplicaListResponse {
            replicas: vec![memory_replica()],
            lease_ttl_ms: 1_000,
            object_checksum: None,
        }))
    }

    async fn batch_exist_key(
        &self,
        keys: Vec<String>,
        _tenant_id: String,
    ) -> Result<BatchExistResponse, RpcFailure> {
        Ok(keys.into_iter().map(|_| Ok(false)).collect())
    }

    async fn batch_get_replica_list(
        &self,
        keys: Vec<String>,
        _tenant_id: String,
    ) -> Result<BatchGetResponse, RpcFailure> {
        Ok(keys
            .into_iter()
            .map(|_| {
                Ok(GetReplicaListResponse {
                    replicas: vec![memory_replica()],
                    lease_ttl_ms: 1_000,
                    object_checksum: None,
                })
            })
            .collect())
    }

    async fn batch_put_start(
        &self,
        _client_id: Uuid,
        keys: Vec<String>,
        _slice_lengths: Vec<u64>,
        _config: ReplicateConfig,
        _tenant_id: String,
    ) -> Result<BatchPutStartResponse, RpcFailure> {
        Ok(keys
            .into_iter()
            .map(|_| Ok(vec![memory_replica()]))
            .collect())
    }

    async fn batch_put_end(
        &self,
        _client_id: Uuid,
        object_metas: Vec<ObjectMeta>,
        _replica_type: ReplicaType,
        _tenant_id: String,
    ) -> Result<BatchVoidResponse, RpcFailure> {
        Ok(object_metas.into_iter().map(|_| Ok(())).collect())
    }

    async fn batch_put_revoke(
        &self,
        _client_id: Uuid,
        keys: Vec<String>,
        _replica_type: ReplicaType,
        _tenant_id: String,
    ) -> Result<BatchVoidResponse, RpcFailure> {
        Ok(keys.into_iter().map(|_| Ok(())).collect())
    }
}

fn memory_replica() -> ReplicaDescriptor {
    ReplicaDescriptor {
        id: 1,
        descriptor_variant: DescriptorVariant::Memory(MemoryDescriptor {
            buffer_descriptor: BufferDescriptor {
                size: 4_096,
                buffer_address: 0x1_000,
                protocol: "tcp".to_owned(),
                transport_endpoint: "127.0.0.1:12345".to_owned(),
            },
        }),
        status: ReplicaStatus::Complete,
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        Some("server") => {
            let address = arguments
                .next()
                .unwrap_or_else(|| "127.0.0.1:19094".to_owned());
            let threads = parse_or(arguments.next(), 1_usize)?;
            runtime(threads)?.block_on(run_server(&address))
        }
        Some("client") => {
            let address = arguments
                .next()
                .unwrap_or_else(|| "127.0.0.1:19094".to_owned());
            let operation = Operation::parse(arguments.next().as_deref().unwrap_or("exists"))?;
            let batch_size = parse_or(arguments.next(), 16_usize)?.max(1);
            let iterations = parse_or(arguments.next(), 100_000_u64)?;
            let pipeline = parse_or(arguments.next(), 1_usize)?.max(1);
            let warmup = parse_or(arguments.next(), 10_000_u64)?;
            runtime(1)?.block_on(run_client(
                &address, operation, batch_size, iterations, pipeline, warmup,
            ))
        }
        Some("metadata") => {
            print_metadata();
            Ok(())
        }
        _ => {
            print_usage();
            Ok(())
        }
    }
}

fn print_metadata() {
    print_type::<SingleKeyRequest>("single_key_request");
    print_type::<SingleExistResponse>("single_exists_response");
    print_type::<SingleGetResponse>("single_get_response");
    print_type::<BatchKeyRequest>("batch_key_request");
    print_type::<BatchExistResponse>("batch_exists_response");
    print_type::<BatchGetResponse>("batch_get_response");
    print_type::<BatchPutStartRequest>("batch_put_start_request");
    print_type::<BatchPutStartResponse>("batch_put_start_response");
    print_type::<BatchPutEndRequest>("batch_put_end_request");
    print_type::<BatchPutRevokeRequest>("batch_put_revoke_request");
    print_type::<BatchVoidResponse>("batch_void_response");
    print_bytes(
        "batch_put_end_sample",
        &serialize(&(
            Uuid { high: 1, low: 2 },
            vec![ObjectMeta {
                key: "benchmark-key-00000000".to_owned(),
                object_checksum: None,
            }],
            ReplicaType::All,
            "default".to_owned(),
        ))
        .expect("benchmark metadata sample must serialize"),
    );
    print_bytes(
        "batch_put_start_sample",
        &serialize(&(
            Uuid { high: 1, low: 2 },
            vec!["benchmark-key-00000000".to_owned()],
            vec![4_096_u64],
            ReplicateConfig {
                soft_pin_action: SoftPinAction::Enable,
                soft_pin_ttl_ms: Some(1_234),
                ..benchmark_config()
            },
            "default".to_owned(),
        ))
        .expect("benchmark metadata sample must serialize"),
    );
    println!("single_exists_route={}", function_id(EXIST_KEY));
    println!("single_get_route={}", function_id(GET_REPLICA_LIST));
    println!("batch_exists_route={}", function_id(BATCH_EXIST_KEY));
    println!("batch_get_route={}", function_id(BATCH_GET_REPLICA_LIST));
    println!("batch_put_start_route={}", function_id(BATCH_PUT_START));
    println!("batch_put_end_route={}", function_id(BATCH_PUT_END));
    println!("batch_put_revoke_route={}", function_id(BATCH_PUT_REVOKE));
}

fn print_type<T: StructPack>(label: &str) {
    let literal = type_literal::<T>();
    print_bytes(label, &literal);
    println!("{label}_hash={}", type_hash::<T>());
}

fn print_bytes(label: &str, bytes: &[u8]) {
    let hex = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    println!("{label}={hex}");
}

fn parse_or<T>(value: Option<String>, default: T) -> Result<T, T::Err>
where
    T: std::str::FromStr,
{
    value.map_or(Ok(default), |value| value.parse())
}

fn runtime(threads: usize) -> std::io::Result<tokio::runtime::Runtime> {
    if threads == 1 {
        Builder::new_current_thread().enable_all().build()
    } else {
        Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
    }
}

async fn run_server(address: &str) -> Result<(), Box<dyn Error>> {
    let server = WrappedMasterServiceServer::new(BenchmarkMasterService).into_rpc_server()?;
    let bound = server.bind(address).await?;
    println!("rust_mooncake_server_ready={}", bound.local_addr()?);
    bound
        .run_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn run_client(
    address: &str,
    operation: Operation,
    batch_size: usize,
    iterations: u64,
    pipeline: usize,
    warmup: u64,
) -> Result<(), Box<dyn Error>> {
    let client = RpcClient::connect_with_config(
        address,
        ClientConfig {
            request_timeout: None,
            ..ClientConfig::default()
        },
    )
    .await?;

    run_operation(&client, operation, batch_size, warmup, pipeline).await?;
    let started = Instant::now();
    run_operation(&client, operation, batch_size, iterations, pipeline).await?;
    let elapsed = started.elapsed();
    let qps = iterations as f64 / elapsed.as_secs_f64();
    let item_qps = qps * operation.item_count(batch_size) as f64;
    let completion_us = elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;
    println!(
        "client=rust operation={} batch_size={batch_size} iterations={iterations} pipeline={pipeline} elapsed_s={:.6} qps={qps:.0} item_qps={item_qps:.0} us_per_completion={completion_us:.3}",
        operation.name(),
        elapsed.as_secs_f64()
    );
    Ok(())
}

async fn run_operation(
    client: &RpcClient,
    operation: Operation,
    batch_size: usize,
    iterations: u64,
    pipeline: usize,
) -> Result<(), RpcError> {
    let keys = benchmark_keys(batch_size);
    let tenant_id = "default".to_owned();
    match operation {
        Operation::SingleExists => {
            let request = (
                keys.into_iter().next().expect("batch size is non-zero"),
                tenant_id,
            );
            run_calls(
                client,
                RpcMethod::<SingleKeyRequest, SingleExistResponse>::new(EXIST_KEY),
                &request,
                iterations,
                pipeline,
                |response| response == &Ok(false),
            )
            .await
        }
        Operation::SingleGet => {
            let request = (
                keys.into_iter().next().expect("batch size is non-zero"),
                tenant_id,
            );
            run_calls(
                client,
                RpcMethod::<SingleKeyRequest, SingleGetResponse>::new(GET_REPLICA_LIST),
                &request,
                iterations,
                pipeline,
                |response| {
                    response
                        .as_ref()
                        .is_ok_and(|value| value.replicas.len() == 1)
                },
            )
            .await
        }
        Operation::Exists => {
            let request = (keys, tenant_id);
            run_calls(
                client,
                RpcMethod::<BatchKeyRequest, BatchExistResponse>::new(BATCH_EXIST_KEY),
                &request,
                iterations,
                pipeline,
                |response| {
                    response.len() == batch_size && response.iter().all(|item| item == &Ok(false))
                },
            )
            .await
        }
        Operation::Get => {
            let request = (keys, tenant_id);
            run_calls(
                client,
                RpcMethod::<BatchKeyRequest, BatchGetResponse>::new(BATCH_GET_REPLICA_LIST),
                &request,
                iterations,
                pipeline,
                |response| {
                    response.len() == batch_size
                        && response
                            .iter()
                            .all(|item| item.as_ref().is_ok_and(|value| value.replicas.len() == 1))
                },
            )
            .await
        }
        Operation::PutStart => {
            let request = (
                Uuid { high: 1, low: 2 },
                keys,
                vec![4_096; batch_size],
                benchmark_config(),
                tenant_id,
            );
            run_calls(
                client,
                RpcMethod::<BatchPutStartRequest, BatchPutStartResponse>::new(BATCH_PUT_START),
                &request,
                iterations,
                pipeline,
                |response| {
                    response.len() == batch_size
                        && response
                            .iter()
                            .all(|item| item.as_ref().is_ok_and(|value| value.len() == 1))
                },
            )
            .await
        }
        Operation::PutEnd => {
            let object_metas = keys
                .into_iter()
                .map(|key| ObjectMeta {
                    key,
                    object_checksum: None,
                })
                .collect();
            let request = (
                Uuid { high: 1, low: 2 },
                object_metas,
                ReplicaType::All,
                tenant_id,
            );
            run_calls(
                client,
                RpcMethod::<BatchPutEndRequest, BatchVoidResponse>::new(BATCH_PUT_END),
                &request,
                iterations,
                pipeline,
                |response| response.len() == batch_size && response.iter().all(Result::is_ok),
            )
            .await
        }
        Operation::PutRevoke => {
            let request = (Uuid { high: 1, low: 2 }, keys, ReplicaType::All, tenant_id);
            run_calls(
                client,
                RpcMethod::<BatchPutRevokeRequest, BatchVoidResponse>::new(BATCH_PUT_REVOKE),
                &request,
                iterations,
                pipeline,
                |response| response.len() == batch_size && response.iter().all(Result::is_ok),
            )
            .await
        }
    }
}

async fn run_calls<Request, Response>(
    client: &RpcClient,
    method: RpcMethod<Request, Response>,
    request: &Request,
    iterations: u64,
    pipeline: usize,
    validate: impl Fn(&Response) -> bool,
) -> Result<(), RpcError>
where
    Request: StructPack,
    Response: StructPack,
{
    if pipeline == 1 {
        for _ in 0..iterations {
            let response = client.call(method, request).await?;
            validate_response(method.name(), &response, &validate)?;
        }
        return Ok(());
    }

    let mut issued = 0_u64;
    while issued < iterations {
        let batch_size = usize::try_from((iterations - issued).min(pipeline as u64))
            .expect("pipeline batch size fits usize");
        let calls = (0..batch_size).map(|_| client.call(method, request));
        for response in join_all(calls).await {
            let response = response?;
            validate_response(method.name(), &response, &validate)?;
        }
        issued += u64::try_from(batch_size).expect("pipeline batch size fits u64");
    }
    Ok(())
}

fn validate_response<Response>(
    method: &str,
    response: &Response,
    validate: &impl Fn(&Response) -> bool,
) -> Result<(), RpcError> {
    if validate(response) {
        Ok(())
    } else {
        Err(RpcError::Protocol(format!(
            "unexpected benchmark response from {method}"
        )))
    }
}

fn benchmark_keys(batch_size: usize) -> Vec<String> {
    (0..batch_size)
        .map(|index| format!("benchmark-key-{index:08}"))
        .collect()
}

fn benchmark_config() -> ReplicateConfig {
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

fn print_usage() {
    eprintln!(
        "usage: mooncake_benchmark server [address] [threads] | \
         client [address] [single-exists|single-get|exists|get|put-start|put-end|put-revoke] \
         [batch-size] [iterations] [pipeline] [warmup]"
    );
}
