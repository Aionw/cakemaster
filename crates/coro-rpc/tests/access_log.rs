use std::collections::BTreeMap;
use std::sync::Mutex;

use coro_rpc::{
    RemoteError, RpcClient, RpcError, RpcErrorCode, RpcFailure, RpcMethod, RpcServer, ServerConfig,
};
use log::kv::{self, Key, Value, VisitSource};
use log::{Level, LevelFilter, Log, Metadata, Record};
use tokio::sync::oneshot;

const ACCESS_LOG_TARGET: &str = "coro_rpc::access";

#[derive(Debug)]
struct CapturedRecord {
    message: String,
    fields: BTreeMap<String, String>,
}

#[derive(Default)]
struct CaptureLogger {
    records: Mutex<Vec<CapturedRecord>>,
}

impl CaptureLogger {
    fn clear(&self) {
        self.records.lock().unwrap().clear();
    }

    fn take(&self) -> Vec<CapturedRecord> {
        std::mem::take(&mut *self.records.lock().unwrap())
    }
}

impl Log for CaptureLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= Level::Info && metadata.target() == ACCESS_LOG_TARGET
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }

        struct FieldCollector<'a>(&'a mut BTreeMap<String, String>);

        impl<'kvs> VisitSource<'kvs> for FieldCollector<'_> {
            fn visit_pair(&mut self, key: Key<'kvs>, value: Value<'kvs>) -> Result<(), kv::Error> {
                self.0.insert(key.to_string(), value.to_string());
                Ok(())
            }
        }

        let mut fields = BTreeMap::new();
        record
            .key_values()
            .visit(&mut FieldCollector(&mut fields))
            .unwrap();
        self.records.lock().unwrap().push(CapturedRecord {
            message: record.args().to_string(),
            fields,
        });
    }

    fn flush(&self) {}
}

static LOGGER: CaptureLogger = CaptureLogger {
    records: Mutex::new(Vec::new()),
};

async fn start_server(
    config: ServerConfig,
) -> (
    RpcClient,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<std::io::Result<()>>,
    RpcMethod<String, String>,
    RpcMethod<String, String>,
) {
    let echo = RpcMethod::<String, String>::new("echo");
    let fail = RpcMethod::<String, String>::new("fail");
    let mut server = RpcServer::with_config(config);
    server
        .register(echo, |value| async move { Ok(value) })
        .unwrap()
        .register(fail, |_| async { Err(RpcFailure::new(7, "denied")) })
        .unwrap();

    let bound = server.bind("127.0.0.1:0").await.unwrap();
    let address = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(bound.run_until(async {
        let _ = shutdown_rx.await;
    }));
    let client = RpcClient::connect(address).await.unwrap();
    (client, shutdown_tx, server_task, echo, fail)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn access_log_is_disabled_by_default_and_records_completed_requests_when_enabled() {
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(LevelFilter::Info);
    assert!(!ServerConfig::default().access_log);

    let (client, shutdown_tx, server_task, echo, _) = start_server(ServerConfig::default()).await;
    assert_eq!(
        client.call(echo, &"quiet".to_owned()).await.unwrap(),
        "quiet"
    );
    drop(client);
    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();
    assert!(LOGGER.take().is_empty());

    LOGGER.clear();
    let (client, shutdown_tx, server_task, echo, fail) =
        start_server(ServerConfig::default().with_access_log(true)).await;
    let missing = RpcMethod::<String, String>::new("missing");

    assert_eq!(
        client.call(echo, &"hello".to_owned()).await.unwrap(),
        "hello"
    );
    assert_eq!(
        client.call(fail, &String::new()).await.unwrap_err(),
        RpcError::Remote(RemoteError {
            code: 7,
            message: "denied".to_owned(),
        })
    );
    assert_eq!(
        client.call(missing, &String::new()).await.unwrap_err(),
        RpcError::Remote(RemoteError {
            code: RpcErrorCode::FunctionNotRegistered as u16,
            message: "function not registered".to_owned(),
        })
    );

    drop(client);
    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();

    let records = LOGGER.take();
    assert_eq!(records.len(), 3);
    assert!(records.iter().all(|record| record.message == "RPC request"));

    let success = &records[0].fields;
    assert_eq!(success.get("rpc_method").map(String::as_str), Some("echo"));
    assert_eq!(success.get("status").map(String::as_str), Some("ok"));
    assert_eq!(success.get("error_code").map(String::as_str), Some("0"));
    assert_eq!(success["function_id"], echo.route_id().to_string());
    assert!(success["peer_addr"].starts_with("127.0.0.1:"));
    assert!(success["request_body_bytes"].parse::<usize>().unwrap() > 0);
    assert!(success["response_body_bytes"].parse::<usize>().unwrap() > 0);
    success["duration_us"].parse::<u64>().unwrap();

    let failure = &records[1].fields;
    assert_eq!(failure.get("rpc_method").map(String::as_str), Some("fail"));
    assert_eq!(failure.get("status").map(String::as_str), Some("error"));
    assert_eq!(failure.get("error_code").map(String::as_str), Some("7"));

    let unregistered = &records[2].fields;
    assert_eq!(
        unregistered.get("rpc_method").map(String::as_str),
        Some("<unregistered>")
    );
    assert_eq!(
        unregistered["error_code"],
        (RpcErrorCode::FunctionNotRegistered as u8).to_string()
    );
}
