use cakemaster::mooncake::{ErrorCode, Segment, Uuid, WrappedMasterServiceClient};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn child(&mut self) -> &mut Child {
        self.0.as_mut().expect("child process is still owned")
    }

    fn stop(mut self) -> Output {
        let mut child = self.0.take().expect("child process is still owned");
        let _ = child.kill();
        child.wait_with_output().unwrap()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[tokio::test]
async fn production_logs_correlate_transport_and_business_failures() {
    let child = Command::new(env!("CARGO_BIN_EXE_cakemaster"))
        .args([
            "--listen",
            "127.0.0.1:0",
            "--log-output",
            "stderr",
            "--log-filter",
            "off,coro_rpc::server::connection=warn,coro_rpc::server::request=debug,cakemaster::server::rpc::business=debug,cakemaster::client::lifecycle=info",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(Some(child));
    let stdout = child.child().stdout.take().unwrap();
    let mut ready = String::new();
    BufReader::new(stdout).read_line(&mut ready).unwrap();
    let address = ready
        .trim()
        .strip_prefix("cakemaster_ready=")
        .expect("server prints its bound address")
        .parse::<std::net::SocketAddr>()
        .unwrap();

    let client = WrappedMasterServiceClient::connect(address).await.unwrap();
    let client_id = Uuid { high: 7, low: 11 };
    let invalid_segment = Segment {
        id: Uuid { high: 13, low: 17 },
        name: "invalid-segment".to_owned(),
        base: 4096,
        size: 4096,
        te_endpoint: "127.0.0.1:1".to_owned(),
        protocol: "INVALID".to_owned(),
        host_id: String::new(),
    };
    assert_eq!(
        client
            .mount_segment(invalid_segment, client_id.clone())
            .await
            .unwrap(),
        Err(ErrorCode::InvalidParams)
    );
    let valid_segment = Segment {
        id: Uuid { high: 19, low: 23 },
        name: "valid-segment".to_owned(),
        base: 8192,
        size: 4096,
        te_endpoint: "127.0.0.1:2".to_owned(),
        protocol: "tcp".to_owned(),
        host_id: String::new(),
    };
    assert_eq!(
        client
            .mount_segment(valid_segment, client_id)
            .await
            .unwrap(),
        Ok(())
    );
    drop(client);

    let mut malformed = std::net::TcpStream::connect(address).unwrap();
    malformed.write_all(b"BAD").unwrap();
    drop(malformed);

    tokio::time::sleep(Duration::from_millis(100)).await;
    let output = child.stop();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("Mooncake RPC business operation was rejected"));
    assert!(stderr.contains("operation=mount_segment"));
    assert!(stderr.contains("request_sequence=0"));
    assert!(stderr.contains("client_id=0000000000000007000000000000000b"));
    assert!(stderr.contains("error_name=InvalidParams"));
    assert!(stderr.contains("RPC request completed"));
    assert!(stderr.contains("sequence=0"));
    assert!(stderr.contains("method=mooncake::WrappedMasterService::MountSegment"));
    assert!(stderr.contains("RPC connection failed"));
    assert!(stderr.contains("invalid magic 66"));
    assert!(stderr.contains("client lifecycle state changed"));
    assert!(stderr.contains("generation=1"));
    assert!(stderr.contains("old_state=absent"));
    assert!(stderr.contains("new_state=active"));
    assert!(stderr.contains("reason=mount_or_remount"));
}
