use coro_rpc::{
    ClientConfig, RemoteError, RpcClient, RpcError, RpcErrorCode, RpcFailure, RpcMethod,
    RpcNoArgsMethod, RpcResponse, RpcServer,
};
use tokio::sync::{mpsc, oneshot};

struct DropNotice(mpsc::UnboundedSender<()>);

impl Drop for DropNotice {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tokio_client_and_server_support_pipeline_errors_and_attachments() {
    let echo = RpcMethod::<String, String>::new("echo");
    let add = RpcMethod::<(i32, i32), i32>::new("add");
    let ping = RpcNoArgsMethod::<String>::new("ping");
    let attachment_echo = RpcMethod::<String, String>::new("attachment_echo");
    let fail = RpcMethod::<String, String>::new("fail");
    let missing = RpcMethod::<String, String>::new("missing");
    let mut server = RpcServer::new();
    server
        .register(echo, |value| async move { Ok(value) })
        .unwrap()
        .register(add, |(left, right)| async move { Ok(left + right) })
        .unwrap()
        .register_no_args(ping, || async { Ok("pong".to_owned()) })
        .unwrap()
        .register_with_context(attachment_echo, |value, context| async move {
            Ok(RpcResponse::with_attachment(value, context.attachment))
        })
        .unwrap()
        .register(fail, |_| async {
            Err(RpcFailure::new(1001, "error from Rust server"))
        })
        .unwrap();

    let bound = server.bind("127.0.0.1:0").await.unwrap();
    let address = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(bound.run_until(async {
        let _ = shutdown_rx.await;
    }));

    let client = RpcClient::connect(address).await.unwrap();
    assert_eq!(
        client.call(echo, &"hello".to_owned()).await.unwrap(),
        "hello"
    );
    assert_eq!(client.call(add, &(20, 22)).await.unwrap(), 42);
    assert_eq!(client.call_no_args(ping).await.unwrap(), "pong");

    let reply = client
        .call_with_attachment(
            attachment_echo,
            &"body".to_owned(),
            b"raw attachment".to_vec(),
        )
        .await
        .unwrap();
    assert_eq!(reply.value, "body");
    assert_eq!(reply.attachment.as_ref(), b"raw attachment");

    assert_eq!(
        client.call(fail, &String::new()).await.unwrap_err(),
        RpcError::Remote(RemoteError {
            code: 1001,
            message: "error from Rust server".to_owned(),
        })
    );
    assert_eq!(
        client.call(missing, &String::new()).await.unwrap_err(),
        RpcError::Remote(RemoteError {
            code: RpcErrorCode::FunctionNotRegistered as u16,
            message: "function not registered".to_owned(),
        })
    );

    let mut calls = tokio::task::JoinSet::new();
    for value in 0..200_i32 {
        let client = client.clone();
        calls.spawn(async move {
            let result = client.call(add, &(value, value + 1)).await.unwrap();
            (value, result)
        });
    }
    while let Some(result) = calls.join_next().await {
        let (value, sum) = result.unwrap();
        assert_eq!(sum, value * 2 + 1);
    }

    drop(client);
    let _ = shutdown_tx.send(());
    server_task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_cancels_handlers_and_waits_for_connections() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (dropped_tx, mut dropped_rx) = mpsc::unbounded_channel();
    let block = RpcNoArgsMethod::<()>::new("block");
    let mut server = RpcServer::new();
    server
        .register_no_args(block, move || {
            let started_tx = started_tx.clone();
            let dropped_tx = dropped_tx.clone();
            async move {
                let _drop_notice = DropNotice(dropped_tx);
                let _ = started_tx.send(());
                std::future::pending::<Result<(), RpcFailure>>().await
            }
        })
        .unwrap();

    let bound = server.bind("127.0.0.1:0").await.unwrap();
    let address = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(bound.run_until(async {
        let _ = shutdown_rx.await;
    }));

    let client = RpcClient::connect(address).await.unwrap();
    let call = tokio::spawn(async move { client.call_no_args(block).await });
    tokio::time::timeout(std::time::Duration::from_secs(1), started_rx.recv())
        .await
        .expect("handler did not start")
        .expect("handler start channel closed");

    let _ = shutdown_tx.send(());
    tokio::time::timeout(std::time::Duration::from_secs(1), server_task)
        .await
        .expect("server did not finish graceful shutdown")
        .unwrap()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx.recv())
        .await
        .expect("handler was not cancelled")
        .expect("handler drop channel closed");
    assert_eq!(call.await.unwrap().unwrap_err(), RpcError::ConnectionClosed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_cancellation_releases_client_dispatch_capacity() {
    let block = RpcNoArgsMethod::<()>::new("block");
    let ping = RpcNoArgsMethod::<String>::new("ping");
    let mut server = RpcServer::new();
    server
        .register_no_args(block, || async {
            std::future::pending::<Result<(), RpcFailure>>().await
        })
        .unwrap()
        .register_no_args(ping, || async { Ok("pong".to_owned()) })
        .unwrap();

    let bound = server.bind("127.0.0.1:0").await.unwrap();
    let address = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(bound.run_until(async {
        let _ = shutdown_rx.await;
    }));

    let client = RpcClient::connect_with_config(
        address,
        ClientConfig {
            request_timeout: Some(std::time::Duration::from_millis(20)),
            max_in_flight_requests: 1,
            pending_request_buffer: 1,
            ..ClientConfig::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        client.call_no_args(block).await.unwrap_err(),
        RpcError::TimedOut
    );
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), client.call_no_args(ping))
            .await
            .expect("client dispatch capacity was not released")
            .unwrap(),
        "pong"
    );

    drop(client);
    let _ = shutdown_tx.send(());
    server_task.await.unwrap().unwrap();
}
