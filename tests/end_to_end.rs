use cakemaster::{
    RemoteError, RpcClient, RpcError, RpcErrorCode, RpcFailure, RpcResponse, RpcServer,
};
use tokio::sync::oneshot;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tokio_client_and_server_support_pipeline_errors_and_attachments() {
    let mut server = RpcServer::new();
    server
        .register::<String, String, _, _>("echo", |value| async move { Ok(value) })
        .unwrap()
        .register::<(i32, i32), i32, _, _>("add", |(left, right)| async move {
            Ok(left + right)
        })
        .unwrap()
        .register_no_args::<String, _, _>("ping", || async { Ok("pong".to_owned()) })
        .unwrap()
        .register_with_context::<String, String, _, _>(
            "attachment_echo",
            |value, context| async move {
                Ok(RpcResponse::with_attachment(value, context.attachment))
            },
        )
        .unwrap()
        .register::<String, String, _, _>("fail", |_| async {
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
        client
            .call::<String, String>("echo", &"hello".to_owned())
            .await
            .unwrap(),
        "hello"
    );
    assert_eq!(
        client
            .call::<(i32, i32), i32>("add", &(20, 22))
            .await
            .unwrap(),
        42
    );
    assert_eq!(client.call_no_args::<String>("ping").await.unwrap(), "pong");

    let reply = client
        .call_with_attachment::<String, String>(
            "attachment_echo",
            &"body".to_owned(),
            b"raw attachment".to_vec(),
        )
        .await
        .unwrap();
    assert_eq!(reply.value, "body");
    assert_eq!(reply.attachment, b"raw attachment");

    assert_eq!(
        client
            .call::<String, String>("fail", &String::new())
            .await
            .unwrap_err(),
        RpcError::Remote(RemoteError {
            code: 1001,
            message: "error from Rust server".to_owned(),
        })
    );
    assert_eq!(
        client
            .call::<String, String>("missing", &String::new())
            .await
            .unwrap_err(),
        RpcError::Remote(RemoteError {
            code: RpcErrorCode::FunctionNotRegistered as u16,
            message: "function not registered".to_owned(),
        })
    );

    let mut calls = tokio::task::JoinSet::new();
    for value in 0..200_i32 {
        let client = client.clone();
        calls.spawn(async move {
            let result = client
                .call::<(i32, i32), i32>("add", &(value, value + 1))
                .await
                .unwrap();
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
