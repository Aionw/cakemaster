use std::net::SocketAddr;
use std::rc::Rc;

use coro_rpc::protocol::{ClientCodec, FrameLimits, RequestFrame};
use coro_rpc::struct_pack::{deserialize, serialize};
use coro_rpc::{Bytes, RpcFailure, RpcMethod, RpcResponse, RpcServer};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::Framed;

#[tokio::test]
async fn external_stream_preserves_pipeline_errors_attachments_and_partial_io() {
    let echo = RpcMethod::<(u32,), u32>::new("echo");
    let fail = RpcMethod::<(), ()>::new("fail");
    let peer: SocketAddr = "192.0.2.1:1234".parse().unwrap();
    let mut server = RpcServer::new();
    server
        .register_with_context(echo, move |(value,), context| async move {
            assert_eq!(context.peer_addr, peer);
            // Exercise pending handlers, not only ready futures.
            tokio::task::yield_now().await;
            Ok(RpcResponse::with_attachment(value, context.attachment))
        })
        .unwrap();
    server
        .register(fail, |()| async { Err(RpcFailure::new(1001, "expected")) })
        .unwrap();
    let handler = server.into_connection_handler();
    let (server_io, client_io) = tokio::io::duplex(31);
    let serve = handler.serve(server_io, peer);
    // No tokio::spawn: external transports may be !Send (as F-Stack is).
    let local = Rc::new(());
    let client = async move {
        let _local = local;
        let mut framed = Framed::new(client_io, ClientCodec::new(FrameLimits::default()));
        for sequence in 0..4 {
            framed
                .feed(
                    RequestFrame::new(
                        sequence,
                        echo.route_id(),
                        serialize(&(sequence,)).unwrap(),
                        Bytes::new(),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        framed.flush().await.unwrap();
        let mut values = Vec::new();
        for _ in 0..4 {
            let response = framed.next().await.unwrap().unwrap();
            assert_eq!(
                response.header.sequence,
                deserialize::<u32>(&response.body).unwrap()
            );
            values.push(response.header.sequence);
        }
        values.sort_unstable();
        assert_eq!(values, [0, 1, 2, 3]);
        let attachment = Bytes::from(vec![0x5a; 128 * 1024]);
        framed
            .send(
                RequestFrame::new(
                    5,
                    echo.route_id(),
                    serialize(&(5_u32,)).unwrap(),
                    attachment.clone(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let response = framed.next().await.unwrap().unwrap();
        assert_eq!(response.attachment, attachment);
        framed
            .send(
                RequestFrame::new(6, fail.route_id(), serialize(&()).unwrap(), Bytes::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        let response = framed.next().await.unwrap().unwrap();
        assert_eq!(response.header.error_code, 255);
        assert_eq!(
            deserialize::<(u16, String)>(&response.body).unwrap(),
            (1001, "expected".into())
        );
        // Half-close must drain completed responses and terminate the driver.
        framed.get_mut().shutdown().await.unwrap();
        assert!(framed.next().await.is_none());
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(serve, client)
    })
    .await
    .unwrap();
    result.unwrap();
}

#[tokio::test]
async fn dropping_external_connection_closes_transport() {
    let handler = RpcServer::new().into_connection_handler();
    let (server, mut client) = tokio::io::duplex(8);
    let connection = handler.serve(server, "192.0.2.1:1".parse().unwrap());
    drop(connection);
    assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
}
