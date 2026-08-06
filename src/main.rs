use std::error::Error;

use cakemaster::{RpcClient, RpcError, RpcFailure, RpcResponse, RpcServer};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args().skip(1);
    let Some(mode) = arguments.next() else {
        print_usage();
        return Ok(());
    };
    let address = arguments
        .next()
        .unwrap_or_else(|| "127.0.0.1:9000".to_owned());

    match mode.as_str() {
        "server" => run_server(&address).await?,
        "client" => run_client(&address).await?,
        _ => print_usage(),
    }
    Ok(())
}

async fn run_server(address: &str) -> Result<(), Box<dyn Error>> {
    let mut server = RpcServer::new();
    server
        .register::<String, String, _, _>("echo", |value| async move { Ok(value) })?
        .register::<(i32, i32), i32, _, _>("add", |(left, right)| async move { Ok(left + right) })?
        .register_no_args::<String, _, _>("ping", || async { Ok("pong".to_owned()) })?
        .register_no_args::<(), _, _>("fail", || async {
            Err(RpcFailure::new(1001, "expected interop error"))
        })?
        .register_no_args_with_context::<(), _, _>("attachment_echo", |context| async move {
            Ok(RpcResponse::with_attachment((), context.attachment))
        })?;

    let bound = server.bind(address).await?;
    println!("Tokio coro_rpc server listening on {}", bound.local_addr()?);
    bound.run().await?;
    Ok(())
}

async fn run_client(address: &str) -> Result<(), Box<dyn Error>> {
    let client = RpcClient::connect(address).await?;
    let echo = client
        .call::<String, String>("echo", &"hello from Rust".to_owned())
        .await?;
    let sum = client.call::<(i32, i32), i32>("add", &(20, 22)).await?;
    let pong = client.call_no_args::<String>("ping").await?;
    let failure = client.call_no_args::<()>("fail").await.unwrap_err();
    if !matches!(
        failure,
        RpcError::Remote(ref remote)
            if remote.code == 1001 && remote.message == "expected interop error"
    ) {
        return Err(failure.into());
    }
    let attachment = client
        .call_no_args_with_attachment::<()>("attachment_echo", b"Rust attachment".to_vec())
        .await?;
    if attachment.attachment != b"Rust attachment" {
        return Err("attachment mismatch".into());
    }
    println!("echo={echo:?}, add={sum}, ping={pong:?}, error=1001, attachment=OK");
    Ok(())
}

fn print_usage() {
    eprintln!("usage: cakemaster server [address] | client [address]");
    eprintln!("default address: 127.0.0.1:9000");
}
