use std::error::Error;

use cakemaster_proto::api::{
    DemoService, DemoServiceClient, DemoServiceServer, DescriptorVariant, DiskDescriptor,
    ErrorCode, ReplicaDescriptor,
};
use coro_rpc::{RequestContext, RpcError, RpcFailure, RpcResponse};

struct DemoServiceImpl;

impl DemoService for DemoServiceImpl {
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
        Err(RpcFailure::new(1001, "expected interop error"))
    }

    async fn attachment_echo(
        &self,
        context: RequestContext,
    ) -> Result<RpcResponse<()>, RpcFailure> {
        Ok(RpcResponse::with_attachment((), context.attachment))
    }
}

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
    let server = DemoServiceServer::new(DemoServiceImpl).into_rpc_server()?;
    let bound = server.bind(address).await?;
    println!("Tokio coro_rpc server listening on {}", bound.local_addr()?);
    bound
        .run_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn run_client(address: &str) -> Result<(), Box<dyn Error>> {
    let client = DemoServiceClient::connect(address).await?;
    let echo = client.echo("hello from Rust".to_owned()).await?;
    let sum = client.add(20, 22).await?;
    let error = client.echo_error(ErrorCode::ObjectNotFound).await?;
    if error != ErrorCode::ObjectNotFound {
        return Err("enum mismatch".into());
    }
    let descriptor = ReplicaDescriptor {
        id: 7,
        descriptor_variant: DescriptorVariant::Disk(DiskDescriptor {
            path: "/tmp/cake".to_owned(),
            object_size: 4096,
        }),
        status: 3,
    };
    let echoed_descriptor = client.echo_descriptor(descriptor.clone()).await?;
    if echoed_descriptor != descriptor {
        return Err("variant mismatch".into());
    }
    let pong = client.ping().await?;
    match client.fail().await {
        Err(RpcError::Remote(remote))
            if remote.code == 1001 && remote.message == "expected interop error" => {}
        Err(error) => return Err(error.into()),
        Ok(()) => return Err("fail unexpectedly succeeded".into()),
    }
    let attachment = client.attachment_echo(b"Rust attachment".to_vec()).await?;
    if attachment.attachment.as_ref() != b"Rust attachment" {
        return Err("attachment mismatch".into());
    }
    println!(
        "echo={echo:?}, add={sum}, enum={error:?}, variant={echoed_descriptor:?}, \
         ping={pong:?}, error=1001, attachment=OK"
    );
    Ok(())
}

fn print_usage() {
    eprintln!("usage: cakemaster server [address] | client [address]");
    eprintln!("default address: 127.0.0.1:9000");
}
