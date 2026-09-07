//! Live native transport probe, run by interop/fstack/probe.sh.
#[cfg(all(feature = "dpdk", target_os = "linux"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use coro_rpc::{Bytes, RpcClient, RpcFailure, RpcMethod, RpcResponse, RpcServer};
    use std::time::Duration;
    let mut args = std::env::args().skip(1);
    let echo = RpcMethod::<(u32,), u32>::new("probe_echo");
    let fail = RpcMethod::<(), ()>::new("probe_fail");
    match args.next().as_deref() {
        Some("server") => {
            let library = args.next().ok_or("missing library")?;
            let config = coro_rpc::fstack::Config::new(args.next().ok_or("missing INI")?);
            let address = args.next().ok_or("missing address")?.parse()?;
            let mut server = RpcServer::new();
            server.register_with_context(echo, |(value,), context| async move {
                tokio::time::sleep(Duration::from_millis(1)).await;
                Ok(RpcResponse::with_attachment(value, context.attachment))
            })?;
            server.register(fail, |()| async {
                Err(RpcFailure::new(1001, "probe error"))
            })?;
            // SAFETY: only use the trusted, pinned library built by build.sh.
            unsafe { coro_rpc::fstack::FStack::load(library)? }.run(
                config,
                server,
                address,
                async {
                    let _ = tokio::signal::ctrl_c().await;
                },
            )?;
        }
        Some("client") => {
            let address = args.next().ok_or("missing address")?;
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    for _ in 0..3 {
                        let client = RpcClient::connect(&address).await?;
                        let attachment = Bytes::from(vec![0x5a; 512 * 1024]);
                        let reply = client
                            .call_with_attachment(echo, &(42,), attachment.clone())
                            .await?;
                        assert_eq!(reply.value, 42);
                        assert_eq!(reply.attachment, attachment);
                        let replies = futures_util::future::join_all((0..256).map(|value| {
                            let client = client.clone();
                            async move {
                                assert_eq!(client.call(echo, &(value,)).await.unwrap(), value);
                            }
                        }))
                        .await;
                        assert_eq!(replies.len(), 256);
                        match client.call(fail, &()).await {
                            Err(coro_rpc::RpcError::Remote(error)) => assert_eq!(error.code, 1001),
                            result => panic!("unexpected failure reply: {result:?}"),
                        }
                    }
                    Ok::<_, coro_rpc::RpcError>(())
                })?;
            println!(
                "probe_ok: timers, 512KiB attachment, pipeline=256, extended error, reconnect"
            );
        }
        _ => {
            return Err(
                "usage: fstack_probe server library.so config.ini IPv4:port | client IPv4:port"
                    .into(),
            );
        }
    }
    Ok(())
}

#[cfg(not(all(feature = "dpdk", target_os = "linux")))]
fn main() {
    eprintln!("fstack_probe requires Linux and --features dpdk");
    std::process::exit(1);
}
