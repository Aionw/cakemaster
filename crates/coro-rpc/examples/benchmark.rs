use std::error::Error;
use std::time::Instant;

use coro_rpc::{ClientConfig, RpcClient, RpcError, RpcMethod, RpcServer};
use futures_util::future::join_all;
use tokio::runtime::Builder;

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        Some("server") => {
            let address = arguments
                .next()
                .unwrap_or_else(|| "127.0.0.1:19092".to_owned());
            let threads = parse_or(arguments.next(), 1_usize)?;
            runtime(threads)?.block_on(run_server(&address))
        }
        #[cfg(all(feature = "dpdk", target_os = "linux"))]
        Some("server-dpdk") => {
            let library = arguments.next().ok_or("missing F-Stack shared library")?;
            let ini = arguments.next().ok_or("missing F-Stack INI")?;
            let address = arguments
                .next()
                .ok_or("missing IPv4 listen address")?
                .parse()?;
            let seconds = parse_or(arguments.next(), 0_u64)?;
            let mut server = RpcServer::new();
            server.register(
                RpcMethod::<(i32, i32), i32>::new("add"),
                |(left, right)| async move { Ok(left + right) },
            )?;
            // SAFETY: this explicit CLI path must be the trusted library built by
            // interop/fstack/build.sh. No other code initializes or calls DPDK.
            let backend = unsafe { coro_rpc::fstack::FStack::load(library)? };
            backend.run(coro_rpc::fstack::Config::new(ini), server, address, async {
                if seconds == 0 {
                    let _ = tokio::signal::ctrl_c().await;
                } else {
                    tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
                }
            })?;
            Ok(())
        }
        Some("client") => {
            let address = arguments
                .next()
                .unwrap_or_else(|| "127.0.0.1:19092".to_owned());
            let iterations = parse_or(arguments.next(), 200_000_u64)?;
            let pipeline = parse_or(arguments.next(), 1_usize)?.max(1);
            let warmup = parse_or(arguments.next(), 10_000_u64)?;
            runtime(1)?.block_on(run_client(&address, iterations, pipeline, warmup))
        }
        _ => {
            eprintln!(
                "usage: benchmark server [address] [threads] | \
                 client [address] [iterations] [pipeline] [warmup] | \
                 server-dpdk <library.so> <config.ini> <IPv4:port> [seconds] (feature dpdk)"
            );
            Ok(())
        }
    }
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
    let add = RpcMethod::<(i32, i32), i32>::new("add");
    let mut server = RpcServer::new();
    server.register(add, |(left, right)| async move { Ok(left + right) })?;
    let bound = server.bind(address).await?;
    println!("rust_server_ready={}", bound.local_addr()?);
    bound
        .run_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn run_client(
    address: &str,
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
    let add = RpcMethod::<(i32, i32), i32>::new("add");

    run_calls(&client, add, warmup, pipeline).await?;
    let started = Instant::now();
    run_calls(&client, add, iterations, pipeline).await?;
    let elapsed = started.elapsed();
    let qps = iterations as f64 / elapsed.as_secs_f64();
    let completion_us = elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;
    println!(
        "client=rust iterations={iterations} pipeline={pipeline} elapsed_s={:.6} qps={qps:.0} us_per_completion={completion_us:.3}",
        elapsed.as_secs_f64()
    );
    Ok(())
}

async fn run_calls(
    client: &RpcClient,
    add: RpcMethod<(i32, i32), i32>,
    iterations: u64,
    pipeline: usize,
) -> Result<(), RpcError> {
    if pipeline == 1 {
        for index in 0..iterations {
            call_add(client.clone(), add, value_for(index)).await?;
        }
        return Ok(());
    }

    let mut issued = 0_u64;
    while issued < iterations {
        let batch_size = usize::try_from((iterations - issued).min(pipeline as u64))
            .expect("batch size is bounded by usize pipeline");
        let calls = (0..batch_size).map(|offset| {
            call_add(
                client.clone(),
                add,
                value_for(issued + u64::try_from(offset).expect("offset fits u64")),
            )
        });
        for result in join_all(calls).await {
            result?;
        }
        issued += u64::try_from(batch_size).expect("batch size fits u64");
    }
    Ok(())
}

async fn call_add(
    client: RpcClient,
    add: RpcMethod<(i32, i32), i32>,
    value: i32,
) -> Result<(), RpcError> {
    let result = client.call(add, &(value, 1)).await?;
    if result != value + 1 {
        return Err(RpcError::Protocol(format!(
            "unexpected add result {result} for {value}"
        )));
    }
    Ok(())
}

fn value_for(index: u64) -> i32 {
    (index % 1_000_000) as i32
}
