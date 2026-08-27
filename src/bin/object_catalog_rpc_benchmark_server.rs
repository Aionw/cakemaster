//! Production-composed RPC server used by the Mooncake 1:1:1 pressure benchmark.

use cakemaster::client::ClientLifecycleConfig;
use cakemaster::object::reclamation::CollectBudget;
use cakemaster::object::{MemoryEvictionConfig, ObjectCatalogConfig};
use cakemaster::segment::{
    ClientId, MemoryRegion, SegmentId, SegmentIdentity, SegmentPoolConfig, SegmentSpec,
    TransportEndpoint, TransportProtocol,
};
use cakemaster::server::{MasterReconcileConfig, MooncakeServerConfig};
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;

const DEFAULT_ADDRESS: &str = "127.0.0.1:19094";
const DEFAULT_THREADS: usize = 8;
// 1,000,000 1-KiB objects occupy 89% of this segment.
const DEFAULT_SEGMENT_BYTES: u64 = 1_150_561_798;
const DEFAULT_EXPECTED_OBJECTS: usize = 4_000_000;
const DEFAULT_MAX_ALLOCATIONS: u32 = 2_000_000;
const DEFAULT_HIGH_WATERMARK: f64 = 0.90;
const DEFAULT_LOW_WATERMARK: f64 = 0.85;
const BENCHMARK_RECONCILE_INTERVAL: Duration = Duration::from_millis(10);
const BENCHMARK_CLIENT_TTL_TICKS: u64 = 60 * 60 * 1_000;
const BENCHMARK_CLIENTS: [ClientId; 4] = [
    ClientId::new(0xBEEF, 1),
    ClientId::new(0xCAFE, 1),
    ClientId::new(0xCAFE, 2),
    ClientId::new(0xCAFE, 3),
];

#[derive(Clone, Copy)]
struct Arguments {
    metadata_shards: usize,
    segment_bytes: u64,
    expected_objects: usize,
    max_allocations: u32,
    high_watermark: f64,
    low_watermark: f64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut values = std::env::args().skip(1);
    let address = values.next().unwrap_or_else(|| DEFAULT_ADDRESS.to_owned());
    let metadata_shards = parse_or(values.next(), DEFAULT_THREADS)?.max(1);
    let arguments = Arguments {
        metadata_shards,
        segment_bytes: parse_or(values.next(), DEFAULT_SEGMENT_BYTES)?,
        expected_objects: parse_or(values.next(), DEFAULT_EXPECTED_OBJECTS)?.max(1),
        max_allocations: parse_or(values.next(), DEFAULT_MAX_ALLOCATIONS)?,
        high_watermark: parse_or(values.next(), DEFAULT_HIGH_WATERMARK)?,
        low_watermark: parse_or(values.next(), DEFAULT_LOW_WATERMARK)?,
    };
    validate(arguments)?;

    compio::runtime::Runtime::new()?.block_on(run_server(&address, arguments))
}

async fn run_server(address: &str, arguments: Arguments) -> Result<(), Box<dyn Error>> {
    let listen_addr: SocketAddr = address.parse()?;
    let memory_eviction =
        MemoryEvictionConfig::new(arguments.high_watermark, arguments.low_watermark)?;
    let reconcile =
        MasterReconcileConfig::new(BENCHMARK_RECONCILE_INTERVAL, CollectBudget::default())?;
    let config = MooncakeServerConfig::default()
        .with_listen_addr(listen_addr)
        .with_segment_pool(SegmentPoolConfig::new(arguments.max_allocations))
        .with_object_catalog(ObjectCatalogConfig::new(arguments.expected_objects))
        .with_metadata_shards(arguments.metadata_shards)
        .with_memory_eviction(memory_eviction)
        .with_client_lifecycle(
            ClientLifecycleConfig::new(BENCHMARK_CLIENTS.len())
                .with_ttl(BENCHMARK_CLIENT_TTL_TICKS),
        )
        .with_reconcile(reconcile);
    let composition = config.build()?;
    let candidate = composition
        .pool()
        .attach(SegmentSpec::memory(
            SegmentIdentity::new(
                SegmentId::new(1, 1),
                ClientId::new(1, 1),
                "rpc-benchmark-memory",
            ),
            MemoryRegion::new(0x4_0000_0000, arguments.segment_bytes),
            TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
        ))?
        .direct_candidate()
        .expect("a Memory segment supports direct reservations");
    for client in BENCHMARK_CLIENTS {
        composition.service().client_manager().remount(
            client,
            Vec::new(),
            composition.clock().client_now(),
        )?;
    }
    let manager = composition.manager().clone();
    let bound = composition.bind().await?;
    println!(
        "object_catalog_rpc_server_ready={} runtime=compio metadata_shards={} segment_bytes={} expected_objects={} high_watermark={:.3} low_watermark={:.3} reconcile_interval_ms={}",
        bound.local_addr()?,
        arguments.metadata_shards,
        arguments.segment_bytes,
        arguments.expected_objects,
        arguments.high_watermark,
        arguments.low_watermark,
        BENCHMARK_RECONCILE_INTERVAL.as_millis(),
    );
    bound
        .run_until(async {
            let _ = compio::signal::ctrl_c().await;
        })
        .await?;

    let catalog = manager.catalog_stats();
    let shard_stats = manager.shard_stats();
    let minimum_shard_objects = shard_stats
        .iter()
        .map(|shard| shard.catalog.published_objects)
        .min()
        .unwrap_or(0);
    let maximum_shard_objects = shard_stats
        .iter()
        .map(|shard| shard.catalog.published_objects)
        .max()
        .unwrap_or(0);
    let space = candidate.stats().space;
    let eviction = manager
        .memory_eviction_stats()
        .expect("the benchmark uses the production memory eviction controller");
    let final_used_ratio = space.used_bytes as f64 / space.capacity_bytes as f64;
    let maximum_used_ratio = eviction.maximum_used_ratio_ppm as f64 / 1_000_000.0;
    let watermark_triggered = eviction.trigger_events != 0;
    let settled_to_low = space.used_bytes <= eviction.low_watermark_bytes
        && catalog.reclaim_debt == 0
        && catalog.retired_bytes == 0;
    println!(
        "object_catalog_rpc_server_final published_objects={} minimum_shard_objects={} maximum_shard_objects={} pending_bytes={} live_bytes={} retired_bytes={} reclaim_debt={} requested_reclaim_debt={} allocation_reclaim_debt={} watermark_reclaim_debt={} capacity_bytes={} used_bytes={} used_ratio={:.6} high_watermark_bytes={} low_watermark_bytes={} maximum_used_bytes={} maximum_used_ratio={:.6} watermark_triggered={} settled_to_low={} trigger_events={} controller_steps={} busy_steps={} controller_retired_objects={} controller_retired_bytes={} controller_reclaimed_objects={} controller_reclaimed_bytes={} allocation_failures={} allocation_retries={} allocation_retry_successes={} wakeups={}",
        catalog.published_objects,
        minimum_shard_objects,
        maximum_shard_objects,
        catalog.pending_bytes,
        catalog.live_bytes,
        catalog.retired_bytes,
        catalog.reclaim_debt,
        eviction.requested_reclaim_debt_bytes,
        eviction.allocation_reclaim_debt_bytes,
        eviction.watermark_reclaim_debt_bytes,
        space.capacity_bytes,
        space.used_bytes,
        final_used_ratio,
        eviction.high_watermark_bytes,
        eviction.low_watermark_bytes,
        eviction.maximum_used_bytes,
        maximum_used_ratio,
        watermark_triggered,
        settled_to_low,
        eviction.trigger_events,
        eviction.controller_steps,
        eviction.busy_steps,
        eviction.retired_objects,
        eviction.retired_bytes_total,
        eviction.reclaimed_objects,
        eviction.reclaimed_bytes_total,
        eviction.allocation_failures,
        eviction.allocation_retries,
        eviction.allocation_retry_successes,
        eviction.wakeups,
    );
    if !watermark_triggered {
        return Err("benchmark did not cross the configured high watermark".into());
    }
    Ok(())
}

fn parse_or<T>(value: Option<String>, default: T) -> Result<T, T::Err>
where
    T: std::str::FromStr,
{
    value.map_or(Ok(default), |value| value.parse())
}

fn validate(arguments: Arguments) -> Result<(), Box<dyn Error>> {
    if arguments.segment_bytes == 0 {
        return Err("segment_bytes must be positive".into());
    }
    if arguments.max_allocations < 3 || arguments.max_allocations >= u32::MAX - 1 {
        return Err("max_allocations must be in [3, u32::MAX - 1)".into());
    }
    MemoryEvictionConfig::new(arguments.high_watermark, arguments.low_watermark)?;
    Ok(())
}
