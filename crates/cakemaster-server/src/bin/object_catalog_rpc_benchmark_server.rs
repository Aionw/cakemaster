//! Real ObjectManager RPC server used by the Mooncake mixed-workload benchmark.

use cakemaster::object::reclamation::CollectBudget;
use cakemaster::object::{ObjectCatalogConfig, ObjectManager};
use cakemaster::segment::{
    ClientId, DirectCandidate, MemoryRegion, SegmentId, SegmentIdentity, SegmentPool,
    SegmentPoolConfig, SegmentSpec, TransportEndpoint, TransportProtocol,
};
use cakemaster_proto::mooncake::WrappedMasterServiceServer;
use cakemaster_server::{MasterClock, ObjectCatalogRpcService};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;

const DEFAULT_ADDRESS: &str = "127.0.0.1:19094";
const DEFAULT_THREADS: usize = 8;
// 1,000,000 1-KiB objects occupy 89% of this segment.
const DEFAULT_SEGMENT_BYTES: u64 = 1_150_561_798;
const DEFAULT_EXPECTED_OBJECTS: usize = 4_000_000;
const DEFAULT_MAX_ALLOCATIONS: u32 = 2_000_000;
const DEFAULT_HIGH_WATERMARK: f64 = 0.90;
const DEFAULT_EVICTION_RATIO: f64 = 0.05;
const EVICTION_POLL_INTERVAL: Duration = Duration::from_millis(10);
const IDLE_SAMPLE_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Clone, Copy)]
struct Arguments {
    threads: usize,
    segment_bytes: u64,
    expected_objects: usize,
    max_allocations: u32,
    high_watermark: f64,
    eviction_ratio: f64,
}

#[derive(Clone, Copy, Debug, Default)]
struct EvictionStats {
    reclaim_events: u64,
    reclaimed_objects: u64,
    reclaimed_bytes: u64,
    observed_capacity_drops: u64,
    maximum_used_ratio: f64,
}

struct EvictionController {
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<EvictionStats>>,
}

impl EvictionController {
    fn start(
        manager: Arc<ObjectManager>,
        candidate: DirectCandidate,
        high_watermark: f64,
        eviction_ratio: f64,
        clock: MasterClock,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let worker = thread::spawn(move || {
            run_eviction_controller(
                manager,
                candidate,
                high_watermark,
                eviction_ratio,
                worker_stop,
                clock,
            )
        });
        Self {
            stop,
            worker: Some(worker),
        }
    }

    fn stop(mut self) -> EvictionStats {
        self.stop.store(true, Ordering::Release);
        self.worker
            .take()
            .expect("eviction worker is present")
            .join()
            .expect("eviction worker must not panic")
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut values = std::env::args().skip(1);
    let address = values.next().unwrap_or_else(|| DEFAULT_ADDRESS.to_owned());
    let arguments = Arguments {
        threads: parse_or(values.next(), DEFAULT_THREADS)?.max(1),
        segment_bytes: parse_or(values.next(), DEFAULT_SEGMENT_BYTES)?,
        expected_objects: parse_or(values.next(), DEFAULT_EXPECTED_OBJECTS)?.max(1),
        max_allocations: parse_or(values.next(), DEFAULT_MAX_ALLOCATIONS)?,
        high_watermark: parse_or(values.next(), DEFAULT_HIGH_WATERMARK)?,
        eviction_ratio: parse_or(values.next(), DEFAULT_EVICTION_RATIO)?,
    };
    validate(arguments)?;

    Builder::new_multi_thread()
        .worker_threads(arguments.threads)
        .enable_all()
        .build()?
        .block_on(run_server(&address, arguments))
}

async fn run_server(address: &str, arguments: Arguments) -> Result<(), Box<dyn Error>> {
    let pool = Arc::new(SegmentPool::with_config(SegmentPoolConfig::new(
        arguments.max_allocations,
    ))?);
    let candidate = pool
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
    let manager = Arc::new(ObjectManager::with_config(
        pool,
        ObjectCatalogConfig::new(arguments.expected_objects),
    )?);
    let clock = MasterClock::new();
    let controller = EvictionController::start(
        manager.clone(),
        candidate.clone(),
        arguments.high_watermark,
        arguments.eviction_ratio,
        clock.clone(),
    );
    let server = WrappedMasterServiceServer::new(ObjectCatalogRpcService::new_with_clock(
        manager.clone(),
        clock,
    ))
    .into_rpc_server()?;
    let bound = server.bind(address).await?;
    println!(
        "object_catalog_rpc_server_ready={} threads={} segment_bytes={} expected_objects={} high_watermark={:.3} eviction_ratio={:.3}",
        bound.local_addr()?,
        arguments.threads,
        arguments.segment_bytes,
        arguments.expected_objects,
        arguments.high_watermark,
        arguments.eviction_ratio,
    );
    bound
        .run_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    let eviction = controller.stop();
    let catalog = manager.catalog().stats();
    let space = candidate.stats().space;
    println!(
        "object_catalog_rpc_server_final published_objects={} live_bytes={} retired_bytes={} reclaim_debt={} used_bytes={} used_ratio={:.6} reclaim_events={} controller_reclaimed_objects={} controller_reclaimed_bytes={} observed_capacity_drops={} maximum_used_ratio={:.6}",
        catalog.published_objects,
        catalog.live_bytes,
        catalog.retired_bytes,
        catalog.reclaim_debt,
        space.used_bytes,
        space.used_bytes as f64 / space.capacity_bytes as f64,
        eviction.reclaim_events,
        eviction.reclaimed_objects,
        eviction.reclaimed_bytes,
        eviction.observed_capacity_drops,
        eviction.maximum_used_ratio,
    );
    Ok(())
}

fn run_eviction_controller(
    manager: Arc<ObjectManager>,
    candidate: DirectCandidate,
    high_watermark: f64,
    eviction_ratio: f64,
    stop: Arc<AtomicBool>,
    clock: MasterClock,
) -> EvictionStats {
    let mut stats = EvictionStats::default();
    let mut next_trigger = Instant::now();
    let mut previous_used = 0_u64;

    while !stop.load(Ordering::Acquire) {
        let now = Instant::now();
        let space = candidate.stats().space;
        let used_ratio = space.used_bytes as f64 / space.capacity_bytes as f64;
        stats.maximum_used_ratio = stats.maximum_used_ratio.max(used_ratio);
        if previous_used > space.used_bytes {
            stats.observed_capacity_drops += 1;
        }
        previous_used = space.used_bytes;

        let catalog = manager.catalog().stats();
        if now >= next_trigger {
            if used_ratio > high_watermark && catalog.reclaim_debt == 0 {
                let target_ratio = eviction_ratio
                    .max(used_ratio - high_watermark + eviction_ratio)
                    .min(1.0);
                let target_bytes = ((catalog.live_bytes as f64) * target_ratio).ceil() as u64;
                if target_bytes != 0 {
                    manager.catalog().request_reclaim(target_bytes);
                    stats.reclaim_events += 1;
                }
            }
            next_trigger = now + EVICTION_POLL_INTERVAL;
        }

        let catalog = manager.catalog().stats();
        if catalog.reclaim_debt != 0 || catalog.retired_bytes != 0 {
            let report = manager.maintenance(clock.now(), CollectBudget::default());
            stats.reclaimed_objects = stats
                .reclaimed_objects
                .saturating_add(report.catalog.reclaimed_objects as u64);
            stats.reclaimed_bytes = stats
                .reclaimed_bytes
                .saturating_add(report.catalog.reclaimed_bytes);
            continue;
        }
        thread::sleep(IDLE_SAMPLE_INTERVAL);
    }
    stats
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
    if !(0.0..1.0).contains(&arguments.high_watermark) {
        return Err("high_watermark must be in [0, 1)".into());
    }
    if !(0.0..=1.0).contains(&arguments.eviction_ratio) || arguments.eviction_ratio == 0.0 {
        return Err("eviction_ratio must be in (0, 1]".into());
    }
    Ok(())
}
