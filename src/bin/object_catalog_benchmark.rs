//! Object catalog throughput benchmark.

use cakemaster::object::reclamation::{CatalogTick, CollectBudget};
use cakemaster::object::{
    DirectReplica, NamespaceId, ObjectCatalog, ObjectCatalogConfig, ObjectCommit, ObjectContent,
    ObjectIdentity, ReplicaId, ReplicaLease, ReplicaSet, WriteAdmission,
};
use cakemaster::segment::config::DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT;
use cakemaster::segment::{
    ClientId, MemoryRegion, PoolSnapshot, SegmentId, SegmentIdentity, SegmentPool,
    SegmentPoolConfig, SegmentSpec, TransportEndpoint, TransportProtocol,
};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const OBJECT_BYTES: u64 = 4096;
const SEGMENT_CAPACITY: u64 = 8_u64 << 30;
const MAX_COLLECTOR_SAMPLES: usize = 1_000_000;
const COLLECT_EVERY_OPERATIONS: u64 = 32;
const COLLECT_BATCH: usize = 16;
const OWNER: ClientId = ClientId::new(41, 73);

#[derive(Clone, Copy)]
enum Scenario {
    Baseline,
    IncrementalGc,
}

impl Scenario {
    const ALL: [Self; 2] = [Self::Baseline, Self::IncrementalGc];

    const fn name(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::IncrementalGc => "incremental-gc",
        }
    }

    const fn has_collector(self) -> bool {
        matches!(self, Self::IncrementalGc)
    }
}

struct BenchmarkResult {
    elapsed: Duration,
    put_latencies: Vec<u64>,
    get_latencies: Vec<u64>,
    get_hits: usize,
    collector: CollectorResult,
}

#[derive(Default)]
struct CollectorResult {
    latencies: Vec<u64>,
    steps: usize,
    scanned_candidates: usize,
    reclaimed_objects: usize,
}

struct WorkerResult {
    put_latencies: Vec<u64>,
    get_latencies: Vec<u64>,
    get_hits: usize,
}

fn main() {
    let available_threads = thread::available_parallelism().map_or(4, usize::from);
    let threads = argument(1).unwrap_or(available_threads.clamp(1, 8));
    let operations_per_thread = argument(2).unwrap_or(50_000);
    let segments = argument(3).unwrap_or(threads.clamp(1, 8));
    let rounds = argument(4).unwrap_or(3);
    let hot_objects = argument(5).unwrap_or((threads * 256).max(1024));
    assert!(threads != 0, "threads must be non-zero");
    assert!(
        operations_per_thread >= 2,
        "operations per thread must be at least 2"
    );
    assert!(segments != 0, "segments must be non-zero");
    assert!(rounds != 0, "rounds must be non-zero");
    assert!(hot_objects != 0, "hot object count must be non-zero");

    let put_count = threads * operations_per_thread.div_ceil(2);
    let max_allocations_per_segment = (put_count + hot_objects).div_ceil(segments);
    assert!(
        max_allocations_per_segment < DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT as usize,
        "increase segment count or reduce operations: the baseline needs {max_allocations_per_segment} allocator nodes per segment"
    );
    assert!(
        (max_allocations_per_segment as u64) * OBJECT_BYTES < SEGMENT_CAPACITY,
        "increase segment count or reduce operations: the baseline exceeds segment capacity"
    );

    println!("ObjectCatalog 50:50 put/get benchmark");
    println!(
        "threads={threads} segments={segments} ops/thread={operations_per_thread} hot_objects={hot_objects} object={OBJECT_BYTES}B rounds={rounds}"
    );
    println!(
        "scenario        ops/s    put p50/p99/max ns    get p50/p99/max ns   hit%   collector p99/max us"
    );

    for scenario in Scenario::ALL {
        let mut samples = (0..rounds)
            .map(|_| {
                run_once(
                    scenario,
                    threads,
                    operations_per_thread,
                    segments,
                    hot_objects,
                )
            })
            .collect::<Vec<_>>();
        samples.sort_unstable_by_key(|sample| sample.elapsed);
        let mut sample = samples.swap_remove(samples.len() / 2);
        print_result(scenario, threads * operations_per_thread, &mut sample);
    }
}

fn run_once(
    scenario: Scenario,
    threads: usize,
    operations_per_thread: usize,
    segments: usize,
    hot_objects: usize,
) -> BenchmarkResult {
    let pool = build_pool(segments);
    let snapshot = pool.snapshot();
    let put_count = threads * operations_per_thread.div_ceil(2);
    let expected_objects = hot_objects
        .checked_add(put_count)
        .and_then(|count| count.checked_add(1024))
        .expect("object count overflowed usize");
    let lease_ticks = (threads * 1024).max(1024) as u64;
    let catalog = Arc::new(
        ObjectCatalog::with_config(
            ObjectCatalogConfig::new(expected_objects)
                .with_lease(lease_ticks, lease_ticks / 2)
                .with_pending_timeout(1_000_000_000)
                .with_empty_slot_grace(lease_ticks)
                .with_max_retired_bytes(u64::MAX),
        )
        .unwrap(),
    );
    let hot_keys =
        Arc::<[ObjectIdentity]>::from(preload_hot_objects(&catalog, &pool, &snapshot, hot_objects));
    let put_keys = make_put_keys(threads, operations_per_thread);
    let tick = Arc::new(AtomicU64::new(1));
    let stop = Arc::new(AtomicBool::new(false));
    let participants = threads + 1 + usize::from(scenario.has_collector());
    let barrier = Arc::new(Barrier::new(participants));
    let watermark = ((hot_objects + threads * 1024) as u64).saturating_mul(OBJECT_BYTES);

    let (elapsed, workers, collector) = thread::scope(|scope| {
        let mut worker_handles = Vec::with_capacity(threads);
        for (worker, keys) in put_keys.into_iter().enumerate() {
            let pool = pool.clone();
            let catalog = catalog.clone();
            let hot_keys = hot_keys.clone();
            let tick = tick.clone();
            let barrier = barrier.clone();
            let candidate = snapshot.candidates()[worker % snapshot.len()].clone();
            worker_handles.push(scope.spawn(move || {
                run_worker(
                    worker,
                    operations_per_thread,
                    keys,
                    pool,
                    candidate,
                    catalog,
                    hot_keys,
                    tick,
                    barrier,
                )
            }));
        }

        let collector_handle = scenario.has_collector().then(|| {
            let catalog = catalog.clone();
            let tick = tick.clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            scope.spawn(move || run_collector(catalog, tick, stop, barrier, watermark))
        });

        barrier.wait();
        let started = Instant::now();
        let workers = worker_handles
            .into_iter()
            .map(|handle| handle.join().expect("benchmark worker must not panic"))
            .collect::<Vec<_>>();
        let elapsed = started.elapsed();
        stop.store(true, Ordering::Release);
        let collector = collector_handle
            .map(|handle| handle.join().expect("collector must not panic"))
            .unwrap_or_default();
        (elapsed, workers, collector)
    });

    let mut put_latencies = Vec::with_capacity(put_count);
    let mut get_latencies = Vec::with_capacity(threads * (operations_per_thread / 2));
    let mut get_hits = 0;
    for worker in workers {
        put_latencies.extend(worker.put_latencies);
        get_latencies.extend(worker.get_latencies);
        get_hits += worker.get_hits;
    }
    BenchmarkResult {
        elapsed,
        put_latencies,
        get_latencies,
        get_hits,
        collector,
    }
}

#[allow(clippy::too_many_arguments)]
fn run_worker(
    worker: usize,
    operations: usize,
    put_keys: Vec<ObjectIdentity>,
    pool: Arc<SegmentPool>,
    candidate: cakemaster::segment::DirectCandidate,
    catalog: Arc<ObjectCatalog>,
    hot_keys: Arc<[ObjectIdentity]>,
    tick: Arc<AtomicU64>,
    barrier: Arc<Barrier>,
) -> WorkerResult {
    let mut put_keys = put_keys.into_iter();
    let mut put_latencies = Vec::with_capacity(operations.div_ceil(2));
    let mut get_latencies = Vec::with_capacity(operations / 2);
    let mut get_hits = 0;
    let mut random = (worker as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    barrier.wait();

    for operation in 0..operations {
        let now = CatalogTick::new(tick.fetch_add(1, Ordering::Relaxed));
        if operation & 1 == 0 {
            let identity = put_keys.next().expect("a put key exists for every put");
            let started = Instant::now();
            let reservation = pool
                .reserve(&candidate, OBJECT_BYTES)
                .expect("benchmark segment must have capacity");
            let ticket = catalog
                .claim_put(identity, WriteAdmission::unmanaged(OWNER), now)
                .expect("benchmark keys are unique")
                .stage(
                    ObjectContent::new(OBJECT_BYTES),
                    ReplicaSet::one(ReplicaLease::Direct(DirectReplica::new(
                        ReplicaId::new(1),
                        reservation,
                    ))),
                )
                .expect("benchmark objects are valid");
            let handle = catalog
                .publish(&ticket, ObjectCommit::default())
                .expect("a fresh ticket can be published");
            black_box(handle.identity());
            put_latencies.push(elapsed_nanos(started));
        } else {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let key = &hot_keys[random as usize % hot_keys.len()];
            let started = Instant::now();
            if let Ok(read) = catalog.get(key.as_lookup(), now) {
                get_hits += 1;
                black_box(read.object().replicas());
            }
            get_latencies.push(elapsed_nanos(started));
        }
    }

    WorkerResult {
        put_latencies,
        get_latencies,
        get_hits,
    }
}

fn run_collector(
    catalog: Arc<ObjectCatalog>,
    tick: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
    watermark: u64,
) -> CollectorResult {
    let mut result = CollectorResult::default();
    let mut next_collection = COLLECT_EVERY_OPERATIONS;
    barrier.wait();
    while !stop.load(Ordering::Acquire) {
        let observed_tick = tick.load(Ordering::Relaxed);
        if observed_tick < next_collection {
            thread::yield_now();
            continue;
        }
        next_collection = next_collection.saturating_add(COLLECT_EVERY_OPERATIONS);
        let stats = catalog.stats();
        if stats.live_bytes > watermark {
            catalog.request_reclaim(stats.live_bytes - watermark);
        }

        let started = Instant::now();
        let report = catalog.collect_step(
            CatalogTick::new(observed_tick),
            CollectBudget::new(COLLECT_BATCH, COLLECT_BATCH, COLLECT_BATCH / 4),
        );
        let latency = elapsed_nanos(started);
        result.steps += 1;
        result.scanned_candidates += report.scanned_candidates;
        result.reclaimed_objects += report.reclaimed_objects;
        if result.latencies.len() < MAX_COLLECTOR_SAMPLES {
            result.latencies.push(latency);
        }
    }
    result
}

fn build_pool(segments: usize) -> Arc<SegmentPool> {
    let pool = Arc::new(
        SegmentPool::with_config(SegmentPoolConfig::new(
            DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT,
        ))
        .unwrap(),
    );
    for index in 0..segments {
        let index = index as u64;
        pool.attach(SegmentSpec::memory(
            SegmentIdentity::new(
                SegmentId::new(1, index + 1),
                OWNER,
                format!("catalog-memory-{index}"),
            ),
            MemoryRegion::new(
                0x10_0000_0000 + index * (SEGMENT_CAPACITY * 2),
                SEGMENT_CAPACITY,
            ),
            TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
        ))
        .unwrap();
    }
    pool
}

fn preload_hot_objects(
    catalog: &ObjectCatalog,
    pool: &SegmentPool,
    snapshot: &PoolSnapshot,
    count: usize,
) -> Vec<ObjectIdentity> {
    let mut identities = Vec::with_capacity(count);
    for index in 0..count {
        let identity = ObjectIdentity::new(NamespaceId::DEFAULT, format!("hot-{index:08x}"));
        let reservation = pool
            .reserve(&snapshot.candidates()[index % snapshot.len()], OBJECT_BYTES)
            .unwrap();
        let ticket = catalog
            .claim_put(
                identity.clone(),
                WriteAdmission::unmanaged(OWNER),
                CatalogTick::ZERO,
            )
            .unwrap()
            .stage(
                ObjectContent::new(OBJECT_BYTES),
                ReplicaSet::one(ReplicaLease::Direct(DirectReplica::new(
                    ReplicaId::new(1),
                    reservation,
                ))),
            )
            .unwrap();
        drop(catalog.publish(&ticket, ObjectCommit::default()).unwrap());
        drop(
            catalog
                .get(identity.as_lookup(), CatalogTick::ZERO)
                .unwrap(),
        );
        identities.push(identity);
    }
    identities
}

fn make_put_keys(threads: usize, operations: usize) -> Vec<Vec<ObjectIdentity>> {
    (0..threads)
        .map(|worker| {
            (0..operations.div_ceil(2))
                .map(|sequence| {
                    ObjectIdentity::new(
                        NamespaceId::DEFAULT,
                        format!("put-{worker:04x}-{sequence:016x}"),
                    )
                })
                .collect()
        })
        .collect()
}

fn print_result(scenario: Scenario, operations: usize, result: &mut BenchmarkResult) {
    result.put_latencies.sort_unstable();
    result.get_latencies.sort_unstable();
    result.collector.latencies.sort_unstable();
    let gets = result.get_latencies.len();
    let hit_rate = result.get_hits as f64 * 100.0 / gets.max(1) as f64;
    let collector_p99 = percentile(&result.collector.latencies, 99) as f64 / 1000.0;
    let collector_max = result.collector.latencies.last().copied().unwrap_or(0) as f64 / 1000.0;
    println!(
        "{:<14} {:>9.0}  {:>6}/{:>6}/{:>7}  {:>6}/{:>6}/{:>7}  {:>5.1}  {:>9.1}/{:>7.1}",
        scenario.name(),
        operations as f64 / result.elapsed.as_secs_f64(),
        percentile(&result.put_latencies, 50),
        percentile(&result.put_latencies, 99),
        result.put_latencies.last().copied().unwrap_or(0),
        percentile(&result.get_latencies, 50),
        percentile(&result.get_latencies, 99),
        result.get_latencies.last().copied().unwrap_or(0),
        hit_rate,
        collector_p99,
        collector_max,
    );
    if scenario.has_collector() {
        println!(
            "  collector steps={} scanned={} reclaimed={} samples={}",
            result.collector.steps,
            result.collector.scanned_candidates,
            result.collector.reclaimed_objects,
            result.collector.latencies.len(),
        );
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn elapsed_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn argument(index: usize) -> Option<usize> {
    std::env::args().nth(index).map(|value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("argument {index} must be a positive integer"))
    })
}
