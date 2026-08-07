use cakemaster::object::{
    CatalogTick, CollectBudget, MemoryReplica, NamespaceId, ObjectCatalog, ObjectCatalogConfig,
    ObjectCommit, ObjectContent, ObjectIdentity, ReclaimReason, ReclaimTarget, ReplicaId,
    ReplicaLease, ReplicaSet, WriteOwner,
};
use cakemaster::segment::{
    ClientId, MemoryRegion, MemorySegmentSpec, SegmentCandidate, SegmentId, SegmentIdentity,
    SegmentPool, SegmentPoolConfig, TransportEndpoint, TransportProtocol,
};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const OBJECT_BYTES: u64 = 1024;
const SEGMENT_BASE: u64 = 0x3_0000_0000;
const OWNER: ClientId = ClientId::new(41, 73);
const SAMPLE_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Clone, Copy)]
struct Arguments {
    num_objects: usize,
    threads: usize,
    operations_per_thread: usize,
    hot_objects: usize,
    initial_used_ratio: f64,
    high_watermark_ratio: f64,
    eviction_ratio: f64,
    collector_interval_ms: u64,
    collect_budget: usize,
    slot_cleanup_budget: usize,
    settle_timeout_ms: u64,
    rounds: usize,
}

#[derive(Default)]
struct WorkerResult {
    before_put_latencies_ns: Vec<u64>,
    before_get_latencies_ns: Vec<u64>,
    pressure_put_latencies_ns: Vec<u64>,
    pressure_put_success_latencies_ns: Vec<u64>,
    pressure_put_failure_latencies_ns: Vec<u64>,
    pressure_get_latencies_ns: Vec<u64>,
    put_successes: usize,
    put_start_failures: usize,
    put_end_failures: usize,
    get_hits: usize,
    get_failures: usize,
}

#[derive(Default)]
struct CollectorResult {
    maximum_used_ratio: f64,
    observed_drops: usize,
    reclaim_events: usize,
    reclaimed_objects: usize,
    step_latencies_ns: Vec<u64>,
}

struct BenchmarkResult {
    initial_ratio: f64,
    final_ratio: f64,
    elapsed: Duration,
    objects_before: usize,
    objects_after: usize,
    workers: Vec<WorkerResult>,
    collector: CollectorResult,
}

fn main() {
    let arguments = parse_arguments();
    validate_arguments(arguments);
    println!(
        "objects_before,initial_ratio,max_ratio,final_ratio,operations,ops_per_sec,put_success,put_start_fail,put_end_fail,get_hits,get_fail,evicted_objects,reclaim_events,observed_drops,before_put_samples,before_put_p99_ns,before_get_samples,before_get_p99_ns,pressure_put_samples,pressure_put_p99_ns,pressure_put_p999_ns,pressure_put_max_ns,pressure_put_success_samples,pressure_put_success_p99_ns,pressure_put_success_p999_ns,pressure_put_success_max_ns,pressure_put_failure_p99_ns,pressure_get_samples,pressure_get_p99_ns,pressure_get_p999_ns,pressure_get_max_ns,collector_step_p99_ns,collector_step_p999_ns,collector_step_max_ns"
    );
    for _ in 0..arguments.rounds {
        print_result(arguments, run_once(arguments));
    }
}

fn run_once(arguments: Arguments) -> BenchmarkResult {
    let put_count = arguments.threads * arguments.operations_per_thread.div_ceil(2);
    let expected_objects = arguments
        .num_objects
        .checked_add(put_count)
        .and_then(|count| count.checked_add(1024))
        .expect("expected object count overflowed usize");
    let max_allocator_nodes = u32::try_from(expected_objects)
        .expect("benchmark object count exceeds allocator addressing");
    let segment_size = segment_size_for(arguments);
    let pool = Arc::new(
        SegmentPool::with_config(SegmentPoolConfig::new(max_allocator_nodes))
            .expect("allocator node count must be valid"),
    );
    let candidate = pool
        .attach(MemorySegmentSpec::new(
            SegmentIdentity::new(
                SegmentId::new(1, 1),
                OWNER,
                "object_catalog_watermark_segment",
            ),
            MemoryRegion::new(SEGMENT_BASE, segment_size),
            TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
        ))
        .expect("benchmark segment must be valid")
        .candidate()
        .clone();
    let catalog = Arc::new(
        ObjectCatalog::with_config(
            ObjectCatalogConfig::new(expected_objects)
                .with_lease(1_000_000_000, 500_000_000)
                .with_pending_timeout(u64::MAX)
                .with_empty_slot_grace(0)
                .with_max_retired_bytes(u64::MAX),
        )
        .expect("benchmark catalog configuration must be valid"),
    );

    let hot_keys = Arc::<[ObjectIdentity]>::from(preload_objects(
        &catalog,
        &pool,
        &candidate,
        arguments.num_objects,
        arguments.hot_objects,
    ));
    for identity in hot_keys.iter() {
        drop(
            catalog
                .get(identity.as_lookup(), CatalogTick::new(1))
                .expect("hot object must exist"),
        );
    }
    let initial_ratio = used_ratio(&candidate);
    assert!(
        initial_ratio < arguments.high_watermark_ratio,
        "initial usage already crossed the high watermark"
    );

    let put_keys = make_put_keys(arguments.threads, arguments.operations_per_thread);
    let pressure_started = Arc::new(AtomicBool::new(false));
    let workload_running = Arc::new(AtomicBool::new(true));
    let start = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(arguments.threads + 2));
    let objects_before = catalog.stats().published_objects;

    let (elapsed, workers, collector) = thread::scope(|scope| {
        let collector_handle = {
            let catalog = catalog.clone();
            let candidate = candidate.clone();
            let pressure_started = pressure_started.clone();
            let workload_running = workload_running.clone();
            let start = start.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                run_collector(
                    arguments,
                    catalog,
                    candidate,
                    pressure_started,
                    workload_running,
                    start,
                    barrier,
                )
            })
        };
        let mut worker_handles = Vec::with_capacity(arguments.threads);
        for (worker, keys) in put_keys.into_iter().enumerate() {
            let catalog = catalog.clone();
            let pool = pool.clone();
            let candidate = candidate.clone();
            let hot_keys = hot_keys.clone();
            let pressure_started = pressure_started.clone();
            let start = start.clone();
            let barrier = barrier.clone();
            worker_handles.push(scope.spawn(move || {
                run_worker(
                    arguments,
                    worker,
                    keys,
                    catalog,
                    pool,
                    candidate,
                    hot_keys,
                    pressure_started,
                    start,
                    barrier,
                )
            }));
        }

        barrier.wait();
        let begin = Instant::now();
        start.store(true, Ordering::Release);
        let workers = worker_handles
            .into_iter()
            .map(|handle| handle.join().expect("watermark worker must not panic"))
            .collect::<Vec<_>>();
        let elapsed = begin.elapsed();
        workload_running.store(false, Ordering::Release);
        let collector = collector_handle
            .join()
            .expect("watermark collector must not panic");
        (elapsed, workers, collector)
    });

    let final_ratio = used_ratio(&candidate);
    BenchmarkResult {
        initial_ratio,
        final_ratio,
        elapsed,
        objects_before,
        objects_after: catalog.stats().published_objects,
        workers,
        collector,
    }
}

#[allow(clippy::too_many_arguments)]
fn run_worker(
    arguments: Arguments,
    worker: usize,
    put_keys: Vec<ObjectIdentity>,
    catalog: Arc<ObjectCatalog>,
    pool: Arc<SegmentPool>,
    candidate: SegmentCandidate,
    hot_keys: Arc<[ObjectIdentity]>,
    pressure_started: Arc<AtomicBool>,
    start: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) -> WorkerResult {
    let mut result = WorkerResult {
        before_put_latencies_ns: Vec::with_capacity(arguments.operations_per_thread.div_ceil(2)),
        before_get_latencies_ns: Vec::with_capacity(arguments.operations_per_thread / 2),
        pressure_put_latencies_ns: Vec::with_capacity(arguments.operations_per_thread.div_ceil(2)),
        pressure_put_success_latencies_ns: Vec::with_capacity(
            arguments.operations_per_thread.div_ceil(2),
        ),
        pressure_put_failure_latencies_ns: Vec::with_capacity(
            arguments.operations_per_thread.div_ceil(2),
        ),
        pressure_get_latencies_ns: Vec::with_capacity(arguments.operations_per_thread / 2),
        ..WorkerResult::default()
    };
    let mut keys = put_keys.into_iter();
    let mut random = (worker as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    barrier.wait();
    while !start.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }
    for operation in 0..arguments.operations_per_thread {
        let pressure = pressure_started.load(Ordering::Acquire);
        let begin = Instant::now();
        if operation & 1 == 0 {
            let identity = keys.next().expect("one key exists for every put");
            let succeeded = match pool.reserve(&candidate, OBJECT_BYTES) {
                Ok(reservation) => {
                    match catalog.claim_put(identity, WriteOwner::new(OWNER), CatalogTick::ZERO) {
                        Ok(claim) => match claim.stage(
                            ObjectContent::new(OBJECT_BYTES),
                            ReplicaSet::one(ReplicaLease::Memory(MemoryReplica::new(
                                ReplicaId::new(1),
                                reservation,
                            ))),
                        ) {
                            Ok(ticket) => match catalog.publish(&ticket, ObjectCommit::default()) {
                                Ok(handle) => {
                                    black_box(handle.identity());
                                    true
                                }
                                Err(_) => {
                                    result.put_end_failures += 1;
                                    false
                                }
                            },
                            Err(_) => {
                                result.put_end_failures += 1;
                                false
                            }
                        },
                        Err(_) => {
                            result.put_start_failures += 1;
                            false
                        }
                    }
                }
                Err(_) => {
                    result.put_start_failures += 1;
                    false
                }
            };
            if succeeded {
                result.put_successes += 1;
            }
            let latency = elapsed_nanos(begin);
            if pressure {
                result.pressure_put_latencies_ns.push(latency);
                if succeeded {
                    result.pressure_put_success_latencies_ns.push(latency);
                } else {
                    result.pressure_put_failure_latencies_ns.push(latency);
                }
            } else {
                result.before_put_latencies_ns.push(latency);
            }
        } else {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            match catalog.get(
                hot_keys[random as usize % hot_keys.len()].as_lookup(),
                CatalogTick::new(1),
            ) {
                Ok(read) => {
                    black_box(read.object().replicas());
                    result.get_hits += 1;
                }
                Err(_) => result.get_failures += 1,
            }
            let latency = elapsed_nanos(begin);
            if pressure {
                result.pressure_get_latencies_ns.push(latency);
            } else {
                result.before_get_latencies_ns.push(latency);
            }
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn run_collector(
    arguments: Arguments,
    catalog: Arc<ObjectCatalog>,
    candidate: SegmentCandidate,
    pressure_started: Arc<AtomicBool>,
    workload_running: Arc<AtomicBool>,
    start: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) -> CollectorResult {
    let mut result = CollectorResult::default();
    let collect_budget = CollectBudget::new(
        arguments.collect_budget,
        arguments.collect_budget,
        arguments.slot_cleanup_budget,
    );
    let trigger_interval = Duration::from_millis(arguments.collector_interval_ms);
    barrier.wait();
    while !start.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }
    let mut next_sample = Instant::now();
    let mut next_trigger = Instant::now() + trigger_interval;
    let mut previous_used = candidate.stats().space.used_bytes;
    let mut last_ratio = used_ratio(&candidate);
    let mut settle_deadline = None;

    loop {
        let now = Instant::now();
        if now >= next_sample {
            let stats = candidate.stats();
            let used = stats.space.used_bytes;
            last_ratio = used as f64 / stats.space.capacity_bytes as f64;
            result.maximum_used_ratio = result.maximum_used_ratio.max(last_ratio);
            if last_ratio > arguments.high_watermark_ratio {
                pressure_started.store(true, Ordering::Release);
            }
            if previous_used > used.saturating_add(OBJECT_BYTES) {
                result.observed_drops += 1;
            }
            previous_used = used;
            next_sample = now + SAMPLE_INTERVAL;
        }

        let stats = catalog.stats();
        if now >= next_trigger {
            if last_ratio > arguments.high_watermark_ratio && stats.reclaim_debt == 0 {
                let target_ratio = arguments
                    .eviction_ratio
                    .max(last_ratio - arguments.high_watermark_ratio + arguments.eviction_ratio);
                let target_bytes = ((stats.live_bytes as f64) * target_ratio).ceil() as u64;
                catalog.request_reclaim(ReclaimTarget::new(
                    target_bytes,
                    ReclaimReason::CapacityPressure,
                ));
                result.reclaim_events += 1;
            }
            next_trigger = now + trigger_interval;
        }

        let stats = catalog.stats();
        if stats.reclaim_debt != 0 || stats.retired_bytes != 0 {
            let begin = Instant::now();
            let report = catalog.collect_step(CatalogTick::new(1), collect_budget);
            result.step_latencies_ns.push(elapsed_nanos(begin));
            result.reclaimed_objects += report.reclaimed_objects;
            continue;
        }

        if !workload_running.load(Ordering::Acquire) {
            let deadline = *settle_deadline.get_or_insert_with(|| {
                Instant::now() + Duration::from_millis(arguments.settle_timeout_ms)
            });
            if !pressure_started.load(Ordering::Acquire)
                || last_ratio <= arguments.high_watermark_ratio
                || Instant::now() >= deadline
            {
                break;
            }
        }
        thread::sleep(SAMPLE_INTERVAL);
    }
    result
}

fn preload_objects(
    catalog: &ObjectCatalog,
    pool: &SegmentPool,
    candidate: &SegmentCandidate,
    count: usize,
    hot_objects: usize,
) -> Vec<ObjectIdentity> {
    let mut hot_keys = Vec::with_capacity(hot_objects);
    for index in 0..count {
        let identity = ObjectIdentity::new(
            NamespaceId::DEFAULT,
            format!("batch_evict_bench_key_{index}"),
        );
        if index < hot_objects {
            hot_keys.push(identity.clone());
        }
        let reservation = pool
            .reserve(candidate, OBJECT_BYTES)
            .expect("prefill must fit below the high watermark");
        let ticket = catalog
            .claim_put(identity, WriteOwner::new(OWNER), CatalogTick::ZERO)
            .expect("prefill keys are unique")
            .stage(
                ObjectContent::new(OBJECT_BYTES),
                ReplicaSet::one(ReplicaLease::Memory(MemoryReplica::new(
                    ReplicaId::new(1),
                    reservation,
                ))),
            )
            .expect("prefill replica must be valid");
        drop(
            catalog
                .publish(&ticket, ObjectCommit::default())
                .expect("prefill publish must succeed"),
        );
    }
    hot_keys
}

fn make_put_keys(threads: usize, operations: usize) -> Vec<Vec<ObjectIdentity>> {
    (0..threads)
        .map(|worker| {
            (0..operations.div_ceil(2))
                .map(|sequence| {
                    ObjectIdentity::new(
                        NamespaceId::DEFAULT,
                        format!("watermark-{worker:04x}-{sequence:016x}"),
                    )
                })
                .collect()
        })
        .collect()
}

fn print_result(arguments: Arguments, mut result: BenchmarkResult) {
    let mut before_puts = Vec::new();
    let mut before_gets = Vec::new();
    let mut pressure_puts = Vec::new();
    let mut pressure_put_successes = Vec::new();
    let mut pressure_put_failures = Vec::new();
    let mut pressure_gets = Vec::new();
    let mut put_successes = 0;
    let mut put_start_failures = 0;
    let mut put_end_failures = 0;
    let mut get_hits = 0;
    let mut get_failures = 0;
    for worker in result.workers {
        before_puts.extend(worker.before_put_latencies_ns);
        before_gets.extend(worker.before_get_latencies_ns);
        pressure_puts.extend(worker.pressure_put_latencies_ns);
        pressure_put_successes.extend(worker.pressure_put_success_latencies_ns);
        pressure_put_failures.extend(worker.pressure_put_failure_latencies_ns);
        pressure_gets.extend(worker.pressure_get_latencies_ns);
        put_successes += worker.put_successes;
        put_start_failures += worker.put_start_failures;
        put_end_failures += worker.put_end_failures;
        get_hits += worker.get_hits;
        get_failures += worker.get_failures;
    }
    before_puts.sort_unstable();
    before_gets.sort_unstable();
    pressure_puts.sort_unstable();
    pressure_put_successes.sort_unstable();
    pressure_put_failures.sort_unstable();
    pressure_gets.sort_unstable();
    result.collector.step_latencies_ns.sort_unstable();

    let expected_without_eviction = result.objects_before + put_successes;
    assert!(result.objects_after <= expected_without_eviction);
    let evicted_objects = expected_without_eviction - result.objects_after;
    let operations = arguments.threads * arguments.operations_per_thread;
    println!(
        "{},{:.6},{:.6},{:.6},{},{:.0},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        result.objects_before,
        result.initial_ratio,
        result.collector.maximum_used_ratio,
        result.final_ratio,
        operations,
        operations as f64 / result.elapsed.as_secs_f64(),
        put_successes,
        put_start_failures,
        put_end_failures,
        get_hits,
        get_failures,
        evicted_objects,
        result.collector.reclaim_events,
        result.collector.observed_drops,
        before_puts.len(),
        percentile_fraction(&before_puts, 99, 100),
        before_gets.len(),
        percentile_fraction(&before_gets, 99, 100),
        pressure_puts.len(),
        percentile_fraction(&pressure_puts, 99, 100),
        percentile_fraction(&pressure_puts, 999, 1000),
        pressure_puts.last().copied().unwrap_or(0),
        pressure_put_successes.len(),
        percentile_fraction(&pressure_put_successes, 99, 100),
        percentile_fraction(&pressure_put_successes, 999, 1000),
        pressure_put_successes.last().copied().unwrap_or(0),
        percentile_fraction(&pressure_put_failures, 99, 100),
        pressure_gets.len(),
        percentile_fraction(&pressure_gets, 99, 100),
        percentile_fraction(&pressure_gets, 999, 1000),
        pressure_gets.last().copied().unwrap_or(0),
        percentile_fraction(&result.collector.step_latencies_ns, 99, 100),
        percentile_fraction(&result.collector.step_latencies_ns, 999, 1000),
        result
            .collector
            .step_latencies_ns
            .last()
            .copied()
            .unwrap_or(0),
    );
    assert!(result.collector.reclaim_events != 0);
    assert!(evicted_objects != 0);
    assert!(!pressure_puts.is_empty());
    assert!(!pressure_gets.is_empty());
}

fn segment_size_for(arguments: Arguments) -> u64 {
    let needed = (arguments.num_objects as u64)
        .checked_mul(OBJECT_BYTES)
        .expect("prefill bytes overflowed u64");
    ((needed as f64) / arguments.initial_used_ratio).ceil() as u64
}

fn used_ratio(candidate: &SegmentCandidate) -> f64 {
    let space = candidate.stats().space;
    space.used_bytes as f64 / space.capacity_bytes as f64
}

fn percentile_fraction(sorted: &[u64], numerator: usize, denominator: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = sorted.len().saturating_mul(numerator).div_ceil(denominator);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn elapsed_nanos(begin: Instant) -> u64 {
    u64::try_from(begin.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn parse_arguments() -> Arguments {
    let mut result = Arguments {
        num_objects: 100_000,
        threads: 8,
        operations_per_thread: 50_000,
        hot_objects: 2_048,
        initial_used_ratio: 0.89,
        high_watermark_ratio: 0.90,
        eviction_ratio: 0.05,
        collector_interval_ms: 10,
        collect_budget: 64,
        slot_cleanup_budget: 16,
        settle_timeout_ms: 5_000,
        rounds: 1,
    };
    for argument in std::env::args().skip(1) {
        if argument == "--help" || argument == "-h" {
            println!(
                "Usage: object_catalog_watermark_benchmark [--num_objects=N] [--threads=N] [--operations_per_thread=N] [--hot_objects=N] [--initial_used_ratio=R] [--high_watermark_ratio=R] [--eviction_ratio=R] [--collector_interval_ms=N] [--collect_budget=N] [--slot_cleanup_budget=N] [--settle_timeout_ms=N] [--rounds=N]"
            );
            std::process::exit(0);
        }
        let (name, value) = argument
            .split_once('=')
            .unwrap_or_else(|| panic!("argument must use --name=value syntax: {argument}"));
        match name {
            "--num_objects" => result.num_objects = parse_value(name, value),
            "--threads" => result.threads = parse_value(name, value),
            "--operations_per_thread" => {
                result.operations_per_thread = parse_value(name, value);
            }
            "--hot_objects" => result.hot_objects = parse_value(name, value),
            "--initial_used_ratio" => result.initial_used_ratio = parse_value(name, value),
            "--high_watermark_ratio" => {
                result.high_watermark_ratio = parse_value(name, value);
            }
            "--eviction_ratio" => result.eviction_ratio = parse_value(name, value),
            "--collector_interval_ms" => {
                result.collector_interval_ms = parse_value(name, value);
            }
            "--collect_budget" => result.collect_budget = parse_value(name, value),
            "--slot_cleanup_budget" => {
                result.slot_cleanup_budget = parse_value(name, value);
            }
            "--settle_timeout_ms" => {
                result.settle_timeout_ms = parse_value(name, value);
            }
            "--rounds" => result.rounds = parse_value(name, value),
            _ => panic!("unknown argument: {name}"),
        }
    }
    result
}

fn validate_arguments(arguments: Arguments) {
    assert!(arguments.num_objects != 0, "num_objects must be non-zero");
    assert!(arguments.threads != 0, "threads must be non-zero");
    assert!(
        arguments.operations_per_thread >= 2,
        "operations_per_thread must be at least two"
    );
    assert!(
        arguments.hot_objects != 0 && arguments.hot_objects < arguments.num_objects,
        "hot_objects must be in 1..num_objects"
    );
    assert!(
        arguments.initial_used_ratio > 0.0
            && arguments.initial_used_ratio < arguments.high_watermark_ratio
            && arguments.high_watermark_ratio < 1.0,
        "require 0 < initial_used_ratio < high_watermark_ratio < 1"
    );
    assert!(
        arguments.eviction_ratio > 0.0 && arguments.eviction_ratio < 1.0,
        "eviction_ratio must be in (0, 1)"
    );
    assert!(
        arguments.collector_interval_ms != 0,
        "collector_interval_ms must be non-zero"
    );
    assert!(
        arguments.collect_budget != 0,
        "collect_budget must be non-zero"
    );
    assert!(arguments.rounds != 0, "rounds must be non-zero");
}

fn parse_value<T>(name: &str, value: &str) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .unwrap_or_else(|error| panic!("invalid value for {name}: {error}"))
}
