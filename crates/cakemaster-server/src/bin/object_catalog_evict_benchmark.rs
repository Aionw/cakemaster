//! Object catalog eviction benchmark.

use cakemaster::object::reclamation::{CatalogTick, CollectBudget};
use cakemaster::object::{
    DirectReplica, NamespaceId, ObjectCatalog, ObjectCatalogConfig, ObjectCommit, ObjectContent,
    ObjectIdentity, ReplicaId, ReplicaLease, ReplicaSet, WriteOwner,
};
use cakemaster::segment::{
    ClientId, MemoryRegion, SegmentId, SegmentIdentity, SegmentPool, SegmentPoolConfig,
    SegmentSpec, TransportEndpoint, TransportProtocol,
};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const OBJECT_BYTES: u64 = 1024;
const MIN_SEGMENT_BYTES: u64 = 16 * 1024 * 1024;
const SEGMENT_BASE: u64 = 0x3_0000_0000;
const OWNER: ClientId = ClientId::new(41, 73);

#[derive(Clone, Copy)]
struct Arguments {
    num_objects: usize,
    target_ratio: f64,
    lowerbound_ratio: f64,
    collect_budget: usize,
    slot_cleanup_budget: usize,
    rounds: usize,
    lookup_threads: usize,
    hot_objects: usize,
    warmup_ms: u64,
}

#[derive(Default)]
struct EvictResult {
    total: Duration,
    objects_before: usize,
    objects_after: usize,
    evicted_objects: usize,
    freed_bytes: u64,
    steps: usize,
    step_latencies_ns: Vec<u64>,
    removed_slots: usize,
    deferred_cleanup: Duration,
    lookup_latencies_ns: Vec<u64>,
    lookup_operations: usize,
    lookup_failures: usize,
}

#[derive(Default)]
struct LookupWorkerResult {
    latencies_ns: Vec<u64>,
    operations: usize,
    failures: usize,
}

fn main() {
    let arguments = parse_arguments();
    validate_arguments(arguments);

    println!(
        "num_objects,total_us,cleanup_us,full_work_us,objects_before,objects_after,evicted_count,freed_bytes,collect_budget,slot_cleanup_budget,steps,step_p50_ns,step_p99_ns,step_max_ns,removed_slots,lookup_threads,lookup_samples,lookup_p50_ns,lookup_p99_ns,lookup_max_ns,lookup_operations,lookup_failures"
    );
    for _ in 0..arguments.rounds {
        let mut result = run_once(arguments);
        result.step_latencies_ns.sort_unstable();
        result.lookup_latencies_ns.sort_unstable();
        println!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            arguments.num_objects,
            result.total.as_micros(),
            result.deferred_cleanup.as_micros(),
            (result.total + result.deferred_cleanup).as_micros(),
            result.objects_before,
            result.objects_after,
            result.evicted_objects,
            result.freed_bytes,
            arguments.collect_budget,
            arguments.slot_cleanup_budget,
            result.steps,
            percentile(&result.step_latencies_ns, 50),
            percentile(&result.step_latencies_ns, 99),
            result.step_latencies_ns.last().copied().unwrap_or(0),
            result.removed_slots,
            arguments.lookup_threads,
            result.lookup_latencies_ns.len(),
            percentile(&result.lookup_latencies_ns, 50),
            percentile(&result.lookup_latencies_ns, 99),
            result.lookup_latencies_ns.last().copied().unwrap_or(0),
            result.lookup_operations,
            result.lookup_failures,
        );
    }
}

fn run_once(arguments: Arguments) -> EvictResult {
    let target_objects = ((arguments.num_objects as f64) * arguments.target_ratio).ceil() as usize;
    let target_bytes = (target_objects as u64)
        .checked_mul(OBJECT_BYTES)
        .expect("reclaim byte target overflowed u64");
    let max_allocator_nodes = u32::try_from(arguments.num_objects.saturating_add(2))
        .expect("num_objects is too large for the offset allocator");
    let segment_size = segment_size_for(arguments.num_objects);

    let pool = SegmentPool::with_config(SegmentPoolConfig::new(max_allocator_nodes))
        .expect("allocator node limit must be valid");
    let candidate = pool
        .attach(SegmentSpec::memory(
            SegmentIdentity::new(SegmentId::new(1, 1), OWNER, "batch_evict_bench_segment"),
            MemoryRegion::new(SEGMENT_BASE, segment_size),
            TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
        ))
        .expect("benchmark segment must be valid")
        .direct_candidate()
        .expect("memory segment must be directly allocatable");
    let catalog = Arc::new(
        ObjectCatalog::with_config(
            ObjectCatalogConfig::new(arguments.num_objects)
                .with_lease(1, 1)
                .with_pending_timeout(u64::MAX)
                .with_empty_slot_grace(0)
                .with_max_retired_bytes(u64::MAX),
        )
        .expect("benchmark catalog configuration must be valid"),
    );
    let mut hot_keys = Vec::with_capacity(arguments.hot_objects);

    for index in 0..arguments.num_objects {
        let identity = ObjectIdentity::new(
            NamespaceId::DEFAULT,
            format!("batch_evict_bench_key_{index}"),
        );
        if index < arguments.hot_objects {
            hot_keys.push(identity.clone());
        }
        let reservation = pool
            .reserve(&candidate, OBJECT_BYTES)
            .expect("benchmark segment must have enough capacity");
        let ticket = catalog
            .claim_put(identity, WriteOwner::new(OWNER), CatalogTick::ZERO)
            .expect("benchmark keys are unique")
            .stage(
                ObjectContent::new(OBJECT_BYTES),
                ReplicaSet::one(ReplicaLease::Direct(DirectReplica::new(
                    ReplicaId::new(1),
                    reservation,
                ))),
            )
            .expect("benchmark replica is valid");
        drop(
            catalog
                .publish(&ticket, ObjectCommit::default())
                .expect("fresh benchmark objects can be published"),
        );
    }

    let objects_before = catalog.stats().published_objects;
    let used_before = candidate.stats().space.used_bytes;
    assert_eq!(objects_before, arguments.num_objects);

    if arguments.lookup_threads != 0 {
        for identity in &hot_keys {
            drop(
                catalog
                    .get(identity.as_lookup(), CatalogTick::new(1))
                    .expect("hot benchmark objects must exist"),
            );
        }
    }

    let mut result = EvictResult {
        objects_before,
        step_latencies_ns: Vec::with_capacity(target_objects.div_ceil(arguments.collect_budget)),
        ..EvictResult::default()
    };
    let budget = CollectBudget::new(
        arguments.collect_budget,
        arguments.collect_budget,
        arguments.slot_cleanup_budget,
    );
    if arguments.lookup_threads == 0 {
        run_collection(&catalog, target_bytes, budget, &mut result);
    } else {
        let hot_keys = Arc::<[ObjectIdentity]>::from(hot_keys);
        let evicting = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(arguments.lookup_threads + 1));
        let workers = thread::scope(|scope| {
            let mut handles = Vec::with_capacity(arguments.lookup_threads);
            for worker in 0..arguments.lookup_threads {
                let catalog = catalog.clone();
                let hot_keys = hot_keys.clone();
                let evicting = evicting.clone();
                let stop = stop.clone();
                let barrier = barrier.clone();
                handles.push(scope.spawn(move || {
                    run_lookup_worker(worker, catalog, hot_keys, evicting, stop, barrier)
                }));
            }
            barrier.wait();
            thread::sleep(Duration::from_millis(arguments.warmup_ms));
            evicting.store(true, Ordering::Release);
            run_collection(&catalog, target_bytes, budget, &mut result);
            evicting.store(false, Ordering::Release);
            stop.store(true, Ordering::Release);
            handles
                .into_iter()
                .map(|handle| handle.join().expect("lookup worker must not panic"))
                .collect::<Vec<_>>()
        });
        for worker in workers {
            result.lookup_latencies_ns.extend(worker.latencies_ns);
            result.lookup_operations += worker.operations;
            result.lookup_failures += worker.failures;
        }
    }

    if result.removed_slots < target_objects {
        let cleanup_started = Instant::now();
        let cleanup_budget = CollectBudget::new(0, 0, arguments.collect_budget);
        while result.removed_slots < target_objects {
            let report = catalog.collect_step(CatalogTick::new(1), cleanup_budget);
            assert!(!report.busy, "single collector cannot be busy");
            assert!(
                report.removed_empty_slots != 0,
                "deferred empty-slot cleanup made no progress"
            );
            result.removed_slots += report.removed_empty_slots;
        }
        result.deferred_cleanup = cleanup_started.elapsed();
    }

    let stats = catalog.stats();
    let used_after = candidate.stats().space.used_bytes;
    result.objects_after = stats.published_objects;
    assert_eq!(result.evicted_objects, target_objects);
    assert_eq!(result.objects_after, objects_before - target_objects);
    assert_eq!(result.removed_slots, target_objects);
    assert_eq!(result.freed_bytes, used_before - used_after);
    assert_eq!(stats.reclaim_debt, 0);
    assert_eq!(stats.retired_bytes, 0);
    result
}

fn run_collection(
    catalog: &ObjectCatalog,
    target_bytes: u64,
    budget: CollectBudget,
    result: &mut EvictResult,
) {
    let started = Instant::now();
    catalog.request_reclaim(target_bytes);
    while result.freed_bytes < target_bytes {
        let step_started = Instant::now();
        let report = catalog.collect_step(CatalogTick::new(1), budget);
        result.step_latencies_ns.push(elapsed_nanos(step_started));
        assert!(!report.busy, "single collector cannot be busy");
        result.steps += 1;
        result.evicted_objects += report.reclaimed_objects;
        result.freed_bytes = result
            .freed_bytes
            .checked_add(report.reclaimed_bytes)
            .expect("freed byte counter overflowed u64");
        result.removed_slots += report.removed_empty_slots;
    }
    result.total = started.elapsed();
}

fn run_lookup_worker(
    worker: usize,
    catalog: Arc<ObjectCatalog>,
    hot_keys: Arc<[ObjectIdentity]>,
    evicting: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) -> LookupWorkerResult {
    let mut result = LookupWorkerResult::default();
    let mut random = (worker as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    barrier.wait();
    while !stop.load(Ordering::Acquire) {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let sample = evicting.load(Ordering::Acquire);
        let started = Instant::now();
        match catalog.get(
            hot_keys[random as usize % hot_keys.len()].as_lookup(),
            CatalogTick::new(1),
        ) {
            Ok(read) => {
                black_box(read.object().replicas());
            }
            Err(_) => result.failures += 1,
        }
        let elapsed = elapsed_nanos(started);
        result.operations += 1;
        if sample {
            result.latencies_ns.push(elapsed);
        }
    }
    result
}

fn segment_size_for(num_objects: usize) -> u64 {
    let needed = (num_objects as u64)
        .checked_mul(OBJECT_BYTES)
        .expect("segment size overflowed u64");
    let headroom = needed / 8 + 1024 * OBJECT_BYTES;
    MIN_SEGMENT_BYTES.max(
        needed
            .checked_add(headroom)
            .expect("segment headroom overflowed u64"),
    )
}

fn parse_arguments() -> Arguments {
    let mut result = Arguments {
        num_objects: 100_000,
        target_ratio: 0.50,
        lowerbound_ratio: 0.25,
        collect_budget: 64,
        slot_cleanup_budget: 64,
        rounds: 1,
        lookup_threads: 0,
        hot_objects: 2048,
        warmup_ms: 10,
    };
    for argument in std::env::args().skip(1) {
        if argument == "--help" || argument == "-h" {
            println!(
                "Usage: object_catalog_evict_benchmark [--num_objects=N] [--evict_ratio_target=R] [--evict_ratio_lowerbound=R] [--collect_budget=N] [--slot_cleanup_budget=N] [--rounds=N] [--lookup_threads=N] [--hot_objects=N] [--warmup_ms=N]"
            );
            std::process::exit(0);
        }
        let (name, value) = argument
            .split_once('=')
            .unwrap_or_else(|| panic!("argument must use --name=value syntax: {argument}"));
        match name {
            "--num_objects" => result.num_objects = parse_value(name, value),
            "--evict_ratio_target" => result.target_ratio = parse_value(name, value),
            "--evict_ratio_lowerbound" => {
                result.lowerbound_ratio = parse_value(name, value);
            }
            "--collect_budget" => result.collect_budget = parse_value(name, value),
            "--slot_cleanup_budget" => {
                result.slot_cleanup_budget = parse_value(name, value);
            }
            "--rounds" => result.rounds = parse_value(name, value),
            "--lookup_threads" => result.lookup_threads = parse_value(name, value),
            "--hot_objects" => result.hot_objects = parse_value(name, value),
            "--warmup_ms" => result.warmup_ms = parse_value(name, value),
            _ => panic!("unknown argument: {name}"),
        }
    }
    result
}

fn validate_arguments(arguments: Arguments) {
    assert!(arguments.num_objects != 0, "num_objects must be non-zero");
    assert!(
        arguments.collect_budget != 0,
        "collect_budget must be non-zero"
    );
    assert!(arguments.rounds != 0, "rounds must be non-zero");
    assert!(arguments.hot_objects != 0, "hot_objects must be non-zero");
    assert!(
        arguments.hot_objects < arguments.num_objects,
        "hot_objects must be smaller than num_objects"
    );
    assert!(
        arguments.lowerbound_ratio > 0.0
            && arguments.lowerbound_ratio <= arguments.target_ratio
            && arguments.target_ratio <= 1.0,
        "require 0 < evict_ratio_lowerbound <= evict_ratio_target <= 1"
    );
    assert!(
        arguments.num_objects <= (u32::MAX as usize).saturating_sub(4),
        "num_objects exceeds allocator node addressing"
    );
    let target_objects = ((arguments.num_objects as f64) * arguments.target_ratio).ceil() as usize;
    assert!(
        arguments.lookup_threads == 0
            || target_objects <= arguments.num_objects - arguments.hot_objects,
        "there must be enough cold objects to meet the reclaim target"
    );
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
