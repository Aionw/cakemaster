//! Direct-path 50:50 put/get benchmark with a concurrent client-exit storm.

use cakemaster::client::{CleanupReason, ClientLifecycleConfig, ClientManager, ClientTick};
use cakemaster::object::reclamation::{CatalogTick, CollectBudget};
use cakemaster::object::{
    DirectReplica, NamespaceId, ObjectCatalogConfig, ObjectCommit, ObjectContent, ObjectIdentity,
    ObjectManager, ReplicaId, ReplicaLease, ReplicaSet, WriteAdmission,
};
use cakemaster::segment::{
    ClientId, CxlArenaId, CxlArenaSpec, SegmentId, SegmentIdentity, SegmentPool, SegmentPoolConfig,
    SegmentSpec,
};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const OBJECT_BYTES: u64 = 4096;
const MIN_ARENA_BYTES: u64 = 1 << 30;
const HEALTHY_CLIENT: ClientId = ClientId::new(1, 1);
const COLLECT_BUDGET: usize = 256;

#[derive(Clone, Copy)]
struct Arguments {
    workers: usize,
    operations_per_worker: usize,
    exiting_clients: usize,
    objects_per_exiting_client: usize,
    pending_writes_per_exiting_client: usize,
    hot_objects: usize,
    rounds: usize,
}

#[derive(Default)]
struct WorkerResult {
    put: Vec<u64>,
    get: Vec<u64>,
    get_hits: usize,
    errors: usize,
}

struct ResultRow {
    elapsed: Duration,
    workers: Vec<WorkerResult>,
    cleanup_ns: u64,
    cleanup_segments: usize,
    revoked_pending: usize,
    invalidated_pending: usize,
    invalidated_published: usize,
    settle_ns: u64,
    remaining_segments: usize,
    initial_segments: usize,
}

fn main() {
    let defaults = Arguments {
        workers: thread::available_parallelism()
            .map_or(4, usize::from)
            .clamp(1, 8),
        operations_per_worker: 50_000,
        exiting_clients: 1_000,
        objects_per_exiting_client: 4,
        pending_writes_per_exiting_client: 0,
        hot_objects: 2048,
        rounds: 5,
    };
    let arguments = Arguments {
        workers: argument("workers").unwrap_or(defaults.workers),
        operations_per_worker: argument("operations").unwrap_or(defaults.operations_per_worker),
        exiting_clients: argument("clients").unwrap_or(defaults.exiting_clients),
        objects_per_exiting_client: argument("objects-per-client")
            .unwrap_or(defaults.objects_per_exiting_client),
        pending_writes_per_exiting_client: argument("pending-per-client")
            .unwrap_or(defaults.pending_writes_per_exiting_client),
        hot_objects: argument("hot-objects").unwrap_or(defaults.hot_objects),
        rounds: argument("rounds").unwrap_or(defaults.rounds),
    };
    assert!(arguments.workers > 0);
    assert!(arguments.exiting_clients > 0 && arguments.rounds > 0);
    assert!(arguments.operations_per_worker == 0 || arguments.hot_objects > 0);

    println!(
        "scenario,clients,segments,published_objects,pending_writes,ops_per_sec,put_p50_ns,put_p99_ns,put_p999_ns,get_p50_ns,get_p99_ns,get_p999_ns,get_hit_ratio,errors,cleanup_us,settle_us,invalidated_segments,revoked_pending,invalidated_pending,invalidated_published,remaining_segments"
    );
    for storm in [false, true] {
        let mut rows = (0..arguments.rounds)
            .map(|round| run_once(arguments, storm, round))
            .collect::<Vec<_>>();
        rows.sort_unstable_by_key(|row| row.elapsed);
        print_row(
            if storm { "exit-storm" } else { "baseline" },
            arguments,
            rows.swap_remove(rows.len() / 2),
        );
    }
}

fn run_once(arguments: Arguments, storm: bool, round: usize) -> ResultRow {
    let max_allocations = u32::try_from(
        arguments
            .workers
            .saturating_mul(arguments.operations_per_worker.div_ceil(2))
            .saturating_add(arguments.hot_objects)
            .saturating_add(
                arguments.exiting_clients.saturating_mul(
                    arguments
                        .objects_per_exiting_client
                        .saturating_add(arguments.pending_writes_per_exiting_client),
                ),
            )
            .saturating_add(1024)
            .saturating_mul(2),
    )
    .unwrap_or(u32::MAX - 2)
    .clamp(3, u32::MAX - 2);
    let pool = Arc::new(SegmentPool::with_config(SegmentPoolConfig::new(max_allocations)).unwrap());
    let expected = arguments
        .hot_objects
        .saturating_add(
            arguments.exiting_clients.saturating_mul(
                arguments
                    .objects_per_exiting_client
                    .saturating_add(arguments.pending_writes_per_exiting_client),
            ),
        )
        .saturating_add(
            arguments
                .workers
                .saturating_mul(arguments.operations_per_worker.div_ceil(2)),
        )
        .saturating_add(1024);
    let manager = Arc::new(
        ObjectManager::with_config(
            pool.clone(),
            ObjectCatalogConfig::new(expected)
                .with_pending_timeout(u64::MAX / 2)
                .with_max_retired_bytes(u64::MAX),
        )
        .unwrap(),
    );
    let clients = ClientManager::with_config(
        pool.clone(),
        manager.pending_write_revoker(),
        ClientLifecycleConfig::new(arguments.exiting_clients.saturating_add(1))
            .with_cleanup_scan_budget(arguments.exiting_clients.max(1)),
    )
    .unwrap();
    let arena_bytes = u64::from(max_allocations)
        .saturating_mul(OBJECT_BYTES)
        .max(MIN_ARENA_BYTES);
    let cleanup_only = arguments.operations_per_worker == 0
        && arguments.objects_per_exiting_client == 0
        && arguments.pending_writes_per_exiting_client == 0
        && arguments.hot_objects == 0;
    let mut exiting_sessions = Vec::with_capacity(arguments.exiting_clients);
    clients
        .remount(
            HEALTHY_CLIENT,
            vec![segment(0, HEALTHY_CLIENT, arena_bytes)],
            ClientTick::ZERO,
        )
        .unwrap();
    for client in 0..arguments.exiting_clients {
        let segments = if cleanup_only {
            Vec::new()
        } else {
            vec![segment(client + 1, exiting_client(client), arena_bytes)]
        };
        let session = clients
            .remount(exiting_client(client), segments, ClientTick::ZERO)
            .unwrap()
            .session();
        exiting_sessions.push(session);
    }
    let healthy_candidate = pool
        .segment(SegmentId::new(1, 1))
        .unwrap()
        .direct_candidate()
        .unwrap();
    let hot = Arc::<[ObjectIdentity]>::from(preload_healthy(&manager, arguments.hot_objects));
    preload_exiting(&manager, arguments, round);
    preload_exiting_pending(&manager, &clients, arguments, round);
    let healthy_admission = clients.write_admission(HEALTHY_CLIENT).unwrap();

    let tick = Arc::new(AtomicU64::new(1));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(arguments.workers + 2));
    let exiting_sessions: Arc<[_]> = exiting_sessions.into();
    let (elapsed, workers, cleanup_ns, cleanup_segments, revoked_pending, during_cleanup) =
        thread::scope(|scope| {
            let mut worker_handles = Vec::with_capacity(arguments.workers);
            for worker in 0..arguments.workers {
                let manager = manager.clone();
                let healthy_candidate = healthy_candidate.clone();
                let hot = hot.clone();
                let tick = tick.clone();
                let barrier = barrier.clone();
                let admission = healthy_admission.clone();
                worker_handles.push(scope.spawn(move || {
                    run_worker(
                        manager,
                        healthy_candidate,
                        hot,
                        tick,
                        barrier,
                        arguments,
                        worker,
                        round,
                        admission,
                    )
                }));
            }
            let cleanup_clients = clients.clone();
            let cleanup_tick = tick.clone();
            let cleanup_barrier = barrier.clone();
            let cleanup = scope.spawn(move || {
                cleanup_barrier.wait();
                if !storm {
                    return (0, 0, 0);
                }
                thread::yield_now();
                let started = Instant::now();
                let report = cleanup_clients
                    .drain_sessions(
                        exiting_sessions.iter().copied(),
                        CleanupReason::GracefulUnmount,
                        CatalogTick::new(cleanup_tick.load(Ordering::Relaxed)),
                    )
                    .unwrap();
                (
                    nanos(started),
                    report.invalidated_segments,
                    report.revoked_pending_writes,
                )
            });
            let collector_manager = manager.clone();
            let collector_tick = tick.clone();
            let collector_stop = stop.clone();
            let collector_barrier = barrier.clone();
            let collector = scope.spawn(move || {
                collector_barrier.wait();
                let mut invalidated_pending = 0;
                let mut invalidated_published = 0;
                while !collector_stop.load(Ordering::Acquire) {
                    let report = collector_manager.maintenance(
                        CatalogTick::new(collector_tick.load(Ordering::Relaxed)),
                        CollectBudget::new(COLLECT_BUDGET, COLLECT_BUDGET, COLLECT_BUDGET / 4),
                    );
                    invalidated_pending += report.catalog.invalidated_pending;
                    invalidated_published += report.catalog.invalidated_published;
                    if report.catalog.invalidated_pending == 0
                        && report.catalog.invalidated_published == 0
                        && report.catalog.reclaimed_objects == 0
                    {
                        thread::sleep(Duration::from_micros(50));
                    } else {
                        thread::yield_now();
                    }
                }
                (invalidated_pending, invalidated_published)
            });

            let started = Instant::now();
            let workers = worker_handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>();
            let elapsed = started.elapsed();
            let (cleanup_ns, cleanup_segments, revoked_pending) = cleanup.join().unwrap();
            stop.store(true, Ordering::Release);
            let invalidated = collector.join().unwrap();
            (
                elapsed,
                workers,
                cleanup_ns,
                cleanup_segments,
                revoked_pending,
                invalidated,
            )
        });

    let settle_started = Instant::now();
    let mut invalidated_pending = during_cleanup.0;
    let mut invalidated_published = during_cleanup.1;
    loop {
        let report = manager.maintenance(
            CatalogTick::new(tick.fetch_add(1, Ordering::Relaxed)),
            CollectBudget::new(COLLECT_BUDGET, COLLECT_BUDGET, COLLECT_BUDGET / 4),
        );
        invalidated_pending += report.catalog.invalidated_pending;
        invalidated_published += report.catalog.invalidated_published;
        let stats = manager.catalog().stats();
        if stats.liveness_scan_remaining == 0 && stats.retired_candidates == 0 {
            break;
        }
    }
    ResultRow {
        elapsed,
        workers,
        cleanup_ns,
        cleanup_segments,
        revoked_pending,
        invalidated_pending,
        invalidated_published,
        settle_ns: nanos(settle_started),
        remaining_segments: pool.len(),
        initial_segments: if cleanup_only {
            1
        } else {
            arguments.exiting_clients + 1
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn run_worker(
    manager: Arc<ObjectManager>,
    healthy_candidate: cakemaster::segment::DirectCandidate,
    hot: Arc<[ObjectIdentity]>,
    tick: Arc<AtomicU64>,
    barrier: Arc<Barrier>,
    arguments: Arguments,
    worker: usize,
    round: usize,
    admission: WriteAdmission,
) -> WorkerResult {
    let mut result = WorkerResult::default();
    let mut random = (worker as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    barrier.wait();
    for operation in 0..arguments.operations_per_worker {
        let now = CatalogTick::new(tick.fetch_add(1, Ordering::Relaxed));
        if operation & 1 == 0 {
            let identity = ObjectIdentity::new(
                NamespaceId::DEFAULT,
                format!("put-{round:04x}-{worker:04x}-{operation:016x}"),
            );
            let started = Instant::now();
            let write = (|| {
                let claim = manager
                    .catalog()
                    .claim_put(identity, admission.clone(), now)
                    .ok()?;
                let reservation = manager
                    .pool()
                    .reserve(&healthy_candidate, OBJECT_BYTES)
                    .ok()?;
                let ticket = claim
                    .stage(
                        ObjectContent::new(OBJECT_BYTES),
                        ReplicaSet::one(ReplicaLease::Direct(DirectReplica::new(
                            ReplicaId::new(1),
                            reservation,
                        ))),
                    )
                    .ok()?;
                manager
                    .catalog()
                    .publish(&ticket, ObjectCommit::default())
                    .ok()?;
                Some(())
            })();
            if write.is_none() {
                result.errors += 1;
            }
            result.put.push(nanos(started));
        } else {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let key = &hot[random as usize % hot.len()];
            let started = Instant::now();
            match manager.get(key.as_lookup(), now) {
                Ok(read) => {
                    result.get_hits += 1;
                    black_box(read.object().replicas());
                }
                Err(_) => result.errors += 1,
            }
            result.get.push(nanos(started));
        }
    }
    result
}

fn preload_healthy(manager: &ObjectManager, count: usize) -> Vec<ObjectIdentity> {
    (0..count)
        .map(|index| {
            let identity = ObjectIdentity::new(NamespaceId::DEFAULT, format!("hot-{index:08x}"));
            publish_on(
                manager,
                identity.clone(),
                HEALTHY_CLIENT,
                SegmentId::new(1, 1),
            );
            identity
        })
        .collect()
}

fn preload_exiting(manager: &ObjectManager, arguments: Arguments, round: usize) {
    for client in 0..arguments.exiting_clients {
        let owner = exiting_client(client);
        for object in 0..arguments.objects_per_exiting_client {
            let identity = ObjectIdentity::new(
                NamespaceId::DEFAULT,
                format!("exit-{round:04x}-{client:08x}-{object:04x}"),
            );
            publish_on(
                manager,
                identity,
                owner,
                SegmentId::new(1, client as u64 + 2),
            );
        }
    }
}

fn preload_exiting_pending(
    manager: &ObjectManager,
    clients: &ClientManager,
    arguments: Arguments,
    round: usize,
) {
    for client in 0..arguments.exiting_clients {
        let owner = exiting_client(client);
        let write_admission = clients.write_admission(owner).unwrap();
        for write in 0..arguments.pending_writes_per_exiting_client {
            let identity = ObjectIdentity::new(
                NamespaceId::DEFAULT,
                format!("exit-pending-{round:04x}-{client:08x}-{write:04x}"),
            );
            let reservation = manager
                .pool()
                .reserve_on(SegmentId::new(1, client as u64 + 2), OBJECT_BYTES)
                .unwrap();
            manager
                .catalog()
                .claim_put(identity, write_admission.clone(), CatalogTick::ZERO)
                .unwrap()
                .stage(
                    ObjectContent::new(OBJECT_BYTES),
                    ReplicaSet::one(ReplicaLease::Direct(DirectReplica::new(
                        ReplicaId::new(1),
                        reservation,
                    ))),
                )
                .unwrap();
        }
    }
}

fn publish_on(
    manager: &ObjectManager,
    identity: ObjectIdentity,
    owner: ClientId,
    segment: SegmentId,
) {
    let reservation = manager.pool().reserve_on(segment, OBJECT_BYTES).unwrap();
    let ticket = manager
        .catalog()
        .claim_put(
            identity,
            WriteAdmission::unmanaged(owner),
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
    drop(
        manager
            .catalog()
            .publish(&ticket, ObjectCommit::default())
            .unwrap(),
    );
}

fn segment(index: usize, owner: ClientId, arena_bytes: u64) -> SegmentSpec {
    let index = index as u64;
    SegmentSpec::cxl(
        SegmentIdentity::new(
            SegmentId::new(1, index + 1),
            owner,
            format!("client-{index}"),
        ),
        CxlArenaSpec::new(CxlArenaId::new("client-cleanup-benchmark"), arena_bytes),
    )
}

fn exiting_client(index: usize) -> ClientId {
    ClientId::new(2, index as u64 + 1)
}

fn print_row(name: &str, arguments: Arguments, mut row: ResultRow) {
    let mut put = Vec::new();
    let mut get = Vec::new();
    let mut hits = 0;
    let mut errors = 0;
    for worker in row.workers.drain(..) {
        put.extend(worker.put);
        get.extend(worker.get);
        hits += worker.get_hits;
        errors += worker.errors;
    }
    put.sort_unstable();
    get.sort_unstable();
    let operations = put.len() + get.len();
    println!(
        "{name},{},{},{},{},{:.0},{},{},{},{},{},{},{:.6},{},{:.3},{:.3},{},{},{},{},{}",
        arguments.exiting_clients,
        row.initial_segments,
        arguments.exiting_clients * arguments.objects_per_exiting_client,
        arguments.exiting_clients * arguments.pending_writes_per_exiting_client,
        operations as f64 / row.elapsed.as_secs_f64(),
        percentile(&put, 500, 1000),
        percentile(&put, 990, 1000),
        percentile(&put, 999, 1000),
        percentile(&get, 500, 1000),
        percentile(&get, 990, 1000),
        percentile(&get, 999, 1000),
        hits as f64 / get.len().max(1) as f64,
        errors,
        row.cleanup_ns as f64 / 1000.0,
        row.settle_ns as f64 / 1000.0,
        row.cleanup_segments,
        row.revoked_pending,
        row.invalidated_pending,
        row.invalidated_published,
        row.remaining_segments,
    );
}

fn percentile(sorted: &[u64], numerator: usize, denominator: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = sorted.len().saturating_mul(numerator).div_ceil(denominator);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn argument(name: &str) -> Option<usize> {
    let prefix = format!("--{name}=");
    std::env::args()
        .skip(1)
        .find_map(|value| value.strip_prefix(&prefix).map(str::to_owned))
        .map(|value| value.parse().unwrap_or_else(|_| panic!("invalid --{name}")))
}
