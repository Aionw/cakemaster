use cakemaster::segment::{
    ClientId, DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT, MemoryRegion, MemorySegmentSpec, SegmentId,
    SegmentIdentity, SegmentPool, SegmentPoolConfig, SegmentTopology, TransportEndpoint,
    TransportProtocol,
};
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const SEGMENT_CAPACITY: u64 = 1_u64 << 30;
const ALLOCATION_SIZE: u64 = 4096;
const PRESSURE_SIZE: u64 = SEGMENT_CAPACITY / 16 * 15;
const FAILED_ALLOCATION_SIZE: u64 = 128_u64 << 20;

#[derive(Clone, Copy)]
enum Scenario {
    Sharded,
    Striped,
    Contended,
    CapacityFailed,
}

impl Scenario {
    const ALL: [Self; 3] = [Self::Sharded, Self::Striped, Self::Contended];

    const fn name(self) -> &'static str {
        match self {
            Self::Sharded => "sharded",
            Self::Striped => "striped",
            Self::Contended => "contended",
            Self::CapacityFailed => "capacity_failed",
        }
    }

    fn candidate_index(self, worker: usize, operation: usize, segments: usize) -> usize {
        match self {
            Self::Sharded => worker % segments,
            Self::Striped => (worker + operation) % segments,
            Self::Contended | Self::CapacityFailed => 0,
        }
    }
}

#[derive(Clone, Copy)]
enum ExpectedResult {
    Success,
    Failure,
}

fn main() {
    let available_threads = thread::available_parallelism().map_or(4, usize::from);
    let threads = argument(1).unwrap_or(available_threads.clamp(1, 16));
    let operations_per_thread = argument(2).unwrap_or(250_000);
    let segments = argument(3).unwrap_or(threads.clamp(1, 16));
    let rounds = argument(4).unwrap_or(3);
    let allocator_nodes = argument(5).unwrap_or(DEFAULT_MAX_ALLOCATOR_NODES_PER_SEGMENT as usize);
    assert!(threads != 0, "threads must be non-zero");
    assert!(
        operations_per_thread != 0,
        "operations per thread must be non-zero"
    );
    assert!(segments != 0, "segments must be non-zero");
    assert!(rounds != 0, "rounds must be non-zero");
    let allocator_nodes = u32::try_from(allocator_nodes).expect("allocator nodes must fit in u32");

    let pool = Arc::new(
        SegmentPool::with_config(SegmentPoolConfig::new(allocator_nodes))
            .expect("benchmark pool configuration is valid"),
    );
    let owner = ClientId::new(1, 1);
    for index in 0..segments {
        let index = index as u64;
        pool.attach(
            MemorySegmentSpec::new(
                SegmentIdentity::new(
                    SegmentId::new(1, index + 1),
                    owner,
                    format!("memory-{index}"),
                ),
                MemoryRegion::new(
                    0x1_0000_0000 + index * (SEGMENT_CAPACITY * 2),
                    SEGMENT_CAPACITY,
                ),
                TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
            )
            .with_topology(SegmentTopology::on_host(format!("host-{index}"))),
        )
        .expect("benchmark segments must be valid");
    }
    let snapshot = pool.snapshot();

    println!("SegmentPool reserve/drop benchmark");
    println!(
        "threads={threads} segments={segments} ops/thread={operations_per_thread} allocation={ALLOCATION_SIZE}B rounds={rounds} allocator_nodes={allocator_nodes}"
    );
    println!("scenario       median ops/s       median ns/op");

    let warmup_operations = operations_per_thread.min(10_000);
    for scenario in Scenario::ALL {
        let _ = run_once(
            pool.clone(),
            snapshot.clone(),
            threads,
            warmup_operations,
            scenario,
            ALLOCATION_SIZE,
            ExpectedResult::Success,
        );

        let mut samples: Vec<_> = (0..rounds)
            .map(|_| {
                run_once(
                    pool.clone(),
                    snapshot.clone(),
                    threads,
                    operations_per_thread,
                    scenario,
                    ALLOCATION_SIZE,
                    ExpectedResult::Success,
                )
            })
            .collect();
        samples.sort_unstable();
        let elapsed = samples[samples.len() / 2];
        let operations = threads * operations_per_thread;
        let operations_per_second = operations as f64 / elapsed.as_secs_f64();
        let nanoseconds_per_operation = elapsed.as_nanos() as f64 / operations as f64;
        println!(
            "{:<12} {:>16.0} {:>18.1}",
            scenario.name(),
            operations_per_second,
            nanoseconds_per_operation
        );
    }

    let pressure: Vec<_> = snapshot
        .iter()
        .map(|candidate| {
            pool.reserve(candidate, PRESSURE_SIZE)
                .expect("capacity-pressure setup must fit")
        })
        .collect();
    let _ = run_once(
        pool.clone(),
        snapshot.clone(),
        threads,
        warmup_operations,
        Scenario::CapacityFailed,
        FAILED_ALLOCATION_SIZE,
        ExpectedResult::Failure,
    );
    let mut samples: Vec<_> = (0..rounds)
        .map(|_| {
            run_once(
                pool.clone(),
                snapshot.clone(),
                threads,
                operations_per_thread,
                Scenario::CapacityFailed,
                FAILED_ALLOCATION_SIZE,
                ExpectedResult::Failure,
            )
        })
        .collect();
    samples.sort_unstable();
    let elapsed = samples[samples.len() / 2];
    let operations = threads * operations_per_thread;
    println!(
        "{:<12} {:>16.0} {:>18.1}",
        Scenario::CapacityFailed.name(),
        operations as f64 / elapsed.as_secs_f64(),
        elapsed.as_nanos() as f64 / operations as f64
    );
    drop(pressure);

    for candidate in snapshot.iter() {
        let stats = candidate.stats();
        assert_eq!(stats.space.used_bytes, 0);
        assert_eq!(stats.reservations.live, 0);
        assert_eq!(stats.space.available_bytes, stats.space.capacity_bytes);
    }
}

fn run_once(
    pool: Arc<SegmentPool>,
    snapshot: cakemaster::segment::PoolSnapshot,
    threads: usize,
    operations_per_thread: usize,
    scenario: Scenario,
    allocation_size: u64,
    expected_result: ExpectedResult,
) -> Duration {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut workers = Vec::with_capacity(threads);
    for worker in 0..threads {
        let pool = pool.clone();
        let snapshot = snapshot.clone();
        let barrier = barrier.clone();
        workers.push(thread::spawn(move || {
            barrier.wait();
            for operation in 0..operations_per_thread {
                let index = scenario.candidate_index(worker, operation, snapshot.len());
                let result = pool.reserve(&snapshot.candidates()[index], allocation_size);
                match (expected_result, result) {
                    (ExpectedResult::Success, Ok(reservation)) => {
                        black_box(reservation.descriptor().region().base());
                        drop(reservation);
                    }
                    (ExpectedResult::Failure, Err(error)) => {
                        black_box(error);
                    }
                    (ExpectedResult::Success, Err(error)) => {
                        panic!("expected successful reservation, got {error}")
                    }
                    (ExpectedResult::Failure, Ok(_)) => {
                        panic!("expected capacity failure, reservation succeeded")
                    }
                }
            }
        }));
    }

    barrier.wait();
    let started = Instant::now();
    for worker in workers {
        worker.join().expect("benchmark worker must not panic");
    }
    started.elapsed()
}

fn argument(index: usize) -> Option<usize> {
    std::env::args().nth(index).map(|value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("argument {index} must be a positive integer"))
    })
}
