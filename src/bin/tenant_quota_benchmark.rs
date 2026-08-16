//! Reproducible direct-path benchmark for optional tenant isolation and quota.

use cakemaster::object::reclamation::CatalogTick;
use cakemaster::object::{
    NamespaceId, ObjectCatalogConfig, ObjectContent, ObjectIdentity, ObjectKind, ObjectManager,
    ObjectPutPlan, ReplicaSelector, ResolvedTenant, TenantConfig, TenantId, TenantObjectManager,
    TenantPolicy, TenantPutRequest, TenantQuotaLimits, WriteAdmission, WriteOwner,
};
use cakemaster::segment::placement::{
    AllocationSpec, FulfillmentPolicy, PlacementRequest, ReplicaPolicy,
};
use cakemaster::segment::{
    ClientId, MemoryRegion, ReplicaClass, SegmentId, SegmentIdentity, SegmentPool,
    SegmentPoolConfig, SegmentSpec, TransportEndpoint, TransportProtocol,
};
use std::error::Error;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

const OBJECT_BYTES: u64 = 1024;
const SEGMENT_BYTES: u64 = 8_u64 << 30;
const HOT_KEYS_PER_TENANT: usize = 2048;
const OWNER: ClientId = ClientId::new(71, 91);
const BATCH_SIZES: [usize; 2] = [1, 333];

#[derive(Clone, Copy)]
enum Operation {
    Put,
    Get,
    Exists,
}

impl Operation {
    const fn name(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::Get => "get",
            Self::Exists => "exists",
        }
    }
}

struct Sample {
    elapsed: Duration,
    items: usize,
    batch_latencies_ns: Vec<u64>,
}

struct PutBreakdownSample {
    elapsed: Duration,
    start_elapsed: Duration,
    finish_elapsed: Duration,
    items: usize,
}

impl Sample {
    fn items_per_second(&self) -> f64 {
        self.items as f64 / self.elapsed.as_secs_f64()
    }

    fn p99_us(&mut self) -> f64 {
        self.batch_latencies_ns.sort_unstable();
        let index = self
            .batch_latencies_ns
            .len()
            .saturating_sub(1)
            .saturating_mul(99)
            / 100;
        self.batch_latencies_ns[index] as f64 / 1000.0
    }
}

struct PutBatch {
    tenant: usize,
    keys: Vec<Arc<str>>,
    requests: Vec<TenantPutRequest>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args().skip(1);
    let put_items = parse_or(arguments.next(), 50_000_usize)?;
    let lookup_items = parse_or(arguments.next(), 200_000_usize)?;
    let rounds = parse_or(arguments.next(), 3_usize)?;
    let mode = arguments.next();
    if put_items == 0 || lookup_items == 0 || rounds == 0 {
        return Err("put_items, lookup_items, and rounds must be positive".into());
    }
    if mode.as_deref() == Some("breakdown") {
        print_put_breakdown(put_items, rounds);
        return Ok(());
    }
    if let Some(mode) = mode {
        return Err(format!("unknown benchmark mode `{mode}`").into());
    }

    println!("Tenant quota direct-path benchmark");
    println!(
        "put_items={put_items} lookup_items={lookup_items} rounds={rounds} object={OBJECT_BYTES}B"
    );
    println!("operation batch mode          Mitems/s   delta-vs-single   batch-p99-us   p99-delta");

    for batch_size in BATCH_SIZES {
        for operation in [Operation::Get, Operation::Exists] {
            let mut baseline = median_sample(
                (0..rounds)
                    .map(|_| benchmark_raw_lookup(operation, batch_size, lookup_items))
                    .collect(),
            );
            let baseline_throughput = baseline.items_per_second();
            let baseline_p99 = baseline.p99_us();
            print_sample(
                operation,
                batch_size,
                "single",
                &baseline,
                baseline_throughput,
                baseline_p99,
            );
            for tenants in [1, 4] {
                let sample = median_sample(
                    (0..rounds)
                        .map(|_| {
                            benchmark_multi_lookup(operation, batch_size, lookup_items, tenants)
                        })
                        .collect(),
                );
                print_sample(
                    operation,
                    batch_size,
                    if tenants == 1 { "multi-1" } else { "multi-4" },
                    &sample,
                    baseline_throughput,
                    baseline_p99,
                );
            }
        }

        print_put_samples(batch_size, put_items, rounds);
    }
    Ok(())
}

fn print_put_samples(batch_size: usize, items: usize, rounds: usize) {
    let mut raw_samples = Vec::with_capacity(rounds);
    let mut facade_samples = Vec::with_capacity(rounds);
    let mut multi_one_samples = Vec::with_capacity(rounds);
    let mut multi_four_samples = Vec::with_capacity(rounds);
    for round in 0..rounds {
        // Rotate execution order so frequency and thermal drift are spread
        // across modes instead of always being charged to the same mode.
        for offset in 0..4 {
            match (round + offset) % 4 {
                0 => raw_samples.push(benchmark_raw_put(batch_size, items, round)),
                1 => facade_samples.push(benchmark_single_facade_put(batch_size, items, round)),
                2 => multi_one_samples.push(benchmark_multi_put(batch_size, items, 1, round)),
                3 => multi_four_samples.push(benchmark_multi_put(batch_size, items, 4, round)),
                _ => unreachable!("put benchmark mode is modulo four"),
            }
        }
    }

    let mut baseline = median_sample(raw_samples);
    let facade = median_sample(facade_samples);
    let multi_one = median_sample(multi_one_samples);
    let multi_four = median_sample(multi_four_samples);
    let baseline_throughput = baseline.items_per_second();
    let baseline_p99 = baseline.p99_us();
    for (mode, sample) in [
        ("single", &baseline),
        ("facade", &facade),
        ("multi-1", &multi_one),
        ("multi-4", &multi_four),
    ] {
        print_sample(
            Operation::Put,
            batch_size,
            mode,
            sample,
            baseline_throughput,
            baseline_p99,
        );
    }
}

fn benchmark_raw_lookup(operation: Operation, batch_size: usize, items: usize) -> Sample {
    let manager = raw_manager(HOT_KEYS_PER_TENANT + 1024);
    let keys = preload_raw(&manager);
    let lookups: Vec<_> = (0..items)
        .map(|index| keys[index % keys.len()].as_lookup())
        .collect();
    let batches = items.div_ceil(batch_size);
    let mut latencies = Vec::with_capacity(batches);
    let started = Instant::now();
    for lookups in lookups.chunks(batch_size) {
        let batch_started = Instant::now();
        match operation {
            Operation::Get => {
                let results: Vec<_> = lookups
                    .iter()
                    .map(|lookup| manager.get(*lookup, CatalogTick::ZERO).unwrap())
                    .collect();
                black_box(results);
            }
            Operation::Exists => {
                let results: Vec<_> = lookups
                    .iter()
                    .map(|lookup| manager.exists(*lookup, CatalogTick::ZERO))
                    .collect();
                black_box(results);
            }
            Operation::Put => unreachable!(),
        }
        latencies.push(nanos(batch_started.elapsed()));
    }
    Sample {
        elapsed: started.elapsed(),
        items,
        batch_latencies_ns: latencies,
    }
}

fn benchmark_multi_lookup(
    operation: Operation,
    batch_size: usize,
    items: usize,
    tenant_count: usize,
) -> Sample {
    let hot_keys_per_tenant = HOT_KEYS_PER_TENANT.div_ceil(tenant_count);
    let (manager, tenants) = tenant_manager(tenant_count, HOT_KEYS_PER_TENANT + 1024);
    let keys = preload_multi(&manager, &tenants, hot_keys_per_tenant);
    let selected: Vec<&str> = (0..items)
        .map(|index| {
            let batch = index / batch_size;
            let tenant = batch % tenant_count;
            keys[tenant][index % keys[tenant].len()].as_ref()
        })
        .collect();
    let batches = items.div_ceil(batch_size);
    let mut latencies = Vec::with_capacity(batches);
    let started = Instant::now();
    for (batch, selected) in selected.chunks(batch_size).enumerate() {
        let tenant = batch % tenant_count;
        let batch_started = Instant::now();
        match operation {
            Operation::Get => {
                black_box(
                    manager
                        .get_batch(
                            &tenants[tenant],
                            selected.iter().copied(),
                            CatalogTick::ZERO,
                        )
                        .unwrap(),
                );
            }
            Operation::Exists => {
                black_box(
                    manager
                        .exists_batch(
                            &tenants[tenant],
                            selected.iter().copied(),
                            CatalogTick::ZERO,
                        )
                        .unwrap(),
                );
            }
            Operation::Put => unreachable!(),
        }
        latencies.push(nanos(batch_started.elapsed()));
    }
    Sample {
        elapsed: started.elapsed(),
        items,
        batch_latencies_ns: latencies,
    }
}

fn benchmark_raw_put(batch_size: usize, items: usize, round: usize) -> Sample {
    let manager = raw_manager(items + 1024);
    let batches = make_put_batches(batch_size, items, 1, format!("put-{round}"));
    let mut latencies = Vec::with_capacity(batches.len());
    let started = Instant::now();
    for batch in batches {
        let batch_started = Instant::now();
        let start_results: Vec<_> = batch
            .requests
            .into_iter()
            .map(|request| {
                let (key, plan) = request.into_parts();
                manager.start_put(
                    ObjectIdentity::new(NamespaceId::DEFAULT, key),
                    admission(),
                    plan,
                    CatalogTick::ZERO,
                )
            })
            .collect();
        for result in start_results {
            result.unwrap();
        }
        let finish_results: Vec<_> = batch
            .keys
            .iter()
            .map(|key| {
                manager.finish_put(
                    &ObjectIdentity::new(NamespaceId::DEFAULT, key.clone()),
                    owner(),
                    ReplicaSelector::All,
                )
            })
            .collect();
        for result in finish_results {
            result.unwrap();
        }
        latencies.push(nanos(batch_started.elapsed()));
    }
    Sample {
        elapsed: started.elapsed(),
        items,
        batch_latencies_ns: latencies,
    }
}

fn benchmark_multi_put(
    batch_size: usize,
    items: usize,
    tenant_count: usize,
    round: usize,
) -> Sample {
    let (manager, tenants) = tenant_manager(tenant_count, items + 1024);
    let batches = make_put_batches(batch_size, items, tenant_count, format!("put-{round}"));
    benchmark_tenant_put(&manager, &tenants, batches, items)
}

fn benchmark_single_facade_put(batch_size: usize, items: usize, round: usize) -> Sample {
    let (manager, tenant) = single_tenant_manager(items + 1024);
    let batches = make_put_batches(batch_size, items, 1, format!("put-{round}"));
    benchmark_tenant_put(&manager, &[tenant], batches, items)
}

fn benchmark_tenant_put(
    manager: &TenantObjectManager,
    tenants: &[ResolvedTenant],
    batches: Vec<PutBatch>,
    items: usize,
) -> Sample {
    let mut latencies = Vec::with_capacity(batches.len());
    let started = Instant::now();
    for batch in batches {
        let batch_started = Instant::now();
        for result in manager.start_put_batch(
            &tenants[batch.tenant],
            admission(),
            batch.requests,
            CatalogTick::ZERO,
        ) {
            result.unwrap();
        }
        for result in manager
            .finish_put_batch(
                &tenants[batch.tenant],
                batch.keys.iter().map(AsRef::as_ref),
                owner(),
                ReplicaSelector::All,
            )
            .unwrap()
        {
            result.unwrap();
        }
        latencies.push(nanos(batch_started.elapsed()));
    }
    Sample {
        elapsed: started.elapsed(),
        items,
        batch_latencies_ns: latencies,
    }
}

fn print_put_breakdown(items: usize, rounds: usize) {
    println!("Tenant quota batch-put breakdown");
    println!("items={items} rounds={rounds} batch=333 object={OBJECT_BYTES}B");
    println!("mode          total-ns/item   start-ns/item  finish-ns/item");

    let baseline = median_breakdown_sample(
        (0..rounds)
            .map(|round| benchmark_raw_put_breakdown(items, round))
            .collect(),
    );
    print_breakdown_sample("single", &baseline);
    let facade = median_breakdown_sample(
        (0..rounds)
            .map(|round| benchmark_single_facade_put_breakdown(items, round))
            .collect(),
    );
    print_breakdown_sample("facade", &facade);
    for tenants in [1, 4] {
        let sample = median_breakdown_sample(
            (0..rounds)
                .map(|round| benchmark_multi_put_breakdown(items, tenants, round))
                .collect(),
        );
        print_breakdown_sample(if tenants == 1 { "multi-1" } else { "multi-4" }, &sample);
    }
}

fn benchmark_raw_put_breakdown(items: usize, round: usize) -> PutBreakdownSample {
    let manager = raw_manager(items + 1024);
    let batches = make_put_batches(333, items, 1, format!("put-breakdown-{round}"));
    let mut start_elapsed = Duration::ZERO;
    let mut finish_elapsed = Duration::ZERO;
    let started = Instant::now();
    for batch in batches {
        let phase_started = Instant::now();
        let start_results: Vec<_> = batch
            .requests
            .into_iter()
            .map(|request| {
                let (key, plan) = request.into_parts();
                manager.start_put(
                    ObjectIdentity::new(NamespaceId::DEFAULT, key),
                    admission(),
                    plan,
                    CatalogTick::ZERO,
                )
            })
            .collect();
        for result in start_results {
            result.unwrap();
        }
        start_elapsed += phase_started.elapsed();

        let phase_started = Instant::now();
        let finish_results: Vec<_> = batch
            .keys
            .iter()
            .map(|key| {
                manager.finish_put(
                    &ObjectIdentity::new(NamespaceId::DEFAULT, key.clone()),
                    owner(),
                    ReplicaSelector::All,
                )
            })
            .collect();
        for result in finish_results {
            result.unwrap();
        }
        finish_elapsed += phase_started.elapsed();
    }
    PutBreakdownSample {
        elapsed: started.elapsed(),
        start_elapsed,
        finish_elapsed,
        items,
    }
}

fn benchmark_single_facade_put_breakdown(items: usize, round: usize) -> PutBreakdownSample {
    let (manager, tenant) = single_tenant_manager(items + 1024);
    let batches = make_put_batches(333, items, 1, format!("put-breakdown-{round}"));
    benchmark_tenant_put_breakdown(&manager, &[tenant], batches, items)
}

fn benchmark_multi_put_breakdown(
    items: usize,
    tenant_count: usize,
    round: usize,
) -> PutBreakdownSample {
    let (manager, tenants) = tenant_manager(tenant_count, items + 1024);
    let batches = make_put_batches(333, items, tenant_count, format!("put-breakdown-{round}"));
    benchmark_tenant_put_breakdown(&manager, &tenants, batches, items)
}

fn benchmark_tenant_put_breakdown(
    manager: &TenantObjectManager,
    tenants: &[ResolvedTenant],
    batches: Vec<PutBatch>,
    items: usize,
) -> PutBreakdownSample {
    let mut start_elapsed = Duration::ZERO;
    let mut finish_elapsed = Duration::ZERO;
    let started = Instant::now();
    for batch in batches {
        let phase_started = Instant::now();
        for result in manager.start_put_batch(
            &tenants[batch.tenant],
            admission(),
            batch.requests,
            CatalogTick::ZERO,
        ) {
            result.unwrap();
        }
        start_elapsed += phase_started.elapsed();

        let phase_started = Instant::now();
        for result in manager
            .finish_put_batch(
                &tenants[batch.tenant],
                batch.keys.iter().map(AsRef::as_ref),
                owner(),
                ReplicaSelector::All,
            )
            .unwrap()
        {
            result.unwrap();
        }
        finish_elapsed += phase_started.elapsed();
    }
    PutBreakdownSample {
        elapsed: started.elapsed(),
        start_elapsed,
        finish_elapsed,
        items,
    }
}

fn single_tenant_manager(expected_objects: usize) -> (Arc<TenantObjectManager>, ResolvedTenant) {
    let pool = benchmark_pool(expected_objects);
    let manager = Arc::new(
        TenantObjectManager::with_config(
            pool,
            ObjectCatalogConfig::new(expected_objects.max(1)),
            TenantConfig::Single,
        )
        .unwrap(),
    );
    let tenant = manager
        .resolve_tenant(&TenantId::new("benchmark-single").unwrap())
        .unwrap();
    (manager, tenant)
}

fn raw_manager(expected_objects: usize) -> Arc<ObjectManager> {
    let pool = benchmark_pool(expected_objects);
    Arc::new(
        ObjectManager::with_config(pool, ObjectCatalogConfig::new(expected_objects.max(1)))
            .unwrap(),
    )
}

fn tenant_manager(
    tenant_count: usize,
    expected_objects: usize,
) -> (Arc<TenantObjectManager>, Vec<ResolvedTenant>) {
    let pool = benchmark_pool(expected_objects);
    let quota = SEGMENT_BYTES / tenant_count as u64;
    let policies: Vec<_> = (0..tenant_count)
        .map(|index| {
            (
                TenantId::new(format!("tenant-{index}")).unwrap(),
                TenantPolicy::new(TenantQuotaLimits::new(quota, 0)),
            )
        })
        .collect();
    let ids: Vec<_> = policies.iter().map(|(id, _)| id.clone()).collect();
    let manager = Arc::new(
        TenantObjectManager::with_config(
            pool,
            ObjectCatalogConfig::new(expected_objects.max(1)),
            TenantConfig::multi(policies),
        )
        .unwrap(),
    );
    let tenants = ids
        .iter()
        .map(|id| manager.resolve_tenant(id).unwrap())
        .collect();
    (manager, tenants)
}

fn benchmark_pool(expected_objects: usize) -> Arc<SegmentPool> {
    let max_allocations = u32::try_from(expected_objects.saturating_add(4096))
        .unwrap_or(u32::MAX - 2)
        .clamp(3, u32::MAX - 2);
    let pool = Arc::new(SegmentPool::with_config(SegmentPoolConfig::new(max_allocations)).unwrap());
    pool.attach(SegmentSpec::memory(
        SegmentIdentity::new(SegmentId::new(1, 1), OWNER, "tenant-benchmark"),
        MemoryRegion::new(0x6_0000_0000, SEGMENT_BYTES),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
    ))
    .unwrap();
    pool
}

fn preload_raw(manager: &ObjectManager) -> Vec<ObjectIdentity> {
    (0..HOT_KEYS_PER_TENANT)
        .map(|index| {
            let identity = ObjectIdentity::new(NamespaceId::DEFAULT, format!("hot-{index}"));
            manager
                .start_put(identity.clone(), admission(), plan(), CatalogTick::ZERO)
                .unwrap();
            manager
                .finish_put(&identity, owner(), ReplicaSelector::All)
                .unwrap();
            identity
        })
        .collect()
}

fn preload_multi(
    manager: &TenantObjectManager,
    tenants: &[ResolvedTenant],
    hot_keys_per_tenant: usize,
) -> Vec<Vec<Arc<str>>> {
    tenants
        .iter()
        .enumerate()
        .map(|(tenant_index, tenant)| {
            (0..hot_keys_per_tenant)
                .map(|index| {
                    let key: Arc<str> = format!("hot-{tenant_index}-{index}").into();
                    manager
                        .start_put(tenant, key.clone(), admission(), plan(), CatalogTick::ZERO)
                        .unwrap();
                    manager
                        .finish_put(tenant, &key, owner(), ReplicaSelector::All)
                        .unwrap();
                    key
                })
                .collect()
        })
        .collect()
}

fn make_put_batches(
    batch_size: usize,
    items: usize,
    tenant_count: usize,
    prefix: String,
) -> Vec<PutBatch> {
    (0..items.div_ceil(batch_size))
        .map(|batch| {
            let count = batch_size.min(items - batch * batch_size);
            let tenant = batch % tenant_count;
            let keys: Vec<Arc<str>> = (0..count)
                .map(|offset| format!("{prefix}-{}", batch * batch_size + offset).into())
                .collect();
            let requests = keys
                .iter()
                .map(|key| TenantPutRequest::new(key.clone(), plan()))
                .collect();
            PutBatch {
                tenant,
                keys,
                requests,
            }
        })
        .collect()
}

fn plan() -> ObjectPutPlan {
    ObjectPutPlan::new(
        ObjectContent::new(OBJECT_BYTES).with_kind(ObjectKind::KvCache),
        PlacementRequest::new(AllocationSpec::new(OBJECT_BYTES), ReplicaPolicy::new(1))
            .for_replica_class(ReplicaClass::Memory)
            .with_fulfillment(FulfillmentPolicy::BestEffort),
    )
}

fn owner() -> WriteOwner {
    WriteOwner::new(OWNER)
}

fn admission() -> WriteAdmission {
    WriteAdmission::unmanaged(OWNER)
}

fn median_sample(mut samples: Vec<Sample>) -> Sample {
    samples.sort_unstable_by_key(|sample| sample.elapsed);
    samples.swap_remove(samples.len() / 2)
}

fn median_breakdown_sample(mut samples: Vec<PutBreakdownSample>) -> PutBreakdownSample {
    samples.sort_unstable_by_key(|sample| sample.elapsed);
    samples.swap_remove(samples.len() / 2)
}

fn print_breakdown_sample(mode: &str, sample: &PutBreakdownSample) {
    let items = sample.items as f64;
    println!(
        "{mode:<12} {:>13.2} {:>15.2} {:>15.2}",
        sample.elapsed.as_nanos() as f64 / items,
        sample.start_elapsed.as_nanos() as f64 / items,
        sample.finish_elapsed.as_nanos() as f64 / items,
    );
}

fn print_sample(
    operation: Operation,
    batch_size: usize,
    mode: &str,
    sample: &Sample,
    baseline_throughput: f64,
    baseline_p99: f64,
) {
    let mut latencies = sample.batch_latencies_ns.clone();
    latencies.sort_unstable();
    let index = latencies.len().saturating_sub(1).saturating_mul(99) / 100;
    let p99 = latencies[index] as f64 / 1000.0;
    let throughput = sample.items_per_second();
    println!(
        "{:<9} {:>5} {:<12} {:>10.3} {:>+15.2}% {:>14.3} {:>+10.2}%",
        operation.name(),
        batch_size,
        mode,
        throughput / 1_000_000.0,
        (throughput / baseline_throughput - 1.0) * 100.0,
        p99,
        (p99 / baseline_p99 - 1.0) * 100.0,
    );
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn parse_or<T>(value: Option<String>, default: T) -> Result<T, T::Err>
where
    T: std::str::FromStr,
{
    value.map_or(Ok(default), |value| value.parse())
}
