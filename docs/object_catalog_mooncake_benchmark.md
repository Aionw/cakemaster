# ObjectCatalog and Mooncake performance comparison under matched conditions

> The 2026-08-10 comparison pins Mooncake `8c6095c` wire and is a historical
> performance record. Current `interop/mooncake_benchmark.cpp` has moved to
> `5c0724d` with the mainline contract. Current results for the reimplementation
> based on per-key MVCC `main` appear first, followed by the original PR #19
> implementation and earlier historical comparisons.

## Production Master maximum throughput and the 1-second p99 boundary (2026-08-27)

This series uses the synthetic master workload from Mooncake
[`PR #3147`](https://github.com/kvcache-ai/Mooncake/pull/3147)
(commit `28223ae4a2de0d4aae7e69618e7a60a8f2bb5a5f`) to sweep fixed arrival rates
against complete production Masters. Cakemaster is based on
`6337bb6086813624881b091eddcc79a122dbac4e` plus the capacity-tuning changes in
this update; C++ Mooncake is `cafc50785855f7904c7de7727a6c0baf5c7a3dc8`.
Because of wire drift, the same benchmark source was built separately against
`5c0724d` and current Mooncake headers; workload logic is identical.

Local benchmark instrumentation extends PR #3147's original statistics with
per-operation avg/p50/p95/p99/p99.9/max and `end_to_end` latency from scheduled
arrival to operation completion. The SLO uses the latter, not just RPC service
time: sustained frontend worker-queue buildup is reflected in end-to-end p99.

### Environment and workload

- AMD Ryzen 7 9700X (8C/16T), 46 GiB RAM, Linux 7.1.3; no CPU pinning.
- Cakemaster uses Rust 1.97.1 release builds and mimalloc. C++ uses GCC 16.1.1
  release builds and 16 RPC threads; `ldd` confirms linkage to
  `/usr/lib/libjemalloc.so.2`.
- Three 256 GiB Memory segments, 448 KiB values, preferred placement.
- Exist/Put/Get batch sizes of 86/45/128. A complete Put transaction includes
  `BatchPutStart`, a 1,152 us commit delay, and `BatchPutEnd`.
- Get remains enabled. Baseline per-segment rates are 6.3359 Exist, 3.1431 Put
  transactions, and 2.0242 Get QPS, all scaled by the same factor.
- Fixed open-loop arrivals, 16 synchronous client workers per segment, and at
  most 1M committed key IDs retained per segment. Each level generates traffic
  for 30 seconds, then waits for the queues to drain completely.
- Cakemaster uses 1M allocator nodes per segment, 20M expected object slots,
  and 2,688 background candidates/reclaims per step. C++ disables metrics and
  keeps other eviction settings at their defaults.

An initial coarse sweep with 8 workers per segment hit the client-worker limit
first at about 58K logical tasks/s: the Put commit delay occupies synchronous
workers. The tables use 16 workers per segment to avoid misreporting client
concurrency limits as maximum server throughput. Ten-second samples also
overestimate sustained capacity, so the highest passing and adjacent failing
levels use 30-second windows. Passing levels were repeated twice with the same
workload.

### Highest passing level with p99 at or below 1 second

`offered logical` is the scheduled number of Exist/Get/Put transactions per
second. `completed logical` divides total completions by full elapsed time,
including drain. `actual RPC` counts only RPCs actually sent to the server: if
PutStart returns no placement, the client sends no PutEnd, so this is lower
than the target business RPC QPS assuming Start+End for every Put.
`committed keys` and Put key success rates are listed separately to avoid fast
failures inflating apparent throughput.

| Implementation | Highest passing factor | offered logical/s | completed logical/s | actual RPC/s | committed keys/s | Put key success rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Cakemaster | 2,950x | 101,804 | 98,747 | 114,268 | 639,907 | 52.70% |
| C++ Mooncake + jemalloc | 1,750x | 60,392 | 59,976 | 62,103 | 80,520 | 10.92% |

| Implementation | end-to-end avg | p50 | p95 | p99 | p99.9 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Cakemaster | 152.786 ms | 27.041 ms | 782.756 ms | 928.468 ms | 950.677 ms | 956.479 ms |
| C++ Mooncake + jemalloc | 148.443 ms | 84.929 ms | 469.574 ms | 663.856 ms | 734.288 ms | 751.560 ms |

Corresponding server-operation p99 values follow. Put transactions include the
commit delay; other columns measure single-batch RPC service time, excluding
client queue wait.

| Implementation | Exist | Get | PutStart | PutEnd | Put transaction |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cakemaster | 0.744 ms | 0.794 ms | 1.122 ms | 0.858 ms | 2.611 ms |
| C++ Mooncake + jemalloc | 6.048 ms | 6.851 ms | 5.847 ms | 0.666 ms | 6.016 ms |

Under this SLO, Cakemaster reaches 1.65x C++ completed logical throughput and
1.84x actual RPC throughput. Successfully committed key throughput is 7.95x,
but that ratio also reflects C++'s lower Put success rate under sustained
eviction/admission pressure; it is not a pure RPC execution comparison.

Adjacent levels confirm a capacity boundary rather than client rate limiting.
Cakemaster at 3,000x completes 99,517 logical/s, but queue-dispatch p99 alone
reaches 1.185 seconds. C++ at 1,800x completes 59,894 logical/s with dispatch p99
of 1.101 seconds. End-to-end latency cannot be lower than dispatch latency, so
both fail clearly. At the current sweep resolution, the boundaries are
`[2,950x, 3,000x)` and `[1,750x, 1,800x)`. Cakemaster at 2,950x is close to the
1-second line; for production jitter headroom, the 2,900x 30-second sample has
a dispatch p99 of 654 ms.

In the two additional passing-level runs, Cakemaster completed logical throughput
was 98,725/98,945 ops/s with dispatch p99 of 904/865 ms; C++ measured
59,426/59,616 ops/s and 636/333 ms. All repeat samples met the SLO, but these
remain capacity baselines for this unpinned machine, not fixed cross-hardware ratios.

With the 20M `expected-objects` capacity hint, Cakemaster's sampled VmHWM was
13.2 GiB versus C++'s 1.79 GiB. This option reserves object-index capacity; this
run aimed to remove runtime growth as a maximum-throughput confounder, not to
optimize for equal memory. These results therefore cannot establish memory
efficiency. Sampled peak server CPU usage was about 709% and 1,017%, respectively.

### Reproduction parameters

Cakemaster server:

```bash
cargo build --release --bin cakemaster
target/release/cakemaster \
  --listen 127.0.0.1:50450 \
  --max-allocator-nodes-per-segment 1000000 \
  --expected-objects 20000000 \
  --object-collection-budget-per-step 2688 \
  --log-level off
```

C++ server:

```bash
/path/to/Mooncake/build/mooncake-store/src/mooncake_master \
  --rpc_port=50450 --rpc_thread_num=16 \
  --enable_metric_reporting=false
ldd /path/to/Mooncake/build/mooncake-store/src/mooncake_master | grep jemalloc
```

The two highest passing levels use the following per-segment QPS values. Other
parameters retain the workload values above and PR #3147's default of 3 segments:

```bash
# Cakemaster 2,950x
master_synthetic_bench \
  --master_server=127.0.0.1:50450 --duration=30 \
  --arrival_model=fixed --workers_per_segment=16 \
  --exist_qps_per_segment=18690.905 \
  --put_qps_per_segment=9272.145 \
  --get_qps_per_segment=5971.390 \
  --max_pending_events_per_segment=1000000

# C++ Mooncake 1,750x
master_synthetic_bench \
  --master_server=127.0.0.1:50450 --duration=30 \
  --arrival_model=fixed --workers_per_segment=16 \
  --exist_qps_per_segment=11087.825 \
  --put_qps_per_segment=5500.425 \
  --get_qps_per_segment=3542.350 \
  --max_pending_events_per_segment=1000000
```

## Current MVCC implementation: production 1:1:1 high-pressure feedback loop (2026-08-17)

The current reimplementation uses the same pressure settings as the PR #19 sample
in the next section: a 115,056,180-byte Memory segment, 1 KiB objects, and
0.90/0.85 high/low watermarks. It prefills 100K objects, touches the trailing 50K
hot set, warms up for 3 seconds, then measures for 10 seconds. `BatchPut`,
`BatchGet`, and `BatchExists` each use an independent connection limited to
300 batch QPS, with batch size 333. The server uses 8 Tokio workers. Three rounds
run sequentially without CPU pinning on an Apple M5 (10 cores), 32 GiB RAM,
Rust 1.95.0, and Apple Clang 21.0.0.

All three rounds complete exactly 3,000 logical batches per operation, with a
median item rate of 299,747 ops/s. This rate-limited 900 logical batch QPS workload
measures target completion rather than saturation capacity. The latency table
shows three-round medians, with full ranges in parentheses:

| logical batch | p50 | p99 | p99.9 |
| --- | ---: | ---: | ---: |
| BatchPut (Start + End) | 903.167 us (785.625–1,123.292) | 2,641.125 us (1,411.125–2,679.667) | 8,911.250 us (2,390.583–11,300.292) |
| BatchGet | 493.958 us (426.333–680.125) | 2,034.833 us (1,009.500–2,098.958) | 5,868.208 us (1,716.666–8,259.875) |
| BatchExists | 412.917 us (373.875–528.000) | 1,680.458 us (879.459–1,716.917) | 8,000.625 us (1,661.042–8,479.666) |

All 999,000 PUTs succeed in each round, with zero `NO_AVAILABLE_HANDLE` or other
errors. GET/EXISTS each complete 999,000 hits with zero misses/errors. Peak
physical usage in all three rounds is 115,055,616 bytes (99.9995%), with
`watermark_triggered=true`. Median physical reclamation by the controller is
1,297,511 objects, or 1,328,651,264 bytes. At exit, `retired_bytes` and explicit,
allocation, and watermark debt are all zero in every round.

After traffic stops, one round settles at 84.9994% low; the others finish in the
88.8523%/89.4450% hysteresis band. The latter is not unfinished debt: the previous
high-to-low cycle completed during sustained writes, and the final writes stopped
below high, so no new watermark cycle began. Rounds 2 and 3 recorded 135/128
late client dispatches, with a maximum of about 57 ms. Their tail latency is
noticeably higher than round 1, which had no late dispatches. Full ranges are
therefore retained rather than presenting three desktop samples as stable
tail-latency conclusions.

Reproduction commands are the same as in the next section. These results show
that production composition sustains the target 1:1:1 rate and completes all
writes under continuous eviction. Stable, comparable tail-latency measurements
require pinning server/client to separate physical cores and more rounds.

## Original PR #19 implementation: production 1:1:1 high-pressure feedback loop (2026-08-17)

This sample comes from the original PR #19 implementation based on `e4ab526`.
The current `object_catalog_rpc_benchmark_server` likewise no longer owns a
private eviction thread; it directly constructs production
`MooncakeServerComposition`. The RPC listener and `MasterReconciler` share
startup, pressure wakeups, shutdown, and join lifecycles. The benchmark fails
at exit if high was never crossed.

This run uses a 115,056,180-byte Memory segment, 1 KiB objects, and 0.90/0.85
high/low watermarks. It successfully prefills 100K objects (about 89%), touches
the trailing 50K hot set, warms up for 3 seconds, and measures for 10 seconds.
`BatchPut`, `BatchGet`, and `BatchExists` each have an independent connection
limited to 300 batch QPS with batch size 333: 900 logical batch QPS, or about
299,700 item ops/s. There is no CPU pinning. This single high-pressure sample
records policy validation and a machine baseline for the original implementation,
not a fixed conclusion across hardware or for the current MVCC reimplementation.

| logical batch | Completions | p50 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: |
| BatchPut (Start + End) | 3,000 | 1,260.430 us | 1,853.818 us | 2,017.554 us |
| BatchGet | 3,000 | 473.565 us | 1,577.689 us | 1,741.405 us |
| BatchExists | 3,000 | 279.819 us | 1,416.955 us | 1,492.927 us |

Measurement-window request counts are exactly 3,000:3,000:3,000, delivering
900 logical batch QPS and 299,761 item ops/s with zero late dispatches. GET and
EXISTS each complete 999,000 hits with zero misses/errors. Of 999,000 attempted
PUT items, 996,216 succeed and 2,784 return `NO_AVAILABLE_HANDLE` within bounded
work near full capacity; there are no other errors. The success rate is 99.72%.

The watermark feedback loop actually triggers rather than merely consuming
reserved headroom. Maximum sampled usage is 115,055,616 bytes (99.9995%), with
`watermark_triggered=true`; the controller physically reclaims 1,299,737 objects
and 1,330,930,688 bytes. After write pressure stops and reconciliation continues,
final usage is 97,797,120 bytes (84.9994%, no higher than low at 97,797,753 bytes).
At that point `retired_bytes=0`, both debt categories are zero, and
`settled_to_low=true`. The run records 12,554 allocation-failure wakeups and
9,096 bounded retries, all successful. Failure paths still execute only the
configured finite collector budget, without unbounded request-path scans.

Reproduction commands (two terminals; after the client finishes, wait about
3 seconds before sending Ctrl-C to the server to print settled diagnostics):

```bash
g++ -std=c++20 -O3 -DNDEBUG \
  -I /path/to/Mooncake/extern/yalantinglibs/include \
  -I /path/to/Mooncake/extern/yalantinglibs/include/ylt/thirdparty \
  interop/mooncake_benchmark.cpp -pthread -o /tmp/mooncake_benchmark

cargo build --release --bin object_catalog_rpc_benchmark_server
target/release/object_catalog_rpc_benchmark_server \
  127.0.0.1:19094 8 115056180 2000000 500000 0.90 0.85

/tmp/mooncake_benchmark mixed-client \
  127.0.0.1 19094 333 300 10 3 100000 50000 1024
```

## Batch RPC load test at production ratios (2026-08-10)

This series uses real TCP, Mooncake `WrappedMasterService` wire, a real catalog,
placement, OffsetAllocator, and automatic high-watermark eviction. Data itself
does not pass through the Master; measurements cover metadata-plane RPCs,
encoding/decoding, transaction coordination, allocation, and eviction. The same
`-O3` C++ client drives both Rust and C++ servers to remove client implementation
differences.

Workload settings reflect production scale:

- `BatchPut`, `BatchGet`, and `BatchExists` each use an independent connection
  and rate-limited stream, with a 1:1:1 logical request ratio and 150 batch QPS
  per operation.
- Batch size is 333, adding `333 * 150 = 49,950` keys per second for a total of
  450 logical batch QPS and 149,850 item ops/s.
- One logical `BatchPut` includes serial `BatchPutStart + BatchPutEnd`;
  GET/EXISTS each consist of one RPC.
- Objects are 1 KiB, with one 1,150,561,798-byte Memory segment. Successfully
  prefilling 1M keys produces exactly 89% initial usage.
- The hot set is the trailing 500K prefilled keys, fully touched by GET/EXISTS
  before the run. GET covers it about every 10 seconds, matching both sides'
  10-second leases.
- All three streams warm up for 10 seconds, then measure for 30 seconds. Each
  operation completes exactly 4,500 batches in the measurement window; PUT adds
  1,498,500 keys. Including warmup, 1,998,000 keys are added, far beyond initial
  free space, so success necessarily depends on sustained eviction.

Both use Release builds, 8 RPC workers, `high_watermark=0.90`,
`eviction_ratio=0.05`, and 10-second leases. Hardware is an AMD Ryzen 7 9700X
(8C/16T), with Rust 1.97.1 and GCC 16.1.1. Cakemaster is based on
`dccb3b70582739132b62db90a65a2f0ba9f15724` plus the worktree for this run;
Mooncake is `8c6095c06e20848506cbf91ef4a714924e7b03b1`. Implementations run
sequentially from cold starts without CPU pinning.

Rust completed 3 rounds, reported as medians. C++ completed 2 rounds, reported
as their midpoint. A third C++ round produced no sample because the execution
environment did not approve local process startup; the failed launch was not
counted as a sample. Every valid round reached 450 logical batch QPS with zero
PUT start/end failures and zero GET/EXISTS misses/errors.

| logical batch | Rust p50 / p99 / p99.9 | C++ Mooncake p50 / p99 / p99.9 | Rust vs C++ (lower is better) |
| --- | ---: | ---: | ---: |
| BatchPut (Start + End) | 1,144.988 / 1,764.960 / 2,243.624 us | 907.538 / 3,467.195 / 9,630.238 us | +26.2% / -49.1% / -76.7% |
| BatchGet | 479.338 / 1,369.211 / 1,695.677 us | 466.695 / 1,811.577 / 7,761.078 us | +2.7% / -24.4% / -78.2% |
| BatchExists | 541.390 / 1,151.615 / 1,450.461 us | 351.778 / 1,807.117 / 7,994.771 us | +53.9% / -36.3% / -81.9% |

Raw round ranges are retained so aggregation does not hide variability:

| logical batch | Implementation | p50 range | p99 range | p99.9 range |
| --- | --- | ---: | ---: | ---: |
| BatchPut | Rust | 1,098.403–1,166.047 us | 1,751.768–1,856.980 us | 2,216.206–2,513.313 us |
| BatchPut | C++ | 898.277–916.798 us | 3,431.867–3,502.524 us | 9,413.523–9,846.953 us |
| BatchGet | Rust | 467.347–581.860 us | 1,363.258–1,376.187 us | 1,612.992–1,713.787 us |
| BatchGet | C++ | 441.748–491.642 us | 1,658.295–1,964.859 us | 6,558.256–8,963.900 us |
| BatchExists | Rust | 300.646–545.454 us | 1,132.894–1,253.671 us | 1,364.077–1,673.612 us |
| BatchExists | C++ | 338.002–365.553 us | 1,370.170–2,244.064 us | 6,874.585–9,114.957 us |

At this production target rate, rather than saturation throughput, both sides
meet admission and correctness requirements. C++ has lower BatchPut/BatchExists
median latency; BatchGet medians are roughly equivalent. Rust has lower p99
for all three and about 77%–82% lower p99.9. C++ records 35/68 late dispatches
in its two rounds, with maxima of about 4.1/4.3 ms; Rust records none in three
rounds. Without CPU pinning and with only two C++ rounds, these numbers are
measurements on this machine, not fixed ratios for other hardware.

Each Rust round triggers 40 reclaims, reclaiming about 2.01M objects, with peak
watermarks of 90.036%–90.037% and final watermarks of 85.94%–86.06%. C++ also
successfully writes 1.998M objects during warmup and measurement, far beyond
remaining capacity, so it too performs rolling eviction rather than only
consuming reserved headroom.

Reproduction commands:

```bash
g++ -std=c++20 -O3 -DNDEBUG \
  -I /path/to/Mooncake/extern/yalantinglibs/include \
  -I /path/to/Mooncake/extern/yalantinglibs/include/ylt/thirdparty \
  interop/mooncake_benchmark.cpp -pthread -o /tmp/mooncake_benchmark

cargo build --release --bin object_catalog_rpc_benchmark_server

# Rust server
target/release/object_catalog_rpc_benchmark_server \
  127.0.0.1:19094 8 1150561798 4000000 2000000 0.90 0.05
/tmp/mooncake_benchmark mixed-client \
  127.0.0.1 19094 333 150 30 10 1000000 500000 1024

# C++ Mooncake server; master_bench only mounts segments and maintains heartbeats
/path/to/Mooncake/build/mooncake-store/src/mooncake_master \
  --rpc_address=127.0.0.1 --rpc_port=19095 --rpc_thread_num=8 \
  --enable_metric_reporting=false --memory_allocator=offset \
  --eviction_high_watermark_ratio=0.90 --eviction_ratio=0.05 \
  --default_kv_lease_ttl=10000
/path/to/Mooncake/build/mooncake-store/benchmarks/master_bench \
  --master_server=127.0.0.1:19095 --num_segments=1 \
  --segment_size=1150561798 --num_clients=0 --duration=600 \
  --prefill_ratio=0
/tmp/mooncake_benchmark mixed-client \
  127.0.0.1 19095 333 150 30 10 1000000 500000 1024
```

That Mooncake commit's
`mooncake-store/include/ha/snapshot/object/snapshot_object_store.h` uses `uint8_t`
without directly including `<cstdint>`. Its source was not changed for this run;
the Release build added only `-include cstdint` as a build-time workaround,
without changing the measured Master's business paths.

These historical results used the benchmark server's then-independent
Memory-only pressure controller. The current server uses production high/low
watermark composition. The catalog still retains scoped reclaim filters by
tenant/replica class; see [object_catalog_rpc.md](object_catalog_rpc.md).

## Direct API comparison (2026-08-07)

Tests ran on 2026-08-07. Mooncake used commit
`bdacc80a478cdf574dfd30fbc85ca33d34195a48`; the Cakemaster worktree was based on
`765eaf8ff6610649229cc56ba37c1068923ed49d`. Hardware was an AMD Ryzen 7 9700X
(8 cores, 16 threads), with Rust 1.97.1 and GCC 16.1.1. Both used Release/O3
builds and direct in-process calls, excluding RPC and data transfer.

## Real automatic high-watermark triggering (production conclusions)

This series replaces manual `BatchEvict(50%)` calls as the basis for production
performance conclusions. Both sides use one Memory segment, 1 KiB objects, and
OffsetAllocator. Prefilling 100K or 1M objects brings allocator usage to 89%.
All cold-object leases expire, while gets continuously renew 2048 hot keys.
Then 8 workers each run 50K alternating put/get operations, totaling 400K
closed-loop operations, including 200K puts. Mooncake enables default
`high_watermark=0.90` and `eviction_ratio=0.05`, automatically triggered by its
10 ms eviction thread. ObjectCatalog uses the same detection interval and target
formula, with at most 64 candidates, 64 reclaims, and 16 empty slots per
collector step.

Results are medians of 5 rounds at 100K and 3 rounds at 1M. This is a
maximum-throughput closed-loop stress test; failed requests return quickly, so
total ops/s cannot measure useful write throughput. Successful puts are reported
separately below.

| Initial objects | Implementation | Peak/final watermark | Successful puts / 200K | Failure rate | Successful puts/s | Successful put p99 / p99.9 | get p99 / p99.9 |
|---:|---|---:|---:|---:|---:|---:|---:|
| 100K | Mooncake | 100.0% / 85.6% | 31,910 | 84.0% | 685K | 9.91 / 26.44 us | 1.27 / 6.01 us |
| 100K | ObjectCatalog stable-entry | 100.0% / 85.0% | 29,213 | 85.4% | 955K | 5.45 / 9.28 us | 0.23 / 0.37 us |
| 1M | Mooncake | 100.0% / 88.5% | 166,003 | 17.0% | 1.590M | 11.82 / 145.25 us | 1.62 / 89.78 us |
| 1M | ObjectCatalog stable-entry | 100.0% / 85.4% | 159,609 | 20.2% | 3.251M | 7.90 / 58.22 us | 0.24 / 0.51 us |

Conclusions have two parts:

- After stable-entry removes two ArcSwap pointer publications from successful
  fresh-key puts, ObjectCatalog reaches about 1.39x/2.04x Mooncake useful write
  throughput at 100K/1M. 100K remains slightly below the 1.5x target; 1M exceeds
  it. At 100K, the main limit is that the roughly 30 ms stress window allows
  only two 10 ms reclamation cycles, so freed capacity rather than catalog
  compute throughput determines successful object count.
- At 1M, Mooncake's low-ratio selective-frontier eviction scans large amounts
  of metadata. Successful put p99.9 reaches about 145 us and get p99.9 about
  90 us. ObjectCatalog's bounded collector step p99 is about 0.073 ms, get p99.9
  stays at 0.51 us, and successful put p99.9 is about 58 us.

At 1M, ObjectCatalog finishes near 85.4% versus Mooncake's median of about
88.5%, indicating more reclamation work under sustained pressure. This helps
success rates but must not obscure lower useful write throughput. These are
single-key direct APIs; batch gets increase the probability of a Mooncake request
hitting a locked shard and should be measured separately, not mixed into this table.

High failure rates are themselves a finding: a 90% watermark with 10 ms polling
hits 100% before reclamation starts or finishes at current maximum write rates.
Beyond catalog optimization, consider earlier trigger watermarks, write-rate-based
headroom estimates, and explicit backpressure rather than allocator fail-fast.

## Total synchronous reclamation work

This series directly follows Mooncake's official `batch_evict_bench` semantics:
one Memory segment, one completed replica, 1 KiB objects, all leases expired,
no pins, and 50% of objects reclaimed. Prefill is untimed; candidate selection,
metadata deletion, and OffsetAllocator release are all timed. ObjectCatalog uses
`collect_budget=64`, repeating bounded steps until the same object count is
reclaimed, and clears empty slots within the timed interval. Results are medians
of 5 rounds at 10K/100K and 3 rounds at 1M.

| Initial objects | Reclaimed objects | Mooncake `BatchEvict` | ObjectCatalog total incremental time | ObjectCatalog relative performance | collector step p99 |
|---:|---:|---:|---:|---:|---:|
| 10K | 5K | 3.234 ms | 2.568 ms | 1.26x | about 36 us |
| 100K | 50K | 44.706 ms | 23.167 ms | 1.93x | about 33 us |
| 1M | 500K | 434.132 ms | 252.116 ms | 1.72x | about 35 us |

At 100K and 1M, total reclamation meets the goal of exceeding 1.5x Mooncake
performance. Fixed overhead matters more at 10K, yielding only 1.26x. A single
large ObjectCatalog step does not significantly reduce total work but creates
pauses of about 22 ms and 275 ms, respectively, so production should retain
bounded steps.

## Foreground lookups during BatchEvict

Eight lookup threads repeatedly access 2048 pregenerated hot keys, warm up for
10 ms, then reclaim 50% of cold objects. Both sides use real single-key catalog
APIs without RPC. Mooncake deletes metadata synchronously in `BatchEvict`.
ObjectCatalog first completes logical deletion and allocation release, deferring
empty-slot cleanup until reclamation pressure subsides. Metrics remain medians
across rounds.

| Initial objects | Mooncake reclamation time | ObjectCatalog capacity release | ObjectCatalog background slot cleanup | Mooncake lookup p99 | ObjectCatalog lookup p99 |
|---:|---:|---:|---:|---:|---:|
| 100K | 56.036 ms | 46.914 ms (1.19x) | 27.190 ms | 1.57 us | 0.21 us (7.48x) |
| 1M | 593.823 ms | 451.880 ms (1.31x) | 310.710 ms | 1.43 us | 0.21 us (6.81x) |

Under foreground read pressure, ObjectCatalog does not reach 1.5x capacity-release
throughput, but lookup p99 is substantially lower. Including deferred slot
cleanup gives total times of about 73.9 ms at 100K and 762.6 ms at 1M, slower
than Mooncake. It addresses stop-the-world/large-batch tail latency rather than
eliminating all cleanup cost.

## 50:50 put/get

Settings are 8 workers, 8 Memory segments, 50K alternating put/get operations per
thread, 4 KiB objects, and 2048 hot get keys. Both sides generate keys before
timing. Put includes allocator reservation, metadata creation, and
publish/PutEnd; get performs real replica lookup. Both use median samples from
5 rounds.

| Implementation | Throughput | put p50 / p99 | get p50 / p99 |
|---|---:|---:|---:|
| Mooncake | 6.027 M ops/s | 1.71 / 5.12 us | 0.51 / 0.91 us |
| ObjectCatalog stable-entry, no collector | 9.724 M ops/s | 1.14 / 3.38 us | 0.12 / 0.24 us |
| ObjectCatalog stable-entry, incremental collector | 8.394 M ops/s | 1.11 / 2.41 us | 0.12 / 0.22 us |

Without a collector, ObjectCatalog reaches 1.61x Mooncake throughput, exceeding
the 1.5x target; continuous incremental collection reduces this to 1.39x.
Compared with the old implementation in the same test session, 8-thread
throughput without collection rises from 2.65 M to 9.72 M ops/s, about 3.66x.
The main gains come from keeping a CatalogNode stable throughout
claim/stage/publish and inlining single-replica ReplicaSet. The fresh-key success
path no longer scans ArcSwap writer reader debt.

The next optimization is sharded counters for slots, claims, pending, published,
and byte accounting, plus sharded young/pending ingress queues, reducing cache-line
sharing between collection and foreground puts. Single-segment high-watermark
scenarios also need further allocator serial-lock optimization.

## Reproduction entry points

- Rust reclamation and concurrent lookup: `src/bin/object_catalog_evict_benchmark.rs`
- Rust automatic watermark stress: `src/bin/object_catalog_watermark_benchmark.rs`
- Rust 50:50: `src/bin/object_catalog_benchmark.rs`
- Mooncake direct benchmark: `interop/mooncake_object_catalog_benchmark.cpp`
- Official Mooncake baseline: `mooncake-store/benchmarks/batch_evict_bench.cpp`

ObjectCatalog reclamation example:

```bash
cargo run --release --bin object_catalog_evict_benchmark -- \
  --num_objects=100000 --evict_ratio_target=0.50 \
  --evict_ratio_lowerbound=0.25 --collect_budget=64 \
  --slot_cleanup_budget=0 --lookup_threads=8 --hot_objects=2048 --rounds=5
```

50:50 example:

```bash
cargo run --release --bin object_catalog_benchmark -- 8 50000 8 3 2048
```
