# ObjectCatalog 与 Mooncake 同口径性能对比

## Batch RPC 线上比例压测（2026-08-10）

这一组走真实 TCP、Mooncake `WrappedMasterService` wire、真实 catalog、placement、
OffsetAllocator 和自动高水位淘汰。数据本身不经过 Master；测量的是 metadata plane
的 RPC、编解码、事务协调、allocator 和 eviction。为了排除 client 实现差异，Rust
和 C++ server 都由同一个 `-O3` C++ client 驱动。

工作负载按线上量级设置：

- `BatchPut`、`BatchGet`、`BatchExists` 各自一条独立连接和限速流，逻辑请求比例
  1:1:1，每类 150 batch QPS；
- batch size 为 333，因此每秒新增 `333 * 150 = 49,950` 个 key，总计 450 logical
  batch QPS、149,850 item ops/s；
- 一个 logical `BatchPut` 完整包含串行的 `BatchPutStart + BatchPutEnd`，GET/EXISTS
  各包含一个 RPC；
- 对象大小 1 KiB，单 Memory segment 为 1,150,561,798 bytes；先成功写入 1M 个
  key，初始占用恰为 89%；
- 热集为预填充尾部 500K key，正式启动前由 GET/EXISTS 全量触达；GET 流约每 10 秒
  覆盖一次热集，与双方 10 秒 lease 一致；
- 在三条流下预热 10 秒，再测量 30 秒。测量窗口内每类恰好 4,500 batch，PUT 新增
  1,498,500 key；加上预热后总新增 1,998,000 key，远大于初始剩余空间，因此成功
  完成必然依赖持续 eviction。

双方均为 Release 构建、8 个 RPC worker、`high_watermark=0.90`、
`eviction_ratio=0.05`、10 秒 lease。测试机为 AMD Ryzen 7 9700X（8C/16T），Rust
1.97.1、GCC 16.1.1；Cakemaster 基于 `dccb3b70582739132b62db90a65a2f0ba9f15724`
加本次工作树，Mooncake 为 `8c6095c06e20848506cbf91ef4a714924e7b03b1`。测试按实现
顺序冷启动执行，未绑定 CPU。

Rust 完成 3 轮，表中取中位数；C++ 完成 2 轮，表中取两轮中点。第三轮 C++ 因
执行环境未能批准本地进程启动而没有产生样本，未用失败启动补数。所有有效轮次均
达到 450 logical batch QPS，PUT start/end 零失败，GET/EXISTS 零 miss/零错误。

| logical batch | Rust p50 / p99 / p99.9 | C++ Mooncake p50 / p99 / p99.9 | Rust 相对 C++（越低越好） |
| --- | ---: | ---: | ---: |
| BatchPut（Start + End） | 1,144.988 / 1,764.960 / 2,243.624 us | 907.538 / 3,467.195 / 9,630.238 us | +26.2% / -49.1% / -76.7% |
| BatchGet | 479.338 / 1,369.211 / 1,695.677 us | 466.695 / 1,811.577 / 7,761.078 us | +2.7% / -24.4% / -78.2% |
| BatchExists | 541.390 / 1,151.615 / 1,450.461 us | 351.778 / 1,807.117 / 7,994.771 us | +53.9% / -36.3% / -81.9% |

原始轮次范围如下，避免聚合值掩盖抖动：

| logical batch | 实现 | p50 范围 | p99 范围 | p99.9 范围 |
| --- | --- | ---: | ---: | ---: |
| BatchPut | Rust | 1,098.403–1,166.047 us | 1,751.768–1,856.980 us | 2,216.206–2,513.313 us |
| BatchPut | C++ | 898.277–916.798 us | 3,431.867–3,502.524 us | 9,413.523–9,846.953 us |
| BatchGet | Rust | 467.347–581.860 us | 1,363.258–1,376.187 us | 1,612.992–1,713.787 us |
| BatchGet | C++ | 441.748–491.642 us | 1,658.295–1,964.859 us | 6,558.256–8,963.900 us |
| BatchExists | Rust | 300.646–545.454 us | 1,132.894–1,253.671 us | 1,364.077–1,673.612 us |
| BatchExists | C++ | 338.002–365.553 us | 1,370.170–2,244.064 us | 6,874.585–9,114.957 us |

结论是：在这个线上目标速率而非饱和吞吐测试中，两边 admission 和正确性都满足
要求。C++ 的 BatchPut/BatchExists 中位延迟更低，BatchGet 中位基本相当；Rust 的
三类 p99 都更低，p99.9 低约 77%–82%。C++ 两轮分别出现 35/68 次调度迟到，最大
约 4.1/4.3 ms；Rust 三轮没有调度迟到。由于没有绑核且 C++ 只有两轮，这组数字应
作为当前机器上的实测基线，不应外推成不同硬件上的固定倍率。

Rust 三轮均触发 40 次 reclaim，回收约 2.01M 个对象，最高水位
90.036%–90.037%，结束水位 85.94%–86.06%。C++ 在预热和测量中也成功写入远超
剩余容量的 1.998M 个对象，因而同样实际触发了滚动 eviction，而不是只消耗预留
headroom。

可复现命令：

```bash
g++ -std=c++20 -O3 -DNDEBUG \
  -I /path/to/Mooncake/extern/yalantinglibs/include \
  -I /path/to/Mooncake/extern/yalantinglibs/include/ylt/thirdparty \
  interop/mooncake_benchmark.cpp -pthread -o /tmp/mooncake_benchmark

cargo build --release -p cakemaster-server \
  --bin object_catalog_rpc_benchmark_server

# Rust server
target/release/object_catalog_rpc_benchmark_server \
  127.0.0.1:19094 8 1150561798 4000000 2000000 0.90 0.05
/tmp/mooncake_benchmark mixed-client \
  127.0.0.1 19094 333 150 30 10 1000000 500000 1024

# C++ Mooncake server；master_bench 只负责挂载 segment 并维持心跳
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

当前 Mooncake 提交的
`mooncake-store/include/ha/snapshot/object/snapshot_object_store.h` 使用 `uint8_t`
但没有直接包含 `<cstdint>`。本次没有修改其源码，Release 构建只增加了
`-include cstdint` 作为构建期 workaround；这不会改变被测 Master 的业务路径。

Rust benchmark server 的自动压力控制器明确是 Memory-only 测试设施。catalog
现已支持按 tenant/replica class 的 scoped reclaim filter；通用生产控制器仍需按
各 class 的物理水位分别产生全局压力目标，详见
[`object_catalog_rpc.md`](object_catalog_rpc.md)。

## Direct API 对比（2026-08-07）

测试日期为 2026-08-07。Mooncake 使用提交
`bdacc80a478cdf574dfd30fbc85ca33d34195a48`，Cakemaster 工作树基于提交
`765eaf8ff6610649229cc56ba37c1068923ed49d`。测试机为 AMD Ryzen 7 9700X
（8 核 16 线程），Rust 1.97.1，GCC 16.1.1；双方均使用 Release/O3 构建，
均为同进程直接调用，不包含 RPC 和数据传输。

## 真实高水位自动触发（生产结论）

这一组取代手动调用 `BatchEvict(50%)` 作为生产性能结论。双方使用一个
Memory segment、1 KiB 对象和 OffsetAllocator：先写入 100K 或 1M 个对象，
使 allocator 位于 89%；冷对象 lease 全部过期，2048 个 hot key 由 get 持续
续租。随后 8 个 worker 各执行 50K 次交替 put/get，共 400K 次闭环操作，其中
200K 次为 put。Mooncake 开启默认 `high_watermark=0.90`、`eviction_ratio=0.05`，
由其 10 ms eviction thread 自动触发；ObjectCatalog 使用相同检测周期和目标公式，
每个 collector step 最多处理 64 个候选、64 个 reclaim 和 16 个空 slot。

100K 取 5 轮中位数，1M 取 3 轮中位数。由于这是最大吞吐闭环压力测试，失败
请求返回很快，不能用总 ops/s 判断有效写吞吐；下表单独统计成功 put。

| 初始对象 | 实现 | 最高/结束水位 | 成功 put / 200K | 失败率 | 成功 put/s | 成功 put p99 / p99.9 | get p99 / p99.9 |
|---:|---|---:|---:|---:|---:|---:|---:|
| 100K | Mooncake | 100.0% / 85.6% | 31,910 | 84.0% | 685K | 9.91 / 26.44 us | 1.27 / 6.01 us |
| 100K | ObjectCatalog stable-entry | 100.0% / 85.0% | 29,213 | 85.4% | 955K | 5.45 / 9.28 us | 0.23 / 0.37 us |
| 1M | Mooncake | 100.0% / 88.5% | 166,003 | 17.0% | 1.590M | 11.82 / 145.25 us | 1.62 / 89.78 us |
| 1M | ObjectCatalog stable-entry | 100.0% / 85.4% | 159,609 | 20.2% | 3.251M | 7.90 / 58.22 us | 0.24 / 0.51 us |

结论分为两部分：

- stable-entry 去掉 fresh-key 成功 put 的两次 ArcSwap 指针发布后，ObjectCatalog 的有效写吞吐
  在 100K/1M 分别达到 Mooncake 的约 1.39x/2.04x。100K 尚略低于 1.5x；1M 已超过
  目标。100K 的主要限制是仅约 30 ms 的压力窗口内只能经历两个 10 ms 回收周期，
  成功对象数主要由释放容量而非 catalog 计算吞吐决定。
- 规模达到 1M 后，Mooncake 的低比例 selective-frontier eviction 会扫描大量
  metadata；成功 put p99.9 达约 145 us，get p99.9 达约 90 us。ObjectCatalog 的
  bounded collector step p99 约 0.073 ms，get p99.9 保持在 0.51 us，成功 put
  p99.9 约 58 us。

1M 时 ObjectCatalog 最终回收到约 85.4%，Mooncake 中位数约 88.5%，说明前者在
持续压力下完成了更多回收工作；这有利于成功率，但不能用于掩盖其有效写吞吐较低。
此外，此处是单 key direct API；批量 get 会增加 Mooncake 单次请求撞上被锁 shard
的概率，应另设 batch 口径，不与本表混用。

高失败率本身也是结论：90% 水位配合 10 ms 轮询，在当前最大写入速率下会在回收
开始或完成前撞到 100%。除了优化 catalog，还应考虑更早的触发水位、按写入速率
估算 headroom，以及显式 backpressure，而不是让 allocator fail-fast。

## 同步回收总工作量

这一组直接复用 Mooncake 官方 `batch_evict_bench` 的语义：一个 Memory
segment、一个完成态 replica、1 KiB 对象、lease 全部过期、无 pin，回收
50% 对象。预填充不计时；候选选择、元数据删除和 OffsetAllocator 释放全部计时。
ObjectCatalog 使用 `collect_budget=64`，重复 bounded step 直到完成相同数量的
回收，并在计时区间内清除空 slot。10K/100K 取 5 轮中位数，1M 取 3 轮中位数。

| 初始对象 | 回收对象 | Mooncake `BatchEvict` | ObjectCatalog 增量总耗时 | ObjectCatalog 相对性能 | collector step p99 |
|---:|---:|---:|---:|---:|---:|
| 10K | 5K | 3.234 ms | 2.568 ms | 1.26x | 约 36 us |
| 100K | 50K | 44.706 ms | 23.167 ms | 1.93x | 约 33 us |
| 1M | 500K | 434.132 ms | 252.116 ms | 1.72x | 约 35 us |

在 100K 和 1M 规模上，完整回收工作量达到超过 Mooncake 1.5x 的目标；10K
规模的固定开销占比更高，只达到 1.26x。把 ObjectCatalog 改成单个大 step
不会显著降低总工作量，却分别形成约 22 ms 和 275 ms 的单次停顿，因此生产
配置应保留 bounded step。

## BatchEvict 期间的前台 lookup

这一组使用 8 个 lookup 线程循环访问预生成的 2048 个 hot key，预热 10 ms
后回收 50% 冷对象。双方都走真实的单 key catalog API，不包含 RPC。Mooncake
在 `BatchEvict` 内同步删除 metadata；ObjectCatalog 先完成逻辑删除和 allocation
释放，把空 slot 清扫留到回收压力解除后。指标仍取各轮中位数。

| 初始对象 | Mooncake 回收耗时 | ObjectCatalog 释放容量 | ObjectCatalog 后台 slot 清扫 | Mooncake lookup p99 | ObjectCatalog lookup p99 |
|---:|---:|---:|---:|---:|---:|
| 100K | 56.036 ms | 46.914 ms（1.19x） | 27.190 ms | 1.57 us | 0.21 us（7.48x） |
| 1M | 593.823 ms | 451.880 ms（1.31x） | 310.710 ms | 1.43 us | 0.21 us（6.81x） |

ObjectCatalog 在前台有读压力时没有达到 1.5x 的容量释放吞吐，但 lookup p99
显著更低。若把延后的 slot 清扫也加入总工作量，100K/1M 分别约为 73.9 ms
和 762.6 ms，慢于 Mooncake；它解决的是 stop-the-world/大批次尾延迟，不是消除
全部清扫成本。

## 50:50 put/get

参数为 8 worker、8 个 Memory segment、每线程 50K 次交替 put/get、4 KiB
对象、2048 个 hot get key。双方都在计时前生成 key；put 包含 allocator reserve、
元数据创建和 publish/PutEnd，get 使用真实 replica lookup。双方均取 5 轮
中位样本。

| 实现 | 吞吐 | put p50 / p99 | get p50 / p99 |
|---|---:|---:|---:|
| Mooncake | 6.027 M ops/s | 1.71 / 5.12 us | 0.51 / 0.91 us |
| ObjectCatalog stable-entry，无 collector | 9.724 M ops/s | 1.14 / 3.38 us | 0.12 / 0.24 us |
| ObjectCatalog stable-entry，增量 collector | 8.394 M ops/s | 1.11 / 2.41 us | 0.12 / 0.22 us |

无 collector 时 ObjectCatalog 达到 Mooncake 的 1.61x，超过 1.5x 目标；启用持续
增量 collector 后为 1.39x。与同一测试会话中的旧实现相比，8 线程无 collector
从 2.65 M 提升到 9.72 M ops/s，约 3.66x。主要收益来自让一个 CatalogNode 在
claim/stage/publish 全生命周期保持稳定，并将单副本 ReplicaSet 内联；fresh-key
成功路径不再执行 ArcSwap writer 的 reader-debt 扫描。

下一优化项是把 slots、claims、pending、published 和 byte accounting 改为分片
计数，并将 young/pending ingress queue 分片，以缩小 collector 与前台 put 的共享
cache line。单 segment 高水位场景还需要继续优化 allocator 的串行锁。

## 可复现入口

- Rust 回收与并发 lookup：`crates/cakemaster-server/src/bin/object_catalog_evict_benchmark.rs`
- Rust 自动水位压力：`crates/cakemaster-server/src/bin/object_catalog_watermark_benchmark.rs`
- Rust 50:50：`crates/cakemaster-server/src/bin/object_catalog_benchmark.rs`
- Mooncake direct benchmark：`interop/mooncake_object_catalog_benchmark.cpp`
- Mooncake 官方基线：`mooncake-store/benchmarks/batch_evict_bench.cpp`

ObjectCatalog 回收示例：

```bash
cargo run --release -p cakemaster-server --bin object_catalog_evict_benchmark -- \
  --num_objects=100000 --evict_ratio_target=0.50 \
  --evict_ratio_lowerbound=0.25 --collect_budget=64 \
  --slot_cleanup_budget=0 --lookup_threads=8 --hot_objects=2048 --rounds=5
```

50:50 示例：

```bash
cargo run --release -p cakemaster-server --bin object_catalog_benchmark -- 8 50000 8 3 2048
```
