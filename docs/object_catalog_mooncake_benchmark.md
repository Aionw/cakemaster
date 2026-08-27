# ObjectCatalog 与 Mooncake 同口径性能对比

> 2026-08-10 的对比固定在 Mooncake `8c6095c` wire，是历史性能记录。当前
> `interop/mooncake_benchmark.cpp` 已随主线契约升级到 `5c0724d`；下面先记录基于
> per-key MVCC `main` 的当前重实现，再保留 PR #19 原实现和更早的历史对比。

## Fixed-owner sharding A/B（2026-08-27，Apple M5）

这一组专门隔离 Issue #25 阶段一的 ownership sharding：baseline 是未修改的 PR #24
提交 `c2f649619b5b0871c491c328c45d4581fd909b8c`，候选版本保留 8 个 Tokio worker，
在其后增加 8 个 fixed-owner metadata thread；每个 shard 独占 catalog、collector、
eviction、SegmentPool 和 allocator extent，完全空闲的 extent 只通过消息转移。两边都
使用同一个 `-O3` C++ client、同一个 1:1:1 workload 和同一台
10-core Apple M5；未绑 client/server 到互斥 CPU，Rust 为 1.95.0。

每轮使用 115,056,180-byte Memory segment、1 KiB object、100K prefill、50K hot set、
0.90/0.85 watermark、batch size 333，预热 3 秒后测量 10 秒。三类操作各 600 batch
QPS，总计 1,800 logical batch/s、约 599.4K item/s。两边三轮均完成全部 1,998,000
Put key，Get/Exist 全 hit、零业务错误，并实际越过 high watermark、持续物理 reclaim。
下表取三轮中位数：

| 操作 | 指标 | PR #24 baseline | 阶段一初版 | 当前 lock-shrunk | 当前相对阶段一 |
| --- | --- | ---: | ---: | ---: | ---: |
| Put transaction | p50 | 764.958 us | 998.958 us | 1,160.041 us | +16.1% |
| Put transaction | p99 | 1,719.500 us | 1,478.166 us | 1,652.750 us | +11.8% |
| Put transaction | p99.9 | 6,412.000 us | 3,362.208 us | 2,136.333 us | -36.5% |
| Get | p50 | 366.167 us | 392.125 us | 487.417 us | +24.3% |
| Get | p99 | 1,313.833 us | 777.084 us | 925.166 us | +19.1% |
| Get | p99.9 | 4,884.917 us | 1,872.125 us | 1,099.375 us | -41.3% |
| Exist | p50 | 345.459 us | 304.791 us | 282.708 us | -7.2% |
| Exist | p99 | 1,117.750 us | 711.709 us | 656.084 us | -7.8% |
| Exist | p99.9 | 4,459.667 us | 1,677.167 us | 820.791 us | -51.1% |

fixed-rate 吞吐相同，因为三个版本都完整跟上目标；当前版本的三轮中位数为
599,414 item/s，scheduled-late request 中位数为 316。相对阶段一初版，当前版本用
shard-local collector gate、atomic segment state、lock-free extent owner snapshot 和
版本内嵌的 immutable replica snapshot 继续收掉 owner-thread 内无意义的同步。结果不是
所有分位单向改善：Put/Get p50 与 p99 回退，但三类 p99.9 分别下降 36.5%/41.3%/51.1%，
Exist p50/p99 也分别下降 7.2%/7.8%。相对 PR #24 baseline，当前三类 p99.9 下降
66.7%/77.5%/81.6%。每轮结束时各 shard 的 published object 最大/最小差仍约 0%–5.6%。

额外的 1,200 batch QPS/类单轮压力点用于找容量方向：baseline、阶段一初版和当前版本
都在 10 秒内完成目标，当前版本为 1,198,815 item/s，scheduled-late request 从阶段一
初版的 1,871 降到 44。当前 Put/Get/Exist p99 分别为 716/419/373 us。这个压力点仍是
未绑核桌面环境的单轮结果，不适合外推稳定尾延迟。这说明当前 M5 上的阶段一实现
显著改善中高负载尾延迟和 collector/allocator 争用，同时把这一压力点吞吐恢复到
baseline 水平，但尚未证明提高了极限吞吐；不能把它外推为 Issue #25 使用的
Ryzen 9700X、16 client workers/segment、PR #3147 open-loop workload 验收结果。该验收
仍需在原 Linux 机器上做 30 秒 factor sweep、三轮最大通过点和 5 分钟稳定性测试。

### End-to-end Compio shard runtime（2026-08-27）

最初的 Compio prototype 把连接放在一组 Compio worker、metadata 放在另一组 owner
thread，RPC 必须经历跨线程 decode → mailbox → oneshot → encode；它并不是 thread-per-core
shared nothing。当前实现删除这组双 runtime：每个 fixed-owner shard thread 自己运行 Compio
runtime，并同时承载 listener accept、connection driver、`ObjectManager`、local
`SegmentPool`/allocator 和 reconciler。原生 Compio framer 复用连接读写 buffer，不经过
Tokio compatibility adapter，也不为每个 RPC spawn task。

在与上节相同的 Apple M5、8 shard、599.4K item/s workload 下，使用最终 shared-nothing
代码和保存的 Tokio transport binary 交错各跑三轮。下表取中位数：

| 操作 | Compio p50 / p99 / p99.9 | Tokio p50 / p99 / p99.9 | Compio 相对 Tokio |
| --- | ---: | ---: | ---: |
| Put transaction | 1,172.125 / 1,715.334 / 2,090.750 us | 1,158.583 / 1,676.750 / 2,070.834 us | +1.2% / +2.3% / +1.0% |
| Get | 591.875 / 1,009.125 / 1,194.250 us | 487.959 / 913.458 / 1,175.667 us | +21.3% / +10.5% / +1.6% |
| Exist | 366.959 / 668.833 / 880.000 us | 301.416 / 652.791 / 893.667 us | +21.7% / +2.5% / -1.5% |

两者中位吞吐同为 599,413 item/s；scheduled-late request 为 416 vs 412，最大 scheduler
lag 中位数为 1.810 ms vs 2.949 ms。相对错误的双线程池 Compio prototype，当前 Put/Get/
Exist p99.9 分别下降 76.4%/81.7%/85.3%，异常长尾已消失；但 macOS polling backend 下
Get/Exist p50 仍比 Tokio 高约 21%，所以这组结果证明的是架构修正和尾延迟恢复，不是
Compio 的绝对性能领先。Linux production 会使用 io_uring backend，仍需在 Issue #25 的
Ryzen 9700X 环境做同口径三轮和 5 分钟稳定性验证，不能从 macOS 结果外推收益。

## Production Master 最大吞吐与 1 秒 p99 边界（2026-08-27）

这一组使用 Mooncake
[`PR #3147`](https://github.com/kvcache-ai/Mooncake/pull/3147) 的 synthetic master
workload（提交 `28223ae4a2de0d4aae7e69618e7a60a8f2bb5a5f`）对完整 production
Master 做固定到达率扫描。Cakemaster 基于 `6337bb6086813624881b091eddcc79a122dbac4e`
加本次容量调优改动；C++ Mooncake 为
`cafc50785855f7904c7de7727a6c0baf5c7a3dc8`。两边因 wire 漂移分别使用同一份
benchmark 源码针对 `5c0724d` 和当前 Mooncake headers 构建，工作负载逻辑完全相同。

本地 benchmark instrumentation 在 PR #3147 原有统计上增加了每类操作的
avg/p50/p95/p99/p99.9/max，并记录从计划到达时间到操作完成的 `end_to_end` 延迟。
这里的 SLO 判定使用后者，而不是只看 RPC service time；只要前端 worker queue 持续
积压，端到端 p99 就会反映出来。

### 环境与 workload

- AMD Ryzen 7 9700X（8C/16T）、46 GiB RAM、Linux 7.1.3；未绑核；
- Cakemaster 使用 Rust 1.97.1 release build 和 mimalloc；C++ 使用 GCC 16.1.1
  release build、16 个 RPC thread，并由 `ldd` 确认链接
  `/usr/lib/libjemalloc.so.2`；
- 3 个 256 GiB Memory segment，448 KiB value，preferred placement；
- Exist/Put/Get batch size 分别为 86/45/128；完整 Put transaction 包含
  `BatchPutStart`、1,152 us commit delay 和 `BatchPutEnd`；
- 保持 Get 开启；每个 segment 的基准速率为 6.3359 Exist、3.1431 Put transaction、
  2.0242 Get QPS，再按同一个 factor 放大；
- fixed open-loop 到达模型、16 个同步 client worker/segment、每个 segment 最多保留
  1M 个 committed key ID；每档生成 30 秒并等待队列完全排空；
- Cakemaster 使用 1M allocator node/segment、20M expected object slot 和每步 2,688
  个 background candidate/reclaim；C++ 关闭 metrics，其他 eviction 参数保持默认。

最初的 8 worker/segment 粗扫在约 58K logical task/s 处先撞到 client worker 上限：
Put commit delay 会占住同步 worker。下表改用 16 worker/segment，避免把 client
并发限制误报成服务端最大吞吐。10 秒样本也会高估持续容量，因此最大通过点和相邻
失败点均使用 30 秒窗口；通过点另做两轮相同 workload 复测。

### p99 不超过 1 秒的最大通过点

`offered logical` 是 client 每秒计划的 Exist/Get/Put transaction 数；`completed
logical` 用总完成数除以包含 drain 的完整 elapsed time。`actual RPC` 只统计真正调用
服务端的 RPC：如果 PutStart 没有返回任何 placement，client 不再发送 PutEnd，因此它
低于假设每个 Put 都执行 Start+End 的目标 business RPC QPS。`committed keys` 和 Put
key 成功率单列，避免失败快速返回抬高表面吞吐。

| 实现 | 最大通过 factor | offered logical/s | completed logical/s | actual RPC/s | committed keys/s | Put key 成功率 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Cakemaster | 2,950x | 101,804 | 98,747 | 114,268 | 639,907 | 52.70% |
| C++ Mooncake + jemalloc | 1,750x | 60,392 | 59,976 | 62,103 | 80,520 | 10.92% |

| 实现 | end-to-end avg | p50 | p95 | p99 | p99.9 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Cakemaster | 152.786 ms | 27.041 ms | 782.756 ms | 928.468 ms | 950.677 ms | 956.479 ms |
| C++ Mooncake + jemalloc | 148.443 ms | 84.929 ms | 469.574 ms | 663.856 ms | 734.288 ms | 751.560 ms |

对应的服务端操作 p99 如下。Put transaction 包含 commit delay；其余为单次 batch RPC
service time，不包含 client queue wait。

| 实现 | Exist | Get | PutStart | PutEnd | Put transaction |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cakemaster | 0.744 ms | 0.794 ms | 1.122 ms | 0.858 ms | 2.611 ms |
| C++ Mooncake + jemalloc | 6.048 ms | 6.851 ms | 5.847 ms | 0.666 ms | 6.016 ms |

在这个 SLO 限制下，Cakemaster 的 completed logical throughput 是 C++ 的 1.65x，
actual RPC throughput 是 1.84x。有效提交 key 吞吐是 7.95x，但这部分倍率同时包含
C++ 在持续 eviction/admission 压力下更低的 Put 成功率，不能解释成纯 RPC 执行性能。

相邻档位确认了边界，而不是 client 限速：Cakemaster 3,000x 完成 99,517 logical/s，
仅 queue dispatch p99 已达 1.185 秒；C++ 1,800x 完成 59,894 logical/s，dispatch
p99 为 1.101 秒。端到端延迟不会低于 dispatch，因此两档均明确失败。当前扫描精度下
边界分别为 `[2,950x, 3,000x)` 和 `[1,750x, 1,800x)`。Cakemaster 2,950x 已靠近
1 秒线；需要为生产抖动留余量时，2,900x 的 30 秒 dispatch p99 为 654 ms。

通过点的另两轮复测中，Cakemaster completed logical throughput 为
98,725/98,945 ops/s，dispatch p99 为 904/865 ms；C++ 为
59,426/59,616 ops/s 和 636/333 ms。重复样本都满足 SLO，但仍应把这些数据视作这台
未绑核机器的容量基线，不外推成跨硬件固定倍率。

Cakemaster 在 20M `expected-objects` 容量提示下的采样 VmHWM 为 13.2 GiB，C++ 为
1.79 GiB。该参数会预留 object index 容量，本轮目标是排除运行时扩容对最大吞吐的
干扰，并未做等内存优化；因此这组数据不能用于声明内存效率。服务端峰值 CPU 采样约为
709% 和 1,017%。

### 复现参数

Cakemaster server：

```bash
cargo build --release --bin cakemaster
target/release/cakemaster \
  --listen 127.0.0.1:50450 \
  --max-allocator-nodes-per-segment 1000000 \
  --expected-objects 20000000 \
  --object-collection-budget-per-step 2688 \
  --log-level off
```

C++ server：

```bash
/path/to/Mooncake/build/mooncake-store/src/mooncake_master \
  --rpc_port=50450 --rpc_thread_num=16 \
  --enable_metric_reporting=false
ldd /path/to/Mooncake/build/mooncake-store/src/mooncake_master | grep jemalloc
```

两个最大通过点分别使用下面的 per-segment QPS；其他参数保留上述 workload 值和 PR
#3147 默认的 3 个 segment：

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

## 当前 MVCC 实现的 Production 1:1:1 高压闭环（2026-08-17）

当前重实现使用与下一节 PR #19 样本相同的压力参数：115,056,180-byte Memory
segment、1 KiB 对象、0.90/0.85 high/low watermark，先预填 100K 对象并触达尾部
50K 热集，再预热 3 秒、测量 10 秒。`BatchPut`、`BatchGet`、`BatchExists` 各有独立
连接且各限速 300 batch QPS，batch size 为 333。服务端使用 8 个 Tokio worker，三轮
顺序执行，未绑核；测试机为 Apple M5（10 核）、32 GiB 内存，Rust 1.95.0、Apple
Clang 21.0.0。

三轮都准确完成每类 3,000 个 logical batch，中位 item rate 为 299,747 ops/s。由于
这是 900 logical batch QPS 的限速负载，吞吐代表目标完成度而非饱和上限；延迟表给出
三轮中位数，括号中保留完整范围：

| logical batch | p50 | p99 | p99.9 |
| --- | ---: | ---: | ---: |
| BatchPut（Start + End） | 903.167 us（785.625–1,123.292） | 2,641.125 us（1,411.125–2,679.667） | 8,911.250 us（2,390.583–11,300.292） |
| BatchGet | 493.958 us（426.333–680.125） | 2,034.833 us（1,009.500–2,098.958） | 5,868.208 us（1,716.666–8,259.875） |
| BatchExists | 412.917 us（373.875–528.000） | 1,680.458 us（879.459–1,716.917） | 8,000.625 us（1,661.042–8,479.666） |

每轮 999,000 个 PUT 均成功，`NO_AVAILABLE_HANDLE` 和其他错误均为 0；GET/EXISTS
也各完成 999,000 个 hit，零 miss/错误。三轮最大物理用量均为 115,055,616 bytes
（99.9995%），`watermark_triggered=true`。控制器物理回收对象数中位数为 1,297,511，
对应 1,328,651,264 bytes；退出时三轮的 `retired_bytes` 和显式、allocation、watermark
debt 均为 0。

停止流量后，一轮停在 84.9994% low，另两轮停在 88.8523%/89.4450% hysteresis band。
后者不是未完成的 debt：持续写入期间上一轮 high-to-low cycle 已结束，最后一段写入在
低于 high 时停止，因此不会启动新的水位周期。第 2、3 轮分别出现 135/128 次客户端
调度迟到，最大约 57 ms；对应的尾延迟明显高于无迟到的第 1 轮，所以上表同时保留范围，
不把桌面环境的三轮样本包装成稳定的尾延迟结论。

复现命令与下一节相同。当前结果证明 production composition 在持续 eviction 下能维持
目标 1:1:1 速率并完成全部写入；若要得到可横向比较的稳定尾延迟，应将 server/client
固定到不同物理核并增加轮次。

## PR #19 原实现的 Production 1:1:1 高压闭环（2026-08-17）

该样本来自基于 `e4ab526` 的 PR #19 原实现。当前
`object_catalog_rpc_benchmark_server` 同样不再拥有私有 eviction thread，而是直接构造
production `MooncakeServerComposition`。RPC listener 与 `MasterReconciler` 共用启动、
pressure wakeup、shutdown 和 join 生命周期；服务退出时若从未越过 high，benchmark 会
返回失败。

本轮使用 115,056,180-byte Memory segment、1 KiB 对象、0.90/0.85 high/low watermark。
先成功预填 100K 对象（约 89%）并触达尾部 50K 热集，然后预热 3 秒、测量 10 秒。
`BatchPut`、`BatchGet`、`BatchExists` 各有独立连接且各限速 300 batch QPS，batch size
为 333；这等于 900 logical batch QPS、约 299,700 item ops/s。未绑核，以下是一轮
高压闭环样本，适合记录原实现的策略验证和机器基线，不作为跨硬件或当前 MVCC
重实现的固定结论。

| logical batch | 完成数 | p50 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: |
| BatchPut（Start + End） | 3,000 | 1,260.430 us | 1,853.818 us | 2,017.554 us |
| BatchGet | 3,000 | 473.565 us | 1,577.689 us | 1,741.405 us |
| BatchExists | 3,000 | 279.819 us | 1,416.955 us | 1,492.927 us |

测量窗口三类请求数严格为 3,000:3,000:3,000，实际为 900 logical batch QPS、
299,761 item ops/s，调度迟到为 0。GET 和 EXISTS 各完成 999,000 个 hit，零 miss/错误。
PUT 尝试 999,000 个 item，其中 996,216 个成功，2,784 个在接近满容量时有界返回
`NO_AVAILABLE_HANDLE`，无其他错误；成功率为 99.72%。

这轮确实触发了水位闭环，而不只是消耗预留 headroom：最大采样用量为
115,055,616 bytes（99.9995%），`watermark_triggered=true`；控制器累计物理回收
1,299,737 个对象、1,330,930,688 bytes。停止写压力并继续 reconcile 后，最终用量为
97,797,120 bytes（84.9994%，不高于 low 的 97,797,753 bytes），此时
`retired_bytes=0`、两类 debt 均为 0、`settled_to_low=true`。期间记录 12,554 次
allocation failure wakeup，9,096 次有限重试且全部成功；失败路径仍只执行配置的有限
collector budget，没有请求路径无界扫描。

可复现命令（两个终端；client 完成后等待约 3 秒再向 server 发送 Ctrl-C，以打印稳定态
诊断）：

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

cargo build --release --bin object_catalog_rpc_benchmark_server

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

上述历史结果使用当时 benchmark server 的独立 Memory-only 压力控制器。当前 server
已经切换到 production high/low watermark composition；catalog 继续保留按
tenant/replica class 的 scoped reclaim filter，详见
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

- Rust 回收与并发 lookup：`src/bin/object_catalog_evict_benchmark.rs`
- Rust 自动水位压力：`src/bin/object_catalog_watermark_benchmark.rs`
- Rust 50:50：`src/bin/object_catalog_benchmark.rs`
- Mooncake direct benchmark：`interop/mooncake_object_catalog_benchmark.cpp`
- Mooncake 官方基线：`mooncake-store/benchmarks/batch_evict_bench.cpp`

ObjectCatalog 回收示例：

```bash
cargo run --release --bin object_catalog_evict_benchmark -- \
  --num_objects=100000 --evict_ratio_target=0.50 \
  --evict_ratio_lowerbound=0.25 --collect_budget=64 \
  --slot_cleanup_budget=0 --lookup_threads=8 --hot_objects=2048 --rounds=5
```

50:50 示例：

```bash
cargo run --release --bin object_catalog_benchmark -- 8 50000 8 3 2048
```
