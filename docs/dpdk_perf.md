# DPDK 实验后端：延迟与 perf 诊断（2026-09-07）

## 延迟没有改善

以下是前一组**未启用 perf**、三轮串行测试（pipeline=1）的平均 RPC 完成耗时，
由总计时/完成数计算。Get/Exists 是 batch=16、空业务 handler。

| 操作 | Tokio/Linux TCP | F-Stack/AF_PACKET | 耗时变化 |
| --- | ---: | ---: | ---: |
| add | 4.357 µs | 5.852 µs | +34.3% |
| BatchGet | 8.172 µs | 9.004 µs | +10.2% |
| BatchExists | 5.948 µs | 6.423 µs | +8.0% |

这包含客户端调度、编解码和 RPC，不是纯网络 RTT。**没有测 p50/p99**；流水线模式的
`us_per_completion` 也不能当作单个 RPC 延迟。原始对照见 [`dpdk.md`](dpdk.md)。

## perf 怎么采的

保留相同 release binaries、native library、veth/offload 配置和固定 Rust client。
server/client/perf helper 分别绑定 CPU 2/4/6（不同物理核）。分别采集：

- add/pipeline=1：1,000,000 RPC；
- BatchExists/pipeline=32、256：各 3,000,000 RPC；
- BatchGet/pipeline=32：1,000,000 RPC；
- 空闲、无 client connection：3 秒。

每个负载先单独 warmup 100,000 次，再分别运行 `perf stat` 和 `perf record`，两者不同时
争用 PMU。使用 control pipe 的 enable/disable ACK 划定计数窗口，排除 native 初始化、
warmup 和退出。窗口包含计时 client 的启动/连接和结束，负载持续约 4–7 秒；不是严格的
逐请求 tracing。`record` 使用 `cycles:u`、997 Hz、DWARF 16 KiB stack。

**权限和解释边界：** `perf_event_paranoid=2`、`kptr_restrict=2`，实际尝试 `cycles:k`
被拒绝。没有修改宿主 sysctl，也没有采到可用的内核、软中断或 off-CPU profile。
下文百分比是 **server 用户态 cycles 的 flat/self 样本占比**，不是整个请求墙钟耗时。
F-Stack 把 TCP 工作移到用户态，而 Linux TCP 的那部分不在此 profile 内；不能把两端
`cycles:u` 直接当作完整 TCP CPU 成本对比。部分 DWARF 调用栈截断，也存在少量 unknown
采样点，不能把 inclusive/children 百分比相加归因。

perf 本身会扰动性能，尤其频繁 context switch 的串行负载：本次内核 add 的 stat/record
QPS 与未插桩值有明显差异。因此上方延迟表仍使用未插桩结果，而不拿 profile 期间的
QPS 宣称延迟变化。

## 找到的主要热点

### 1. 分配器的 arena 加锁路径是实际吞吐瓶颈之一

默认配置的 F-Stack BatchExists/pipeline=32：

| flat/self 符号 | 用户态 cycles 占比 |
| --- | ---: |
| `__libc_malloc2` | 21.50% |
| `_int_free_chunk` | 11.54% |
| 两者合计 | **33.04%** |

pipeline=256 两者合计约 **40.45%**。相同 workload 的内核/pipeline=32 两者合计约 1.15%，
其主要可见热点是 UTF-8 检查、RPC handler/codec 和其他分配器路径。

最初 libc 被 strip，热点显示为地址。通过匹配 build ID
`46554afd5282a7de42075035b9bdaac8968c6e03` 的 glibc debuginfo 完成符号化，并检查了反汇编：
`__libc_malloc2` 的热点落在 arena mutex 的 `lock cmpxchg` / `xchg` 附近。

F-Stack/DPDK 初始化会创建辅助 pthread，glibc 不能再使用单线程进程的免锁分支；
Rust-only current-thread benchmark 没有同样的线程形态。batch 请求又有大量 String/Vec
分配与释放。这与观察到的 arena 慢路径吻合。但这里说的是**加锁成本**，不等于证明
有多个线程持续争抢同一把锁；仅凭 samples/skid 不能推出锁竞争或每个 allocation 的调用点。

还有一个范围限制：production `src/main.rs` 使用 MiMalloc，而当前两个 benchmark binary
使用系统 allocator。因此这个热点不能直接外推为 production ObjectCatalog 的瓶颈。

### 2. 目前的 Tokio/F-Stack 接入循环有明显额外开销

本实现每个 F-Stack tick 都调用 `Runtime::block_on`，轮询连接后 yield，以驱动 Tokio
reactor 并保留 cooperative-budget 的延后唤醒。profile 中可见大量：

- runtime context 的 set/restore、take_core、CoreGuard drop；
- `process_at_time` / `park_internal`；
- waker clone/register/wake、Notify 和 runtime RNG/metrics 维护。

在**完全空闲**的 F-Stack profile 中，按 flat/self 的 Tokio 相关符号归类约 **69%**；
3 秒窗口内进程消耗约 3 秒 CPU，Linux 监听器则接近 0。串行 add 中同类符号约 40%
（“符号含 Tokio”是粗分组，不意味着该比例全部可以消除）。

此外，当前 scan reactor 每轮 `ff_accept`，没有 pending connection 也会走
`kern_accept4 → _falloc_noinstall → fdclose` 等路径。空闲 profile 还显示
`__tcp_run_hpts` 约 5.7%。所以目前忙轮询并不只是“在等网卡包”，也在反复做 runtime、
listener 和 TCP timer 的工作。

这支持优先优化**接入 reactor**，而不是直接断言 DPDK TCP 慢。无 SDK 单测已经证明
不能简单删掉 yield：那会丢失 Tokio 的延后唤醒，使 256 个异步 handler 停滞。

### 3. AF_PACKET 仍有内核网络成本，但这次没有量化它

native profile 能看到 `process_packets`、TCP input/output、`ip_output`、`ipfw_chk` 等。
但 AF_PACKET 的 kernel socket/veth 收发、拷贝、软中断开销受权限限制不可见。
因此“AF_PACKET 不是物理 NIC kernel bypass”是确定的路径事实，**它解释了多少性能差距，
本次 perf 不能给出比例**。

## 做了一个可控对照，而不只看热点

只对 **server** 设置：

```text
GLIBC_TUNABLES=glibc.malloc.tcache_count=4096
```

client、代码、SDK、TCP 参数、CPU 绑定均不变。默认与大 tcache 两组顺序在三轮中交替；
BatchExists、batch=16、pipeline=32/256，各 warmup 30,000 + 1,000,000 RPC。
**吞吐对照期间不运行 perf**，下面取三轮中位数：

| 后端 | pipeline | 默认 QPS | 大 tcache QPS | 提升 |
| --- | ---: | ---: | ---: | ---: |
| Linux TCP | 32 | 656,645 | 718,717 | +9.5% |
| F-Stack/AF_PACKET | 32 | 513,267 | 598,732 | **+16.7%** |
| Linux TCP | 256 | 769,420 | 856,141 | +11.3% |
| F-Stack/AF_PACKET | 256 | 618,757 | 753,829 | **+21.8%** |

随后对大 tcache 的 native/pipeline=32 单独重采 perf：前述两个分配器慢路径从
33.04% 降到 **不到 1%**（`_int_free_chunk` 0.64%，`__libc_malloc2` 低于 report 的 0.2% 阈值）。
这为“分配器慢路径确实造成了部分吞吐损失”提供了对照证据，不只是相关性猜测。

但两边都调整后，F-Stack 仍慢约 16.7%（pipeline=32）和 12.0%（256），
不能宣称 allocator 已经解释或解决全部问题。这里也**没有重新测串行/p99 延迟**。
大 tcache 会增加缓存内存/保留量，这只是诊断实验，不作为新的默认值或生产建议。

## 复现与原始结果

先按 [`dpdk.md`](dpdk.md) 构建。在原有 namespace 脚本后加 `profile`：

```bash
export work="$HOME/.cache/cakemaster-dpdk"
# 若 libc 热点显示裸地址，需要匹配的 debuginfo；只写用户 perf cache。
perf buildid-cache --update /usr/lib/libc.so.6 --debuginfod

LC_ALL=C SERVER_CPU=2 CLIENT_CPU=4 PERF_CPU=6 \
  bash interop/fstack/compare.sh "$work/libcakemaster_fstack.so" "$work/perf-new" profile
```

输出 `.data`、flat/callgraph reports、`stat.csv`、server/client 日志、二进制 SHA-256 和
`runs.json`。可用 `PROFILE_CASES=exists-p32`、`PROFILE_BACKENDS=dpdk-af-packet` 缩小范围。
server-only tunable 对照（每次用新的空输出目录；正式对照建议轮间交替两组顺序）：

```bash
# 默认组
LC_ALL=C ROUNDS=3 WORKLOADS=exists PIPELINES=32,256 ITERATIONS=1000000 WARMUP=30000 \
  SERVER_GLIBC_TUNABLES='' bash interop/fstack/compare.sh \
  "$work/libcakemaster_fstack.so" "$work/allocator-default-new"

# 大 tcache 组；环境参数只注入 server，固定 client 不受影响
LC_ALL=C ROUNDS=3 WORKLOADS=exists PIPELINES=32,256 ITERATIONS=1000000 WARMUP=30000 \
  SERVER_GLIBC_TUNABLES=glibc.malloc.tcache_count=4096 bash interop/fstack/compare.sh \
  "$work/libcakemaster_fstack.so" "$work/allocator-tcache-new"

# 重采优化组的用户态 profile
LC_ALL=C PROFILE_CASES=exists-p32 PROFILE_BACKENDS=dpdk-af-packet \
  SERVER_GLIBC_TUNABLES=glibc.malloc.tcache_count=4096 bash interop/fstack/compare.sh \
  "$work/libcakemaster_fstack.so" "$work/perf-tcache-new" profile
```

文本原始数据和 24 条 allocator 对照样本保存在
[`benchmarks/dpdk-perf-2026-09-07/`](benchmarks/dpdk-perf-2026-09-07/)。
体积较大的原始 perf.data 留在本机 `$work/perf-results-2/` 与 `$work/perf-tcache4096/`，
未提交到仓库。

后续优先级：在保持取消/计时器/背压正确性的前提下改进 readiness reactor 和 Tokio 接入，
用 production allocator/真实业务重测；再补 p50/p99 和允许 kernel/off-CPU tracing 的
完整 profile，最后在物理 NIC 上比较等 CPU 预算的两条路径。
