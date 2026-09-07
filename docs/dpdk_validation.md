# DPDK PR 提交前验证记录

验证时间：2026-09-07 19:30（UTC+08:00）。本机 Linux 7.2.2-1-cachyos、Rust/Cargo 1.98.0、
clang-format 22.1.8。代码已 rebase 到 `main` 的 `6edcf65`，受测实现提交为
`e6a5351e62d41003726a448be4acf466dc04b68b`；后续仅补充文档和测试记录。

这是**本机验证记录，不是 GitHub CI 状态**。逐项命令、退出码、计数和诊断保存在
[`checks.json`](benchmarks/dpdk-validation-2026-09-07/checks.json)，完整 Rust 测试日志和
Clippy/native probe 输出也在[同一目录](benchmarks/dpdk-validation-2026-09-07/)。

## 功能与构建检查

| 检查 | 结果 |
| --- | --- |
| `cargo test --workspace --all-targets --all-features` | **191 passed / 0 failed / 0 ignored** |
| `cargo test --workspace --all-targets` | **187 passed / 0 failed / 0 ignored** |
| `cargo fmt --all --check` | 通过 |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | **未通过：原有文件触发一条新 lint，见下文** |
| 上一命令另加 `-A clippy::chunks_exact_to_as_chunks` | 通过，未豁免其他 lint |
| `cargo build --release -p coro-rpc --features dpdk --examples` | 通过 |
| `cargo build --release --features dpdk --bin mooncake_benchmark` | 通过 |
| `interop/fstack/build.sh` | 在已构建的 pinned SDK 上重新构建 shim 成功 |
| `PROBE_ONLY=1 ... compare.sh` | 真实 F-Stack/AF_PACKET probe 通过 |
| `... compare.sh ... profile` | kernel/native 两后端的 stat/record 控制和报告生成冒烟通过 |
| 两个 shell 脚本 `bash -n`、shim 的 clang-format dry-run、PR 全量 `git diff origin/main --check` | 通过 |

严格 Clippy 的诊断为：

```text
crates/coro-rpc/tests/upstream_golden.rs:24:10
error: using `chunks_exact` with a constant chunk size
clippy::chunks_exact_to_as_chunks
```

入库文本日志/report 仅清理行尾空白和末尾空行，不改变指标、符号或测试输出内容。
统一 diff 补丁的空 context 行必须保留单个空格，由 `.gitattributes` 为该 patch 单独声明。

`upstream_golden.rs` 未被本 PR 修改。未为此变更修改历史 golden test，也没有把严格检查标记为通过；
只在另一次诊断运行中豁免此单一 lint。原始失败和豁免后输出均保存。

## 新增路径的覆盖

无需 native SDK 的 Rust 测试覆盖：

- 外部 async 字节流复用原 RPC driver，31-byte duplex buffer 强制部分读写；
- 流水线响应、128 KiB attachment、扩展错误、peer context、half-close/EOF；
- drop connection 关闭底层流；
- IPv4 地址的网络字节序、缺失 native 库的加载失败；
- 256 个异步 timer future 的 cooperative-budget 延后唤醒不丢失；
- callback 捕获 panic，停止 native loop 前释放 application。

真实 native probe **不是 kernel socket mock**：

```text
probe_ok: timers, 512KiB attachment, pipeline=256, extended error, reconnect
```

probe 在临时 user/mount/network namespace 中使用 veth + DPDK AF_PACKET，验证
512 KiB attachment、带 1ms timer 的 handler、256 并发请求、扩展错误、三次连接循环和
SIGINT 正常退出。没有解绑宿主网卡，也没有更改宿主 hugepages/sysctl。

提交前 perf 脚本冒烟参数为 `PROFILE_CASES=add-p1`、
`PROFILE_BACKENDS=kernel,dpdk-af-packet`、`PROFILE_ITERATIONS=10000`；它验证采集控制和
报告链路，不作为新的性能测量结果。

## 性能结果摘要（原始测量，不是提交前重跑）

完整结果和复现命令分别见 [`dpdk.md`](dpdk.md)、[`dpdk_perf.md`](dpdk_perf.md)。
同日首次实现上的 **54 条吞吐样本**、**24 条 allocator 对照样本**、perf counters/flat
reports 已原样纳入仓库；补充报告时校验了样本数与所有三轮中位数。
本次提交前复验没有重新跑完整性能矩阵，不用新构建二进制的哈希替换历史测量记录。

| 指标 | 结果 |
| --- | --- |
| BatchGet，16 keys，pipeline=1/32/256 | F-Stack/AF_PACKET 比内核 TCP 低 0.6%–10.4% |
| BatchExists，16 keys，pipeline=1/32/256 | 低 7.4%–23.1% |
| 串行 add 平均 RPC 完成耗时 | 4.357 → 5.852 µs |
| 串行 BatchGet / BatchExists | 8.172 → 9.004 µs / 5.948 → 6.423 µs |
| allocator 对照，仅改变 server tcache | native 流水线吞吐提升 16.7%–21.8%，双方调整后 native 仍落后 |
| native Exists/pipeline=32 用户态 perf | malloc/free 加锁慢路径约 33%；大 tcache 对照后降到不足 1% |

add/pipeline=256 的单项中位数为 +12.3%，但基线范围覆盖 native 范围，不作为稳定收益。
没有测 p50/p99；pipeline>1 的 `us_per_completion` 不是逐请求延迟。perf 受本机权限限制，
仅有用户态采样，未量化内核/软中断/off-CPU 成本。

## 未验证／未实现

- 物理 NIC/VFIO kernel bypass、两机实网、多核/RSS、高连接数扩展性；
- production ObjectCatalog 业务和 MiMalloc 配置下的性能；
- IPv6、DPDK client、production composition 接入、native 栈反复初始化/热切换。

此 PR 是默认关闭的单核 Linux/IPv4 **实验后端**；默认 production Tokio TCP 不变。
