# 实验性 F-Stack / DPDK TCP 后端

## 结论与范围

DPDK 是包收发框架，不是 TCP 协议栈。本实现使用 **F-Stack 的 FreeBSD TCP + DPDK**，
并让现有 coro_rpc connection driver 消费其异步字节流。

当前只建议作为实验后端，**不切换 production 默认值**：

- Linux、IPv4、单进程、单 polling core；仅服务端，Rust/C++ TCP client 不变。
- `coro-rpc` 与根 package 的 `dpdk` feature 默认关闭。开启 feature 也不需要 native SDK
  才能编译/单测；启动时显式加载本仓库 shim 构建的可信 `.so`，不会静默回退到内核 TCP。
- `benchmark` example、`mooncake_benchmark` 提供 `server-dpdk`。
  production `cakemaster` binary/composition 尚未接入，真实 ObjectCatalog 业务性能未测。
- 本机无线联网，有线口未连接，没有预留 hugepages。实测使用隔离 namespace 中的
  **DPDK AF_PACKET 软件 PMD**，不是 VFIO/物理网卡 kernel bypass。

## 2026-09-07 本机结果

Ryzen 7 9700X，Linux 7.2.2-1-cachyos，Rust 1.98.0 `--release`，GCC 16.2.1；
F-Stack `34065f1396c7695408066c4bccc9dc98c02f60dc` + 本仓库退出修补，bundled DPDK 24.11.6。
固定同一 Rust client：server CPU 2、client CPU 4（不同物理核），单连接；每项
30,000 次 warmup + 300,000 次计时请求，三轮取中位数，轮间交替后端顺序。
未锁频，也未隔离宿主的其他任务。Get/Exists 是现有 Mooncake **空业务** benchmark，batch=16。

两种路径均跨同一对 veth，而非用 loopback 对比另一种网络拓扑：

```text
内核：  Rust client → Linux TCP → veth → Linux TCP → 同一个 RPC driver/handler
DPDK：  Rust client → Linux TCP → veth → AF_PACKET PMD → F-Stack TCP → 同一个 RPC driver/handler
```

两端 veth 均关闭 checksum/GSO/GRO/TSO offload，避免把 Linux `CHECKSUM_PARTIAL`
或超 MTU GSO 包交给没有对应 offload 的用户态栈。两种服务端均启用 TCP_NODELAY。
F-Stack 使用 `pkt_tx_delay=0`、关闭 delayed ACK、64 KiB 初始 TCP 收发缓冲；完整配置已保存。
这比较的是两套具体 transport/runtime 配置，不是只改变网卡驱动的消融实验。

| 操作 | pipeline | Linux TCP QPS | F-Stack/AF_PACKET QPS | 中位数变化 |
| --- | ---: | ---: | ---: | ---: |
| add | 1 | 229,529 | 170,889 | -25.5% |
| add | 32 | 1,254,251 | 1,212,888 | -3.3% |
| add | 256 | 1,390,776 | 1,562,280 | +12.3% |
| BatchGet，16 keys | 1 | 122,362 | 111,066 | -9.2% |
| BatchGet，16 keys | 32 | 243,080 | 217,821 | -10.4% |
| BatchGet，16 keys | 256 | 266,853 | 265,178 | -0.6% |
| BatchExists，16 keys | 1 | 168,137 | 155,698 | -7.4% |
| BatchExists，16 keys | 32 | 663,397 | 510,248 | -23.1% |
| BatchExists，16 keys | 256 | 771,130 | 618,964 | -19.7% |

QPS 是 RPC/batch QPS，不是 key QPS。串行 add 平均每次完成耗时约 4.357 → 5.852 µs；
Get 约 8.172 → 9.004 µs，Exists 约 5.948 → 6.423 µs。
`pipeline>1` 的 `us_per_completion` 是总耗时/完成数，**不是单个请求延迟**；没有测 p50/p99。

各项服务进程 CPU 使用率的三轮中位数：内核约 30%–52%，F-Stack 约 82%–96%
（100% 为一个逻辑 CPU）。此计数包含 warmup，不包含客户端和全部系统软中断，不能视为
全机 CPU 开销。polling 后端也不会在空闲时节省一个核心。

**判断：此软件路径没有整体优势，暂不值得替换默认 TCP。** 唯一正向的 add/pipeline=256
结果也有明显噪声：内核三轮为 1.323M–1.614M，覆盖 F-Stack 的 1.558M–1.573M，
不能宣称稳定的 12% 提升。结果既不能证明、也不能否定物理 NIC DPDK 的收益；AF_PACKET
仍经过内核 packet socket/veth，有额外搬运成本，client 或编解码也可能限制吞吐。

后续补采的用户态 perf、串行延迟说明和 allocator 对照见
[`dpdk_perf.md`](dpdk_perf.md)：已确认当前接入循环与分配器慢路径有明显成本，
不能把全部差距直接归因于 DPDK/AF_PACKET。

原始数据：[`benchmarks/dpdk-af-packet-2026-09-07/`](benchmarks/dpdk-af-packet-2026-09-07/)，
含 54 条原始 client 输出、环境、配置、native probe 结果与汇总。

## 构建 native SDK（不安装到系统）

依赖：Linux C toolchain、make、pkg-config、Ninja、Python/uv、NUMA/OpenSSL 开发文件；
DPDK 按本机探测到的可选库还可能链接 libbsd、libarchive、libbpf 等。
以下命令在仓库根执行，所有 SDK 输出都放在单独的 cache 目录。

```bash
export work="$HOME/.cache/cakemaster-dpdk"
export FF_PATH="$work/f-stack"
mkdir -p "$work"
git clone --depth 1 https://github.com/F-Stack/f-stack.git "$FF_PATH"
git -C "$FF_PATH" fetch --depth 1 origin 34065f1396c7695408066c4bccc9dc98c02f60dc
git -C "$FF_PATH" checkout --detach 34065f1396c7695408066c4bccc9dc98c02f60dc
uv venv "$work/venv"
uv pip install --python "$work/venv/bin/python" meson==1.12.0 pyelftools==0.33
export PATH="$work/venv/bin:$PATH"
meson setup "$FF_PATH/dpdk/build" "$FF_PATH/dpdk" \
  --prefix="$work/install" --libdir=lib \
  -Denable_kmods=false -Dtests=false -Dexamples= -Ddisable_apps='*' \
  -Denable_drivers=net/af_packet,net/tap,net/virtio,net/bonding,net/ring
ninja -C "$FF_PATH/dpdk/build" -j8
ninja -C "$FF_PATH/dpdk/build" install
export PKG_CONFIG_PATH="$work/install/lib/pkgconfig"
# 首次构建前若该 checkout 曾用非 PIC 配置编译，先 make -C "$FF_PATH/lib" clean。
bash interop/fstack/build.sh "$work/libcakemaster_fstack.so"

cargo build --release -p coro-rpc --features dpdk --examples
cargo build --release --features dpdk --bin mooncake_benchmark
```

`build.sh` 检查 F-Stack revision/DPDK version，为 F-Stack 加 `-fPIC`，把静态 DPDK PMD
链接进共享库，并应用 [`eal-argv.patch`](../interop/fstack/eal-argv.patch)。固定的上游版本
把 owning `dpdk_argv` 数组直接交给 EAL/getopt，后者会重排、重复指针，导致
`ff_unload_config` 在正常退出时 double-free。补丁让 EAL 借用独立的指针数组，保留原始
所有权数组；本机实际复现过故障并验证了修补后的正常退出。

## 隔离验证和重复测量

需要允许非特权 user namespaces，以及 `unshare`、`ip`、`mount`、`ethtool`、`taskset`、Python。
不需要 sudo、Docker、VFIO、宿主 hugepages 或宿主网卡配置变更。脚本创建临时 user/mount/net
namespaces、私有 `/run` 和 veth；退出后不保留网络设备。输出目录必须为空。

```bash
# 仅 native correctness probe：512 KiB attachment、256 并发延时 handler、扩展错误、重连、SIGINT 退出
PROBE_ONLY=1 bash interop/fstack/compare.sh \
  "$work/libcakemaster_fstack.so" "$work/probe-new"

# 本次测量的完整参数；脚本会先跑 correctness probe，再开始计时
SERVER_CPU=2 CLIENT_CPU=4 ROUNDS=3 ITERATIONS=300000 WARMUP=30000 \
PIPELINES=1,32,256 bash interop/fstack/compare.sh \
  "$work/libcakemaster_fstack.so" "$work/results-new"
```

脚本输出 `samples.json`、`summary.json`、`environment.json` 及每个 server 的完整日志。
启动就绪条件是成功的 TCP/RPC，不是固定 sleep；错误/超时/非零退出会终止测量。
CPU 编号可调整，但应选择不同物理核，而不是同一核的 SMT siblings。

单独启动接口（需自行准备 INI 对应的网络环境，不能把 F-Stack 地址配置到 Linux 本地接口）：

```bash
target/release/examples/benchmark server-dpdk \
  "$work/libcakemaster_fstack.so" config.ini 198.18.0.2:19092
# example 最后还可加 seconds，验证 Tokio timer 驱动的定时退出。
target/release/mooncake_benchmark server-dpdk \
  "$work/libcakemaster_fstack.so" config.ini 198.18.0.2:19094
```

## 接入结构与限制

- `RpcServer::into_connection_handler()` 冻结共享路由；`handler.serve(stream, peer)` 使用
  同一个泛型 connection driver。默认 TCP 路径仍单态化，没有额外 trait-object socket 调用。
- F-Stack fd 不是 OS fd，绝不交给 Tokio epoll/libc close。
  私有 socket 用 `Rc` 保证 `!Send/!Sync`，在初始化线程使用 `ff_accept/read/write/close`。
  处理 nonblocking、部分读写、EOF 和 EAGAIN；frame 限制及单连接背压沿用现有 driver。
- `ff_run` 每轮推进本地 Tokio runtime。必须在轮询应用 **之后** yield，以处理 Tokio
  cooperative-budget 的延后唤醒；否则 256 个异步 handler 会丢唤醒停住。已有无 SDK
  单测及 native probe 覆盖此回归。
- 当前是简单 scan reactor：EAGAIN 后安排下一轮 poll，默认最多 1024 连接，accept/completion
  有 poll budget。尚无 `ff_epoll` readiness reactor、跨核 RSS、多进程 sharding、零拷贝 mbuf
  API。不能据此推断高连接数扩展性。INIs 中的多核/thread-mode 配置会被拒绝。
- callback 捕获 panic；关闭时先 drop 连接/handler，再 `ff_stop_run`，避免在 native EAL
  清理后调用 `ff_close`。native 库保留加载到进程结束，且一个进程只允许初始化一次。
  上游初始化错误可能直接 `exit()`；这不是支持任意热启停、热加载的 production SDK。
- `FStack::load` 是 unsafe 边界：调用者必须提供可信、匹配 shim ABI 的 native 库，且进程
  内不得有其他代码操作 F-Stack。不要加载不可信路径；`run` 是阻塞入口，不能嵌套 Tokio runtime。

验证命令：

```bash
cargo test --workspace --all-targets --all-features
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

本机完整测试通过。Rust 1.98 的严格 Clippy 在原有 `upstream_golden.rs` 的
`chunks_exact_to_as_chunks` lint 处失败；仅豁免该既有 lint 后，workspace all-targets/all-features
检查通过。native correctness probe 另行真实运行，不用 kernel socket mock 冒充 DPDK。

下一步若要决定生产采用：准备两机有线链路和受支持的专用 NIC，在等核数、等 NUMA、等
MTU/offload、等客户端负载下比较内核 TCP 与物理 DPDK PMD 路径；记录吞吐、p50/p99、完整
CPU/软中断成本和多连接饱和点，再运行真实 ObjectCatalog workload。当前默认构建只包含
软件 PMD，物理测试还需选择对应硬件驱动；不能直接复用本表作为硬件测试结论。

参考：[F-Stack API](https://github.com/F-Stack/f-stack/blob/34065f1396c7695408066c4bccc9dc98c02f60dc/doc/F-Stack_API_Reference.md)、
[DPDK AF_PACKET PMD](https://doc.dpdk.org/guides-24.11/nics/af_packet.html)。
