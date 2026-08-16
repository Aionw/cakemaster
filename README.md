# cakemaster

Cakemaster workspace 包含高并发 object catalog、异构 segment/placement 核心库，以及基于 Tokio 的 yalantinglibs `coro_rpc` v0 兼容实现，可让 Rust 与 C++ `coro_rpc` 客户端/服务端通过 TCP 直接互调。

兼容基线为 `alibaba/yalantinglibs` 的 `c1cef74057b139944c982d840c09c9940f26e08e` 提交。协议实现依据该版本的 [`coro_rpc_protocol.hpp`](https://github.com/alibaba/yalantinglibs/blob/c1cef74057b139944c982d840c09c9940f26e08e/include/ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp) 和 [`struct_pack`](https://alibaba.github.io/yalantinglibs/en/struct_pack/struct_pack_layout.html)。

## 已实现

- coro_rpc v0 请求头（20 字节）和响应头（16 字节）
- 与 `coro_rpc::func_id` 一致的函数 ID：C++ 完整函数名的 MD5 前 32 位
- struct_pack release/debug 元信息、类型哈希和全局容器长度宽度
- 固定宽度整数、浮点、`bool`、`char32_t`、`std::monostate`
- `std::string`、顺序容器、定长数组、`optional`、`expected`、map、set
- 1–8 元 tuple/pair，以及通过宏声明的 `YLT_REFL` 风格结构体
- 底层 typed `RpcMethod<Request, Response>` API，route ID 与请求/响应 schema hash 只计算一次
- 基于 Thrift IDL 的 Rust-only codegen，route ID 与 schema hash 在构建时固化
- 生成业务 struct、i32/u8-backed enum、union-backed data enum、`tl::expected` 语义 union、typed client、server trait 与整组服务注册代码
- Tokio 多路复用客户端、流水线并发服务端、请求/响应 attachment
- 基于 `tokio-util::codec` 的流式拆帧，以及 `Bytes` payload 零拷贝切分
- 私有 typed route registry 与 erased handler 执行边界
- poll-based connection driver：一个连接 future 同时驱动读、handler futures、写入和 flush
- tarpc 风格的 client handle/dispatch、bounded backpressure 与 drop/timeout cancellation
- 基于 `CancellationToken`/`TaskTracker` 的连接级优雅关闭
- 标准错误码和大于 255 的扩展错误码
- 入站帧大小、容器大小和单连接并发上限
- 真实 ObjectCatalog/SegmentPool 驱动的 Mooncake `Ping`、`MountSegment`、
  `ReMountSegment`、`UnmountSegment`、`GracefulUnmountSegment`、single/batch exists/get
  与 batch put、single/batch upsert、single/batch/regex/all remove RPC adapter，以及 client
  初始化所需的 `ServiceReady`/`GetStorageConfig`

暂不包含 TLS/NTLS、RDMA/CUDA transport、struct_pack varint 配置、IDL 外的自定义 variant/多态指针，以及 C++ 未使用 `YLT_REFL` 的 ABI/padding 结构体。大二进制建议放在 coro_rpc attachment 中，无需经过 struct_pack。

## 直接运行

仓库以根 `cakemaster` package 作为完整 server product；只有可独立复用的 RPC runtime 和
codegen 保留为 workspace 子 crate：

```text
build.rs + idl/             # Mooncake Thrift contract 与唯一的生成入口
src/                        # 领域模块、生成契约门面、server composition 和主 binary
src/bin/                    # 诊断与性能工具
tests/                      # core、wire、RPC 和 production runtime 集成测试
crates/coro-rpc/src/        # 可复用的 RPC 协议、client 与 server
crates/coro-rpc-codegen/    # Thrift AST 校验与 Rust stub 生成
crates/coro-rpc/tests/      # RPC crate 的兼容与端到端测试
crates/coro-rpc/examples/   # RPC crate 的 benchmark
```

根 package 内仍保持 `server → client/object/segment + proto → coro-rpc` 的模块依赖方向；
领域模块不导入 Tokio、RPC 或 codegen API，但不再用额外 Cargo package 人为隔离。
领域 API 通过 `object`、`segment` 两个门面暴露；错误、回收控制、placement 和诊断类型位于各自的具名子模块，内部实现文件保持私有。

SegmentPool 对 Memory、CXL、NoF 和 LocalSSD 的领域建模与扩展约束见 [`docs/segment_pool_backends.md`](docs/segment_pool_backends.md)。
ObjectManager、ReplicaAllocator、异步 RPC 边界和当前 Mooncake 兼容子集见
[`docs/object_catalog_rpc.md`](docs/object_catalog_rpc.md)。
与当前上游 C++ Mooncake Store 的完整功能差距、最新 wire 漂移和建议实施顺序见
[`docs/mooncake_feature_gap.md`](docs/mooncake_feature_gap.md)。
可选的 tenant namespace、Memory/NoF quota、RAII 计费与定向回收见
[`docs/tenant_quota.md`](docs/tenant_quota.md)。

核心类型保持短路径，扩展接口按职责导入：

```rust
use cakemaster::object::{ObjectCatalog, ObjectCatalogConfig, ObjectIdentity};
use cakemaster::object::error::LookupError;
use cakemaster::object::reclamation::{CatalogTick, CollectBudget};
use cakemaster::segment::{SegmentSpec, SegmentPool};
use cakemaster::segment::placement::{PlacementRequest, ReplicaAllocator};
```

值对象和 ID 使用私有字段加构造/读取方法来维持语义边界；统计快照使用公开字段。配置修改采用消费式 `with_*` builder，不提供共享可变 setter。

启动可部署的 Mooncake RPC 服务端：

```bash
cargo run --release -- \
  --listen 127.0.0.1:50051 \
  --eviction-high-watermark 0.90 \
  --eviction-low-watermark 0.80
```

不传参数时默认监听 `127.0.0.1:50051`；也可传 `--listen 127.0.0.1:0` 让系统选择
测试端口。RPC access 日志默认关闭；传 `--access-log` 后，每个完成的请求会以 info 级别
记录来源地址、路由名/function ID、sequence、结果、请求/响应大小和耗时。通用库调用方也可
使用 `ServerConfig::default().with_access_log(true)` 开启。binary 在同一个 composition
root 中只构建一次 `SegmentPool`、内存态
`ObjectManager`、`MasterClock` 和 `ObjectCatalogRpcService`，并从 service 派生共享
`ClientManager`、clock、deadline `Notify` 和 memory-pressure `Notify` 的
`MasterReconciler`。production manager 默认用 90%/80% 高低水位驱动有界 Memory eviction；
水位、物理容量/used、live/retired/debt accounting、allocation-failure 有限重试与诊断详见
[`docs/memory_eviction.md`](docs/memory_eviction.md)。RPC server 与 reconciler
并发运行；Unix 上 Ctrl-C/SIGTERM、其他平台上 Ctrl-C 会通知两者停止，进程等待监听器、
所有连接 task 和 reconciler 完整退出后才返回。

运行日志默认同时写入 stderr 和 `logs/cakemaster.log`，使用带系统时区的 ISO 8601
时间戳。日志文件每天滚动一次，单文件达到 100 MiB 时也会提前滚动，最多保留 14 个
文件。滚动文件名形如 `cakemaster.2026-08-16.1.log`。日志等级、模块过滤、目录和输出
目标均可通过命令行配置：

```bash
cakemaster \
  --log-filter 'info,cakemaster::server=debug,coro_rpc=warn' \
  --log-dir /var/log/cakemaster \
  --log-output both
```

只需全局等级时可使用 `--log-level debug`；它与 `--log-filter` 互斥。`--log-output`
接受 `stderr`、`file` 或 `both`，选择 `stderr` 时不会创建日志目录。环境变量
`RUST_LOG`、`CAKEMASTER_LOG_LEVEL`、`CAKEMASTER_LOG_DIR` 和
`CAKEMASTER_LOG_OUTPUT` 提供相同配置能力，优先级为命令行、环境变量、内置默认值。
过滤器在进程启动时解析，当前不支持运行时热更新。常用的稳定日志 target 包括：

- `coro_rpc::access`：通过 `--access-log` 显式开启的逐请求 access 日志；
- `coro_rpc::server::connection`：连接建立、关闭和帧/协议故障；
- `coro_rpc::server::request`：请求方法、peer、sequence、耗时和 RPC transport 错误；
- `coro_rpc::client::{connection,request}`：客户端连接驱动错误和请求超时；
- `cakemaster::server::rpc::{business,mapping}`：Mooncake 业务错误摘要及内部错误映射；
- `cakemaster::client::{lifecycle,manager}`：client generation 状态转换和 cleanup/unmount 重试；
- `cakemaster::object::{manager,eviction}`、`cakemaster::server::{reconciler,eviction}`：领域
  invariant、allocation-failure bounded work、后台收敛与 memory watermark/debt 诊断。

例如，保留默认业务日志、打开 RPC 请求完成记录并压低正常连接日志：

```bash
cakemaster --log-filter \
  'info,coro_rpc::server::request=debug,coro_rpc::server::connection=warn,cakemaster::server::rpc::business=debug'
```

成功 RPC 只在 `DEBUG` 记录（包含耗时），收到请求的 payload 大小只在 `TRACE` 记录；
协议/RPC 失败进入 `WARN`。未打开请求 `DEBUG` 时，默认热路径不会为每个成功请求读取时钟。
业务 `InternalError` 进入 `ERROR`，容量或暂时不可用进入 `WARN`，参数、not-found 等调用方
可处理的拒绝进入 `DEBUG`。batch 只记录聚合错误数，不记录 object key，避免高基数和敏感
数据进入日志。Access 日志同样受等级和模块过滤规则控制。

当前 production 默认值是保守的单进程、单租户内存态配置：最多 65,536 个 client 和预期
65,536 个 object，client TTL/lease TTL 均为 10 秒，pending write timeout 为 30 秒，
reconcile 周期为 100ms；未配置初始 segment，client 必须通过 Mount/ReMount 注册容量。
重启会丢失 metadata，且当前不包含 TLS、HA、持久化、HTTP metadata、NoF/LocalSSD
工作流。当前 `WrappedMasterService` 已包含 `ServiceReady` 和返回空持久化配置的
`GetStorageConfig`，足够让无持久化 Mooncake Client 完成初始化。

## Thrift IDL 与生成接口

Thrift 只用作接口定义，不引入 Thrift transport、protocol 或 runtime，也不生成 C++ 代码。线上 wire 仍然是 coro_rpc v0 + struct_pack，C++ 代码保持原来的 yalanting `coro_rpc` 写法。

IDL 语法树由维护中的 [`arborium-thrift`](https://docs.rs/arborium-thrift/latest/arborium_thrift/) 与 [Tree-sitter](https://github.com/tree-sitter/tree-sitter) 解析；本项目只负责把语法树降到 coro_rpc 所需的语义模型、校验 struct_pack 子集并生成 stub。Rust 代码使用 `quote`/`syn`/`prettyplease` 生成和格式化，MD5 使用 `md-5` crate，没有自写 Thrift lexer/parser、Rust 源码拼接器或 MD5 实现。

[`idl/cakemaster.thrift`](idl/cakemaster.thrift) 中的 codegen fixture service：

```thrift
namespace rs api

enum ErrorCode {
  OK = 0
  INTERNAL_ERROR = -1
  OBJECT_NOT_FOUND = -704
}

struct MemoryDescriptor {
  1: required i64 address
}

struct DiskDescriptor {
  1: required string path
  2: required i64 object_size
}

union DescriptorVariant {
  1: MemoryDescriptor memory
  2: DiskDescriptor disk
}

struct ReplicaDescriptor {
  1: required i64 id
  2: required DescriptorVariant descriptor_variant
  3: required i32 status
}

service DemoService {
  string echo(1: required string value)
  i32 add(1: required i32 left, 2: required i32 right)
  ErrorCode echo_error(1: required ErrorCode error)
  ReplicaDescriptor echo_descriptor(1: required ReplicaDescriptor descriptor)
  string ping()
  void fail()
  void attachment_echo() (coro_rpc.attachment = "true")
}
```

根 [`build.rs`](build.rs) 调用 codegen：

```rust
let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
coro_rpc_codegen::Builder::new()
    .compile("idl/cakemaster.thrift", output.join("cakemaster_rpc.rs"))?;
```

生成代码只在 [`src/proto.rs`](src/proto.rs) 中通过 Cargo 的 `OUT_DIR` 引入一次：

```rust
include!(concat!(env!("OUT_DIR"), "/cakemaster_rpc.rs"));
include!(concat!(env!("OUT_DIR"), "/mooncake_master_rpc.rs"));
```

客户端不再声明 `RpcMethod`：

```rust
use cakemaster::api::DemoServiceClient;

let client = DemoServiceClient::connect("127.0.0.1:9000").await?;
let value = client.echo("hello".to_owned()).await?;
let sum = client.add(20, 22).await?;
```

服务端只实现生成 trait，再一次性构造路由：

```rust
use coro_rpc::{RequestContext, RpcFailure, RpcResponse};
use cakemaster::api::{DemoService, DemoServiceServer, ErrorCode, ReplicaDescriptor};

struct Service;

impl DemoService for Service {
    async fn echo(&self, value: String) -> Result<String, RpcFailure> {
        Ok(value)
    }

    async fn add(&self, left: i32, right: i32) -> Result<i32, RpcFailure> {
        Ok(left + right)
    }

    async fn echo_error(&self, error: ErrorCode) -> Result<ErrorCode, RpcFailure> {
        Ok(error)
    }

    async fn echo_descriptor(
        &self,
        descriptor: ReplicaDescriptor,
    ) -> Result<ReplicaDescriptor, RpcFailure> {
        Ok(descriptor)
    }

    async fn ping(&self) -> Result<String, RpcFailure> {
        Ok("pong".to_owned())
    }

    async fn fail(&self) -> Result<(), RpcFailure> {
        Err(RpcFailure::new(1001, "expected error"))
    }

    async fn attachment_echo(
        &self,
        context: RequestContext,
    ) -> Result<RpcResponse<()>, RpcFailure> {
        Ok(RpcResponse::with_attachment((), context.attachment))
    }
}

let server = DemoServiceServer::new(Service).into_rpc_server()?;
server.serve("127.0.0.1:9000").await?;
```

默认 wire function name 是方法名；`namespace cpp demo` 会生成 `demo::method`。已有 C++ 名字不符合这个规则时，可在方法上使用 `(coro_rpc.name = "Service::method")`。需要请求/响应 attachment 的方法使用 `(coro_rpc.attachment = "true")`，只有这类生成接口会暴露 attachment 与 `RequestContext`。

字段与参数必须提供正数且唯一的 Thrift field ID，生成器按 ID 升序确定 struct_pack wire 顺序。支持 `bool`、`byte/i8`、`i16`、`i32`、`i64`、`float`、`double`、`string`、`binary`、`list`、`set`、`map`、`optional`、typedef、enum、union 和 struct；为匹配 C++ wire 还提供 `u8`、`u16`、`u32`、`u64` 扩展类型。Thrift enum 默认生成 `#[repr(i32)]` Rust enum；`(coro_rpc.repr = "u8")` 用于底层类型为 `std::uint8_t` 的 C++ enum。未知判别值会返回 `StructPackError::InvalidEnumDiscriminant`。

普通 Thrift union 会生成带 payload 的 Rust enum，对应 C++ `std::variant`；其 field ID 必须从 1 连续编号，`field ID - 1` 即 variant index，且 alternative 不允许 `required`/`optional` 修饰。`(coro_rpc.expected)` 则把 `value/error` union 生成为 Rust `Result<T, E>`；只有 `error` 字段时对应 `Result<(), E>` 和 `tl::expected<void, E>`。Mooncake 的 `UUID` 是 `std::pair<uint64_t, uint64_t>`，其 struct_pack type literal 带 pair 的对齐信息，不能按普通反射 struct 处理；精确的两个 required `u64` 字段可用 `(coro_rpc.cpp_u64_pair)` 声明。Mooncake 当前固定的 yalantinglibs 版本中，C++ client 对裸的顶层 `std::variant` RPC 返回值存在模板限制，因此跨语言接口应将 union 放进 `YLT_REFL` 结构体；Mooncake 的 `Replica::Descriptor` 已经是这种形态。`set` 元素和 `map` key 还必须能映射为 Rust `Ord`；目前会拒绝浮点数、生成 struct 和 union。当前也会明确拒绝 include、senum、oneway、service inheritance、默认值和 typed `throws`，避免静默生成与 coro_rpc 不兼容的代码。

## 运行结构

服务端只为每条 TCP 连接创建一个 Tokio task。该 task 内的 `ServerConnection` 本身实现 `Future`，并持有：

- `Framed<TcpStream, ServerCodec>`：同一个 owner 负责双向协议 I/O
- 按 function ID 查找 typed handler 的轻量路由器
- `FuturesUnordered`：并发轮询已经开始的 handler futures
- 有界的待发送/已编码响应状态

driver 在一次 poll 中尽可能读取一批请求、轮询 handler、调用 `start_send`，最后统一 `poll_flush`。单连接背压由未 flush 的请求数量表达，不需要每请求 `tokio::spawn`、`Semaphore`、共享 writer 或响应 `mpsc`。

服务端 route registry 直接把 function ID 映射到类型擦除后的 handler；该边界当前不开放 middleware，因此不再引入只会永远返回 ready 的 Tower `Service` 包装。客户端采用 handle/dispatch 分离：可 clone 的 `RpcClient` 通过 bounded `mpsc` 向唯一的 `ClientConnection` 提交请求；connection 独占 socket 与 in-flight map，通过 sequence 完成对应 `oneshot`。调用 future 被 drop 或超时时，guard 会通知 dispatch 删除等待项。这个结构分别参考了 [tonic Channel](https://docs.rs/tonic/latest/tonic/transport/channel/struct.Channel.html)、[tarpc client dispatch](https://docs.rs/crate/tarpc/latest/source/src/client.rs) 和 [tokio-postgres Connection](https://docs.rs/tokio-postgres/latest/src/tokio_postgres/connection.rs.html) 的职责划分。

## 结构体互通

业务结构体优先直接写在 Thrift IDL 中，生成器会同时生成 Rust struct 和对应的 struct_pack 实现，并按 field ID 升序固定 wire 字段顺序。只有需要接入 IDL 外的底层类型时，才需要手动声明宏：

Rust 字段顺序必须和 C++ `YLT_REFL` 顺序相同：

```cpp
struct person {
  std::int32_t id;
  std::string name;
};
YLT_REFL(person, id, name);
```

```rust
use coro_rpc::impl_struct_pack;

struct Person {
    id: i32,
    name: String,
}
impl_struct_pack!(Person { id: i32, name: String });
```

IDL `string` 生成 Rust `String`，IDL `binary` 生成 `coro_rpc::ByteString`，二者在 C++ 侧都对应 `std::string`。`Vec<u8>` 对应 `std::vector<std::uint8_t>`，它与 `std::string` 的类型哈希不同。RPC attachment 使用 `coro_rpc::Bytes`，调用接口同时接受可转换为 `Bytes` 的 `Vec<u8>`。

## 验证

```bash
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

[`crates/coro-rpc/tests/upstream_golden.rs`](crates/coro-rpc/tests/upstream_golden.rs) 使用 C++ 上游生成的固定字节序列验证类型哈希和编码；[`crates/coro-rpc/tests/end_to_end.rs`](crates/coro-rpc/tests/end_to_end.rs) 验证流水线、错误与 attachment；[`crates/coro-rpc-codegen/tests/codegen.rs`](crates/coro-rpc-codegen/tests/codegen.rs) 验证 Thrift 解析、校验和 stub 生成；[`tests/generated_end_to_end.rs`](tests/generated_end_to_end.rs) 使用同一份生成契约验证 client/server 与业务 struct。`interop/` 中还包含直接编译 yalantinglibs 的向量生成器和双向 C++ peer。

## ObjectCatalog 的 Mooncake RPC

[`ObjectCatalogRpcService`](src/server/rpc/mod.rs) 实现生成的
异步 `WrappedMasterService` trait：`Ping`、三种 segment mount/unmount 和 `ReMountSegment`
接到 core `ClientManager`，单 key
`ExistKey`、`GetReplicaList` 以及 `BatchExistKey`、`BatchGetReplicaList`、
`BatchPutStart`、`BatchPutEnd`、`BatchPutRevoke`、六个 Upsert 路由和四个 Remove 路由接到
真实 `ObjectManager`。RPC 层只负责 wire 校验、plan 转换和错误码映射；同步、线程安全的
ObjectManager 负责 owner、pending/published/upsert 生命周期、lease 和 reservation 协调。

client bootstrap 路由也已对齐固定的 Mooncake `5c0724d` 基线：`ServiceReady` 返回其
严格版本校验所需的 `2.0.0`，`GetStorageConfig` 返回 `fsdir=""`、
`enable_disk_eviction=false`、`quota_bytes=0`，明确禁用持久化。上游 Client 只在
`GetStorageConfig` 失败时才用 `GetFsdir` 做旧版本兼容 fallback；当前成功的空配置已足够
让无持久化 Client 完成初始化，`GetFsdir` 本身仍未实现。

[`MooncakeServerConfig`](src/server/runtime.rs) 是 production composition
入口；它生成可检查、可单测的 `MooncakeServerComposition`，bind 后的 runtime 并发管理
RPC server 和 `MasterReconciler` 的统一 shutdown/join。workspace 的主 binary
`cakemaster` 就是该 production 入口，benchmark server 仍只用于性能测量。

当前明确不支持 checksum：PutEnd 携带 checksum 返回 `INVALID_PARAMS`，Get/BatchGet
固定返回 `None`。Memory-only replica 使用与 C++ 一致的 best-effort 语义，NoF-only
使用 all-or-nothing；混合 Memory+NoF、group、pin 和 Disk 仍需领域模型支持，不在 RPC
handler 中静默降级。multi-tenant 构造会解析 tenant、隔离 namespace 并执行
Memory/NoF quota admission。

真实 TCP 测试位于
[`tests/rpc.rs`](tests/rpc.rs)
和
[`tests/client_lifecycle_rpc.rs`](tests/client_lifecycle_rpc.rs)。
线上比例 benchmark 使用三条独立的 BatchPut/Get/Exists 流，三者采用相同 batch 和
目标 QPS。benchmark server 复用 production `MooncakeServerComposition` 和
`MasterReconciler`；它的默认 high/low watermark 为 0.90/0.85，并在退出时输出最大/最终
物理水位、debt、retired/reclaimed、allocation failure/retry 等闭环诊断。下面的缩放
高压配置先把 1 KiB 对象预填到约 89%，再以每类 300 batch QPS 持续越过 high：

```bash
cargo build --release --bin object_catalog_rpc_benchmark_server
target/release/object_catalog_rpc_benchmark_server \
  127.0.0.1:19094 8 115056180 2000000 500000 0.90 0.85

g++ -std=c++20 -O3 -DNDEBUG \
  -I /path/to/Mooncake/extern/yalantinglibs/include \
  -I /path/to/Mooncake/extern/yalantinglibs/include/ylt/thirdparty \
  interop/mooncake_benchmark.cpp -pthread -o /tmp/mooncake_benchmark
/tmp/mooncake_benchmark mixed-client \
  127.0.0.1 19094 333 300 10 3 100000 50000 1024
```

同一个 C++ client 对 Rust 和 C++ Mooncake Master 的实测结果、原始范围与完整复现
命令见
[`docs/object_catalog_mooncake_benchmark.md`](docs/object_catalog_mooncake_benchmark.md)。

## Tokio ClientTaskQueue

[`ClientTaskQueue`](src/server/client_task_queue.rs) 是 Master 侧的
per-client Tokio channel。Master producer 持有可克隆的 `ClientTaskTx`，client 的 fetch
RPC handler 持有唯一的 `ClientTaskRx`；有界 `mpsc` 负责 FIFO、异步背压、唤醒和取消
安全。完成上报走独立 RPC 路径，不属于该队列；具体任务和 RPC wire 类型也由上层定义。

## Mooncake wire/RPC 空服务性能对比

[`idl/mooncake_master.thrift`](idl/mooncake_master.thrift) 和 [`src/bin/mooncake_benchmark.rs`](src/bin/mooncake_benchmark.rs) 提供与 Mooncake `WrappedMasterService` 相同 wire 的 Rust peer，[`interop/mooncake_benchmark.cpp`](interop/mooncake_benchmark.cpp) 是使用 Mooncake 自带 yalantinglibs 的 C++ peer。两端只实现最小合法返回值，不维护 segment、replica、lease 或 object 状态，适合单独比较 RPC framing、struct_pack 编解码、调度和网络开销。

推理框架的三个操作会落到以下五个 Master RPC：

| 基准参数 | Mooncake RPC | 用途 |
| --- | --- | --- |
| `exists` | `BatchExistKey` | `BatchExists` |
| `get` | `BatchGetReplicaList` | `BatchGet` |
| `put-start` | `BatchPutStart` | `BatchPut` 分配 replica |
| `put-end` | `BatchPutEnd` | `BatchPut` 写入成功提交 |
| `put-revoke` | `BatchPutRevoke` | `BatchPut` 失败回滚 |

`single-exists` 和 `single-get` 分别压测单 key `ExistKey`、`GetReplicaList`。

构建 Rust 与 C++ 版本：

```bash
cargo build --release --bin mooncake_benchmark

g++ -std=c++20 -O3 -DNDEBUG \
  -I ~/src/cpp/Mooncake/extern/yalantinglibs/include \
  -I ~/src/cpp/Mooncake/extern/yalantinglibs/include/ylt/thirdparty \
  interop/mooncake_benchmark.cpp -pthread -o /tmp/mooncake_benchmark
```

服务端性能建议固定同一个 C++ client，只替换 server。下面以 `get`、batch 16、串行请求为例；其余操作只需替换 `get`：

```bash
# Rust server
target/release/mooncake_benchmark server 127.0.0.1:19094 1
/tmp/mooncake_benchmark client 127.0.0.1 19094 get 16 200000 1 20000

# C++ server
/tmp/mooncake_benchmark server 19095 1
/tmp/mooncake_benchmark client 127.0.0.1 19095 get 16 200000 1 20000
```

客户端性能则固定 C++ server，分别运行两个 client：

```bash
/tmp/mooncake_benchmark server 19095 1
target/release/mooncake_benchmark client 127.0.0.1:19095 get 16 200000 1 20000
/tmp/mooncake_benchmark client 127.0.0.1 19095 get 16 200000 1 20000
```

client 参数依次是 `operation batch-size iterations pipeline warmup`。输出同时包含每批 QPS、每 key QPS 和单次完成延迟。`pipeline=1` 最适合比较串行延迟；提高 pipeline 可测单连接饱和吞吐，但两种客户端提交异步请求的方式不同，因此判断 runtime/server 差异时优先采用“固定 C++ client、替换 server”的结果。正式测量应将 server/client 固定到不同物理核，并重复至少三轮取中位数。

两端的 schema 和 route 元数据可直接比对：

```bash
target/release/mooncake_benchmark metadata
/tmp/mooncake_benchmark metadata
```

[`tests/mooncake_wire.rs`](tests/mooncake_wire.rs) 固定了二十四个接口的 C++ type metadata 与 route hash，以及 bootstrap 响应、代表性的 `BatchPutEnd` 和最新 `BatchPutStart` 字节序列。

## 本机性能对比

仓库提供相同 `add(i32, i32) -> i32` wire payload 的 Rust 与 C++ 基准入口。Rust 侧：

```bash
cargo build --release -p coro-rpc --example benchmark
target/release/examples/benchmark server 127.0.0.1:19092 1
target/release/examples/benchmark client 127.0.0.1:19092 200000 1 20000
target/release/examples/benchmark client 127.0.0.1:19092 1000000 256 50000
```

C++ 侧使用仓库兼容基线对应的 yalantinglibs checkout：

```bash
g++ -std=c++20 -O3 -DNDEBUG \
  -I /path/to/yalantinglibs/include \
  -I /path/to/yalantinglibs/include/ylt/thirdparty \
  interop/upstream_benchmark.cpp -pthread -o /tmp/upstream_benchmark

/tmp/upstream_benchmark server 19093 1
/tmp/upstream_benchmark client 127.0.0.1 19093 200000 1 20000
/tmp/upstream_benchmark client 127.0.0.1 19093 1000000 256 50000
```

建议固定服务端和客户端到不同物理核，并至少重复三轮。`pipeline=1` 用于观察串行 ping-pong；较大的 pipeline 用于观察单连接饱和吞吐。

本次 connection-driver 重构在 Ryzen 7 9700X 上的实测如下。Rust 为 `--release`；C++ 为 GCC 16.1.1 `-O3 -DNDEBUG`；服务端固定 CPU 2、客户端固定 CPU 4；C++ 基线为 yalantinglibs `c1cef74057b139944c982d840c09c9940f26e08e`。单连接取三轮中位数：

| 固定官方 C++ client，只替换 server | 重构前 Rust | 重构后 Rust | C++ coro_rpc | 重构收益 | Rust 对 C++ |
| --- | ---: | ---: | ---: | ---: | ---: |
| pipeline=1 | 157.6K QPS / 6.345 µs | 202.7K / 4.932 µs | 216.7K / 4.614 µs | +28.6% | -6.5% |
| pipeline=256 | 452.8K QPS | 541.2K QPS | 429.5K QPS | +19.5% | +26.0% |
| 4 连接、4 server threads | 1.95M QPS | 2.17–2.18M QPS | 1.59–1.61M QPS | 约 +11.8% | 约 +35% |

固定官方 C++ server 时，Rust client 串行中位数由 177.4K 提升到 190.0K QPS，仍比同场景 C++ client 低约 12.3%；pipeline=256 由 440.2K 提升到 486.7K QPS。客户端 pipeline 数字受两种语言提交 batch 的 API 差异影响，服务端对比更适合判断 transport/runtime 结构本身。
