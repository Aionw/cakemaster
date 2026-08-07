# cakemaster

基于 Tokio 的 yalantinglibs `coro_rpc` v0 兼容实现，可让 Rust 与 C++ `coro_rpc` 客户端/服务端通过 TCP 直接互调。

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
- 生成业务 struct、i32-backed enum、typed client、server trait 与整组服务注册代码
- Tokio 多路复用客户端、流水线并发服务端、请求/响应 attachment
- 基于 `tokio-util::codec` 的流式拆帧，以及 `Bytes` payload 零拷贝切分
- 基于 `tower_service::Service` 的路由/执行边界
- poll-based connection driver：一个连接 future 同时驱动读、handler futures、写入和 flush
- tarpc 风格的 client handle/dispatch、bounded backpressure 与 drop/timeout cancellation
- 基于 `CancellationToken`/`TaskTracker` 的连接级优雅关闭
- 标准错误码和大于 255 的扩展错误码
- 入站帧大小、容器大小和单连接并发上限

暂不包含 TLS/NTLS、RDMA/CUDA transport、struct_pack varint 配置、自定义 variant/多态指针，以及 C++ 未使用 `YLT_REFL` 的 ABI/padding 结构体。大二进制建议放在 coro_rpc attachment 中，无需经过 struct_pack。

## 直接运行

仓库按常见的 application + internal crate 方式组织：

```text
src/main.rs                 # cakemaster 应用入口
crates/coro-rpc/src/        # 可复用的 RPC 协议、client 与 server
crates/coro-rpc-codegen/    # Thrift AST 校验与 Rust stub 生成
crates/coro-rpc/tests/      # RPC crate 的兼容与端到端测试
crates/coro-rpc/examples/   # RPC crate 的 benchmark
idl/                        # 应用 RPC 契约
build.rs                    # 将 Thrift IDL 生成到 Cargo OUT_DIR
```

根 package 通过 path dependency 使用 `coro-rpc`，因此 RPC 框架不会和应用代码混在根 `src/` 下。

启动 Rust 服务端：

```bash
cargo run --release -- server 127.0.0.1:9000
```

运行 Rust 客户端：

```bash
cargo run --release -- client 127.0.0.1:9000
```

演示程序注册了以下与 C++ 同名、同签名的接口：

```cpp
std::string echo(std::string value);
std::int32_t add(std::int32_t lhs, std::int32_t rhs);
ErrorCode echo_error(ErrorCode error);            // int32_t-backed enum
std::string ping();
void fail(coro_rpc::context<void> context);  // 返回扩展错误码 1001
void attachment_echo();                     // 回显 attachment
```

## Thrift IDL 与生成接口

Thrift 只用作接口定义，不引入 Thrift transport、protocol 或 runtime，也不生成 C++ 代码。线上 wire 仍然是 coro_rpc v0 + struct_pack，C++ 代码保持原来的 yalanting `coro_rpc` 写法。

IDL 语法树由维护中的 [`arborium-thrift`](https://docs.rs/arborium-thrift/latest/arborium_thrift/) 与 [Tree-sitter](https://github.com/tree-sitter/tree-sitter) 解析；本项目只负责把语法树降到 coro_rpc 所需的语义模型、校验 struct_pack 子集并生成 stub。Rust 代码使用 `quote`/`syn`/`prettyplease` 生成和格式化，MD5 使用 `md-5` crate，没有自写 Thrift lexer/parser、Rust 源码拼接器或 MD5 实现。

[`idl/cakemaster.thrift`](idl/cakemaster.thrift) 中的核心 service：

```thrift
namespace rs api

enum ErrorCode {
  OK = 0
  INTERNAL_ERROR = -1
  OBJECT_NOT_FOUND = -704
}

service DemoService {
  string echo(1: required string value)
  i32 add(1: required i32 left, 2: required i32 right)
  ErrorCode echo_error(1: required ErrorCode error)
  string ping()
  void fail()
  void attachment_echo() (coro_rpc.attachment = "true")
}
```

应用的 `build.rs` 调用 codegen：

```rust
let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
coro_rpc_codegen::Builder::new()
    .compile("idl/cakemaster.thrift", output.join("cakemaster_rpc.rs"))?;
```

生成代码通过 Cargo 的 `OUT_DIR` 引入：

```rust
pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/cakemaster_rpc.rs"));
}
```

客户端不再声明 `RpcMethod`：

```rust
use generated::api::DemoServiceClient;

let client = DemoServiceClient::connect("127.0.0.1:9000").await?;
let value = client.echo("hello".to_owned()).await?;
let sum = client.add(20, 22).await?;
```

服务端只实现生成 trait，再一次性构造路由：

```rust
use coro_rpc::{RequestContext, RpcFailure, RpcResponse};
use generated::api::{DemoService, DemoServiceServer, ErrorCode};

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

字段与参数必须提供正数且唯一的 Thrift field ID，生成器按 ID 升序确定 struct_pack wire 顺序。支持 `bool`、`byte/i8`、`i16`、`i32`、`i64`、`float`、`double`、`string`、`binary`、`list`、`set`、`map`、`optional`、typedef、enum 和 struct。Thrift enum 会生成 `#[repr(i32)]` Rust enum，并按 C++ enum 的 `int32_t` 底层值参与 struct_pack 编解码及类型哈希；未知判别值会返回 `StructPackError::InvalidEnumDiscriminant`。`set` 元素和 `map` key 还必须能映射为 Rust `Ord`；目前会拒绝浮点数和生成 struct。当前也会明确拒绝 include、senum、union、oneway、service inheritance、默认值和 typed `throws`，避免静默生成与 coro_rpc 不兼容的代码。

## 运行结构

服务端只为每条 TCP 连接创建一个 Tokio task。该 task 内的 `ServerConnection` 本身实现 `Future`，并持有：

- `Framed<TcpStream, ServerCodec>`：同一个 owner 负责双向协议 I/O
- 实现 `tower_service::Service` 的路由器
- `FuturesUnordered`：并发轮询已经开始的 handler futures
- 有界的待发送/已编码响应状态

driver 在一次 poll 中尽可能读取一批请求、轮询 handler、调用 `start_send`，最后统一 `poll_flush`。单连接背压由未 flush 的请求数量表达，不需要每请求 `tokio::spawn`、`Semaphore`、共享 writer 或响应 `mpsc`。

路由与执行边界采用 [Tower `Service`](https://docs.rs/tower/latest/tower/trait.Service.html)。客户端采用 handle/dispatch 分离：可 clone 的 `RpcClient` 通过 bounded `mpsc` 向唯一的 `ClientConnection` 提交请求；connection 独占 socket 与 in-flight map，通过 sequence 完成对应 `oneshot`。调用 future 被 drop 或超时时，guard 会通知 dispatch 删除等待项。这个结构分别参考了 [tonic Channel](https://docs.rs/tonic/latest/tonic/transport/channel/struct.Channel.html)、[tarpc client dispatch](https://docs.rs/crate/tarpc/latest/source/src/client.rs) 和 [tokio-postgres Connection](https://docs.rs/tokio-postgres/latest/src/tokio_postgres/connection.rs.html) 的职责划分。

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
