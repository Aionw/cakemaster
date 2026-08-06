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
- Tokio 多路复用客户端、流水线并发服务端、请求/响应 attachment
- 标准错误码和大于 255 的扩展错误码
- 入站帧大小、容器大小和单连接并发上限

暂不包含 TLS/NTLS、RDMA/CUDA transport、struct_pack varint 配置、自定义 variant/多态指针，以及 C++ 未使用 `YLT_REFL` 的 ABI/padding 结构体。大二进制建议放在 coro_rpc attachment 中，无需经过 struct_pack。

## 直接运行

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
std::string ping();
void fail(coro_rpc::context<void> context);  // 返回扩展错误码 1001
void attachment_echo();                     // 回显 attachment
```

## 作为库使用

```rust
use cakemaster::{RpcFailure, RpcServer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = RpcServer::new();
    server
        .register::<String, String, _, _>("echo", |value| async move {
            Ok::<_, RpcFailure>(value)
        })?
        .register::<(i32, i32), i32, _, _>("add", |(a, b)| async move {
            Ok::<_, RpcFailure>(a + b)
        })?;

    server.serve("127.0.0.1:9000").await?;
    Ok(())
}
```

客户端调用：

```rust
use cakemaster::RpcClient;

let client = RpcClient::connect("127.0.0.1:9000").await?;
let value = client
    .call::<String, String>("echo", &"hello".to_owned())
    .await?;
let sum = client.call::<(i32, i32), i32>("add", &(20, 22)).await?;
```

函数名必须与 C++ `get_func_name` 产生的完整名字一致。全局函数通常是 `echo`，命名空间或成员函数可能是 `demo::echo`、`Service::echo`。

## 结构体互通

Rust 字段顺序必须和 C++ `YLT_REFL` 顺序相同：

```cpp
struct person {
  std::int32_t id;
  std::string name;
};
YLT_REFL(person, id, name);
```

```rust
use cakemaster::impl_struct_pack;

struct Person {
    id: i32,
    name: String,
}
impl_struct_pack!(Person { id: i32, name: String });
```

`String` 对应 UTF-8 `std::string`；如 C++ 字符串可能包含任意字节，使用 `cakemaster::ByteString`。`Vec<u8>` 对应 `std::vector<std::uint8_t>`，它与 `std::string` 的类型哈希不同。

## 验证

```bash
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

[`tests/upstream_golden.rs`](tests/upstream_golden.rs) 使用 C++ 上游生成的固定字节序列验证类型哈希和编码；[`tests/end_to_end.rs`](tests/end_to_end.rs) 验证流水线、错误与 attachment。`interop/` 中还包含直接编译 yalantinglibs 的向量生成器和双向 C++ peer。
