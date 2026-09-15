# Cakemaster technical reference

This document covers runtime configuration, the RPC protocol, code generation,
and benchmarks. See the [README](../README.md) for an introduction and quick start.

The Cakemaster workspace contains a high-concurrency object catalog, Memory
segment/placement core libraries, and a Tokio-based implementation compatible
with yalantinglibs `coro_rpc` v0, enabling direct Rust/C++ `coro_rpc`
client/server interoperability over TCP.

The compatibility baseline is `alibaba/yalantinglibs` commit
`c1cef74057b139944c982d840c09c9940f26e08e`. The implementation follows that version's
[`coro_rpc_protocol.hpp`](https://github.com/alibaba/yalantinglibs/blob/c1cef74057b139944c982d840c09c9940f26e08e/include/ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp)
and [`struct_pack`](https://alibaba.github.io/yalantinglibs/en/struct_pack/struct_pack_layout.html).

## Implemented features

- coro_rpc v0 request headers (20 bytes) and response headers (16 bytes)
- Function IDs matching `coro_rpc::func_id`: the first 32 bits of the MD5 of the fully qualified C++ function name
- struct_pack release/debug metadata, type hashes, and global container-length width
- Fixed-width integers, floating-point values, `bool`, `char32_t`, `std::monostate`
- `std::string`, sequential containers, fixed-size arrays, `optional`, `expected`, maps, sets
- 1–8-element tuples/pairs and macro-declared `YLT_REFL`-style structs
- Low-level typed `RpcMethod<Request, Response>` API, computing route IDs and request/response schema hashes only once
- Thrift IDL-based Rust-only codegen, fixing route IDs and schema hashes at build time
- Generated business structs, i32/u8-backed enums, union-backed data enums, unions with `tl::expected` semantics, typed clients, server traits, and whole-service registration code
- Tokio multiplexed clients, pipelined concurrent servers, request/response attachments
- Streaming framing through `tokio-util::codec` and zero-copy `Bytes` payload slicing
- Private typed route registry and type-erased handler execution boundary
- Poll-based connection driver: one connection future drives reads, handler futures, writes, and flushes
- tarpc-style client handle/dispatch, bounded backpressure, and drop/timeout cancellation
- Connection-level graceful shutdown through `CancellationToken`/`TaskTracker`
- Standard error codes and extended codes above 255
- Inbound frame size, container size, and per-connection concurrency limits
- Real ObjectCatalog/SegmentPool-backed Mooncake `Ping`, `MountSegment`,
  `ReMountSegment`, `UnmountSegment`, `GracefulUnmountSegment`, single/batch
  exists/get, single/batch put, single/batch upsert, and single/batch/regex/all
  remove RPC adapters, plus `ServiceReady`/`GetStorageConfig` for client initialization

Not yet included: TLS/NTLS, RDMA/CUDA transport, struct_pack varint configuration,
custom variants/polymorphic pointers outside the IDL, and ABI/padded C++ structs
without `YLT_REFL`. Large binary payloads should use coro_rpc attachments,
bypassing struct_pack.

## Running directly

The root `cakemaster` package is the complete server product. Only independently
reusable RPC runtime and codegen components remain workspace subcrates:

```text
build.rs + idl/             # Mooncake Thrift contract and sole generation entry point
src/                        # domain modules, generated contract façade, server composition, main binary
src/bin/                    # diagnostics and performance tools
tests/                      # core, wire, RPC, and production runtime integration tests
crates/coro-rpc/src/        # reusable RPC protocol, client, and server
crates/coro-rpc-codegen/    # Thrift AST validation and Rust stub generation
crates/coro-rpc/tests/      # RPC crate compatibility and end-to-end tests
crates/coro-rpc/examples/   # RPC crate benchmarks
```

The root package preserves the module dependency direction
`server → client/object/segment + proto → coro-rpc`. Domain modules import no
Tokio, RPC, or codegen APIs but are no longer artificially isolated by extra
Cargo packages. Domain APIs are exposed through the `object` and `segment`
façades; errors, reclamation controls, placement, and diagnostics types live in
named submodules, while implementation files remain private.

For Memory-only SegmentPool structure, lifecycle, and API changes, see
[`docs/segment_pool_backends.md`](../docs/segment_pool_backends.md).
For ObjectManager, ReplicaAllocator, asynchronous RPC boundaries, and the current
Mooncake compatibility subset, see
[`docs/object_catalog_rpc.md`](../docs/object_catalog_rpc.md).
For the full feature gap against upstream C++ Mooncake Store, latest wire drift,
and recommended implementation order, see
[`docs/mooncake_feature_gap.md`](../docs/mooncake_feature_gap.md).
For optional tenant namespaces, Memory quota, RAII accounting, and targeted
reclamation, see [`docs/tenant_quota.md`](../docs/tenant_quota.md).

Core types retain short paths; import extended interfaces by responsibility:

```rust
use cakemaster::object::{ObjectCatalog, ObjectCatalogConfig, ObjectIdentity};
use cakemaster::object::error::LookupError;
use cakemaster::object::reclamation::{CatalogTick, CollectBudget};
use cakemaster::segment::{SegmentSpec, SegmentPool};
use cakemaster::segment::placement::{PlacementRequest, ReplicaAllocator};
```

Value objects and IDs preserve semantic boundaries through private fields and
constructors/accessors; statistical snapshots use public fields. Configuration
changes use consuming `with_*` builders, not shared mutable setters.

Start the deployable Mooncake RPC server:

```bash
cargo run --release -- \
  --listen 127.0.0.1:50051
```

With no arguments, the server listens on `127.0.0.1:50051`; use
`--listen 127.0.0.1:0` to let the OS choose a test port. Each direct-memory
allocator preallocates 128K offset metadata nodes by default. Size
`--max-allocator-nodes-per-segment` for concurrent live slices plus free-range
headroom; it limits allocation ranges, not segment byte capacity. The object
catalog defaults to an initial capacity hint of 64K slots. `--expected-objects`
should cover peak indexed objects, including empty slots retained during the
grace period, to avoid tail-latency spikes from concurrent hash-table growth.
It is not a hard object-count limit. Background collection scans at most 256
candidates and reclaims 256 retired objects per step by default;
`--object-collection-budget-per-step` adjusts both limits. Larger budgets can
speed watermark eviction convergence but increase collector occupancy per step.
The allocation-failure request path keeps an independent small budget, so tune
against target-load success rate and RPC tail latency rather than maximizing it.
RPC access logging is disabled by default. `--access-log` logs each completed
request's source address, route name/function ID, sequence, result,
request/response sizes, and duration at info level. Library callers can enable
it with `ServerConfig::default().with_access_log(true)`. The binary constructs
`SegmentPool`, the in-memory `ObjectManager`, `MasterClock`, and
`ObjectCatalogRpcService` once in the same composition root, then derives a
`MasterReconciler` sharing the service's `ClientManager`, clock, deadline
`Notify`, and memory-pressure `Notify`. The production manager uses internally
fixed 90%/80% high/low watermarks for bounded Memory eviction. See
[`docs/memory_eviction.md`](../docs/memory_eviction.md) for watermarks, physical
capacity/used, live/retired/debt accounting, bounded allocation-failure retries,
and diagnostics. The RPC server and reconciler run concurrently. Ctrl-C/SIGTERM
on Unix, or Ctrl-C elsewhere, notifies both to stop; the process returns only
after the listener, all connection tasks, and reconciler have fully exited.

Runtime logs go to both stderr and `logs/cakemaster.log` by default, with ISO 8601
timestamps in the system timezone. Files rotate daily or early at 100 MiB, with
at most 14 retained. Rotated names look like `cakemaster.2026-08-16.1.log`.
Command-line options configure level, module filtering, directory, and output:

```bash
cakemaster \
  --log-filter 'info,cakemaster::server=debug,coro_rpc=warn' \
  --log-dir /var/log/cakemaster \
  --log-output both
```

Use `--log-level debug` for a global level only; it is mutually exclusive with
`--log-filter`. `--log-output` accepts `stderr`, `file`, or `both`; choosing
`stderr` creates no log directory. Environment variables `RUST_LOG`,
`CAKEMASTER_LOG_LEVEL`, `CAKEMASTER_LOG_DIR`, and `CAKEMASTER_LOG_OUTPUT` provide
the same settings, with precedence: command line, environment, built-in defaults.
Filters are parsed at startup; runtime hot reload is not supported. Common
stable log targets include:

- `coro_rpc::access`: per-request access logs explicitly enabled with `--access-log`;
- `coro_rpc::server::connection`: connection establishment/closure and frame/protocol failures;
- `coro_rpc::server::request`: request method, peer, sequence, duration, and RPC transport errors;
- `coro_rpc::client::{connection,request}`: client connection-driver errors and request timeouts;
- `cakemaster::server::rpc::{business,mapping}`: Mooncake business-error summaries and internal error mapping;
- `cakemaster::client::{lifecycle,manager}`: client-generation state transitions and cleanup/unmount retries;
- `cakemaster::object::{manager,eviction}`, `cakemaster::server::{reconciler,eviction}`:
  domain invariants, bounded allocation-failure work, background convergence,
  and memory watermark/debt diagnostics.

For example, retain default business logs, enable RPC completion records, and
suppress normal connection logs:

```bash
cakemaster --log-filter \
  'info,coro_rpc::server::request=debug,coro_rpc::server::connection=warn,cakemaster::server::rpc::business=debug'
```

Successful RPCs are logged only at `DEBUG` (including duration); received request
payload sizes only at `TRACE`. Protocol/RPC failures use `WARN`. Without request
`DEBUG`, the default hot path does not read the clock for each successful
request. Business `InternalError` uses `ERROR`; capacity or temporary
unavailability uses `WARN`; caller-handled rejections such as invalid parameters
and not-found use `DEBUG`. Batches log only aggregate error counts, not object
keys, avoiding high cardinality and sensitive data. Access logs also obey level
and module filters.

Production defaults are conservative, single-process, single-tenant, and
in-memory: at most 65,536 clients, 65,536 expected object slots, at most 128K
metadata nodes per direct-memory allocator, 256 candidates/reclaims per
background collection step, 10-second client/lease TTLs, a 30-second pending
write timeout, and a 100ms reconcile interval. There are no initial segments;
clients must register capacity through Mount/ReMount. Restart loses metadata.
TLS, HA, persistence, HTTP metadata, and NoF/LocalSSD workflows are not included.
`WrappedMasterService` includes `ServiceReady` and `GetStorageConfig` returning
an empty persistence configuration, sufficient for nonpersistent Mooncake Client
initialization.

## Thrift IDL and generated interfaces

Thrift is used only for interface definitions: no Thrift transport, protocol,
runtime, or generated C++ code is introduced. The wire remains coro_rpc v0 +
struct_pack, and C++ code keeps its existing yalanting `coro_rpc` style.

IDL syntax trees are parsed by the maintained
[`arborium-thrift`](https://docs.rs/arborium-thrift/latest/arborium_thrift/) and
[Tree-sitter](https://github.com/tree-sitter/tree-sitter) projects. This project
only lowers the syntax tree into the semantic model needed by coro_rpc,
validates the struct_pack subset, and generates stubs. Rust code is generated
and formatted with `quote`/`syn`/`prettyplease`; MD5 uses the `md-5` crate. There
is no handwritten Thrift lexer/parser, Rust source-string builder, or MD5 implementation.

The codegen fixture service in [`idl/cakemaster.thrift`](../idl/cakemaster.thrift):

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

The root [`build.rs`](../build.rs) invokes codegen:

```rust
let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
coro_rpc_codegen::Builder::new()
    .compile("idl/cakemaster.thrift", output.join("cakemaster_rpc.rs"))?;
```

Generated code is included once through Cargo's `OUT_DIR` in
[`src/proto.rs`](../src/proto.rs):

```rust
include!(concat!(env!("OUT_DIR"), "/cakemaster_rpc.rs"));
include!(concat!(env!("OUT_DIR"), "/mooncake_master_rpc.rs"));
```

Clients no longer declare `RpcMethod`:

```rust
use cakemaster::api::DemoServiceClient;

let client = DemoServiceClient::connect("127.0.0.1:9000").await?;
let value = client.echo("hello".to_owned()).await?;
let sum = client.add(20, 22).await?;
```

Servers implement the generated trait and construct all routes at once:

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

The default wire function name is the method name; `namespace cpp demo` generates
`demo::method`. For existing C++ names that do not follow this rule, annotate the
method with `(coro_rpc.name = "Service::method")`. Methods needing request/response
attachments use `(coro_rpc.attachment = "true")`; only those generated interfaces
expose attachments and `RequestContext`.

Fields and parameters require positive, unique Thrift field IDs. The generator
orders struct_pack wire fields by ascending ID. Supported types include `bool`,
`byte/i8`, `i16`, `i32`, `i64`, `float`, `double`, `string`, `binary`, `list`, `set`,
`map`, `optional`, typedef, enum, union, and struct. Extensions `u8`, `u16`, `u32`,
and `u64` match C++ wire types. Thrift enums generate `#[repr(i32)]` Rust enums by
default; `(coro_rpc.repr = "u8")` supports C++ enums backed by `std::uint8_t`.
Unknown discriminants return `StructPackError::InvalidEnumDiscriminant`.

Ordinary Thrift unions generate payload-bearing Rust enums corresponding to C++
`std::variant`. Field IDs must be consecutive from 1; `field ID - 1` is the
variant index, and alternatives cannot use `required`/`optional` qualifiers.
`(coro_rpc.expected)` instead generates a `value/error` union as Rust
`Result<T, E>`; an error-only union corresponds to `Result<(), E>` and
`tl::expected<void, E>`. Mooncake's `UUID` is `std::pair<uint64_t, uint64_t>`;
its struct_pack type literal includes pair alignment and cannot be treated as
an ordinary reflected struct. Exactly two required `u64` fields can be annotated
with `(coro_rpc.cpp_u64_pair)`. In Mooncake's pinned yalantinglibs version, C++
clients have a template limitation on bare top-level `std::variant` RPC return
values, so cross-language interfaces should wrap unions in `YLT_REFL` structs;
Mooncake's `Replica::Descriptor` already does this. `set` elements and `map` keys
must also map to Rust `Ord`; floating-point types, generated structs, and unions
are currently rejected. Includes, senum, oneway, service inheritance, default
values, and typed `throws` are explicitly rejected to avoid silently generating
code incompatible with coro_rpc.

## Runtime architecture

The server creates only one Tokio task per TCP connection. Its
`ServerConnection` implements `Future` and owns:

- `Framed<TcpStream, ServerCodec>`: one owner for bidirectional protocol I/O
- A lightweight router looking up typed handlers by function ID
- `FuturesUnordered`: concurrent polling of started handler futures
- Bounded pending/encoded response state

Within one poll, the driver reads a batch of requests where possible, polls
handlers, calls `start_send`, and finishes with a shared `poll_flush`.
Per-connection backpressure is represented by the count of unflushed requests,
without per-request `tokio::spawn`, `Semaphore`, shared writers, or response `mpsc`.

The server route registry directly maps function IDs to type-erased handlers.
This boundary currently exposes no middleware, so it no longer adds a Tower
`Service` wrapper that always returns ready. The client separates handle and
dispatch: cloneable `RpcClient` handles submit requests through bounded `mpsc` to
a single `ClientConnection`. The connection exclusively owns the socket and
in-flight map, completing the appropriate `oneshot` by sequence. Dropping or
timing out a call future causes its guard to tell dispatch to remove the waiter.
This separation draws on [tonic Channel](https://docs.rs/tonic/latest/tonic/transport/channel/struct.Channel.html),
[tarpc client dispatch](https://docs.rs/crate/tarpc/latest/source/src/client.rs),
and [tokio-postgres Connection](https://docs.rs/tokio-postgres/latest/src/tokio_postgres/connection.rs.html).

## Struct interoperability

Prefer declaring business structs directly in Thrift IDL. The generator produces
both Rust structs and their struct_pack implementations, fixing wire order by
ascending field ID. Manual macros are needed only for low-level types outside
the IDL:

Rust field order must match the C++ `YLT_REFL` order:

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

IDL `string` generates Rust `String`; IDL `binary` generates
`coro_rpc::ByteString`. Both correspond to C++ `std::string`. `Vec<u8>` corresponds
to `std::vector<std::uint8_t>`, whose type hash differs from `std::string`.
RPC attachments use `coro_rpc::Bytes`; call interfaces also accept `Vec<u8>`
convertible to `Bytes`.

## Validation

```bash
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

[`crates/coro-rpc/tests/upstream_golden.rs`](../crates/coro-rpc/tests/upstream_golden.rs)
validates type hashes and encoding against fixed upstream C++ byte sequences.
[`crates/coro-rpc/tests/end_to_end.rs`](../crates/coro-rpc/tests/end_to_end.rs)
tests pipelining, errors, and attachments.
[`crates/coro-rpc-codegen/tests/codegen.rs`](../crates/coro-rpc-codegen/tests/codegen.rs)
tests Thrift parsing, validation, and stub generation.
[`tests/generated_end_to_end.rs`](../tests/generated_end_to_end.rs) validates
clients/servers and business structs using the same generated contract.
`interop/` also contains vector generators that compile yalantinglibs directly
and bidirectional C++ peers.

## ObjectCatalog Mooncake RPCs

[`ObjectCatalogRpcService`](../src/server/rpc/mod.rs) implements the generated
asynchronous `WrappedMasterService` trait. `Ping`, the three segment
mount/unmount operations, and `ReMountSegment` connect to the core
`ClientManager`. Single-key `ExistKey`/`GetReplicaList`,
`BatchExistKey`/`BatchGetReplicaList`, single/batch `PutStart`/`PutEnd`/`PutRevoke`,
six Upsert routes, and four Remove routes connect to a real `ObjectManager`.
The RPC layer handles only wire validation, plan conversion, and error mapping;
the synchronous, thread-safe ObjectManager coordinates owners, per-key
transactions/committed versions, leases, soft/hard pins, and reservations.

Client bootstrap routes also match the pinned Mooncake `5c0724d` baseline:
`ServiceReady` returns `2.0.0` for its strict version check; `GetStorageConfig`
returns `fsdir=""`, `enable_disk_eviction=false`, and `quota_bytes=0`, explicitly
disabling persistence. Upstream Clients use legacy `GetFsdir` only when
`GetStorageConfig` fails. A successful empty configuration already initializes
nonpersistent Clients; `GetFsdir` itself remains unimplemented.

[`MooncakeServerConfig`](../src/server/runtime.rs) is the production composition
entry point. It produces an inspectable, unit-testable
`MooncakeServerComposition`; after binding, the runtime concurrently manages
unified shutdown/join for the RPC server and `MasterReconciler`. The workspace's
main `cakemaster` binary is this production entry point; benchmark servers remain
for performance measurement only.

Checksums are explicitly unsupported: PutEnd with a checksum returns
`INVALID_PARAMS`, and Get/BatchGet always returns `None`. Memory replicas use
best-effort semantics matching C++. `ReplicateConfig` soft-pin action/TTL and
hard pins participate in transactions, eviction, and forced deletion. CXL, NoF,
and LocalSSD backends have been removed, and non-Memory requests are explicitly
rejected. Groups and Disk requests likewise never silently fall back in RPC
handlers. Multi-tenant construction resolves tenants, isolates namespaces, and
performs Memory quota admission.

Real-TCP tests are in [`tests/rpc.rs`](../tests/rpc.rs) and
[`tests/client_lifecycle_rpc.rs`](../tests/client_lifecycle_rpc.rs).
The production-ratio benchmark uses three independent BatchPut/Get/Exists
streams with identical batch sizes and target QPS. Its server reuses production
`MooncakeServerComposition` and `MasterReconciler`, defaults to 0.90/0.85 high/low
watermarks, and reports closed-loop diagnostics at exit: maximum/final physical
watermarks, debt, retired/reclaimed values, and allocation failures/retries.
The following scaled high-pressure configuration prefills 1 KiB objects to about
89%, then repeatedly crosses high at 300 batch QPS per operation:

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

For measurements of Rust and C++ Mooncake Masters with the same C++ client, raw
ranges, and full reproduction commands, see
[`docs/object_catalog_mooncake_benchmark.md`](../docs/object_catalog_mooncake_benchmark.md).

## Tokio ClientTaskQueue

[`ClientTaskQueue`](../src/server/client_task_queue.rs) is a Master-side
per-client Tokio channel. Master producers hold cloneable `ClientTaskTx`
handles; the client's fetch RPC handler holds the unique `ClientTaskRx`.
Bounded `mpsc` provides FIFO ordering, asynchronous backpressure, wakeups, and
cancellation safety. Completion reports use a separate RPC path outside the
queue; upper layers also define concrete task and RPC wire types.

## Mooncake wire/RPC no-op service performance comparison

[`idl/mooncake_master.thrift`](../idl/mooncake_master.thrift) and
[`src/bin/mooncake_benchmark.rs`](../src/bin/mooncake_benchmark.rs) provide a Rust
peer with the same wire as Mooncake `WrappedMasterService`.
[`interop/mooncake_benchmark.cpp`](../interop/mooncake_benchmark.cpp) is the C++
peer using Mooncake's bundled yalantinglibs. Both return only minimal valid
responses without segment, replica, lease, or object state, isolating RPC
framing, struct_pack encoding/decoding, scheduling, and network overhead.

The inference framework's three operations map to these five Master RPCs:

| Benchmark argument | Mooncake RPC | Purpose |
| --- | --- | --- |
| `exists` | `BatchExistKey` | `BatchExists` |
| `get` | `BatchGetReplicaList` | `BatchGet` |
| `put-start` | `BatchPutStart` | Allocate replicas for `BatchPut` |
| `put-end` | `BatchPutEnd` | Commit successful `BatchPut` writes |
| `put-revoke` | `BatchPutRevoke` | Roll back failed `BatchPut` writes |

`single-exists` and `single-get` benchmark single-key `ExistKey` and
`GetReplicaList`, respectively.

Build the Rust and C++ versions:

```bash
cargo build --release --bin mooncake_benchmark

g++ -std=c++20 -O3 -DNDEBUG \
  -I ~/src/cpp/Mooncake/extern/yalantinglibs/include \
  -I ~/src/cpp/Mooncake/extern/yalantinglibs/include/ylt/thirdparty \
  interop/mooncake_benchmark.cpp -pthread -o /tmp/mooncake_benchmark
```

For server comparisons, keep the same C++ client and swap only the server.
This example uses `get`, batch size 16, and serial requests; replace `get` for
other operations:

```bash
# Rust server
target/release/mooncake_benchmark server 127.0.0.1:19094 1
/tmp/mooncake_benchmark client 127.0.0.1 19094 get 16 200000 1 20000

# C++ server
/tmp/mooncake_benchmark server 19095 1
/tmp/mooncake_benchmark client 127.0.0.1 19095 get 16 200000 1 20000
```

For client comparisons, keep the C++ server and run each client separately:

```bash
/tmp/mooncake_benchmark server 19095 1
target/release/mooncake_benchmark client 127.0.0.1:19095 get 16 200000 1 20000
/tmp/mooncake_benchmark client 127.0.0.1 19095 get 16 200000 1 20000
```

Client arguments are `operation batch-size iterations pipeline warmup`. Output
includes batch QPS, key QPS, and per-call completion latency. `pipeline=1` best
compares serial latency; larger pipelines measure single-connection saturation
throughput. Because the clients submit asynchronous requests differently, prefer
fixed-C++-client/server-swap results when assessing runtime/server differences.
For formal measurements, pin server/client to different physical cores and take
the median of at least three rounds.

Schema and route metadata can be compared directly:

```bash
target/release/mooncake_benchmark metadata
/tmp/mooncake_benchmark metadata
```

[`tests/mooncake_wire.rs`](../tests/mooncake_wire.rs) pins C++ type metadata and
route hashes for twenty-seven interfaces, plus bootstrap responses and
representative `BatchPutEnd` and latest `BatchPutStart` byte sequences.

## Local performance comparison

The repository provides Rust and C++ benchmarks with the same
`add(i32, i32) -> i32` wire payload. Rust:

```bash
cargo build --release -p coro-rpc --example benchmark
target/release/examples/benchmark server 127.0.0.1:19092 1
target/release/examples/benchmark client 127.0.0.1:19092 200000 1 20000
target/release/examples/benchmark client 127.0.0.1:19092 1000000 256 50000
```

For C++, use a yalantinglibs checkout matching the repository's compatibility baseline:

```bash
g++ -std=c++20 -O3 -DNDEBUG \
  -I /path/to/yalantinglibs/include \
  -I /path/to/yalantinglibs/include/ylt/thirdparty \
  interop/upstream_benchmark.cpp -pthread -o /tmp/upstream_benchmark

/tmp/upstream_benchmark server 19093 1
/tmp/upstream_benchmark client 127.0.0.1 19093 200000 1 20000
/tmp/upstream_benchmark client 127.0.0.1 19093 1000000 256 50000
```

Pin the server and client to different physical cores and repeat at least three
rounds. `pipeline=1` measures serial ping-pong; larger pipelines measure
single-connection saturation throughput.

The connection-driver refactor was measured on a Ryzen 7 9700X. Rust used
`--release`; C++ used GCC 16.1.1 `-O3 -DNDEBUG`. The server was pinned to CPU 2,
the client to CPU 4. The C++ baseline was yalantinglibs
`c1cef74057b139944c982d840c09c9940f26e08e`. Single-connection results are medians
of three rounds:

| Fixed official C++ client, server swap only | Rust before refactor | Rust after refactor | C++ coro_rpc | Refactor gain | Rust vs C++ |
| --- | ---: | ---: | ---: | ---: | ---: |
| pipeline=1 | 157.6K QPS / 6.345 µs | 202.7K / 4.932 µs | 216.7K / 4.614 µs | +28.6% | -6.5% |
| pipeline=256 | 452.8K QPS | 541.2K QPS | 429.5K QPS | +19.5% | +26.0% |
| 4 connections, 4 server threads | 1.95M QPS | 2.17–2.18M QPS | 1.59–1.61M QPS | about +11.8% | about +35% |

With the official C++ server fixed, Rust client serial median throughput rose
from 177.4K to 190.0K QPS, still about 12.3% below the C++ client in the same
scenario. At pipeline=256 it rose from 440.2K to 486.7K QPS. Client pipeline
figures depend on language-specific batch submission APIs; server comparisons
better isolate transport/runtime architecture.
