<p align="center">
  <img src="docs/assets/cakemaster-logo.png" alt="Cakemaster logo: a moon rabbit with a mooncake" width="240">
</p>

<h1 align="center">Cakemaster</h1>

<p align="center">
  A Mooncake-compatible metadata server built in Rust
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> ·
  <a href="#compatibility-and-scope">Compatibility</a> ·
  <a href="#documentation">Documentation</a>
</p>

## Overview

Cakemaster manages **object metadata, memory segments, and replica placement** for in-memory [Mooncake Store](https://github.com/kvcache-ai/Mooncake) workloads, exposing Mooncake-compatible Master RPC interfaces.

It tracks where objects live, allocates space, and coordinates writes and reclamation. Clients register memory capacity, request write locations, commit objects, and look up replicas through RPC. Object data moves through the client and data plane—not through Cakemaster.

The project focuses on concurrent metadata management and also provides a reusable Rust `coro_rpc` runtime and interface code generator.

> **Project status:** A runnable, single-process, in-memory subset of the Mooncake Master. It is not yet a complete replacement for upstream `mooncake_master` or a complete Mooncake Store implementation. Metadata is lost on restart.

## Features

- **Object lifecycle:** Single and batch exists/get/put/upsert, plus removal operations; write commit and abort, leases, soft/hard pins, and timeout-based reclamation.
- **Memory and replica management:** Segment mount, remount, and unmount; replica allocation with rollback on failure; bounded eviction driven by memory watermarks.
- **Client lifecycle:** Heartbeats, expired-session fencing, and resource cleanup, maintained by a background reconciler.
- **Cross-language RPC:** `coro_rpc v0 + struct_pack` over TCP, interoperable with C++ clients at a pinned baseline; multiplexing, backpressure, timeouts, and cancellation.
- **Embeddable core:** Object catalog and segment logic independent of Tokio and RPC, with optional tenant namespaces and memory quotas available through library APIs.

## Quick Start

You need a Rust toolchain supporting the Rust 2024 edition and Cargo. Run these commands from the repository root:

```bash
cargo build --release --bin cakemaster
./target/release/cakemaster --listen 127.0.0.1:50051
```

The server listens on `127.0.0.1:50051` by default. It starts without registered storage capacity: a compatible Mooncake client must register memory segments through `MountSegment` / `ReMountSegment` before objects can be allocated.

List all runtime options:

```bash
./target/release/cakemaster --help
```

Logs go to stderr and `logs/cakemaster.log` by default. For local debugging, log only to the terminal and enable per-request access logs:

```bash
./target/release/cakemaster --log-output stderr --access-log
```

Ctrl-C triggers graceful shutdown; SIGTERM is also supported on Unix. See [runtime configuration](docs/technical_reference.md#直接运行) for capacity planning, reclamation budgets, and log filters.

## Compatibility and Scope

The current contract targets Mooncake [`5c0724d`](https://github.com/kvcache-ai/Mooncake/tree/5c0724d22e7f04513a3453c8b6642a5a21b80b47), with the RPC protocol based on yalantinglibs [`c1cef740`](https://github.com/alibaba/yalantinglibs/tree/c1cef74057b139944c982d840c09c9940f26e08e). Compatibility with arbitrary upstream versions is not guaranteed.

| Area | Current support |
| --- | --- |
| Client initialization | `ServiceReady` and `GetStorageConfig`, returning a configuration with persistence disabled |
| Memory Master | Basic object reads/writes, segment lifecycle, and memory reclamation |
| Deployment | Single-process, single-tenant server with in-memory metadata |
| Persistence and availability | No persistent recovery or HA yet |
| Tiered storage and data plane | No NoF / LocalSSD workflows or RDMA / CUDA data transfer |
| Other limitations | No TLS, checksums, groups, or complete upstream RPC API coverage yet |

Tenant isolation and quotas are currently library features, not a multi-tenant deployment mode of the default server. See the [feature gap analysis](docs/mooncake_feature_gap.md) for detailed coverage, wire changes, and implementation priorities.

## Repository Layout

```text
src/                      Metadata, client, and segment modules; server entry point
idl/ + build.rs           Mooncake contracts and build-time code generation
crates/coro-rpc/           Reusable Rust RPC client/server and struct_pack support
crates/coro-rpc-codegen/   Thrift IDL to Rust types, clients, and server traits
tests/                    Core, wire, and server integration tests
interop/                  C++ interoperability and benchmark tools
docs/                     Design notes, runtime configuration, and benchmarks
```

Thrift is used only for interface definitions. The wire protocol remains `coro_rpc + struct_pack`, without a Thrift transport or runtime dependency.

## Documentation

The detailed documentation below is currently in Chinese.

| Document | Topics |
| --- | --- |
| [Technical reference](docs/technical_reference.md) | Runtime options, logging, RPC runtime, IDL/codegen, and interoperability examples |
| [Object catalog and RPC](docs/object_catalog_rpc.md) | Object transactions, allocators, and RPC boundaries |
| [Segment management](docs/segment_pool_backends.md) | Memory segment structure, lifecycle, and APIs |
| [Client lifecycle](docs/client_lifecycle_and_task_queue.md) | Session management, cleanup, and task queue design |
| [Memory eviction](docs/memory_eviction.md) | Watermarks, reclamation, accounting, and memory pressure |
| [Tenants and quotas](docs/tenant_quota.md) | Namespace isolation, memory quotas, and targeted reclamation |
| [Mooncake feature gaps](docs/mooncake_feature_gap.md) | Compatibility baselines, implemented capabilities, and remaining work |
| [Benchmarks](docs/object_catalog_mooncake_benchmark.md) | Comparisons with the C++ Mooncake Master and reproduction commands |

## Development

```bash
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Tests cover core domain logic, C++ golden wire vectors, generated interfaces, and RPC over real TCP connections. See the [technical reference](docs/technical_reference.md#验证) for cross-language interoperability and benchmark build instructions.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
