# Cakemaster 与上游 C++ Mooncake Store 功能差距

## 对比基线与范围

本文固定以下源码基线，避免 `main` 持续变化后表格失去含义：

- 上游：[kvcache-ai/Mooncake `5c0724d22e7f04513a3453c8b6642a5a21b80b47`](https://github.com/kvcache-ai/Mooncake/tree/5c0724d22e7f04513a3453c8b6642a5a21b80b47)，提交时间 2026-08-11；
- 本仓库：`a69e6ceec209971000ad001d9bd32e59b5f75e0b`，提交时间 2026-08-11；
- 旧 wire 基线：本仓库现有 Mooncake IDL 和 golden vectors 对应上游
  `8c6095c06e20848506cbf91ef4a714924e7b03b1`。

主要比较上游 `mooncake-store` 的 Master、Store Client 和与二者直接相关的数据面，
不把独立的 Transfer Engine、P2P Store、Mooncake EP/PG、调度器以及 vLLM/SGLang
集成本身列为 Cakemaster 的缺失功能。Store Client 为完成 Put/Get 而使用 Transfer
Engine 的部分仍计入，因为它属于完整 Mooncake Store 的必要数据面。

状态按下面的口径判断：

- **已实现**：当前代码有可调用入口、完整状态流转和测试；
- **部分实现**：只有领域模型、底层 primitive、旧版本契约或 benchmark 组合，尚不能
  完成上游端到端行为；
- **未实现**：当前源码中没有对应的业务状态机或运行时；
- **范围待定**：若目标只是替换 Mooncake Master，可以复用上游 Client 而不在本仓库
  重写；若目标是完整 Store，则必须补齐。

设计文档不等于实现。例如 [client_lifecycle_and_task_queue.md](client_lifecycle_and_task_queue.md)
目前是接入方案，仓库里还没有其中规划的 `ClientRegistry`、`ClientRuntime`、
`TaskLedger` 或 `ClientTaskHub`。

## 结论

当前 Cakemaster 是一个高并发 metadata/placement 内核，加上一组兼容旧版 Mooncake
wire 的 batch RPC adapter；它还不是可以替换 `mooncake_master` 的服务，更不是完整的
Mooncake Store。

- 上游实际在 `RegisterRpcService` 中注册 **60** 个 coro_rpc 路由；本仓库 contract 和
  adapter 只有 **5** 个，缺少 **55** 个路由。
- 这 5 个路由只由 benchmark server 组合起来；默认 `cakemaster server` 运行的是
  `DemoService`，没有可部署的 Mooncake Master composition root。
- 对最新上游而言，5 个路由中只有 **4** 个仍保持当前 wire 契约；
  `BatchPutStart` 已发生 schema drift，最新 C++ Client 的 BatchPut 主路径无法直接使用。
- 已有 `ObjectCatalog`、`SegmentPool`、tenant quota 和 LocalSSD primitive 能复用，但
  client 生命周期、完整对象 API、分层存储任务、HA/恢复、数据面和运维面仍未完成。

因此，“基础 Memory Master 可替换”与“完整 Mooncake Store 对等”应作为两个里程碑，
不能用已经通过的 5-route benchmark 代表完整兼容。

## 当前已经具备的基础

| 能力 | 当前实现 | 边界 |
| --- | --- | --- |
| Object metadata | `ObjectCatalog` 和 `ObjectManager` 已有 claim、pending、publish、revoke、get/exists、lease、pending timeout 和有界回收 | 没有完整上游 API；checksum、pin、group、upsert 等语义未接入 |
| Segment/placement | `SegmentPool` 建模 Memory、CXL、NoF、LocalSSD，支持 direct reservation、CXL 共享 arena、LocalSSD permit/lease 和 owner/state 校验 | 没有 Mount/Ping/Remount/Unmount RPC、client TTL、NoF 探活和真实 I/O |
| Placement | 支持 preferred segment、free-capacity 排序、replica failure domain 和 RAII 回滚 | 不是上游可配置的五种策略；不支持 mixed Memory+NoF 和 host-local placement |
| Tenant | `TenantObjectManager` 已有 namespace 隔离、Memory/NoF 分账、quota admission、RAII accounting 和定向回收 | 没有上游 policy connector、HTTP admin、持久化和启动恢复 |
| Mooncake RPC | 有 `BatchExistKey`、`BatchGetReplicaList`、`BatchPutStart`、`BatchPutEnd`、`BatchPutRevoke` | 面向旧基线；只在 benchmark/测试入口组合，最新 `BatchPutStart` 已不兼容 |
| RPC runtime | TCP 上兼容 coro_rpc v0/struct_pack，支持 multiplexing、attachment、timeout、取消和流式拆帧 | 没有上游可选的 RDMA RPC socket、leader-aware client pool 和 Store API |
| Task primitive | 有单 client 的有界 `ClientTaskQueue<T>` | 只是 channel；没有 client registry、任务事实表、状态、重试、恢复或 RPC |

对应实现说明见 [object_catalog_rpc.md](object_catalog_rpc.md)、
[segment_pool_backends.md](segment_pool_backends.md) 和 [tenant_quota.md](tenant_quota.md)。

## 阻断最新上游兼容的契约漂移

上游 `5c0724d` 的 `ReplicateConfig` 已将：

```cpp
bool with_soft_pin;
```

改为：

```cpp
SoftPinAction soft_pin_action;
std::optional<uint64_t> soft_pin_ttl_ms;
```

参见上游 [`replica.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/replica.h#L63-L126)。
本仓库 [mooncake_master.thrift](../crates/cakemaster-proto/idl/mooncake_master.thrift)
仍然声明 `with_soft_pin: bool`，且没有 TTL 字段。字段类型和布局改变后，
`BatchPutStart` 请求的 struct_pack type literal/type hash 随之改变；当前 server 会以
`InvalidTypeHash` 拒绝最新 C++ 请求，而不是只把新字段忽略。

短期至少需要：

1. 更新 IDL、生成类型和 `ReplicateConfig` 请求转换；
2. 重新生成 C++ golden type literal/hash/bytes；
3. 若同时保留旧协议，需要在 decode boundary 按 type hash 显式分流两个 DTO，或提供
   版本化 endpoint，不能把两种 schema 静默解释成同一结构；
4. 增加“最新上游 C++ Client -> Rust server”的真实 BatchPutStart/BatchPut 测试。

在这项工作完成前，准确表述应是：`BatchExistKey`、`BatchGetReplicaList`、
`BatchPutEnd`、`BatchPutRevoke` 对最新基线仍兼容，`BatchPutStart` 只兼容旧基线。

## 功能差距明细

### 1. 完整对象 API 与对象语义

状态：**部分实现，基础 Memory Master 的 P0 缺口**。

当前只有五个 batch handler。以下上游能力尚未形成兼容的公开服务：

- 单 key `ExistKey`、`GetReplicaList`、`PutStart/End/Revoke`；
- `GetReplicaListByRegex`；
- `UpsertStart/End/Revoke` 及 batch 版本，包括原 allocation 复用、尺寸变化和失败回滚；
- `Remove`、`BatchRemove`、`RemoveByRegex`、`RemoveAll` 及 `force` 语义；
- `BatchReplicaClear` 和 `BatchQueryIp`；
- checksum 的保存、返回、数据面校验、snapshot/oplog 兼容；当前 RPC 明确拒绝非空
  checksum，get 永远返回 `None`；
- soft pin 的 `PRESERVE/ENABLE/DISABLE`、请求级 TTL、过期和 eviction priority；
- hard pin 及 force remove；
- optional object group 的同 shard 路由、group lease refresh 和 best-effort group eviction；
- 同一对象同时拥有 Memory 与 NoF replica；当前请求转换只允许二选一；
- Disk/LocalDisk replica 的对象提交和选择；
- `prefer_alloc_in_same_node`、`host_id` 和完整 `ObjectDataType` 行为；当前除 KVCACHE、
  TENSOR 外都折叠成 `General`；
- 上游分开的 `put_start_discard_timeout` 与 `put_start_release_timeout`。当前只有一个
  pending timeout，抢占与延迟释放的行为不等价；
- 上游 `ReplicaID` 是全局递增的 `uint64_t`；当前领域 `ReplicaId` 是每个对象内从 1
  开始的 `u32` ordinal，wire 虽扩宽成 u64，唯一性和 ID 空间仍不对等。

已经可复用的部分是 pending/published 可见性、owner 校验、lease 获取、显式 revoke、
catalog remove primitive 和资源 RAII。需要在 `ObjectManager`/tenant façade 上补齐统一的
公开事务，而不是直接从 RPC handler 操作 catalog。

上游依据：[`rpc_service.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/rpc_service.h)、
[`Master/Store 设计`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/docs/source/design/mooncake-store.md)。

### 2. Placement、淘汰和后台维护

状态：**部分实现，基础 Memory Master 的 P0/P1 缺口**。

上游可以选择 `random`、`free_ratio_first`、`ssd_free_ratio_first`、`cxl`、
`local_first` 五种策略。当前只有一个遍历 snapshot 后按 preferred name、free ratio、
最大连续空闲区排序的 `FreeCapacityPolicy`：

- 没有默认 random 和 best-of-N sampling 行为；
- 没有基于 SSD free ratio 的联动；
- CXL 有共享容量模型，但没有上游 `cxl` strategy 的单 preferred target 约束；
- 没有 host-aware `local_first`；
- 没有 deployment config 选择策略。

当前 catalog 有 second-chance 风格的有界回收，benchmark binary 也有独立 watermark
controller，但产品 server 没有上游的持续后台控制：

- Memory 和 NoF 独立 high watermark/eviction ratio；
- allocation failure 触发同步/有界 eviction retry；
- lease、soft pin、hard pin、group、replica busy 和 incomplete write 的完整候选过滤；
- soft-pin deadline 管理、Count-Min Sketch promotion admission；
- client/segment 故障后的定向清理和容量重算。

因此现有回收内核可保留，但还不能声称与 Mooncake eviction policy 等价。

上游依据：[`mooncake-store.md#AllocationStrategy`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/docs/source/design/mooncake-store.md#allocationstrategy)、
[`master_config.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/master_config.h#L24-L148)。

### 3. Client 生命周期与动态 segment 控制面

状态：**领域 primitive 已有，运行时未实现；基础 Master 的 P0 缺口**。

缺少的端到端能力包括：

- `MountSegment`、`MountNoFSegment`、`ReMount*`、`Unmount*` 和
  `GracefulUnmountSegment`；
- `Ping` 返回 view version 与 `OK/NEED_REMOUNT`；
- client registry、heartbeat TTL、session fencing、超时清理和安全重新加入；
- client 超时后 object、segment、task、offload queue 和 metadata service 注册信息的
  清理顺序；
- NoF heartbeat probe、超时、连续失败阈值与自动摘除；
- `GetAllNoFSegments`、`GetNoFSegmentsByName`、`QuerySegmentStatus*`、
  `GetStorageConfig`、`GetFsdir`、`ServiceReady`；
- graceful drain 和 segment drain job。

`SegmentPool::attach/quiesce/reactivate/remove` 可以作为这些流程的底层 capability，
但目前没有全局 client session 或 server runtime 来保证调用顺序。详细约束已在
[client_lifecycle_and_task_queue.md](client_lifecycle_and_task_queue.md) 中设计，尚待实现。

### 4. SSD/NoF/DFS 分层存储

状态：**LocalSSD/NoF 只完成模型和容量 primitive，工作流与 I/O 未实现**。

当前 LocalSSD 能做 capacity report、admission、commit/drop accounting，但没有任何
对象或 RPC 路径持有这些能力。上游仍领先的部分包括：

- `MountLocalDiskSegment`、SSD capacity heartbeat 和 per-client offload queue；
- eager offload 与 `offload_on_evict` 两种 memory -> SSD 策略；
- `OffloadObjectHeartbeat`、`NotifyOffloadSuccess`、`PollRemoveAll`；
- LOCAL_DISK descriptor 写入对象 metadata，远端读取和 buffer TTL/GC；
- SSD-only hit promotion、admission threshold、分配、成功/失败通知；
- `EvictDiskReplica`/batch、磁盘 high/low watermark、FIFO/LRU；
- bucket、file-per-key、offset-allocator 三种本地存储 backend、restart scan/recovery、
  POSIX/io_uring I/O；
- legacy DFS persistence、distributed storage/HF3FS/3FS adapter；
- NoF SSD 的真实 namespace 管理、探活和数据传输；当前 NoF 只是 range allocator。

上游依据：[`SSD Offload 设计`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/docs/source/design/ssd-offload.md)、
[`storage_backend.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/storage_backend.h)。

### 5. Copy/Move、异步任务和 drain job

状态：**未实现，只有 channel primitive**。

上游包含：

- 用户侧 `CreateCopyTask`、`CreateMoveTask`、`QueryTask`；
- client polling 的 `FetchTasks` 和 `MarkTaskToComplete`；
- `CopyStart/End/Revoke`、`MoveStart/End/Revoke` 三阶段 metadata transaction；
- pending/processing/finished 状态、上限、timeout、retry attempt 和 client assignment；
- segment drain job 的 create/query/cancel、并发度、进度和失败统计；
- HA/切主后可恢复或可重建的任务事实。

现有 `ClientTaskQueue<T>` 不保存任务事实，也没有 mailbox generation、状态、retry 或
completion validation，不能直接等同于上游 TaskManager。

上游依据：[`task_manager.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/task_manager.h)、
[`transfer_task.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/transfer_task.h)。

### 6. Multi-tenant 对等能力

状态：**核心 admission/accounting 已实现，配置、管理和恢复未实现**。

当前实现甚至比上游 memory-only quota 多了独立 NoF 账本，但它还不是可部署的上游
tenant feature：

- 没有 `enable_multi_tenants` 启动模式和 production composition；
- 没有 file/etcd YAML policy connector，也没有 connector-first 的原子 policy 更新；
- 没有 HTTP list/get/upsert/delete admin API；
- 没有 quota Prometheus metrics；
- 没有 snapshot 恢复后根据 connector policy 重建 usage/effective quota；
- 没有 HA active-only admin fencing；
- LocalSSD quota、group accounting 和上游 orphan tenant 恢复规则未实现。

本仓库 Memory/NoF 两类 quota 是有意扩展，不应为了“字段相同”退化；但 wire/admin
行为和上游不同的部分需要明确版本化和文档化。

上游依据：[`multi-tenancy.md`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/docs/source/deployment/multi-tenancy.md)、
[`tenant_quota_policy_store.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/tenant_quota_policy_store.h)。

### 7. HA、OpLog、snapshot 与恢复

状态：**未实现**。

缺少的上游能力包括：

- etcd、Redis、可选 K8s Lease leader election；
- master runtime state、leader discovery、view version 和 client switch/remount；
- Primary/Standby supervisor、active-only RPC/HTTP serving；
- etcd ordered batch OpLog、standby strict sequence apply、gap/retry/catch-up；
- snapshot fork/COW、object/segment/allocator/checksum metadata codec；
- local/S3 snapshot object store，embedded/Redis snapshot catalog，retention 与 restore；
- snapshot bootstrap + OpLog catch-up 后 promotion；
- tenant policy/usage、client liveness、soft pin、task 等各类状态的明确恢复规则。

没有这些能力时，Master 进程重启会丢失全部 metadata，且多实例不能安全共同服务。

上游依据：[`master_service_supervisor.cpp`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/ha/leadership/master_service_supervisor.cpp)、
[`ha/oplog`](https://github.com/kvcache-ai/Mooncake/tree/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/ha/oplog)、
[`ha/snapshot`](https://github.com/kvcache-ai/Mooncake/tree/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/ha/snapshot)。

### 8. Store Client 与真实数据面

状态：**未实现，范围待定**。

本仓库的 `RpcClient` 是通用 coro_rpc client，不是 Mooncake Store Client。完整 Store
仍缺少：

- high-level `Put/Get/BatchPut/BatchGet/Upsert/Remove/Query`；
- local buffer 注册、slice/stripe 规划、并行传输和 replica selection；
- TCP/RDMA/CXL、multi-NIC、GPUDirect 和自动 failover 的 Transfer Engine 数据路径；
- CPU/CUDA/HIP/Ascend/Sunrise buffer/device 支持、pinned host memory；
- checksum 计算、写入与 Get 后验证，lease 在传输完成后的再校验；
- local memcpy、local hot cache、same-node 优化；
- embedded real client、dummy-real client、standalone service、UDS/shared-memory 转发；
- C/C++、Python、Go 和上游 Rust Store API/binding。

如果 Cakemaster 的目标只是兼容 Master，应明确复用上游 C++/Python Client，并用它做
跨语言验收；此时本节不要求在本仓库重写。如果目标是独立完整 Store，本节全部是必要
工作。

上游依据：[`client_service.h`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/include/client_service.h)、
[`real_client.cpp`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/real_client.cpp)。

### 9. Production server、管理面和可观测性

状态：**未实现；benchmark 中只有局部替代**。

默认 binary 目前只启动 demo RPC。上游 production master 还具备：

- JSON/YAML/gflags 配置和完整参数校验；
- 独立 RPC/HTTP 监听地址、线程数、connection timeout、TCP_NODELAY；
- 21 个 HTTP method/route，包括 metrics、health、role/leader/HA status、key/segment
  查询、drain、tenant quota 和 remove-all；
- Prometheus metrics、summary、cache/tenant/SSD/HA/task 指标；
- KV event publisher 与 `/kv_events/status`；
- 内置 HTTP metadata server，以及 client timeout 后 metadata cleanup；
- readiness、graceful shutdown、后台 worker 生命周期和故障传播。

benchmark binary 中的固定 segment 和 eviction thread 不能替代 production
composition/configuration，也不应成为上游兼容行为的唯一入口。

上游依据：[`master.cpp`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/master.cpp)、
[`MasterAdminServer 路由`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/master_admin_service.cpp#L1190-L1291)、
[`http_metadata_server.cpp`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/http_metadata_server.cpp)。

## coro_rpc 路由差距

上游实际注册表见 [`RegisterRpcService`](https://github.com/kvcache-ai/Mooncake/blob/5c0724d22e7f04513a3453c8b6642a5a21b80b47/mooncake-store/src/rpc_service.cpp#L1637-L1775)。
按功能分组如下：

| 分组 | 上游路由数 | 当前 contract/adapter | 结论 |
| --- | ---: | ---: | --- |
| 对象、metadata 与查询 | 23 | 5 | 18 个缺失；1 个已实现路由发生最新 schema drift |
| Segment、client lifecycle 与配置 | 15 | 0 | 全部缺失 |
| LocalSSD offload/promotion | 11 | 0 | 全部缺失 |
| Copy/Move 与异步任务 | 11 | 0 | 全部缺失 |
| **合计** | **60** | **5** | **55 个路由缺失；最新完整 wire 兼容为 4/60** |

### 已有的五个路由

```text
BatchExistKey
BatchGetReplicaList
BatchPutStart          # 只兼容旧 8c6095c ReplicateConfig
BatchPutEnd
BatchPutRevoke
```

### 缺少的 55 个路由

对象与 metadata（18）：

```text
ExistKey
BatchQueryIp
BatchReplicaClear
GetReplicaListByRegex
GetReplicaList
PutStart
PutEnd
PutRevoke
UpsertStart
UpsertEnd
UpsertRevoke
BatchUpsertStart
BatchUpsertEnd
BatchUpsertRevoke
Remove
RemoveByRegex
RemoveAll
BatchRemove
```

Segment、client lifecycle 与配置（15）：

```text
MountSegment
MountNoFSegment
ReMountSegment
ReMountNoFSegment
UnmountSegment
GracefulUnmountSegment
UnmountNoFSegment
GetAllNoFSegments
GetNoFSegmentsByName
Ping
GetFsdir
QuerySegmentStatus
QuerySegmentStatusById
GetStorageConfig
ServiceReady
```

LocalSSD offload/promotion（11）：

```text
MountLocalDiskSegment
OffloadObjectHeartbeat
ReportSsdCapacity
NotifyOffloadSuccess
PromotionObjectHeartbeat
PromotionAllocStart
NotifyPromotionSuccess
NotifyPromotionFailure
EvictDiskReplica
BatchEvictDiskReplica
PollRemoveAll
```

Copy/Move 与异步任务（11）：

```text
CopyStart
CopyEnd
CopyRevoke
MoveStart
MoveEnd
MoveRevoke
CreateCopyTask
CreateMoveTask
QueryTask
FetchTasks
MarkTaskToComplete
```

这里按“实际注册”计数，不把 `WrappedMasterService` 中仅供 HTTP admin delegate 使用、
但没有注册成 coro_rpc route 的方法混入。例如 tenant quota admin 和 drain job 走 HTTP，
应在管理面单独追踪。

## 建议实施顺序

### M0：恢复最新 wire 基线

- 固定并自动检查上游 commit；
- 更新 `ReplicateConfig`、golden vectors 和 C++ 双向互通测试；
- CI 中分别跑 latest C++ -> Rust 与 Rust -> latest C++；
- 明确是否需要同时支持旧 `8c6095c` client。

### M1：可替换的基础 Memory Master

- 增加 production server composition/config；
- 实现 ClientRegistry、Ping、Mount/Remount/Unmount、TTL cleanup；
- 补齐单 key、remove、upsert、query 和管理所需的对象 API；
- 补 checksum、pin、group、mixed replica 和两个 pending timeout 语义；
- 接入 production watermark/eviction controller 与 metrics；
- 用上游 C++ Client 完成 mount -> put -> get metadata -> remove -> remount 的 E2E。

### M2：动态运维与分层存储

- graceful unmount/drain、NoF heartbeat；
- TaskLedger、copy/move 和 client task runtime；
- LocalSSD offload/promotion、disk eviction 和 storage backend；
- tenant connector/admin/persistence；
- HTTP admin、health、readiness 和 KV events。

### M3：可靠性与完整产品能力

- snapshot/restore；
- leader election、OpLog、standby catch-up/promotion；
- 如果决定自研完整 Store，再实现 Client/Transfer Engine 数据面和多语言 binding。

每个里程碑的“已实现”应以真实 C++ upstream peer 的跨进程 E2E 为准，不能只以 Rust
领域单测、生成代码存在或无状态 benchmark 返回成功为准。

## 后续更新清单

上游基线变化时，按以下顺序更新本文：

1. 记录新的 Mooncake commit 和日期；
2. diff `rpc_service.cpp::RegisterRpcService`，重新计算 route 数量；
3. diff `replica.h`、`rpc_types.h`、`segment.h`、`task_manager.h` 的所有 wire struct；
4. 重新生成 type literal/hash/bytes golden vectors；
5. 检查 `master_config.h`、admin HTTP routes、HA/snapshot 和 Store Client public API；
6. 只有入口、状态机和 E2E 都完成后才把条目从“部分实现”改为“已实现”。
