# SegmentPool 异构后端设计

## Mooncake 中四类 segment 的本质差异

这份设计以 Mooncake 的 `mooncake-store/include/segment.h`、
`mooncake-store/src/segment.cpp` 和 `mooncake-store/include/replica.h` 为基线。
四种类型并不是同一个内存分配器换了 transport 名称：

| 类型 | Mooncake 的容量模型 | 分配流程 | 产出的 replica |
| --- | --- | --- | --- |
| Memory | 每个挂载拥有独立的进程地址区间和 allocator | 同步分配连续区间 | `MEMORY`，descriptor 包含地址区间和 transport |
| CXL | 多个逻辑 segment name 复用一个 `cxl_global_allocator_` | 从共享物理池同步分配 | 仍是 `MEMORY`，transport protocol 为 `cxl` |
| NoF | 独立 manager 管理 NVMe-oF namespace 和 allocator；base 是 namespace offset，允许为 0 | 从指定 namespace 同步分配区间 | `NOF_SSD`，descriptor 保留 namespace transport |
| LocalSSD | 按 client 挂载，不携带固定地址区间或挂载容量；容量与启停状态由 heartbeat 更新 | master 准入后异步 offload，成功通知才形成 replica | `LOCAL_DISK`；client id、object size 和对象级 transport endpoint 在完成时确定 |

因此，CXL 和 Memory 可以共享 replica class，却不能假装拥有独立物理容量；
LocalSSD 也不能接入 `reserve(bytes)`，因为准入成功不代表数据已经落盘。

## 当前 Rust 映射

`SegmentPool` 统一负责 identity、owner 校验、catalog、状态切换和统计，但不强行
统一不同的容量与 I/O 工作流：

- `SegmentSpec` 是一个类型，公共 identity 只保存一次；四个受控构造函数写入
  私有的 configuration variant，避免公开一组互斥的 optional 字段，也避免四个
  wrapper spec 重复转发 getter。host/rack 等调度拓扑不属于 segment 挂载事实，
  不放进 spec；当前 placement 只使用确定存在的 owner/resource failure domain。
- `SegmentKind` 表示具体实现，`ReplicaClass` 表示产物类型，
  `SegmentResourceId` 表示真正共享容量或故障域的资源。这三个维度相互独立。
- 每个 logical segment 对应一个 `SegmentEntry`，只包含 `spec + state + resource +
  usage`。`state` 是明确的 `Accepting/Quiesced/Removed`，不再增加含义重叠的通用
  包装层。
- `MountedResource::Range` 表示同步区间分配能力，Memory/NoF 使用独立 allocator，
  CXL entry 从 `ResourceRegistry` 绑定同一个 arena allocator；
  `MountedResource::LocalSsd` 保存 heartbeat capacity、offload enable、pending 和
  committed bytes。
- `ResourceRegistry` 只管理确实跨 logical segment 共享的物理资源。目前就是 CXL
  arena；最后一个 CXL entry 移除时才解除 arena 注册。Offload 不是 CXL 的子类或
  关联对象，而是另一种 capability/workflow。
- Catalog 只保存 entry、维护 candidate index，并在 attach/remove 时调用
  `mount/unmount` 接口；具体 kind 的构造、冲突检查和容量操作都不在 Catalog
  分支。新增已有 workflow 的 segment kind 不修改 Catalog。
- `PoolSnapshot` 暴露 `DirectCandidate`，只进入同步 placement；
  `OffloadSnapshot` 暴露 `OffloadTarget`，只进入异步 offload。两条路径在类型上
  不能混用。
- Memory 和 NoF descriptor 共用 `RangeDescriptor` payload，由外层 variant 标识
  replica class；LocalSSD descriptor 保留独立结构，因为对象 endpoint 只能在
  offload 完成时确定。
- direct allocation 返回 `Reservation`；LocalSSD admission 返回
  `OffloadPermit`。permit drop/`abort` 归还 pending capacity，带对象级 endpoint
  的 `commit` 将其转换为 `LocalSsdLease`，lease drop 归还 committed capacity。
- `UsageToken` 只由真正占用资源的 reservation/permit/lease 持有。
  `active_allocations` 不是上层 object handle 数；object handle 只是间接让
  `ReplicaLease` 存活。普通 `SegmentHandle` 和 snapshot 不阻止 segment remove。

核心关系如下：

```text
Catalog
├── segments[id] ──> SegmentEntry(spec, state, capacity, usage)
│                    ├── Range ─────> DirectCandidate ──> Reservation
│                    └── LocalSsd ──> OffloadTarget ────> OffloadPermit
│                                                          ├── abort/drop
│                                                          └── commit(endpoint)
│                                                              └── LocalSsdLease
└── resources[CxlArenaId] ──> shared ByteAllocator
                    ▲                         ▲
                    └── CXL entry A           └── CXL entry B

Reservation / LocalSsdLease ──> ReplicaLease ──> Object
```

## 新增后端时的约束

新增 segment 类型时按语义回答下面的问题；给私有 configuration 增加一个
variant 和受控构造函数，不要给 `SegmentSpec` 增加一组公开的可选字段：

1. 哪些字段是不可变挂载事实，哪些是 heartbeat 或任务运行态？
2. 它产出哪个 `ReplicaClass`，物理容量由哪个 `SegmentResourceId` 唯一标识？
3. 它是 direct reservation、异步 admission，还是新的工作流？
4. 多个 logical segment 是否共享 allocator；若共享，active allocation 是否仍
   需逐 logical segment 维护？
5. descriptor 的字段在挂载、分配还是完成阶段才能确定？
6. 失败、取消、对象回收和 segment remove 时，哪个 RAII handle 负责归还资源？

只有容量和工作流都相同的后端才复用 capability。具体 kind 的验证和资源冲突
规则留在 attach 边界；共享资源的 bind/unbind 留在 `ResourceRegistry`；placement
不通过 protocol string 猜测后端类型。
