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

`SegmentPool` 统一负责 identity、owner 校验、catalog、quiesce/reactivate/remove
和统计，但不强行统一后端工作流：

- `SegmentSpec` 保存不可变的挂载事实；运行时 heartbeat 状态不写回 spec。
- `SegmentKind` 表示具体实现，`ReplicaClass` 表示产物类型，
  `SegmentResourceId` 表示真正共享容量或故障域的资源。这三个维度相互独立。
- `SegmentBackend::Direct` 服务 Memory、CXL、NoF；CXL logical segment clone
  同一个 arena allocator，另外保留各自的 live reservation 计数。
- `SegmentBackend::LocalSsd` 保存 heartbeat capacity、当前 offload enable 状态、
  pending bytes 和 committed bytes。
- `PoolSnapshot` 只进入同步 placement；`OffloadSnapshot` 只用于异步 offload，
  类型上阻止 LocalSSD target 被误传给 direct placement policy。
- direct allocation 返回 `Reservation`；LocalSSD admission 返回
  `OffloadPermit`。permit drop/`abort` 归还 pending capacity，带对象级 endpoint
  的 `commit` 将其转换为 `LocalSsdLease`，lease drop 归还 committed capacity。
- `ReplicaLease` 持有上述 RAII handle，所以对象发布失败、过期回收和 segment
  remove 都不需要另建一套易泄漏的容量记账。

核心关系如下：

```text
SegmentSpec (immutable mount facts)
        |
        v
Catalog + lifecycle -----> SegmentCandidate
        |                         |
        |                         +--> PoolSnapshot --> reserve --> Reservation
        |                         |
        |                         +--> OffloadSnapshot --> admit
        |                                                |
        |                                   abort/drop <--+--> commit(endpoint)
        |                                                        |
        +------------------------------------------------> LocalSsdLease
                                                                  |
                                                                  v
                                                            ReplicaLease
```

## 新增后端时的约束

新增 segment 类型时按语义回答下面的问题，不要直接给 `SegmentSpec` 增加一组
可选字段：

1. 哪些字段是不可变挂载事实，哪些是 heartbeat 或任务运行态？
2. 它产出哪个 `ReplicaClass`，物理容量由哪个 `SegmentResourceId` 唯一标识？
3. 它是 direct reservation、异步 admission，还是新的工作流？
4. 多个 logical segment 是否共享 allocator；若共享，生命周期计数是否仍需逐
   logical segment 维护？
5. descriptor 的字段在挂载、分配还是完成阶段才能确定？
6. 失败、取消、对象回收和 segment remove 时，哪个 RAII handle 负责归还资源？

只有工作流相同的后端才复用 backend capability。具体 kind 的验证和资源冲突
规则留在 attach 边界；placement 不通过 protocol string 猜测后端类型。
