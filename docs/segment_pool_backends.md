# Memory SegmentPool

当前实现仅支持 Memory segment。CXL 共享 arena、NoF namespace allocator、LocalSSD
容量上报及 offload permit/lease 已移除，不再为尚未接入的后端保留分派层。
Mooncake IDL 的 variant 顺序和类型哈希保持不变；`cxl` / `nvmeof` 挂载、NoF replica
请求和 disk selector 在 RPC 边界返回 `INVALID_PARAMS`，不会降级成 Memory。

## 结构

```text
Catalog
├── segments[id] ──> SegmentEntry(spec, state, allocator, lifetime)
├── segments_by_owner
└── accepting snapshot: Arc<[SegmentHandle]>

SegmentHandle ── reserve ──> Reservation ──> ReplicaLease ──> Object
```

- `SegmentSpec::memory` 保存 identity、非零 base 的内存范围和 transport，`region()` /
  `transport()` 直接返回值或引用，不再返回 `Option`。
- 每个 segment 独占 `ByteAllocator`；相同 owner + transport 下禁止地址重叠。
  不同 owner 可使用相同虚拟地址。挂载验证也拒绝伪装成 custom protocol 的
  `cxl` / `nvmeof`。
- `SegmentPool::reserve` 直接接收 `SegmentHandle`，检查 pool identity 和当前状态；
  不再需要 `DirectCandidate` 或其他 capability wrapper。
- placement 保留 preferred names、excluded segments、free-capacity 排序，以及
  Segment / Owner failure domain；移除 kind 过滤和与 Segment 等价的 Resource domain。
- `ReservationDescriptor` / `ReservationDescriptorRef` 是 range descriptor 的别名，
  不再包装 Memory/NoF enum；`ReplicaLease` 直接持有 reservation，`DirectReplica`
  保留为它的类型别名。
- catalog 只维护一份按 segment ID 排序的 accepting snapshot；无按后端分类的索引、
  共享 resource registry 或 offload snapshot。

## 生命周期与统计

`Accepting` 接收分配，`Quiesced` 退出 placement 但仍可读，`Removed` 立即使该次挂载
的 reservation 逻辑失效。旧 snapshot 和同 ID 的新挂载不能绕过 incarnation fence。
普通 handle 不阻止卸载；已经发出的 reservation 持有 allocator 和 lease，最后一个
RAII owner 释放后才归还物理资源。

`capacity_for(ReplicaClass::Memory)` 汇总 accepting 容量；
`space_for(ReplicaClass::Memory)` 同时包含 accepting 和 quiesced 容量，避免 graceful
unmount 造成假的水位峰值。每个 segment 的容量只计一次，不需要共享资源去重。
`active_allocations` 统计仍持有容量的 reservation，不是 object reader 数量。

将来新增后端应先补齐真实生命周期和 RPC/I/O 工作流，再引入必要的抽象，而不是预置
不能端到端使用的 backend variant。
