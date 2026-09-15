# ObjectCatalog 的 Mooncake RPC 设计

这条接口只把 Mooncake `WrappedMasterService` 的元数据 RPC 接到现有领域层，不在 RPC
handler 中复制 catalog、placement 或事务规则。当前实现提供 client lifecycle 的
`Ping`、`MountSegment`、`ReMountSegment`、`UnmountSegment`、
`GracefulUnmountSegment`，单 key `ExistKey`、`GetReplicaList`，以及
`BatchExistKey`、`BatchGetReplicaList`；`PutStart/End/Revoke` 和对应 batch 版本均已覆盖。
对象更新已覆盖 `UpsertStart/End/Revoke` 及其 batch 版本；删除已覆盖
`Remove`、`BatchRemove`、`RemoveByRegex` 和 `RemoveAll`。此外，`ServiceReady` 和
`GetStorageConfig` 提供上游 Client 初始化所需的版本握手与无持久化配置。

## 分层与同步/异步边界

```text
async WrappedMasterService handler
        ├── ServiceReady / GetStorageConfig → 固定 wire 兼容配置
        ├── Ping / segment lifecycle → ClientManager → ClientRegistry + SegmentPool
        └── object RPC → ObjectManager / TenantObjectManager
                         ├── ObjectCatalog：key 生命周期、owner、lease、pin、回收状态
                         └── ReplicaAllocator：placement 与 SegmentPool reservation
```

生成的 RPC trait 使用 `async fn`，因此网络入口可以直接被 Tokio/coro_rpc driver
调度。`ObjectManager` 刻意保持同步：目前它只访问并发内存结构和本地
`SegmentPool`，没有需要等待的 I/O，而且 maintenance 每步都有预算上限。以后若
placement 需要访问远端调度器，应把异步引入 placement/coordinator 边界，而不是
让 catalog 的纯内存状态机整体异步化。

`ObjectCatalogRpcService` 自己持有共享的 `MasterClock`。object handler 先校验 wire
请求并归一化领域输入，再取得单调 tick、执行一次有界 maintenance、解析一次 batch
tenant、调用领域 batch API 并映射返回值。RPC 与后台 controller 必须 clone 同一个 clock，避免把
不同时间原点产生的 `CatalogTick` 交给同一个 manager。批内每个 key 独立成功或失败，
只有连接/编解码失败才返回 transport-level `RpcFailure`。

所有 client/segment lifecycle RPC 使用同一个 core `ClientManager`：`Ping` 只刷新已有 session 的
heartbeat，未知 client 返回 `NEED_REMOUNT`；后者把 wire segment 转成 `SegmentSpec`，
在 per-client 锁下完成 attach/reactivate 与 session 激活，失败时回滚本次资源变更。
segment 全部 reactivate 后才发布 active session，因此 object write 不会观察到半挂载状态。
`MountSegment` 也允许 absent client 原子建立首次 session，active client 则可动态追加；
普通 `UnmountSegment` 立即 quiesce/remove 目标 segment，但保留 client session 和其他
segment。`GracefulUnmountSegment` 立即 quiesce，在 grace window 内保留已有 replica 的
liveness，到期后才 remove；它不等待 allocation 清零，也不执行数据迁移。segment remove
之后，读路径只返回仍 live 的 replica；catalog maintenance 会从 published object 原地剪掉
stale replica 并同步释放容量和 tenant quota。只要至少一个 replica 存活，对象仍然可见；
最后一个 replica 失效时才退休整个对象。当前不会自动补齐被剪掉的 replica。

常规 object maintenance 由 composition root 从 service 构造的
`MasterReconciler` 驱动，不再放在每个 RPC batch 的前置路径；write finish/revoke/remove
批次只在操作完成后补一个与 batch width 匹配的有界 step，用来及时排空本批产生的候选项，
读请求不参与维护。reconciler 默认每 100ms 依次执行
client cleanup、到期 Graceful unmount 和有界 object maintenance；topology、deadline 和
memory pressure 通知会立即触发，产生物理进展的 step 会主动 yield 并重新调度下一轮，
不把多个 step 合并成一个不可中断循环。production `ObjectManager` 同时启用 90%/80% Memory 高低水位；每个 step 在同一
collector gate 内先回收、再按去重后的物理 capacity/used 计算本轮 Memory byte target。
allocation failure 仍允许请求线程尝试一次有界回收。RPC service 与 reconciler 共享
deadline/topology `Notify`，controller 另有 memory-pressure `Notify`，因此 Graceful 不受
100ms 周期量化。
它使用 `MissedTickBehavior::Skip` 且
由调用方显式运行、停止并 join；同步
领域 manager 和单个 handler 不会隐式启动后台任务。

`ServiceReady` 返回固定上游基线的握手版本 `2.0.0`；该字符串集中定义在
`cakemaster::MOONCAKE_STORE_VERSION`，与上游 `MasterClient::Connect()` 的严格相等
校验一致。`GetStorageConfig` 固定返回 `fsdir=""`、`enable_disk_eviction=false`、
`quota_bytes=0`，表示不创建 storage backend。上游当前仅在该 RPC 失败时回退到旧
`GetFsdir`；因此空配置成功响应足以初始化无持久化 Client，兼容 fallback `GetFsdir`
仍未实现。

服务类型为 `ObjectCatalogRpcService<B>`，默认 backend 是 `ObjectManager`。RPC
adapter 内部用私有 `ObjectBatchBackend` trait 统一 batch 接口：single backend 的
request tenant 是 `()`，multi backend 是 `ResolvedTenant`。trait 通过泛型静态分发，
不引入每批虚调用；其默认 `execute_batch` 统一 maintenance、tenant 解析和逐项错误
展开，两种实现只保留实际领域调用的差异。两种具体 manager 仍是核心层的显式安全
边界。类型化 accessor 只允许 single 服务取得 raw `ObjectManager`，multi 服务只能
取得 `TenantObjectManager`。

## Production composition 与生命周期

`MooncakeServerConfig::build` 是 production composition root，默认构造 single-tenant、
process-local 的内存 backend。它按以下顺序只构造一份状态：

```text
SegmentPool
    └── Arc<ObjectManager>
          ├── MemoryEvictionController + pressure Notify
          └── ObjectCatalogRpcService + MasterClock + ClientManager + deadline Notify
                ├── WrappedMasterServiceServer
                └── MasterReconciler（从同一 service 派生）
```

`MooncakeServerComposition` 在 bind 前暴露只读 accessor，测试可直接核对 pool/manager 的
`Arc` identity、clock epoch 以及 client manager state identity。bind 成功后
`BoundMooncakeServer::run_until` 以结构化并发同时轮询 RPC server 和 reconciler；外部
shutdown、server 提前退出或 reconciler 提前退出中的任一事件都会广播停止，随后完整
等待另一个 participant。coro_rpc server 自身会 cancel 并 join 所有 connection task。
reconciler 的 shutdown 分支优先于 interval/deadline，因此 shutdown 不会补跑一次
maintenance 或 Graceful deadline。

production binary 为 `cakemaster`：

```bash
cargo run --release -- \
  --listen 127.0.0.1:50051
```

`--listen` 必须是明确的 socket address；默认是保守的 loopback
`127.0.0.1:50051`。`--max-allocator-nodes-per-segment` 默认 128K，控制每个
direct-memory allocator 预分配的区间 metadata node 数；应按最大同时存活 slice 数和
碎片余量设置，并不改变 segment 声明的字节容量。
`--expected-objects` 默认 64K，是 object index 的初始容量提示而非数量硬上限；应覆盖
峰值 indexed object，并计入 grace period 内保留的空 slot。配置偏小会允许并发 hash
table 在请求路径上扩容，形成孤立的尾延迟尖峰。
`--object-collection-budget-per-step` 默认 256，同时控制每个后台 reconcile step 的
candidate scan 和 retired-object reclaim 上限；增大它可以提高 watermark 收敛速度，
但也会扩大单步 collector 占用时间，应以目标负载下的成功率和 RPC 尾延迟共同调优，
并非越大越好。allocation-failure 请求路径仍保留独立的 64/64 预算，不受该参数影响。
逐请求 access 日志默认关闭，可通过
`--access-log` 开启；开启后会
以 info 级别记录来源、路由、sequence、结果、请求/响应大小和耗时。Unix 同时监听
Ctrl-C 和 SIGTERM，其他 Tokio 支持的平台监听
Ctrl-C。当前默认沿用 core 已验证配置：64K expected objects、64K clients、10s client
TTL、10s object lease、30s pending timeout、1GiB retired-byte ceiling、内部固定的 90%/80%
Memory 水位和 100ms reconcile interval。完整 accounting、
有界 failure retry 和 diagnostics 语义见 [`memory_eviction.md`](memory_eviction.md)。
配置及 metadata 都只在内存中，重启不恢复；没有预挂载 segment，
由 client 的 Mount/ReMount RPC 注册 Memory 容量。

这个入口只部署本文列出的当前 `WrappedMasterService` 子集，不附带 HA、持久化、TLS、
HTTP metadata、NoF/LocalSSD workflow 或 multi-tenant policy connector。已合并的
`ServiceReady` 和空 `GetStorageConfig` 由同一个生成的 `WrappedMasterServiceServer`
注册，不需要额外的 bootstrap server。

Vec 数量、wire config、replica selector 和 checksum 等纯请求校验全部发生在
`execute_batch` 之前，非法请求不会触发 maintenance 或 tenant lookup。`put_end` 的
逐项 checksum 校验会先分流合法项，只把合法 key 交给 backend，再按原索引合并结果；
backend callback 因此只包含对应的领域 batch 调用。

## ObjectManager 的职责与行为

`ObjectManager` 是 put/get/exists 的领域协调器，拥有一个 `ObjectCatalog`、一个
`ReplicaAllocator` 和可选的 production Memory eviction controller。owner、
per-key transaction、timeout candidate 和 committed version 全部由 catalog 维护，
不在 manager 中复制事务表与 deadline heap。controller 的阈值来自 composition 配置，
manager 只负责采样、设定 byte debt 和协调有界 collection。

每个 catalog slot 只有两个正交状态：一个由 `ArcSwapOption` 保存、供 reader 无锁加载的
immutable committed version，以及一个由 slot-local mutex 保护的 active transaction。
active transaction 只有 `Claimed` 和 `Staged` 两个 phase；它不改变 committed pointer。
因此不再用 `Claimed/Pending/Published/Updating/Retiring` 组合状态表达可见性，也没有
rollback pointer。retired version 属于独立的物理回收队列。controller 只决定何时设置
全局 byte debt；second-chance、lease、pin、tenant scope、segment invalidation 和 RAII
仍由 catalog/replica 层执行。

`start_put` 的顺序为：

1. 校验 object size、allocation size、replica count 和 replica class。
2. 在 catalog 中原子 claim key；同一个 key 同时只能有一个成功者。
3. 让 `ReplicaAllocator` 按 placement plan 预留空间。
4. 把 reservation 转成由 catalog 持有的 `ReplicaSet`，并将 claim stage 为
   pending version。
5. Catalog slot 只保留纯身份 `WriteOwner`、`TransactionId`、replica、pin 和超时
   deadline；
   start/stage 使用的 `WriteAdmission` fence 随 claim 离开后即释放，Manager 丢弃临时
   ticket，向 RPC 返回可写 descriptor。

任何中途失败都依靠 claim/reservation 的 RAII drop 回滚；all-or-nothing
placement 的部分 reservation 也会在返回错误前释放。

`finish_put` 按 key 查找当前 active transaction，检查 client owner 和请求的 replica
selector，再原子 commit。提交元数据固定为 `checksum=None`。相同 transaction 和 metadata
的重复 finish 是幂等成功；owner 不同返回 `ILLEGAL_CLIENT`，class 不
匹配或已失效写事务返回 `INVALID_WRITE`。

`revoke_put` 做同样的 owner/class 校验，然后撤销 active transaction。reservation
随 catalog record 进入回收流程并最终归还 allocator；已 committed 的对象不能用
revoke 删除。

`start_upsert` 对缺失 key 等价 insert；无论尺寸是否变化都申请新的 allocation，并把当前
committed version 记为 transaction base。事务期间旧版本持续可读。`finish_put` 完成所有
可能失败的校验和 quota 转换后，一次原子切换 committed pointer；旧版本按最后一次 reader
刷新后的 lease deadline 进入延迟回收，并且必须等所有本地 handle 释放后才归还 allocation
和 quota。`revoke_put`、pending timeout 或 client session fencing 只丢弃 candidate，旧
committed version 和 pin metadata 完全不变。同一对象已有 active transaction 时返回冲突，
不实现上游 UpsertStart 对旧 PROCESSING writer 的立即抢占。

当前 Mooncake wire 不携带 transaction id。Manager 只能按 `key + owner` 解析 active
transaction，因此同一 client session 对同一 key 开启新事务后，旧请求迟到的 End 无法与
新事务区分；这是保持 IDL 不变时的明确限制。

pin 变更与 write transaction 一起提交：`ENABLE` 使用请求 TTL，缺省为 30 分钟，单次
请求上限为 24 小时；`PRESERVE`/`DISABLE` 携带 TTL 会被拒绝，`ENABLE + 0` 表示提交后
没有 soft pin。deadline 从 `finish_put_at` 的提交 tick 起算。Upsert 的 soft pin action
只在 End 生效，Revoke、timeout 和 session fence 都保留旧 deadline；`PRESERVE` 继承
尚未过期的 deadline，`ENABLE` 从 commit tick 重新计算，`DISABLE` 清除。hard pin 对普通
Put 在创建时固定；replacement 保留旧 hard pin，并允许请求把它从 false 提升为 true。

`remove` 只删除已发布对象；普通删除受 lease 和 hard pin 保护，`force=true` 同时绕过
两者。pending 或
upsert 中对象返回 `REPLICA_IS_NOT_READY`，缺失对象返回 `OBJECT_NOT_FOUND`。batch 删除逐项
返回结果；regex/all 删除只统计实际成功删除的对象，并跳过仍受保护或未完成的对象。
wire 没有 hard-pin 专用错误码，普通删除 hard-pinned 对象复用 `OBJECT_HAS_LEASE`。
replication task 仍未建模，因此 force 不会绕过这类尚不存在的状态。

`get` 只返回完整 committed version：pending insert 返回 `REPLICA_IS_NOT_READY`，pending
upsert 返回旧 committed version。reader 先刷新该 version 的 lease，再验证 committed
pointer；如果 pointer 已切换就重试，因此结果在线性化上只可能属于 commit 前或 commit 后。
`exists` 与 get 使用同一可见性和 lease 语义，但只返回 bool。
`maintenance(now, budget)` 由 catalog 的单一 bounded collector 同时处理到期
soft pin、pending write、淘汰、物理回收和空 slot；soft-pin queue 每步最多扫描
`max_candidates` 个注册项，旧 generation 和被刷新 deadline 的 stale 项通过 weak node
与 deadline CAS 自动失效，不需要全表扫描。它不会在一次调用中无限扫描。诊断 snapshot
同时暴露各 candidate queue 深度，用于发现清理吞吐落后于写入吞吐。

## ReplicaAllocator 具体负责什么

`ReplicaAllocator` 只负责把一个 `PlacementRequest` 变成一组持有 reservation 的
结果：

- 从 `SegmentPool` 获取指定 `ReplicaClass` 的无锁快照；
- 过滤非 accepting、空间不足、被排除或 kind 不允许的 segment；
- 先按 preferred name，再按空闲比例、最大连续空闲区和稳定 segment id 排序；
- 按 `Segment`、`Resource` 或 `Owner` failure domain 去重；
- 逐个调用 `SegmentPool::reserve`，容忍快照过期造成的 `OutOfSpace` 或
  `NotAccepting` 并继续尝试下一个候选；
- `AllOrNothing` 未满足数量时释放全部部分结果，`BestEffort` 则返回至少一个已经
  成功的 replica。

它不负责 key 去重、write owner、pending/published 状态、lease、淘汰选择、RPC
错误码或 descriptor wire 格式。这些职责分别属于 `ObjectManager`、
`ObjectCatalog`、压力控制器和 RPC adapter。

## 当前 Mooncake 兼容子集

| Mooncake 输入 | 当前行为 |
| --- | --- |
| `replica_num > 0, nof_replica_num == 0` | Memory，和 C++ 一致使用 best-effort，但至少要成功一个 replica |
| `nof_replica_num > 0` 或非空 `preferred_nof_segments` | `INVALID_PARAMS` |
| Memory 与 NoF 同时请求 | `INVALID_PARAMS` |
| preferred Memory segment | 转成 placement preferred names |
| soft pin `PRESERVE/ENABLE/DISABLE` | 完整接入事务；TTL 只允许用于 `ENABLE`，缺省 30 分钟、最大 24 小时、0 表示不 pin |
| hard pin | 保存到 metadata；eviction 永远跳过，普通 Remove 拒绝，force Remove 可删除 |
| same-node、host/group | `INVALID_PARAMS`，避免静默降级 |
| Disk/LocalDisk selector | `INVALID_PARAMS` |
| `ObjectMeta.object_checksum=Some(...)` | `INVALID_PARAMS` |
| Get/BatchGet checksum | 永远返回 `None` |
| `ServiceReady` | 返回固定基线握手版本 `2.0.0`，满足上游 `MasterClient::Connect()` 的严格版本校验 |
| `GetStorageConfig` | 返回空 `fsdir`、关闭 disk eviction、quota 为 0；不初始化持久化 backend |
| `GetFsdir` | 尚未实现；它只是在 `GetStorageConfig` 调用失败时供旧 Client 使用的兼容 fallback |
| `Ping` | 返回 view version；已激活 session 为 `OK`，其余为 `NEED_REMOUNT` |
| `MountSegment` | absent client 原子建立 session；active client 动态追加；相同配置幂等，冲突返回 `SEGMENT_ALREADY_EXISTS` |
| `ReMountSegment` | 支持 Memory segment 的原子激活与幂等重挂载；CXL、NoF 和冲突配置返回错误 |
| `UnmountSegment` | 立即摘除单个 segment；不存在幂等成功，client session 保持 active |
| `GracefulUnmountSegment` | 立即停止新分配并在 grace deadline 摘除；不存在返回 `SEGMENT_NOT_FOUND`；依赖显式运行的 `MasterReconciler` |
| `UpsertStart/End/Revoke` + batch | 缺失 key 等价 put；始终申请新 allocation；pending 时旧 committed version 可读；end 原子切换，revoke/timeout/session-fence 保留旧版本 |
| `Remove` / `BatchRemove` | 普通模式遵守 lease 和 hard pin，force 同时绕过两者；pending/upsert 中对象拒绝删除 |
| `RemoveByRegex` / `RemoveAll` | 删除所有当前可删除的匹配对象并返回成功数量；multi-tenant 下空 tenant 的 `RemoveAll` 覆盖所有租户 |
| tenant id（single 构造） | 忽略并统一映射到 `NamespaceId::DEFAULT` |
| tenant id（multi 构造） | 映射到隔离 namespace；未知租户和超额分别返回现有 tenant 错误码 |

`ObjectDataType::KVCACHE` 和 `TENSOR` 会保留为对应的 `ObjectKind`，其余类型暂归为
`General`。空 key、零长度、batch key/length 数量不一致等均逐项返回明确错误。

## 仍需补齐的设计

RPC adapter 本身已经是薄层。tenant quota 仅计 Memory，并通过 scoped
filter 在现有 generation queue 上定向回收；整体物理水位控制仍由部署侧 controller
决定。支持混合 Memory+NoF replica 仍需要把一个 object plan 从单 class 扩展成多
class 子计划及原子回滚。group 和 checksum 是当前明确不支持的能力，不在 RPC
层用占位实现掩盖。pin 已覆盖内存态生命周期，但尚无 snapshot/oplog 恢复和独立指标。
tenant 的完整约束见 `docs/tenant_quota.md`。

## 与当前 C++ Mooncake 的已知边角差异

行为核对基于 Mooncake 提交 [`07422af7d81eb905fb8054c0f8f87bea243343f7`](https://github.com/kvcache-ai/Mooncake/tree/07422af7d81eb905fb8054c0f8f87bea243343f7)
的源码（其 `extern/yalantinglibs` 子模块有本地改动，但不影响 Master pin 逻辑）。本实现对齐提交/回滚、Upsert preserve/enable/disable、hard-pin eviction 保护以及 TTL
校验等可观察语义，但没有复制 C++ 内部数据结构：Rust 使用单调 `CatalogTick`、原子
deadline 和现有 bounded collector；C++ 使用 system clock、deadline index 和 metadata
shard。仍有这些已知差异：

- C++ 在允许 soft-pin eviction 时做“先无 pin、仍不足再 soft pin”的全局两阶段选择；
  Rust 在有界分代队列内允许该候选，不能保证跨全部对象的严格 soft-pin 低优先级。
- 本实现按需求让非 force Remove 受 hard pin 保护、force 同时绕过 lease/hard pin；当前
  C++ `Remove` 实际只检查 lease，hard pin 主要保护 eviction，因此这是有意的安全增强。
- C++ 对 mixed Memory/NoF 的 pending pin action 可在首个合格 replica 完成时提交，并让
  后续 replica End 不刷新 TTL；Rust 当前每个对象只支持单一 replica class，End 一次提交
  整个对象，所以还没有该 partial-End 边角。
- Rust 的 Upsert 始终申请 fresh allocation，包括同尺寸更新；C++ 可复用原 allocation。
  这是 per-key MVCC 保证 pending 期间旧版本持续可读的有意差异。
- pin metadata 仍是纯内存状态，服务重启不会恢复；也尚未接入上游 soft-pin key metric。

实现入口：

- `src/object/manager.rs`：领域协调器；
- `src/object/tenant/mod.rs`：tenant 公共模型与模块出口；
- `src/object/tenant/manager.rs`：tenant-safe object façade；
- `src/object/tenant/registry.rs`：ID/namespace 解析、注册与生命周期；
- `src/object/tenant/quota.rs`：quota admission、accounting 与 RAII token；
- `src/segment/placement.rs`：placement 与 reservation；
- `src/server/rpc/mod.rs`：service 与 RPC handler；
- `src/server/rpc/backend.rs`：静态 backend 契约与公共 batch 流程；
- `src/server/rpc/single_tenant.rs`、`multi_tenant.rs`：两种领域 backend 适配；
- `src/server/rpc/request.rs`、`response.rs`：wire 请求归一化与响应映射；
- `src/client/manager.rs`：client remount、session fencing 与资源清理协调；
- `tests/rpc.rs`、`client_lifecycle_rpc.rs`：真实 TCP 跨层测试。
