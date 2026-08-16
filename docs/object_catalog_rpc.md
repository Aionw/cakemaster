# ObjectCatalog 的 Mooncake RPC 设计

这条接口只把 Mooncake `WrappedMasterService` 的元数据 RPC 接到现有领域层，不在 RPC
handler 中复制 catalog、placement 或事务规则。当前实现提供 client lifecycle 的
`Ping`、`MountSegment`、`ReMountSegment`、`UnmountSegment`、
`GracefulUnmountSegment`，单 key `ExistKey`、`GetReplicaList`，以及
`BatchExistKey`、`BatchGetReplicaList`、`BatchPutStart`、`BatchPutEnd` 和
`BatchPutRevoke`。对象更新已覆盖 `UpsertStart/End/Revoke` 及其 batch 版本；删除已覆盖
`Remove`、`BatchRemove`、`RemoveByRegex` 和 `RemoveAll`。此外，`ServiceReady` 和
`GetStorageConfig` 提供上游 Client 初始化所需的版本握手与无持久化配置。

## 分层与同步/异步边界

```text
async WrappedMasterService handler
        ├── ServiceReady / GetStorageConfig → 固定 wire 兼容配置
        ├── Ping / segment lifecycle → ClientManager → ClientRegistry + SegmentPool
        └── object RPC → ObjectManager / TenantObjectManager
                         ├── ObjectCatalog：key 生命周期、owner、lease、回收状态
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

每次 RPC 的 maintenance candidate budget 至少等于当前 batch item 数，因此批量写入
不会固定每批加入 333 个 timeout candidate、却长期只清理默认的 64 个；reclaim 和空
slot budget 仍使用固定上限。没有请求时，composition root 可从 service 构造
`MasterReconciler`，默认每 100ms 依次执行 client cleanup、到期 Graceful unmount 和
有界 object maintenance。RPC service 与 reconciler 共享 `Notify`：新增或提前 deadline
会重算 timer，因此 Graceful 不受 100ms 周期量化。它使用 `MissedTickBehavior::Skip` 且
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
          └── ObjectCatalogRpcService + MasterClock + ClientManager + Notify
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
`127.0.0.1:50051`。逐请求 access 日志默认关闭，可通过 `--access-log` 开启；开启后会
以 info 级别记录来源、路由、sequence、结果、请求/响应大小和耗时。Unix 同时监听
Ctrl-C 和 SIGTERM，其他 Tokio 支持的平台监听
Ctrl-C。当前默认沿用 core 已验证配置：64K expected objects、64K clients、10s client
TTL、10s object lease、30s pending timeout、1GiB retired-byte ceiling 和 100ms
reconcile interval。配置及 metadata 都只在内存中，重启不恢复；没有预挂载 segment，
由 client 的 Mount/ReMount RPC 注册 Memory/CXL 容量。

这个入口只部署本文列出的当前 `WrappedMasterService` 子集，不附带 HA、持久化、TLS、
HTTP metadata、NoF/LocalSSD workflow 或 multi-tenant policy connector。已合并的
`ServiceReady` 和空 `GetStorageConfig` 由同一个生成的 `WrappedMasterServiceServer`
注册，不需要额外的 bootstrap server。

Vec 数量、wire config、replica selector 和 checksum 等纯请求校验全部发生在
`execute_batch` 之前，非法请求不会触发 maintenance 或 tenant lookup。`put_end` 的
逐项 checksum 校验会先分流合法项，只把合法 key 交给 backend，再按原索引合并结果；
backend callback 因此只包含对应的领域 batch 调用。

## ObjectManager 的职责与行为

`ObjectManager` 是 put/get/exists 的领域协调器，只拥有一个 `ObjectCatalog` 和一个
`ReplicaAllocator`。pending node、owner、write generation、ticket 可重建信息和 timeout
candidate 全部由 catalog 维护，不再在 manager 中复制事务表与 deadline heap。它不决定
watermark 或淘汰比例。

Catalog node 的对外 lifecycle 收敛为 `Claimed`、`Pending`、`Published`、`Updating` 和
`Retiring` 五态。publish 和 replica pruning 只是 node `write_gate` 内的临界区，不再暴露
额外中间态；同尺寸更新和变尺寸更新保留的旧 generation 都使用 `Updating`，具体回滚路径
由 previous generation/owner metadata 区分。这样状态只表达可见性与资源归属差异，CAS 和
内存序统一封装在 typed lifecycle 中。

`start_put` 的顺序为：

1. 校验 object size、allocation size、replica count 和 replica class。
2. 在 catalog 中原子 claim key；同一个 key 同时只能有一个成功者。
3. 让 `ReplicaAllocator` 按 placement plan 预留空间。
4. 把 reservation 转成由 catalog 持有的 `ReplicaSet`，并将 claim stage 为
   pending object。
5. Catalog node 只保留纯身份 `WriteOwner`、`WriteId`、replica 和超时 deadline；
   start/stage 使用的 `WriteAdmission` fence 随 claim 离开后即释放，Manager 丢弃临时
   ticket，向 RPC 返回可写 descriptor。

任何中途失败都依靠 claim/reservation 的 RAII drop 回滚；all-or-nothing
placement 的部分 reservation 也会在返回错误前释放。

`finish_put` 从 catalog 当前 generation 重建 pending ticket，检查 client owner 和请求的
replica selector，再原子 publish。提交元数据固定为 `checksum=None`。相同 owner 对已经
publish 的对象重复调用 finish 是幂等成功；owner 不同返回 `ILLEGAL_CLIENT`，class 不
匹配或已失效写事务返回 `INVALID_WRITE`。

`revoke_put` 做同样的 owner/class 校验，然后撤销 pending ticket。reservation
随 catalog record 进入回收流程并最终归还 allocator；已 publish 的对象不能用
revoke 删除。

`start_upsert` 对缺失 key 复用 put 流程；对同尺寸已发布对象进入不可读的更新状态并复用
原 replica 地址；对变尺寸对象则安装新的 pending generation，同时保留旧 generation
用于失败回滚。`finish_put` 同时提交普通 put 和 upsert：变尺寸提交后旧 generation 进入
延迟回收，外部 read handle 释放后才归还 reservation 和 quota；`revoke_put`、pending
timeout 或 client session fencing 都会恢复旧 generation。同一对象已有 pending write 时
当前仍返回冲突，不实现上游 UpsertStart 对旧 PROCESSING writer 的立即抢占。

`remove` 只删除已发布对象；普通删除受 lease 保护，`force=true` 绕过 lease。pending 或
upsert 中对象返回 `REPLICA_IS_NOT_READY`，缺失对象返回 `OBJECT_NOT_FOUND`。batch 删除逐项
返回结果；regex/all 删除只统计实际成功删除的对象，并跳过仍受保护或未完成的对象。
当前没有 hard pin 和 replication task，因此 force 尚没有这两类额外状态可绕过或检查。

`get` 只返回完整 publish 的对象并刷新 lease，pending object 返回
`REPLICA_IS_NOT_READY`。`exists` 与 get 使用同一可见性和 lease 语义，但只返回
bool。`maintenance(now, budget)` 由 catalog 的单一 bounded collector 同时处理到期
pending write、淘汰、物理回收和空 slot；它不会在一次调用中无限扫描。诊断 snapshot
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
| `replica_num == 0, nof_replica_num > 0` | NoF，all-or-nothing |
| Memory 与 NoF 同时请求 | `INVALID_PARAMS` |
| preferred Memory/NoF segment | 转成 placement preferred names |
| soft pin `PRESERVE` 或无 TTL 的 `DISABLE` | 接受；当前对象保持未 soft-pin 状态 |
| soft pin `ENABLE`、任意 request TTL、hard pin、same-node、host/group | `INVALID_PARAMS`，避免静默降级 |
| Disk/LocalDisk selector | `INVALID_PARAMS` |
| `ObjectMeta.object_checksum=Some(...)` | `INVALID_PARAMS` |
| Get/BatchGet checksum | 永远返回 `None` |
| `ServiceReady` | 返回固定基线握手版本 `2.0.0`，满足上游 `MasterClient::Connect()` 的严格版本校验 |
| `GetStorageConfig` | 返回空 `fsdir`、关闭 disk eviction、quota 为 0；不初始化持久化 backend |
| `GetFsdir` | 尚未实现；它只是在 `GetStorageConfig` 调用失败时供旧 Client 使用的兼容 fallback |
| `Ping` | 返回 view version；已激活 session 为 `OK`，其余为 `NEED_REMOUNT` |
| `MountSegment` | absent client 原子建立 session；active client 动态追加；相同配置幂等，冲突返回 `SEGMENT_ALREADY_EXISTS` |
| `ReMountSegment` | 支持 Memory/CXL segment 的原子激活与幂等重挂载；NoF 和冲突配置返回错误 |
| `UnmountSegment` | 立即摘除单个 segment；不存在幂等成功，client session 保持 active |
| `GracefulUnmountSegment` | 立即停止新分配并在 grace deadline 摘除；不存在返回 `SEGMENT_NOT_FOUND`；依赖显式运行的 `MasterReconciler` |
| `UpsertStart/End/Revoke` + batch | 缺失 key 等价 put；同尺寸复用 allocation；变尺寸保留旧 generation 并支持 end/revoke/timeout/session-fence 回滚 |
| `Remove` / `BatchRemove` | 普通模式遵守 lease，force 绕过 lease；pending/upsert 中对象拒绝删除 |
| `RemoveByRegex` / `RemoveAll` | 删除所有当前可删除的匹配对象并返回成功数量；multi-tenant 下空 tenant 的 `RemoveAll` 覆盖所有租户 |
| tenant id（single 构造） | 忽略并统一映射到 `NamespaceId::DEFAULT` |
| tenant id（multi 构造） | 映射到隔离 namespace；未知租户和超额分别返回现有 tenant 错误码 |

`ObjectDataType::KVCACHE` 和 `TENSOR` 会保留为对应的 `ObjectKind`，其余类型暂归为
`General`。空 key、零长度、batch key/length 数量不一致等均逐项返回明确错误。

## 仍需补齐的设计

RPC adapter 本身已经是薄层。tenant quota 已按 Memory/NoF 分账，并通过 scoped
filter 在现有 generation queue 上定向回收；整体物理水位控制仍由部署侧 controller
决定。支持混合 Memory+NoF replica 仍需要把一个 object plan 从单 class 扩展成多
class 子计划及原子回滚。group、checksum 和 pin 是当前明确不支持的能力，不在 RPC
层用占位实现掩盖。tenant 的完整约束见 `docs/tenant_quota.md`。

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
