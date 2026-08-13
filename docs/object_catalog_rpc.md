# ObjectCatalog 的 Mooncake RPC 设计

这条接口只把 Mooncake `WrappedMasterService` 的元数据 RPC 接到现有领域层，不在 RPC
handler 中复制 catalog、placement 或事务规则。当前实现提供 client lifecycle 的
`Ping`、`ReMountSegment`，单 key `ExistKey`、`GetReplicaList`，以及
`BatchExistKey`、`BatchGetReplicaList`、`BatchPutStart`、`BatchPutEnd` 和
`BatchPutRevoke`。

## 分层与同步/异步边界

```text
async WrappedMasterService handler
        ├── Ping / ReMountSegment → ClientRuntime → ClientRegistry + SegmentPool
        └── object RPC → ObjectManager / TenantObjectManager
                         ├── ObjectCatalog：key 生命周期、owner、lease、回收状态
                         └── ReplicaAllocator：placement 与 SegmentPool reservation
```

生成的 RPC trait 使用 `async fn`，因此网络入口可以直接被 Tokio/coro_rpc driver
调度。`ObjectManager` 刻意保持同步：目前它只访问并发内存结构和本地
`SegmentPool`，没有需要等待的 I/O，而且 maintenance 每步都有预算上限。以后若
placement 需要访问远端调度器，应把异步引入 placement/coordinator 边界，而不是
让 catalog 的纯内存状态机整体异步化。

`ObjectCatalogRpcService` 的 object handler 先校验 wire 请求并归一化领域输入，再从注入的
`MasterClock` 取得单调 tick、执行一次有界 maintenance、解析一次 batch tenant、调用
领域 batch API 并映射返回值。RPC 与后台 controller 必须 clone 同一个 clock，避免把
不同时间原点产生的 `CatalogTick` 交给同一个 manager。批内每个 key 独立成功或失败，
只有连接/编解码失败才返回 transport-level `RpcFailure`。

`Ping` 和 `ReMountSegment` 则使用同一个 `ClientRuntime`：前者只刷新已有 session 的
heartbeat，未知 client 返回 `NEED_REMOUNT`；后者把 wire segment 转成 `SegmentSpec`，
在 per-client 锁下完成 attach/reactivate 与 session 激活，失败时回滚本次资源变更。

每次 RPC 的 maintenance candidate budget 至少等于当前 batch item 数，因此批量写入
不会固定每批加入 333 个 timeout candidate、却长期只清理默认的 64 个；reclaim 和空
slot budget 仍使用固定上限。没有请求时的定时维护属于 server composition root，不由
同步领域 manager 或单个 handler 隐式启动后台任务。

服务类型为 `ObjectCatalogRpcService<B>`，默认 backend 是 `ObjectManager`。RPC
adapter 内部用私有 `ObjectBatchBackend` trait 统一 batch 接口：single backend 的
request tenant 是 `()`，multi backend 是 `ResolvedTenant`。trait 通过泛型静态分发，
不引入每批虚调用；其默认 `execute_batch` 统一 maintenance、tenant 解析和逐项错误
展开，两种实现只保留实际领域调用的差异。两种具体 manager 仍是核心层的显式安全
边界。类型化 accessor 只允许 single 服务取得 raw `ObjectManager`，multi 服务只能
取得 `TenantObjectManager`。

Vec 数量、wire config、replica selector 和 checksum 等纯请求校验全部发生在
`execute_batch` 之前，非法请求不会触发 maintenance 或 tenant lookup。`put_end` 的
逐项 checksum 校验会先分流合法项，只把合法 key 交给 backend，再按原索引合并结果；
backend callback 因此只包含对应的领域 batch 调用。

## ObjectManager 的职责与行为

`ObjectManager` 是 put/get/exists 的领域协调器，只拥有一个 `ObjectCatalog` 和一个
`ReplicaAllocator`。pending node、owner、write generation、ticket 可重建信息和 timeout
candidate 全部由 catalog 维护，不再在 manager 中复制事务表与 deadline heap。它不决定
watermark 或淘汰比例。

`start_put` 的顺序为：

1. 校验 object size、allocation size、replica count 和 replica class。
2. 在 catalog 中原子 claim key；同一个 key 同时只能有一个成功者。
3. 让 `ReplicaAllocator` 按 placement plan 预留空间。
4. 把 reservation 转成由 catalog 持有的 `ReplicaSet`，并将 claim stage 为
   pending object。
5. Catalog node 保留 `WriteOwner`、`WriteId`、replica 和超时 deadline；Manager
   丢弃临时 ticket，向 RPC 返回可写 descriptor。

任何中途失败都依靠 claim/reservation 的 RAII drop 回滚；all-or-nothing
placement 的部分 reservation 也会在返回错误前释放。

`finish_put` 从 catalog 当前 generation 重建 pending ticket，检查 client owner 和请求的
replica selector，再原子 publish。提交元数据固定为 `checksum=None`。相同 owner 对已经
publish 的对象重复调用 finish 是幂等成功；owner 不同返回 `ILLEGAL_CLIENT`，class 不
匹配或已失效写事务返回 `INVALID_WRITE`。

`revoke_put` 做同样的 owner/class 校验，然后撤销 pending ticket。reservation
随 catalog record 进入回收流程并最终归还 allocator；已 publish 的对象不能用
revoke 删除。

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
| `Ping` | 返回 view version；已激活 session 为 `OK`，其余为 `NEED_REMOUNT` |
| `ReMountSegment` | 支持 Memory/CXL segment 的原子激活与幂等重挂载；NoF 和冲突配置返回错误 |
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
- `crates/cakemaster-server/src/object_catalog_rpc/mod.rs`：service 与 RPC handler；
- `crates/cakemaster-server/src/object_catalog_rpc/backend.rs`：静态 backend 契约与公共 batch 流程；
- `crates/cakemaster-server/src/object_catalog_rpc/single_tenant.rs`、`multi_tenant.rs`：两种领域 backend 适配；
- `crates/cakemaster-server/src/object_catalog_rpc/request.rs`、`response.rs`：wire 请求归一化与响应映射；
- `crates/cakemaster-server/src/client_runtime.rs`：client session、remount 与 segment 协调；
- `crates/cakemaster-server/tests/object_catalog_rpc.rs`、`client_lifecycle_rpc.rs`：真实 TCP 跨层测试。
