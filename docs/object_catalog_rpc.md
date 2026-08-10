# ObjectCatalog 的 Mooncake Batch RPC 设计

这条接口只把 Mooncake `WrappedMasterService` 的批量元数据 RPC 接到现有领域层，
不在 RPC handler 中复制 catalog、placement 或事务规则。当前实现只提供
`BatchExistKey`、`BatchGetReplicaList`、`BatchPutStart`、`BatchPutEnd` 和
`BatchPutRevoke`，没有单 key 接口。

## 分层与同步/异步边界

```text
async WrappedMasterService handler
        │ wire 校验、类型转换、错误码映射
        ▼
ObjectManager（同步、线程安全、一次调用内的有界内存工作）
        ├── ObjectCatalog：key 生命周期、owner、lease、回收状态
        └── ReplicaAllocator：placement 与 SegmentPool reservation
```

生成的 RPC trait 使用 `async fn`，因此网络入口可以直接被 Tokio/coro_rpc driver
调度。`ObjectManager` 刻意保持同步：目前它只访问并发内存结构和本地
`SegmentPool`，没有需要等待的 I/O，而且 maintenance 每步都有预算上限。以后若
placement 需要访问远端调度器，应把异步引入 placement/coordinator 边界，而不是
让 catalog 的纯内存状态机整体异步化。

`ObjectCatalogRpcService` 的 handler 只做四件事：取得单调时钟、执行一次有界
maintenance、把 wire config 归一化为领域 plan、逐项调用 `ObjectManager` 并映射
返回值。批内每个 key 独立成功或失败，只有连接/编解码失败才返回 transport-level
`RpcFailure`。

## ObjectManager 的职责与行为

`ObjectManager` 是 put/get/exists 的领域协调器，拥有一个 `ObjectCatalog`、一个
`ReplicaAllocator`，以及尚未完成的 put 事务表和 deadline heap。它不决定
watermark 或淘汰比例。

`start_put` 的顺序为：

1. 校验 object size、allocation size、replica count 和 replica class。
2. 在 catalog 中原子 claim key；同一个 key 同时只能有一个成功者。
3. 让 `ReplicaAllocator` 按 placement plan 预留空间。
4. 把 reservation 转成由 catalog 持有的 `ReplicaSet`，并将 claim stage 为
   pending object。
5. 记录 `WriteOwner`、`WriteId`、replica class、`PutTicket` 和超时 deadline，向
   RPC 返回可写 descriptor。

任何中途失败都依靠 claim/reservation 的 RAII drop 回滚；all-or-nothing
placement 的部分 reservation 也会在返回错误前释放。

`finish_put` 先检查 client owner 和请求的 replica selector，再用 pending ticket
原子 publish。提交元数据固定为 `checksum=None`。相同 owner 对已经 publish 的
对象重复调用 finish 是幂等成功；owner 不同返回 `ILLEGAL_CLIENT`，class 不匹配或
已失效写事务返回 `INVALID_WRITE`。

`revoke_put` 做同样的 owner/class 校验，然后撤销 pending ticket。reservation
随 catalog record 进入回收流程并最终归还 allocator；已 publish 的对象不能用
revoke 删除。

`get` 只返回完整 publish 的对象并刷新 lease，pending object 返回
`REPLICA_IS_NOT_READY`。`exists` 与 get 使用同一可见性和 lease 语义，但只返回
bool。`maintenance(now, budget)` 同时处理到期的 pending write 和 catalog 的
bounded collector；它不会在一次调用中无限扫描。

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
| soft/hard pin、same-node、host/group | `INVALID_PARAMS`，避免静默降级 |
| Disk/LocalDisk selector | `INVALID_PARAMS` |
| `ObjectMeta.object_checksum=Some(...)` | `INVALID_PARAMS` |
| BatchGet checksum | 永远返回 `None` |
| tenant id | 当前忽略，统一映射到 `NamespaceId::DEFAULT` |

`ObjectDataType::KVCACHE` 和 `TENSOR` 会保留为对应的 `ObjectKind`，其余类型暂归为
`General`。空 key、零长度、batch key/length 数量不一致等均逐项返回明确错误。

## 仍需补齐的设计

RPC adapter 本身已经是薄层。当前不能直接做成通用生产服务的主要缺口在压力控制
而不在 RPC：`ObjectCatalog` 目前只有全局 reclaim debt/候选队列，而生产环境需要
按 Memory/NoF class 统计水位、发起回收并保证只回收目标资源池。仓库中的 RPC
benchmark server 因此只提供明确标注的 Memory-only controller；在增加
class-aware reclaim queue/debt 前，不应把它伪装成通用后台策略。

另外，真正启用多租户前需要定义 tenant 到 `NamespaceId` 的稳定映射和 quota
admission；支持混合 Memory+NoF replica 前需要把一个 object plan 从单 class
扩展成多 class 子计划及原子回滚。checksum 和 pin 是本次明确不支持的能力，不在
RPC 层用占位实现掩盖。

实现入口：

- `src/object/manager.rs`：领域协调器；
- `src/segment/placement.rs`：placement 与 reservation；
- `crates/cakemaster-server/src/object_catalog_rpc.rs`：Mooncake wire adapter；
- `crates/cakemaster-server/tests/object_catalog_rpc.rs`：真实 TCP 跨层测试。
