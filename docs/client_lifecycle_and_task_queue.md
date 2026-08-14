# Client 生命周期与 TaskQueue 接入设计

## 结论与实施顺序

`ClientTaskQueue` 只解决一个已知 client 的异步任务投递，不负责判断 client 是否
存在、是否仍然存活、一次重连是否属于旧进程，也不负责 client 超时后的资源清理。
因此，在接入 Mooncake 的 `FetchTasks`、offload heartbeat 和 promotion heartbeat
之前，先实现统一的 Client 生命周期管理。

建议按下面的顺序落地：

1. 实现同步、可确定性测试的 `ClientRegistry`，负责 session、状态、TTL 和超时
   发现，不依赖 Tokio、RPC、`SegmentPool` 或 TaskQueue。
2. 在 core crate 实现同步 `ClientManager`，串联注册/重挂载、`Ping`、写入 fencing、
   超时清理和 per-client mount slot；server 只传入显式 tick。
3. 成功激活 client 时创建 mailbox，client 进入 draining/expired 时关闭 mailbox。
4. 生命周期稳定后再实现任务 ledger、`FetchTasks` 和各类 typed task lane。

第一阶段不实现通用任务状态机，也不把所有带 `client_id` 的 RPC 变成任务。Put、
Mount、容量上报和 completion RPC 仍然是同步领域操作；TaskQueue 只承载 Master
主动交给 Client 执行的异步工作。

## 背景与当前缺口

当前 `ClientId` 是一对 `u64` 组成的值对象，主要用作 segment owner 和 write
owner。`SegmentPool` 能校验 owner，但没有一个全局组件回答下面的问题：

- client 是否已经完成挂载并可以接收工作；
- 最近一次有效 heartbeat 是何时，何时应判定超时；
- 超时 client 的 queue、segment、pending write 和 task 应按什么顺序清理；
- 同一个 `ClientId` 再次出现时，旧清理流程是否还能影响新会话；
- Master 重启或 HA 切主后，client 是否必须 remount。

现有 `ClientTaskQueue<T>` 是有界的 per-client Tokio channel。Master producer 持有
可克隆的 `ClientTaskTx<T>`，fetch RPC handler 持有唯一的
`ClientTaskRx<T>`。这个抽象提供 FIFO、唤醒、异步背压和等待期间的取消安全，但
刻意不包含 client registry、任务完成状态和持久化。

C++ Mooncake 当前的相关语义可作为兼容基线：

- `Ping(client_id)` 返回 view version 和 `OK/NEED_REMOUNT`；
- `ReMountSegment` 用于首次连接或 heartbeat TTL 过期后的重挂载；
- `FetchTasks(client_id, batch_size)` 按 client 取得任务，
  `MarkTaskToComplete` 走独立完成路径；
- offload 和 promotion heartbeat 也会拉取按 client 保存的待办工作。

上游参考：

- [`MasterService::Ping`](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store/src/master_service.cpp#L5499-L5518)
- [`FetchTasks`/`MarkTaskToComplete`](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store/src/master_service.cpp#L9391-L9413)
- [`ClientTaskManager`](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store/include/task_manager.h#L182-L233)

## 设计目标

生命周期层需要保证以下不变量：

1. 只有成功完成注册或 remount 的 client 才是 `Active`。
2. 未知 client 的 `Ping` 只返回 `NEED_REMOUNT`，不能隐式创建 registry entry 或
   TaskQueue，避免任意 ID 撑大 Master 内存。
3. 同一时刻一个 `ClientId` 最多有一个 active session。
4. heartbeat 只能延长当前 active session，不能复活 draining 或 expired session。
5. 超时处理先将 session 从服务路径中封禁，再异步清理资源。
6. 所有清理操作都携带 session generation；旧 session 的清理不能删除新 session
   的 mailbox 或状态。
7. client 清理完成前，不允许相同 `ClientId` 建立新 session。当前 segment 只记录
   owner `ClientId`，不记录 session generation；提前重挂载会让旧清理误删新资源。
8. `ClientRegistry` 内不执行 `.await`，也不在其锁内调用 `SegmentPool`、RPC、
   TaskQueue 或任务 completion。
9. Master 重启或切主后不恢复 liveness。新进程从空 registry 开始，所有 client
   先收到 `NEED_REMOUNT`，通过 remount 重新证明资源仍然有效。

第一版假定 C++ client 每次进程启动生成新的 `ClientId`。现有 wire 只携带
`client_id`，旧进程和复用同一 ID 的新进程无法被强认证地区分；server-side
generation 可以隔离内部异步清理，但不能阻止两个进程同时使用同一 ID。若以后要
支持显式 ID 复用，需要把 session token 加入 wire，或把 session 绑定到 RPC
connection。

## Identity 与 Session

`ClientId` 是调用方提供的稳定标识；`ClientSession` 是 Master 为一次成功激活分配
的内部 incarnation：

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ClientSession {
    client_id: ClientId,
    generation: u64,
}
```

generation 由 Master 全局单调分配，进程生命周期内不复用。内部的 mailbox、清理
事件和以后任务 ledger 的 assignment 都使用 `ClientSession`，不能只使用裸
`ClientId`。RPC adapter 收到裸 `client_id` 后，必须先通过 registry 解析当前 active
session。

第一阶段继续复用 `cakemaster::segment::ClientId`，避免为了模块命名迁移所有 owner
API。生命周期实现稳定后，可以把定义提升到 `cakemaster::client::ClientId`，并在
`segment` 门面保留兼容 re-export；这个重构不应阻塞生命周期功能。

## 状态机

Registry 只持有正在建立服务关系或等待清理的 entry；完全移除后的状态用 map 中
不存在表示。

| 状态 | 含义 | 接受 Ping | 接受新工作 | 允许 remount |
| --- | --- | --- | --- | --- |
| Absent | registry 中不存在 | 返回 `NEED_REMOUNT` | 否 | 是 |
| Active | 挂载已成功，TTL 有效 | 刷新 deadline，返回 `OK` | 是 | 幂等重试可复用当前 session |
| Draining | 主动卸载或 Master shutdown | 返回 `NEED_REMOUNT` | 否 | 否，等待清理完成 |
| Expired | TTL 到期，强制清理中 | 返回 `NEED_REMOUNT` | 否 | 否，等待清理完成 |

状态转换如下：

```text
                      successful register/remount
             ┌─────────────────────────────────────┐
             │                                     ▼
          Absent                                Active
             ▲                                  │    │
             │                   graceful close │    │ TTL reached
             │                                  ▼    ▼
             └──── cleanup finished ─────── Draining Expired
```

`Draining` 和 `Expired` 的业务清理不同，但 fencing 规则相同：一旦进入这两个状态，
不能再被 heartbeat 改回 `Active`。清理完成后删除 entry；下一次 remount 才创建更高
generation 的 session。

`MountSegment` 对 active client 动态追加单个 segment；对 absent client 则显式执行
`attach_quiesced → activate session → reactivate` 的首次建立事务，任何失败都会回滚本次
attachment/session。TTL 进入 `Expired` 后仍必须先完成 cleanup，随后
`MountSegment` 或 `ReMountSegment` 才能创建更高 generation。普通 `Ping` 和 object RPC
不会隐式注册 client。

## 时间与 TTL

生命周期使用 server 端单调时间，不使用 wall clock。为便于无 sleep 的确定性测试，
core 层使用显式 tick 值：

```rust
pub struct ClientTick(u64);

pub struct ClientLifecycleConfig {
    ttl_ticks: u64,
    max_clients: usize,
    cleanup_scan_budget: usize,
}
```

配置字段保持私有，通过校验构造函数和消费式 `with_*` builder 修改，与现有 core
配置风格一致。

成功激活和有效 `Ping` 将 `expires_at` 更新为
`max(old_expires_at, now + ttl)`，乱序到达的旧 heartbeat 不能缩短 deadline。成功的
remount、mount lifecycle RPC 可以同时视为一次 heartbeat；Put/Get、task fetch 和
completion 默认不刷新 liveness，避免普通业务流量掩盖已经失效的 heartbeat loop。

过期条件统一为 `now >= expires_at`。`ttl_ticks == 0`、`max_clients == 0` 和
`cleanup_scan_budget == 0` 在配置构造时拒绝。

### Deadline 索引

第一版使用 `BinaryHeap<Reverse<DeadlineRecord>>`，但不为每个 heartbeat 都追加一条
新记录：

1. session 激活时放入一条 deadline record；
2. heartbeat 只更新 entry 中的 `expires_at`；
3. heap record 到期时重新读取 entry；若 heartbeat 已将 deadline 延后，则只把最新
   deadline 重新放回 heap；否则将 entry 标记为 `Expired`。

这样 heap 正常情况下接近每个 client 一条记录，不会随 heartbeat 次数增长。record
携带 generation；已经删除或 generation 不匹配的 record 直接丢弃。

`claim_due_cleanups(now, budget)` 最多处理 `budget` 条到期/重排记录并返回需要清理的
session。`ClientManager::run_cleanup_step` 使用配置中的 `cleanup_scan_budget` 调用它；
server timer 以后可以周期驱动 manager，再优化成按 `next_deadline()` 唤醒。core
行为不依赖具体定时器。

## ClientRegistry 与公开 Manager API

`ClientRegistry` 是同步、线程安全的底层状态机。方法返回值是状态事实或独占 cleanup
claim，不执行外部副作用。普通 RPC、timer 和 benchmark 不直接持有 registry，而是统一
通过 `ClientManager`，避免只完成状态迁移却漏掉 segment 或 pending-write cleanup。

```rust
pub enum ClientState {
    Active,
    Draining,
    Expired,
}

pub enum ActivateOutcome {
    Activated(ClientSession),
    AlreadyActive(ClientSession),
}

pub enum HeartbeatOutcome {
    Alive(ClientSession),
    NeedRemount,
}

pub enum CleanupReason {
    GracefulUnmount,
    HeartbeatExpired,
    ServerShutdown,
}

pub struct ClientCleanup {
    // 独占一个 fenced session 的 cleanup claim。
    // 未 finish 就 Drop 时会回到 maintenance retry queue。
}

impl ClientRegistry {
    pub fn activate(
        &self,
        client_id: ClientId,
        now: ClientTick,
    ) -> Result<ActivateOutcome, ClientLifecycleError>;

    pub fn heartbeat(
        &self,
        client_id: ClientId,
        now: ClientTick,
    ) -> HeartbeatOutcome;

    pub fn active_session(
        &self,
        client_id: ClientId,
    ) -> Result<ClientSession, ClientLifecycleError>;

    pub fn begin_drain(
        &self,
        session: ClientSession,
        reason: CleanupReason,
    ) -> Result<Option<ClientCleanup>, ClientLifecycleError>;

    pub fn claim_due_cleanups(
        &self,
        now: ClientTick,
        budget: usize,
    ) -> Vec<ClientCleanup>;

}

impl ClientCleanup {
    pub fn finish(self) -> Result<(), ClientLifecycleError>;
}
```

主要错误包括 `NilClientId`、`CapacityExceeded`、`CleanupInProgress`、
`ClientNotActive` 和 `StaleSession`。以下操作必须幂等：

- active client 重复激活返回 `AlreadyActive`，不分配新 generation；
- 相同 session 同时最多存在一个 cleanup claim；
- claim 未完成即 Drop 时自动回到 retry queue，不会因 future cancellation 丢失；
- 只有 claim 自身能 `finish`，因此旧 generation 不可能越过 cleanup 边界影响新 session。

Registry 对外只返回复制出的 snapshot/outcome，不暴露持锁 guard。第一版可以用一个
`parking_lot::Mutex<Inner>` 保持 map、generation allocator 和 deadline heap 的原子
关系；heartbeat 频率通常远低于 object RPC，先以正确性为主，再根据 benchmark 决定
是否分片。

`ClientManager` 的公开面只保留 composition 和业务入口：`new/with_config`、
`heartbeat`、`remount`、`write_admission`、`write_owner`、`drain_sessions` 和
`run_cleanup_step`。registry、segment pool、mount slot、cleanup claim 和 fencing guard
都不从 manager 暴露；`PendingWriteRevoker` 只是构造 manager 时传入的不透明 capability，
调用方不能直接执行 catalog cleanup。

## Core Manager 与 Server 边界

资源事务位于 core，时钟和异步驱动位于 server：

```text
cakemaster::client::ClientManager
├── ClientRegistry                       # 同步状态机
├── mount_slots[ClientId]                # per-client 资源事务串行门
├── Arc<SegmentPool>                     # segment 生命周期
└── PendingWriteRevoker                  # 不透明的 pending-write 撤销能力

cakemaster::server
├── ObjectCatalogRpcService              # wire 转换和错误映射
├── MasterClock + view version           # server/HA concern
├── MasterReconciler                     # 显式启动的有界定时收敛
└── ClientTaskHub                        # 后续接入
```

每个 mount slot 使用同步 `parking_lot::Mutex`，只串行化会改变资源归属的
register/remount、mount/unmount 和 cleanup。这里没有 I/O 或 `.await`；`Ping` 只做
registry 中的短更新，不取得 mount slot，object RPC 也不在全局 client lock 下执行。

### 注册或 remount

RPC handler 的顺序为：

1. 获取该 `ClientId` 的 mount slot；
2. 获取 slot mutex；
3. 确认 registry 中是 `Absent`，或是可幂等处理的 `Active`；
4. 校验并以 quiesced 状态挂载完整的 segment 列表；失败时回滚本次新挂载；
5. 原子 reactivate 本次 segment 集合，使资源先进入 accepting；
6. 调用 `registry.activate`，此后 write admission 才能取得 session fence；
7. 记录该 session 的完整 segment 集合并返回 RPC 成功。

步骤 4 到 7 之间没有网络 I/O。对于新 session，挂载和 registry 激活
由 manager 做成一个可回滚事务，不能出现 RPC 返回失败但部分 segment 永久
留在 pool 的情况。`registry.activate` 必须位于最后一个可失败的资源步骤之后，避免
object RPC 观察到 segment 尚未 ready 的半激活 session。

### Ping

`Ping` 调用 `registry.heartbeat(client_id, now)`：

- `Alive` 映射为 C++ `ClientStatus::OK`；
- `NeedRemount` 映射为 `ClientStatus::NEED_REMOUNT`；
- response 中同时返回当前 Master view version。

未知 `client_id` 不分配 slot、entry 或 queue。为了避免攻击者只靠 Ping 创建大量
per-client mutex，mount slot 也只在 register/remount 路径创建，或使用不持久化的
临时 gate。

### TTL 过期

过期必须先 fence、后清理：

1. `claim_due_cleanups` 在 registry 锁内把 `Active` 原子改为 `Expired`；
2. 同时失效该 session 的内部 write-admission guard，阻止已经 claim 但尚未 stage 的写入；
3. manager 获取同一个 mount slot mutex，等待正在进行的 remount 完成；
4. cleanup claim 保持该 generation 的独占权；若中途返回或 unwind，Drop 自动重试；
5. manager 在 catalog 现有 pending queue 上按本批 session 做一次筛选，主动撤销仍处于
   PENDING 的 write；
6. coordinator 将本轮到期 client 批量提交给 `SegmentPool::invalidate_owners`，一次持锁
   失效并摘除其 segment，一次重建 placement snapshot；
7. 失效 segment 上的 pending write 无需等待 write timeout；published object 的读路径
   立即过滤 stale replica，catalog maintenance 通过有界 liveness pass 剪掉并释放这些
   replica。只要仍有 live replica，Get/Exist 继续可见；最后一个 replica 失效时才退休
   metadata，外部 handle 仍可通过 RAII 延迟物理释放；
8. processing task、mailbox 和 LocalSSD workflow 在对应 subsystem 接入后由 manager
   边界扩展清理；
9. 删除已清空且无人使用的 mount slot，最后调用 `cleanup.finish()`。

每一步都必须对相同 session 幂等。任何一步失败时保留 `Expired` entry 并重试，不能先
删除 registry entry 再留下仍可分配的孤儿资源。

`ClientManager::run_cleanup_step` 已实现 1 到 7 和 9 的同步单轮入口；
server 层的 `MasterReconciler` 默认每 100ms 显式调度一轮 client cleanup、Graceful
unmount 和 object maintenance；RPC 新增或提前 Graceful deadline 时会通过共享 `Notify`
精确唤醒重算 timer，但仍由 composition root 统一管理其启动、shutdown 和 join。
task ledger 和 LocalSSD workflow 尚未接入。cleanup 完成表示资源已经逻辑失效，
不表示所有外部 object handle 都已释放或物理回收结束。

### Pending write 的两个归属维度

segment cleanup 与 writer cleanup 是两个不同问题：

- replica 所在 segment 属于已退出 client：segment incarnation 立即失效，PutEnd 失败，
  pending metadata 由 liveness pass 提前退休；
- write 由已退出 session 发起，但 replica 全部位于其他健康 client 的 segment：cleanup
  按完整 `(ClientId, generation)` 主动 Revoke，不等待 pending timeout。

第一版把纯 `(ClientId, generation)` 身份保存在可复制的 `WriteOwner`，只在
start/stage 阶段由 `WriteAdmission` 携带内部 session guard，catalog node 不持有 guard，
也不为正常 write 维护第二套 owner 索引。stage 先进入 catalog 本来就需要的 pending
timeout queue，再复查 guard；registry fence 后，批量 cleanup 在 collector consumer gate
下只扫描一次该队列。因此竞态中的 write 要么被 cleanup 看见，要么由 staging 方自行
revoke，正常 publish 路径没有额外的全局锁和索引增删。cleanup revoke 与 PutEnd 通过现有
object lifecycle CAS 竞争，谁先成功谁决定最终状态，完整 `(ClientId, generation)` 匹配也
保证旧 session 不影响新 session。

### 主动卸载与 shutdown

单 segment 的普通 `UnmountSegment` 不进入 client `Draining`：它只校验当前 session、
slot 与 owner，然后立即 `quiesce → remove`，其他 segment 和 session 继续可用。
`GracefulUnmountSegment` 同样不关闭 client session；它立即 quiesce，并以
`(ClientSession, SegmentId)` 保存最早 deadline。deadline 只持有弱 incarnation token，
到期时同时校验 session generation 与 segment incarnation，避免旧任务删除复用相同 ID
的新挂载。普通 unmount 和 client expiry 都会取消 pending job。

整个 client 或 server graceful shutdown 才进入 `Draining`，reason 使用
`GracefulUnmount` 或 `ServerShutdown`。进入 Draining 后不接受新工作；已经进入同步
handler 的有界操作可以完成，随后 cleanup worker 统一收尾。segment deadline 由显式
运行的 `MasterReconciler` 驱动，不在 RPC handler 内启动独立 worker。

## 与 TaskQueue 的连接

生命周期层稳定后增加 `ClientTaskHub`：

```rust
pub struct ClientMailbox {
    session: ClientSession,
    // 为兼容现有 C++ RPC，初期使用不同返回类型的 typed lanes。
    replication: ClientTaskQueue<DispatchToken>,
    offload: ClientTaskQueue<DispatchToken>,
    promotion: ClientTaskQueue<DispatchToken>,
    control: ClientTaskQueue<DispatchToken>,
}
```

typed lanes 是 wire 兼容要求：`FetchTasks`、`OffloadObjectHeartbeat`、
`PromotionObjectHeartbeat` 和 `PollRemoveAll` 的返回类型不同，不能让四个 RPC handler
竞争消费同一条异构 FIFO。以后增加统一的 tagged-union `FetchClientTasks` 后，才可以
考虑合并 lane。

Hub 必须遵守下面的规则：

- 只允许 `Activated(ClientSession)` 创建 mailbox，不能在第一次 enqueue 或 fetch 时
  lazy-create；
- producer 先通过 registry 解析 active session，再 clone `Tx`，释放 registry/hub
  锁后执行 `send().await`；
- fetch handler 使用唯一 `Rx`，同 client 同 lane 的并发 fetch 要串行化或明确拒绝；
- `Draining/Expired` 关闭精确 generation 的 mailbox；旧 cleanup 不能关闭新 session
  的 mailbox；
- channel 不是任务事实来源。关闭时被丢弃的 `DispatchToken` 必须能从 TaskLedger
  重建、失败或重分配；不能把 mpsc buffer 做 snapshot；
- completion RPC 继续独立处理，并校验 assigned session/task attempt。

现有 `recv_many` 会等待至少一项，而 C++ `FetchTasks` 当前允许立即返回空 batch。
接入 RPC 前需要为 queue 增加 `try_recv_many` 或有上限的
`recv_many_timeout`；producer 侧也需要 `try_send`/`send_timeout`，避免 queue 已满时
在领域锁内或 RPC handler 中无限等待。

## Master 重启与 HA

Client liveness、Tokio channel 和未确认的网络连接都不进入 snapshot。Master 新进程
或新 leader 使用新的 view version，并从空 `ClientRegistry`/`ClientTaskHub` 开始：

1. client 的下一次 `Ping` 得到 `NEED_REMOUNT`；
2. client 重新提交完整 segment 描述；
3. Master 将恢复出的 segment 状态与 remount 请求核对；
4. 核对成功后建立新 `ClientSession` 并发布 mailbox；
5. 持久化 TaskLedger 中仍可执行的任务重新 dispatch。

在 client 完成 remount 前，snapshot 恢复出的远端资源不能进入 accepting placement
snapshot。view version 属于 server/HA runtime，不放进每个 client entry。

## 可观测性

至少提供以下 gauge/counter：

- active、draining、expired 和 cleanup-retry client 数；
- activate、idempotent activate、heartbeat、unknown heartbeat、expiry 次数；
- client entry capacity rejection；
- 从 expiry 到 mailbox close、segment quiesce、cleanup finish 的延迟；
- stale generation cleanup/finish 次数；
- 每个 task lane 的深度、full/closed send 和 fetch batch size。

日志必须同时带 `client_id`、generation、旧状态、新状态和 cleanup reason。正常 heartbeat
不逐次打 INFO，避免大集群日志放大；状态转换和异常才进入 INFO/WARN。

## 测试计划

### Core 单元测试

- unknown Ping 返回 `NeedRemount` 且 registry 长度不变；
- activate 创建 session，重复 activate 保持同一 generation；
- heartbeat 延长 deadline，乱序 tick 不缩短 deadline；
- 旧 heap deadline 到达时按 entry 最新 deadline 重排，而不是误过期；
- `now == expires_at` 时进入 Expired，后续 heartbeat 不能复活；
- cleanup 完成前相同 ID activate 返回 `CleanupInProgress`；
- 旧 generation 的 finish 不能删除当前 entry；
- cleanup budget 生效，剩余到期 client 留给下一轮；
- max_clients、nil ID 和非法配置返回明确错误。

### Manager 并发与资源测试

- concurrent Ping 不丢失更新且 deadline 单调；
- remount 与 timeout cleanup 通过同一 client slot 串行；
- Activated 只创建一次 mailbox，Expired 立即关闭对应 generation；
- 旧 cleanup 与新 session 交错时不会关闭或删除新 mailbox；
- server shutdown 将所有 active client 转入 Draining，并有界等待 cleanup；
- handler cancellation/挂载失败不会留下 Active entry 或部分新挂载。

### C++ wire 跨层测试

- unknown client：`Ping -> NEED_REMOUNT`；
- remount 成功：`Ping -> OK`；
- TTL 到期：`Ping -> NEED_REMOUNT`，task fetch 被拒绝；
- cleanup 完成后 remount：建立新 server generation；
- Master view version 改变后 client 必须重新 remount。

### Client 退出风暴性能测试

`client_cleanup_benchmark` 在 direct path 上持续执行 50:50 put/get，并在流量中间通过
真实 `ClientManager::drain_sessions` 批量退出 client。默认使用 8 worker、1K client、每
client 4 个已发布对象、2048 个健康 hot key，5 轮取中位样本；
`--pending-per-client` 可同时预置由这些 session 发起的 PENDING write。健康流量也使用
`ClientManager::write_admission`，确保 benchmark 不绕过 session fence。10K client 用作手工
压力档，64K client 只验证 cleanup 可扩展性，避免把 RPC 编解码成本混入
catalog/placement 锁竞争。

```bash
cargo run --release --bin client_cleanup_benchmark -- \
  --workers=8 --operations=50000 --clients=10000 \
  --objects-per-client=4 --pending-per-client=4 \
  --hot-objects=2048 --rounds=5
```

逻辑 segment 共用一个 CXL arena，所以 allocator 节点只预分配一次，不随 client 数量
相乘。64K cleanup-only 可用下面的低内存档验证 registry/slot/claim 扩展性：

```bash
cargo run --release --bin client_cleanup_benchmark -- \
  --workers=1 --operations=0 --clients=64000 \
  --objects-per-client=0 --hot-objects=0 --rounds=3
```

输出同时包含 baseline 与 exit-storm 的吞吐、put/get p50/p99/p99.9、cleanup/settle
耗时、失效 metadata 数和健康请求错误数。验收使用同机同轮相对值：无退出 liveness
check 的吞吐/p99 退化不超过 10%；10K 风暴期间吞吐下降不超过 25%、p99 不超过
baseline 2 倍、逻辑 cleanup 在 2 秒内完成；健康对象必须零错误。64K cleanup-only
目标为 10 秒内完成且不出现超线性增长。

## 建议文件布局

第一阶段预计新增：

```text
src/client.rs                              # client 领域门面
src/client/lifecycle.rs                    # registry、状态机、deadline heap
src/client/manager.rs                      # remount、write fence 与批量资源 cleanup
src/client/config.rs                       # TTL/容量配置
src/client/error.rs                        # lifecycle errors/outcomes
tests/client_lifecycle.rs                  # 确定性 core 测试
tests/client_manager.rs                    # manager 跨资源测试
tests/client_lifecycle_rpc.rs
```

第二阶段再新增 TaskLedger、`ClientTaskHub` 和 Mooncake task RPC adapter。现有
`src/server/client_task_queue.rs` 保持为最底层 channel primitive，
不向其中加入 registry、segment cleanup 或任务持久化逻辑。

## 第一阶段验收标准

Client 生命周期管理可以在下面条件全部满足后视为完成：

1. core 状态机和 TTL 测试不依赖 wall-clock sleep；
2. unknown Ping 不分配持久状态；
3. timeout 在任何资源清理前先 fence client；
4. 相同 ID 在 cleanup 完成前无法建立新 session；
5. 所有 cleanup 都按 generation 校验，未完成的 claim 会自动进入重试队列；
6. Master restart 明确要求 remount，不恢复旧 liveness；
7. server/RPC 只使用 `ClientManager`，不取得 registry、slot 或 cleanup claim；
8. 没有在 client registry 锁内执行 await 或调用其他领域 manager。

完成这些约束后，再接入 TaskQueue 不会反过来决定 client 是否存活，也不会让 queue
关闭、RPC 取消或旧 session 清理破坏 segment/object 的领域状态。
