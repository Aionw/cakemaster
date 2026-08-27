//! Fixed-owner metadata shards behind an executor-independent RPC boundary.

use super::backend::{
    ObjectBatchBackend, ObjectReadSnapshot, mutation_maintenance_budget, snapshot_object_read,
};
use super::response::{map_lookup_error, map_manager_error, map_remove_error};
use crate::mooncake::{ErrorCode, ExpectedBool, ExpectedVoid};
use crate::object::diagnostics::ObjectCatalogStats;
use crate::object::reclamation::{CatalogTick, CollectBudget};
use crate::object::{
    MemoryEvictionConfig, MemoryEvictionStats, NamespaceId, ObjectCatalogConfig, ObjectIdentity,
    ObjectLookup, ObjectManager, ObjectManagerMaintenance, PendingWriteRevoker, ReplicaSelector,
    StartedPut, TenantPutRequest, WriteAdmission, WriteOwner,
};
use crate::segment::{ReplicaClass, SegmentExtentTransfer, SegmentPool, SegmentTopologyEvent};
use coro_rpc::CompioLocalTask;
use regex::Regex;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc as std_mpsc;
use std::thread::JoinHandle;
use tokio::sync::{mpsc, oneshot};

pub struct ShardedObjectManager {
    pool: Arc<SegmentPool>,
    shards: Arc<[ShardSender]>,
    threads: Vec<JoinHandle<()>>,
}

struct LocalShardContext {
    shard: usize,
    manager: Rc<ObjectManager>,
    fenced_generations: Rc<RefCell<HashMap<crate::segment::ClientId, u64>>>,
}

thread_local! {
    static LOCAL_SHARD: RefCell<Option<LocalShardContext>> = const { RefCell::new(None) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataShardStats {
    pub shard: usize,
    pub segment_pool_instance: u64,
    pub catalog: ObjectCatalogStats,
    pub memory_eviction: Option<MemoryEvictionStats>,
}

#[derive(Clone)]
struct ShardSender {
    commands: mpsc::UnboundedSender<ShardCommand>,
}

enum ShardCommand {
    Exists {
        keys: Vec<String>,
        now: CatalogTick,
        response: oneshot::Sender<Vec<ExpectedBool>>,
    },
    Get {
        keys: Vec<String>,
        now: CatalogTick,
        response: oneshot::Sender<Vec<Result<ObjectReadSnapshot, ErrorCode>>>,
    },
    StartPut {
        owner: WriteOwner,
        requests: Vec<TenantPutRequest>,
        now: CatalogTick,
        upsert: bool,
        response: oneshot::Sender<Vec<Result<StartedPut, ErrorCode>>>,
    },
    FinishPut {
        keys: Vec<String>,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
        response: oneshot::Sender<Vec<ExpectedVoid>>,
    },
    RevokePut {
        keys: Vec<String>,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
        response: oneshot::Sender<Vec<ExpectedVoid>>,
    },
    Remove {
        keys: Vec<String>,
        now: CatalogTick,
        force: bool,
        response: oneshot::Sender<Vec<ExpectedVoid>>,
    },
    RemoveMatching {
        pattern: Option<Regex>,
        now: CatalogTick,
        force: bool,
        response: oneshot::Sender<usize>,
    },
    Maintenance {
        now: CatalogTick,
        budget: CollectBudget,
        response: oneshot::Sender<ObjectManagerMaintenance>,
    },
    RevokePending {
        owners: Vec<WriteOwner>,
        now: CatalogTick,
        response: std_mpsc::Sender<usize>,
    },
    Stats {
        shard: usize,
        response: std_mpsc::Sender<MetadataShardStats>,
    },
    Topology {
        event: SegmentTopologyEvent,
        response: std_mpsc::Sender<()>,
    },
    TakeEmptyExtent {
        shard: usize,
        replica_class: ReplicaClass,
        minimum_bytes: u64,
        response: oneshot::Sender<Option<SegmentExtentTransfer>>,
    },
    AddExtent {
        shard: usize,
        transfer: SegmentExtentTransfer,
        response: oneshot::Sender<()>,
    },
    StartRuntimeTask {
        task: CompioLocalTask,
        exit: oneshot::Sender<io::Result<()>>,
        response: std_mpsc::Sender<()>,
    },
    StopRuntimeTasks {
        response: oneshot::Sender<()>,
    },
    Shutdown,
}

impl ShardedObjectManager {
    pub(crate) fn new(
        pool: Arc<SegmentPool>,
        object_config: ObjectCatalogConfig,
        eviction_config: MemoryEvictionConfig,
        shard_count: usize,
    ) -> Result<Self, crate::object::error::ObjectCatalogConfigError> {
        assert_ne!(shard_count, 0, "metadata shard count must be positive");
        let cores = core_affinity::get_core_ids().unwrap_or_default();
        let mut shards = Vec::with_capacity(shard_count);
        let mut threads = Vec::with_capacity(shard_count);
        let per_shard_config = object_config.split_for_shards(shard_count);

        for shard in 0..shard_count {
            let local_pool = Arc::new(
                SegmentPool::with_config(pool.config().for_allocator_shard(shard, shard_count))
                    .expect("a valid control pool config remains valid for one shard-local extent"),
            );
            let manager =
                ObjectManager::for_shard(local_pool, per_shard_config, eviction_config, shard)?;
            let (commands, receiver) = mpsc::unbounded_channel();
            let core = (!cores.is_empty()).then(|| cores[shard % cores.len()]);
            let thread = std::thread::Builder::new()
                .name(format!("cakemaster-meta-{shard}"))
                .spawn(move || run_shard(shard, manager, receiver, core))
                .expect("metadata shard threads must start");
            shards.push(ShardSender { commands });
            threads.push(thread);
        }

        let shards: Arc<[ShardSender]> = shards.into();
        let topology_shards = shards.clone();
        pool.install_topology_sink(move |event| {
            let mut receivers = Vec::with_capacity(topology_shards.len());
            for (shard_index, shard) in topology_shards.iter().enumerate() {
                let event =
                    topology_event_for_shard(event.clone(), shard_index, topology_shards.len());
                if apply_local_topology(shard_index, event.clone()) {
                    continue;
                }
                let (response, receiver) = std_mpsc::channel();
                if shard
                    .commands
                    .send(ShardCommand::Topology { event, response })
                    .is_ok()
                {
                    receivers.push(receiver);
                }
            }
            for receiver in receivers {
                receiver
                    .recv()
                    .expect("metadata shard applies topology before control RPC returns");
            }
        });

        Ok(Self {
            pool,
            shards,
            threads,
        })
    }

    pub const fn pool(&self) -> &Arc<SegmentPool> {
        &self.pool
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub(crate) fn start_runtime_tasks(
        &self,
        tasks: Vec<CompioLocalTask>,
    ) -> io::Result<Vec<oneshot::Receiver<io::Result<()>>>> {
        if tasks.len() != self.shards.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "received {} runtime tasks for {} metadata shards",
                    tasks.len(),
                    self.shards.len()
                ),
            ));
        }
        let mut exits = Vec::with_capacity(tasks.len());
        let mut starts = Vec::with_capacity(tasks.len());
        for (shard, task) in self.shards.iter().zip(tasks) {
            let (exit, exit_receiver) = oneshot::channel();
            let (response, receiver) = std_mpsc::channel();
            shard
                .commands
                .send(ShardCommand::StartRuntimeTask {
                    task,
                    exit,
                    response,
                })
                .map_err(|_| io::Error::other("metadata shard stopped before RPC startup"))?;
            exits.push(exit_receiver);
            starts.push(receiver);
        }
        for start in starts {
            start
                .recv()
                .map_err(|_| io::Error::other("metadata shard stopped during RPC startup"))?;
        }
        Ok(exits)
    }

    pub(crate) async fn stop_runtime_tasks(&self) {
        let mut receivers = Vec::with_capacity(self.shards.len());
        for shard in self.shards.iter() {
            let (response, receiver) = oneshot::channel();
            if shard
                .commands
                .send(ShardCommand::StopRuntimeTasks { response })
                .is_ok()
            {
                receivers.push(receiver);
            }
        }
        for receiver in receivers {
            let _ = receiver.await;
        }
    }

    pub fn pending_write_revoker(&self) -> PendingWriteRevoker {
        let shards = self.shards.clone();
        PendingWriteRevoker::from_fn(move |sessions, now| {
            let owners = sessions
                .iter()
                .copied()
                .map(WriteOwner::for_session)
                .collect::<Vec<_>>();
            if owners.is_empty() {
                return 0;
            }
            let mut receivers = Vec::with_capacity(shards.len());
            let mut revoked = 0;
            for (shard_index, shard) in shards.iter().enumerate() {
                if let Some(local_revoked) = revoke_local_pending(shard_index, &owners, now) {
                    revoked += local_revoked;
                    continue;
                }
                let (response, receiver) = std_mpsc::channel();
                if shard
                    .commands
                    .send(ShardCommand::RevokePending {
                        owners: owners.clone(),
                        now,
                        response,
                    })
                    .is_ok()
                {
                    receivers.push(receiver);
                }
            }
            revoked
                + receivers
                    .into_iter()
                    .filter_map(|receiver| receiver.recv().ok())
                    .sum::<usize>()
        })
    }

    pub fn catalog_stats(&self) -> ObjectCatalogStats {
        self.shard_stats()
            .into_iter()
            .map(|shard| shard.catalog)
            .fold(ObjectCatalogStats::default(), add_catalog_stats)
    }

    pub fn shard_stats(&self) -> Vec<MetadataShardStats> {
        let mut receivers = Vec::with_capacity(self.shards.len());
        for (shard_index, shard) in self.shards.iter().enumerate() {
            let (response, receiver) = std_mpsc::channel();
            if shard
                .commands
                .send(ShardCommand::Stats {
                    shard: shard_index,
                    response,
                })
                .is_ok()
            {
                receivers.push(receiver);
            }
        }
        receivers
            .into_iter()
            .filter_map(|receiver| receiver.recv().ok())
            .collect()
    }

    pub fn memory_eviction_stats(&self) -> Option<MemoryEvictionStats> {
        self.shard_stats()
            .into_iter()
            .filter_map(|shard| shard.memory_eviction)
            .reduce(add_eviction_stats)
    }

    pub(crate) async fn maintenance(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
    ) -> ObjectManagerMaintenance {
        let mut receivers = Vec::with_capacity(self.shards.len());
        for (shard_index, shard) in self.shards.iter().enumerate() {
            let (response, receiver) = oneshot::channel();
            if shard
                .commands
                .send(ShardCommand::Maintenance {
                    now,
                    budget: divide_budget(budget, self.shards.len(), shard_index),
                    response,
                })
                .is_ok()
            {
                receivers.push(receiver);
            }
        }
        let mut total = ObjectManagerMaintenance::default();
        for receiver in receivers {
            if let Ok(report) = receiver.await {
                total = add_maintenance(total, report);
            }
        }
        total
    }

    async fn rebalance_capacity(
        &self,
        borrower: usize,
        replica_class: ReplicaClass,
        minimum_bytes: u64,
    ) -> bool {
        for donor in 0..self.shards.len() {
            if donor == borrower {
                continue;
            }
            let (response, receiver) = oneshot::channel();
            if self.shards[donor]
                .commands
                .send(ShardCommand::TakeEmptyExtent {
                    shard: donor,
                    replica_class,
                    minimum_bytes,
                    response,
                })
                .is_err()
            {
                continue;
            }
            let Ok(Some(transfer)) = receiver.await else {
                continue;
            };
            let (response, receiver) = oneshot::channel();
            if self.shards[borrower]
                .commands
                .send(ShardCommand::AddExtent {
                    shard: borrower,
                    transfer,
                    response,
                })
                .is_err()
            {
                return false;
            }
            return receiver.await.is_ok();
        }
        false
    }

    fn owner(&self, tenant: &str, key: &str) -> usize {
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in tenant
            .bytes()
            .chain([0xff])
            .chain(NamespaceId::DEFAULT.get().to_le_bytes())
            .chain([0xfe])
            .chain(key.bytes())
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        (hash as usize) % self.shards.len()
    }

    async fn dispatch_grouped<T, R>(
        &self,
        tenant: &str,
        items: Vec<T>,
        key: impl Fn(&T) -> &str,
        command: impl Fn(Vec<T>, oneshot::Sender<Vec<Result<R, ErrorCode>>>) -> ShardCommand,
    ) -> Vec<Result<R, ErrorCode>>
    where
        T: Send + 'static,
        R: Send + 'static,
    {
        let item_count = items.len();
        let mut groups = (0..self.shards.len())
            .map(|_| Vec::new())
            .collect::<Vec<Vec<(usize, T)>>>();
        for (index, item) in items.into_iter().enumerate() {
            groups[self.owner(tenant, key(&item))].push((index, item));
        }

        let mut pending = Vec::new();
        let mut output = (0..item_count).map(|_| None).collect::<Vec<_>>();
        for (shard_index, group) in groups.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let (indices, items): (Vec<_>, Vec<_>) = group.into_iter().unzip();
            let (response, receiver) = oneshot::channel();
            if self.shards[shard_index]
                .commands
                .send(command(items, response))
                .is_err()
            {
                for index in indices {
                    output[index] = Some(Err(ErrorCode::InternalError));
                }
            } else {
                pending.push((indices, receiver));
            }
        }

        for (indices, receiver) in pending {
            match receiver.await {
                Ok(results) if results.len() == indices.len() => {
                    for (index, result) in indices.into_iter().zip(results) {
                        output[index] = Some(result);
                    }
                }
                _ => {
                    for index in indices {
                        output[index] = Some(Err(ErrorCode::InternalError));
                    }
                }
            }
        }
        output
            .into_iter()
            .map(|result| result.unwrap_or(Err(ErrorCode::InternalError)))
            .collect()
    }

    async fn dispatch_start(
        &self,
        tenant: &str,
        admission: WriteAdmission,
        requests: Vec<TenantPutRequest>,
        now: CatalogTick,
        upsert: bool,
    ) -> Vec<Result<StartedPut, ErrorCode>> {
        if !admission.is_active() {
            return requests
                .into_iter()
                .map(|_| Err(ErrorCode::InvalidWrite))
                .collect();
        }
        let owner = admission.owner();
        let originals = requests.clone();
        let mut results = self
            .dispatch_grouped(
                tenant,
                requests,
                TenantPutRequest::key,
                |requests, response| ShardCommand::StartPut {
                    owner,
                    requests,
                    now,
                    upsert,
                    response,
                },
            )
            .await;
        if self
            .reject_fenced_started(tenant, &admission, &originals, &mut results, now)
            .await
        {
            return results;
        }

        let mut requirements = HashMap::<(usize, ReplicaClass), u64>::new();
        for (request, result) in originals.iter().zip(&results) {
            if !matches!(result, Err(ErrorCode::NoAvailableHandle)) {
                continue;
            }
            let placement = request.plan().placement();
            requirements
                .entry((self.owner(tenant, request.key()), placement.replica_class()))
                .and_modify(|bytes| *bytes = (*bytes).max(placement.allocation().bytes()))
                .or_insert(placement.allocation().bytes());
        }

        let mut moved_capacity = false;
        for ((borrower, replica_class), minimum_bytes) in requirements {
            moved_capacity |= self
                .rebalance_capacity(borrower, replica_class, minimum_bytes)
                .await;
        }
        if !moved_capacity {
            return results;
        }

        let retry = originals
            .iter()
            .cloned()
            .enumerate()
            .filter(|(index, _)| matches!(results[*index], Err(ErrorCode::NoAvailableHandle)))
            .collect::<Vec<_>>();
        let (indices, requests): (Vec<_>, Vec<_>) = retry.into_iter().unzip();
        let retry_results = self
            .dispatch_grouped(
                tenant,
                requests,
                TenantPutRequest::key,
                |requests, response| ShardCommand::StartPut {
                    owner,
                    requests,
                    now,
                    upsert,
                    response,
                },
            )
            .await;
        for (index, result) in indices.into_iter().zip(retry_results) {
            results[index] = result;
        }
        self.reject_fenced_started(tenant, &admission, &originals, &mut results, now)
            .await;
        results
    }

    async fn reject_fenced_started(
        &self,
        tenant: &str,
        admission: &WriteAdmission,
        requests: &[TenantPutRequest],
        results: &mut [Result<StartedPut, ErrorCode>],
        now: CatalogTick,
    ) -> bool {
        if admission.is_active() {
            return false;
        }

        let mut started_by_class = HashMap::<ReplicaClass, Vec<String>>::new();
        for (request, result) in requests.iter().zip(results.iter()) {
            if let Ok(started) = result {
                started_by_class
                    .entry(started.replica_class())
                    .or_default()
                    .push(request.key().to_owned());
            }
        }
        for (replica_class, keys) in started_by_class {
            let _ = self
                .dispatch_grouped(tenant, keys, String::as_str, |keys, response| {
                    ShardCommand::RevokePut {
                        keys,
                        owner: admission.owner(),
                        selector: ReplicaSelector::Class(replica_class),
                        now,
                        response,
                    }
                })
                .await;
        }
        results.fill_with(|| Err(ErrorCode::InvalidWrite));
        true
    }

    async fn scatter_remove_matching(
        &self,
        pattern: Option<Regex>,
        now: CatalogTick,
        force: bool,
    ) -> Result<usize, ErrorCode> {
        let mut receivers = Vec::with_capacity(self.shards.len());
        for shard in self.shards.iter() {
            let (response, receiver) = oneshot::channel();
            shard
                .commands
                .send(ShardCommand::RemoveMatching {
                    pattern: pattern.clone(),
                    now,
                    force,
                    response,
                })
                .map_err(|_| ErrorCode::InternalError)?;
            receivers.push(receiver);
        }
        let mut removed = 0_usize;
        for receiver in receivers {
            removed = removed.saturating_add(receiver.await.map_err(|_| ErrorCode::InternalError)?);
        }
        Ok(removed)
    }
}

impl Drop for ShardedObjectManager {
    fn drop(&mut self) {
        for shard in self.shards.iter() {
            let _ = shard.commands.send(ShardCommand::Shutdown);
        }
        let current_thread = std::thread::current().id();
        for thread in self.threads.drain(..) {
            if thread.thread().id() != current_thread {
                let _ = thread.join();
            }
        }
    }
}

#[async_trait::async_trait]
impl ObjectBatchBackend for ShardedObjectManager {
    type Tenant = Arc<str>;

    fn resolve_tenant(&self, tenant_id: &str) -> Result<Self::Tenant, ErrorCode> {
        Ok(Arc::from(tenant_id))
    }

    async fn exists_batch(
        &self,
        tenant: Self::Tenant,
        keys: Vec<String>,
        now: CatalogTick,
    ) -> Vec<ExpectedBool> {
        self.dispatch_grouped(&tenant, keys, String::as_str, |keys, response| {
            ShardCommand::Exists {
                keys,
                now,
                response,
            }
        })
        .await
    }

    async fn get_batch(
        &self,
        tenant: Self::Tenant,
        keys: Vec<String>,
        now: CatalogTick,
    ) -> Vec<Result<ObjectReadSnapshot, ErrorCode>> {
        self.dispatch_grouped(&tenant, keys, String::as_str, |keys, response| {
            ShardCommand::Get {
                keys,
                now,
                response,
            }
        })
        .await
    }

    async fn start_put_batch(
        &self,
        tenant: Self::Tenant,
        admission: WriteAdmission,
        requests: Vec<TenantPutRequest>,
        now: CatalogTick,
    ) -> Vec<Result<StartedPut, ErrorCode>> {
        self.dispatch_start(&tenant, admission, requests, now, false)
            .await
    }

    async fn start_upsert_batch(
        &self,
        tenant: Self::Tenant,
        admission: WriteAdmission,
        requests: Vec<TenantPutRequest>,
        now: CatalogTick,
    ) -> Vec<Result<StartedPut, ErrorCode>> {
        self.dispatch_start(&tenant, admission, requests, now, true)
            .await
    }

    async fn finish_put_batch(
        &self,
        tenant: Self::Tenant,
        keys: Vec<String>,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Vec<ExpectedVoid> {
        self.dispatch_grouped(&tenant, keys, String::as_str, |keys, response| {
            ShardCommand::FinishPut {
                keys,
                owner,
                selector,
                now,
                response,
            }
        })
        .await
    }

    async fn revoke_put_batch(
        &self,
        tenant: Self::Tenant,
        keys: Vec<String>,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Vec<ExpectedVoid> {
        self.dispatch_grouped(&tenant, keys, String::as_str, |keys, response| {
            ShardCommand::RevokePut {
                keys,
                owner,
                selector,
                now,
                response,
            }
        })
        .await
    }

    async fn remove_batch(
        &self,
        tenant: Self::Tenant,
        keys: Vec<String>,
        now: CatalogTick,
        force: bool,
    ) -> Vec<ExpectedVoid> {
        self.dispatch_grouped(&tenant, keys, String::as_str, |keys, response| {
            ShardCommand::Remove {
                keys,
                now,
                force,
                response,
            }
        })
        .await
    }

    async fn remove_matching(
        &self,
        _tenant: Self::Tenant,
        pattern: Option<Regex>,
        now: CatalogTick,
        force: bool,
    ) -> Result<usize, ErrorCode> {
        self.scatter_remove_matching(pattern, now, force).await
    }

    async fn remove_all(
        &self,
        _tenant_id: &str,
        now: CatalogTick,
        force: bool,
    ) -> Result<usize, ErrorCode> {
        self.scatter_remove_matching(None, now, force).await
    }
}

fn run_shard(
    shard: usize,
    manager: ObjectManager,
    mut receiver: mpsc::UnboundedReceiver<ShardCommand>,
    core: Option<core_affinity::CoreId>,
) {
    if let Some(core) = core
        && !core_affinity::set_for_current(core)
    {
        log::warn!(
            target: "cakemaster::server::sharded",
            core_id = core.id;
            "failed to pin metadata shard thread"
        );
    }
    let runtime = compio::runtime::Runtime::builder()
        .event_interval(32)
        .sync_queue_size(4096)
        .local_queue_size(1024)
        .build()
        .expect("metadata shard Compio runtime must start");
    runtime.block_on(async move {
        let manager = Rc::new(manager);
        let fenced_generations =
            Rc::new(RefCell::new(HashMap::<crate::segment::ClientId, u64>::new()));
        LOCAL_SHARD.with(|local| {
            *local.borrow_mut() = Some(LocalShardContext {
                shard,
                manager: manager.clone(),
                fenced_generations: fenced_generations.clone(),
            });
        });
        let mut runtime_tasks = Vec::<compio::runtime::JoinHandle<()>>::new();
        while let Some(command) = receiver.recv().await {
            match command {
                ShardCommand::Exists {
                    keys,
                    now,
                    response,
                } => {
                    let _ = response.send(
                        keys.into_iter()
                            .map(|key| {
                                Ok(manager
                                    .exists(ObjectLookup::new(NamespaceId::DEFAULT, &key), now))
                            })
                            .collect(),
                    );
                }
                ShardCommand::Get {
                    keys,
                    now,
                    response,
                } => {
                    let _ = response.send(
                        keys.into_iter()
                            .map(|key| {
                                manager
                                    .get(ObjectLookup::new(NamespaceId::DEFAULT, &key), now)
                                    .map_err(map_lookup_error)
                                    .and_then(snapshot_object_read)
                            })
                            .collect(),
                    );
                }
                ShardCommand::StartPut {
                    owner,
                    requests,
                    now,
                    upsert,
                    response,
                } => {
                    let owner_is_fenced = owner.session_generation() != 0
                        && fenced_generations
                            .borrow()
                            .get(&owner.client())
                            .is_some_and(|generation| owner.session_generation() <= *generation);
                    let results = if owner_is_fenced {
                        requests
                            .into_iter()
                            .map(|_| Err(ErrorCode::InvalidWrite))
                            .collect()
                    } else {
                        let admission = WriteAdmission::from_owner(owner);
                        requests
                            .into_iter()
                            .map(|request| {
                                let (key, plan) = request.into_parts();
                                let identity = ObjectIdentity::new(NamespaceId::DEFAULT, key);
                                if upsert {
                                    manager.start_upsert(identity, admission.clone(), plan, now)
                                } else {
                                    manager.start_put(identity, admission.clone(), plan, now)
                                }
                                .map_err(map_manager_error)
                            })
                            .collect()
                    };
                    let _ = response.send(results);
                }
                ShardCommand::FinishPut {
                    keys,
                    owner,
                    selector,
                    now,
                    response,
                } => {
                    let item_count = keys.len();
                    let results = keys
                        .into_iter()
                        .map(|key| {
                            manager
                                .finish_put_at(
                                    &ObjectIdentity::new(NamespaceId::DEFAULT, key),
                                    owner,
                                    selector,
                                    now,
                                )
                                .map_err(map_manager_error)
                        })
                        .collect();
                    let _ = manager.maintenance(now, mutation_maintenance_budget(item_count));
                    let _ = response.send(results);
                }
                ShardCommand::RevokePut {
                    keys,
                    owner,
                    selector,
                    now,
                    response,
                } => {
                    let item_count = keys.len();
                    let results = keys
                        .into_iter()
                        .map(|key| {
                            manager
                                .revoke_put(
                                    &ObjectIdentity::new(NamespaceId::DEFAULT, key),
                                    owner,
                                    selector,
                                    now,
                                )
                                .map_err(map_manager_error)
                        })
                        .collect();
                    let _ = manager.maintenance(now, mutation_maintenance_budget(item_count));
                    let _ = response.send(results);
                }
                ShardCommand::Remove {
                    keys,
                    now,
                    force,
                    response,
                } => {
                    let item_count = keys.len();
                    let results = keys
                        .into_iter()
                        .map(|key| {
                            manager
                                .remove(ObjectLookup::new(NamespaceId::DEFAULT, &key), now, force)
                                .map_err(map_remove_error)
                        })
                        .collect();
                    let _ = manager.maintenance(now, mutation_maintenance_budget(item_count));
                    let _ = response.send(results);
                }
                ShardCommand::RemoveMatching {
                    pattern,
                    now,
                    force,
                    response,
                } => {
                    let removed =
                        manager.remove_matching(NamespaceId::DEFAULT, pattern.as_ref(), now, force);
                    let _ = manager.maintenance(now, mutation_maintenance_budget(removed));
                    let _ = response.send(removed);
                }
                ShardCommand::Maintenance {
                    now,
                    budget,
                    response,
                } => {
                    let _ = response.send(manager.maintenance(now, budget));
                }
                ShardCommand::RevokePending {
                    owners,
                    now,
                    response,
                } => {
                    for owner in &owners {
                        fenced_generations
                            .borrow_mut()
                            .entry(owner.client())
                            .and_modify(|generation| {
                                *generation = (*generation).max(owner.session_generation());
                            })
                            .or_insert(owner.session_generation());
                    }
                    let _ = response.send(manager.revoke_pending_owners(&owners, now));
                }
                ShardCommand::Stats { shard, response } => {
                    let _ = response.send(MetadataShardStats {
                        shard,
                        segment_pool_instance: manager.pool().instance_id(),
                        catalog: manager.catalog().stats(),
                        memory_eviction: manager.memory_eviction_stats(),
                    });
                }
                ShardCommand::Topology { event, response } => {
                    apply_topology(manager.pool(), event);
                    let _ = response.send(());
                }
                ShardCommand::TakeEmptyExtent {
                    shard,
                    replica_class,
                    minimum_bytes,
                    response,
                } => {
                    let _ = response.send(manager.pool().take_empty_extent(
                        replica_class,
                        shard,
                        minimum_bytes,
                    ));
                }
                ShardCommand::AddExtent {
                    shard,
                    transfer,
                    response,
                } => {
                    manager.pool().add_extent(shard, transfer);
                    let _ = response.send(());
                }
                ShardCommand::StartRuntimeTask {
                    task,
                    exit,
                    response,
                } => {
                    runtime_tasks.push(compio::runtime::spawn(async move {
                        let _ = exit.send(task().await);
                    }));
                    let _ = response.send(());
                }
                ShardCommand::StopRuntimeTasks { response } => {
                    for task in runtime_tasks.drain(..) {
                        let _ = task.cancel().await;
                    }
                    let _ = response.send(());
                }
                ShardCommand::Shutdown => {
                    for task in runtime_tasks.drain(..) {
                        let _ = task.cancel().await;
                    }
                    break;
                }
            }
        }
        LOCAL_SHARD.with(|local| *local.borrow_mut() = None);
    });
}

fn apply_local_topology(shard: usize, event: SegmentTopologyEvent) -> bool {
    LOCAL_SHARD.with(|local| {
        let local = local.borrow();
        let Some(local) = local.as_ref().filter(|local| local.shard == shard) else {
            return false;
        };
        apply_topology(local.manager.pool(), event);
        true
    })
}

fn revoke_local_pending(shard: usize, owners: &[WriteOwner], now: CatalogTick) -> Option<usize> {
    LOCAL_SHARD.with(|local| {
        let local = local.borrow();
        local
            .as_ref()
            .filter(|local| local.shard == shard)
            .map(|local| {
                let mut fenced = local.fenced_generations.borrow_mut();
                for owner in owners {
                    fenced
                        .entry(owner.client())
                        .and_modify(|generation| {
                            *generation = (*generation).max(owner.session_generation());
                        })
                        .or_insert(owner.session_generation());
                }
                drop(fenced);
                local.manager.revoke_pending_owners(owners, now)
            })
    })
}

fn topology_event_for_shard(
    event: SegmentTopologyEvent,
    shard: usize,
    shard_count: usize,
) -> SegmentTopologyEvent {
    match event {
        SegmentTopologyEvent::ReportLocalSsdCapacity {
            owner,
            id,
            capacity_bytes,
        } => SegmentTopologyEvent::ReportLocalSsdCapacity {
            owner,
            id,
            capacity_bytes: shard_capacity(capacity_bytes, shard, shard_count).1,
        },
        event => event,
    }
}

fn shard_capacity(capacity: u64, shard: usize, shard_count: usize) -> (u64, u64) {
    let shard_count = u64::try_from(shard_count).expect("metadata shard count fits u64");
    let shard = u64::try_from(shard).expect("metadata shard index fits u64");
    let base = capacity / shard_count;
    let remainder = capacity % shard_count;
    let offset = shard
        .checked_mul(base)
        .and_then(|offset| offset.checked_add(shard.min(remainder)))
        .expect("segment shard offset fits declared capacity");
    (offset, base + u64::from(shard < remainder))
}

fn apply_topology(pool: &Arc<SegmentPool>, event: SegmentTopologyEvent) {
    match event {
        SegmentTopologyEvent::Attach { spec, state } => {
            let outcome = if state.is_accepting() {
                pool.attach(spec)
            } else {
                pool.attach_quiesced(spec)
            };
            outcome.expect("validated control topology mounts in every metadata shard");
        }
        SegmentTopologyEvent::ReportLocalSsdCapacity {
            owner,
            id,
            capacity_bytes,
        } => pool
            .report_local_ssd_capacity(owner, id, capacity_bytes)
            .expect("control LocalSSD capacity applies to every metadata shard"),
        SegmentTopologyEvent::SetLocalSsdOffloadEnabled { owner, id, enabled } => pool
            .set_local_ssd_offload_enabled(owner, id, enabled)
            .expect("control LocalSSD state applies to every metadata shard"),
        SegmentTopologyEvent::Quiesce { owner, id } => pool
            .quiesce(owner, id)
            .expect("control quiesce applies to every metadata shard"),
        SegmentTopologyEvent::Reactivate { owner, ids } => pool
            .reactivate_many(owner, &ids)
            .expect("control reactivation applies to every metadata shard"),
        SegmentTopologyEvent::Remove { owner, id } => pool
            .remove(owner, id)
            .expect("control removal applies to every metadata shard"),
        SegmentTopologyEvent::InvalidateOwners { owners } => {
            pool.invalidate_owners(owners.iter().copied());
        }
    }
}

fn divide_budget(budget: CollectBudget, shards: usize, shard: usize) -> CollectBudget {
    fn share(value: usize, shards: usize, shard: usize) -> usize {
        value / shards + usize::from(shard < value % shards)
    }
    CollectBudget::new(
        share(budget.max_candidates(), shards, shard),
        share(budget.max_reclaims(), shards, shard),
        share(budget.max_empty_slots(), shards, shard),
    )
}

fn add_catalog_stats(
    mut total: ObjectCatalogStats,
    next: ObjectCatalogStats,
) -> ObjectCatalogStats {
    total.slots += next.slots;
    total.claims += next.claims;
    total.pending_objects += next.pending_objects;
    total.published_objects += next.published_objects;
    total.pending_bytes += next.pending_bytes;
    total.live_bytes += next.live_bytes;
    total.retired_bytes += next.retired_bytes;
    total.retired_memory_bytes += next.retired_memory_bytes;
    total.reclaim_debt += next.reclaim_debt;
    total.pending_candidates += next.pending_candidates;
    total.soft_pin_candidates += next.soft_pin_candidates;
    total.liveness_scan_remaining += next.liveness_scan_remaining;
    total.young_candidates += next.young_candidates;
    total.protected_candidates += next.protected_candidates;
    total.retired_candidates += next.retired_candidates;
    total.empty_slot_candidates += next.empty_slot_candidates;
    total
}

fn add_eviction_stats(
    mut total: MemoryEvictionStats,
    next: MemoryEvictionStats,
) -> MemoryEvictionStats {
    total.active |= next.active;
    total.capacity_bytes += next.capacity_bytes;
    total.used_bytes += next.used_bytes;
    total.maximum_used_bytes += next.maximum_used_bytes;
    total.maximum_used_ratio_ppm = total
        .maximum_used_ratio_ppm
        .max(next.maximum_used_ratio_ppm);
    total.available_bytes += next.available_bytes;
    total.high_watermark_bytes += next.high_watermark_bytes;
    total.low_watermark_bytes += next.low_watermark_bytes;
    total.pending_bytes += next.pending_bytes;
    total.live_bytes += next.live_bytes;
    total.retired_bytes += next.retired_bytes;
    total.reclaim_debt_bytes += next.reclaim_debt_bytes;
    total.requested_reclaim_debt_bytes += next.requested_reclaim_debt_bytes;
    total.allocation_reclaim_debt_bytes += next.allocation_reclaim_debt_bytes;
    total.watermark_reclaim_debt_bytes += next.watermark_reclaim_debt_bytes;
    total.trigger_events += next.trigger_events;
    total.controller_steps += next.controller_steps;
    total.busy_steps += next.busy_steps;
    total.retired_objects += next.retired_objects;
    total.retired_bytes_total += next.retired_bytes_total;
    total.reclaimed_objects += next.reclaimed_objects;
    total.reclaimed_bytes_total += next.reclaimed_bytes_total;
    total.allocation_failures += next.allocation_failures;
    total.allocation_retries += next.allocation_retries;
    total.allocation_retry_successes += next.allocation_retry_successes;
    total.wakeups += next.wakeups;
    total
}

fn add_maintenance(
    mut total: ObjectManagerMaintenance,
    next: ObjectManagerMaintenance,
) -> ObjectManagerMaintenance {
    total.expired_writes += next.expired_writes;
    total.catalog.busy |= next.catalog.busy;
    total.catalog.scanned_candidates += next.catalog.scanned_candidates;
    total.catalog.expired_pending += next.catalog.expired_pending;
    total.catalog.scanned_soft_pins += next.catalog.scanned_soft_pins;
    total.catalog.expired_soft_pins += next.catalog.expired_soft_pins;
    total.catalog.invalidated_pending += next.catalog.invalidated_pending;
    total.catalog.invalidated_published += next.catalog.invalidated_published;
    total.catalog.pruned_objects += next.catalog.pruned_objects;
    total.catalog.pruned_replicas += next.catalog.pruned_replicas;
    total.catalog.pruned_replica_bytes += next.catalog.pruned_replica_bytes;
    total.catalog.retired_objects += next.catalog.retired_objects;
    total.catalog.retired_bytes += next.catalog.retired_bytes;
    total.catalog.reclaimed_objects += next.catalog.reclaimed_objects;
    total.catalog.reclaimed_bytes += next.catalog.reclaimed_bytes;
    total.catalog.reclaimed_memory_bytes += next.catalog.reclaimed_memory_bytes;
    total.catalog.removed_empty_slots += next.catalog.removed_empty_slots;
    total.catalog.scoped_retired_objects += next.catalog.scoped_retired_objects;
    total.catalog.scoped_retired_bytes += next.catalog.scoped_retired_bytes;
    total.memory_eviction = match (total.memory_eviction, next.memory_eviction) {
        (Some(left), Some(right)) => Some(add_eviction_stats(left, right)),
        (None, value) | (value, None) => value,
    };
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{
        CleanupReason, ClientId, ClientLifecycleConfig, ClientManager, ClientTick,
    };
    use crate::object::{ObjectContent, ObjectKind, ObjectPutPlan};
    use crate::segment::placement::{AllocationSpec, PlacementRequest, ReplicaPolicy};
    use crate::segment::{
        MemoryRegion, SegmentId, SegmentIdentity, SegmentSpec, TransportEndpoint, TransportProtocol,
    };

    const CLIENT: ClientId = ClientId::new(17, 19);

    fn segment(index: u64) -> SegmentSpec {
        SegmentSpec::memory(
            SegmentIdentity::new(SegmentId::new(23, index), CLIENT, format!("memory-{index}")),
            MemoryRegion::new(index << 20, 1 << 20),
            TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
        )
    }

    fn request(key: &str) -> TenantPutRequest {
        TenantPutRequest::new(
            key,
            ObjectPutPlan::new(
                ObjectContent::new(4096).with_kind(ObjectKind::Tensor),
                PlacementRequest::new(AllocationSpec::new(4096), ReplicaPolicy::new(1)),
            ),
        )
    }

    #[tokio::test]
    async fn fenced_session_generation_is_local_to_every_shard() {
        let pool = Arc::new(SegmentPool::new());
        let manager = ShardedObjectManager::new(
            pool.clone(),
            ObjectCatalogConfig::new(32),
            MemoryEvictionConfig::default(),
            2,
        )
        .unwrap();
        let clients = ClientManager::with_config(
            pool,
            manager.pending_write_revoker(),
            ClientLifecycleConfig::new(8),
        )
        .unwrap();
        let first = clients
            .remount(CLIENT, vec![segment(1)], ClientTick::ZERO)
            .unwrap()
            .session();
        let stale_admission = clients.write_admission(CLIENT).unwrap();

        clients
            .drain_sessions([first], CleanupReason::GracefulUnmount, CatalogTick::ZERO)
            .unwrap();
        let second = clients
            .remount(CLIENT, vec![segment(2)], ClientTick::new(1))
            .unwrap()
            .session();
        assert!(second.generation() > first.generation());

        let stale = manager
            .start_put_batch(
                Arc::from("tenant"),
                stale_admission,
                vec![request("stale")],
                CatalogTick::new(1),
            )
            .await;
        assert_eq!(stale[0], Err(ErrorCode::InvalidWrite));

        let copied_stale_owner = manager
            .start_put_batch(
                Arc::from("tenant"),
                WriteAdmission::from_owner(WriteOwner::for_session(first)),
                vec![request("copied-stale-owner")],
                CatalogTick::new(1),
            )
            .await;
        assert_eq!(copied_stale_owner[0], Err(ErrorCode::InvalidWrite));

        let fresh = manager
            .start_put_batch(
                Arc::from("tenant"),
                clients.write_admission(CLIENT).unwrap(),
                vec![request("fresh")],
                CatalogTick::new(1),
            )
            .await;
        assert!(fresh[0].is_ok());
    }
}
