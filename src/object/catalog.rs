use super::config::{
    ObjectCatalogConfig, ObjectPinRequest, ResolvedObjectPinRequest, SoftPinAction,
};
use super::content::ObjectContent;
use super::diagnostics::ObjectCatalogStats;
use super::error::{LookupError, ObjectCatalogConfigError, RemoveError, StageError};
use super::identity::{NamespaceId, ObjectIdentity, ObjectLookup};
use super::reclamation::{CatalogTick, CollectBudget, CollectReport, ReclaimFilter, ReclaimTarget};
use super::replica::{
    ReplicaLease, ReplicaPartition, ReplicaReclaimBatch, ReplicaSet, ReplicaSnapshot,
    ReplicaSnapshotSet,
};
use super::tenant::{QuotaReservationGuard, TenantQuotaCharge};
use super::write::{ObjectCommit, TransactionId, VersionId, WriteAdmission, WriteMode, WriteOwner};
use arc_swap::ArcSwapOption;
use crossbeam_queue::SegQueue;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use scc::HashMap;
use std::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};

mod collector;
mod read;
mod version;
mod write;

#[derive(Clone)]
pub struct ObjectCatalog {
    inner: Arc<CatalogInner>,
}

struct CatalogInner {
    config: ObjectCatalogConfig,
    index: CatalogIndex,
    lifecycle: LifecycleCounters,
    collector: CollectorState,
}

struct CatalogIndex {
    entries: HashMap<ObjectIdentity, Arc<ObjectSlot>>,
    slots: AtomicUsize,
}

struct LifecycleCounters {
    claims: AtomicUsize,
    pending_objects: AtomicUsize,
    published_objects: AtomicUsize,
    pending_bytes: AtomicU64,
    live_bytes: AtomicU64,
    retired_bytes: AtomicU64,
    retired_memory_bytes: AtomicU64,
}

/// Mutable state owned by the incremental collector. The nested groups keep
/// queue synchronization, reclaim accounting, and liveness progress separate
/// while the public diagnostics remain a flat metrics projection.
struct CollectorState {
    eviction: EvictionQueues,
    pending: PendingQueue,
    retired: SegQueue<RetiredObject>,
    empty_slots: SegQueue<EmptySlotCandidate>,
    step_gate: Option<Mutex<()>>,
    reclaim: ReclaimDebt,
    liveness: LivenessSweep,
}

struct EvictionQueues {
    young: SegQueue<GcCandidate>,
    protected: SegQueue<GcCandidate>,
    soft_pins: SegQueue<SoftPinCandidate>,
}

/// The queue and gate form one producer/cleanup synchronization boundary.
struct PendingQueue {
    candidates: SegQueue<PendingCandidate>,
    stage_gate: Option<RwLock<()>>,
}

/// Explicit global reclaim debt. Memory watermark and allocation pressure are
/// evaluated per collector step so stale samples cannot become persistent.
struct ReclaimDebt {
    requested: AtomicU64,
}

/// Progress of a bounded segment-liveness sweep across both eviction queues.
struct LivenessSweep {
    requested: AtomicBool,
    young_remaining: AtomicUsize,
    protected_remaining: AtomicUsize,
}

struct ObjectSlot {
    identity: Arc<ObjectIdentity>,
    committed: OnceLock<ArcSwapOption<ObjectVersion>>,
    control: Mutex<SlotControl>,
}

struct SlotControl {
    next_transaction_id: u64,
    next_version_id: u64,
    active: Option<ActiveTransaction>,
}

struct ActiveTransaction {
    id: TransactionId,
    owner: WriteOwner,
    base: Option<Arc<ObjectVersion>>,
    pins: ResolvedObjectPinRequest,
    phase: TransactionPhase,
}

enum TransactionPhase {
    Claimed,
    Staged(Arc<ObjectVersion>),
}

struct ObjectVersion {
    origin: TransactionId,
    owner: WriteOwner,
    record: ObjectRecord,
    committed: OnceLock<CommittedVersion>,
}

struct CommittedVersion {
    id: VersionId,
    commit: ObjectCommit,
    access: AccessControl,
}

struct ObjectRecord {
    identity: Arc<ObjectIdentity>,
    content: ObjectContent,
    replicas: ReplicaStorage,
    accounting: Option<TenantQuotaCharge>,
}

struct ReplicaStorage {
    set: RwLock<ReplicaSet>,
    snapshot: ReplicaSnapshotSet,
    reserved_bytes: AtomicU64,
}

enum ReplicaPrune {
    AllLive,
    AllStale,
    Mixed { stale: ReplicaSet },
}

struct AccessControl {
    lease_until: AtomicU64,
    recent: AtomicBool,
    soft_pin_until: AtomicU64,
    hard_pinned: bool,
}

#[derive(Clone)]
struct GcCandidate {
    slot: Weak<ObjectSlot>,
    version: Weak<ObjectVersion>,
}

struct PendingCandidate {
    slot: Weak<ObjectSlot>,
    pending: Weak<ObjectVersion>,
    transaction_id: TransactionId,
    deadline: CatalogTick,
}

struct SoftPinCandidate {
    candidate: GcCandidate,
    deadline: CatalogTick,
}

struct EmptySlotCandidate {
    identity: Arc<ObjectIdentity>,
    slot: Weak<ObjectSlot>,
    deadline: CatalogTick,
}

struct RetiredObject {
    version: Arc<ObjectVersion>,
    reserved_bytes: u64,
    reclaim_after: CatalogTick,
}

pub struct WriteClaim {
    catalog: Weak<CatalogInner>,
    slot: Weak<ObjectSlot>,
    identity: Arc<ObjectIdentity>,
    id: TransactionId,
    admission: WriteAdmission,
    started_at: CatalogTick,
    armed: bool,
}

#[derive(Clone)]
pub struct WriteTransaction {
    catalog: Weak<CatalogInner>,
    slot: Weak<ObjectSlot>,
    pending: Arc<ObjectVersion>,
    id: TransactionId,
}

#[derive(Clone)]
pub struct ObjectHandle {
    version: Arc<ObjectVersion>,
}

pub struct ReplicaSetView<'a> {
    guard: RwLockReadGuard<'a, ReplicaSet>,
}

pub struct LiveReplicaView<'a> {
    guard: RwLockReadGuard<'a, ReplicaSet>,
}

#[derive(Clone, Debug)]
pub struct ObjectRead {
    object: ObjectHandle,
    lease_expires_at: CatalogTick,
}

pub(super) enum WriteResolution {
    Active(WriteTransaction),
    Committed(ObjectHandle),
}

impl ObjectCatalog {
    pub fn new() -> Self {
        Self::with_config(ObjectCatalogConfig::default())
            .expect("the default ObjectCatalog configuration is valid")
    }

    pub fn with_config(config: ObjectCatalogConfig) -> Result<Self, ObjectCatalogConfigError> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(CatalogInner {
                config,
                index: CatalogIndex::new(config.expected_objects),
                lifecycle: LifecycleCounters::new(),
                collector: CollectorState::new(config.shard_local),
            }),
        })
    }

    pub fn stats(&self) -> ObjectCatalogStats {
        let index = &self.inner.index;
        let lifecycle = &self.inner.lifecycle;
        let collector = &self.inner.collector;
        ObjectCatalogStats {
            slots: index.slots.load(Ordering::Relaxed),
            claims: lifecycle.claims.load(Ordering::Relaxed),
            pending_objects: lifecycle.pending_objects.load(Ordering::Relaxed),
            published_objects: lifecycle.published_objects.load(Ordering::Relaxed),
            pending_bytes: lifecycle.pending_bytes.load(Ordering::Relaxed),
            live_bytes: lifecycle.live_bytes.load(Ordering::Relaxed),
            retired_bytes: lifecycle.retired_bytes.load(Ordering::Relaxed),
            retired_memory_bytes: lifecycle.retired_memory_bytes.load(Ordering::Relaxed),
            reclaim_debt: collector.reclaim.requested(),
            pending_candidates: collector.pending.candidates.len(),
            soft_pin_candidates: collector.eviction.soft_pins.len(),
            liveness_scan_remaining: collector.liveness.remaining(),
            young_candidates: collector.eviction.young.len(),
            protected_candidates: collector.eviction.protected.len(),
            retired_candidates: collector.retired.len(),
            empty_slot_candidates: collector.empty_slots.len(),
        }
    }

    pub(in crate::object) fn identities(&self, namespace: NamespaceId) -> Vec<ObjectIdentity> {
        let mut identities = Vec::new();
        self.inner.index.entries.iter_sync(|identity, slot| {
            if identity.namespace() == namespace && slot.has_committed() {
                identities.push(identity.clone());
            }
            true
        });
        identities
    }

    pub(in crate::object) fn resolve_pin_request(
        &self,
        request: ObjectPinRequest,
    ) -> Result<ResolvedObjectPinRequest, ObjectCatalogConfigError> {
        self.inner.config.resolve_pin_request(request)
    }
}

impl Default for ObjectCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogInner {
    fn lookup_slot(&self, lookup: ObjectLookup<'_>) -> Option<Arc<ObjectSlot>> {
        self.index
            .entries
            .read_sync(&lookup, |_, slot| Arc::clone(slot))
    }

    fn slot_is_indexed(&self, slot: &Arc<ObjectSlot>) -> bool {
        self.lookup_slot(slot.identity.as_lookup())
            .is_some_and(|indexed| Arc::ptr_eq(&indexed, slot))
    }
}

impl CatalogIndex {
    fn new(expected_objects: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(expected_objects),
            slots: AtomicUsize::new(0),
        }
    }

    fn on_slot_inserted(&self) {
        self.slots.fetch_add(1, Ordering::Relaxed);
    }

    fn on_slot_removed(&self) {
        self.slots.fetch_sub(1, Ordering::Relaxed);
    }
}

impl SlotControl {
    fn allocate_transaction_id(&mut self) -> TransactionId {
        let id = TransactionId::new(self.next_transaction_id);
        self.next_transaction_id = self
            .next_transaction_id
            .checked_add(1)
            .expect("per-slot transaction identifier space is exhausted");
        id
    }

    fn allocate_version_id(&mut self) -> VersionId {
        let id = VersionId::new(self.next_version_id);
        self.next_version_id = self
            .next_version_id
            .checked_add(1)
            .expect("per-slot version identifier space is exhausted");
        id
    }
}

impl LifecycleCounters {
    const fn new() -> Self {
        Self {
            claims: AtomicUsize::new(0),
            pending_objects: AtomicUsize::new(0),
            published_objects: AtomicUsize::new(0),
            pending_bytes: AtomicU64::new(0),
            live_bytes: AtomicU64::new(0),
            retired_bytes: AtomicU64::new(0),
            retired_memory_bytes: AtomicU64::new(0),
        }
    }

    fn on_claim(&self) {
        self.claims.fetch_add(1, Ordering::Relaxed);
    }

    fn on_claim_dropped(&self) {
        self.claims.fetch_sub(1, Ordering::Relaxed);
    }

    fn on_stage(&self, reserved_bytes: u64) {
        self.claims.fetch_sub(1, Ordering::Relaxed);
        self.pending_objects.fetch_add(1, Ordering::Relaxed);
        self.pending_bytes
            .fetch_add(reserved_bytes, Ordering::Relaxed);
    }

    fn on_commit(
        &self,
        committed_bytes: u64,
        replaced_bytes: Option<u64>,
        replaced_memory_bytes: u64,
    ) {
        self.pending_objects.fetch_sub(1, Ordering::Relaxed);
        atomic_saturating_sub(&self.pending_bytes, committed_bytes);
        self.live_bytes
            .fetch_add(committed_bytes, Ordering::Relaxed);
        if let Some(replaced_bytes) = replaced_bytes {
            atomic_saturating_sub(&self.live_bytes, replaced_bytes);
            self.retired_bytes
                .fetch_add(replaced_bytes, Ordering::Relaxed);
            self.retired_memory_bytes
                .fetch_add(replaced_memory_bytes, Ordering::Relaxed);
        } else {
            self.published_objects.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn on_abort_pending(&self, reserved_bytes: u64, memory: bool) {
        self.pending_objects.fetch_sub(1, Ordering::Relaxed);
        atomic_saturating_sub(&self.pending_bytes, reserved_bytes);
        self.retired_bytes
            .fetch_add(reserved_bytes, Ordering::Relaxed);
        if memory {
            self.retired_memory_bytes
                .fetch_add(reserved_bytes, Ordering::Relaxed);
        }
    }

    fn on_retire_published(&self, reserved_bytes: u64, memory: bool) {
        self.published_objects.fetch_sub(1, Ordering::Relaxed);
        atomic_saturating_sub(&self.live_bytes, reserved_bytes);
        self.retired_bytes
            .fetch_add(reserved_bytes, Ordering::Relaxed);
        if memory {
            self.retired_memory_bytes
                .fetch_add(reserved_bytes, Ordering::Relaxed);
        }
    }

    fn on_prune_published(&self, reserved_bytes: u64) {
        atomic_saturating_sub(&self.live_bytes, reserved_bytes);
    }

    fn on_reclaim(&self, reserved_bytes: u64, memory: bool) {
        atomic_saturating_sub(&self.retired_bytes, reserved_bytes);
        if memory {
            atomic_saturating_sub(&self.retired_memory_bytes, reserved_bytes);
        }
    }
}

impl CollectorState {
    fn new(shard_local: bool) -> Self {
        Self {
            eviction: EvictionQueues::new(),
            pending: PendingQueue::new(shard_local),
            retired: SegQueue::new(),
            empty_slots: SegQueue::new(),
            step_gate: (!shard_local).then(|| Mutex::new(())),
            reclaim: ReclaimDebt::new(),
            liveness: LivenessSweep::new(),
        }
    }
}

impl EvictionQueues {
    fn new() -> Self {
        Self {
            young: SegQueue::new(),
            protected: SegQueue::new(),
            soft_pins: SegQueue::new(),
        }
    }
}

impl PendingQueue {
    fn new(shard_local: bool) -> Self {
        Self {
            candidates: SegQueue::new(),
            stage_gate: (!shard_local).then(|| RwLock::new(())),
        }
    }
}

impl ReclaimDebt {
    const fn new() -> Self {
        Self {
            requested: AtomicU64::new(0),
        }
    }

    fn request(&self, bytes: u64) {
        self.requested.fetch_max(bytes, Ordering::Relaxed);
    }

    fn requested(&self) -> u64 {
        self.requested.load(Ordering::Relaxed)
    }

    fn on_reclaim(&self, bytes: u64) {
        atomic_saturating_sub(&self.requested, bytes);
    }
}

impl LivenessSweep {
    const fn new() -> Self {
        Self {
            requested: AtomicBool::new(false),
            young_remaining: AtomicUsize::new(0),
            protected_remaining: AtomicUsize::new(0),
        }
    }

    fn request(&self) {
        self.requested.store(true, Ordering::Release);
    }

    fn remaining(&self) -> usize {
        self.young_remaining
            .load(Ordering::Relaxed)
            .saturating_add(self.protected_remaining.load(Ordering::Relaxed))
    }

    fn begin_if_requested(&self, eviction: &EvictionQueues) {
        if !self.requested.swap(false, Ordering::AcqRel) {
            return;
        }
        self.young_remaining
            .fetch_max(eviction.young.len(), Ordering::Release);
        self.protected_remaining
            .fetch_max(eviction.protected.len(), Ordering::Release);
    }

    fn is_incomplete(&self) -> bool {
        self.young_remaining.load(Ordering::Acquire) != 0
            || self.protected_remaining.load(Ordering::Acquire) != 0
    }
}

impl GcCandidate {
    fn new(slot: &Arc<ObjectSlot>, version: &Arc<ObjectVersion>) -> Self {
        Self {
            slot: Arc::downgrade(slot),
            version: Arc::downgrade(version),
        }
    }
}

fn committed_points_to(slot: &ObjectSlot, expected: &Arc<ObjectVersion>) -> bool {
    slot.committed
        .get()
        .and_then(ArcSwapOption::load_full)
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, expected))
}

impl ObjectSlot {
    fn load_committed(&self) -> Option<Arc<ObjectVersion>> {
        self.committed.get().and_then(ArcSwapOption::load_full)
    }

    fn has_committed(&self) -> bool {
        self.committed
            .get()
            .is_some_and(|committed| committed.load().is_some())
    }

    fn publish_committed(&self, version: Arc<ObjectVersion>) {
        if let Some(committed) = self.committed.get() {
            committed.store(Some(version));
        } else {
            assert!(
                self.committed
                    .set(ArcSwapOption::from(Some(version)))
                    .is_ok(),
                "slot control serializes initial publication"
            );
        }
    }

    fn clear_committed(&self) {
        if let Some(committed) = self.committed.get() {
            committed.store(None);
        }
    }
}

fn atomic_saturating_sub(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(amount))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_local_catalog_omits_cross_thread_collector_gates() {
        let local =
            ObjectCatalog::with_config(ObjectCatalogConfig::new(8).split_for_shards(2)).unwrap();
        assert!(local.inner.collector.step_gate.is_none());
        assert!(local.inner.collector.pending.stage_gate.is_none());

        let concurrent = ObjectCatalog::with_config(ObjectCatalogConfig::new(8)).unwrap();
        assert!(concurrent.inner.collector.step_gate.is_some());
        assert!(concurrent.inner.collector.pending.stage_gate.is_some());
    }
}
