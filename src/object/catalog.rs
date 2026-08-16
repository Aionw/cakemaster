use super::config::ObjectCatalogConfig;
use super::content::ObjectContent;
use super::diagnostics::ObjectCatalogStats;
use super::error::{
    LookupError, ObjectCatalogConfigError, PublishError, PutError, RemoveError, RevokeError,
    StageError,
};
use super::identity::{ObjectIdentity, ObjectLookup};
use super::reclamation::{CatalogTick, CollectBudget, CollectReport, ReclaimFilter, ReclaimTarget};
use super::replica::{ReplicaLease, ReplicaPartition, ReplicaReclaimBatch, ReplicaSet};
use super::tenant::{QuotaReservationGuard, TenantQuotaCharge};
use super::write::{ObjectCommit, WriteAdmission, WriteId, WriteOwner};
use arc_swap::ArcSwapOption;
use crossbeam_queue::SegQueue;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use scc::HashMap;
use scc::hash_map::Entry;
use std::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};

mod collector;
mod read;
mod write;

const SLOT_OPEN: u8 = 0;
const SLOT_CLOSING: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum ObjectState {
    /// The key is reserved, but its immutable record is not installed yet.
    Claimed,
    /// The record and reservations exist but are not readable yet.
    Pending,
    /// The current slot generation is readable and lease-protected.
    Published,
    /// The node is detached or being detached before deferred reclamation.
    Retiring,
}

impl ObjectState {
    fn from_raw(value: u8) -> Self {
        match value {
            value if value == Self::Claimed as u8 => Self::Claimed,
            value if value == Self::Pending as u8 => Self::Pending,
            value if value == Self::Published as u8 => Self::Published,
            value if value == Self::Retiring as u8 => Self::Retiring,
            _ => unreachable!("object lifecycle only stores valid states"),
        }
    }
}

struct ObjectLifecycle(AtomicU8);

impl ObjectLifecycle {
    const fn new(state: ObjectState) -> Self {
        Self(AtomicU8::new(state as u8))
    }

    fn state(&self) -> ObjectState {
        ObjectState::from_raw(self.0.load(Ordering::Acquire))
    }

    fn transition(&self, from: ObjectState, to: ObjectState) -> Result<(), ObjectState> {
        self.0
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(ObjectState::from_raw)
    }

    fn store(&self, state: ObjectState) {
        self.0.store(state as u8, Ordering::Release);
    }
}

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
}

struct CollectorState {
    young: SegQueue<GcCandidate>,
    protected: SegQueue<GcCandidate>,
    pending: SegQueue<PendingCandidate>,
    empty_slots: SegQueue<EmptySlotCandidate>,
    retired: SegQueue<RetiredObject>,
    gate: Mutex<()>,
    pending_stage_gate: RwLock<()>,
    reclaim_debt: AtomicU64,
    liveness_scan_requested: AtomicBool,
    liveness_young_remaining: AtomicUsize,
    liveness_protected_remaining: AtomicUsize,
}

struct ObjectSlot {
    identity: Arc<ObjectIdentity>,
    state: AtomicU8,
    generation: AtomicU64,
    current: ArcSwapOption<CatalogNode>,
}

/// Stable across claim, stage, and publish. The record is initialized once;
/// published replica pruning mutates only its lock-protected replica set and
/// matching byte counter without replacing this Arc.
struct CatalogNode {
    record: OnceLock<ObjectRecord>,
    mutation: MutationControl,
    access: AccessControl,
}

struct ObjectRecord {
    identity: Arc<ObjectIdentity>,
    content: ObjectContent,
    replicas: ReplicaStorage,
    accounting: Option<TenantQuotaCharge>,
}

/// The replica set and its physical byte total change as one pruning unit.
struct ReplicaStorage {
    set: RwLock<ReplicaSet>,
    reserved_bytes: AtomicU64,
}

enum ReplicaPrune {
    AllLive,
    AllStale,
    Mixed { stale: ReplicaSet },
}

/// State that changes as an object moves through a write transaction.
struct MutationControl {
    lifecycle: ObjectLifecycle,
    /// Serializes lifecycle mutations and replica pruning for this node.
    /// Ordinary lookups still use the atomic lifecycle and access controls.
    gate: Mutex<()>,
    metadata: WriteMetadata,
    commit: OnceLock<ObjectCommit>,
}

struct WriteMetadata {
    id: WriteId,
    owner: WriteOwner,
}

/// Read-side signals consumed by lease enforcement and second-chance eviction.
struct AccessControl {
    lease_until: AtomicU64,
    recent: AtomicBool,
}

#[derive(Clone)]
struct GcCandidate {
    slot: Weak<ObjectSlot>,
    node: Weak<CatalogNode>,
}

struct PendingCandidate {
    candidate: GcCandidate,
    deadline: CatalogTick,
}

struct EmptySlotCandidate {
    identity: Arc<ObjectIdentity>,
    slot: Weak<ObjectSlot>,
    deadline: CatalogTick,
}

struct RetiredObject {
    node: Arc<CatalogNode>,
    reserved_bytes: u64,
    retry_at: CatalogTick,
}

pub struct PutClaim {
    catalog: Weak<CatalogInner>,
    slot: Weak<ObjectSlot>,
    node: Option<Arc<CatalogNode>>,
    identity: Arc<ObjectIdentity>,
    id: WriteId,
    admission: WriteAdmission,
    started_at: CatalogTick,
}

#[derive(Clone)]
pub struct PutTicket {
    catalog: Weak<CatalogInner>,
    slot: Weak<ObjectSlot>,
    node: Arc<CatalogNode>,
    id: WriteId,
}

#[derive(Clone)]
pub struct ObjectHandle {
    node: Arc<CatalogNode>,
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

pub(super) enum ObjectWriteState {
    Pending(PutTicket),
    Published(ObjectHandle),
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
                index: CatalogIndex {
                    entries: HashMap::with_capacity(config.expected_objects),
                    slots: AtomicUsize::new(0),
                },
                lifecycle: LifecycleCounters {
                    claims: AtomicUsize::new(0),
                    pending_objects: AtomicUsize::new(0),
                    published_objects: AtomicUsize::new(0),
                    pending_bytes: AtomicU64::new(0),
                    live_bytes: AtomicU64::new(0),
                    retired_bytes: AtomicU64::new(0),
                },
                collector: CollectorState {
                    young: SegQueue::new(),
                    protected: SegQueue::new(),
                    pending: SegQueue::new(),
                    empty_slots: SegQueue::new(),
                    retired: SegQueue::new(),
                    gate: Mutex::new(()),
                    pending_stage_gate: RwLock::new(()),
                    reclaim_debt: AtomicU64::new(0),
                    liveness_scan_requested: AtomicBool::new(false),
                    liveness_young_remaining: AtomicUsize::new(0),
                    liveness_protected_remaining: AtomicUsize::new(0),
                },
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
            reclaim_debt: collector.reclaim_debt.load(Ordering::Relaxed),
            pending_candidates: collector.pending.len(),
            liveness_scan_remaining: collector
                .liveness_young_remaining
                .load(Ordering::Relaxed)
                .saturating_add(
                    collector
                        .liveness_protected_remaining
                        .load(Ordering::Relaxed),
                ),
            young_candidates: collector.young.len(),
            protected_candidates: collector.protected.len(),
            retired_candidates: collector.retired.len(),
            empty_slot_candidates: collector.empty_slots.len(),
        }
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
    fn on_slot_inserted(&self) {
        self.slots.fetch_add(1, Ordering::Relaxed);
    }

    fn on_slot_removed(&self) {
        self.slots.fetch_sub(1, Ordering::Relaxed);
    }
}

impl LifecycleCounters {
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

    fn on_publish(&self, reserved_bytes: u64) {
        self.pending_objects.fetch_sub(1, Ordering::Relaxed);
        self.published_objects.fetch_add(1, Ordering::Relaxed);
        atomic_saturating_sub(&self.pending_bytes, reserved_bytes);
        self.live_bytes.fetch_add(reserved_bytes, Ordering::Relaxed);
    }

    fn on_retire_pending(&self, reserved_bytes: u64) {
        self.pending_objects.fetch_sub(1, Ordering::Relaxed);
        atomic_saturating_sub(&self.pending_bytes, reserved_bytes);
        self.retired_bytes
            .fetch_add(reserved_bytes, Ordering::Relaxed);
    }

    fn on_retire_published(&self, reserved_bytes: u64) {
        self.published_objects.fetch_sub(1, Ordering::Relaxed);
        atomic_saturating_sub(&self.live_bytes, reserved_bytes);
        self.retired_bytes
            .fetch_add(reserved_bytes, Ordering::Relaxed);
    }

    fn on_prune_published(&self, reserved_bytes: u64) {
        atomic_saturating_sub(&self.live_bytes, reserved_bytes);
    }

    fn on_reclaim(&self, reserved_bytes: u64) {
        atomic_saturating_sub(&self.retired_bytes, reserved_bytes);
    }
}

impl CollectorState {
    fn request_reclaim(&self, bytes: u64) {
        self.reclaim_debt.fetch_max(bytes, Ordering::Relaxed);
    }

    fn on_reclaim(&self, bytes: u64) {
        atomic_saturating_sub(&self.reclaim_debt, bytes);
    }
}

impl ObjectSlot {
    fn next_write_id(&self) -> WriteId {
        WriteId::new(self.generation.fetch_add(1, Ordering::Relaxed) + 1)
    }
}

impl CatalogNode {
    fn claimed(id: WriteId, owner: WriteOwner) -> Self {
        Self {
            record: OnceLock::new(),
            mutation: MutationControl {
                lifecycle: ObjectLifecycle::new(ObjectState::Claimed),
                gate: Mutex::new(()),
                metadata: WriteMetadata { id, owner },
                commit: OnceLock::new(),
            },
            access: AccessControl {
                lease_until: AtomicU64::new(0),
                recent: AtomicBool::new(false),
            },
        }
    }

    fn record(&self) -> &ObjectRecord {
        self.record
            .get()
            .expect("staged objects always have immutable records")
    }

    fn owner(&self) -> WriteOwner {
        self.mutation.owner()
    }

    fn write_id(&self) -> WriteId {
        self.mutation.write_id()
    }

    fn commit_accounting(&self) {
        if let Some((charge, class, bytes)) = self.record().tenant_accounting() {
            charge.commit(class, bytes);
        }
    }

    fn abort_accounting(&self) {
        if let Some((charge, class, bytes)) = self.record().tenant_accounting() {
            charge.abort(class, bytes);
        }
    }

    fn mark_accounting_retiring(&self) {
        if let Some((charge, class, bytes)) = self.record().tenant_accounting() {
            charge.mark_retiring(class, bytes);
        }
    }

    fn release_accounting(&self) {
        let Some(record) = self.record.get() else {
            return;
        };
        if let Some((charge, class, bytes)) = record.tenant_accounting() {
            charge.release(class, bytes);
        }
    }

    fn release_pruned_accounting(&self, stale: &ReplicaSet) {
        let record = self.record();
        let Some(charge) = record.accounting.as_ref() else {
            return;
        };
        let Some(replica_class) = ObjectRecord::direct_replica_class(stale) else {
            return;
        };
        let replica_count = u64::try_from(stale.len())
            .expect("replica count was representable when the object was staged");
        let bytes = record
            .content
            .logical_bytes()
            .checked_mul(replica_count)
            .expect("tenant replica charge was representable when the object was staged");
        charge.release_committed_partial(replica_class, bytes);
    }
}

impl Drop for CatalogNode {
    fn drop(&mut self) {
        self.release_accounting();
    }
}

impl ObjectRecord {
    fn tenant_accounting(&self) -> Option<(&TenantQuotaCharge, crate::segment::ReplicaClass, u64)> {
        let replicas = self.replicas.read();
        let charge = self.accounting.as_ref()?;
        let replica_class = Self::direct_replica_class(&replicas)?;
        let replica_count = u64::try_from(replicas.len()).ok()?;
        let bytes = self.content.logical_bytes().checked_mul(replica_count)?;
        Some((charge, replica_class, bytes))
    }

    fn direct_replica_class(replicas: &ReplicaSet) -> Option<crate::segment::ReplicaClass> {
        Some(replicas.replicas().first()?.direct()?.replica_class())
    }

    fn current_direct_replica_class(&self) -> Option<crate::segment::ReplicaClass> {
        Self::direct_replica_class(&self.replicas.read())
    }

    fn is_accounted(&self) -> bool {
        self.accounting.is_some()
    }

    fn reserved_bytes(&self) -> u64 {
        self.replicas.reserved_bytes()
    }
}

impl ReplicaStorage {
    fn new(replicas: ReplicaSet) -> Self {
        let reserved_bytes = replicas.reserved_bytes();
        Self {
            set: RwLock::new(replicas),
            reserved_bytes: AtomicU64::new(reserved_bytes),
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, ReplicaSet> {
        self.set.read()
    }

    fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes.load(Ordering::Relaxed)
    }

    /// Removes stale replicas and updates the physical byte total before the
    /// pruned set becomes visible to readers.
    fn prune_invalidated(&self) -> ReplicaPrune {
        let mut replicas = self.set.write();
        if replicas.all_live() {
            return ReplicaPrune::AllLive;
        }
        match std::mem::take(&mut *replicas).partition_by_liveness() {
            ReplicaPartition::AllLive(current) => {
                *replicas = current;
                ReplicaPrune::AllLive
            }
            ReplicaPartition::AllStale(current) => {
                *replicas = current;
                ReplicaPrune::AllStale
            }
            ReplicaPartition::Mixed { live, stale } => {
                atomic_saturating_sub(&self.reserved_bytes, stale.reserved_bytes());
                *replicas = live;
                ReplicaPrune::Mixed { stale }
            }
        }
    }

    fn into_inner(self) -> ReplicaSet {
        self.set.into_inner()
    }
}

impl<'a> ReplicaSetView<'a> {
    fn new(guard: RwLockReadGuard<'a, ReplicaSet>) -> Self {
        Self { guard }
    }
}

impl Deref for ReplicaSetView<'_> {
    type Target = [ReplicaLease];

    fn deref(&self) -> &Self::Target {
        self.guard.replicas()
    }
}

impl fmt::Debug for ReplicaSetView<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl<'a> LiveReplicaView<'a> {
    fn new(guard: RwLockReadGuard<'a, ReplicaSet>) -> Self {
        Self { guard }
    }

    pub fn iter(&self) -> impl Iterator<Item = &ReplicaLease> {
        self.guard.live_iter()
    }

    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.first().is_none()
    }

    pub fn first(&self) -> Option<&ReplicaLease> {
        self.iter().next()
    }
}

impl fmt::Debug for LiveReplicaView<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl MutationControl {
    fn state(&self) -> ObjectState {
        self.lifecycle.state()
    }

    fn transition(&self, from: ObjectState, to: ObjectState) -> Result<(), ObjectState> {
        self.lifecycle.transition(from, to)
    }

    fn store(&self, state: ObjectState) {
        self.lifecycle.store(state);
    }

    fn lock(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.gate.lock()
    }

    fn write_id(&self) -> WriteId {
        self.metadata.id
    }

    fn owner(&self) -> WriteOwner {
        self.metadata.owner
    }

    fn commit(&self) -> Option<ObjectCommit> {
        self.commit.get().copied()
    }

    fn set_commit(&self, commit: ObjectCommit) -> Result<(), ObjectCommit> {
        self.commit.set(commit)
    }
}

impl AccessControl {
    fn acquire_lease(
        &self,
        now: CatalogTick,
        lease_ttl_ticks: u64,
        lease_refresh_ticks: u64,
    ) -> CatalogTick {
        let refresh_at = now.saturating_add(lease_refresh_ticks).get();
        let desired = now.saturating_add(lease_ttl_ticks).get();
        let mut current = self.lease_until.load(Ordering::Relaxed);
        while current < refresh_at {
            match self.lease_until.compare_exchange_weak(
                current,
                desired,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return CatalogTick::new(desired),
                Err(observed) => current = observed,
            }
        }
        CatalogTick::new(current)
    }

    fn record_access(
        &self,
        now: CatalogTick,
        lease_ttl_ticks: u64,
        lease_refresh_ticks: u64,
    ) -> CatalogTick {
        let lease_until = self.acquire_lease(now, lease_ttl_ticks, lease_refresh_ticks);
        self.recent.store(true, Ordering::Relaxed);
        lease_until
    }

    fn take_recent(&self) -> bool {
        self.recent.swap(false, Ordering::Relaxed)
    }

    fn lease_until(&self) -> CatalogTick {
        CatalogTick::new(self.lease_until.load(Ordering::Acquire))
    }

    fn is_leased(&self, now: CatalogTick) -> bool {
        self.lease_until() > now
    }
}

impl GcCandidate {
    fn new(slot: &Arc<ObjectSlot>, node: &Arc<CatalogNode>) -> Self {
        Self {
            slot: Arc::downgrade(slot),
            node: Arc::downgrade(node),
        }
    }
}

fn slot_points_to(slot: &ObjectSlot, expected: &Arc<CatalogNode>) -> bool {
    slot.current
        .load()
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, expected))
}

fn clear_slot(slot: &ObjectSlot, expected: &Arc<CatalogNode>) -> bool {
    let previous = slot.current.compare_and_swap(expected, None);
    previous
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, expected))
}

fn atomic_saturating_sub(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(amount))
    });
}
