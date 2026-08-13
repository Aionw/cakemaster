use super::config::ObjectCatalogConfig;
use super::content::ObjectContent;
use super::diagnostics::ObjectCatalogStats;
use super::error::{
    LookupError, ObjectCatalogConfigError, PublishError, PutError, RemoveError, RevokeError,
    StageError,
};
use super::identity::{ObjectIdentity, ObjectLookup};
use super::reclamation::{CatalogTick, CollectBudget, CollectReport, ReclaimFilter, ReclaimTarget};
use super::replica::{ReplicaLease, ReplicaReclaimBatch, ReplicaSet};
use super::tenant::{CHARGE_RESERVED, QuotaReservationGuard, TenantQuotaCharge};
use super::write::{ObjectCommit, WriteId, WriteOwner};
use arc_swap::ArcSwapOption;
use crossbeam_queue::SegQueue;
use parking_lot::Mutex;
use scc::HashMap;
use scc::hash_map::Entry;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};

mod collector;
mod read;
mod write;

const SLOT_OPEN: u8 = 0;
const SLOT_CLOSING: u8 = 1;

const OBJECT_CLAIMED: u8 = 0;
const OBJECT_PENDING: u8 = 1;
const OBJECT_PUBLISHING: u8 = 2;
const OBJECT_PUBLISHED: u8 = 3;
const OBJECT_RETIRING: u8 = 4;

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
    reclaim_debt: AtomicU64,
}

struct ObjectSlot {
    identity: Arc<ObjectIdentity>,
    state: AtomicU8,
    generation: AtomicU64,
    current: ArcSwapOption<CatalogNode>,
}

/// Stable across claim, stage, and publish. The immutable record is initialized
/// once; successful puts only advance the lifecycle and never replace this Arc.
struct CatalogNode {
    record: OnceLock<ObjectRecord>,
    control: ObjectControl,
}

struct ObjectRecord {
    identity: Arc<ObjectIdentity>,
    content: ObjectContent,
    replicas: ReplicaSet,
    reserved_bytes: u64,
    accounting: Option<TenantQuotaCharge>,
}

struct ObjectControl {
    lifecycle: AtomicU8,
    accounting_phase: AtomicU8,
    write_id: WriteId,
    owner: WriteOwner,
    commit: OnceLock<ObjectCommit>,
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
    owner: WriteOwner,
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
                    reclaim_debt: AtomicU64::new(0),
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
            control: ObjectControl {
                lifecycle: AtomicU8::new(OBJECT_CLAIMED),
                // Unaccounted records ignore this field. Accounted records
                // start reserved, so staging does not need another hot-path
                // atomic write.
                accounting_phase: AtomicU8::new(CHARGE_RESERVED),
                write_id: id,
                owner,
                commit: OnceLock::new(),
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

    fn commit_accounting(&self) {
        if let Some((charge, class, bytes)) = self.record().tenant_accounting() {
            charge.commit(&self.control.accounting_phase, class, bytes);
        }
    }

    fn abort_accounting(&self) {
        if let Some((charge, class, bytes)) = self.record().tenant_accounting() {
            charge.abort(&self.control.accounting_phase, class, bytes);
        }
    }

    fn mark_accounting_retiring(&self) {
        if let Some((charge, class, bytes)) = self.record().tenant_accounting() {
            charge.mark_retiring(&self.control.accounting_phase, class, bytes);
        }
    }

    fn release_accounting(&self) {
        let Some(record) = self.record.get() else {
            return;
        };
        if let Some((charge, class, bytes)) = record.tenant_accounting() {
            charge.release(&self.control.accounting_phase, class, bytes);
        }
    }
}

impl Drop for CatalogNode {
    fn drop(&mut self) {
        self.release_accounting();
    }
}

impl ObjectRecord {
    fn tenant_accounting(&self) -> Option<(&TenantQuotaCharge, crate::segment::ReplicaClass, u64)> {
        let charge = self.accounting.as_ref()?;
        let replica_class = self.direct_replica_class()?;
        let replica_count = u64::try_from(self.replicas.len()).ok()?;
        let bytes = self.content.logical_bytes().checked_mul(replica_count)?;
        Some((charge, replica_class, bytes))
    }

    fn direct_replica_class(&self) -> Option<crate::segment::ReplicaClass> {
        Some(self.replicas.replicas().first()?.direct()?.replica_class())
    }
}

impl ObjectControl {
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
