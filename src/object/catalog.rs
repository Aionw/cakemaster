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
mod handles;

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
    entries: HashMap<ObjectIdentity, Arc<ObjectSlot>>,
    config: ObjectCatalogConfig,
    young: SegQueue<GcCandidate>,
    protected: SegQueue<GcCandidate>,
    pending: SegQueue<PendingCandidate>,
    empty_slots: SegQueue<EmptySlotCandidate>,
    retired: SegQueue<RetiredObject>,
    collector_gate: Mutex<()>,
    slots: AtomicUsize,
    claims: AtomicUsize,
    pending_objects: AtomicUsize,
    published_objects: AtomicUsize,
    pending_bytes: AtomicU64,
    live_bytes: AtomicU64,
    retired_bytes: AtomicU64,
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
        validate_config(config)?;
        Ok(Self {
            inner: Arc::new(CatalogInner {
                entries: HashMap::with_capacity(config.expected_objects),
                config,
                young: SegQueue::new(),
                protected: SegQueue::new(),
                pending: SegQueue::new(),
                empty_slots: SegQueue::new(),
                retired: SegQueue::new(),
                collector_gate: Mutex::new(()),
                slots: AtomicUsize::new(0),
                claims: AtomicUsize::new(0),
                pending_objects: AtomicUsize::new(0),
                published_objects: AtomicUsize::new(0),
                pending_bytes: AtomicU64::new(0),
                live_bytes: AtomicU64::new(0),
                retired_bytes: AtomicU64::new(0),
                reclaim_debt: AtomicU64::new(0),
            }),
        })
    }

    pub fn claim_put(
        &self,
        identity: ObjectIdentity,
        owner: WriteOwner,
        now: CatalogTick,
    ) -> Result<PutClaim, PutError> {
        if identity.key().is_empty() {
            return Err(PutError::EmptyKey);
        }
        if self.inner.retired_bytes.load(Ordering::Relaxed) >= self.inner.config.max_retired_bytes {
            return Err(PutError::ReclamationBacklog);
        }

        loop {
            let slot = match self.inner.entries.entry_sync(identity.clone()) {
                Entry::Occupied(entry) => entry.get().clone(),
                Entry::Vacant(entry) => {
                    let id = WriteId::new(1);
                    let node = Arc::new(CatalogNode::claimed(id, owner));
                    // Publish an already-claimed slot into the index so the
                    // fresh-key fast path never invokes an ArcSwap writer.
                    let slot = Arc::new(ObjectSlot {
                        identity: Arc::new(entry.key().clone()),
                        state: AtomicU8::new(SLOT_OPEN),
                        generation: AtomicU64::new(id.generation()),
                        current: ArcSwapOption::new(Some(node.clone())),
                    });
                    drop(entry.insert_entry(slot.clone()));
                    self.inner.slots.fetch_add(1, Ordering::Relaxed);
                    self.inner.claims.fetch_add(1, Ordering::Relaxed);
                    return Ok(PutClaim {
                        catalog: Arc::downgrade(&self.inner),
                        slot: Arc::downgrade(&slot),
                        node: Some(node),
                        identity: slot.identity.clone(),
                        id,
                        owner,
                        started_at: now,
                    });
                }
            };

            if slot.state.load(Ordering::Acquire) == SLOT_CLOSING {
                let _ = slot.state.compare_exchange(
                    SLOT_CLOSING,
                    SLOT_OPEN,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
            if slot.state.load(Ordering::Acquire) != SLOT_OPEN || !self.inner.slot_is_indexed(&slot)
            {
                continue;
            }

            let id = slot.next_write_id();
            let node = Arc::new(CatalogNode::claimed(id, owner));
            let previous = slot
                .current
                .compare_and_swap(&None::<Arc<CatalogNode>>, Some(node.clone()));
            if previous.is_none() {
                if !self.inner.slot_is_indexed(&slot) {
                    let _ = clear_slot(&slot, &node);
                    continue;
                }
                let _ = slot.state.compare_exchange(
                    SLOT_CLOSING,
                    SLOT_OPEN,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                self.inner.claims.fetch_add(1, Ordering::Relaxed);
                return Ok(PutClaim {
                    catalog: Arc::downgrade(&self.inner),
                    slot: Arc::downgrade(&slot),
                    node: Some(node),
                    identity: slot.identity.clone(),
                    id,
                    owner,
                    started_at: now,
                });
            }

            let lifecycle = previous
                .as_ref()
                .expect("the slot was occupied")
                .control
                .lifecycle
                .load(Ordering::Acquire);
            return Err(match lifecycle {
                OBJECT_CLAIMED | OBJECT_PENDING | OBJECT_PUBLISHING => PutError::WriteInProgress,
                OBJECT_PUBLISHED | OBJECT_RETIRING => PutError::AlreadyExists,
                _ => unreachable!("object lifecycle is validated internally"),
            });
        }
    }

    pub fn publish(
        &self,
        ticket: &PutTicket,
        commit: ObjectCommit,
    ) -> Result<ObjectHandle, PublishError> {
        let ticket_catalog = ticket.catalog.upgrade().ok_or(PublishError::ObjectGone)?;
        if !Arc::ptr_eq(&ticket_catalog, &self.inner) {
            return Err(PublishError::ForeignCatalog);
        }
        let slot = ticket.slot.upgrade().ok_or(PublishError::ObjectGone)?;
        if !slot_points_to(&slot, &ticket.node) {
            return Err(PublishError::ObjectGone);
        }
        if ticket.node.control.write_id != ticket.id || ticket.node.record.get().is_none() {
            return Err(PublishError::ObjectGone);
        }

        match ticket.node.control.lifecycle.compare_exchange(
            OBJECT_PENDING,
            OBJECT_PUBLISHING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                ticket
                    .node
                    .control
                    .commit
                    .set(commit)
                    .expect("the publishing transition has a single owner");
                let reserved_bytes = ticket
                    .node
                    .record
                    .get()
                    .expect("pending objects always have records")
                    .reserved_bytes;
                ticket.node.commit_accounting();
                self.inner.pending_objects.fetch_sub(1, Ordering::Relaxed);
                self.inner.published_objects.fetch_add(1, Ordering::Relaxed);
                atomic_saturating_sub(&self.inner.pending_bytes, reserved_bytes);
                self.inner
                    .live_bytes
                    .fetch_add(reserved_bytes, Ordering::Relaxed);
                ticket
                    .node
                    .control
                    .lifecycle
                    .store(OBJECT_PUBLISHED, Ordering::Release);
                self.inner.young.push(GcCandidate::new(&slot, &ticket.node));
                Ok(ObjectHandle {
                    node: ticket.node.clone(),
                })
            }
            Err(OBJECT_PUBLISHING) => Err(PublishError::PublicationInProgress),
            Err(OBJECT_PUBLISHED) => {
                if ticket.node.control.commit.get() == Some(&commit) {
                    Ok(ObjectHandle {
                        node: ticket.node.clone(),
                    })
                } else {
                    Err(PublishError::CommitConflict)
                }
            }
            Err(_) => Err(PublishError::NotPending),
        }
    }

    pub fn revoke(&self, ticket: &PutTicket, now: CatalogTick) -> Result<(), RevokeError> {
        let ticket_catalog = ticket.catalog.upgrade().ok_or(RevokeError::ObjectGone)?;
        if !Arc::ptr_eq(&ticket_catalog, &self.inner) {
            return Err(RevokeError::ForeignCatalog);
        }
        let slot = ticket.slot.upgrade().ok_or(RevokeError::ObjectGone)?;
        if ticket.node.record.get().is_none() {
            return Err(RevokeError::ObjectGone);
        }
        match ticket.node.control.lifecycle.compare_exchange(
            OBJECT_PENDING,
            OBJECT_RETIRING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(OBJECT_PUBLISHING | OBJECT_PUBLISHED) => {
                return Err(RevokeError::AlreadyPublished);
            }
            Err(_) => return Err(RevokeError::ObjectGone),
        }
        if !clear_slot(&slot, &ticket.node) {
            return Err(RevokeError::ObjectGone);
        }
        self.inner.retire_pending(slot, ticket.node.clone(), now);
        Ok(())
    }

    pub fn get(
        &self,
        lookup: ObjectLookup<'_>,
        now: CatalogTick,
    ) -> Result<ObjectRead, LookupError> {
        let slot = self
            .inner
            .lookup_slot(lookup)
            .ok_or(LookupError::NotFound)?;
        for _ in 0..3 {
            let node = slot.current.load_full().ok_or(LookupError::NotFound)?;
            match node.control.lifecycle.load(Ordering::Acquire) {
                OBJECT_CLAIMED | OBJECT_PENDING | OBJECT_PUBLISHING => {
                    return Err(LookupError::NotReady);
                }
                OBJECT_RETIRING => return Err(LookupError::NotFound),
                OBJECT_PUBLISHED => {}
                _ => unreachable!("object lifecycle is validated internally"),
            }

            let lease_expires_at = node.control.acquire_lease(
                now,
                self.inner.config.lease_ttl_ticks,
                self.inner.config.lease_refresh_ticks,
            );
            node.control.recent.store(true, Ordering::Relaxed);
            if node.control.lifecycle.load(Ordering::Acquire) == OBJECT_PUBLISHED
                && slot_points_to(&slot, &node)
            {
                return Ok(ObjectRead {
                    object: ObjectHandle { node },
                    lease_expires_at,
                });
            }
        }
        Err(LookupError::NotFound)
    }

    pub(super) fn inspect_write(
        &self,
        lookup: ObjectLookup<'_>,
    ) -> Result<ObjectWriteState, LookupError> {
        let slot = self
            .inner
            .lookup_slot(lookup)
            .ok_or(LookupError::NotFound)?;
        for _ in 0..3 {
            let node = slot.current.load_full().ok_or(LookupError::NotFound)?;
            let lifecycle = node.control.lifecycle.load(Ordering::Acquire);
            match lifecycle {
                OBJECT_CLAIMED | OBJECT_PUBLISHING => return Err(LookupError::NotReady),
                OBJECT_RETIRING => return Err(LookupError::NotFound),
                OBJECT_PENDING | OBJECT_PUBLISHED => {}
                _ => unreachable!("object lifecycle is validated internally"),
            }
            if !slot_points_to(&slot, &node)
                || node.control.lifecycle.load(Ordering::Acquire) != lifecycle
            {
                continue;
            }
            return Ok(if lifecycle == OBJECT_PENDING {
                ObjectWriteState::Pending(PutTicket {
                    catalog: Arc::downgrade(&self.inner),
                    slot: Arc::downgrade(&slot),
                    id: node.control.write_id,
                    node,
                })
            } else {
                ObjectWriteState::Published(ObjectHandle { node })
            });
        }
        Err(LookupError::NotFound)
    }

    pub fn get_batch_into<'a, I>(
        &self,
        lookups: I,
        now: CatalogTick,
        output: &mut Vec<Result<ObjectRead, LookupError>>,
    ) where
        I: IntoIterator<Item = ObjectLookup<'a>>,
    {
        output.extend(lookups.into_iter().map(|lookup| self.get(lookup, now)));
    }

    pub fn remove(&self, lookup: ObjectLookup<'_>, now: CatalogTick) -> Result<(), RemoveError> {
        let slot = self
            .inner
            .lookup_slot(lookup)
            .ok_or(RemoveError::NotFound)?;
        let node = slot.current.load_full().ok_or(RemoveError::NotFound)?;
        match node.control.lifecycle.compare_exchange(
            OBJECT_PUBLISHED,
            OBJECT_RETIRING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(OBJECT_CLAIMED | OBJECT_PENDING | OBJECT_PUBLISHING) => {
                return Err(RemoveError::NotReady);
            }
            Err(_) => return Err(RemoveError::NotFound),
        }

        let lease_until = CatalogTick::new(node.control.lease_until.load(Ordering::Acquire));
        if lease_until > now {
            let _ = node.control.lifecycle.compare_exchange(
                OBJECT_RETIRING,
                OBJECT_PUBLISHED,
                Ordering::Release,
                Ordering::Relaxed,
            );
            return Err(RemoveError::Leased {
                expires_at: lease_until,
            });
        }
        if !clear_slot(&slot, &node) {
            return Err(RemoveError::NotFound);
        }
        self.inner.retire_published(slot, node, now);
        Ok(())
    }

    pub fn request_reclaim(&self, bytes: u64) {
        self.inner.reclaim_debt.fetch_max(bytes, Ordering::Relaxed);
    }

    pub fn collect_step(&self, now: CatalogTick, budget: CollectBudget) -> CollectReport {
        self.collect_step_with_targets(now, budget, &[])
    }

    pub(super) fn collect_step_with_targets(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        targets: &[ReclaimTarget],
    ) -> CollectReport {
        let Some(_collector) = self.inner.collector_gate.try_lock() else {
            return CollectReport {
                busy: true,
                ..CollectReport::default()
            };
        };

        let mut report = CollectReport::default();
        self.inner.expire_pending(now, budget, &mut report);
        let scoped_scanned = self.inner.evict_scoped(now, budget, targets, &mut report);
        let global_budget = CollectBudget::new(
            budget.max_candidates.saturating_sub(scoped_scanned),
            budget.max_reclaims,
            budget.max_empty_slots,
        );
        self.inner.evict(now, global_budget, &mut report);
        self.inner.reclaim(now, budget, &mut report);
        self.inner.clean_empty_slots(now, budget, &mut report);
        report
    }

    pub fn stats(&self) -> ObjectCatalogStats {
        ObjectCatalogStats {
            slots: self.inner.slots.load(Ordering::Relaxed),
            claims: self.inner.claims.load(Ordering::Relaxed),
            pending_objects: self.inner.pending_objects.load(Ordering::Relaxed),
            published_objects: self.inner.published_objects.load(Ordering::Relaxed),
            pending_bytes: self.inner.pending_bytes.load(Ordering::Relaxed),
            live_bytes: self.inner.live_bytes.load(Ordering::Relaxed),
            retired_bytes: self.inner.retired_bytes.load(Ordering::Relaxed),
            reclaim_debt: self.inner.reclaim_debt.load(Ordering::Relaxed),
            pending_candidates: self.inner.pending.len(),
            young_candidates: self.inner.young.len(),
            protected_candidates: self.inner.protected.len(),
            retired_candidates: self.inner.retired.len(),
            empty_slot_candidates: self.inner.empty_slots.len(),
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
        self.entries.read_sync(&lookup, |_, slot| Arc::clone(slot))
    }

    fn slot_is_indexed(&self, slot: &Arc<ObjectSlot>) -> bool {
        self.lookup_slot(slot.identity.as_lookup())
            .is_some_and(|indexed| Arc::ptr_eq(&indexed, slot))
    }

    fn enqueue_empty(&self, slot: &Arc<ObjectSlot>, now: CatalogTick) {
        self.empty_slots.push(EmptySlotCandidate {
            identity: slot.identity.clone(),
            slot: Arc::downgrade(slot),
            deadline: now.saturating_add(self.config.empty_slot_grace_ticks),
        });
    }

    fn retire_pending(&self, slot: Arc<ObjectSlot>, node: Arc<CatalogNode>, now: CatalogTick) {
        let record = node
            .record
            .get()
            .expect("only staged objects can be retired");
        let bytes = record.reserved_bytes;
        node.abort_accounting();
        self.pending_objects.fetch_sub(1, Ordering::Relaxed);
        atomic_saturating_sub(&self.pending_bytes, bytes);
        self.retired_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.retired.push(RetiredObject {
            node,
            reserved_bytes: bytes,
            retry_at: now,
        });
        self.enqueue_empty(&slot, now);
    }

    fn retire_published(&self, slot: Arc<ObjectSlot>, node: Arc<CatalogNode>, now: CatalogTick) {
        let record = node
            .record
            .get()
            .expect("only published objects can be retired");
        let bytes = record.reserved_bytes;
        node.mark_accounting_retiring();
        self.published_objects.fetch_sub(1, Ordering::Relaxed);
        atomic_saturating_sub(&self.live_bytes, bytes);
        self.retired_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.retired.push(RetiredObject {
            node,
            reserved_bytes: bytes,
            retry_at: now,
        });
        self.enqueue_empty(&slot, now);
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

fn validate_config(config: ObjectCatalogConfig) -> Result<(), ObjectCatalogConfigError> {
    if config.expected_objects == 0 {
        return Err(ObjectCatalogConfigError::ZeroExpectedObjects);
    }
    if config.lease_ttl_ticks == 0 {
        return Err(ObjectCatalogConfigError::ZeroLeaseTtl);
    }
    if config.lease_refresh_ticks > config.lease_ttl_ticks {
        return Err(ObjectCatalogConfigError::LeaseRefreshExceedsTtl {
            lease_ttl_ticks: config.lease_ttl_ticks,
            lease_refresh_ticks: config.lease_refresh_ticks,
        });
    }
    if config.pending_timeout_ticks == 0 {
        return Err(ObjectCatalogConfigError::ZeroPendingTimeout);
    }
    if config.max_retired_bytes == 0 {
        return Err(ObjectCatalogConfigError::ZeroMaxRetiredBytes);
    }
    Ok(())
}

fn atomic_saturating_sub(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(amount))
    });
}
