use super::types::{
    CatalogTick, CollectBudget, ObjectCommit, ObjectContent, ObjectIdentity, ObjectLookup,
    ReclaimTarget, ReplicaLease, ReplicaReclaimBatch, ReplicaSet, WriteId, WriteOwner,
};
use arc_swap::ArcSwapOption;
use crossbeam_queue::SegQueue;
use parking_lot::Mutex;
use scc::hash_map::Entry;
use scc::{Equivalent, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};

const SLOT_OPEN: u8 = 0;
const SLOT_CLOSING: u8 = 1;

const OBJECT_CLAIMED: u8 = 0;
const OBJECT_PENDING: u8 = 1;
const OBJECT_PUBLISHING: u8 = 2;
const OBJECT_PUBLISHED: u8 = 3;
const OBJECT_RETIRING: u8 = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectCatalogConfig {
    index: CatalogIndexConfig,
    leases: ObjectLeasePolicy,
    reclamation: ReclamationPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogIndexConfig {
    expected_objects: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectLeasePolicy {
    lease_ttl_ticks: u64,
    lease_refresh_ticks: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReclamationPolicy {
    pending_timeout_ticks: u64,
    empty_slot_grace_ticks: u64,
    max_retired_bytes: u64,
}

impl ObjectCatalogConfig {
    pub const fn new(expected_objects: usize) -> Self {
        Self {
            index: CatalogIndexConfig::new(expected_objects),
            leases: ObjectLeasePolicy::new(10_000, 5_000),
            reclamation: ReclamationPolicy::new(30_000, 60_000, 1_u64 << 30),
        }
    }

    pub const fn with_index(mut self, index: CatalogIndexConfig) -> Self {
        self.index = index;
        self
    }

    pub const fn with_lease_policy(mut self, leases: ObjectLeasePolicy) -> Self {
        self.leases = leases;
        self
    }

    pub const fn with_reclamation_policy(mut self, reclamation: ReclamationPolicy) -> Self {
        self.reclamation = reclamation;
        self
    }

    pub const fn with_lease(mut self, ttl_ticks: u64, refresh_ticks: u64) -> Self {
        self.leases = ObjectLeasePolicy::new(ttl_ticks, refresh_ticks);
        self
    }

    pub const fn with_pending_timeout(mut self, ticks: u64) -> Self {
        self.reclamation.pending_timeout_ticks = ticks;
        self
    }

    pub const fn with_empty_slot_grace(mut self, ticks: u64) -> Self {
        self.reclamation.empty_slot_grace_ticks = ticks;
        self
    }

    pub const fn with_max_retired_bytes(mut self, bytes: u64) -> Self {
        self.reclamation.max_retired_bytes = bytes;
        self
    }

    pub const fn index(self) -> CatalogIndexConfig {
        self.index
    }

    pub const fn leases(self) -> ObjectLeasePolicy {
        self.leases
    }

    pub const fn reclamation(self) -> ReclamationPolicy {
        self.reclamation
    }
}

impl CatalogIndexConfig {
    pub const fn new(expected_objects: usize) -> Self {
        Self { expected_objects }
    }

    pub const fn expected_objects(self) -> usize {
        self.expected_objects
    }
}

impl ObjectLeasePolicy {
    pub const fn new(ttl_ticks: u64, refresh_ticks: u64) -> Self {
        Self {
            lease_ttl_ticks: ttl_ticks,
            lease_refresh_ticks: refresh_ticks,
        }
    }

    pub const fn lease_ttl_ticks(self) -> u64 {
        self.lease_ttl_ticks
    }

    pub const fn lease_refresh_ticks(self) -> u64 {
        self.lease_refresh_ticks
    }
}

impl ReclamationPolicy {
    pub const fn new(
        pending_timeout_ticks: u64,
        empty_slot_grace_ticks: u64,
        max_retired_bytes: u64,
    ) -> Self {
        Self {
            pending_timeout_ticks,
            empty_slot_grace_ticks,
            max_retired_bytes,
        }
    }

    pub const fn pending_timeout_ticks(self) -> u64 {
        self.pending_timeout_ticks
    }

    pub const fn empty_slot_grace_ticks(self) -> u64 {
        self.empty_slot_grace_ticks
    }

    pub const fn max_retired_bytes(self) -> u64 {
        self.max_retired_bytes
    }
}

impl Default for ObjectCatalogConfig {
    fn default() -> Self {
        Self::new(64 * 1024)
    }
}

impl Default for CatalogIndexConfig {
    fn default() -> Self {
        ObjectCatalogConfig::default().index()
    }
}

impl Default for ObjectLeasePolicy {
    fn default() -> Self {
        ObjectCatalogConfig::default().leases()
    }
}

impl Default for ReclamationPolicy {
    fn default() -> Self {
        ObjectCatalogConfig::default().reclamation()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectCatalogConfigError {
    ZeroExpectedObjects,
    ZeroLeaseTtl,
    RefreshExceedsLease,
    ZeroPendingTimeout,
    ZeroRetiredLimit,
}

impl fmt::Display for ObjectCatalogConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroExpectedObjects => "expected object count must not be zero",
            Self::ZeroLeaseTtl => "object lease TTL must not be zero",
            Self::RefreshExceedsLease => "lease refresh threshold must not exceed the lease TTL",
            Self::ZeroPendingTimeout => "pending object timeout must not be zero",
            Self::ZeroRetiredLimit => "retired byte limit must not be zero",
        })
    }
}

impl std::error::Error for ObjectCatalogConfigError {}

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
}

struct ObjectControl {
    lifecycle: AtomicU8,
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

#[derive(Clone)]
pub struct ObjectRead {
    object: ObjectHandle,
    lease_expires_at: CatalogTick,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutError {
    EmptyKey,
    AlreadyExists,
    WriteInProgress,
    ReclamationBacklog,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StageError {
    CatalogDropped,
    ClaimLost,
    ZeroSize,
    NoReplicas,
    ReplicaTooSmall {
        replica: super::types::ReplicaId,
        required_bytes: u64,
        capacity_bytes: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublishError {
    ForeignCatalog,
    ObjectGone,
    PublicationInProgress,
    NotPending,
    CommitConflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RevokeError {
    ForeignCatalog,
    ObjectGone,
    AlreadyPublished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LookupError {
    NotFound,
    NotReady,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveError {
    NotFound,
    NotReady,
    Leased { expires_at: CatalogTick },
}

macro_rules! impl_error {
    ($type:ty) => {
        impl std::error::Error for $type {}

        impl fmt::Display for $type {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{self:?}")
            }
        }
    };
}

impl_error!(PutError);
impl_error!(StageError);
impl_error!(PublishError);
impl_error!(RevokeError);
impl_error!(LookupError);
impl_error!(RemoveError);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CollectReport {
    pub busy: bool,
    pub scanned_candidates: usize,
    pub expired_pending: usize,
    pub retired_objects: usize,
    pub retired_bytes: u64,
    pub reclaimed_objects: usize,
    pub reclaimed_bytes: u64,
    pub removed_empty_slots: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectCatalogStats {
    pub slots: usize,
    pub claims: usize,
    pub pending_objects: usize,
    pub published_objects: usize,
    pub pending_bytes: u64,
    pub live_bytes: u64,
    pub retired_bytes: u64,
    pub reclaim_debt: u64,
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
                entries: HashMap::with_capacity(config.index.expected_objects),
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

    pub fn config(&self) -> ObjectCatalogConfig {
        self.inner.config
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
        if self.inner.retired_bytes.load(Ordering::Relaxed)
            >= self.inner.config.reclamation.max_retired_bytes
        {
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

            let lease_expires_at = node.control.acquire_lease(now, self.inner.config.leases);
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

    pub fn request_reclaim(&self, target: ReclaimTarget) {
        self.inner
            .reclaim_debt
            .fetch_max(target.bytes(), Ordering::Relaxed);
    }

    pub fn collect_step(&self, now: CatalogTick, budget: CollectBudget) -> CollectReport {
        let Some(_collector) = self.inner.collector_gate.try_lock() else {
            return CollectReport {
                busy: true,
                ..CollectReport::default()
            };
        };

        let mut report = CollectReport::default();
        self.inner.expire_pending(now, budget, &mut report);
        self.inner.evict(now, budget, &mut report);
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

    fn enqueue_empty(&self, slot: Arc<ObjectSlot>, now: CatalogTick) {
        self.empty_slots.push(EmptySlotCandidate {
            identity: slot.identity.clone(),
            slot: Arc::downgrade(&slot),
            deadline: now.saturating_add(self.config.reclamation.empty_slot_grace_ticks),
        });
    }

    fn retire_pending(&self, slot: Arc<ObjectSlot>, node: Arc<CatalogNode>, now: CatalogTick) {
        let bytes = node
            .record
            .get()
            .expect("only staged objects can be retired")
            .reserved_bytes;
        self.pending_objects.fetch_sub(1, Ordering::Relaxed);
        atomic_saturating_sub(&self.pending_bytes, bytes);
        self.retired_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.retired.push(RetiredObject {
            node,
            reserved_bytes: bytes,
            retry_at: now,
        });
        self.enqueue_empty(slot, now);
    }

    fn retire_published(&self, slot: Arc<ObjectSlot>, node: Arc<CatalogNode>, now: CatalogTick) {
        let bytes = node
            .record
            .get()
            .expect("only published objects can be retired")
            .reserved_bytes;
        self.published_objects.fetch_sub(1, Ordering::Relaxed);
        atomic_saturating_sub(&self.live_bytes, bytes);
        self.retired_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.retired.push(RetiredObject {
            node,
            reserved_bytes: bytes,
            retry_at: now,
        });
        self.enqueue_empty(slot, now);
    }

    fn expire_pending(&self, now: CatalogTick, budget: CollectBudget, report: &mut CollectReport) {
        let candidates = self.pending.len().min(budget.max_candidates());
        for _ in 0..candidates {
            let Some(pending) = self.pending.pop() else {
                break;
            };
            if pending.deadline > now {
                self.pending.push(pending);
                continue;
            }
            let Some(slot) = pending.candidate.slot.upgrade() else {
                continue;
            };
            let Some(node) = pending.candidate.node.upgrade() else {
                continue;
            };
            if node.record.get().is_none() {
                continue;
            }
            if node
                .control
                .lifecycle
                .compare_exchange(
                    OBJECT_PENDING,
                    OBJECT_RETIRING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                continue;
            }
            if clear_slot(&slot, &node) {
                self.retire_pending(slot, node, now);
                report.expired_pending += 1;
            }
        }
    }

    fn evict(&self, now: CatalogTick, budget: CollectBudget, report: &mut CollectReport) {
        // Snapshot both generation sizes before scanning. An object promoted
        // from young to protected must not be reconsidered in the same pause.
        let young_candidates = self.young.len();
        let protected_candidates = self.protected.len();
        let mut remaining = budget.max_candidates();

        for _ in 0..young_candidates.min(remaining) {
            if self.reclaim_target_is_covered() {
                return;
            }
            let Some(candidate) = self.young.pop() else {
                break;
            };
            remaining -= 1;
            self.evict_candidate(candidate, false, now, report);
        }
        for _ in 0..protected_candidates.min(remaining) {
            if self.reclaim_target_is_covered() {
                return;
            }
            let Some(candidate) = self.protected.pop() else {
                break;
            };
            self.evict_candidate(candidate, true, now, report);
        }
    }

    fn reclaim_target_is_covered(&self) -> bool {
        self.reclaim_debt.load(Ordering::Relaxed) <= self.retired_bytes.load(Ordering::Relaxed)
    }

    fn evict_candidate(
        &self,
        candidate: GcCandidate,
        from_protected: bool,
        now: CatalogTick,
        report: &mut CollectReport,
    ) {
        report.scanned_candidates += 1;
        let Some(slot) = candidate.slot.upgrade() else {
            return;
        };
        let Some(node) = candidate.node.upgrade() else {
            return;
        };
        if node.control.lifecycle.load(Ordering::Acquire) != OBJECT_PUBLISHED
            || !slot_points_to(&slot, &node)
        {
            return;
        }

        if node.control.recent.swap(false, Ordering::Relaxed) {
            self.protected.push(candidate);
            return;
        }
        if node
            .control
            .lifecycle
            .compare_exchange(
                OBJECT_PUBLISHED,
                OBJECT_RETIRING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }

        if node.control.lease_until.load(Ordering::Acquire) > now.get() {
            node.control
                .lifecycle
                .store(OBJECT_PUBLISHED, Ordering::Release);
            self.protected.push(candidate);
            return;
        }
        if clear_slot(&slot, &node) {
            let bytes = node
                .record
                .get()
                .expect("published objects always have records")
                .reserved_bytes;
            self.retire_published(slot, node, now);
            report.retired_objects += 1;
            report.retired_bytes = report.retired_bytes.saturating_add(bytes);
        } else {
            node.control
                .lifecycle
                .store(OBJECT_PUBLISHED, Ordering::Release);
            if from_protected {
                self.protected.push(candidate);
            } else {
                self.young.push(candidate);
            }
        }
    }

    fn reclaim(&self, now: CatalogTick, budget: CollectBudget, report: &mut CollectReport) {
        let retired_objects = self.retired.len().min(budget.max_reclaims());
        let mut resources = ReplicaReclaimBatch::with_capacity(retired_objects);
        for _ in 0..retired_objects {
            let Some(retired) = self.retired.pop() else {
                break;
            };
            if retired.retry_at > now {
                self.retired.push(retired);
                continue;
            }

            match Arc::try_unwrap(retired.node) {
                Ok(node) => {
                    let record = node
                        .record
                        .into_inner()
                        .expect("retired objects always have records");
                    resources.extend(record.replicas);
                    atomic_saturating_sub(&self.retired_bytes, retired.reserved_bytes);
                    atomic_saturating_sub(&self.reclaim_debt, retired.reserved_bytes);
                    report.reclaimed_objects += 1;
                    report.reclaimed_bytes = report
                        .reclaimed_bytes
                        .saturating_add(retired.reserved_bytes);
                }
                Err(node) => self.retired.push(RetiredObject {
                    node,
                    reserved_bytes: retired.reserved_bytes,
                    retry_at: now.saturating_add(1),
                }),
            }
        }
        resources.release();
    }

    fn clean_empty_slots(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        report: &mut CollectReport,
    ) {
        let candidates = self.empty_slots.len().min(budget.max_empty_slots());
        for _ in 0..candidates {
            let Some(candidate) = self.empty_slots.pop() else {
                break;
            };
            if candidate.deadline > now {
                self.empty_slots.push(candidate);
                continue;
            }
            let Some(slot) = candidate.slot.upgrade() else {
                continue;
            };
            if slot.current.load().is_some()
                || slot
                    .state
                    .compare_exchange(SLOT_OPEN, SLOT_CLOSING, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
            {
                continue;
            }
            let removed = self
                .entries
                .remove_if_sync(&candidate.identity.as_lookup(), |indexed| {
                    Arc::ptr_eq(indexed, &slot)
                        && slot.state.load(Ordering::Acquire) == SLOT_CLOSING
                        && slot.current.load().is_none()
                });
            if removed.is_some() {
                self.slots.fetch_sub(1, Ordering::Relaxed);
                report.removed_empty_slots += 1;
            } else {
                slot.state.store(SLOT_OPEN, Ordering::Release);
            }
        }
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
}

impl ObjectControl {
    fn acquire_lease(&self, now: CatalogTick, policy: ObjectLeasePolicy) -> CatalogTick {
        let refresh_at = now.saturating_add(policy.lease_refresh_ticks).get();
        let desired = now.saturating_add(policy.lease_ttl_ticks).get();
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

impl PutClaim {
    pub const fn id(&self) -> WriteId {
        self.id
    }

    pub fn identity(&self) -> &ObjectIdentity {
        &self.identity
    }

    pub const fn owner(&self) -> WriteOwner {
        self.owner
    }

    pub fn stage(
        mut self,
        content: ObjectContent,
        replicas: ReplicaSet,
    ) -> Result<PutTicket, StageError> {
        if content.logical_bytes() == 0 {
            return Err(StageError::ZeroSize);
        }
        if replicas.is_empty() {
            return Err(StageError::NoReplicas);
        }
        if let Some(replica) = replicas
            .replicas()
            .iter()
            .find(|replica| replica.capacity_bytes() < content.logical_bytes())
        {
            return Err(StageError::ReplicaTooSmall {
                replica: replica.id(),
                required_bytes: content.logical_bytes(),
                capacity_bytes: replica.capacity_bytes(),
            });
        }
        let catalog = self.catalog.upgrade().ok_or(StageError::CatalogDropped)?;
        let slot = self.slot.upgrade().ok_or(StageError::ClaimLost)?;
        let node = self.node.as_ref().ok_or(StageError::ClaimLost)?;
        if !slot_points_to(&slot, node) {
            return Err(StageError::ClaimLost);
        }
        if node.control.write_id != self.id
            || node.control.owner != self.owner
            || node.control.lifecycle.load(Ordering::Acquire) != OBJECT_CLAIMED
        {
            return Err(StageError::ClaimLost);
        }

        let reserved_bytes = replicas.reserved_bytes();
        if node
            .record
            .set(ObjectRecord {
                identity: self.identity.clone(),
                content,
                replicas,
                reserved_bytes,
            })
            .is_err()
            || node
                .control
                .lifecycle
                .compare_exchange(
                    OBJECT_CLAIMED,
                    OBJECT_PENDING,
                    Ordering::Release,
                    Ordering::Acquire,
                )
                .is_err()
        {
            return Err(StageError::ClaimLost);
        }

        catalog.claims.fetch_sub(1, Ordering::Relaxed);
        catalog.pending_objects.fetch_add(1, Ordering::Relaxed);
        catalog
            .pending_bytes
            .fetch_add(reserved_bytes, Ordering::Relaxed);
        let node = node.clone();
        let candidate = GcCandidate::new(&slot, &node);
        catalog.pending.push(PendingCandidate {
            candidate,
            deadline: self
                .started_at
                .saturating_add(catalog.config.reclamation.pending_timeout_ticks),
        });
        self.node = None;

        Ok(PutTicket {
            catalog: Arc::downgrade(&catalog),
            slot: Arc::downgrade(&slot),
            node,
            id: self.id,
        })
    }
}

impl Drop for PutClaim {
    fn drop(&mut self) {
        let Some(node) = self.node.take() else {
            return;
        };
        let Some(catalog) = self.catalog.upgrade() else {
            return;
        };
        let Some(slot) = self.slot.upgrade() else {
            return;
        };
        if clear_slot(&slot, &node) {
            catalog.claims.fetch_sub(1, Ordering::Relaxed);
            catalog.enqueue_empty(slot, self.started_at);
        }
    }
}

impl fmt::Debug for PutClaim {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PutClaim")
            .field("identity", &self.identity)
            .field("id", &self.id)
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}

impl PutTicket {
    pub const fn id(&self) -> WriteId {
        self.id
    }

    pub fn identity(&self) -> &ObjectIdentity {
        &self.node.record().identity
    }

    pub fn content(&self) -> ObjectContent {
        self.node.record().content
    }

    pub fn replicas(&self) -> &[ReplicaLease] {
        self.node.record().replicas.replicas()
    }
}

impl fmt::Debug for PutTicket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PutTicket")
            .field("identity", &self.identity())
            .field("id", &self.id)
            .field("content", &self.content())
            .field("replicas", &self.replicas())
            .finish()
    }
}

impl ObjectHandle {
    pub fn identity(&self) -> &ObjectIdentity {
        &self.node.record().identity
    }

    pub fn content(&self) -> ObjectContent {
        self.node.record().content
    }

    pub fn commit(&self) -> ObjectCommit {
        *self
            .node
            .control
            .commit
            .get()
            .expect("published objects always have commit metadata")
    }

    pub fn replicas(&self) -> &[ReplicaLease] {
        self.node.record().replicas.replicas()
    }

    pub fn owner(&self) -> WriteOwner {
        self.node.control.owner
    }
}

impl fmt::Debug for ObjectHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectHandle")
            .field("identity", &self.identity())
            .field("content", &self.content())
            .field("commit", &self.commit())
            .field("replicas", &self.replicas())
            .finish()
    }
}

impl ObjectRead {
    pub const fn object(&self) -> &ObjectHandle {
        &self.object
    }

    pub const fn lease_expires_at(&self) -> CatalogTick {
        self.lease_expires_at
    }
}

impl fmt::Debug for ObjectRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectRead")
            .field("object", &self.object)
            .field("lease_expires_at", &self.lease_expires_at)
            .finish()
    }
}

impl Equivalent<ObjectIdentity> for ObjectLookup<'_> {
    fn equivalent(&self, key: &ObjectIdentity) -> bool {
        self.namespace() == key.namespace() && self.key() == key.key().as_str()
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
    if config.index.expected_objects == 0 {
        return Err(ObjectCatalogConfigError::ZeroExpectedObjects);
    }
    if config.leases.lease_ttl_ticks == 0 {
        return Err(ObjectCatalogConfigError::ZeroLeaseTtl);
    }
    if config.leases.lease_refresh_ticks > config.leases.lease_ttl_ticks {
        return Err(ObjectCatalogConfigError::RefreshExceedsLease);
    }
    if config.reclamation.pending_timeout_ticks == 0 {
        return Err(ObjectCatalogConfigError::ZeroPendingTimeout);
    }
    if config.reclamation.max_retired_bytes == 0 {
        return Err(ObjectCatalogConfigError::ZeroRetiredLimit);
    }
    Ok(())
}

fn atomic_saturating_sub(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(amount))
    });
}
