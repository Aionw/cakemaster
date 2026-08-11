use super::catalog::{ObjectCatalog, ObjectHandle, ObjectRead, PutClaim, PutTicket};
use super::config::ObjectCatalogConfig;
use super::content::ObjectContent;
use super::error::{
    LookupError, ObjectCatalogConfigError, ObjectManagerError, PublishError, RevokeError,
};
use super::identity::{ObjectIdentity, ObjectLookup};
use super::reclamation::{CatalogTick, CollectBudget, CollectReport, ReclaimTarget};
use super::replica::{ReplicaId, ReplicaSet};
use super::tenant::QuotaReservationGuard;
use super::write::{ObjectCommit, WriteId, WriteOwner};
use crate::segment::placement::{PlacementRequest, ReplicaAllocator};
use crate::segment::{ReplicaClass, ReservationDescriptor, SegmentPool};
use parking_lot::Mutex;
use scc::{Equivalent, HashMap};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::hash::Hash;
use std::sync::Arc;

pub struct ObjectManager {
    catalog: ObjectCatalog,
    allocator: ReplicaAllocator,
    pending: HashMap<ObjectIdentity, Arc<PendingPut>>,
    deadlines: Mutex<BinaryHeap<Reverse<PendingDeadline>>>,
    pending_timeout_ticks: u64,
}

struct PendingPut {
    owner: WriteOwner,
    id: WriteId,
    replica_class: ReplicaClass,
    ticket: Mutex<Option<PutTicket>>,
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct PendingDeadline {
    deadline: CatalogTick,
    identity: ObjectIdentity,
    generation: u64,
}

#[derive(Clone, Debug)]
pub struct ObjectPutPlan {
    content: ObjectContent,
    placement: PlacementRequest,
}

impl ObjectPutPlan {
    pub const fn new(content: ObjectContent, placement: PlacementRequest) -> Self {
        Self { content, placement }
    }

    pub const fn content(&self) -> ObjectContent {
        self.content
    }

    pub const fn placement(&self) -> &PlacementRequest {
        &self.placement
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaSelector {
    All,
    Class(ReplicaClass),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AllocatedReplica {
    id: ReplicaId,
    descriptor: ReservationDescriptor,
}

impl AllocatedReplica {
    pub const fn id(&self) -> ReplicaId {
        self.id
    }

    pub const fn descriptor(&self) -> &ReservationDescriptor {
        &self.descriptor
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartedPut {
    replica_class: ReplicaClass,
    replicas: Vec<AllocatedReplica>,
}

impl StartedPut {
    pub const fn replica_class(&self) -> ReplicaClass {
        self.replica_class
    }

    pub fn replicas(&self) -> &[AllocatedReplica] {
        &self.replicas
    }
}

/// A claimed object whose replicas have been allocated but whose catalog
/// record has not been staged yet. Keeping this phase explicit lets callers
/// adjust RAII accounting after placement without changing core error types.
pub(super) struct PreparedPut {
    identity: ObjectIdentity,
    owner: WriteOwner,
    content: ObjectContent,
    claim: PutClaim,
    replicas: ReplicaSet,
    started: StartedPut,
}

impl PreparedPut {
    #[inline]
    pub(super) fn actual_charge_bytes(&self) -> Result<u64, ObjectManagerError> {
        let replica_count =
            u64::try_from(self.replicas.len()).map_err(|_| ObjectManagerError::Internal)?;
        self.content
            .logical_bytes()
            .checked_mul(replica_count)
            .ok_or(ObjectManagerError::InvalidPlan)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectManagerMaintenance {
    pub expired_writes: usize,
    pub catalog: CollectReport,
}

impl ObjectManager {
    pub fn new(pool: Arc<SegmentPool>) -> Self {
        Self::with_config(pool, ObjectCatalogConfig::default())
            .expect("the default ObjectCatalog configuration is valid")
    }

    pub fn with_config(
        pool: Arc<SegmentPool>,
        config: ObjectCatalogConfig,
    ) -> Result<Self, ObjectCatalogConfigError> {
        let pending_timeout_ticks = config.pending_timeout_ticks;
        Ok(Self {
            catalog: ObjectCatalog::with_config(config)?,
            allocator: ReplicaAllocator::new(pool),
            pending: HashMap::with_capacity(config.expected_objects),
            deadlines: Mutex::new(BinaryHeap::new()),
            pending_timeout_ticks,
        })
    }

    pub const fn catalog(&self) -> &ObjectCatalog {
        &self.catalog
    }

    pub fn start_put(
        &self,
        identity: ObjectIdentity,
        owner: WriteOwner,
        plan: ObjectPutPlan,
        now: CatalogTick,
    ) -> Result<StartedPut, ObjectManagerError> {
        let prepared = self.prepare_put(identity, owner, plan, now)?;
        self.finalize_start_put(prepared, None, now)
    }

    #[inline]
    pub(super) fn prepare_put(
        &self,
        identity: ObjectIdentity,
        owner: WriteOwner,
        plan: ObjectPutPlan,
        now: CatalogTick,
    ) -> Result<PreparedPut, ObjectManagerError> {
        self.validate_plan(&plan)?;
        let claim = self.catalog.claim_put(identity.clone(), owner, now)?;
        let reservations = self.allocator.reserve(plan.placement())?;
        let replicas = ReplicaSet::from_reservations(reservations);
        if replicas.is_empty() {
            return Err(ObjectManagerError::NoAvailableReplicas);
        }

        let mut allocated = Vec::with_capacity(replicas.len());
        for replica in replicas.replicas() {
            let direct = replica.direct().ok_or(ObjectManagerError::Internal)?;
            allocated.push(AllocatedReplica {
                id: direct.id(),
                descriptor: direct.owned_descriptor(),
            });
        }

        let replica_class = plan.placement().replica_class();
        Ok(PreparedPut {
            identity,
            owner,
            content: plan.content(),
            claim,
            replicas,
            started: StartedPut {
                replica_class,
                replicas: allocated,
            },
        })
    }

    #[inline]
    pub(super) fn finalize_start_put(
        &self,
        prepared: PreparedPut,
        accounting: Option<QuotaReservationGuard>,
        now: CatalogTick,
    ) -> Result<StartedPut, ObjectManagerError> {
        let PreparedPut {
            identity,
            owner,
            content,
            claim,
            replicas,
            started,
        } = prepared;
        let ticket = match accounting {
            Some(reservation) => claim.stage_accounted(content, replicas, reservation)?,
            None => claim.stage(content, replicas)?,
        };
        let id = ticket.id();
        let pending = Arc::new(PendingPut {
            owner,
            id,
            replica_class: started.replica_class,
            ticket: Mutex::new(Some(ticket)),
        });

        if let Err((_identity, pending)) = self.pending.insert_sync(identity.clone(), pending) {
            if let Some(ticket) = pending.ticket.lock().take() {
                let _ = self.catalog.revoke(&ticket, now);
            }
            return Err(ObjectManagerError::Internal);
        }
        self.deadlines.lock().push(Reverse(PendingDeadline {
            deadline: now.saturating_add(self.pending_timeout_ticks),
            identity,
            generation: id.generation(),
        }));

        Ok(started)
    }

    pub fn finish_put(
        &self,
        identity: &ObjectIdentity,
        owner: WriteOwner,
        selector: ReplicaSelector,
    ) -> Result<(), ObjectManagerError> {
        self.finish_put_key(identity, identity.as_lookup(), owner, selector)
    }

    pub(super) fn finish_put_lookup(
        &self,
        lookup: ObjectLookup<'_>,
        owner: WriteOwner,
        selector: ReplicaSelector,
    ) -> Result<(), ObjectManagerError> {
        self.finish_put_key(&lookup, lookup, owner, selector)
    }

    fn finish_put_key<Q>(
        &self,
        pending_key: &Q,
        lookup: ObjectLookup<'_>,
        owner: WriteOwner,
        selector: ReplicaSelector,
    ) -> Result<(), ObjectManagerError>
    where
        Q: Equivalent<ObjectIdentity> + Hash + ?Sized,
    {
        let Some(pending) = self
            .pending
            .read_sync(pending_key, |_, pending| Arc::clone(pending))
        else {
            return self.finish_published(lookup, owner, selector);
        };
        validate_pending(&pending, owner, selector)?;

        let mut ticket_slot = pending.ticket.lock();
        let Some(ticket) = ticket_slot.take() else {
            drop(ticket_slot);
            return self.finish_published(lookup, owner, selector);
        };
        match self.catalog.publish(&ticket, ObjectCommit::new(None)) {
            Ok(_) => {
                self.remove_pending(pending_key, &pending);
                Ok(())
            }
            Err(PublishError::ObjectGone | PublishError::NotPending) => {
                self.remove_pending(pending_key, &pending);
                Err(ObjectManagerError::NotFound)
            }
            Err(PublishError::PublicationInProgress) => {
                *ticket_slot = Some(ticket);
                Err(ObjectManagerError::InvalidWrite)
            }
            Err(PublishError::ForeignCatalog | PublishError::CommitConflict) => {
                *ticket_slot = Some(ticket);
                Err(ObjectManagerError::Internal)
            }
        }
    }

    pub fn revoke_put(
        &self,
        identity: &ObjectIdentity,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Result<(), ObjectManagerError> {
        self.revoke_put_key(identity, identity.as_lookup(), owner, selector, now)
    }

    pub(super) fn revoke_put_lookup(
        &self,
        lookup: ObjectLookup<'_>,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Result<(), ObjectManagerError> {
        self.revoke_put_key(&lookup, lookup, owner, selector, now)
    }

    fn revoke_put_key<Q>(
        &self,
        pending_key: &Q,
        lookup: ObjectLookup<'_>,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Result<(), ObjectManagerError>
    where
        Q: Equivalent<ObjectIdentity> + Hash + ?Sized,
    {
        let Some(pending) = self
            .pending
            .read_sync(pending_key, |_, pending| Arc::clone(pending))
        else {
            return match self.catalog.inspect_published(lookup) {
                Ok(object) => {
                    validate_published(&object, owner, selector)?;
                    Err(ObjectManagerError::InvalidWrite)
                }
                Err(LookupError::NotFound | LookupError::NotReady) => {
                    Err(ObjectManagerError::NotFound)
                }
            };
        };
        validate_pending(&pending, owner, selector)?;

        let mut ticket_slot = pending.ticket.lock();
        let Some(ticket) = ticket_slot.take() else {
            return Err(ObjectManagerError::NotFound);
        };
        match self.catalog.revoke(&ticket, now) {
            Ok(()) => {
                self.remove_pending(pending_key, &pending);
                Ok(())
            }
            Err(RevokeError::ObjectGone) => {
                self.remove_pending(pending_key, &pending);
                Err(ObjectManagerError::NotFound)
            }
            Err(RevokeError::AlreadyPublished) => {
                self.remove_pending(pending_key, &pending);
                Err(ObjectManagerError::InvalidWrite)
            }
            Err(RevokeError::ForeignCatalog) => {
                *ticket_slot = Some(ticket);
                Err(ObjectManagerError::Internal)
            }
        }
    }

    pub fn get(
        &self,
        lookup: ObjectLookup<'_>,
        now: CatalogTick,
    ) -> Result<ObjectRead, LookupError> {
        self.catalog.get(lookup, now)
    }

    pub fn exists(&self, lookup: ObjectLookup<'_>, now: CatalogTick) -> bool {
        self.get(lookup, now).is_ok()
    }

    pub fn maintenance(&self, now: CatalogTick, budget: CollectBudget) -> ObjectManagerMaintenance {
        self.maintenance_with_targets(now, budget, &[])
    }

    pub(super) fn maintenance_with_targets(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        targets: &[ReclaimTarget],
    ) -> ObjectManagerMaintenance {
        let expired = {
            let mut deadlines = self.deadlines.lock();
            let mut expired = Vec::new();
            while expired.len() < budget.max_candidates {
                let Some(Reverse(deadline)) = deadlines.peek() else {
                    break;
                };
                if deadline.deadline > now {
                    break;
                }
                let Reverse(deadline) = deadlines.pop().expect("the deadline heap minimum exists");
                expired.push(deadline);
            }
            expired
        };

        let mut expired_writes = 0;
        for deadline in expired {
            let Some(pending) = self
                .pending
                .read_sync(&deadline.identity, |_, pending| Arc::clone(pending))
            else {
                continue;
            };
            if pending.id.generation() != deadline.generation {
                continue;
            }
            let mut ticket_slot = pending.ticket.lock();
            let Some(ticket) = ticket_slot.take() else {
                continue;
            };
            match self.catalog.revoke(&ticket, now) {
                Ok(()) | Err(RevokeError::ObjectGone) => {
                    expired_writes += 1;
                    self.remove_pending(&deadline.identity, &pending);
                }
                Err(RevokeError::AlreadyPublished) => {
                    self.remove_pending(&deadline.identity, &pending);
                }
                Err(RevokeError::ForeignCatalog) => {
                    *ticket_slot = Some(ticket);
                }
            }
        }

        ObjectManagerMaintenance {
            expired_writes,
            catalog: self.catalog.collect_step_with_targets(now, budget, targets),
        }
    }

    fn validate_plan(&self, plan: &ObjectPutPlan) -> Result<(), ObjectManagerError> {
        if plan.content().logical_bytes() == 0
            || plan.placement().allocation().bytes() != plan.content().logical_bytes()
            || plan.placement().replicas().count() == 0
            || !matches!(
                plan.placement().replica_class(),
                ReplicaClass::Memory | ReplicaClass::Nof
            )
        {
            return Err(ObjectManagerError::InvalidPlan);
        }
        Ok(())
    }

    fn finish_published(
        &self,
        lookup: ObjectLookup<'_>,
        owner: WriteOwner,
        selector: ReplicaSelector,
    ) -> Result<(), ObjectManagerError> {
        match self.catalog.inspect_published(lookup) {
            Ok(object) => validate_published(&object, owner, selector),
            Err(LookupError::NotFound) => Err(ObjectManagerError::NotFound),
            Err(LookupError::NotReady) => Err(ObjectManagerError::InvalidWrite),
        }
    }

    fn remove_pending<Q>(&self, pending_key: &Q, expected: &Arc<PendingPut>)
    where
        Q: Equivalent<ObjectIdentity> + Hash + ?Sized,
    {
        let _ = self.pending.remove_if_sync(pending_key, |pending| {
            pending.id == expected.id && Arc::ptr_eq(pending, expected)
        });
    }
}

fn validate_pending(
    pending: &PendingPut,
    owner: WriteOwner,
    selector: ReplicaSelector,
) -> Result<(), ObjectManagerError> {
    if pending.owner != owner {
        return Err(ObjectManagerError::IllegalOwner);
    }
    validate_selector(pending.replica_class, selector)
}

fn validate_published(
    object: &ObjectHandle,
    owner: WriteOwner,
    selector: ReplicaSelector,
) -> Result<(), ObjectManagerError> {
    if object.owner() != owner {
        return Err(ObjectManagerError::IllegalOwner);
    }
    let Some(replica) = object.replicas().first() else {
        return Err(ObjectManagerError::Internal);
    };
    let actual = replica
        .direct()
        .map(|replica| replica.replica_class())
        .unwrap_or(ReplicaClass::LocalSsd);
    if object.replicas().iter().any(|replica| {
        replica
            .direct()
            .map(|replica| replica.replica_class())
            .unwrap_or(ReplicaClass::LocalSsd)
            != actual
    }) {
        return Err(ObjectManagerError::Internal);
    }
    validate_selector(actual, selector)
}

fn validate_selector(
    actual: ReplicaClass,
    selector: ReplicaSelector,
) -> Result<(), ObjectManagerError> {
    match selector {
        ReplicaSelector::All => Ok(()),
        ReplicaSelector::Class(requested) if requested == actual => Ok(()),
        ReplicaSelector::Class(requested) => {
            Err(ObjectManagerError::ReplicaClassMismatch { requested, actual })
        }
    }
}
