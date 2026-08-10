use super::catalog::{ObjectCatalog, ObjectHandle, ObjectRead, PutTicket};
use super::config::ObjectCatalogConfig;
use super::content::ObjectContent;
use super::error::{
    LookupError, ObjectCatalogConfigError, ObjectManagerError, PublishError, PutError, RevokeError,
    StageError,
};
use super::identity::{ObjectIdentity, ObjectLookup};
use super::reclamation::{CatalogTick, CollectBudget, CollectReport};
use super::replica::{ReplicaId, ReplicaSet};
use super::write::{ObjectCommit, WriteId, WriteOwner};
use crate::segment::error::ReserveError;
use crate::segment::placement::{PlacementError, PlacementRequest, ReplicaAllocator};
use crate::segment::{ReplicaClass, ReservationDescriptor, SegmentPool};
use parking_lot::Mutex;
use scc::HashMap;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
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
        self.validate_plan(&plan)?;
        let claim = self
            .catalog
            .claim_put(identity.clone(), owner, now)
            .map_err(map_put_error)?;
        let reservations = self
            .allocator
            .reserve(plan.placement())
            .map_err(map_placement_error)?;
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
        let ticket = claim
            .stage(plan.content(), replicas)
            .map_err(map_stage_error)?;
        let id = ticket.id();
        let pending = Arc::new(PendingPut {
            owner,
            id,
            replica_class,
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

        Ok(StartedPut {
            replica_class,
            replicas: allocated,
        })
    }

    pub fn finish_put(
        &self,
        identity: &ObjectIdentity,
        owner: WriteOwner,
        selector: ReplicaSelector,
    ) -> Result<(), ObjectManagerError> {
        let Some(pending) = self
            .pending
            .read_sync(identity, |_, pending| Arc::clone(pending))
        else {
            return self.finish_published(identity.as_lookup(), owner, selector);
        };
        validate_pending(&pending, owner, selector)?;

        let mut ticket_slot = pending.ticket.lock();
        let Some(ticket) = ticket_slot.take() else {
            drop(ticket_slot);
            return self.finish_published(identity.as_lookup(), owner, selector);
        };
        match self.catalog.publish(&ticket, ObjectCommit::new(None)) {
            Ok(_) => {
                self.remove_pending(identity, &pending);
                Ok(())
            }
            Err(PublishError::ObjectGone | PublishError::NotPending) => {
                self.remove_pending(identity, &pending);
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
        let Some(pending) = self
            .pending
            .read_sync(identity, |_, pending| Arc::clone(pending))
        else {
            return match self.catalog.inspect_published(identity.as_lookup()) {
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
                self.remove_pending(identity, &pending);
                Ok(())
            }
            Err(RevokeError::ObjectGone) => {
                self.remove_pending(identity, &pending);
                Err(ObjectManagerError::NotFound)
            }
            Err(RevokeError::AlreadyPublished) => {
                self.remove_pending(identity, &pending);
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
            catalog: self.catalog.collect_step(now, budget),
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

    fn remove_pending(&self, identity: &ObjectIdentity, expected: &Arc<PendingPut>) {
        let _ = self.pending.remove_if_sync(identity, |pending| {
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

fn map_put_error(error: PutError) -> ObjectManagerError {
    match error {
        PutError::EmptyKey => ObjectManagerError::InvalidPlan,
        PutError::AlreadyExists | PutError::WriteInProgress => ObjectManagerError::AlreadyExists,
        PutError::ReclamationBacklog => ObjectManagerError::NoAvailableReplicas,
    }
}

fn map_placement_error(error: PlacementError) -> ObjectManagerError {
    match error {
        PlacementError::ZeroSize | PlacementError::ZeroReplicas => ObjectManagerError::InvalidPlan,
        PlacementError::InsufficientReplicas { .. } => ObjectManagerError::NoAvailableReplicas,
        PlacementError::Reserve(
            ReserveError::NotAccepting(_) | ReserveError::OutOfSpace(_) | ReserveError::NotFound(_),
        ) => ObjectManagerError::NoAvailableReplicas,
        PlacementError::Reserve(ReserveError::ZeroSize) => ObjectManagerError::InvalidPlan,
        PlacementError::Reserve(
            ReserveError::ForeignCandidate
            | ReserveError::NotDirectlyAllocatable(_)
            | ReserveError::AddressOverflow(_),
        ) => ObjectManagerError::Internal,
    }
}

fn map_stage_error(error: StageError) -> ObjectManagerError {
    match error {
        StageError::ZeroSize => ObjectManagerError::InvalidPlan,
        StageError::NoReplicas => ObjectManagerError::NoAvailableReplicas,
        StageError::CatalogDropped | StageError::ClaimLost | StageError::ReplicaTooSmall { .. } => {
            ObjectManagerError::Internal
        }
    }
}
