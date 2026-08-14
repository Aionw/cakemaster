use super::catalog::{
    ObjectCatalog, ObjectHandle, ObjectRead, ObjectWriteState, PutClaim, PutTicket,
};
use super::config::ObjectCatalogConfig;
use super::content::ObjectContent;
use super::error::{
    LookupError, ObjectCatalogConfigError, ObjectManagerError, PublishError, RevokeError,
};
use super::identity::{ObjectIdentity, ObjectLookup};
use super::reclamation::{CatalogTick, CollectBudget, CollectReport, ReclaimTarget};
use super::replica::{ReplicaId, ReplicaLease, ReplicaSet};
use super::tenant::QuotaReservationGuard;
use super::write::{ObjectCommit, WriteAdmission, WriteOwner};
use crate::segment::placement::{PlacementRequest, ReplicaAllocator};
use crate::segment::{ReplicaClass, ReservationDescriptor, SegmentPool};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct ObjectManager {
    catalog: ObjectCatalog,
    allocator: ReplicaAllocator,
    observed_segment_epoch: AtomicU64,
}

/// Narrow capability for revoking pending writes after their sessions fence.
#[derive(Clone)]
pub struct PendingWriteRevoker {
    catalog: ObjectCatalog,
}

impl PendingWriteRevoker {
    /// Revokes pending writes for multiple fenced sessions with one catalog
    /// queue pass.
    pub(crate) fn revoke_sessions(
        &self,
        sessions: impl IntoIterator<Item = crate::client::ClientSession>,
        now: CatalogTick,
    ) -> usize {
        self.catalog
            .revoke_pending_owners(sessions.into_iter().map(WriteOwner::for_session), now)
    }
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
        let observed_segment_epoch = pool.invalidation_epoch();
        Ok(Self {
            catalog: ObjectCatalog::with_config(config)?,
            allocator: ReplicaAllocator::new(pool),
            observed_segment_epoch: AtomicU64::new(observed_segment_epoch),
        })
    }

    pub const fn catalog(&self) -> &ObjectCatalog {
        &self.catalog
    }

    pub fn pending_write_revoker(&self) -> PendingWriteRevoker {
        PendingWriteRevoker {
            catalog: self.catalog.clone(),
        }
    }

    pub fn pool(&self) -> &Arc<SegmentPool> {
        self.allocator.pool()
    }

    pub fn start_put(
        &self,
        identity: ObjectIdentity,
        admission: WriteAdmission,
        plan: ObjectPutPlan,
        now: CatalogTick,
    ) -> Result<StartedPut, ObjectManagerError> {
        let prepared = self.prepare_put(identity, admission, plan, now)?;
        self.finalize_start_put(prepared, None)
    }

    #[inline]
    pub(super) fn prepare_put(
        &self,
        identity: ObjectIdentity,
        admission: WriteAdmission,
        plan: ObjectPutPlan,
        now: CatalogTick,
    ) -> Result<PreparedPut, ObjectManagerError> {
        self.validate_plan(&plan)?;
        let claim = self.catalog.claim_put(identity, admission, now)?;
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
    ) -> Result<StartedPut, ObjectManagerError> {
        let PreparedPut {
            content,
            claim,
            replicas,
            started,
        } = prepared;
        match accounting {
            Some(reservation) => claim.stage_accounted(content, replicas, reservation)?,
            None => claim.stage(content, replicas)?,
        };
        Ok(started)
    }

    pub fn finish_put(
        &self,
        identity: &ObjectIdentity,
        owner: WriteOwner,
        selector: ReplicaSelector,
    ) -> Result<(), ObjectManagerError> {
        self.finish_put_lookup(identity.as_lookup(), owner, selector)
    }

    pub(super) fn finish_put_lookup(
        &self,
        lookup: ObjectLookup<'_>,
        owner: WriteOwner,
        selector: ReplicaSelector,
    ) -> Result<(), ObjectManagerError> {
        let write = match self.catalog.inspect_write(lookup) {
            Ok(write) => write,
            Err(LookupError::NotFound) => return Err(ObjectManagerError::NotFound),
            Err(LookupError::NotReady) => return Err(ObjectManagerError::InvalidWrite),
        };
        let ticket = match write {
            ObjectWriteState::Pending(ticket) => ticket,
            ObjectWriteState::Published(object) => {
                return validate_published(&object, owner, selector);
            }
        };
        validate_pending(&ticket, owner, selector)?;
        match self.catalog.publish(&ticket, ObjectCommit::new(None)) {
            Ok(_) => Ok(()),
            Err(PublishError::ObjectGone | PublishError::NotPending) => {
                Err(ObjectManagerError::NotFound)
            }
            Err(PublishError::ReplicasInvalidated) => Err(ObjectManagerError::NoAvailableReplicas),
            Err(PublishError::PublicationInProgress) => Err(ObjectManagerError::InvalidWrite),
            Err(PublishError::ForeignCatalog | PublishError::CommitConflict) => {
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
        self.revoke_put_lookup(identity.as_lookup(), owner, selector, now)
    }

    pub(super) fn revoke_put_lookup(
        &self,
        lookup: ObjectLookup<'_>,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Result<(), ObjectManagerError> {
        let write = match self.catalog.inspect_write(lookup) {
            Ok(write) => write,
            Err(LookupError::NotFound | LookupError::NotReady) => {
                return Err(ObjectManagerError::NotFound);
            }
        };
        let ticket = match write {
            ObjectWriteState::Pending(ticket) => ticket,
            ObjectWriteState::Published(object) => {
                validate_published(&object, owner, selector)?;
                return Err(ObjectManagerError::InvalidWrite);
            }
        };
        validate_pending(&ticket, owner, selector)?;
        match self.catalog.revoke(&ticket, now) {
            Ok(()) => Ok(()),
            Err(RevokeError::ObjectGone) => Err(ObjectManagerError::NotFound),
            Err(RevokeError::AlreadyPublished) => Err(ObjectManagerError::InvalidWrite),
            Err(RevokeError::ForeignCatalog) => Err(ObjectManagerError::Internal),
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
        let segment_epoch = self.pool().invalidation_epoch();
        if self
            .observed_segment_epoch
            .swap(segment_epoch, Ordering::AcqRel)
            != segment_epoch
        {
            self.catalog.request_liveness_scan();
        }
        let catalog = self.catalog.collect_step_with_targets(now, budget, targets);
        ObjectManagerMaintenance {
            expired_writes: catalog.expired_pending,
            catalog,
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
}

fn validate_pending(
    ticket: &PutTicket,
    owner: WriteOwner,
    selector: ReplicaSelector,
) -> Result<(), ObjectManagerError> {
    if ticket.owner() != owner {
        return Err(ObjectManagerError::IllegalOwner);
    }
    validate_replicas(ticket.replicas(), selector)
}

fn validate_published(
    object: &ObjectHandle,
    owner: WriteOwner,
    selector: ReplicaSelector,
) -> Result<(), ObjectManagerError> {
    if object.owner() != owner {
        return Err(ObjectManagerError::IllegalOwner);
    }
    validate_replicas(object.replicas(), selector)
}

fn validate_replicas(
    replicas: &[ReplicaLease],
    selector: ReplicaSelector,
) -> Result<(), ObjectManagerError> {
    let Some(replica) = replicas.first() else {
        return Err(ObjectManagerError::Internal);
    };
    let actual = replica
        .direct()
        .map(|replica| replica.replica_class())
        .unwrap_or(ReplicaClass::LocalSsd);
    if replicas.iter().any(|replica| {
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
