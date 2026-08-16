use super::catalog::{
    ObjectCatalog, ObjectHandle, ObjectRead, ObjectWriteState, PutClaim, PutTicket, UpsertClaim,
};
use super::config::ObjectCatalogConfig;
use super::content::ObjectContent;
use super::error::{
    LookupError, ObjectCatalogConfigError, ObjectManagerError, ObjectRemoveError, PublishError,
    RevokeError,
};
use super::identity::{NamespaceId, ObjectIdentity, ObjectLookup};
use super::reclamation::{CatalogTick, CollectBudget, CollectReport, ReclaimTarget};
use super::replica::{ReplicaId, ReplicaLease, ReplicaSet};
use super::tenant::QuotaReservationGuard;
use super::write::{ObjectCommit, WriteAdmission, WriteOwner};
use crate::segment::placement::{PlacementRequest, ReplicaAllocator};
use crate::segment::{ReplicaClass, ReservationDescriptor, SegmentPool};
use regex::Regex;
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

pub(super) enum PreparedUpsert {
    Reused(StartedPut),
    Write(PreparedPut),
}

impl PreparedPut {
    #[inline]
    pub(super) fn actual_charge_bytes(&self) -> Result<u64, ObjectManagerError> {
        let replica_count = match u64::try_from(self.replicas.len()) {
            Ok(replica_count) => replica_count,
            Err(_) => {
                log::error!(
                    target: "cakemaster::object::manager",
                    replica_count = self.replicas.len();
                    "replica count cannot be represented by object accounting"
                );
                return Err(ObjectManagerError::Internal);
            }
        };
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

    pub fn start_upsert(
        &self,
        identity: ObjectIdentity,
        admission: WriteAdmission,
        plan: ObjectPutPlan,
        now: CatalogTick,
    ) -> Result<StartedPut, ObjectManagerError> {
        match self.prepare_upsert(identity, admission, plan, now)? {
            PreparedUpsert::Reused(started) => Ok(started),
            PreparedUpsert::Write(prepared) => self.finalize_start_put(prepared, None),
        }
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
        self.prepare_claimed_put(claim, plan)
    }

    pub(super) fn prepare_upsert(
        &self,
        identity: ObjectIdentity,
        admission: WriteAdmission,
        plan: ObjectPutPlan,
        now: CatalogTick,
    ) -> Result<PreparedUpsert, ObjectManagerError> {
        self.validate_plan(&plan)?;
        match self
            .catalog
            .claim_upsert(identity, admission, plan.content(), now)?
        {
            UpsertClaim::Reuse(ticket) => {
                let started = {
                    let replicas = ticket.replicas();
                    if replicas.is_empty() || replicas.iter().any(|replica| !replica.is_live()) {
                        Err(ObjectManagerError::NoAvailableReplicas)
                    } else {
                        replicas
                            .first()
                            .and_then(ReplicaLease::direct)
                            .map(|replica| replica.replica_class())
                            .ok_or(ObjectManagerError::Internal)
                            .and_then(|replica_class| started_put(replica_class, replicas.iter()))
                    }
                };
                let started = match started {
                    Ok(started) => started,
                    Err(error) => {
                        // The catalog is already hidden in the upserting state. Restore the
                        // published generation if its reusable descriptors cannot be returned.
                        let _ = self.catalog.revoke(&ticket, now);
                        return Err(error);
                    }
                };
                Ok(PreparedUpsert::Reused(started))
            }
            UpsertClaim::Write(claim) => self
                .prepare_claimed_put(claim, plan)
                .map(PreparedUpsert::Write),
        }
    }

    fn prepare_claimed_put(
        &self,
        claim: PutClaim,
        plan: ObjectPutPlan,
    ) -> Result<PreparedPut, ObjectManagerError> {
        let reservations = self.allocator.reserve(plan.placement())?;
        let replicas = ReplicaSet::from_reservations(reservations);
        if replicas.is_empty() {
            return Err(ObjectManagerError::NoAvailableReplicas);
        }

        let replica_class = plan.placement().replica_class();
        let started = started_put(replica_class, replicas.replicas().iter())?;
        Ok(PreparedPut {
            content: plan.content(),
            claim,
            replicas,
            started,
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
            Err(error @ (PublishError::ForeignCatalog | PublishError::CommitConflict)) => {
                log::error!(
                    target: "cakemaster::object::manager",
                    namespace = lookup.namespace().get(),
                    source_error:% = error;
                    "object publication violated a catalog invariant"
                );
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
            Err(error @ RevokeError::ForeignCatalog) => {
                log::error!(
                    target: "cakemaster::object::manager",
                    namespace = lookup.namespace().get(),
                    source_error:% = error;
                    "object revocation violated a catalog invariant"
                );
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

    pub fn remove(
        &self,
        lookup: ObjectLookup<'_>,
        now: CatalogTick,
        force: bool,
    ) -> Result<(), ObjectRemoveError> {
        self.catalog
            .remove_with_force(lookup, now, force)
            .map_err(Into::into)
    }

    pub fn remove_batch<'a>(
        &self,
        lookups: impl IntoIterator<Item = ObjectLookup<'a>>,
        now: CatalogTick,
        force: bool,
    ) -> Vec<Result<(), ObjectRemoveError>> {
        lookups
            .into_iter()
            .map(|lookup| self.remove(lookup, now, force))
            .collect()
    }

    pub fn remove_matching(
        &self,
        namespace: NamespaceId,
        pattern: Option<&Regex>,
        now: CatalogTick,
        force: bool,
    ) -> usize {
        self.catalog
            .identities(namespace)
            .into_iter()
            .filter(|identity| {
                pattern.is_none_or(|pattern| pattern.is_match(identity.key().as_str()))
            })
            .filter(|identity| self.remove(identity.as_lookup(), now, force).is_ok())
            .count()
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

fn started_put<'a>(
    replica_class: ReplicaClass,
    replicas: impl IntoIterator<Item = &'a ReplicaLease>,
) -> Result<StartedPut, ObjectManagerError> {
    let replicas = replicas
        .into_iter()
        .map(|replica| {
            let Some(direct) = replica.direct() else {
                log::error!(
                    target: "cakemaster::object::manager",
                    replica_id = replica.id().get();
                    "direct placement produced a non-direct replica"
                );
                return Err(ObjectManagerError::Internal);
            };
            Ok::<AllocatedReplica, ObjectManagerError>(AllocatedReplica {
                id: direct.id(),
                descriptor: direct.owned_descriptor(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(StartedPut {
        replica_class,
        replicas,
    })
}

fn validate_pending(
    ticket: &PutTicket,
    owner: WriteOwner,
    selector: ReplicaSelector,
) -> Result<(), ObjectManagerError> {
    if ticket.owner() != owner {
        return Err(ObjectManagerError::IllegalOwner);
    }
    let replicas = ticket.replicas();
    validate_replicas(replicas.iter(), selector)
}

fn validate_published(
    object: &ObjectHandle,
    owner: WriteOwner,
    selector: ReplicaSelector,
) -> Result<(), ObjectManagerError> {
    if object.owner() != owner {
        return Err(ObjectManagerError::IllegalOwner);
    }
    let replicas = object.replicas();
    validate_replicas(replicas.iter(), selector)
}

fn validate_replicas<'a>(
    mut replicas: impl Iterator<Item = &'a ReplicaLease>,
    selector: ReplicaSelector,
) -> Result<(), ObjectManagerError> {
    let Some(replica) = replicas.next() else {
        log::error!(
            target: "cakemaster::object::manager",
            "published or pending object has no replicas"
        );
        return Err(ObjectManagerError::Internal);
    };
    let actual = replica
        .direct()
        .map(|replica| replica.replica_class())
        .unwrap_or(ReplicaClass::LocalSsd);
    if replicas.any(|replica| {
        replica
            .direct()
            .map(|replica| replica.replica_class())
            .unwrap_or(ReplicaClass::LocalSsd)
            != actual
    }) {
        log::error!(
            target: "cakemaster::object::manager",
            "object replicas have inconsistent storage classes"
        );
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
