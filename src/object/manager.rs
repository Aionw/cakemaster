use super::catalog::{
    ObjectCatalog, ObjectHandle, ObjectRead, ObjectWriteState, PutClaim, PutTicket, UpsertClaim,
};
use super::config::ObjectCatalogConfig;
use super::content::ObjectContent;
use super::error::{
    LookupError, ObjectCatalogConfigError, ObjectManagerError, ObjectRemoveError, PublishError,
    RevokeError,
};
use super::eviction::{MemoryEvictionConfig, MemoryEvictionController, MemoryEvictionStats};
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
    memory_eviction: Option<MemoryEvictionController>,
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
    pub memory_eviction: Option<MemoryEvictionStats>,
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
        Self::with_optional_eviction(pool, config, None)
    }

    /// Creates a manager with production memory-watermark eviction enabled.
    pub fn with_eviction_config(
        pool: Arc<SegmentPool>,
        config: ObjectCatalogConfig,
        eviction: MemoryEvictionConfig,
    ) -> Result<Self, ObjectCatalogConfigError> {
        Self::with_optional_eviction(pool, config, Some(MemoryEvictionController::new(eviction)))
    }

    fn with_optional_eviction(
        pool: Arc<SegmentPool>,
        config: ObjectCatalogConfig,
        memory_eviction: Option<MemoryEvictionController>,
    ) -> Result<Self, ObjectCatalogConfigError> {
        let observed_segment_epoch = pool.invalidation_epoch();
        Ok(Self {
            catalog: ObjectCatalog::with_config(config)?,
            allocator: ReplicaAllocator::new(pool),
            observed_segment_epoch: AtomicU64::new(observed_segment_epoch),
            memory_eviction,
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

    /// Returns production memory-eviction diagnostics when the controller is enabled.
    pub fn memory_eviction_stats(&self) -> Option<MemoryEvictionStats> {
        self.memory_eviction
            .as_ref()
            .map(MemoryEvictionController::stats)
    }

    pub(crate) fn memory_eviction_notify(&self) -> Option<Arc<tokio::sync::Notify>> {
        self.memory_eviction
            .as_ref()
            .map(MemoryEvictionController::notify)
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
        let retry_limit = self.allocation_retry_limit(plan.placement().replica_class());
        let mut retries = 0;
        loop {
            let claim = self
                .catalog
                .claim_put(identity.clone(), admission.clone(), now)?;
            match self.prepare_claimed_put(claim, plan.clone()) {
                Ok(prepared) => {
                    if retries != 0 {
                        self.record_allocation_retry_success();
                    }
                    return Ok(prepared);
                }
                Err(ObjectManagerError::NoAvailableReplicas) => {
                    let made_progress = plan.placement().replica_class() == ReplicaClass::Memory
                        && self.evict_after_allocation_failure(
                            allocation_failure_reclaim_bytes(&plan),
                            plan.placement().allocation().bytes(),
                            now,
                        );
                    if retries >= retry_limit || !made_progress {
                        return Err(ObjectManagerError::NoAvailableReplicas);
                    }
                    retries += 1;
                    self.record_allocation_retry();
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(super) fn prepare_upsert(
        &self,
        identity: ObjectIdentity,
        admission: WriteAdmission,
        plan: ObjectPutPlan,
        now: CatalogTick,
    ) -> Result<PreparedUpsert, ObjectManagerError> {
        self.validate_plan(&plan)?;
        let retry_limit = self.allocation_retry_limit(plan.placement().replica_class());
        let mut retries = 0;
        loop {
            match self.catalog.claim_upsert(
                identity.clone(),
                admission.clone(),
                plan.content(),
                now,
            )? {
                UpsertClaim::Reuse(ticket) => {
                    let started = {
                        let replicas = ticket.replicas();
                        if replicas.is_empty() || replicas.iter().any(|replica| !replica.is_live())
                        {
                            Err(ObjectManagerError::NoAvailableReplicas)
                        } else {
                            replicas
                                .first()
                                .and_then(ReplicaLease::direct)
                                .map(|replica| replica.replica_class())
                                .ok_or(ObjectManagerError::Internal)
                                .and_then(|replica_class| {
                                    started_put(replica_class, replicas.iter())
                                })
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
                    if retries != 0 {
                        self.record_allocation_retry_success();
                    }
                    return Ok(PreparedUpsert::Reused(started));
                }
                UpsertClaim::Write(claim) => match self.prepare_claimed_put(claim, plan.clone()) {
                    Ok(prepared) => {
                        if retries != 0 {
                            self.record_allocation_retry_success();
                        }
                        return Ok(PreparedUpsert::Write(prepared));
                    }
                    Err(ObjectManagerError::NoAvailableReplicas) => {
                        let made_progress = plan.placement().replica_class()
                            == ReplicaClass::Memory
                            && self.evict_after_allocation_failure(
                                allocation_failure_reclaim_bytes(&plan),
                                plan.placement().allocation().bytes(),
                                now,
                            );
                        if retries >= retry_limit || !made_progress {
                            return Err(ObjectManagerError::NoAvailableReplicas);
                        }
                        retries += 1;
                        self.record_allocation_retry();
                    }
                    Err(error) => return Err(error),
                },
            }
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
        let watermark_reclaim = self.memory_eviction.as_ref().map(|eviction| {
            eviction.prepare_step(
                self.pool().space_for(ReplicaClass::Memory),
                self.catalog.stats(),
            )
        });
        let catalog = self.catalog.collect_step_with_targets_and_watermark(
            now,
            budget,
            targets,
            watermark_reclaim,
        );
        if let Some(eviction) = &self.memory_eviction {
            let active = eviction.finish_step(
                self.pool().space_for(ReplicaClass::Memory),
                self.catalog.stats(),
                catalog,
            );
            if !active && self.catalog.stats().watermark_reclaim_debt != 0 {
                self.catalog.try_clear_watermark_reclaim();
            }
            eviction.refresh_catalog(self.catalog.stats());
        }
        ObjectManagerMaintenance {
            expired_writes: catalog.expired_pending,
            catalog,
            memory_eviction: self.memory_eviction_stats(),
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

    fn allocation_retry_limit(&self, replica_class: ReplicaClass) -> usize {
        if replica_class != ReplicaClass::Memory {
            return 0;
        }
        self.memory_eviction
            .as_ref()
            .map_or(0, |eviction| eviction.config().allocation_retry_limit())
    }

    fn evict_after_allocation_failure(
        &self,
        reclaim_bytes: u64,
        allocation_bytes: u64,
        now: CatalogTick,
    ) -> bool {
        let Some(eviction) = &self.memory_eviction else {
            return false;
        };
        eviction.record_allocation_failure();
        self.catalog.request_reclaim(reclaim_bytes);
        let before = self.pool().space_for(ReplicaClass::Memory);
        let report = self
            .maintenance(now, eviction.config().allocation_failure_budget())
            .catalog;
        let after = self.pool().space_for(ReplicaClass::Memory);
        let should_retry = !report.busy
            && after.largest_free_region_bytes >= allocation_bytes
            && (report.reclaimed_bytes != 0
                || after.used_bytes < before.used_bytes
                || after.largest_free_region_bytes > before.largest_free_region_bytes);
        log::debug!(
            target: "cakemaster::object::eviction",
            reclaim_bytes = reclaim_bytes,
            allocation_bytes = allocation_bytes,
            used_bytes_before = before.used_bytes,
            used_bytes_after = after.used_bytes,
            largest_free_region_before = before.largest_free_region_bytes,
            largest_free_region_after = after.largest_free_region_bytes,
            reclaimed_bytes = report.reclaimed_bytes,
            collector_busy = report.busy,
            should_retry = should_retry;
            "bounded allocation-failure eviction completed"
        );
        should_retry
    }

    fn record_allocation_retry(&self) {
        if let Some(eviction) = &self.memory_eviction {
            eviction.record_allocation_retry();
        }
    }

    fn record_allocation_retry_success(&self) {
        if let Some(eviction) = &self.memory_eviction {
            eviction.record_allocation_retry_success();
        }
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

fn allocation_failure_reclaim_bytes(plan: &ObjectPutPlan) -> u64 {
    let replica_count = u64::try_from(plan.placement().replicas().count()).unwrap_or(u64::MAX);
    plan.placement()
        .allocation()
        .bytes()
        .saturating_mul(replica_count)
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
