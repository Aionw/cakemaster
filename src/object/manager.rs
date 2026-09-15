use super::catalog::{
    ObjectCatalog, ObjectHandle, ObjectRead, WriteClaim, WriteResolution, WriteTransaction,
};
use super::config::ObjectCatalogConfig;
use super::config::{ObjectPinRequest, ResolvedObjectPinRequest};
use super::content::ObjectContent;
use super::error::{
    AbortError, CommitError, LookupError, ObjectCatalogConfigError, ObjectManagerError,
    ObjectRemoveError,
};
use super::eviction::{MemoryEvictionConfig, MemoryEvictionController, MemoryEvictionStats};
use super::identity::{NamespaceId, ObjectIdentity, ObjectLookup};
use super::reclamation::{CatalogTick, CollectBudget, CollectReport, ReclaimTarget};
use super::replica::{ReplicaId, ReplicaLease, ReplicaSet};
use super::tenant::QuotaReservationGuard;
use super::write::{ObjectCommit, WriteAdmission, WriteMode, WriteOwner};
use crate::segment::placement::{
    FulfillmentPolicy, PlacementError, PlacementRequest, ReplicaAllocator,
};
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
    pins: ObjectPinRequest,
}

impl ObjectPutPlan {
    pub const fn new(content: ObjectContent, placement: PlacementRequest) -> Self {
        Self {
            content,
            placement,
            pins: ObjectPinRequest::new(super::config::SoftPinAction::Preserve, None, false),
        }
    }

    pub const fn content(&self) -> ObjectContent {
        self.content
    }

    pub const fn placement(&self) -> &PlacementRequest {
        &self.placement
    }

    pub const fn with_pins(mut self, pins: ObjectPinRequest) -> Self {
        self.pins = pins;
        self
    }

    pub const fn pins(&self) -> ObjectPinRequest {
        self.pins
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
    claim: WriteClaim,
    replicas: ReplicaSet,
    started: StartedPut,
}

struct AllocationShortfall {
    reclaim_bytes: u64,
    allocation_bytes: u64,
}

enum PrepareClaimedPutError {
    Allocation(AllocationShortfall),
    Manager(ObjectManagerError),
}

struct AllocationEviction {
    made_progress: bool,
    generation: u64,
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
        let prepared = self.prepare_upsert(identity, admission, plan, now)?;
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
        let pins = self.validate_plan(&plan)?;
        self.prepare_write(identity, admission, plan, WriteMode::Insert, pins, now)
    }

    pub(super) fn prepare_upsert(
        &self,
        identity: ObjectIdentity,
        admission: WriteAdmission,
        plan: ObjectPutPlan,
        now: CatalogTick,
    ) -> Result<PreparedPut, ObjectManagerError> {
        let pins = self.validate_plan(&plan)?;
        self.prepare_write(identity, admission, plan, WriteMode::Upsert, pins, now)
    }

    fn prepare_write(
        &self,
        identity: ObjectIdentity,
        admission: WriteAdmission,
        plan: ObjectPutPlan,
        mode: WriteMode,
        pins: ResolvedObjectPinRequest,
        now: CatalogTick,
    ) -> Result<PreparedPut, ObjectManagerError> {
        let retry_limit = self.allocation_retry_limit();
        let mut retries = 0;
        let mut allocation_generation = None;
        loop {
            let claim = match self.catalog.begin_write_resolved(
                identity.clone(),
                admission.clone(),
                mode,
                pins,
                now,
            ) {
                Ok(claim) => claim,
                Err(error) => {
                    self.clear_allocation_reclaim(allocation_generation);
                    return Err(error.into());
                }
            };
            match self.prepare_claimed_put(claim, plan.clone()) {
                Ok(prepared) => {
                    self.clear_allocation_reclaim(allocation_generation);
                    if retries != 0 {
                        self.record_allocation_retry_success();
                    }
                    return Ok(prepared);
                }
                Err(PrepareClaimedPutError::Allocation(shortfall)) => {
                    let eviction = (plan.placement().replica_class() == ReplicaClass::Memory)
                        .then(|| self.evict_after_allocation_failure(shortfall, now))
                        .flatten();
                    let made_progress = eviction
                        .as_ref()
                        .is_some_and(|eviction| eviction.made_progress);
                    if let Some(eviction) = eviction {
                        allocation_generation = Some(eviction.generation);
                    }
                    if retries >= retry_limit || !made_progress {
                        return Err(ObjectManagerError::NoAvailableReplicas);
                    }
                    retries += 1;
                    self.record_allocation_retry();
                }
                Err(PrepareClaimedPutError::Manager(error)) => {
                    self.clear_allocation_reclaim(allocation_generation);
                    return Err(error);
                }
            }
        }
    }

    fn prepare_claimed_put(
        &self,
        claim: WriteClaim,
        plan: ObjectPutPlan,
    ) -> Result<PreparedPut, PrepareClaimedPutError> {
        let reservations = match self.allocator.reserve(plan.placement()) {
            Ok(reservations) => reservations,
            Err(PlacementError::InsufficientReplicas {
                requested,
                allocated,
            }) => {
                return Err(PrepareClaimedPutError::Allocation(allocation_shortfall(
                    &plan, requested, allocated,
                )));
            }
            Err(error) => return Err(PrepareClaimedPutError::Manager(error.into())),
        };
        let replicas = ReplicaSet::from_reservations(reservations);
        if replicas.is_empty() {
            return Err(PrepareClaimedPutError::Allocation(allocation_shortfall(
                &plan,
                plan.placement().replicas().count(),
                0,
            )));
        }

        let replica_class = plan.placement().replica_class();
        let started = started_put(replica_class, replicas.replicas().iter());
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
        self.finish_put_at(identity, owner, selector, CatalogTick::ZERO)
    }

    /// Finishes a write using the supplied catalog time for commit-time pin changes.
    pub fn finish_put_at(
        &self,
        identity: &ObjectIdentity,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Result<(), ObjectManagerError> {
        self.finish_put_lookup_at(identity.as_lookup(), owner, selector, now)
    }

    pub(super) fn finish_put_lookup(
        &self,
        lookup: ObjectLookup<'_>,
        owner: WriteOwner,
        selector: ReplicaSelector,
    ) -> Result<(), ObjectManagerError> {
        self.finish_put_lookup_at(lookup, owner, selector, CatalogTick::ZERO)
    }

    pub(super) fn finish_put_lookup_at(
        &self,
        lookup: ObjectLookup<'_>,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Result<(), ObjectManagerError> {
        let write = match self.catalog.resolve_write(lookup) {
            Ok(write) => write,
            Err(LookupError::NotFound) => return Err(ObjectManagerError::NotFound),
            Err(LookupError::NotReady) => return Err(ObjectManagerError::InvalidWrite),
        };
        let ticket = match write {
            WriteResolution::Active(transaction) => transaction,
            WriteResolution::Committed(object) => {
                return validate_published(&object, owner, selector);
            }
        };
        validate_pending(&ticket, owner, selector)?;
        match self.catalog.commit(&ticket, ObjectCommit::new(None), now) {
            Ok(_) => Ok(()),
            Err(CommitError::TransactionGone | CommitError::NotStaged) => {
                Err(ObjectManagerError::NotFound)
            }
            Err(CommitError::ReplicasInvalidated) => Err(ObjectManagerError::NoAvailableReplicas),
            Err(error @ (CommitError::ForeignCatalog | CommitError::CommitConflict)) => {
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
        let write = match self.catalog.resolve_write(lookup) {
            Ok(write) => write,
            Err(LookupError::NotFound | LookupError::NotReady) => {
                return Err(ObjectManagerError::NotFound);
            }
        };
        let ticket = match write {
            WriteResolution::Active(transaction) => transaction,
            WriteResolution::Committed(object) => {
                validate_published(&object, owner, selector)?;
                return Err(ObjectManagerError::InvalidWrite);
            }
        };
        validate_pending(&ticket, owner, selector)?;
        match self.catalog.abort(&ticket, now) {
            Ok(()) => Ok(()),
            Err(AbortError::TransactionGone) => Err(ObjectManagerError::NotFound),
            Err(AbortError::AlreadyCommitted) => Err(ObjectManagerError::InvalidWrite),
            Err(error @ AbortError::ForeignCatalog) => {
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
        let catalog = match &self.memory_eviction {
            Some(eviction) => {
                let report = self.catalog.collect_step_with_targets_and_watermark(
                    now,
                    budget,
                    targets,
                    || {
                        eviction.prepare_step(
                            self.pool().space_for(ReplicaClass::Memory),
                            self.catalog.stats(),
                        )
                    },
                    |report| {
                        eviction.finish_step(
                            self.pool().space_for(ReplicaClass::Memory),
                            self.catalog.stats(),
                            report,
                        );
                    },
                );
                if report.busy {
                    eviction.record_busy_step();
                }
                report
            }
            None => self.catalog.collect_step_with_targets(now, budget, targets),
        };
        ObjectManagerMaintenance {
            expired_writes: catalog.expired_pending,
            catalog,
            memory_eviction: self.memory_eviction_stats(),
        }
    }

    fn validate_plan(
        &self,
        plan: &ObjectPutPlan,
    ) -> Result<ResolvedObjectPinRequest, ObjectManagerError> {
        if plan.content().logical_bytes() == 0
            || plan.placement().allocation().bytes() != plan.content().logical_bytes()
            || plan.placement().replicas().count() == 0
        {
            return Err(ObjectManagerError::InvalidPlan);
        }
        self.catalog
            .resolve_pin_request(plan.pins())
            .map_err(|_| ObjectManagerError::InvalidPlan)
    }

    fn allocation_retry_limit(&self) -> usize {
        self.memory_eviction
            .as_ref()
            .map_or(0, |eviction| eviction.config().allocation_retry_limit())
    }

    fn evict_after_allocation_failure(
        &self,
        shortfall: AllocationShortfall,
        now: CatalogTick,
    ) -> Option<AllocationEviction> {
        let Some(eviction) = &self.memory_eviction else {
            return None;
        };
        let generation = eviction.request_allocation_reclaim(shortfall.reclaim_bytes);
        let before = self.pool().space_for(ReplicaClass::Memory);
        let report = self
            .maintenance(now, eviction.config().allocation_failure_budget())
            .catalog;
        let after = self.pool().space_for(ReplicaClass::Memory);
        let should_retry = !report.busy
            && after.largest_free_region_bytes >= shortfall.allocation_bytes
            && (report.reclaimed_memory_bytes != 0
                || after.used_bytes < before.used_bytes
                || after.largest_free_region_bytes > before.largest_free_region_bytes);
        log::debug!(
            target: "cakemaster::object::eviction",
            reclaim_bytes = shortfall.reclaim_bytes,
            allocation_bytes = shortfall.allocation_bytes,
            used_bytes_before = before.used_bytes,
            used_bytes_after = after.used_bytes,
            largest_free_region_before = before.largest_free_region_bytes,
            largest_free_region_after = after.largest_free_region_bytes,
            reclaimed_bytes = report.reclaimed_bytes,
            collector_busy = report.busy,
            should_retry = should_retry;
            "bounded allocation-failure eviction completed"
        );
        Some(AllocationEviction {
            made_progress: should_retry,
            generation,
        })
    }

    fn clear_allocation_reclaim(&self, generation: Option<u64>) {
        if let (Some(eviction), Some(generation)) = (&self.memory_eviction, generation) {
            eviction.clear_allocation_reclaim(generation);
        }
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
) -> StartedPut {
    let replicas = replicas
        .into_iter()
        .map(|replica| AllocatedReplica {
            id: replica.id(),
            descriptor: replica.owned_descriptor(),
        })
        .collect();
    StartedPut {
        replica_class,
        replicas,
    }
}

fn allocation_shortfall(
    plan: &ObjectPutPlan,
    requested_replicas: usize,
    allocated_replicas: usize,
) -> AllocationShortfall {
    let missing_replicas = match plan.placement().fulfillment() {
        FulfillmentPolicy::AllOrNothing => requested_replicas.saturating_sub(allocated_replicas),
        FulfillmentPolicy::BestEffort => 1,
    };
    let replica_count = u64::try_from(missing_replicas).unwrap_or(u64::MAX);
    let allocation_bytes = plan.placement().allocation().bytes();
    AllocationShortfall {
        allocation_bytes,
        reclaim_bytes: allocation_bytes.saturating_mul(replica_count),
    }
}

fn validate_pending(
    ticket: &WriteTransaction,
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
    let actual = replica.replica_class();
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
