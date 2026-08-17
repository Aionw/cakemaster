//! Immutable object version data and the narrowly scoped mutable state owned
//! by a committed version.

use super::*;

impl ObjectVersion {
    pub(super) fn pending(origin: TransactionId, owner: WriteOwner, record: ObjectRecord) -> Self {
        Self {
            origin,
            owner,
            record,
            committed: OnceLock::new(),
        }
    }

    pub(super) fn record(&self) -> &ObjectRecord {
        &self.record
    }

    pub(super) fn initialize_commit(
        &self,
        id: VersionId,
        commit: ObjectCommit,
        access: AccessControl,
    ) {
        assert!(
            self.committed
                .set(CommittedVersion { id, commit, access })
                .is_ok(),
            "a pending version is committed once"
        );
    }

    fn committed(&self) -> &CommittedVersion {
        self.committed
            .get()
            .expect("published versions have commit metadata")
    }

    pub(super) fn is_committed(&self) -> bool {
        self.committed.get().is_some()
    }

    pub(super) fn id(&self) -> VersionId {
        self.committed().id
    }

    pub(super) fn commit(&self) -> ObjectCommit {
        self.committed().commit
    }

    pub(super) fn access(&self) -> &AccessControl {
        &self.committed().access
    }
}

impl ObjectRecord {
    pub(super) fn commit_accounting(&self) {
        if let Some((charge, class, bytes)) = self.tenant_accounting() {
            charge.commit(class, bytes);
        }
    }

    pub(super) fn abort_accounting(&self) {
        if let Some((charge, class, bytes)) = self.tenant_accounting() {
            charge.abort(class, bytes);
        }
    }

    pub(super) fn mark_accounting_retiring(&self) {
        if let Some((charge, class, bytes)) = self.tenant_accounting() {
            charge.mark_retiring(class, bytes);
        }
    }

    pub(super) fn release_accounting(&self) {
        if let Some((charge, class, bytes)) = self.tenant_accounting() {
            charge.release(class, bytes);
        }
    }

    pub(super) fn release_pruned_accounting(&self, stale: &ReplicaSet) {
        let Some(charge) = self.accounting.as_ref() else {
            return;
        };
        let Some(replica_class) = Self::direct_replica_class(stale) else {
            return;
        };
        let replica_count = u64::try_from(stale.len())
            .expect("replica count was representable when the object was staged");
        let bytes = self
            .content
            .logical_bytes()
            .checked_mul(replica_count)
            .expect("tenant replica charge was representable when the object was staged");
        charge.release_committed_partial(replica_class, bytes);
    }

    pub(super) fn tenant_accounting(
        &self,
    ) -> Option<(&TenantQuotaCharge, crate::segment::ReplicaClass, u64)> {
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

    pub(super) fn current_direct_replica_class(&self) -> Option<crate::segment::ReplicaClass> {
        Self::direct_replica_class(&self.replicas.read())
    }

    pub(super) fn is_accounted(&self) -> bool {
        self.accounting.is_some()
    }

    pub(super) fn reserved_bytes(&self) -> u64 {
        self.replicas.reserved_bytes()
    }
}

impl Drop for ObjectRecord {
    fn drop(&mut self) {
        self.release_accounting();
    }
}

impl ReplicaStorage {
    pub(super) fn new(replicas: ReplicaSet) -> Self {
        let reserved_bytes = replicas.reserved_bytes();
        Self {
            set: RwLock::new(replicas),
            reserved_bytes: AtomicU64::new(reserved_bytes),
        }
    }

    pub(super) fn read(&self) -> RwLockReadGuard<'_, ReplicaSet> {
        self.set.read()
    }

    pub(super) fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes.load(Ordering::Relaxed)
    }

    pub(super) fn prune_invalidated(&self) -> ReplicaPrune {
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

    pub(super) fn take_exclusive(&mut self) -> ReplicaSet {
        self.reserved_bytes.store(0, Ordering::Relaxed);
        std::mem::take(self.set.get_mut())
    }
}

impl AccessControl {
    pub(super) fn for_commit(
        base: Option<&ObjectVersion>,
        pins: ResolvedObjectPinRequest,
        now: CatalogTick,
    ) -> Self {
        let inherited_soft_pin = base.and_then(|version| version.access().soft_pin_until());
        let soft_pin_until = match pins.soft_pin.action {
            SoftPinAction::Preserve => inherited_soft_pin
                .filter(|deadline| *deadline > now)
                .map_or(0, CatalogTick::get),
            SoftPinAction::Enable if pins.soft_pin.ttl_ticks != 0 => {
                now.saturating_add(pins.soft_pin.ttl_ticks).get()
            }
            SoftPinAction::Enable | SoftPinAction::Disable => 0,
        };
        Self {
            lease_until: AtomicU64::new(0),
            recent: AtomicBool::new(false),
            soft_pin_until: AtomicU64::new(soft_pin_until),
            hard_pinned: base.is_some_and(|version| version.access().hard_pinned())
                || pins.with_hard_pin,
        }
    }

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
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return CatalogTick::new(desired),
                Err(observed) => current = observed,
            }
        }
        CatalogTick::new(current)
    }

    pub(super) fn record_access(
        &self,
        now: CatalogTick,
        lease_ttl_ticks: u64,
        lease_refresh_ticks: u64,
    ) -> CatalogTick {
        let lease_until = self.acquire_lease(now, lease_ttl_ticks, lease_refresh_ticks);
        self.recent.store(true, Ordering::Relaxed);
        lease_until
    }

    pub(super) fn take_recent(&self) -> bool {
        self.recent.swap(false, Ordering::Relaxed)
    }

    pub(super) fn lease_until(&self) -> CatalogTick {
        CatalogTick::new(self.lease_until.load(Ordering::Acquire))
    }

    pub(super) fn is_leased(&self, now: CatalogTick) -> bool {
        self.lease_until() > now
    }

    pub(super) fn hard_pinned(&self) -> bool {
        self.hard_pinned
    }

    pub(super) fn soft_pin_until(&self) -> Option<CatalogTick> {
        match self.soft_pin_until.load(Ordering::Acquire) {
            0 => None,
            deadline => Some(CatalogTick::new(deadline)),
        }
    }

    pub(super) fn is_soft_pinned(&self, now: CatalogTick) -> bool {
        self.soft_pin_until().is_some_and(|deadline| deadline > now)
    }

    pub(super) fn expire_soft_pin(&self, expected: CatalogTick, now: CatalogTick) -> bool {
        expected <= now
            && self
                .soft_pin_until
                .compare_exchange(expected.get(), 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }
}

impl<'a> ReplicaSetView<'a> {
    pub(super) fn new(guard: RwLockReadGuard<'a, ReplicaSet>) -> Self {
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
    pub(super) fn new(guard: RwLockReadGuard<'a, ReplicaSet>) -> Self {
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
