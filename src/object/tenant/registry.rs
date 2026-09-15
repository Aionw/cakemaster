use super::super::identity::NamespaceId;
use super::super::reclamation::{ReclaimFilter, ReclaimTarget};
use super::quota::{QuotaAccount, distribute_effective_quota};
use super::{
    TenantAdminError, TenantConfig, TenantConfigError, TenantId, TenantObjectError, TenantPolicy,
    TenantResourceClass, TenantSnapshot,
};
use crate::segment::{ReplicaClass, SegmentPool};
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const TENANT_REGISTERED: u8 = 0;
const TENANT_CLOSING: u8 = 1;
const TENANT_REMOVED: u8 = 2;
const TENANT_STATE_BITS: u32 = 2;
const TENANT_STATE_MASK: u64 = (1 << TENANT_STATE_BITS) - 1;
const TENANT_MAX_GENERATION: u64 = u64::MAX >> TENANT_STATE_BITS;

static NEXT_TENANT_MANAGER_ID: AtomicU64 = AtomicU64::new(1);

/// Opaque, manager-bound resolution result. Resolving once per request batch
/// avoids repeated string hashing and directory lookups on the item path.
#[derive(Clone)]
pub struct ResolvedTenant {
    pub(super) manager_id: u64,
    pub(super) namespace: NamespaceId,
    pub(super) version: u64,
    pub(super) entry: Option<Arc<TenantEntry>>,
}

impl ResolvedTenant {
    pub const fn namespace(&self) -> NamespaceId {
        self.namespace
    }
}

impl fmt::Debug for ResolvedTenant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedTenant")
            .field("namespace", &self.namespace)
            .field("generation", &tenant_generation(self.version))
            .finish_non_exhaustive()
    }
}

struct TenantDirectory {
    entries: HashMap<TenantId, Arc<TenantEntry>>,
}

pub(super) struct TenantRegistry {
    manager_id: u64,
    single: bool,
    directory: ArcSwap<TenantDirectory>,
    update: Mutex<()>,
    next_namespace: AtomicU64,
    pool: Arc<SegmentPool>,
    capacity_generation: AtomicU64,
    // Low bit means scoped reclaim may be needed; upper bits form a revision
    // so a collector cannot clear a newer writer's hint.
    reclaim_state: AtomicU64,
}

pub(super) struct TenantEntry {
    pub(super) id: TenantId,
    pub(super) namespace: NamespaceId,
    pub(super) version: AtomicU64,
    pub(super) memory: QuotaAccount,
}

impl TenantRegistry {
    pub(super) fn new(
        pool: Arc<SegmentPool>,
        config: TenantConfig,
    ) -> Result<Self, TenantConfigError> {
        let manager_id = NEXT_TENANT_MANAGER_ID.fetch_add(1, Ordering::Relaxed);
        let (single, initial_policies) = match config {
            TenantConfig::Single => (true, Vec::new()),
            TenantConfig::Multi { initial_policies } => (false, initial_policies),
        };
        let mut entries = HashMap::with_capacity(initial_policies.len());
        let mut next_namespace = 1_u64;
        for (id, policy) in initial_policies {
            if entries.contains_key(&id) {
                return Err(TenantConfigError::DuplicateTenant(id));
            }
            let namespace = NamespaceId::new(next_namespace);
            next_namespace = next_namespace
                .checked_add(1)
                .ok_or(TenantConfigError::NamespaceExhausted)?;
            entries.insert(
                id.clone(),
                Arc::new(TenantEntry::new(id, namespace, policy, TENANT_REGISTERED)),
            );
        }
        let registry = Self {
            manager_id,
            single,
            directory: ArcSwap::from_pointee(TenantDirectory { entries }),
            update: Mutex::new(()),
            next_namespace: AtomicU64::new(next_namespace),
            pool,
            capacity_generation: AtomicU64::new(u64::MAX),
            reclaim_state: AtomicU64::new(0),
        };
        registry.refresh_capacity();
        Ok(registry)
    }

    pub(super) fn resolve(&self, id: &TenantId) -> Result<ResolvedTenant, TenantObjectError> {
        if self.single {
            return Ok(ResolvedTenant {
                manager_id: self.manager_id,
                namespace: NamespaceId::DEFAULT,
                version: tenant_version(0, TENANT_REGISTERED),
                entry: None,
            });
        }
        let directory = self.directory.load();
        let entry = directory
            .entries
            .get(id)
            .cloned()
            .ok_or(TenantObjectError::TenantNotRegistered)?;
        let version = entry.version.load(Ordering::Acquire);
        if tenant_state(version) != TENANT_REGISTERED {
            return Err(TenantObjectError::TenantNotRegistered);
        }
        Ok(ResolvedTenant {
            manager_id: self.manager_id,
            namespace: entry.namespace,
            version,
            entry: Some(entry),
        })
    }

    #[inline]
    pub(super) fn validate(&self, tenant: &ResolvedTenant) -> Result<(), TenantObjectError> {
        self.validate_binding(tenant)?;
        let Some(entry) = &tenant.entry else {
            return Ok(());
        };
        if entry.version.load(Ordering::Acquire) != tenant.version {
            return Err(TenantObjectError::InvalidTenantHandle);
        }
        Ok(())
    }

    #[inline]
    pub(super) fn validate_binding(
        &self,
        tenant: &ResolvedTenant,
    ) -> Result<(), TenantObjectError> {
        if tenant.manager_id != self.manager_id {
            return Err(TenantObjectError::InvalidTenantHandle);
        }
        debug_assert_eq!(tenant.entry.is_none(), self.single);
        if let Some(entry) = &tenant.entry {
            debug_assert_eq!(entry.namespace, tenant.namespace);
        }
        Ok(())
    }

    pub(super) fn upsert(
        &self,
        id: TenantId,
        policy: TenantPolicy,
    ) -> Result<ResolvedTenant, TenantAdminError> {
        if self.single {
            return Err(TenantAdminError::SingleTenantMode);
        }
        let _update = self.update.lock();
        let current = self.directory.load_full();
        let mut publish_version = None;
        let entry = if let Some(entry) = current.entries.get(&id) {
            let version = entry.version.load(Ordering::Acquire);
            match tenant_state(version) {
                TENANT_REGISTERED => {}
                TENANT_REMOVED => {
                    if !entry.is_empty() {
                        return Err(TenantAdminError::TenantNotEmpty);
                    }
                    let generation = tenant_generation(version)
                        .checked_add(1)
                        .filter(|generation| *generation <= TENANT_MAX_GENERATION)
                        .ok_or(TenantAdminError::NamespaceExhausted)?;
                    entry.version.store(
                        tenant_version(generation, TENANT_CLOSING),
                        Ordering::Release,
                    );
                    publish_version = Some(tenant_version(generation, TENANT_REGISTERED));
                }
                _ => return Err(TenantAdminError::TenantNotRegistered),
            }
            entry.set_requested(policy.quota());
            entry.clone()
        } else {
            let raw_namespace = self
                .next_namespace
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current.checked_add(1)
                })
                .map_err(|_| TenantAdminError::NamespaceExhausted)?;
            let entry = Arc::new(TenantEntry::new(
                id.clone(),
                NamespaceId::new(raw_namespace),
                policy,
                TENANT_CLOSING,
            ));
            publish_version = Some(tenant_version(1, TENANT_REGISTERED));
            let mut entries = current.entries.clone();
            entries.insert(id, entry.clone());
            self.directory.store(Arc::new(TenantDirectory { entries }));
            entry
        };
        self.recompute_quotas_locked();
        if let Some(version) = publish_version {
            entry.version.store(version, Ordering::Release);
        }
        Ok(ResolvedTenant {
            manager_id: self.manager_id,
            namespace: entry.namespace,
            version: entry.version.load(Ordering::Acquire),
            entry: Some(entry),
        })
    }

    pub(super) fn delete(&self, id: &TenantId) -> Result<(), TenantAdminError> {
        if self.single {
            return Err(TenantAdminError::SingleTenantMode);
        }
        let _update = self.update.lock();
        let directory = self.directory.load();
        let entry = directory
            .entries
            .get(id)
            .ok_or(TenantAdminError::TenantNotRegistered)?;
        let registered = entry.version.load(Ordering::Acquire);
        if tenant_state(registered) != TENANT_REGISTERED
            || entry
                .version
                .compare_exchange(
                    registered,
                    with_tenant_state(registered, TENANT_CLOSING),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
        {
            return Err(TenantAdminError::TenantNotRegistered);
        }
        if !entry.is_empty() {
            entry.version.store(registered, Ordering::Release);
            return Err(TenantAdminError::TenantNotEmpty);
        }
        entry.version.store(
            with_tenant_state(registered, TENANT_REMOVED),
            Ordering::Release,
        );
        entry.memory.set_effective(0);
        self.recompute_quotas_locked();
        Ok(())
    }

    pub(super) fn refresh_capacity(&self) {
        if self.single {
            return;
        }
        // Segment updates are rare. The accepting snapshot generation lets
        // the common maintenance call avoid rewriting quota state.
        let generation = self.pool.direct_capacity_epoch();
        if self.capacity_generation.load(Ordering::Acquire) == generation {
            return;
        }
        let (capacity, observed_generation) = self.stable_capacity();
        let _update = self.update.lock();
        if self.capacity_generation.load(Ordering::Acquire) != observed_generation {
            self.recompute_with_capacity_locked(capacity, observed_generation);
        }
    }

    fn recompute_quotas_locked(&self) {
        let (capacity, generation) = self.stable_capacity();
        self.recompute_with_capacity_locked(capacity, generation);
    }

    fn stable_capacity(&self) -> (u64, u64) {
        loop {
            let generation = self.pool.direct_capacity_epoch();
            let capacity = self
                .pool
                .capacity_for(ReplicaClass::Memory)
                .capacity_bytes();
            if generation == self.pool.direct_capacity_epoch() {
                return (capacity, generation);
            }
        }
    }

    fn recompute_with_capacity_locked(&self, memory_capacity: u64, generation: u64) {
        let directory = self.directory.load();
        let mut entries: Vec<_> = directory
            .entries
            .values()
            .filter(|entry| {
                // CLOSING is used while a newly inserted or reactivated
                // tenant receives its effective quota before publication.
                tenant_state(entry.version.load(Ordering::Acquire)) != TENANT_REMOVED
            })
            .cloned()
            .collect();
        entries.sort_unstable_by(|left, right| left.id.cmp(&right.id));
        distribute_effective_quota(&entries, TenantResourceClass::Memory, memory_capacity);
        let has_reclaim_debt = entries.iter().any(|entry| {
            let account = &entry.memory;
            account.demand.load(Ordering::Relaxed)
                > account
                    .effective
                    .load(Ordering::Relaxed)
                    .saturating_add(account.retiring.load(Ordering::Relaxed))
        });
        let _ = self
            .reclaim_state
            .fetch_update(Ordering::Release, Ordering::Relaxed, |state| {
                let revision = state.wrapping_add(2) & !1;
                Some(revision | u64::from(has_reclaim_debt))
            });
        self.capacity_generation
            .store(generation, Ordering::Release);
    }

    pub(super) fn snapshot(&self, id: &TenantId) -> Option<TenantSnapshot> {
        if self.single {
            return None;
        }
        let directory = self.directory.load();
        directory
            .entries
            .get(id)
            .filter(|entry| {
                tenant_state(entry.version.load(Ordering::Acquire)) == TENANT_REGISTERED
            })
            .map(|entry| entry.snapshot())
    }

    pub(super) fn list(&self) -> Vec<TenantSnapshot> {
        if self.single {
            return Vec::new();
        }
        let directory = self.directory.load();
        let mut tenants: Vec<_> = directory
            .entries
            .values()
            .filter(|entry| {
                tenant_state(entry.version.load(Ordering::Acquire)) == TENANT_REGISTERED
            })
            .map(|entry| entry.snapshot())
            .collect();
        tenants.sort_unstable_by(|left, right| left.id.cmp(&right.id));
        tenants
    }

    pub(super) fn reclaim_targets(&self) -> Vec<ReclaimTarget> {
        let reclaim_state = self.reclaim_state.load(Ordering::Acquire);
        if self.single || reclaim_state & 1 == 0 {
            return Vec::new();
        }
        let directory = self.directory.load();
        let mut targets = Vec::new();
        for entry in directory.entries.values().filter(|entry| {
            tenant_state(entry.version.load(Ordering::Acquire)) == TENANT_REGISTERED
        }) {
            let account = &entry.memory;
            let demand = account.demand.load(Ordering::Relaxed);
            let retiring = account.retiring.load(Ordering::Relaxed);
            let effective = account.effective.load(Ordering::Acquire);
            let bytes = demand.saturating_sub(retiring).saturating_sub(effective);
            if bytes > 0 {
                targets.push(ReclaimTarget {
                    filter: ReclaimFilter::Scope {
                        namespace: entry.namespace,
                        replica_class: ReplicaClass::Memory,
                    },
                    bytes,
                });
            }
        }
        targets.sort_unstable_by_key(|target| match target.filter {
            ReclaimFilter::Scope { namespace, .. } => namespace.get(),
            ReclaimFilter::Any | ReclaimFilter::Class(_) => 0,
        });
        if targets.is_empty() {
            let _ = self.reclaim_state.compare_exchange(
                reclaim_state,
                reclaim_state & !1,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
        targets
    }
}

pub(super) const fn tenant_version(generation: u64, state: u8) -> u64 {
    (generation << TENANT_STATE_BITS) | state as u64
}

pub(super) const fn tenant_generation(version: u64) -> u64 {
    version >> TENANT_STATE_BITS
}

const fn tenant_state(version: u64) -> u8 {
    (version & TENANT_STATE_MASK) as u8
}

const fn with_tenant_state(version: u64, state: u8) -> u64 {
    (version & !TENANT_STATE_MASK) | state as u64
}
