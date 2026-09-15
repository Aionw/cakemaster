use super::super::error::ObjectManagerError;
use super::super::identity::NamespaceId;
use super::super::manager::ObjectPutPlan;
use super::registry::{TenantEntry, tenant_generation, tenant_version};
use super::{
    TenantId, TenantObjectError, TenantPolicy, TenantQuotaLimits, TenantQuotaSnapshot,
    TenantResourceClass, TenantSnapshot,
};
use crate::segment::ReplicaClass;
use crate::segment::placement::FulfillmentPolicy;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

const CHARGE_RESERVED: u8 = 0;
const CHARGE_COMMITTED: u8 = 1;
const CHARGE_RETIRING: u8 = 2;
const CHARGE_RELEASED: u8 = 3;

pub(super) struct QuotaAccount {
    pub(super) policy_epoch: AtomicU64,
    pub(super) requested: AtomicU64,
    pub(super) effective: AtomicU64,
    pub(super) demand: AtomicU64,
    pub(super) used: AtomicU64,
    pub(super) retiring: AtomicU64,
}

/// Temporary quota reservation made before replica placement. Dropping it
/// rolls back admission; successful staging transfers it into a
/// [`TenantQuotaCharge`].
pub(crate) struct QuotaReservationGuard {
    entry: Option<Arc<TenantEntry>>,
    class: TenantResourceClass,
    bytes: u64,
}

/// RAII accounting token stored with an object record. Explicit lifecycle
/// transitions keep diagnostics precise; the owning catalog node's `Drop` is
/// the final safety net when physical replicas are actually released.
pub(crate) struct TenantQuotaCharge {
    entry: Arc<TenantEntry>,
    phase: AtomicU8,
}

impl TenantEntry {
    pub(super) fn new(
        id: TenantId,
        namespace: NamespaceId,
        policy: TenantPolicy,
        state: u8,
    ) -> Self {
        Self {
            id,
            namespace,
            version: AtomicU64::new(tenant_version(1, state)),
            memory: QuotaAccount::new(policy.quota().memory_bytes()),
        }
    }

    pub(super) fn account(&self, class: TenantResourceClass) -> &QuotaAccount {
        match class {
            TenantResourceClass::Memory => &self.memory,
        }
    }

    pub(super) fn reserve(
        self: &Arc<Self>,
        class: TenantResourceClass,
        bytes: u64,
        expected_version: u64,
    ) -> Result<QuotaReservationGuard, TenantObjectError> {
        self.reserve_checked(class, bytes, expected_version)?;
        Ok(QuotaReservationGuard::from_reserved(
            self.clone(),
            class,
            bytes,
        ))
    }

    pub(super) fn reserve_batch_total(
        &self,
        class: TenantResourceClass,
        total: u64,
        expected_version: u64,
    ) -> Result<(), TenantObjectError> {
        self.reserve_checked(class, total, expected_version)
    }

    #[inline]
    fn reserve_checked(
        &self,
        class: TenantResourceClass,
        bytes: u64,
        expected_version: u64,
    ) -> Result<(), TenantObjectError> {
        if let Err(error) = self.account(class).reserve_demand(class, bytes) {
            return if self.version.load(Ordering::Acquire) == expected_version {
                Err(error)
            } else {
                Err(TenantObjectError::InvalidTenantHandle)
            };
        }
        if self.version.load(Ordering::Acquire) != expected_version {
            self.account(class).release_reserved(bytes);
            return Err(TenantObjectError::InvalidTenantHandle);
        }
        Ok(())
    }

    pub(super) fn set_requested(&self, limits: TenantQuotaLimits) {
        self.memory
            .requested
            .store(limits.memory_bytes(), Ordering::Relaxed);
    }

    pub(super) fn is_empty(&self) -> bool {
        self.memory.demand.load(Ordering::Acquire) == 0
    }

    pub(super) fn snapshot(&self) -> TenantSnapshot {
        let memory = self.memory.snapshot();
        TenantSnapshot {
            id: self.id.clone(),
            namespace: self.namespace,
            generation: tenant_generation(self.version.load(Ordering::Acquire)),
            policy: TenantPolicy::new(TenantQuotaLimits::new(memory.requested_bytes)),
            memory,
        }
    }
}

impl QuotaAccount {
    fn new(requested: u64) -> Self {
        Self {
            policy_epoch: AtomicU64::new(0),
            requested: AtomicU64::new(requested),
            effective: AtomicU64::new(0),
            demand: AtomicU64::new(0),
            used: AtomicU64::new(0),
            retiring: AtomicU64::new(0),
        }
    }

    pub(super) fn set_effective(&self, effective: u64) {
        self.policy_epoch.fetch_add(1, Ordering::AcqRel);
        self.effective.store(effective, Ordering::Release);
        self.policy_epoch.fetch_add(1, Ordering::Release);
    }

    fn reserve_demand(
        &self,
        class: TenantResourceClass,
        bytes: u64,
    ) -> Result<(), TenantObjectError> {
        loop {
            let epoch = self.policy_epoch.load(Ordering::Acquire);
            if epoch & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let effective = self.effective.load(Ordering::Acquire);
            let mut demand = self.demand.load(Ordering::Relaxed);
            loop {
                let Some(next) = demand.checked_add(bytes) else {
                    return Err(TenantObjectError::TenantQuotaExceeded {
                        class,
                        requested_bytes: bytes,
                        demand_bytes: demand,
                        effective_bytes: effective,
                    });
                };
                if next > effective {
                    return Err(TenantObjectError::TenantQuotaExceeded {
                        class,
                        requested_bytes: bytes,
                        demand_bytes: demand,
                        effective_bytes: effective,
                    });
                }
                match self.demand.compare_exchange_weak(
                    demand,
                    next,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        if self.policy_epoch.load(Ordering::Acquire) == epoch {
                            return Ok(());
                        }
                        atomic_saturating_sub(&self.demand, bytes);
                        break;
                    }
                    Err(observed) => demand = observed,
                }
            }
        }
    }

    fn release_reserved(&self, bytes: u64) {
        atomic_saturating_sub(&self.demand, bytes);
    }

    fn snapshot(&self) -> TenantQuotaSnapshot {
        loop {
            let epoch = self.policy_epoch.load(Ordering::Acquire);
            if epoch & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let demand = self.demand.load(Ordering::Relaxed);
            let used = self.used.load(Ordering::Relaxed);
            let snapshot = TenantQuotaSnapshot {
                requested_bytes: self.requested.load(Ordering::Relaxed),
                effective_bytes: self.effective.load(Ordering::Relaxed),
                demand_bytes: demand,
                reserved_bytes: demand.saturating_sub(used),
                used_bytes: used,
                retiring_bytes: self.retiring.load(Ordering::Relaxed),
            };
            if self.policy_epoch.load(Ordering::Acquire) == epoch {
                return snapshot;
            }
        }
    }
}

impl QuotaReservationGuard {
    pub(super) fn from_reserved(
        entry: Arc<TenantEntry>,
        class: TenantResourceClass,
        bytes: u64,
    ) -> Self {
        Self {
            entry: Some(entry),
            class,
            bytes,
        }
    }

    pub(crate) fn resize(&mut self, bytes: u64) -> Result<(), TenantObjectError> {
        if bytes == self.bytes {
            return Ok(());
        }
        let account = self
            .entry
            .as_ref()
            .expect("live quota reservations retain their tenant")
            .account(self.class);
        if bytes < self.bytes {
            let released = self.bytes - bytes;
            atomic_saturating_sub(&account.demand, released);
        } else {
            let additional = bytes - self.bytes;
            account.reserve_demand(self.class, additional)?;
        }
        self.bytes = bytes;
        Ok(())
    }

    pub(crate) fn into_charge(mut self) -> TenantQuotaCharge {
        let entry = self
            .entry
            .take()
            .expect("live quota reservations retain their tenant");
        TenantQuotaCharge {
            entry,
            phase: AtomicU8::new(CHARGE_RESERVED),
        }
    }
}

impl Drop for QuotaReservationGuard {
    fn drop(&mut self) {
        if let Some(entry) = &self.entry {
            entry.account(self.class).release_reserved(self.bytes);
        }
    }
}

impl TenantQuotaCharge {
    fn account(&self, replica_class: ReplicaClass) -> &QuotaAccount {
        let class = TenantResourceClass::from_replica_class(replica_class)
            .expect("tenant charges only support Memory replicas");
        self.entry.account(class)
    }

    pub(crate) fn commit(&self, replica_class: ReplicaClass, bytes: u64) {
        debug_assert_eq!(self.phase.load(Ordering::Relaxed), CHARGE_RESERVED);
        self.account(replica_class)
            .used
            .fetch_add(bytes, Ordering::Relaxed);
        self.phase.store(CHARGE_COMMITTED, Ordering::Release);
    }

    pub(crate) fn abort(&self, replica_class: ReplicaClass, bytes: u64) {
        debug_assert_eq!(self.phase.load(Ordering::Relaxed), CHARGE_RESERVED);
        self.account(replica_class).release_reserved(bytes);
        self.phase.store(CHARGE_RELEASED, Ordering::Release);
    }

    pub(crate) fn mark_retiring(&self, replica_class: ReplicaClass, bytes: u64) {
        debug_assert_eq!(self.phase.load(Ordering::Relaxed), CHARGE_COMMITTED);
        self.account(replica_class)
            .retiring
            .fetch_add(bytes, Ordering::Relaxed);
        self.phase.store(CHARGE_RETIRING, Ordering::Release);
    }

    pub(crate) fn release_committed_partial(&self, replica_class: ReplicaClass, bytes: u64) {
        debug_assert_eq!(self.phase.load(Ordering::Acquire), CHARGE_COMMITTED);
        let account = self.account(replica_class);
        atomic_saturating_sub(&account.used, bytes);
        atomic_saturating_sub(&account.demand, bytes);
    }

    pub(crate) fn release(&self, replica_class: ReplicaClass, bytes: u64) {
        let current_phase = self.phase.load(Ordering::Acquire);
        self.phase.store(CHARGE_RELEASED, Ordering::Release);
        let account = self.account(replica_class);
        match current_phase {
            CHARGE_RESERVED => {
                account.release_reserved(bytes);
            }
            CHARGE_COMMITTED => {
                atomic_saturating_sub(&account.used, bytes);
                atomic_saturating_sub(&account.demand, bytes);
            }
            CHARGE_RETIRING => {
                atomic_saturating_sub(&account.used, bytes);
                atomic_saturating_sub(&account.retiring, bytes);
                atomic_saturating_sub(&account.demand, bytes);
            }
            CHARGE_RELEASED => {}
            _ => unreachable!("quota charge phase is validated internally"),
        }
    }
}

pub(super) fn distribute_effective_quota(
    entries: &[Arc<TenantEntry>],
    class: TenantResourceClass,
    capacity: u64,
) {
    let total_requested = entries.iter().fold(0_u128, |total, entry| {
        total + u128::from(entry.account(class).requested.load(Ordering::Relaxed))
    });
    if total_requested <= u128::from(capacity) {
        for entry in entries {
            let requested = entry.account(class).requested.load(Ordering::Relaxed);
            entry.account(class).set_effective(requested);
        }
        return;
    }

    let mut effective = Vec::with_capacity(entries.len());
    let mut assigned = 0_u64;
    for entry in entries {
        let requested = entry.account(class).requested.load(Ordering::Relaxed);
        let share = ((u128::from(requested) * u128::from(capacity)) / total_requested) as u64;
        effective.push((requested, share));
        assigned = assigned.saturating_add(share);
    }
    let mut remainder = capacity.saturating_sub(assigned);
    for (requested, share) in &mut effective {
        if remainder == 0 {
            break;
        }
        if *share < *requested {
            *share += 1;
            remainder -= 1;
        }
    }
    for (entry, (_, share)) in entries.iter().zip(effective) {
        entry.account(class).set_effective(share);
    }
}

pub(super) fn admission_charge(
    plan: &ObjectPutPlan,
) -> Result<(TenantResourceClass, u64), ObjectManagerError> {
    let class = TenantResourceClass::from_replica_class(plan.placement().replica_class())
        .ok_or(ObjectManagerError::InvalidPlan)?;
    let requested_replica_count = u64::try_from(plan.placement().replicas().count())
        .map_err(|_| ObjectManagerError::InvalidPlan)?;
    let admission_replica_count = match plan.placement().fulfillment() {
        FulfillmentPolicy::AllOrNothing => requested_replica_count,
        FulfillmentPolicy::BestEffort => 1,
    };
    let bytes = plan
        .content()
        .logical_bytes()
        .checked_mul(admission_replica_count)
        .ok_or(ObjectManagerError::InvalidPlan)?;
    Ok((class, bytes))
}

fn atomic_saturating_sub(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        debug_assert!(current >= amount, "tenant accounting must not underflow");
        Some(current.saturating_sub(amount))
    });
}
