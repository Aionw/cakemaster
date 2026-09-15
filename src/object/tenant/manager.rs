use super::super::MemoryEvictionConfig;
use super::super::catalog::{ObjectCatalog, ObjectRead};
use super::super::config::ObjectCatalogConfig;
use super::super::diagnostics::ObjectCatalogStats;
use super::super::error::{
    LookupError, ObjectCatalogConfigError, ObjectManagerError, ObjectRemoveError,
};
use super::super::identity::{ObjectIdentity, ObjectLookup};
use super::super::manager::{
    ObjectManager, ObjectManagerMaintenance, ObjectPutPlan, PendingWriteRevoker, ReplicaSelector,
    StartedPut,
};
use super::super::reclamation::{CatalogTick, CollectBudget};
use super::super::write::{WriteAdmission, WriteMode, WriteOwner};
use super::quota::{QuotaReservationGuard, admission_charge};
use super::registry::{ResolvedTenant, TenantRegistry};
use super::{
    TenantAdminError, TenantConfig, TenantConfigError, TenantId, TenantObjectError, TenantPolicy,
    TenantResourceClass, TenantSnapshot,
};
use crate::segment::SegmentPool;
use regex::Regex;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct TenantPutRequest {
    key: Arc<str>,
    plan: ObjectPutPlan,
}

impl TenantPutRequest {
    pub fn new(key: impl Into<Arc<str>>, plan: ObjectPutPlan) -> Self {
        Self {
            key: key.into(),
            plan,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub const fn plan(&self) -> &ObjectPutPlan {
        &self.plan
    }

    pub fn into_parts(self) -> (Arc<str>, ObjectPutPlan) {
        (self.key, self.plan)
    }
}

#[derive(Debug, Error)]
pub enum TenantObjectManagerCreateError {
    #[error(transparent)]
    Catalog(#[from] ObjectCatalogConfigError),
    #[error(transparent)]
    Tenant(#[from] TenantConfigError),
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TenantGetError {
    #[error(transparent)]
    Tenant(#[from] TenantObjectError),
    #[error(transparent)]
    Lookup(#[from] LookupError),
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TenantRemoveError {
    #[error(transparent)]
    Tenant(#[from] TenantObjectError),
    #[error(transparent)]
    Remove(#[from] ObjectRemoveError),
}

/// Tenant-aware façade around [`ObjectManager`]. It is the quota-safe public
/// path for multi-tenant object operations.
pub struct TenantObjectManager {
    object: ObjectManager,
    catalog: TenantCatalog,
    registry: TenantRegistry,
}

/// Read/control view that intentionally omits object mutation APIs, keeping
/// multi-tenant writes behind [`TenantObjectManager`].
#[derive(Clone)]
pub struct TenantCatalog {
    inner: ObjectCatalog,
}

impl TenantCatalog {
    pub fn stats(&self) -> ObjectCatalogStats {
        self.inner.stats()
    }

    pub fn request_reclaim(&self, bytes: u64) {
        self.inner.request_reclaim(bytes);
    }
}

impl TenantObjectManager {
    pub fn new(
        pool: Arc<SegmentPool>,
        tenant_config: TenantConfig,
    ) -> Result<Self, TenantObjectManagerCreateError> {
        Self::with_config(pool, ObjectCatalogConfig::default(), tenant_config)
    }

    pub fn with_config(
        pool: Arc<SegmentPool>,
        object_config: ObjectCatalogConfig,
        tenant_config: TenantConfig,
    ) -> Result<Self, TenantObjectManagerCreateError> {
        let object = ObjectManager::with_config(pool.clone(), object_config)?;
        Self::from_object_manager(pool, tenant_config, object)
    }

    pub fn with_eviction_config(
        pool: Arc<SegmentPool>,
        object_config: ObjectCatalogConfig,
        tenant_config: TenantConfig,
        eviction_config: MemoryEvictionConfig,
    ) -> Result<Self, TenantObjectManagerCreateError> {
        let object =
            ObjectManager::with_eviction_config(pool.clone(), object_config, eviction_config)?;
        Self::from_object_manager(pool, tenant_config, object)
    }

    fn from_object_manager(
        pool: Arc<SegmentPool>,
        tenant_config: TenantConfig,
        object: ObjectManager,
    ) -> Result<Self, TenantObjectManagerCreateError> {
        let catalog = TenantCatalog {
            inner: object.catalog().clone(),
        };
        let registry = TenantRegistry::new(pool, tenant_config)?;
        Ok(Self {
            object,
            catalog,
            registry,
        })
    }

    pub const fn catalog(&self) -> &TenantCatalog {
        &self.catalog
    }

    pub fn pool(&self) -> &Arc<SegmentPool> {
        self.object.pool()
    }

    pub fn pending_write_revoker(&self) -> PendingWriteRevoker {
        self.object.pending_write_revoker()
    }

    pub(crate) fn memory_eviction_notify(&self) -> Option<Arc<tokio::sync::Notify>> {
        self.object.memory_eviction_notify()
    }

    pub fn resolve_tenant(
        &self,
        tenant_id: &TenantId,
    ) -> Result<ResolvedTenant, TenantObjectError> {
        self.registry.resolve(tenant_id)
    }

    pub fn start_put(
        &self,
        tenant: &ResolvedTenant,
        key: impl Into<Arc<str>>,
        admission: WriteAdmission,
        plan: ObjectPutPlan,
        now: CatalogTick,
    ) -> Result<StartedPut, TenantObjectError> {
        self.start_write(tenant, key, admission, plan, now, WriteMode::Insert)
    }

    pub fn start_upsert(
        &self,
        tenant: &ResolvedTenant,
        key: impl Into<Arc<str>>,
        admission: WriteAdmission,
        plan: ObjectPutPlan,
        now: CatalogTick,
    ) -> Result<StartedPut, TenantObjectError> {
        self.start_write(tenant, key, admission, plan, now, WriteMode::Upsert)
    }

    fn start_write(
        &self,
        tenant: &ResolvedTenant,
        key: impl Into<Arc<str>>,
        admission: WriteAdmission,
        plan: ObjectPutPlan,
        now: CatalogTick,
        mode: WriteMode,
    ) -> Result<StartedPut, TenantObjectError> {
        // The reservation's post-CAS version check is the linearization
        // point for multi-tenant admission, so only validate the immutable
        // opaque handle's manager binding here.
        self.registry.validate_binding(tenant)?;
        let identity = ObjectIdentity::new(tenant.namespace, key);
        let Some(entry) = &tenant.entry else {
            return Ok(match mode {
                WriteMode::Insert => self.object.start_put(identity, admission, plan, now)?,
                WriteMode::Upsert => self.object.start_upsert(identity, admission, plan, now)?,
            });
        };
        let (class, charge_bytes) = admission_charge(&plan)?;
        let reservation = entry.reserve(class, charge_bytes, tenant.version)?;
        self.start_write_accounted(identity, admission, plan, now, mode, reservation)
    }

    pub fn start_upsert_batch(
        &self,
        tenant: &ResolvedTenant,
        admission: WriteAdmission,
        requests: Vec<TenantPutRequest>,
        now: CatalogTick,
    ) -> Vec<Result<StartedPut, TenantObjectError>> {
        self.start_write_batch(tenant, admission, requests, now, WriteMode::Upsert)
    }

    #[inline]
    fn start_write_accounted(
        &self,
        identity: ObjectIdentity,
        admission: WriteAdmission,
        plan: ObjectPutPlan,
        now: CatalogTick,
        mode: WriteMode,
        mut reservation: QuotaReservationGuard,
    ) -> Result<StartedPut, TenantObjectError> {
        let prepared = match mode {
            WriteMode::Insert => self.object.prepare_put(identity, admission, plan, now)?,
            WriteMode::Upsert => self.object.prepare_upsert(identity, admission, plan, now)?,
        };
        reservation.resize(prepared.actual_charge_bytes()?)?;
        Ok(self
            .object
            .finalize_start_put(prepared, Some(reservation))?)
    }

    /// Starts a batch after one tenant validation and at most one initial
    /// quota CAS per resource class. Placement and staging remain
    /// item-independent.
    pub fn start_put_batch(
        &self,
        tenant: &ResolvedTenant,
        admission: WriteAdmission,
        requests: Vec<TenantPutRequest>,
        now: CatalogTick,
    ) -> Vec<Result<StartedPut, TenantObjectError>> {
        self.start_write_batch(tenant, admission, requests, now, WriteMode::Insert)
    }

    fn start_write_batch(
        &self,
        tenant: &ResolvedTenant,
        admission: WriteAdmission,
        requests: Vec<TenantPutRequest>,
        now: CatalogTick,
        mode: WriteMode,
    ) -> Vec<Result<StartedPut, TenantObjectError>> {
        if requests.len() == 1 {
            let request = requests
                .into_iter()
                .next()
                .expect("a one-item batch contains one request");
            let result = self.start_write(tenant, request.key, admission, request.plan, now, mode);
            return vec![result];
        }
        if let Err(error) = self.registry.validate(tenant) {
            return requests.into_iter().map(|_| Err(error)).collect();
        }
        let Some(entry) = &tenant.entry else {
            return requests
                .into_iter()
                .map(|request| -> Result<StartedPut, TenantObjectError> {
                    let identity = ObjectIdentity::new(tenant.namespace, request.key);
                    Ok(match mode {
                        WriteMode::Insert => {
                            self.object
                                .start_put(identity, admission.clone(), request.plan, now)?
                        }
                        WriteMode::Upsert => self.object.start_upsert(
                            identity,
                            admission.clone(),
                            request.plan,
                            now,
                        )?,
                    })
                })
                .collect();
        };

        let charges: Vec<_> = requests
            .iter()
            .map(|request| admission_charge(&request.plan))
            .collect();
        let mut valid_count = 0;
        let total = charges
            .iter()
            .filter_map(|charge| charge.as_ref().ok())
            .try_fold(0_u64, |total, (_, bytes)| {
                valid_count += 1;
                total.checked_add(*bytes)
            });
        let batch_error = if valid_count == 0 {
            None
        } else {
            match total {
                Some(total) => entry
                    .reserve_batch_total(TenantResourceClass::Memory, total, tenant.version)
                    .err(),
                None => Some(TenantObjectError::Object(ObjectManagerError::InvalidPlan)),
            }
        };

        requests
            .into_iter()
            .zip(charges)
            .map(|(request, charge)| {
                let (class, bytes) = charge?;
                if let Some(error) = batch_error {
                    return Err(error);
                }
                let reservation = QuotaReservationGuard::from_reserved(entry.clone(), class, bytes);
                self.start_write_accounted(
                    ObjectIdentity::new(tenant.namespace, request.key),
                    admission.clone(),
                    request.plan,
                    now,
                    mode,
                    reservation,
                )
            })
            .collect()
    }

    pub fn finish_put(
        &self,
        tenant: &ResolvedTenant,
        key: &str,
        owner: WriteOwner,
        selector: ReplicaSelector,
    ) -> Result<(), TenantObjectError> {
        self.registry.validate(tenant)?;
        self.object
            .finish_put_lookup(ObjectLookup::new(tenant.namespace, key), owner, selector)?;
        Ok(())
    }

    pub fn finish_put_at(
        &self,
        tenant: &ResolvedTenant,
        key: &str,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Result<(), TenantObjectError> {
        self.registry.validate(tenant)?;
        self.object.finish_put_lookup_at(
            ObjectLookup::new(tenant.namespace, key),
            owner,
            selector,
            now,
        )?;
        Ok(())
    }

    pub fn revoke_put(
        &self,
        tenant: &ResolvedTenant,
        key: &str,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Result<(), TenantObjectError> {
        self.registry.validate(tenant)?;
        self.object.revoke_put_lookup(
            ObjectLookup::new(tenant.namespace, key),
            owner,
            selector,
            now,
        )?;
        Ok(())
    }

    pub fn get(
        &self,
        tenant: &ResolvedTenant,
        key: &str,
        now: CatalogTick,
    ) -> Result<ObjectRead, TenantGetError> {
        self.registry.validate(tenant)?;
        Ok(self
            .object
            .get(ObjectLookup::new(tenant.namespace, key), now)?)
    }

    pub fn exists(
        &self,
        tenant: &ResolvedTenant,
        key: &str,
        now: CatalogTick,
    ) -> Result<bool, TenantObjectError> {
        self.registry.validate(tenant)?;
        Ok(self
            .object
            .exists(ObjectLookup::new(tenant.namespace, key), now))
    }

    pub fn remove(
        &self,
        tenant: &ResolvedTenant,
        key: &str,
        now: CatalogTick,
        force: bool,
    ) -> Result<(), TenantRemoveError> {
        self.registry.validate(tenant)?;
        self.object
            .remove(ObjectLookup::new(tenant.namespace, key), now, force)?;
        Ok(())
    }

    pub fn remove_batch<'a, I>(
        &self,
        tenant: &ResolvedTenant,
        keys: I,
        now: CatalogTick,
        force: bool,
    ) -> Result<Vec<Result<(), ObjectRemoveError>>, TenantObjectError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.map_validated_keys(tenant, keys, |lookup| {
            self.object.remove(lookup, now, force)
        })
    }

    pub fn remove_matching(
        &self,
        tenant: &ResolvedTenant,
        pattern: Option<&Regex>,
        now: CatalogTick,
        force: bool,
    ) -> Result<usize, TenantObjectError> {
        self.registry.validate(tenant)?;
        Ok(self
            .object
            .remove_matching(tenant.namespace, pattern, now, force))
    }

    pub fn remove_all_tenants(&self, now: CatalogTick, force: bool) -> usize {
        self.registry
            .list()
            .into_iter()
            .filter_map(|snapshot| self.registry.resolve(&snapshot.id).ok())
            .map(|tenant| {
                self.object
                    .remove_matching(tenant.namespace, None, now, force)
            })
            .sum()
    }

    pub fn exists_batch<'a, I>(
        &self,
        tenant: &ResolvedTenant,
        keys: I,
        now: CatalogTick,
    ) -> Result<Vec<bool>, TenantObjectError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.map_validated_keys(tenant, keys, |lookup| self.object.exists(lookup, now))
    }

    pub fn get_batch<'a, I>(
        &self,
        tenant: &ResolvedTenant,
        keys: I,
        now: CatalogTick,
    ) -> Result<Vec<Result<ObjectRead, LookupError>>, TenantObjectError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.map_validated_keys(tenant, keys, |lookup| self.object.get(lookup, now))
    }

    pub fn finish_put_batch<'a, I>(
        &self,
        tenant: &ResolvedTenant,
        keys: I,
        owner: WriteOwner,
        selector: ReplicaSelector,
    ) -> Result<Vec<Result<(), ObjectManagerError>>, TenantObjectError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.map_validated_keys(tenant, keys, |lookup| {
            self.object.finish_put_lookup(lookup, owner, selector)
        })
    }

    pub fn finish_put_batch_at<'a, I>(
        &self,
        tenant: &ResolvedTenant,
        keys: I,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Result<Vec<Result<(), ObjectManagerError>>, TenantObjectError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.map_validated_keys(tenant, keys, |lookup| {
            self.object
                .finish_put_lookup_at(lookup, owner, selector, now)
        })
    }

    pub fn revoke_put_batch<'a, I>(
        &self,
        tenant: &ResolvedTenant,
        keys: I,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Result<Vec<Result<(), ObjectManagerError>>, TenantObjectError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.map_validated_keys(tenant, keys, |lookup| {
            self.object.revoke_put_lookup(lookup, owner, selector, now)
        })
    }

    #[inline]
    fn map_validated_keys<'a, I, T>(
        &self,
        tenant: &ResolvedTenant,
        keys: I,
        mut operation: impl FnMut(ObjectLookup<'a>) -> T,
    ) -> Result<Vec<T>, TenantObjectError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.registry.validate(tenant)?;
        Ok(keys
            .into_iter()
            .map(|key| operation(ObjectLookup::new(tenant.namespace, key)))
            .collect())
    }

    pub fn maintenance(&self, now: CatalogTick, budget: CollectBudget) -> ObjectManagerMaintenance {
        self.registry.refresh_capacity();
        let targets = self.registry.reclaim_targets();
        self.object.maintenance_with_targets(now, budget, &targets)
    }

    pub fn upsert_tenant(
        &self,
        id: TenantId,
        policy: TenantPolicy,
    ) -> Result<ResolvedTenant, TenantAdminError> {
        self.registry.upsert(id, policy)
    }

    pub fn delete_tenant(&self, id: &TenantId) -> Result<(), TenantAdminError> {
        self.registry.delete(id)
    }

    pub fn tenant_snapshot(&self, id: &TenantId) -> Option<TenantSnapshot> {
        self.registry.snapshot(id)
    }

    pub fn list_tenants(&self) -> Vec<TenantSnapshot> {
        self.registry.list()
    }
}
