//! Multi-tenant RPC backend adapter.

use super::backend::{ObjectBatchBackend, batch_error, mutation_maintenance_budget};
use super::response::{map_lookup_error, map_manager_error, map_remove_error, map_tenant_error};
use crate::mooncake::{ErrorCode, ExpectedBool, ExpectedVoid};
use crate::object::reclamation::CatalogTick;
use crate::object::{
    ObjectRead, ReplicaSelector, ResolvedTenant, StartedPut, TenantId, TenantObjectError,
    TenantObjectManager, TenantPutRequest, WriteAdmission, WriteOwner,
};
use regex::Regex;

impl ObjectBatchBackend for TenantObjectManager {
    type Tenant = ResolvedTenant;

    fn resolve_tenant(&self, tenant_id: &str) -> Result<Self::Tenant, ErrorCode> {
        let tenant_id = TenantId::try_from(tenant_id).map_err(|_| ErrorCode::InvalidParams)?;
        TenantObjectManager::resolve_tenant(self, &tenant_id).map_err(map_tenant_error)
    }

    fn exists_batch(
        &self,
        tenant: &Self::Tenant,
        keys: &[String],
        now: CatalogTick,
    ) -> Vec<ExpectedBool> {
        map_tenant_batch(
            keys.len(),
            TenantObjectManager::exists_batch(self, tenant, keys.iter().map(String::as_str), now),
            Ok,
        )
    }

    fn get_batch(
        &self,
        tenant: &Self::Tenant,
        keys: &[String],
        now: CatalogTick,
    ) -> Vec<Result<ObjectRead, ErrorCode>> {
        map_tenant_batch(
            keys.len(),
            TenantObjectManager::get_batch(self, tenant, keys.iter().map(String::as_str), now),
            |value| value.map_err(map_lookup_error),
        )
    }

    fn start_put_batch(
        &self,
        tenant: &Self::Tenant,
        admission: WriteAdmission,
        requests: Vec<TenantPutRequest>,
        now: CatalogTick,
    ) -> Vec<Result<StartedPut, ErrorCode>> {
        TenantObjectManager::start_put_batch(self, tenant, admission, requests, now)
            .into_iter()
            .map(|result| result.map_err(map_tenant_error))
            .collect()
    }

    fn start_upsert_batch(
        &self,
        tenant: &Self::Tenant,
        admission: WriteAdmission,
        requests: Vec<TenantPutRequest>,
        now: CatalogTick,
    ) -> Vec<Result<StartedPut, ErrorCode>> {
        TenantObjectManager::start_upsert_batch(self, tenant, admission, requests, now)
            .into_iter()
            .map(|result| result.map_err(map_tenant_error))
            .collect()
    }

    fn finish_put_batch(
        &self,
        tenant: &Self::Tenant,
        keys: &[&str],
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Vec<ExpectedVoid> {
        let results = map_tenant_batch(
            keys.len(),
            TenantObjectManager::finish_put_batch_at(
                self,
                tenant,
                keys.iter().copied(),
                owner,
                selector,
                now,
            ),
            |result| result.map_err(map_manager_error),
        );
        let _ =
            TenantObjectManager::maintenance(self, now, mutation_maintenance_budget(keys.len()));
        results
    }

    fn revoke_put_batch(
        &self,
        tenant: &Self::Tenant,
        keys: &[String],
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Vec<ExpectedVoid> {
        let results = map_tenant_batch(
            keys.len(),
            TenantObjectManager::revoke_put_batch(
                self,
                tenant,
                keys.iter().map(String::as_str),
                owner,
                selector,
                now,
            ),
            |result| result.map_err(map_manager_error),
        );
        let _ =
            TenantObjectManager::maintenance(self, now, mutation_maintenance_budget(keys.len()));
        results
    }

    fn remove_batch(
        &self,
        tenant: &Self::Tenant,
        keys: &[String],
        now: CatalogTick,
        force: bool,
    ) -> Vec<ExpectedVoid> {
        let results = map_tenant_batch(
            keys.len(),
            TenantObjectManager::remove_batch(
                self,
                tenant,
                keys.iter().map(String::as_str),
                now,
                force,
            ),
            |result| result.map_err(map_remove_error),
        );
        let _ =
            TenantObjectManager::maintenance(self, now, mutation_maintenance_budget(keys.len()));
        results
    }

    fn remove_matching(
        &self,
        tenant: &Self::Tenant,
        pattern: Option<&Regex>,
        now: CatalogTick,
        force: bool,
    ) -> Result<usize, ErrorCode> {
        let removed = TenantObjectManager::remove_matching(self, tenant, pattern, now, force)
            .map_err(map_tenant_error)?;
        let _ = TenantObjectManager::maintenance(self, now, mutation_maintenance_budget(removed));
        Ok(removed)
    }

    fn remove_all(
        &self,
        tenant_id: &str,
        now: CatalogTick,
        force: bool,
    ) -> Result<usize, ErrorCode> {
        if tenant_id.is_empty() {
            let removed = TenantObjectManager::remove_all_tenants(self, now, force);
            let _ =
                TenantObjectManager::maintenance(self, now, mutation_maintenance_budget(removed));
            return Ok(removed);
        }
        let tenant = <Self as ObjectBatchBackend>::resolve_tenant(self, tenant_id)?;
        let removed = TenantObjectManager::remove_matching(self, &tenant, None, now, force)
            .map_err(map_tenant_error)?;
        let _ = TenantObjectManager::maintenance(self, now, mutation_maintenance_budget(removed));
        Ok(removed)
    }
}

fn map_tenant_batch<T, U>(
    item_count: usize,
    batch: Result<Vec<T>, TenantObjectError>,
    map_item: impl FnMut(T) -> Result<U, ErrorCode>,
) -> Vec<Result<U, ErrorCode>> {
    match batch {
        Ok(items) => items.into_iter().map(map_item).collect(),
        Err(error) => batch_error(item_count, map_tenant_error(error)),
    }
}
