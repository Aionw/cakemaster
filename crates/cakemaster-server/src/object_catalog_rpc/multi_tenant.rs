use super::backend::{ObjectBatchBackend, batch_error, batch_maintenance_budget};
use super::response::{map_lookup_error, map_manager_error, map_tenant_error};
use cakemaster::object::reclamation::CatalogTick;
use cakemaster::object::{
    ObjectRead, ReplicaSelector, ResolvedTenant, StartedPut, TenantId, TenantObjectError,
    TenantObjectManager, TenantPutRequest, WriteAdmission, WriteOwner,
};
use cakemaster_proto::mooncake::{ErrorCode, ExpectedBool, ExpectedVoid};

impl ObjectBatchBackend for TenantObjectManager {
    type Tenant = ResolvedTenant;

    fn maintain(&self, now: CatalogTick, item_count: usize) {
        let _ = TenantObjectManager::maintenance(self, now, batch_maintenance_budget(item_count));
    }

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

    fn finish_put_batch(
        &self,
        tenant: &Self::Tenant,
        keys: &[&str],
        owner: WriteOwner,
        selector: ReplicaSelector,
    ) -> Vec<ExpectedVoid> {
        map_tenant_batch(
            keys.len(),
            TenantObjectManager::finish_put_batch(
                self,
                tenant,
                keys.iter().copied(),
                owner,
                selector,
            ),
            |result| result.map_err(map_manager_error),
        )
    }

    fn revoke_put_batch(
        &self,
        tenant: &Self::Tenant,
        keys: &[String],
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Vec<ExpectedVoid> {
        map_tenant_batch(
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
        )
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
