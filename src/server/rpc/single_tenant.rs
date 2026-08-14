//! Single-tenant RPC backend adapter.

use super::backend::{ObjectBatchBackend, batch_maintenance_budget};
use super::response::{map_lookup_error, map_manager_error};
use crate::mooncake::{ErrorCode, ExpectedBool, ExpectedVoid};
use crate::object::reclamation::CatalogTick;
use crate::object::{
    NamespaceId, ObjectIdentity, ObjectLookup, ObjectManager, ObjectRead, ReplicaSelector,
    StartedPut, TenantPutRequest, WriteAdmission, WriteOwner,
};

impl ObjectBatchBackend for ObjectManager {
    type Tenant = ();

    fn maintain(&self, now: CatalogTick, item_count: usize) {
        let _ = ObjectManager::maintenance(self, now, batch_maintenance_budget(item_count));
    }

    fn resolve_tenant(&self, _tenant_id: &str) -> Result<Self::Tenant, ErrorCode> {
        Ok(())
    }

    fn exists_batch(
        &self,
        _tenant: &Self::Tenant,
        keys: &[String],
        now: CatalogTick,
    ) -> Vec<ExpectedBool> {
        keys.iter()
            .map(|key| {
                Ok(ObjectManager::exists(
                    self,
                    ObjectLookup::new(NamespaceId::DEFAULT, key),
                    now,
                ))
            })
            .collect()
    }

    fn get_batch(
        &self,
        _tenant: &Self::Tenant,
        keys: &[String],
        now: CatalogTick,
    ) -> Vec<Result<ObjectRead, ErrorCode>> {
        keys.iter()
            .map(|key| {
                ObjectManager::get(self, ObjectLookup::new(NamespaceId::DEFAULT, key), now)
                    .map_err(map_lookup_error)
            })
            .collect()
    }

    fn start_put_batch(
        &self,
        _tenant: &Self::Tenant,
        admission: WriteAdmission,
        requests: Vec<TenantPutRequest>,
        now: CatalogTick,
    ) -> Vec<Result<StartedPut, ErrorCode>> {
        requests
            .into_iter()
            .map(|request| {
                let (key, plan) = request.into_parts();
                ObjectManager::start_put(
                    self,
                    ObjectIdentity::new(NamespaceId::DEFAULT, key),
                    admission.clone(),
                    plan,
                    now,
                )
                .map_err(map_manager_error)
            })
            .collect()
    }

    fn finish_put_batch(
        &self,
        _tenant: &Self::Tenant,
        keys: &[&str],
        owner: WriteOwner,
        selector: ReplicaSelector,
    ) -> Vec<ExpectedVoid> {
        keys.iter()
            .map(|key| {
                ObjectManager::finish_put(
                    self,
                    &ObjectIdentity::new(NamespaceId::DEFAULT, *key),
                    owner,
                    selector,
                )
                .map_err(map_manager_error)
            })
            .collect()
    }

    fn revoke_put_batch(
        &self,
        _tenant: &Self::Tenant,
        keys: &[String],
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Vec<ExpectedVoid> {
        keys.iter()
            .map(|key| {
                ObjectManager::revoke_put(
                    self,
                    &ObjectIdentity::new(NamespaceId::DEFAULT, key.as_str()),
                    owner,
                    selector,
                    now,
                )
                .map_err(map_manager_error)
            })
            .collect()
    }
}
