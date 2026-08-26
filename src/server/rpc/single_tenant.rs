//! Single-tenant RPC backend adapter.

use super::backend::{ObjectBatchBackend, mutation_maintenance_budget};
use super::response::{map_lookup_error, map_manager_error, map_remove_error};
use crate::mooncake::{ErrorCode, ExpectedBool, ExpectedVoid};
use crate::object::reclamation::CatalogTick;
use crate::object::{
    NamespaceId, ObjectIdentity, ObjectLookup, ObjectManager, ObjectRead, ReplicaSelector,
    StartedPut, TenantPutRequest, WriteAdmission, WriteOwner,
};
use regex::Regex;

impl ObjectBatchBackend for ObjectManager {
    type Tenant = ();

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

    fn start_upsert_batch(
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
                ObjectManager::start_upsert(
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
        now: CatalogTick,
    ) -> Vec<ExpectedVoid> {
        let results = keys
            .iter()
            .map(|key| {
                ObjectManager::finish_put_at(
                    self,
                    &ObjectIdentity::new(NamespaceId::DEFAULT, *key),
                    owner,
                    selector,
                    now,
                )
                .map_err(map_manager_error)
            })
            .collect();
        let _ = ObjectManager::maintenance(self, now, mutation_maintenance_budget(keys.len()));
        results
    }

    fn revoke_put_batch(
        &self,
        _tenant: &Self::Tenant,
        keys: &[String],
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Vec<ExpectedVoid> {
        let results = keys
            .iter()
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
            .collect();
        let _ = ObjectManager::maintenance(self, now, mutation_maintenance_budget(keys.len()));
        results
    }

    fn remove_batch(
        &self,
        _tenant: &Self::Tenant,
        keys: &[String],
        now: CatalogTick,
        force: bool,
    ) -> Vec<ExpectedVoid> {
        let results = ObjectManager::remove_batch(
            self,
            keys.iter()
                .map(|key| ObjectLookup::new(NamespaceId::DEFAULT, key)),
            now,
            force,
        )
        .into_iter()
        .map(|result| result.map_err(map_remove_error))
        .collect();
        let _ = ObjectManager::maintenance(self, now, mutation_maintenance_budget(keys.len()));
        results
    }

    fn remove_matching(
        &self,
        _tenant: &Self::Tenant,
        pattern: Option<&Regex>,
        now: CatalogTick,
        force: bool,
    ) -> Result<usize, ErrorCode> {
        let removed =
            ObjectManager::remove_matching(self, NamespaceId::DEFAULT, pattern, now, force);
        let _ = ObjectManager::maintenance(self, now, mutation_maintenance_budget(removed));
        Ok(removed)
    }

    fn remove_all(
        &self,
        _tenant_id: &str,
        now: CatalogTick,
        force: bool,
    ) -> Result<usize, ErrorCode> {
        let removed = ObjectManager::remove_matching(self, NamespaceId::DEFAULT, None, now, force);
        let _ = ObjectManager::maintenance(self, now, mutation_maintenance_budget(removed));
        Ok(removed)
    }
}
