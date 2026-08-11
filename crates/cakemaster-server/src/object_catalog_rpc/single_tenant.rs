use super::backend::ObjectBatchBackend;
use super::response::{map_lookup_error, map_manager_error};
use cakemaster::object::reclamation::{CatalogTick, CollectBudget};
use cakemaster::object::{
    NamespaceId, ObjectIdentity, ObjectLookup, ObjectManager, ObjectRead, ReplicaSelector,
    StartedPut, TenantPutRequest, WriteOwner,
};
use cakemaster_proto::mooncake::{ErrorCode, ExpectedBool, ExpectedVoid};

impl ObjectBatchBackend for ObjectManager {
    type Tenant = ();

    fn maintain(&self, now: CatalogTick) {
        let _ = ObjectManager::maintenance(self, now, CollectBudget::default());
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
        owner: WriteOwner,
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
                    owner,
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
