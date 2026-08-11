mod backend;
mod multi_tenant;
mod request;
mod response;
mod single_tenant;

use backend::{ObjectBatchBackend, batch_error};
use cakemaster::object::reclamation::CatalogTick;
use cakemaster::object::{ObjectManager, TenantObjectManager, TenantPutRequest, WriteOwner};
use cakemaster_proto::mooncake::{
    ErrorCode, ExpectedBool, ExpectedGetReplicaListResponse, ExpectedReplicaDescriptors,
    ExpectedVoid, GetReplicaListResponse, ObjectMeta, ReplicaStatus, ReplicaType, ReplicateConfig,
    Uuid, WrappedMasterService,
};
use coro_rpc::RpcFailure;
use request::{PutPlanTemplate, client_id_from_uuid, replica_selector};
use response::{replica_descriptor, started_replica_descriptor};
use std::sync::Arc;
use std::time::Instant;

pub struct ObjectCatalogRpcService<B = ObjectManager> {
    backend: Arc<B>,
    epoch: Instant,
}

impl<B> ObjectCatalogRpcService<B> {
    fn from_backend(backend: Arc<B>) -> Self {
        Self {
            backend,
            epoch: Instant::now(),
        }
    }

    fn now(&self) -> CatalogTick {
        let millis = self.epoch.elapsed().as_millis();
        CatalogTick::new(u64::try_from(millis).unwrap_or(u64::MAX))
    }
}

impl ObjectCatalogRpcService<ObjectManager> {
    pub fn new(manager: Arc<ObjectManager>) -> Self {
        Self::from_backend(manager)
    }

    pub fn manager(&self) -> &Arc<ObjectManager> {
        &self.backend
    }
}

impl ObjectCatalogRpcService<TenantObjectManager> {
    pub fn with_tenants(manager: Arc<TenantObjectManager>) -> Self {
        Self::from_backend(manager)
    }

    pub fn tenant_manager(&self) -> &Arc<TenantObjectManager> {
        &self.backend
    }
}

impl<B: ObjectBatchBackend> WrappedMasterService for ObjectCatalogRpcService<B> {
    async fn batch_exist_key(
        &self,
        keys: Vec<String>,
        tenant_id: String,
    ) -> Result<Vec<ExpectedBool>, RpcFailure> {
        let now = self.now();
        let item_count = keys.len();
        Ok(self
            .backend
            .execute_batch(&tenant_id, item_count, now, |backend, tenant| {
                backend.exists_batch(tenant, &keys, now)
            }))
    }

    async fn batch_get_replica_list(
        &self,
        keys: Vec<String>,
        tenant_id: String,
    ) -> Result<Vec<ExpectedGetReplicaListResponse>, RpcFailure> {
        let now = self.now();
        let item_count = keys.len();
        Ok(self
            .backend
            .execute_batch(&tenant_id, item_count, now, |backend, tenant| {
                backend.get_batch(tenant, &keys, now)
            })
            .into_iter()
            .map(|read| {
                let read = read?;
                let replicas = read
                    .object()
                    .replicas()
                    .iter()
                    .map(|replica| replica_descriptor(replica, ReplicaStatus::Complete))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(GetReplicaListResponse {
                    replicas,
                    lease_ttl_ms: read.lease_expires_at().get().saturating_sub(now.get()),
                    object_checksum: None,
                })
            })
            .collect())
    }

    async fn batch_put_start(
        &self,
        client_id: Uuid,
        keys: Vec<String>,
        slice_lengths: Vec<u64>,
        config: ReplicateConfig,
        tenant_id: String,
    ) -> Result<Vec<ExpectedReplicaDescriptors>, RpcFailure> {
        let item_count = keys.len();
        if item_count != slice_lengths.len() {
            return Ok(batch_error(item_count, ErrorCode::InvalidParams));
        }
        let template = match PutPlanTemplate::try_from(&config) {
            Ok(template) => template,
            Err(error) => return Ok(batch_error(item_count, error)),
        };
        let owner = WriteOwner::new(client_id_from_uuid(&client_id));
        let requests = keys
            .into_iter()
            .zip(slice_lengths)
            .map(|(key, logical_bytes)| TenantPutRequest::new(key, template.plan(logical_bytes)))
            .collect();
        let now = self.now();
        let started =
            self.backend
                .execute_batch(&tenant_id, item_count, now, move |backend, tenant| {
                    backend.start_put_batch(tenant, owner, requests, now)
                });
        Ok(started
            .into_iter()
            .map(|started| {
                let started = started?;
                started
                    .replicas()
                    .iter()
                    .map(started_replica_descriptor)
                    .collect()
            })
            .collect())
    }

    async fn batch_put_end(
        &self,
        client_id: Uuid,
        object_metas: Vec<ObjectMeta>,
        replica_type: ReplicaType,
        tenant_id: String,
    ) -> Result<Vec<ExpectedVoid>, RpcFailure> {
        let item_count = object_metas.len();
        let selector = match replica_selector(replica_type) {
            Ok(selector) => selector,
            Err(error) => return Ok(batch_error(item_count, error)),
        };
        let owner = WriteOwner::new(client_id_from_uuid(&client_id));
        let mut output = batch_error(item_count, ErrorCode::InvalidParams);
        let valid: Vec<_> = object_metas
            .iter()
            .enumerate()
            .filter(|(_, metadata)| metadata.object_checksum.is_none())
            .map(|(index, metadata)| (index, metadata.key.as_str()))
            .collect();
        if valid.is_empty() {
            return Ok(output);
        }
        let keys: Vec<_> = valid.iter().map(|(_, key)| *key).collect();
        let now = self.now();
        let results = self
            .backend
            .execute_batch(&tenant_id, keys.len(), now, |backend, tenant| {
                backend.finish_put_batch(tenant, &keys, owner, selector)
            });
        debug_assert_eq!(valid.len(), results.len());
        for ((index, _), result) in valid.into_iter().zip(results) {
            output[index] = result;
        }
        Ok(output)
    }

    async fn batch_put_revoke(
        &self,
        client_id: Uuid,
        keys: Vec<String>,
        replica_type: ReplicaType,
        tenant_id: String,
    ) -> Result<Vec<ExpectedVoid>, RpcFailure> {
        let item_count = keys.len();
        let selector = match replica_selector(replica_type) {
            Ok(selector) => selector,
            Err(error) => return Ok(batch_error(item_count, error)),
        };
        let owner = WriteOwner::new(client_id_from_uuid(&client_id));
        let now = self.now();
        Ok(self
            .backend
            .execute_batch(&tenant_id, item_count, now, move |backend, tenant| {
                backend.revoke_put_batch(tenant, &keys, owner, selector, now)
            }))
    }
}
