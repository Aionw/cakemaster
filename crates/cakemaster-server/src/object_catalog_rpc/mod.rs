mod backend;
mod multi_tenant;
mod request;
mod response;
mod single_tenant;

use crate::{ClientRuntime, ClientRuntimeError, MasterClock};
use backend::{ObjectBatchBackend, batch_error};
use cakemaster::client::{ClientLifecycleError, HeartbeatOutcome};
use cakemaster::object::{ObjectManager, TenantObjectManager, TenantPutRequest, WriteOwner};
use cakemaster::segment::error::AttachError;
use cakemaster_proto::mooncake::{
    ClientStatus, ErrorCode, ExpectedBool, ExpectedGetReplicaListResponse, ExpectedPingResponse,
    ExpectedReplicaDescriptors, ExpectedVoid, GetReplicaListResponse, ObjectMeta, PingResponse,
    ReplicaStatus, ReplicaType, ReplicateConfig, Segment, Uuid, WrappedMasterService,
};
use coro_rpc::RpcFailure;
use request::{PutPlanTemplate, client_id_from_uuid, replica_selector, segment_spec_from_wire};
use response::{replica_descriptor, started_replica_descriptor};
use std::sync::Arc;

pub struct ObjectCatalogRpcService<B = ObjectManager> {
    backend: Arc<B>,
    clock: MasterClock,
    clients: ClientRuntime,
}

impl<B> ObjectCatalogRpcService<B> {
    fn from_backend(backend: Arc<B>, clients: ClientRuntime) -> Self {
        let clock = clients.clock().clone();
        Self {
            backend,
            clock,
            clients,
        }
    }

    pub const fn clock(&self) -> &MasterClock {
        &self.clock
    }

    pub const fn client_runtime(&self) -> &ClientRuntime {
        &self.clients
    }
}

impl ObjectCatalogRpcService<ObjectManager> {
    pub fn new(manager: Arc<ObjectManager>) -> Self {
        Self::new_with_clock(manager, MasterClock::new())
    }

    pub fn new_with_clock(manager: Arc<ObjectManager>, clock: MasterClock) -> Self {
        let clients = ClientRuntime::new(manager.pool().clone(), clock);
        Self::from_backend(manager, clients)
    }

    pub fn new_with_runtime(manager: Arc<ObjectManager>, clients: ClientRuntime) -> Self {
        assert!(
            Arc::ptr_eq(manager.pool(), clients.pool()),
            "the client runtime and object manager must share one SegmentPool"
        );
        Self::from_backend(manager, clients)
    }

    pub fn manager(&self) -> &Arc<ObjectManager> {
        &self.backend
    }
}

impl ObjectCatalogRpcService<TenantObjectManager> {
    pub fn with_tenants(manager: Arc<TenantObjectManager>) -> Self {
        Self::with_tenants_and_clock(manager, MasterClock::new())
    }

    pub fn with_tenants_and_clock(manager: Arc<TenantObjectManager>, clock: MasterClock) -> Self {
        let clients = ClientRuntime::new(manager.pool().clone(), clock);
        Self::from_backend(manager, clients)
    }

    pub fn with_tenants_and_runtime(
        manager: Arc<TenantObjectManager>,
        clients: ClientRuntime,
    ) -> Self {
        assert!(
            Arc::ptr_eq(manager.pool(), clients.pool()),
            "the client runtime and tenant object manager must share one SegmentPool"
        );
        Self::from_backend(manager, clients)
    }

    pub fn tenant_manager(&self) -> &Arc<TenantObjectManager> {
        &self.backend
    }
}

impl<B: ObjectBatchBackend> WrappedMasterService for ObjectCatalogRpcService<B> {
    async fn ping(&self, client_id: Uuid) -> Result<ExpectedPingResponse, RpcFailure> {
        let ping = self.clients.ping(client_id_from_uuid(&client_id));
        let client_status = match ping.heartbeat() {
            HeartbeatOutcome::Alive(_) => ClientStatus::Ok,
            HeartbeatOutcome::NeedRemount => ClientStatus::NeedRemount,
        };
        Ok(Ok(PingResponse {
            view_version_id: ping.view_version(),
            client_status,
        }))
    }

    async fn re_mount_segment(
        &self,
        segments: Vec<Segment>,
        client_id: Uuid,
    ) -> Result<ExpectedVoid, RpcFailure> {
        let client_id = client_id_from_uuid(&client_id);
        let segments = match segments
            .into_iter()
            .map(|segment| segment_spec_from_wire(segment, client_id))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(segments) => segments,
            Err(error) => return Ok(Err(error)),
        };
        Ok(self
            .clients
            .remount(client_id, segments)
            .await
            .map(|_| ())
            .map_err(client_runtime_error))
    }

    async fn batch_exist_key(
        &self,
        keys: Vec<String>,
        tenant_id: String,
    ) -> Result<Vec<ExpectedBool>, RpcFailure> {
        let now = self.clock.now();
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
        let now = self.clock.now();
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
        let now = self.clock.now();
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
        let now = self.clock.now();
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
        let now = self.clock.now();
        Ok(self
            .backend
            .execute_batch(&tenant_id, item_count, now, move |backend, tenant| {
                backend.revoke_put_batch(tenant, &keys, owner, selector, now)
            }))
    }
}

fn client_runtime_error(error: ClientRuntimeError) -> ErrorCode {
    match error {
        ClientRuntimeError::Lifecycle(ClientLifecycleError::NilClientId) => {
            ErrorCode::InvalidParams
        }
        ClientRuntimeError::Lifecycle(ClientLifecycleError::CleanupInProgress { .. }) => {
            ErrorCode::UnavailableInCurrentStatus
        }
        ClientRuntimeError::Lifecycle(
            ClientLifecycleError::ClientNotActive
            | ClientLifecycleError::CleanupNotStarted
            | ClientLifecycleError::StaleSession,
        ) => ErrorCode::ClientNotFound,
        ClientRuntimeError::Lifecycle(
            ClientLifecycleError::CapacityExceeded { .. }
            | ClientLifecycleError::GenerationExhausted,
        )
        | ClientRuntimeError::InconsistentSlot
        | ClientRuntimeError::Rollback { .. }
        | ClientRuntimeError::SegmentState(_) => ErrorCode::InternalError,
        ClientRuntimeError::Attach(AttachError::ConflictingSegmentId(_)) => {
            ErrorCode::SegmentAlreadyExists
        }
        ClientRuntimeError::Attach(_) | ClientRuntimeError::ActiveRemountConflict => {
            ErrorCode::InvalidParams
        }
    }
}
