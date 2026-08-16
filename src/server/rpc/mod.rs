//! Mooncake RPC adapters backed by Cakemaster domain managers.

mod backend;
mod multi_tenant;
mod observability;
mod request;
mod response;
mod single_tenant;

use super::{MasterClock, MasterReconcileConfig, MasterReconciler};
use crate::MOONCAKE_STORE_VERSION;
use crate::client::{
    ClientLifecycleConfig, ClientLifecycleConfigError, ClientLifecycleError, ClientManager,
    ClientManagerError, HeartbeatOutcome,
};
use crate::mooncake::{
    ClientStatus, ErrorCode, ExpectedBool, ExpectedGetReplicaListResponse,
    ExpectedGetStorageConfigResponse, ExpectedI64, ExpectedPingResponse,
    ExpectedReplicaDescriptors, ExpectedString, ExpectedVoid, GetReplicaListResponse,
    GetStorageConfigResponse, ObjectMeta, PingResponse, ReplicaStatus, ReplicaType,
    ReplicateConfig, Segment, Uuid, WrappedMasterService,
};
use crate::object::{
    ObjectManager, ObjectRead, ReplicaSelector, TenantObjectManager, TenantPutRequest,
};
use crate::segment::error::AttachError;
use backend::{ObjectBatchBackend, batch_error};
use coro_rpc::RpcFailure;
use observability::{RpcLabels, observe_batch, observe_internal_mapping, observe_result};
use regex::Regex;
use request::{
    PutPlanTemplate, client_id_from_uuid, replica_selector, segment_id_from_uuid,
    segment_spec_from_wire,
};
use response::{replica_descriptor, started_replica_descriptor};
use std::sync::Arc;
use tokio::sync::Notify;

pub const DEFAULT_MASTER_VIEW_VERSION: i64 = 1;

pub struct ObjectCatalogRpcService<B = ObjectManager> {
    backend: Arc<B>,
    clients: ClientManager,
    clock: MasterClock,
    reconcile_notify: Arc<Notify>,
    view_version: i64,
}

impl<B> ObjectCatalogRpcService<B> {
    fn from_backend(
        backend: Arc<B>,
        clients: ClientManager,
        clock: MasterClock,
        view_version: i64,
    ) -> Self {
        Self {
            backend,
            clients,
            clock,
            reconcile_notify: Arc::new(Notify::new()),
            view_version,
        }
    }

    pub fn clock(&self) -> &MasterClock {
        &self.clock
    }

    pub const fn client_manager(&self) -> &ClientManager {
        &self.clients
    }
}

impl ObjectCatalogRpcService<ObjectManager> {
    pub fn new(manager: Arc<ObjectManager>) -> Self {
        Self::new_with_clock(manager, MasterClock::new())
    }

    pub fn new_with_clock(manager: Arc<ObjectManager>, clock: MasterClock) -> Self {
        let clients = ClientManager::new(manager.pool().clone(), manager.pending_write_revoker());
        Self::from_backend(manager, clients, clock, DEFAULT_MASTER_VIEW_VERSION)
    }

    pub fn new_with_client_config(
        manager: Arc<ObjectManager>,
        config: ClientLifecycleConfig,
        clock: MasterClock,
        view_version: i64,
    ) -> Result<Self, ClientLifecycleConfigError> {
        let clients = ClientManager::with_config(
            manager.pool().clone(),
            manager.pending_write_revoker(),
            config,
        )?;
        Ok(Self::from_backend(manager, clients, clock, view_version))
    }

    pub fn manager(&self) -> &Arc<ObjectManager> {
        &self.backend
    }

    /// Creates an explicitly driven reconciler sharing this service's managers and clock.
    pub fn reconciler(&self, config: MasterReconcileConfig) -> MasterReconciler {
        MasterReconciler::for_object_manager(
            self.clients.clone(),
            self.backend.clone(),
            self.clock.clone(),
            self.reconcile_notify.clone(),
            config,
        )
    }
}

impl ObjectCatalogRpcService<TenantObjectManager> {
    pub fn with_tenants(manager: Arc<TenantObjectManager>) -> Self {
        Self::with_tenants_and_clock(manager, MasterClock::new())
    }

    pub fn with_tenants_and_clock(manager: Arc<TenantObjectManager>, clock: MasterClock) -> Self {
        let clients = ClientManager::new(manager.pool().clone(), manager.pending_write_revoker());
        Self::from_backend(manager, clients, clock, DEFAULT_MASTER_VIEW_VERSION)
    }

    pub fn tenant_manager(&self) -> &Arc<TenantObjectManager> {
        &self.backend
    }

    /// Creates an explicitly driven reconciler sharing this service's managers and clock.
    pub fn reconciler(&self, config: MasterReconcileConfig) -> MasterReconciler {
        MasterReconciler::for_tenant_object_manager(
            self.clients.clone(),
            self.backend.clone(),
            self.clock.clone(),
            self.reconcile_notify.clone(),
            config,
        )
    }
}

impl<B: ObjectBatchBackend> WrappedMasterService for ObjectCatalogRpcService<B> {
    async fn ping(&self, client_id: Uuid) -> Result<ExpectedPingResponse, RpcFailure> {
        let heartbeat = self
            .clients
            .heartbeat(client_id_from_uuid(&client_id), self.clock.client_now());
        let client_status = match heartbeat {
            HeartbeatOutcome::Alive(_) => ClientStatus::Ok,
            HeartbeatOutcome::NeedRemount => ClientStatus::NeedRemount,
        };
        Ok(Ok(PingResponse {
            view_version_id: self.view_version,
            client_status,
        }))
    }

    async fn get_storage_config(&self) -> Result<ExpectedGetStorageConfigResponse, RpcFailure> {
        Ok(Ok(GetStorageConfigResponse {
            fsdir: String::new(),
            enable_disk_eviction: false,
            quota_bytes: 0,
        }))
    }

    async fn service_ready(&self) -> Result<ExpectedString, RpcFailure> {
        Ok(Ok(MOONCAKE_STORE_VERSION.to_owned()))
    }

    async fn mount_segment(
        &self,
        segment: Segment,
        client_id: Uuid,
    ) -> Result<ExpectedVoid, RpcFailure> {
        let client_id = client_id_from_uuid(&client_id);
        let segment_id = segment_id_from_uuid(&segment.id);
        let labels = RpcLabels::new("mount_segment")
            .with_client(client_id)
            .with_segment(segment_id);
        let segment = match segment_spec_from_wire(segment, client_id) {
            Ok(segment) => segment,
            Err(error) => return Ok(observe_result(labels, Err(error))),
        };
        let result = self
            .clients
            .mount_segment(client_id, segment, self.clock.client_now())
            .map(|_| ())
            .map_err(mount_segment_error);
        Ok(observe_result(labels, result))
    }

    async fn re_mount_segment(
        &self,
        segments: Vec<Segment>,
        client_id: Uuid,
    ) -> Result<ExpectedVoid, RpcFailure> {
        let client_id = client_id_from_uuid(&client_id);
        let labels = RpcLabels::new("re_mount_segment").with_client(client_id);
        let segments = match segments
            .into_iter()
            .map(|segment| segment_spec_from_wire(segment, client_id))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(segments) => segments,
            Err(error) => return Ok(observe_result(labels, Err(error))),
        };
        let result = self
            .clients
            .remount(client_id, segments, self.clock.client_now())
            .map(|_| ())
            .map_err(client_manager_error);
        Ok(observe_result(labels, result))
    }

    async fn unmount_segment(
        &self,
        segment_id: Uuid,
        client_id: Uuid,
    ) -> Result<ExpectedVoid, RpcFailure> {
        let client_id = client_id_from_uuid(&client_id);
        let segment_id = segment_id_from_uuid(&segment_id);
        let labels = RpcLabels::new("unmount_segment")
            .with_client(client_id)
            .with_segment(segment_id);
        let result = self.clients.unmount_segment(client_id, segment_id);
        if result.is_ok() {
            self.reconcile_notify.notify_one();
        }
        Ok(observe_result(
            labels,
            result.map(|_| ()).map_err(client_manager_error),
        ))
    }

    async fn graceful_unmount_segment(
        &self,
        segment_id: Uuid,
        client_id: Uuid,
        grace_period_ms: u64,
    ) -> Result<ExpectedVoid, RpcFailure> {
        let client_id = client_id_from_uuid(&client_id);
        let segment_id = segment_id_from_uuid(&segment_id);
        let labels = RpcLabels::new("graceful_unmount_segment")
            .with_client(client_id)
            .with_segment(segment_id);
        let deadline = self.clock.client_now().saturating_add(grace_period_ms);
        let result = self
            .clients
            .schedule_graceful_unmount(client_id, segment_id, deadline);
        if result.is_ok() {
            self.reconcile_notify.notify_one();
        }
        Ok(observe_result(labels, result.map_err(client_manager_error)))
    }

    async fn exist_key(&self, key: String, tenant_id: String) -> Result<ExpectedBool, RpcFailure> {
        let now = self.clock().now();
        let result = only_item(self.backend.execute_batch(
            &tenant_id,
            1,
            now,
            |backend, tenant| backend.exists_batch(tenant, std::slice::from_ref(&key), now),
        ));
        Ok(observe_result(
            RpcLabels::new("exist_key").with_tenant(&tenant_id),
            result,
        ))
    }

    async fn get_replica_list(
        &self,
        key: String,
        tenant_id: String,
    ) -> Result<ExpectedGetReplicaListResponse, RpcFailure> {
        let now = self.clock().now();
        let read = only_item(
            self.backend
                .execute_batch(&tenant_id, 1, now, |backend, tenant| {
                    backend.get_batch(tenant, std::slice::from_ref(&key), now)
                }),
        );
        Ok(observe_result(
            RpcLabels::new("get_replica_list").with_tenant(&tenant_id),
            get_replica_list_response(read, now),
        ))
    }

    async fn batch_exist_key(
        &self,
        keys: Vec<String>,
        tenant_id: String,
    ) -> Result<Vec<ExpectedBool>, RpcFailure> {
        let now = self.clock().now();
        let item_count = keys.len();
        let results = self
            .backend
            .execute_batch(&tenant_id, item_count, now, |backend, tenant| {
                backend.exists_batch(tenant, &keys, now)
            });
        Ok(observe_batch(
            RpcLabels::new("batch_exist_key").with_tenant(&tenant_id),
            results,
        ))
    }

    async fn batch_get_replica_list(
        &self,
        keys: Vec<String>,
        tenant_id: String,
    ) -> Result<Vec<ExpectedGetReplicaListResponse>, RpcFailure> {
        let now = self.clock().now();
        let item_count = keys.len();
        let results = self
            .backend
            .execute_batch(&tenant_id, item_count, now, |backend, tenant| {
                backend.get_batch(tenant, &keys, now)
            })
            .into_iter()
            .map(|read| get_replica_list_response(read, now))
            .collect();
        Ok(observe_batch(
            RpcLabels::new("batch_get_replica_list").with_tenant(&tenant_id),
            results,
        ))
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
        let client_id = client_id_from_uuid(&client_id);
        let labels = RpcLabels::new("batch_put_start")
            .with_client(client_id)
            .with_tenant(&tenant_id);
        if item_count != slice_lengths.len() {
            return Ok(observe_batch(
                labels,
                batch_error(item_count, ErrorCode::InvalidParams),
            ));
        }
        let template = match PutPlanTemplate::try_from(&config) {
            Ok(template) => template,
            Err(error) => return Ok(observe_batch(labels, batch_error(item_count, error))),
        };
        let admission = match self.clients.write_admission(client_id) {
            Ok(admission) => admission,
            Err(error) => {
                return Ok(observe_batch(
                    labels,
                    batch_error(item_count, client_lifecycle_error(error)),
                ));
            }
        };
        let requests = keys
            .into_iter()
            .zip(slice_lengths)
            .map(|(key, logical_bytes)| TenantPutRequest::new(key, template.plan(logical_bytes)))
            .collect();
        let now = self.clock().now();
        let started =
            self.backend
                .execute_batch(&tenant_id, item_count, now, move |backend, tenant| {
                    backend.start_put_batch(tenant, admission, requests, now)
                });
        let results = started
            .into_iter()
            .map(|started| {
                let started = started?;
                started
                    .replicas()
                    .iter()
                    .map(started_replica_descriptor)
                    .collect()
            })
            .collect();
        Ok(observe_batch(labels, results))
    }

    async fn batch_put_end(
        &self,
        client_id: Uuid,
        object_metas: Vec<ObjectMeta>,
        replica_type: ReplicaType,
        tenant_id: String,
    ) -> Result<Vec<ExpectedVoid>, RpcFailure> {
        let item_count = object_metas.len();
        let client_id = client_id_from_uuid(&client_id);
        let labels = RpcLabels::new("batch_put_end")
            .with_client(client_id)
            .with_tenant(&tenant_id);
        let selector = match replica_selector(replica_type) {
            Ok(selector) => selector,
            Err(error) => return Ok(observe_batch(labels, batch_error(item_count, error))),
        };
        let owner = match self.clients.write_owner(client_id) {
            Ok(owner) => owner,
            Err(error) => {
                return Ok(observe_batch(
                    labels,
                    batch_error(item_count, client_lifecycle_error(error)),
                ));
            }
        };
        let mut output = batch_error(item_count, ErrorCode::InvalidParams);
        let valid: Vec<_> = object_metas
            .iter()
            .enumerate()
            .filter(|(_, metadata)| metadata.object_checksum.is_none())
            .map(|(index, metadata)| (index, metadata.key.as_str()))
            .collect();
        if valid.is_empty() {
            return Ok(observe_batch(labels, output));
        }
        let keys: Vec<_> = valid.iter().map(|(_, key)| *key).collect();
        let now = self.clock().now();
        let results = self
            .backend
            .execute_batch(&tenant_id, keys.len(), now, |backend, tenant| {
                backend.finish_put_batch(tenant, &keys, owner, selector)
            });
        debug_assert_eq!(valid.len(), results.len());
        for ((index, _), result) in valid.into_iter().zip(results) {
            output[index] = result;
        }
        Ok(observe_batch(labels, output))
    }

    async fn batch_put_revoke(
        &self,
        client_id: Uuid,
        keys: Vec<String>,
        replica_type: ReplicaType,
        tenant_id: String,
    ) -> Result<Vec<ExpectedVoid>, RpcFailure> {
        let item_count = keys.len();
        let client_id = client_id_from_uuid(&client_id);
        let labels = RpcLabels::new("batch_put_revoke")
            .with_client(client_id)
            .with_tenant(&tenant_id);
        let selector = match replica_selector(replica_type) {
            Ok(selector) => selector,
            Err(error) => return Ok(observe_batch(labels, batch_error(item_count, error))),
        };
        let owner = match self.clients.write_owner(client_id) {
            Ok(owner) => owner,
            Err(error) => {
                return Ok(observe_batch(
                    labels,
                    batch_error(item_count, client_lifecycle_error(error)),
                ));
            }
        };
        let now = self.clock().now();
        let results =
            self.backend
                .execute_batch(&tenant_id, item_count, now, move |backend, tenant| {
                    backend.revoke_put_batch(tenant, &keys, owner, selector, now)
                });
        Ok(observe_batch(labels, results))
    }

    async fn upsert_start(
        &self,
        client_id: Uuid,
        key: String,
        slice_length: u64,
        config: ReplicateConfig,
        tenant_id: String,
    ) -> Result<ExpectedReplicaDescriptors, RpcFailure> {
        let client_id = client_id_from_uuid(&client_id);
        let labels = RpcLabels::new("upsert_start")
            .with_client(client_id)
            .with_tenant(&tenant_id);
        let template = match PutPlanTemplate::try_from(&config) {
            Ok(template) => template,
            Err(error) => return Ok(observe_result(labels, Err(error))),
        };
        let admission = match self.clients.write_admission(client_id) {
            Ok(admission) => admission,
            Err(error) => {
                return Ok(observe_result(labels, Err(client_lifecycle_error(error))));
            }
        };
        let requests = vec![TenantPutRequest::new(key, template.plan(slice_length))];
        let now = self.clock().now();
        let started = only_item(self.backend.execute_batch(
            &tenant_id,
            1,
            now,
            move |backend, tenant| backend.start_upsert_batch(tenant, admission, requests, now),
        ));
        let result = started.and_then(|started| {
            started
                .replicas()
                .iter()
                .map(started_replica_descriptor)
                .collect()
        });
        Ok(observe_result(labels, result))
    }

    async fn upsert_end(
        &self,
        client_id: Uuid,
        object_meta: ObjectMeta,
        replica_type: ReplicaType,
        tenant_id: String,
    ) -> Result<ExpectedVoid, RpcFailure> {
        let client_id = client_id_from_uuid(&client_id);
        let labels = RpcLabels::new("upsert_end")
            .with_client(client_id)
            .with_tenant(&tenant_id);
        if object_meta.object_checksum.is_some() {
            return Ok(observe_result(labels, Err(ErrorCode::InvalidParams)));
        }
        let selector = match replica_selector(replica_type) {
            Ok(selector) => selector,
            Err(error) => return Ok(observe_result(labels, Err(error))),
        };
        let owner = match self.clients.write_owner(client_id) {
            Ok(owner) => owner,
            Err(error) => {
                return Ok(observe_result(labels, Err(client_lifecycle_error(error))));
            }
        };
        let now = self.clock().now();
        let result = only_item(self.backend.execute_batch(
            &tenant_id,
            1,
            now,
            |backend, tenant| {
                backend.finish_put_batch(tenant, &[object_meta.key.as_str()], owner, selector)
            },
        ));
        Ok(observe_result(labels, result))
    }

    async fn upsert_revoke(
        &self,
        client_id: Uuid,
        key: String,
        replica_type: ReplicaType,
        tenant_id: String,
    ) -> Result<ExpectedVoid, RpcFailure> {
        let client_id = client_id_from_uuid(&client_id);
        let labels = RpcLabels::new("upsert_revoke")
            .with_client(client_id)
            .with_tenant(&tenant_id);
        let selector = match replica_selector(replica_type) {
            Ok(selector) => selector,
            Err(error) => return Ok(observe_result(labels, Err(error))),
        };
        let owner = match self.clients.write_owner(client_id) {
            Ok(owner) => owner,
            Err(error) => {
                return Ok(observe_result(labels, Err(client_lifecycle_error(error))));
            }
        };
        let now = self.clock().now();
        let result = only_item(self.backend.execute_batch(
            &tenant_id,
            1,
            now,
            |backend, tenant| {
                backend.revoke_put_batch(tenant, std::slice::from_ref(&key), owner, selector, now)
            },
        ));
        Ok(observe_result(labels, result))
    }

    async fn batch_upsert_start(
        &self,
        client_id: Uuid,
        keys: Vec<String>,
        slice_lengths: Vec<u64>,
        config: ReplicateConfig,
        tenant_id: String,
    ) -> Result<Vec<ExpectedReplicaDescriptors>, RpcFailure> {
        let item_count = keys.len();
        let client_id = client_id_from_uuid(&client_id);
        let labels = RpcLabels::new("batch_upsert_start")
            .with_client(client_id)
            .with_tenant(&tenant_id);
        if item_count != slice_lengths.len() {
            return Ok(observe_batch(
                labels,
                batch_error(item_count, ErrorCode::InvalidParams),
            ));
        }
        let template = match PutPlanTemplate::try_from(&config) {
            Ok(template) => template,
            Err(error) => return Ok(observe_batch(labels, batch_error(item_count, error))),
        };
        let admission = match self.clients.write_admission(client_id) {
            Ok(admission) => admission,
            Err(error) => {
                return Ok(observe_batch(
                    labels,
                    batch_error(item_count, client_lifecycle_error(error)),
                ));
            }
        };
        let requests = keys
            .into_iter()
            .zip(slice_lengths)
            .map(|(key, logical_bytes)| TenantPutRequest::new(key, template.plan(logical_bytes)))
            .collect();
        let now = self.clock().now();
        let results = self
            .backend
            .execute_batch(&tenant_id, item_count, now, move |backend, tenant| {
                backend.start_upsert_batch(tenant, admission, requests, now)
            })
            .into_iter()
            .map(|started| {
                started.and_then(|started| {
                    started
                        .replicas()
                        .iter()
                        .map(started_replica_descriptor)
                        .collect()
                })
            })
            .collect();
        Ok(observe_batch(labels, results))
    }

    async fn batch_upsert_end(
        &self,
        client_id: Uuid,
        object_metas: Vec<ObjectMeta>,
        tenant_id: String,
    ) -> Result<Vec<ExpectedVoid>, RpcFailure> {
        let item_count = object_metas.len();
        let client_id = client_id_from_uuid(&client_id);
        let labels = RpcLabels::new("batch_upsert_end")
            .with_client(client_id)
            .with_tenant(&tenant_id);
        let owner = match self.clients.write_owner(client_id) {
            Ok(owner) => owner,
            Err(error) => {
                return Ok(observe_batch(
                    labels,
                    batch_error(item_count, client_lifecycle_error(error)),
                ));
            }
        };
        let mut output = batch_error(item_count, ErrorCode::InvalidParams);
        let valid: Vec<_> = object_metas
            .iter()
            .enumerate()
            .filter(|(_, metadata)| metadata.object_checksum.is_none())
            .map(|(index, metadata)| (index, metadata.key.as_str()))
            .collect();
        if valid.is_empty() {
            return Ok(observe_batch(labels, output));
        }
        let keys: Vec<_> = valid.iter().map(|(_, key)| *key).collect();
        let now = self.clock().now();
        let results = self
            .backend
            .execute_batch(&tenant_id, keys.len(), now, |backend, tenant| {
                backend.finish_put_batch(tenant, &keys, owner, ReplicaSelector::All)
            });
        debug_assert_eq!(valid.len(), results.len());
        for ((index, _), result) in valid.into_iter().zip(results) {
            output[index] = result;
        }
        Ok(observe_batch(labels, output))
    }

    async fn batch_upsert_revoke(
        &self,
        client_id: Uuid,
        keys: Vec<String>,
        tenant_id: String,
    ) -> Result<Vec<ExpectedVoid>, RpcFailure> {
        let item_count = keys.len();
        let client_id = client_id_from_uuid(&client_id);
        let labels = RpcLabels::new("batch_upsert_revoke")
            .with_client(client_id)
            .with_tenant(&tenant_id);
        let owner = match self.clients.write_owner(client_id) {
            Ok(owner) => owner,
            Err(error) => {
                return Ok(observe_batch(
                    labels,
                    batch_error(item_count, client_lifecycle_error(error)),
                ));
            }
        };
        let now = self.clock().now();
        let results =
            self.backend
                .execute_batch(&tenant_id, item_count, now, move |backend, tenant| {
                    backend.revoke_put_batch(tenant, &keys, owner, ReplicaSelector::All, now)
                });
        Ok(observe_batch(labels, results))
    }

    async fn remove(
        &self,
        key: String,
        force: bool,
        tenant_id: String,
    ) -> Result<ExpectedVoid, RpcFailure> {
        let labels = RpcLabels::new("remove").with_tenant(&tenant_id);
        let now = self.clock().now();
        let result = only_item(self.backend.execute_batch(
            &tenant_id,
            1,
            now,
            |backend, tenant| backend.remove_batch(tenant, std::slice::from_ref(&key), now, force),
        ));
        Ok(observe_result(labels, result))
    }

    async fn remove_by_regex(
        &self,
        regex: String,
        force: bool,
        tenant_id: String,
    ) -> Result<ExpectedI64, RpcFailure> {
        let labels = RpcLabels::new("remove_by_regex").with_tenant(&tenant_id);
        let pattern = match Regex::new(&regex) {
            Ok(pattern) => pattern,
            Err(_) => return Ok(observe_result(labels, Err(ErrorCode::InvalidParams))),
        };
        let now = self.clock().now();
        let removed =
            only_item(
                self.backend
                    .execute_batch(&tenant_id, 1, now, |backend, tenant| {
                        vec![
                            backend
                                .remove_matching(tenant, Some(&pattern), now, force)
                                .and_then(usize_to_i64),
                        ]
                    }),
            );
        Ok(observe_result(labels, removed))
    }

    async fn remove_all(&self, force: bool, tenant_id: String) -> Result<i64, RpcFailure> {
        let labels = RpcLabels::new("remove_all").with_tenant(&tenant_id);
        let now = self.clock().now();
        let removed = self
            .backend
            .remove_all(&tenant_id, now, force)
            .and_then(usize_to_i64);
        Ok(observe_result(labels, removed).unwrap_or(0))
    }

    async fn batch_remove(
        &self,
        keys: Vec<String>,
        force: bool,
        tenant_id: String,
    ) -> Result<Vec<ExpectedVoid>, RpcFailure> {
        let item_count = keys.len();
        let labels = RpcLabels::new("batch_remove").with_tenant(&tenant_id);
        let now = self.clock().now();
        let results = self
            .backend
            .execute_batch(&tenant_id, item_count, now, |backend, tenant| {
                backend.remove_batch(tenant, &keys, now, force)
            });
        Ok(observe_batch(labels, results))
    }
}

fn usize_to_i64(value: usize) -> Result<i64, ErrorCode> {
    i64::try_from(value).map_err(|_| ErrorCode::InternalError)
}

fn client_manager_error(error: ClientManagerError) -> ErrorCode {
    let error_code = match &error {
        ClientManagerError::Lifecycle(ClientLifecycleError::NilClientId) => {
            ErrorCode::InvalidParams
        }
        ClientManagerError::Lifecycle(ClientLifecycleError::CleanupInProgress { .. }) => {
            ErrorCode::UnavailableInCurrentStatus
        }
        ClientManagerError::Lifecycle(
            ClientLifecycleError::ClientNotActive
            | ClientLifecycleError::CleanupNotStarted
            | ClientLifecycleError::StaleSession,
        ) => ErrorCode::ClientNotFound,
        ClientManagerError::Lifecycle(
            ClientLifecycleError::CapacityExceeded { .. }
            | ClientLifecycleError::GenerationExhausted,
        )
        | ClientManagerError::InconsistentSlot
        | ClientManagerError::Rollback { .. } => ErrorCode::InternalError,
        ClientManagerError::SegmentUnavailable(_) => ErrorCode::UnavailableInCurrentStatus,
        ClientManagerError::SegmentState(
            crate::segment::error::SegmentStateError::OwnerMismatch { .. },
        ) => ErrorCode::InvalidParams,
        ClientManagerError::SegmentState(crate::segment::error::SegmentStateError::NotFound(
            segment,
        )) => {
            if segment.is_nil() {
                ErrorCode::InvalidParams
            } else {
                ErrorCode::SegmentNotFound
            }
        }
        ClientManagerError::SegmentState(
            crate::segment::error::SegmentStateError::StillAccepting(_)
            | crate::segment::error::SegmentStateError::Busy { .. },
        ) => ErrorCode::UnavailableInCurrentStatus,
        ClientManagerError::Attach(AttachError::ConflictingSegmentId(_)) => {
            ErrorCode::SegmentAlreadyExists
        }
        ClientManagerError::Attach(_) | ClientManagerError::ActiveRemountConflict => {
            ErrorCode::InvalidParams
        }
    };
    observe_internal_mapping("client_manager", &error, error_code);
    error_code
}

fn mount_segment_error(error: ClientManagerError) -> ErrorCode {
    match error {
        ClientManagerError::ActiveRemountConflict
        | ClientManagerError::Attach(AttachError::ConflictingSegmentId(_)) => {
            ErrorCode::SegmentAlreadyExists
        }
        error => client_manager_error(error),
    }
}

fn client_lifecycle_error(error: ClientLifecycleError) -> ErrorCode {
    client_manager_error(ClientManagerError::Lifecycle(error))
}

fn only_item<T>(mut items: Vec<Result<T, ErrorCode>>) -> Result<T, ErrorCode> {
    if items.len() == 1 {
        items.pop().expect("one-item batch contains one response")
    } else {
        Err(ErrorCode::InternalError)
    }
}

fn get_replica_list_response(
    read: Result<ObjectRead, ErrorCode>,
    now: crate::object::reclamation::CatalogTick,
) -> ExpectedGetReplicaListResponse {
    let read = read?;
    let replica_view = read.object().replicas();
    if replica_view.is_empty() {
        return Err(ErrorCode::ObjectNotFound);
    }
    let replicas = replica_view
        .iter()
        .map(|replica| replica_descriptor(replica, ReplicaStatus::Complete))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(GetReplicaListResponse {
        replicas,
        lease_ttl_ms: read.lease_expires_at().get().saturating_sub(now.get()),
        object_checksum: None,
    })
}
