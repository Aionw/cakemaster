//! Shared batch backend boundary for RPC handlers.

use crate::mooncake::{ErrorCode, ExpectedBool, ExpectedVoid};
use crate::object::reclamation::{CatalogTick, CollectBudget};
use crate::object::{
    ObjectRead, ReplicaSelector, ReplicaSnapshot, ReplicaSnapshotSet, StartedPut, TenantPutRequest,
    WriteAdmission, WriteOwner,
};
use regex::Regex;

#[derive(Clone)]
pub(super) struct ObjectReadSnapshot {
    pub(super) replicas: ReplicaSnapshotSet,
    pub(super) lease_expires_at: CatalogTick,
}

pub(super) fn snapshot_object_read(read: ObjectRead) -> Result<ObjectReadSnapshot, ErrorCode> {
    let replicas = read.object().replica_snapshot();
    if !replicas.iter().any(ReplicaSnapshot::is_live) {
        return Err(ErrorCode::ObjectNotFound);
    }
    Ok(ObjectReadSnapshot {
        replicas,
        lease_expires_at: read.lease_expires_at(),
    })
}

/// Private adapter boundary: core managers remain concrete, while the RPC
/// service is monomorphized over one batch-capable backend.
#[async_trait::async_trait]
pub(super) trait ObjectBatchBackend: Send + Sync + 'static {
    type Tenant: Send + Sync;

    async fn execute_batch<T, F>(
        &self,
        tenant_id: &str,
        item_count: usize,
        operation: F,
    ) -> Vec<Result<T, ErrorCode>>
    where
        Self: Sized,
        T: Send,
        F: for<'a> FnOnce(
                &'a Self,
                Self::Tenant,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Vec<Result<T, ErrorCode>>> + Send + 'a>,
            > + Send,
    {
        match self.resolve_tenant(tenant_id) {
            Ok(tenant) => operation(self, tenant).await,
            Err(error) => batch_error(item_count, error),
        }
    }

    fn resolve_tenant(&self, tenant_id: &str) -> Result<Self::Tenant, ErrorCode>;
    async fn exists_batch(
        &self,
        tenant: Self::Tenant,
        keys: Vec<String>,
        now: CatalogTick,
    ) -> Vec<ExpectedBool>;
    async fn get_batch(
        &self,
        tenant: Self::Tenant,
        keys: Vec<String>,
        now: CatalogTick,
    ) -> Vec<Result<ObjectReadSnapshot, ErrorCode>>;
    async fn start_put_batch(
        &self,
        tenant: Self::Tenant,
        admission: WriteAdmission,
        requests: Vec<TenantPutRequest>,
        now: CatalogTick,
    ) -> Vec<Result<StartedPut, ErrorCode>>;
    async fn start_upsert_batch(
        &self,
        tenant: Self::Tenant,
        admission: WriteAdmission,
        requests: Vec<TenantPutRequest>,
        now: CatalogTick,
    ) -> Vec<Result<StartedPut, ErrorCode>>;
    async fn finish_put_batch(
        &self,
        tenant: Self::Tenant,
        keys: Vec<String>,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Vec<ExpectedVoid>;
    async fn revoke_put_batch(
        &self,
        tenant: Self::Tenant,
        keys: Vec<String>,
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Vec<ExpectedVoid>;
    async fn remove_batch(
        &self,
        tenant: Self::Tenant,
        keys: Vec<String>,
        now: CatalogTick,
        force: bool,
    ) -> Vec<ExpectedVoid>;
    async fn remove_matching(
        &self,
        tenant: Self::Tenant,
        pattern: Option<Regex>,
        now: CatalogTick,
        force: bool,
    ) -> Result<usize, ErrorCode>;
    async fn remove_all(
        &self,
        tenant_id: &str,
        now: CatalogTick,
        force: bool,
    ) -> Result<usize, ErrorCode>;
}

pub(super) fn batch_error<T>(item_count: usize, error: ErrorCode) -> Vec<Result<T, ErrorCode>> {
    (0..item_count).map(|_| Err(error)).collect()
}

pub(super) fn mutation_maintenance_budget(item_count: usize) -> CollectBudget {
    let baseline = CollectBudget::default();
    CollectBudget::new(
        baseline.max_candidates().max(item_count),
        baseline.max_reclaims(),
        baseline.max_empty_slots(),
    )
}
