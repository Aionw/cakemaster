//! Shared batch backend boundary for RPC handlers.

use crate::mooncake::{ErrorCode, ExpectedBool, ExpectedVoid};
use crate::object::reclamation::{CatalogTick, CollectBudget};
use crate::object::{
    ObjectRead, ReplicaSelector, StartedPut, TenantPutRequest, WriteAdmission, WriteOwner,
};
use regex::Regex;

/// Private adapter boundary: core managers remain concrete, while the RPC
/// service is monomorphized over one batch-capable backend.
pub(super) trait ObjectBatchBackend: Send + Sync + 'static {
    type Tenant: Send;

    /// Resolves the tenant once and fans an error out to every batch item. The
    /// generic operation remains statically dispatched.
    fn execute_batch<T>(
        &self,
        tenant_id: &str,
        item_count: usize,
        operation: impl FnOnce(&Self, &Self::Tenant) -> Vec<Result<T, ErrorCode>>,
    ) -> Vec<Result<T, ErrorCode>>
    where
        Self: Sized,
    {
        match self.resolve_tenant(tenant_id) {
            Ok(tenant) => operation(self, &tenant),
            Err(error) => batch_error(item_count, error),
        }
    }

    fn resolve_tenant(&self, tenant_id: &str) -> Result<Self::Tenant, ErrorCode>;
    fn exists_batch(
        &self,
        tenant: &Self::Tenant,
        keys: &[String],
        now: CatalogTick,
    ) -> Vec<ExpectedBool>;
    fn get_batch(
        &self,
        tenant: &Self::Tenant,
        keys: &[String],
        now: CatalogTick,
    ) -> Vec<Result<ObjectRead, ErrorCode>>;
    fn start_put_batch(
        &self,
        tenant: &Self::Tenant,
        admission: WriteAdmission,
        requests: Vec<TenantPutRequest>,
        now: CatalogTick,
    ) -> Vec<Result<StartedPut, ErrorCode>>;
    fn start_upsert_batch(
        &self,
        tenant: &Self::Tenant,
        admission: WriteAdmission,
        requests: Vec<TenantPutRequest>,
        now: CatalogTick,
    ) -> Vec<Result<StartedPut, ErrorCode>>;
    fn finish_put_batch(
        &self,
        tenant: &Self::Tenant,
        keys: &[&str],
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Vec<ExpectedVoid>;
    fn revoke_put_batch(
        &self,
        tenant: &Self::Tenant,
        keys: &[String],
        owner: WriteOwner,
        selector: ReplicaSelector,
        now: CatalogTick,
    ) -> Vec<ExpectedVoid>;
    fn remove_batch(
        &self,
        tenant: &Self::Tenant,
        keys: &[String],
        now: CatalogTick,
        force: bool,
    ) -> Vec<ExpectedVoid>;
    fn remove_matching(
        &self,
        tenant: &Self::Tenant,
        pattern: Option<&Regex>,
        now: CatalogTick,
        force: bool,
    ) -> Result<usize, ErrorCode>;
    fn remove_all(
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
