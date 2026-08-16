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

    /// Runs the common batch prologue once and fans a tenant-resolution error
    /// out to every item. The generic closure remains statically dispatched.
    fn execute_batch<T>(
        &self,
        tenant_id: &str,
        item_count: usize,
        now: CatalogTick,
        operation: impl FnOnce(&Self, &Self::Tenant) -> Vec<Result<T, ErrorCode>>,
    ) -> Vec<Result<T, ErrorCode>>
    where
        Self: Sized,
    {
        self.maintain(now, item_count);
        match self.resolve_tenant(tenant_id) {
            Ok(tenant) => operation(self, &tenant),
            Err(error) => batch_error(item_count, error),
        }
    }

    fn maintain(&self, now: CatalogTick, item_count: usize);
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

pub(super) fn batch_maintenance_budget(item_count: usize) -> CollectBudget {
    let baseline = CollectBudget::default();
    CollectBudget::new(
        baseline.max_candidates().max(item_count),
        baseline.max_reclaims(),
        baseline.max_empty_slots(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_candidate_budget_keeps_up_with_batch_width() {
        let baseline = CollectBudget::default();
        assert_eq!(
            batch_maintenance_budget(0).max_candidates(),
            baseline.max_candidates()
        );
        assert_eq!(batch_maintenance_budget(333).max_candidates(), 333);
        assert_eq!(
            batch_maintenance_budget(333).max_reclaims(),
            baseline.max_reclaims()
        );
    }
}
