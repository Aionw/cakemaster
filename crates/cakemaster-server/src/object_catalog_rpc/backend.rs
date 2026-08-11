use cakemaster::object::reclamation::CatalogTick;
use cakemaster::object::{ObjectRead, ReplicaSelector, StartedPut, TenantPutRequest, WriteOwner};
use cakemaster_proto::mooncake::{ErrorCode, ExpectedBool, ExpectedVoid};

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
        self.maintain(now);
        match self.resolve_tenant(tenant_id) {
            Ok(tenant) => operation(self, &tenant),
            Err(error) => batch_error(item_count, error),
        }
    }

    fn maintain(&self, now: CatalogTick);
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
        owner: WriteOwner,
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
}

pub(super) fn batch_error<T>(item_count: usize, error: ErrorCode) -> Vec<Result<T, ErrorCode>> {
    (0..item_count).map(|_| Err(error)).collect()
}
