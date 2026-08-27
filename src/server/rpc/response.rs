//! Domain-to-Mooncake response and error mapping.

use super::observability::observe_internal_mapping;
use crate::mooncake::{
    BufferDescriptor, DescriptorVariant, ErrorCode, MemoryDescriptor, NoFDescriptor,
    ReplicaDescriptor, ReplicaStatus,
};
use crate::object::error::{LookupError, ObjectManagerError, ObjectRemoveError};
use crate::object::{AllocatedReplica, ReplicaId, TenantObjectError};
use crate::segment::ReservationDescriptor;

pub(super) fn started_replica_descriptor(
    replica: &AllocatedReplica,
) -> Result<ReplicaDescriptor, ErrorCode> {
    Ok(ReplicaDescriptor {
        id: u64::from(replica.id().get()),
        descriptor_variant: owned_descriptor_variant(replica.descriptor())?,
        status: ReplicaStatus::Processing,
    })
}

pub(super) fn owned_replica_descriptor(
    id: ReplicaId,
    descriptor: &ReservationDescriptor,
    status: ReplicaStatus,
) -> Result<ReplicaDescriptor, ErrorCode> {
    Ok(ReplicaDescriptor {
        id: u64::from(id.get()),
        descriptor_variant: owned_descriptor_variant(descriptor)?,
        status,
    })
}

fn owned_descriptor_variant(
    descriptor: &ReservationDescriptor,
) -> Result<DescriptorVariant, ErrorCode> {
    let buffer_descriptor = buffer_descriptor(
        descriptor.region().base(),
        descriptor.region().size(),
        descriptor.transport().protocol().as_str(),
        descriptor.transport().endpoint(),
    );
    Ok(match descriptor {
        ReservationDescriptor::Memory(_) => {
            DescriptorVariant::Memory(MemoryDescriptor { buffer_descriptor })
        }
        ReservationDescriptor::Nof(_) => {
            DescriptorVariant::NofSsd(NoFDescriptor { buffer_descriptor })
        }
    })
}

fn buffer_descriptor(
    address: u64,
    size: u64,
    protocol: &str,
    transport_endpoint: &str,
) -> BufferDescriptor {
    BufferDescriptor {
        size,
        buffer_address: address,
        protocol: protocol.to_owned(),
        transport_endpoint: transport_endpoint.to_owned(),
    }
}

pub(super) fn map_lookup_error(error: LookupError) -> ErrorCode {
    match error {
        LookupError::NotFound => ErrorCode::ObjectNotFound,
        LookupError::NotReady => ErrorCode::ReplicaIsNotReady,
    }
}

pub(super) fn map_manager_error(error: ObjectManagerError) -> ErrorCode {
    let error_code = match error {
        ObjectManagerError::InvalidPlan => ErrorCode::InvalidParams,
        ObjectManagerError::AlreadyExists => ErrorCode::ObjectAlreadyExists,
        ObjectManagerError::NoAvailableReplicas => ErrorCode::NoAvailableHandle,
        ObjectManagerError::NotFound => ErrorCode::ObjectNotFound,
        ObjectManagerError::IllegalOwner => ErrorCode::IllegalClient,
        ObjectManagerError::ReplicaClassMismatch { .. } | ObjectManagerError::InvalidWrite => {
            ErrorCode::InvalidWrite
        }
        ObjectManagerError::Internal => ErrorCode::InternalError,
    };
    observe_internal_mapping("object_manager", &error, error_code);
    error_code
}

pub(super) fn map_remove_error(error: ObjectRemoveError) -> ErrorCode {
    match error {
        ObjectRemoveError::NotFound => ErrorCode::ObjectNotFound,
        ObjectRemoveError::NotReady => ErrorCode::ReplicaIsNotReady,
        ObjectRemoveError::Leased { .. } => ErrorCode::ObjectHasLease,
        // The upstream wire has no dedicated hard-pin removal error. Reuse
        // its existing "protected object" response for non-force removal.
        ObjectRemoveError::HardPinned => ErrorCode::ObjectHasLease,
    }
}

pub(super) fn map_tenant_error(error: TenantObjectError) -> ErrorCode {
    match error {
        TenantObjectError::TenantNotRegistered | TenantObjectError::InvalidTenantHandle => {
            ErrorCode::TenantNotRegistered
        }
        TenantObjectError::TenantQuotaExceeded { .. } => ErrorCode::TenantQuotaExceeded,
        TenantObjectError::Object(error) => map_manager_error(error),
    }
}
