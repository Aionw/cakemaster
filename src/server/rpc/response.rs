//! Domain-to-Mooncake response and error mapping.

use crate::mooncake::{
    BufferDescriptor, DescriptorVariant, ErrorCode, MemoryDescriptor, NoFDescriptor,
    ReplicaDescriptor, ReplicaStatus,
};
use crate::object::error::{LookupError, ObjectManagerError};
use crate::object::{AllocatedReplica, ReplicaLease, TenantObjectError};
use crate::segment::{ReservationDescriptor, ReservationDescriptorRef};

pub(super) fn started_replica_descriptor(
    replica: &AllocatedReplica,
) -> Result<ReplicaDescriptor, ErrorCode> {
    Ok(ReplicaDescriptor {
        id: u64::from(replica.id().get()),
        descriptor_variant: owned_descriptor_variant(replica.descriptor())?,
        status: ReplicaStatus::Processing,
    })
}

pub(super) fn replica_descriptor(
    replica: &ReplicaLease,
    status: ReplicaStatus,
) -> Result<ReplicaDescriptor, ErrorCode> {
    let Some(direct) = replica.direct() else {
        return Err(ErrorCode::InternalError);
    };
    let descriptor = direct.descriptor();
    let buffer_descriptor = buffer_descriptor(
        descriptor.region().base(),
        descriptor.region().size(),
        descriptor.transport().protocol().as_str(),
        descriptor.transport().endpoint(),
    );
    let descriptor_variant = match descriptor {
        ReservationDescriptorRef::Memory(_) => {
            DescriptorVariant::Memory(MemoryDescriptor { buffer_descriptor })
        }
        ReservationDescriptorRef::Nof(_) => {
            DescriptorVariant::NofSsd(NoFDescriptor { buffer_descriptor })
        }
    };
    Ok(ReplicaDescriptor {
        id: u64::from(replica.id().get()),
        descriptor_variant,
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
    match error {
        ObjectManagerError::InvalidPlan => ErrorCode::InvalidParams,
        ObjectManagerError::AlreadyExists => ErrorCode::ObjectAlreadyExists,
        ObjectManagerError::NoAvailableReplicas => ErrorCode::NoAvailableHandle,
        ObjectManagerError::NotFound => ErrorCode::ObjectNotFound,
        ObjectManagerError::IllegalOwner => ErrorCode::IllegalClient,
        ObjectManagerError::ReplicaClassMismatch { .. } | ObjectManagerError::InvalidWrite => {
            ErrorCode::InvalidWrite
        }
        ObjectManagerError::Internal => ErrorCode::InternalError,
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
