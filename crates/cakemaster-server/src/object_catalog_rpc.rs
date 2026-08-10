use cakemaster::object::error::{LookupError, ObjectManagerError};
use cakemaster::object::reclamation::{CatalogTick, CollectBudget};
use cakemaster::object::{
    AllocatedReplica, NamespaceId, ObjectContent, ObjectIdentity, ObjectKind, ObjectManager,
    ObjectPutPlan, ReplicaSelector, WriteOwner,
};
use cakemaster::segment::placement::{
    AllocationSpec, FulfillmentPolicy, PlacementConstraints, PlacementRequest, ReplicaPolicy,
};
use cakemaster::segment::{
    ClientId, RangeDescriptor, ReplicaClass, ReservationDescriptor, ReservationDescriptorRef,
};
use cakemaster_proto::mooncake::{
    BufferDescriptor, DescriptorVariant, ErrorCode, ExpectedBool, ExpectedGetReplicaListResponse,
    ExpectedReplicaDescriptors, ExpectedVoid, GetReplicaListResponse, MemoryDescriptor,
    NoFDescriptor, ObjectDataType, ObjectMeta, ReplicaDescriptor, ReplicaStatus, ReplicaType,
    ReplicateConfig, Uuid, WrappedMasterService,
};
use coro_rpc::RpcFailure;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

pub struct ObjectCatalogRpcService {
    manager: Arc<ObjectManager>,
    epoch: Instant,
}

impl ObjectCatalogRpcService {
    pub fn new(manager: Arc<ObjectManager>) -> Self {
        Self {
            manager,
            epoch: Instant::now(),
        }
    }

    pub fn manager(&self) -> &Arc<ObjectManager> {
        &self.manager
    }

    fn now(&self) -> CatalogTick {
        let millis = self.epoch.elapsed().as_millis();
        CatalogTick::new(u64::try_from(millis).unwrap_or(u64::MAX))
    }

    fn maintain(&self, now: CatalogTick) {
        let _ = self.manager.maintenance(now, CollectBudget::default());
    }
}

impl WrappedMasterService for ObjectCatalogRpcService {
    async fn batch_exist_key(
        &self,
        keys: Vec<String>,
        _tenant_id: String,
    ) -> Result<Vec<ExpectedBool>, RpcFailure> {
        let now = self.now();
        self.maintain(now);
        Ok(keys
            .iter()
            .map(|key| {
                Ok(self.manager.exists(
                    ObjectIdentity::new(NamespaceId::DEFAULT, key.as_str()).as_lookup(),
                    now,
                ))
            })
            .collect())
    }

    async fn batch_get_replica_list(
        &self,
        keys: Vec<String>,
        _tenant_id: String,
    ) -> Result<Vec<ExpectedGetReplicaListResponse>, RpcFailure> {
        let now = self.now();
        self.maintain(now);
        Ok(keys
            .iter()
            .map(|key| {
                let identity = ObjectIdentity::new(NamespaceId::DEFAULT, key.as_str());
                let read = self
                    .manager
                    .get(identity.as_lookup(), now)
                    .map_err(map_lookup_error)?;
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
        _tenant_id: String,
    ) -> Result<Vec<ExpectedReplicaDescriptors>, RpcFailure> {
        let now = self.now();
        self.maintain(now);
        if keys.len() != slice_lengths.len() {
            return Ok(keys
                .into_iter()
                .map(|_| Err(ErrorCode::InvalidParams))
                .collect());
        }
        let template = match PutPlanTemplate::try_from(&config) {
            Ok(template) => template,
            Err(error) => {
                return Ok(keys.into_iter().map(|_| Err(error)).collect());
            }
        };
        let owner = WriteOwner::new(client_id_from_uuid(&client_id));

        Ok(keys
            .into_iter()
            .zip(slice_lengths)
            .map(|(key, logical_bytes)| {
                let identity = ObjectIdentity::new(NamespaceId::DEFAULT, key);
                let started = self
                    .manager
                    .start_put(identity, owner, template.plan(logical_bytes), now)
                    .map_err(map_manager_error)?;
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
        _tenant_id: String,
    ) -> Result<Vec<ExpectedVoid>, RpcFailure> {
        let now = self.now();
        self.maintain(now);
        let selector = match replica_selector(replica_type) {
            Ok(selector) => selector,
            Err(error) => {
                return Ok(object_metas.into_iter().map(|_| Err(error)).collect());
            }
        };
        let owner = WriteOwner::new(client_id_from_uuid(&client_id));
        Ok(object_metas
            .into_iter()
            .map(|metadata| {
                if metadata.object_checksum.is_some() {
                    return Err(ErrorCode::InvalidParams);
                }
                let identity = ObjectIdentity::new(NamespaceId::DEFAULT, metadata.key);
                self.manager
                    .finish_put(&identity, owner, selector)
                    .map_err(map_manager_error)
            })
            .collect())
    }

    async fn batch_put_revoke(
        &self,
        client_id: Uuid,
        keys: Vec<String>,
        replica_type: ReplicaType,
        _tenant_id: String,
    ) -> Result<Vec<ExpectedVoid>, RpcFailure> {
        let now = self.now();
        self.maintain(now);
        let selector = match replica_selector(replica_type) {
            Ok(selector) => selector,
            Err(error) => {
                return Ok(keys.into_iter().map(|_| Err(error)).collect());
            }
        };
        let owner = WriteOwner::new(client_id_from_uuid(&client_id));
        Ok(keys
            .into_iter()
            .map(|key| {
                let identity = ObjectIdentity::new(NamespaceId::DEFAULT, key);
                self.manager
                    .revoke_put(&identity, owner, selector, now)
                    .map_err(map_manager_error)
            })
            .collect())
    }
}

struct PutPlanTemplate {
    replica_class: ReplicaClass,
    replica_count: usize,
    fulfillment: FulfillmentPolicy,
    constraints: PlacementConstraints,
    object_kind: ObjectKind,
}

impl PutPlanTemplate {
    fn plan(&self, logical_bytes: u64) -> ObjectPutPlan {
        let placement = PlacementRequest::new(
            AllocationSpec::new(logical_bytes),
            ReplicaPolicy::new(self.replica_count),
        )
        .for_replica_class(self.replica_class)
        .with_fulfillment(self.fulfillment)
        .constrained_by(self.constraints.clone());
        ObjectPutPlan::new(
            ObjectContent::new(logical_bytes).with_kind(self.object_kind),
            placement,
        )
    }
}

impl TryFrom<&ReplicateConfig> for PutPlanTemplate {
    type Error = ErrorCode;

    fn try_from(config: &ReplicateConfig) -> Result<Self, Self::Error> {
        if config.with_soft_pin
            || config.with_hard_pin
            || config.prefer_alloc_in_same_node
            || !config.host_id.is_empty()
            || config.group_ids.is_some()
        {
            return Err(ErrorCode::InvalidParams);
        }

        let (replica_class, raw_count, fulfillment, preferred) =
            match (config.replica_num > 0, config.nof_replica_num > 0) {
                (true, false) if config.preferred_nof_segments.is_empty() => {
                    let preferred = if config.preferred_segment.is_empty() {
                        config.preferred_segments.clone()
                    } else {
                        vec![config.preferred_segment.clone()]
                    };
                    (
                        ReplicaClass::Memory,
                        config.replica_num,
                        FulfillmentPolicy::BestEffort,
                        preferred,
                    )
                }
                (false, true)
                    if config.preferred_segment.is_empty()
                        && config.preferred_segments.is_empty() =>
                {
                    (
                        ReplicaClass::Nof,
                        config.nof_replica_num,
                        FulfillmentPolicy::AllOrNothing,
                        config.preferred_nof_segments.clone(),
                    )
                }
                _ => return Err(ErrorCode::InvalidParams),
            };
        let replica_count = usize::try_from(raw_count).map_err(|_| ErrorCode::InvalidParams)?;
        if raw_count > u64::from(u32::MAX) {
            return Err(ErrorCode::InvalidParams);
        }

        Ok(Self {
            replica_class,
            replica_count,
            fulfillment,
            constraints: PlacementConstraints::default()
                .with_preferred_names(normalize_names(preferred)),
            object_kind: object_kind(config.data_type),
        })
    }
}

fn normalize_names(names: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::with_capacity(names.len());
    names
        .into_iter()
        .filter(|name| !name.is_empty() && seen.insert(name.clone()))
        .collect()
}

fn object_kind(data_type: ObjectDataType) -> ObjectKind {
    match data_type {
        ObjectDataType::Kvcache => ObjectKind::KvCache,
        ObjectDataType::Tensor => ObjectKind::Tensor,
        ObjectDataType::Unknown
        | ObjectDataType::Weight
        | ObjectDataType::Sample
        | ObjectDataType::Activation
        | ObjectDataType::Gradient
        | ObjectDataType::OptimizerState
        | ObjectDataType::Metadata
        | ObjectDataType::General => ObjectKind::General,
    }
}

fn replica_selector(replica_type: ReplicaType) -> Result<ReplicaSelector, ErrorCode> {
    match replica_type {
        ReplicaType::All => Ok(ReplicaSelector::All),
        ReplicaType::Memory => Ok(ReplicaSelector::Class(ReplicaClass::Memory)),
        ReplicaType::NofSsd => Ok(ReplicaSelector::Class(ReplicaClass::Nof)),
        ReplicaType::Disk | ReplicaType::LocalDisk => Err(ErrorCode::InvalidParams),
    }
}

fn client_id_from_uuid(client_id: &Uuid) -> ClientId {
    ClientId::new(client_id.high, client_id.low)
}

fn started_replica_descriptor(replica: &AllocatedReplica) -> Result<ReplicaDescriptor, ErrorCode> {
    Ok(ReplicaDescriptor {
        id: u64::from(replica.id().get()),
        descriptor_variant: owned_descriptor_variant(replica.descriptor())?,
        status: ReplicaStatus::Processing,
    })
}

fn replica_descriptor(
    replica: &cakemaster::object::ReplicaLease,
    status: ReplicaStatus,
) -> Result<ReplicaDescriptor, ErrorCode> {
    let Some(direct) = replica.direct() else {
        return Err(ErrorCode::InternalError);
    };
    let descriptor_variant = match direct.descriptor() {
        ReservationDescriptorRef::Memory(descriptor) => {
            DescriptorVariant::Memory(MemoryDescriptor {
                buffer_descriptor: buffer_descriptor(
                    descriptor.region().base(),
                    descriptor.region().size(),
                    descriptor.transport().protocol().as_str(),
                    descriptor.transport().endpoint(),
                ),
            })
        }
        ReservationDescriptorRef::Nof(descriptor) => DescriptorVariant::NofSsd(NoFDescriptor {
            buffer_descriptor: buffer_descriptor(
                descriptor.region().base(),
                descriptor.region().size(),
                descriptor.transport().protocol().as_str(),
                descriptor.transport().endpoint(),
            ),
        }),
        _ => return Err(ErrorCode::InternalError),
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
    Ok(match descriptor {
        ReservationDescriptor::Memory(descriptor) => DescriptorVariant::Memory(MemoryDescriptor {
            buffer_descriptor: range_buffer_descriptor(descriptor),
        }),
        ReservationDescriptor::Nof(descriptor) => DescriptorVariant::NofSsd(NoFDescriptor {
            buffer_descriptor: range_buffer_descriptor(descriptor),
        }),
        _ => return Err(ErrorCode::InternalError),
    })
}

fn range_buffer_descriptor(descriptor: &RangeDescriptor) -> BufferDescriptor {
    buffer_descriptor(
        descriptor.region().base(),
        descriptor.region().size(),
        descriptor.transport().protocol().as_str(),
        descriptor.transport().endpoint(),
    )
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

fn map_lookup_error(error: LookupError) -> ErrorCode {
    match error {
        LookupError::NotFound => ErrorCode::ObjectNotFound,
        LookupError::NotReady => ErrorCode::ReplicaIsNotReady,
    }
}

fn map_manager_error(error: ObjectManagerError) -> ErrorCode {
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
