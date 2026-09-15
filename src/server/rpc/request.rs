//! Mooncake wire request validation and domain normalization.

use crate::mooncake::{
    ErrorCode, ObjectDataType, ReplicaType, ReplicateConfig, Segment,
    SoftPinAction as WireSoftPinAction, Uuid,
};
use crate::object::{
    ObjectContent, ObjectKind, ObjectPinRequest, ObjectPutPlan, ReplicaSelector, SoftPinAction,
};
use crate::segment::placement::{
    AllocationSpec, FulfillmentPolicy, PlacementConstraints, PlacementRequest, ReplicaPolicy,
};
use crate::segment::{
    ClientId, MemoryRegion, ReplicaClass, SegmentId, SegmentIdentity, SegmentSpec,
    TransportEndpoint, TransportProtocol,
};
use std::collections::HashSet;

pub(super) struct PutPlanTemplate {
    replica_count: usize,
    constraints: PlacementConstraints,
    object_kind: ObjectKind,
    pins: ObjectPinRequest,
}

impl PutPlanTemplate {
    pub(super) fn plan(&self, logical_bytes: u64) -> ObjectPutPlan {
        let placement = PlacementRequest::new(
            AllocationSpec::new(logical_bytes),
            ReplicaPolicy::new(self.replica_count),
        )
        .with_fulfillment(FulfillmentPolicy::BestEffort)
        .constrained_by(self.constraints.clone());
        ObjectPutPlan::new(
            ObjectContent::new(logical_bytes).with_kind(self.object_kind),
            placement,
        )
        .with_pins(self.pins)
    }
}

impl TryFrom<&ReplicateConfig> for PutPlanTemplate {
    type Error = ErrorCode;

    fn try_from(config: &ReplicateConfig) -> Result<Self, Self::Error> {
        if config.prefer_alloc_in_same_node
            || !config.host_id.is_empty()
            || config.group_ids.is_some()
        {
            return Err(ErrorCode::InvalidParams);
        }

        if config.replica_num == 0
            || config.replica_num > u64::from(u32::MAX)
            || config.nof_replica_num != 0
            || !config.preferred_nof_segments.is_empty()
        {
            return Err(ErrorCode::InvalidParams);
        }
        let replica_count =
            usize::try_from(config.replica_num).map_err(|_| ErrorCode::InvalidParams)?;
        let preferred = if config.preferred_segment.is_empty() {
            config.preferred_segments.clone()
        } else {
            vec![config.preferred_segment.clone()]
        };
        let soft_pin_action = match config.soft_pin_action {
            WireSoftPinAction::Preserve => SoftPinAction::Preserve,
            WireSoftPinAction::Enable => SoftPinAction::Enable,
            WireSoftPinAction::Disable => SoftPinAction::Disable,
        };
        if soft_pin_action != SoftPinAction::Enable && config.soft_pin_ttl_ms.is_some() {
            return Err(ErrorCode::InvalidParams);
        }

        Ok(Self {
            replica_count,
            constraints: PlacementConstraints::default()
                .with_preferred_names(normalize_names(preferred)),
            object_kind: object_kind(config.data_type),
            pins: ObjectPinRequest::new(
                soft_pin_action,
                config.soft_pin_ttl_ms,
                config.with_hard_pin,
            ),
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

pub(super) fn replica_selector(replica_type: ReplicaType) -> Result<ReplicaSelector, ErrorCode> {
    match replica_type {
        ReplicaType::All => Ok(ReplicaSelector::All),
        ReplicaType::Memory => Ok(ReplicaSelector::Class(ReplicaClass::Memory)),
        ReplicaType::NofSsd | ReplicaType::Disk | ReplicaType::LocalDisk => {
            Err(ErrorCode::InvalidParams)
        }
    }
}

pub(super) fn client_id_from_uuid(client_id: &Uuid) -> ClientId {
    ClientId::new(client_id.high, client_id.low)
}

pub(super) fn segment_id_from_uuid(segment_id: &Uuid) -> SegmentId {
    SegmentId::new(segment_id.high, segment_id.low)
}

pub(super) fn segment_spec_from_wire(
    segment: Segment,
    owner: ClientId,
) -> Result<SegmentSpec, ErrorCode> {
    let protocol = segment
        .protocol
        .parse::<TransportProtocol>()
        .map_err(|_| ErrorCode::InvalidParams)?;
    let identity = SegmentIdentity::new(
        SegmentId::new(segment.id.high, segment.id.low),
        owner,
        segment.name,
    )
    .with_host_id(segment.host_id);
    match protocol {
        TransportProtocol::Cxl | TransportProtocol::NvmeOf => Err(ErrorCode::InvalidParams),
        protocol => Ok(SegmentSpec::memory(
            identity,
            MemoryRegion::new(segment.base, segment.size),
            TransportEndpoint::new(protocol, segment.te_endpoint),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(replica_num: u64, nof_replica_num: u64) -> ReplicateConfig {
        ReplicateConfig {
            replica_num,
            nof_replica_num,
            soft_pin_action: WireSoftPinAction::Preserve,
            soft_pin_ttl_ms: None,
            with_hard_pin: false,
            preferred_segments: Vec::new(),
            preferred_segment: String::new(),
            preferred_nof_segments: Vec::new(),
            prefer_alloc_in_same_node: false,
            data_type: ObjectDataType::Kvcache,
            host_id: String::new(),
            group_ids: None,
        }
    }

    #[test]
    fn put_template_normalizes_a_supported_memory_request() {
        let mut config = config(2, 0);
        config.preferred_segments = vec!["memory-a".into(), "".into(), "memory-a".into()];
        let template = PutPlanTemplate::try_from(&config).unwrap();

        assert_eq!(template.replica_count, 2);
        assert_eq!(template.object_kind, ObjectKind::KvCache);
        assert_eq!(normalize_names(config.preferred_segments), vec!["memory-a"]);

        let plan = template.plan(4096);
        assert_eq!(plan.content().logical_bytes(), 4096);
        assert_eq!(plan.placement().allocation().bytes(), 4096);
        assert_eq!(plan.placement().replicas().count(), 2);
        assert_eq!(plan.placement().replica_class(), ReplicaClass::Memory);
        assert_eq!(
            plan.placement().fulfillment(),
            FulfillmentPolicy::BestEffort
        );
    }

    #[test]
    fn put_template_rejects_unsupported_shapes() {
        assert!(PutPlanTemplate::try_from(&config(0, 2)).is_err());
        assert!(PutPlanTemplate::try_from(&config(0, 0)).is_err());
        assert!(PutPlanTemplate::try_from(&config(1, 1)).is_err());
        let mut preferred_nof = config(1, 0);
        preferred_nof
            .preferred_nof_segments
            .push("unsupported".into());
        assert!(PutPlanTemplate::try_from(&preferred_nof).is_err());
        let mut pinned = config(1, 0);
        pinned.soft_pin_action = WireSoftPinAction::Enable;
        assert!(PutPlanTemplate::try_from(&pinned).is_ok());

        let mut disabled = config(1, 0);
        disabled.soft_pin_action = WireSoftPinAction::Disable;
        assert!(PutPlanTemplate::try_from(&disabled).is_ok());

        let mut ttl_without_enable = config(1, 0);
        ttl_without_enable.soft_pin_ttl_ms = Some(1_000);
        assert_eq!(
            PutPlanTemplate::try_from(&ttl_without_enable).err(),
            Some(ErrorCode::InvalidParams)
        );
    }

    #[test]
    fn replica_selector_rejects_unsupported_disk_requests() {
        assert_eq!(replica_selector(ReplicaType::All), Ok(ReplicaSelector::All));
        assert_eq!(
            replica_selector(ReplicaType::Memory),
            Ok(ReplicaSelector::Class(ReplicaClass::Memory))
        );
        for kind in [
            ReplicaType::NofSsd,
            ReplicaType::Disk,
            ReplicaType::LocalDisk,
        ] {
            assert_eq!(replica_selector(kind), Err(ErrorCode::InvalidParams));
        }
    }
}
