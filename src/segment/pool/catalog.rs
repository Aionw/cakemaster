use super::entry::SegmentEntry;
use super::resource::{CandidateCapability, ResourceRegistry, TransferableExtent};
use super::{
    AttachOutcome, DirectCandidate, OffloadSnapshot, OffloadTarget, PoolSnapshot, SegmentHandle,
    SegmentTopologyEvent,
};
use crate::segment::error::{AttachError, LocalSsdError, SegmentStateError};
use crate::segment::identity::{ClientId, SegmentId};
use crate::segment::spec::{ReplicaClass, SegmentSpec};
use crate::segment::stats::ReplicaClassSpaceStats;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(super) struct Catalog {
    pool_id: u64,
    segments: HashMap<SegmentId, Arc<SegmentEntry>>,
    segments_by_owner: HashMap<ClientId, HashSet<SegmentId>>,
    resources: ResourceRegistry,
    indexes: CandidateIndexes,
}

#[derive(Default)]
struct CandidateIndexes {
    direct_generation: u64,
    direct: HashMap<ReplicaClass, Arc<[DirectCandidate]>>,
    offload_generation: u64,
    offload: Arc<[OffloadTarget]>,
}

impl Catalog {
    pub(super) fn new(pool_id: u64) -> Self {
        Self {
            pool_id,
            segments: HashMap::new(),
            segments_by_owner: HashMap::new(),
            resources: ResourceRegistry::default(),
            indexes: CandidateIndexes::default(),
        }
    }

    pub(super) fn attach(
        &mut self,
        spec: SegmentSpec,
        max_allocator_nodes: u32,
        allocator_shards: usize,
        allocator_shard_index: Option<usize>,
    ) -> Result<AttachOutcome, AttachError> {
        if let Some(existing) = self.segment(spec.identity().id()) {
            return if existing.spec() == &spec {
                Ok(AttachOutcome::AlreadyAttached(existing))
            } else {
                Err(AttachError::ConflictingSegmentId(spec.identity().id()))
            };
        }

        let resource = self.resources.mount(
            &spec,
            self.segments.values().map(|entry| entry.spec()),
            max_allocator_nodes,
            allocator_shards,
            allocator_shard_index,
        )?;
        let entry = Arc::new(SegmentEntry::new(
            Arc::new(spec),
            resource,
            allocator_shard_index.is_some(),
        ));
        let segment = self.handle(entry.clone());
        self.segments.insert(segment.id(), entry);
        self.segments_by_owner
            .entry(segment.spec().identity().owner())
            .or_default()
            .insert(segment.id());
        self.rebuild_indexes();
        Ok(AttachOutcome::Attached(segment))
    }

    pub(super) fn snapshot(&self, replica_class: ReplicaClass) -> PoolSnapshot {
        PoolSnapshot {
            generation: self.indexes.direct_generation,
            replica_class,
            candidates: self
                .indexes
                .direct
                .get(&replica_class)
                .cloned()
                .unwrap_or_else(|| Arc::from([])),
        }
    }

    pub(super) fn space_for(&self, replica_class: ReplicaClass) -> ReplicaClassSpaceStats {
        self.space_for_shard(replica_class, None)
    }

    pub(super) fn space_for_shard(
        &self,
        replica_class: ReplicaClass,
        allocator_shard: Option<usize>,
    ) -> ReplicaClassSpaceStats {
        let mut resources = HashSet::new();
        let mut capacity_bytes = 0_u64;
        let mut used_bytes = 0_u64;
        let mut available_bytes = 0_u64;
        let mut largest_free_region_bytes = 0_u64;
        for entry in self
            .segments
            .values()
            .filter(|entry| entry.spec().replica_class() == replica_class)
        {
            if !resources.insert(entry.spec().resource_id()) {
                continue;
            }
            let space = allocator_shard
                .map_or_else(|| entry.stats().space, |shard| entry.space_for_shard(shard));
            capacity_bytes = capacity_bytes.saturating_add(space.capacity_bytes);
            used_bytes = used_bytes.saturating_add(space.used_bytes);
            available_bytes = available_bytes.saturating_add(space.available_bytes);
            largest_free_region_bytes =
                largest_free_region_bytes.max(space.largest_free_region_bytes);
        }
        ReplicaClassSpaceStats {
            generation: self.indexes.direct_generation,
            capacity_bytes,
            used_bytes,
            available_bytes,
            largest_free_region_bytes,
        }
    }

    pub(super) fn offload_snapshot(&self) -> OffloadSnapshot {
        OffloadSnapshot {
            generation: self.indexes.offload_generation,
            targets: self.indexes.offload.clone(),
        }
    }

    pub(super) fn segment(&self, id: SegmentId) -> Option<SegmentHandle> {
        self.segments
            .get(&id)
            .cloned()
            .map(|entry| self.handle(entry))
    }

    pub(super) fn len(&self) -> usize {
        self.segments.len()
    }

    pub(super) fn take_empty_extent(
        &self,
        replica_class: ReplicaClass,
        allocator_shard: usize,
        minimum_bytes: u64,
    ) -> Option<(SegmentId, TransferableExtent)> {
        let mut segments = self
            .segments
            .iter()
            .filter(|(_, entry)| entry.spec().replica_class() == replica_class)
            .collect::<Vec<_>>();
        segments.sort_unstable_by_key(|(id, _)| **id);
        segments.into_iter().find_map(|(id, entry)| {
            entry
                .take_empty_extent(allocator_shard, minimum_bytes)
                .map(|extent| (*id, extent))
        })
    }

    pub(super) fn add_extent(
        &self,
        segment: SegmentId,
        allocator_shard: usize,
        extent: TransferableExtent,
    ) {
        self.segments
            .get(&segment)
            .expect("capacity transfer retains matching topology on every shard")
            .add_extent(allocator_shard, extent)
            .then_some(())
            .expect("capacity transfer targets an accepting direct segment");
    }

    pub(super) fn topology_events(&self) -> Vec<SegmentTopologyEvent> {
        let mut events = Vec::with_capacity(self.segments.len());
        for entry in self.segments.values() {
            events.push(SegmentTopologyEvent::Attach {
                spec: entry.spec().clone(),
                state: entry.stats().state,
            });
            if let Some(stats) = entry.local_ssd_stats() {
                events.push(SegmentTopologyEvent::SetLocalSsdOffloadEnabled {
                    owner: entry.spec().identity().owner(),
                    id: entry.spec().identity().id(),
                    enabled: stats.offload_enabled,
                });
                if stats.capacity_reported {
                    events.push(SegmentTopologyEvent::ReportLocalSsdCapacity {
                        owner: entry.spec().identity().owner(),
                        id: entry.spec().identity().id(),
                        capacity_bytes: stats.capacity_bytes,
                    });
                }
            }
        }
        events
    }

    pub(super) fn report_local_ssd_capacity(
        &self,
        owner: ClientId,
        id: SegmentId,
        capacity_bytes: u64,
    ) -> Result<(), LocalSsdError> {
        self.owned_offload_entry(owner, id)?
            .report_local_ssd_capacity(capacity_bytes)
    }

    pub(super) fn set_local_ssd_offload_enabled(
        &mut self,
        owner: ClientId,
        id: SegmentId,
        enabled: bool,
    ) -> Result<(), LocalSsdError> {
        self.owned_offload_entry(owner, id)?
            .set_local_ssd_offload_enabled(enabled)?;
        self.rebuild_indexes();
        Ok(())
    }

    pub(super) fn quiesce(
        &mut self,
        owner: ClientId,
        id: SegmentId,
    ) -> Result<(), SegmentStateError> {
        self.owned_entry(owner, id)?.quiesce();
        self.rebuild_indexes();
        Ok(())
    }

    pub(super) fn reactivate(
        &mut self,
        owner: ClientId,
        id: SegmentId,
    ) -> Result<(), SegmentStateError> {
        self.owned_entry(owner, id)?.reactivate();
        self.rebuild_indexes();
        Ok(())
    }

    pub(super) fn reactivate_many(
        &mut self,
        owner: ClientId,
        ids: &[SegmentId],
    ) -> Result<(), SegmentStateError> {
        let entries = ids
            .iter()
            .map(|id| self.owned_entry(owner, *id))
            .collect::<Result<Vec<_>, _>>()?;
        for entry in entries {
            entry.reactivate();
        }
        self.rebuild_indexes();
        Ok(())
    }

    pub(super) fn remove(
        &mut self,
        owner: ClientId,
        id: SegmentId,
    ) -> Result<(), SegmentStateError> {
        let entry = self.owned_entry(owner, id)?;
        entry.prepare_remove()?;
        let removed = self
            .segments
            .remove(&id)
            .expect("owned entries remain registered while the catalog is write-locked");
        self.remove_owner_segment(owner, id);
        self.resources.unmount(removed.spec());
        self.rebuild_indexes();
        Ok(())
    }

    /// Invalidates and detaches every segment owned by a fenced client.
    /// Outstanding leases retain their resource handles but are logically
    /// unusable immediately.
    pub(super) fn invalidate_owners(
        &mut self,
        owners: impl IntoIterator<Item = ClientId>,
    ) -> usize {
        let mut invalidated = 0;

        for owner in owners {
            let Some(ids) = self.segments_by_owner.remove(&owner) else {
                continue;
            };
            for id in ids {
                let removed = self
                    .segments
                    .remove(&id)
                    .expect("the owner index only contains mounted segments");
                removed.invalidate();
                self.resources.unmount(removed.spec());
                invalidated += 1;
            }
        }
        if invalidated != 0 {
            self.rebuild_indexes();
        }
        invalidated
    }

    fn remove_owner_segment(&mut self, owner: ClientId, id: SegmentId) {
        let remove_owner = self
            .segments_by_owner
            .get_mut(&owner)
            .is_some_and(|segments| {
                let removed = segments.remove(&id);
                debug_assert!(removed, "mounted segments remain indexed by owner");
                segments.is_empty()
            });
        if remove_owner {
            self.segments_by_owner.remove(&owner);
        }
    }

    fn handle(&self, entry: Arc<SegmentEntry>) -> SegmentHandle {
        SegmentHandle {
            pool_id: self.pool_id,
            entry,
        }
    }

    fn owned_entry(
        &self,
        owner: ClientId,
        id: SegmentId,
    ) -> Result<Arc<SegmentEntry>, SegmentStateError> {
        let entry = self
            .segments
            .get(&id)
            .cloned()
            .ok_or(SegmentStateError::NotFound(id))?;
        if entry.spec().identity().owner() != owner {
            return Err(SegmentStateError::OwnerMismatch {
                segment: id,
                expected: entry.spec().identity().owner(),
                actual: owner,
            });
        }
        Ok(entry)
    }

    fn owned_offload_entry(
        &self,
        owner: ClientId,
        id: SegmentId,
    ) -> Result<Arc<SegmentEntry>, LocalSsdError> {
        let entry = self
            .segments
            .get(&id)
            .cloned()
            .ok_or(LocalSsdError::NotFound(id))?;
        let expected = entry.spec().identity().owner();
        if expected != owner {
            return Err(LocalSsdError::OwnerMismatch {
                segment: id,
                expected,
                actual: owner,
            });
        }
        if !entry.supports_offload() {
            return Err(LocalSsdError::NotLocalSsd(id));
        }
        Ok(entry)
    }

    fn rebuild_indexes(&mut self) {
        let mut direct: HashMap<_, Vec<_>> = HashMap::new();
        let mut offload = Vec::new();

        for entry in self.segments.values() {
            let Some(capability) = entry.candidate_capability() else {
                continue;
            };
            let segment = self.handle(entry.clone());
            match capability {
                CandidateCapability::Direct(replica_class) => direct
                    .entry(replica_class)
                    .or_default()
                    .push(DirectCandidate { segment }),
                CandidateCapability::Offload => offload.push(OffloadTarget { segment }),
            }
        }

        for candidates in direct.values_mut() {
            candidates.sort_unstable_by_key(|candidate| candidate.id());
        }
        offload.sort_unstable_by_key(|target| target.id());

        if !same_direct_index(&direct, &self.indexes.direct) {
            self.indexes.direct = direct
                .into_iter()
                .map(|(class, candidates)| (class, Arc::from(candidates)))
                .collect();
            self.indexes.direct_generation = self.indexes.direct_generation.wrapping_add(1);
        }
        if !same_offload_index(&offload, &self.indexes.offload) {
            self.indexes.offload = Arc::from(offload);
            self.indexes.offload_generation = self.indexes.offload_generation.wrapping_add(1);
        }
    }
}

impl Drop for Catalog {
    fn drop(&mut self) {
        for entry in self.segments.values() {
            entry.invalidate();
        }
    }
}

fn same_direct_index(
    left: &HashMap<ReplicaClass, Vec<DirectCandidate>>,
    right: &HashMap<ReplicaClass, Arc<[DirectCandidate]>>,
) -> bool {
    left.len() == right.len()
        && left.iter().all(|(class, candidates)| {
            right.get(class).is_some_and(|current| {
                candidates.len() == current.len()
                    && candidates
                        .iter()
                        .zip(current.iter())
                        .all(|(left, right)| Arc::ptr_eq(&left.entry, &right.entry))
            })
        })
}

fn same_offload_index(left: &[OffloadTarget], right: &[OffloadTarget]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| Arc::ptr_eq(&left.entry, &right.entry))
}
