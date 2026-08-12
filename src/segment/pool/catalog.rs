use super::entry::SegmentEntry;
use super::resource::{CandidateCapability, ResourceRegistry};
use super::{
    AttachOutcome, DirectCandidate, OffloadSnapshot, OffloadTarget, PoolSnapshot, SegmentHandle,
};
use crate::segment::error::{AttachError, LocalSsdError, SegmentStateError};
use crate::segment::identity::{ClientId, SegmentId};
use crate::segment::spec::{ReplicaClass, SegmentSpec};
use std::collections::HashMap;
use std::sync::Arc;

pub(super) struct Catalog {
    pool_id: u64,
    segments: HashMap<SegmentId, Arc<SegmentEntry>>,
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
            resources: ResourceRegistry::default(),
            indexes: CandidateIndexes::default(),
        }
    }

    pub(super) fn attach(
        &mut self,
        spec: SegmentSpec,
        max_allocator_nodes: u32,
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
        )?;
        let entry = Arc::new(SegmentEntry::new(Arc::new(spec), resource));
        let segment = self.handle(entry.clone());
        self.segments.insert(segment.id(), entry);
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
        self.resources.unmount(removed.spec());
        self.rebuild_indexes();
        Ok(())
    }

    /// Invalidates and detaches every segment owned by a fenced client.
    /// Outstanding leases retain their resource handles but are logically
    /// unusable immediately.
    pub(super) fn invalidate_owner(&mut self, owner: ClientId) -> usize {
        let owned: Vec<_> = self
            .segments
            .iter()
            .filter(|(_, entry)| entry.spec().identity().owner() == owner)
            .map(|(id, entry)| (*id, entry.clone()))
            .collect();

        for (id, entry) in &owned {
            entry.invalidate();
            let removed = self
                .segments
                .remove(id)
                .expect("owned entries remain registered while the catalog is write-locked");
            self.resources.unmount(removed.spec());
        }
        if !owned.is_empty() {
            self.rebuild_indexes();
        }
        owned.len()
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
