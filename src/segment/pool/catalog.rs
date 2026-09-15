use super::entry::SegmentEntry;
use super::{AttachOutcome, PoolSnapshot, SegmentHandle};
use crate::segment::error::{AttachError, SegmentStateError};
use crate::segment::identity::{ClientId, SegmentId};
use crate::segment::offset_allocator::ByteAllocator;
use crate::segment::spec::{ReplicaClass, SegmentSpec};
use crate::segment::stats::ReplicaClassSpaceStats;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(super) struct Catalog {
    pool_id: u64,
    segments: HashMap<SegmentId, Arc<SegmentEntry>>,
    segments_by_owner: HashMap<ClientId, HashSet<SegmentId>>,
    indexes: CandidateIndexes,
}

#[derive(Default)]
struct CandidateIndexes {
    direct_generation: u64,
    direct: Arc<[SegmentHandle]>,
}

impl Catalog {
    pub(super) fn new(pool_id: u64) -> Self {
        Self {
            pool_id,
            segments: HashMap::new(),
            segments_by_owner: HashMap::new(),
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

        let region = spec.region();
        let end = region.end().ok_or(AttachError::AddressOverflow)?;
        for existing in self.segments.values().map(|entry| entry.spec()) {
            if existing.identity().owner() == spec.identity().owner()
                && existing.transport() == spec.transport()
                && region.base() < existing.region().end().expect("mounted ranges are valid")
                && existing.region().base() < end
            {
                return Err(AttachError::OverlappingAddressRange {
                    existing: existing.identity().id(),
                });
            }
        }
        let allocator = ByteAllocator::new(region.size(), max_allocator_nodes);
        let entry = Arc::new(SegmentEntry::new(Arc::new(spec), allocator));
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
            candidates: self.indexes.direct.clone(),
        }
    }

    pub(super) fn space_for(&self, replica_class: ReplicaClass) -> ReplicaClassSpaceStats {
        let mut capacity_bytes = 0_u64;
        let mut used_bytes = 0_u64;
        let mut available_bytes = 0_u64;
        let mut largest_free_region_bytes = 0_u64;
        for entry in self
            .segments
            .values()
            .filter(|entry| entry.spec().replica_class() == replica_class)
        {
            let space = entry.stats().space;
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

    pub(super) fn segment(&self, id: SegmentId) -> Option<SegmentHandle> {
        self.segments
            .get(&id)
            .cloned()
            .map(|entry| self.handle(entry))
    }

    pub(super) fn len(&self) -> usize {
        self.segments.len()
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
        self.segments
            .remove(&id)
            .expect("owned entries remain registered while the catalog is write-locked");
        self.remove_owner_segment(owner, id);
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

    fn rebuild_indexes(&mut self) {
        let mut direct: Vec<_> = self
            .segments
            .values()
            .filter(|entry| entry.is_accepting())
            .map(|entry| self.handle(entry.clone()))
            .collect();
        direct.sort_unstable_by_key(|candidate| candidate.id());
        if direct.len() != self.indexes.direct.len()
            || !direct
                .iter()
                .zip(self.indexes.direct.iter())
                .all(|(left, right)| Arc::ptr_eq(&left.entry, &right.entry))
        {
            self.indexes.direct = Arc::from(direct);
            self.indexes.direct_generation = self.indexes.direct_generation.wrapping_add(1);
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
