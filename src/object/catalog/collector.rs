//! Incremental transaction expiration, version retirement, and resource
//! reclamation.

use super::*;
use crate::segment::ReplicaClass;
use std::collections::HashSet;

enum ScopedCandidate {
    Stale,
    Unmatched,
    Matches(Vec<usize>),
}

#[derive(Clone, Copy, Default)]
struct RetiredBytes {
    physical: u64,
    quota: u64,
}

impl ObjectCatalog {
    pub fn remove(&self, lookup: ObjectLookup<'_>, now: CatalogTick) -> Result<(), RemoveError> {
        self.remove_with_force(lookup, now, false)
    }

    pub fn remove_with_force(
        &self,
        lookup: ObjectLookup<'_>,
        now: CatalogTick,
        force: bool,
    ) -> Result<(), RemoveError> {
        let slot = self
            .inner
            .lookup_slot(lookup)
            .ok_or(RemoveError::NotFound)?;
        let control = slot.control.lock();
        if control.active.is_some() {
            return Err(RemoveError::NotReady);
        }
        let version = slot.load_committed().ok_or(RemoveError::NotFound)?;
        if !committed_points_to(&slot, &version) {
            return Err(RemoveError::NotFound);
        }
        if !force {
            let lease_until = version.access().lease_until();
            if lease_until > now {
                return Err(RemoveError::Leased {
                    expires_at: lease_until,
                });
            }
            if version.access().hard_pinned() {
                return Err(RemoveError::HardPinned);
            }
        }
        slot.clear_committed();
        if !force {
            // Detach before the final lease sample. A reader refreshes the
            // lease before validating this pointer, so a reader that can
            // still return this version is reflected in this load.
            let lease_until = version.access().lease_until();
            if lease_until > now {
                slot.publish_committed(version);
                return Err(RemoveError::Leased {
                    expires_at: lease_until,
                });
            }
        }
        drop(control);
        self.inner.retire_published(slot, version, now, true);
        Ok(())
    }

    pub fn request_reclaim(&self, bytes: u64) {
        self.inner.collector.reclaim.request(bytes);
    }

    pub(in crate::object) fn request_liveness_scan(&self) {
        self.inner.collector.liveness.request();
    }

    pub fn collect_step(&self, now: CatalogTick, budget: CollectBudget) -> CollectReport {
        self.collect_step_with_targets(now, budget, &[])
    }

    pub(in super::super) fn collect_step_with_targets(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        targets: &[ReclaimTarget],
    ) -> CollectReport {
        self.collect_step_inner(now, budget, targets, false, || 0, |_| {})
    }

    pub(in super::super) fn collect_step_with_targets_and_watermark<P, F>(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        targets: &[ReclaimTarget],
        prepare_watermark: P,
        finish_watermark: F,
    ) -> CollectReport
    where
        P: FnOnce() -> u64,
        F: FnOnce(CollectReport),
    {
        self.collect_step_inner(
            now,
            budget,
            targets,
            true,
            prepare_watermark,
            finish_watermark,
        )
    }

    fn collect_step_inner<P, F>(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        targets: &[ReclaimTarget],
        reclaim_first: bool,
        prepare_watermark: P,
        finish_watermark: F,
    ) -> CollectReport
    where
        P: FnOnce() -> u64,
        F: FnOnce(CollectReport),
    {
        let Some(_collector) = self.inner.collector.step_gate.try_lock() else {
            return CollectReport {
                busy: true,
                ..CollectReport::default()
            };
        };

        let mut report = CollectReport::default();
        let reclaim_attempts = if reclaim_first {
            self.inner.reclaim(now, budget, &mut report)
        } else {
            0
        };
        let watermark_reclaim_bytes = prepare_watermark();
        let mut scoped_targets = Vec::with_capacity(targets.len() + 1);
        scoped_targets.extend_from_slice(targets);
        if watermark_reclaim_bytes != 0 {
            scoped_targets.push(ReclaimTarget {
                filter: ReclaimFilter::Class(ReplicaClass::Memory),
                bytes: watermark_reclaim_bytes,
            });
        }

        self.inner
            .collector
            .liveness
            .begin_if_requested(&self.inner.collector.eviction);

        self.inner.expire_soft_pins(now, budget, &mut report);
        let liveness_scanned = self
            .inner
            .retire_invalidated_published(now, budget, &mut report);
        self.inner.expire_pending(now, budget, &mut report);
        let liveness_incomplete = self.inner.collector.liveness.is_incomplete();
        if !liveness_incomplete {
            let eviction_budget = CollectBudget::new(
                budget.max_candidates.saturating_sub(liveness_scanned),
                budget.max_reclaims,
                budget.max_empty_slots,
            );
            let scoped_scanned =
                self.inner
                    .evict_scoped(now, eviction_budget, &scoped_targets, &mut report);
            let global_budget = CollectBudget::new(
                eviction_budget
                    .max_candidates
                    .saturating_sub(scoped_scanned),
                budget.max_reclaims,
                budget.max_empty_slots,
            );
            self.inner.evict(now, global_budget, &mut report);
        }
        let remaining_reclaims = budget.max_reclaims.saturating_sub(reclaim_attempts);
        self.inner.reclaim(
            now,
            CollectBudget::new(
                budget.max_candidates,
                remaining_reclaims,
                budget.max_empty_slots,
            ),
            &mut report,
        );
        self.inner.clean_empty_slots(now, budget, &mut report);
        finish_watermark(report);
        report
    }
}

impl CatalogInner {
    fn expire_soft_pins(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        report: &mut CollectReport,
    ) {
        let candidates = self
            .collector
            .eviction
            .soft_pins
            .len()
            .min(budget.max_candidates);
        for _ in 0..candidates {
            let Some(candidate) = self.collector.eviction.soft_pins.pop() else {
                break;
            };
            report.scanned_soft_pins += 1;
            let Some(slot) = candidate.candidate.slot.upgrade() else {
                continue;
            };
            let Some(version) = candidate.candidate.version.upgrade() else {
                continue;
            };
            if !committed_points_to(&slot, &version) {
                continue;
            }
            if candidate.deadline > now {
                self.collector.eviction.soft_pins.push(candidate);
                continue;
            }
            if version.access().expire_soft_pin(candidate.deadline, now) {
                report.expired_soft_pins += 1;
            }
        }
    }

    fn retire_invalidated_published(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        report: &mut CollectReport,
    ) -> usize {
        let generations = [
            (
                &self.collector.eviction.young,
                &self.collector.liveness.young_remaining,
            ),
            (
                &self.collector.eviction.protected,
                &self.collector.liveness.protected_remaining,
            ),
        ];
        let mut resources = ReplicaReclaimBatch::default();
        let mut remaining_budget = budget.max_candidates;
        let mut scanned = 0;
        for (queue, remaining) in generations {
            let candidates = remaining.load(Ordering::Acquire).min(remaining_budget);
            for _ in 0..candidates {
                let Some(candidate) = queue.pop() else {
                    remaining.store(0, Ordering::Release);
                    break;
                };
                remaining.fetch_sub(1, Ordering::AcqRel);
                remaining_budget -= 1;
                scanned += 1;
                report.scanned_candidates += 1;
                let Some(slot) = candidate.slot.upgrade() else {
                    continue;
                };
                let Some(version) = candidate.version.upgrade() else {
                    continue;
                };
                if !committed_points_to(&slot, &version) {
                    continue;
                }

                let control = slot.control.lock();
                if !committed_points_to(&slot, &version) {
                    continue;
                }
                match version.record().replicas.prune_invalidated() {
                    ReplicaPrune::AllLive => {
                        drop(control);
                        queue.push(candidate);
                    }
                    ReplicaPrune::AllStale if control.active.is_some() => {
                        // The candidate transaction may still publish a healthy
                        // replacement. Its commit or abort will make this base
                        // collectable without destroying the transaction.
                        drop(control);
                        queue.push(candidate);
                    }
                    ReplicaPrune::AllStale => {
                        let bytes = version.record().reserved_bytes();
                        slot.clear_committed();
                        drop(control);
                        self.retire_published(slot, version, now, true);
                        report.invalidated_published += 1;
                        report.retired_objects += 1;
                        report.retired_bytes = report.retired_bytes.saturating_add(bytes);
                    }
                    ReplicaPrune::Mixed { stale } => {
                        let stale_count = stale.len();
                        let stale_bytes = stale.reserved_bytes();
                        version.record().release_pruned_accounting(&stale);
                        self.lifecycle.on_prune_published(stale_bytes);
                        self.collector.reclaim.on_reclaim(stale_bytes);
                        drop(control);
                        resources.extend(stale);
                        report.pruned_objects += 1;
                        report.pruned_replicas += stale_count;
                        report.pruned_replica_bytes =
                            report.pruned_replica_bytes.saturating_add(stale_bytes);
                        queue.push(candidate);
                    }
                }
            }
            if remaining_budget == 0 {
                break;
            }
        }
        resources.release();
        scanned
    }

    pub(super) fn enqueue_empty(&self, slot: &Arc<ObjectSlot>, now: CatalogTick) {
        self.collector.empty_slots.push(EmptySlotCandidate {
            identity: slot.identity.clone(),
            slot: Arc::downgrade(slot),
            deadline: now.saturating_add(self.config.empty_slot_grace_ticks),
        });
    }

    pub(super) fn retire_aborted_pending(&self, pending: Arc<ObjectVersion>, now: CatalogTick) {
        let bytes = pending.record().reserved_bytes();
        let memory = pending.record().current_direct_replica_class() == Some(ReplicaClass::Memory);
        pending.record().abort_accounting();
        self.lifecycle.on_abort_pending(bytes, memory);
        self.collector.retired.push(RetiredObject {
            version: pending,
            reserved_bytes: bytes,
            reclaim_after: now,
        });
    }

    fn abort_pending(
        &self,
        slot: &Arc<ObjectSlot>,
        id: TransactionId,
        expected: &Arc<ObjectVersion>,
        now: CatalogTick,
    ) -> bool {
        let mut control = slot.control.lock();
        let Some(active) = control.active.as_ref() else {
            return false;
        };
        if active.id != id {
            return false;
        }
        let TransactionPhase::Staged(pending) = &active.phase else {
            return false;
        };
        if !Arc::ptr_eq(pending, expected) {
            return false;
        }
        let pending = pending.clone();
        let base_is_invalid = active
            .base
            .as_ref()
            .is_some_and(|base| !base.record().replicas.read().has_live());
        control.active = None;
        let enqueue_empty = !slot.has_committed();
        drop(control);
        self.retire_aborted_pending(pending, now);
        if enqueue_empty {
            self.enqueue_empty(slot, now);
        }
        if base_is_invalid {
            self.collector.liveness.request();
        }
        true
    }

    fn pending_snapshot(
        &self,
        candidate: &PendingCandidate,
    ) -> Option<(Arc<ObjectSlot>, Arc<ObjectVersion>, WriteOwner)> {
        let pending = candidate.pending.upgrade()?;
        if pending.is_committed() {
            return None;
        }
        let slot = candidate.slot.upgrade()?;
        let control = slot.control.lock();
        let active = control.active.as_ref()?;
        if active.id != candidate.transaction_id {
            return None;
        }
        let TransactionPhase::Staged(active_pending) = &active.phase else {
            return None;
        };
        if !Arc::ptr_eq(active_pending, &pending) {
            return None;
        }
        Some((slot.clone(), pending, active.owner))
    }

    pub(super) fn revoke_pending_owners(
        &self,
        owners: &HashSet<WriteOwner>,
        now: CatalogTick,
    ) -> usize {
        if owners.is_empty() {
            return 0;
        }
        let _collector = self.collector.step_gate.lock();
        let candidates = {
            let _stages = self.collector.pending.stage_gate.write();
            let mut candidates = Vec::with_capacity(self.collector.pending.candidates.len());
            while let Some(pending) = self.collector.pending.candidates.pop() {
                candidates.push(pending);
            }
            candidates
        };
        let mut revoked = 0;
        for candidate in candidates {
            let Some((slot, pending, owner)) = self.pending_snapshot(&candidate) else {
                continue;
            };
            if !owners.contains(&owner) {
                self.collector.pending.candidates.push(candidate);
                continue;
            }
            if self.abort_pending(&slot, candidate.transaction_id, &pending, now) {
                revoked += 1;
            }
        }
        revoked
    }

    fn expire_pending(&self, now: CatalogTick, budget: CollectBudget, report: &mut CollectReport) {
        let candidates = self
            .collector
            .pending
            .candidates
            .len()
            .min(budget.max_candidates);
        for _ in 0..candidates {
            let Some(candidate) = self.collector.pending.candidates.pop() else {
                break;
            };
            let Some(pending) = candidate.pending.upgrade() else {
                continue;
            };
            if pending.is_committed() {
                continue;
            }
            let all_live = pending.record().replicas.read().all_live();
            if all_live && candidate.deadline > now {
                self.collector.pending.candidates.push(candidate);
                continue;
            }
            let Some(slot) = candidate.slot.upgrade() else {
                continue;
            };
            if !all_live {
                if self.abort_pending(&slot, candidate.transaction_id, &pending, now) {
                    report.invalidated_pending += 1;
                }
                continue;
            }
            if self.abort_pending(&slot, candidate.transaction_id, &pending, now) {
                report.expired_pending += 1;
            }
        }
    }

    fn retire_published(
        &self,
        slot: Arc<ObjectSlot>,
        version: Arc<ObjectVersion>,
        now: CatalogTick,
        enqueue_empty: bool,
    ) {
        let bytes = version.record().reserved_bytes();
        let memory = version.record().current_direct_replica_class() == Some(ReplicaClass::Memory);
        version.record().mark_accounting_retiring();
        self.lifecycle.on_retire_published(bytes, memory);
        self.collector.retired.push(RetiredObject {
            version,
            reserved_bytes: bytes,
            reclaim_after: now,
        });
        if enqueue_empty {
            self.enqueue_empty(&slot, now);
        }
    }

    fn evict(&self, now: CatalogTick, budget: CollectBudget, report: &mut CollectReport) {
        let generations = [
            (
                &self.collector.eviction.young,
                self.collector.eviction.young.len(),
                false,
            ),
            (
                &self.collector.eviction.protected,
                self.collector.eviction.protected.len(),
                true,
            ),
        ];
        let mut remaining = budget.max_candidates;
        for (queue, candidates, from_protected) in generations {
            for _ in 0..candidates.min(remaining) {
                if self.reclaim_target_is_covered() {
                    return;
                }
                let Some(candidate) = queue.pop() else {
                    break;
                };
                remaining -= 1;
                self.evict_candidate(candidate, from_protected, now, report);
            }
        }
    }

    fn evict_scoped(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        targets: &[ReclaimTarget],
        report: &mut CollectReport,
    ) -> usize {
        if targets.is_empty() || budget.max_candidates == 0 {
            return 0;
        }
        let mut debts: Vec<_> = targets
            .iter()
            .filter(|target| target.bytes > 0)
            .map(|target| (target.filter, target.bytes))
            .collect();
        if debts.is_empty() {
            return 0;
        }
        let generations = [
            (
                &self.collector.eviction.young,
                self.collector.eviction.young.len(),
                false,
            ),
            (
                &self.collector.eviction.protected,
                self.collector.eviction.protected.len(),
                true,
            ),
        ];
        let mut scanned = 0;
        for (queue, candidates, from_protected) in generations {
            let remaining = budget.max_candidates.saturating_sub(scanned);
            for _ in 0..candidates.min(remaining) {
                if debts.iter().all(|(_, debt)| *debt == 0) {
                    return scanned;
                }
                let Some(candidate) = queue.pop() else {
                    break;
                };
                scanned += 1;
                match self.classify_scoped_candidate(&candidate, &debts) {
                    ScopedCandidate::Matches(indices) => {
                        let retired = self.evict_candidate(candidate, from_protected, now, report);
                        if retired.physical > 0 {
                            for index in indices {
                                let amount = match debts[index].0 {
                                    ReclaimFilter::Scope { .. } => retired.quota,
                                    ReclaimFilter::Any | ReclaimFilter::Class(_) => {
                                        retired.physical
                                    }
                                };
                                debts[index].1 = debts[index].1.saturating_sub(amount);
                            }
                            report.scoped_retired_objects += 1;
                            report.scoped_retired_bytes =
                                report.scoped_retired_bytes.saturating_add(retired.physical);
                        }
                    }
                    ScopedCandidate::Unmatched => {
                        report.scanned_candidates += 1;
                        queue.push(candidate);
                    }
                    ScopedCandidate::Stale => report.scanned_candidates += 1,
                }
            }
        }
        scanned
    }

    fn classify_scoped_candidate(
        &self,
        candidate: &GcCandidate,
        debts: &[(ReclaimFilter, u64)],
    ) -> ScopedCandidate {
        let Some(slot) = candidate.slot.upgrade() else {
            return ScopedCandidate::Stale;
        };
        let Some(version) = candidate.version.upgrade() else {
            return ScopedCandidate::Stale;
        };
        if !committed_points_to(&slot, &version) {
            return ScopedCandidate::Stale;
        }
        let record = version.record();
        let matches: Vec<_> = debts
            .iter()
            .enumerate()
            .filter_map(|(index, (filter, debt))| {
                (*debt > 0
                    && match *filter {
                        ReclaimFilter::Any => true,
                        ReclaimFilter::Class(replica_class) => {
                            record.current_direct_replica_class() == Some(replica_class)
                        }
                        ReclaimFilter::Scope {
                            namespace,
                            replica_class,
                        } => {
                            record.is_accounted()
                                && record.identity.namespace() == namespace
                                && record.current_direct_replica_class() == Some(replica_class)
                        }
                    })
                .then_some(index)
            })
            .collect();
        if matches.is_empty() {
            ScopedCandidate::Unmatched
        } else {
            ScopedCandidate::Matches(matches)
        }
    }

    fn reclaim_target_is_covered(&self) -> bool {
        self.collector.reclaim.requested() <= self.lifecycle.retired_bytes.load(Ordering::Relaxed)
    }

    fn evict_candidate(
        &self,
        candidate: GcCandidate,
        from_protected: bool,
        now: CatalogTick,
        report: &mut CollectReport,
    ) -> RetiredBytes {
        report.scanned_candidates += 1;
        let Some(slot) = candidate.slot.upgrade() else {
            return RetiredBytes::default();
        };
        let Some(version) = candidate.version.upgrade() else {
            return RetiredBytes::default();
        };
        if !committed_points_to(&slot, &version) {
            return RetiredBytes::default();
        }
        if version.access().hard_pinned()
            || (version.access().is_soft_pinned(now)
                && !self.config.allow_evict_soft_pinned_objects)
        {
            if from_protected {
                self.collector.eviction.protected.push(candidate);
            } else {
                self.collector.eviction.young.push(candidate);
            }
            return RetiredBytes::default();
        }

        let control = slot.control.lock();
        if control.active.is_some() || !committed_points_to(&slot, &version) {
            drop(control);
            if committed_points_to(&slot, &version) {
                self.collector.eviction.protected.push(candidate);
            }
            return RetiredBytes::default();
        }
        if version.access().hard_pinned()
            || (version.access().is_soft_pinned(now)
                && !self.config.allow_evict_soft_pinned_objects)
        {
            drop(control);
            if from_protected {
                self.collector.eviction.protected.push(candidate);
            } else {
                self.collector.eviction.young.push(candidate);
            }
            return RetiredBytes::default();
        }
        if version.access().take_recent() {
            drop(control);
            self.collector.eviction.protected.push(candidate);
            return RetiredBytes::default();
        }
        if version.access().is_leased(now) {
            drop(control);
            self.collector.eviction.protected.push(candidate);
            return RetiredBytes::default();
        }

        slot.clear_committed();
        // Close the race with a reader that refreshed its access state after
        // the fast checks but before the pointer was detached. Readers that
        // observe the detached pointer retry instead of returning `version`.
        if version.access().take_recent() || version.access().is_leased(now) {
            slot.publish_committed(version.clone());
            drop(control);
            self.collector.eviction.protected.push(candidate);
            return RetiredBytes::default();
        }
        let bytes = version.record().reserved_bytes();
        let quota_bytes = version
            .record()
            .tenant_accounting()
            .map_or(bytes, |(_, _, quota_bytes)| quota_bytes);
        drop(control);
        self.retire_published(slot, version, now, true);
        report.retired_objects += 1;
        report.retired_bytes = report.retired_bytes.saturating_add(bytes);
        RetiredBytes {
            physical: bytes,
            quota: quota_bytes,
        }
    }

    fn reclaim(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        report: &mut CollectReport,
    ) -> usize {
        let retired_objects = self.collector.retired.len().min(budget.max_reclaims);
        let mut resources = ReplicaReclaimBatch::with_capacity(retired_objects);
        let mut attempts = 0;
        for _ in 0..retired_objects {
            let Some(retired) = self.collector.retired.pop() else {
                break;
            };
            attempts += 1;
            if retired.reclaim_after > now {
                self.collector.retired.push(retired);
                continue;
            }
            let RetiredObject {
                version,
                reserved_bytes,
                reclaim_after,
            } = retired;
            let version = match Arc::try_unwrap(version) {
                Ok(version) => version,
                Err(version) => {
                    self.collector.retired.push(RetiredObject {
                        version,
                        reserved_bytes,
                        reclaim_after,
                    });
                    continue;
                }
            };
            let mut record = version.record;
            let reclaimed_memory =
                record.current_direct_replica_class() == Some(ReplicaClass::Memory);
            record.release_accounting();
            resources.extend(record.replicas.take_exclusive());
            self.lifecycle.on_reclaim(reserved_bytes, reclaimed_memory);
            self.collector.reclaim.on_reclaim(reserved_bytes);
            report.reclaimed_objects += 1;
            report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(reserved_bytes);
            if reclaimed_memory {
                report.reclaimed_memory_bytes =
                    report.reclaimed_memory_bytes.saturating_add(reserved_bytes);
            }
        }
        resources.release();
        attempts
    }

    fn clean_empty_slots(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        report: &mut CollectReport,
    ) {
        let candidates = self.collector.empty_slots.len().min(budget.max_empty_slots);
        for _ in 0..candidates {
            let Some(candidate) = self.collector.empty_slots.pop() else {
                break;
            };
            if candidate.deadline > now {
                self.collector.empty_slots.push(candidate);
                continue;
            }
            let Some(slot) = candidate.slot.upgrade() else {
                continue;
            };
            let control = slot.control.lock();
            if slot.has_committed() || control.active.is_some() {
                continue;
            }
            let removed =
                self.index
                    .entries
                    .remove_if_sync(&candidate.identity.as_lookup(), |indexed| {
                        Arc::ptr_eq(indexed, &slot)
                            && !slot.has_committed()
                            && control.active.is_none()
                    });
            if removed.is_some() {
                self.index.on_slot_removed();
                report.removed_empty_slots += 1;
            }
        }
    }
}
