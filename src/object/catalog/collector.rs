//! Incremental collection separates object retirement, resource reclamation,
//! and slot cleanup. Expiration, removal, and eviction first detach a node and
//! update lifecycle accounting. Reclaim releases replica resources only after
//! external handles drop; empty stable slots are removed independently after a
//! grace period.

use super::*;
use std::collections::HashSet;

enum ScopedCandidate {
    Stale,
    Unmatched,
    Match(usize),
}

impl ObjectCatalog {
    pub fn remove(&self, lookup: ObjectLookup<'_>, now: CatalogTick) -> Result<(), RemoveError> {
        let slot = self
            .inner
            .lookup_slot(lookup)
            .ok_or(RemoveError::NotFound)?;
        let node = loop {
            let node = slot.current.load_full().ok_or(RemoveError::NotFound)?;
            let write = node.mutation.lock();
            if !slot_points_to(&slot, &node) {
                drop(write);
                continue;
            }
            match node.mutation.state() {
                ObjectState::Published => {}
                ObjectState::Claimed | ObjectState::Pending => {
                    return Err(RemoveError::NotReady);
                }
                ObjectState::Retiring => return Err(RemoveError::NotFound),
            }
            if node
                .mutation
                .transition(ObjectState::Published, ObjectState::Retiring)
                .is_err()
            {
                drop(write);
                continue;
            }

            let lease_until = node.access.lease_until();
            if lease_until > now {
                node.mutation.store(ObjectState::Published);
                return Err(RemoveError::Leased {
                    expires_at: lease_until,
                });
            }
            if !clear_slot(&slot, &node) {
                node.mutation.store(ObjectState::Published);
                return Err(RemoveError::NotFound);
            }
            drop(write);
            break node;
        };
        self.inner.retire_published(slot, node, now);
        Ok(())
    }

    pub fn request_reclaim(&self, bytes: u64) {
        self.inner.collector.request_reclaim(bytes);
    }

    /// Requests one bounded liveness sweep after segment membership changes.
    /// Repeated requests extend the current sweep to cover the full queue.
    pub(in crate::object) fn request_liveness_scan(&self) {
        self.inner
            .collector
            .liveness_scan_requested
            .store(true, Ordering::Release);
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
        let Some(_collector) = self.inner.collector.gate.try_lock() else {
            return CollectReport {
                busy: true,
                ..CollectReport::default()
            };
        };

        if self
            .inner
            .collector
            .liveness_scan_requested
            .swap(false, Ordering::AcqRel)
        {
            self.inner
                .collector
                .liveness_young_remaining
                .fetch_max(self.inner.collector.young.len(), Ordering::Release);
            self.inner
                .collector
                .liveness_protected_remaining
                .fetch_max(self.inner.collector.protected.len(), Ordering::Release);
        }

        // Pending expiration has its own queue allowance. Liveness, scoped
        // eviction, and global eviction share the eviction allowance. Normal
        // eviction stays paused while a liveness sweep is incomplete so it
        // cannot move an unscanned candidate between generations.
        let mut report = CollectReport::default();
        let liveness_scanned = self
            .inner
            .retire_invalidated_published(now, budget, &mut report);
        self.inner.expire_pending(now, budget, &mut report);
        let liveness_incomplete = self
            .inner
            .collector
            .liveness_young_remaining
            .load(Ordering::Acquire)
            != 0
            || self
                .inner
                .collector
                .liveness_protected_remaining
                .load(Ordering::Acquire)
                != 0;
        if !liveness_incomplete {
            let eviction_budget = CollectBudget::new(
                budget.max_candidates.saturating_sub(liveness_scanned),
                budget.max_reclaims,
                budget.max_empty_slots,
            );
            let scoped_scanned =
                self.inner
                    .evict_scoped(now, eviction_budget, targets, &mut report);
            let global_budget = CollectBudget::new(
                eviction_budget
                    .max_candidates
                    .saturating_sub(scoped_scanned),
                budget.max_reclaims,
                budget.max_empty_slots,
            );
            self.inner.evict(now, global_budget, &mut report);
        }
        self.inner.reclaim(now, budget, &mut report);
        self.inner.clean_empty_slots(now, budget, &mut report);
        report
    }
}

impl CatalogInner {
    pub(super) fn retire_invalidated_published(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        report: &mut CollectReport,
    ) -> usize {
        let generations = [
            (
                &self.collector.young,
                &self.collector.liveness_young_remaining,
            ),
            (
                &self.collector.protected,
                &self.collector.liveness_protected_remaining,
            ),
        ];
        // Budgets may intentionally be unbounded (`usize::MAX`) for drain-style
        // collection. Grow this cold-path batch from the replicas we actually
        // prune instead of treating the budget as an allocation size.
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
                let Some(node) = candidate.node.upgrade() else {
                    continue;
                };
                if node.mutation.state() != ObjectState::Published
                    || !slot_points_to(&slot, &node)
                    || node.record.get().is_none()
                {
                    continue;
                }
                let write = node.mutation.lock();
                if node.mutation.state() != ObjectState::Published || !slot_points_to(&slot, &node)
                {
                    continue;
                }
                let record = node.record();
                match record.replicas.prune_invalidated() {
                    ReplicaPrune::AllLive => {
                        drop(write);
                        queue.push(candidate);
                    }
                    ReplicaPrune::AllStale => {
                        node.mutation.store(ObjectState::Retiring);
                        if !clear_slot(&slot, &node) {
                            node.mutation.store(ObjectState::Published);
                            drop(write);
                            queue.push(candidate);
                            continue;
                        }
                        let bytes = record.reserved_bytes();
                        drop(write);
                        self.retire_published(slot, node, now);
                        report.invalidated_published += 1;
                        report.retired_objects += 1;
                        report.retired_bytes = report.retired_bytes.saturating_add(bytes);
                    }
                    ReplicaPrune::Mixed { stale } => {
                        let stale_count = stale.len();
                        let stale_bytes = stale.reserved_bytes();
                        node.release_pruned_accounting(&stale);
                        self.lifecycle.on_prune_published(stale_bytes);
                        self.collector.on_reclaim(stale_bytes);
                        drop(write);
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

    pub(super) fn retire_pending(
        &self,
        slot: Arc<ObjectSlot>,
        node: Arc<CatalogNode>,
        now: CatalogTick,
    ) {
        let record = node
            .record
            .get()
            .expect("only staged objects can be retired");
        let bytes = record.reserved_bytes();
        node.abort_accounting();
        self.lifecycle.on_retire_pending(bytes);
        self.collector.retired.push(RetiredObject {
            node,
            reserved_bytes: bytes,
            retry_at: now,
        });
        self.enqueue_empty(&slot, now);
    }

    fn resolve_pending_candidate(
        &self,
        pending: &PendingCandidate,
    ) -> Option<(Arc<ObjectSlot>, Arc<CatalogNode>)> {
        let slot = pending.candidate.slot.upgrade()?;
        let node = pending.candidate.node.upgrade()?;
        if node.mutation.state() != ObjectState::Pending
            || !slot_points_to(&slot, &node)
            || node.record.get().is_none()
        {
            return None;
        }
        Some((slot, node))
    }

    fn try_revoke_pending(
        &self,
        slot: Arc<ObjectSlot>,
        node: Arc<CatalogNode>,
        now: CatalogTick,
    ) -> bool {
        let write = node.mutation.lock();
        if !slot_points_to(&slot, &node) {
            return false;
        }
        if node
            .mutation
            .transition(ObjectState::Pending, ObjectState::Retiring)
            .is_err()
        {
            return false;
        }
        if !clear_slot(&slot, &node) {
            return false;
        }
        drop(write);
        self.retire_pending(slot, node, now);
        true
    }

    /// Removes pending candidates for a set of already-fenced sessions in one
    /// pass. Producers enqueue before their final fence check, so a racing
    /// stage is either present in this snapshot or revokes itself.
    pub(super) fn revoke_pending_owners(
        &self,
        owners: &HashSet<WriteOwner>,
        now: CatalogTick,
    ) -> usize {
        if owners.is_empty() {
            return 0;
        }

        let _collector = self.collector.gate.lock();
        let candidates = {
            let _stages = self.collector.pending_stage_gate.write();
            let mut candidates = Vec::with_capacity(self.collector.pending.len());
            while let Some(pending) = self.collector.pending.pop() {
                candidates.push(pending);
            }
            candidates
        };
        let mut revoked = 0;
        for pending in candidates {
            let Some((slot, node)) = self.resolve_pending_candidate(&pending) else {
                continue;
            };
            if !owners.contains(&node.owner()) {
                self.collector.pending.push(pending);
                continue;
            }
            if self.try_revoke_pending(slot, node, now) {
                revoked += 1;
            }
        }
        revoked
    }

    fn retire_published(&self, slot: Arc<ObjectSlot>, node: Arc<CatalogNode>, now: CatalogTick) {
        let record = node
            .record
            .get()
            .expect("only published objects can be retired");
        let bytes = record.reserved_bytes();
        node.mark_accounting_retiring();
        self.lifecycle.on_retire_published(bytes);
        self.collector.retired.push(RetiredObject {
            node,
            reserved_bytes: bytes,
            retry_at: now,
        });
        self.enqueue_empty(&slot, now);
    }

    pub(super) fn expire_pending(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        report: &mut CollectReport,
    ) {
        let candidates = self.collector.pending.len().min(budget.max_candidates);
        for _ in 0..candidates {
            let Some(pending) = self.collector.pending.pop() else {
                break;
            };
            let Some((slot, node)) = self.resolve_pending_candidate(&pending) else {
                continue;
            };
            if !node.record().replicas.read().all_live() {
                if self.try_revoke_pending(slot, node, now) {
                    report.invalidated_pending += 1;
                }
                continue;
            }
            if pending.deadline > now {
                self.collector.pending.push(pending);
                continue;
            }
            if self.try_revoke_pending(slot, node, now) {
                report.expired_pending += 1;
            }
        }
    }

    pub(super) fn evict(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        report: &mut CollectReport,
    ) {
        // Snapshot both generation sizes before scanning. An object promoted
        // from young to protected must not be reconsidered in the same pause.
        let generations = [
            (&self.collector.young, self.collector.young.len(), false),
            (
                &self.collector.protected,
                self.collector.protected.len(),
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

    pub(super) fn evict_scoped(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        targets: &[ReclaimTarget],
        report: &mut CollectReport,
    ) -> usize {
        if targets.is_empty() || budget.max_candidates == 0 {
            return 0;
        }
        // Scope-filtered debt uses tenant-accounting bytes (logical bytes
        // across replicas); the common retirement report uses reserved bytes.
        let mut debts: Vec<_> = targets
            .iter()
            .filter(|target| target.bytes > 0)
            .map(|target| (target.filter, target.bytes))
            .collect();
        if debts.is_empty() {
            return 0;
        }

        // All scopes share the existing generation queues. A candidate is
        // scanned once and matched against the small active-debt set.
        let generations = [
            (&self.collector.young, self.collector.young.len(), false),
            (
                &self.collector.protected,
                self.collector.protected.len(),
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
                    ScopedCandidate::Match(index) => {
                        let retired = self.evict_candidate(candidate, from_protected, now, report);
                        if retired > 0 {
                            debts[index].1 = debts[index].1.saturating_sub(retired);
                            report.scoped_retired_objects += 1;
                            report.scoped_retired_bytes =
                                report.scoped_retired_bytes.saturating_add(retired);
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
        let Some(node) = candidate.node.upgrade() else {
            return ScopedCandidate::Stale;
        };
        if node.mutation.state() != ObjectState::Published || !slot_points_to(&slot, &node) {
            return ScopedCandidate::Stale;
        }
        let Some(record) = node.record.get() else {
            return ScopedCandidate::Stale;
        };
        debts
            .iter()
            .position(|(filter, debt)| {
                *debt > 0
                    && match filter {
                        ReclaimFilter::Any => true,
                        ReclaimFilter::Scope {
                            namespace,
                            replica_class,
                        } => {
                            record.is_accounted()
                                && record.identity.namespace() == *namespace
                                && record.current_direct_replica_class() == Some(*replica_class)
                        }
                    }
            })
            .map_or(ScopedCandidate::Unmatched, ScopedCandidate::Match)
    }

    fn reclaim_target_is_covered(&self) -> bool {
        self.collector.reclaim_debt.load(Ordering::Relaxed)
            <= self.lifecycle.retired_bytes.load(Ordering::Relaxed)
    }

    fn evict_candidate(
        &self,
        candidate: GcCandidate,
        from_protected: bool,
        now: CatalogTick,
        report: &mut CollectReport,
    ) -> u64 {
        report.scanned_candidates += 1;
        let Some(slot) = candidate.slot.upgrade() else {
            return 0;
        };
        let Some(node) = candidate.node.upgrade() else {
            return 0;
        };
        if node.mutation.state() != ObjectState::Published || !slot_points_to(&slot, &node) {
            return 0;
        }

        let write = node.mutation.lock();
        if node.mutation.state() != ObjectState::Published || !slot_points_to(&slot, &node) {
            return 0;
        }
        if node.access.take_recent() {
            self.collector.protected.push(candidate);
            return 0;
        }
        if node
            .mutation
            .transition(ObjectState::Published, ObjectState::Retiring)
            .is_err()
        {
            return 0;
        }

        if node.access.is_leased(now) {
            node.mutation.store(ObjectState::Published);
            self.collector.protected.push(candidate);
            return 0;
        }
        if clear_slot(&slot, &node) {
            let bytes = node
                .record
                .get()
                .expect("published objects always have records")
                .reserved_bytes();
            let quota_bytes = node
                .record
                .get()
                .and_then(ObjectRecord::tenant_accounting)
                .map_or(bytes, |(_, _, quota_bytes)| quota_bytes);
            drop(write);
            self.retire_published(slot, node, now);
            report.retired_objects += 1;
            report.retired_bytes = report.retired_bytes.saturating_add(bytes);
            quota_bytes
        } else {
            node.mutation.store(ObjectState::Published);
            drop(write);
            if from_protected {
                self.collector.protected.push(candidate);
            } else {
                self.collector.young.push(candidate);
            }
            0
        }
    }

    pub(super) fn reclaim(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        report: &mut CollectReport,
    ) {
        let retired_objects = self.collector.retired.len().min(budget.max_reclaims);
        let mut resources = ReplicaReclaimBatch::with_capacity(retired_objects);
        for _ in 0..retired_objects {
            let Some(retired) = self.collector.retired.pop() else {
                break;
            };
            if retired.retry_at > now {
                self.collector.retired.push(retired);
                continue;
            }

            match Arc::try_unwrap(retired.node) {
                Ok(mut node) => {
                    node.release_accounting();
                    let record = node
                        .record
                        .take()
                        .expect("retired objects always have records");
                    resources.extend(record.replicas.into_inner());
                    self.lifecycle.on_reclaim(retired.reserved_bytes);
                    self.collector.on_reclaim(retired.reserved_bytes);
                    report.reclaimed_objects += 1;
                    report.reclaimed_bytes = report
                        .reclaimed_bytes
                        .saturating_add(retired.reserved_bytes);
                }
                Err(node) => self.collector.retired.push(RetiredObject {
                    node,
                    reserved_bytes: retired.reserved_bytes,
                    retry_at: now.saturating_add(1),
                }),
            }
        }
        resources.release();
    }

    // SLOT_CLOSING is a handshake with claim_put. Removal succeeds only while
    // the same slot remains indexed and empty; a concurrent claimant can
    // reopen it, or detect its removal and retry against the current entry.
    pub(super) fn clean_empty_slots(
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
            if slot.current.load().is_some()
                || slot
                    .state
                    .compare_exchange(SLOT_OPEN, SLOT_CLOSING, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
            {
                continue;
            }
            let removed =
                self.index
                    .entries
                    .remove_if_sync(&candidate.identity.as_lookup(), |indexed| {
                        Arc::ptr_eq(indexed, &slot)
                            && slot.state.load(Ordering::Acquire) == SLOT_CLOSING
                            && slot.current.load().is_none()
                    });
            if removed.is_some() {
                self.index.on_slot_removed();
                report.removed_empty_slots += 1;
            } else {
                slot.state.store(SLOT_OPEN, Ordering::Release);
            }
        }
    }
}
