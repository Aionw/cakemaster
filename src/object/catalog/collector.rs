use super::*;

enum ScopedCandidate {
    Stale,
    Unmatched,
    Match(usize),
}

impl CatalogInner {
    pub(super) fn expire_pending(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        report: &mut CollectReport,
    ) {
        let candidates = self.pending.len().min(budget.max_candidates);
        for _ in 0..candidates {
            let Some(pending) = self.pending.pop() else {
                break;
            };
            let Some(slot) = pending.candidate.slot.upgrade() else {
                continue;
            };
            let Some(node) = pending.candidate.node.upgrade() else {
                continue;
            };
            if node.control.lifecycle.load(Ordering::Acquire) != OBJECT_PENDING
                || !slot_points_to(&slot, &node)
            {
                continue;
            }
            if pending.deadline > now {
                self.pending.push(pending);
                continue;
            }
            if node.record.get().is_none() {
                continue;
            }
            if node
                .control
                .lifecycle
                .compare_exchange(
                    OBJECT_PENDING,
                    OBJECT_RETIRING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                continue;
            }
            if clear_slot(&slot, &node) {
                self.retire_pending(slot, node, now);
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
            (&self.young, self.young.len(), false),
            (&self.protected, self.protected.len(), true),
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
            (&self.young, self.young.len(), false),
            (&self.protected, self.protected.len(), true),
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
        if node.control.lifecycle.load(Ordering::Acquire) != OBJECT_PUBLISHED
            || !slot_points_to(&slot, &node)
        {
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
                            record.accounting.is_some()
                                && record.identity.namespace() == *namespace
                                && record.direct_replica_class() == Some(*replica_class)
                        }
                    }
            })
            .map_or(ScopedCandidate::Unmatched, ScopedCandidate::Match)
    }

    fn reclaim_target_is_covered(&self) -> bool {
        self.reclaim_debt.load(Ordering::Relaxed) <= self.retired_bytes.load(Ordering::Relaxed)
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
        if node.control.lifecycle.load(Ordering::Acquire) != OBJECT_PUBLISHED
            || !slot_points_to(&slot, &node)
        {
            return 0;
        }

        if node.control.recent.swap(false, Ordering::Relaxed) {
            self.protected.push(candidate);
            return 0;
        }
        if node
            .control
            .lifecycle
            .compare_exchange(
                OBJECT_PUBLISHED,
                OBJECT_RETIRING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return 0;
        }

        if node.control.lease_until.load(Ordering::Acquire) > now.get() {
            node.control
                .lifecycle
                .store(OBJECT_PUBLISHED, Ordering::Release);
            self.protected.push(candidate);
            return 0;
        }
        if clear_slot(&slot, &node) {
            let bytes = node
                .record
                .get()
                .expect("published objects always have records")
                .reserved_bytes;
            let quota_bytes = node
                .record
                .get()
                .and_then(ObjectRecord::tenant_accounting)
                .map_or(bytes, |(_, _, quota_bytes)| quota_bytes);
            self.retire_published(slot, node, now);
            report.retired_objects += 1;
            report.retired_bytes = report.retired_bytes.saturating_add(bytes);
            quota_bytes
        } else {
            node.control
                .lifecycle
                .store(OBJECT_PUBLISHED, Ordering::Release);
            if from_protected {
                self.protected.push(candidate);
            } else {
                self.young.push(candidate);
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
        let retired_objects = self.retired.len().min(budget.max_reclaims);
        let mut resources = ReplicaReclaimBatch::with_capacity(retired_objects);
        for _ in 0..retired_objects {
            let Some(retired) = self.retired.pop() else {
                break;
            };
            if retired.retry_at > now {
                self.retired.push(retired);
                continue;
            }

            match Arc::try_unwrap(retired.node) {
                Ok(mut node) => {
                    node.release_accounting();
                    let record = node
                        .record
                        .take()
                        .expect("retired objects always have records");
                    resources.extend(record.replicas);
                    atomic_saturating_sub(&self.retired_bytes, retired.reserved_bytes);
                    atomic_saturating_sub(&self.reclaim_debt, retired.reserved_bytes);
                    report.reclaimed_objects += 1;
                    report.reclaimed_bytes = report
                        .reclaimed_bytes
                        .saturating_add(retired.reserved_bytes);
                }
                Err(node) => self.retired.push(RetiredObject {
                    node,
                    reserved_bytes: retired.reserved_bytes,
                    retry_at: now.saturating_add(1),
                }),
            }
        }
        resources.release();
    }

    pub(super) fn clean_empty_slots(
        &self,
        now: CatalogTick,
        budget: CollectBudget,
        report: &mut CollectReport,
    ) {
        let candidates = self.empty_slots.len().min(budget.max_empty_slots);
        for _ in 0..candidates {
            let Some(candidate) = self.empty_slots.pop() else {
                break;
            };
            if candidate.deadline > now {
                self.empty_slots.push(candidate);
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
            let removed = self
                .entries
                .remove_if_sync(&candidate.identity.as_lookup(), |indexed| {
                    Arc::ptr_eq(indexed, &slot)
                        && slot.state.load(Ordering::Acquire) == SLOT_CLOSING
                        && slot.current.load().is_none()
                });
            if removed.is_some() {
                self.slots.fetch_sub(1, Ordering::Relaxed);
                report.removed_empty_slots += 1;
            } else {
                slot.state.store(SLOT_OPEN, Ordering::Release);
            }
        }
    }
}
