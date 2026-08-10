use super::*;

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
            if pending.deadline > now {
                self.pending.push(pending);
                continue;
            }
            let Some(slot) = pending.candidate.slot.upgrade() else {
                continue;
            };
            let Some(node) = pending.candidate.node.upgrade() else {
                continue;
            };
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
        let young_candidates = self.young.len();
        let protected_candidates = self.protected.len();
        let mut remaining = budget.max_candidates;

        for _ in 0..young_candidates.min(remaining) {
            if self.reclaim_target_is_covered() {
                return;
            }
            let Some(candidate) = self.young.pop() else {
                break;
            };
            remaining -= 1;
            self.evict_candidate(candidate, false, now, report);
        }
        for _ in 0..protected_candidates.min(remaining) {
            if self.reclaim_target_is_covered() {
                return;
            }
            let Some(candidate) = self.protected.pop() else {
                break;
            };
            self.evict_candidate(candidate, true, now, report);
        }
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
    ) {
        report.scanned_candidates += 1;
        let Some(slot) = candidate.slot.upgrade() else {
            return;
        };
        let Some(node) = candidate.node.upgrade() else {
            return;
        };
        if node.control.lifecycle.load(Ordering::Acquire) != OBJECT_PUBLISHED
            || !slot_points_to(&slot, &node)
        {
            return;
        }

        if node.control.recent.swap(false, Ordering::Relaxed) {
            self.protected.push(candidate);
            return;
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
            return;
        }

        if node.control.lease_until.load(Ordering::Acquire) > now.get() {
            node.control
                .lifecycle
                .store(OBJECT_PUBLISHED, Ordering::Release);
            self.protected.push(candidate);
            return;
        }
        if clear_slot(&slot, &node) {
            let bytes = node
                .record
                .get()
                .expect("published objects always have records")
                .reserved_bytes;
            self.retire_published(slot, node, now);
            report.retired_objects += 1;
            report.retired_bytes = report.retired_bytes.saturating_add(bytes);
        } else {
            node.control
                .lifecycle
                .store(OBJECT_PUBLISHED, Ordering::Release);
            if from_protected {
                self.protected.push(candidate);
            } else {
                self.young.push(candidate);
            }
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
                Ok(node) => {
                    let record = node
                        .record
                        .into_inner()
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
