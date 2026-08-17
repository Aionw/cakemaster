//! Per-key write transaction protocol.
//!
//! A slot has two orthogonal pieces of state: one lock-free committed pointer
//! and at most one transaction under the slot control mutex. Insert keeps the
//! committed pointer empty until commit. Upsert leaves the previous committed
//! version readable and stages a fresh allocation beside it.

use super::super::error::{AbortError, BeginError, CommitError};
use super::*;
use crate::segment::ReplicaClass;
use scc::hash_map::Entry;
use std::collections::HashSet;

fn validate_stage_replicas(replicas: &ReplicaSet, required_bytes: u64) -> Result<(), StageError> {
    if replicas.is_empty() {
        return Err(StageError::NoReplicas);
    }
    let mut undersized = None;
    for replica in replicas.replicas() {
        if !replica.is_live() {
            return Err(StageError::ReplicaInvalidated {
                replica: replica.id(),
            });
        }
        if undersized.is_none() && replica.capacity_bytes() < required_bytes {
            undersized = Some((replica.id(), replica.capacity_bytes()));
        }
    }
    if let Some((replica, capacity_bytes)) = undersized {
        return Err(StageError::ReplicaTooSmall {
            replica,
            required_bytes,
            capacity_bytes,
        });
    }
    Ok(())
}

impl ObjectCatalog {
    pub fn begin_write(
        &self,
        identity: ObjectIdentity,
        admission: WriteAdmission,
        mode: WriteMode,
        pins: ObjectPinRequest,
        now: CatalogTick,
    ) -> Result<WriteClaim, BeginError> {
        let pins = self
            .inner
            .config
            .resolve_pin_request(pins)
            .map_err(BeginError::InvalidPinRequest)?;
        self.begin_write_resolved(identity, admission, mode, pins, now)
    }

    pub(in super::super) fn begin_write_resolved(
        &self,
        identity: ObjectIdentity,
        admission: WriteAdmission,
        mode: WriteMode,
        pins: ResolvedObjectPinRequest,
        now: CatalogTick,
    ) -> Result<WriteClaim, BeginError> {
        if identity.key().is_empty() {
            return Err(BeginError::EmptyKey);
        }
        if self.inner.lifecycle.retired_bytes.load(Ordering::Relaxed)
            >= self.inner.config.max_retired_bytes
        {
            return Err(BeginError::ReclamationBacklog);
        }

        loop {
            let slot = match self.inner.index.entries.entry_sync(identity.clone()) {
                Entry::Occupied(entry) => entry.get().clone(),
                Entry::Vacant(entry) => {
                    let id = TransactionId::new(1);
                    let slot = Arc::new(ObjectSlot {
                        identity: Arc::new(entry.key().clone()),
                        committed: OnceLock::new(),
                        control: Mutex::new(SlotControl {
                            next_transaction_id: 2,
                            next_version_id: 1,
                            active: Some(ActiveTransaction {
                                id,
                                owner: admission.owner(),
                                base: None,
                                pins,
                                phase: TransactionPhase::Claimed,
                            }),
                        }),
                    });
                    drop(entry.insert_entry(slot.clone()));
                    self.inner.index.on_slot_inserted();
                    self.inner.lifecycle.on_claim();
                    return Ok(WriteClaim {
                        catalog: Arc::downgrade(&self.inner),
                        slot: Arc::downgrade(&slot),
                        identity: slot.identity.clone(),
                        id,
                        admission,
                        started_at: now,
                        armed: true,
                    });
                }
            };

            let mut control = slot.control.lock();
            if !self.inner.slot_is_indexed(&slot) {
                continue;
            }
            if control.active.is_some() {
                return Err(BeginError::WriteInProgress);
            }

            let base = slot.load_committed();
            if mode == WriteMode::Insert && base.is_some() {
                return Err(BeginError::AlreadyExists);
            }
            let id = control.allocate_transaction_id();
            control.active = Some(ActiveTransaction {
                id,
                owner: admission.owner(),
                base,
                pins,
                phase: TransactionPhase::Claimed,
            });
            self.inner.lifecycle.on_claim();
            drop(control);
            return Ok(WriteClaim {
                catalog: Arc::downgrade(&self.inner),
                slot: Arc::downgrade(&slot),
                identity: slot.identity.clone(),
                id,
                admission,
                started_at: now,
                armed: true,
            });
        }
    }

    pub fn commit(
        &self,
        transaction: &WriteTransaction,
        commit: ObjectCommit,
        now: CatalogTick,
    ) -> Result<ObjectHandle, CommitError> {
        let transaction_catalog = transaction
            .catalog
            .upgrade()
            .ok_or(CommitError::TransactionGone)?;
        if !Arc::ptr_eq(&transaction_catalog, &self.inner) {
            return Err(CommitError::ForeignCatalog);
        }
        let slot = transaction
            .slot
            .upgrade()
            .ok_or(CommitError::TransactionGone)?;
        let mut control = slot.control.lock();

        let (base, pins, pending) = {
            let Some(active) = control.active.as_ref() else {
                let Some(version) = slot.load_committed() else {
                    return Err(CommitError::TransactionGone);
                };
                if version.origin != transaction.id {
                    return Err(CommitError::TransactionGone);
                }
                return if version.commit() == commit {
                    Ok(ObjectHandle { version })
                } else {
                    Err(CommitError::CommitConflict)
                };
            };
            if active.id != transaction.id {
                return Err(CommitError::TransactionGone);
            }
            let TransactionPhase::Staged(pending) = &active.phase else {
                return Err(CommitError::NotStaged);
            };
            if !Arc::ptr_eq(pending, &transaction.pending) {
                return Err(CommitError::TransactionGone);
            }
            if !pending.record().replicas.read().all_live() {
                return Err(CommitError::ReplicasInvalidated);
            }
            let base_matches = match &active.base {
                Some(base) => committed_points_to(&slot, base),
                None => !slot.has_committed(),
            };
            if !base_matches {
                return Err(CommitError::TransactionGone);
            }
            (active.base.clone(), active.pins, pending.clone())
        };
        let version = pending;
        version.initialize_commit(
            control.allocate_version_id(),
            commit,
            AccessControl::for_commit(base.as_deref(), pins, now),
        );

        // No fallible operation may follow the accounting transition.
        version.record().commit_accounting();
        slot.publish_committed(version.clone());
        control.active = None;
        let committed_bytes = version.record().reserved_bytes();
        let replaced_bytes = base.as_ref().map(|old| old.record().reserved_bytes());
        let replaced_memory_bytes = base.as_ref().map_or(0, |old| {
            if old.record().current_direct_replica_class() == Some(ReplicaClass::Memory) {
                old.record().reserved_bytes()
            } else {
                0
            }
        });
        self.inner
            .lifecycle
            .on_commit(committed_bytes, replaced_bytes, replaced_memory_bytes);
        drop(control);

        self.inner
            .collector
            .eviction
            .young
            .push(GcCandidate::new(&slot, &version));
        if let Some(deadline) = version.access().soft_pin_until() {
            self.inner
                .collector
                .eviction
                .soft_pins
                .push(SoftPinCandidate {
                    candidate: GcCandidate::new(&slot, &version),
                    deadline,
                });
        }
        if let Some(old) = base {
            old.record().mark_accounting_retiring();
            // Load after publication: a reader that linearized on the old
            // pointer has already refreshed this lease.
            let reclaim_after = old.access().lease_until();
            self.inner.collector.retired.push(RetiredObject {
                reserved_bytes: old.record().reserved_bytes(),
                version: old,
                reclaim_after,
            });
        }
        if !version.record().replicas.read().all_live() {
            self.inner.collector.liveness.request();
        }
        Ok(ObjectHandle { version })
    }

    pub fn abort(
        &self,
        transaction: &WriteTransaction,
        now: CatalogTick,
    ) -> Result<(), AbortError> {
        let transaction_catalog = transaction
            .catalog
            .upgrade()
            .ok_or(AbortError::TransactionGone)?;
        if !Arc::ptr_eq(&transaction_catalog, &self.inner) {
            return Err(AbortError::ForeignCatalog);
        }
        let slot = transaction
            .slot
            .upgrade()
            .ok_or(AbortError::TransactionGone)?;
        let mut control = slot.control.lock();
        let Some(active) = control.active.as_ref() else {
            return if slot
                .load_committed()
                .is_some_and(|version| version.origin == transaction.id)
            {
                Err(AbortError::AlreadyCommitted)
            } else {
                Err(AbortError::TransactionGone)
            };
        };
        if active.id != transaction.id {
            return Err(AbortError::TransactionGone);
        }
        let TransactionPhase::Staged(pending) = &active.phase else {
            return Err(AbortError::TransactionGone);
        };
        if !Arc::ptr_eq(pending, &transaction.pending) {
            return Err(AbortError::TransactionGone);
        }
        let pending = pending.clone();
        let base_is_invalid = active
            .base
            .as_ref()
            .is_some_and(|base| !base.record().replicas.read().has_live());
        control.active = None;
        let enqueue_empty = !slot.has_committed();
        drop(control);

        self.inner.retire_aborted_pending(pending, now);
        if enqueue_empty {
            self.inner.enqueue_empty(&slot, now);
        }
        if base_is_invalid {
            self.inner.collector.liveness.request();
        }
        Ok(())
    }

    pub(in super::super) fn resolve_write(
        &self,
        lookup: ObjectLookup<'_>,
    ) -> Result<WriteResolution, LookupError> {
        let slot = self
            .inner
            .lookup_slot(lookup)
            .ok_or(LookupError::NotFound)?;
        let control = slot.control.lock();
        if let Some(active) = &control.active {
            return match &active.phase {
                TransactionPhase::Claimed => Err(LookupError::NotReady),
                TransactionPhase::Staged(pending) => {
                    Ok(WriteResolution::Active(WriteTransaction {
                        catalog: Arc::downgrade(&self.inner),
                        slot: Arc::downgrade(&slot),
                        pending: pending.clone(),
                        id: active.id,
                    }))
                }
            };
        }
        slot.load_committed()
            .map(|version| WriteResolution::Committed(ObjectHandle { version }))
            .ok_or(LookupError::NotFound)
    }

    pub(in super::super) fn revoke_pending_owners(
        &self,
        owners: impl IntoIterator<Item = WriteOwner>,
        now: CatalogTick,
    ) -> usize {
        let owners: HashSet<_> = owners
            .into_iter()
            .filter(|owner| owner.session_generation() != 0)
            .collect();
        self.inner.revoke_pending_owners(&owners, now)
    }

    fn enqueue_pending_candidate(
        &self,
        slot: &Arc<ObjectSlot>,
        pending: &Arc<ObjectVersion>,
        transaction_id: TransactionId,
        now: CatalogTick,
    ) {
        self.inner
            .collector
            .pending
            .candidates
            .push(PendingCandidate {
                slot: Arc::downgrade(slot),
                pending: Arc::downgrade(pending),
                transaction_id,
                deadline: now.saturating_add(self.inner.config.pending_timeout_ticks),
            });
    }
}

impl WriteClaim {
    pub const fn id(&self) -> TransactionId {
        self.id
    }

    pub fn identity(&self) -> &ObjectIdentity {
        &self.identity
    }

    pub fn owner(&self) -> WriteOwner {
        self.admission.owner()
    }

    pub fn stage(
        self,
        content: ObjectContent,
        replicas: ReplicaSet,
    ) -> Result<WriteTransaction, StageError> {
        self.stage_inner(content, replicas, None)
    }

    pub(crate) fn stage_accounted(
        self,
        content: ObjectContent,
        replicas: ReplicaSet,
        reservation: QuotaReservationGuard,
    ) -> Result<WriteTransaction, StageError> {
        self.stage_inner(content, replicas, Some(reservation))
    }

    fn stage_inner(
        mut self,
        content: ObjectContent,
        replicas: ReplicaSet,
        accounting: Option<QuotaReservationGuard>,
    ) -> Result<WriteTransaction, StageError> {
        if !self.admission.is_active() {
            return Err(StageError::OwnerInactive);
        }
        let logical_bytes = content.logical_bytes();
        if logical_bytes == 0 {
            return Err(StageError::ZeroSize);
        }
        validate_stage_replicas(&replicas, logical_bytes)?;
        let catalog = self.catalog.upgrade().ok_or(StageError::CatalogDropped)?;
        let slot = self.slot.upgrade().ok_or(StageError::ClaimLost)?;
        let record = ObjectRecord {
            identity: self.identity.clone(),
            content,
            replicas: ReplicaStorage::new(replicas),
            accounting: accounting.map(QuotaReservationGuard::into_charge),
        };
        let pending = Arc::new(ObjectVersion::pending(
            self.id,
            self.admission.owner(),
            record,
        ));
        let reserved_bytes = pending.record().reserved_bytes();

        let _stage = catalog.collector.pending.stage_gate.read();
        let mut control = slot.control.lock();
        let Some(active) = control.active.as_mut() else {
            return Err(StageError::ClaimLost);
        };
        if active.id != self.id
            || active.owner != self.admission.owner()
            || !matches!(active.phase, TransactionPhase::Claimed)
        {
            return Err(StageError::ClaimLost);
        }
        active.phase = TransactionPhase::Staged(pending.clone());
        catalog.lifecycle.on_stage(reserved_bytes);
        let transaction = WriteTransaction {
            catalog: Arc::downgrade(&catalog),
            slot: Arc::downgrade(&slot),
            pending: pending.clone(),
            id: self.id,
        };
        self.armed = false;
        drop(control);
        ObjectCatalog {
            inner: catalog.clone(),
        }
        .enqueue_pending_candidate(&slot, &pending, self.id, self.started_at);
        drop(_stage);

        if !self.admission.is_active() {
            let _ = ObjectCatalog { inner: catalog }.abort(&transaction, self.started_at);
            return Err(StageError::OwnerInactive);
        }
        Ok(transaction)
    }
}

impl Drop for WriteClaim {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(catalog) = self.catalog.upgrade() else {
            return;
        };
        let Some(slot) = self.slot.upgrade() else {
            return;
        };
        let mut control = slot.control.lock();
        let Some(active) = control.active.as_ref() else {
            return;
        };
        if active.id != self.id || !matches!(active.phase, TransactionPhase::Claimed) {
            return;
        }
        let base_is_invalid = active
            .base
            .as_ref()
            .is_some_and(|base| !base.record().replicas.read().has_live());
        control.active = None;
        catalog.lifecycle.on_claim_dropped();
        let enqueue_empty = !slot.has_committed();
        drop(control);
        if enqueue_empty {
            catalog.enqueue_empty(&slot, self.started_at);
        }
        if base_is_invalid {
            catalog.collector.liveness.request();
        }
    }
}

impl fmt::Debug for WriteClaim {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WriteClaim")
            .field("identity", &self.identity)
            .field("id", &self.id)
            .field("owner", &self.admission.owner())
            .finish_non_exhaustive()
    }
}

impl WriteTransaction {
    pub const fn id(&self) -> TransactionId {
        self.id
    }

    pub fn identity(&self) -> &ObjectIdentity {
        &self.pending.record().identity
    }

    pub fn content(&self) -> ObjectContent {
        self.pending.record().content
    }

    pub fn replicas(&self) -> ReplicaSetView<'_> {
        ReplicaSetView::new(self.pending.record().replicas.read())
    }

    pub fn owner(&self) -> WriteOwner {
        self.pending.owner
    }
}

impl fmt::Debug for WriteTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WriteTransaction")
            .field("identity", &self.identity())
            .field("id", &self.id)
            .field("content", &self.content())
            .field("replicas", &self.replicas())
            .finish()
    }
}
