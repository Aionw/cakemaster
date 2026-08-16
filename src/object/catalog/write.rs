//! Write-side object lifecycle and ownership protocol.
//!
//! ```text
//! CLAIMED --stage--> PENDING --publish--> PUBLISHED --remove/evict--> RETIRING
//!    | drop            +--revoke/timeout--> RETIRING
//!    v
//!  cleared
//!
//! PUBLISHED --upsert--> UPDATING --reuse commit/rollback--> PUBLISHED
//!                            +----replacement commit--------> RETIRING
//! ```
//!
//! A single object's client RPCs map onto that internal lifecycle as follows.
//! Batch RPCs apply the same transition independently to each item and are
//! omitted. `Claimed` exists only inside a Start handler; a successful Start
//! response always means the write has reached `Pending` or `Updating`.
//!
//! ```text
//! PutStart                              missing -> Claimed -> Pending
//! PutEnd                                Pending -> Published
//! PutRevoke                             Pending -> Retiring
//!
//! UpsertStart
//!   missing key                         missing -> Claimed -> Pending
//!   same-size key                       Published(current) -> Updating(current)
//!   size-changing key                   Published(old) -> Updating(old)
//!                                       missing -> Claimed(new) -> Pending(new)
//!
//! UpsertEnd
//!   same-size key                       Updating(current) -> Published(current)
//!   size-changing key                   Pending(new) -> Published(new)
//!                                       Updating(old) -> Retiring(old)
//!
//! UpsertRevoke
//!   same-size key                       Updating(current) -> Published(current)
//!   size-changing key                   Pending(new) -> Retiring(new)
//!                                       Updating(old) -> Published(old)
//!
//! Remove                                Published -> Retiring
//! ExistKey / GetReplicaList             observe Published only
//! ```
//!
//! The current wire exposes ordinary Put only through `BatchPut*`; `Put*`
//! above names the transition of one item in that request.
//!
//! Pending timeout, replica invalidation, and client-session fencing follow
//! the same rollback transitions as the corresponding Revoke RPC.
//!
//! A slot's `current` pointer is the authority for ownership. The immutable
//! record is installed before `PENDING` is released, and commit metadata plus
//! accounting are finalized before `PUBLISHED` becomes visible to readers.
//! Publication and replica pruning are write-gate critical sections rather
//! than externally visible lifecycle states.
//! Claiming an existing empty slot can race slot collection, so the write path
//! revalidates both index membership and the current pointer before returning.

use super::*;
use std::collections::HashSet;

enum ClaimedOrOccupied {
    Claimed(PutClaim),
    Occupied(Arc<ObjectSlot>),
}

enum TicketValidationError {
    ForeignCatalog,
    ObjectGone,
}

fn put_conflict(state: ObjectState) -> PutError {
    match state {
        ObjectState::Claimed | ObjectState::Pending | ObjectState::Updating => {
            PutError::WriteInProgress
        }
        ObjectState::Published | ObjectState::Retiring => PutError::AlreadyExists,
    }
}

fn validate_stage_replicas(replicas: &ReplicaSet, required_bytes: u64) -> Result<(), StageError> {
    if replicas.is_empty() {
        return Err(StageError::NoReplicas);
    }
    let mut undersized = None;
    for replica in replicas.replicas() {
        // Preserve liveness as the primary error even when an earlier replica
        // was too small: invalid reservations cannot be staged at any size.
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

impl From<TicketValidationError> for PublishError {
    fn from(error: TicketValidationError) -> Self {
        match error {
            TicketValidationError::ForeignCatalog => Self::ForeignCatalog,
            TicketValidationError::ObjectGone => Self::ObjectGone,
        }
    }
}

impl From<TicketValidationError> for RevokeError {
    fn from(error: TicketValidationError) -> Self {
        match error {
            TicketValidationError::ForeignCatalog => Self::ForeignCatalog,
            TicketValidationError::ObjectGone => Self::ObjectGone,
        }
    }
}

impl ObjectCatalog {
    pub fn claim_put(
        &self,
        identity: ObjectIdentity,
        admission: WriteAdmission,
        now: CatalogTick,
    ) -> Result<PutClaim, PutError> {
        self.validate_claim_request(&identity)?;

        loop {
            let slot = match self.claim_or_get_slot(&identity, &admission, now) {
                ClaimedOrOccupied::Claimed(claim) => return Ok(claim),
                ClaimedOrOccupied::Occupied(slot) => slot,
            };
            if !self.prepare_indexed_slot(&slot) {
                continue;
            }
            if let Some(claim) = self.try_claim_empty_slot(&slot, &admission, now) {
                return Ok(claim);
            }

            let Some(previous) = slot.current.load_full() else {
                continue;
            };
            return Err(put_conflict(previous.mutation.state()));
        }
    }

    /// Claims an insert-or-update transaction.
    ///
    /// Published objects with the same logical size are updated in place and
    /// keep their reservations. Size-changing updates install a private
    /// pending generation while retaining the published generation for
    /// rollback. Missing keys use the ordinary put path.
    pub(in super::super) fn claim_upsert(
        &self,
        identity: ObjectIdentity,
        admission: WriteAdmission,
        content: ObjectContent,
        now: CatalogTick,
    ) -> Result<UpsertClaim, PutError> {
        self.validate_claim_request(&identity)?;

        loop {
            let slot = match self.claim_or_get_slot(&identity, &admission, now) {
                ClaimedOrOccupied::Claimed(claim) => return Ok(UpsertClaim::Write(claim)),
                ClaimedOrOccupied::Occupied(slot) => slot,
            };
            if !self.prepare_indexed_slot(&slot) {
                continue;
            }

            let Some(previous) = slot.current.load_full() else {
                if let Some(claim) = self.try_claim_empty_slot(&slot, &admission, now) {
                    return Ok(UpsertClaim::Write(claim));
                }
                continue;
            };

            let write = previous.mutation.lock();
            if !slot_points_to(&slot, &previous) {
                drop(write);
                continue;
            }
            let lifecycle = previous.mutation.state();
            if lifecycle != ObjectState::Published {
                return Err(put_conflict(lifecycle));
            }

            let reuses_allocation =
                previous.record().content.logical_bytes() == content.logical_bytes();
            previous
                .mutation
                .transition(ObjectState::Published, ObjectState::Updating)
                .expect("the node write gate keeps the published state stable");

            if reuses_allocation {
                // Reusing the allocation also reuses the CatalogNode. The
                // generation fences timeout candidates from older updates.
                let id = slot.next_write_id();
                previous
                    .mutation
                    .begin_in_place_update(id, admission.owner());
                drop(write);

                let ticket = PutTicket::new(&self.inner, &slot, previous.clone(), id);
                self.enqueue_pending_candidate(&slot, &previous, now);
                // Enqueue before the final session fence so cleanup either
                // observes this update or this side revokes it directly.
                if !admission.is_active() {
                    let _ = self.revoke(&ticket, now);
                    return Err(PutError::WriteInProgress);
                }
                return Ok(UpsertClaim::Reuse(ticket));
            }

            // The pointer swap is the replacement's linearization point. The
            // old node remains attached to the new claim for rollback.
            let id = slot.next_write_id();
            let node = Arc::new(CatalogNode::claimed_replacement(
                id,
                admission.owner(),
                Some(previous.clone()),
            ));
            let observed = slot.current.compare_and_swap(&previous, Some(node.clone()));
            if observed
                .as_ref()
                .is_some_and(|observed| Arc::ptr_eq(observed, &previous))
            {
                drop(write);
                return Ok(UpsertClaim::Write(
                    self.register_claim(&slot, node, id, admission, now),
                ));
            }
            previous.mutation.store(ObjectState::Published);
            drop(write);
        }
    }

    pub fn publish(
        &self,
        ticket: &PutTicket,
        commit: ObjectCommit,
    ) -> Result<ObjectHandle, PublishError> {
        let (slot, _write) = self.lock_ticket(ticket).map_err(PublishError::from)?;
        if !ticket.node.record().replicas.read().all_live() {
            return Err(PublishError::ReplicasInvalidated);
        }

        match ticket.node.mutation.state() {
            ObjectState::Updating => {
                if ticket.node.mutation.commit() != Some(commit) {
                    return Err(PublishError::CommitConflict);
                }
                ticket.node.mutation.commit_in_place_update();
                ticket.node.mutation.store(ObjectState::Published);
            }
            ObjectState::Pending => {
                ticket
                    .node
                    .mutation
                    .set_commit(commit)
                    .expect("the node write gate gives publication one owner");
                let reserved_bytes = ticket.node.record().reserved_bytes();
                ticket.node.commit_accounting();
                self.inner.lifecycle.on_publish(reserved_bytes);
                ticket.node.mutation.store(ObjectState::Published);
                if let Some(previous) = ticket.node.mutation.take_replacement() {
                    self.inner.retire_replaced(previous);
                }
                // Segment incarnation invalidation is independent of the
                // node gate, so publication needs one final liveness check.
                if !ticket.node.record().replicas.read().all_live() {
                    self.inner.collector.liveness.request();
                }
            }
            ObjectState::Published => {
                if ticket.node.mutation.commit() == Some(commit) {
                    return Ok(ObjectHandle {
                        node: ticket.node.clone(),
                    });
                } else {
                    return Err(PublishError::CommitConflict);
                }
            }
            ObjectState::Claimed | ObjectState::Retiring => {
                return Err(PublishError::NotPending);
            }
        }
        self.inner
            .collector
            .eviction
            .young
            .push(GcCandidate::new(&slot, &ticket.node));
        Ok(ObjectHandle {
            node: ticket.node.clone(),
        })
    }

    pub fn revoke(&self, ticket: &PutTicket, now: CatalogTick) -> Result<(), RevokeError> {
        let (slot, _write) = self.lock_ticket(ticket).map_err(RevokeError::from)?;
        match ticket.node.mutation.state() {
            ObjectState::Updating => {
                self.inner.rollback_in_place_update(&slot, &ticket.node);
                return Ok(());
            }
            ObjectState::Pending => ticket.node.mutation.store(ObjectState::Retiring),
            ObjectState::Published => return Err(RevokeError::AlreadyPublished),
            ObjectState::Claimed | ObjectState::Retiring => {
                return Err(RevokeError::ObjectGone);
            }
        }
        if !self
            .inner
            .retire_revoked_pending(slot, ticket.node.clone(), now)
        {
            return Err(RevokeError::ObjectGone);
        }
        Ok(())
    }

    /// Revokes every currently pending write owned by the fenced sessions.
    ///
    /// Callers fence every owner before invoking this method. Races with an
    /// write that is being published are serialized by the node write gate:
    /// either publication or revocation wins.
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

    pub(in super::super) fn inspect_write(
        &self,
        lookup: ObjectLookup<'_>,
    ) -> Result<ObjectWriteState, LookupError> {
        let slot = self
            .inner
            .lookup_slot(lookup)
            .ok_or(LookupError::NotFound)?;
        for _ in 0..3 {
            let node = slot.current.load_full().ok_or(LookupError::NotFound)?;
            let lifecycle = node.mutation.state();
            match lifecycle {
                ObjectState::Claimed => return Err(LookupError::NotReady),
                ObjectState::Retiring => return Err(LookupError::NotFound),
                ObjectState::Pending | ObjectState::Published | ObjectState::Updating => {}
            }
            if !slot_points_to(&slot, &node) || node.mutation.state() != lifecycle {
                continue;
            }
            return Ok(
                if matches!(lifecycle, ObjectState::Pending | ObjectState::Updating) {
                    let id = node.write_id();
                    ObjectWriteState::Pending(PutTicket::new(&self.inner, &slot, node, id))
                } else {
                    ObjectWriteState::Published(ObjectHandle { node })
                },
            );
        }
        Err(LookupError::NotFound)
    }
}

// Claim acquisition and ticket validation are kept below the public workflow
// so the lifecycle operations above remain readable from top to bottom.
impl ObjectCatalog {
    fn validate_claim_request(&self, identity: &ObjectIdentity) -> Result<(), PutError> {
        if identity.key().is_empty() {
            return Err(PutError::EmptyKey);
        }
        if self.inner.lifecycle.retired_bytes.load(Ordering::Relaxed)
            >= self.inner.config.max_retired_bytes
        {
            return Err(PutError::ReclamationBacklog);
        }
        Ok(())
    }

    fn claim_or_get_slot(
        &self,
        identity: &ObjectIdentity,
        admission: &WriteAdmission,
        now: CatalogTick,
    ) -> ClaimedOrOccupied {
        match self.inner.index.entries.entry_sync(identity.clone()) {
            Entry::Occupied(entry) => ClaimedOrOccupied::Occupied(entry.get().clone()),
            Entry::Vacant(entry) => {
                let id = WriteId::new(1);
                let node = Arc::new(CatalogNode::claimed(id, admission.owner()));
                // Publish an already-claimed slot into the index so the
                // fresh-key fast path never invokes an ArcSwap writer.
                let slot = Arc::new(ObjectSlot {
                    identity: Arc::new(entry.key().clone()),
                    state: AtomicU8::new(SLOT_OPEN),
                    generation: AtomicU64::new(id.generation()),
                    current: ArcSwapOption::new(Some(node.clone())),
                });
                drop(entry.insert_entry(slot.clone()));
                self.inner.index.on_slot_inserted();
                ClaimedOrOccupied::Claimed(self.register_claim(
                    &slot,
                    node,
                    id,
                    admission.clone(),
                    now,
                ))
            }
        }
    }

    /// Reopens a slot that the empty-slot collector may be trying to close.
    /// The index membership check fences a collector that already won removal.
    fn prepare_indexed_slot(&self, slot: &Arc<ObjectSlot>) -> bool {
        if slot.state.load(Ordering::Acquire) == SLOT_CLOSING {
            let _ = slot.state.compare_exchange(
                SLOT_CLOSING,
                SLOT_OPEN,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
        slot.state.load(Ordering::Acquire) == SLOT_OPEN && self.inner.slot_is_indexed(slot)
    }

    fn try_claim_empty_slot(
        &self,
        slot: &Arc<ObjectSlot>,
        admission: &WriteAdmission,
        now: CatalogTick,
    ) -> Option<PutClaim> {
        let id = slot.next_write_id();
        let node = Arc::new(CatalogNode::claimed(id, admission.owner()));
        if slot
            .current
            .compare_and_swap(&None::<Arc<CatalogNode>>, Some(node.clone()))
            .is_some()
        {
            return None;
        }
        // The empty-slot collector can remove the index entry between the
        // caller's first check and the pointer CAS. Do not return an orphaned
        // claim when that happens.
        if !self.inner.slot_is_indexed(slot) {
            let _ = clear_slot(slot, &node);
            return None;
        }
        let _ = slot.state.compare_exchange(
            SLOT_CLOSING,
            SLOT_OPEN,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        Some(self.register_claim(slot, node, id, admission.clone(), now))
    }

    fn register_claim(
        &self,
        slot: &Arc<ObjectSlot>,
        node: Arc<CatalogNode>,
        id: WriteId,
        admission: WriteAdmission,
        now: CatalogTick,
    ) -> PutClaim {
        debug_assert!(slot_points_to(slot, &node));
        self.inner.lifecycle.on_claim();
        PutClaim {
            catalog: Arc::downgrade(&self.inner),
            slot: Arc::downgrade(slot),
            node: Some(node),
            identity: slot.identity.clone(),
            id,
            admission,
            started_at: now,
        }
    }

    fn enqueue_pending_candidate(
        &self,
        slot: &Arc<ObjectSlot>,
        node: &Arc<CatalogNode>,
        now: CatalogTick,
    ) {
        let _stage = self.inner.collector.pending.stage_gate.read();
        self.inner
            .collector
            .pending
            .candidates
            .push(PendingCandidate {
                candidate: GcCandidate::new(slot, node),
                write_id: node.write_id(),
                deadline: now.saturating_add(self.inner.config.pending_timeout_ticks),
            });
    }

    fn lock_ticket<'ticket>(
        &self,
        ticket: &'ticket PutTicket,
    ) -> Result<(Arc<ObjectSlot>, parking_lot::MutexGuard<'ticket, ()>), TicketValidationError>
    {
        let ticket_catalog = ticket
            .catalog
            .upgrade()
            .ok_or(TicketValidationError::ObjectGone)?;
        if !Arc::ptr_eq(&ticket_catalog, &self.inner) {
            return Err(TicketValidationError::ForeignCatalog);
        }
        let slot = ticket
            .slot
            .upgrade()
            .ok_or(TicketValidationError::ObjectGone)?;
        let write = ticket.node.mutation.lock();
        // The slot pointer and write generation must be checked after taking
        // the gate: an in-place upsert can reuse the same CatalogNode.
        if !slot_points_to(&slot, &ticket.node)
            || ticket.node.write_id() != ticket.id
            || ticket.node.record.get().is_none()
        {
            return Err(TicketValidationError::ObjectGone);
        }
        Ok((slot, write))
    }
}

impl PutClaim {
    pub const fn id(&self) -> WriteId {
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
    ) -> Result<PutTicket, StageError> {
        self.stage_inner(content, replicas, None)
    }

    pub(crate) fn stage_accounted(
        self,
        content: ObjectContent,
        replicas: ReplicaSet,
        reservation: QuotaReservationGuard,
    ) -> Result<PutTicket, StageError> {
        self.stage_inner(content, replicas, Some(reservation))
    }

    fn stage_inner(
        mut self,
        content: ObjectContent,
        replicas: ReplicaSet,
        accounting: Option<QuotaReservationGuard>,
    ) -> Result<PutTicket, StageError> {
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
        let node = self.node.as_ref().ok_or(StageError::ClaimLost)?;
        if !slot_points_to(&slot, node) {
            return Err(StageError::ClaimLost);
        }
        if node.write_id() != self.id
            || node.owner() != self.admission.owner()
            || node.mutation.state() != ObjectState::Claimed
        {
            return Err(StageError::ClaimLost);
        }

        let replicas = ReplicaStorage::new(replicas);
        let reserved_bytes = replicas.reserved_bytes();
        let record = ObjectRecord {
            identity: self.identity.clone(),
            content,
            replicas,
            accounting: accounting.map(QuotaReservationGuard::into_charge),
        };
        if let Err(record) = node.record.set(record) {
            if let Some((charge, class, bytes)) = record.tenant_accounting() {
                charge.release_reserved(class, bytes);
            }
            return Err(StageError::ClaimLost);
        }
        if node
            .mutation
            .transition(ObjectState::Claimed, ObjectState::Pending)
            .is_err()
        {
            return Err(StageError::ClaimLost);
        }

        catalog.lifecycle.on_stage(reserved_bytes);
        let node = node.clone();
        let catalog_handle = ObjectCatalog {
            inner: catalog.clone(),
        };
        catalog_handle.enqueue_pending_candidate(&slot, &node, self.started_at);
        let ticket = PutTicket::new(&catalog, &slot, node, self.id);
        // The ticket now owns the staged transaction. Disarm PutClaim's drop
        // rollback before the final admission fence or before returning it.
        self.node = None;
        // Cleanup fences the guard before scanning the pending queue. Enqueue
        // first so cleanup either sees this node or this side revokes it.
        if !self.admission.is_active() {
            let _ = catalog_handle.revoke(&ticket, self.started_at);
            return Err(StageError::OwnerInactive);
        }
        Ok(ticket)
    }
}

impl Drop for PutClaim {
    fn drop(&mut self) {
        let Some(node) = self.node.take() else {
            return;
        };
        let Some(catalog) = self.catalog.upgrade() else {
            return;
        };
        let Some(slot) = self.slot.upgrade() else {
            return;
        };
        // Replacement claims own the old generation until stage succeeds;
        // dropping them must restore it instead of merely clearing the slot.
        if let Some(previous) = restore_previous(&slot, &node) {
            catalog.lifecycle.on_claim_dropped();
            catalog
                .collector
                .eviction
                .young
                .push(GcCandidate::new(&slot, &previous));
        } else if clear_slot(&slot, &node) {
            catalog.lifecycle.on_claim_dropped();
            catalog.enqueue_empty(&slot, self.started_at);
        }
    }
}

impl fmt::Debug for PutClaim {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PutClaim")
            .field("identity", &self.identity)
            .field("id", &self.id)
            .field("owner", &self.admission.owner())
            .finish_non_exhaustive()
    }
}

impl PutTicket {
    fn new(
        catalog: &Arc<CatalogInner>,
        slot: &Arc<ObjectSlot>,
        node: Arc<CatalogNode>,
        id: WriteId,
    ) -> Self {
        Self {
            catalog: Arc::downgrade(catalog),
            slot: Arc::downgrade(slot),
            node,
            id,
        }
    }

    pub const fn id(&self) -> WriteId {
        self.id
    }

    pub fn identity(&self) -> &ObjectIdentity {
        &self.node.record().identity
    }

    pub fn content(&self) -> ObjectContent {
        self.node.record().content
    }

    pub fn replicas(&self) -> ReplicaSetView<'_> {
        ReplicaSetView::new(self.node.record().replicas.read())
    }

    pub fn owner(&self) -> WriteOwner {
        self.node.owner()
    }
}

impl fmt::Debug for PutTicket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PutTicket")
            .field("identity", &self.identity())
            .field("id", &self.id)
            .field("content", &self.content())
            .field("replicas", &self.replicas())
            .finish()
    }
}
