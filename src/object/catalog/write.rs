//! Write-side object lifecycle and ownership protocol.
//!
//! ```text
//! CLAIMED --stage--> PENDING --publish--> PUBLISHING --> PUBLISHED
//!    | drop            | revoke/timeout                     | remove/evict
//!    v                 +---------------> RETIRING <----------+
//!  cleared                                  |
//!                                           v
//!                                      retired queue
//! ```
//!
//! A slot's `current` pointer is the authority for ownership. The immutable
//! record is installed before `PENDING` is released, and commit metadata plus
//! accounting are finalized before `PUBLISHED` becomes visible to readers.
//! Claiming an existing empty slot can race slot collection, so the write path
//! revalidates both index membership and the current pointer before returning.

use super::*;

impl ObjectCatalog {
    pub fn claim_put(
        &self,
        identity: ObjectIdentity,
        owner: WriteOwner,
        now: CatalogTick,
    ) -> Result<PutClaim, PutError> {
        if identity.key().is_empty() {
            return Err(PutError::EmptyKey);
        }
        if self.inner.lifecycle.retired_bytes.load(Ordering::Relaxed)
            >= self.inner.config.max_retired_bytes
        {
            return Err(PutError::ReclamationBacklog);
        }

        loop {
            let slot = match self.inner.index.entries.entry_sync(identity.clone()) {
                Entry::Occupied(entry) => entry.get().clone(),
                Entry::Vacant(entry) => {
                    let id = WriteId::new(1);
                    let node = Arc::new(CatalogNode::claimed(id, owner));
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
                    self.inner.lifecycle.on_claim();
                    return Ok(PutClaim {
                        catalog: Arc::downgrade(&self.inner),
                        slot: Arc::downgrade(&slot),
                        node: Some(node),
                        identity: slot.identity.clone(),
                        id,
                        owner,
                        started_at: now,
                    });
                }
            };

            if slot.state.load(Ordering::Acquire) == SLOT_CLOSING {
                let _ = slot.state.compare_exchange(
                    SLOT_CLOSING,
                    SLOT_OPEN,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
            if slot.state.load(Ordering::Acquire) != SLOT_OPEN || !self.inner.slot_is_indexed(&slot)
            {
                continue;
            }

            let id = slot.next_write_id();
            let node = Arc::new(CatalogNode::claimed(id, owner));
            let previous = slot
                .current
                .compare_and_swap(&None::<Arc<CatalogNode>>, Some(node.clone()));
            if previous.is_none() {
                if !self.inner.slot_is_indexed(&slot) {
                    let _ = clear_slot(&slot, &node);
                    continue;
                }
                let _ = slot.state.compare_exchange(
                    SLOT_CLOSING,
                    SLOT_OPEN,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                self.inner.lifecycle.on_claim();
                return Ok(PutClaim {
                    catalog: Arc::downgrade(&self.inner),
                    slot: Arc::downgrade(&slot),
                    node: Some(node),
                    identity: slot.identity.clone(),
                    id,
                    owner,
                    started_at: now,
                });
            }

            let lifecycle = previous
                .as_ref()
                .expect("the slot was occupied")
                .control
                .lifecycle
                .load(Ordering::Acquire);
            return Err(match lifecycle {
                OBJECT_CLAIMED | OBJECT_PENDING | OBJECT_PUBLISHING => PutError::WriteInProgress,
                OBJECT_PUBLISHED | OBJECT_RETIRING => PutError::AlreadyExists,
                _ => unreachable!("object lifecycle is validated internally"),
            });
        }
    }

    pub fn publish(
        &self,
        ticket: &PutTicket,
        commit: ObjectCommit,
    ) -> Result<ObjectHandle, PublishError> {
        let ticket_catalog = ticket.catalog.upgrade().ok_or(PublishError::ObjectGone)?;
        if !Arc::ptr_eq(&ticket_catalog, &self.inner) {
            return Err(PublishError::ForeignCatalog);
        }
        let slot = ticket.slot.upgrade().ok_or(PublishError::ObjectGone)?;
        if !slot_points_to(&slot, &ticket.node) {
            return Err(PublishError::ObjectGone);
        }
        if ticket.node.control.write_id != ticket.id || ticket.node.record.get().is_none() {
            return Err(PublishError::ObjectGone);
        }

        match ticket.node.control.lifecycle.compare_exchange(
            OBJECT_PENDING,
            OBJECT_PUBLISHING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                ticket
                    .node
                    .control
                    .commit
                    .set(commit)
                    .expect("the publishing transition has a single owner");
                let reserved_bytes = ticket
                    .node
                    .record
                    .get()
                    .expect("pending objects always have records")
                    .reserved_bytes;
                ticket.node.commit_accounting();
                self.inner.lifecycle.on_publish(reserved_bytes);
                ticket
                    .node
                    .control
                    .lifecycle
                    .store(OBJECT_PUBLISHED, Ordering::Release);
                self.inner
                    .collector
                    .young
                    .push(GcCandidate::new(&slot, &ticket.node));
                Ok(ObjectHandle {
                    node: ticket.node.clone(),
                })
            }
            Err(OBJECT_PUBLISHING) => Err(PublishError::PublicationInProgress),
            Err(OBJECT_PUBLISHED) => {
                if ticket.node.control.commit.get() == Some(&commit) {
                    Ok(ObjectHandle {
                        node: ticket.node.clone(),
                    })
                } else {
                    Err(PublishError::CommitConflict)
                }
            }
            Err(_) => Err(PublishError::NotPending),
        }
    }

    pub fn revoke(&self, ticket: &PutTicket, now: CatalogTick) -> Result<(), RevokeError> {
        let ticket_catalog = ticket.catalog.upgrade().ok_or(RevokeError::ObjectGone)?;
        if !Arc::ptr_eq(&ticket_catalog, &self.inner) {
            return Err(RevokeError::ForeignCatalog);
        }
        let slot = ticket.slot.upgrade().ok_or(RevokeError::ObjectGone)?;
        if ticket.node.record.get().is_none() {
            return Err(RevokeError::ObjectGone);
        }
        match ticket.node.control.lifecycle.compare_exchange(
            OBJECT_PENDING,
            OBJECT_RETIRING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(OBJECT_PUBLISHING | OBJECT_PUBLISHED) => {
                return Err(RevokeError::AlreadyPublished);
            }
            Err(_) => return Err(RevokeError::ObjectGone),
        }
        if !clear_slot(&slot, &ticket.node) {
            return Err(RevokeError::ObjectGone);
        }
        self.inner.retire_pending(slot, ticket.node.clone(), now);
        Ok(())
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
            let lifecycle = node.control.lifecycle.load(Ordering::Acquire);
            match lifecycle {
                OBJECT_CLAIMED | OBJECT_PUBLISHING => return Err(LookupError::NotReady),
                OBJECT_RETIRING => return Err(LookupError::NotFound),
                OBJECT_PENDING | OBJECT_PUBLISHED => {}
                _ => unreachable!("object lifecycle is validated internally"),
            }
            if !slot_points_to(&slot, &node)
                || node.control.lifecycle.load(Ordering::Acquire) != lifecycle
            {
                continue;
            }
            return Ok(if lifecycle == OBJECT_PENDING {
                ObjectWriteState::Pending(PutTicket {
                    catalog: Arc::downgrade(&self.inner),
                    slot: Arc::downgrade(&slot),
                    id: node.control.write_id,
                    node,
                })
            } else {
                ObjectWriteState::Published(ObjectHandle { node })
            });
        }
        Err(LookupError::NotFound)
    }
}

impl PutClaim {
    pub const fn id(&self) -> WriteId {
        self.id
    }

    pub fn identity(&self) -> &ObjectIdentity {
        &self.identity
    }

    pub const fn owner(&self) -> WriteOwner {
        self.owner
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
        if content.logical_bytes() == 0 {
            return Err(StageError::ZeroSize);
        }
        if replicas.is_empty() {
            return Err(StageError::NoReplicas);
        }
        if let Some(replica) = replicas
            .replicas()
            .iter()
            .find(|replica| replica.capacity_bytes() < content.logical_bytes())
        {
            return Err(StageError::ReplicaTooSmall {
                replica: replica.id(),
                required_bytes: content.logical_bytes(),
                capacity_bytes: replica.capacity_bytes(),
            });
        }
        let catalog = self.catalog.upgrade().ok_or(StageError::CatalogDropped)?;
        let slot = self.slot.upgrade().ok_or(StageError::ClaimLost)?;
        let node = self.node.as_ref().ok_or(StageError::ClaimLost)?;
        if !slot_points_to(&slot, node) {
            return Err(StageError::ClaimLost);
        }
        if node.control.write_id != self.id
            || node.control.owner != self.owner
            || node.control.lifecycle.load(Ordering::Acquire) != OBJECT_CLAIMED
        {
            return Err(StageError::ClaimLost);
        }

        let reserved_bytes = replicas.reserved_bytes();
        let record = ObjectRecord {
            identity: self.identity.clone(),
            content,
            replicas,
            reserved_bytes,
            accounting: accounting.map(QuotaReservationGuard::into_charge),
        };
        if let Err(record) = node.record.set(record) {
            if let Some((charge, class, bytes)) = record.tenant_accounting() {
                charge.release_reserved(class, bytes);
            }
            return Err(StageError::ClaimLost);
        }
        if node
            .control
            .lifecycle
            .compare_exchange(
                OBJECT_CLAIMED,
                OBJECT_PENDING,
                Ordering::Release,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(StageError::ClaimLost);
        }

        catalog.lifecycle.on_stage(reserved_bytes);
        let node = node.clone();
        let candidate = GcCandidate::new(&slot, &node);
        catalog.collector.pending.push(PendingCandidate {
            candidate,
            deadline: self
                .started_at
                .saturating_add(catalog.config.pending_timeout_ticks),
        });
        self.node = None;

        Ok(PutTicket {
            catalog: Arc::downgrade(&catalog),
            slot: Arc::downgrade(&slot),
            node,
            id: self.id,
        })
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
        if clear_slot(&slot, &node) {
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
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}

impl PutTicket {
    pub const fn id(&self) -> WriteId {
        self.id
    }

    pub fn identity(&self) -> &ObjectIdentity {
        &self.node.record().identity
    }

    pub fn content(&self) -> ObjectContent {
        self.node.record().content
    }

    pub fn replicas(&self) -> &[ReplicaLease] {
        self.node.record().replicas.replicas()
    }

    pub fn owner(&self) -> WriteOwner {
        self.node.control.owner
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
