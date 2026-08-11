use super::*;

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

        catalog.claims.fetch_sub(1, Ordering::Relaxed);
        catalog.pending_objects.fetch_add(1, Ordering::Relaxed);
        catalog
            .pending_bytes
            .fetch_add(reserved_bytes, Ordering::Relaxed);
        let node = node.clone();
        let candidate = GcCandidate::new(&slot, &node);
        catalog.pending.push(PendingCandidate {
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
            catalog.claims.fetch_sub(1, Ordering::Relaxed);
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

impl ObjectHandle {
    pub fn identity(&self) -> &ObjectIdentity {
        &self.node.record().identity
    }

    pub fn content(&self) -> ObjectContent {
        self.node.record().content
    }

    pub fn commit(&self) -> ObjectCommit {
        *self
            .node
            .control
            .commit
            .get()
            .expect("published objects always have commit metadata")
    }

    pub fn replicas(&self) -> &[ReplicaLease] {
        self.node.record().replicas.replicas()
    }

    pub fn owner(&self) -> WriteOwner {
        self.node.control.owner
    }
}

impl fmt::Debug for ObjectHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectHandle")
            .field("identity", &self.identity())
            .field("content", &self.content())
            .field("commit", &self.commit())
            .field("replicas", &self.replicas())
            .finish()
    }
}

impl ObjectRead {
    pub const fn object(&self) -> &ObjectHandle {
        &self.object
    }

    pub const fn lease_expires_at(&self) -> CatalogTick {
        self.lease_expires_at
    }
}
