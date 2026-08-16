use super::*;

impl ObjectCatalog {
    pub fn get(
        &self,
        lookup: ObjectLookup<'_>,
        now: CatalogTick,
    ) -> Result<ObjectRead, LookupError> {
        let slot = self
            .inner
            .lookup_slot(lookup)
            .ok_or(LookupError::NotFound)?;
        for _ in 0..3 {
            let node = slot.current.load_full().ok_or(LookupError::NotFound)?;
            match node.mutation.state() {
                ObjectState::Claimed | ObjectState::Pending => {
                    return Err(LookupError::NotReady);
                }
                ObjectState::Retiring => return Err(LookupError::NotFound),
                ObjectState::Published => {}
            }
            let lease_expires_at = node.access.record_access(
                now,
                self.inner.config.lease_ttl_ticks,
                self.inner.config.lease_refresh_ticks,
            );
            let has_live_replica = node.record().replicas.read().has_live();
            if node.mutation.state() == ObjectState::Published
                && slot_points_to(&slot, &node)
                && has_live_replica
            {
                return Ok(ObjectRead {
                    object: ObjectHandle { node },
                    lease_expires_at,
                });
            }
        }
        Err(LookupError::NotFound)
    }

    pub fn get_batch_into<'a, I>(
        &self,
        lookups: I,
        now: CatalogTick,
        output: &mut Vec<Result<ObjectRead, LookupError>>,
    ) where
        I: IntoIterator<Item = ObjectLookup<'a>>,
    {
        output.extend(lookups.into_iter().map(|lookup| self.get(lookup, now)));
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
        self.node
            .mutation
            .commit()
            .expect("published objects always have commit metadata")
    }

    pub fn replicas(&self) -> LiveReplicaView<'_> {
        LiveReplicaView::new(self.node.record().replicas.read())
    }

    /// Whether at least one replica still belongs to its original live segment
    /// incarnation.
    pub fn is_live(&self) -> bool {
        self.node.record().replicas.read().has_live()
    }

    pub fn owner(&self) -> WriteOwner {
        self.node.owner()
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

    pub fn is_live(&self) -> bool {
        self.object.is_live()
    }
}
