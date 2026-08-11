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
            match node.control.lifecycle.load(Ordering::Acquire) {
                OBJECT_CLAIMED | OBJECT_PENDING | OBJECT_PUBLISHING => {
                    return Err(LookupError::NotReady);
                }
                OBJECT_RETIRING => return Err(LookupError::NotFound),
                OBJECT_PUBLISHED => {}
                _ => unreachable!("object lifecycle is validated internally"),
            }

            let lease_expires_at = node.control.acquire_lease(
                now,
                self.inner.config.lease_ttl_ticks,
                self.inner.config.lease_refresh_ticks,
            );
            node.control.recent.store(true, Ordering::Relaxed);
            if node.control.lifecycle.load(Ordering::Acquire) == OBJECT_PUBLISHED
                && slot_points_to(&slot, &node)
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
