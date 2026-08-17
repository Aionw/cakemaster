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
        loop {
            let Some(version) = slot.load_committed() else {
                let control = slot.control.lock();
                if slot.has_committed() {
                    continue;
                }
                return if control.active.is_some() {
                    Err(LookupError::NotReady)
                } else {
                    Err(LookupError::NotFound)
                };
            };
            let lease_expires_at = version.access().record_access(
                now,
                self.inner.config.lease_ttl_ticks,
                self.inner.config.lease_refresh_ticks,
            );
            let has_live_replica = version.record().replicas.read().has_live();
            if committed_points_to(&slot, &version) && has_live_replica {
                return Ok(ObjectRead {
                    object: ObjectHandle { version },
                    lease_expires_at,
                });
            }
            if committed_points_to(&slot, &version) {
                return Err(LookupError::NotFound);
            }
        }
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
        &self.version.record().identity
    }

    pub fn version_id(&self) -> VersionId {
        self.version.id()
    }

    pub fn content(&self) -> ObjectContent {
        self.version.record().content
    }

    pub fn commit(&self) -> ObjectCommit {
        self.version.commit()
    }

    pub fn replicas(&self) -> LiveReplicaView<'_> {
        LiveReplicaView::new(self.version.record().replicas.read())
    }

    pub fn is_live(&self) -> bool {
        self.version.record().replicas.read().has_live()
    }

    pub fn owner(&self) -> WriteOwner {
        self.version.owner
    }

    pub fn is_hard_pinned(&self) -> bool {
        self.version.access().hard_pinned()
    }

    pub fn soft_pin_expires_at(&self) -> Option<CatalogTick> {
        self.version.access().soft_pin_until()
    }

    pub fn is_soft_pinned(&self, now: CatalogTick) -> bool {
        self.version.access().is_soft_pinned(now)
    }
}

impl fmt::Debug for ObjectHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectHandle")
            .field("version_id", &self.version_id())
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
