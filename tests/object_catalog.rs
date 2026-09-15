use cakemaster::object::error::{
    BeginError, CommitError, LookupError, ObjectCatalogConfigError, RemoveError, StageError,
};
use cakemaster::object::reclamation::{CatalogTick, CollectBudget};
use cakemaster::object::{
    DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS, DEFAULT_MAX_SOFT_PIN_TTL_TICKS,
    DEFAULT_SOFT_PIN_TTL_TICKS, DirectReplica, NamespaceId, ObjectCatalog, ObjectCatalogConfig,
    ObjectCommit, ObjectContent, ObjectIdentity, ObjectPinRequest, ReplicaId, ReplicaSet,
    WriteAdmission, WriteClaim, WriteMode, WriteOwner,
};
use cakemaster::segment::{
    ClientId, MemoryRegion, SegmentId, SegmentIdentity, SegmentPool, SegmentPoolConfig,
    SegmentSpec, TransportEndpoint, TransportProtocol,
};
use std::sync::Arc;

const OWNER: ClientId = ClientId::new(17, 23);
const SEGMENT_ID: SegmentId = SegmentId::new(9, 1);

fn pool(capacity: u64, allocator_nodes: u32) -> Arc<SegmentPool> {
    let pool = Arc::new(SegmentPool::with_config(SegmentPoolConfig::new(allocator_nodes)).unwrap());
    pool.attach(SegmentSpec::memory(
        SegmentIdentity::new(SEGMENT_ID, OWNER, "catalog-memory"),
        MemoryRegion::new(0x2_0000_0000, capacity),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
    ))
    .unwrap();
    pool
}

fn identity(key: impl Into<Arc<str>>) -> ObjectIdentity {
    ObjectIdentity::new(NamespaceId::DEFAULT, key)
}

fn owner() -> WriteOwner {
    WriteOwner::new(OWNER)
}

fn admission() -> WriteAdmission {
    WriteAdmission::unmanaged(OWNER)
}

fn begin_insert(
    catalog: &ObjectCatalog,
    identity: ObjectIdentity,
    admission: WriteAdmission,
    now: CatalogTick,
) -> Result<WriteClaim, BeginError> {
    catalog.begin_write(
        identity,
        admission,
        WriteMode::Insert,
        ObjectPinRequest::default(),
        now,
    )
}

fn replica(pool: &SegmentPool, bytes: u64) -> ReplicaSet {
    ReplicaSet::one(DirectReplica::new(
        ReplicaId::new(1),
        pool.reserve_on(SEGMENT_ID, bytes).unwrap(),
    ))
}

#[test]
fn rejects_inconsistent_lease_configuration_with_context() {
    assert_eq!(
        ObjectCatalog::with_config(ObjectCatalogConfig::new(16).with_lease(10, 11)).err(),
        Some(ObjectCatalogConfigError::LeaseRefreshExceedsTtl {
            lease_ttl_ticks: 10,
            lease_refresh_ticks: 11,
        })
    );
}

#[test]
fn soft_pin_configuration_matches_upstream_defaults_and_validates_bounds() {
    let config = ObjectCatalogConfig::new(16);
    assert_eq!(
        config.default_soft_pin_ttl_ticks(),
        DEFAULT_SOFT_PIN_TTL_TICKS
    );
    assert_eq!(
        config.max_soft_pin_ttl_ticks(),
        DEFAULT_MAX_SOFT_PIN_TTL_TICKS
    );
    assert_eq!(
        config.allow_evict_soft_pinned_objects(),
        DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS
    );
    assert_eq!(
        ObjectCatalog::with_config(config.with_soft_pin_ttl(101, 100)).err(),
        Some(ObjectCatalogConfigError::DefaultSoftPinTtlExceedsMaximum {
            default_soft_pin_ttl_ticks: 101,
            max_soft_pin_ttl_ticks: 100,
        })
    );
}

#[test]
fn replica_set_preserves_inline_and_multiple_replica_views() {
    let pool = pool(1 << 20, 64);
    let inline = replica(&pool, 1024);
    assert_eq!(inline.len(), 1);
    assert_eq!(inline.reserved_bytes(), 1024);
    assert_eq!(inline.replicas()[0].id(), ReplicaId::new(1));
    drop(inline);

    let multiple = ReplicaSet::new([
        DirectReplica::new(
            ReplicaId::new(1),
            pool.reserve_on(SEGMENT_ID, 1024).unwrap(),
        ),
        DirectReplica::new(
            ReplicaId::new(2),
            pool.reserve_on(SEGMENT_ID, 2048).unwrap(),
        ),
    ]);
    assert_eq!(multiple.len(), 2);
    assert_eq!(multiple.reserved_bytes(), 3072);
    assert_eq!(multiple.replicas()[1].id(), ReplicaId::new(2));
    drop(multiple);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 0);
}

#[test]
fn dropped_claim_reopens_the_stable_slot() {
    let catalog = ObjectCatalog::with_config(
        ObjectCatalogConfig::new(16)
            .with_empty_slot_grace(2)
            .with_pending_timeout(10),
    )
    .unwrap();

    let first = begin_insert(
        &catalog,
        identity("same-key"),
        admission(),
        CatalogTick::new(1),
    )
    .unwrap();
    assert!(matches!(
        begin_insert(
            &catalog,
            identity("same-key"),
            admission(),
            CatalogTick::new(1)
        ),
        Err(BeginError::WriteInProgress)
    ));
    let first_id = first.id();
    drop(first);

    let second = begin_insert(
        &catalog,
        identity("same-key"),
        admission(),
        CatalogTick::new(2),
    )
    .unwrap();
    assert!(second.id().get() > first_id.get());
    drop(second);

    let report = catalog.collect_step(CatalogTick::new(4), CollectBudget::new(0, 0, 8));
    assert_eq!(report.removed_empty_slots, 1);
    assert_eq!(catalog.stats().slots, 0);
}

#[test]
fn publish_lookup_remove_and_reclaim_preserve_lease_and_ownership() {
    let pool = pool(1 << 20, 4096);
    let catalog = ObjectCatalog::with_config(
        ObjectCatalogConfig::new(16)
            .with_lease(10, 5)
            .with_empty_slot_grace(2),
    )
    .unwrap();
    let object = identity("published");
    let ticket = begin_insert(&catalog, object.clone(), admission(), CatalogTick::ZERO)
        .unwrap()
        .stage(ObjectContent::new(4096), replica(&pool, 4096))
        .unwrap();

    assert!(matches!(
        catalog.get(object.as_lookup(), CatalogTick::ZERO),
        Err(LookupError::NotReady)
    ));
    let published = catalog
        .commit(
            &ticket,
            ObjectCommit::new(Some(0xfeed_beef)),
            CatalogTick::ZERO,
        )
        .unwrap();
    drop(
        catalog
            .commit(
                &ticket,
                ObjectCommit::new(Some(0xfeed_beef)),
                CatalogTick::ZERO,
            )
            .unwrap(),
    );
    assert!(matches!(
        catalog.commit(&ticket, ObjectCommit::new(Some(7)), CatalogTick::ZERO),
        Err(CommitError::CommitConflict)
    ));
    assert_eq!(published.identity(), &object);
    assert_eq!(published.content().logical_bytes(), 4096);
    assert_eq!(published.commit().checksum(), Some(0xfeed_beef));
    assert_eq!(published.owner(), owner());

    let read = catalog
        .get(object.as_lookup(), CatalogTick::new(1))
        .unwrap();
    assert_eq!(read.lease_expires_at(), CatalogTick::new(11));
    assert_eq!(read.object().identity(), &object);
    assert_eq!(
        catalog.remove(object.as_lookup(), CatalogTick::new(1)),
        Err(RemoveError::Leased {
            expires_at: CatalogTick::new(11)
        })
    );

    drop(read);
    drop(published);
    drop(ticket);
    catalog
        .remove(object.as_lookup(), CatalogTick::new(11))
        .unwrap();
    let report = catalog.collect_step(CatalogTick::new(11), CollectBudget::new(0, 8, 0));
    assert_eq!(report.reclaimed_objects, 1);
    assert_eq!(report.reclaimed_bytes, 4096);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 0);
    assert_eq!(catalog.stats().published_objects, 0);
    assert_eq!(catalog.stats().retired_bytes, 0);

    let report = catalog.collect_step(CatalogTick::new(13), CollectBudget::new(0, 0, 8));
    assert_eq!(report.removed_empty_slots, 1);
    assert_eq!(catalog.stats().slots, 0);
}

#[test]
fn pending_timeout_reclaims_memory_without_a_batch_pause() {
    let pool = pool(1 << 20, 4096);
    let catalog = ObjectCatalog::with_config(
        ObjectCatalogConfig::new(16)
            .with_pending_timeout(5)
            .with_empty_slot_grace(1),
    )
    .unwrap();
    let object = identity("abandoned-write");
    let ticket = begin_insert(&catalog, object.clone(), admission(), CatalogTick::ZERO)
        .unwrap()
        .stage(ObjectContent::new(1024), replica(&pool, 1024))
        .unwrap();
    drop(ticket);

    let report = catalog.collect_step(CatalogTick::new(5), CollectBudget::new(8, 8, 0));
    assert_eq!(report.expired_pending, 1);
    assert_eq!(report.reclaimed_objects, 1);
    assert_eq!(catalog.stats().pending_objects, 0);
    assert_eq!(catalog.stats().retired_bytes, 0);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 0);
    assert!(matches!(
        catalog.get(object.as_lookup(), CatalogTick::new(5)),
        Err(LookupError::NotFound)
    ));
}

#[test]
fn pending_expiration_has_no_future_deadline_head_of_line_blocking() {
    let pool = pool(1 << 20, 4096);
    let catalog =
        ObjectCatalog::with_config(ObjectCatalogConfig::new(16).with_pending_timeout(10)).unwrap();
    let future = begin_insert(
        &catalog,
        identity("future"),
        admission(),
        CatalogTick::new(100),
    )
    .unwrap()
    .stage(ObjectContent::new(1024), replica(&pool, 1024))
    .unwrap();
    let expired = begin_insert(
        &catalog,
        identity("expired"),
        admission(),
        CatalogTick::ZERO,
    )
    .unwrap()
    .stage(ObjectContent::new(1024), replica(&pool, 1024))
    .unwrap();
    drop(future);
    drop(expired);

    let report = catalog.collect_step(CatalogTick::new(10), CollectBudget::new(2, 2, 0));
    assert_eq!(report.expired_pending, 1);
    assert_eq!(report.reclaimed_objects, 1);
    assert_eq!(catalog.stats().pending_objects, 1);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 1);

    drop(catalog);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 0);
}

#[test]
fn invalid_staging_rolls_back_claim_and_reservation_by_raii() {
    let pool = pool(1 << 20, 4096);
    let catalog = ObjectCatalog::new();
    let object = identity("undersized");
    let result = begin_insert(&catalog, object.clone(), admission(), CatalogTick::ZERO)
        .unwrap()
        .stage(ObjectContent::new(4096), replica(&pool, 1024));

    assert!(matches!(
        result,
        Err(StageError::ReplicaTooSmall {
            replica,
            required_bytes: 4096,
            capacity_bytes: 1024,
        }) if replica == ReplicaId::new(1)
    ));
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 0);
    assert_eq!(catalog.stats().claims, 0);
    assert!(begin_insert(&catalog, object, admission(), CatalogTick::new(1)).is_ok());
}

#[test]
fn invalidated_segment_replicas_cannot_be_staged_or_published() {
    let pool = pool(1 << 20, 4096);
    let catalog = ObjectCatalog::new();
    let publish_object = identity("invalidate-before-publish");
    let stage_object = identity("invalidate-before-stage");

    let ticket = begin_insert(&catalog, publish_object, admission(), CatalogTick::ZERO)
        .unwrap()
        .stage(ObjectContent::new(1024), replica(&pool, 1024))
        .unwrap();
    let claim = begin_insert(&catalog, stage_object, admission(), CatalogTick::ZERO).unwrap();
    let unstaged_replicas = replica(&pool, 1024);

    pool.quiesce(OWNER, SEGMENT_ID).unwrap();
    pool.remove(OWNER, SEGMENT_ID).unwrap();

    assert!(matches!(
        catalog.commit(&ticket, ObjectCommit::new(None), CatalogTick::ZERO),
        Err(CommitError::ReplicasInvalidated)
    ));
    assert!(matches!(
        claim.stage(ObjectContent::new(1024), unstaged_replicas),
        Err(StageError::ReplicaInvalidated { replica }) if replica == ReplicaId::new(1)
    ));
}

#[test]
fn reclamation_promotes_recent_objects_and_defers_pinned_resources() {
    let pool = pool(1 << 20, 4096);
    let catalog = ObjectCatalog::with_config(
        ObjectCatalogConfig::new(16)
            .with_lease(5, 2)
            .with_empty_slot_grace(1),
    )
    .unwrap();
    let hot = identity("hot");
    let cold = identity("cold");

    let hot_ticket = begin_insert(&catalog, hot.clone(), admission(), CatalogTick::ZERO)
        .unwrap()
        .stage(ObjectContent::new(4096), replica(&pool, 4096))
        .unwrap();
    let hot_handle = catalog
        .commit(&hot_ticket, ObjectCommit::default(), CatalogTick::ZERO)
        .unwrap();
    let cold_ticket = begin_insert(&catalog, cold.clone(), admission(), CatalogTick::ZERO)
        .unwrap()
        .stage(ObjectContent::new(4096), replica(&pool, 4096))
        .unwrap();
    let cold_handle = catalog
        .commit(&cold_ticket, ObjectCommit::default(), CatalogTick::ZERO)
        .unwrap();

    let pinned_hot = catalog.get(hot.as_lookup(), CatalogTick::ZERO).unwrap();
    drop(hot_ticket);
    drop(hot_handle);
    drop(cold_ticket);
    drop(cold_handle);

    catalog.request_reclaim(4096);
    let report = catalog.collect_step(CatalogTick::new(5), CollectBudget::new(8, 8, 0));
    assert_eq!(report.scanned_candidates, 2);
    assert_eq!(report.retired_objects, 1);
    assert_eq!(report.reclaimed_objects, 1);
    assert_eq!(catalog.stats().published_objects, 1);
    assert!(matches!(
        catalog.get(cold.as_lookup(), CatalogTick::new(5)),
        Err(LookupError::NotFound)
    ));

    // The read handle is an epoch pin: retirement detaches the object after
    // its lease expires, but reclamation cannot return the reservation yet.
    catalog.request_reclaim(4096);
    let report = catalog.collect_step(CatalogTick::new(10), CollectBudget::new(8, 8, 0));
    assert_eq!(report.retired_objects, 1);
    assert_eq!(report.reclaimed_objects, 0);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 1);
    assert_eq!(catalog.stats().retired_bytes, 4096);

    drop(pinned_hot);
    let report = catalog.collect_step(CatalogTick::new(11), CollectBudget::new(8, 8, 0));
    assert_eq!(report.reclaimed_objects, 1);
    assert_eq!(pool.stats(SEGMENT_ID).unwrap().usage.active_allocations, 0);
}
