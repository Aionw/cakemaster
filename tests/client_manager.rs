use cakemaster::client::{
    ActivateOutcome, CleanupReason, ClientCleanupReport, ClientId, ClientLifecycleConfig,
    ClientManager, ClientManagerError, ClientState, ClientTick, HeartbeatOutcome,
    SegmentUnmountOutcome,
};
use cakemaster::object::reclamation::CatalogTick;
use cakemaster::object::{
    DirectReplica, NamespaceId, ObjectContent, ObjectIdentity, ObjectKind, ObjectManager,
    ObjectPinRequest, ObjectPutPlan, ReplicaId, ReplicaLease, ReplicaSelector, ReplicaSet,
    WriteMode, WriteOwner,
};
use cakemaster::segment::error::AttachError;
use cakemaster::segment::placement::{AllocationSpec, PlacementRequest, ReplicaPolicy};
use cakemaster::segment::stats::SegmentState;
use cakemaster::segment::{
    MemoryRegion, SegmentId, SegmentIdentity, SegmentPool, SegmentSpec, TransportEndpoint,
    TransportProtocol,
};
use std::sync::Arc;

const CLIENT: ClientId = ClientId::new(7, 9);
const STORAGE_CLIENT: ClientId = ClientId::new(7, 10);

fn segment(index: u64, base: u64) -> SegmentSpec {
    segment_for(index, base, CLIENT)
}

fn segment_for(index: u64, base: u64, owner: ClientId) -> SegmentSpec {
    SegmentSpec::memory(
        SegmentIdentity::new(SegmentId::new(3, index), owner, format!("memory-{index}")),
        MemoryRegion::new(base, 4096),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
    )
}

fn client_manager_with_objects(
    ttl_ticks: u64,
) -> (ClientManager, Arc<ObjectManager>, Arc<SegmentPool>) {
    let config = ClientLifecycleConfig::new(16)
        .with_ttl(ttl_ticks)
        .with_cleanup_scan_budget(16);
    let pool = Arc::new(SegmentPool::new());
    let objects = Arc::new(ObjectManager::new(pool.clone()));
    let clients =
        ClientManager::with_config(pool.clone(), objects.pending_write_revoker(), config).unwrap();
    (clients, objects, pool)
}

fn client_manager(ttl_ticks: u64) -> (ClientManager, Arc<SegmentPool>) {
    let (clients, _, pool) = client_manager_with_objects(ttl_ticks);
    (clients, pool)
}

fn remount(
    clients: &ClientManager,
    client_id: ClientId,
    segments: Vec<SegmentSpec>,
) -> Result<ActivateOutcome, ClientManagerError> {
    clients.remount(client_id, segments, ClientTick::ZERO)
}

#[test]
fn heartbeat_and_remount_follow_the_public_session_contract() {
    let (clients, pool) = client_manager(10_000);
    assert_eq!(
        clients.heartbeat(CLIENT, ClientTick::ZERO),
        HeartbeatOutcome::NeedRemount
    );
    assert!(pool.is_empty());

    let session = match remount(&clients, CLIENT, vec![segment(1, 0x1000)]).unwrap() {
        ActivateOutcome::Activated(session) => session,
        ActivateOutcome::AlreadyActive(_) => panic!("first remount must activate the client"),
    };
    assert_eq!(
        pool.segment(SegmentId::new(3, 1)).unwrap().stats().state,
        SegmentState::Accepting
    );
    assert_eq!(
        clients.heartbeat(CLIENT, ClientTick::ZERO),
        HeartbeatOutcome::Alive(session)
    );
    assert!(matches!(
        remount(&clients, CLIENT, vec![segment(1, 0x1000)]),
        Ok(ActivateOutcome::AlreadyActive(current)) if current == session
    ));
    assert_eq!(
        clients.write_owner(CLIENT).unwrap(),
        WriteOwner::for_session(session)
    );

    assert!(matches!(
        remount(&clients, CLIENT, Vec::new()),
        Err(ClientManagerError::ActiveRemountConflict)
    ));
    assert_eq!(pool.len(), 1);
}

#[test]
fn failed_remount_rolls_back_new_segments() {
    let (clients, pool) = client_manager(10_000);
    let error = remount(
        &clients,
        CLIENT,
        vec![segment(1, 0x1000), segment(2, 0x3000), segment(3, 0x3800)],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        ClientManagerError::Attach(AttachError::OverlappingAddressRange { existing })
            if existing == SegmentId::new(3, 2)
    ));
    assert!(pool.is_empty());
    assert_eq!(
        clients.heartbeat(CLIENT, ClientTick::ZERO),
        HeartbeatOutcome::NeedRemount
    );
}

#[test]
fn activation_failure_restores_existing_quiesced_segments() {
    let config = ClientLifecycleConfig::new(1)
        .with_ttl(10_000)
        .with_cleanup_scan_budget(1);
    let pool = Arc::new(SegmentPool::new());
    let objects = ObjectManager::new(pool.clone());
    let clients =
        ClientManager::with_config(pool.clone(), objects.pending_write_revoker(), config).unwrap();
    let blocker = ClientId::new(7, 99);
    remount(&clients, blocker, Vec::new()).unwrap();

    let spec = segment(1, 0x1000);
    pool.attach_quiesced(spec.clone()).unwrap();
    let error = remount(&clients, CLIENT, vec![spec]).unwrap_err();

    assert!(matches!(
        error,
        ClientManagerError::Lifecycle(cakemaster::client::ClientLifecycleError::CapacityExceeded {
            max_clients: 1
        })
    ));
    assert_eq!(
        pool.segment(SegmentId::new(3, 1)).unwrap().stats().state,
        SegmentState::Quiesced
    );
    assert_eq!(
        clients.heartbeat(CLIENT, ClientTick::ZERO),
        HeartbeatOutcome::NeedRemount
    );
}

#[test]
fn first_mount_activation_failure_removes_the_new_attachment() {
    let config = ClientLifecycleConfig::new(1)
        .with_ttl(10_000)
        .with_cleanup_scan_budget(1);
    let pool = Arc::new(SegmentPool::new());
    let objects = ObjectManager::new(pool.clone());
    let clients =
        ClientManager::with_config(pool.clone(), objects.pending_write_revoker(), config).unwrap();
    remount(&clients, ClientId::new(7, 99), Vec::new()).unwrap();

    assert!(matches!(
        clients.mount_segment(CLIENT, segment(1, 0x1000), ClientTick::ZERO),
        Err(ClientManagerError::Lifecycle(
            cakemaster::client::ClientLifecycleError::CapacityExceeded { max_clients: 1 }
        ))
    ));
    assert!(pool.is_empty());
    assert_eq!(
        clients.heartbeat(CLIENT, ClientTick::ZERO),
        HeartbeatOutcome::NeedRemount
    );
}

#[test]
fn expiry_fences_remount_until_cleanup_finishes() {
    let (clients, _) = client_manager(5);
    remount(&clients, CLIENT, vec![segment(1, 0x1000)]).unwrap();

    assert_eq!(
        clients.heartbeat(CLIENT, ClientTick::new(u64::MAX)),
        HeartbeatOutcome::NeedRemount
    );
    assert!(matches!(
        clients.remount(CLIENT, vec![segment(1, 0x1000)], ClientTick::new(u64::MAX),),
        Err(ClientManagerError::Lifecycle(
            cakemaster::client::ClientLifecycleError::CleanupInProgress {
                state: ClientState::Expired,
            }
        ))
    ));
}

#[test]
fn cleanup_step_invalidates_segments_and_allows_a_new_session() {
    let (clients, pool) = client_manager(10_000);
    let first = remount(&clients, CLIENT, vec![segment(1, 0x1000)])
        .unwrap()
        .session();

    assert_eq!(
        clients
            .run_cleanup_step(ClientTick::ZERO, CatalogTick::ZERO)
            .unwrap(),
        ClientCleanupReport::default()
    );

    let report = clients
        .run_cleanup_step(ClientTick::new(u64::MAX), CatalogTick::ZERO)
        .unwrap();
    assert_eq!(report.completed_sessions, 1);
    assert_eq!(report.invalidated_segments, 1);
    assert!(pool.is_empty());

    let second = remount(&clients, CLIENT, vec![segment(2, 0x3000)])
        .unwrap()
        .session();
    assert!(second.generation() > first.generation());
}

#[test]
fn cleanup_revokes_pending_writes_for_the_exact_session() {
    let (clients, objects, pool) = client_manager_with_objects(10_000);
    remount(
        &clients,
        STORAGE_CLIENT,
        vec![segment_for(1, 0x1000, STORAGE_CLIENT)],
    )
    .unwrap();
    let session = remount(&clients, CLIENT, Vec::new()).unwrap().session();
    let object = ObjectIdentity::new(NamespaceId::DEFAULT, "pending");
    let plan = ObjectPutPlan::new(
        ObjectContent::new(4096).with_kind(ObjectKind::Tensor),
        PlacementRequest::new(AllocationSpec::new(4096), ReplicaPolicy::new(1)),
    );
    objects
        .start_put(
            object.clone(),
            clients.write_admission(CLIENT).unwrap(),
            plan,
            CatalogTick::ZERO,
        )
        .unwrap();

    let report = clients
        .drain_sessions([session], CleanupReason::GracefulUnmount, CatalogTick::ZERO)
        .unwrap();

    assert_eq!(report.revoked_pending_writes, 1);
    assert_eq!(report.invalidated_segments, 0);
    assert_eq!(objects.catalog().stats().pending_objects, 0);
    assert!(pool.segment(SegmentId::new(3, 1)).is_some());
    assert!(matches!(
        objects.finish_put(
            &object,
            WriteOwner::for_session(session),
            ReplicaSelector::All
        ),
        Err(cakemaster::object::error::ObjectManagerError::NotFound)
    ));
}

#[test]
fn fenced_write_admission_cannot_reach_pending() {
    let (clients, objects, pool) = client_manager_with_objects(10_000);
    remount(
        &clients,
        STORAGE_CLIENT,
        vec![segment_for(1, 0x1000, STORAGE_CLIENT)],
    )
    .unwrap();
    let session = remount(&clients, CLIENT, Vec::new()).unwrap().session();
    let claim = objects
        .catalog()
        .begin_write(
            ObjectIdentity::new(NamespaceId::DEFAULT, "fenced-before-stage"),
            clients.write_admission(CLIENT).unwrap(),
            WriteMode::Insert,
            ObjectPinRequest::default(),
            CatalogTick::ZERO,
        )
        .unwrap();
    let reservation = pool.reserve_on(SegmentId::new(3, 1), 4096).unwrap();

    let report = clients
        .drain_sessions([session], CleanupReason::GracefulUnmount, CatalogTick::ZERO)
        .unwrap();
    assert!(matches!(
        claim.stage(
            ObjectContent::new(4096),
            ReplicaSet::one(ReplicaLease::Direct(DirectReplica::new(
                ReplicaId::new(1),
                reservation,
            ))),
        ),
        Err(cakemaster::object::error::StageError::OwnerInactive)
    ));
    assert_eq!(report.revoked_pending_writes, 0);
    assert_eq!(objects.catalog().stats().pending_objects, 0);
}

#[test]
fn batch_drain_revokes_only_the_target_sessions() {
    let (clients, objects, _) = client_manager_with_objects(10_000);
    remount(
        &clients,
        STORAGE_CLIENT,
        vec![segment_for(1, 0x1000, STORAGE_CLIENT)],
    )
    .unwrap();
    let other_client = ClientId::new(7, 11);
    let surviving_client = ClientId::new(7, 12);
    let sessions = [
        remount(&clients, CLIENT, Vec::new()).unwrap().session(),
        remount(&clients, other_client, Vec::new())
            .unwrap()
            .session(),
    ];
    let surviving_session = remount(&clients, surviving_client, Vec::new())
        .unwrap()
        .session();
    let plan = ObjectPutPlan::new(
        ObjectContent::new(1024).with_kind(ObjectKind::Tensor),
        PlacementRequest::new(AllocationSpec::new(1024), ReplicaPolicy::new(1)),
    );
    let object_ids = [
        ObjectIdentity::new(NamespaceId::DEFAULT, "pending-a"),
        ObjectIdentity::new(NamespaceId::DEFAULT, "pending-b"),
        ObjectIdentity::new(NamespaceId::DEFAULT, "pending-survivor"),
    ];
    for (client, object) in [CLIENT, other_client, surviving_client]
        .into_iter()
        .zip(&object_ids)
    {
        objects
            .start_put(
                object.clone(),
                clients.write_admission(client).unwrap(),
                plan.clone(),
                CatalogTick::ZERO,
            )
            .unwrap();
    }

    let report = clients
        .drain_sessions(sessions, CleanupReason::GracefulUnmount, CatalogTick::ZERO)
        .unwrap();

    assert_eq!(report.completed_sessions, 2);
    assert_eq!(report.revoked_pending_writes, 2);
    assert_eq!(objects.catalog().stats().pending_objects, 1);
    for (session, object) in sessions.into_iter().zip(&object_ids[..2]) {
        assert!(matches!(
            objects.finish_put(
                object,
                WriteOwner::for_session(session),
                ReplicaSelector::All
            ),
            Err(cakemaster::object::error::ObjectManagerError::NotFound)
        ));
    }
    objects
        .finish_put(
            &object_ids[2],
            WriteOwner::for_session(surviving_session),
            ReplicaSelector::All,
        )
        .unwrap();
    assert_eq!(objects.catalog().stats().pending_objects, 0);
}

#[test]
fn mount_segment_establishes_a_session_and_appends_atomically() {
    let (clients, pool) = client_manager(10_000);
    let first = segment(1, 0x1000);
    let second = segment(2, 0x3000);

    let session = clients
        .mount_segment(CLIENT, first.clone(), ClientTick::ZERO)
        .unwrap()
        .session();
    assert!(matches!(
        clients.mount_segment(CLIENT, second.clone(), ClientTick::new(1)),
        Ok(ActivateOutcome::AlreadyActive(current)) if current == session
    ));
    assert!(matches!(
        clients.mount_segment(CLIENT, second.clone(), ClientTick::new(2)),
        Ok(ActivateOutcome::AlreadyActive(current)) if current == session
    ));

    let conflicting = segment(2, 0x5000);
    assert!(matches!(
        clients.mount_segment(CLIENT, conflicting, ClientTick::new(3)),
        Err(ClientManagerError::ActiveRemountConflict)
    ));
    let overlapping = segment(3, 0x3800);
    assert!(matches!(
        clients.mount_segment(CLIENT, overlapping, ClientTick::new(4)),
        Err(ClientManagerError::Attach(
            AttachError::OverlappingAddressRange { existing }
        )) if existing == second.identity().id()
    ));
    assert_eq!(pool.len(), 2);
    assert_eq!(
        pool.segment(second.identity().id()).unwrap().spec(),
        &second
    );
}

#[test]
fn unmount_segment_only_removes_the_target_and_keeps_the_session_active() {
    let (clients, pool) = client_manager(10_000);
    let first = segment(1, 0x1000);
    let second = segment(2, 0x3000);
    let session = remount(&clients, CLIENT, vec![first.clone(), second.clone()])
        .unwrap()
        .session();

    assert_eq!(
        clients
            .unmount_segment(CLIENT, first.identity().id())
            .unwrap(),
        SegmentUnmountOutcome::Unmounted
    );
    assert!(pool.segment(first.identity().id()).is_none());
    assert!(pool.reserve_on(second.identity().id(), 512).is_ok());
    assert_eq!(
        clients.write_owner(CLIENT).unwrap(),
        WriteOwner::for_session(session)
    );
    assert_eq!(
        clients
            .unmount_segment(CLIENT, first.identity().id())
            .unwrap(),
        SegmentUnmountOutcome::AlreadyAbsent
    );
}

#[test]
fn graceful_unmount_quiesces_now_and_invalidates_at_the_earliest_deadline() {
    let (clients, pool) = client_manager(10_000);
    let spec = segment(1, 0x1000);
    let id = spec.identity().id();
    clients
        .mount_segment(CLIENT, spec, ClientTick::ZERO)
        .unwrap();
    let reservation = pool.reserve_on(id, 512).unwrap();

    clients
        .schedule_graceful_unmount(CLIENT, id, ClientTick::new(100))
        .unwrap();
    clients
        .schedule_graceful_unmount(CLIENT, id, ClientTick::new(40))
        .unwrap();
    clients
        .schedule_graceful_unmount(CLIENT, id, ClientTick::new(80))
        .unwrap();
    assert_eq!(
        pool.segment(id).unwrap().stats().state,
        SegmentState::Quiesced
    );
    assert!(reservation.is_live());
    assert!(matches!(
        pool.reserve_on(id, 1),
        Err(cakemaster::segment::error::ReserveError::NotAccepting(current)) if current == id
    ));
    assert_eq!(
        clients.next_graceful_unmount_deadline(),
        Some(ClientTick::new(40))
    );
    assert_eq!(
        clients.run_graceful_unmount_step(ClientTick::new(39)),
        Default::default()
    );
    assert!(reservation.is_live());

    let report = clients.run_graceful_unmount_step(ClientTick::new(40));
    assert_eq!(report.completed, 1);
    assert_eq!(report.stale_or_cancelled, 0);
    assert_eq!(report.retried, 0);
    assert!(!reservation.is_live());
    assert!(pool.segment(id).is_none());
}

#[test]
fn cancelled_graceful_job_cannot_remove_a_reused_segment_id() {
    let (clients, pool) = client_manager(10_000);
    let spec = segment(1, 0x1000);
    let id = spec.identity().id();
    clients
        .mount_segment(CLIENT, spec.clone(), ClientTick::ZERO)
        .unwrap();
    clients
        .schedule_graceful_unmount(CLIENT, id, ClientTick::new(100))
        .unwrap();
    clients.unmount_segment(CLIENT, id).unwrap();
    clients
        .mount_segment(CLIENT, spec, ClientTick::new(1))
        .unwrap();

    assert_eq!(clients.next_graceful_unmount_deadline(), None);
    let report = clients.run_graceful_unmount_step(ClientTick::new(100));
    assert_eq!(report.completed, 0);
    assert_eq!(report.stale_or_cancelled, 1);
    assert_eq!(report.retried, 0);
    assert_eq!(
        pool.segment(id).unwrap().stats().state,
        SegmentState::Accepting
    );
}

#[test]
fn unexpected_graceful_transition_is_retried_after_100_ticks() {
    let (clients, pool) = client_manager(10_000);
    let spec = segment(1, 0x1000);
    let id = spec.identity().id();
    clients
        .mount_segment(CLIENT, spec, ClientTick::ZERO)
        .unwrap();
    clients
        .schedule_graceful_unmount(CLIENT, id, ClientTick::new(10))
        .unwrap();

    // Simulate an unexpected external transition after scheduling. The
    // graceful worker restores quiescence but preserves the job for retry.
    pool.reactivate(CLIENT, id).unwrap();
    let first = clients.run_graceful_unmount_step(ClientTick::new(10));
    assert_eq!(first.completed, 0);
    assert_eq!(first.stale_or_cancelled, 0);
    assert_eq!(first.retried, 1);
    assert_eq!(
        clients.next_graceful_unmount_deadline(),
        Some(ClientTick::new(110))
    );
    assert_eq!(
        pool.segment(id).unwrap().stats().state,
        SegmentState::Quiesced
    );

    let second = clients.run_graceful_unmount_step(ClientTick::new(110));
    assert_eq!(second.completed, 1);
    assert_eq!(second.stale_or_cancelled, 0);
    assert_eq!(second.retried, 0);
    assert!(pool.segment(id).is_none());
}

#[test]
fn segment_unmount_immediately_changes_object_read_liveness() {
    let (clients, objects, _) = client_manager_with_objects(10_000);
    let spec = segment(1, 0x1000);
    let id = spec.identity().id();
    clients
        .mount_segment(CLIENT, spec, ClientTick::ZERO)
        .unwrap();
    let object = ObjectIdentity::new(NamespaceId::DEFAULT, "unmount-liveness");
    let plan = ObjectPutPlan::new(
        ObjectContent::new(512),
        PlacementRequest::new(AllocationSpec::new(512), ReplicaPolicy::new(1)),
    );
    objects
        .start_put(
            object.clone(),
            clients.write_admission(CLIENT).unwrap(),
            plan,
            CatalogTick::ZERO,
        )
        .unwrap();
    objects
        .finish_put(
            &object,
            clients.write_owner(CLIENT).unwrap(),
            ReplicaSelector::All,
        )
        .unwrap();
    assert!(objects.get(object.as_lookup(), CatalogTick::ZERO).is_ok());

    clients.unmount_segment(CLIENT, id).unwrap();
    assert!(matches!(
        objects.get(object.as_lookup(), CatalogTick::ZERO),
        Err(cakemaster::object::error::LookupError::NotFound)
    ));
    assert_eq!(objects.catalog().stats().published_objects, 1);
}
