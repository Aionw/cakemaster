use cakemaster::client::{
    ActivateOutcome, ClientId, ClientLifecycleConfig, ClientRegistry, ClientState, ClientTick,
    HeartbeatOutcome,
};
use cakemaster::segment::error::AttachError;
use cakemaster::segment::stats::SegmentState;
use cakemaster::segment::{
    MemoryRegion, SegmentId, SegmentIdentity, SegmentPool, SegmentSpec, TransportEndpoint,
    TransportProtocol,
};
use cakemaster_server::{ClientRuntime, ClientRuntimeError, MasterClock};
use std::sync::Arc;

const CLIENT: ClientId = ClientId::new(7, 9);

fn segment(index: u64, base: u64) -> SegmentSpec {
    SegmentSpec::memory(
        SegmentIdentity::new(SegmentId::new(3, index), CLIENT, format!("memory-{index}")),
        MemoryRegion::new(base, 4096),
        TransportEndpoint::new(TransportProtocol::Tcp, "127.0.0.1:12345"),
    )
}

fn runtime(ttl_ticks: u64) -> ClientRuntime {
    let registry = Arc::new(
        ClientRegistry::with_config(
            ClientLifecycleConfig::new(16)
                .with_ttl(ttl_ticks)
                .with_maintenance_budget(16),
        )
        .unwrap(),
    );
    ClientRuntime::with_registry(
        Arc::new(SegmentPool::new()),
        registry,
        MasterClock::new(),
        17,
    )
}

#[tokio::test]
async fn unknown_ping_does_not_allocate_and_remount_is_idempotent() {
    let runtime = runtime(10_000);
    assert_eq!(runtime.slot_count().await, 0);
    assert_eq!(
        runtime.ping(CLIENT).heartbeat(),
        HeartbeatOutcome::NeedRemount
    );
    assert_eq!(runtime.slot_count().await, 0);

    let first = runtime.remount(CLIENT, vec![segment(1, 0x1000)]).await;
    let session = match first.unwrap() {
        ActivateOutcome::Activated(session) => session,
        ActivateOutcome::AlreadyActive(_) => panic!("first remount must activate the client"),
    };
    assert_eq!(runtime.pool().len(), 1);
    assert_eq!(
        runtime
            .pool()
            .segment(SegmentId::new(3, 1))
            .unwrap()
            .stats()
            .state,
        SegmentState::Accepting
    );
    assert_eq!(runtime.slot_count().await, 1);
    assert_eq!(
        runtime.ping(CLIENT).heartbeat(),
        HeartbeatOutcome::Alive(session)
    );
    assert!(matches!(
        runtime.remount(CLIENT, vec![segment(1, 0x1000)]).await,
        Ok(ActivateOutcome::AlreadyActive(current)) if current == session
    ));

    assert!(matches!(
        runtime.remount(CLIENT, Vec::new()).await,
        Err(ClientRuntimeError::ActiveRemountConflict)
    ));
    assert_eq!(runtime.pool().len(), 1);
    assert_eq!(runtime.registry().active_session(CLIENT), Ok(session));
}

#[tokio::test]
async fn failed_remount_rolls_back_new_segments_and_unused_slot() {
    let runtime = runtime(10_000);
    let error = runtime
        .remount(
            CLIENT,
            vec![segment(1, 0x1000), segment(2, 0x3000), segment(3, 0x3800)],
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ClientRuntimeError::Attach(AttachError::OverlappingAddressRange { existing })
            if existing == SegmentId::new(3, 2)
    ));
    assert!(runtime.pool().is_empty());
    assert!(runtime.registry().is_empty());
    assert_eq!(runtime.slot_count().await, 0);
}

#[tokio::test]
async fn expiry_fences_ping_and_remount_until_cleanup_finishes() {
    let runtime = runtime(5);
    let session = runtime
        .remount(CLIENT, vec![segment(1, 0x1000)])
        .await
        .unwrap()
        .session();

    let cleanup = runtime
        .registry()
        .maintenance(ClientTick::new(u64::MAX), 16);
    assert_eq!(cleanup.len(), 1);
    assert_eq!(cleanup[0].session(), session);
    assert_eq!(runtime.registry().state(CLIENT), Some(ClientState::Expired));
    assert_eq!(
        runtime.ping(CLIENT).heartbeat(),
        HeartbeatOutcome::NeedRemount
    );
    assert!(matches!(
        runtime.remount(CLIENT, vec![segment(1, 0x1000)]).await,
        Err(ClientRuntimeError::Lifecycle(
            cakemaster::client::ClientLifecycleError::CleanupInProgress {
                state: ClientState::Expired,
            }
        ))
    ));
}
