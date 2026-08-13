use cakemaster::client::error::{ClientLifecycleConfigError, ClientLifecycleError};
use cakemaster::client::{
    ActivateOutcome, CleanupReason, ClientId, ClientLifecycleConfig, ClientRegistry, ClientState,
    ClientTick, HeartbeatOutcome,
};

fn client(value: u64) -> ClientId {
    ClientId::new(0, value)
}

fn registry(ttl_ticks: u64, max_clients: usize) -> ClientRegistry {
    ClientRegistry::with_config(
        ClientLifecycleConfig::new(max_clients)
            .with_ttl(ttl_ticks)
            .with_maintenance_budget(16),
    )
    .unwrap()
}

#[test]
fn rejects_invalid_configuration_and_nil_activation() {
    assert_eq!(
        ClientRegistry::with_config(ClientLifecycleConfig::new(8).with_ttl(0)).err(),
        Some(ClientLifecycleConfigError::ZeroTtl)
    );
    assert_eq!(
        ClientRegistry::with_config(ClientLifecycleConfig::new(0)).err(),
        Some(ClientLifecycleConfigError::ZeroMaxClients)
    );
    assert_eq!(
        ClientRegistry::with_config(ClientLifecycleConfig::new(8).with_maintenance_budget(0)).err(),
        Some(ClientLifecycleConfigError::ZeroMaintenanceBudget)
    );

    let registry = registry(10, 8);
    assert_eq!(
        registry.activate(ClientId::NIL, ClientTick::ZERO),
        Err(ClientLifecycleError::NilClientId)
    );
    assert!(registry.is_empty());
}

#[test]
fn unknown_heartbeat_requests_remount_without_allocating_state() {
    let registry = registry(10, 8);

    assert_eq!(
        registry.heartbeat(client(1), ClientTick::ZERO),
        HeartbeatOutcome::NeedRemount
    );
    assert_eq!(registry.len(), 0);
}

#[test]
fn repeated_activation_reuses_generation_and_refreshes_deadline() {
    let registry = registry(10, 8);
    let first = registry.activate(client(1), ClientTick::ZERO).unwrap();
    let session = match first {
        ActivateOutcome::Activated(session) => session,
        ActivateOutcome::AlreadyActive(_) => panic!("first activation must create a session"),
    };

    assert_eq!(
        registry.activate(client(1), ClientTick::new(5)),
        Ok(ActivateOutcome::AlreadyActive(session))
    );
    assert!(registry.maintenance(ClientTick::new(10), 8).is_empty());
    assert_eq!(registry.state(client(1)), Some(ClientState::Active));

    let cleanup = registry.maintenance(ClientTick::new(15), 8);
    assert_eq!(cleanup.len(), 1);
    assert_eq!(cleanup[0].session(), session);
    assert_eq!(cleanup[0].reason(), CleanupReason::HeartbeatExpired);
}

#[test]
fn heartbeat_deadline_is_monotonic_and_old_heap_record_is_reordered() {
    let registry = registry(10, 8);
    let session = registry
        .activate(client(1), ClientTick::new(10))
        .unwrap()
        .session();

    assert_eq!(
        registry.heartbeat(client(1), ClientTick::new(15)),
        HeartbeatOutcome::Alive(session)
    );
    assert_eq!(
        registry.heartbeat(client(1), ClientTick::new(12)),
        HeartbeatOutcome::Alive(session)
    );

    assert!(registry.maintenance(ClientTick::new(20), 8).is_empty());
    assert!(registry.maintenance(ClientTick::new(24), 8).is_empty());
    assert_eq!(registry.state(client(1)), Some(ClientState::Active));

    let cleanup = registry.maintenance(ClientTick::new(25), 8);
    assert_eq!(cleanup.len(), 1);
    assert_eq!(cleanup[0].session(), session);
}

#[test]
fn heartbeat_at_deadline_fences_session_until_cleanup_finishes() {
    let registry = registry(5, 8);
    let session = registry
        .activate(client(1), ClientTick::ZERO)
        .unwrap()
        .session();

    assert_eq!(
        registry.heartbeat(client(1), ClientTick::new(5)),
        HeartbeatOutcome::NeedRemount
    );
    assert_eq!(registry.state(client(1)), Some(ClientState::Expired));
    assert_eq!(
        registry.heartbeat(client(1), ClientTick::new(4)),
        HeartbeatOutcome::NeedRemount
    );
    assert_eq!(
        registry.activate(client(1), ClientTick::new(5)),
        Err(ClientLifecycleError::CleanupInProgress {
            state: ClientState::Expired,
        })
    );

    let cleanup = registry.maintenance(ClientTick::new(5), 8);
    assert_eq!(cleanup.len(), 1);
    assert_eq!(cleanup[0].session(), session);
    assert_eq!(cleanup[0].reason(), CleanupReason::HeartbeatExpired);
    assert!(registry.maintenance(ClientTick::new(5), 8).is_empty());

    registry.finish_cleanup(session).unwrap();
    let next = registry
        .activate(client(1), ClientTick::new(5))
        .unwrap()
        .session();
    assert!(next.generation() > session.generation());
}

#[test]
fn drain_and_finish_are_idempotent_and_generation_fenced() {
    let registry = registry(10, 8);
    let old = registry
        .activate(client(1), ClientTick::ZERO)
        .unwrap()
        .session();

    let cleanup = registry
        .begin_drain(old, CleanupReason::GracefulUnmount)
        .unwrap()
        .expect("first drain must emit cleanup work");
    assert_eq!(cleanup.session(), old);
    assert_eq!(cleanup.reason(), CleanupReason::GracefulUnmount);
    assert_eq!(registry.state(client(1)), Some(ClientState::Draining));
    assert_eq!(
        registry.begin_drain(old, CleanupReason::ServerShutdown),
        Ok(None)
    );
    assert_eq!(
        registry.activate(client(1), ClientTick::new(1)),
        Err(ClientLifecycleError::CleanupInProgress {
            state: ClientState::Draining,
        })
    );

    registry.finish_cleanup(old).unwrap();
    registry.finish_cleanup(old).unwrap();
    let current = registry
        .activate(client(1), ClientTick::new(1))
        .unwrap()
        .session();
    assert!(current.generation() > old.generation());
    assert_eq!(
        registry.finish_cleanup(old),
        Err(ClientLifecycleError::StaleSession)
    );
    assert_eq!(registry.active_session(client(1)), Ok(current));
}

#[test]
fn cleanup_cannot_finish_before_work_is_issued() {
    let registry = registry(5, 8);
    let session = registry
        .activate(client(1), ClientTick::ZERO)
        .unwrap()
        .session();

    assert_eq!(
        registry.heartbeat(client(1), ClientTick::new(5)),
        HeartbeatOutcome::NeedRemount
    );
    assert_eq!(
        registry.finish_cleanup(session),
        Err(ClientLifecycleError::CleanupNotStarted)
    );
    let cleanup = registry
        .begin_drain(session, CleanupReason::ServerShutdown)
        .unwrap()
        .expect("fenced session still needs one cleanup event");
    assert_eq!(cleanup.reason(), CleanupReason::HeartbeatExpired);
    registry.finish_cleanup(session).unwrap();
}

#[test]
fn maintenance_budget_leaves_remaining_due_clients_for_later_rounds() {
    let registry = ClientRegistry::with_config(
        ClientLifecycleConfig::new(8)
            .with_ttl(5)
            .with_maintenance_budget(1),
    )
    .unwrap();
    for value in 1..=3 {
        registry.activate(client(value), ClientTick::ZERO).unwrap();
    }

    assert_eq!(registry.maintenance(ClientTick::new(5), 8).len(), 1);
    assert_eq!(
        (1..=3)
            .filter(|value| registry.state(client(*value)) == Some(ClientState::Expired))
            .count(),
        1
    );
    assert_eq!(registry.maintenance(ClientTick::new(5), 8).len(), 1);
    assert_eq!(registry.maintenance(ClientTick::new(5), 8).len(), 1);
    assert!(registry.maintenance(ClientTick::new(5), 8).is_empty());
}

#[test]
fn capacity_counts_fenced_entries_until_cleanup_finishes() {
    let registry = registry(10, 2);
    let first = registry
        .activate(client(1), ClientTick::ZERO)
        .unwrap()
        .session();
    registry.activate(client(2), ClientTick::ZERO).unwrap();

    assert_eq!(
        registry.activate(client(3), ClientTick::ZERO),
        Err(ClientLifecycleError::CapacityExceeded { max_clients: 2 })
    );
    registry
        .begin_drain(first, CleanupReason::GracefulUnmount)
        .unwrap();
    assert_eq!(
        registry.activate(client(3), ClientTick::ZERO),
        Err(ClientLifecycleError::CapacityExceeded { max_clients: 2 })
    );

    registry.finish_cleanup(first).unwrap();
    assert!(matches!(
        registry.activate(client(3), ClientTick::ZERO),
        Ok(ActivateOutcome::Activated(_))
    ));
}
