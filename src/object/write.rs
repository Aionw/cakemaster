use crate::client::{ClientSession, ClientSessionGuard};
use crate::segment::ClientId;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectCommit {
    checksum: Option<u64>,
}

/// Whether a transaction requires an absent key or replaces the current
/// committed version when one exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteMode {
    Insert,
    Upsert,
}

impl ObjectCommit {
    pub const fn new(checksum: Option<u64>) -> Self {
        Self { checksum }
    }

    pub const fn checksum(self) -> Option<u64> {
        self.checksum
    }
}

/// Stable identity of the client incarnation that owns a write.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WriteOwner {
    client: ClientId,
    session_generation: u64,
}

/// Admission capability retained only while a claimed write can become
/// pending. Catalog nodes store the compact [`WriteOwner`] identity instead.
#[derive(Clone, Debug)]
pub struct WriteAdmission {
    owner: WriteOwner,
    session_guard: Option<ClientSessionGuard>,
}

impl WriteOwner {
    pub const fn new(client: ClientId) -> Self {
        Self {
            client,
            session_generation: 0,
        }
    }

    pub const fn for_session(session: ClientSession) -> Self {
        Self {
            client: session.client_id(),
            session_generation: session.generation(),
        }
    }

    pub const fn client(&self) -> ClientId {
        self.client
    }

    /// Zero identifies legacy/direct callers that do not participate in
    /// server-managed client sessions.
    pub const fn session_generation(&self) -> u64 {
        self.session_generation
    }
}

impl WriteAdmission {
    /// Creates an admission capability for direct callers that do not
    /// participate in server-managed client sessions.
    pub const fn unmanaged(client: ClientId) -> Self {
        Self {
            owner: WriteOwner::new(client),
            session_guard: None,
        }
    }

    pub(crate) fn guarded(guard: ClientSessionGuard) -> Self {
        Self {
            owner: WriteOwner::for_session(guard.session()),
            session_guard: Some(guard),
        }
    }

    pub const fn owner(&self) -> WriteOwner {
        self.owner
    }

    pub(crate) fn is_active(&self) -> bool {
        self.session_guard
            .as_ref()
            .is_none_or(ClientSessionGuard::is_active)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TransactionId(u64);

impl TransactionId {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct VersionId(u64);

impl VersionId {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}
