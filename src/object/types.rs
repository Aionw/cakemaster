use crate::segment::{
    ClientId, MemoryDescriptor, MemoryDescriptorRef, Reservation, ReservationSet, SegmentId,
};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NamespaceId(u64);

impl NamespaceId {
    pub const DEFAULT: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObjectKey(Arc<str>);

impl ObjectKey {
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObjectIdentity {
    namespace: NamespaceId,
    key: ObjectKey,
}

impl ObjectIdentity {
    pub fn new(namespace: NamespaceId, key: impl Into<Arc<str>>) -> Self {
        Self {
            namespace,
            key: ObjectKey::new(key),
        }
    }

    pub const fn namespace(&self) -> NamespaceId {
        self.namespace
    }

    pub const fn key(&self) -> &ObjectKey {
        &self.key
    }

    pub fn as_lookup(&self) -> ObjectLookup<'_> {
        ObjectLookup::new(self.namespace, self.key.as_str())
    }
}

impl Hash for ObjectIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.namespace.hash(state);
        self.key.as_str().hash(state);
    }
}

impl fmt::Display for ObjectIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.namespace.get(), self.key)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectLookup<'a> {
    namespace: NamespaceId,
    key: &'a str,
}

impl<'a> ObjectLookup<'a> {
    pub const fn new(namespace: NamespaceId, key: &'a str) -> Self {
        Self { namespace, key }
    }

    pub const fn namespace(self) -> NamespaceId {
        self.namespace
    }

    pub const fn key(self) -> &'a str {
        self.key
    }
}

impl Hash for ObjectLookup<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.namespace.hash(state);
        self.key.hash(state);
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ObjectKind {
    #[default]
    KvCache,
    Tensor,
    General,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectContent {
    logical_bytes: u64,
    kind: ObjectKind,
}

impl ObjectContent {
    pub const fn new(logical_bytes: u64) -> Self {
        Self {
            logical_bytes,
            kind: ObjectKind::KvCache,
        }
    }

    pub const fn with_kind(mut self, kind: ObjectKind) -> Self {
        self.kind = kind;
        self
    }

    pub const fn logical_bytes(self) -> u64 {
        self.logical_bytes
    }

    pub const fn kind(self) -> ObjectKind {
        self.kind
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReplicaId(u32);

impl ReplicaId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

pub struct MemoryReplica {
    id: ReplicaId,
    reservation: Reservation,
}

impl MemoryReplica {
    pub fn new(id: ReplicaId, reservation: Reservation) -> Self {
        Self { id, reservation }
    }

    pub const fn id(&self) -> ReplicaId {
        self.id
    }

    pub fn segment_id(&self) -> SegmentId {
        self.reservation.segment_id()
    }

    pub const fn reserved_bytes(&self) -> u64 {
        self.reservation.reserved_bytes()
    }

    pub const fn capacity_bytes(&self) -> u64 {
        self.reservation.requested_bytes()
    }

    pub fn descriptor(&self) -> MemoryDescriptorRef<'_> {
        self.reservation.descriptor()
    }

    pub fn owned_descriptor(&self) -> MemoryDescriptor {
        self.reservation.owned_descriptor()
    }

    pub(crate) fn into_reservation(self) -> Reservation {
        self.reservation
    }
}

impl fmt::Debug for MemoryReplica {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryReplica")
            .field("id", &self.id)
            .field("segment_id", &self.segment_id())
            .field("reserved_bytes", &self.reserved_bytes())
            .field("descriptor", &self.descriptor())
            .finish()
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ReplicaLease {
    Memory(MemoryReplica),
}

impl ReplicaLease {
    pub const fn id(&self) -> ReplicaId {
        match self {
            Self::Memory(replica) => replica.id(),
        }
    }

    pub fn segment_id(&self) -> SegmentId {
        match self {
            Self::Memory(replica) => replica.segment_id(),
        }
    }

    pub const fn reserved_bytes(&self) -> u64 {
        match self {
            Self::Memory(replica) => replica.reserved_bytes(),
        }
    }

    pub const fn capacity_bytes(&self) -> u64 {
        match self {
            Self::Memory(replica) => replica.capacity_bytes(),
        }
    }

    pub fn memory(&self) -> Option<&MemoryReplica> {
        match self {
            Self::Memory(replica) => Some(replica),
        }
    }
}

#[derive(Debug, Default)]
pub struct ReplicaSet {
    storage: ReplicaStorage,
}

#[derive(Debug, Default)]
enum ReplicaStorage {
    #[default]
    Empty,
    One(ReplicaLease),
    Many(Box<[ReplicaLease]>),
}

#[derive(Default)]
pub(crate) struct ReplicaReclaimBatch {
    memory: Vec<Reservation>,
}

impl ReplicaSet {
    pub fn new(replicas: impl IntoIterator<Item = ReplicaLease>) -> Self {
        let mut replicas = replicas.into_iter();
        let Some(first) = replicas.next() else {
            return Self::default();
        };
        let Some(second) = replicas.next() else {
            return Self::one(first);
        };

        let (remaining, _) = replicas.size_hint();
        let mut many = Vec::with_capacity(remaining.saturating_add(2));
        many.push(first);
        many.push(second);
        many.extend(replicas);
        Self {
            storage: ReplicaStorage::Many(many.into_boxed_slice()),
        }
    }

    pub fn one(replica: ReplicaLease) -> Self {
        Self {
            storage: ReplicaStorage::One(replica),
        }
    }

    pub fn from_reservations(reservations: ReservationSet) -> Self {
        Self::new(
            reservations
                .into_iter()
                .enumerate()
                .map(|(index, reservation)| {
                    let id = u32::try_from(index + 1).expect("replica count exceeds u32::MAX");
                    ReplicaLease::Memory(MemoryReplica::new(ReplicaId::new(id), reservation))
                }),
        )
    }

    pub fn replicas(&self) -> &[ReplicaLease] {
        match &self.storage {
            ReplicaStorage::Empty => &[],
            ReplicaStorage::One(replica) => std::slice::from_ref(replica),
            ReplicaStorage::Many(replicas) => replicas,
        }
    }

    pub fn len(&self) -> usize {
        match &self.storage {
            ReplicaStorage::Empty => 0,
            ReplicaStorage::One(_) => 1,
            ReplicaStorage::Many(replicas) => replicas.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.storage, ReplicaStorage::Empty)
    }

    pub fn reserved_bytes(&self) -> u64 {
        self.replicas().iter().fold(0_u64, |total, replica| {
            total.saturating_add(replica.reserved_bytes())
        })
    }

    pub fn minimum_capacity_bytes(&self) -> Option<u64> {
        self.replicas()
            .iter()
            .map(ReplicaLease::capacity_bytes)
            .min()
    }

    pub(crate) fn append_reclaim_resources(self, batch: &mut ReplicaReclaimBatch) {
        match self.storage {
            ReplicaStorage::Empty => {}
            ReplicaStorage::One(replica) => batch.push(replica),
            ReplicaStorage::Many(replicas) => {
                for replica in replicas {
                    batch.push(replica);
                }
            }
        }
    }
}

impl ReplicaReclaimBatch {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            memory: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn extend(&mut self, replicas: ReplicaSet) {
        replicas.append_reclaim_resources(self);
    }

    fn push(&mut self, replica: ReplicaLease) {
        match replica {
            ReplicaLease::Memory(replica) => self.memory.push(replica.into_reservation()),
        }
    }

    pub(crate) fn release(self) {
        Reservation::release_batch(self.memory);
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectCommit {
    checksum: Option<u64>,
}

impl ObjectCommit {
    pub const fn new(checksum: Option<u64>) -> Self {
        Self { checksum }
    }

    pub const fn checksum(self) -> Option<u64> {
        self.checksum
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteOwner {
    client: ClientId,
}

impl WriteOwner {
    pub const fn new(client: ClientId) -> Self {
        Self { client }
    }

    pub const fn client(self) -> ClientId {
        self.client
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WriteId {
    generation: u64,
}

impl WriteId {
    pub const fn new(generation: u64) -> Self {
        Self { generation }
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct CatalogTick(u64);

impl CatalogTick {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn saturating_add(self, delta: u64) -> Self {
        Self(self.0.saturating_add(delta))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReclaimTarget {
    bytes: u64,
    reason: ReclaimReason,
}

impl ReclaimTarget {
    pub const fn new(bytes: u64, reason: ReclaimReason) -> Self {
        Self { bytes, reason }
    }

    pub const fn bytes(self) -> u64 {
        self.bytes
    }

    pub const fn reason(self) -> ReclaimReason {
        self.reason
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReclaimReason {
    CapacityPressure,
    Explicit,
    PendingTimeout,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectBudget {
    max_candidates: usize,
    max_reclaims: usize,
    max_empty_slots: usize,
}

impl CollectBudget {
    pub const fn new(max_candidates: usize, max_reclaims: usize, max_empty_slots: usize) -> Self {
        Self {
            max_candidates,
            max_reclaims,
            max_empty_slots,
        }
    }

    pub const fn max_candidates(self) -> usize {
        self.max_candidates
    }

    pub const fn max_reclaims(self) -> usize {
        self.max_reclaims
    }

    pub const fn max_empty_slots(self) -> usize {
        self.max_empty_slots
    }
}

impl Default for CollectBudget {
    fn default() -> Self {
        Self::new(64, 64, 16)
    }
}
