use crate::segment::placement::ReservationSet;
use crate::segment::{
    LocalSsdDescriptor, LocalSsdDescriptorRef, LocalSsdLease, ReplicaClass, Reservation,
    ReservationDescriptor, ReservationDescriptorRef, SegmentId,
};
use std::fmt;

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

pub struct DirectReplica {
    id: ReplicaId,
    reservation: Reservation,
}

impl DirectReplica {
    pub fn new(id: ReplicaId, reservation: Reservation) -> Self {
        assert_ne!(
            reservation.replica_class(),
            ReplicaClass::LocalSsd,
            "DirectReplica requires a direct reservation"
        );
        Self { id, reservation }
    }

    pub const fn id(&self) -> ReplicaId {
        self.id
    }

    pub fn segment_id(&self) -> SegmentId {
        self.reservation.segment_id()
    }

    pub fn replica_class(&self) -> ReplicaClass {
        self.reservation.replica_class()
    }

    pub fn is_live(&self) -> bool {
        self.reservation.is_live()
    }

    pub const fn reserved_bytes(&self) -> u64 {
        self.reservation.reserved_bytes()
    }

    pub const fn capacity_bytes(&self) -> u64 {
        self.reservation.requested_bytes()
    }

    pub fn descriptor(&self) -> ReservationDescriptorRef<'_> {
        self.reservation.descriptor()
    }

    pub fn owned_descriptor(&self) -> ReservationDescriptor {
        self.reservation.owned_descriptor()
    }

    pub(crate) fn into_reservation(self) -> Reservation {
        self.reservation
    }
}

impl fmt::Debug for DirectReplica {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectReplica")
            .field("id", &self.id)
            .field("segment_id", &self.segment_id())
            .field("replica_class", &self.replica_class())
            .field("reserved_bytes", &self.reserved_bytes())
            .field("descriptor", &self.descriptor())
            .finish()
    }
}

pub struct LocalSsdReplica {
    id: ReplicaId,
    lease: LocalSsdLease,
}

impl LocalSsdReplica {
    pub const fn new(id: ReplicaId, lease: LocalSsdLease) -> Self {
        Self { id, lease }
    }

    pub const fn id(&self) -> ReplicaId {
        self.id
    }

    pub fn segment_id(&self) -> SegmentId {
        self.lease.segment_id()
    }

    pub const fn reserved_bytes(&self) -> u64 {
        self.lease.bytes()
    }

    pub const fn capacity_bytes(&self) -> u64 {
        self.lease.bytes()
    }

    pub fn is_live(&self) -> bool {
        self.lease.is_live()
    }

    pub fn descriptor(&self) -> LocalSsdDescriptorRef<'_> {
        self.lease.descriptor()
    }

    pub fn owned_descriptor(&self) -> LocalSsdDescriptor {
        self.descriptor().to_owned()
    }

    pub(crate) fn into_lease(self) -> LocalSsdLease {
        self.lease
    }
}

impl fmt::Debug for LocalSsdReplica {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalSsdReplica")
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
    Direct(DirectReplica),
    LocalSsd(LocalSsdReplica),
}

impl ReplicaLease {
    pub const fn id(&self) -> ReplicaId {
        match self {
            Self::Direct(replica) => replica.id(),
            Self::LocalSsd(replica) => replica.id(),
        }
    }

    pub fn segment_id(&self) -> SegmentId {
        match self {
            Self::Direct(replica) => replica.segment_id(),
            Self::LocalSsd(replica) => replica.segment_id(),
        }
    }

    /// Whether the exact mounted segment incarnation backing this replica is
    /// still logically valid.
    pub fn is_live(&self) -> bool {
        match self {
            Self::Direct(replica) => replica.is_live(),
            Self::LocalSsd(replica) => replica.is_live(),
        }
    }

    pub const fn reserved_bytes(&self) -> u64 {
        match self {
            Self::Direct(replica) => replica.reserved_bytes(),
            Self::LocalSsd(replica) => replica.reserved_bytes(),
        }
    }

    pub const fn capacity_bytes(&self) -> u64 {
        match self {
            Self::Direct(replica) => replica.capacity_bytes(),
            Self::LocalSsd(replica) => replica.capacity_bytes(),
        }
    }

    pub fn direct(&self) -> Option<&DirectReplica> {
        match self {
            Self::Direct(replica) => Some(replica),
            Self::LocalSsd(_) => None,
        }
    }

    pub fn memory(&self) -> Option<&DirectReplica> {
        match self {
            Self::Direct(replica) if replica.replica_class() == ReplicaClass::Memory => {
                Some(replica)
            }
            Self::Direct(_) | Self::LocalSsd(_) => None,
        }
    }

    pub fn nof(&self) -> Option<&DirectReplica> {
        match self {
            Self::Direct(replica) if replica.replica_class() == ReplicaClass::Nof => Some(replica),
            Self::Direct(_) | Self::LocalSsd(_) => None,
        }
    }

    pub fn local_ssd(&self) -> Option<&LocalSsdReplica> {
        match self {
            Self::Direct(_) => None,
            Self::LocalSsd(replica) => Some(replica),
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
    direct: Vec<Reservation>,
    local_ssd: Vec<LocalSsdLease>,
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
                    ReplicaLease::Direct(DirectReplica::new(ReplicaId::new(id), reservation))
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

    /// Objects currently use strict replica validity: losing any associated
    /// segment invalidates the object rather than silently degrading its
    /// replication contract.
    pub(crate) fn is_live(&self) -> bool {
        !self.is_empty() && self.replicas().iter().all(ReplicaLease::is_live)
    }

    pub fn reserved_bytes(&self) -> u64 {
        self.replicas().iter().fold(0_u64, |total, replica| {
            total.saturating_add(replica.reserved_bytes())
        })
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
            direct: Vec::with_capacity(capacity),
            local_ssd: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn extend(&mut self, replicas: ReplicaSet) {
        replicas.append_reclaim_resources(self);
    }

    fn push(&mut self, replica: ReplicaLease) {
        match replica {
            ReplicaLease::Direct(replica) => self.direct.push(replica.into_reservation()),
            ReplicaLease::LocalSsd(replica) => self.local_ssd.push(replica.into_lease()),
        }
    }

    pub(crate) fn release(self) {
        Reservation::release_batch(self.direct);
        drop(self.local_ssd);
    }
}
