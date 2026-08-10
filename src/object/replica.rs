use crate::segment::placement::ReservationSet;
use crate::segment::{
    MemoryDescriptor, MemoryDescriptorRef, NofDescriptor, NofDescriptorRef, ReplicaClass,
    Reservation, SegmentId,
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

pub struct MemoryReplica {
    id: ReplicaId,
    reservation: Reservation,
}

impl MemoryReplica {
    pub fn new(id: ReplicaId, reservation: Reservation) -> Self {
        assert_eq!(
            reservation.replica_class(),
            ReplicaClass::Memory,
            "MemoryReplica requires a memory reservation"
        );
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
        self.reservation
            .descriptor()
            .memory()
            .expect("MemoryReplica requires a memory reservation")
    }

    pub fn owned_descriptor(&self) -> MemoryDescriptor {
        self.descriptor().to_owned()
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

pub struct NofReplica {
    id: ReplicaId,
    reservation: Reservation,
}

impl NofReplica {
    pub fn new(id: ReplicaId, reservation: Reservation) -> Self {
        assert_eq!(
            reservation.replica_class(),
            ReplicaClass::Nof,
            "NofReplica requires a NoF reservation"
        );
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

    pub fn descriptor(&self) -> NofDescriptorRef<'_> {
        self.reservation
            .descriptor()
            .nof()
            .expect("NofReplica requires a NoF reservation")
    }

    pub fn owned_descriptor(&self) -> NofDescriptor {
        self.descriptor().to_owned()
    }

    pub(crate) fn into_reservation(self) -> Reservation {
        self.reservation
    }
}

impl fmt::Debug for NofReplica {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NofReplica")
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
    Nof(NofReplica),
}

impl ReplicaLease {
    pub const fn id(&self) -> ReplicaId {
        match self {
            Self::Memory(replica) => replica.id(),
            Self::Nof(replica) => replica.id(),
        }
    }

    pub fn segment_id(&self) -> SegmentId {
        match self {
            Self::Memory(replica) => replica.segment_id(),
            Self::Nof(replica) => replica.segment_id(),
        }
    }

    pub const fn reserved_bytes(&self) -> u64 {
        match self {
            Self::Memory(replica) => replica.reserved_bytes(),
            Self::Nof(replica) => replica.reserved_bytes(),
        }
    }

    pub const fn capacity_bytes(&self) -> u64 {
        match self {
            Self::Memory(replica) => replica.capacity_bytes(),
            Self::Nof(replica) => replica.capacity_bytes(),
        }
    }

    pub fn memory(&self) -> Option<&MemoryReplica> {
        match self {
            Self::Memory(replica) => Some(replica),
            Self::Nof(_) => None,
        }
    }

    pub fn nof(&self) -> Option<&NofReplica> {
        match self {
            Self::Memory(_) => None,
            Self::Nof(replica) => Some(replica),
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
                    match reservation.replica_class() {
                        ReplicaClass::Memory => ReplicaLease::Memory(MemoryReplica::new(
                            ReplicaId::new(id),
                            reservation,
                        )),
                        ReplicaClass::Nof => {
                            ReplicaLease::Nof(NofReplica::new(ReplicaId::new(id), reservation))
                        }
                    }
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
        }
    }

    pub(crate) fn extend(&mut self, replicas: ReplicaSet) {
        replicas.append_reclaim_resources(self);
    }

    fn push(&mut self, replica: ReplicaLease) {
        match replica {
            ReplicaLease::Memory(replica) => self.direct.push(replica.into_reservation()),
            ReplicaLease::Nof(replica) => self.direct.push(replica.into_reservation()),
        }
    }

    pub(crate) fn release(self) {
        Reservation::release_batch(self.direct);
    }
}
