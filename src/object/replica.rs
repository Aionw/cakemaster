use crate::segment::placement::ReservationSet;
use crate::segment::{
    ReplicaClass, Reservation, ReservationDescriptor, ReservationDescriptorRef, SegmentId,
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

pub struct ReplicaLease {
    id: ReplicaId,
    reservation: Reservation,
}

impl ReplicaLease {
    pub fn new(id: ReplicaId, reservation: Reservation) -> Self {
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

impl fmt::Debug for ReplicaLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplicaLease")
            .field("id", &self.id)
            .field("segment_id", &self.segment_id())
            .field("replica_class", &self.replica_class())
            .field("reserved_bytes", &self.reserved_bytes())
            .field("descriptor", &self.descriptor())
            .finish()
    }
}

pub type DirectReplica = ReplicaLease;

#[derive(Debug, Default)]
pub struct ReplicaSet {
    storage: ReplicaStorage,
}

#[derive(Debug)]
pub(crate) enum ReplicaPartition {
    AllLive(ReplicaSet),
    AllStale(ReplicaSet),
    Mixed { live: ReplicaSet, stale: ReplicaSet },
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

    fn from_vec(mut replicas: Vec<ReplicaLease>) -> Self {
        match replicas.len() {
            0 => Self::default(),
            1 => Self::one(
                replicas
                    .pop()
                    .expect("one-replica vectors contain one replica"),
            ),
            _ => Self {
                storage: ReplicaStorage::Many(replicas.into_boxed_slice()),
            },
        }
    }

    pub fn from_reservations(reservations: ReservationSet) -> Self {
        Self::new(
            reservations
                .into_iter()
                .enumerate()
                .map(|(index, reservation)| {
                    let id = u32::try_from(index + 1).expect("replica count exceeds u32::MAX");
                    ReplicaLease::new(ReplicaId::new(id), reservation)
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

    pub(crate) fn all_live(&self) -> bool {
        !self.is_empty() && self.replicas().iter().all(ReplicaLease::is_live)
    }

    pub(crate) fn has_live(&self) -> bool {
        self.replicas().iter().any(ReplicaLease::is_live)
    }

    pub(crate) fn live_iter(&self) -> impl Iterator<Item = &ReplicaLease> {
        self.replicas().iter().filter(|replica| replica.is_live())
    }

    pub(crate) fn partition_by_liveness(self) -> ReplicaPartition {
        let live_count = self.live_iter().count();
        if live_count == 0 {
            return ReplicaPartition::AllStale(self);
        }
        if live_count == self.len() {
            return ReplicaPartition::AllLive(self);
        }

        let mut live = Vec::with_capacity(live_count);
        let mut stale = Vec::with_capacity(self.len() - live_count);
        let mut push = |replica: ReplicaLease| {
            if replica.is_live() {
                live.push(replica);
            } else {
                stale.push(replica);
            }
        };
        match self.storage {
            ReplicaStorage::Empty => {}
            ReplicaStorage::One(replica) => push(replica),
            ReplicaStorage::Many(replicas) => {
                for replica in replicas {
                    push(replica);
                }
            }
        }
        ReplicaPartition::Mixed {
            live: Self::from_vec(live),
            stale: Self::from_vec(stale),
        }
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
        self.direct.push(replica.into_reservation());
    }

    pub(crate) fn release(self) {
        Reservation::release_batch(self.direct);
    }
}
