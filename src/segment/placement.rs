use super::error::ReserveError;
use super::identity::SegmentId;
use super::pool::{PoolSnapshot, SegmentCandidate, SegmentPool};
use super::reservation::Reservation;
use super::stats::SegmentStats;
use std::collections::HashSet;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FulfillmentPolicy {
    #[default]
    AllOrNothing,
    BestEffort,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FailureDomain {
    #[default]
    Segment,
    Host,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AllocationSpec {
    bytes: u64,
}

impl AllocationSpec {
    pub const fn new(bytes: u64) -> Self {
        Self { bytes }
    }

    pub const fn bytes(self) -> u64 {
        self.bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaPolicy {
    count: usize,
    failure_domain: FailureDomain,
}

impl ReplicaPolicy {
    pub const fn new(count: usize) -> Self {
        Self {
            count,
            failure_domain: FailureDomain::Segment,
        }
    }

    pub const fn across(mut self, failure_domain: FailureDomain) -> Self {
        self.failure_domain = failure_domain;
        self
    }

    pub const fn count(self) -> usize {
        self.count
    }

    pub const fn failure_domain(self) -> FailureDomain {
        self.failure_domain
    }
}

#[derive(Clone, Debug, Default)]
pub struct PlacementConstraints {
    preferred_names: Vec<Arc<str>>,
    excluded_segments: HashSet<SegmentId>,
}

impl PlacementConstraints {
    pub fn with_preferred_names<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        self.preferred_names = names.into_iter().map(Into::into).collect();
        self
    }

    pub fn excluding<I>(mut self, segments: I) -> Self
    where
        I: IntoIterator<Item = SegmentId>,
    {
        self.excluded_segments.extend(segments);
        self
    }

    pub fn preferred_names(&self) -> &[Arc<str>] {
        &self.preferred_names
    }

    pub const fn excluded_segments(&self) -> &HashSet<SegmentId> {
        &self.excluded_segments
    }
}

#[derive(Clone, Debug)]
pub struct PlacementRequest {
    allocation: AllocationSpec,
    replicas: ReplicaPolicy,
    fulfillment: FulfillmentPolicy,
    constraints: PlacementConstraints,
}

impl PlacementRequest {
    pub fn new(allocation: AllocationSpec, replicas: ReplicaPolicy) -> Self {
        Self {
            allocation,
            replicas,
            fulfillment: FulfillmentPolicy::AllOrNothing,
            constraints: PlacementConstraints::default(),
        }
    }

    pub const fn with_fulfillment(mut self, fulfillment: FulfillmentPolicy) -> Self {
        self.fulfillment = fulfillment;
        self
    }

    pub fn constrained_by(mut self, constraints: PlacementConstraints) -> Self {
        self.constraints = constraints;
        self
    }

    pub const fn allocation(&self) -> AllocationSpec {
        self.allocation
    }

    pub const fn replicas(&self) -> ReplicaPolicy {
        self.replicas
    }

    pub const fn fulfillment(&self) -> FulfillmentPolicy {
        self.fulfillment
    }

    pub const fn constraints(&self) -> &PlacementConstraints {
        &self.constraints
    }
}

pub trait PlacementPolicy: Send + Sync {
    fn order(&self, snapshot: &PoolSnapshot, request: &PlacementRequest) -> Vec<SegmentCandidate>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FreeCapacityPolicy;

impl PlacementPolicy for FreeCapacityPolicy {
    fn order(&self, snapshot: &PoolSnapshot, request: &PlacementRequest) -> Vec<SegmentCandidate> {
        let mut candidates: Vec<_> = snapshot
            .iter()
            .filter_map(|candidate| {
                let stats = candidate.stats();
                (stats.state.is_accepting()
                    && stats.space.available_bytes >= request.allocation.bytes
                    && !request
                        .constraints
                        .excluded_segments
                        .contains(&candidate.id()))
                .then(|| RankedCandidate {
                    preference: preference_rank(request, candidate),
                    candidate: candidate.clone(),
                    stats,
                })
            })
            .collect();

        candidates.sort_unstable_by(|left, right| {
            left.preference
                .cmp(&right.preference)
                .then_with(|| {
                    let left_scaled = u128::from(left.stats.space.available_bytes)
                        * u128::from(right.stats.space.capacity_bytes);
                    let right_scaled = u128::from(right.stats.space.available_bytes)
                        * u128::from(left.stats.space.capacity_bytes);
                    right_scaled.cmp(&left_scaled)
                })
                .then_with(|| {
                    right
                        .stats
                        .space
                        .largest_free_region_bytes
                        .cmp(&left.stats.space.largest_free_region_bytes)
                })
                .then_with(|| left.candidate.id().cmp(&right.candidate.id()))
        });
        candidates
            .into_iter()
            .map(|ranked| ranked.candidate)
            .collect()
    }
}

pub struct ReplicaAllocator<P = FreeCapacityPolicy> {
    pool: Arc<SegmentPool>,
    policy: P,
}

impl ReplicaAllocator<FreeCapacityPolicy> {
    pub fn new(pool: Arc<SegmentPool>) -> Self {
        Self {
            pool,
            policy: FreeCapacityPolicy,
        }
    }
}

impl<P> ReplicaAllocator<P>
where
    P: PlacementPolicy,
{
    pub fn with_policy(pool: Arc<SegmentPool>, policy: P) -> Self {
        Self { pool, policy }
    }

    pub fn reserve(&self, request: &PlacementRequest) -> Result<ReservationSet, PlacementError> {
        if request.allocation.bytes == 0 {
            return Err(PlacementError::ZeroSize);
        }
        if request.replicas.count == 0 {
            return Err(PlacementError::ZeroReplicas);
        }

        let snapshot = self.pool.snapshot();
        let candidates = self.policy.order(&snapshot, request);
        let mut domains = HashSet::with_capacity(request.replicas.count);
        let mut reservations = Vec::with_capacity(request.replicas.count);

        for candidate in candidates {
            let domain = domain_key(&candidate, request.replicas.failure_domain);
            if domains.contains(&domain) {
                continue;
            }

            match self.pool.reserve(&candidate, request.allocation.bytes) {
                Ok(reservation) => {
                    domains.insert(domain);
                    reservations.push(reservation);
                    if reservations.len() == request.replicas.count {
                        return Ok(ReservationSet(reservations));
                    }
                }
                Err(ReserveError::NotAccepting(_) | ReserveError::OutOfSpace(_)) => {
                    // Snapshots are intentionally lock-free and may be stale.
                }
                Err(error) => return Err(error.into()),
            }
        }

        if request.fulfillment == FulfillmentPolicy::BestEffort {
            Ok(ReservationSet(reservations))
        } else {
            let allocated = reservations.len();
            drop(reservations);
            Err(PlacementError::InsufficientReplicas {
                requested: request.replicas.count,
                allocated,
            })
        }
    }
}

#[derive(Debug, Default)]
pub struct ReservationSet(Vec<Reservation>);

impl ReservationSet {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Reservation> {
        self.0.iter()
    }
}

impl IntoIterator for ReservationSet {
    type Item = Reservation;
    type IntoIter = std::vec::IntoIter<Reservation>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PlacementError {
    #[error("placement size must not be zero")]
    ZeroSize,
    #[error("replica count must not be zero")]
    ZeroReplicas,
    #[error("could allocate only {allocated} of {requested} requested replicas")]
    InsufficientReplicas { requested: usize, allocated: usize },
    #[error("failed to reserve a replica: {0}")]
    Reserve(#[from] ReserveError),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum DomainKey {
    Segment(SegmentId),
    Host(Arc<str>),
    SegmentWithoutHost(SegmentId),
}

struct RankedCandidate {
    candidate: SegmentCandidate,
    stats: SegmentStats,
    preference: usize,
}

fn domain_key(candidate: &SegmentCandidate, domain: FailureDomain) -> DomainKey {
    match domain {
        FailureDomain::Segment => DomainKey::Segment(candidate.id()),
        FailureDomain::Host => candidate.spec().topology().host_id_arc().map_or_else(
            || DomainKey::SegmentWithoutHost(candidate.id()),
            DomainKey::Host,
        ),
    }
}

fn preference_rank(request: &PlacementRequest, candidate: &SegmentCandidate) -> usize {
    request
        .constraints
        .preferred_names
        .iter()
        .position(|name| name.as_ref() == candidate.spec().identity().name())
        .unwrap_or(usize::MAX)
}
