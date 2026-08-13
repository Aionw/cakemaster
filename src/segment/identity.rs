use std::fmt;
use std::sync::Arc;

macro_rules! uuid_pair_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name {
            high: u64,
            low: u64,
        }

        impl $name {
            pub const NIL: Self = Self { high: 0, low: 0 };

            pub const fn new(high: u64, low: u64) -> Self {
                Self { high, low }
            }

            pub const fn high(self) -> u64 {
                self.high
            }

            pub const fn low(self) -> u64 {
                self.low
            }

            pub const fn is_nil(self) -> bool {
                self.high == 0 && self.low == 0
            }
        }

        impl From<u128> for $name {
            fn from(value: u128) -> Self {
                Self {
                    high: (value >> 64) as u64,
                    low: value as u64,
                }
            }
        }

        impl From<$name> for u128 {
            fn from(value: $name) -> Self {
                (u128::from(value.high) << 64) | u128::from(value.low)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{:016x}{:016x}", self.high, self.low)
            }
        }
    };
}

uuid_pair_id!(SegmentId);
uuid_pair_id!(ClientId);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentIdentity {
    id: SegmentId,
    owner: ClientId,
    name: Arc<str>,
    host_id: Arc<str>,
}

impl SegmentIdentity {
    pub fn new(id: SegmentId, owner: ClientId, name: impl Into<Arc<str>>) -> Self {
        Self {
            id,
            owner,
            name: name.into(),
            host_id: Arc::from(""),
        }
    }

    pub fn with_host_id(mut self, host_id: impl Into<Arc<str>>) -> Self {
        self.host_id = host_id.into();
        self
    }

    pub const fn id(&self) -> SegmentId {
        self.id
    }

    pub const fn owner(&self) -> ClientId {
        self.owner
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn host_id(&self) -> &str {
        &self.host_id
    }
}
