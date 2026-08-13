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
