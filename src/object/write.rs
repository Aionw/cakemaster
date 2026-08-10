use crate::segment::ClientId;

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
    pub(crate) const fn new(generation: u64) -> Self {
        Self { generation }
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }
}
