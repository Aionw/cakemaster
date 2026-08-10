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
