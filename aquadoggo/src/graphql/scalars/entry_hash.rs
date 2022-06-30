// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{scalar, Value};
use p2panda_rs::hash::Hash;
use serde::{Deserialize, Serialize};

/// Hash of a signed bamboo entry.
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub struct EntryHash(Hash);

impl From<EntryHash> for Hash {
    fn from(hash: EntryHash) -> Self {
        hash.0
    }
}

impl From<Hash> for EntryHash {
    fn from(hash: Hash) -> Self {
        Self(hash)
    }
}

impl From<EntryHash> for Value {
    fn from(entry: EntryHash) -> Self {
        async_graphql::ScalarType::to_value(&entry)
    }
}

scalar!(EntryHash);

#[cfg(test)]
mod tests {
    use p2panda_rs::document::DocumentViewId;

    use super::EntryHash;

    impl From<EntryHash> for DocumentViewId {
        fn from(hash: EntryHash) -> Self {
            hash.0.into()
        }
    }
}
