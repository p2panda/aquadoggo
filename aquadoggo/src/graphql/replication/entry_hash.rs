// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::*;
use p2panda_rs::hash::Hash;
use serde::{Deserialize, Serialize};

/// The p2panda_rs hash of an entry
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EntryHash(Hash);

impl From<EntryHash> for Hash {
    fn from(e: EntryHash) -> Self {
        e.0
    }
}

scalar!(EntryHash);

impl From<EntryHash> for Value {
    fn from(entry: EntryHash) -> Self {
        async_graphql::ScalarType::to_value(&entry)
    }
}
