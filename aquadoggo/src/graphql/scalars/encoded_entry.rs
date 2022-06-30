// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{scalar, Value};
use p2panda_rs::entry::EntrySigned;
use serde::{Deserialize, Serialize};

/// Signed bamboo entry, encoded as a hexadecimal string.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EncodedEntry(EntrySigned);

impl From<EntrySigned> for EncodedEntry {
    fn from(entry: EntrySigned) -> Self {
        Self(entry)
    }
}

impl From<EncodedEntry> for Value {
    fn from(entry: EncodedEntry) -> Self {
        async_graphql::ScalarType::to_value(&entry)
    }
}

scalar!(EncodedEntry);
