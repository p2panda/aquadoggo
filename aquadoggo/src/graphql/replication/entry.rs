// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::*;
use p2panda_rs::entry::EntrySigned as PandaEntry;
use serde::{Deserialize, Serialize};

/// A p2panda_rs entry encoded as a String
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Entry(pub PandaEntry);

impl AsRef<PandaEntry> for Entry {
    fn as_ref(&self) -> &PandaEntry {
        &self.0
    }
}

scalar!(Entry);

impl From<Entry> for Value {
    fn from(entry: Entry) -> Self {
        async_graphql::ScalarType::to_value(&entry)
    }
}
