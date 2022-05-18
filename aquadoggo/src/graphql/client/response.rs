// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::Object;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::storage_provider::traits::{AsEntryArgsResponse, AsPublishEntryResponse};

use crate::db::models::EntryRow;

/// Response body of `panda_getEntryArguments`.
///
/// `seq_num` and `log_id` are returned as strings to be able to represent large integers in JSON.
#[derive(Debug, Eq, PartialEq)]
pub struct EntryArgsResponse {
    pub log_id: LogId,
    pub seq_num: SeqNum,
    pub backlink: Option<Hash>,
    pub skiplink: Option<Hash>,
}

#[Object]
impl EntryArgsResponse {
    #[graphql(name = "logId")]
    async fn log_id(&self) -> String {
        self.log_id.clone().as_u64().to_string()
    }

    #[graphql(name = "seqNum")]
    async fn seq_num(&self) -> String {
        self.seq_num.clone().as_u64().to_string()
    }

    async fn backlink(&self) -> Option<String> {
        self.backlink.clone().map(|hash| hash.as_str().to_string())
    }

    async fn skiplink(&self) -> Option<String> {
        self.skiplink.clone().map(|hash| hash.as_str().to_string())
    }
}

impl AsEntryArgsResponse for EntryArgsResponse {
    fn new(backlink: Option<Hash>, skiplink: Option<Hash>, seq_num: SeqNum, log_id: LogId) -> Self {
        EntryArgsResponse {
            log_id,
            seq_num,
            backlink,
            skiplink,
        }
    }
}

impl Serialize for EntryArgsResponse {
    /// Serialise using camel case  and converting `u64` values to `String`.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("EntryArgsResponse", 4)?;
        state.serialize_field("logId", &self.log_id.as_u64().to_string())?;
        state.serialize_field("seqNum", &self.seq_num.as_u64().to_string())?;
        state.serialize_field(
            "backlink",
            &self.backlink.clone().map(|hash| hash.as_str().to_string()),
        )?;
        state.serialize_field(
            "skiplink",
            &self.skiplink.clone().map(|hash| hash.as_str().to_string()),
        )?;
        state.end()
    }
}

/// Response body of `panda_publishEntry`.
///
/// `seq_num` and `log_id` are returned as strings to be able to represent large integers in JSON.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishEntryResponse {
    pub entry_hash_backlink: Option<Hash>,
    pub entry_hash_skiplink: Option<Hash>,
    pub seq_num: SeqNum,
    pub log_id: LogId,
}

impl AsPublishEntryResponse for PublishEntryResponse {
    fn new(
        entry_hash_backlink: Option<Hash>,
        entry_hash_skiplink: Option<Hash>,
        seq_num: SeqNum,
        log_id: LogId,
    ) -> Self {
        PublishEntryResponse {
            entry_hash_backlink,
            entry_hash_skiplink,
            seq_num,
            log_id,
        }
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryEntriesResponse {
    pub entries: Vec<EntryRow>,
}
