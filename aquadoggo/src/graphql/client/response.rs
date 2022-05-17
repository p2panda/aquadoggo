// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::SimpleObject;
use serde::Serialize;

use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::storage_provider::traits::{AsEntryArgsResponse, AsPublishEntryResponse};

use crate::db::models::entry::EntryRow;

/// Response body of `panda_getEntryArguments`.
///
/// `seq_num` and `log_id` are returned as strings to be able to represent large integers in JSON.
#[derive(Serialize, Debug, SimpleObject)]
#[serde(rename_all = "camelCase")]
pub struct EntryArgsResponse {
    pub backlink: Option<String>,
    pub skiplink: Option<String>,
    pub seq_num: String,
    pub log_id: String,
}

impl AsEntryArgsResponse for EntryArgsResponse {
    fn new(backlink: Option<Hash>, skiplink: Option<Hash>, seq_num: SeqNum, log_id: LogId) -> Self {
        EntryArgsResponse {
            backlink: backlink.map(|hash| hash.as_str().to_string()),
            skiplink: skiplink.map(|hash| hash.as_str().to_string()),
            seq_num: seq_num.as_u64().to_string(),
            log_id: log_id.as_u64().to_string(),
        }
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
