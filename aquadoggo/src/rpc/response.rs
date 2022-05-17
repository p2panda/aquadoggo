// SPDX-License-Identifier: AGPL-3.0-or-later

use serde::Serialize;

use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::storage_provider::traits::{AsEntryArgsResponse, AsPublishEntryResponse};

use crate::db::models::EntryRow;

/// Response body of `panda_getEntryArguments`.
///
/// `seq_num` and `log_id` are returned as strings to be able to represent large integers in JSON.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EntryArgsResponse {
    pub entry_hash_backlink: Option<Hash>,
    pub entry_hash_skiplink: Option<Hash>,
    pub seq_num: SeqNum,
    pub log_id: LogId,
}

impl AsEntryArgsResponse for EntryArgsResponse {
    fn new(
        entry_hash_backlink: Option<Hash>,
        entry_hash_skiplink: Option<Hash>,
        seq_num: SeqNum,
        log_id: LogId,
    ) -> Self {
        EntryArgsResponse {
            entry_hash_backlink,
            entry_hash_skiplink,
            seq_num,
            log_id,
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
