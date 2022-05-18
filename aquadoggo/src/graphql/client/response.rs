// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::Object;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};

use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::storage_provider::traits::{AsEntryArgsResponse, AsPublishEntryResponse};

use crate::db::models::EntryRow;

use super::utils::U64StringVisitor;

/// Response body of `panda_getEntryArguments`.
///
/// `seq_num` and `log_id` are returned as strings to be able to represent large integers in JSON.
#[derive(Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EntryArgsResponse {
    #[serde(deserialize_with = "deserialize_log_id_string")]
    pub log_id: LogId,

    #[serde(deserialize_with = "deserialize_seq_num_string")]
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

fn deserialize_log_id_string<'de, D>(deserializer: D) -> Result<LogId, D::Error>
where
    D: Deserializer<'de>,
{
    let u64_val = deserializer.deserialize_u64(U64StringVisitor)?;
    Ok(LogId::new(u64_val))
}

fn deserialize_seq_num_string<'de, D>(deserializer: D) -> Result<SeqNum, D::Error>
where
    D: Deserializer<'de>,
{
    let u64_val = deserializer.deserialize_u64(U64StringVisitor)?;
    SeqNum::new(u64_val).map_err(D::Error::custom)
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
