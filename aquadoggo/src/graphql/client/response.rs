// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{scalar, Object};
use p2panda_rs::document::DocumentView;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::storage_provider::traits::{AsEntryArgsResponse, AsPublishEntryResponse};

use crate::db::models::EntryRow;

/// Response body of `panda_getEntryArguments`.
///
/// `seq_num` and `log_id` are returned as strings to be able to represent large integers in JSON.
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EntryArgsResponse {
    #[serde(with = "super::u64_string::log_id_string_serialisation")]
    pub log_id: LogId,

    #[serde(with = "super::u64_string::seq_num_string_serialisation")]
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

/// Response body of `panda_publishEntry`.
///
/// `seq_num` and `log_id` are returned as strings to be able to represent large integers in JSON.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PublishEntryResponse {
    #[serde(with = "super::u64_string::log_id_string_serialisation")]
    pub log_id: LogId,

    #[serde(with = "super::u64_string::seq_num_string_serialisation")]
    pub seq_num: SeqNum,

    pub backlink: Option<Hash>,

    pub skiplink: Option<Hash>,
}

#[Object]
impl PublishEntryResponse {
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

impl AsPublishEntryResponse for PublishEntryResponse {
    fn new(
        entry_hash_backlink: Option<Hash>,
        entry_hash_skiplink: Option<Hash>,
        seq_num: SeqNum,
        log_id: LogId,
    ) -> Self {
        PublishEntryResponse {
            backlink: entry_hash_backlink,
            skiplink: entry_hash_skiplink,
            seq_num,
            log_id,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct DocumentResponse {
    pub view: Option<DocumentView>,
}

#[derive(Serialize, Deserialize)]
struct DocumentField(String, String);
scalar!(DocumentField);

#[Object]
impl DocumentResponse {
    async fn view(&self) -> Vec<DocumentField> {
        match self.view.clone() {
            Some(view) => view
                .iter()
                .map(|(field, value)| {
                    let value = format!("{:?}", value.value());
                    DocumentField(field.to_owned(), value)
                })
                .collect(),
            None => vec![],
        }
    }
}
