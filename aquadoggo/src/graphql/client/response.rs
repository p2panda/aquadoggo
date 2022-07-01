// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::SimpleObject;
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::storage_provider::traits::{AsEntryArgsResponse, AsPublishEntryResponse};
use serde::{Deserialize, Serialize};

use crate::graphql::scalars;

/// Arguments required to sign and encode the next entry for an author.
#[derive(SimpleObject, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct NextEntryArguments {
    /// Log id of the entry.
    #[graphql(name = "logId")]
    pub log_id: scalars::LogId,

    /// Sequence number of the entry.
    #[graphql(name = "seqNum")]
    pub seq_num: scalars::SeqNum,

    /// Hash of the entry backlink.
    pub backlink: Option<scalars::EntryHash>,

    /// Hash of the entry skiplink.
    pub skiplink: Option<scalars::EntryHash>,
}

impl AsEntryArgsResponse for NextEntryArguments {
    fn new(backlink: Option<Hash>, skiplink: Option<Hash>, seq_num: SeqNum, log_id: LogId) -> Self {
        Self {
            log_id: log_id.into(),
            seq_num: seq_num.into(),
            backlink: backlink.map(scalars::EntryHash::from),
            skiplink: skiplink.map(scalars::EntryHash::from),
        }
    }
}

impl AsPublishEntryResponse for NextEntryArguments {
    fn new(backlink: Option<Hash>, skiplink: Option<Hash>, seq_num: SeqNum, log_id: LogId) -> Self {
        Self {
            log_id: log_id.into(),
            seq_num: seq_num.into(),
            backlink: backlink.map(scalars::EntryHash::from),
            skiplink: skiplink.map(scalars::EntryHash::from),
        }
    }
}
