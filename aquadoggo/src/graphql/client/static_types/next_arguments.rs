// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::SimpleObject;
use serde::{Deserialize, Serialize};

use crate::graphql::scalars;

/// Arguments required to sign and encode the next entry for an author.
#[derive(SimpleObject, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct NextArguments {
    /// Log id of the entry.
    #[graphql(name = "logId")]
    pub log_id: scalars::LogIdScalar,

    /// Sequence number of the entry.
    #[graphql(name = "seqNum")]
    pub seq_num: scalars::SeqNumScalar,

    /// Hash of the entry backlink.
    pub backlink: Option<scalars::EntryHash>,

    /// Hash of the entry skiplink.
    pub skiplink: Option<scalars::EntryHash>,
}
