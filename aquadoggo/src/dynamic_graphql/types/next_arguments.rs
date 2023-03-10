// SPDX-License-Identifier: AGPL-3.0-or-later

use dynamic_graphql::SimpleObject;

use crate::dynamic_graphql::scalars::{EntryHashScalar, LogIdScalar, SeqNumScalar};

/// Values used to in the construction of p2panda entries and operations.
#[derive(SimpleObject)]
pub struct NextArguments {
    /// Log id of the entry.
    #[graphql(name = "logId")]
    pub log_id: LogIdScalar,

    /// Sequence number of the entry.
    #[graphql(name = "seqNum")]
    pub seq_num: SeqNumScalar,

    /// Hash of the entry backlink.
    pub backlink: Option<EntryHashScalar>,

    /// Hash of the entry skiplink.
    pub skiplink: Option<EntryHashScalar>,
}
