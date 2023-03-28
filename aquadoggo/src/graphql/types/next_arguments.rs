// SPDX-License-Identifier: AGPL-3.0-or-later

//! Return type for `next_args` and `publish` queries.

use dynamic_graphql::SimpleObject;

use crate::graphql::scalars::{EntryHashScalar, LogIdScalar, SeqNumScalar};

/// Arguments required to sign and encode the next entry for a public_key.
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
