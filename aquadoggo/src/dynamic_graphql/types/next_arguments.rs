// SPDX-License-Identifier: AGPL-3.0-or-later

use dynamic_graphql::SimpleObject;

/// NextArguments type which can be used with dynamically created graphql objects. 
#[derive(SimpleObject)]
pub struct NextArguments {
    /// Log id of the entry.
    #[graphql(name = "logId")]
    pub log_id: String,

    /// Sequence number of the entry.
    #[graphql(name = "seqNum")]
    pub seq_num: String,

    /// Hash of the entry backlink.
    pub backlink: Option<String>,

    /// Hash of the entry skiplink.
    pub skiplink: Option<String>,
}