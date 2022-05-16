// SPDX-License-Identifier: AGPL-3.0-or-later

use std::str::FromStr;

use async_graphql::{ComplexObject, Context, Object, OutputType, SimpleObject};
use p2panda_rs::hash::Hash;

#[derive(Default, Debug, Copy, Clone)]
/// The root graphql object for ping
pub struct ClientRoot;

#[derive(SimpleObject)]
pub struct EntryArgs {
    pub log_id: String,
    pub seq_num: String,
    pub backlink: Option<String>,
    pub skiplink: Option<String>,
}

#[Object]
impl ClientRoot {
    // Return required arguments for publishing the next entry.
    async fn next_entry_args(
        &self,
        _context: &Context<'_>,
        #[graphql(desc = "Public key that will publish using the returned entry arguments")]
        public_key: String,
    ) -> EntryArgs {
        return EntryArgs {
            log_id: "5".to_string(),
            seq_num: "10".to_string(),
            backlink: Some(Hash::new_from_bytes(vec![1, 2, 3]).unwrap().as_str().into()),
            skiplink: None,
        };
    }
}
