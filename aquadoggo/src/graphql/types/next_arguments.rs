// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{dynamic::ResolverContext, Error};
use dynamic_graphql::{FieldValue, ScalarValue, SimpleObject};
use log::debug;
use p2panda_rs::api;

use crate::db::SqlStore;
use crate::graphql::scalars::{
    DocumentViewIdScalar, EntryHashScalar, LogIdScalar, PublicKeyScalar, SeqNumScalar,
};
use crate::graphql::utils::downcast_next_args_arguments;

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

impl NextArguments {
    /// Resolve `NextArgs` from a resolver context and it's contained arguments.
    pub async fn resolve<'a>(ctx: ResolverContext<'a>) -> Result<Option<FieldValue<'a>>, Error> {
        let (public_key, document_view_id) = downcast_next_args_arguments(&ctx);
        let store = ctx.data_unchecked::<SqlStore>();
        
        // Calculate next entry's arguments.
        let (backlink, skiplink, seq_num, log_id) = api::next_args(
            store,
            &public_key.into(),
            document_view_id.map(|id| id.into()).as_ref(),
        )
        .await?;

        let next_args = Self {
            log_id: log_id.into(),
            seq_num: seq_num.into(),
            backlink: backlink.map(|hash| hash.into()),
            skiplink: skiplink.map(|hash| hash.into()),
        };

        Ok(Some(FieldValue::owned_any(next_args)))
    }
}
