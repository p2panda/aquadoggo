// SPDX-License-Identifier: AGPL-3.0-or-later

//! Static fields of the client api.
use async_graphql::{Context, Object, Result};
use p2panda_rs::api;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::identity::PublicKey;

use crate::db::SqlStore;
use crate::graphql::client::NextArguments;
use crate::graphql::scalars;

/// Return required arguments for publishing the next entry.
pub async fn next_args(
    ctx: &Context<'_>,
    public_key: scalars::PublicKeyScalar,
    document_view_id: Option<scalars::DocumentViewIdScalar>,
) -> Result<NextArguments> {
    // Access the store from context.
    let store = ctx.data::<SqlStore>()?;

    // Convert and validate passed parameters.
    let public_key: PublicKey = public_key.into();
    let document_view_id = document_view_id.map(|val| DocumentViewId::from(&val));

    // Calculate next entry's arguments.
    let (backlink, skiplink, seq_num, log_id) =
        api::next_args(store, &public_key, document_view_id.as_ref()).await?;

    Ok(NextArguments {
        log_id: log_id.into(),
        seq_num: seq_num.into(),
        backlink: backlink.map(|hash| hash.into()),
        skiplink: skiplink.map(|hash| hash.into()),
    })
}
