// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, ServerError, ServerResult};
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::schema::SchemaId;

use crate::db::provider::SqlStorage;

/// Validate that the given view matches the given schema.
///
/// Panics if the view id does not exist in the store.
pub async fn validate_view_matches_schema(
    document_view_id: &DocumentViewId,
    schema_id: &SchemaId,
    ctx: &Context<'_>,
) -> ServerResult<()> {
    let store = ctx.data_unchecked::<SqlStorage>();

    let document_schema_id = store
        .get_schema_by_document_view(document_view_id)
        .await
        .map_err(|err| ServerError::new(err.to_string(), None))?
        .unwrap();

    match &document_schema_id == schema_id {
        true => Ok(()),
        false => Err(ServerError::new(
            format!(
                "The document of view id {} does not belong to the schema {}",
                document_view_id, schema_id
            ),
            Some(ctx.item.pos),
        )),
    }
}
