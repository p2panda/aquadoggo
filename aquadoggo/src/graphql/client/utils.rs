// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Pos, ServerError, ServerResult};
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::schema::SchemaId;

use crate::db::provider::SqlStorage;

/// Validate that the given view matches the given schema.
///
/// Panics if the view id does not exist in the store.
pub async fn validate_view_matches_schema(
    document_view_id: &DocumentViewId,
    schema_id: &SchemaId,
    store: &SqlStorage,
    pos: Option<Pos>,
) -> ServerResult<()> {
    let document_schema_id = store
        .get_schema_by_document_view(document_view_id)
        .await
        .map_err(|err| ServerError::new(err.to_string(), None))?
        .ok_or_else(|| ServerError::new("View not found".to_string(), None))?;

    match &document_schema_id == schema_id {
        true => Ok(()),
        false => Err(ServerError::new(
            format!(
                "Found <DocumentView {}> but it does not belong to expected <Schema {}>",
                document_view_id, schema_id
            ),
            pos,
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use p2panda_rs::schema::{FieldType, SchemaId};
    use p2panda_rs::test_utils::fixtures::random_key_pair;
    use rstest::rstest;

    use crate::db::stores::test_utils::{add_document, test_db, TestDatabase, TestDatabaseRunner};
    use crate::graphql::client::utils::validate_view_matches_schema;

    #[rstest]
    fn test_validate_view_matches_schema(#[from(test_db)] runner: TestDatabaseRunner) {
        runner.with_db_teardown(&|mut db: TestDatabase| async move {
            let key_pair = random_key_pair();

            let view_id = add_document(
                &mut db,
                &SchemaId::SchemaFieldDefinition(1),
                vec![
                    ("name", "test_field".into()),
                    ("type", FieldType::String.into()),
                ]
                .try_into()
                .unwrap(),
                &key_pair,
            )
            .await;

            assert!(validate_view_matches_schema(
                &view_id,
                &SchemaId::SchemaFieldDefinition(1),
                &db.store,
                None
            )
            .await
            .is_ok());

            assert!(validate_view_matches_schema(
                &view_id,
                &SchemaId::SchemaDefinition(1),
                &db.store,
                None
            )
            .await
            .is_err());
        });
    }
}
