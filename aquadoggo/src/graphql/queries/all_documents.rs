// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, Object, ResolverContext, TypeRef};
use async_graphql::Error;
use dynamic_graphql::FieldValue;
use log::debug;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::DocumentStore;
use p2panda_rs::schema::Schema;
use p2panda_rs::document::traits::AsDocument;

use crate::db::SqlStore;
use crate::graphql::constants;
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};

/// Adds GraphQL query for getting all documents of a certain p2panda schema to the root query
/// object.
///
/// The query follows the format `all_<SCHEMA_ID>`.
pub fn build_all_documents_query(query: Object, schema: &Schema) -> Object {
    let schema_id = schema.id().clone();
    query.field(
        Field::new(
            format!("{}{}", constants::QUERY_ALL_PREFIX, schema_id),
            TypeRef::named_list(schema_id.to_string()),
            move |ctx| {
                let schema_id = schema_id.clone();
                FieldFuture::new(async move {
                    debug!(
                        "Query to {}{} received",
                        constants::QUERY_ALL_PREFIX,
                        schema_id
                    );

                    // Resolve documents.
                    let documents = resolve_documents(&ctx, &schema_id).await?;

                    // Pass the list up to the children query fields.
                    Ok(Some(FieldValue::list(documents)))
                })
            },
        )
        .description(format!("Get all {} documents.", schema.name())),
    )
}

async fn resolve_documents<'a>(ctx: &ResolverContext<'a>, schema_id: &SchemaId) -> Result<Vec<FieldValue<'a>>, Error> {
    // Access the store.
    let store = ctx.data_unchecked::<SqlStore>();

    // Fetch all documents of the schema this endpoint serves and compose the
    // field value (a list) which will bubble up the query tree.
    let documents: Vec<FieldValue> = store
        .get_documents_by_schema(&schema_id)
        .await?
        .iter()
        .map(|document| {
            FieldValue::owned_any((
                Some(DocumentIdScalar::from(document.id())),
                None::<DocumentViewIdScalar>,
            ))
        })
        .collect();

    Ok(documents)
}

#[cfg(test)]
mod test {
    use async_graphql::{value, Response};
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::schema::FieldType;
    use p2panda_rs::test_utils::fixtures::random_key_pair;
    use rstest::rstest;
    use serde_json::json;

    use crate::test_utils::{add_document, add_schema, graphql_test_client, test_runner, TestNode};

    #[rstest]
    fn collection_query(#[from(random_key_pair)] key_pair: KeyPair) {
        // Test collection query parameter variations.
        test_runner(move |mut node: TestNode| async move {
            // Add schema to node.
            let schema = add_schema(
                &mut node,
                "schema_name",
                vec![("bool", FieldType::Boolean)],
                &key_pair,
            )
            .await;

            // Publish document on node.
            add_document(
                &mut node,
                schema.id(),
                vec![("bool", true.into())],
                &key_pair,
            )
            .await;

            // Configure and send test query.
            let client = graphql_test_client(&node).await;
            let query = format!(
                r#"{{
                collection: all_{type_name} {{
                    fields {{ bool }}
                }},
            }}"#,
                type_name = schema.id(),
            );

            let response = client
                .post("/graphql")
                .json(&json!({
                    "query": query,
                }))
                .send()
                .await;

            let response: Response = response.json().await;

            let expected_data = value!({
                "collection": value!([{ "fields": { "bool": true, } }]),
            });
            assert_eq!(response.data, expected_data, "{:#?}", response.errors);
        });
    }
}