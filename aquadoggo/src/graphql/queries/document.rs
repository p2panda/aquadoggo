// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, InputValue, Object, ResolverContext, TypeRef};
use async_graphql::Error;
use dynamic_graphql::{FieldValue, ScalarValue};
use log::debug;
use p2panda_rs::schema::Schema;

use crate::graphql::constants;
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};

/// Adds GraphQL query for getting a single p2panda document, selected by its document id or
/// document view id to the root query object.
///
/// The query follows the format `<SCHEMA_ID>`.
pub fn build_document_query(query: Object, schema: &Schema) -> Object {
    let schema_id = schema.id().clone();
    query.field(
        Field::new(
            schema_id.to_string(),
            TypeRef::named(schema_id.to_string()),
            move |ctx| {
                FieldFuture::new(async move {
                    // Validate the received arguments.
                    let args = validate_args(&ctx)?;

                    // Pass them up to the children query fields.
                    Ok(Some(FieldValue::owned_any(args)))
                })
            },
        )
        .argument(InputValue::new(
            constants::DOCUMENT_ID_ARG,
            TypeRef::named(constants::DOCUMENT_ID),
        ))
        .argument(InputValue::new(
            constants::DOCUMENT_VIEW_ID_ARG,
            TypeRef::named(constants::DOCUMENT_VIEW_ID),
        ))
        .description(format!(
            "Query a {} document by id or view id.",
            schema.name()
        )),
    )
}

fn validate_args<'a>(
    ctx: &ResolverContext<'a>,
) -> Result<(Option<DocumentIdScalar>, Option<DocumentViewIdScalar>), Error> {
    // Parse arguments
    let schema_id = ctx.field().name();
    let mut document_id = None;
    let mut document_view_id = None;
    for (name, id) in ctx.field().arguments()?.into_iter() {
        match name.as_str() {
            constants::DOCUMENT_ID_ARG => {
                document_id = Some(DocumentIdScalar::from_value(id)?);
            }
            constants::DOCUMENT_VIEW_ID_ARG => {
                document_view_id = Some(DocumentViewIdScalar::from_value(id)?)
            }
            _ => (),
        }
    }

    // Check a valid combination of arguments was passed
    match (&document_id, &document_view_id) {
        (None, None) => return Err(Error::new("Must provide either `id` or `viewId` argument")),
        (Some(_), Some(_)) => {
            return Err(Error::new("Must only provide `id` or `viewId` argument"))
        }
        (Some(id), None) => {
            debug!("Query to {} received for document {}", schema_id, id);
        }
        (None, Some(id)) => {
            debug!(
                "Query to {} received for document at view id {}",
                schema_id, id
            );
        }
    };

    Ok((document_id, document_view_id))
}

#[cfg(test)]
mod test {
    use async_graphql::{value, Response, Value};
    use p2panda_rs::document::traits::AsDocument;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::schema::FieldType;
    use p2panda_rs::storage_provider::traits::DocumentStore;
    use p2panda_rs::test_utils::fixtures::random_key_pair;
    use rstest::rstest;
    use serde_json::json;

    use crate::test_utils::{add_document, add_schema, graphql_test_client, test_runner, TestNode};

    #[rstest]
    fn single_query(#[from(random_key_pair)] key_pair: KeyPair) {
        // Test single query parameter variations.

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
            let view_id = add_document(
                &mut node,
                schema.id(),
                vec![("bool", true.into())],
                &key_pair,
            )
            .await;

            // Get the materialised document.
            let document = node
                .context
                .store
                .get_document_by_view_id(&view_id)
                .await
                .expect("Query succeeds")
                .expect("There to be a document");

            let document_id = document.id();

            // Configure and send test query.
            let client = graphql_test_client(&node).await;
            let query = format!(
                r#"{{
                byViewId: {type_name}(viewId: "{view_id}") {{
                    fields {{ bool }}
                }},
                byDocumentId: {type_name}(id: "{document_id}") {{
                    fields {{ bool }}
                }}
            }}"#,
                type_name = schema.id().to_string(),
                view_id = view_id,
                document_id = document_id.as_str()
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
                "byViewId": value!({ "fields": { "bool": true, } }),
                "byDocumentId": value!({ "fields": { "bool": true, } }),
            });
            assert_eq!(response.data, expected_data, "{:#?}", response.errors);
        });
    }

    #[rstest]
    #[case::unknown_document_id(
        "(id: \"00208f7492d6eb01360a886dac93da88982029484d8c04a0bd2ac0607101b80a6634\")",
        value!({
            "view": {
                "fields": {
                    "name": Value::Null
                }
            }
        }),
        vec![]
    )]
    #[case::unknown_view_id(
        "(viewId: \"00208f7492d6eb01360a886dac93da88982029484d8c04a0bd2ac0607101b80a6634\")",
        value!({
            "view": {
                "fields": {
                    "name": Value::Null
                }
            }
        }),
        vec![]
    )]
    #[case::malformed_document_id(
        "(id: \"verboten\")",
        Value::Null,
        vec!["invalid hex encoding in hash string".to_string()]
    )]
    #[case::malformed_view_id(
        "(viewId: \"verboten\")",
        Value::Null,
        vec!["invalid hex encoding in hash string".to_string()]
    )]
    #[case::missing_parameters(
        "",
        Value::Null,
        vec!["Must provide either `id` or `viewId` argument".to_string()]
    )]
    #[case::unknown_view_id(
        "(id: \"00208f7492d6eb01360a886dac93da88982029484d8c04a0bd2ac0607101b80a6634\" viewId: \"00208f7492d6eb01360a886dac93da88982029484d8c04a0bd2ac0607101b80a6634\")",
        Value::Null,
        vec!["Must only provide `id` or `viewId` argument".to_string()]
    )]
    fn single_query_error_handling(
        #[case] params: String,
        #[case] expected_value: Value,
        #[case] expected_errors: Vec<String>,
    ) {
        // Test single query parameter variations.
        test_runner(move |node: TestNode| async move {
            // Configure and send test query.
            let client = graphql_test_client(&node).await;
            let query = format!(
                r#"{{
                view: schema_definition_v1{params} {{
                    fields {{ name }}
                }}
            }}"#,
                params = params
            );

            let response = client
                .post("/graphql")
                .json(&json!({
                    "query": query,
                }))
                .send()
                .await;

            let response: Response = response.json().await;

            // Assert response data.
            assert_eq!(response.data, expected_value, "{:#?}", response.data);

            // Assert error messages.
            let err_msgs: Vec<String> = response
                .errors
                .iter()
                .map(|err| err.message.to_string())
                .collect();
            assert_eq!(err_msgs, expected_errors);
        });
    }

    #[rstest]
    fn type_name(#[from(random_key_pair)] key_pair: KeyPair) {
        // Test availability of `__typename` on all objects.
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
            let view_id = add_document(
                &mut node,
                &schema.id(),
                vec![("bool", true.into())],
                &key_pair,
            )
            .await;

            // Configure and send test query.
            let client = graphql_test_client(&node).await;
            let query = format!(
                r#"{{
                single: {type_name}(id: "{view_id}") {{
                    __typename,
                    meta {{ __typename }}
                    fields {{ __typename }}
                }},
                collection: all_{type_name} {{
                    __typename,
                }},
            }}"#,
                type_name = schema.id(),
                view_id = view_id,
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
                "single": {
                    "__typename": schema.id(),
                    "meta": { "__typename": "DocumentMeta" },
                    "fields": { "__typename": format!("{}Fields", schema.id()), }
                },
                "collection": [{
                    "__typename": schema.id()
                }]
            });
            assert_eq!(response.data, expected_data, "{:#?}", response.errors);
        });
    }
}
