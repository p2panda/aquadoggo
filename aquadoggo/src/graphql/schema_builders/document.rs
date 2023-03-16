// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, InputValue, Object, TypeRef};
use dynamic_graphql::{Error, FieldValue, ScalarValue};
use log::debug;
use p2panda_rs::storage_provider::traits::DocumentStore;
use p2panda_rs::{document::traits::AsDocument, schema::Schema};

use crate::db::SqlStore;
use crate::graphql::constants;
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};
use crate::graphql::types::DocumentMeta;
use crate::graphql::utils::{downcast_id_params, fields_name, get_document_from_params};

/// Build a GraphQL object type for a p2panda schema.
///
/// Contains resolvers for both `fields` and `meta`. The former simply passes up the query
/// arguments to it's children query fields. The latter retrieves the document being queried and
/// already constructs and returns the `DocumentMeta` object.
pub fn build_document_schema(schema: &Schema) -> Object {
    let document_fields_name = fields_name(schema.id());
    Object::new(schema.id().to_string())
        // The `fields` field of a document, passes up the query arguments to it's children.
        .field(Field::new(
            constants::FIELDS_FIELD,
            TypeRef::named(document_fields_name),
            move |ctx| {
                FieldFuture::new(async move {
                    // Here we just pass up the root query parameters to be used in the fields resolver
                    let params = downcast_id_params(&ctx);
                    Ok(Some(FieldValue::owned_any(params)))
                })
            },
        ))
        // The `meta` field of a document, resolves the `DocumentMeta` object.
        .field(Field::new(
            constants::META_FIELD,
            TypeRef::named(constants::DOCUMENT_META),
            move |ctx| {
                FieldFuture::new(async move {
                    let store = ctx.data_unchecked::<SqlStore>();

                    // Downcast the parameters passed up from the parent query field
                    let (document_id, document_view_id) = downcast_id_params(&ctx);
                    // Get the whole document
                    let document =
                        get_document_from_params(store, &document_id, &document_view_id).await?;

                    // Construct `DocumentMeta` and return it. We defined the document meta
                    // type and already registered it in the schema. It's derived resolvers
                    // will handle field selection.
                    let field_value = match document {
                        Some(document) => {
                            let document_meta = DocumentMeta {
                                document_id: document.id().into(),
                                view_id: document.view_id().into(),
                            };
                            Some(FieldValue::owned_any(document_meta))
                        }
                        None => Some(FieldValue::NULL),
                    };

                    Ok(field_value)
                })
            },
        ))
        .description(schema.description().to_string())
}

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
                let schema_id = schema_id.clone();
                FieldFuture::new(async move {
                    // Parse arguments
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
                        (None, None) => {
                            return Err(Error::new("Must provide either `id` or `viewId` argument"))
                        }
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
                    // Pass them up to the children query fields
                    Ok(Some(FieldValue::owned_any((document_id, document_view_id))))
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

/// Adds GraphQL query for getting all documents of a certain p2panda schema to the root query
/// object.
///
/// The query follows the format `all_<SCHEMA_ID>`.
pub fn build_all_document_query(query: Object, schema: &Schema) -> Object {
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

                    // Pass the list up to the children query fields.
                    Ok(Some(FieldValue::list(documents)))
                })
            },
        )
        .description(format!("Get all {} documents.", schema.name())),
    )
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
