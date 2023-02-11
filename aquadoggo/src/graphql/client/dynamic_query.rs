// SPDX-License-Identifier: AGPL-3.0-or-later

//! Resolver for dynamic fields of the client API.
use async_graphql::indexmap::IndexMap;
use async_graphql::{
    ContainerType, Context, Name, SelectionField, ServerError, ServerResult, Value,
};
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::future;
use log::{debug, error, info};
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{DocumentId, DocumentView, DocumentViewId};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::DocumentStore;
use p2panda_rs::Human;

use crate::db::SqlStore;
use crate::graphql::client::dynamic_types;
use crate::graphql::client::dynamic_types::DocumentMeta;
use crate::graphql::client::utils::validate_view_matches_schema;
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};
use crate::schema::SchemaProvider;

/// Resolves queries for documents based on p2panda schemas.
///
/// Implements [`ContainerType`] to be able to resolve arbitrary fields selected by a query on the
/// root GraphQL schema.
///
/// This implementation always has to match what is defined by the corresponding `OutputType` implementation for `DynamicQuery`.
#[derive(Debug, Default)]
pub struct DynamicQuery;

#[async_trait]
impl ContainerType for DynamicQuery {
    /// This resolver is called for all queries but we only want to resolve if the queried field
    /// can actually be parsed in of the two forms:
    ///
    /// - `<SchemaId>` - query a single document
    /// - `all_<SchemaId>` - query a collection of documents
    async fn resolve_field(&self, ctx: &Context<'_>) -> ServerResult<Option<Value>> {
        let field_name = ctx.field().name();

        // Optimistically parse as collection query if field name begins with `all_` (it might
        // still be a single document query if the schema name itself starts with `all_`).
        if field_name.starts_with("all_") {
            if let Ok(schema_id) = field_name.split_at(4).1.parse::<SchemaId>() {
                // Retrieve the schema to make sure that this is actually a schema and doesn't just
                // look like one.
                let schema_provider = ctx.data_unchecked::<SchemaProvider>();
                if schema_provider.get(&schema_id).await.is_some() {
                    return self.query_collection(&schema_id, ctx).await;
                }
            }
        }

        // Continue by trying to parse it as a schema and, if that's successful, checking whether
        // this schema is available in the schema provider. If both are successfull, continue by
        // resolving this query as a query for a single document.
        if let Ok(schema_id) = field_name.parse::<SchemaId>() {
            let schema_provider = ctx.data_unchecked::<SchemaProvider>();
            if schema_provider.get(&schema_id).await.is_some() {
                return self.query_single(&schema_id, ctx).await;
            }
        }

        // Return `None` to signal that other resolvers should be tried for this query.
        Ok(None)
    }
}

impl DynamicQuery {
    /// Returns a single document as a GraphQL value.
    async fn query_single(
        &self,
        schema_id: &SchemaId,
        ctx: &Context<'_>,
    ) -> ServerResult<Option<Value>> {
        info!("Handling single query for {}", schema_id.display());

        let document_id_arg = ctx.param_value::<Option<DocumentIdScalar>>(
            dynamic_types::dynamic_query_output::DOCUMENT_ID_ARGUMENT,
            None,
        )?;
        let view_id_arg = ctx.param_value::<Option<DocumentViewIdScalar>>(
            dynamic_types::dynamic_query_output::VIEW_ID_ARGUMENT,
            None,
        )?;

        // Answer queries where the `viewId` argument is given.
        if let Some(view_id_scalar) = view_id_arg.clone().1 {
            let view_id = DocumentViewId::from(&view_id_scalar);

            // Return early and ignore the `id` argument because it doesn't provide any additional
            // information that we don't get from the view id.
            return Ok(Some(
                self.get_by_document_view_id(
                    view_id,
                    ctx,
                    ctx.field().selection_set().collect(),
                    Some(schema_id),
                )
                .await?,
            ));
        }

        // Answer queries where the `id` argument is given.
        if let Some(document_id_scalar) = document_id_arg.1 {
            return Ok(Some(
                self.get_by_document_id(
                    DocumentId::from(&document_id_scalar),
                    ctx,
                    ctx.field().selection_set().collect(),
                    Some(schema_id),
                )
                .await?,
            ));
        }

        Err(ServerError::new(
            "Must provide either `id` or `viewId` argument".to_string(),
            Some(ctx.item.pos),
        ))
    }

    /// Returns all documents for the given schema as a GraphQL value.
    async fn query_collection(
        &self,
        schema_id: &SchemaId,
        ctx: &Context<'_>,
    ) -> ServerResult<Option<Value>> {
        info!("Handling collection query for {}", schema_id.display());

        let store = ctx.data_unchecked::<SqlStore>();

        // Retrieve all documents for schema from storage.
        let documents = store
            .get_documents_by_schema(schema_id)
            .await
            .map_err(|err| ServerError::new(err.to_string(), None))?;

        // Assemble views async
        let documents_graphql_values = documents.into_iter().map(|document| async move {
            let selected_fields = ctx.field().selection_set().collect();
            match document.view() {
                Some(view) => {
                    self.document_response(
                        document.id(),
                        &view,
                        document.schema_id(),
                        ctx,
                        selected_fields,
                    )
                    .await
                }
                None => Ok(Value::Null),
            }
        });

        Ok(Some(Value::List(
            future::try_join_all(documents_graphql_values).await?,
        )))
    }

    /// Fetches the latest view for the given document id from the store and returns it as a
    /// GraphQL value.
    ///
    /// Recurses into relations when those are selected in `selected_fields`.
    ///
    /// If the `validate_schema` parameter has a value, returns an error if the resolved document
    /// doesn't match this schema.
    #[async_recursion]
    async fn get_by_document_id(
        &self,
        document_id: DocumentId,
        ctx: &Context<'_>,
        selected_fields: Vec<SelectionField<'async_recursion>>,
        validate_schema: Option<&'async_recursion SchemaId>,
    ) -> ServerResult<Value> {
        debug!("Fetching {} from store", document_id.display());

        let store = ctx.data_unchecked::<SqlStore>();
        let document = store.get_document(&document_id).await.unwrap();
        match document {
            Some(document) => {
                // Validate the document's schema if the `validate_schema` argument is set.
                if let Some(expected_schema_id) = validate_schema {
                    validate_view_matches_schema(
                        document.view_id(),
                        expected_schema_id,
                        store,
                        Some(ctx.item.pos),
                    )
                    .await?;
                }

                // We can unwrap the document view here as documents returned from this store method all contain views.
                self.document_response(
                    document.id(),
                    &document.view().unwrap(),
                    document.schema_id(),
                    ctx,
                    selected_fields,
                )
                .await
            }
            None => {
                error!("No view found for document {}", document_id.as_str());
                Ok(Value::Null)
            }
        }
    }

    /// Fetches a document from the store by view id and returns it as a GraphQL value.
    ///
    /// Recurses into relations when those are selected in `selected_fields`.
    ///
    /// If the `validate_schema` parameter has a value, returns an error if the resolved document
    /// doesn't match this schema.
    #[async_recursion]
    async fn get_by_document_view_id(
        &self,
        document_view_id: DocumentViewId,
        ctx: &Context<'_>,
        selected_fields: Vec<SelectionField<'async_recursion>>,
        validate_schema: Option<&'async_recursion SchemaId>,
    ) -> ServerResult<Value> {
        debug!("Fetching {} from store", document_view_id.display());

        let store = ctx.data_unchecked::<SqlStore>();
        let document = store
            .get_document_by_view_id(&document_view_id)
            .await
            // @TODO: Not sure why it's ok to unwrap here, needs checking and comment adding.
            .unwrap();
        match document {
            Some(document) => {
                // Validate the document's schema if the `validate_schema` argument is set.
                if let Some(expected_schema_id) = validate_schema {
                    validate_view_matches_schema(
                        document.view_id(),
                        expected_schema_id,
                        store,
                        Some(ctx.item.pos),
                    )
                    .await?;
                }
                // We can unwrap the document view here as documents returned from this store method all contain views.
                self.document_response(
                    document.id(),
                    &document.view().unwrap(),
                    document.schema_id(),
                    ctx,
                    selected_fields,
                )
                .await
            }
            None => Ok(Value::Null),
        }
    }

    /// Builds a GraphQL response value for a document.
    ///
    /// This uses unstable, undocumented features of `async_graphql`.
    #[async_recursion]
    async fn document_response(
        &self,
        document_id: &DocumentId,
        document_view: &DocumentView,
        schema_id: &SchemaId,
        ctx: &Context<'_>,
        selected_fields: Vec<SelectionField<'async_recursion>>,
    ) -> ServerResult<Value> {
        let mut document_fields = IndexMap::new();

        for field in selected_fields {
            // Name with which this field appears in the response
            let response_key = Name::new(field.alias().unwrap_or_else(|| field.name()));

            match field.name() {
                "__typename" => {
                    document_fields.insert(response_key, Value::String(schema_id.to_string()));
                }
                dynamic_types::document::META_FIELD => {
                    document_fields.insert(
                        response_key,
                        DocumentMeta::resolve(field, Some(document_id), Some(document_view.id()))?,
                    );
                }
                dynamic_types::document::FIELDS_FIELD => {
                    let subselection = field.selection_set().collect();
                    document_fields.insert(
                        response_key,
                        self.document_fields_response(document_view, schema_id, ctx, subselection)
                            .await?,
                    );
                }
                _ => Err(ServerError::new(
                    format!("Field '{}' does not exist on {}", field.name(), schema_id),
                    None,
                ))?,
            }
        }

        Ok(Value::Object(document_fields))
    }

    /// Builds a GraphQL response value for a document's fields.
    ///
    /// This uses unstable, undocumented features of `async_graphql`.
    #[async_recursion]
    async fn document_fields_response(
        &self,
        document_view: &DocumentView,
        schema_id: &SchemaId,
        ctx: &Context<'_>,
        selected_fields: Vec<SelectionField<'async_recursion>>,
    ) -> ServerResult<Value> {
        let schema_provider = ctx.data_unchecked::<SchemaProvider>();
        // Unwrap because this schema id comes from the store.
        let schema = schema_provider.get(schema_id).await.unwrap();

        // Construct GraphQL value for every field of the given view that has been selected.
        let mut view_fields = IndexMap::new();
        for selected_field in selected_fields {
            // Name with which this field appears in the response
            let response_key = Name::new(
                selected_field
                    .alias()
                    .unwrap_or_else(|| selected_field.name()),
            );

            // Handle `__typename` field and continue to next selection.
            if selected_field.name() == "__typename" {
                let type_name = dynamic_types::document_fields::type_name(&schema);
                view_fields.insert(response_key, Value::String(type_name));
                continue;
            }

            // Handle document fields.
            if !schema.fields().contains_key(selected_field.name()) {
                return Err(ServerError::new(
                    format!(
                        "Field {} does not exist for schema {}",
                        selected_field.name(),
                        schema
                    ),
                    None,
                ));
            }
            // Retrieve the current field's value from the document view. Unwrap because we have
            // checked that this field exists on the schema.
            let document_view_value = document_view.get(selected_field.name()).unwrap();

            // Collect any further fields that have been selected on the current field.
            let next_selection: Vec<SelectionField<'async_recursion>> =
                selected_field.selection_set().collect();

            let value = match document_view_value.value() {
                // Recurse into single views.
                OperationValue::Relation(rel) => {
                    self.get_by_document_id(rel.document_id().clone(), ctx, next_selection, None)
                        .await?
                }
                OperationValue::PinnedRelation(rel) => {
                    self.get_by_document_view_id(rel.view_id().clone(), ctx, next_selection, None)
                        .await?
                }

                // Recurse into view lists.
                OperationValue::RelationList(rel) => {
                    let queries = rel.iter().map(|doc_id| {
                        self.get_by_document_id(
                            doc_id.to_owned(),
                            ctx,
                            next_selection.clone(),
                            None,
                        )
                    });
                    Value::List(future::try_join_all(queries).await?)
                }
                OperationValue::PinnedRelationList(rel) => {
                    let queries = rel.iter().map(|view_id| {
                        self.get_by_document_view_id(
                            view_id.to_owned(),
                            ctx,
                            next_selection.clone(),
                            None,
                        )
                    });
                    Value::List(future::try_join_all(queries).await?)
                }

                // Convert all simple fields to scalar values.
                _ => gql_scalar(document_view_value.value()),
            };
            view_fields.insert(response_key, value);
        }
        Ok(Value::Object(view_fields))
    }
}

/// Convert non-relation operation values into GraphQL values.
///
/// Panics when given a relation field value.
fn gql_scalar(operation_value: &OperationValue) -> Value {
    match operation_value {
        OperationValue::Boolean(value) => value.to_owned().into(),
        OperationValue::Integer(value) => value.to_owned().into(),
        OperationValue::Float(value) => value.to_owned().into(),
        OperationValue::String(value) => value.to_owned().into(),
        // only use for scalars
        _ => panic!("can only return scalar values"),
    }
}

#[cfg(test)]
mod test {
    use async_graphql::{value, Response, Value};
    use p2panda_rs::document::DocumentId;
    use p2panda_rs::schema::FieldType;
    use p2panda_rs::test_utils::fixtures::random_key_pair;
    use rstest::rstest;
    use serde_json::json;
    use serial_test::serial;

    use crate::test_utils::graphql_test_client;
    use crate::test_utils::{add_document, add_schema, test_db, TestDatabase, TestDatabaseRunner};

    #[rstest]
    // Note: This and more tests in this file use the underlying static schema provider which is a
    // static mutable data store, accessible across all test runner threads in parallel mode. To
    // prevent overwriting data across threads we have to run this test in serial.
    //
    // Read more: https://users.rust-lang.org/t/static-mutables-in-tests/49321
    #[serial]
    fn single_query(#[from(test_db)] runner: TestDatabaseRunner) {
        // Test single query parameter variations.

        runner.with_db_teardown(&|mut db: TestDatabase| async move {
            let key_pair = random_key_pair();

            // Add schema to node.
            let schema = add_schema(
                &mut db,
                "schema_name",
                vec![("bool", FieldType::Boolean)],
                &key_pair,
            )
            .await;

            // Publish document on node.
            let view_id =
                add_document(&mut db, schema.id(), vec![("bool", true.into())], &key_pair).await;
            let document_id =
                DocumentId::from(view_id.graph_tips().first().unwrap().as_hash().to_owned());

            // Configure and send test query.
            let client = graphql_test_client(&db).await;
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
    #[serial] // See note above on why we execute this test in series
    #[case::unknown_document_id(
        "id: \"00208f7492d6eb01360a886dac93da88982029484d8c04a0bd2ac0607101b80a6634\"",
        value!({
            "view": Value::Null
        }),
        vec![]
    )]
    #[case::unknown_view_id(
        "viewId: \"00208f7492d6eb01360a886dac93da88982029484d8c04a0bd2ac0607101b80a6634\"",
        value!({
            "view": Value::Null
        }),
        vec![]
    )]
    #[case::malformed_document_id(
        "id: \"verboten\"",
        Value::Null,
        vec!["Failed to parse \"DocumentId\": invalid hex encoding in hash string".to_string()]
    )]
    #[case::malformed_view_id(
        "viewId: \"verboten\"",
        Value::Null,
        vec!["Failed to parse \"DocumentViewId\": invalid hex encoding in hash string".to_string()]
    )]
    #[case::missing_parameters(
        "id: null",
        Value::Null,
        vec!["Must provide either `id` or `viewId` argument".to_string()]
    )]
    fn single_query_error_handling(
        #[from(test_db)] runner: TestDatabaseRunner,
        #[case] params: String,
        #[case] expected_value: Value,
        #[case] expected_errors: Vec<String>,
    ) {
        // Test single query parameter variations.

        runner.with_db_teardown(move |db: TestDatabase| async move {
            // Configure and send test query.
            let client = graphql_test_client(&db).await;
            let query = format!(
                r#"{{
                view: schema_definition_v1({params}) {{
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
            assert_eq!(response.data, expected_value, "{:#?}", response);

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
    #[serial] // See note above on why we execute this test in series
    fn collection_query(#[from(test_db)] runner: TestDatabaseRunner) {
        // Test collection query parameter variations.

        runner.with_db_teardown(&|mut db: TestDatabase| async move {
            let key_pair = random_key_pair();

            // Add schema to node.
            let schema = add_schema(
                &mut db,
                "schema_name",
                vec![("bool", FieldType::Boolean)],
                &key_pair,
            )
            .await;

            // Publish document on node.
            add_document(&mut db, schema.id(), vec![("bool", true.into())], &key_pair).await;

            // Configure and send test query.
            let client = graphql_test_client(&db).await;
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
    #[serial] // See note above on why we execute this test in series
    fn type_name(#[from(test_db)] runner: TestDatabaseRunner) {
        // Test availability of `__typename` on all objects.

        runner.with_db_teardown(&|mut db: TestDatabase| async move {
            let key_pair = random_key_pair();

            // Add schema to node.
            let schema = add_schema(
                &mut db,
                "schema_name",
                vec![("bool", FieldType::Boolean)],
                &key_pair,
            )
            .await;

            // Publish document on node.
            let view_id = add_document(
                &mut db,
                &schema.id(),
                vec![("bool", true.into())],
                &key_pair,
            )
            .await;

            // Configure and send test query.
            let client = graphql_test_client(&db).await;
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
