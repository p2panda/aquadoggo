// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;

use async_graphql::indexmap::IndexMap;

use async_graphql::{
    ContainerType, Context, Name, SelectionField, ServerError, ServerResult, Value,
};
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::future;
use log::{debug, error, info};
use p2panda_rs::document::{DocumentId, DocumentView, DocumentViewId, DocumentViewIdError};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::SchemaId;

use crate::db::provider::SqlStorage;
use crate::db::traits::DocumentStore;
use crate::graphql::client::dynamic_types::DocumentMetaType;
use crate::graphql::scalars::{
    DocumentId as DocumentIdScalar, DocumentViewId as DocumentViewIdScalar,
};
use crate::schema::SchemaProvider;

use crate::graphql::client::utils::validate_view_matches_schema;

/// Resolves queries for documents based on p2panda schemas.
///
/// Implements [`ContainerType`] to be able to resolve arbitrary fields selected by a query on the
/// root GraphQL schema.
///
/// This implementation always has to match what is defined by the
/// [`dynamic_output_type`][`crate::graphql::client::dynamic_types::dynamic_output_type`] module.
#[derive(Debug, Default)]
pub struct DynamicQuery;

#[async_trait]
impl ContainerType for DynamicQuery {
    /// This resolver is called for all queries but we only want to resolve if the queried field
    /// can actually be parsed in of the two forms:
    ///
    /// - `<SchemaId>` - query a single document
    /// - `all_<SchemaId>` - query a listing of documents
    async fn resolve_field(&self, ctx: &Context<'_>) -> ServerResult<Option<Value>> {
        let field_name = ctx.field().name();

        // Optimistically parse as listing query if field name begins with `all_` (it might still
        // be a single document query if the schema name itself starts with `all_`).
        if field_name.starts_with("all_") {
            if let Ok(schema_id) = field_name.split_at(4).1.parse::<SchemaId>() {
                // Retrieve the schema to make sure that this is actually a schema and doesn't just
                // look like one.
                let schema_provider = ctx.data_unchecked::<SchemaProvider>();
                if schema_provider.get(&schema_id).await.is_some() {
                    return self.query_listing(&schema_id, ctx).await;
                }
            }
        }

        // We now know that this is not a listing query. Continue by treating it as a single
        // document query. Return `Ok(None)` if that doesn't work to signal that other resolvers
        // should be tried for this field.
        match field_name.parse::<SchemaId>() {
            Ok(schema) => self.query_single(&schema, ctx).await,
            Err(_) => Ok(None),
        }
    }
}

impl DynamicQuery {
    /// Returns a single document as a GraphQL value.
    async fn query_single(
        &self,
        schema_id: &SchemaId,
        ctx: &Context<'_>,
    ) -> ServerResult<Option<Value>> {
        info!("Handling single query for {}", schema_id);

        let document_id_arg = ctx.param_value::<Option<DocumentIdScalar>>("id", None)?;
        let view_id_arg = ctx.param_value::<Option<DocumentViewIdScalar>>("viewId", None)?;

        if let Some(view_id_scalar) = view_id_arg.clone().1 {
            let view_id = view_id_scalar
                .try_into()
                .map_err(|err: DocumentViewIdError| {
                    ServerError::new(
                        format!("Could not parse `viewId` argument: {}", err),
                        Some(view_id_arg.0),
                    )
                })?;

            // If the `viewId` argument is set we ignore the `id` argument because it doesn't
            // provide any additional information that we don't get from the view id.
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

        match document_id_arg.1 {
            Some(document_id_scalar) => Ok(Some(
                self.get_by_document_id(
                    document_id_scalar.into(),
                    ctx,
                    ctx.field().selection_set().collect(),
                    Some(schema_id),
                )
                .await?,
            )),
            None => Err(ServerError::new(
                "Must provide either `id` or `viewId` argument.".to_string(),
                Some(ctx.item.pos),
            )),
        }
    }

    /// Returns all documents for the given schema as a GraphQL value.
    async fn query_listing(
        &self,
        schema_id: &SchemaId,
        ctx: &Context<'_>,
    ) -> ServerResult<Option<Value>> {
        info!("Handling listing query for {}", schema_id);

        let store = ctx.data_unchecked::<SqlStorage>();

        // Retrieve all documents for schema from storage.
        let documents = store
            .get_documents_by_schema(schema_id)
            .await
            .map_err(|err| ServerError::new(err.to_string(), None))?;

        // Assemble views async
        let documents_graphql_values = documents.into_iter().map(|view| async move {
            let selected_fields = ctx.field().selection_set().collect();
            self.document_response(view, ctx, selected_fields).await
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
        debug!("Fetching <Document {}> from store", document_id);

        let store = ctx.data_unchecked::<SqlStorage>();
        let view = store.get_document_by_id(&document_id).await.unwrap();
        match view {
            Some(view) => {
                // Validate the document's schema if the `validate_schema` argument is set.
                if let Some(expected_schema_id) = validate_schema {
                    validate_view_matches_schema(view.id(), expected_schema_id, ctx).await?;
                }

                self.document_response(view, ctx, selected_fields).await
            }
            None => {
                error!("No view found for document {}", document_id.as_str());
                Ok(Value::Null)
            }
        }
    }

    /// Fetches the given document view id from the store and returns it as a GraphQL value.
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
        debug!("Fetching <DocumentView {}> from store", document_view_id);

        let store = ctx.data_unchecked::<SqlStorage>();
        let view = store
            .get_document_view_by_id(&document_view_id)
            .await
            .unwrap();
        match view {
            Some(view) => {
                // Validate the document's schema if the `validate_schema` argument is set.
                if let Some(expected_schema_id) = validate_schema {
                    validate_view_matches_schema(view.id(), expected_schema_id, ctx).await?;
                }
                self.document_response(view, ctx, selected_fields).await
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
        view: DocumentView,
        ctx: &Context<'_>,
        selected_fields: Vec<SelectionField<'async_recursion>>,
    ) -> ServerResult<Value> {
        let mut document_fields = IndexMap::new();

        for field in selected_fields {
            // Assemble selected metadata values.
            if field.name() == "meta" {
                document_fields.insert(
                    Name::new(field.alias().unwrap_or_else(|| field.name())),
                    DocumentMetaType::resolve(field, None, Some(view.id())),
                );
            }

            // Assemble selected document field values.
            if field.name() == "fields" {
                let subselection = field.selection_set().collect();
                document_fields.insert(
                    Name::new(field.alias().unwrap_or_else(|| field.name())),
                    self.document_fields_response(view.clone(), ctx, subselection)
                        .await?,
                );
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
        view: DocumentView,
        ctx: &Context<'_>,
        selected_fields: Vec<SelectionField<'async_recursion>>,
    ) -> ServerResult<Value> {
        let store = ctx.data_unchecked::<SqlStorage>();
        let schema_id = store
            .get_schema_by_document_view(view.id())
            .await
            .map_err(|err| ServerError::new(err.to_string(), None))?
            .unwrap();

        let schema_provider = ctx.data_unchecked::<SchemaProvider>();
        // Unwrap because this schema id comes from the store.
        let schema = schema_provider.get(&schema_id).await.unwrap();

        // Construct GraphQL value for every field of the given view that has been selected.
        let mut view_fields = IndexMap::new();
        for selected_field in selected_fields {
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
            let document_view_value = view.get(selected_field.name()).unwrap();

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
                    let queries = rel.clone().into_iter().map(|doc_id| {
                        self.get_by_document_id(doc_id, ctx, next_selection.clone(), None)
                    });
                    Value::List(future::try_join_all(queries).await?)
                }
                OperationValue::PinnedRelationList(rel) => {
                    let queries = rel.clone().into_iter().map(|view_id| {
                        self.get_by_document_view_id(view_id, ctx, next_selection.clone(), None)
                    });
                    Value::List(future::try_join_all(queries).await?)
                }

                // Convert all simple fields to scalar values.
                _ => gql_scalar(document_view_value.value()),
            };
            view_fields.insert(
                Name::new(
                    selected_field
                        .alias()
                        .unwrap_or_else(|| selected_field.name()),
                ),
                value,
            );
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
        OperationValue::Text(value) => value.to_owned().into(),
        // only use for scalars
        _ => panic!("can only return scalar values"),
    }
}
