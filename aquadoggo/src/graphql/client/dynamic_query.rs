// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use async_graphql::parser::types::Field;

use async_graphql::{
    ContainerType, ContextBase, Name, Positioned, SelectionField, ServerError, ServerResult, Value,
};
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::future;
use log::debug;
use p2panda_rs::document::{DocumentId, DocumentView, DocumentViewId};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::SchemaId;

use crate::db::provider::SqlStorage;
use crate::db::traits::DocumentStore;
use crate::graphql::client::dynamic_types::DocumentMetaType;
use crate::schema::SchemaProvider;

/// Container object that injects registered p2panda schemas when it is added to a GraphQL schema.
#[derive(Debug, Default)]
pub struct DynamicQuery;

#[async_trait]
impl ContainerType for DynamicQuery {
    async fn resolve_field(
        &self,
        ctx: &ContextBase<&Positioned<Field>>,
    ) -> ServerResult<Option<Value>> {
        // This resolver is called for all queries but we only want to resolve if the queried field
        // can actually be parsed as a schema id.
        let schema_parsed = ctx.field().name().parse::<SchemaId>();
        match schema_parsed {
            Ok(schema) => self.list_schema(schema, ctx).await,
            Err(_) => Ok(None),
        }
    }
}

impl DynamicQuery {
    /// Fetches the latest view for the given document id from the store and returns it as a
    /// GraphQL value.
    ///
    /// Recurses into relations when those are selected in `selected_fields`.
    #[async_recursion]
    async fn get_by_document_id(
        &self,
        document_id: DocumentId,
        ctx: &ContextBase<'_, &Positioned<Field>>,
        selected_fields: Vec<SelectionField<'async_recursion>>,
    ) -> Value {
        let store = ctx.data_unchecked::<SqlStorage>();
        let view = store.get_document_by_id(&document_id).await.unwrap();
        match view {
            Some(view) => self
                .document_response(view, ctx, selected_fields)
                .await
                .unwrap(),
            None => Value::Null,
        }
    }

    /// Fetches the given document view id from the store and returns it as a GraphQL value.
    ///
    /// Recurses into relations when those are selected in `selected_fields`.
    #[async_recursion]
    async fn get_by_document_view_id(
        &self,
        document_view_id: DocumentViewId,
        ctx: &ContextBase<'_, &Positioned<Field>>,
        selected_fields: Vec<SelectionField<'async_recursion>>,
    ) -> Value {
        let store = ctx.data_unchecked::<SqlStorage>();
        let view = store
            .get_document_view_by_id(&document_view_id)
            .await
            .unwrap();
        match view {
            Some(view) => self
                .document_response(view, ctx, selected_fields)
                .await
                .unwrap(),
            None => Value::Null,
        }
    }

    /// Builds a GraphQL response value for a document.
    ///
    /// This uses unstable, undocumented features of `async_graphql`.
    #[async_recursion]
    async fn document_response(
        &self,
        view: DocumentView,
        ctx: &ContextBase<'_, &Positioned<Field>>,
        selected_fields: Vec<SelectionField<'async_recursion>>,
    ) -> ServerResult<Value> {
        let mut document_fields = IndexMap::new();

        for field in selected_fields {
            // Assemble selected metadata values.
            if field.name() == "meta" {
                document_fields.insert(
                    Name::new("meta"),
                    DocumentMetaType::resolve(field, None, Some(view.id())),
                );
            }

            // Assemble selected document field valuues.
            if field.name() == "fields" {
                let subselection = field.selection_set().collect();
                document_fields.insert(
                    Name::new("fields"),
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
        ctx: &ContextBase<'_, &Positioned<Field>>,
        selected_fields: Vec<SelectionField<'async_recursion>>,
    ) -> ServerResult<Value> {
        debug!("Get {}", view);

        // We assume that the query root has been configured with an SQL storage context.
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
            // Retrieve the current field's value from the document view.
            let document_view_value = view.get(selected_field.name()).ok_or_else(|| {
                ServerError::new(
                    format!(
                        "Field {} does not exist for schema {}",
                        selected_field.name(),
                        schema
                    ),
                    None,
                )
            })?;

            // Collect any further fields that have been selected on the current field.
            let next_selection: Vec<SelectionField<'async_recursion>> =
                selected_field.selection_set().collect();

            let value = match document_view_value.value() {
                // Recurse into single views.
                OperationValue::Relation(rel) => {
                    self.get_by_document_id(rel.document_id().clone(), ctx, next_selection)
                        .await
                }
                OperationValue::PinnedRelation(rel) => {
                    self.get_by_document_view_id(rel.view_id().clone(), ctx, next_selection)
                        .await
                }

                // Recurse into view lists.
                OperationValue::RelationList(rel) => {
                    let queries = rel
                        .iter()
                        .map(|doc_id| self.get_by_document_id(doc_id, ctx, next_selection.clone()));
                    Value::List(future::join_all(queries).await)
                }
                OperationValue::PinnedRelationList(rel) => {
                    let queries = rel.iter().map(|view_id| {
                        self.get_by_document_view_id(view_id, ctx, next_selection.clone())
                    });
                    Value::List(future::join_all(queries).await)
                }

                // Convert all simple fields to scalar values.
                _ => gql_scalar(document_view_value.value()),
            };
            view_fields.insert(Name::new(selected_field.name()), value);
        }
        Ok(Value::Object(view_fields))
    }

    /// Returns all documents for the given schema as a GraphQL value.
    async fn list_schema(
        &self,
        schema_id: SchemaId,
        ctx: &ContextBase<'_, &Positioned<Field>>,
    ) -> ServerResult<Option<Value>> {
        // We assume that the query root has been configured with a `SchemaProvider` context.
        let schema_provider = ctx.data_unchecked::<SchemaProvider>();

        if schema_provider.get(&schema_id).await.is_none() {
            // Abort resolving this as a document query if we don't know this schema.
            return Ok(None);
        }

        // We assume that an SQL storage exists in the context at this point.
        let store = ctx.data_unchecked::<SqlStorage>();

        // Retrieve all documents for schema from storage.
        let documents = store
            .get_documents_by_schema(&schema_id)
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
