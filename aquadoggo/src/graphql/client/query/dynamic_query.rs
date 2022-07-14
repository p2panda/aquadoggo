// SPDX-License-Identifier: AGPL-3.0-or-later

use std::borrow::Cow;

use async_graphql::indexmap::IndexMap;
use async_graphql::parser::types::Field;
use async_graphql::registry::{MetaType, MetaTypeId};
use async_graphql::{
    ContainerType, ContextBase, ContextSelectionSet, Name, OutputType, Positioned, SelectionField,
    ServerError, ServerResult, Value,
};
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::future;
use log::debug;
use p2panda_rs::document::{DocumentId, DocumentView, DocumentViewId};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{Schema, SchemaId};

use crate::db::provider::SqlStorage;
use crate::db::traits::DocumentStore;
use crate::schema::{load_static_schemas, SchemaProvider};

use super::schema::{get_schema_metafield, get_schema_metatype};

/// Container object that injects registered p2panda schemas when it is added to a GraphQL schema.
#[derive(Debug, Default)]
pub struct DynamicQuery;

impl DynamicQuery {
    /// Resolve a document id to a gql value.
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
            Some(view) => self.get_view(view, ctx, selected_fields).await.unwrap(),
            None => self.get_placeholder(document_id),
        }
    }

    /// Resolve a document view id to a gql value.
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
            Some(view) => self.get_view(view, ctx, selected_fields).await.unwrap(),
            None => Value::String("not found".to_string()),
        }
    }

    /// Resolves a given view and recurses into relations to produce a gql value.
    ///
    /// This uses unstable, undocumented features of `async_graphql`.
    #[async_recursion]
    async fn get_view(
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

    /// Returns a placeholder value for documents that this node doesn't have access to (yet).
    ///
    /// The placeholder contains:
    ///
    /// - meta
    ///     - document_id
    fn get_placeholder(&self, document_id: DocumentId) -> Value {
        let mut meta_fields = IndexMap::new();
        meta_fields.insert(
            Name::new("document_id"),
            Value::String(document_id.as_str().to_string()),
        );
        let meta_value = Value::Object(meta_fields);

        let mut document_fields = IndexMap::new();
        document_fields.insert(Name::new("meta"), meta_value);

        Value::Object(document_fields)
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
            self.get_view(view, ctx, selected_fields).await
        });
        Ok(Some(Value::List(
            future::try_join_all(documents_graphql_values).await?,
        )))
    }
}

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

#[async_trait::async_trait]
impl OutputType for DynamicQuery {
    fn type_name() -> Cow<'static, str> {
        Cow::Owned("document_container".into())
    }

    /// Insert all registered p2panda schemas into the graphql schema. This function doesn't have
    /// access to the pool though...
    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        // Load schema definitions
        let schemas: &'static Vec<Schema> = load_static_schemas();

        // This callback is given a mutable reference to the registry!
        registry.create_output_type::<DynamicQuery, _>(MetaTypeId::Object, |reg| {
            // Insert queries for all registered schemas.
            let mut fields = IndexMap::new();

            for schema in schemas.iter() {
                // Insert GraphQL types for all registered schemas.
                let metatype = get_schema_metatype(schema);
                reg.types.insert(schema.id().as_str(), metatype);

                // Insert queries.
                let metafield = get_schema_metafield(schema);
                fields.insert(schema.id().as_str(), metafield);
            }

            MetaType::Object {
                name: "document_container".into(),
                description: Some("Container for dynamically generated document api"),
                visible: Some(|_| true),
                fields,
                cache_control: Default::default(),
                extends: false,
                keys: None,
                is_subscription: false,
                rust_typename: "__fake2__",
            }
        })
    }

    async fn resolve(
        &self,
        _ctx: &ContextSelectionSet<'_>,
        _field: &Positioned<Field>,
    ) -> ServerResult<Value> {
        // I don't know when this is called or whether we need it...
        todo!()
    }

    fn qualified_type_name() -> String {
        format!("{}!", <Self as OutputType>::type_name())
    }

    fn introspection_type_name(&self) -> Cow<'static, str> {
        // I don't know when this is called or whether we need it...
        todo!()
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
