// SPDX-License-Identifier: AGPL-3.0-or-later

use std::borrow::Cow;

use async_graphql::indexmap::IndexMap;
use async_graphql::parser::types::Field;
use async_graphql::registry::{MetaType, MetaTypeId};
use async_graphql::{
    ContainerType, ContextBase, ContextSelectionSet, Name, OutputType, Positioned, SelectionField,
    ServerResult, Value,
};
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::future;
use log::{debug, info};
use p2panda_rs::document::{DocumentId, DocumentView, DocumentViewId};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{Schema, SchemaId};

use crate::db::traits::DocumentStore;
use crate::graphql::{TempFile, TEMP_FILE_FNAME};
use crate::schema::SchemaProvider;
use crate::SqlStorage;

use super::schema::{get_schema_metafield, get_schema_metatype};

/// Container object that injects registered p2panda schemas when it is added to a GraphQL schema.
#[derive(Debug)]
pub struct DynamicQuery {
    schema_provider: SchemaProvider,
}

impl DynamicQuery {
    /// Returns a GraphQL container object given a database pool.
    pub fn new(schema_provider: SchemaProvider) -> Self {
        Self { schema_provider }
    }

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
            Some(view) => self
                .get_view(view, ctx, selected_fields)
                .await
                .unwrap()
                .unwrap(),
            None => Value::String("not found".to_string()),
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
            Some(view) => self
                .get_view(view, ctx, selected_fields)
                .await
                .unwrap()
                .unwrap(),
            None => Value::String("not found".to_string()),
        }
    }

    /// Resolves a given view and recurses into relations to produce a gql value.
    #[async_recursion]
    async fn get_view(
        &self,
        view: DocumentView,
        ctx: &ContextBase<'_, &Positioned<Field>>,
        selected_fields: Vec<SelectionField<'async_recursion>>,
    ) -> ServerResult<Option<Value>> {
        debug!("Get {}", view);
        let mut obj = IndexMap::new();
        for selected_field in selected_fields {
            let document_view_value = view.get(selected_field.name()).unwrap();
            let selected_fields: Vec<SelectionField<'async_recursion>> =
                selected_field.selection_set().collect();
            let value = match document_view_value.value() {
                // single views
                OperationValue::Relation(rel) => {
                    self.get_by_document_id(rel.document_id().clone(), ctx, selected_fields)
                        .await
                }
                OperationValue::PinnedRelation(rel) => {
                    self.get_by_document_view_id(rel.view_id().clone(), ctx, selected_fields)
                        .await
                }

                // view lists
                OperationValue::RelationList(rel) => {
                    let queries = rel.iter().map(|doc_id| {
                        self.get_by_document_id(doc_id, ctx, selected_fields.clone())
                    });
                    Value::List(future::join_all(queries).await)
                }
                OperationValue::PinnedRelationList(rel) => {
                    let queries = rel.iter().map(|view_id| {
                        self.get_by_document_view_id(view_id, ctx, selected_fields.clone())
                    });
                    Value::List(future::join_all(queries).await)
                }

                // Convert all simple fields to scalar values.
                _ => gql_scalar(document_view_value.value()),
            };
            obj.insert(Name::new(selected_field.name()), value);
        }
        Ok(Some(Value::Object(obj)))
    }

    /// Returns all documents for the given schema as a gql value.
    async fn list_schema(
        &self,
        schema_id: SchemaId,
        ctx: &ContextBase<'_, &Positioned<Field>>,
    ) -> ServerResult<Option<Value>> {
        if self.schema_provider.get(&schema_id).is_none() {
            // Abort resolving this as a document query if we don't know this schema.
            return Ok(None);
        }
        let mut result = Vec::new();
        let store = ctx.data_unchecked::<SqlStorage>();
        let views = store.get_documents_by_schema(&schema_id).await.unwrap();
        for view in views {
            if let Some(value) = self
                .get_view(view, ctx, ctx.field().selection_set().collect())
                .await?
            {
                result.push(value);
            }
        }

        Ok(Some(Value::List(result)))
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
        // This callback is given a mutable reference to the registry!
        registry.create_output_type::<DynamicQuery, _>(MetaTypeId::Object, |reg| {
            // Insert queries for all registered schemas.
            let mut fields = IndexMap::new();

            // Load schema definitions and keep them in memory until the node shuts down.
            info!("Loading schemas from temp file");
            let schemas: &'static Vec<Schema> = TempFile::load_static(TEMP_FILE_FNAME);

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
