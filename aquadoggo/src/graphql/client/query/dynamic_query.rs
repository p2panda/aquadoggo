// SPDX-License-Identifier: AGPL-3.0-or-later

use std::borrow::Cow;

use async_graphql::indexmap::IndexMap;
use async_graphql::parser::types::Field;
use async_graphql::registry::{MetaType, MetaTypeId};
use async_graphql::{
    ContainerType, ContextSelectionSet, Name, OutputType, Positioned, SelectionField, ServerResult,
    Value,
};
use async_recursion::async_recursion;
use async_trait::async_trait;
use p2panda_rs::schema::{FieldType, Schema, SchemaId};

use crate::graphql::TEMP_FILE_PATH;
use crate::schema_service::{SchemaService, TempFile};

use super::schema::{get_schema_metafield, get_schema_metatype};

/// Container object that injects registered p2panda schemas when it is added to a GraphQL schema.
#[derive(Debug)]
pub struct DynamicQuery {
    schema_service: SchemaService,
}

impl DynamicQuery {
    /// Returns a GraphQL container object given a database pool.
    pub fn new(schema_service: SchemaService) -> Self {
        Self { schema_service }
    }

    /// Query database for selected field values and return a JSON result.
    #[async_recursion]
    async fn resolve_dynamic(
        &self,
        schema_id: SchemaId,
        ctx: SelectionField<'async_recursion>,
    ) -> ServerResult<Option<Value>> {
        let schema = self
            .schema_service
            .get_schema(schema_id)
            .await
            .unwrap()
            .unwrap();

        let mut fields: IndexMap<Name, Value> = IndexMap::new();

        let selected_fields = ctx
            .selection_set()
            .map(|field| {
                let (field_name, field_type) = schema
                    .fields()
                    .iter()
                    .find(|f| f.0 == field.name())
                    .unwrap();
                (field, field_name.to_owned(), field_type.to_owned())
            })
            .collect::<Vec<(SelectionField, String, FieldType)>>();

        for (graphql_field, field_name, p2panda_field_type) in selected_fields {
            let value = match p2panda_field_type {
                FieldType::Bool => Some(Value::Boolean(true)),
                FieldType::Int => Some(Value::Number(5u8.into())),
                FieldType::Float => Some(1.5f32.into()),
                FieldType::String => Some("Hello".into()),
                FieldType::Relation(schema) => {
                    self.resolve_dynamic(schema, graphql_field).await.unwrap()
                }
                FieldType::RelationList(schema) => {
                    self.resolve_dynamic(schema, graphql_field).await.unwrap()
                }
                FieldType::PinnedRelation(schema) => {
                    self.resolve_dynamic(schema, graphql_field).await.unwrap()
                }
                FieldType::PinnedRelationList(schema) => {
                    self.resolve_dynamic(schema, graphql_field).await.unwrap()
                }
            }
            .unwrap();

            fields.insert(Name::new(field_name), value);
        }

        Ok(Some(Value::List(vec![Value::Object(fields)])))
    }
}

#[async_trait]
impl ContainerType for DynamicQuery {
    async fn resolve_field(&self, ctx: &async_graphql::Context<'_>) -> ServerResult<Option<Value>> {
        // @TODO: This needs to return `None` if that schema does not exist
        let schema: SchemaId = ctx.field().name().parse().unwrap();
        self.resolve_dynamic(schema, ctx.field()).await
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
            let schemas: &'static Vec<Schema> = TempFile::load_static(TEMP_FILE_PATH);

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
