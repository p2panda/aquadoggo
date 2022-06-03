// SPDX-License-Identifier: AGPL-3.0-or-later

use std::borrow::Cow;

use async_graphql::indexmap::IndexMap;
use async_graphql::parser::types::Field;
use async_graphql::registry::{MetaField, MetaType, MetaTypeId};
use async_graphql::{
    ContainerType, ContextSelectionSet, Name, OutputType, Positioned, SelectionField, ServerResult,
    Value,
};
use async_trait::async_trait;
use p2panda_rs::schema::SchemaId;

/// Returns the schema of a relation field.
fn get_schema_for_field(parent_schema: SchemaId, _field_name: &str) -> SchemaId {
    // In this example it's always the same schema.
    parent_schema
}

// Generate graphql query and type information for a schema.
fn get_meta_for_schema(schema: SchemaId) -> (MetaType, MetaField) {
    let mut fields = IndexMap::new();
    fields.insert(
        "test".to_string(),
        MetaField {
            name: "test".to_string(),
            description: Some("A simple field with string value."),
            args: Default::default(),
            ty: "String!".to_string(),
            deprecation: Default::default(),
            cache_control: Default::default(),
            external: false,
            requires: None,
            provides: None,
            visible: None,
            compute_complexity: None,
            oneof: false,
        },
    );
    fields.insert(
        "fk_test".to_string(),
        MetaField {
            name: "fk_test".to_string(),
            description: Some("A relation field where related documents can be queried."),
            args: Default::default(),
            ty: "events_00201c221b573b1e0c67c5e2c624a93419774cdf46b3d62414c44a698df1237b1c16!"
                .to_string(),
            deprecation: Default::default(),
            cache_control: Default::default(),
            external: false,
            requires: None,
            provides: None,
            visible: None,
            compute_complexity: None,
            oneof: false,
        },
    );

    // The graphql type of the test schema's documents.
    let metatype = MetaType::Object {
        name: schema.as_str(),
        description: Some("A test schema with a simple field and a relation field."),
        visible: Some(|_| true),
        fields,
        cache_control: Default::default(),
        extends: false,
        keys: None,
        is_subscription: false,
        rust_typename: "__fake2__",
    };

    // Allows querying the schema
    let metafield = MetaField {
        name: schema.name().to_owned(),
        description: Some("Query documents of the test schema with this."),
        args: Default::default(),
        ty: schema.as_str(),
        deprecation: Default::default(),
        cache_control: Default::default(),
        external: false,
        requires: None,
        provides: None,
        visible: None,
        compute_complexity: None,
        oneof: false,
    };
    (metatype, metafield)
}

/// Container object that injects registered p2panda schemas when it is added to a GraphQL schema.
pub struct DynamicQuery(crate::db::Pool);

impl DynamicQuery {
    /// Returns a GraphQL container object given a database pool.
    pub fn new(pool: crate::db::Pool) -> Self {
        Self(pool)
    }
    /// Query database for selected field values and return a JSON result.
    fn resolve_dynamic(
        &self,
        schema: SchemaId,
        ctx: SelectionField,
    ) -> ServerResult<Option<Value>> {
        let mut fields: IndexMap<Name, Value> = IndexMap::new();
        for field in ctx.selection_set() {
            let selection: Vec<SelectionField> = field.selection_set().collect();
            // this should really do schema introspection to decide whether to recurse
            if selection.len() == 0 {
                // this field is not a relation and the document view value should be returned.
                fields.insert(Name::new(field.name()), "Hello".into());
            } else {
                // This is a relation so the field's schema is used
                let schema = get_schema_for_field(schema.clone(), field.name());
                match self.resolve_dynamic(schema, field)? {
                    Some(value) => fields.insert(Name::new(field.name()), value),
                    None => None,
                };
            }
        }
        Ok(Some(Value::List(vec![Value::Object(fields)])))
    }
}

#[async_trait]
impl ContainerType for DynamicQuery {
    async fn resolve_field(&self, ctx: &async_graphql::Context<'_>) -> ServerResult<Option<Value>> {
        let schema: SchemaId = ctx.field().name().parse().unwrap();
        self.resolve_dynamic(schema, ctx.field())
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
            let schema: SchemaId =
                "events_00201c221b573b1e0c67c5e2c624a93419774cdf46b3d62414c44a698df1237b1c16"
                    .parse()
                    .unwrap();
            let (metatype, metafield) = get_meta_for_schema(schema.clone());

            // Insert (return) types for all registered schemas.
            reg.types.insert(schema.as_str(), metatype);

            // Insert queries for all registered schemas.
            let mut fields = IndexMap::new();
            fields.insert(schema.as_str(), metafield);
            MetaType::Object {
                name: "document_container".into(),
                description: Some("Test event input type"),
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
