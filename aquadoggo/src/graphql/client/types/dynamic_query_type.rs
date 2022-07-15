// SPDX-License-Identifier: AGPL-3.0-or-later

//! Registers GraphQL types for all [schemas][`Schema`] available in the schema provider.
//!
//! `async_graphql` doesn't provide an API for registering custom schemas that don't correspond to
//! any Rust type. This module uses undocumented, internal functionality of `async_graphql` to
//! circumvent this restriction. By implementing `OutputType` for [`DynamicQuery`] we are given
//! mutable access to the type registry and can insert custom schemas into it.

use std::borrow::Cow;

use async_graphql::indexmap::IndexMap;
use async_graphql::parser::types::Field;
use async_graphql::registry::MetaTypeId;
use async_graphql::{
    ContextSelectionSet, OutputType, Positioned, ServerError, ServerResult, Value,
};
use p2panda_rs::schema::Schema;

use crate::graphql::client::types::utils::metaobject;
use crate::graphql::client::types::{DocumentMetaType, SchemaType};
use crate::graphql::client::DynamicQuery;
use crate::schema::load_static_schemas;

#[async_trait::async_trait]
impl OutputType for DynamicQuery {
    /// Register all GraphQL types for schemas currently available in the schema provider.
    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        // Load schema definitions
        let schemas: &'static Vec<Schema> = load_static_schemas();

        // This callback is given a mutable reference to the registry!
        registry.create_output_type::<DynamicQuery, _>(MetaTypeId::Object, |reg| {
            // Insert queries for all registered schemas.
            let mut fields = IndexMap::new();

            // Generic type of document metadata.
            reg.types
                .insert("DocumentMeta".to_string(), DocumentMetaType::meta_type());

            reg.types.insert(
                "DocumentStatus".to_string(),
                DocumentMetaType::status_type(),
            );

            // Insert GraphQL types for all registered schemas.
            for schema in schemas.iter() {
                let schema_type = SchemaType::new(schema);
                reg.types.insert(schema_type.name(), schema_type.metatype());

                let fields_type = schema_type.get_fields_type();
                reg.types.insert(fields_type.name(), fields_type.metatype());

                // Insert queries.
                fields.insert(schema_type.name(), schema_type.metafield());
            }

            metaobject(
                "dynamic_query_api",
                Some("Container for dynamically generated document api"),
                fields,
            )
        })
    }

    /// We don't expect this resolver to ever be called because the `dynamic_query_api` type is
    /// hidden.
    async fn resolve(
        &self,
        _ctx: &ContextSelectionSet<'_>,
        _field: &Positioned<Field>,
    ) -> ServerResult<Value> {
        Err(ServerError::new(
            "Unexpected call to dynamic query root resolver. This is a bug, please report it!",
            None,
        ))
    }

    fn type_name() -> Cow<'static, str> {
        Cow::Owned("dynamic_query_api".into())
    }

    fn qualified_type_name() -> String {
        format!("{}!", <Self as OutputType>::type_name())
    }

    fn introspection_type_name(&self) -> Cow<'static, str> {
        <Self as OutputType>::type_name()
    }
}
