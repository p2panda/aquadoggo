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

use crate::graphql::client::dynamic_types::utils::metaobject;
use crate::graphql::client::dynamic_types::DocumentType;
use crate::graphql::client::DynamicQuery;
use crate::schema::load_static_schemas;

use crate::graphql::client::dynamic_types::utils::metafield;
use crate::graphql::client::dynamic_types::DocumentMetaType;

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
            DocumentMetaType {}.register_type(reg);

            // Insert GraphQL types for all registered schemas.
            for schema in schemas.iter() {
                let document_type = DocumentType::new(schema);
                document_type.register_type(reg);

                // Insert queries.
                fields.insert(
                    document_type.type_name(),
                    metafield(
                        &document_type.type_name(),
                        Some(document_type.schema().description()),
                        &document_type.type_name(),
                    ),
                );
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
