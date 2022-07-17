// SPDX-License-Identifier: AGPL-3.0-or-later

//! Registers GraphQL types for all [schemas][`Schema`] available in the schema provider.
//!
//! `async_graphql` doesn't provide an API for registering types that don't correspond to
//! any Rust type. This module uses undocumented, internal functionality of `async_graphql` to
//! circumvent this restriction. By implementing `OutputType` for [`DynamicQuery`] we are given
//! mutable access to the type registry and can insert types into it.

use std::borrow::Cow;

use crate::graphql::scalars::{
    DocumentId as DocumentIdScalar, DocumentViewId as DocumentViewIdScalar,
};
use async_graphql::indexmap::IndexMap;
use async_graphql::parser::types::Field;
use async_graphql::registry::{MetaField, MetaInputValue, MetaTypeId};
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
            DocumentMetaType::register_type(reg);

            // Insert GraphQL types for all registered schemas.
            for schema in schemas {
                // Register types for both this schema's `DocumentType` and its
                // `DocumentFieldsType`.
                let document_type = DocumentType::new(schema);
                document_type.register_type(reg);

                // Insert a single and listing query field for every schema with which documents of
                // that schema can be queried.
                fields.insert(
                    document_type.type_name(),
                    MetaField {
                        name: document_type.type_name(),
                        description: Some("Query a single document of this schema."),
                        ty: document_type.type_name(),
                        args: {
                            let mut single_args = IndexMap::new();
                            single_args.insert(
                                "id".to_string(),
                                MetaInputValue {
                                    name: "id",
                                    description: None,
                                    ty: DocumentIdScalar::type_name().to_string(),
                                    default_value: None,
                                    visible: None,
                                    is_secret: false,
                                },
                            );
                            single_args.insert(
                                "viewId".to_string(),
                                MetaInputValue {
                                    name: "viewId",
                                    description: None,
                                    ty: DocumentViewIdScalar::type_name().to_string(),
                                    default_value: None,
                                    visible: None,
                                    is_secret: false,
                                },
                            );
                            single_args
                        },
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
                    format!("all_{}", document_type.type_name()),
                    metafield(
                        &format!("all_{}", document_type.type_name()),
                        Some("Query all documents of this schema."),
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
