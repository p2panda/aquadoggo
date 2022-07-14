// SPDX-License-Identifier: AGPL-3.0-or-later

use std::borrow::Cow;

use async_graphql::indexmap::IndexMap;
use async_graphql::parser::types::Field;
use async_graphql::registry::{Deprecation, MetaEnumValue, MetaField, MetaType, MetaTypeId};
use async_graphql::{ContextSelectionSet, OutputType, Positioned, ServerResult, Value};
use p2panda_rs::schema::{FieldType, Schema};

use crate::graphql::client::query::DynamicQuery;
use crate::schema::load_static_schemas;

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

            reg.types.insert(
                "DocumentMetadata".to_string(),
                get_document_metadata_metatype(),
            );

            reg.types
                .insert("DocumentStatus".to_string(), get_document_status_metatype());

            for schema in schemas.iter() {
                // Insert GraphQL types for all registered schemas.
                reg.types
                    .insert(schema.id().as_str().to_owned(), get_schema_metatype(schema));

                reg.types.insert(
                    get_schema_fields_type(schema),
                    get_schema_fields_metatype(schema),
                );

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

fn get_schema_metafield(schema: &'static Schema) -> MetaField {
    MetaField {
        name: schema.id().as_str(),
        description: Some(schema.description()),
        args: Default::default(),
        ty: schema.id().as_str(),
        deprecation: Default::default(),
        cache_control: Default::default(),
        external: false,
        requires: None,
        provides: None,
        visible: None,
        compute_complexity: None,
        oneof: false,
    }
}
/// Return metatype for generic document metadata.
fn get_document_status_metatype() -> MetaType {
    let mut enum_values = IndexMap::new();

    enum_values.insert(
        "Unavailable",
        MetaEnumValue {
            name: "Unavailable",
            description: Some("We don't have any information about this document."),
            deprecation: Deprecation::NoDeprecated,
            visible: None,
        },
    );

    enum_values.insert(
        "Incomplete",
        MetaEnumValue {
            name: "Incomplete",
            description: Some(
                "We have some operations for this document but it's not materialised yet.",
            ),
            deprecation: Deprecation::NoDeprecated,
            visible: None,
        },
    );

    enum_values.insert(
        "Ok",
        MetaEnumValue {
            name: "Ok",
            description: Some("The document has some materialised view available."),
            deprecation: Deprecation::NoDeprecated,
            visible: None,
        },
    );

    enum_values.insert(
        "Deleted",
        MetaEnumValue {
            name: "Deleted",
            description: Some("The document has been deleted."),
            deprecation: Deprecation::NoDeprecated,
            visible: None,
        },
    );

    MetaType::Enum {
        name: "DocumentStatus".to_string(),
        description: None,
        enum_values,
        visible: None,
        rust_typename: "__fake__",
    }
}

/// Return metatype for generic document metadata.
fn get_document_metadata_metatype() -> MetaType {
    let mut fields = IndexMap::new();

    fields.insert(
        "document_id".to_string(),
        MetaField {
            name: "document_id".to_string(),
            description: None,
            args: Default::default(),
            ty: "String".to_string(),
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
        "document_view_id".to_string(),
        MetaField {
            name: "document_view_id".to_string(),
            description: None,
            args: Default::default(),
            ty: "String".to_string(),
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
        "status".to_string(),
        MetaField {
            name: "status".to_string(),
            description: None,
            args: Default::default(),
            ty: "DocumentStatus".to_string(),
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

    MetaType::Object {
        name: "DocumentMetadata".to_string(),
        description: Some("Metadata for documents of this schema."),
        visible: Some(|_| true),
        fields,
        cache_control: Default::default(),
        extends: false,
        keys: None,
        is_subscription: false,
        rust_typename: "__fake__",
    }
}

/// Returns the root metatype for a schema.
fn get_schema_metatype(schema: &'static Schema) -> MetaType {
    let mut fields = IndexMap::new();

    fields.insert(
        "meta".to_string(),
        MetaField {
            name: "meta".to_string(),
            description: Some("Metadata for documents of this schema."),
            args: Default::default(),
            ty: "DocumentMetadata".to_string(),
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
        "fields".to_string(),
        MetaField {
            name: "fields".to_string(),
            description: None,
            args: Default::default(),
            ty: get_schema_fields_type(schema),
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

    MetaType::Object {
        name: schema.id().as_str(),
        description: Some(schema.description()),
        visible: Some(|_| true),
        fields,
        cache_control: Default::default(),
        extends: false,
        keys: None,
        is_subscription: false,
        rust_typename: "__fake__",
    }
}

fn get_schema_fields_type(schema: &'static Schema) -> String {
    format!("FieldValues_{}", schema.id().as_str())
}

/// Returns the metatype for a schema's fields.
fn get_schema_fields_metatype(schema: &'static Schema) -> MetaType {
    let mut fields = IndexMap::new();
    schema.fields().iter().for_each(|(field_name, field_type)| {
        fields.insert(
            field_name.to_string(),
            MetaField {
                name: field_name.to_string(),
                description: None,
                args: Default::default(),
                ty: get_graphql_type(field_type),
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
    });
    MetaType::Object {
        name: get_schema_fields_type(schema),
        description: Some("Data fields available on documents of this schema."),
        visible: Some(|_| true),
        fields,
        cache_control: Default::default(),
        extends: false,
        keys: None,
        is_subscription: false,
        rust_typename: "__fake__",
    }
}

/// Return GraphQL type name for a p2panda field type.
///
/// GraphQL types for relations use the same name as the p2panda schema itself.
fn get_graphql_type(operation_field_type: &FieldType) -> String {
    match operation_field_type {
        // Scalars
        FieldType::Bool => "Boolean".to_string(),
        FieldType::Int => "Int".to_string(),
        FieldType::Float => "Float".to_string(),
        FieldType::String => "String".to_string(),

        // Relations
        FieldType::Relation(schema) => schema.as_str(),
        FieldType::PinnedRelation(schema) => schema.as_str(),
        FieldType::RelationList(schema) => format!("[{}]", schema.as_str()),
        FieldType::PinnedRelationList(schema) => format!("[{}]", schema.as_str()),
    }
}
