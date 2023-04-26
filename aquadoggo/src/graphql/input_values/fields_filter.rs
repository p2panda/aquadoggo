// SPDX-License-Identifier: AGPL-3.0-or-later

//! GraphQL input objects used to specify filter parameters in collection queries.
//!
//! Different document field types have different filter capabilities. This module contains filter
//! objects for all p2panda core types: `String`, `Integer`, `Float`, `Boolean`, `Relation`,
//! `PinnedRelation`, `RelationList` and `PinnedRelationList` as well as for `owner`, `documentId`
//! and `viewId` meta fields.
use async_graphql::dynamic::{InputObject, InputValue, TypeRef};
use dynamic_graphql::InputObject;
use p2panda_rs::schema::{FieldType, Schema};

use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar, PublicKeyScalar};
use crate::graphql::utils::filter_name;

/// Build a filter input object for a p2panda schema. It can be used to filter collection queries
/// based on the values each document contains.
///
/// The resulting input objects are used passed to the `filter` argument on a document collection
/// query or list relation fields.
pub fn build_filter_input_object(schema: &Schema) -> InputObject {
    // Construct the document fields object which will be named `<schema_id>Filter`
    let schema_field_name = filter_name(schema.id());
    let mut filter_input = InputObject::new(&schema_field_name);

    // For every field in the schema we create a type with a resolver
    for (name, field_type) in schema.fields().iter() {
        match field_type {
            FieldType::Boolean => {
                filter_input =
                    filter_input.field(InputValue::new(name, TypeRef::named("BooleanFilter")));
            }
            FieldType::Integer => {
                filter_input =
                    filter_input.field(InputValue::new(name, TypeRef::named("IntegerFilter")));
            }
            FieldType::Float => {
                filter_input =
                    filter_input.field(InputValue::new(name, TypeRef::named("FloatFilter")));
            }
            FieldType::String => {
                filter_input =
                    filter_input.field(InputValue::new(name, TypeRef::named("StringFilter")));
            }
            FieldType::Relation(_) => {
                filter_input =
                    filter_input.field(InputValue::new(name, TypeRef::named("RelationFilter")));
            }
            FieldType::RelationList(_) => {
                filter_input =
                    filter_input.field(InputValue::new(name, TypeRef::named("RelationListFilter")));
            }
            FieldType::PinnedRelation(_) => {
                filter_input = filter_input.field(InputValue::new(
                    name,
                    TypeRef::named("PinnedRelationFilter"),
                ));
            }
            FieldType::PinnedRelationList(_) => {
                filter_input = filter_input.field(InputValue::new(
                    name,
                    TypeRef::named("PinnedRelationListFilter"),
                ));
            }
        };
    }

    filter_input
}

/// A filter input type for owner field on meta object.
#[derive(InputObject)]
#[allow(dead_code)]
pub struct OwnerFilter {
    /// Filter by values in set.
    #[graphql(name = "in")]
    is_in: Option<Vec<PublicKeyScalar>>,

    /// Filter by values not in set.
    #[graphql(name = "notIn")]
    is_not_in: Option<Vec<PublicKeyScalar>>,

    /// Filter by equal to.
    #[graphql(name = "eq")]
    eq: Option<PublicKeyScalar>,

    /// Filter by not equal to.
    #[graphql(name = "notEq")]
    not_eq: Option<PublicKeyScalar>,
}

/// A filter input type for document id field on meta object.
#[derive(InputObject)]
#[allow(dead_code)]
pub struct DocumentIdFilter {
    /// Filter by values in set.
    #[graphql(name = "in")]
    is_in: Option<Vec<DocumentIdScalar>>,

    /// Filter by values not in set.
    #[graphql(name = "notIn")]
    is_not_in: Option<Vec<DocumentIdScalar>>,

    /// Filter by equal to.
    #[graphql(name = "eq")]
    eq: Option<DocumentIdScalar>,

    /// Filter by not equal to.
    #[graphql(name = "notEq")]
    not_eq: Option<DocumentIdScalar>,
}

/// A filter input type for document view id field on meta object.
#[derive(InputObject)]
#[allow(dead_code)]
pub struct DocumentViewIdFilter {
    /// Filter by values in set.
    #[graphql(name = "in")]
    is_in: Option<Vec<DocumentViewIdScalar>>,

    /// Filter by values not in set.
    #[graphql(name = "notIn")]
    is_not_in: Option<Vec<DocumentViewIdScalar>>,

    /// Filter by equal to.
    #[graphql(name = "eq")]
    eq: Option<DocumentViewIdScalar>,

    /// Filter by not equal to.
    #[graphql(name = "notEq")]
    not_eq: Option<DocumentViewIdScalar>,
}

/// A filter input type for string field values.
#[derive(InputObject)]
#[allow(dead_code)]
pub struct StringFilter {
    /// Filter by values in set.
    #[graphql(name = "in")]
    is_in: Option<Vec<String>>,

    /// Filter by values not in set.
    #[graphql(name = "notIn")]
    is_not_in: Option<Vec<String>>,

    /// Filter by equal to.
    #[graphql(name = "eq")]
    eq: Option<String>,

    /// Filter by not equal to.
    #[graphql(name = "notEq")]
    not_eq: Option<String>,

    /// Filter by greater than or equal to.
    gte: Option<String>,

    /// Filter by greater than.
    gt: Option<String>,

    /// Filter by less than or equal to.
    lte: Option<String>,

    /// Filter by less than.
    lt: Option<String>,

    /// Filter for items which contain given value.
    contains: Option<String>,

    /// Filter for items which don't contain given value.
    #[graphql(name = "notContains")]
    not_contains: Option<String>,
}

/// A filter input type for integer field values.
#[derive(InputObject)]
#[allow(dead_code)]
pub struct IntegerFilter {
    /// Filter by values in set.
    #[graphql(name = "in")]
    is_in: Option<Vec<u64>>,

    /// Filter by values not in set.
    #[graphql(name = "notIn")]
    is_not_in: Option<Vec<u64>>,

    /// Filter by equal to.
    #[graphql(name = "eq")]
    eq: Option<u64>,

    /// Filter by not equal to.
    #[graphql(name = "notEq")]
    not_eq: Option<u64>,

    /// Filter by greater than or equal to.
    gte: Option<u64>,

    /// Filter by greater than.
    gt: Option<u64>,

    /// Filter by less than or equal to.
    lte: Option<u64>,

    /// Filter by less than.
    lt: Option<u64>,
}

/// A filter input type for float field values.
#[derive(InputObject)]
#[allow(dead_code)]
pub struct FloatFilter {
    /// Filter by values in set.
    #[graphql(name = "in")]
    is_in: Option<Vec<f64>>,

    /// Filter by values not in set.
    #[graphql(name = "notIn")]
    is_not_in: Option<Vec<f64>>,

    /// Filter by equal to.
    #[graphql(name = "eq")]
    eq: Option<f64>,

    /// Filter by not equal to.
    #[graphql(name = "notEq")]
    not_eq: Option<f64>,

    /// Filter by greater than or equal to.
    gte: Option<f64>,

    /// Filter by greater than.
    gt: Option<f64>,

    /// Filter by less than or equal to.
    lte: Option<f64>,

    /// Filter by less than.
    lt: Option<f64>,
}

/// A filter input type for boolean field values.
#[derive(InputObject)]
#[allow(dead_code)]
pub struct BooleanFilter {
    /// Filter by equal to.
    #[graphql(name = "eq")]
    eq: Option<bool>,

    /// Filter by not equal to.
    #[graphql(name = "notEq")]
    not_eq: Option<bool>,
}

/// A filter input type for relation field values.
#[derive(InputObject)]
#[allow(dead_code)]
pub struct RelationFilter {
    /// Filter by equal to.
    #[graphql(name = "eq")]
    eq: Option<DocumentIdScalar>,

    /// Filter by not equal to.
    #[graphql(name = "notEq")]
    not_eq: Option<DocumentIdScalar>,

    /// Filter by values in set.
    #[graphql(name = "in")]
    is_in: Option<Vec<DocumentIdScalar>>,

    /// Filter by values not in set.
    #[graphql(name = "notIn")]
    is_not_in: Option<Vec<DocumentIdScalar>>,
}

/// A filter input type for pinned relation field values.
#[derive(InputObject)]
#[allow(dead_code)]
pub struct PinnedRelationFilter {
    /// Filter by equal to.
    #[graphql(name = "eq")]
    eq: Option<DocumentViewIdScalar>,

    /// Filter by not equal to.
    #[graphql(name = "notEq")]
    not_eq: Option<DocumentViewIdScalar>,

    /// Filter by values in set.
    #[graphql(name = "in")]
    is_in: Option<Vec<DocumentViewIdScalar>>,

    /// Filter by values not in set.
    #[graphql(name = "notIn")]
    is_not_in: Option<Vec<DocumentViewIdScalar>>,
}

/// A filter input type for relation list field values.
#[derive(InputObject)]
#[allow(dead_code)]
pub struct RelationListFilter {
    /// Filter by values in set.
    #[graphql(name = "in")]
    is_in: Option<Vec<DocumentIdScalar>>,

    /// Filter by values not in set.
    #[graphql(name = "notIn")]
    not_in: Option<Vec<DocumentIdScalar>>,
}

/// A filter input type for pinned relation list field values.
#[derive(InputObject)]
#[allow(dead_code)]
pub struct PinnedRelationListFilter {
    /// Filter by values in set.
    #[graphql(name = "in")]
    is_in: Option<Vec<DocumentViewIdScalar>>,

    /// Filter by values not in set.
    #[graphql(name = "notIn")]
    not_in: Option<Vec<DocumentViewIdScalar>>,
}
