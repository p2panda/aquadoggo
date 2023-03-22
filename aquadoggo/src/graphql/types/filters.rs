// SPDX-License-Identifier: AGPL-3.0-or-later

use dynamic_graphql::InputObject;

use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};

/// A filter input type for string field values.
#[derive(InputObject)]
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

    /// Filter for items which contain given value.
    contains: Option<u64>,

    /// Filter for items which don't contain given value.
    #[graphql(name = "notContains")]
    not_contains: Option<u64>,
}

/// A filter input type for float field values.
#[derive(InputObject)]
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

    /// Filter for items which contain given value.
    contains: Option<f64>,

    /// Filter for items which don't contain given value.
    #[graphql(name = "notContains")]
    not_contains: Option<f64>,
}

/// A filter input type for boolean field values.
#[derive(InputObject)]
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
pub struct RelationFilter {
    /// Filter by equal to.
    #[graphql(name = "eq")]
    eq: Option<DocumentIdScalar>,

    /// Filter by not equal to.
    #[graphql(name = "notEq")]
    not_eq: Option<DocumentIdScalar>,
}

/// A filter input type for pinned relation field values.
#[derive(InputObject)]
pub struct PinnedRelationFilter {
    /// Filter by equal to.
    #[graphql(name = "eq")]
    eq: Option<DocumentViewIdScalar>,

    /// Filter by not equal to.
    #[graphql(name = "notEq")]
    not_eq: Option<DocumentViewIdScalar>,
}

/// A filter input type for relation list field values.
#[derive(InputObject)]
pub struct RelationListFilter {
    /// Filter for values which contain given list items.
    contains: Option<Vec<DocumentIdScalar>>,

    /// Filter for values which don't contain given list items.
    #[graphql(name = "notContains")]
    not_contains: Option<Vec<DocumentIdScalar>>,
}

/// A filter input type for pinned relation list field values.
#[derive(InputObject)]
pub struct PinnedRelationListFilter {
    /// Filter for values which contain given list items.
    contains: Option<Vec<DocumentViewIdScalar>>,

    /// Filter for values which don't contain given list items.
    #[graphql(name = "notContains")]
    not_contains: Option<Vec<DocumentViewIdScalar>>,
}
