// SPDX-License-Identifier: AGPL-3.0-or-later

use dynamic_graphql::InputObject;

use crate::graphql::input_values::{
    BooleanFilter, DocumentIdFilter, DocumentViewIdFilter, OwnerFilter,
};

/// Filter input object containing all meta fields a collection of documents can be filtered by.
///
/// Is passed to the `meta` argument on a document collection query or list relation fields.
#[derive(InputObject)]
#[allow(dead_code)]
pub struct MetaFilterInputObject {
    /// Document id filter.
    document_id: Option<DocumentIdFilter>,

    /// Document view id filter.
    #[graphql(name = "viewId")]
    document_view_id: Option<DocumentViewIdFilter>,

    /// Owner filter.
    owner: Option<OwnerFilter>,

    /// Edited filter.
    edited: Option<BooleanFilter>,

    /// Deleted filter.
    deleted: Option<BooleanFilter>,
}
