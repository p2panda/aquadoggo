// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::ResolverContext;
use async_graphql::Value;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::storage_provider::error::DocumentStorageError;
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::db::{types::StorageDocument, SqlStore};
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};

// Correctly formats the name of a document field type.
pub fn fields_name(name: &str) -> String {
    format!("{name}Fields")
}

/// Convert non-relation operation values into GraphQL values.
///
/// Panics when given a relation field value.
pub fn gql_scalar(operation_value: &OperationValue) -> Value {
    match operation_value {
        OperationValue::Boolean(value) => value.to_owned().into(),
        OperationValue::Integer(value) => value.to_owned().into(),
        OperationValue::Float(value) => value.to_owned().into(),
        OperationValue::String(value) => value.to_owned().into(),
        // Recursion not implemented yet.
        _ => todo!(),
    }
}

/// Downcast document id and document view id from parameters passed up the query fields and
/// retrieved via the `ResolverContext`.
pub fn downcast_id_params(
    ctx: &ResolverContext,
) -> (Option<DocumentIdScalar>, Option<DocumentViewIdScalar>) {
    ctx.parent_value
        .downcast_ref::<(Option<DocumentIdScalar>, Option<DocumentViewIdScalar>)>()
        .expect("Values passed from query parent should match expected")
        .to_owned()
}

/// Helper for getting a document from score by either the document id or document view id.
pub async fn get_document_from_params(
    store: &SqlStore,
    document_id: &Option<DocumentIdScalar>,
    document_view_id: &Option<DocumentViewIdScalar>,
) -> Result<Option<StorageDocument>, DocumentStorageError> {
    match (document_id, document_view_id) {
        (None, Some(document_view_id)) => {
            store
                .get_document_by_view_id(&DocumentViewId::from(document_view_id.to_owned()))
                .await
        }
        (Some(document_id), None) => store.get_document(&DocumentId::from(document_id)).await,
        _ => panic!("Invalid values passed from query field parent"),
    }
}
