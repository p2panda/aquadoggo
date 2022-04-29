// SPDX-License-Identifier: AGPL-3.0-or-later
use async_trait::async_trait;
use futures::future::try_join_all;
use p2panda_rs::document::{DocumentView, DocumentViewId};
use sqlx::{query, query_as, FromRow};

use p2panda_rs::identity::Author;
use p2panda_rs::operation::{
    AsOperation, Operation, OperationAction, OperationFields, OperationId, OperationValue,
};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::Validate;

use crate::db::store::SqlStorage;

#[derive(FromRow, Debug)]
pub struct DocumentViewRow {
    /// The id of this document view.
    document_view_id: String,

    /// The id of the schema this document view follows.
    schema_id_short: String,
}

#[derive(FromRow, Debug)]
pub struct DocumentViewFieldRow {
    document_view_id: String,

    operation_id: String,

    name: String,
}

#[derive(Debug, Clone)]
pub struct DoggoDocumentView {
    id: DocumentViewId,

    view: DocumentView,
}

