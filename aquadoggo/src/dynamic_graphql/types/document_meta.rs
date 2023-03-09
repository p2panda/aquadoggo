// SPDX-License-Identifier: AGPL-3.0-or-later

use dynamic_graphql::SimpleObject;

use crate::dynamic_graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};

/// The meta fields of a document.
#[derive(SimpleObject)]
pub struct DocumentMeta {
    pub id: DocumentIdScalar,
    pub view_id: DocumentViewIdScalar,
}
