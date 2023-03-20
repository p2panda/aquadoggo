// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::Error;
use async_graphql::dynamic::{Field, FieldFuture, Object, TypeRef, ResolverContext};
use dynamic_graphql::{FieldValue, ScalarValue};
use log::debug;
use p2panda_rs::schema::Schema;

use crate::graphql::constants;
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};
use crate::graphql::types::{DocumentMeta, DocumentFields};
use crate::graphql::utils::fields_name;

pub struct Document;

impl Document {
    /// Build a GraphQL object type for a p2panda schema.
    ///
    /// Contains resolvers for both `fields` and `meta`. The former simply passes up the query
    /// arguments to it's children query fields. The latter retrieves the document being queried and
    /// already constructs and returns the `DocumentMeta` object.
    pub fn build(schema: &Schema) -> Object {
        let document_fields_name = fields_name(schema.id());
        Object::new(schema.id().to_string())
            // The `fields` field of a document, passes up the query arguments to it's children.
            .field(Field::new(
                constants::FIELDS_FIELD,
                TypeRef::named(document_fields_name),
                move |ctx| FieldFuture::new(async move { DocumentFields::resolve(ctx).await }),
            ))
            // The `meta` field of a document, resolves the `DocumentMeta` object.
            .field(Field::new(
                constants::META_FIELD,
                TypeRef::named(constants::DOCUMENT_META),
                move |ctx| FieldFuture::new(async move { DocumentMeta::resolve(ctx).await }),
            ))
            .description(schema.description().to_string())
    }

    pub async fn resolve<'a>(ctx: ResolverContext<'a>) -> Result<Option<FieldValue<'a>>, Error> {
        // Parse arguments
        let schema_id = ctx.field().name();
        let mut document_id = None;
        let mut document_view_id = None;
        for (name, id) in ctx.field().arguments()?.into_iter() {
            match name.as_str() {
                constants::DOCUMENT_ID_ARG => {
                    document_id = Some(DocumentIdScalar::from_value(id)?);
                }
                constants::DOCUMENT_VIEW_ID_ARG => {
                    document_view_id = Some(DocumentViewIdScalar::from_value(id)?)
                }
                _ => (),
            }
        }

        // Check a valid combination of arguments was passed
        match (&document_id, &document_view_id) {
            (None, None) => {
                return Err(Error::new("Must provide either `id` or `viewId` argument"))
            }
            (Some(_), Some(_)) => {
                return Err(Error::new("Must only provide `id` or `viewId` argument"))
            }
            (Some(id), None) => {
                debug!("Query to {} received for document {}", schema_id, id);
            }
            (None, Some(id)) => {
                debug!(
                    "Query to {} received for document at view id {}",
                    schema_id, id
                );
            }
        };
        // Pass them up to the children query fields
        Ok(Some(FieldValue::owned_any((document_id, document_view_id))))
    }
}
