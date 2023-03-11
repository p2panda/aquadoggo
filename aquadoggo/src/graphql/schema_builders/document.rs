// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Object, Field, TypeRef, InputValue, FieldFuture};
use p2panda_rs::{schema::Schema, document::traits::AsDocument};
use dynamic_graphql::{FieldValue, Error, ScalarValue};

use crate::graphql::scalars::{DocumentViewIdScalar, DocumentIdScalar};
use crate::graphql::types::DocumentMeta;
use crate:: db::SqlStore;
use crate::graphql::utils::{downcast_id_params, get_document_from_params, fields_name};

/// Build a graphql object type for a p2panda schema.
pub fn build_document_schema(schema: &Schema) -> Object {
    let document_fields_name = fields_name(&schema.id().to_string());
    Object::new(&schema.id().to_string())
            .field(Field::new(
                "fields",
                TypeRef::named_nn(document_fields_name),
                move |ctx| {
                    FieldFuture::new(async move {
                        // Here we just pass up the root query parameters to be used in the fields resolver.
                        let params = downcast_id_params(&ctx);
                        Ok(Some(FieldValue::owned_any(params)))
                    })
                },
            ))
            .field(Field::new(
                "meta",
                TypeRef::named_nn("DocumentMeta"),
                move |ctx| {
                    FieldFuture::new(async move {
                        let store = ctx.data_unchecked::<SqlStore>();

                        // Downcast the parameters passed up from the parent query field.
                        let (document_id, document_view_id) = downcast_id_params(&ctx);
                        // Get the whole document.
                        let document =
                            get_document_from_params(store, &document_id, &document_view_id).await?;

                        // Construct `DocumentMeta` and return it. We defined the document meta
                        // type and already registered it in the schema. It's derived resolvers
                        // will handle field selection.
                        //
                        // TODO: We could again optimize here by defining our own resolver logic
                        // for each field.
                        let field_value = match document {
                            Some(document) => {
                                let document_meta = DocumentMeta {
                                    document_id: document.id().into(),
                                    view_id: document.view_id().into(),
                                };
                                Some(FieldValue::owned_any(document_meta))
                            }
                            None => Some(FieldValue::NULL),
                        };

                        Ok(field_value)
                    })
                },
            ))
            .description(schema.description())
}

// Add next args to the query object.
pub fn build_document_query(query: Object, schema: &Schema) -> Object {
    query.field(
        Field::new(
            schema.id().to_string(),
            TypeRef::named(schema.id().to_string()),
            move |ctx| {
                FieldFuture::new(async move {
                    let mut args = ctx.field().arguments()?.into_iter().map(|(_, value)| value);

                    // Parse document id.
                    let document_id = match args.next() {
                        Some(id) => Some(DocumentIdScalar::from_value(id)?),
                        None => None,
                    };

                    // Parse document view id.
                    let document_view_id = match args.next() {
                        Some(id) => Some(DocumentViewIdScalar::from_value(id)?),
                        None => None,
                    };

                    // Check a valid combination of id's was passed.
                    match (&document_id, &document_view_id) {
                        (None, None) => return Err(Error::new(
                            "Either document id or document view id arguments must be passed",
                        )),
                        (Some(_), Some(_)) => return Err(Error::new(
                            "Both document id and document view id arguments cannot be passed",
                        )),
                        (_, _) => (),
                    };
                    // Pass them up to the children query fields.
                    Ok(Some(FieldValue::owned_any((document_id, document_view_id))))
                })
            },
        )
        .argument(InputValue::new("id", TypeRef::named("DocumentId")))
        .argument(InputValue::new("viewId", TypeRef::named("DocumentViewId")))
        .description(format!(
            "Query a {} document by id or view id",
            schema.name()
        )),
    )
}
