// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, Object, ResolverContext};
use async_graphql::Error;
use dynamic_graphql::FieldValue;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::Schema;

use crate::db::SqlStore;
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};
use crate::graphql::utils::{
    downcast_id_params, fields_name, get_document_from_params, gql_scalar, graphql_type,
};

/// Struct representing the `fields` field which all document schema types define on the graphQL
/// schema contain.
pub struct DocumentFields;

impl DocumentFields {
    pub fn build(schema: &Schema) -> Object {
        // Construct the document fields object which will be named `<schema_id>Field`.
        let schema_field_name = fields_name(schema.id());
        let mut document_schema_fields = Object::new(&schema_field_name);

        // For every field in the schema we create a type with a resolver.
        for (name, field_type) in schema.fields().iter() {
            document_schema_fields = document_schema_fields.field(Field::new(
                name,
                graphql_type(field_type),
                move |ctx| FieldFuture::new(async move { resolve_field(ctx).await }),
            ));
        }

        document_schema_fields
    }

    pub async fn resolve<'a>(ctx: ResolverContext<'a>) -> Result<Option<FieldValue<'a>>, Error> {
        // Here we just pass up the root query parameters to be used in the fields resolver
        let params = downcast_id_params(&ctx);
        Ok(Some(FieldValue::owned_any(params)))
    }
}

/// Resolve a document field value as a graphql `FieldValue`. If the value is a relation, then
/// the relevant document id or document view id is determined and passed along the query chain.
/// If the value is a simple type (meaning it is also a query leaf) then it is directly resolved.
///
/// Requires a `ResolverContext` to be passed into the method.
async fn resolve_field<'a>(ctx: ResolverContext<'a>) -> Result<Option<FieldValue<'a>>, Error> {
    let store = ctx.data_unchecked::<SqlStore>();
    let name = ctx.field().name();

    // Parse the bubble up message.
    let (document_id, document_view_id) = downcast_id_params(&ctx);

    // Get the whole document from the store.
    let document = match get_document_from_params(store, &document_id, &document_view_id).await? {
        Some(document) => document,
        None => return Ok(FieldValue::NONE),
    };

    // Get the field this query is concerned with.
    match document.get(&name).unwrap() {
        // Relation fields are expected to resolve to the related document so we pass
        // along the document id which will be processed through it's own resolver.
        OperationValue::Relation(rel) => Ok(Some(FieldValue::owned_any((
            Some(DocumentIdScalar::from(rel.document_id())),
            None::<DocumentViewIdScalar>,
        )))),
        // Relation lists are handled by collecting and returning a list of all document
        // id's in the relation list. Each of these in turn are processed and queries
        // forwarded up the tree via their own respective resolvers.
        OperationValue::RelationList(rel) => {
            let mut fields = vec![];
            for document_id in rel.iter() {
                fields.push(FieldValue::owned_any((
                    Some(DocumentIdScalar::from(document_id)),
                    None::<DocumentViewIdScalar>,
                )));
            }
            Ok(Some(FieldValue::list(fields)))
        }
        // Pinned relation behaves the same as relation but passes along a document view id.
        OperationValue::PinnedRelation(rel) => Ok(Some(FieldValue::owned_any((
            None::<DocumentIdScalar>,
            Some(DocumentViewIdScalar::from(rel.view_id())),
        )))),
        // Pinned relation lists behave the same as relation lists but pass along view ids.
        OperationValue::PinnedRelationList(rel) => {
            let mut fields = vec![];
            for document_view_id in rel.iter() {
                fields.push(FieldValue::owned_any((
                    None::<DocumentIdScalar>,
                    Some(DocumentViewIdScalar::from(document_view_id)),
                )));
            }
            Ok(Some(FieldValue::list(fields)))
        }
        // All other fields are simply resolved to their scalar value.
        value => Ok(Some(FieldValue::value(gql_scalar(value)))),
    }
}
