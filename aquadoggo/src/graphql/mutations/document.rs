// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use async_graphql::dynamic::{Field, FieldFuture, InputValue, Object, ResolverContext, TypeRef};
use async_graphql::{Error, Name, Value};
use dynamic_graphql::FieldValue;
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::identity::KeyPair;
use p2panda_rs::operation::{OperationBuilder, OperationId, OperationValue};
use p2panda_rs::schema::Schema;
use p2panda_rs::test_utils::memory_store::helpers::send_to_store;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::db::SqlStore;
use crate::graphql::utils::{graphql_input, graphql_to_operation_value};

/// Construct a mutation query based on a schema, for creating, updating and deleting documents.
pub fn build_document_mutation(query: Object, schema: &Schema) -> Object {
    let schema_id = schema.id().clone();
    let schema_clone = schema.clone();
    let mut mutation_field = Field::new(
        schema_id.to_string(),
        TypeRef::named(TypeRef::BOOLEAN),
        move |ctx| {
            let schema_clone = schema_clone.clone();
            FieldFuture::new(async move {
                let schema_clone = schema_clone.clone();
                let fields = parse_arguments(&ctx, &schema_clone)?;
                let store = ctx.data_unchecked::<SqlStore>();
                let tx = ctx.data_unchecked::<ServiceSender>();

                let create_op = OperationBuilder::new(schema_clone.id())
                    .fields(&fields)
                    .build()
                    .expect("Build operation");

                let (encoded_entry, _) =
                    send_to_store(store, &create_op, &schema_clone, &KeyPair::new())
                        .await
                        .expect("Publish CREATE operation");

                let operation_id: OperationId = encoded_entry.hash().into();

                if tx.send(ServiceMessage::NewOperation(operation_id)).is_err() {
                    // Silently fail here as we don't mind if there are no subscribers. We have
                    // tests in other places to check if messages arrive.
                }

                Ok(None::<FieldValue>)
            })
        },
    );

    for (name, field_type) in schema.fields().iter() {
        mutation_field = mutation_field.argument(InputValue::new(name, graphql_input(field_type)));
    }

    query.field(mutation_field)
}

/// Parse the provided arguments into field k-v tuples.
fn parse_arguments(
    ctx: &ResolverContext,
    schema: &Schema,
) -> Result<Vec<(String, OperationValue)>, Error> {
    let mut fields = vec![];

    let arguments: HashMap<Name, Value> = ctx.field().arguments()?.into_iter().collect();

    for (name, field_type) in schema.fields().iter() {
        let value = arguments.get(&Name::new(name));

        if let Some(value) = value {
            fields.push((
                name.to_owned(),
                graphql_to_operation_value(&value, field_type.to_owned()),
            ))
        }
    }

    Ok(fields)
}
