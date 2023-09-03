// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use async_graphql::dynamic::{Field, FieldFuture, InputValue, Object, ResolverContext, TypeRef};
use async_graphql::{Error, Name, Value};
use dynamic_graphql::{FieldValue, ScalarValue};
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::identity::KeyPair;
use p2panda_rs::operation::{OperationAction, OperationBuilder, OperationId, OperationValue};
use p2panda_rs::schema::Schema;
use p2panda_rs::test_utils::memory_store::helpers::send_to_store;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::db::SqlStore;
use crate::graphql::scalars::DocumentViewIdScalar;
use crate::graphql::utils::{fields_input_name, graphql_to_operation_value};

/// Construct a mutation query based on a schema, for creating, updating and deleting documents.
pub fn build_document_mutation(query: Object, schema: &Schema) -> Object {
    let schema_id = schema.id().clone();
    let schema_clone = schema.clone();
    let mutation_field = Field::new(
        schema_id.to_string(),
        TypeRef::named(TypeRef::BOOLEAN),
        move |ctx| {
            let schema_clone = schema_clone.clone();
            FieldFuture::new(async move {
                let schema_clone = schema_clone.clone();
                let (fields, previous) = parse_arguments(&ctx, &schema_clone)?;
                let store = ctx.data_unchecked::<SqlStore>();
                let tx = ctx.data_unchecked::<ServiceSender>();

                let mut operation_builder = OperationBuilder::new(schema_clone.id());

                if let Some(previous) = previous {
                    operation_builder = operation_builder
                        .previous(&previous)
                        .action(OperationAction::Update);
                }

                if let Some(fields) = fields {
                    operation_builder = operation_builder.fields(&fields);
                } else {
                    operation_builder = operation_builder.action(OperationAction::Delete)
                }

                let operation = operation_builder.build()?;

                // @TODO: We need to consider what key pair we want to use here (the node's?) and how to 
                // make it available here to sign entries.
                let (encoded_entry, _) =
                    send_to_store(store, &operation, &schema_clone, &KeyPair::new()).await?;

                let operation_id: OperationId = encoded_entry.hash().into();

                if tx.send(ServiceMessage::NewOperation(operation_id)).is_err() {
                    // Silently fail here as we don't mind if there are no subscribers. We have
                    // tests in other places to check if messages arrive.
                }

                Ok(None::<FieldValue>)
            })
        },
    );

    let schema_field_name = fields_input_name(schema.id());
    let mutation_field = mutation_field
        .argument(InputValue::new("fields", TypeRef::named(schema_field_name)))
        .argument(InputValue::new(
            "previous",
            TypeRef::named("DocumentViewId"),
        ));

    query.field(mutation_field)
}

/// Parse the provided arguments into field k-v tuples.
fn parse_arguments(
    ctx: &ResolverContext,
    schema: &Schema,
) -> Result<
    (
        Option<Vec<(String, OperationValue)>>,
        Option<DocumentViewId>,
    ),
    Error,
> {
    let mut fields = vec![];

    let arguments: HashMap<Name, Value> = ctx.field().arguments()?.into_iter().collect();

    if let Some(fields_arguments) = arguments.get("fields") {
        if let Value::Object(fields_arguments) = fields_arguments {
            for (name, field_type) in schema.fields().iter() {
                let value = fields_arguments.get(&Name::new(name));

                if let Some(value) = value {
                    fields.push((
                        name.to_owned(),
                        graphql_to_operation_value(&value, field_type.to_owned()),
                    ))
                }
            }
        }
    }

    let fields = if !fields.is_empty() {
        Some(fields)
    } else {
        None
    };

    let previous = match arguments.get("previous") {
        Some(value) => {
            let document_view_id = DocumentViewIdScalar::from_value(value.to_owned())
                .expect("Value is valid document view id");

            Some(document_view_id.into())
        }
        None => None,
    };

    Ok((fields, previous))
}
