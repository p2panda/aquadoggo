// SPDX-License-Identifier: AGPL-3.0-or-later

//! Mutation root.
use async_graphql::{Context, Object, Result};
use p2panda_rs::entry::decode::decode_entry;
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::operation::decode::decode_operation;
use p2panda_rs::operation::traits::Schematic;
use p2panda_rs::operation::{EncodedOperation, Operation, OperationId};
use p2panda_rs::Validate;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::db::provider::SqlStorage;
use crate::domain::publish;
use crate::graphql::client::NextEntryArguments;
use crate::graphql::scalars;
use crate::SchemaProvider;

/// GraphQL queries for the Client API.
#[derive(Default, Debug, Copy, Clone)]
pub struct ClientMutationRoot;

#[Object]
impl ClientMutationRoot {
    /// Publish an entry using parameters obtained through `nextEntryArgs` query.
    ///
    /// Returns arguments for publishing the next entry in the same log.
    async fn publish_entry(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "entry", desc = "Signed and encoded entry to publish")]
        entry: scalars::EntrySignedScalar,
        #[graphql(
            name = "operation",
            desc = "p2panda operation representing the entry payload."
        )]
        operation: scalars::EncodedOperationScalar,
    ) -> Result<NextEntryArguments> {
        let store = ctx.data::<SqlStorage>()?;
        let tx = ctx.data::<ServiceSender>()?;
        let schema_provider = ctx.data::<SchemaProvider>()?;

        let encoded_entry: EncodedEntry = entry.into();
        let encoded_operation: EncodedOperation = operation.into();

        let operation = decode_operation(&encoded_operation)?;

        // @TODO: Need error for when schema not present.
        let schema = schema_provider
            .get(operation.schema_id())
            .await
            .expect("Get schema");

        /////////////////////////////////////
        // PUBLISH THE ENTRY AND OPERATION //
        /////////////////////////////////////

        let next_args = publish(
            store,
            &schema,
            &encoded_entry,
            &operation,
            &encoded_operation,
        )
        .await?;

        ////////////////////////////////////////
        // SEND THE OPERATION TO MATERIALIZER //
        ////////////////////////////////////////

        // Send new operation on service communication bus, this will arrive eventually at
        // the materializer service

        let operation_id: OperationId = encoded_entry.hash().into();

        if tx.send(ServiceMessage::NewOperation(operation_id)).is_err() {
            // Silently fail here as we don't mind if there are no subscribers. We have
            // tests in other places to check if messages arrive.
        }

        Ok(next_args)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use async_graphql::{value, Request, Variables};
    use ciborium::cbor;
    use once_cell::sync::Lazy;
    use p2panda_rs::document::{DocumentId, DocumentViewId};
    use p2panda_rs::entry::encode::sign_and_encode_entry;
    use p2panda_rs::entry::traits::AsEncodedEntry;
    use p2panda_rs::entry::EncodedEntry;
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::encode::encode_operation;
    use p2panda_rs::operation::{EncodedOperation, Operation, OperationValue};
    use p2panda_rs::schema::{FieldType, Schema, SchemaId};
    use p2panda_rs::serde::serialize_value;
    use p2panda_rs::storage_provider::traits::{EntryStore, EntryWithOperation};
    use p2panda_rs::test_utils::constants::{HASH, PRIVATE_KEY, SCHEMA_ID};
    use p2panda_rs::test_utils::fixtures::{
        create_operation, delete_operation, encoded_entry, encoded_operation,
        entry_signed_encoded_unvalidated, key_pair, operation, operation_fields, random_hash,
        update_operation,
    };
    use rstest::{fixture, rstest};
    use serde_json::json;
    use tokio::sync::broadcast;

    use crate::bus::ServiceMessage;
    use crate::db::stores::test_utils::{
        doggo_fields, doggo_schema, test_db, TestDatabase, TestDatabaseRunner,
    };
    use crate::domain::next_args;
    use crate::graphql::GraphQLSchemaManager;
    use crate::http::{build_server, HttpServiceContext};
    use crate::schema::SchemaProvider;
    use crate::test_helpers::TestClient;

    fn test_schema() -> Schema {
        Schema::new(
            &SchemaId::from_str(
                "message_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b",
            )
            .unwrap(),
            "My test message schema",
            vec![("message", FieldType::String)],
        )
        .unwrap()
    }

    const PUBLISH_ENTRY_QUERY: &str = r#"
        mutation TestPublishEntry($entry: String!, $operation: String!) {
            publishEntry(entry: $entry, operation: $operation) {
                logId,
                seqNum,
                backlink,
                skiplink
            }
        }"#;

    pub static ENTRY_ENCODED: Lazy<Vec<u8>> = Lazy::new(|| {
        encoded_entry(
            1,
            0,
            None,
            None,
            EncodedOperation::new(&OPERATION_ENCODED),
            key_pair(PRIVATE_KEY),
        )
        .into_bytes()
    });

    pub static OPERATION_ENCODED: Lazy<Vec<u8>> = Lazy::new(|| {
        serialize_value(cbor!([
            1, 0, "message_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b",
            {
                "message" => "Ohh, my first message!",
            },
        ]))
    });

    pub static CREATE_OPERATION_WITH_PREVIOUS_OPS: Lazy<Vec<u8>> = Lazy::new(|| {
        serialize_value(cbor!([
            1, 0, "message_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b", [
                "002065f74f6fd81eb1bae19eb0d8dce145faa6a56d7b4076d7fba4385410609b2bae"
            ],
            {
                "message" => "Which I now update.",
            },
        ]))
    });

    pub static UPDATE_OPERATION_NO_PREVIOUS_OPS: Lazy<Vec<u8>> = Lazy::new(|| {
        serialize_value(cbor!([
            1, 1, "message_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b",
            {
                "message" => "Ohh, my first message!",
            },
        ]))
    });

    pub static DELETE_OPERATION_NO_PREVIOUS_OPS: Lazy<Vec<u8>> =
        Lazy::new(|| serialize_value(cbor!([1, 2, test_schema().id().to_string(),])));

    #[fixture]
    fn publish_entry_request(
        #[default(&EncodedEntry::new(&ENTRY_ENCODED).to_string())] entry_encoded: &str,
        #[default(&EncodedOperation::new(&OPERATION_ENCODED).to_string())] encoded_operation: &str,
    ) -> Request {
        // Prepare GraphQL mutation publishing an entry
        let parameters = Variables::from_value(value!({
            "entry": entry_encoded,
            "operation": encoded_operation,
        }));

        Request::new(PUBLISH_ENTRY_QUERY).variables(parameters)
    }

    #[rstest]
    fn publish_entry(
        #[from(test_db)]
        #[with(0, 0, 0, false, test_schema())]
        runner: TestDatabaseRunner,
        publish_entry_request: Request,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let (tx, _rx) = broadcast::channel(16);
            let manager = GraphQLSchemaManager::new(db.store, tx, db.context.schema_provider.clone()).await;
            let context = HttpServiceContext::new(manager);

            let response = context.schema.execute(publish_entry_request).await;

            assert_eq!(
                response.data,
                value!({
                    "publishEntry": {
                        "logId": "0",
                        "seqNum": "2",
                        "backlink": "0020dda3b3977477e4c621ce124903a736e54b139afcb033e99677a6c8470b26514c",
                        "skiplink": null,
                    }
                })
            );
        });
    }

    #[rstest]
    fn sends_message_on_communication_bus(
        #[from(test_db)]
        #[with(0, 0, 0, false, test_schema())]
        runner: TestDatabaseRunner,
        publish_entry_request: Request,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let (tx, mut rx) = broadcast::channel(16);
            let manager =
                GraphQLSchemaManager::new(db.store, tx, db.context.schema_provider.clone()).await;
            let context = HttpServiceContext::new(manager);

            context.schema.execute(publish_entry_request).await;

            // Find out hash of test entry to determine operation id
            let entry_encoded = EncodedEntry::new(&ENTRY_ENCODED);

            // Expect receiver to receive sent message
            let message = rx.recv().await.unwrap();
            assert_eq!(
                message,
                ServiceMessage::NewOperation(entry_encoded.hash().into())
            );
        });
    }

    #[rstest]
    fn post_gql_mutation(
        #[from(test_db)]
        #[with(0, 0, 0, false, test_schema())]
        runner: TestDatabaseRunner,
        publish_entry_request: Request,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let (tx, _rx) = broadcast::channel(16);
            let manager =
                GraphQLSchemaManager::new(db.store, tx, db.context.schema_provider.clone()).await;
            let context = HttpServiceContext::new(manager);
            let client = TestClient::new(build_server(context));

            let response = client
                .post("/graphql")
                .json(&json!({
                  "query": publish_entry_request.query,
                  "variables": publish_entry_request.variables
                }
                ))
                .send()
                .await;

            assert_eq!(
                response.json::<serde_json::Value>().await,
                json!({
                    "data": {
                        "publishEntry": {
                            "logId": "0",
                            "seqNum": "2",
                            "backlink": "0020dda3b3977477e4c621ce124903a736e54b139afcb033e99677a6c8470b26514c",
                            "skiplink": null
                        }
                    }
                })
            );
        });
    }

    #[rstest]
    #[case::no_entry(
        "",
        &OPERATION_ENCODED,
        "Bytes to decode had length of 0"
    )]
    #[case::invalid_entry_bytes(
        "AB01",
        &OPERATION_ENCODED,
        "Could not decode author public key from bytes"
    )]
    #[case::invalid_entry_hex_encoding(
        "-/74='4,.=4-=235m-0   34.6-3",
        &OPERATION_ENCODED,
        "Failed to parse \"EntrySignedScalar\": Invalid character '-' at position 0"
    )]
    #[case::no_operation(
        &EncodedEntry::new(&ENTRY_ENCODED).to_string(),
        "".as_bytes(),
        "operation needs to match payload hash of encoded entry"
    )]
    #[case::invalid_operation_bytes(
        &EncodedEntry::new(&ENTRY_ENCODED).to_string(),
        "AB01".as_bytes(),
        "invalid type: bytes, expected array"
    )]
    #[case::invalid_operation_hex_encoding(
        &EncodedEntry::new(&ENTRY_ENCODED).to_string(),
        "0-25.-%5930n3544[{{{   @@@".as_bytes(),
        "invalid type: integer `-17`, expected array"
    )]
    #[case::operation_does_not_match(
        &EncodedEntry::new(&ENTRY_ENCODED).to_string(),
        &{encoded_operation(
            Some(
                operation_fields(
                    vec![("message", OperationValue::String("Mwahaha!".to_string()))]
                )
            ),
            None,
            test_schema().id().to_owned()
        ).into_bytes()},
        "operation needs to match payload hash of encoded entry"
    )]
    #[case::valid_entry_with_extra_hex_char_at_end(
        &{EncodedEntry::new(&ENTRY_ENCODED).to_string() + "A"},
        &OPERATION_ENCODED,
        "Failed to parse \"EntrySignedScalar\": Odd number of digits"
    )]
    #[case::valid_entry_with_extra_hex_char_at_start(
        &{"A".to_string() + &EncodedEntry::new(&ENTRY_ENCODED).to_string()},
        &OPERATION_ENCODED,
        "Failed to parse \"EntrySignedScalar\": Odd number of digits"
    )]
    #[case::should_not_have_skiplink(
        &entry_signed_encoded_unvalidated(
            1,
            0,
            None,
            Some(random_hash()),
            Some(EncodedOperation::new(&OPERATION_ENCODED)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Could not decode payload hash DecodeError"
    )]
    #[case::should_not_have_backlink(
        &entry_signed_encoded_unvalidated(
            1,
            0,
            Some(random_hash()),
            None,
            Some(EncodedOperation::new(&OPERATION_ENCODED)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Could not decode payload hash DecodeError"
    )]
    #[case::should_not_have_backlink_or_skiplink(
        &entry_signed_encoded_unvalidated(
            1,
            0,
            Some(HASH.parse().unwrap()),
            Some(HASH.parse().unwrap()),
            Some(EncodedOperation::new(&OPERATION_ENCODED)) ,
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Could not decode payload hash DecodeError"
    )]
    #[case::missing_backlink(
        &entry_signed_encoded_unvalidated(
            2,
            0,
            None,
            None,
            Some(EncodedOperation::new(&OPERATION_ENCODED)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Could not decode backlink yamf hash: DecodeError"
    )]
    #[case::missing_skiplink(
        &entry_signed_encoded_unvalidated(
            8,
            0,
            Some(random_hash()),
            None,
            Some(EncodedOperation::new(&OPERATION_ENCODED)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Could not decode backlink yamf hash: DecodeError"
    )]
    #[case::should_not_include_skiplink(
        &entry_signed_encoded_unvalidated(
            14,
            0,
            Some(HASH.parse().unwrap()),
            Some(HASH.parse().unwrap()),
            Some(EncodedOperation::new(&OPERATION_ENCODED)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Could not decode payload hash DecodeError"
    )]
    #[case::payload_hash_and_size_missing(
        &entry_signed_encoded_unvalidated(
            14,
            0,
            Some(random_hash()),
            Some(HASH.parse().unwrap()),
            None,
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Could not decode payload hash DecodeError"
    )]
    #[case::create_operation_with_previous_operations(
        &entry_signed_encoded_unvalidated(
            1,
            0,
            None,
            None,
            Some(EncodedOperation::new(&CREATE_OPERATION_WITH_PREVIOUS_OPS)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &CREATE_OPERATION_WITH_PREVIOUS_OPS,
        "invalid type: sequence, expected map"
    )]
    #[case::update_operation_no_previous_operations(
        &entry_signed_encoded_unvalidated(
            1,
            0,
            None,
            None,
            Some(EncodedOperation::new(&UPDATE_OPERATION_NO_PREVIOUS_OPS)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &UPDATE_OPERATION_NO_PREVIOUS_OPS,
        "invalid type: map, expected array"
    )]
    #[case::delete_operation_no_previous_operations(
        &entry_signed_encoded_unvalidated(
            1,
            0,
            None,
            None,
            Some(EncodedOperation::new(&DELETE_OPERATION_NO_PREVIOUS_OPS)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &DELETE_OPERATION_NO_PREVIOUS_OPS,
        "missing previous_operations for this operation action"
    )]
    fn validates_encoded_entry_and_operation_integrity(
        #[case] entry_encoded: &str,
        #[case] encoded_operation: &[u8],
        #[case] expected_error_message: &str,
        #[from(test_db)]
        #[with(0, 0, 0, false, test_schema())]
        runner: TestDatabaseRunner,
    ) {
        let entry_encoded = entry_encoded.to_string();
        let encoded_operation = hex::encode(encoded_operation.to_owned());
        let expected_error_message = expected_error_message.to_string();

        runner.with_db_teardown(move |db: TestDatabase| async move {
            let (tx, _rx) = broadcast::channel(16);
            let manager =
                GraphQLSchemaManager::new(db.store, tx, db.context.schema_provider.clone()).await;
            let context = HttpServiceContext::new(manager);
            let client = TestClient::new(build_server(context));

            let publish_entry_request = publish_entry_request(&entry_encoded, &encoded_operation);

            let response = client
                .post("/graphql")
                .json(&json!({
                  "query": publish_entry_request.query,
                  "variables": publish_entry_request.variables
                }
                ))
                .send()
                .await;

            let response = response.json::<serde_json::Value>().await;
            for error in response.get("errors").unwrap().as_array().unwrap() {
                assert_eq!(
                    error.get("message").unwrap().as_str().unwrap(),
                    expected_error_message
                )
            }
        });
    }

    #[rstest]
    #[case::backlink_and_skiplink_not_in_db(
        &entry_signed_encoded_unvalidated(
            8,
            1,
            Some(HASH.parse().unwrap()),
            Some(Hash::new_from_bytes(&vec![2, 3, 4])),
            Some(EncodedOperation::new(&OPERATION_ENCODED)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Entry's claimed seq num of 8 does not match expected seq num of 1 for given author and log"
    )]
    #[case::backlink_not_in_db(
        &entry_signed_encoded_unvalidated(
            11,
            0,
            Some(random_hash()),
            None,
            Some(EncodedOperation::new(&OPERATION_ENCODED)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "claimed hash does not match backlink entry"
    )]
    #[case::not_the_next_seq_num(
        &entry_signed_encoded_unvalidated(
            14,
            0,
            Some(random_hash()),
            None,
            Some(EncodedOperation::new(&OPERATION_ENCODED)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Entry's claimed seq num of 14 does not match expected seq num of 11 for given author and log"
    )]
    #[case::occupied_seq_num(
        &entry_signed_encoded_unvalidated(
            6,
            0,
            Some(random_hash()),
            None,
            Some(EncodedOperation::new(&OPERATION_ENCODED)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Entry's claimed seq num of 6 does not match expected seq num of 11 for given author and log"
    )]
    #[case::previous_operations_not_in_db(
        &entry_signed_encoded_unvalidated(
            1,
            1,
            None,
            None,
            Some(
                encoded_operation(
                    Some(
                        operation_fields(
                            vec![("message", OperationValue::String("Sausage".to_string()))]
                        )
                    ),
                    Some(HASH.parse().unwrap()),
                    test_schema().id().to_owned()
                )
            ),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &{encoded_operation(
                Some(
                    operation_fields(
                        vec![("message", OperationValue::String("Sausage".to_string()))]
                    )
                ),
                Some(HASH.parse().unwrap()),
                test_schema().id().to_owned()
            ).into_bytes()
        },
        "<Operation 496543> not found, could not determine document id"
    )]
    #[case::claimed_log_id_does_not_match_expected(
        &entry_signed_encoded_unvalidated(
            1,
            2,
            None,
            None,
            Some(EncodedOperation::new(&OPERATION_ENCODED)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Entry's claimed log id of 2 does not match expected next log id of 1 for given author"
    )]
    fn validation_of_entry_and_operation_values(
        #[case] entry_encoded: &str,
        #[case] encoded_operation: &[u8],
        #[case] expected_error_message: &str,
        #[from(test_db)]
        #[with(10, 1, 1, false, test_schema(), vec![("message", OperationValue::String("Hello!".to_string()))], vec![("message", OperationValue::String("Hello!".to_string()))])]
        runner: TestDatabaseRunner,
    ) {
        let entry_encoded = entry_encoded.to_string();
        let encoded_operation = hex::encode(encoded_operation.to_owned());
        let expected_error_message = expected_error_message.to_string();

        runner.with_db_teardown(move |db: TestDatabase| async move {
            let (tx, _rx) = broadcast::channel(16);
            let manager =
                GraphQLSchemaManager::new(db.store, tx, db.context.schema_provider.clone()).await;
            let context = HttpServiceContext::new(manager);
            let client = TestClient::new(build_server(context));

            let publish_entry_request = publish_entry_request(&entry_encoded, &encoded_operation);

            let response = client
                .post("/graphql")
                .json(&json!({
                  "query": publish_entry_request.query,
                  "variables": publish_entry_request.variables
                }
                ))
                .send()
                .await;

            let response = response.json::<serde_json::Value>().await;
            for error in response.get("errors").unwrap().as_array().unwrap() {
                assert_eq!(
                    error.get("message").unwrap().as_str().unwrap(),
                    expected_error_message
                )
            }
        });
    }

    #[rstest]
    fn publish_many_entries(
        #[from(test_db)]
        #[with(0, 0, 0, false, doggo_schema())]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let key_pairs = vec![KeyPair::new(), KeyPair::new()];
            let num_of_entries = 13;

            let (tx, _rx) = broadcast::channel(16);
            let manager =
                GraphQLSchemaManager::new(db.store.clone(), tx, db.context.schema_provider.clone())
                    .await;
            let context = HttpServiceContext::new(manager);
            let client = TestClient::new(build_server(context));

            for key_pair in &key_pairs {
                let mut document_id: Option<DocumentId> = None;
                let author = Author::from(key_pair.public_key());
                for index in 0..num_of_entries {
                    let document_view_id: Option<DocumentViewId> =
                        document_id.clone().map(|id| id.as_str().parse().unwrap());

                    let next_entry_args = next_args(&db.store, &author, document_view_id.as_ref())
                        .await
                        .unwrap();

                    let operation = if index == 0 {
                        create_operation(doggo_fields(), doggo_schema().id().to_owned())
                    } else if index == (num_of_entries - 1) {
                        delete_operation(
                            next_entry_args.backlink.clone().unwrap().into(),
                            doggo_schema().id().to_owned(),
                        )
                    } else {
                        update_operation(
                            doggo_fields(),
                            next_entry_args.backlink.clone().unwrap().into(),
                            doggo_schema().id().to_owned(),
                        )
                    };

                    let encoded_operation = encode_operation(&operation).unwrap();
                    let entry_encoded = sign_and_encode_entry(
                        &next_entry_args.log_id.into(),
                        &next_entry_args.seq_num.into(),
                        next_entry_args.skiplink.map(Hash::from).as_ref(),
                        next_entry_args.backlink.map(Hash::from).as_ref(),
                        &encoded_operation,
                        key_pair,
                    )
                    .unwrap();

                    if index == 0 {
                        document_id = Some(entry_encoded.hash().into());
                    }

                    // Prepare a publish entry request for each entry.
                    let publish_entry_request = publish_entry_request(
                        &entry_encoded.to_string(),
                        &encoded_operation.to_string(),
                    );

                    // Publish the entry.
                    let result = client
                        .post("/graphql")
                        .json(&json!({
                              "query": publish_entry_request.query,
                              "variables": publish_entry_request.variables
                            }
                        ))
                        .send()
                        .await;

                    assert!(result.status().is_success())
                }
            }
        });
    }

    #[rstest]
    fn duplicate_publishing_of_entries(
        #[from(test_db)]
        #[with(1, 1, 1, false, doggo_schema())]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let (tx, _rx) = broadcast::channel(16);
            let manager =
                GraphQLSchemaManager::new(db.store.clone(), tx, db.context.schema_provider.clone()).await;
            let context = HttpServiceContext::new(manager);
            let client = TestClient::new(build_server(context));

            // Get the one entry from the store.
            let entries = db
                .store
                .get_entries_by_schema(doggo_schema().id())
                .await
                .unwrap();
            let entry = entries.first().unwrap();
            let encoded_entry: EncodedEntry = entry.to_owned().into();

            // Prepare a publish entry request for the entry.
            let publish_entry_request = publish_entry_request(
                &encoded_entry.to_string(),
                &entry.payload().unwrap().to_string(),
            );

            // Publish the entry and parse response.
            let response = client
                .post("/graphql")
                .json(&json!({
                  "query": publish_entry_request.query,
                  "variables": publish_entry_request.variables
                }
                ))
                .send()
                .await;

            let response = response.json::<serde_json::Value>().await;

            for error in response.get("errors").unwrap().as_array().unwrap() {
                assert_eq!(error.get("message").unwrap(), "Entry's claimed seq num of 1 does not match expected seq num of 2 for given author and log")
            }
        });
    }

    //@TODO: Test for publishing to schema not supported by this node.
}
