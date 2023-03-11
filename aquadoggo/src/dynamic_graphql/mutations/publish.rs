// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::anyhow;
use dynamic_graphql::{Mutation, MutationFields, MutationRoot, Context, Result};
use p2panda_rs::api::publish;
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::operation::decode::decode_operation;
use p2panda_rs::operation::traits::Schematic;
use p2panda_rs::operation::{EncodedOperation, OperationId};

use crate::bus::{ServiceMessage, ServiceSender};
use crate::db::SqlStore;
use crate::dynamic_graphql::types::NextArguments;
use crate::dynamic_graphql::scalars::{EncodedOperationScalar, EncodedEntryScalar};
use crate::schema::SchemaProvider;

/// GraphQL mutatation root.
#[derive(MutationRoot, Default, Debug, Copy, Clone)]
pub struct MutationRoot;

/// GraphQL publish mutation.
#[derive(Mutation, Default, Debug, Copy, Clone)]
pub struct Publish(MutationRoot);

#[MutationFields]
impl Publish {
    /// Publish an entry using parameters obtained through `nextArgs` query.
    ///
    /// Returns arguments for publishing the next entry in the same log.
    async fn publish(
        ctx: &Context<'_>,
        // Signed and encoded entry to publish
        entry: EncodedEntryScalar,
        // p2panda operation representing the entry payload.
        operation: EncodedOperationScalar,
    ) -> Result<NextArguments> {
        let store = ctx.data::<SqlStore>()?;
        let tx = ctx.data::<ServiceSender>()?;
        let schema_provider = ctx.data::<SchemaProvider>()?;

        let encoded_entry: EncodedEntry = entry.into();
        let encoded_operation: EncodedOperation = operation.into();

        let operation = decode_operation(&encoded_operation)?;

        let schema = schema_provider
            .get(operation.schema_id())
            .await
            .ok_or_else(|| anyhow!("Schema not found"))?;

        /////////////////////////////////////
        // PUBLISH THE ENTRY AND OPERATION //
        /////////////////////////////////////

        let (backlink, skiplink, seq_num, log_id) = publish(
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

        Ok(NextArguments {
            log_id: log_id.into(),
            seq_num: seq_num.into(),
            backlink: backlink.map(|hash| hash.into()),
            skiplink: skiplink.map(|hash| hash.into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use async_graphql::{value, Request, Variables};
    use ciborium::cbor;
    use once_cell::sync::Lazy;
    use p2panda_rs::api::next_args;
    use p2panda_rs::document::{DocumentId, DocumentViewId};
    use p2panda_rs::entry::encode::sign_and_encode_entry;
    use p2panda_rs::entry::traits::AsEncodedEntry;
    use p2panda_rs::entry::EncodedEntry;
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::{KeyPair, PublicKey};
    use p2panda_rs::operation::encode::encode_operation;
    use p2panda_rs::operation::{EncodedOperation, OperationValue};
    use p2panda_rs::schema::{FieldType, Schema, SchemaId};
    use p2panda_rs::serde::serialize_value;
    use p2panda_rs::storage_provider::traits::EntryStore;
    use p2panda_rs::test_utils::constants::{HASH, PRIVATE_KEY};
    use p2panda_rs::test_utils::fixtures::{
        create_operation, delete_operation, encoded_entry, encoded_operation,
        entry_signed_encoded_unvalidated, key_pair, operation_fields, random_hash,
        update_operation,
    };
    use p2panda_rs::test_utils::memory_store::helpers::PopulateStoreConfig;
    use rstest::{fixture, rstest};
    use serde_json::json;
    use serial_test::serial;
    use tokio::sync::broadcast;

    use crate::bus::ServiceMessage;
    use crate::dynamic_graphql::GraphQLSchemaManager;
    use crate::http::HttpServiceContext;
    use crate::test_utils::{
        doggo_fields, doggo_schema, graphql_test_client, populate_and_materialize,
        populate_store_config, test_runner, TestNode,
    };

    // Schema used in some of the tests in this module, it only has one field so it's easy to
    // documents for it.
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

    // Query string for a publish request.
    const PUBLISH_QUERY: &str = r#"
        mutation TestPublish($entry: String!, $operation: String!) {
            publish(entry: $entry, operation: $operation) {
                logId,
                seqNum,
                backlink,
                skiplink
            }
        }"#;

    // Encoded entry bytes used in tests below.
    pub static ENTRY_ENCODED: Lazy<Vec<u8>> = Lazy::new(|| {
        encoded_entry(
            1,
            0,
            None,
            None,
            EncodedOperation::from_bytes(&OPERATION_ENCODED),
            key_pair(PRIVATE_KEY),
        )
        .into_bytes()
    });

    // Encoded CREATE operation.
    pub static OPERATION_ENCODED: Lazy<Vec<u8>> = Lazy::new(|| {
        serialize_value(cbor!([
            1, 0, "message_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b",
            {
                "message" => "Ohh, my first message!",
            },
        ]))
    });

    // Encoded CREATE operation which incorrectly includes a previous field.
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

    // Encoded UPDATE operation which incorrectly doesn't include a previous field.
    pub static UPDATE_OPERATION_NO_PREVIOUS_OPS: Lazy<Vec<u8>> = Lazy::new(|| {
        serialize_value(cbor!([
            1, 1, "message_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b",
            {
                "message" => "Ohh, my first message!",
            },
        ]))
    });

    // Encoded DELETE operation which incorrectly doesn't include a previous field.
    pub static DELETE_OPERATION_NO_PREVIOUS_OPS: Lazy<Vec<u8>> =
        Lazy::new(|| serialize_value(cbor!([1, 2, test_schema().id().to_string(),])));

    #[fixture]
    fn publish_request(
        #[default(&EncodedEntry::from_bytes(&ENTRY_ENCODED).to_string())] entry_encoded: &str,
        #[default(&EncodedOperation::from_bytes(&OPERATION_ENCODED).to_string())] encoded_operation: &str,
    ) -> Request {
        // Prepare GraphQL mutation publishing an entry
        let parameters = Variables::from_value(value!({
            "entry": entry_encoded,
            "operation": encoded_operation,
        }));

        Request::new(PUBLISH_QUERY).variables(parameters)
    }

    #[rstest]
    // Note: This and more tests in this file use the underlying static schema provider which is a
    // static mutable data store, accessible across all test runner threads in parallel mode. To
    // prevent overwriting data across threads we have to run this test in serial.
    //
    // Read more: https://users.rust-lang.org/t/static-mutables-in-tests/49321
    #[serial]
    fn publish_entry(
        #[from(populate_store_config)]
        #[with(0, 0, 0, false, test_schema())]
        config: PopulateStoreConfig,
        publish_request: Request,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Adds the test_schema to the store and schema provider.
            populate_and_materialize(&mut node, &config).await;

            let (tx, _rx) = broadcast::channel(120);
            let manager = GraphQLSchemaManager::new(
                node.context.store.clone(),
                tx,
                node.context.schema_provider.clone(),
            )
            .await;
            let context = HttpServiceContext::new(manager);

            let response = context.schema.execute(publish_request).await;

            assert_eq!(
                response.data,
                value!({
                    "publish": {
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
    #[serial] // See note above on why we execute this test in series
    fn sends_message_on_communication_bus(
        #[from(populate_store_config)]
        #[with(0, 0, 0, false, test_schema())]
        config: PopulateStoreConfig,
        publish_request: Request,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Adds the test_schema to the store and schema provider.
            populate_and_materialize(&mut node, &config).await;
            let (tx, mut rx) = broadcast::channel(120);
            let manager = GraphQLSchemaManager::new(
                node.context.store.clone(),
                tx,
                node.context.schema_provider.clone(),
            )
            .await;
            let context = HttpServiceContext::new(manager);

            context.schema.execute(publish_request).await;

            // Find out hash of test entry to determine operation id
            let entry_encoded = EncodedEntry::from_bytes(&ENTRY_ENCODED);

            // Expect receiver to receive sent message
            let message = rx.recv().await.unwrap();
            assert_eq!(
                message,
                ServiceMessage::NewOperation(entry_encoded.hash().into())
            );
        });
    }

    #[rstest]
    #[serial] // See note above on why we execute this test in series
    fn post_gql_mutation(
        #[from(populate_store_config)]
        #[with(0, 0, 0, false, test_schema())]
        config: PopulateStoreConfig,
        publish_request: Request,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Adds the test_schema to the store and schema provider.
            populate_and_materialize(&mut node, &config).await;

            // Init the test client.
            let client = graphql_test_client(&node).await;

            let response = client
                .post("/graphql")
                .json(&json!({
                  "query": publish_request.query,
                  "variables": publish_request.variables
                }
                ))
                .send()
                .await;

            assert_eq!(
                response.json::<serde_json::Value>().await,
                json!({
                    "data": {
                        "publish": {
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
    #[serial] // See note above on why we execute this test in series
    #[case::invalid_entry_bytes(
        "AB01",
        &OPERATION_ENCODED,
        "Could not decode author public key from bytes"
    )]
    #[case::invalid_entry_hex_encoding(
        "-/74='4,.=4-=235m-0   34.6-3",
        &OPERATION_ENCODED,
        "Invalid value for argument \"entry\": Failed to parse \"EntryEncoded\": Invalid character '-' at position 0"
    )]
    #[case::no_entry(
        "",
        &OPERATION_ENCODED,
        "Bytes to decode had length of 0"
    )]
    #[case::no_operation(
        &EncodedEntry::from_bytes(&ENTRY_ENCODED).to_string(),
        "".as_bytes(),
        "cbor decoder failed failed to fill whole buffer"
    )]
    #[case::invalid_operation_bytes(
        &EncodedEntry::from_bytes(&ENTRY_ENCODED).to_string(),
        "AB01".as_bytes(),
        "invalid type: bytes, expected array"
    )]
    #[case::invalid_operation_hex_encoding(
        &EncodedEntry::from_bytes(&ENTRY_ENCODED).to_string(),
        "0-25.-%5930n3544[{{{   @@@".as_bytes(),
        "invalid type: integer `-17`, expected array"
    )]
    #[case::operation_does_not_match(
        &EncodedEntry::from_bytes(&ENTRY_ENCODED).to_string(),
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
        &{EncodedEntry::from_bytes(&ENTRY_ENCODED).to_string() + "A"},
        &OPERATION_ENCODED,
        "Invalid value for argument \"entry\": Failed to parse \"EntryEncoded\": Odd number of digits"
    )]
    #[case::valid_entry_with_extra_hex_char_at_start(
        &{"A".to_string() + &EncodedEntry::from_bytes(&ENTRY_ENCODED).to_string()},
        &OPERATION_ENCODED,
        "Invalid value for argument \"entry\": Failed to parse \"EntryEncoded\": Odd number of digits"
    )]
    #[case::should_not_have_skiplink(
        &entry_signed_encoded_unvalidated(
            1,
            0,
            None,
            Some(random_hash()),
            Some(EncodedOperation::from_bytes(&OPERATION_ENCODED)),
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
            Some(EncodedOperation::from_bytes(&OPERATION_ENCODED)),
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
            Some(EncodedOperation::from_bytes(&OPERATION_ENCODED)) ,
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
            Some(EncodedOperation::from_bytes(&OPERATION_ENCODED)),
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
            Some(EncodedOperation::from_bytes(&OPERATION_ENCODED)),
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
            Some(EncodedOperation::from_bytes(&OPERATION_ENCODED)),
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
            Some(EncodedOperation::from_bytes(&CREATE_OPERATION_WITH_PREVIOUS_OPS)),
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
            Some(EncodedOperation::from_bytes(&UPDATE_OPERATION_NO_PREVIOUS_OPS)),
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
            Some(EncodedOperation::from_bytes(&DELETE_OPERATION_NO_PREVIOUS_OPS)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &DELETE_OPERATION_NO_PREVIOUS_OPS,
        "missing previous for this operation action"
    )]
    fn validates_encoded_entry_and_operation_integrity(
        #[case] entry_encoded: &str,
        #[case] encoded_operation: &[u8],
        #[case] expected_error_message: &str,
        #[from(populate_store_config)]
        #[with(0, 0, 0, false, test_schema())]
        config: PopulateStoreConfig,
    ) {
        // Test that encoded entries and operations are correctly validated when passed into
        // qraphql publish endpoint. This is a public facing method so we should expect any
        // junk data to arrive.

        // Encode the entry and operation as string values.
        let entry_encoded = entry_encoded.to_string();
        let encoded_operation = hex::encode(encoded_operation);
        let expected_error_message = expected_error_message.to_string();

        test_runner(|mut node: TestNode| async move {
            // Adds the test_schema to the store and schema provider.
            populate_and_materialize(&mut node, &config).await;

            // Init the test client.
            let client = graphql_test_client(&node).await;

            // Prepare the GQL publish request,
            let publish_request = publish_request(&entry_encoded, &encoded_operation);

            // Send the publish request.
            let response = client
                .post("/graphql")
                .json(&json!({
                  "query": publish_request.query,
                  "variables": publish_request.variables
                }
                ))
                .send()
                .await;

            // Parse the response and check any errors match the expected ones.
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
    #[serial] // See note above on why we execute this test in series
    #[case::backlink_and_skiplink_not_in_db(
        &entry_signed_encoded_unvalidated(
            8,
            1,
            Some(HASH.parse().unwrap()),
            Some(Hash::new_from_bytes(&vec![2, 3, 4])),
            Some(EncodedOperation::from_bytes(&OPERATION_ENCODED)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Entry's claimed seq num of 8 does not match expected seq num of 1 for given public key and log"
    )]
    #[case::backlink_not_in_db(
        &entry_signed_encoded_unvalidated(
            11,
            0,
            Some(random_hash()),
            None,
            Some(EncodedOperation::from_bytes(&OPERATION_ENCODED)),
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
            Some(EncodedOperation::from_bytes(&OPERATION_ENCODED)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Entry's claimed seq num of 14 does not match expected seq num of 11 for given public key and log"
    )]
    #[case::occupied_seq_num(
        &entry_signed_encoded_unvalidated(
            6,
            0,
            Some(random_hash()),
            None,
            Some(EncodedOperation::from_bytes(&OPERATION_ENCODED)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Entry's claimed seq num of 6 does not match expected seq num of 11 for given public key and log"
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
        "Operation 0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543 not found, could not determine document id"
    )]
    #[case::claimed_log_id_does_not_match_expected(
        &entry_signed_encoded_unvalidated(
            1,
            2,
            None,
            None,
            Some(EncodedOperation::from_bytes(&OPERATION_ENCODED)),
            key_pair(PRIVATE_KEY)
        ).to_string(),
        &OPERATION_ENCODED,
        "Entry's claimed log id of 2 does not match expected next log id of 1 for given public key"
    )]
    fn validation_of_entry_and_operation_values(
        #[case] entry_encoded: &str,
        #[case] encoded_operation: &[u8],
        #[case] expected_error_message: &str,
        #[from(populate_store_config)]
        #[with(10, 1, 1, false, test_schema(), vec![("message", OperationValue::String("Hello!".to_string()))], vec![("message", OperationValue::String("Hello!".to_string()))])]
        config: PopulateStoreConfig,
    ) {
        // Test that entries and operations passed into the qraphql publish endpoint adhere to the
        // p2panda specification in relation to each other and values stored in the target node's database. This is a
        // public facing method so we should expect any junk data to arrive.

        let entry_encoded = entry_encoded.to_string();
        let encoded_operation = hex::encode(encoded_operation.to_owned());
        let expected_error_message = expected_error_message.to_string();

        test_runner(|mut node: TestNode| async move {
            // Populates the node with entries, operations and schemas.
            populate_and_materialize(&mut node, &config).await;

            // Init the test client.
            let client = graphql_test_client(&node).await;

            let publish_request = publish_request(&entry_encoded, &encoded_operation);

            let response = client
                .post("/graphql")
                .json(&json!({
                  "query": publish_request.query,
                  "variables": publish_request.variables
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
    #[serial] // See note above on why we execute this test in series
    fn publish_many_entries(
        #[from(populate_store_config)]
        #[with(0, 0, 0, false, doggo_schema())]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Adds the test_schema to the store and schema provider.
            populate_and_materialize(&mut node, &config).await;

            // Init the test client.
            let client = graphql_test_client(&node).await;

            // Two key pairs representing two different authors
            let key_pairs = vec![KeyPair::new(), KeyPair::new()];
            // Each will publish 13 entries (unlucky for some!).
            let num_of_entries = 13;

            // Iterate over each key pair.
            for key_pair in &key_pairs {
                let mut document_id: Option<DocumentId> = None;
                let public_key = PublicKey::from(key_pair.public_key());

                // Iterate of the number of entries we want to publish.
                for index in 0..num_of_entries {
                    // Derive the document_view_id from the document id.
                    let document_view_id: Option<DocumentViewId> =
                        document_id.clone().map(|id| id.as_str().parse().unwrap());

                    // Get the next entry args for the document view id and public_key.
                    let (backlink, skiplink, seq_num, log_id) =
                        next_args(&node.context.store, &public_key, document_view_id.as_ref())
                            .await
                            .unwrap();

                    // Construct a CREATE, UPDATE or DELETE operation based on the iterator index.
                    let operation = if index == 0 {
                        create_operation(doggo_fields(), doggo_schema().id().to_owned())
                    } else if index == (num_of_entries - 1) {
                        delete_operation(
                            backlink.clone().unwrap().into(),
                            doggo_schema().id().to_owned(),
                        )
                    } else {
                        update_operation(
                            doggo_fields(),
                            backlink.clone().unwrap().into(),
                            doggo_schema().id().to_owned(),
                        )
                    };

                    // Encode the operation.
                    let encoded_operation = encode_operation(&operation).expect("Encode operation");

                    // Encode the entry.
                    let entry_encoded = sign_and_encode_entry(
                        &log_id,
                        &seq_num,
                        skiplink.as_ref(),
                        backlink.as_ref(),
                        &encoded_operation,
                        key_pair,
                    )
                    .expect("Encode entry");

                    if index == 0 {
                        // Set the document id based on the first entry in this log (index == 0)
                        document_id = Some(entry_encoded.hash().into());
                    }

                    // Prepare a publish entry request for each entry.
                    let publish_request =
                        publish_request(&entry_encoded.to_string(), &encoded_operation.to_string());

                    // Publish the entry.
                    let result = client
                        .post("/graphql")
                        .json(&json!({
                              "query": publish_request.query,
                              "variables": publish_request.variables
                            }
                        ))
                        .send()
                        .await;

                    // Every publish request should succeed.
                    assert!(result.status().is_success())
                }
            }
        });
    }

    #[rstest]
    #[serial] // See note above on why we execute this test in series
    fn duplicate_publishing_of_entries(
        #[from(populate_store_config)]
        #[with(1, 1, 1, false, doggo_schema())]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populates the node with entries, operations and schemas.
            populate_and_materialize(&mut node, &config).await;

            // Init the test client.
            let client = graphql_test_client(&node).await;

            // Get the one entry from the store.
            let entries = node
                .context
                .store
                .get_entries_by_schema(doggo_schema().id())
                .await
                .unwrap();
            let entry = entries.first().unwrap();

            // Prepare a publish entry request for the entry.
            let publish_request = publish_request(
                &entry.encoded_entry.to_string(),
                &entry.payload().unwrap().to_string(),
            );

            // Publish the entry and parse response.
            let response = client
                .post("/graphql")
                .json(&json!({
                  "query": publish_request.query,
                  "variables": publish_request.variables
                }
                ))
                .send()
                .await;

            let response = response.json::<serde_json::Value>().await;

            for error in response.get("errors").unwrap().as_array().unwrap() {
                assert_eq!(error.get("message").unwrap(), "Entry's claimed seq num of 1 does not match expected seq num of 2 for given public key and log")
            }
        });
    }

    #[rstest]
    #[serial] // See note above on why we execute this test in series
    fn publish_unsupported_schema(
        #[from(encoded_entry)] entry_with_unsupported_schema: EncodedEntry,
        #[from(encoded_operation)] operation_with_unsupported_schema: EncodedOperation,
    ) {
        test_runner(|node: TestNode| async move {
            // Init the test client.
            let client = graphql_test_client(&node).await;

            // Prepare a publish entry request for the entry.
            let publish_entry = publish_request(
                &entry_with_unsupported_schema.to_string(),
                &operation_with_unsupported_schema.to_string(),
            );

            // Publish the entry and parse response.
            let response = client
                .post("/graphql")
                .json(&json!({
                  "query": publish_entry.query,
                  "variables": publish_entry.variables
                }
                ))
                .send()
                .await;

            let response = response.json::<serde_json::Value>().await;

            for error in response.get("errors").unwrap().as_array().unwrap() {
                assert_eq!(error.get("message").unwrap(), "Schema not found")
            }
        });
    }
}
