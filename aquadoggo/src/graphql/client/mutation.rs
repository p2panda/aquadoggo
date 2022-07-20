// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Object, Result};
use p2panda_rs::entry::decode_entry;
use p2panda_rs::entry::EntrySigned;
use p2panda_rs::operation::Operation;
use p2panda_rs::operation::OperationEncoded;
use p2panda_rs::operation::OperationId;
use p2panda_rs::Validate;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::db::provider::SqlStorage;
use crate::domain::publish;
use crate::graphql::client::NextEntryArguments;
use crate::graphql::scalars;

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
        entry: scalars::EncodedEntry,
        #[graphql(
            name = "operation",
            desc = "p2panda operation representing the entry payload."
        )]
        operation: scalars::EncodedOperation,
    ) -> Result<NextEntryArguments> {
        let store = ctx.data::<SqlStorage>()?;
        let tx = ctx.data::<ServiceSender>()?;

        let entry_signed: EntrySigned = entry.into();
        let operation_encoded: OperationEncoded = operation.into();

        /////////////////////////////////////////////////////
        // VALIDATE ENTRY AND OPERATION INTERNAL INTEGRITY //
        /////////////////////////////////////////////////////

        //@TODO: This pre-publishing validation needs to be reviewed in detail. Some problems come up here
        // because we are not verifying the encoded operations against cddl yet. We should consider the
        // role and expectations of `VerifiedOperation` and `StorageEntry` as well (they perform validation
        // themselves bt are only constructed _after_ all other validation has taken place). Lot's to be
        // improved here in general I think. Nice to see it as a very seperate step before `publish` i think.

        // Validate the encoded entry
        entry_signed.validate()?;

        // Validate the encoded operation
        operation_encoded.validate()?;

        // Decode the entry with it's operation.
        //
        // @TODO: Without this `publish` fails
        decode_entry(&entry_signed, Some(&operation_encoded))?;

        // Also need to validate the decoded operation to catch internally invalid operations
        //
        // @TODO: Without this `publish` fails
        let operation = Operation::from(&operation_encoded);
        operation.validate()?;

        /////////////////////////////////////
        // PUBLISH THE ENTRY AND OPERATION //
        /////////////////////////////////////

        let next_args = publish(store, &entry_signed, &operation_encoded).await?;

        ////////////////////////////////////////
        // SEND THE OPERATION TO MATERIALIZER //
        ////////////////////////////////////////

        // Send new operation on service communication bus, this will arrive eventually at
        // the materializer service

        let operation_id: OperationId = entry_signed.hash().into();

        if tx.send(ServiceMessage::NewOperation(operation_id)).is_err() {
            // Silently fail here as we don't mind if there are no subscribers. We have
            // tests in other places to check if messages arrive.
        }

        Ok(next_args)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use async_graphql::{value, Request, Variables};
    use ciborium::cbor;
    use ciborium::value::Value;
    use once_cell::sync::Lazy;
    use p2panda_rs::document::{DocumentId, DocumentViewId};
    use p2panda_rs::entry::{sign_and_encode, Entry, EntrySigned, LogId, SeqNum};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{Operation, OperationEncoded, OperationValue};
    use p2panda_rs::storage_provider::traits::EntryStore;
    use p2panda_rs::test_utils::constants::{HASH, PRIVATE_KEY, SCHEMA_ID};
    use p2panda_rs::test_utils::fixtures::{
        create_operation, delete_operation, entry_signed_encoded, entry_signed_encoded_unvalidated,
        key_pair, operation, operation_encoded, operation_fields, random_hash, update_operation,
    };
    use rstest::{fixture, rstest};
    use serde_json::json;
    use tokio::sync::broadcast;

    use crate::bus::ServiceMessage;
    use crate::db::provider::SqlStorage;
    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
    use crate::domain::next_args;
    use crate::http::{build_server, HttpServiceContext};
    use crate::test_helpers::TestClient;

    fn to_hex(value: Value) -> String {
        let mut cbor_bytes = Vec::new();
        ciborium::ser::into_writer(&value, &mut cbor_bytes).unwrap();
        hex::encode(cbor_bytes)
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

    pub static ENTRY_ENCODED: Lazy<String> = Lazy::new(|| {
        entry_signed_encoded(
            Entry::new(
                &LogId::default(),
                Some(&Operation::from(
                    &OperationEncoded::new(&OPERATION_ENCODED).unwrap(),
                )),
                None,
                None,
                &SeqNum::default(),
            )
            .unwrap(),
            key_pair(PRIVATE_KEY),
        )
        .as_str()
        .to_string()
    });

    pub static OPERATION_ENCODED: Lazy<String> = Lazy::new(|| {
        to_hex(cbor!({
        "action" => "create",
        "schema" => "chat_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b",
        "version" => 1,
        "fields" => {
          "message" => {
            "type" => "str",
            "value" => "Ohh, my first message!"
          }
        }
      }).unwrap())
    });

    pub static CREATE_OPERATION_WITH_PREVIOUS_OPS: Lazy<String> = Lazy::new(|| {
        to_hex(cbor!({
            "action" => "create",
            "schema" => "chat_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b",
            "version" => 1,
            "previous_operations" => [
              "002065f74f6fd81eb1bae19eb0d8dce145faa6a56d7b4076d7fba4385410609b2bae"
            ],
            "fields" => {
              "message" => {
                "type" => "str",
                "value" => "Which I now update."
              }
            }
        })
        .unwrap())
    });

    pub static UPDATE_OPERATION_NO_PREVIOUS_OPS: Lazy<String> = Lazy::new(|| {
        to_hex(cbor!({
            "action" => "update",
            "schema" => "chat_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b",
            "version" => 1,
            "fields" => {
              "message" => {
                "type" => "str",
                "value" => "Ohh, my first message!"
              }
            }
        }).unwrap())
    });

    pub static DELETE_OPERATION_NO_PREVIOUS_OPS: Lazy<String> = Lazy::new(|| {
        to_hex(
        cbor!({
          "action" => "delete",
          "schema" => "chat_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b",
          "version" => 1
        })
        .unwrap(),
    )
    });

    #[fixture]
    fn publish_entry_request(
        #[default(&ENTRY_ENCODED)] entry_encoded: &str,
        #[default(&OPERATION_ENCODED)] operation_encoded: &str,
    ) -> Request {
        // Prepare GraphQL mutation publishing an entry
        let parameters = Variables::from_value(value!({
            "entry": entry_encoded,
            "operation": operation_encoded,
        }));

        Request::new(PUBLISH_ENTRY_QUERY).variables(parameters)
    }

    #[rstest]
    fn publish_entry(#[from(test_db)] runner: TestDatabaseRunner, publish_entry_request: Request) {
        runner.with_db_teardown(move |db: TestDatabase<SqlStorage>| async move {
            let (tx, _) = broadcast::channel(16);
            let context = HttpServiceContext::new(db.store, tx);
            let response = context.schema.execute(publish_entry_request).await;

            println!("{:#?}", response);

            assert_eq!(
                response.data,
                value!({
                    "publishEntry": {
                        "logId": "0",
                        "seqNum": "2",
                        "backlink": "0020c096422b3c865e5b85ec67a82d5c1d19de43d57c4a3d902ea62b90d96ad32fda",
                        "skiplink": null,
                    }
                })
            );
        });
    }

    #[rstest]
    fn sends_message_on_communication_bus(
        #[from(test_db)] runner: TestDatabaseRunner,
        publish_entry_request: Request,
    ) {
        runner.with_db_teardown(move |db: TestDatabase<SqlStorage>| async move {
            let (tx, mut rx) = broadcast::channel(16);
            let context = HttpServiceContext::new(db.store, tx);

            context.schema.execute(publish_entry_request).await;

            // Find out hash of test entry to determine operation id
            let entry_encoded = EntrySigned::new(&ENTRY_ENCODED).unwrap();

            // Expect receiver to receive sent message
            let message = rx.recv().await.unwrap();
            assert_eq!(
                message,
                ServiceMessage::NewOperation(entry_encoded.hash().into())
            );
        });
    }

    #[rstest]
    fn publish_entry_error_handling(#[from(test_db)] runner: TestDatabaseRunner) {
        runner.with_db_teardown(move |db: TestDatabase<SqlStorage>| async move {
            let (tx, _rx) = broadcast::channel(16);
            let context = HttpServiceContext::new(db.store, tx);

            let parameters = Variables::from_value(value!({
                "entry": ENTRY_ENCODED.to_string(),
                "operation": "".to_string()
            }));
            let request = Request::new(PUBLISH_ENTRY_QUERY).variables(parameters);
            let response = context.schema.execute(request).await;

            assert!(response.is_err());
            assert_eq!(
                "operation needs to match payload hash of encoded entry".to_string(),
                response.errors[0].to_string()
            );
        });
    }

    #[rstest]
    fn post_gql_mutation(
        #[from(test_db)] runner: TestDatabaseRunner,
        publish_entry_request: Request,
    ) {
        runner.with_db_teardown(move |db: TestDatabase<SqlStorage>| async move {
            let (tx, _rx) = broadcast::channel(16);
            let context = HttpServiceContext::new(db.store, tx);
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
                            "backlink": "0020c096422b3c865e5b85ec67a82d5c1d19de43d57c4a3d902ea62b90d96ad32fda",
                            "skiplink": null
                        }
                    }
                })
            );
        });
    }

    #[rstest]
    #[case::no_entry("", "", "Bytes to decode had length of 0")]
    #[case::invalid_entry_bytes("AB01", "", "Could not decode author public key from bytes")]
    #[case::invalid_entry_hex_encoding(
        "-/74='4,.=4-=235m-0   34.6-3",
        &OPERATION_ENCODED,
        "invalid hex encoding in entry"
    )]
    #[case::no_operation(
        &ENTRY_ENCODED,
        "",
        "operation needs to match payload hash of encoded entry"
    )]
    #[case::invalid_operation_bytes(
        &ENTRY_ENCODED,
        "AB01",
        "operation needs to match payload hash of encoded entry"
    )]
    #[case::invalid_operation_hex_encoding(
        &ENTRY_ENCODED,
        "0-25.-%5930n3544[{{{   @@@",
        "invalid hex encoding in operation"
    )]
    #[case::operation_does_not_match(
        &ENTRY_ENCODED,
        &{operation_encoded(
            Some(
                operation_fields(
                    vec![("silly", OperationValue::Text("Sausage".to_string()))]
                )
            ),
            None,
            None
        ).as_str().to_owned()},
        "operation needs to match payload hash of encoded entry"
    )]
    #[case::valid_entry_with_extra_hex_char_at_end(
        &{ENTRY_ENCODED.to_string() + "A"},
        &OPERATION_ENCODED,
        "invalid hex encoding in entry"
    )]
    #[case::valid_entry_with_extra_hex_char_at_start(
        &{"A".to_string() + &ENTRY_ENCODED},
        &OPERATION_ENCODED,
        "invalid hex encoding in entry"
    )]
    #[case::should_not_have_skiplink(
        &entry_signed_encoded_unvalidated(
            1,
            0,
            None,
            Some(random_hash()),
            Some(Operation::from(&OperationEncoded::new(&OPERATION_ENCODED).unwrap())),
            key_pair(PRIVATE_KEY)
        ),
        &OPERATION_ENCODED,
        "Could not decode payload hash DecodeError"
    )]
    #[case::should_not_have_backlink(
        &entry_signed_encoded_unvalidated(
            1,
            0,
            Some(random_hash()),
            None,
            Some(Operation::from(&OperationEncoded::new(&OPERATION_ENCODED).unwrap())),
            key_pair(PRIVATE_KEY)
        ),
        &OPERATION_ENCODED,
        "Could not decode payload hash DecodeError"
    )]
    #[case::should_not_have_backlink_or_skiplink(
        &entry_signed_encoded_unvalidated(
            1,
            0,
            Some(HASH.parse().unwrap()),
            Some(HASH.parse().unwrap()),
            Some(Operation::from(&OperationEncoded::new(&OPERATION_ENCODED).unwrap())) ,
            key_pair(PRIVATE_KEY)
        ),
        &OPERATION_ENCODED,
        "Could not decode payload hash DecodeError"
    )]
    #[case::missing_backlink(
        &entry_signed_encoded_unvalidated(
            2,
            0,
            None,
            None,
            Some(Operation::from(&OperationEncoded::new(&OPERATION_ENCODED).unwrap())),
            key_pair(PRIVATE_KEY)
        ),
        &OPERATION_ENCODED,
        "Could not decode backlink yamf hash: DecodeError"
    )]
    #[case::missing_skiplink(
        &entry_signed_encoded_unvalidated(
            8,
            0,
            Some(random_hash()),
            None,
            Some(Operation::from(&OperationEncoded::new(&OPERATION_ENCODED).unwrap())),
            key_pair(PRIVATE_KEY)
        ),
        &OPERATION_ENCODED,
        "Could not decode backlink yamf hash: DecodeError"
    )]
    #[case::should_not_include_skiplink(
        &entry_signed_encoded_unvalidated(
            14,
            0,
            Some(HASH.parse().unwrap()),
            Some(HASH.parse().unwrap()),
            Some(Operation::from(&OperationEncoded::new(&OPERATION_ENCODED).unwrap())),
            key_pair(PRIVATE_KEY)
        ),
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
        ),
        &OPERATION_ENCODED,
        "Could not decode payload hash DecodeError"
    )]
    #[case::create_operation_with_previous_operations(
        &entry_signed_encoded_unvalidated(
            1,
            0,
            None,
            None,
            Some(Operation::from(&OperationEncoded::new(&CREATE_OPERATION_WITH_PREVIOUS_OPS).unwrap())),
            key_pair(PRIVATE_KEY)
        ),
        &CREATE_OPERATION_WITH_PREVIOUS_OPS,
        "previous_operations field should be empty"
    )]
    #[case::update_operation_no_previous_operations(
        &entry_signed_encoded_unvalidated(
            1,
            0,
            None,
            None,
            Some(Operation::from(&OperationEncoded::new(&UPDATE_OPERATION_NO_PREVIOUS_OPS).unwrap())),
            key_pair(PRIVATE_KEY)
        ),
        &UPDATE_OPERATION_NO_PREVIOUS_OPS,
        "previous_operations field can not be empty"
    )]
    #[case::delete_operation_no_previous_operations(
        &entry_signed_encoded_unvalidated(
            1,
            0,
            None,
            None,
            Some(Operation::from(&OperationEncoded::new(&DELETE_OPERATION_NO_PREVIOUS_OPS).unwrap())),
            key_pair(PRIVATE_KEY)
        ),
        &DELETE_OPERATION_NO_PREVIOUS_OPS,
        "previous_operations field can not be empty"
    )]
    fn validates_encoded_entry_and_operation_integrity(
        #[case] entry_encoded: &str,
        #[case] operation_encoded: &str,
        #[case] expected_error_message: &str,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        let entry_encoded = entry_encoded.to_string();
        let operation_encoded = operation_encoded.to_string();
        let expected_error_message = expected_error_message.to_string();

        runner.with_db_teardown(move |db: TestDatabase<SqlStorage>| async move {
            let (tx, _rx) = broadcast::channel(16);
            let context = HttpServiceContext::new(db.store, tx);
            let client = TestClient::new(build_server(context));

            let publish_entry_request = publish_entry_request(&entry_encoded, &operation_encoded);

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
            Some(Hash::new_from_bytes(vec![2, 3, 4]).unwrap()),
            Some(Operation::from(&OperationEncoded::new(&OPERATION_ENCODED).unwrap())),
            key_pair(PRIVATE_KEY)
        ),
        &OPERATION_ENCODED,
        "Entry's claimed seq num of 8 does not match expected seq num of 1 for given author and log"
    )]
    #[case::backlink_not_in_db(
        &entry_signed_encoded_unvalidated(
            11,
            0,
            Some(random_hash()),
            None,
            Some(Operation::from(&OperationEncoded::new(&OPERATION_ENCODED).unwrap())),
            key_pair(PRIVATE_KEY)
        ),
        &OPERATION_ENCODED,
        "The backlink hash encoded in the entry does not match the lipmaa entry provided" //Think this error message is wrong
    )]
    #[case::not_the_next_seq_num(
        &entry_signed_encoded_unvalidated(
            14,
            0,
            Some(random_hash()),
            None,
            Some(Operation::from(&OperationEncoded::new(&OPERATION_ENCODED).unwrap())),
            key_pair(PRIVATE_KEY)
        ),
        &OPERATION_ENCODED,
        "Entry's claimed seq num of 14 does not match expected seq num of 11 for given author and log"
    )]
    #[case::occupied_seq_num(
        &entry_signed_encoded_unvalidated(
            6,
            0,
            Some(random_hash()),
            None,
            Some(Operation::from(&OperationEncoded::new(&OPERATION_ENCODED).unwrap())),
            key_pair(PRIVATE_KEY)
        ),
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
                operation(
                    Some(
                        operation_fields(
                            vec![("silly", OperationValue::Text("Sausage".to_string()))]
                        )
                    ),
                    Some(HASH.parse().unwrap()),
                    None
                )
            ),
            key_pair(PRIVATE_KEY)
        ),
        &{operation_encoded(
                Some(
                    operation_fields(
                        vec![("silly", OperationValue::Text("Sausage".to_string()))]
                    )
                ),
                Some(HASH.parse().unwrap()),
                None
            ).as_str().to_owned()
        },
        "<Operation 496543> not found, could not determine document id"
    )]
    #[case::claimed_log_id_does_not_match_expected(
        &entry_signed_encoded_unvalidated(
            1,
            2,
            None,
            None,
            Some(Operation::from(&OperationEncoded::new(&OPERATION_ENCODED).unwrap())),
            key_pair(PRIVATE_KEY)
        ),
        &OPERATION_ENCODED,
        "Entry's claimed log id of 2 does not match expected next log id of 1 for given author"
    )]
    fn validation_of_entry_and_operation_values(
        #[case] entry_encoded: &str,
        #[case] operation_encoded: &str,
        #[case] expected_error_message: &str,
        #[from(test_db)]
        #[with(10, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        let entry_encoded = entry_encoded.to_string();
        let operation_encoded = operation_encoded.to_string();
        let expected_error_message = expected_error_message.to_string();

        runner.with_db_teardown(move |db: TestDatabase<SqlStorage>| async move {
            let (tx, _rx) = broadcast::channel(16);
            let context = HttpServiceContext::new(db.store, tx);
            let client = TestClient::new(build_server(context));

            let publish_entry_request = publish_entry_request(&entry_encoded, &operation_encoded);

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
    fn publish_many_entries(#[from(test_db)] runner: TestDatabaseRunner) {
        runner.with_db_teardown(|db: TestDatabase<SqlStorage>| async move {
            let key_pairs = vec![KeyPair::new(), KeyPair::new()];
            let num_of_entries = 13;

            let (tx, _rx) = broadcast::channel(16);
            let context = HttpServiceContext::new(db.store.clone(), tx);
            let client = TestClient::new(build_server(context));

            for key_pair in &key_pairs {
                let mut document_id: Option<DocumentId> = None;
                let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
                for index in 0..num_of_entries {
                    let document_view_id: Option<DocumentViewId> =
                        document_id.clone().map(|id| id.as_str().parse().unwrap());

                    let next_entry_args = next_args(&db.store, &author, document_view_id.as_ref())
                        .await
                        .unwrap();

                    let operation = if index == 0 {
                        create_operation(&[("name", OperationValue::Text("Panda".to_string()))])
                    } else if index == (num_of_entries - 1) {
                        delete_operation(&next_entry_args.backlink.clone().unwrap().into())
                    } else {
                        update_operation(
                            &[("name", OperationValue::Text("🐼".to_string()))],
                            &next_entry_args.backlink.clone().unwrap().into(),
                        )
                    };

                    let entry = Entry::new(
                        &next_entry_args.log_id.into(),
                        Some(&operation),
                        next_entry_args.skiplink.map(Hash::from).as_ref(),
                        next_entry_args.backlink.map(Hash::from).as_ref(),
                        &next_entry_args.seq_num.into(),
                    )
                    .unwrap();

                    let entry_encoded = sign_and_encode(&entry, key_pair).unwrap();
                    let operation_encoded = OperationEncoded::try_from(&operation).unwrap();

                    if index == 0 {
                        document_id = Some(entry_encoded.hash().into());
                    }

                    // Prepare a publish entry request for each entry.
                    let publish_entry_request =
                        publish_entry_request(entry_encoded.as_str(), operation_encoded.as_str());

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
        #[with(1, 1, 1, false, SCHEMA_ID.parse().unwrap())]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|populated_db: TestDatabase<SqlStorage>| async move {
            let (tx, _rx) = broadcast::channel(16);
            let context = HttpServiceContext::new(populated_db.store.clone(), tx);
            let client = TestClient::new(build_server(context));

            // Get the one entry from the store.
            let entries = populated_db
                .store
                .get_entries_by_schema(&SCHEMA_ID.parse().unwrap())
                .await
                .unwrap();
            let entry = entries.first().unwrap();

            // Prepare a publish entry request for the entry.
            let publish_entry_request = publish_entry_request(
                entry.entry_signed().as_str(),
                entry.operation_encoded().unwrap().as_str(),
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
}
