// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Error, Object, Result};
use p2panda_rs::entry::EntrySigned;
use p2panda_rs::operation::{AsVerifiedOperation, OperationEncoded, VerifiedOperation};
use p2panda_rs::storage_provider::traits::{OperationStore, StorageProvider};

use crate::bus::{ServiceMessage, ServiceSender};
use crate::db::provider::SqlStorage;
use crate::graphql::client::{NextEntryArguments, PublishEntryRequest};

/// Mutations for use by p2panda clients.
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
        entry_param: String,
        #[graphql(
            name = "operation",
            desc = "p2panda operation representing the entry payload."
        )]
        operation_param: String,
    ) -> Result<NextEntryArguments> {
        let store = ctx.data::<SqlStorage>()?;
        let tx = ctx.data::<ServiceSender>()?;

        // Parse and validate parameters
        let args = PublishEntryRequest {
            entry: EntrySigned::new(&entry_param)?,
            operation: OperationEncoded::new(&operation_param)?,
        };

        // Validate and store entry in database
        // @TODO: Check all validation steps here for both entries and operations. Also, there is
        // probably overlap in what replication needs in terms of validation?
        let response = store.publish_entry(&args).await.map_err(Error::from)?;

        // Load related document from database
        // @TODO: We probably have this instance already inside of "publish_entry"?
        match store.get_document_by_entry(&args.entry.hash()).await? {
            Some(document_id) => {
                let verified_operation =
                    VerifiedOperation::new_from_entry(&args.entry, &args.operation)?;

                // Store operation in database
                // @TODO: This is not done by "publish_entry", maybe it needs to move there as
                // well?
                store
                    .insert_operation(&verified_operation, &document_id)
                    .await?;

                // Send new operation on service communication bus, this will arrive eventually at
                // the materializer service
                if tx
                    .send(ServiceMessage::NewOperation(
                        verified_operation.operation_id().to_owned(),
                    ))
                    .is_err()
                {
                    // Silently fail here as we don't mind if there are no subscribers. We have
                    // tests in other places to check if messages arrive.
                }

                Ok(response)
            }
            None => Err(Error::new("No related document found in database")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use async_graphql::{value, Request, Variables};
    use p2panda_rs::document::DocumentId;
    use p2panda_rs::entry::{sign_and_encode, Entry, EntrySigned};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{Operation, OperationEncoded, OperationValue};
    use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore, StorageProvider};
    use p2panda_rs::test_utils::constants::{DEFAULT_HASH, DEFAULT_PRIVATE_KEY, TEST_SCHEMA_ID};
    use p2panda_rs::test_utils::fixtures::{
        create_operation, delete_operation, entry_signed_encoded_unvalidated, key_pair, operation,
        operation_encoded, operation_fields, random_hash, update_operation,
    };
    use rstest::{fixture, rstest};
    use serde_json::json;
    use tokio::sync::broadcast;

    use crate::bus::ServiceMessage;
    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
    use crate::graphql::client::EntryArgsRequest;
    use crate::http::{build_server, HttpServiceContext};
    use crate::test_helpers::TestClient;

    const ENTRY_ENCODED: &str = "00bedabb435758855968b3e2de2aa1f653adfbb392fcf9cb2295a68b2eca3c\
                                 fb030101a200204b771d59d76e820cbae493682003e99b795e4e7c86a8d6b4\
                                 c9ad836dc4c9bf1d3970fb39f21542099ba2fbfd6ec5076152f26c02445c62\
                                 1b43a7e2898d203048ec9f35d8c2a1547f2b83da8e06cadd8a60bb45d3b500\
                                 451e63f7cccbcbd64d09";

    const OPERATION_ENCODED: &str = "a466616374696f6e6663726561746566736368656d61784a76656e7565\
                                     5f30303230633635353637616533376566656132393365333461396337\
                                     6431336638663262663233646264633362356337623961623436323933\
                                     31313163343866633738626776657273696f6e01666669656c6473a167\
                                     6d657373616765a26474797065637374726576616c7565764f68682c20\
                                     6d79206669727374206d65737361676521";

    const PUBLISH_ENTRY_QUERY: &str = r#"
        mutation TestPublishEntry($entry: String!, $operation: String!) {
            publishEntry(entry: $entry, operation: $operation) {
                logId,
                seqNum,
                backlink,
                skiplink
            }
        }"#;

    const UPDATE_OPERATION_NO_PREVIOUS_OPS: &str = "A466616374696F6E6675706461746566736368656D617849636861745F30303230633635353637616533376566656132393365333461396337643133663866326266323364626463336235633762396162343632393331313163343866633738626776657273696F6E01666669656C6473A1676D657373616765A26474797065637374726576616C7565764F68682C206D79206669727374206D65737361676521";

    const CREATE_OPERATION_WITH_PREVIOUS_OPS: &str = "A566616374696F6E6663726561746566736368656D617849636861745F30303230633635353637616533376566656132393365333461396337643133663866326266323364626463336235633762396162343632393331313163343866633738626776657273696F6E017370726576696F75735F6F7065726174696F6E738178443030323036356637346636666438316562316261653139656230643864636531343566616136613536643762343037366437666261343338353431303630396232626165666669656C6473A1676D657373616765A26474797065637374726576616C75657357686963682049206E6F77207570646174652E";

    const DELETE_OPERATION_NO_PREVIOUS_OPS: &str = "A366616374696F6E6664656C65746566736368656D617849636861745F30303230633635353637616533376566656132393365333461396337643133663866326266323364626463336235633762396162343632393331313163343866633738626776657273696F6E01";

    #[fixture]
    fn publish_entry_request(
        #[default(ENTRY_ENCODED)] entry_encoded: &str,
        #[default(OPERATION_ENCODED)] operation_encoded: &str,
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
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let (tx, _) = broadcast::channel(16);
            let context = HttpServiceContext::new(db.store, tx);
            let response = context.schema.execute(publish_entry_request).await;

            assert_eq!(
                response.data,
                value!({
                    "publishEntry": {
                        "logId": "1",
                        "seqNum": "2",
                        "backlink": "00201c221b573b1e0c67c5e2c624a93419774cdf46b3d62414c44a698df1237b1c16",
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
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let (tx, mut rx) = broadcast::channel(16);
            let context = HttpServiceContext::new(db.store, tx);

            context.schema.execute(publish_entry_request).await;

            // Find out hash of test entry to determine operation id
            let entry_encoded = EntrySigned::new(ENTRY_ENCODED).unwrap();

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
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let (tx, _rx) = broadcast::channel(16);
            let context = HttpServiceContext::new(db.store, tx);

            let parameters = Variables::from_value(value!({
                "entry": ENTRY_ENCODED,
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
        runner.with_db_teardown(move |db: TestDatabase| async move {
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
                            "logId": "1",
                            "seqNum": "2",
                            "backlink": "00201c221b573b1e0c67c5e2c624a93419774cdf46b3d62414c44a698df1237b1c16",
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
        OPERATION_ENCODED,
        "invalid hex encoding in entry"
    )]
    #[case::no_operation(
        ENTRY_ENCODED,
        "",
        "operation needs to match payload hash of encoded entry"
    )]
    #[case::invalid_operation_bytes(
        ENTRY_ENCODED,
        "AB01",
        "operation needs to match payload hash of encoded entry"
    )]
    #[case::invalid_operation_hex_encoding(
        ENTRY_ENCODED,
        "0-25.-%5930n3544[{{{   @@@",
        "invalid hex encoding in operation"
    )]
    #[case::operation_does_not_match(
        ENTRY_ENCODED,
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
        OPERATION_ENCODED,
        "invalid hex encoding in entry"
    )]
    #[case::valid_entry_with_extra_hex_char_at_start(
        &{"A".to_string() + ENTRY_ENCODED},
        OPERATION_ENCODED,
        "invalid hex encoding in entry"
    )]
    #[case::should_not_have_skiplink(
        &entry_signed_encoded_unvalidated(
            1,
            1,
            None,
            Some(random_hash()),
            Some(Operation::from(&OperationEncoded::new(OPERATION_ENCODED).unwrap())),
            key_pair(DEFAULT_PRIVATE_KEY)
        ),
        OPERATION_ENCODED,
        "Could not decode payload hash DecodeError"
    )]
    #[case::should_not_have_backlink(
        &entry_signed_encoded_unvalidated(
            1,
            1,
            Some(random_hash()),
            None,
            Some(Operation::from(&OperationEncoded::new(OPERATION_ENCODED).unwrap())),
            key_pair(DEFAULT_PRIVATE_KEY)
        ),
        OPERATION_ENCODED,
        "Could not decode payload hash DecodeError"
    )]
    #[case::should_not_have_backlink_or_skiplink(
        &entry_signed_encoded_unvalidated(
            1,
            1,
            Some(DEFAULT_HASH.parse().unwrap()),
            Some(DEFAULT_HASH.parse().unwrap()),
            Some(Operation::from(&OperationEncoded::new(OPERATION_ENCODED).unwrap())) ,
            key_pair(DEFAULT_PRIVATE_KEY)
        ),
        OPERATION_ENCODED,
        "Could not decode payload hash DecodeError"
    )]
    #[case::missing_backlink(
        &entry_signed_encoded_unvalidated(
            2,
            1,
            None,
            None,
            Some(Operation::from(&OperationEncoded::new(OPERATION_ENCODED).unwrap())),
            key_pair(DEFAULT_PRIVATE_KEY)
        ),
        OPERATION_ENCODED,
        "Could not decode backlink yamf hash: DecodeError"
    )]
    #[case::missing_skiplink(
        &entry_signed_encoded_unvalidated(
            8,
            1,
            Some(random_hash()),
            None,
            Some(Operation::from(&OperationEncoded::new(OPERATION_ENCODED).unwrap())),
            key_pair(DEFAULT_PRIVATE_KEY)
        ),
        OPERATION_ENCODED,
        "Could not decode backlink yamf hash: DecodeError"
    )]
    #[case::should_not_include_skiplink(
        &entry_signed_encoded_unvalidated(
            14,
            1,
            Some(DEFAULT_HASH.parse().unwrap()),
            Some(DEFAULT_HASH.parse().unwrap()),
            Some(Operation::from(&OperationEncoded::new(OPERATION_ENCODED).unwrap())),
            key_pair(DEFAULT_PRIVATE_KEY)
        ),
        OPERATION_ENCODED,
        "Could not decode payload hash DecodeError"
    )]
    #[case::payload_hash_and_size_missing(
        &entry_signed_encoded_unvalidated(
            14,
            1,
            Some(random_hash()),
            Some(DEFAULT_HASH.parse().unwrap()),
            None,
            key_pair(DEFAULT_PRIVATE_KEY)
        ),
        OPERATION_ENCODED,
        "Could not decode payload hash DecodeError"
    )]
    #[case::backlink_and_skiplink_not_in_db(
        &entry_signed_encoded_unvalidated(
            8,
            1,
            Some(DEFAULT_HASH.parse().unwrap()),
            Some(Hash::new_from_bytes(vec![2, 3, 4]).unwrap()),
            Some(Operation::from(&OperationEncoded::new(OPERATION_ENCODED).unwrap())),
            key_pair(DEFAULT_PRIVATE_KEY)
        ),
        OPERATION_ENCODED,
        "Could not find expected backlink in database for entry with id: <Hash f7c017>"
    )]
    #[case::backlink_not_in_db(
        &entry_signed_encoded_unvalidated(
            2,
            1,
            Some(DEFAULT_HASH.parse().unwrap()),
            None,
            Some(Operation::from(&OperationEncoded::new(OPERATION_ENCODED).unwrap())),
            key_pair(DEFAULT_PRIVATE_KEY)
        ),
        OPERATION_ENCODED,
        "Could not find expected backlink in database for entry with id: <Hash d3832b>"
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
                    Some(DEFAULT_HASH.parse().unwrap()),
                    None
                )
            ),
            key_pair(DEFAULT_PRIVATE_KEY)
        ),
        &{operation_encoded(
                Some(
                    operation_fields(
                        vec![("silly", OperationValue::Text("Sausage".to_string()))]
                    )
                ),
                Some(DEFAULT_HASH.parse().unwrap()),
                None
            ).as_str().to_owned()
        },
        "Could not find document for entry in database with id: <Hash f03236>"
    )]
    #[case::create_operation_with_previous_operations(
        &entry_signed_encoded_unvalidated(
            1,
            1,
            None,
            None,
            Some(Operation::from(&OperationEncoded::new(CREATE_OPERATION_WITH_PREVIOUS_OPS).unwrap())),
            key_pair(DEFAULT_PRIVATE_KEY)
        ),
        CREATE_OPERATION_WITH_PREVIOUS_OPS,
        "previous_operations field should be empty"
    )]
    #[case::update_operation_no_previous_operations(
        &entry_signed_encoded_unvalidated(
            1,
            1,
            None,
            None,
            Some(Operation::from(&OperationEncoded::new(UPDATE_OPERATION_NO_PREVIOUS_OPS).unwrap())),
            key_pair(DEFAULT_PRIVATE_KEY)
        ),
        UPDATE_OPERATION_NO_PREVIOUS_OPS,
        "previous_operations field can not be empty"
    )]
    #[case::delete_operation_no_previous_operations(
        &entry_signed_encoded_unvalidated(
            1,
            1,
            None,
            None,
            Some(Operation::from(&OperationEncoded::new(DELETE_OPERATION_NO_PREVIOUS_OPS).unwrap())),
            key_pair(DEFAULT_PRIVATE_KEY)
        ),
        DELETE_OPERATION_NO_PREVIOUS_OPS,
        "previous_operations field can not be empty"
    )]
    fn invalid_requests_fail(
        #[case] entry_encoded: &str,
        #[case] operation_encoded: &str,
        #[case] expected_error_message: &str,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        let entry_encoded = entry_encoded.to_string();
        let operation_encoded = operation_encoded.to_string();
        let expected_error_message = expected_error_message.to_string();

        runner.with_db_teardown(move |db: TestDatabase| async move {
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
        runner.with_db_teardown(|db: TestDatabase| async move {
            let key_pairs = vec![KeyPair::new(), KeyPair::new()];
            let num_of_entries = 100;

            let (tx, _rx) = broadcast::channel(16);
            let context = HttpServiceContext::new(db.store.clone(), tx);
            let client = TestClient::new(build_server(context));

            for key_pair in &key_pairs {
                let mut document: Option<DocumentId> = None;
                let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
                for index in 0..num_of_entries {
                    let next_entry_args = db
                        .store
                        .get_entry_args(&EntryArgsRequest {
                            public_key: author.clone(),
                            document_id: document.as_ref().cloned(),
                        })
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
                        document = Some(entry_encoded.hash().into());
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
        #[with(1, 1, 1, false, TEST_SCHEMA_ID.parse().unwrap())]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|populated_db: TestDatabase| async move {
            let (tx, _rx) = broadcast::channel(16);
            let context = HttpServiceContext::new(populated_db.store.clone(), tx);
            let client = TestClient::new(build_server(context));

            // Get the entries from the prepopulated store.
            let mut entries = populated_db
                .store
                .get_entries_by_schema(&TEST_SCHEMA_ID.parse().unwrap())
                .await
                .unwrap();

            // Sort them by seq_num.
            entries.sort_by_key(|entry| entry.seq_num().as_u64());

            let duplicate_entry = entries.first().unwrap();

            // Prepare a publish entry request for each entry.
            let publish_entry_request = publish_entry_request(
                duplicate_entry.entry_signed().as_str(),
                duplicate_entry.operation_encoded().unwrap().as_str(),
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

            // @TODO: This currently throws an internal SQL error to the API user, I think we'd
            // like a nicer error message here:
            // https://github.com/p2panda/aquadoggo/issues/159
            for error in response.get("errors").unwrap().as_array().unwrap() {
                assert!(error.get("message").is_some())
            }
        });
    }
}
