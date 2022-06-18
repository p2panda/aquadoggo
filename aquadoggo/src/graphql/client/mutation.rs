// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Error, Object, Result};
use p2panda_rs::entry::EntrySigned;
use p2panda_rs::operation::{AsVerifiedOperation, OperationEncoded, VerifiedOperation};
use p2panda_rs::storage_provider::traits::{OperationStore, StorageProvider};

use crate::bus::{ServiceMessage, ServiceSender};
use crate::db::provider::SqlStorage;
use crate::graphql::client::{PublishEntryRequest, PublishEntryResponse};

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
        #[graphql(name = "entryEncoded", desc = "Encoded entry to publish")]
        entry_encoded_param: String,
        #[graphql(
            name = "operationEncoded",
            desc = "Encoded entry payload, which contains a p2panda operation matching the \
            provided encoded entry."
        )]
        operation_encoded_param: String,
    ) -> Result<PublishEntryResponse> {
        let store = ctx.data::<SqlStorage>()?;
        let tx = ctx.data::<ServiceSender>()?;

        // Parse and validate parameters
        let args = PublishEntryRequest {
            entry_encoded: EntrySigned::new(&entry_encoded_param)?,
            operation_encoded: OperationEncoded::new(&operation_encoded_param)?,
        };

        // Validate and store entry in database
        // @TODO: Check all validation steps here for both entries and operations. Also, there is
        // probably overlap in what replication needs in terms of validation?
        let response = store.publish_entry(&args).await.map_err(Error::from)?;

        // Load related document from database
        // @TODO: We probably have this instance already inside of "publish_entry"?
        match store
            .get_document_by_entry(&args.entry_encoded.hash())
            .await?
        {
            Some(document_id) => {
                let verified_operation = VerifiedOperation::new_from_entry(
                    &args.entry_encoded,
                    &args.operation_encoded,
                )?;

                // Store operation in database
                // @TODO: This is not done by "publish_entry", maybe it needs to move there as
                // well?
                store
                    .insert_operation(&verified_operation, &document_id)
                    .await?;

                // Send new operation on service communication bus, this will arrive eventually at
                // the materializer service
                tx.send(ServiceMessage::NewOperation(
                    verified_operation.operation_id().to_owned(),
                ))?;

                Ok(response)
            }
            None => Err(Error::new("No related document found in database")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use async_graphql::{from_value, value, Request, Value, Variables};
    use bamboo_rs_core_ed25519_yasmf::entry::is_lipmaa_required;
    use p2panda_rs::entry::{EntrySigned, LogId, SeqNum};
    use p2panda_rs::identity::Author;
    use p2panda_rs::operation::{Operation, OperationEncoded, OperationValue};
    use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore};
    use p2panda_rs::test_utils::constants::{DEFAULT_HASH, DEFAULT_PRIVATE_KEY, TEST_SCHEMA_ID};
    use p2panda_rs::test_utils::fixtures::{
        entry_signed_encoded_unvalidated, key_pair, operation, operation_encoded, operation_fields,
        random_hash,
    };
    use rstest::{fixture, rstest};
    use serde_json::json;
    use tokio::sync::broadcast;

    use crate::bus::ServiceMessage;
    use crate::db::stores::test_utils::{test_db, TestSqlStore};
    use crate::graphql::client::PublishEntryResponse;
    use crate::http::{build_server, HttpServiceContext};
    use crate::test_helpers::{initialize_store, TestClient};

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
        mutation TestPublishEntry($entryEncoded: String!, $operationEncoded: String!) {
            publishEntry(entryEncoded: $entryEncoded, operationEncoded: $operationEncoded) {
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
            "entryEncoded": entry_encoded,
            "operationEncoded": operation_encoded,
        }));

        Request::new(PUBLISH_ENTRY_QUERY).variables(parameters)
    }

    #[rstest]
    #[tokio::test]
    async fn publish_entry(publish_entry_request: Request) {
        let (tx, _rx) = broadcast::channel(16);
        let store = initialize_store().await;
        let context = HttpServiceContext::new(store, tx);

        let response = context.schema.execute(publish_entry_request).await;
        let received: PublishEntryResponse = match response.data {
            Value::Object(result_outer) => {
                from_value(result_outer.get("publishEntry").unwrap().to_owned()).unwrap()
            }
            _ => panic!("Expected return value to be an object"),
        };

        // The response should contain args for the next entry in the same log
        let expected = PublishEntryResponse {
            log_id: LogId::new(1),
            seq_num: SeqNum::new(2).unwrap(),
            backlink: Some(
                "00201c221b573b1e0c67c5e2c624a93419774cdf46b3d62414c44a698df1237b1c16"
                    .parse()
                    .unwrap(),
            ),
            skiplink: None,
        };
        assert_eq!(expected, received);
    }

    #[rstest]
    #[tokio::test]
    async fn sends_message_on_communication_bus(publish_entry_request: Request) {
        let (tx, mut rx) = broadcast::channel(16);
        let store = initialize_store().await;
        let context = HttpServiceContext::new(store, tx);

        context.schema.execute(publish_entry_request).await;

        // Find out hash of test entry to determine operation id
        let entry_encoded = EntrySigned::new(ENTRY_ENCODED).unwrap();

        // Expect receiver to receive sent message
        let message = rx.recv().await.unwrap();
        assert_eq!(
            message,
            ServiceMessage::NewOperation(entry_encoded.hash().into())
        );
    }

    #[tokio::test]
    async fn publish_entry_error_handling() {
        let (tx, _rx) = broadcast::channel(16);
        let store = initialize_store().await;
        let context = HttpServiceContext::new(store, tx);

        let parameters = Variables::from_value(value!({
            "entryEncoded": ENTRY_ENCODED,
            "operationEncoded": "".to_string()
        }));
        let request = Request::new(PUBLISH_ENTRY_QUERY).variables(parameters);
        let response = context.schema.execute(request).await;

        assert!(response.is_err());
        assert_eq!(
            "operation needs to match payload hash of encoded entry".to_string(),
            response.errors[0].to_string()
        );
    }

    #[rstest]
    #[tokio::test]
    async fn post_gql_mutation(publish_entry_request: Request) {
        let (tx, _rx) = broadcast::channel(16);
        let store = initialize_store().await;
        let context = HttpServiceContext::new(store, tx);
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
                        "logId":"1",
                        "seqNum":"2",
                        "backlink":"00201c221b573b1e0c67c5e2c624a93419774cdf46b3d62414c44a698df1237b1c16",
                        "skiplink":null
                    }
                }
            })
        );
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
        &{operation_encoded(Some(operation_fields(vec![("silly", OperationValue::Text("Sausage".to_string()))])), None, None).as_str().to_owned()},
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
                Some(Operation::from(&OperationEncoded::new(OPERATION_ENCODED).unwrap()))
,
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
                Some(Operation::from(&OperationEncoded::new(OPERATION_ENCODED).unwrap()))
,
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
        &entry_signed_encoded_unvalidated(8, 1, Some(DEFAULT_HASH.parse().unwrap()), Some(DEFAULT_HASH.parse().unwrap()), Some(Operation::from(&OperationEncoded::new(OPERATION_ENCODED).unwrap())), key_pair(DEFAULT_PRIVATE_KEY)),
        OPERATION_ENCODED,
        "Could not find expected backlink in database for entry with id: <Hash 1bbde6>"
    )]
    #[case::backlink_not_in_db(
        &entry_signed_encoded_unvalidated(2, 1, Some(DEFAULT_HASH.parse().unwrap()), None, Some(Operation::from(&OperationEncoded::new(OPERATION_ENCODED).unwrap())), key_pair(DEFAULT_PRIVATE_KEY)),
        OPERATION_ENCODED,
        "Could not find expected backlink in database for entry with id: <Hash d3832b>"
    )]
    #[case::previous_operations_not_in_db(
        &entry_signed_encoded_unvalidated(1, 1, None, None, Some(operation(Some(operation_fields(vec![("silly", OperationValue::Text("Sausage".to_string()))])), Some(DEFAULT_HASH.parse().unwrap()), None)), key_pair(DEFAULT_PRIVATE_KEY)),
        &{operation_encoded(Some(operation_fields(vec![("silly", OperationValue::Text("Sausage".to_string()))])), Some(DEFAULT_HASH.parse().unwrap()), None).as_str().to_owned()},
        "Could not find document for entry in database with id: <Hash f03236>"
    )]
    #[case::create_operation_with_previous_operations(
        &entry_signed_encoded_unvalidated(1, 1, None, None, Some(Operation::from(&OperationEncoded::new(CREATE_OPERATION_WITH_PREVIOUS_OPS).unwrap())), key_pair(DEFAULT_PRIVATE_KEY)),
        CREATE_OPERATION_WITH_PREVIOUS_OPS,
        "previous_operations field should be empty"
    )]
    #[case::update_operation_no_previous_operations(
        &entry_signed_encoded_unvalidated(1, 1, None, None, Some(Operation::from(&OperationEncoded::new(UPDATE_OPERATION_NO_PREVIOUS_OPS).unwrap())), key_pair(DEFAULT_PRIVATE_KEY)),
        UPDATE_OPERATION_NO_PREVIOUS_OPS,
        "previous_operations field can not be empty"
    )]
    #[case::delete_operation_no_previous_operations(
        &entry_signed_encoded_unvalidated(1, 1, None, None, Some(Operation::from(&OperationEncoded::new(DELETE_OPERATION_NO_PREVIOUS_OPS).unwrap())), key_pair(DEFAULT_PRIVATE_KEY)),
        DELETE_OPERATION_NO_PREVIOUS_OPS,
        "previous_operations field can not be empty"
    )]
    #[tokio::test]
    async fn invalid_requests_fail(
        #[case] entry_encoded: &str,
        #[case] operation_encoded: &str,
        #[case] expected_error_message: &str,
        #[future]
        #[from(test_db)]
        db: TestSqlStore,
    ) {
        let db = db.await;

        let (tx, _rx) = broadcast::channel(16);
        let context = HttpServiceContext::new(db.store, tx);
        let client = TestClient::new(build_server(context));

        let publish_entry_request = publish_entry_request(entry_encoded, operation_encoded);

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
    }

    #[rstest]
    #[tokio::test]
    async fn publish_many_entries(
        #[from(test_db)]
        #[future]
        #[with(100, 1, true, TEST_SCHEMA_ID.parse().unwrap())]
        db: TestSqlStore,
    ) {
        // test db populated with 100 entries.
        let populated_db = db.await;
        // Get the author.
        let author = Author::try_from(
            populated_db
                .key_pairs
                .first()
                .unwrap()
                .public_key()
                .to_owned(),
        )
        .unwrap();

        // Setup the server and client with a new empty store.
        let (tx, _rx) = broadcast::channel(16);
        let store = initialize_store().await;
        let context = HttpServiceContext::new(store, tx);
        let client = TestClient::new(build_server(context));

        // Get the entries from the prepopulated store.
        let mut entries = populated_db
            .store
            .get_entries_by_schema(&TEST_SCHEMA_ID.parse().unwrap())
            .await
            .unwrap();

        // Sort them by seq_num.
        entries.sort_by_key(|entry| entry.seq_num().as_u64());

        for entry in entries {
            // Prepare a publish entry request for each entry.
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
            let publish_entry_response = response.get("data").unwrap().get("publishEntry").unwrap();

            // Calculate the skiplink we expect in the repsonse.
            let next_seq_num = entry.seq_num().next().unwrap();
            let skiplink_seq_num = next_seq_num.skiplink_seq_num();
            let skiplink_entry = match skiplink_seq_num {
                Some(seq_num) if is_lipmaa_required(next_seq_num.as_u64()) => populated_db
                    .store
                    .get_entry_at_seq_num(&author, &entry.log_id(), &seq_num)
                    .await
                    .unwrap()
                    .map(|entry| entry.hash().as_str().to_owned()),
                _ => None,
            };

            // Assert the returned log_id, seq_num, backlink and skiplink match our expectations.
            assert_eq!(
                publish_entry_response
                    .get("logId")
                    .unwrap()
                    .as_str()
                    .unwrap(),
                "1"
            );
            assert_eq!(
                publish_entry_response
                    .get("seqNum")
                    .unwrap()
                    .as_str()
                    .unwrap(),
                next_seq_num.as_u64().to_string()
            );
            assert_eq!(
                publish_entry_response
                    .get("skiplink")
                    .unwrap()
                    .as_str()
                    .map(|hash| hash.to_string()),
                skiplink_entry
            );
            assert_eq!(
                publish_entry_response
                    .get("backlink")
                    .unwrap()
                    .as_str()
                    .unwrap(),
                entry.hash().as_str()
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn duplicate_publishing_of_entries(
        #[from(test_db)]
        #[future]
        #[with(1, 1, false, TEST_SCHEMA_ID.parse().unwrap())]
        db: TestSqlStore,
    ) {
        let populated_db = db.await;

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

        // TODO: I think we'd like a nicer error message here.
        for error in response.get("errors").unwrap().as_array().unwrap() {
            assert_eq!(error.get("message").unwrap().as_str().unwrap(), "Error occured during `LogStorage` request in storage provider: error returned from database: UNIQUE constraint failed: logs.author, logs.log_id")
        }
    }
}
