// SPDX-License-Identifier: AGPL-3.0-or-later

use jsonrpc_v2::{Data, Params};
use p2panda_rs::storage_provider::traits::StorageProvider;

use crate::db::store::SqlStorage;
use crate::errors::StorageProviderResult;
use crate::rpc::{PublishEntryRequest, PublishEntryResponse};

/// Implementation of `panda_publishEntry` RPC method.
///
/// Stores an author's Bamboo entry with operation payload in database after validating it.
pub async fn publish_entry(
    storage_provider: Data<SqlStorage>,
    Params(params): Params<PublishEntryRequest>,
) -> StorageProviderResult<PublishEntryResponse> {
    let response = storage_provider.publish_entry(&params).await?;
    Ok(response)
}
#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::entry::{sign_and_encode, Entry, EntrySigned, LogId, SeqNum};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{Operation, OperationEncoded, OperationFields, OperationValue};
    use p2panda_rs::schema::SchemaId;

    use crate::server::{build_server, ApiState};
    use crate::test_helpers::{handle_http, initialize_db, rpc_request, rpc_response, TestClient};

    /// Create encoded entries and operations for testing.
    fn create_test_entry(
        key_pair: &KeyPair,
        schema: &SchemaId,
        log_id: &LogId,
        document: Option<&Hash>,
        skiplink: Option<&EntrySigned>,
        backlink: Option<&EntrySigned>,
        seq_num: &SeqNum,
    ) -> (EntrySigned, OperationEncoded) {
        // Create operation with dummy data
        let mut fields = OperationFields::new();
        fields
            .add("test", OperationValue::Text("Hello".to_owned()))
            .unwrap();
        let operation = match document {
            Some(_) => Operation::new_update(
                schema.clone(),
                vec![backlink.unwrap().hash().into()],
                fields,
            )
            .unwrap(),
            None => Operation::new_create(schema.clone(), fields).unwrap(),
        };

        // Encode operation
        let operation_encoded = OperationEncoded::try_from(&operation).unwrap();

        // Create, sign and encode entry
        let entry = Entry::new(
            log_id,
            Some(&operation),
            skiplink.map(|e| e.hash()).as_ref(),
            backlink.map(|e| e.hash()).as_ref(),
            seq_num,
        )
        .unwrap();
        let entry_encoded = sign_and_encode(&entry, key_pair).unwrap();

        (entry_encoded, operation_encoded)
    }

    /// Compare API response from publishing an encoded entry and operation to expected skiplink,
    /// log id and sequence number.
    async fn assert_request(
        client: &TestClient,
        entry_encoded: &EntrySigned,
        operation_encoded: &OperationEncoded,
        expect_skiplink: Option<&EntrySigned>,
        expect_log_id: &LogId,
        expect_seq_num: &SeqNum,
    ) {
        // Prepare request to API
        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "operationEncoded": "{}"
                }}"#,
                entry_encoded.as_str(),
                operation_encoded.as_str(),
            ),
        );

        // Prepare expected response result
        let skiplink_str = match expect_skiplink {
            Some(entry) => {
                format!("\"{}\"", entry.hash().as_str())
            }
            None => "null".to_owned(),
        };

        let response = rpc_response(&format!(
            r#"{{
                "entryHashBacklink": "{}",
                "entryHashSkiplink": {},
                "seqNum": "{}",
                "logId": "{}"
            }}"#,
            entry_encoded.hash().as_str(),
            skiplink_str,
            expect_seq_num.as_u64(),
            expect_log_id.as_u64(),
        ));

        assert_eq!(handle_http(&client, request).await, response);
    }

    #[tokio::test]
    async fn publish_entry() {
        // Create key pair for author
        let key_pair = KeyPair::new();

        // Prepare test database
        let pool = initialize_db().await;

        // Create tide server with endpoints
        let state = ApiState::new(pool.clone());
        let app = build_server(state);
        let client = TestClient::new(app);

        // Define schema and log id for entries
        let schema = SchemaId::new_application(
            "venue",
            &Hash::new_from_bytes(vec![1, 2, 3]).unwrap().into(),
        );
        let log_id = LogId::default();
        let seq_num_1 = SeqNum::new(1).unwrap();

        // Create a couple of entries in the same log and check for consistency. The little diagrams
        // show back links and skip links analogous to this diagram from the bamboo spec:
        // https://github.com/AljoschaMeyer/bamboo#links-and-entry-verification
        //
        // [1] --
        let (entry_1, operation_1) =
            create_test_entry(&key_pair, &schema, &log_id, None, None, None, &seq_num_1);
        assert_request(
            &client,
            &entry_1,
            &operation_1,
            None,
            &log_id,
            &SeqNum::new(2).unwrap(),
        )
        .await;

        // [1] <-- [2]
        let (entry_2, operation_2) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            None,
            Some(&entry_1),
            &SeqNum::new(2).unwrap(),
        );
        assert_request(
            &client,
            &entry_2,
            &operation_2,
            None,
            &log_id,
            &SeqNum::new(3).unwrap(),
        )
        .await;

        // [1] <-- [2] <-- [3]
        let (entry_3, operation_3) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            None,
            Some(&entry_2),
            &SeqNum::new(3).unwrap(),
        );
        assert_request(
            &client,
            &entry_3,
            &operation_3,
            Some(&entry_1),
            &log_id,
            &SeqNum::new(4).unwrap(),
        )
        .await;

        //  /------------------ [4]
        // [1] <-- [2] <-- [3]
        let (entry_4, operation_4) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            Some(&entry_1),
            Some(&entry_3),
            &SeqNum::new(4).unwrap(),
        );
        assert_request(
            &client,
            &entry_4,
            &operation_4,
            None,
            &log_id,
            &SeqNum::new(5).unwrap(),
        )
        .await;

        //  /------------------ [4]
        // [1] <-- [2] <-- [3]   \-- [5] --
        let (entry_5, operation_5) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            None,
            Some(&entry_4),
            &SeqNum::new(5).unwrap(),
        );
        assert_request(
            &client,
            &entry_5,
            &operation_5,
            None,
            &log_id,
            &SeqNum::new(6).unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn validate() {
        // Create key pair for author
        let key_pair = KeyPair::new();

        // Prepare test database
        let pool = initialize_db().await;

        // Create tide server with endpoints
        let state = ApiState::new(pool.clone());
        let app = build_server(state);
        let client = TestClient::new(app);

        // Define schema and log id for entries
        let schema = SchemaId::new_application(
            "venue",
            &Hash::new_from_bytes(vec![1, 2, 3]).unwrap().into(),
        );
        let log_id = LogId::new(1);
        let seq_num = SeqNum::new(1).unwrap();

        // Create two valid entries for testing
        let (entry_1, operation_1) =
            create_test_entry(&key_pair, &schema, &log_id, None, None, None, &seq_num);
        assert_request(
            &client,
            &entry_1,
            &operation_1,
            None,
            &log_id,
            &SeqNum::new(2).unwrap(),
        )
        .await;

        let (entry_2, operation_2) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            None,
            Some(&entry_1),
            &SeqNum::new(2).unwrap(),
        );
        assert_request(
            &client,
            &entry_2,
            &operation_2,
            None,
            &log_id,
            &SeqNum::new(3).unwrap(),
        )
        .await;

        // // Send invalid log id for a new document: The entries entry_1 and entry_2 are assigned to
        // // log 1, which makes log 2 the required log for the next new document.
        // let (entry_wrong_log_id, operation_wrong_log_id) = create_test_entry(
        //     &key_pair,
        //     &schema,
        //     &LogId::new(3),
        //     None,
        //     None,
        //     None,
        //     &SeqNum::new(1).unwrap(),
        // );

        // let request = rpc_request(
        //     "panda_publishEntry",
        //     &format!(
        //         r#"{{
        //             "entryEncoded": "{}",
        //             "operationEncoded": "{}"
        //         }}"#,
        //         entry_wrong_log_id.as_str(),
        //         operation_wrong_log_id.as_str(),
        //     ),
        // );

        // let response = rpc_error("Requested log id 3 does not match expected log id 2");
        // assert_eq!(handle_http(&client, request).await, response);

        // // Send invalid log id for an existing document: This entry is an update for the existing
        // // document in log 1, however, we are trying to publish it in log 3.
        // let (entry_wrong_log_id, operation_wrong_log_id) = create_test_entry(
        //     &key_pair,
        //     &schema,
        //     &LogId::new(3),
        //     Some(&entry_1.hash()),
        //     None,
        //     Some(&entry_1),
        //     &SeqNum::new(2).unwrap(),
        // );

        // let request = rpc_request(
        //     "panda_publishEntry",
        //     &format!(
        //         r#"{{
        //             "entryEncoded": "{}",
        //             "operationEncoded": "{}"
        //         }}"#,
        //         entry_wrong_log_id.as_str(),
        //         operation_wrong_log_id.as_str(),
        //     ),
        // );

        // let response = rpc_error("Requested log id 3 does not match expected log id 1");
        // assert_eq!(handle_http(&client, request).await, response);

        // // Send invalid backlink entry / hash
        // let (entry_wrong_hash, operation_wrong_hash) = create_test_entry(
        //     &key_pair,
        //     &schema,
        //     &log_id,
        //     Some(&entry_1.hash()),
        //     None,
        //     Some(&entry_1),
        //     &SeqNum::new(3).unwrap(),
        // );

        // let request = rpc_request(
        //     "panda_publishEntry",
        //     &format!(
        //         r#"{{
        //             "entryEncoded": "{}",
        //             "operationEncoded": "{}"
        //         }}"#,
        //         entry_wrong_hash.as_str(),
        //         operation_wrong_hash.as_str(),
        //     ),
        // );

        // let response = rpc_error(
        //     "The backlink hash encoded in the entry does not match the lipmaa entry provided",
        // );
        // assert_eq!(handle_http(&client, request).await, response);

        // // Send invalid sequence number
        // let (entry_wrong_seq_num, operation_wrong_seq_num) = create_test_entry(
        //     &key_pair,
        //     &schema,
        //     &log_id,
        //     Some(&entry_1.hash()),
        //     None,
        //     Some(&entry_2),
        //     &SeqNum::new(5).unwrap(),
        // );

        // let request = rpc_request(
        //     "panda_publishEntry",
        //     &format!(
        //         r#"{{
        //             "entryEncoded": "{}",
        //             "operationEncoded": "{}"
        //         }}"#,
        //         entry_wrong_seq_num.as_str(),
        //         operation_wrong_seq_num.as_str(),
        //     ),
        // );

        // let response = rpc_error("Could not find backlink entry in database");
        // assert_eq!(handle_http(&client, request).await, response);
    }
}
