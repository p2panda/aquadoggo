// SPDX-License-Identifier: AGPL-3.0-or-later

use jsonrpc_v2::{Data, Params};
use p2panda_rs::Validate;
use p2panda_rs::entry::decode_entry;
use p2panda_rs::operation::Operation;

use crate::db::models::{Entry, Log};
use crate::errors::Result;
use crate::rpc::RpcApiState;
use crate::rpc::request::PublishEntryRequest;
use crate::rpc::response::PublishEntryResponse;

#[derive(thiserror::Error, Debug)]
#[allow(missing_copy_implementations)]
pub enum PublishEntryError {
    #[error("Could not find backlink entry in database")]
    BacklinkMissing,

    #[error("Could not find skiplink entry in database")]
    SkiplinkMissing,

    #[error("Requested log id {0} does not match expected log id {1}")]
    InvalidLogId(i64, i64),
}

/// Implementation of `panda_publishEntry` RPC method.
///
/// Stores an author's Bamboo entry with operation payload in database after validating it.
pub async fn publish_entry(
    data: Data<RpcApiState>,
    Params(params): Params<PublishEntryRequest>,
) -> Result<PublishEntryResponse> {
    // Validate request parameters
    params.entry_encoded.validate()?;
    params.operation_encoded.validate()?;

    // Get database connection pool
    let pool = data.pool.clone();

    // Handle error as this conversion validates operation hash
    let entry = decode_entry(&params.entry_encoded, Some(&params.operation_encoded))?;
    let operation = Operation::from(&params.operation_encoded);

    let author = params.entry_encoded.author();

    // A document is identified by either the hash of its `CREATE` operation or .. @TODO
    let document_hash = if operation.is_create() {
        params.entry_encoded.hash()
    } else {
        // @TODO: Get this from database instead, we can't trust that the author doesn't lie to us
        // about the id here
        operation.id().unwrap().to_owned()
    };

    // Determine expected log id for new entry: a `CREATE` entry is always stored in the next free
    // log.
    let document_log_id = Log::find_document_log_id(&pool, &author, Some(&document_hash)).await?;

    // Check if provided log id matches expected log id
    if &document_log_id != entry.log_id() {
        Err(PublishEntryError::InvalidLogId(
            entry.log_id().as_i64(),
            document_log_id.as_i64(),
        ))?;
    }

    // Get related bamboo backlink and skiplink entries
    let entry_backlink_bytes = if !entry.seq_num().is_first() {
        Entry::at_seq_num(
            &pool,
            &author,
            &entry.log_id(),
            &entry.seq_num_backlink().unwrap(),
        )
        .await?
        .map(|link| {
            Some(
                hex::decode(link.entry_bytes)
                    .expect("Backlink entry with invalid hex-encoding detected in database"),
            )
        })
        .ok_or(PublishEntryError::BacklinkMissing)
    } else {
        Ok(None)
    }?;

    let entry_skiplink_bytes = if !entry.seq_num().is_first() {
        Entry::at_seq_num(
            &pool,
            &author,
            &entry.log_id(),
            &entry.seq_num_skiplink().unwrap(),
        )
        .await?
        .map(|link| {
            Some(
                hex::decode(link.entry_bytes)
                    .expect("Skiplink entry with invalid hex-encoding detected in database"),
            )
        })
        .ok_or(PublishEntryError::SkiplinkMissing)
    } else {
        Ok(None)
    }?;

    // Verify bamboo entry integrity
    bamboo_rs_core_ed25519_yasmf::verify(
        &params.entry_encoded.to_bytes(),
        Some(&params.operation_encoded.to_bytes()),
        entry_skiplink_bytes.as_deref(),
        entry_backlink_bytes.as_deref(),
    )?;

    // Register log in database when a new document is created
    if operation.is_create() {
        Log::insert(
            &pool,
            &author,
            &params.entry_encoded.hash(),
            &operation.schema(),
            entry.log_id(),
        )
        .await?;
    }

    // Finally insert Entry in database
    Entry::insert(
        &pool,
        &author,
        &params.entry_encoded,
        &params.entry_encoded.hash(),
        &entry.log_id(),
        &params.operation_encoded,
        &params.operation_encoded.hash(),
        &entry.seq_num(),
    )
    .await?;

    // Already return arguments for next entry creation
    let mut entry_latest = Entry::latest(&pool, &author, &entry.log_id())
        .await?
        .expect("Database does not contain any entries");
    let entry_hash_skiplink = super::entry_args::determine_skiplink(pool, &entry_latest).await?;

    let next_seq_num = entry_latest.seq_num.next().unwrap();

    Ok(PublishEntryResponse {
        entry_hash_backlink: Some(params.entry_encoded.hash()),
        entry_hash_skiplink,
        seq_num: next_seq_num,
        log_id: entry.log_id().to_owned(),
    })
}
#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::entry::{sign_and_encode, Entry, EntrySigned, LogId, SeqNum};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{Operation, OperationEncoded, OperationFields, OperationValue};

    use crate::rpc::api::build_rpc_api_service;
    use crate::rpc::server::{build_rpc_server, RpcServer};
    use crate::test_helpers::{handle_http, initialize_db, rpc_error, rpc_request, rpc_response};

    /// Create encoded entries and operations for testing.
    fn create_test_entry(
        key_pair: &KeyPair,
        schema: &Hash,
        log_id: &LogId,
        instance: Option<&Hash>,
        skiplink: Option<&EntrySigned>,
        backlink: Option<&EntrySigned>,
        seq_num: &SeqNum,
    ) -> (EntrySigned, OperationEncoded) {
        // Create operation with dummy data
        let mut fields = OperationFields::new();
        fields
            .add("test", OperationValue::Text("Hello".to_owned()))
            .unwrap();
        let operation = match instance {
            Some(instance_id) => {
                Operation::new_update(schema.clone(), instance_id.clone(), fields).unwrap()
            }
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
        app: &RpcServer,
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
                "logId": {},
                "seqNum": {}
            }}"#,
            entry_encoded.hash().as_str(),
            skiplink_str,
            expect_log_id.as_i64(),
            expect_seq_num.as_i64(),
        ));

        assert_eq!(handle_http(&app, request).await, response);
    }

    #[async_std::test]
    async fn publish_entry() {
        // Create key pair for author
        let key_pair = KeyPair::new();

        // Prepare test database
        let pool = initialize_db().await;

        // Create tide server with endpoints
        let rpc_api = build_rpc_api_service(pool);
        let app = build_rpc_server(rpc_api);

        // Define schema and log id for entries
        let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();
        let log_id_1 = LogId::default();
        let seq_num_1 = SeqNum::new(1).unwrap();

        // Create a couple of entries in the same log and check for consistency. The little diagrams
        // show back links and skip links analogous to this diagram from the bamboo spec:
        // https://github.com/AljoschaMeyer/bamboo/blob/61415747af34bfcb8f40a47ae3b02136083d3276/README.md#links-and-entry-verification
        //
        // [1] --
        let (entry_1, operation_1) =
            create_test_entry(&key_pair, &schema, &log_id_1, None, None, None, &seq_num_1);
        assert_request(
            &app,
            &entry_1,
            &operation_1,
            None,
            &log_id_1,
            &SeqNum::new(2).unwrap(),
        )
        .await;

        // [1] <-- [2]
        let (entry_2, operation_2) = create_test_entry(
            &key_pair,
            &schema,
            &log_id_1,
            Some(&entry_1.hash()),
            None,
            Some(&entry_1),
            &SeqNum::new(2).unwrap(),
        );
        assert_request(
            &app,
            &entry_2,
            &operation_2,
            None,
            &log_id_1,
            &SeqNum::new(3).unwrap(),
        )
        .await;

        // [1] <-- [2] <-- [3]
        let (entry_3, operation_3) = create_test_entry(
            &key_pair,
            &schema,
            &log_id_1,
            Some(&entry_1.hash()),
            None,
            Some(&entry_2),
            &SeqNum::new(3).unwrap(),
        );
        assert_request(
            &app,
            &entry_3,
            &operation_3,
            Some(&entry_1),
            &log_id_1,
            &SeqNum::new(4).unwrap(),
        )
        .await;

        //  /------------------ [4]
        // [1] <-- [2] <-- [3]
        let (entry_4, operation_4) = create_test_entry(
            &key_pair,
            &schema,
            &log_id_1,
            Some(&entry_1.hash()),
            Some(&entry_1),
            Some(&entry_3),
            &SeqNum::new(4).unwrap(),
        );
        assert_request(
            &app,
            &entry_4,
            &operation_4,
            None,
            &log_id_1,
            &SeqNum::new(5).unwrap(),
        )
        .await;

        //  /------------------ [4]
        // [1] <-- [2] <-- [3]   \-- [5] --
        let (entry_5, operation_5) = create_test_entry(
            &key_pair,
            &schema,
            &log_id_1,
            Some(&entry_1.hash()),
            None,
            Some(&entry_4),
            &SeqNum::new(5).unwrap(),
        );
        assert_request(
            &app,
            &entry_5,
            &operation_5,
            None,
            &log_id_1,
            &SeqNum::new(6).unwrap(),
        )
        .await;
    }

    #[async_std::test]
    async fn validate() {
        // Create key pair for author
        let key_pair = KeyPair::new();

        // Prepare test database
        let pool = initialize_db().await;

        // Create tide server with endpoints
        let rpc_api = build_rpc_api_service(pool);
        let app = build_rpc_server(rpc_api);

        // Define schema and log id for entries
        let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();
        let log_id = LogId::new(1);
        let seq_num = SeqNum::new(1).unwrap();

        // Create two valid entries for testing
        let (entry_1, operation_1) =
            create_test_entry(&key_pair, &schema, &log_id, None, None, None, &seq_num);
        assert_request(
            &app,
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
            &app,
            &entry_2,
            &operation_2,
            None,
            &log_id,
            &SeqNum::new(3).unwrap(),
        )
        .await;

        // Send invalid log id for a new document: The entries entry_1 and entry_2 are assigned to
        // log 1, which makes log 2 the required log for the next new document.
        let (entry_wrong_log_id, operation_wrong_log_id) = create_test_entry(
            &key_pair,
            &schema,
            &LogId::new(3),
            None,
            None,
            None,
            &SeqNum::new(1).unwrap(),
        );

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "operationEncoded": "{}"
                }}"#,
                entry_wrong_log_id.as_str(),
                operation_wrong_log_id.as_str(),
            ),
        );

        let response = rpc_error("Requested log id 3 does not match expected log id 2");

        assert_eq!(handle_http(&app, request).await, response);

        // Send invalid log id for an existing document: This entry is an update for the existing
        // document in log 1, however, we are trying to publish it in log 3.
        let (entry_wrong_log_id, operation_wrong_log_id) = create_test_entry(
            &key_pair,
            &schema,
            &LogId::new(3),
            Some(&entry_2.hash()),
            None,
            None,
            &SeqNum::new(1).unwrap(),
        );

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "operationEncoded": "{}"
                }}"#,
                entry_wrong_log_id.as_str(),
                operation_wrong_log_id.as_str(),
            ),
        );

        let response = rpc_error("Requested log id 3 does not match expected log id 2");

        assert_eq!(handle_http(&app, request).await, response);

        // Send invalid backlink entry / hash
        let (entry_wrong_hash, operation_wrong_hash) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            Some(&entry_2),
            Some(&entry_1),
            &SeqNum::new(3).unwrap(),
        );

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "operationEncoded": "{}"
                }}"#,
                entry_wrong_hash.as_str(),
                operation_wrong_hash.as_str(),
            ),
        );

        let response = rpc_error(
            "The backlink hash encoded in the entry does not match the lipmaa entry provided",
        );

        assert_eq!(handle_http(&app, request).await, response);

        // Send invalid seq num
        let (entry_wrong_seq_num, operation_wrong_seq_num) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            None,
            Some(&entry_2),
            &SeqNum::new(5).unwrap(),
        );

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "operationEncoded": "{}"
                }}"#,
                entry_wrong_seq_num.as_str(),
                operation_wrong_seq_num.as_str(),
            ),
        );

        let response = rpc_error("Could not find backlink entry in database");

        assert_eq!(handle_http(&app, request).await, response);
    }
}
