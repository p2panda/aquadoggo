// SPDX-License-Identifier: AGPL-3.0-or-later

use jsonrpc_v2::{Data, Params};
use p2panda_rs::entry::decode_entry;
use p2panda_rs::operation::{AsOperation, Operation};
use p2panda_rs::Validate;

use crate::db::models::{Entry, Log};
use crate::errors::Result;
use crate::materialisation::materialise;
use crate::rpc::request::PublishEntryRequest;
use crate::rpc::response::PublishEntryResponse;
use crate::rpc::RpcApiState;

#[derive(thiserror::Error, Debug)]
#[allow(missing_copy_implementations)]
pub enum PublishEntryError {
    #[error("Could not find backlink entry in database")]
    BacklinkMissing,

    #[error("Could not find skiplink entry in database")]
    SkiplinkMissing,

    #[error("Could not find document hash for entry in database")]
    DocumentMissing,

    #[error("UPDATE & DELETE operation must have at previous operations")]
    PreviousOperationsMissing,

    #[error("UPDATE & DELETE operation must have at least 1 previous operation")]
    PreviousOperationsLengthZero,

    #[error("The provided schema does not match this documents schema")]
    InvalidSchema,

    #[error("Requested log id {0} does not match expected log id {1}")]
    InvalidLogId(u64, u64),
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

    // Decode author, entry and operation. This conversion validates the operation hash
    let author = params.entry_encoded.author();
    let entry = decode_entry(&params.entry_encoded, Some(&params.operation_encoded))?;
    let operation = Operation::from(&params.operation_encoded);

    // Every operation refers to a document we need to determine. A document is identified by the
    // hash of its first `CREATE` operation, it is the root operation of every document graph.
    let document_id = if operation.is_create() {
        // This is easy: We just use the entry hash directly to determine the document id.
        params.entry_encoded.hash()
    } else {
        // For any other operations which followed after creation we need to either walk the operation
        // graph back to its `CREATE` operation or more easily look up the database since we keep track
        // of all log ids and documents there.
        //
        // We can determine the used document hash by looking at what we know about the log which contains the
        // previous_operation which this operation refers to.

        let previous_operations = match operation.previous_operations() {
            Some(ops) if !ops.is_empty() => Ok(ops),
            Some(_) => Err(PublishEntryError::PreviousOperationsLengthZero),
            None => Err(PublishEntryError::PreviousOperationsMissing),
        }?;

        // Here we determine the document id using the first graph tip this operation
        // refers to in it's previous_operations.
        let document_id = Log::get_document_by_entry(&pool, &previous_operations[0])
            .await?
            .ok_or(PublishEntryError::DocumentMissing)?;

        // Get the expected schema hash for this document
        let schema = Log::get_schema_by_document(&pool, &document_id).await?;

        // Check if provided schema matches the expected document schema
        if schema.unwrap() != operation.schema() {
            return Err(PublishEntryError::InvalidSchema.into());
        };

        document_id
    };

    // Determine expected log id for new entry
    let document_log_id = Log::find_document_log_id(&pool, &author, Some(&document_id)).await?;

    // Check if provided log id matches expected log id
    if &document_log_id != entry.log_id() {
        return Err(PublishEntryError::InvalidLogId(
            entry.log_id().as_u64(),
            document_log_id.as_u64(),
        )
        .into());
    }

    // Get related bamboo backlink and skiplink entries
    let entry_backlink_bytes = if !entry.seq_num().is_first() {
        Entry::at_seq_num(
            &pool,
            &author,
            entry.log_id(),
            &entry.seq_num_backlink().unwrap(),
        )
        .await?
        .map(|link| {
            let bytes = hex::decode(link.entry_bytes)
                .expect("Backlink entry with invalid hex-encoding detected in database");
            Some(bytes)
        })
        .ok_or(PublishEntryError::BacklinkMissing)
    } else {
        Ok(None)
    }?;

    let entry_skiplink_bytes = if !entry.seq_num().is_first() {
        Entry::at_seq_num(
            &pool,
            &author,
            entry.log_id(),
            &entry.seq_num_skiplink().unwrap(),
        )
        .await?
        .map(|link| {
            let bytes = hex::decode(link.entry_bytes)
                .expect("Backlink entry with invalid hex-encoding detected in database");
            Some(bytes)
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

    // Register log in database when entry is first item in a log.
    if entry.seq_num().is_first() {
        Log::insert(
            &pool,
            &author,
            &document_id,
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
        entry.log_id(),
        &params.operation_encoded,
        &params.operation_encoded.hash(),
        entry.seq_num(),
    )
    .await?;

    materialise(&pool, &document_id).await?;

    // Already return arguments for next entry creation
    //
    // @TODO: Here we already want to return the graph tips/previous operations for this document.
    let mut entry_latest = Entry::latest(&pool, &author, entry.log_id())
        .await?
        .expect("Database does not contain any entries");
    let entry_hash_skiplink = super::entry_args::determine_skiplink(pool, &entry_latest).await?;
    let next_seq_num = entry_latest.seq_num.next().unwrap();

    Ok(PublishEntryResponse {
        entry_hash_backlink: Some(params.entry_encoded.hash()),
        entry_hash_skiplink,
        seq_num: next_seq_num.as_u64().to_string(),
        log_id: entry.log_id().as_u64().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::entry::{sign_and_encode, Entry, EntrySigned, LogId, SeqNum};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{Operation, OperationEncoded, OperationFields, OperationValue};
    use p2panda_rs::test_utils::mocks::logs::LogEntry;
    use p2panda_rs::test_utils::mocks::{send_to_node, Client, Node};
    use p2panda_rs::test_utils::utils::{create_operation, operation_fields, update_operation};

    use crate::rpc::build_rpc_api_service;
    use crate::server::{build_server, ApiServer, ApiState};
    use crate::test_helpers::{handle_http, initialize_db, rpc_error, rpc_request, rpc_response};

    /// Get an entry from an array by it's hash.
    /// Helper method for retrieving entries from the mock node db.
    fn get_entry_by_hash(node: &Node, entry_hash: &Hash) -> LogEntry {
        node.all_entries()
            .iter()
            .find(|entry| entry.hash() == entry_hash.clone())
            .unwrap()
            .clone()
    }

    /// Create encoded entries and operations for testing.
    fn create_test_entry(
        key_pair: &KeyPair,
        schema: &Hash,
        log_id: &LogId,
        document: Option<&Hash>,
        skiplink: Option<&EntrySigned>,
        backlink: Option<&EntrySigned>,
        previous_operations: Option<&[Hash]>,
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
                previous_operations.unwrap().to_owned(),
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
        app: &ApiServer,
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
                "logId": "{}",
                "seqNum": "{}"
            }}"#,
            entry_encoded.hash().as_str(),
            skiplink_str,
            expect_log_id.as_u64(),
            expect_seq_num.as_u64(),
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
        let state = ApiState::new(pool.clone());
        let app = build_server(state);

        // Define schema and log id for entries
        let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();
        let log_id = LogId::default();
        let seq_num_1 = SeqNum::new(1).unwrap();

        // Create a couple of entries in the same log and check for consistency. The little diagrams
        // show back links and skip links analogous to this diagram from the bamboo spec:
        // https://github.com/AljoschaMeyer/bamboo#links-and-entry-verification
        //
        // [1] --
        let (entry_1, operation_1) = create_test_entry(
            &key_pair, &schema, &log_id, None, None, None, None, &seq_num_1,
        );
        assert_request(
            &app,
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
            Some(&[entry_1.hash()]),
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

        // [1] <-- [2] <-- [3]
        let (entry_3, operation_3) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            None,
            Some(&entry_2),
            Some(&[entry_2.hash()]),
            &SeqNum::new(3).unwrap(),
        );
        assert_request(
            &app,
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
            Some(&[entry_3.hash()]),
            &SeqNum::new(4).unwrap(),
        );
        assert_request(
            &app,
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
            Some(&[entry_4.hash()]),
            &SeqNum::new(5).unwrap(),
        );
        assert_request(
            &app,
            &entry_5,
            &operation_5,
            None,
            &log_id,
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
        let state = ApiState::new(pool.clone());
        let app = build_server(state);

        // Define schema and log id for entries
        let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();
        let log_id = LogId::new(1);
        let seq_num = SeqNum::new(1).unwrap();

        // Create two valid entries for testing
        let (entry_1, operation_1) = create_test_entry(
            &key_pair, &schema, &log_id, None, None, None, None, &seq_num,
        );
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
            Some(&[entry_1.hash()]),
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
            Some(&entry_1.hash()),
            None,
            Some(&entry_1),
            Some(&[entry_1.hash()]),
            &SeqNum::new(2).unwrap(),
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

        let response = rpc_error("Requested log id 3 does not match expected log id 1");
        assert_eq!(handle_http(&app, request).await, response);

        // Send invalid backlink entry / hash
        let (entry_wrong_hash, operation_wrong_hash) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            None,
            Some(&entry_1),
            Some(&[entry_1.hash()]),
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

        // Send invalid sequence number
        let (entry_wrong_seq_num, operation_wrong_seq_num) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            None,
            Some(&entry_2),
            Some(&[entry_2.hash()]),
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

    #[async_std::test]
    async fn validate_publish_multiwriter() {
        // Prepare test database
        let pool = initialize_db().await;

        // Create tide server with endpoints
        let state = ApiState::new(pool.clone());
        let app = build_server(state);

        // Create dummy node, this will be used for creating entries.
        let mut node = Node::new();

        // Create some clients and a schema.
        let panda = Client::new("panda".to_string(), KeyPair::new());
        let penguin = Client::new("penguin".to_string(), KeyPair::new());
        let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();

        // Panda publishes a create operation.
        // This instantiates a new document.
        //
        // PANDA  : [1]
        // PENGUIN:
        let (panda_entry_1_hash, _) = send_to_node(
            &mut node,
            &panda,
            &create_operation(
                schema.clone(),
                operation_fields(vec![(
                    "name",
                    OperationValue::Text("Panda Cafe".to_string()),
                )]),
            ),
        )
        .unwrap();

        let panda_entry_1 = get_entry_by_hash(&node, &panda_entry_1_hash);

        assert_request(
            &app,
            &panda_entry_1.entry_encoded(),
            &panda_entry_1.operation_encoded(),
            None,
            &LogId::new(1),
            &SeqNum::new(2).unwrap(),
        )
        .await;

        // Panda publishes an update operation.
        // It contains the hash of the current graph tip in it's `previous_operations`.
        //
        // PANDA  : [1] <-- [2]
        // PENGUIN:
        let (panda_entry_2_hash, _) = send_to_node(
            &mut node,
            &panda,
            &update_operation(
                schema.clone(),
                vec![panda_entry_1_hash.clone()],
                operation_fields(vec![(
                    "name",
                    OperationValue::Text("Panda Cafe!".to_string()),
                )]),
            ),
        )
        .unwrap();

        let panda_entry_2 = get_entry_by_hash(&node, &panda_entry_2_hash);

        assert_request(
            &app,
            &panda_entry_2.entry_encoded(),
            &panda_entry_2.operation_encoded(),
            None,
            &LogId::new(1),
            &SeqNum::new(3).unwrap(),
        )
        .await;

        // Penguin publishes an update operation which refers to panda's last operation
        // as the graph tip.
        //
        // PANDA  : [1] <--[2]
        // PENGUIN:           \--[1]
        let (penguin_entry_1_hash, _) = send_to_node(
            &mut node,
            &penguin,
            &update_operation(
                schema.clone(),
                vec![panda_entry_2_hash.clone()],
                operation_fields(vec![(
                    "name",
                    OperationValue::Text("Penguin Cafe".to_string()),
                )]),
            ),
        )
        .unwrap();

        let penguin_entry_1 = get_entry_by_hash(&node, &penguin_entry_1_hash);

        assert_request(
            &app,
            &penguin_entry_1.entry_encoded(),
            &penguin_entry_1.operation_encoded(),
            None,
            &LogId::new(1),
            &SeqNum::new(2).unwrap(),
        )
        .await;

        // Penguin publishes another update operation refering to their own previous operation
        // as the graph tip.
        //
        // PANDA  : [1] <--[2]
        // PENGUIN:           \--[1] <--[2]
        let (penguin_entry_2_hash, _) = send_to_node(
            &mut node,
            &penguin,
            &update_operation(
                schema.clone(),
                vec![penguin_entry_1_hash],
                operation_fields(vec![(
                    "name",
                    OperationValue::Text("Polar Bear Cafe".to_string()),
                )]),
            ),
        )
        .unwrap();

        let penguin_entry_2 = get_entry_by_hash(&node, &penguin_entry_2_hash);

        assert_request(
            &app,
            &penguin_entry_2.entry_encoded(),
            &penguin_entry_2.operation_encoded(),
            None,
            &LogId::new(1),
            &SeqNum::new(3).unwrap(),
        )
        .await;

        // Panda publishes a new update operation which points at the current graph tip.
        //
        // PANDA  : [1] <--[2]             /--[3]
        // PENGUIN:           \--[1] <--[2]
        let (panda_entry_3_hash, _) = send_to_node(
            &mut node,
            &panda,
            &update_operation(
                schema,
                vec![penguin_entry_2_hash],
                operation_fields(vec![(
                    "name",
                    OperationValue::Text("Polar Bear Cafe!!!!!!!!!!".to_string()),
                )]),
            ),
        )
        .unwrap();

        let panda_entry_3 = get_entry_by_hash(&node, &panda_entry_3_hash);

        assert_request(
            &app,
            &panda_entry_3.entry_encoded(),
            &panda_entry_3.operation_encoded(),
            Some(&panda_entry_1.entry_encoded()),
            &LogId::new(1),
            &SeqNum::new(4).unwrap(),
        )
        .await;
    }
}
