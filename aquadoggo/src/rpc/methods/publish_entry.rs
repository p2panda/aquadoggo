// SPDX-License-Identifier: AGPL-3.0-or-later

use jsonrpc_v2::{Data, Params};
use p2panda_rs::entry::decode_entry;
use p2panda_rs::message::Message;
use p2panda_rs::Validate;

use crate::db::models::{Entry, Log};
use crate::errors::Result;
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

    #[error("Requested log id {0} does not match expected log id {1}")]
    InvalidLogId(i64, i64),

    #[error("The instance this message is referring to is unknown")]
    InstanceMissing,
}

/// Implementation of `panda_publishEntry` RPC method.
///
/// Stores an author's Bamboo entry with message payload in database after validating it.
pub async fn publish_entry(
    data: Data<RpcApiState>,
    Params(params): Params<PublishEntryRequest>,
) -> Result<PublishEntryResponse> {
    // Validate request parameters
    params.entry_encoded.validate()?;
    params.message_encoded.validate()?;

    // Get database connection pool
    let pool = data.pool.clone();

    // Handle error as this conversion validates message hash
    let entry = decode_entry(&params.entry_encoded, Some(&params.message_encoded))?;
    let message = Message::from(&params.message_encoded);

    let author = params.entry_encoded.author();

    // Determine expected log id for new entry: a `CREATE` entry is always stored in the next free
    // user log. An `UPDATE` or `DELETE` message is always stored in the same log that its original
    // `CREATE` message was stored in.
    let document_log_id = match message.is_create() {
        true => {
            // A document is identified by the hash of its `CREATE` message
            let document_hash = &params.entry_encoded.hash();
            Log::find_document_log_id(&pool, &author, document_hash).await?
        }
        false => {
            // An instance is identified by the hash of its previous operation
            let instance_hash = message.id().unwrap();
            Log::find_instance_log_id(&pool, &author, instance_hash)
                .await?
                .ok_or(PublishEntryError::InstanceMissing)?
        }
    };

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
    bamboo_rs_core::verify(
        &params.entry_encoded.to_bytes(),
        Some(&params.message_encoded.to_bytes()),
        entry_skiplink_bytes.as_deref(),
        entry_backlink_bytes.as_deref(),
    )?;

    // Register log in database when a new document is created
    if message.is_create() {
        Log::insert(
            &pool,
            &author,
            &params.entry_encoded.hash(),
            &message.schema(),
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
        &params.message_encoded,
        &params.message_encoded.hash(),
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
    use p2panda_rs::message::{Message, MessageEncoded, MessageFields, MessageValue};

    use crate::rpc::api::build_rpc_api_service;
    use crate::rpc::server::{build_rpc_server, RpcServer};
    use crate::test_helpers::{handle_http, initialize_db, rpc_error, rpc_request, rpc_response};

    /// Create encoded entries and messages for testing.
    fn create_test_entry(
        key_pair: &KeyPair,
        schema: &Hash,
        log_id: &LogId,
        instance: Option<&Hash>,
        skiplink: Option<&EntrySigned>,
        backlink: Option<&EntrySigned>,
        seq_num: &SeqNum,
    ) -> (EntrySigned, MessageEncoded) {
        // Create message with dummy data
        let mut fields = MessageFields::new();
        fields
            .add("test", MessageValue::Text("Hello".to_owned()))
            .unwrap();
        let message = match instance {
            Some(instance_id) => {
                Message::new_update(schema.clone(), instance_id.clone(), fields).unwrap()
            }
            None => Message::new_create(schema.clone(), fields).unwrap(),
        };

        // Encode message
        let message_encoded = MessageEncoded::try_from(&message).unwrap();

        // Create, sign and encode entry
        let entry = Entry::new(
            log_id,
            Some(&message),
            skiplink.map(|e| e.hash()).as_ref(),
            backlink.map(|e| e.hash()).as_ref(),
            seq_num,
        )
        .unwrap();
        let entry_encoded = sign_and_encode(&entry, key_pair).unwrap();

        (entry_encoded, message_encoded)
    }

    /// Compare API response from publishing an encoded entry and message to expected skiplink,
    /// log id and sequence number.
    async fn assert_request(
        app: &RpcServer,
        entry_encoded: &EntrySigned,
        message_encoded: &MessageEncoded,
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
                    "messageEncoded": "{}"
                }}"#,
                entry_encoded.as_str(),
                message_encoded.as_str(),
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
        let log_id_1 = LogId::new(1);
        let seq_num_1 = SeqNum::new(1).unwrap();

        // Create a couple of entries in the same log and check for consistency. The little diagrams
        // show back links and skip links analogous to this diagram from the bamboo spec:
        // https://github.com/AljoschaMeyer/bamboo/blob/61415747af34bfcb8f40a47ae3b02136083d3276/README.md#links-and-entry-verification
        //
        // [1] --
        let (entry_1, message_1) =
            create_test_entry(&key_pair, &schema, &log_id_1, None, None, None, &seq_num_1);
        assert_request(
            &app,
            &entry_1,
            &message_1,
            None,
            &log_id_1,
            &SeqNum::new(2).unwrap(),
        )
        .await;

        // [1] <-- [2]
        let (entry_2, message_2) = create_test_entry(
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
            &message_2,
            None,
            &log_id_1,
            &SeqNum::new(3).unwrap(),
        )
        .await;

        // [1] <-- [2] <-- [3]
        let (entry_3, message_3) = create_test_entry(
            &key_pair,
            &schema,
            &log_id_1,
            Some(&entry_2.hash()),
            None,
            Some(&entry_2),
            &SeqNum::new(3).unwrap(),
        );
        assert_request(
            &app,
            &entry_3,
            &message_3,
            Some(&entry_1),
            &log_id_1,
            &SeqNum::new(4).unwrap(),
        )
        .await;

        //  /------------------ [4]
        // [1] <-- [2] <-- [3]
        let (entry_4, message_4) = create_test_entry(
            &key_pair,
            &schema,
            &log_id_1,
            Some(&entry_3.hash()),
            Some(&entry_1),
            Some(&entry_3),
            &SeqNum::new(4).unwrap(),
        );
        assert_request(
            &app,
            &entry_4,
            &message_4,
            None,
            &log_id_1,
            &SeqNum::new(5).unwrap(),
        )
        .await;

        //  /------------------ [4]
        // [1] <-- [2] <-- [3]   \-- [5] --
        let (entry_5, message_5) = create_test_entry(
            &key_pair,
            &schema,
            &log_id_1,
            Some(&entry_4.hash()),
            None,
            Some(&entry_4),
            &SeqNum::new(5).unwrap(),
        );
        assert_request(
            &app,
            &entry_5,
            &message_5,
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
        let (entry_1, message_1) =
            create_test_entry(&key_pair, &schema, &log_id, None, None, None, &seq_num);
        assert_request(
            &app,
            &entry_1,
            &message_1,
            None,
            &log_id,
            &SeqNum::new(2).unwrap(),
        )
        .await;

        let (entry_2, message_2) = create_test_entry(
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
            &message_2,
            None,
            &log_id,
            &SeqNum::new(3).unwrap(),
        )
        .await;

        // Send invalid log id for a new document: The entries entry_1 and entry_2 are assigned to
        // log 1, which makes log 3 the required log for the next new document.
        let (entry_wrong_log_id, message_wrong_log_id) = create_test_entry(
            &key_pair,
            &schema,
            &LogId::new(5),
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
                    "messageEncoded": "{}"
                }}"#,
                entry_wrong_log_id.as_str(),
                message_wrong_log_id.as_str(),
            ),
        );

        let response = rpc_error("Requested log id 5 does not match expected log id 3");

        assert_eq!(handle_http(&app, request).await, response);

        // Send invalid log id for an existing document: This entry is an update for the existing
        // document in log 1, however, we are trying to publish it in log 3.
        let (entry_wrong_log_id, message_wrong_log_id) = create_test_entry(
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
                    "messageEncoded": "{}"
                }}"#,
                entry_wrong_log_id.as_str(),
                message_wrong_log_id.as_str(),
            ),
        );

        let response = rpc_error("Requested log id 3 does not match expected log id 1");

        assert_eq!(handle_http(&app, request).await, response);

        // Send invalid backlink entry / hash
        let (entry_wrong_hash, message_wrong_hash) = create_test_entry(
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
                    "messageEncoded": "{}"
                }}"#,
                entry_wrong_hash.as_str(),
                message_wrong_hash.as_str(),
            ),
        );

        let response = rpc_error(
            "The backlink hash encoded in the entry does not match the lipmaa entry provided",
        );

        assert_eq!(handle_http(&app, request).await, response);

        // Send invalid seq num
        let (entry_wrong_seq_num, message_wrong_seq_num) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_2.hash()),
            None,
            Some(&entry_2),
            &SeqNum::new(5).unwrap(),
        );

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "messageEncoded": "{}"
                }}"#,
                entry_wrong_seq_num.as_str(),
                message_wrong_seq_num.as_str(),
            ),
        );

        let response = rpc_error("Could not find backlink entry in database");

        assert_eq!(handle_http(&app, request).await, response);
    }
}
