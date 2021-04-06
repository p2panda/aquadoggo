use std::convert::TryFrom;

use bamboo_rs_core::entry::is_lipmaa_required;
use p2panda_rs::atomic::{Entry as EntryUnsigned, Message};

use super::request::{EntryArgsRequest, PublishEntryRequest};
use super::response::{EntryArgsResponse, PublishEntryResponse};
use crate::db::models::{Entry, Log};
use crate::db::Pool;
use crate::errors::Result;

#[derive(thiserror::Error, Debug)]
#[allow(missing_copy_implementations)]
pub enum ApiError {
    #[error("Could not find backlink entry in database")]
    BacklinkMissing,

    #[error("Could not find skiplink entry in database")]
    SkiplinkMissing,

    #[error("Claimed log_id for schema not the same as in database")]
    InvalidLogId,
}

/// Implementation of `panda_getEntryArguments` RPC method.
///
/// Returns required data (backlink and skiplink entry hashes, last sequence number and the schemas
/// log_id) to encode a new bamboo entry.
pub async fn get_entry_args(pool: Pool, params: EntryArgsRequest) -> Result<EntryArgsResponse> {
    // Determine log_id for author's schema
    let log_id = Log::find_schema_log_id(&pool, &params.author, &params.schema).await?;

    // Find latest entry in this log
    let entry_latest = Entry::latest(&pool, &params.author, &log_id).await?;

    match entry_latest {
        Some(entry_backlink) => {
            let next_seq_num = entry_backlink.seq_num.clone().next().unwrap();

            // Unwrap as we know that an skiplink exists as soon as previous entry is given:
            let skiplink_seq_num = next_seq_num.skiplink_seq_num().unwrap();

            // Determine skiplink ("lipmaa"-link) entry in this log, return `None` when no skiplink
            // is required for the next entry
            let entry_hash_skiplink = if is_lipmaa_required(next_seq_num.as_i64() as u64) {
                let entry = Entry::at_seq_num(&pool, &params.author, &log_id, &skiplink_seq_num)
                    .await?
                    .unwrap();
                Some(entry.entry_hash)
            } else {
                None
            };

            Ok(EntryArgsResponse {
                entry_hash_backlink: Some(entry_backlink.entry_hash),
                entry_hash_skiplink,
                last_seq_num: Some(entry_backlink.seq_num),
                log_id,
            })
        }
        None => Ok(EntryArgsResponse {
            entry_hash_backlink: None,
            entry_hash_skiplink: None,
            last_seq_num: None,
            log_id,
        }),
    }
}

/// Implementation of `panda_publishEntry` RPC method.
///
/// Stores an author's Bamboo entry with message payload in database after validating it.
pub async fn publish_entry(
    pool: Pool,
    params: PublishEntryRequest,
) -> Result<PublishEntryResponse> {
    // Handle error as this conversion validates message hash
    let entry = EntryUnsigned::try_from((&params.entry_encoded, Some(&params.message_encoded)))?;
    let message = Message::from(&params.message_encoded);

    // Retreive author and schema
    let author = params.entry_encoded.author();
    let schema = message.schema();

    // Determine log_id for author's schema
    let schema_log_id = Log::get(&pool, &author, &schema).await?;

    // Check if log_id is the same as the previously claimed one (when given)
    if schema_log_id.is_some() && schema_log_id.as_ref() != Some(entry.log_id()) {
        Err(ApiError::InvalidLogId)?;
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
        .ok_or(ApiError::BacklinkMissing)
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
        .ok_or(ApiError::SkiplinkMissing)
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

    // Register used log id in database when not set yet
    if schema_log_id.is_none() {
        Log::insert(&pool, &author, &schema, entry.log_id()).await?;
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

    Ok(PublishEntryResponse {})
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use jsonrpc_core::ErrorCode;
    use p2panda_rs::atomic::{
        Entry, EntrySigned, Hash, LogId, Message, MessageEncoded, MessageFields, MessageValue,
        SeqNum,
    };
    use p2panda_rs::key_pair::KeyPair;

    use crate::rpc::ApiService;
    use crate::test_helpers::{
        initialize_db, random_entry_hash, rpc_error, rpc_request, rpc_response,
    };

    const TEST_AUTHOR: &str = "8b52ae153142288402382fd6d9619e018978e015e6bc372b1b0c7bd40c6a240a";

    // Helper method to create encoded entries and messages
    fn create_test_entry(
        key_pair: &KeyPair,
        schema: &Hash,
        log_id: &LogId,
        skiplink: Option<&Hash>,
        backlink: Option<&Hash>,
        previous_seq_num: Option<&SeqNum>,
    ) -> (EntrySigned, MessageEncoded) {
        // Create message with dummy data
        let mut fields = MessageFields::new();
        fields
            .add("test", MessageValue::Text("Hello".to_owned()))
            .unwrap();
        let message = Message::new_create(schema.clone(), fields).unwrap();

        // Encode message
        let message_encoded = MessageEncoded::try_from(&message).unwrap();

        // Create, sign and encode entry
        let entry = Entry::new(log_id, &message, skiplink, backlink, previous_seq_num).unwrap();
        let entry_encoded = EntrySigned::try_from((&entry, key_pair)).unwrap();

        (entry_encoded, message_encoded)
    }

    #[async_std::test]
    async fn get_entry_arguments() {
        let pool = initialize_db().await;
        let io = ApiService::io_handler(pool);

        let request = rpc_request(
            "panda_getEntryArguments",
            &format!(
                r#"{{
                    "author": "{}",
                    "schema": "{}"
                }}"#,
                TEST_AUTHOR,
                random_entry_hash(),
            ),
        );

        let response = rpc_response(
            r#"{
                "entryHashBacklink": null,
                "entryHashSkiplink": null,
                "lastSeqNum": null,
                "logId": 1
            }"#,
        );

        assert_eq!(io.handle_request_sync(&request), Some(response));
    }

    #[async_std::test]
    async fn publish_entry() {
        // Create key pair for author
        let key_pair = KeyPair::new();

        // Prepare test database
        let pool = initialize_db().await;
        let io = ApiService::io_handler(pool);

        // Define schema and log id for entries
        let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();
        let log_id = LogId::new(5);

        // Create first entry for log and send it to RPC endpoint
        let (entry_encoded_first, message_encoded) =
            create_test_entry(&key_pair, &schema, &log_id, None, None, None);

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "messageEncoded": "{}"
                }}"#,
                entry_encoded_first.as_str(),
                message_encoded.as_str(),
            ),
        );

        let response = rpc_response(r#"{}"#);

        assert_eq!(io.handle_request_sync(&request), Some(response));

        // Create second entry for log and send it to RPC endpoint
        let (entry_encoded_second, message_encoded) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            None,
            Some(&entry_encoded_first.hash()),
            Some(&SeqNum::new(1).unwrap()),
        );

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "messageEncoded": "{}"
                }}"#,
                entry_encoded_second.as_str(),
                message_encoded.as_str(),
            ),
        );

        let response = rpc_response(r#"{}"#);

        assert_eq!(io.handle_request_sync(&request), Some(response));

        // Send invalid log id for this schema
        let (entry_encoded_wrong, message_encoded) = create_test_entry(
            &key_pair,
            &schema,
            &LogId::new(1),
            None,
            Some(&entry_encoded_first.hash()),
            Some(&SeqNum::new(1).unwrap()),
        );

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "messageEncoded": "{}"
                }}"#,
                entry_encoded_wrong.as_str(),
                message_encoded.as_str(),
            ),
        );

        let response = rpc_error(
            ErrorCode::InvalidParams,
            "Invalid params: Claimed log_id for schema not the same as in database.",
        );

        assert_eq!(io.handle_request_sync(&request), Some(response));

        // Send invalid hash
        let (entry_encoded_third, message_encoded) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            None,
            Some(&Hash::new_from_bytes(vec![1, 2, 3]).unwrap()),
            Some(&SeqNum::new(2).unwrap()),
        );

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "messageEncoded": "{}"
                }}"#,
                entry_encoded_third.as_str(),
                message_encoded.as_str(),
            ),
        );

        let response = rpc_error(
            ErrorCode::InvalidParams,
            "Invalid params: The backlink hash encoded in the entry does not match the lipmaa entry provided.",
        );

        assert_eq!(io.handle_request_sync(&request), Some(response));

        // Send invalid seq num
        let (entry_encoded_third, message_encoded) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_encoded_first.hash()),
            Some(&entry_encoded_second.hash()),
            Some(&SeqNum::new(5).unwrap()),
        );

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "messageEncoded": "{}"
                }}"#,
                entry_encoded_third.as_str(),
                message_encoded.as_str(),
            ),
        );

        let response = rpc_error(
            ErrorCode::InvalidParams,
            "Invalid params: Could not find backlink entry in database.",
        );

        assert_eq!(io.handle_request_sync(&request), Some(response));
    }
}
