// SPDX-License-Identifier: AGPL-3.0-or-later

use log::debug;
use p2panda_rs::api::validation::{
    ensure_document_not_deleted, get_checked_document_id_for_view_id, get_expected_skiplink,
    is_next_seq_num, validate_claimed_schema_id,
};
use p2panda_rs::api::DomainError;
use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::decode::decode_entry;
use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::operation::decode::decode_operation;
use p2panda_rs::operation::plain::PlainOperation;
use p2panda_rs::operation::traits::{AsOperation, Schematic};
use p2panda_rs::operation::validate::validate_operation_with_entry;
use p2panda_rs::operation::{EncodedOperation, Operation, OperationAction, OperationId};
use p2panda_rs::schema::Schema;
use p2panda_rs::storage_provider::traits::{EntryStore, LogStore, OperationStore};
use p2panda_rs::Human;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::db::SqlStore;
use crate::replication::errors::IngestError;
use crate::schema::SchemaProvider;

// @TODO: This method exists in `p2panda-rs`, we need to make it public there
async fn validate_entry_and_operation<S: EntryStore + OperationStore + LogStore>(
    store: &S,
    schema: &Schema,
    entry: &impl AsEntry,
    encoded_entry: &impl AsEncodedEntry,
    plain_operation: &PlainOperation,
    encoded_operation: &EncodedOperation,
) -> Result<(Operation, OperationId), DomainError> {
    // Verify that the claimed seq num matches the expected seq num for this public_key and log.
    let latest_entry = store
        .get_latest_entry(entry.public_key(), entry.log_id())
        .await?;
    let latest_seq_num = latest_entry.as_ref().map(|entry| entry.seq_num());
    is_next_seq_num(latest_seq_num, entry.seq_num())?;

    // If a skiplink is claimed, get the expected skiplink from the database, errors if it can't be found.
    let skiplink = match entry.skiplink() {
        Some(_) => Some(
            get_expected_skiplink(store, entry.public_key(), entry.log_id(), entry.seq_num())
                .await?,
        ),
        None => None,
    };

    // Construct params as `validate_operation_with_entry` expects.
    let skiplink_params = skiplink.as_ref().map(|entry| {
        let hash = entry.hash();
        (entry.clone(), hash)
    });

    // The backlink for this entry is the latest entry from this public key's log.
    let backlink_params = latest_entry.as_ref().map(|entry| {
        let hash = entry.hash();
        (entry.clone(), hash)
    });

    // Perform validation of the entry and it's operation.
    let (operation, operation_id) = validate_operation_with_entry(
        entry,
        encoded_entry,
        skiplink_params.as_ref().map(|(entry, hash)| (entry, hash)),
        backlink_params.as_ref().map(|(entry, hash)| (entry, hash)),
        plain_operation,
        encoded_operation,
        schema,
    )?;

    Ok((operation, operation_id))
}

// @TODO: This method exists in `p2panda-rs`, we need to make it public there
async fn determine_document_id<S: OperationStore>(
    store: &S,
    operation: &impl AsOperation,
    operation_id: &OperationId,
) -> Result<DocumentId, DomainError> {
    let document_id = match operation.action() {
        OperationAction::Create => {
            // Derive the document id for this new document.
            Ok::<DocumentId, DomainError>(DocumentId::new(operation_id))
        }
        _ => {
            // We can unwrap previous operations here as we know all UPDATE and DELETE operations contain them.
            let previous = operation.previous().unwrap();

            // Validate claimed schema for this operation matches the expected found in the
            // previous operations.
            validate_claimed_schema_id(store, &operation.schema_id(), &previous).await?;

            // Get the document_id for the document_view_id contained in previous operations.
            // This performs several validation steps (check method doc string).
            let document_id = get_checked_document_id_for_view_id(store, &previous).await?;

            Ok(document_id)
        }
    }?;

    // Ensure the document isn't deleted.
    ensure_document_not_deleted(store, &document_id)
        .await
        .map_err(|_| DomainError::DeletedDocument)?;

    Ok(document_id)
}

#[derive(Debug, Clone)]
pub struct SyncIngest {
    tx: ServiceSender,
    schema_provider: SchemaProvider,
}

impl SyncIngest {
    pub fn new(schema_provider: SchemaProvider, tx: ServiceSender) -> Self {
        Self {
            tx,
            schema_provider,
        }
    }

    pub async fn handle_entry(
        &self,
        store: &SqlStore,
        encoded_entry: &EncodedEntry,
        encoded_operation: &EncodedOperation,
    ) -> Result<(), IngestError> {
        let entry = decode_entry(encoded_entry)?;

        debug!(
            "Received entry {:?} for log {:?} and {}",
            entry.seq_num(),
            entry.log_id(),
            entry.public_key().display()
        );

        let plain_operation = decode_operation(encoded_operation)?;

        let schema = self
            .schema_provider
            .get(plain_operation.schema_id())
            .await
            .ok_or_else(|| IngestError::UnsupportedSchema)?;

        let (operation, operation_id) = validate_entry_and_operation(
            store,
            &schema,
            &entry,
            encoded_entry,
            &plain_operation,
            encoded_operation,
        )
        .await?;

        let document_id = determine_document_id(store, &operation, &operation_id).await?;

        // If the entries' seq num is 1 we insert a new log here
        if entry.seq_num().is_first() {
            store
                .insert_log(
                    entry.log_id(),
                    entry.public_key(),
                    Schematic::schema_id(&operation),
                    &document_id,
                )
                .await
                .expect("Fatal database error");
        }

        store
            .insert_entry(&entry, encoded_entry, Some(encoded_operation))
            .await
            .expect("Fatal database error");

        store
            .insert_operation(&operation_id, entry.public_key(), &operation, &document_id)
            .await
            .expect("Fatal database error");

        // Inform other services about received data
        let operation_id: OperationId = encoded_entry.hash().into();

        if self
            .tx
            .send(ServiceMessage::NewOperation(operation_id))
            .is_err()
        {
            // Silently fail here as we don't mind if there are no subscribers. We have
            // tests in other places to check if messages arrive.
        }

        Ok(())
    }
}
