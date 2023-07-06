// SPDX-License-Identifier: AGPL-3.0-or-later

use log::trace;
use p2panda_rs::api::publish;
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::operation::decode::decode_operation;
use p2panda_rs::operation::traits::Schematic;
use p2panda_rs::operation::{EncodedOperation, OperationId};
use p2panda_rs::storage_provider::traits::EntryStore;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::db::SqlStore;
use crate::replication::errors::IngestError;
use crate::replication::Mode;
use crate::schema::SchemaProvider;

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
        mode: Mode,
        encoded_entry: &EncodedEntry,
        encoded_operation: &EncodedOperation,
    ) -> Result<(), IngestError> {
        trace!("Received entry and operation: {}", encoded_entry.hash());

        // Check if we already have this entry. This can happen if another peer sent it to
        // us during a concurrent sync session.
        let is_duplicate = store
            .get_entry(&encoded_entry.hash())
            .await
            .expect("Fatal database error")
            .is_some();
        if is_duplicate {
            return Err(IngestError::DuplicateEntry(encoded_entry.hash()));
        }

        // Make sure we have the relevant schema materialized on the node.
        let operation = decode_operation(&encoded_operation)?;
        let schema = self
            .schema_provider
            .get(operation.schema_id())
            .await
            .ok_or_else(|| IngestError::UnsupportedSchema)?;

        /////////////////////////////////////
        // PUBLISH THE ENTRY AND OPERATION //
        /////////////////////////////////////

        let _ = publish(
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

        if self
            .tx
            .send(ServiceMessage::NewOperation(operation_id))
            .is_err()
        {
            // Silently fail here as we don't mind if there are no subscribers. We have
            // tests in other places to check if messages arrive.
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::entry::EncodedEntry;
    use p2panda_rs::operation::EncodedOperation;
    use p2panda_rs::schema::Schema;
    use p2panda_rs::test_utils::fixtures::{encoded_entry, encoded_operation, schema};
    use rstest::rstest;
    use tokio::sync::broadcast;

    use crate::replication::errors::IngestError;
    use crate::replication::{Mode, SyncIngest};
    use crate::test_utils::{test_runner_with_manager, TestNodeManager};

    #[rstest]
    fn reject_duplicate_entries(
        schema: Schema,
        encoded_entry: EncodedEntry,
        encoded_operation: EncodedOperation,
    ) {
        test_runner_with_manager(move |manager: TestNodeManager| async move {
            let mut node = manager.create().await;
            node.context.schema_provider.update(schema).await;

            let (tx, _rx) = broadcast::channel(8);
            let ingest = SyncIngest::new(node.context.schema_provider.clone(), tx.clone());

            let result = ingest
                .handle_entry(
                    &node.context.store,
                    Mode::LogHeight,
                    &encoded_entry,
                    &encoded_operation,
                )
                .await;

            assert!(result.is_ok());

            let result = ingest
                .handle_entry(
                    &node.context.store,
                    Mode::LogHeight,
                    &encoded_entry,
                    &encoded_operation,
                )
                .await;

            assert!(matches!(result, Err(IngestError::DuplicateEntry(_))));
        })
    }
}
