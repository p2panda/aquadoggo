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
use crate::schema::SchemaProvider;

#[derive(Debug, Clone)]
pub struct SyncIngest {
    tx: ServiceSender,
    pub schema_provider: SchemaProvider,
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
        trace!("Received entry and operation: {}", encoded_entry.hash());

        // Check if we already have this entry. This can happen if another peer sent it to us
        // during a concurrent sync session.
        if store
            .get_entry(&encoded_entry.hash())
            .await
            .expect("Fatal database error")
            .is_some()
        {
            return Err(IngestError::DuplicateEntry(encoded_entry.hash()));
        }

        let plain_operation = decode_operation(encoded_operation)?;

        // If the node has been configured with an allow-list of supported schema ids, check that
        // the sent operation follows one of our supported schema
        if self.schema_provider.is_allow_list_active()
            && !self
                .schema_provider
                .supported_schema_ids()
                .await
                .contains(plain_operation.schema_id())
        {
            return Err(IngestError::UnsupportedSchema);
        }

        // Retrieve the schema if it has been materialized on the node.
        let schema = self
            .schema_provider
            .get(plain_operation.schema_id())
            .await
            .ok_or_else(|| IngestError::SchemaNotFound)?;

        /////////////////////////////////////
        // PUBLISH THE ENTRY AND OPERATION //
        /////////////////////////////////////

        let _ = publish(
            store,
            &schema,
            encoded_entry,
            &plain_operation,
            encoded_operation,
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
    use crate::replication::SyncIngest;
    use crate::test_utils::{test_runner_with_manager, TestNodeManager};
    use crate::{AllowList, Configuration};

    #[rstest]
    fn reject_duplicate_entries(
        schema: Schema,
        encoded_entry: EncodedEntry,
        encoded_operation: EncodedOperation,
    ) {
        test_runner_with_manager(move |manager: TestNodeManager| async move {
            let node = manager.create().await;
            let _ = node.context.schema_provider.update(schema).await;

            let (tx, _rx) = broadcast::channel(8);
            let ingest = SyncIngest::new(node.context.schema_provider.clone(), tx.clone());

            let result = ingest
                .handle_entry(&node.context.store, &encoded_entry, &encoded_operation)
                .await;

            assert!(result.is_ok());

            let result = ingest
                .handle_entry(&node.context.store, &encoded_entry, &encoded_operation)
                .await;

            assert!(matches!(result, Err(IngestError::DuplicateEntry(_))));
        })
    }

    #[rstest]
    fn allow_supported_schema_ids(
        schema: Schema,
        encoded_entry: EncodedEntry,
        encoded_operation: EncodedOperation,
    ) {
        test_runner_with_manager(move |manager: TestNodeManager| async move {
            let config = Configuration {
                allow_schema_ids: AllowList::Set(vec![schema.id().clone()]),
                ..Configuration::default()
            };
            let node = manager.create_with_config(config).await;

            assert!(node.context.schema_provider.is_allow_list_active());

            let _ = node.context.schema_provider.update(schema.clone()).await;
            let (tx, _rx) = broadcast::channel(8);
            let ingest = SyncIngest::new(node.context.schema_provider.clone(), tx.clone());

            let result = ingest
                .handle_entry(&node.context.store, &encoded_entry, &encoded_operation)
                .await;

            assert!(result.is_ok());
        });
    }
}
