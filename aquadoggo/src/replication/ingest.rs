// SPDX-License-Identifier: AGPL-3.0-or-later

use log::trace;
use p2panda_rs::api::publish;
use p2panda_rs::entry::decode::decode_entry;
use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::operation::decode::decode_operation;
use p2panda_rs::operation::traits::Schematic;
use p2panda_rs::operation::{EncodedOperation, OperationId};
use p2panda_rs::Human;

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
        match mode {
            Mode::Document => {
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
            _ => {
                // @TODO: For now we just don't ingest any enries arriving via other modes, we can
                // remove support for LogHeight replication completely if we want to migrate to
                // the new Document approach.
                Ok(())
            }
        }
    }
}
