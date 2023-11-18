// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::{bail, Result};

use crate::api::{migrate, LockFile};
use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;

/// Interface to interact with the node in a programmatic, "low-level" way.
#[derive(Debug)]
pub struct NodeInterface {
    context: Context,
    tx: ServiceSender,
}

impl NodeInterface {
    pub fn new(context: Context, tx: ServiceSender) -> Self {
        Self { context, tx }
    }

    pub async fn migrate(&self, lock_file: LockFile) -> Result<bool> {
        let committed_operations = migrate(
            &self.context.store,
            &self.context.schema_provider,
            lock_file,
        )
        .await?;

        let did_migration_happen = !committed_operations.is_empty();

        // Send new operations from migration on service communication bus, this will arrive
        // eventually at the materializer service
        for operation_id in committed_operations {
            if self
                .tx
                .send(ServiceMessage::NewOperation(operation_id))
                .is_err()
            {
                bail!("Failed to inform materialization service about migration");
            }
        }

        Ok(did_migration_happen)
    }
}
