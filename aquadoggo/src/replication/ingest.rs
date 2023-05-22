// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::operation::decode::decode_operation;
use p2panda_rs::operation::EncodedOperation;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;

#[derive(Debug)]
pub struct SyncIngest {
    store: SqlStore,
    tx: ServiceSender,
}

impl SyncIngest {
    pub fn new(store: SqlStore, tx: ServiceSender) -> Self {
        Self { store, tx }
    }

    pub fn handle_entry(
        entry_bytes: EncodedEntry,
        operation_bytes: Option<EncodedOperation>,
    ) -> Result<(), ReplicationError> {
        Ok(())
    }
}
