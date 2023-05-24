// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;

use crate::bus::ServiceSender;
use crate::context::Context;
use crate::manager::{ServiceReadySender, Shutdown};
use crate::replication::{SyncIngest, SyncManager};

pub async fn replication_service(
    context: Context,
    signal: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    let local_peer_id = context
        .config
        .network
        .peer_id
        .expect("Peer id needs to be given");

    let ingest = SyncIngest::new(context.schema_provider.clone(), tx);
    let manager = SyncManager::new(context.store.clone(), ingest, local_peer_id);

    Ok(())
}
