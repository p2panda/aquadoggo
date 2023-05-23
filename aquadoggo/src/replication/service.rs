// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;

use crate::bus::ServiceSender;
use crate::context::Context;
use crate::manager::{ServiceReadySender, Shutdown};

pub async fn replication_service(
    context: Context,
    signal: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    Ok(())
}
