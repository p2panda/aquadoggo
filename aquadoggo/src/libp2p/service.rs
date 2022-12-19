// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;

use crate::context::Context;
use crate::bus::ServiceSender;
use crate::manager::{Shutdown, ServiceReadySender};

pub async fn libp2p_service(
    context: Context,
    signal: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    Ok(())
}
