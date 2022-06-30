// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;

use crate::bus::ServiceSender;
use crate::context::Context;
use crate::manager::Shutdown;

/// Replication service polling other nodes frequently to ask them about new entries from a defined
/// set of authors and log ids.
pub async fn replication_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
) -> Result<()> {
    Ok(())
}
