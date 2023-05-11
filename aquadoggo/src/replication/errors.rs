// SPDX-License-Identifier: AGPL-3.0-or-later

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ReplicationError {
    #[error("Remote peer requested unsupported replication mode")]
    UnsupportedMode,

    #[error("Tried to initialise duplicate inbound replication session with id {0}")]
    DuplicateInboundRequest(u64),

    #[error("Tried to initialise duplicate outbound replication session with id {0}")]
    DuplicateOutboundRequest(u64),
}
