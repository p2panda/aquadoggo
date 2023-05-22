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

    #[error("No session found with id {0}")]
    NoSessionFound(u64),

    #[error("Received entry which is not in target set")]
    UnmatchedTargetSet,

    #[error("Replication strategy failed with error: {0}")]
    StrategyFailed(String),

    #[error("Incoming data could not be ingested")]
    Validation(#[from] IngestError),
}

#[derive(Error, Debug)]
pub enum IngestError {
    #[error("Schema is not supported")]
    UnsupportedSchema,

    #[error("Received entry and operation is invalid")]
    Domain(#[from] p2panda_rs::api::DomainError),

    #[error("Decoding entry failed")]
    DecodeEntry(#[from] p2panda_rs::entry::error::DecodeEntryError),

    #[error("Decoding operation failed")]
    DecodeOperation(#[from] p2panda_rs::operation::error::DecodeOperationError),
}

#[derive(Error, Debug)]
pub enum TargetSetError {
    #[error("Target set does not contain any schema ids")]
    ZeroSchemaIds,

    #[error("Target set contains unsorted or duplicate schema ids")]
    UnsortedSchemaIds,
}
