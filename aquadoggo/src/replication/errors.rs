// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::hash::Hash;
use thiserror::Error;

use crate::replication::SchemaIdSet;

#[derive(Error, Debug)]
pub enum ReplicationError {
    #[error("Remote peer requested unsupported replication mode")]
    UnsupportedMode,

    #[error("Sync request received containing unsupported target set")]
    UnsupportedTargetSet,

    #[error("Duplicate session error: {0}")]
    DuplicateSession(#[from] DuplicateSessionRequestError),

    #[error("No session found with id {0} for peer {1}")]
    NoSessionFound(u64, String),

    #[error("No sessions found for peer {0}")]
    NoPeerFound(String),

    #[error("Received entry which is not in target set")]
    UnmatchedTargetSet,

    #[error("Replication strategy failed with error: {0}")]
    StrategyFailed(String),

    #[error("Incoming data could not be ingested: {0}")]
    Validation(#[from] IngestError),
}

#[derive(Error, Debug)]
#[error(transparent)]
pub enum IngestError {
    #[error("Schema is not supported")]
    UnsupportedSchema,

    #[error("Schema not found")]
    SchemaNotFound,

    #[error(transparent)]
    Domain(#[from] p2panda_rs::api::DomainError),

    #[error("Decoding entry failed: {0}")]
    DecodeEntry(#[from] p2panda_rs::entry::error::DecodeEntryError),

    #[error("Decoding operation failed: {0}")]
    DecodeOperation(#[from] p2panda_rs::operation::error::DecodeOperationError),

    #[error("Duplicate entry received: {0}")]
    DuplicateEntry(Hash),
}

#[derive(Error, Debug)]
pub enum SchemaIdSetError {
    #[error("Set contains unsorted or duplicate schema ids")]
    UnsortedSchemaIds,
}

#[derive(Error, Debug, PartialEq)]
pub enum DuplicateSessionRequestError {
    #[error("Remote sent two sync requests for session with id {0}")]
    InboundPendingSession(u64),

    #[error("Tried to initialise duplicate inbound replication for already established session with id {0}")]
    InboundEstablishedSession(u64),

    #[error("Tried to initialise duplicate inbound replication for completed session with id {0}")]
    InboundDoneSession(u64),

    #[error(
        "Tried to initialise duplicate inbound replication session for existing target set {0:?}"
    )]
    InboundExistingTargetSet(SchemaIdSet),

    #[error(
        "Tried to initialise duplicate outbound replication session for existing target set {0:?}"
    )]
    OutboundExistingTargetSet(SchemaIdSet),

    #[error("Tried to initialise duplicate outbound replication session with id {0}")]
    Outbound(u64),
}
