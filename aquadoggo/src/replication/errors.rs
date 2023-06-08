// SPDX-License-Identifier: AGPL-3.0-or-later

use libp2p::PeerId;
use thiserror::Error;

use crate::replication::TargetSet;

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("Reject duplicate inbound connection with peer: {0}")]
    MultipleInboundConnections(PeerId),

    #[error("Reject duplicate outbound connection with peer: {0}")]
    MultipleOutboundConnections(PeerId),
}

#[derive(Error, Debug)]
pub enum ReplicationError {
    #[error("Remote peer requested unsupported replication mode")]
    UnsupportedMode,

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

    #[error("Incoming data could not be ingested")]
    Validation(#[from] IngestError),
}

#[derive(Error, Debug)]
#[error(transparent)]
pub enum IngestError {
    #[error("Schema is not supported")]
    UnsupportedSchema,

    #[error(transparent)]
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
    InboundExistingTargetSet(TargetSet),

    #[error(
        "Tried to initialise duplicate outbound replication session for existing target set {0:?}"
    )]
    OutboundExistingTargetSet(TargetSet),

    #[error("Tried to initialise duplicate outbound replication session with id {0}")]
    Outbound(u64),
}
