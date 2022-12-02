// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::operation::OperationId;

use crate::manager::Sender;
use crate::materializer::{TaskInput, Task};

/// Sender for cross-service communication bus.
pub type ServiceSender = Sender<ServiceMessage>;

/// Messages which can be sent on the communication bus.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceMessage {
    /// A new operation arrived at the node.
    NewOperation(OperationId),
}

/// Messages sent on the service status channel.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ServiceStatusMessage {
    /// Message from the http service announcing the GraphQL Schema has been
    /// built or re-built due to a new schema being materialized.
    GraphQLSchemaBuilt,

    /// Message from the materializer service containing the status of newly
    /// completed tasks.
    MaterializerTaskComplete(Task<TaskInput>),
}