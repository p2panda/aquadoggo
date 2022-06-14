// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::operation::OperationId;

use crate::manager::Sender;

/// Sender for cross-service communication bus.
pub type ServiceSender = Sender<ServiceMessage>;

/// Messages which can be sent on the communication bus.
#[derive(Clone, Debug)]
pub enum ServiceMessage {
    /// A new operation arrived at the node.
    NewOperation(OperationId),
}
