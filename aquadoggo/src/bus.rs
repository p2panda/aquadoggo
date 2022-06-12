// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::entry::Entry;
use p2panda_rs::operation::Operation;

use crate::manager::Sender;

/// Sender for cross-service communication bus.
pub type ServiceSender = Sender<ServiceMessage>;

/// Messages which can be sent on the communication bus.
#[derive(Clone, Debug)]
pub enum ServiceMessage {
    /// New `Entry` with an `Operation` payload arrived at the node.
    #[allow(dead_code)]
    NewEntryAndOperation(Entry, Operation),
}
