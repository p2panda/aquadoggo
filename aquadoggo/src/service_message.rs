// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::service_manager::Sender;

/// Sender for cross-service communication bus.
pub type ServiceSender = Sender<ServiceMessage>;

/// Messages which can be sent on the communication bus.
#[derive(Clone)]
pub enum ServiceMessage {
    // Nothing here to see yet ..!
}
