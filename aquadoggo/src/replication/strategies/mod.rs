// SPDX-License-Identifier: AGPL-3.0-or-later

mod diff;
mod log_height;
mod set_reconciliation;

pub use diff::diff_log_heights;
pub use log_height::LogHeightStrategy;
pub use set_reconciliation::SetReconciliationStrategy;

use crate::replication::Message;

#[derive(Clone, Debug)]
pub struct StrategyResult {
    pub messages: Vec<Message>,
    pub is_local_done: bool,
}

impl StrategyResult {
    pub fn merge(&mut self, result: StrategyResult) {
        self.messages.extend(result.messages);
        self.is_local_done = self.is_local_done || result.is_local_done;
    }
}
