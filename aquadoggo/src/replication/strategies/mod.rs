// SPDX-License-Identifier: AGPL-3.0-or-later

mod diff;
mod naive;
mod set_reconciliation;

pub use diff::diff_log_heights;
pub use naive::NaiveStrategy;
pub use set_reconciliation::SetReconciliationStrategy;

use crate::replication::Message;

#[derive(Clone, Debug)]
pub struct StrategyResult {
    pub messages: Vec<Message>,
    pub is_local_done: bool,
}
