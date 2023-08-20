// SPDX-License-Identifier: AGPL-3.0-or-later

mod announcement;
pub mod errors;
mod ingest;
mod manager;
mod message;
mod mode;
mod service;
mod session;
mod strategies;
mod target_set;
pub mod traits;

pub use announcement::{now, Announcement, AnnouncementMessage};
pub use ingest::SyncIngest;
pub use manager::SyncManager;
pub use message::{LiveMode, LogHeights, Message, SyncMessage};
pub use mode::Mode;
pub use service::replication_service;
pub use session::{Session, SessionId, SessionState};
pub use strategies::{LogHeightStrategy, SetReconciliationStrategy, StrategyResult};
pub use target_set::TargetSet;

/// Currently supported p2panda replication protocol version.
pub const REPLICATION_PROTOCOL_VERSION: u64 = 1;
