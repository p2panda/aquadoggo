// SPDX-License-Identifier: AGPL-3.0-or-later

mod announcement;
pub mod errors;
mod ingest;
mod manager;
mod message;
mod mode;
mod schema_id_set;
mod service;
mod session;
mod strategies;
pub mod traits;

pub use announcement::{now, Announcement, AnnouncementMessage};
pub use ingest::SyncIngest;
pub use manager::SyncManager;
pub use message::{LogHeights, Message, SyncMessage};
pub use mode::Mode;
pub use schema_id_set::SchemaIdSet;
pub use service::replication_service;
pub use session::{Session, SessionId, SessionState};
pub use strategies::{LogHeightStrategy, SetReconciliationStrategy, StrategyResult};

pub type MessageType = u64;

// Integers indicating message type for wire message format.
pub const ANNOUNCE_TYPE: MessageType = 0;
pub const SYNC_REQUEST_TYPE: MessageType = 1;
pub const ENTRY_TYPE: MessageType = 2;
pub const SYNC_DONE_TYPE: MessageType = 3;
pub const HAVE_TYPE: MessageType = 10;

/// Currently supported p2panda replication protocol version.
pub const REPLICATION_PROTOCOL_VERSION: u64 = 1;
