// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::schema::SchemaId;

mod manager;
mod message;
mod session;

pub use manager::SyncManager;
pub use message::SyncMessage;
pub use session::{Session, SessionId, SessionState};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct TargetSet(Vec<SchemaId>);

impl TargetSet {
    pub fn new(schema_ids: &[SchemaId]) -> Self {
        // Sort schema ids to compare target sets easily
        let mut schema_ids = schema_ids.to_vec();
        schema_ids.sort();

        Self(schema_ids)
    }
}
