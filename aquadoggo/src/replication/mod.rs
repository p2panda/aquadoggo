// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::schema::SchemaId;

pub mod errors;
mod manager;
mod message;
mod session;

pub use manager::SyncManager;
pub use message::{SyncMessage, StrategyMessage};
pub use session::{Session, SessionId, SessionState};

/// De-duplicated and sorted set of schema ids which define the target data for the replication
/// session.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct TargetSet(Vec<SchemaId>);

impl TargetSet {
    pub fn new(schema_ids: &[SchemaId]) -> Self {
        // Remove duplicates
        let mut deduplicated_set: Vec<SchemaId> = Vec::new();
        for schema_id in schema_ids {
            if !deduplicated_set.contains(schema_id) {
                deduplicated_set.push(schema_id.clone());
            }
        }

        // Sort schema ids to compare target sets easily
        deduplicated_set.sort();

        Self(deduplicated_set)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mode {
    Naive,
    SetReconciliation,
    Unknown,
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::schema::{SchemaId, SchemaName};
    use p2panda_rs::test_utils::fixtures::random_document_view_id;
    use rstest::rstest;

    use super::TargetSet;

    #[rstest]
    fn compare_target_sets(
        #[from(random_document_view_id)] document_view_id_1: DocumentViewId,
        #[from(random_document_view_id)] document_view_id_2: DocumentViewId,
    ) {
        let schema_id_1 =
            SchemaId::new_application(&SchemaName::new("messages").unwrap(), &document_view_id_1);
        let schema_id_2 =
            SchemaId::new_application(&SchemaName::new("profiles").unwrap(), &document_view_id_2);

        // Sort schema ids
        assert_eq!(
            TargetSet::new(&[schema_id_1.clone(), schema_id_2.clone()]),
            TargetSet::new(&[schema_id_2.clone(), schema_id_1.clone()]),
        );

        // De-duplicate schema ids
        assert_eq!(
            TargetSet::new(&[schema_id_1.clone(), schema_id_1.clone()]),
            TargetSet::new(&[schema_id_1.clone()]),
        );

        // Distinct different target sets from each other
        assert_ne!(
            TargetSet::new(&[schema_id_1.clone()]),
            TargetSet::new(&[schema_id_2.clone()]),
        );
    }
}
