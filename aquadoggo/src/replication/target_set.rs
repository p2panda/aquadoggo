// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::schema::SchemaId;
use p2panda_rs::Validate;
use serde::{Deserialize, Deserializer, Serialize};

use crate::replication::errors::TargetSetError;

/// De-duplicated and sorted set of schema ids which define the target data for the replication
/// session.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
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

    fn from_untrusted(schema_ids: Vec<SchemaId>) -> Result<Self, TargetSetError> {
        // Create target set with potentially invalid data
        let target_set = Self(schema_ids);

        // Make sure its sorted and does not contain any duplicates
        target_set.validate()?;

        Ok(target_set)
    }
}

impl Validate for TargetSet {
    type Error = TargetSetError;

    fn validate(&self) -> Result<(), Self::Error> {
        // Check if at least one schema id is given
        if self.0.is_empty() {
            return Err(TargetSetError::ZeroSchemaIds);
        };

        let mut prev_schema_id: Option<&SchemaId> = None;

        for schema_id in &self.0 {
            // Check if it is sorted, this indirectly also checks against duplicates
            if let Some(prev) = prev_schema_id {
                if prev >= schema_id {
                    return Err(TargetSetError::UnsortedSchemaIds);
                }
            }

            prev_schema_id = Some(schema_id);
        }

        Ok(())
    }
}

impl<'de> Deserialize<'de> for TargetSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Deserializer of `SchemaId` checks internally for the correct format of each id
        let schema_ids: Vec<SchemaId> = Deserialize::deserialize(deserializer)?;
        Self::from_untrusted(schema_ids).map_err(serde::de::Error::custom)
    }
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
