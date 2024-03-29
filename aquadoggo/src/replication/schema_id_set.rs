// SPDX-License-Identifier: AGPL-3.0-or-later

use std::slice::Iter;

use p2panda_rs::schema::SchemaId;
use p2panda_rs::Validate;
use serde::{Deserialize, Deserializer, Serialize};

use crate::replication::errors::SchemaIdSetError;

/// De-duplicated and sorted set of schema ids which define the target data for the replication
/// session.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct SchemaIdSet(Vec<SchemaId>);

impl SchemaIdSet {
    pub fn new(schema_ids: &[SchemaId]) -> Self {
        // Remove duplicates
        let mut deduplicated_set: Vec<SchemaId> = Vec::new();
        for schema_id in schema_ids {
            if !deduplicated_set.contains(schema_id) {
                deduplicated_set.push(schema_id.clone());
            }
        }

        // Sort schema ids to compare sets easily
        deduplicated_set.sort();

        // And now sort system schema to the front of the set.
        deduplicated_set.sort_by(|schema_id_a, schema_id_b| {
            let is_system_schema = |schema_id: &SchemaId| -> bool {
                !matches!(schema_id, SchemaId::Application(_, _))
            };
            is_system_schema(schema_id_b).cmp(&is_system_schema(schema_id_a))
        });

        Self(deduplicated_set)
    }

    pub fn from_intersection(local_set: &SchemaIdSet, remote_set: &SchemaIdSet) -> Self {
        let mut set = local_set.clone();
        set.0.retain(|id| remote_set.contains(id));
        set
    }

    fn from_untrusted(schema_ids: Vec<SchemaId>) -> Result<Self, SchemaIdSetError> {
        // Create set with potentially invalid data
        let set = Self(schema_ids);

        // Make sure its sorted and does not contain any duplicates
        set.validate()?;

        Ok(set)
    }

    pub fn contains(&self, schema_id: &SchemaId) -> bool {
        self.0.contains(schema_id)
    }

    /// Returns true if there are no unknown elements in external set.
    pub fn is_valid_set(&self, set: &SchemaIdSet) -> bool {
        !set.iter().any(|schema_id| !self.contains(schema_id))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> Iter<SchemaId> {
        self.0.iter()
    }
}

impl Validate for SchemaIdSet {
    type Error = SchemaIdSetError;

    fn validate(&self) -> Result<(), Self::Error> {
        let mut prev_schema_id: Option<&SchemaId> = None;
        let mut initial_system_schema = true;

        // We need to validate that:
        // - if system schema are included they are first in the list and ordered alphabetically
        // - any following application schema are also ordered alphabetically
        for (index, schema_id) in self.0.iter().enumerate() {
            // If the first schema id is an application schema then no system schema should be
            // included and we flip the `initial_system_schema` flag.
            if index == 0 {
                initial_system_schema = !matches!(schema_id, SchemaId::Application(_, _))
            }

            // Now validate the order.
            if let Some(prev) = prev_schema_id {
                match schema_id {
                    // If current and previous are application schema compare them.
                    SchemaId::Application(_, _) if !initial_system_schema => {
                        if prev >= schema_id {
                            return Err(SchemaIdSetError::UnsortedSchemaIds);
                        }
                    }
                    // If the current is an application schema and the previous is a system schema
                    // flip the `initial_system_schema` flag.
                    SchemaId::Application(_, _) if initial_system_schema => {
                        initial_system_schema = false
                    }
                    // If the current is a system schema and the `initial_system_schema` flag is
                    // false then there is an out of order system schema.
                    _ if !initial_system_schema => {
                        return Err(SchemaIdSetError::UnsortedSchemaIds);
                    }
                    // If current and previous are both system schema then compare them.
                    _ if initial_system_schema => {
                        if prev >= schema_id {
                            return Err(SchemaIdSetError::UnsortedSchemaIds);
                        }
                    }
                    _ => panic!(),
                };
            }

            prev_schema_id = Some(schema_id);
        }

        Ok(())
    }
}

impl<'de> Deserialize<'de> for SchemaIdSet {
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
    use ciborium::cbor;
    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::schema::{SchemaId, SchemaName};
    use p2panda_rs::serde::{deserialize_into, serialize_value};
    use p2panda_rs::test_utils::fixtures::random_document_view_id;
    use rstest::rstest;

    use crate::test_utils::helpers::random_schema_id_set;

    use super::SchemaIdSet;

    #[rstest]
    fn compare_sets(
        #[from(random_document_view_id)] document_view_id_1: DocumentViewId,
        #[from(random_document_view_id)] document_view_id_2: DocumentViewId,
    ) {
        let schema_id_1 =
            SchemaId::new_application(&SchemaName::new("messages").unwrap(), &document_view_id_1);
        let schema_id_2 =
            SchemaId::new_application(&SchemaName::new("profiles").unwrap(), &document_view_id_2);

        // Sort schema ids
        assert_eq!(
            SchemaIdSet::new(&[schema_id_1.clone(), schema_id_2.clone()]),
            SchemaIdSet::new(&[schema_id_2.clone(), schema_id_1.clone()]),
        );

        // De-duplicate schema ids
        assert_eq!(
            SchemaIdSet::new(&[schema_id_1.clone(), schema_id_1.clone()]),
            SchemaIdSet::new(&[schema_id_1.clone()]),
        );

        // Distinct different target sets from each other
        assert_ne!(
            SchemaIdSet::new(&[schema_id_1.clone()]),
            SchemaIdSet::new(&[schema_id_2.clone()]),
        );
    }

    #[rstest]
    fn calculate_intersection(
        #[from(random_document_view_id)] document_view_id_1: DocumentViewId,
        #[from(random_document_view_id)] document_view_id_2: DocumentViewId,
        #[from(random_document_view_id)] document_view_id_3: DocumentViewId,
    ) {
        // Correctly calculates intersections
        let schema_id_1 =
            SchemaId::new_application(&SchemaName::new("messages").unwrap(), &document_view_id_1);
        let schema_id_2 =
            SchemaId::new_application(&SchemaName::new("profiles").unwrap(), &document_view_id_2);
        let schema_id_3 =
            SchemaId::new_application(&SchemaName::new("events").unwrap(), &document_view_id_3);

        let set_1 = SchemaIdSet::new(&[schema_id_1.clone(), schema_id_2.clone()]);
        let set_2 = SchemaIdSet::new(&[schema_id_3.clone(), schema_id_2.clone()]);

        assert_eq!(
            SchemaIdSet::from_intersection(&set_1, &set_2),
            SchemaIdSet::new(&[schema_id_2.clone()])
        );

        // Correctly verifies if both target sets know about all given elements
        assert!(SchemaIdSet::new(&[schema_id_2.clone()])
            .is_valid_set(&SchemaIdSet::new(&[schema_id_2.clone()])));
        assert!(
            SchemaIdSet::new(&[schema_id_3.clone(), schema_id_2.clone()])
                .is_valid_set(&SchemaIdSet::new(&[schema_id_2.clone()]))
        );
        assert!(
            !SchemaIdSet::new(&[schema_id_1.clone()]).is_valid_set(&SchemaIdSet::new(&[
                schema_id_2.clone(),
                schema_id_1.clone()
            ]))
        );
    }

    #[rstest]
    #[case(vec![
        "venues_0020c13cdc58dfc6f4ebd32992ff089db79980363144bdb2743693a019636fa72ec8".to_string(),
        "alpacas_00202dce4b32cd35d61cf54634b93a526df333c5ed3d93230c2f026f8d1ecabc0cd7".to_string(),
    ])]
    #[case(vec![
        "alpacas_00202dce4b32cd35d61cf54634b93a526df333c5ed3d93230c2f026f8d1ecabc0cd7".to_string(),
        "schema_field_definition_v1".to_string(),
    ])]
    #[case(vec![
        "schema_field_definition_v1".to_string(),
        "schema_definition_v1".to_string(),
    ])]
    #[case(vec![
        "schema_definition_v1".to_string(),
        "alpacas_00202dce4b32cd35d61cf54634b93a526df333c5ed3d93230c2f026f8d1ecabc0cd7".to_string(),
        "schema_field_definition_v1".to_string(),
    ])]
    fn deserialize_unsorted_set(#[case] schema_ids: Vec<String>) {
        let result = deserialize_into::<SchemaIdSet>(&serialize_value(cbor!(schema_ids)));

        let expected_result = ciborium::de::Error::<std::io::Error>::Semantic(
            None,
            "Set contains unsorted or duplicate schema ids".to_string(),
        );
        assert_eq!(result.unwrap_err().to_string(), expected_result.to_string());
    }

    #[rstest]
    fn serialize(#[from(random_schema_id_set)] set: SchemaIdSet) {
        assert_eq!(
            deserialize_into::<SchemaIdSet>(&serialize_value(cbor!(set))).unwrap(),
            set.clone()
        );
    }
}
