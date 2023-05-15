// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::{self, Display};

use p2panda_rs::{schema::SchemaId, Human, Validate};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub mod errors;
mod manager;
mod message;
mod session;
mod strategies;
pub mod traits;

pub use manager::SyncManager;
pub use message::{Message, StrategyMessage, SyncMessage};
pub use session::{Session, SessionId, SessionState};
pub use strategies::{NaiveStrategy, SetReconciliationStrategy, StrategyResult};

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

    fn from_untrusted(schema_ids: Vec<SchemaId>) -> Result<Self, errors::TargetSetError> {
        // Create target set with potentially invalid data
        let target_set = Self(schema_ids);

        // Make sure its sorted and does not contain any duplicates
        target_set.validate()?;

        Ok(target_set)
    }
}

impl Validate for TargetSet {
    type Error = errors::TargetSetError;

    fn validate(&self) -> Result<(), Self::Error> {
        // Check if at least one schema id is given
        if self.0.is_empty() {
            return Err(errors::TargetSetError::ZeroSchemaIds);
        };

        let mut prev_schema_id: Option<&SchemaId> = None;

        for schema_id in &self.0 {
            // Check if it is sorted, this indirectly also checks against duplicates
            if let Some(prev) = prev_schema_id {
                if prev >= schema_id {
                    return Err(errors::TargetSetError::UnsortedSchemaIds);
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mode {
    Naive,
    SetReconciliation,
    Unknown,
}

impl Mode {
    pub fn as_str(&self) -> &str {
        match self {
            Mode::Naive => "naive",
            Mode::SetReconciliation => "set-reconciliation",
            Mode::Unknown => "unknown",
        }
    }

    pub fn as_u64(&self) -> u64 {
        match self {
            Mode::Naive => 0,
            Mode::SetReconciliation => 1,
            Mode::Unknown => unreachable!("Can't create an unknown replication mode"),
        }
    }
}

impl From<u64> for Mode {
    fn from(value: u64) -> Self {
        match value {
            0 => Mode::Naive,
            1 => Mode::SetReconciliation,
            _ => Mode::Unknown,
        }
    }
}

impl Serialize for Mode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.as_u64())
    }
}

impl<'de> Deserialize<'de> for Mode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mode = u64::deserialize(deserializer)?;
        Ok(mode.into())
    }
}

impl Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_u64())
    }
}

impl Human for Mode {
    fn display(&self) -> String {
        self.as_str().to_owned()
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
