// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use anyhow::{anyhow, Error};
use async_graphql::{ComplexObject, Context, SimpleObject};
use p2panda_rs::entry::{decode_entry, EntrySigned};
use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore};
use serde::{Deserialize, Serialize};

use crate::db::provider::SqlStorage;
use crate::db::stores::StorageEntry;
use crate::graphql::scalars;

/// Encoded and signed entry with its regarding encoded operation payload.
#[derive(SimpleObject, Serialize, Deserialize, Debug, Eq, PartialEq)]
#[graphql(complex)]
pub struct EncodedEntryAndOperation {
    /// Signed and encoded bamboo entry.
    pub entry: scalars::EntrySignedScalar,

    /// p2panda operation, CBOR bytes encoded as hexadecimal string.
    pub operation: Option<scalars::EncodedOperationScalar>,
}

#[ComplexObject]
impl EncodedEntryAndOperation {
    /// Get the certificate pool for this entry that can be used to verify the entry is valid.
    async fn certificate_pool<'a>(
        &self,
        ctx: &Context<'a>,
    ) -> async_graphql::Result<Vec<scalars::EntrySignedScalar>> {
        let store = ctx.data::<SqlStorage>()?;

        // Decode entry
        let entry_encoded: EntrySigned = self.entry.clone().into();
        let entry = decode_entry(&entry_encoded, None)?;

        // Load certificate pool from database
        let result = store
            .get_certificate_pool(&entry_encoded.author(), entry.log_id(), entry.seq_num())
            .await?;

        let entries = result
            .into_iter()
            .map(|entry| entry.entry_signed().clone().into())
            .collect();

        Ok(entries)
    }
}

impl From<StorageEntry> for EncodedEntryAndOperation {
    fn from(entry_row: StorageEntry) -> Self {
        let entry = entry_row.entry_signed().to_owned().into();
        let operation = entry_row.operation_encoded().map(|op| op.to_owned().into());
        Self { entry, operation }
    }
}

impl TryFrom<EncodedEntryAndOperation> for StorageEntry {
    type Error = Error;

    fn try_from(encoded: EncodedEntryAndOperation) -> anyhow::Result<StorageEntry> {
        let operation = encoded
            .operation
            .ok_or_else(|| anyhow!("Storage entry requires operation to be given"))?;

        Ok(StorageEntry::new(&encoded.entry.into(), &operation.into())?)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use async_graphql::{EmptyMutation, EmptySubscription, Request, Schema};
    use bamboo_rs_core_ed25519_yasmf::verify_batch;
    use p2panda_rs::entry::{EntrySigned, LogId};
    use p2panda_rs::identity::Author;
    use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore};
    use rstest::rstest;

    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
    use crate::graphql::replication::ReplicationRoot;

    #[rstest]
    fn validate_with_certificate_pool(
        #[from(test_db)]
        #[with(13, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let replication_root = ReplicationRoot::default();
            let schema = Schema::build(replication_root, EmptyMutation, EmptySubscription)
                .data(db.store.clone())
                .finish();

            // Retreive last entry of author from test database
            let author: Author = db
                .test_data
                .key_pairs
                .first()
                .unwrap()
                .public_key()
                .to_owned()
                .try_into()
                .unwrap();

            let latest_entry_hash = db
                .store
                .get_latest_entry(&author, &LogId::default())
                .await
                .unwrap()
                .unwrap()
                .hash();

            // Make GraphQL query
            let gql_query = format!(
                r#"
                    query {{
                        entryByHash(hash: "{}") {{
                            certificatePool
                        }}
                    }}
                "#,
                latest_entry_hash.as_str()
            );

            let result = schema.execute(Request::new(gql_query.clone())).await;
            assert!(result.is_ok(), "{:?}", result);

            // Extract data from result
            let json = result.data.into_json().unwrap();
            let entries = json
                .get("entryByHash")
                .unwrap()
                .get("certificatePool")
                .unwrap()
                .as_array()
                .unwrap();

            // Prepare entries and batch-validate them
            let entries_to_verify: Vec<(Vec<u8>, Option<Vec<u8>>)> = entries
                .iter()
                .map(|entry| {
                    let entry = EntrySigned::new(entry.as_str().unwrap()).unwrap();
                    (entry.to_bytes(), None)
                })
                .collect();

            // Make sure we can validate single entry based on the certificate pool
            assert!(verify_batch(&entries_to_verify).is_ok());
        });
    }
}
