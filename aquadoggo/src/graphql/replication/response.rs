// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use anyhow::{anyhow, Error};
use async_graphql::{ComplexObject, Context, SimpleObject};
use p2panda_rs::entry::decode::decode_entry;
use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::storage_provider::traits::{EntryStore, EntryWithOperation};
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
        let entry_encoded: EncodedEntry = self.entry.clone().into();
        let entry = decode_entry(&entry_encoded)?;

        // Load certificate pool from database
        let result = store
            .get_certificate_pool(entry.public_key(), entry.log_id(), entry.seq_num())
            .await?;

        let entries = result
            .into_iter()
            .map(|entry| EncodedEntry::from_bytes(&entry.into_bytes()).into())
            .collect();

        Ok(entries)
    }
}

impl From<StorageEntry> for EncodedEntryAndOperation {
    fn from(storage_entry: StorageEntry) -> Self {
        let entry = EncodedEntry::from_bytes(&storage_entry.into_bytes()).into();
        let operation = storage_entry
            .payload()
            .map(|payload| payload.to_owned().into());
        Self { entry, operation }
    }
}

impl TryFrom<EncodedEntryAndOperation> for StorageEntry {
    type Error = Error;

    fn try_from(encoded: EncodedEntryAndOperation) -> anyhow::Result<StorageEntry> {
        let operation = encoded
            .operation
            .ok_or_else(|| anyhow!("Storage entry requires operation to be given"))?;
        let encoded_entry = encoded.entry;
        let entry = decode_entry(&encoded_entry.clone().into())?;

        let storage_entry = StorageEntry {
            author: entry.public_key().to_owned(),
            log_id: entry.log_id().to_owned(),
            seq_num: entry.seq_num().to_owned(),
            skiplink: entry.skiplink().cloned(),
            backlink: entry.backlink().cloned(),
            payload_size: entry.payload_size(),
            payload_hash: entry.payload_hash().to_owned(),
            signature: entry.signature().to_owned(),
            encoded_entry: encoded_entry.into(),
            payload: Some(operation.into()),
        };
        Ok(storage_entry)
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::{EmptyMutation, EmptySubscription, Request, Schema};
    use bamboo_rs_core_ed25519_yasmf::verify_batch;
    use p2panda_rs::entry::traits::AsEncodedEntry;
    use p2panda_rs::entry::LogId;
    use p2panda_rs::identity::Author;
    use p2panda_rs::storage_provider::traits::EntryStore;
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
            let author: Author = db.test_data.key_pairs.first().unwrap().public_key().into();

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
                .map(|entry| (hex::decode(entry.as_str().unwrap()).unwrap(), None))
                .collect();

            // Make sure we can validate single entry based on the certificate pool
            assert!(verify_batch(&entries_to_verify).is_ok());
        });
    }
}
