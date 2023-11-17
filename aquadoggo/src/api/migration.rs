// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
use p2panda_rs::api::publish;
use p2panda_rs::entry::decode::decode_entry;
use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
use p2panda_rs::operation::decode::decode_operation;
use p2panda_rs::operation::traits::Schematic;
use p2panda_rs::operation::OperationId;
use p2panda_rs::storage_provider::traits::{EntryStore, LogStore, OperationStore};

use crate::api::LockFile;
use crate::schema::SchemaProvider;

/// Utility method to publish multiple operations and entries in the node database.
///
/// Returns a list of operation ids which have been committed during this migration process. Can be
/// empty if no migration was required.
pub async fn migrate<S>(
    store: &S,
    schema_provider: &SchemaProvider,
    lock_file: LockFile,
) -> Result<Vec<OperationId>>
where
    S: OperationStore + EntryStore + LogStore,
{
    let mut committed_operations = Vec::new();
    let commits = lock_file.commits.unwrap_or_default();

    for commit in commits {
        let planned_entry = decode_entry(&commit.entry)
            .context("Invalid entry encoding encountered in lock file")?;

        let existing_entry = store
            .get_entry_at_seq_num(
                planned_entry.public_key(),
                planned_entry.log_id(),
                planned_entry.seq_num(),
            )
            .await
            .context("Internal database error occurred while retrieving entry")?;

        // Check if node already knows about this commit
        match existing_entry {
            Some(existing_entry) => {
                // Check its integrity with our lock file by comparing entry hashes
                if existing_entry.hash() != commit.entry_hash {
                    bail!("Integrity check failed when comparing planned and existing commit")
                }
            }
            None => {
                // .. otherwise publish the planned commit!
                let plain_operation = decode_operation(&commit.operation)
                    .context("Invalid operation encoding encountered in lock file")?;

                let schema = schema_provider
                    .get(plain_operation.schema_id())
                    .await
                    .ok_or_else(|| anyhow!("Could not migrate commit with unknown schema id"))?;

                publish(
                    store,
                    &schema,
                    &commit.entry,
                    &plain_operation,
                    &commit.operation,
                )
                .await
                .context("Internal database error occurred while publishing migration commit")?;

                committed_operations.push(commit.entry_hash.into());
            }
        };
    }

    Ok(committed_operations)
}
