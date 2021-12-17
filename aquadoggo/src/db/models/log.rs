// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::entry::LogId;
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use sqlx::{query, query_as, FromRow};

use crate::db::Pool;
use crate::errors::Result;

/// Tracks the assigment of an author's logs to documents and records their schema.
///
/// This serves as an indexing layer on top of the lower-level bamboo entries. The node updates
/// this data according to what it sees in the newly incoming entries.
#[derive(FromRow, Debug)]
pub struct Log {
    /// Public key of the author.
    author: Author,

    /// Log id used for this document.
    log_id: LogId,

    /// Hash that identifies the document this log is for.
    document: Hash,

    /// Schema hash used by author.
    schema: Hash,
}

impl Log {
    /// Register any new log_id for a document and author.
    ///
    /// The database will reject duplicate entries.
    pub async fn insert(
        pool: &Pool,
        author: &Author,
        document: &Hash,
        schema: &Hash,
        log_id: &LogId,
    ) -> Result<bool> {
        let rows_affected = query(
            "
            INSERT INTO
                logs (author, log_id, document, schema)
            VALUES
                ($1, $2, $3, $4)
            ",
        )
        .bind(author)
        .bind(log_id)
        .bind(document)
        .bind(schema)
        .execute(pool)
        .await?
        .rows_affected();

        Ok(rows_affected == 1)
    }

    /// Determines the next unused log_id of an author.
    pub async fn next_log_id(pool: &Pool, author: &Author) -> Result<LogId> {
        // Get all log ids from this author
        let log_ids: Vec<LogId> = query_as::<_, LogId>(
            "
            SELECT
                log_id
            FROM
                logs
            WHERE
                author = $1
            ORDER BY
                log_id ASC
            ",
        )
        .bind(author)
        .fetch_all(pool)
        .await?;

        // Find next unused schema log_id by comparing the sequence of known log ids with an
        // sequence of subsequent log ids until we find a gap.
        let mut next_log_id = LogId::default();

        for log_id in log_ids.iter() {
            // Success! Found unused log id
            if next_log_id != *log_id {
                break;
            }

            // Otherwise, try next possible log id
            next_log_id = next_log_id.next().unwrap();
        }

        Ok(next_log_id)
    }

    /// Returns the registered log_id for a document identified by its hash.
    ///
    /// Operations are separated in different logs per document and author. This method checks if a
    /// log has already been registered for a document and author and returns its regarding log id
    /// or None.
    pub async fn get(pool: &Pool, author: &Author, document_hash: &Hash) -> Result<Option<LogId>> {
        let result = query_as::<_, LogId>(
            "
            SELECT
                log_id
            FROM
                logs
            WHERE
                author = $1
                AND document = $2
            ",
        )
        .bind(author)
        .bind(document_hash)
        .fetch_optional(pool)
        .await?;

        Ok(result)
    }

    /// Returns registered or possible log id for a document.
    ///
    /// If no log has been previously registered for this document it automatically returns the
    /// next unused log_id.
    pub async fn find_document_log_id(
        pool: &Pool,
        author: &Author,
        document_hash: Option<&Hash>,
    ) -> Result<LogId> {
        // Determine log_id for this document when a hash was given
        let document_log_id = match document_hash {
            Some(hash) => Log::get(pool, author, hash).await?,
            None => None,
        };

        // Use result or find next possible log_id automatically when nothing was found yet
        let log_id = match document_log_id {
            Some(value) => value,
            None => Log::next_log_id(pool, author).await?,
        };

        Ok(log_id)
    }

    /// Returns the related document for any entry.
    ///
    /// Every entry is part of a document and, through that, associated with a specific log id used
    /// by this document and author. This method returns that document by looking up the log id
    /// that the document's last operation was stored in.
    pub async fn get_document_by_entry(pool: &Pool, entry_hash: &Hash) -> Result<Option<Hash>> {
        let result = query_as::<_, Hash>(
            "
            SELECT
                logs.document
            FROM
                logs
            INNER JOIN entries
                ON (logs.log_id = entries.log_id
                    AND logs.author = entries.author)
            WHERE
                entries.entry_hash = $1
            ",
        )
        .bind(entry_hash)
        .fetch_optional(pool)
        .await?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::entry::{sign_and_encode, Entry, LogId, SeqNum};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{Operation, OperationEncoded, OperationFields, OperationValue};

    use crate::db::models::Entry as dbEntry;
    use crate::test_helpers::{initialize_db, random_entry_hash};

    use super::Log;

    const TEST_AUTHOR: &str = "58223678ab378f1b07d1d8c789e6da01d16a06b1a4d17cc10119a0109181156c";

    #[async_std::test]
    async fn initial_log_id() {
        let pool = initialize_db().await;

        let author = Author::new(TEST_AUTHOR).unwrap();

        let log_id = Log::find_document_log_id(&pool, &author, None)
            .await
            .unwrap();

        assert_eq!(log_id, LogId::new(1));
    }

    #[async_std::test]
    async fn prevent_duplicate_log_ids() {
        let pool = initialize_db().await;

        let author = Author::new(TEST_AUTHOR).unwrap();
        let document = Hash::new(&random_entry_hash()).unwrap();
        let schema = Hash::new(&random_entry_hash()).unwrap();

        assert!(
            Log::insert(&pool, &author, &document, &schema, &LogId::new(1))
                .await
                .is_ok()
        );

        assert!(
            Log::insert(&pool, &author, &document, &schema, &LogId::new(1))
                .await
                .is_err()
        );
    }

    #[async_std::test]
    async fn document_log_id() {
        let pool = initialize_db().await;

        // Create a new document
        // TODO: use p2panda-rs test utils once available
        let key_pair = KeyPair::new();
        let author = Author::try_from(key_pair.public_key().clone()).unwrap();
        let log_id = LogId::new(1);
        let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();
        let seq_num = SeqNum::new(1).unwrap();
        let mut fields = OperationFields::new();
        fields
            .add("test", OperationValue::Text("Hello".to_owned()))
            .unwrap();
        let operation = Operation::new_create(schema.clone(), fields).unwrap();
        let operation_encoded = OperationEncoded::try_from(&operation).unwrap();
        let entry = Entry::new(&log_id, Some(&operation), None, None, &seq_num).unwrap();
        let entry_encoded = sign_and_encode(&entry, &key_pair).unwrap();

        // Expect database to return nothing yet
        assert_eq!(
            Log::get_document_by_entry(&pool, &entry_encoded.hash())
                .await
                .unwrap(),
            None
        );

        // Store entry in database
        assert!(dbEntry::insert(
            &pool,
            &author,
            &entry_encoded,
            &entry_encoded.hash(),
            &log_id,
            &operation_encoded,
            &operation_encoded.hash(),
            &seq_num
        )
        .await
        .is_ok());

        // Store log in database
        assert!(
            Log::insert(&pool, &author, &entry_encoded.hash(), &schema, &log_id)
                .await
                .is_ok()
        );

        // Expect to find document in database. The document hash should be the same as the hash of
        // the entry which referred to the `CREATE` operation.
        assert_eq!(
            Log::get_document_by_entry(&pool, &entry_encoded.hash())
                .await
                .unwrap(),
            Some(entry_encoded.hash())
        );
    }

    #[async_std::test]
    async fn log_ids() {
        let pool = initialize_db().await;

        // Mock author
        let author = Author::new(TEST_AUTHOR).unwrap();

        // Mock schema
        let schema = Hash::new(&random_entry_hash()).unwrap();

        // Mock four different document hashes
        let document_first = Hash::new(&random_entry_hash()).unwrap();
        let document_second = Hash::new(&random_entry_hash()).unwrap();
        let document_third = Hash::new(&random_entry_hash()).unwrap();
        let document_system = Hash::new(&random_entry_hash()).unwrap();

        // Register two log ids at the beginning
        Log::insert(&pool, &author, &document_system, &schema, &LogId::new(1))
            .await
            .unwrap();
        Log::insert(&pool, &author, &document_first, &schema, &LogId::new(3))
            .await
            .unwrap();

        // Find next free log id and register it
        let log_id = Log::next_log_id(&pool, &author).await.unwrap();
        assert_eq!(log_id, LogId::new(2));
        Log::insert(&pool, &author, &document_second, &schema, &log_id)
            .await
            .unwrap();

        // Find next free log id and register it
        let log_id = Log::next_log_id(&pool, &author).await.unwrap();
        assert_eq!(log_id, LogId::new(4));
        Log::insert(&pool, &author, &document_third, &schema, &log_id)
            .await
            .unwrap();

        // Find next free log id
        let log_id = Log::next_log_id(&pool, &author).await.unwrap();
        assert_eq!(log_id, LogId::new(5));
    }
}
