// SPDX-License-Identifier: AGPL-3.0-or-later

use std::str::FromStr;

use sqlx::FromRow;

use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::LogId;
use p2panda_rs::identity::Author;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::AsStorageLog;

/// Tracks the assigment of an author's logs to documents and records their schema.
///
/// This serves as an indexing layer on top of the lower-level bamboo entries. The node updates
/// this data according to what it sees in the newly incoming entries.
///
/// We store the u64 integer values of `log_id` as a string here since not all database backends
/// support large numbers.
#[derive(FromRow, Debug, Clone)]
pub struct Log {
    /// Public key of the author.
    pub author: String,

    /// Log id used for this document.
    pub log_id: String,

    /// Hash that identifies the document this log is for.
    pub document: String,

    /// SchemaId which identifies the schema for operations in this log.
    pub schema: String,
}

impl AsStorageLog for Log {
    fn new(author: &Author, schema: &SchemaId, document: &DocumentId, log_id: &LogId) -> Self {
        Self {
            author: author.as_str().to_string(),
            log_id: log_id.as_u64().to_string(),
            document: document.as_str().to_string(),
            schema: schema.as_str(),
        }
    }

    fn author(&self) -> Author {
        Author::new(&self.author).unwrap()
    }
    fn id(&self) -> LogId {
        LogId::from_str(&self.log_id).unwrap()
    }
    fn document_id(&self) -> DocumentId {
        let document_id: DocumentId = self.document.parse().unwrap();
        document_id
    }
    fn schema_id(&self) -> SchemaId {
        SchemaId::new(&self.schema).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::entry::{sign_and_encode, Entry as P2PandaEntry, LogId, SeqNum};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{Operation, OperationEncoded, OperationFields, OperationValue};
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::traits::{
        AsStorageEntry, AsStorageLog, EntryStore, LogStore, StorageProvider,
    };

    use crate::db::models::{EntryRow, Log};
    use crate::db::sql_storage::SqlStorage;
    use crate::test_helpers::{initialize_db, random_entry_hash};

    const TEST_AUTHOR: &str = "58223678ab378f1b07d1d8c789e6da01d16a06b1a4d17cc10119a0109181156c";

    #[tokio::test]
    async fn initial_log_id() {
        let pool = initialize_db().await;

        let author = Author::new(TEST_AUTHOR).unwrap();

        let storage_provider = SqlStorage { pool };

        let log_id = storage_provider
            .find_document_log_id(&author, None)
            .await
            .unwrap();

        assert_eq!(log_id, LogId::new(1));
    }

    #[tokio::test]
    async fn prevent_duplicate_log_ids() {
        let pool = initialize_db().await;
        let storage_provider = SqlStorage { pool };

        let author = Author::new(TEST_AUTHOR).unwrap();
        let document = Hash::new(&random_entry_hash()).unwrap();
        let schema =
            SchemaId::new_application("venue", &Hash::new(&random_entry_hash()).unwrap().into());

        let log = Log::new(&author, &schema, &document.clone().into(), &LogId::new(1));
        assert!(storage_provider.insert_log(log).await.is_ok());

        let log = Log::new(&author, &schema, &document.into(), &LogId::new(1));
        assert!(storage_provider.insert_log(log).await.is_err());
    }

    #[tokio::test]
    async fn with_multi_hash_schema_id() {
        let pool = initialize_db().await;
        let storage_provider = SqlStorage { pool };

        let author = Author::new(TEST_AUTHOR).unwrap();
        let document = Hash::new(&random_entry_hash()).unwrap();
        let schema = SchemaId::new_application(
            "venue",
            &DocumentViewId::new(&[
                Hash::new(&random_entry_hash()).unwrap().into(),
                Hash::new(&random_entry_hash()).unwrap().into(),
            ]),
        );

        let log = Log::new(&author, &schema, &document.into(), &LogId::new(1));

        assert!(storage_provider.insert_log(log).await.is_ok());
    }

    #[tokio::test]
    async fn selecting_next_log_id() {
        let pool = initialize_db().await;
        let key_pair = KeyPair::new();
        let author = Author::try_from(*key_pair.public_key()).unwrap();
        let schema = SchemaId::new_application(
            "venue",
            &Hash::new_from_bytes(vec![1, 2, 3]).unwrap().into(),
        );

        let storage_provider = SqlStorage { pool };

        let log_id = storage_provider
            .find_document_log_id(&author, None)
            .await
            .unwrap();

        // We expect to be given the next log id when asking for a possible log id for a new
        // document by the same author
        assert_eq!(log_id, LogId::default());

        // Starting with an empty db, we expect to be able to count up from 1 and expect each
        // inserted document's log id to be euqal to the count index
        for n in 1..12 {
            let doc = Hash::new_from_bytes(vec![1, 2, n]).unwrap().into();

            let log_id = storage_provider
                .find_document_log_id(&author, None)
                .await
                .unwrap();
            assert_eq!(LogId::new(n.into()), log_id);
            let log = Log::new(&author, &schema, &doc, &log_id);
            storage_provider.insert_log(log).await.unwrap();
        }
    }

    #[tokio::test]
    async fn document_log_id() {
        let pool = initialize_db().await;

        // Create a new document
        // TODO: use p2panda-rs test utils once available
        let key_pair = KeyPair::new();
        let author = Author::try_from(*key_pair.public_key()).unwrap();
        let log_id = LogId::new(1);
        let schema = SchemaId::new_application(
            "venue",
            &Hash::new_from_bytes(vec![1, 2, 3]).unwrap().into(),
        );
        let seq_num = SeqNum::new(1).unwrap();
        let mut fields = OperationFields::new();
        fields
            .add("test", OperationValue::Text("Hello".to_owned()))
            .unwrap();
        let operation = Operation::new_create(schema.clone(), fields).unwrap();
        let operation_encoded = OperationEncoded::try_from(&operation).unwrap();
        let entry = P2PandaEntry::new(&log_id, Some(&operation), None, None, &seq_num).unwrap();
        let entry_encoded = sign_and_encode(&entry, &key_pair).unwrap();

        let storage_provider = SqlStorage { pool };

        // Expect database to return nothing yet
        assert_eq!(
            storage_provider
                .get_document_by_entry(&entry_encoded.hash())
                .await
                .unwrap(),
            None
        );

        let entry = EntryRow::new(&entry_encoded.clone(), &operation_encoded).unwrap();

        // Store entry in database
        assert!(storage_provider.insert_entry(entry).await.is_ok());

        let log = Log::new(
            &author,
            &schema,
            &entry_encoded.hash().into(),
            &LogId::new(1),
        );

        // Store log in database
        assert!(storage_provider.insert_log(log).await.is_ok());

        // Expect to find document in database. The document hash should be the same as the hash of
        // the entry which referred to the `CREATE` operation.
        assert_eq!(
            storage_provider
                .get_document_by_entry(&entry_encoded.hash())
                .await
                .unwrap(),
            Some(entry_encoded.hash().into())
        );

        // We expect to find this document in the default log
        assert_eq!(
            storage_provider
                .find_document_log_id(&author, Some(&entry_encoded.hash().into()))
                .await
                .unwrap(),
            LogId::default()
        );
    }

    #[tokio::test]
    async fn log_ids() {
        let pool = initialize_db().await;

        // Mock author
        let author = Author::new(TEST_AUTHOR).unwrap();

        // Mock schema
        let schema =
            SchemaId::new_application("venue", &Hash::new(&random_entry_hash()).unwrap().into());

        // Mock four different document hashes
        let document_first = Hash::new(&random_entry_hash()).unwrap();
        let document_second = Hash::new(&random_entry_hash()).unwrap();
        let document_third = Hash::new(&random_entry_hash()).unwrap();
        let document_forth = Hash::new(&random_entry_hash()).unwrap();

        let storage_provider = SqlStorage { pool };

        // Register two log ids at the beginning
        let log_1 = Log::new(&author, &schema, &document_first.into(), &LogId::new(1));
        let log_2 = Log::new(&author, &schema, &document_second.into(), &LogId::new(2));

        storage_provider.insert_log(log_1).await.unwrap();
        storage_provider.insert_log(log_2).await.unwrap();

        // Find next free log id and register it
        let log_id = storage_provider.next_log_id(&author).await.unwrap();
        assert_eq!(log_id, LogId::new(3));

        let log_3 = Log::new(&author, &schema, &document_third.into(), &log_id);

        storage_provider.insert_log(log_3).await.unwrap();

        // Find next free log id and register it
        let log_id = storage_provider.next_log_id(&author).await.unwrap();
        assert_eq!(log_id, LogId::new(4));

        let log_4 = Log::new(&author, &schema, &document_forth.into(), &log_id);

        storage_provider.insert_log(log_4).await.unwrap();

        // Find next free log id
        let log_id = storage_provider.next_log_id(&author).await.unwrap();
        assert_eq!(log_id, LogId::new(5));
    }
}
