// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::entry::decode::decode_entry;
use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
use p2panda_rs::entry::{EncodedEntry, LogId, SeqNum, Signature};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::EncodedOperation;

use crate::db::models::EntryRow;

/// A signed entry and it's encoded operation. Entries are the lowest level data type on the
/// p2panda network, they are signed by authors and form bamboo append only logs. The operation is
/// an entries' payload, it contains the data mutations which authors publish.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StorageEntry {
    /// PublicKey of this entry.
    pub(crate) public_key: PublicKey,

    /// Used log for this entry.
    pub(crate) log_id: LogId,

    /// Sequence number of this entry.
    pub(crate) seq_num: SeqNum,

    /// Hash of skiplink Bamboo entry.
    pub(crate) skiplink: Option<Hash>,

    /// Hash of previous Bamboo entry.
    pub(crate) backlink: Option<Hash>,

    /// Byte size of payload.
    pub(crate) payload_size: u64,

    /// Hash of payload.
    pub(crate) payload_hash: Hash,

    /// Ed25519 signature of entry.
    pub(crate) signature: Signature,

    /// Encoded entry bytes.
    pub(crate) encoded_entry: EncodedEntry,

    /// Encoded entry bytes.
    pub(crate) payload: Option<EncodedOperation>,
}

impl StorageEntry {
    // We will need this again as soon as we implement a new replication protocol
    #[allow(dead_code)]
    pub fn payload(&self) -> Option<&EncodedOperation> {
        self.payload.as_ref()
    }
}

impl AsEntry for StorageEntry {
    /// Returns public key of entry.
    fn public_key(&self) -> &PublicKey {
        &self.public_key
    }

    /// Returns log id of entry.
    fn log_id(&self) -> &LogId {
        &self.log_id
    }

    /// Returns sequence number of entry.
    fn seq_num(&self) -> &SeqNum {
        &self.seq_num
    }

    /// Returns hash of skiplink entry when given.
    fn skiplink(&self) -> Option<&Hash> {
        self.skiplink.as_ref()
    }

    /// Returns hash of backlink entry when given.
    fn backlink(&self) -> Option<&Hash> {
        self.backlink.as_ref()
    }

    /// Returns payload size of operation.
    fn payload_size(&self) -> u64 {
        self.payload_size
    }

    /// Returns payload hash of operation.
    fn payload_hash(&self) -> &Hash {
        &self.payload_hash
    }

    /// Returns signature of entry.
    fn signature(&self) -> &Signature {
        &self.signature
    }
}

impl AsEncodedEntry for StorageEntry {
    /// Generates and returns hash of encoded entry.
    fn hash(&self) -> Hash {
        self.encoded_entry.hash()
    }

    /// Returns entry as bytes.
    fn into_bytes(&self) -> Vec<u8> {
        self.encoded_entry.into_bytes()
    }

    /// Returns payload size (number of bytes) of total encoded entry.
    fn size(&self) -> u64 {
        self.encoded_entry.size()
    }
}

/// `From` implementation for converting an `EntryRow` into a `StorageEntry`. This is needed when
/// retrieving entries from the database. The `sqlx` crate coerces returned entry rows into
/// `EntryRow` but we want them as `StorageEntry` which contains typed values.
impl From<EntryRow> for StorageEntry {
    fn from(entry_row: EntryRow) -> Self {
        let encoded_entry = EncodedEntry::from_bytes(
            &hex::decode(entry_row.entry_bytes)
                .expect("Decode entry hex entry bytes from database"),
        );
        let entry = decode_entry(&encoded_entry).expect("Decoding encoded entry from database");
        StorageEntry {
            public_key: entry.public_key().to_owned(),
            log_id: entry.log_id().to_owned(),
            seq_num: entry.seq_num().to_owned(),
            skiplink: entry.skiplink().cloned(),
            backlink: entry.backlink().cloned(),
            payload_size: entry.payload_size(),
            payload_hash: entry.payload_hash().to_owned(),
            signature: entry.signature().to_owned(),
            encoded_entry,
            payload: entry_row.payload_bytes.map(|payload| {
                EncodedOperation::from_bytes(
                    &hex::decode(payload).expect("Decode entry payload from database"),
                )
            }),
        }
    }
}
