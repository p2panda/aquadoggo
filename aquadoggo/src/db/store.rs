// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use sqlx::{query, query_as, query_scalar};

use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::SeqNum;
use p2panda_rs::hash::Hash;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::errors as p2panda_errors;
use p2panda_rs::storage_provider::traits::{
    AsStorageEntry, AsStorageLog, EntryStore, LogStore, StorageProvider,
};
use p2panda_rs::{entry::LogId, identity::Author};

use crate::db::models::{EntryRow, Log};
use crate::db::Pool;
use crate::errors::StorageProviderResult;
use crate::rpc::{EntryArgsRequest, EntryArgsResponse, PublishEntryRequest, PublishEntryResponse};

pub struct SqlStorage {
    pub(crate) pool: Pool,
}
