// SPDX-License-Identifier: AGPL-3.0-or-later

/// A specialized result type for the storage provider.
pub type StorageProviderResult<T> = anyhow::Result<T, Box<dyn std::error::Error + Send + Sync>>;
