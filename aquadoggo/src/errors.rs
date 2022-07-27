// SPDX-License-Identifier: AGPL-3.0-or-later

/// A result type used in aquadoggo modules.
pub type Result<T> = anyhow::Result<T, Box<dyn std::error::Error + Send + Sync>>;
