// SPDX-License-Identifier: AGPL-3.0-or-later

use thiserror::Error;

#[derive(Error, Debug)]
pub enum TempFileError {
    /// Saving is aborted when an existing temp file is found at the target filename.
    #[error("Temp file already exists.")]
    ExistingTempFile,

    /// Errors from file handling.
    #[error(transparent)]
    File(#[from] std::io::Error),

    /// Errors from serialisation
    #[error("Serialisation failed: {0}.")]
    Serialisation(String),
}

#[derive(Error, Debug)]
pub enum DynamicSchemaError {
    /// Error from helper method used for making graphql schemas from p2panda schemas.
    #[error(transparent)]
    TempFile(#[from] TempFileError),
}
