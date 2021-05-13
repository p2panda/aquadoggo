use p2panda_rs::atomic::error::{
    AuthorError, EntryError, EntrySignedError, HashError, MessageEncodedError, MessageError,
};

/// A specialized `Result` type for the node.
pub type Result<T> = anyhow::Result<T, Error>;

/// Represents all the ways a method can fail within the node.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    // /// Error returned from JSON RPC API.
    // #[error(transparent)]
    // RPC(#[from] jsonrpc_core::Error),
    /// Error returned from validating p2panda-rs `Author` data types.
    #[error(transparent)]
    AuthorValidation(#[from] AuthorError),

    /// Error returned from validating p2panda-rs `Hash` data types.
    #[error(transparent)]
    HashValidation(#[from] HashError),

    /// Error returned from validating p2panda-rs `Entry` data types.
    #[error(transparent)]
    EntryValidation(#[from] EntryError),

    /// Error returned from validating p2panda-rs `EntrySigned` data types.
    #[error(transparent)]
    EntrySignedValidation(#[from] EntrySignedError),

    /// Error returned from validating p2panda-rs `Message` data types.
    #[error(transparent)]
    MessageValidation(#[from] MessageError),

    /// Error returned from validating p2panda-rs `MessageEncoded` data types.
    #[error(transparent)]
    MessageEncodedValidation(#[from] MessageEncodedError),

    /// Error returned from validating Bamboo entries.
    #[error(transparent)]
    BambooValidation(#[from] bamboo_rs_core::verify::Error),

    /// Error returned from `panda_publishEntry` RPC method.
    #[error(transparent)]
    PublishEntryValidation(#[from] crate::rpc::PublishEntryError),

    /// Error returned from the database.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

// impl From<Error> for jsonrpc_core::Error {
//     fn from(error: Error) -> Self {
//         match error {
//             Error::RPC(rpc_error) => rpc_error,
//             Error::AuthorValidation(validation_error) => {
//                 handle_validation_error(format!("{}", validation_error))
//             }
//             Error::HashValidation(validation_error) => {
//                 handle_validation_error(format!("{}", validation_error))
//             }
//             Error::EntryValidation(validation_error) => {
//                 handle_validation_error(format!("{}", validation_error))
//             }
//             Error::EntrySignedValidation(validation_error) => {
//                 handle_validation_error(format!("{}", validation_error))
//             }
//             Error::MessageValidation(validation_error) => {
//                 handle_validation_error(format!("{}", validation_error))
//             }
//             Error::MessageEncodedValidation(validation_error) => {
//                 handle_validation_error(format!("{}", validation_error))
//             }
//             Error::BambooValidation(validation_error) => {
//                 handle_validation_error(format!("{}", validation_error))
//             }
//             Error::PublishEntryValidation(validation_error) => {
//                 handle_validation_error(format!("{}", validation_error))
//             }
//             _ => {
//                 log::error!("{:#}", error);

//                 jsonrpc_core::Error {
//                     code: jsonrpc_core::ErrorCode::InternalError,
//                     message: "Internal server error".to_owned(),
//                     data: Some(jsonrpc_core::Value::String(format!("{}", error))),
//                 }
//             }
//         }
//     }
// }

// fn handle_validation_error(message: String) -> jsonrpc_core::Error {
//     jsonrpc_core::Error {
//         code: jsonrpc_core::ErrorCode::InvalidParams,
//         message: format!("Invalid params: {}.", message),
//         data: None,
//     }
// }
