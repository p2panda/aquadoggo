/// A specialized `Result` type for the node.
pub type Result<T> = anyhow::Result<T, Error>;

/// Represents all the ways a method can fail within the node.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Error returned from validating data types.
    #[error("Invalid params: {0}")]
    Validation(String),
    /// Error returned from the database.
    #[error("{0}")]
    Database(#[from] sqlx::Error),
    /// Error returned from JSON RPC API.
    #[error(transparent)]
    RPC(#[from] jsonrpc_core::Error),
}

impl From<Error> for jsonrpc_core::Error {
    fn from(error: Error) -> Self {
        match error {
            Error::RPC(rpc_error) => rpc_error,
            Error::Validation(validation_error) => jsonrpc_core::Error {
                code: jsonrpc_core::ErrorCode::InvalidParams,
                message: format!("Invalid params: {}", validation_error),
                data: None,
            },
            _ => {
                log::error!("{:#}", error);
                jsonrpc_core::Error {
                    code: jsonrpc_core::ErrorCode::InternalError,
                    message: "Internal server error".to_owned(),
                    data: Some(jsonrpc_core::Value::String(format!("{}", error))),
                }
            }
        }
    }
}
