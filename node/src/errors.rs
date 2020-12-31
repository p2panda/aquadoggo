use validator::{ValidationErrors, ValidationErrorsKind};

/// A specialized `Result` type for the node.
pub type Result<T> = anyhow::Result<T, Error>;

/// Represents all the ways a method can fail within the node.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Error returned from validating data types.
    #[error(transparent)]
    Validation(#[from] ValidationErrors),
    /// Error returned from the database.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    /// Error returned from JSON RPC API.
    #[error(transparent)]
    RPC(#[from] jsonrpc_core::Error),
}

impl From<Error> for jsonrpc_core::Error {
    fn from(error: Error) -> Self {
        match error {
            Error::RPC(rpc_error) => rpc_error,
            Error::Validation(validation_errors) => {
                let message = validation_errors
                    .errors()
                    .iter()
                    .map(|error_kind| match error_kind.1 {
                        ValidationErrorsKind::Struct(struct_err) => {
                            validation_errs_to_str_vec(struct_err).join(", ")
                        }
                        ValidationErrorsKind::Field(field_errs) => field_errs
                            .iter()
                            .map(|fe| format!("{}", fe.code))
                            .collect::<Vec<String>>()
                            .join(", "),
                        ValidationErrorsKind::List(vec_errs) => vec_errs
                            .iter()
                            .map(|ve| validation_errs_to_str_vec(ve.1).join(", ").to_string())
                            .collect::<Vec<String>>()
                            .join(", "),
                    })
                    .collect::<Vec<String>>()
                    .join(", ");

                jsonrpc_core::Error {
                    code: jsonrpc_core::ErrorCode::InvalidParams,
                    message: format!("Invalid params: {}.", message),
                    data: None,
                }
            }
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

fn validation_errs_to_str_vec(errors: &ValidationErrors) -> Vec<String> {
    errors
        .field_errors()
        .iter()
        .map(|fe| {
            fe.1.iter()
                .map(|ve| format!("{}", ve.code))
                .collect::<Vec<String>>()
                .join(", ")
        })
        .collect()
}
