use serde::{Deserialize, Serialize};
use sqlx::Type;
use validator::{Validate, ValidationError, ValidationErrors};

use crate::errors::Result;

// CDDL Schema
const MESSAGE_SCHEMA: &str = r#"
    message = {
        message-context,
        $message-body,
    }

    message-context = {
        schema: message-schema,
        version: message-version,
    }

    message-schema = tstr .regexp "[0-9a-fA-F]{128}"
    message-version = 1

    $message-body /= {
        action: "create",
        fields: message-fields
    }

    $message-body /= {
        action: "update",
        fields: message-fields,
        id: uint,
    }

    $message-body /= {
        action: "delete",
        id: uint,
    }

    message-fields = {
        + message-label => message-value,
    }

    message-label = tstr
    message-value = any
"#;

#[derive(Type, Clone, Debug, Serialize, Deserialize)]
#[sqlx(transparent)]
pub struct EncodedMessage(String);

impl EncodedMessage {
    /// Validates and returns an encoded entry instance when correct.
    #[allow(dead_code)]
    pub fn new(value: &str) -> Result<Self> {
        let encoded_entry = Self(String::from(value));
        encoded_entry.validate()?;
        Ok(encoded_entry)
    }
}

impl Validate for EncodedMessage {
    fn validate(&self) -> anyhow::Result<(), ValidationErrors> {
        let mut errors = ValidationErrors::new();

        // Check if message is hex encoded
        match hex::decode(self.0.to_owned()) {
            Ok(bytes) => {
                if cddl::validate_cbor_from_slice(MESSAGE_SCHEMA, &bytes).is_err() {
                    errors.add(
                        "encoded_message",
                        ValidationError::new("invalid message schema"),
                    );
                }
            }
            Err(_) => {
                errors.add(
                    "encoded_message",
                    ValidationError::new("invalid hex string"),
                );
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::EncodedMessage;

    #[test]
    fn validate() {
        // Invalid hex string
        assert!(EncodedMessage::new("123456789Z").is_err());

        // Invalid CBOR
        assert!(EncodedMessage::new("68656c6c6f2062616d626f6f21").is_err());
    }
}
