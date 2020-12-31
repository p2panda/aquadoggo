use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Type};
use validator::{Validate, ValidationError, ValidationErrors};

use crate::errors::Result;

/// Authors can write to multiple logs identified by their log ids.
///
/// By specification the log id is an u64 integer but since this is not handled by our database
/// library (sqlx) we use the signed variant.
#[derive(Type, FromRow, Clone, Debug, Serialize, Deserialize)]
#[sqlx(transparent)]
pub struct LogId(i64);

impl LogId {
    /// Validates and returns a new log id instance when correct.
    pub fn new(value: i64) -> Result<Self> {
        let log_id = Self(value);
        log_id.validate()?;
        Ok(log_id)
    }

    /// Returns true when log id is a user log (odd-numbered).
    pub fn is_user_log(&self) -> bool {
        self.0 % 2 == 1
    }

    /// Returns true when log id is a system log (even-numbered).
    #[allow(dead_code)]
    pub fn is_system_log(&self) -> bool {
        self.0 % 2 == 0
    }
}

impl Default for LogId {
    fn default() -> Self {
        // We never create system logs during runtime, the default value is therefore an odd user
        // log id.
        Self::new(1).unwrap()
    }
}

impl Validate for LogId {
    fn validate(&self) -> anyhow::Result<(), ValidationErrors> {
        let mut errors = ValidationErrors::new();

        // Numbers have to be positive
        if self.0 < 0 {
            errors.add("logId", ValidationError::new("`logId` can't be negative"));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl Iterator for LogId {
    type Item = LogId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_user_log() {
            Some(Self(self.0 + 2))
        } else {
            None
        }
    }
}

impl PartialEq for LogId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

#[cfg(test)]
mod tests {
    use super::LogId;

    #[test]
    fn validate() {
        assert!(LogId::new(-1).is_err());
        assert!(LogId::new(100).is_ok());
    }

    #[test]
    fn user_log_ids() {
        let mut log_id = LogId::default();
        assert_eq!(log_id.is_user_log(), true);
        assert_eq!(log_id.is_system_log(), false);

        let mut next_log_id = log_id.next().unwrap();
        assert_eq!(next_log_id, LogId::new(3).unwrap());

        let next_log_id = next_log_id.next().unwrap();
        assert_eq!(next_log_id, LogId::new(5).unwrap());
    }

    #[test]
    fn system_log_ids() {
        let mut log_id = LogId::new(0).unwrap();
        assert_eq!(log_id.is_user_log(), false);
        assert_eq!(log_id.is_system_log(), true);

        // Can't iterate on system logs
        assert!(log_id.next().is_none());
    }
}
