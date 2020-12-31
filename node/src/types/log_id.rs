use serde::{Deserialize, Serialize};
use sqlx::Type;

use crate::errors::{Error, Result};

/// Authors can write to multiple logs identified by their log ids.
///
/// By specification the log id is an u64 integer but since this is not handled by our database
/// library (sqlx) we use the signed variant.
#[derive(Type, Clone, Debug, Serialize, Deserialize)]
#[sqlx(transparent)]
pub struct LogId(pub i64);

impl LogId {
    /// Validates and returns a new log id instance when correct.
    pub fn new(inner: i64) -> Result<Self> {
        let log_id = Self(inner);
        log_id.validate()?;
        Ok(log_id)
    }

    /// Checks if log id is valid.
    pub fn validate(&self) -> Result<()> {
        // Numbers have to be positive
        if self.0 < 0 {
            return Err(Error::Validation("`logId` can't be negative".to_owned()));
        }

        Ok(())
    }

    /// Determines the next odd log id given it already is one.
    pub fn next_user_log(&self) -> Result<Self> {
        if self.is_user_log() {
            Self::new(self.0 + 2)
        } else {
            Err(Error::Validation("Invalid user log id".to_owned()))
        }
    }

    /// Returns true when log id is zero.
    pub fn is_zero(&self) -> bool {
        self.0 == 0
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
        Self(1)
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
        let log_id = LogId::default();
        assert_eq!(log_id.is_user_log(), true);
        assert_eq!(log_id.is_system_log(), false);

        let next_log_id = log_id.next_user_log().unwrap();
        assert_eq!(next_log_id.0, 3);

        let next_log_id = next_log_id.next_user_log().unwrap();
        assert_eq!(next_log_id.0, 5);
    }

    #[test]
    fn system_log_ids() {
        let log_id = LogId::new(0).unwrap();
        assert_eq!(log_id.is_zero(), true);
        assert_eq!(log_id.is_user_log(), false);
        assert_eq!(log_id.is_system_log(), true);
        assert_eq!(log_id.next_user_log().is_err(), true);
    }
}
