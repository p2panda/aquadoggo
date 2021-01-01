use bamboo_core::lipmaa;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Type};
use validator::{Validate, ValidationError, ValidationErrors};

use crate::errors::Result;

/// Start counting sequence numbers from here.
const FIRST_SEQ_NUM: i64 = 1;

/// Bamboo append-only log sequence number.
#[derive(Type, FromRow, Clone, Debug, Serialize, Deserialize)]
#[sqlx(transparent)]
pub struct SeqNum(i64);

impl SeqNum {
    /// Validates and returns a new sequence number when correct.
    pub fn new(value: i64) -> Result<Self> {
        let seq_num = Self(value);
        seq_num.validate()?;
        Ok(seq_num)
    }

    /// Return sequence number of skiplink.
    pub fn skiplink_num(&self) -> Self {
        Self(lipmaa(self.0 as u64) as i64 + FIRST_SEQ_NUM)
    }
}

impl Default for SeqNum {
    fn default() -> Self {
        Self::new(FIRST_SEQ_NUM).unwrap()
    }
}

impl Validate for SeqNum {
    fn validate(&self) -> anyhow::Result<(), ValidationErrors> {
        let mut errors = ValidationErrors::new();

        // Numbers have to be larger than zero
        if self.0 < 1 {
            errors.add("logId", ValidationError::new("can't be zero or negative"));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl PartialEq for SeqNum {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

#[cfg(test)]
mod tests {
    use super::SeqNum;

    #[test]
    fn validate() {
        assert!(SeqNum::new(-1).is_err());
        assert!(SeqNum::new(0).is_err());
        assert!(SeqNum::new(100).is_ok());
    }

    #[test]
    fn skiplink_num() {
        assert_eq!(
            SeqNum::new(13).unwrap().skiplink_num(),
            SeqNum::new(5).unwrap()
        );
    }
}
