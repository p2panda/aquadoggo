// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::schema::FieldName;

#[derive(Debug, Clone, PartialEq)]
pub enum MetaField {
    DocumentId,
    DocumentViewId,
    Owner,
    Edited,
    Deleted,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Field {
    Meta(MetaField),
    Field(FieldName),
}

impl Field {
    pub fn new(name: &str) -> Self {
        Self::Field(name.to_string())
    }
}

impl From<&str> for Field {
    fn from(value: &str) -> Self {
        Self::Field(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::Field;

    #[test]
    fn create_field_from_str() {
        let field: Field = "message".into();
        assert_eq!(field, Field::new("message"));
        assert_eq!(Field::Field("favorite".to_string()), Field::new("favorite"));
    }
}
