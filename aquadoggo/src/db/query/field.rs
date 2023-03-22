// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use anyhow::bail;
use p2panda_rs::schema::FieldName;

/// Pre-defined constant fields for every document.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetaField {
    /// Identifier of the document.
    DocumentId,

    /// Latest version of the document.
    DocumentViewId,

    /// Public key of the original author (owner) of the document.
    Owner,

    /// Flag indicating if document was edited at least once.
    Edited,

    /// Flag indicating if document was deleted.
    Deleted,
}

impl TryFrom<&str> for MetaField {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "documentId" => Ok(MetaField::DocumentId),
            "viewId" => Ok(MetaField::DocumentViewId),
            "owner" => Ok(MetaField::Owner),
            "edited" => Ok(MetaField::Edited),
            "deleted" => Ok(MetaField::Deleted),
            _ => bail!("Unknown meta field"),
        }
    }
}

impl ToString for MetaField {
    fn to_string(&self) -> String {
        match self {
            MetaField::DocumentId => "documentId",
            MetaField::DocumentViewId => "viewId",
            MetaField::Owner => "owner",
            MetaField::Edited => "edited",
            MetaField::Deleted => "deleted",
        }
        .to_string()
    }
}

/// Fields can be either defined by the regarding schema (application fields) or are constants
/// (meta fields) like the identifier of the document itself.
///
/// Fields can be selected, ordered or filtered. Use the regarding structs to apply settings on top
/// of these fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Field {
    /// Pre-defined, constant fields for every document.
    Meta(MetaField),

    /// Field defined by the schema.
    Field(FieldName),
}

impl Field {
    /// Returns a new application field.
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
