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
    pub fn new(name: &FieldName) -> Self {
        Self::Field(name.to_string())
    }

    pub fn new_meta(meta: MetaField) -> Self {
        Self::Meta(meta)
    }
}

impl From<&str> for Field {
    fn from(value: &str) -> Self {
        Self::Field(value.to_string())
    }
}
