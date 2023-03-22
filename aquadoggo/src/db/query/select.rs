// SPDX-License-Identifier: AGPL-3.0-or-later

use std::slice::Iter;

use crate::db::query::{Field, MetaField};

#[derive(Clone, Debug, PartialEq)]
pub struct Select {
    fields: Vec<Field>,
}

impl Select {
    pub fn new(fields: &[Field]) -> Self {
        Self {
            fields: fields.to_vec(),
        }
    }

    pub fn add(mut self, field: &Field) -> Self {
        self.fields.push(field.clone());
        self
    }

    pub fn iter(&self) -> Iter<Field> {
        self.fields.iter()
    }
}

impl Default for Select {
    fn default() -> Self {
        Self {
            fields: vec![Field::Meta(MetaField::DocumentId)],
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::db::query::{Field, MetaField};

    use super::Select;

    #[test]
    fn create_select() {
        let select = Select::default().add(&"test".into());

        assert_eq!(
            select,
            Select::new(&[
                Field::Meta(MetaField::DocumentId),
                Field::Field("test".into()),
            ])
        );
    }
}
