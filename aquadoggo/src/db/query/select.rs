// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::hash_set::Iter;
use std::collections::HashSet;

use crate::db::query::{Field, MetaField};

pub type ApplicationFields = Vec<String>;

/// Selection settings which can be used further to construct a database query.
///
/// A selection determines which fields get returned in the response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Select {
    pub fields: HashSet<Field>,
}

impl Select {
    /// Returns a new instance of selection settings.
    pub fn new(fields: &[Field]) -> Self {
        let mut select = Self {
            fields: HashSet::new(),
        };

        for field in fields {
            select.add(field);
        }

        select
    }

    /// Adds another field to the selection.
    ///
    /// This method makes sure that no duplicates are added.
    pub fn add(&mut self, field: &Field) {
        self.fields.insert(field.clone());
    }

    /// Returns the number of selected fields.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns an iterator over the selected fields.
    #[allow(dead_code)]
    pub fn iter(&self) -> Iter<Field> {
        self.fields.iter()
    }

    /// Returns all selected application fields from query.
    pub fn application_fields(&self) -> ApplicationFields {
        self.fields
            .iter()
            .filter_map(|field| {
                // Remove all meta fields
                if let Field::Field(field_name) = field {
                    Some(field_name.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

impl Default for Select {
    fn default() -> Self {
        Self::new(&[Field::Meta(MetaField::DocumentId)])
    }
}

#[cfg(test)]
mod tests {
    use crate::db::query::{Field, MetaField};

    use super::Select;

    #[test]
    fn create_select() {
        let mut select = Select::default();
        select.add(&"test".into());

        assert_eq!(
            select,
            Select::new(&[
                Field::Meta(MetaField::DocumentId),
                Field::Field("test".into()),
            ])
        );
    }

    #[test]
    fn avoid_duplicates() {
        let field: Field = "test".into();

        let mut select = Select::new(&[]);
        select.add(&field);
        select.add(&field);

        assert_eq!(select.len(), 1);
    }
}
