// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module offers a query API to find one or many p2panda documents, filtered or sorted by
//! custom parameters. Multiple results are paginated.
use std::collections::HashMap;
use std::num::NonZeroU64;

use libp2p::identity::PublicKey;
use p2panda_rs::document::Document;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::FieldName;

use crate::db::errors::QueryError;
use crate::db::SqlStore;

const DEFAULT_PAGE_SIZE: u64 = 10;

pub type Cursor = String;

#[derive(Debug, Clone)]
pub enum Direction {
    Ascending,
    Descending,
}

#[derive(Debug, Clone)]
pub enum OrderMeta {
    DocumentId,
}

#[derive(Debug, Clone)]
pub enum OrderField {
    Meta(OrderMeta),
    Field(FieldName),
}

#[derive(Debug, Clone)]
pub struct Order {
    field: OrderField,
    direction: Direction,
}

impl Default for Order {
    fn default() -> Self {
        Self {
            field: OrderField::Meta(OrderMeta::DocumentId),
            direction: Direction::Ascending,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Pagination {
    first: NonZeroU64,
    after: Option<Cursor>,
}

impl Pagination {
    pub fn new(first: NonZeroU64, after: Option<&Cursor>) -> Self {
        Self {
            first,
            after: after.cloned(),
        }
    }
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            // Unwrap here because we know that the default is non-zero
            first: NonZeroU64::new(DEFAULT_PAGE_SIZE).unwrap(),
            after: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilterMeta {
    public_keys: Option<Vec<PublicKey>>,
    edited: Option<bool>,
    deleted: Option<bool>,
}

impl FilterMeta {
    pub fn new() -> Self {
        Self {
            public_keys: None,
            edited: None,
            deleted: None,
        }
    }
}

impl Default for FilterMeta {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpperBound {
    Unbounded,
    Lower(OperationValue),
    LowerEqual(OperationValue),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LowerBound {
    Unbounded,
    Greater(OperationValue),
    GreaterEqual(OperationValue),
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldFilter {
    Single(OperationValue),
    Multiple(Vec<OperationValue>),
    Range(LowerBound, UpperBound),
    Contains(String),
}

#[derive(Debug, Clone)]
pub struct Field {
    field_name: FieldName,
    field_filter: FieldFilter,
    exclusive: bool,
}

impl Field {
    pub fn new(field_name: &FieldName, field_filter: FieldFilter, exclusive: bool) -> Self {
        Self {
            field_name: field_name.to_owned(),
            field_filter,
            exclusive,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Filter {
    meta: FilterMeta,
    fields: Vec<Field>,
}

impl Filter {
    pub fn new() -> Self {
        Self {
            meta: FilterMeta::new(),
            fields: Vec::new(),
        }
    }

    fn upsert_field(&mut self, new_field: Field) {
        // Check if a field exists we potentially can extend. For this the field needs to:
        // - Have the same field name
        // - Be also exclusive or non-exclusive
        let field_index = self.fields.iter().position(|current_field| {
            current_field.field_name == new_field.field_name
                && current_field.exclusive == new_field.exclusive
        });

        // We haven't found anything matching, just add it to the array
        if field_index.is_none() {
            self.fields.push(new_field);
            return;
        }

        // Get a mutable reference to the current field, unwrap since we know that both the index
        // and the element exists at this point
        let current_field = self.fields.get_mut(field_index.unwrap()).unwrap();

        // Merge or extend potentially overlapping filters
        let updated_filter = match (
            current_field.clone().field_filter,
            new_field.clone().field_filter,
        ) {
            (FieldFilter::Single(element_a), FieldFilter::Single(element_b)) => {
                if element_a != element_b {
                    Some(FieldFilter::Multiple(vec![element_a, element_b]))
                } else {
                    Some(FieldFilter::Single(element_a))
                }
            }
            (FieldFilter::Single(element_a), FieldFilter::Multiple(mut elements)) => {
                if !elements.contains(&element_a) {
                    elements.push(element_a);
                }

                Some(FieldFilter::Multiple(elements))
            }
            (FieldFilter::Multiple(mut elements), FieldFilter::Single(element_b)) => {
                if !elements.contains(&element_b) {
                    elements.push(element_b);
                }

                Some(FieldFilter::Multiple(elements))
            }
            (FieldFilter::Multiple(mut elements_a), FieldFilter::Multiple(elements_b)) => {
                for element in elements_b {
                    if !elements_a.contains(&element) {
                        elements_a.push(element);
                    }
                }

                Some(FieldFilter::Multiple(elements_a))
            }
            (FieldFilter::Range(lower_a, upper_a), FieldFilter::Range(lower_b, upper_b)) => {
                match (lower_b.clone(), upper_b.clone()) {
                    (LowerBound::Unbounded, UpperBound::Unbounded) => {
                        Some(FieldFilter::Range(lower_a, upper_a))
                    }
                    (LowerBound::Unbounded, _) => Some(FieldFilter::Range(lower_a, upper_b)),
                    (_, UpperBound::Unbounded) => Some(FieldFilter::Range(lower_b, upper_a)),
                    _ => Some(FieldFilter::Range(lower_b, upper_b)),
                }
            }
            _ => None,
        };

        match updated_filter {
            Some(filter) => {
                current_field.field_filter = filter;
            }
            None => {
                self.fields.push(new_field);
            }
        }
    }

    pub fn add(&mut self, field_name: &FieldName, value: &OperationValue) {
        self.upsert_field(Field::new(
            field_name,
            FieldFilter::Single(value.to_owned()),
            false,
        ));
    }

    pub fn add_not(&mut self, field_name: &FieldName, value: &OperationValue) {
        self.upsert_field(Field::new(
            field_name,
            FieldFilter::Single(value.to_owned()),
            true,
        ));
    }

    pub fn add_in(&mut self, field_name: &FieldName, values: &[OperationValue]) {
        if values.len() == 1 {
            self.upsert_field(Field::new(
                field_name,
                FieldFilter::Single(values[0].to_owned()),
                false,
            ));
        } else {
            self.upsert_field(Field::new(
                field_name,
                FieldFilter::Multiple(values.to_owned()),
                false,
            ));
        }
    }

    pub fn add_not_in(&mut self, field_name: &FieldName, values: &[OperationValue]) {
        if values.len() == 1 {
            self.upsert_field(Field::new(
                field_name,
                FieldFilter::Single(values[0].to_owned()),
                true,
            ));
        } else {
            self.upsert_field(Field::new(
                field_name,
                FieldFilter::Multiple(values.to_owned()),
                true,
            ));
        }
    }

    pub fn add_gt(&mut self, field_name: &FieldName, value: &OperationValue) {
        self.upsert_field(Field::new(
            field_name,
            FieldFilter::Range(LowerBound::Greater(value.to_owned()), UpperBound::Unbounded),
            false,
        ));
    }

    pub fn add_gte(&mut self, field_name: &FieldName, value: &OperationValue) {
        self.upsert_field(Field::new(
            field_name,
            FieldFilter::Range(
                LowerBound::GreaterEqual(value.to_owned()),
                UpperBound::Unbounded,
            ),
            false,
        ));
    }

    pub fn add_lt(&mut self, field_name: &FieldName, value: &OperationValue) {
        self.upsert_field(Field::new(
            field_name,
            FieldFilter::Range(LowerBound::Unbounded, UpperBound::Lower(value.to_owned())),
            false,
        ));
    }

    pub fn add_lte(&mut self, field_name: &FieldName, value: &OperationValue) {
        self.upsert_field(Field::new(
            field_name,
            FieldFilter::Range(
                LowerBound::Unbounded,
                UpperBound::LowerEqual(value.to_owned()),
            ),
            false,
        ));
    }

    pub fn add_contains(&mut self, field_name: &FieldName, value: &str) {
        self.upsert_field(Field::new(
            field_name,
            FieldFilter::Contains(value.to_string()),
            false,
        ));
    }

    pub fn add_not_contains(&mut self, field_name: &FieldName, value: &str) {
        self.upsert_field(Field::new(
            field_name,
            FieldFilter::Contains(value.to_string()),
            true,
        ));
    }
}

impl Default for Filter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct Find {
    filter: Option<Filter>,
    order: Option<Order>,
}

#[derive(Default, Debug, Clone)]
pub struct FindMany {
    pagination: Pagination,
    filter: Filter,
    order: Order,
}

impl FindMany {
    pub fn new(pagination: Pagination, filter: Filter, order: Order) -> Self {
        Self {
            pagination,
            filter,
            order,
        }
    }
}

impl SqlStore {
    pub async fn find(&self, args: &Find) -> Result<Document, QueryError> {
        todo!();
    }

    pub async fn find_many(&self, args: &FindMany) -> Result<Vec<Document>, QueryError> {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::operation::OperationValue;
    use p2panda_rs::schema::FieldName;

    use super::{
        Cursor, Direction, Field, FieldFilter, Filter, FilterMeta, Find, FindMany, Order,
        Pagination,
    };

    #[test]
    fn convert_single_element_filters() {
        let mut query = FindMany::default();
        let field_name: FieldName = "animal".into();
        let panda: OperationValue = "panda".into();

        query.filter.add_in(&field_name, &[panda.clone()]);

        assert_eq!(
            query.filter.fields[0].field_filter,
            FieldFilter::Single(panda)
        );
    }

    #[test]
    fn merge_element_filters() {
        let mut query = FindMany::default();
        let field_name: FieldName = "animal".into();

        let panda: OperationValue = "panda".into();
        let turtle: OperationValue = "turtle".into();
        let llama: OperationValue = "llama".into();

        query.filter.add(&field_name, &panda);
        query.filter.add(&field_name, &turtle);
        query.filter.add(&field_name, &llama);

        assert_eq!(query.filter.fields.len(), 1);
        assert_eq!(query.filter.fields[0].field_name, field_name);
        assert_eq!(query.filter.fields[0].exclusive, false);
        assert_eq!(
            query.filter.fields[0].field_filter,
            FieldFilter::Multiple(vec![panda, turtle, llama])
        );
    }

    #[test]
    fn merge_multiple_element_filters() {
        let mut query = FindMany::default();
        let field_name: FieldName = "animal".into();

        let panda: OperationValue = "panda".into();
        let turtle: OperationValue = "turtle".into();
        let llama: OperationValue = "llama".into();
        let icebear: OperationValue = "icebear".into();
        let penguin: OperationValue = "penguin".into();

        query
            .filter
            .add_in(&field_name, &[panda.clone(), turtle.clone()]);
        query
            .filter
            .add_in(&field_name, &[llama.clone(), icebear.clone()]);
        query.filter.add(&field_name, &penguin);

        assert_eq!(query.filter.fields.len(), 1);
        assert_eq!(query.filter.fields[0].field_name, field_name);
        assert_eq!(query.filter.fields[0].exclusive, false);
        assert_eq!(
            query.filter.fields[0].field_filter,
            FieldFilter::Multiple(vec![panda, turtle, llama, icebear, penguin])
        );
    }
}
