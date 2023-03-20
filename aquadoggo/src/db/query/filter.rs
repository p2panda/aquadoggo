// SPDX-License-Identifier: AGPL-3.0-or-later

use std::str::FromStr;
use std::{convert::TryInto, slice::Iter};

use anyhow::{anyhow, bail, Context, Error, Result};
use p2panda_rs::operation::OperationValue;

use crate::db::query::helpers::parse_str;
use crate::db::query::Field;

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
pub enum FilterBy {
    Element(OperationValue),
    Set(Vec<OperationValue>),
    Interval(LowerBound, UpperBound),
    Contains(OperationValue),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilterItem {
    pub field: Field,
    pub by: FilterBy,
    pub exclusive: bool,
}

impl FilterItem {
    pub fn new(field: &Field, by: FilterBy, exclusive: bool) -> Self {
        Self {
            field: field.to_owned(),
            by,
            exclusive,
        }
    }

    // @TODO: Can we separate this better? This is already introducing a notion of GraphQL
    pub fn from_field_str(key: &str, value: &[OperationValue]) -> Result<Self> {
        let (field_name, by, exclusive) = parse_str(key, value)?;

        Ok(Self {
            field: Field::Field(field_name),
            by,
            exclusive,
        })
    }

    // @TODO: Can we separate this better? This is already introducing a notion of GraphQL
    pub fn from_meta_str(key: &str, value: &[OperationValue]) -> Result<Self> {
        let (field_name, by, exclusive) = parse_str(key, value)?;
        let meta_field = field_name.as_str().try_into()?;

        Ok(Self {
            field: Field::Meta(meta_field),
            by,
            exclusive,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Filter(Vec<FilterItem>);

impl Filter {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    // @TODO: Make sure that sets with one item are converte
    pub fn fields(mut self, fields: &[(&str, &[OperationValue])]) -> Result<Self> {
        for field in fields {
            self.upsert_filter_item(FilterItem::from_field_str(field.0, field.1)?);
        }

        Ok(self)
    }

    // @TODO: Make sure that sets with one item are converte
    pub fn meta_fields(mut self, fields: &[(&str, &[OperationValue])]) -> Result<Self> {
        for field in fields {
            self.upsert_filter_item(FilterItem::from_meta_str(field.0, field.1)?);
        }

        Ok(self)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get(&self, index: usize) -> Option<&FilterItem> {
        self.0.get(index)
    }

    pub fn iter(&self) -> Iter<FilterItem> {
        self.0.iter()
    }

    /// Helper method to merge or extend existing filterings.
    ///
    /// This is a preparation step to pass on well-formed filters to the database backend, since we
    /// can't make sure that the filters were used "efficiently" by the requesting client.
    ///
    /// Note that this method does not merge across exclusivity and does not support multiple
    /// intervals for one field.
    fn upsert_filter_item(&mut self, new_item: FilterItem) {
        // Check if a field exists we potentially can extend. For this the field needs to:
        // - Have the same field name
        // - Be also exclusive or non-exclusive
        let index = self
            .0
            .iter()
            .position(|item| item.field == new_item.field && item.exclusive == new_item.exclusive);

        // We haven't found anything matching, just add it to the array
        if index.is_none() {
            self.0.push(new_item);
            return;
        }

        // Get a mutable reference to the current field, unwrap since we know that both the index
        // and the element exists at this point
        let current_item = self.0.get_mut(index.unwrap()).unwrap();

        // Merge or extend potentially overlapping filters
        let updated_filter = match (current_item.clone().by, new_item.clone().by) {
            (FilterBy::Element(element_a), FilterBy::Element(element_b)) => {
                if element_a != element_b {
                    Some(FilterBy::Set(vec![element_a, element_b]))
                } else {
                    Some(FilterBy::Element(element_a))
                }
            }
            (FilterBy::Element(element_a), FilterBy::Set(mut elements)) => {
                if !elements.contains(&element_a) {
                    elements.push(element_a);
                }

                Some(FilterBy::Set(elements))
            }
            (FilterBy::Set(mut elements), FilterBy::Element(element_b)) => {
                if !elements.contains(&element_b) {
                    elements.push(element_b);
                }

                Some(FilterBy::Set(elements))
            }
            (FilterBy::Set(mut elements_a), FilterBy::Set(elements_b)) => {
                for element in elements_b {
                    if !elements_a.contains(&element) {
                        elements_a.push(element);
                    }
                }

                Some(FilterBy::Set(elements_a))
            }
            (FilterBy::Interval(lower_a, upper_a), FilterBy::Interval(lower_b, upper_b)) => {
                match (lower_b.clone(), upper_b.clone()) {
                    (LowerBound::Unbounded, UpperBound::Unbounded) => {
                        Some(FilterBy::Interval(lower_a, upper_a))
                    }
                    (LowerBound::Unbounded, _) => Some(FilterBy::Interval(lower_a, upper_b)),
                    (_, UpperBound::Unbounded) => Some(FilterBy::Interval(lower_b, upper_a)),
                    _ => Some(FilterBy::Interval(lower_b, upper_b)),
                }
            }
            _ => None,
        };

        match updated_filter {
            Some(filter) => {
                current_item.by = filter;
            }
            None => {
                self.0.push(new_item);
            }
        }
    }

    pub fn add(&mut self, field: &Field, value: &OperationValue) {
        self.upsert_filter_item(FilterItem::new(
            field,
            FilterBy::Element(value.to_owned()),
            false,
        ));
    }

    pub fn add_not(&mut self, field: &Field, value: &OperationValue) {
        self.upsert_filter_item(FilterItem::new(
            field,
            FilterBy::Element(value.to_owned()),
            true,
        ));
    }

    pub fn add_in(&mut self, field: &Field, values: &[OperationValue]) {
        if values.len() == 1 {
            self.upsert_filter_item(FilterItem::new(
                field,
                FilterBy::Element(values[0].to_owned()),
                false,
            ));
        } else {
            self.upsert_filter_item(FilterItem::new(
                field,
                FilterBy::Set(values.to_owned()),
                false,
            ));
        }
    }

    pub fn add_not_in(&mut self, field: &Field, values: &[OperationValue]) {
        if values.len() == 1 {
            self.upsert_filter_item(FilterItem::new(
                field,
                FilterBy::Element(values[0].to_owned()),
                true,
            ));
        } else {
            self.upsert_filter_item(FilterItem::new(
                field,
                FilterBy::Set(values.to_owned()),
                true,
            ));
        }
    }

    pub fn add_gt(&mut self, field: &Field, value: &OperationValue) {
        self.upsert_filter_item(FilterItem::new(
            field,
            FilterBy::Interval(LowerBound::Greater(value.to_owned()), UpperBound::Unbounded),
            false,
        ));
    }

    pub fn add_gte(&mut self, field: &Field, value: &OperationValue) {
        self.upsert_filter_item(FilterItem::new(
            field,
            FilterBy::Interval(
                LowerBound::GreaterEqual(value.to_owned()),
                UpperBound::Unbounded,
            ),
            false,
        ));
    }

    pub fn add_lt(&mut self, field: &Field, value: &OperationValue) {
        self.upsert_filter_item(FilterItem::new(
            field,
            FilterBy::Interval(LowerBound::Unbounded, UpperBound::Lower(value.to_owned())),
            false,
        ));
    }

    pub fn add_lte(&mut self, field: &Field, value: &OperationValue) {
        self.upsert_filter_item(FilterItem::new(
            field,
            FilterBy::Interval(
                LowerBound::Unbounded,
                UpperBound::LowerEqual(value.to_owned()),
            ),
            false,
        ));
    }

    pub fn add_contains(&mut self, field: &Field, value: &str) {
        self.upsert_filter_item(FilterItem::new(
            field,
            FilterBy::Contains(OperationValue::String(value.to_string())),
            false,
        ));
    }

    pub fn add_not_contains(&mut self, field: &Field, value: &str) {
        self.upsert_filter_item(FilterItem::new(
            field,
            FilterBy::Contains(OperationValue::String(value.to_string())),
            true,
        ));
    }
}

impl Default for Filter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::operation::OperationValue;
    use p2panda_rs::schema::FieldName;

    use crate::db::query::Field;

    use super::{Filter, FilterBy, LowerBound, UpperBound};

    #[test]
    fn element_filters() {
        let mut filter = Filter::default();
        filter.add(&"animal".into(), &"panda".into());
        filter.add_in(&"city".into(), &["tokyo".into(), "osaka".into()]);

        assert_eq!(filter.len(), 2);
        assert_eq!(filter.get(0).unwrap().by, FilterBy::Element("panda".into()));
        assert_eq!(
            filter.get(1).unwrap().by,
            FilterBy::Set(vec!["tokyo".into(), "osaka".into()])
        );
    }

    #[test]
    fn range_filters() {
        let mut filter = Filter::default();
        filter.add_gt(&"year".into(), &2004.into());
        filter.add_lte(&"temperature".into(), &15.75.into());

        assert_eq!(filter.len(), 2);
        assert_eq!(
            filter.get(0).unwrap().by,
            FilterBy::Interval(LowerBound::Greater(2004.into()), UpperBound::Unbounded)
        );
        assert_eq!(
            filter.get(1).unwrap().by,
            FilterBy::Interval(LowerBound::Unbounded, UpperBound::LowerEqual(15.75.into()))
        );
    }

    #[test]
    fn contains_filters() {
        let mut filter = Filter::default();
        filter.add_contains(&"description".into(), "Panda is the best");
        filter.add_not_contains(&"description".into(), "Llama");

        assert_eq!(filter.len(), 2);
        assert_eq!(
            filter.get(0).unwrap().by,
            FilterBy::Contains("Panda is the best".into())
        );
        assert_eq!(filter.get(0).unwrap().exclusive, false,);
        assert_eq!(
            filter.get(1).unwrap().by,
            FilterBy::Contains("Llama".into())
        );
        assert_eq!(filter.get(1).unwrap().exclusive, true);
    }

    #[test]
    fn convert_single_element_filter() {
        let mut filter = Filter::default();
        let field: Field = "animal".into();
        let panda: OperationValue = "panda".into();

        // We're filtering "many" elements but the set only contains one
        filter.add_in(&field, &[panda.clone()]);

        assert_eq!(filter.get(0).unwrap().by, FilterBy::Element(panda));
    }

    #[test]
    fn merge_element_filters() {
        let mut filter = Filter::default();
        let field: Field = "animal".into();

        let panda: OperationValue = "panda".into();
        let turtle: OperationValue = "turtle".into();
        let llama: OperationValue = "llama".into();

        // We filter multiple elements but add them one-by-one for the same field
        filter.add(&field, &panda);
        filter.add(&field, &turtle);
        filter.add(&field, &llama);

        assert_eq!(filter.len(), 1);
        assert_eq!(filter.get(0).unwrap().field, field);
        assert_eq!(filter.get(0).unwrap().exclusive, false);
        assert_eq!(
            filter.get(0).unwrap().by,
            FilterBy::Set(vec![panda, turtle, llama])
        );
    }

    #[test]
    fn merge_multiple_element_filters() {
        let mut filter = Filter::default();
        let field: Field = "animal".into();

        let panda: OperationValue = "panda".into();
        let turtle: OperationValue = "turtle".into();
        let llama: OperationValue = "llama".into();
        let icebear: OperationValue = "icebear".into();
        let penguin: OperationValue = "penguin".into();

        // We filter multiple elements for the same field
        filter.add_in(&field, &[panda.clone(), turtle.clone()]);
        filter.add_in(&field, &[llama.clone(), icebear.clone()]);
        filter.add(&field, &penguin);

        assert_eq!(filter.len(), 1);
        assert_eq!(
            filter.get(0).unwrap().by,
            FilterBy::Set(vec![panda, turtle, llama, icebear, penguin])
        );
    }

    #[test]
    fn merge_range_filters() {
        let mut filter = Filter::default();
        let field: Field = "year".into();

        let from: OperationValue = 2020.into();
        let to: OperationValue = 2023.into();

        // We filter over an open interval
        filter.add_gt(&field, &from);
        filter.add_lt(&field, &to);

        assert_eq!(filter.len(), 1);
        assert_eq!(
            filter.get(0).unwrap().by,
            FilterBy::Interval(LowerBound::Greater(from), UpperBound::Lower(to))
        );
    }

    #[test]
    fn overwrite_range_filters() {
        let mut filter = Filter::default();
        let field: Field = "year".into();

        let from: OperationValue = 2020.into();
        let to: OperationValue = 2023.into();
        let to_new: OperationValue = 2025.into();

        // We filter over an open interval
        filter.add_gt(&field, &from);
        filter.add_lt(&field, &to);

        // .. and make it half-open afterwards
        filter.add_lte(&field, &to_new);

        assert_eq!(filter.len(), 1);
        assert_eq!(
            filter.get(0).unwrap().by,
            FilterBy::Interval(LowerBound::Greater(from), UpperBound::LowerEqual(to_new))
        );
    }
}
