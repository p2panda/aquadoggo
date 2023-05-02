// SPDX-License-Identifier: AGPL-3.0-or-later

#[cfg(test)]
use std::convert::TryInto;
use std::slice::Iter;

use p2panda_rs::operation::OperationValue;

#[cfg(test)]
use crate::db::query::test_utils::parse_str;
use crate::db::query::{Field, MetaField};

/// Options to represent the upper bound of an unbounded, open, closed or half-open interval.
#[derive(Debug, Clone, PartialEq)]
pub enum UpperBound {
    Unbounded,
    Lower(OperationValue),
    LowerEqual(OperationValue),
}

/// Options to represent the lower bound of an unbounded, open, closed or half-open interval.
#[derive(Debug, Clone, PartialEq)]
pub enum LowerBound {
    Unbounded,
    Greater(OperationValue),
    GreaterEqual(OperationValue),
}

/// Options of different filters which can be applied on document fields and meta data.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterBy {
    /// Filter elements with exactly this value.
    Element(OperationValue),

    /// Filter elements included in this set of values.
    Set(Vec<OperationValue>),

    /// Filter elements inside the given interval.
    Interval(LowerBound, UpperBound),

    /// Filter elements containing the this search string.
    Contains(OperationValue),
}

/// An item representing a single filter setting.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterSetting {
    /// Field this filter is applied on.
    pub field: Field,

    /// Type of filter.
    pub by: FilterBy,

    /// Flag to indicate if filter is negated / inverted.
    pub exclusive: bool,
}

impl FilterSetting {
    /// Returns a new filter setting which can be added to a whole set of other filters.
    pub fn new(field: &Field, by: FilterBy, exclusive: bool) -> Self {
        Self {
            field: field.to_owned(),
            by,
            exclusive,
        }
    }
}

#[cfg(test)]
impl FilterSetting {
    /// Test helper methods to derive filter settings from text strings.
    pub fn from_field_str(key: &str, value: &[OperationValue]) -> Self {
        let (field_name, by, exclusive) = parse_str(key, value).unwrap();

        Self {
            field: Field::Field(field_name),
            by,
            exclusive,
        }
    }

    /// Test helper methods to derive filter settings on meta fields from text strings.
    pub fn from_meta_str(key: &str, value: &[OperationValue]) -> Self {
        let (field_name, by, exclusive) = parse_str(key, value).unwrap();
        let meta_field = field_name.as_str().try_into().unwrap();

        Self {
            field: Field::Meta(meta_field),
            by,
            exclusive,
        }
    }
}

/// Collection of filter settings which can be used further to construct a database query.
///
/// Internally this struct merges or extends added filter settings as some of them can be optimized
/// in simple ways.
#[derive(Debug, Clone, PartialEq)]
pub struct Filter(Vec<FilterSetting>);

impl Filter {
    /// Returns a new `Filter` instance.
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Returns the total number of filter settings.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns a filter setting from a given index.
    #[allow(dead_code)]
    pub fn get(&self, index: usize) -> Option<&FilterSetting> {
        self.0.get(index)
    }

    /// Returns an iterator over all filter settings.
    pub fn iter(&self) -> Iter<FilterSetting> {
        self.0.iter()
    }

    /// Helper method to merge or extend existing filterings.
    ///
    /// This is a preparation step to pass on well-formed filters to the database backend, since we
    /// can't make sure that the filters were used "efficiently" by the requesting client.
    ///
    /// Note that this method does not merge across exclusivity and does not support multiple
    /// intervals for one field.
    fn upsert_filter_item(&mut self, new_item: FilterSetting) {
        // Check if a field exists we potentially can extend. For this the field needs to:
        // - Have the same field name
        let index = self.0.iter().position(|item| item.field == new_item.field);

        // We haven't found anything matching, just add it to the array
        if index.is_none() {
            self.0.push(new_item);
            return;
        }

        // Get a mutable reference to the current field, unwrap since we know that both the index
        // and the element exists at this point
        let current_item = self.0.get_mut(index.unwrap()).unwrap();

        // Boolean values for the same field we can always easily overwrite
        if let FilterBy::Element(OperationValue::Boolean(_)) = current_item.by {
            *current_item = new_item;
            return;
        }

        // We don't merge other fields with different exclusivity, in this case just add it and
        // return early
        if current_item.exclusive != new_item.exclusive {
            self.0.push(new_item);
            return;
        }

        // If exclusivity setting is the same, merge or extend potentially overlapping filters
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

    /// Add an equality (eq) filter setting matching a value.
    pub fn add(&mut self, field: &Field, value: &OperationValue) {
        self.upsert_filter_item(FilterSetting::new(
            field,
            FilterBy::Element(value.to_owned()),
            false,
        ));
    }

    /// Add a negated equality (not eq) filter setting not matching a value.
    pub fn add_not(&mut self, field: &Field, value: &OperationValue) {
        self.upsert_filter_item(FilterSetting::new(
            field,
            FilterBy::Element(value.to_owned()),
            true,
        ));
    }

    /// Add a filter setting (in) to match all items in the given values set.
    pub fn add_in(&mut self, field: &Field, values: &[OperationValue]) {
        if values.len() == 1 {
            self.upsert_filter_item(FilterSetting::new(
                field,
                FilterBy::Element(values[0].to_owned()),
                false,
            ));
        } else {
            self.upsert_filter_item(FilterSetting::new(
                field,
                FilterBy::Set(values.to_owned()),
                false,
            ));
        }
    }

    /// Add a negated filter setting (not in) to match all items which are not in the given values
    /// set.
    pub fn add_not_in(&mut self, field: &Field, values: &[OperationValue]) {
        if values.len() == 1 {
            self.upsert_filter_item(FilterSetting::new(
                field,
                FilterBy::Element(values[0].to_owned()),
                true,
            ));
        } else {
            self.upsert_filter_item(FilterSetting::new(
                field,
                FilterBy::Set(values.to_owned()),
                true,
            ));
        }
    }

    /// Add an lower bound filter (greater) to match all items larger than the given value.
    pub fn add_gt(&mut self, field: &Field, value: &OperationValue) {
        self.upsert_filter_item(FilterSetting::new(
            field,
            FilterBy::Interval(LowerBound::Greater(value.to_owned()), UpperBound::Unbounded),
            false,
        ));
    }

    /// Add an lower bound filter (greater equal) to match all items larger than or equal the given
    /// value.
    pub fn add_gte(&mut self, field: &Field, value: &OperationValue) {
        self.upsert_filter_item(FilterSetting::new(
            field,
            FilterBy::Interval(
                LowerBound::GreaterEqual(value.to_owned()),
                UpperBound::Unbounded,
            ),
            false,
        ));
    }

    /// Add an upper bound filter (lower) to match all items smaller than the given value.
    pub fn add_lt(&mut self, field: &Field, value: &OperationValue) {
        self.upsert_filter_item(FilterSetting::new(
            field,
            FilterBy::Interval(LowerBound::Unbounded, UpperBound::Lower(value.to_owned())),
            false,
        ));
    }

    /// Add an upper bound filter (lower equal) to match all items smaller than or equal the given
    /// value.
    pub fn add_lte(&mut self, field: &Field, value: &OperationValue) {
        self.upsert_filter_item(FilterSetting::new(
            field,
            FilterBy::Interval(
                LowerBound::Unbounded,
                UpperBound::LowerEqual(value.to_owned()),
            ),
            false,
        ));
    }

    /// Add a filter (contains) to match all items which contain the given search string.
    pub fn add_contains(&mut self, field: &Field, value: &str) {
        self.upsert_filter_item(FilterSetting::new(
            field,
            FilterBy::Contains(OperationValue::String(value.to_string())),
            false,
        ));
    }

    /// Add a negated filter (not contains) to match all items which do not contain the given
    /// search string.
    pub fn add_not_contains(&mut self, field: &Field, value: &str) {
        self.upsert_filter_item(FilterSetting::new(
            field,
            FilterBy::Contains(OperationValue::String(value.to_string())),
            true,
        ));
    }
}

impl Default for Filter {
    fn default() -> Self {
        let mut filter = Self::new();

        // Do not query deleted documents by default
        filter.add(
            &Field::Meta(MetaField::Deleted),
            &OperationValue::Boolean(false),
        );

        filter
    }
}

#[cfg(test)]
impl Filter {
    /// Helper method for tests to insert a range of filter settings.
    pub fn fields(mut self, fields: &[(&str, &[OperationValue])]) -> Self {
        for field in fields {
            self.upsert_filter_item(FilterSetting::from_field_str(field.0, field.1));
        }

        self
    }

    /// Helper method for tests to insert a range of filter settings on meta fields.
    pub fn meta_fields(mut self, fields: &[(&str, &[OperationValue])]) -> Self {
        for field in fields {
            self.upsert_filter_item(FilterSetting::from_meta_str(field.0, field.1));
        }

        self
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::operation::OperationValue;

    use crate::db::query::{Field, MetaField};

    use super::{Filter, FilterBy, LowerBound, UpperBound};

    #[test]
    fn element_filters() {
        let mut filter = Filter::new();
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
        let mut filter = Filter::new();
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
        let mut filter = Filter::new();
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
        let mut filter = Filter::new();
        let field: Field = "animal".into();
        let panda: OperationValue = "panda".into();

        // We're filtering "many" elements but the set only contains one
        filter.add_in(&field, &[panda.clone()]);

        assert_eq!(filter.get(0).unwrap().by, FilterBy::Element(panda));
    }

    #[test]
    fn merge_element_filters() {
        let mut filter = Filter::new();
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
        let mut filter = Filter::new();
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
        let mut filter = Filter::new();
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
        let mut filter = Filter::new();
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

    #[test]
    fn overwrite_deleted_filter() {
        let mut filter = Filter::default();

        // Check if filter is set to false by default
        assert_eq!(
            filter.get(0).unwrap().field,
            Field::Meta(MetaField::Deleted)
        );
        assert_eq!(
            filter.get(0).unwrap().by,
            FilterBy::Element(OperationValue::Boolean(false))
        );
        assert_eq!(filter.get(0).unwrap().exclusive, false);

        // Change exclusive filter manually
        filter.add_not(
            &Field::Meta(MetaField::Deleted),
            &OperationValue::Boolean(true),
        );

        // Make sure that it's still only one filter, but with the exclusive flag flipped
        assert_eq!(filter.len(), 1);
        assert_eq!(
            filter.get(0).unwrap().by,
            FilterBy::Element(OperationValue::Boolean(true))
        );
        assert_eq!(filter.get(0).unwrap().exclusive, true);

        // Change filter manually again
        filter.add(
            &Field::Meta(MetaField::Deleted),
            &OperationValue::Boolean(false),
        );

        // Make sure that it's still only one filter
        assert_eq!(filter.len(), 1);
        assert_eq!(
            filter.get(0).unwrap().by,
            FilterBy::Element(OperationValue::Boolean(false))
        );
        assert_eq!(filter.get(0).unwrap().exclusive, false);
    }

    #[test]
    fn overwrite_bool_fields() {
        let mut filter = Filter::new();

        // Add boolean filter
        filter.add(
            &Field::new("is_admin".into()),
            &OperationValue::Boolean(true),
        );

        // Overwrite the same filter with other value
        filter.add(
            &Field::new("is_admin".into()),
            &OperationValue::Boolean(false),
        );

        // .. check if it got correctly overwritten
        assert_eq!(filter.len(), 1);
        assert_eq!(
            filter.get(0).unwrap().by,
            FilterBy::Element(OperationValue::Boolean(false))
        );
        assert_eq!(filter.get(0).unwrap().exclusive, false);

        // Overwrite it again, but with an exclusive filter
        filter.add_not(
            &Field::new("is_admin".into()),
            &OperationValue::Boolean(false),
        );

        // .. check if it got correctly overwritten
        assert_eq!(filter.len(), 1);
        assert_eq!(
            filter.get(0).unwrap().by,
            FilterBy::Element(OperationValue::Boolean(false))
        );
        assert_eq!(filter.get(0).unwrap().exclusive, true);
    }
}
