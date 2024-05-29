// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::{bail, Context, Result};
use p2panda_rs::operation::OperationValue;

use crate::db::query::{FilterBy, LowerBound, UpperBound};

fn clean_key(key: &str, suffix: &str) -> String {
    key[0..key.len() - suffix.len()].into()
}

pub fn parse_str(key: &str, value: &[OperationValue]) -> Result<(String, FilterBy, bool)> {
    if value.is_empty() {
        bail!("Needs at least one value");
    }

    if key.ends_with("_not_in") {
        return Ok((
            clean_key(key, "_not_in"),
            FilterBy::Set(value.to_vec()),
            true,
        ));
    } else if key.ends_with("_in") {
        return Ok((clean_key(key, "_in"), FilterBy::Set(value.to_vec()), false));
    }

    if value.len() != 1 {
        bail!("Needs to be exactly one value");
    }

    // Unwrap since we know at least one element exists
    let element = value.first().unwrap();

    if key.ends_with("_gt") {
        Ok((
            clean_key(key, "_gt"),
            FilterBy::Interval(
                LowerBound::Greater(element.to_owned()),
                UpperBound::Unbounded,
            ),
            false,
        ))
    } else if key.ends_with("_gte") {
        let element = value.first().context("Needs at least one value")?;

        Ok((
            clean_key(key, "_gte"),
            FilterBy::Interval(
                LowerBound::GreaterEqual(element.to_owned()),
                UpperBound::Unbounded,
            ),
            false,
        ))
    } else if key.ends_with("_lt") {
        Ok((
            clean_key(key, "_lt"),
            FilterBy::Interval(LowerBound::Unbounded, UpperBound::Lower(element.to_owned())),
            false,
        ))
    } else if key.ends_with("_lte") {
        Ok((
            clean_key(key, "_lte"),
            FilterBy::Interval(
                LowerBound::Unbounded,
                UpperBound::LowerEqual(element.to_owned()),
            ),
            false,
        ))
    } else if key.ends_with("_contains") {
        Ok((
            clean_key(key, "_contains"),
            FilterBy::Contains(element.to_owned()),
            false,
        ))
    } else if key.ends_with("_not_contains") {
        Ok((
            clean_key(key, "_not_contains"),
            FilterBy::Contains(element.to_owned()),
            true,
        ))
    } else if key.ends_with("_not") {
        Ok((
            clean_key(key, "_not"),
            FilterBy::Element(element.to_owned()),
            true,
        ))
    } else if key.ends_with('_') {
        bail!("Invalid query string");
    } else {
        Ok((key.into(), FilterBy::Element(element.to_owned()), false))
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::operation::OperationValue;
    use rstest::rstest;

    use crate::db::query::FilterBy;

    use super::{clean_key, parse_str};

    #[test]
    fn clean_suffix_from_str() {
        assert_eq!(
            clean_key("username_some_suffix", "_some_suffix"),
            "username".to_string()
        );
    }

    #[rstest]
    #[case::eq(
        "username", &["Panda_Nattor".into()],
        ("username".into(), FilterBy::Element("Panda_Nattor".into()), false))
    ]
    #[case::not_eq(
        "username_not", &["Panda_Nattor".into()],
        ("username".into(), FilterBy::Element("Panda_Nattor".into()), true))
    ]
    #[case::in_(
        "city_in", &["Berlin".into(), "London".into()],
        (
            "city".into(),
            FilterBy::Set(vec!["Berlin".into(), "London".into()]),
            false,
        )
    )]
    #[case::not_in(
        "city_not_in", &["Berlin".into(), "London".into()],
        (
            "city".into(),
            FilterBy::Set(vec!["Berlin".into(), "London".into()]),
            true,
        )
    )]
    fn parse_fields(
        #[case] key: &str,
        #[case] value: &[OperationValue],
        #[case] expected: (String, FilterBy, bool),
    ) {
        assert_eq!(parse_str(key, value).expect("Should succeed"), expected);
    }
}
