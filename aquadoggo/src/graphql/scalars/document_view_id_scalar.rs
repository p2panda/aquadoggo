// SPDX-License-Identifier: AGPL-3.0-or-later

use std::{fmt::Display, str::FromStr};

use dynamic_graphql::{Scalar, ScalarValue, Value, Result, Error};
use p2panda_rs::document::DocumentViewId;

/// Document view id as a GraphQL scalar.
#[derive(Scalar, Clone, Debug, Eq, PartialEq)]
#[graphql(name = "DocumentViewId")]
pub struct DocumentViewIdScalar(DocumentViewId);

impl ScalarValue for DocumentViewIdScalar {
    fn from_value(value: Value) -> Result<Self>
        where
            Self: Sized {
        
        match &value {
            Value::String(str_value) => {
                let view_id = DocumentViewId::from_str(str_value)?;
                Ok(DocumentViewIdScalar(view_id))
            }
            _ => Err(Error::new(format!("Expected a valid document view id, found: {value}"))),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

impl From<&DocumentViewId> for DocumentViewIdScalar {
    fn from(value: &DocumentViewId) -> Self {
        DocumentViewIdScalar(value.clone())
    }
}

impl From<DocumentViewIdScalar> for DocumentViewId {
    fn from(value: DocumentViewIdScalar) -> Self {
        DocumentViewId::new(value.0.graph_tips())
    }
}

impl Display for DocumentViewIdScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
