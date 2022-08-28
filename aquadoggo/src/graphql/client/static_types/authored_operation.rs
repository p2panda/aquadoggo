// SPDX-License-Identifier: AGPL-3.0-or-later

use std::any::type_name;

use async_graphql::{
    indexmap::IndexMap, scalar, InputType, Name, OutputType, ScalarType, SimpleObject, Value,
};
use p2panda_rs::operation::{AsOperation, AsVerifiedOperation, VerifiedOperation};
use serde::{Deserialize, Serialize};

use crate::graphql::scalars;

/// Arguments required to sign and encode the next entry for an author.
#[derive(SimpleObject, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct AuthoredOperation {
    /// Public key of the key pair which published this entry.
    #[graphql(name = "publicKey")]
    pub public_key: scalars::PublicKeyScalar,

    /// The id of this operation.
    #[graphql(name = "operationId")]
    pub operation_id: scalars::OperationIdScalar,

    /// the previous field of this operation.
    #[graphql(name = "previous")]
    pub previous: Option<scalars::DocumentViewIdScalar>,
}

impl From<VerifiedOperation> for AuthoredOperation {
    fn from(op: VerifiedOperation) -> Self {
        AuthoredOperation {
            public_key: op.public_key().clone().into(),
            operation_id: op.operation_id().into(),
            previous: op
                .previous_operations()
                .as_ref()
                .map(|previous| previous.into()),
        }
    }
}

#[derive(SimpleObject, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct AuthoredOperationList {
    operations: Vec<AuthoredOperation>,
}

impl From<Vec<VerifiedOperation>> for AuthoredOperationList {
    fn from(ops: Vec<VerifiedOperation>) -> Self {
        AuthoredOperationList {
            operations: ops.into_iter().map(|op| op.into()).collect(),
        }
    }
}

//
// impl Into<Value> for AuthoredOperationList {
//     fn into(self) -> Value {
//         let mut operations = self
//             .0
//             .into_iter()
//             .map(|operation| {
//                 let op = IndexMap::new();
//                 op.insert(
//                     Name::new(scalars::PublicKeyScalar::type_name()),
//                     operation.public_key.to_value(),
//                 );
//                 op.insert(
//                     Name::new(scalars::OperationIdScalar::type_name()),
//                     operation.operation_id.to_value(),
//                 );
//                 op.insert(
//                     Name::new(scalars::DocumentViewIdScalar::type_name()),
//                     operation
//                         .previous
//                         .map(|previous| previous.to_value())
//                         .unwrap_or(Value::Null),
//                 );
//                 Value::Object(op)
//             })
//             .collect();
//         Value::List(operations)
//     }
// }
