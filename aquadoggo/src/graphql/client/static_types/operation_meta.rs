// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::SimpleObject;
use p2panda_rs::operation::traits::AsVerifiedOperation;

use crate::graphql::scalars;

/// Metadata of a published operation.
#[derive(SimpleObject, Debug, Eq, PartialEq)]
pub struct OperationMeta {
    /// Public key of the key pair which published this entry.
    #[graphql(name = "publicKey")]
    pub public_key: scalars::PublicKeyScalar,

    /// The id of this operation.
    #[graphql(name = "id")]
    pub id: scalars::OperationIdScalar,

    /// the previous field of this operation.
    #[graphql(name = "previous")]
    pub previous: Option<scalars::DocumentViewIdScalar>,
}

impl<T: AsVerifiedOperation> From<T> for OperationMeta {
    fn from(op: T) -> Self {
        OperationMeta {
            public_key: op.public_key().clone().into(),
            id: op.id().into(),
            previous: op
                .previous_operations()
                .as_ref()
                .map(|previous| previous.into()),
        }
    }
}
