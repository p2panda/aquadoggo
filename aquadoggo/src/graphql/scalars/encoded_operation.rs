// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::scalar;
use p2panda_rs::operation::OperationEncoded;
use serde::{Deserialize, Serialize};

/// Entry payload and p2panda operation, CBOR bytes encoded as a hexadecimal string.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EncodedOperation(OperationEncoded);

scalar!(EncodedOperation);
