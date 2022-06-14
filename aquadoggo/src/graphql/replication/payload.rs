// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::*;
use p2panda_rs::operation::OperationEncoded;
use serde::{Deserialize, Serialize};

/// The payload of an entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Payload(pub OperationEncoded);

scalar!(Payload);
