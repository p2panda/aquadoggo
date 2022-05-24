// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::*;
use p2panda_rs::identity::Author;
use serde::{Deserialize, Serialize};

/// The public key of an entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublicKey(pub Author);

scalar!(PublicKey);
