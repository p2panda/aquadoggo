// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::scalar;
use p2panda_rs::identity::Author;
use serde::{Deserialize, Serialize};

/// Public key of the entry author.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicKey(Author);

impl From<Author> for PublicKey {
    fn from(author: Author) -> Self {
        Self(author)
    }
}

impl From<PublicKey> for Author {
    fn from(public_key: PublicKey) -> Author {
        public_key.0
    }
}

scalar!(PublicKey);
