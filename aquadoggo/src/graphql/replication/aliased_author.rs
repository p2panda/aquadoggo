// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::*;

use super::public_key::PublicKey;

#[derive(Debug, InputObject, SimpleObject)]
pub struct AliasedAuthor {
    /// The author's public key
    pub public_key: PublicKey,
    /// The author alias
    pub alias: ID,
}
