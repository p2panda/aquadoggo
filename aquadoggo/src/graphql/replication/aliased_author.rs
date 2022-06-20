// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::*;

use super::public_key::PublicKey;

/// AliasedAuthor is one of either the public_key or an alias
///
/// The intention of this is to reduce bandwidth when making requests by using a short "alias"
/// rather than the full author public_key
///
/// To get an alias of an author, use the `author_aliases` method which will return this type.
///
/// When using as an input to a query, exactly one of public_key or alias must be set otherwise it is an error.
#[derive(Debug, InputObject, SimpleObject)]
pub struct AliasedAuthor {
    /// The author's public key
    pub public_key: PublicKey,
    /// The author alias
    pub alias: ID,
}
