use super::public_key::PublicKey;
use async_graphql::*;

#[derive(Debug, InputObject, SimpleObject)]
pub struct AliasedAuthor {
    /// The author's public key
    pub public_key: PublicKey,
    /// The author alias
    pub alias: ID,
}
