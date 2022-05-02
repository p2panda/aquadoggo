use super::public_key::PublicKey;
use async_graphql::*;

#[derive(InputObject, SimpleObject)]
pub struct AliasedAuthor {
    public_key: PublicKey,
    alias: ID,
}
