use super::public_key::PublicKey;
use anyhow::{anyhow, Error};
use async_graphql::*;
use std::convert::TryFrom;

/// Either the `public_key` or the `alias` of that author.
#[derive(Debug, InputObject)]
pub struct Author {
    /// The author's public key
    pub public_key: Option<PublicKey>,
    /// The author alias
    pub alias: Option<ID>,
}

#[derive(Debug)]
pub enum AuthorOrAlias {
    PublicKey(PublicKey),
    Alias(ID),
}

impl TryFrom<Author> for AuthorOrAlias {
    type Error = Error;

    fn try_from(author: Author) -> Result<Self, Self::Error> {
        match (author.public_key, author.alias) {
            (Some(key), None) => Ok(AuthorOrAlias::PublicKey(key)),
            (None, Some(alias)) => Ok(AuthorOrAlias::Alias(alias)),
            _ => Err(anyhow!(
                "Author must have only one of public_key or alias set"
            )),
        }
    }
}
