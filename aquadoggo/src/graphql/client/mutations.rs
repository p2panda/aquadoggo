// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Error, Object, Result};
use p2panda_rs::entry::EntrySigned;
use p2panda_rs::operation::OperationEncoded;
use p2panda_rs::storage_provider::traits::StorageProvider;

use crate::db::provider::SqlStorage;
use crate::db::Pool;

use super::{PublishEntryRequest, PublishEntryResponse};

/// Mutations for use by p2panda clients.
#[derive(Default, Debug, Copy, Clone)]
pub struct Mutation;

#[Object]
impl Mutation {
    /// Publish an entry using parameters obtained through `nextEntryArgs` query.
    async fn publish_entry(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "entryEncoded", desc = "Encoded entry to publish")]
        entry_encoded_param: String,
        #[graphql(name = "operationEncoded", desc = "Encoded entry payload.")]
        operation_encoded_param: String,
    ) -> Result<PublishEntryResponse> {
        // Parse and validate parameters
        let args = PublishEntryRequest {
            entry_encoded: EntrySigned::new(&entry_encoded_param)?,
            operation_encoded: OperationEncoded::new(&operation_encoded_param)?,
        };

        // Prepare database connection
        let pool = ctx.data::<Pool>()?;
        let provider = SqlStorage {
            pool: pool.to_owned(),
        };

        provider
            .publish_entry(&args)
            .await
            .map_err(|err| Error::from(err))
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::{value, Request, Variables};
    use p2panda_rs::entry::{LogId, SeqNum};

    use crate::context::Context;
    use crate::graphql::client::PublishEntryResponse;
    use crate::test_helpers::initialize_db;
    use crate::Configuration;

    #[tokio::test]
    async fn publish_entry() {
        let pool = initialize_db().await;
        let context = Context::new(pool.clone(), Configuration::default());

        let entry_encoded = "00bedabb435758855968b3e2de2aa1f653adfbb392fcf9cb2295a68b2eca3cfb0301\
        01a200204b771d59d76e820cbae493682003e99b795e4e7c86a8d6b4c9ad836dc4c9bf1d3970fb39f21542099ba\
        2fbfd6ec5076152f26c02445c621b43a7e2898d203048ec9f35d8c2a1547f2b83da8e06cadd8a60bb45d3b50045\
        1e63f7cccbcbd64d09";

        let operation_encoded = "a466616374696f6e6663726561746566736368656d61784a76656e7565\
        5f30303230633635353637616533376566656132393365333461396337643133663866326266323364626463336\
        235633762396162343632393331313163343866633738626776657273696f6e01666669656c6473a1676d657373\
        616765a26474797065637374726576616c7565764f68682c206d79206669727374206d65737361676521";

        let query = r#"
            mutation TestPublishEntry($entryEncoded: String!, $operationEncoded: String!) {
                publishEntry(entryEncoded: $entryEncoded, operationEncoded: $operationEncoded) {
                    logId,
                    seqNum,
                    backlink,
                    skiplink
                }
            }"#;
        let parameters = Variables::from_value(value!({
            "entryEncoded": entry_encoded,
            "operationEncoded": operation_encoded
        }));
        let request = Request::new(query).variables(parameters);
        let response = context.0.schema.execute(request).await;

        let received: PublishEntryResponse = match response.data {
            async_graphql::Value::Object(result_outer) => {
                async_graphql::from_value(result_outer.get("publishEntry").unwrap().to_owned())
                    .unwrap()
            }
            _ => panic!("Expected return value to be an object"),
        };
        let expected = PublishEntryResponse {
            log_id: LogId::new(1),
            seq_num: SeqNum::new(2).unwrap(),
            backlink: Some(
                "00201c221b573b1e0c67c5e2c624a93419774cdf46b3d62414c44a698df1237b1c16"
                    .parse()
                    .unwrap(),
            ),
            skiplink: None,
        };

        assert_eq!(expected, received);
    }
}
