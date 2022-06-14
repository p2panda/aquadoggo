// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Error, Object, Result};
use p2panda_rs::entry::EntrySigned;
use p2panda_rs::operation::{Operation, OperationEncoded, VerifiedOperation};
use p2panda_rs::storage_provider::traits::{OperationStore, StorageProvider};

use crate::db::provider::SqlStorage;
use crate::graphql::client::{PublishEntryRequest, PublishEntryResponse};

/// Mutations for use by p2panda clients.
#[derive(Default, Debug, Copy, Clone)]
pub struct ClientMutationRoot;

#[Object]
impl ClientMutationRoot {
    /// Publish an entry using parameters obtained through `nextEntryArgs` query.
    ///
    /// Returns arguments for publishing the next entry in the same log.
    async fn publish_entry(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "entryEncoded", desc = "Encoded entry to publish")]
        entry_encoded_param: String,
        #[graphql(
            name = "operationEncoded",
            desc = "Encoded entry payload, which contains a p2panda operation matching the \
            provided encoded entry."
        )]
        operation_encoded_param: String,
    ) -> Result<PublishEntryResponse> {
        let store = ctx.data::<SqlStorage>()?;

        // Parse and validate parameters
        let args = PublishEntryRequest {
            entry_encoded: EntrySigned::new(&entry_encoded_param)?,
            operation_encoded: OperationEncoded::new(&operation_encoded_param)?,
        };

        // Validate and store entry in database
        // @TODO: Check all validation steps here for both entries and operations. Also, there is
        // probably overlap in what replication needs in terms of validation?
        let response = store.publish_entry(&args).await.map_err(Error::from)?;

        // Load related document from database
        // @TODO: We probably have this instance already inside of "publish_entry"?
        match store
            .get_document_by_entry(&args.entry_encoded.hash())
            .await?
        {
            Some(document_id) => {
                let operation = Operation::from(&args.operation_encoded);

                let verified_operation = VerifiedOperation::new(
                    &args.entry_encoded.author(),
                    &args.entry_encoded.hash().into(),
                    &operation,
                )?;

                // Store operation in database
                // @TODO: This is not done by "publish_entry", maybe it needs to move there as
                // well?
                store
                    .insert_operation(&verified_operation, &document_id)
                    .await?;
            }
            None => return Err(Error::new("No related document found in database")),
        }

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::{from_value, value, Request, Value, Variables};
    use p2panda_rs::entry::{LogId, SeqNum};
    use tokio::sync::broadcast;

    use crate::graphql::client::PublishEntryResponse;
    use crate::http::HttpServiceContext;
    use crate::test_helpers::initialize_store;

    const ENTRY_ENCODED: &str =
        "00bedabb435758855968b3e2de2aa1f653adfbb392fcf9cb2295a68b2eca3cfb0301\
    01a200204b771d59d76e820cbae493682003e99b795e4e7c86a8d6b4c9ad836dc4c9bf1d3970fb39f21542099ba\
    2fbfd6ec5076152f26c02445c621b43a7e2898d203048ec9f35d8c2a1547f2b83da8e06cadd8a60bb45d3b50045\
    1e63f7cccbcbd64d09";

    const OPERATION_ENCODED: &str = "a466616374696f6e6663726561746566736368656d61784a76656e7565\
    5f30303230633635353637616533376566656132393365333461396337643133663866326266323364626463336\
    235633762396162343632393331313163343866633738626776657273696f6e01666669656c6473a1676d657373\
    616765a26474797065637374726576616c7565764f68682c206d79206669727374206d65737361676521";

    #[tokio::test]
    async fn publish_entry() {
        let (tx, _) = broadcast::channel(16);
        let store = initialize_store().await;
        let context = HttpServiceContext::new(store, tx);

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
            "entryEncoded": ENTRY_ENCODED,
            "operationEncoded": OPERATION_ENCODED
        }));
        let request = Request::new(query).variables(parameters);
        let response = context.schema.execute(request).await;

        let received: PublishEntryResponse = match response.data {
            Value::Object(result_outer) => {
                from_value(result_outer.get("publishEntry").unwrap().to_owned()).unwrap()
            }
            _ => panic!("Expected return value to be an object"),
        };
        // The response should contain args for the next entry in the same log.
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

    #[tokio::test]
    async fn publish_entry_error_handling() {
        let (tx, _) = broadcast::channel(16);
        let store = initialize_store().await;
        let context = HttpServiceContext::new(store, tx);

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
            "entryEncoded": ENTRY_ENCODED,
            "operationEncoded": "".to_string()
        }));
        let request = Request::new(query).variables(parameters);
        let response = context.schema.execute(request).await;

        assert!(response.is_err());
        assert_eq!(
            "operation needs to match payload hash of encoded entry".to_string(),
            response.errors[0].to_string()
        );
    }
}
