// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Error, Object, Result};
use p2panda_rs::entry::EntrySigned;
use p2panda_rs::operation::{AsVerifiedOperation, OperationEncoded, VerifiedOperation};
use p2panda_rs::storage_provider::traits::{OperationStore, StorageProvider};

use crate::bus::{ServiceMessage, ServiceSender};
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
        let tx = ctx.data::<ServiceSender>()?;

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
                let verified_operation = VerifiedOperation::new_from_entry(
                    &args.entry_encoded,
                    &args.operation_encoded,
                )?;

                // Store operation in database
                // @TODO: This is not done by "publish_entry", maybe it needs to move there as
                // well?
                store
                    .insert_operation(&verified_operation, &document_id)
                    .await?;

                // Send new operation on service communication bus, this will arrive eventually at
                // the materializer service
                tx.send(ServiceMessage::NewOperation(
                    verified_operation.operation_id().to_owned(),
                ))?;

                Ok(response)
            }
            None => Err(Error::new("No related document found in database")),
        }
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::{from_value, value, Request, Value, Variables};
    use p2panda_rs::entry::{EntrySigned, LogId, SeqNum};
    use rstest::{fixture, rstest};
    use serde_json::json;
    use tokio::sync::broadcast;

    use crate::bus::ServiceMessage;
    use crate::db::stores::test_utils::{test_db, TestSqlStore};
    use crate::graphql::client::PublishEntryResponse;
    use crate::http::{build_server, HttpServiceContext};
    use crate::test_helpers::{initialize_store, TestClient};

    const ENTRY_ENCODED: &str = "00bedabb435758855968b3e2de2aa1f653adfbb392fcf9cb2295a68b2eca3c\
                                 fb030101a200204b771d59d76e820cbae493682003e99b795e4e7c86a8d6b4\
                                 c9ad836dc4c9bf1d3970fb39f21542099ba2fbfd6ec5076152f26c02445c62\
                                 1b43a7e2898d203048ec9f35d8c2a1547f2b83da8e06cadd8a60bb45d3b500\
                                 451e63f7cccbcbd64d09";

    const OPERATION_ENCODED: &str = "a466616374696f6e6663726561746566736368656d61784a76656e7565\
                                     5f30303230633635353637616533376566656132393365333461396337\
                                     6431336638663262663233646264633362356337623961623436323933\
                                     31313163343866633738626776657273696f6e01666669656c6473a167\
                                     6d657373616765a26474797065637374726576616c7565764f68682c20\
                                     6d79206669727374206d65737361676521";

    const PUBLISH_ENTRY_QUERY: &str = r#"
        mutation TestPublishEntry($entryEncoded: String!, $operationEncoded: String!) {
            publishEntry(entryEncoded: $entryEncoded, operationEncoded: $operationEncoded) {
                logId,
                seqNum,
                backlink,
                skiplink
            }
        }"#;

    #[fixture]
    fn publish_entry_request(
        #[default(ENTRY_ENCODED)] entry_encoded: &str,
        #[default(OPERATION_ENCODED)] operation_encoded: &str,
    ) -> Request {
        // Prepare GraphQL mutation publishing an entry
        let parameters = Variables::from_value(value!({
            "entryEncoded": entry_encoded,
            "operationEncoded": operation_encoded,
        }));

        Request::new(PUBLISH_ENTRY_QUERY).variables(parameters)
    }

    #[rstest]
    #[tokio::test]
    async fn publish_entry(publish_entry_request: Request) {
        let (tx, _rx) = broadcast::channel(16);
        let store = initialize_store().await;
        let context = HttpServiceContext::new(store, tx);

        let response = context.schema.execute(publish_entry_request).await;
        let received: PublishEntryResponse = match response.data {
            Value::Object(result_outer) => {
                from_value(result_outer.get("publishEntry").unwrap().to_owned()).unwrap()
            }
            _ => panic!("Expected return value to be an object"),
        };

        // The response should contain args for the next entry in the same log
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

    #[rstest]
    #[tokio::test]
    async fn sends_message_on_communication_bus(publish_entry_request: Request) {
        let (tx, mut rx) = broadcast::channel(16);
        let store = initialize_store().await;
        let context = HttpServiceContext::new(store, tx);

        context.schema.execute(publish_entry_request).await;

        // Find out hash of test entry to determine operation id
        let entry_encoded = EntrySigned::new(ENTRY_ENCODED).unwrap();

        // Expect receiver to receive sent message
        let message = rx.recv().await.unwrap();
        assert_eq!(
            message,
            ServiceMessage::NewOperation(entry_encoded.hash().into())
        );
    }

    #[tokio::test]
    async fn publish_entry_error_handling() {
        let (tx, _rx) = broadcast::channel(16);
        let store = initialize_store().await;
        let context = HttpServiceContext::new(store, tx);

        let parameters = Variables::from_value(value!({
            "entryEncoded": ENTRY_ENCODED,
            "operationEncoded": "".to_string()
        }));
        let request = Request::new(PUBLISH_ENTRY_QUERY).variables(parameters);
        let response = context.schema.execute(request).await;

        assert!(response.is_err());
        assert_eq!(
            "operation needs to match payload hash of encoded entry".to_string(),
            response.errors[0].to_string()
        );
    }

    #[rstest]
    #[tokio::test]
    async fn post_gql_mutation(publish_entry_request: Request) {
        let (tx, _rx) = broadcast::channel(16);
        let store = initialize_store().await;
        let context = HttpServiceContext::new(store, tx);
        let client = TestClient::new(build_server(context));

        let response = client
            .post("/graphql")
            .json(&json!({
              "query": publish_entry_request.query,
              "variables": publish_entry_request.variables
            }
            ))
            .send()
            .await;

        assert_eq!(
            response.json::<serde_json::Value>().await,
            json!({
                "data": {
                    "publishEntry": {
                        "logId":"1",
                        "seqNum":"2",
                        "backlink":"00201c221b573b1e0c67c5e2c624a93419774cdf46b3d62414c44a698df1237b1c16",
                        "skiplink":null
                    }
                }
            })
        );
    }

    #[rstest]
    #[tokio::test]
    #[case("adf", "", "invalid hex encoding in entry")]
    async fn invalid_requests_fail(
        #[case] entry_encoded: &str,
        #[case] operation_encoded: &str,
        #[case] expected_error_message: &str,
        #[future]
        #[from(test_db)]
        db: TestSqlStore,
    ) {
        let db = db.await;

        let (tx, _rx) = broadcast::channel(16);
        let context = HttpServiceContext::new(db.store, tx);
        let client = TestClient::new(build_server(context));

        let publish_entry_request = publish_entry_request(entry_encoded, operation_encoded);

        let response = client
            .post("/graphql")
            .json(&json!({
              "query": publish_entry_request.query,
              "variables": publish_entry_request.variables
            }
            ))
            .send()
            .await;

        let response = response.json::<serde_json::Value>().await;
        for error in response.get("errors").unwrap().as_array().unwrap() {
            assert_eq!(
                error.get("message").unwrap().as_str().unwrap(),
                expected_error_message
            )
        }
    }
}
