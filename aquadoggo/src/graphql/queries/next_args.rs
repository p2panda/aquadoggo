// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, InputValue, Object, ResolverContext, TypeRef};
use async_graphql::Error;
use dynamic_graphql::{FieldValue, ScalarValue};
use log::debug;
use p2panda_rs::api;

use crate::db::SqlStore;
use crate::graphql::constants;
use crate::graphql::responses::NextArguments;
use crate::graphql::scalars::{DocumentViewIdScalar, PublicKeyScalar};

/// Add "nextArgs" query to the root query object.
pub fn build_next_args_query(query: Object) -> Object {
    query.field(
        Field::new(
            constants::NEXT_ARGS_QUERY,
            TypeRef::named(constants::NEXT_ARGS),
            |ctx| {
                FieldFuture::new(async move {
                    // Parse arguments.
                    let (public_key, document_view_id) = parse_arguments(&ctx)?;
                    let store = ctx.data_unchecked::<SqlStore>();

                    // Calculate next entry's arguments.
                    let (backlink, skiplink, seq_num, log_id) = api::next_args(
                        store,
                        &public_key.into(),
                        document_view_id.map(|id| id.into()).as_ref(),
                    )
                    .await?;

                    // Construct and return the next args.
                    let next_args = NextArguments {
                        log_id: log_id.into(),
                        seq_num: seq_num.into(),
                        backlink: backlink.map(|hash| hash.into()),
                        skiplink: skiplink.map(|hash| hash.into()),
                    };

                    Ok(Some(FieldValue::owned_any(next_args)))
                })
            },
        )
        .argument(InputValue::new(
            constants::PUBLIC_KEY_ARG,
            TypeRef::named_nn(constants::PUBLIC_KEY),
        ).description("The public key of the author next args are being requested for."))
        .argument(InputValue::new(
            constants::DOCUMENT_VIEW_ID_ARG,
            TypeRef::named(constants::DOCUMENT_VIEW_ID),
        ).description("Optional field for specifying an existing document next args are being requested for."))
        .description("Return required arguments for publishing a entry to a node."),
    )
}

/// Parse and validate the arguments passed to next_args.
fn parse_arguments(
    ctx: &ResolverContext,
) -> Result<(PublicKeyScalar, Option<DocumentViewIdScalar>), Error> {
    let mut args = ctx.field().arguments()?.into_iter().map(|(_, value)| value);

    // Convert and validate passed parameters.
    let public_key = PublicKeyScalar::from_value(args.next().unwrap())?;
    let document_view_id = match args.next() {
        Some(value) => match value {
            async_graphql::Value::Null => None,
            async_graphql::Value::String(_) => Some(value),
            _ => panic!("Unexpected value type received for viewId in nextArgs"),
        },
        None => None,
    };
    let document_view_id = match document_view_id {
        Some(value) => {
            let document_view_id = DocumentViewIdScalar::from_value(value)?;
            debug!(
                "Query to nextArgs received for public key {} and document at view {}",
                public_key, document_view_id
            );
            Some(document_view_id)
        }
        None => {
            debug!("Query to nextArgs received for public key {}", public_key);
            None
        }
    };

    Ok((public_key, document_view_id))
}

#[cfg(test)]
mod tests {
    use async_graphql::{value, Response};
    use p2panda_rs::{document::traits::AsDocument, identity::KeyPair, test_utils::constants};
    use rstest::rstest;
    use serde_json::json;

    use crate::test_utils::{
        http_test_client, populate_and_materialize, populate_store_config, test_runner,
        PopulateStoreConfig, TestNode,
    };

    #[rstest]
    fn next_args_valid_query() {
        test_runner(|node: TestNode| async move {
            let client = http_test_client(&node).await;
            // Selected fields need to be alphabetically sorted because that's what the `json`
            // macro that is used in the assert below produces.
            let received_entry_args = client
                .post("/graphql")
                .json(&json!({
                    "query": r#"{
                        nextArgs(
                            publicKey: "8b52ae153142288402382fd6d9619e018978e015e6bc372b1b0c7bd40c6a240a"
                        ) {
                            logId,
                            seqNum,
                            backlink,
                            skiplink
                        }
                    }"#,
                }))
                .send()
                .await
                .json::<Response>()
                .await;

            assert_eq!(
                received_entry_args.data,
                value!({
                    "nextArgs": {
                        "logId": "0",
                        "seqNum": "1",
                        "backlink": null,
                        "skiplink": null,
                    }
                })
            );
        })
    }

    #[rstest]
    fn next_args_valid_query_with_document_id(
        #[from(populate_store_config)]
        #[with(1, 1, vec![KeyPair::from_private_key_str(constants::PRIVATE_KEY).unwrap()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populates the store and materialises documents and schema.
            let documents = populate_and_materialize(&mut node, &config).await;

            let client = http_test_client(&node).await;
            let document_id = documents[0].id();
            let public_key = config.authors[0].public_key();

            // Selected fields need to be alphabetically sorted because that's what the `json`
            // macro that is used in the assert below produces.
            let received_entry_args = client
                .post("/graphql")
                .json(&json!({
                    "query":
                        format!(
                            "{{
                            nextArgs(
                                publicKey: \"{}\",
                                viewId: \"{}\"
                            ) {{
                                logId,
                                seqNum,
                                backlink,
                                skiplink
                            }}
                        }}",
                            public_key,
                            document_id.as_str()
                        )
                }))
                .send()
                .await
                .json::<Response>()
                .await;

            assert!(received_entry_args.is_ok());
            assert_eq!(
                received_entry_args.data,
                value!({
                    "nextArgs": {
                        "logId": "0",
                        "seqNum": "2",
                        "backlink": "002015f3c7541991918e612be61ab58adf2316c9499a91dbfa012077de77c1620220",
                        "skiplink": null,
                    }
                })
            );
        })
    }

    #[rstest]
    fn next_args_error_response() {
        test_runner(|node: TestNode| async move {
            let client = http_test_client(&node).await;
            let response = client
                .post("/graphql")
                .json(&json!({
                    "query": r#"{
                    nextArgs(publicKey: "nope") {
                        logId
                    }
                }"#,
                }))
                .send()
                .await;

            let response: Response = response.json().await;
            assert_eq!(
                response.errors[0].message,
                "Invalid value for argument \"publicKey\", expected type \"PublicKey\""
            )
        })
    }
}
