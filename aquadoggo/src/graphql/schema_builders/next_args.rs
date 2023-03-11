// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, InputValue, Object, TypeRef};
use dynamic_graphql::{FieldValue, ScalarValue};
use p2panda_rs::api;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::identity::PublicKey;

use crate::db::SqlStore;
use crate::graphql::scalars::{DocumentViewIdScalar, PublicKeyScalar};
use crate::graphql::types::NextArguments;

// Add next args to the query object.
pub fn build_next_args_query(query: Object) -> Object {
    query.field(
        Field::new("nextArgs", TypeRef::named("NextArguments"), |ctx| {
            FieldFuture::new(async move {
                // TODO: In this dynamic query definition the passed args aren't parsed before
                // handing them into this method. They arrive as generic values which we then
                // validate below. This is different from the behavior in statically defined
                // schema, where scalar types are parsed on arrival and any validation errors
                // caught at that point. Maybe we can enable this behavior using the validation
                // traits provided in async-graphql.
                let mut args = ctx.field().arguments()?.into_iter().map(|(_, value)| value);
                let store = ctx.data::<SqlStore>()?;

                // Convert and validate passed parameters.
                let public_key: PublicKey =
                    PublicKeyScalar::from_value(args.next().unwrap())?.into();
                let document_view_id: Option<DocumentViewId> = match args.next() {
                    Some(value) => Some(DocumentViewIdScalar::from_value(value)?.into()),
                    None => None,
                };

                // Calculate next entry's arguments.
                let (backlink, skiplink, seq_num, log_id) =
                    api::next_args(store, &public_key, document_view_id.as_ref()).await?;

                let next_args = NextArguments {
                    log_id: log_id.into(),
                    seq_num: seq_num.into(),
                    backlink: backlink.map(|hash| hash.into()),
                    skiplink: skiplink.map(|hash| hash.into()),
                };

                Ok(Some(FieldValue::owned_any(next_args)))
            })
        })
        .argument(InputValue::new("publicKey", TypeRef::named_nn("PublicKey")))
        .argument(InputValue::new("viewId", TypeRef::named("DocumentViewId")))
        .description("Gimme some sweet sweet next args!"),
    )
}

#[cfg(test)]
mod tests {
    use async_graphql::{value, Response};
    use p2panda_rs::test_utils::memory_store::helpers::PopulateStoreConfig;
    use rstest::rstest;
    use serde_json::json;
    use serial_test::serial;

    use crate::test_utils::{
        graphql_test_client, populate_and_materialize, populate_store_config, test_runner, TestNode,
    };

    #[rstest]
    // Note: This and more tests in this file use the underlying static schema provider which is a
    // static mutable data store, accessible across all test runner threads in parallel mode. To
    // prevent overwriting data across threads we have to run this test in serial.
    //
    // Read more: https://users.rust-lang.org/t/static-mutables-in-tests/49321
    #[serial]
    fn next_args_valid_query() {
        test_runner(|node: TestNode| async move {
            let client = graphql_test_client(&node).await;
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
    #[serial] // See note above on why we execute this test in series
    fn next_args_valid_query_with_document_id(
        #[from(populate_store_config)]
        #[with(1, 1, 1)]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populates the store and materialises documents and schema.
            let (key_pairs, document_ids) = populate_and_materialize(&mut node, &config).await;

            let client = graphql_test_client(&node).await;
            let document_id = document_ids.get(0).expect("There should be a document id");
            let public_key = key_pairs
                .get(0)
                .expect("There should be a key pair")
                .public_key();

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

            print!("{:?}", received_entry_args.errors);
            assert!(received_entry_args.is_ok());
            assert_eq!(
                received_entry_args.data,
                value!({
                    "nextArgs": {
                        "logId": "0",
                        "seqNum": "2",
                        "backlink": "00203c56166a80316aec6b629814ffbafb6bf54d9e30093e122b3cb0f7220e82f15d",
                        "skiplink": null,
                    }
                })
            );
        })
    }

    #[rstest]
    #[serial] // See note above on why we execute this test in series
    fn next_args_error_response() {
        test_runner(|node: TestNode| async move {
            let client = graphql_test_client(&node).await;
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
                "invalid hex encoding in public key string"
            )
        })
    }
}
