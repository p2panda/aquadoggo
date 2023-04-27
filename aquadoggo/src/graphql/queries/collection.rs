// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, Object, TypeRef};
use log::debug;
use p2panda_rs::schema::Schema;

use crate::graphql::constants;
use crate::graphql::resolvers::resolve_document_collection;
use crate::graphql::utils::{collection_name, with_collection_arguments};

/// Adds a GraphQL query for retrieving a paginated, ordered and filtered collection of documents
/// by schema to the passed root query object.
///
/// The query follows the format `all_<SCHEMA_ID>(<...ARGS>)`.
pub fn build_collection_query(query: Object, schema: &Schema) -> Object {
    let schema_id = schema.id().clone();
    let schema = schema.clone();

    query
        .field(with_collection_arguments(
            Field::new(
                format!("{}{}", constants::QUERY_ALL_PREFIX, schema_id),
                TypeRef::named_nn(collection_name(&schema_id)),
                move |ctx| {
                    let schema = schema.clone();
                    debug!(
                        "Query to {}{} received",
                        constants::QUERY_ALL_PREFIX,
                        schema.id()
                    );

                    FieldFuture::new(
                        async move { resolve_document_collection(ctx, schema, None).await },
                    )
                },
            ),
            &schema_id,
        ))
        .description(format!(
            "Query a paginated collection of `{}` documents. \
               The requested collection is filtered and ordered following \
               parameters passed into the query via the available arguments.",
            schema_id.name()
        ))
}

#[cfg(test)]
mod tests {
    use async_graphql::{value, Response, Value};
    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::operation::{PinnedRelationList, RelationList};
    use p2panda_rs::schema::{FieldType, Schema};
    use p2panda_rs::test_utils::fixtures::key_pair;
    use p2panda_rs::{identity::KeyPair, operation::OperationValue};
    use rstest::rstest;
    use serde_json::json;

    use crate::test_utils::{
        add_document, add_schema, add_schema_and_documents, graphql_test_client, test_runner,
        TestNode,
    };

    async fn gimme_some_lyrics(
        node: &mut TestNode,
        key_pair: &KeyPair,
    ) -> (Schema, Vec<DocumentViewId>) {
        add_schema_and_documents(
            node,
            "lyrics",
            vec![
                // X-ray Specs : Oh Bondage Up Yours!
                vec![(
                    "lyric",
                    "Bind me, tie me, chain me to the wall".into(),
                    None,
                )],
                vec![("lyric", "I wanna be a slave to you all".into(), None)],
                vec![("lyric", "Oh bondage, up yours".into(), None)],
                vec![("lyric", "Oh bondage, no more".into(), None)],
                vec![(
                    "lyric",
                    "Chain-store chainsmoke, I consume you all".into(),
                    None,
                )],
                vec![(
                    "lyric",
                    "Chain-gang chainmail, I don't think at all".into(),
                    None,
                )],
                vec![(
                    "lyric",
                    "Thrash, me crush me, beat me till I fall".into(),
                    None,
                )],
                vec![("lyric", "I wanna be a victim for you all".into(), None)],
                vec![(
                    "lyric",
                    "Bind me, tie me, chain me to the wall".into(),
                    None,
                )],
                vec![(
                    "lyric",
                    "I wanna be a slave to you all        ".into(),
                    None,
                )],
                vec![("lyric", "Oh bondage, no more!".into(), None)],
                // Gang of Four : Natural's Not In
                vec![("lyric", "The problem of leisure".into(), None)],
                vec![("lyric", "What to do for pleasure".into(), None)],
                vec![("lyric", "Ideal love, a new purchase".into(), None)],
                vec![("lyric", "A market of the senses".into(), None)],
                vec![("lyric", "Dream of the perfect life".into(), None)],
                vec![("lyric", "Economic circumstances".into(), None)],
                vec![("lyric", "The body is good business".into(), None)],
                vec![("lyric", "Sell out, maintain the interest".into(), None)],
                vec![("lyric", "Remember Lot's wife".into(), None)],
                vec![("lyric", "Renounce all sin and vice".into(), None)],
                vec![("lyric", "Dream of the perfect life".into(), None)],
                vec![("lyric", "This heaven gives me migraine".into(), None)],
                vec![("lyric", "The problem of leisure".into(), None)],
                vec![("lyric", "What to do for pleasure".into(), None)],
                vec![("lyric", "Coercion of the senses".into(), None)],
                vec![("lyric", "We're not so gullible".into(), None)],
                vec![("lyric", "Our great expectations".into(), None)],
                vec![("lyric", "A future for the good".into(), None)],
                vec![("lyric", "Fornication makes you happy".into(), None)],
                vec![("lyric", "No escape from society".into(), None)],
                vec![("lyric", "Natural is not in it".into(), None)],
                vec![("lyric", "Your relations are of power".into(), None)],
                vec![("lyric", "We all have good intentions".into(), None)],
                vec![("lyric", "But all with strings attached".into(), None)],
                vec![("lyric", "Repackaged sex (keeps) your interest".into(), None)],
                vec![("lyric", "This heaven gives me migraine".into(), None)],
            ],
            key_pair,
        )
        .await
    }

    async fn my_karaoke_hits(
        node: &mut TestNode,
        lyrics_view_ids: Vec<DocumentViewId>,
        lyrics_schema: Schema,
        key_pair: &KeyPair,
    ) -> (Schema, Vec<DocumentViewId>) {
        add_schema_and_documents(
            node,
            "songs",
            vec![
                vec![
                    ("artist", "X-ray Specs".into(), None),
                    ("title", "Oh Bondage Up Yours!".into(), None),
                    (
                        "lyrics",
                        vec![
                            lyrics_view_ids[0].clone(),
                            lyrics_view_ids[1].clone(),
                            lyrics_view_ids[2].clone(),
                            lyrics_view_ids[3].clone(),
                            lyrics_view_ids[2].clone(),
                            lyrics_view_ids[3].clone(),
                            lyrics_view_ids[4].clone(),
                            lyrics_view_ids[5].clone(),
                            lyrics_view_ids[2].clone(),
                            lyrics_view_ids[3].clone(),
                            lyrics_view_ids[2].clone(),
                            lyrics_view_ids[3].clone(),
                            lyrics_view_ids[6].clone(),
                            lyrics_view_ids[7].clone(),
                            lyrics_view_ids[2].clone(),
                            lyrics_view_ids[3].clone(),
                            lyrics_view_ids[2].clone(),
                            lyrics_view_ids[3].clone(),
                            lyrics_view_ids[8].clone(),
                            lyrics_view_ids[9].clone(),
                            lyrics_view_ids[2].clone(),
                            lyrics_view_ids[3].clone(),
                            lyrics_view_ids[2].clone(),
                            lyrics_view_ids[3].clone(),
                            lyrics_view_ids[8].clone(),
                            lyrics_view_ids[9].clone(),
                            lyrics_view_ids[2].clone(),
                            lyrics_view_ids[3].clone(),
                            lyrics_view_ids[2].clone(),
                            lyrics_view_ids[3].clone(),
                            lyrics_view_ids[2].clone(),
                            lyrics_view_ids[10].clone(),
                        ]
                        .into(),
                        Some(lyrics_schema.id().to_owned()),
                    ),
                ],
                vec![
                    ("artist", "Gang Of Four".into(), None),
                    ("title", "Natural's Not In".into(), None),
                    (
                        "lyrics",
                        OperationValue::RelationList(RelationList::new(vec![])),
                        Some(lyrics_schema.id().to_owned()),
                    ),
                ],
                vec![
                    ("artist", "David Bowie".into(), None),
                    ("title", "Speed Of Life".into(), None),
                    (
                        "lyrics",
                        OperationValue::RelationList(RelationList::new(vec![])),
                        Some(lyrics_schema.id().to_owned()),
                    ),
                ],
            ],
            &key_pair,
        )
        .await
    }

    #[rstest]
    #[case(
        "".to_string(),
        value!({
            "collection": value!({
                "hasNextPage": false,
                "totalCount": 2,
                "endCursor": "31Ch6qa4mdKcxpWJG4X9Wf5iMvSSxmSGg8cyg9teNR6yKmLncZCmyVUaPFjRNoWcxpeASGqrRiJGR8HSqjWBz5HE",
                "documents": [
                    {
                        "cursor": "273AmFQTk7w6134GhzKUS5tY8qDuaMYBPgbaftZ43G7saiKa73MPapFvjNDixbNjCr5ucNqzNsx2fYdRqRod9U2W",
                        "fields": { "bool": true, },
                        "meta": {
                            "owner": "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96",
                            "documentId": "00200436216389856afb3f3a7d8cb2d2981be85787aebed02031c72eb9c216406c57",
                            "viewId": "00200436216389856afb3f3a7d8cb2d2981be85787aebed02031c72eb9c216406c57",
                        }
                    },
                    {
                        "cursor": "31Ch6qa4mdKcxpWJG4X9Wf5iMvSSxmSGg8cyg9teNR6yKmLncZCmyVUaPFjRNoWcxpeASGqrRiJGR8HSqjWBz5HE",
                        "fields": { "bool": false, },
                        "meta": {
                            "owner": "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96",
                            "documentId": "0020de552d81948f220d09127dc42963071d086a142c9547e701674d4cac83f29872",
                            "viewId": "0020de552d81948f220d09127dc42963071d086a142c9547e701674d4cac83f29872",
                        }
                    }
                ]
            }),
        }),
        vec![]
    )]
    #[case(
        r#"
            (
                first: 1,
                after: "31Ch6qa4mdKcxpWJG4X9Wf5iMvSSxmSGg8cyg9teNR6yKmLncZCmyVUaPFjRNoWcxpeASGqrRiJGR8HSqjWBz5HE",
                orderBy: DOCUMENT_ID,
                orderDirection: ASC,
                filter: {
                    bool: {
                        eq: false
                    }
                }
            )
        "#.to_string(),
        value!({
            "collection": value!({
                "hasNextPage": false,
                "totalCount": 1,
                "endCursor": Value::Null,
                "documents": []
            }),
        }),
        vec![]
    )]
    #[case(
        r#"(first: 0)"#.to_string(),
        Value::Null,
        vec!["out of range integral type conversion attempted".to_string()]
    )]
    #[case(
        r#"(first: "hello")"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"first\", expected type \"Int\"".to_string()]
    )]
    #[case(
        r#"(after: HELLO)"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"after\", expected type \"Cursor\"".to_string()]
    )]
    #[case(
        r#"(after: "00205406410aefce40c5cbbb04488f50714b7d5657b9f17eed7358da35379bc20331")"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"after\", expected type \"Cursor\"".to_string()]
    )]
    #[case(
        r#"(after: 27)"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"after\", expected type \"Cursor\"".to_string()]
    )]
    #[case(
        r#"(orderBy: HELLO)"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"orderBy\", enumeration type \"schema_name_00205406410aefce40c5cbbb04488f50714b7d5657b9f17eed7358da35379bc20331OrderBy\" does not contain the value \"HELLO\"".to_string()]
    )]
    #[case(
        r#"(orderBy: "hello")"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"orderBy\", enumeration type \"schema_name_00205406410aefce40c5cbbb04488f50714b7d5657b9f17eed7358da35379bc20331OrderBy\" does not contain the value \"hello\"".to_string()]
    )]
    #[case(
        r#"(orderDirection: HELLO)"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"orderDirection\", enumeration type \"OrderDirection\" does not contain the value \"HELLO\"".to_string()]
    )]
    #[case(
        r#"(orderDirection: "hello")"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"orderDirection\", enumeration type \"OrderDirection\" does not contain the value \"hello\"".to_string()]
    )]
    #[case(
        r#"(filter: "hello")"#.to_string(),
        Value::Null,
        vec!["internal: is not an object".to_string()]
    )]
    #[case(
        r#"(filter: { bool: { in: ["hello"] }})"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"filter.bool\", unknown field \"in\" of type \"BooleanFilter\"".to_string()]
    )]
    #[case(
        r#"(filter: { hello: { eq: true }})"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"filter\", unknown field \"hello\" of type \"schema_name_00205406410aefce40c5cbbb04488f50714b7d5657b9f17eed7358da35379bc20331Filter\"".to_string()]
    )]
    #[case(
        r#"(filter: { bool: { contains: "hello" }})"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"filter.bool\", unknown field \"contains\" of type \"BooleanFilter\"".to_string()]
    )]
    #[case(
        r#"(meta: "hello")"#.to_string(),
        Value::Null,
        vec!["internal: is not an object".to_string()]
    )]
    #[case(
        r#"(meta: { bool: { in: ["hello"] }})"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"meta\", unknown field \"bool\" of type \"MetaFilterInputObject\"".to_string()]
    )]
    #[case(
        r#"(meta: { owner: { contains: "hello" }})"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"meta.owner\", unknown field \"contains\" of type \"OwnerFilter\"".to_string()]
    )]
    #[case(
        r#"(meta: { documentId: { contains: "hello" }})"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"meta.documentId\", unknown field \"contains\" of type \"DocumentIdFilter\"".to_string()]
    )]
    #[case(
        r#"(meta: { viewId: { contains: "hello" }})"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"meta.viewId\", unknown field \"contains\" of type \"DocumentViewIdFilter\"".to_string()]
    )]
    #[case(
        r#"(meta: { documentId: { eq: 27 }})"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"meta.documentId.eq\", expected type \"DocumentId\"".to_string()]
    )]
    #[case(
        r#"(meta: { viewId: { in: "hello" }})"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"meta.viewId.in\", expected type \"DocumentViewId\"".to_string()]
    )]
    #[case(
        r#"(meta: { owner: { eq: "hello" }})"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"meta.owner.eq\", expected type \"PublicKey\"".to_string()]
    )]
    fn collection_query(
        key_pair: KeyPair,
        #[case] query_args: String,
        #[case] expected_data: Value,
        #[case] expected_errors: Vec<String>,
    ) {
        // Test collection query parameter variations.
        test_runner(move |mut node: TestNode| async move {
            // Add schema to node.
            let schema = add_schema(
                &mut node,
                "schema_name",
                vec![("bool", FieldType::Boolean)],
                &key_pair,
            )
            .await;

            // Publish document on node.
            add_document(
                &mut node,
                schema.id(),
                vec![("bool", true.into())],
                &key_pair,
            )
            .await;

            // Publish another document on node.
            add_document(
                &mut node,
                schema.id(),
                vec![("bool", false.into())],
                &key_pair,
            )
            .await;

            // Configure and send test query.
            let client = graphql_test_client(&node).await;
            let query = format!(
                r#"{{
                collection: all_{type_name}{query_args} {{
                    hasNextPage
                    totalCount
                    endCursor
                    documents {{
                        cursor
                        fields {{
                            bool
                        }}
                        meta {{
                            owner
                            documentId
                            viewId
                        }}
                    }}
                }},
            }}"#,
                type_name = schema.id(),
                query_args = query_args
            );

            let response = client
                .post("/graphql")
                .json(&json!({
                    "query": query,
                }))
                .send()
                .await;

            let response: Response = response.json().await;

            assert_eq!(response.data, expected_data, "{:#?}", response.errors);

            // Assert error messages.
            let err_msgs: Vec<String> = response
                .errors
                .iter()
                .map(|err| err.message.to_string())
                .collect();
            assert_eq!(err_msgs, expected_errors);
        });
    }

    #[rstest]
    #[case(
        r#"fields {
            venues {
                documents {
                    fields {
                        name
                    }
                }
            }
        }"#
    )]
    #[case(
        r#"fields {
            venues {
                documents {
                    meta {
                        documentId
                    }
                }
            }
        }"#
    )]
    fn empty_pinned_relation_list(#[case] query_fields: &str, key_pair: KeyPair) {
        let query_fields = query_fields.to_string();
        test_runner(|mut node: TestNode| async move {
            let venues_schema = add_schema(
                &mut node,
                "venues",
                vec![("name", FieldType::String)],
                &key_pair,
            )
            .await;

            let visited_schema = add_schema(
                &mut node,
                "visited",
                vec![(
                    "venues",
                    FieldType::PinnedRelationList(venues_schema.id().clone()),
                )],
                &key_pair,
            )
            .await;

            add_document(
                &mut node,
                visited_schema.id(),
                vec![(
                    "venues",
                    OperationValue::PinnedRelationList(PinnedRelationList::new(vec![])),
                )],
                &key_pair,
            )
            .await;

            // Configure and send test query.
            let client = graphql_test_client(&node).await;
            let query = format!(
                r#"{{
                    collection: all_{type_name} {{
                        hasNextPage
                        totalCount
                        endCursor
                        documents {{
                            cursor
                            {query_fields}
                        }}
                    }},
                }}"#,
                type_name = visited_schema.id(),
                query_fields = query_fields
            );

            let response = client
                .post("/graphql")
                .json(&json!({
                    "query": query,
                }))
                .send()
                .await;

            let response: Response = response.json().await;

            assert!(response.is_ok());
        });
    }

    #[rstest]
    fn a_funny_bug_which_needs_squishing(key_pair: KeyPair) {
        let schema_fields = vec![("one", FieldType::Boolean), ("two", FieldType::Boolean)];
        let document_values = vec![("one", true.into()), ("two", false.into())];

        test_runner(|mut node: TestNode| async move {
            let schema = add_schema(&mut node, "test_schema", schema_fields, &key_pair).await;

            add_document(&mut node, schema.id(), document_values.clone(), &key_pair).await;
            add_document(&mut node, schema.id(), document_values.clone(), &key_pair).await;
            add_document(&mut node, schema.id(), document_values.clone(), &key_pair).await;
            add_document(&mut node, schema.id(), document_values, &key_pair).await;

            // Configure and send test query.
            let client = graphql_test_client(&node).await;
            let query = format!(
                r#"{{
                    collection: all_{type_name} {{
                        hasNextPage
                        totalCount
                        endCursor
                        documents {{
                            cursor
                            fields {{
                                one
                                two
                            }}
                        }}
                    }},
                }}"#,
                type_name = schema.id(),
            );

            let response = client
                .post("/graphql")
                .json(&json!({
                    "query": query,
                }))
                .send()
                .await;

            let response: Response = response.json().await;

            assert!(response.is_ok());
        });
    }

    #[rstest]
    fn take_me_to_the_karaoke(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let (lyrics_schema, view_ids) = gimme_some_lyrics(&mut node, &key_pair).await;
            let (song_schema, view_ids) =
                my_karaoke_hits(&mut node, view_ids, lyrics_schema, &key_pair).await;

            let client = graphql_test_client(&node).await;
            let query = format!(
                r#"{{
                    collection: all_{type_name}(first: 1) {{
                        hasNextPage
                        totalCount
                        endCursor
                        documents {{
                            cursor
                            fields {{
                                title
                                artist
                                lyrics(first: 5) {{
                                    endCursor
                                    documents {{
                                        fields {{
                                            lyric
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }},
                }}"#,
                type_name = song_schema.id(),
            );

            let response = client
                .post("/graphql")
                .json(&json!({
                    "query": query,
                }))
                .send()
                .await;

            let response: Response = response.json().await;

            assert!(response.is_ok(), "{:#?}", response.errors);
            println!("{:#?}", response.data.into_json().unwrap());
        })
    }
}
