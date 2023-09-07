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
    use p2panda_rs::schema::{FieldType, Schema, SchemaId};
    use p2panda_rs::test_utils::fixtures::key_pair;
    use p2panda_rs::{identity::KeyPair, operation::OperationValue};
    use rstest::rstest;
    use serde_json::{json, Value as JsonValue};

    use crate::test_utils::{
        add_document, add_schema, add_schema_and_documents, http_test_client, test_runner,
        TestClient, TestNode,
    };

    /// Make a GraphQL collection query for songs stored on the node.
    async fn query_songs(
        client: &TestClient,
        schema_id: &SchemaId,
        song_args: &str,
        lyric_args: &str,
    ) -> JsonValue {
        let response = client
            .post("/graphql")
            .json(&json!({
                "query": &songs_collection_query(schema_id, song_args, lyric_args)
            }))
            .send()
            .await;

        let response: Response = response.json().await;
        assert!(response.is_ok(), "{:#?}", response.errors);
        response.data.into_json().unwrap()
    }

    /// Make a GraphQL collection query for songs stored on the node.
    async fn query_songs_meta_fields_only(
        client: &TestClient,
        schema_id: &SchemaId,
        song_args: &str,
    ) -> JsonValue {
        let response = client
            .post("/graphql")
            .json(&json!({
                "query": &songs_collection_query_meta_fields_only(schema_id, song_args)
            }))
            .send()
            .await;

        let response: Response = response.json().await;
        assert!(response.is_ok(), "{:#?}", response.errors);
        response.data.into_json().unwrap()
    }

    /// Make a GraphQL collection query for lyrics stored on the node.
    async fn query_lyrics(
        client: &TestClient,
        schema_id: &SchemaId,
        lyric_args: &str,
    ) -> JsonValue {
        let response = client
            .post("/graphql")
            .json(&json!({
                "query": &lyrics_collection_query(schema_id, lyric_args)
            }))
            .send()
            .await;

        let response: Response = response.json().await;
        assert!(response.is_ok(), "{:#?}", response.errors);
        response.data.into_json().unwrap()
    }

    // Helper for creating paginated queries over songs and lyrics.
    fn songs_collection_query(type_name: &SchemaId, song_args: &str, lyric_args: &str) -> String {
        format!(
            r#"{{
                query: all_{type_name}{song_args} {{
                    hasNextPage
                    totalCount
                    endCursor
                    documents {{
                        cursor
                        meta {{
                            owner
                            documentId
                            viewId
                        }}
                        fields {{
                            title
                            artist
                            release_year
                            lyrics{lyric_args} {{
                                hasNextPage
                                totalCount
                                endCursor
                                documents {{
                                    cursor
                                    meta {{
                                        owner
                                        documentId
                                        viewId
                                    }}
                                    fields {{
                                        line
                                    }}
                                }}
                            }}
                        }}
                    }}
                }},
            }}"#
        )
    }

    // Helper for creating paginated queries over songs and lyrics.
    fn lyrics_collection_query(type_name: &SchemaId, lyric_args: &str) -> String {
        format!(
            r#"{{
                query: all_{type_name}{lyric_args} {{
                    hasNextPage
                    totalCount
                    endCursor
                    documents {{
                        cursor
                        meta {{
                            owner
                            documentId
                            viewId
                        }}
                        fields {{
                            line
                        }}
                    }}
                }},
            }}"#
        )
    }

    // Helper for creating paginated queries over songs and lyrics.
    fn songs_collection_query_meta_fields_only(type_name: &SchemaId, song_args: &str) -> String {
        format!(
            r#"{{
                query: all_{type_name}{song_args} {{
                    hasNextPage
                    totalCount
                    endCursor
                    documents {{
                        cursor
                        meta {{
                            owner
                            documentId
                            viewId
                        }}
                    }}
                }},
            }}"#
        )
    }

    async fn here_be_some_lyrics(
        node: &mut TestNode,
        key_pair: &KeyPair,
    ) -> (Schema, Vec<DocumentViewId>) {
        add_schema_and_documents(
            node,
            "lyrics",
            vec![
                // X-ray Spex : Oh Bondage Up Yours!
                vec![("line", "Bind me, tie me, chain me to the wall".into(), None)],
                vec![("line", "I wanna be a slave to you all".into(), None)],
                vec![("line", "Oh bondage, up yours".into(), None)],
                vec![("line", "Oh bondage, no more".into(), None)],
                vec![(
                    "line",
                    "Chain-store chainsmoke, I consume you all".into(),
                    None,
                )],
                vec![(
                    "line",
                    "Chain-gang chainmail, I don't think at all".into(),
                    None,
                )],
                vec![(
                    "line",
                    "Thrash, me crush me, beat me till I fall".into(),
                    None,
                )],
                vec![("line", "I wanna be a victim for you all".into(), None)],
                vec![("line", "Bind me, tie me, chain me to the wall".into(), None)],
                vec![("line", "I wanna be a slave to you all        ".into(), None)],
                vec![("line", "Oh bondage, no more!".into(), None)],
                // Gang of Four : Natural's Not In
                vec![("line", "The problem of leisure".into(), None)],
                vec![("line", "What to do for pleasure".into(), None)],
                vec![("line", "Ideal love, a new purchase".into(), None)],
                vec![("line", "A market of the senses".into(), None)],
                vec![("line", "Dream of the perfect life".into(), None)],
                vec![("line", "Economic circumstances".into(), None)],
                vec![("line", "The body is good business".into(), None)],
                vec![("line", "Sell out, maintain the interest".into(), None)],
                vec![("line", "Remember Lot's wife".into(), None)],
                vec![("line", "Renounce all sin and vice".into(), None)],
                vec![("line", "Dream of the perfect life".into(), None)],
                vec![("line", "This heaven gives me migraine".into(), None)],
                vec![("line", "The problem of leisure".into(), None)],
                vec![("line", "What to do for pleasure".into(), None)],
                vec![("line", "Coercion of the senses".into(), None)],
                vec![("line", "We're not so gullible".into(), None)],
                vec![("line", "Our great expectations".into(), None)],
                vec![("line", "A future for the good".into(), None)],
                vec![("line", "Fornication makes you happy".into(), None)],
                vec![("line", "No escape from society".into(), None)],
                vec![("line", "Natural is not in it".into(), None)],
                vec![("line", "Your relations are of power".into(), None)],
                vec![("line", "We all have good intentions".into(), None)],
                vec![("line", "But all with strings attached".into(), None)],
                vec![("line", "Repackaged sex (keeps) your interest".into(), None)],
                vec![("line", "This heaven gives me migraine".into(), None)],
            ],
            key_pair,
        )
        .await
    }

    async fn here_be_some_karaoke_hits(
        node: &mut TestNode,
        lyrics_view_ids: &Vec<DocumentViewId>,
        lyrics_schema: &Schema,
        key_pair: &KeyPair,
    ) -> (Schema, Vec<DocumentViewId>) {
        add_schema_and_documents(
            node,
            "songs",
            vec![
                vec![
                    ("artist", "X-ray Spex".into(), None),
                    ("title", "Oh Bondage Up Yours!".into(), None),
                    ("release_year", 1977.into(), None),
                    ("audio", vec![0, 1, 2, 3][..].into(), None),
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
                    ("release_year", 1979.into(), None),
                    ("audio", vec![4, 5, 6, 7][..].into(), None),
                    (
                        "lyrics",
                        vec![
                            lyrics_view_ids[11].clone(),
                            lyrics_view_ids[12].clone(),
                            lyrics_view_ids[13].clone(),
                            lyrics_view_ids[14].clone(),
                            lyrics_view_ids[15].clone(),
                            lyrics_view_ids[16].clone(),
                            lyrics_view_ids[17].clone(),
                            lyrics_view_ids[18].clone(),
                            lyrics_view_ids[19].clone(),
                            lyrics_view_ids[20].clone(),
                            lyrics_view_ids[21].clone(),
                            lyrics_view_ids[22].clone(),
                            lyrics_view_ids[23].clone(),
                            lyrics_view_ids[24].clone(),
                            lyrics_view_ids[25].clone(),
                            lyrics_view_ids[26].clone(),
                            lyrics_view_ids[27].clone(),
                            lyrics_view_ids[28].clone(),
                            lyrics_view_ids[29].clone(),
                            lyrics_view_ids[30].clone(),
                            lyrics_view_ids[31].clone(),
                            lyrics_view_ids[32].clone(),
                            lyrics_view_ids[33].clone(),
                            lyrics_view_ids[34].clone(),
                            lyrics_view_ids[35].clone(),
                            lyrics_view_ids[35].clone(),
                            lyrics_view_ids[35].clone(),
                            lyrics_view_ids[35].clone(),
                            lyrics_view_ids[35].clone(),
                            lyrics_view_ids[35].clone(),
                            lyrics_view_ids[11].clone(),
                            lyrics_view_ids[12].clone(),
                            lyrics_view_ids[13].clone(),
                            lyrics_view_ids[14].clone(),
                            lyrics_view_ids[15].clone(),
                            lyrics_view_ids[16].clone(),
                            lyrics_view_ids[17].clone(),
                            lyrics_view_ids[18].clone(),
                            lyrics_view_ids[19].clone(),
                            lyrics_view_ids[20].clone(),
                            lyrics_view_ids[21].clone(),
                            lyrics_view_ids[22].clone(),
                            lyrics_view_ids[36].clone(),
                            lyrics_view_ids[36].clone(),
                            lyrics_view_ids[36].clone(),
                        ]
                        .into(),
                        Some(lyrics_schema.id().to_owned()),
                    ),
                ],
                vec![
                    ("artist", "David Bowie".into(), None),
                    ("title", "Speed Of Life".into(), None),
                    ("release_year", 1977.into(), None),
                    ("audio", vec![8, 9, 10, 11][..].into(), None),
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
                "endCursor": "24gc7iHafVKTcfRZfVVV8etkSoJMJVsqs1iYJAuHb8oNp32Vi1PcYw6S5GJ8hNhPmHHbP1weVbACYRctHVz4jXjQ",
                "documents": [
                    {
                        "cursor": "24gZVnL75RPvxMVAiuGT2SgCrHneGZgsvEaiCh5g8qgxGBhcunAffueCUTiyuLDamP1G48KYPmRDBBFG43dh3XJ2",
                        "fields": { 
                            "bool": true,
                            "data": "00010203",
                        },
                        "meta": {
                            "owner": "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96",
                            "documentId": "0020223f123be0f9025c591fba1a5800ca64084e837315521d5b65a870e874ed8b4e",
                            "viewId": "0020223f123be0f9025c591fba1a5800ca64084e837315521d5b65a870e874ed8b4e",
                        }
                    },
                    {
                        "cursor": "24gc7iHafVKTcfRZfVVV8etkSoJMJVsqs1iYJAuHb8oNp32Vi1PcYw6S5GJ8hNhPmHHbP1weVbACYRctHVz4jXjQ",
                        "fields": { 
                            "bool": false,
                            "data": "04050607"
                        },
                        "meta": {
                            "owner": "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96",
                            "documentId": "0020c7dbed85159bbea8f1c44f1d4d7dfbdded6cd43c09ab1a292089e9530964cab9",
                            "viewId": "0020c7dbed85159bbea8f1c44f1d4d7dfbdded6cd43c09ab1a292089e9530964cab9",
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
                after: "24gc7iHafVKTcfRZfVVV8etkSoJMJVsqs1iYJAuHb8oNp32Vi1PcYw6S5GJ8hNhPmHHbP1weVbACYRctHVz4jXjQ",
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
        r#"(
            first: 2,
            filter: {
                data: {
                    eq: "00010203"
                }
            }
        )"#.to_string(),
        value!({
            "collection": value!({
                "hasNextPage": false,
                "totalCount": 1,
                "endCursor": "24gZVnL75RPvxMVAiuGT2SgCrHneGZgsvEaiCh5g8qgxGBhcunAffueCUTiyuLDamP1G48KYPmRDBBFG43dh3XJ2",
                "documents": [
                    {
                        "cursor": "24gZVnL75RPvxMVAiuGT2SgCrHneGZgsvEaiCh5g8qgxGBhcunAffueCUTiyuLDamP1G48KYPmRDBBFG43dh3XJ2",
                        "fields": { 
                            "bool": true,
                            "data": "00010203",
                        },
                        "meta": {
                            "owner": "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96",
                            "documentId": "0020223f123be0f9025c591fba1a5800ca64084e837315521d5b65a870e874ed8b4e",
                            "viewId": "0020223f123be0f9025c591fba1a5800ca64084e837315521d5b65a870e874ed8b4e",
                        }
                    }
                ]
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
        r#"(after: "0020d384b69386867b61acebe6b23d4fac8c1425d5dce339bb3ef7c2218c155b3f9a")"#.to_string(),
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
        vec!["Invalid value for argument \"orderBy\", enumeration type \"schema_name_0020d384b69386867b61acebe6b23d4fac8c1425d5dce339bb3ef7c2218c155b3f9aOrderBy\" does not contain the value \"HELLO\"".to_string()]
    )]
    #[case(
        r#"(orderBy: "hello")"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"orderBy\", enumeration type \"schema_name_0020d384b69386867b61acebe6b23d4fac8c1425d5dce339bb3ef7c2218c155b3f9aOrderBy\" does not contain the value \"hello\"".to_string()]
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
        vec!["Invalid value for argument \"filter\", unknown field \"hello\" of type \"schema_name_0020d384b69386867b61acebe6b23d4fac8c1425d5dce339bb3ef7c2218c155b3f9aFilter\"".to_string()]
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
                vec![("bool", FieldType::Boolean), ("data", FieldType::Bytes)],
                &key_pair,
            )
            .await;

            // Publish document on node.
            add_document(
                &mut node,
                schema.id(),
                vec![("bool", true.into()), ("data", vec![0, 1, 2, 3][..].into())],
                &key_pair,
            )
            .await;

            // Publish another document on node.
            add_document(
                &mut node,
                schema.id(),
                vec![("bool", false.into()), ("data", vec![4, 5, 6, 7][..].into())],
                &key_pair,
            )
            .await;

            // Configure and send test query.
            let client = http_test_client(&node).await;
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
                            data
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
            let client = http_test_client(&node).await;
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
            let client = http_test_client(&node).await;
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
    #[case("", "")]
    #[case("(orderDirection: ASC, orderBy: title)", "")]
    #[case("(orderDirection: DESC, orderBy: DOCUMENT_ID)", "")]
    #[case("(orderDirection: ASC, orderBy: DOCUMENT_VIEW_ID)", "")]
    #[case("(meta: { owner: { eq: \"2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96\" } })", "")]
    #[case("(meta: { owner: { notEq: \"2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96\" } })", "")]
    #[case("(meta: { owner: { in: [ \"2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96\" ] } })", "")]
    #[case("(meta: { owner: { notIn: [ \"2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96\" ] } })", "")]
    #[case("(meta: { documentId: { eq: \"0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543\" } })", "")]
    #[case("(meta: { documentId: { notEq: \"0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543\" } })", "")]
    #[case("(meta: { documentId: { in: [ \"0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543\" ] } })", "")]
    #[case("(meta: { documentId: { notIn: [ \"0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543\" ] } })", "")]
    #[case("(meta: { viewId: { eq: \"0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543\" } })", "")]
    #[case("(meta: { viewId: { notEq: \"0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543\" } })", "")]
    #[case("(meta: { viewId: { in: [ \"0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543\" ] } })", "")]
    #[case("(meta: { viewId: { notIn: [ \"0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543\" ] } })", "")]
    #[case("(filter: { release_year: { gte: 1978 } })", "")]
    #[case("(filter: { release_year: { gt: 1978 } })", "")]
    #[case("(filter: { release_year: { lt: 1978 } })", "")]
    #[case("(filter: { release_year: { lte: 1978 } })", "")]
    #[case("(filter: { release_year: { gte: 1971, lt: 1971 } })", "")]
    #[case(
        "(filter: { release_year: { gt: 1978, lte: 1978, lt: 1978, lte: 1978 } })",
        ""
    )]
    #[case("(filter: { title: { gt: \"a\" } })", "")]
    #[case("(filter: { title: { lte: \"a\" } })", "")]
    #[case("(filter: { title: { contains: \"Up\" } })", "")]
    #[case("(filter: { title: { notContains: \"!\" } })", "")]
    #[case(
        "(filter: { title: { in: [ \"Oh Bondage Up Yours!\", \"Speed Of Life\" ] } })",
        ""
    )]
    #[case("(filter: { title: { notIn: [ \"Natural's Not In\" ] } })", "")]
    #[case("(filter: { title: { eq: \"Natural's Not In\" } })", "")]
    #[case("(filter: { title: { notEq: \"Natural's Not In\", in: [ \"Oh Bondage Up Yours!\", \"Speed Of Life\" ] } })", "")]
    #[case("(filter: { title: { notEq: \"Natural's Not In\" }, release_year: { gt: 1978 }, artist: { in: [ \"X-ray Spex\"] } })", "")]
    #[case("(filter: { audio: { notEq: \"aa\" } })", "")]
    #[case("(filter: { audio: { eq: \"E8\" } })", "")]
    #[case("(filter: { audio: { eq: \"\" } })", "")]
    #[case(
        "(orderDirection: DESC, orderBy: title)",
        "(orderDirection: ASC, orderBy: line)"
    )]
    #[case(
        "(orderDirection: ASC, orderBy: title)",
        "(orderDirection: DESC, orderBy: DOCUMENT_ID)"
    )]
    #[case(
        "(orderDirection: ASC, orderBy: title)",
        "(orderDirection: ASC, orderBy: DOCUMENT_VIEW_ID)"
    )]
    #[case("", "(filter: { line: { gt: \"a\" } })")]
    #[case("", "(filter: { line: { lte: \"a\" } })")]
    #[case("", "(filter: { line: { contains: \"Up\" } })")]
    #[case("", "(filter: { line: { notContains: \"!\" } })")]
    #[case(
        "",
        "(filter: { line: { in: [ \"The body is good business\", \"Oh bondage, up yours\" ] } })"
    )]
    #[case("", "(filter: { line: { notIn: [ \"Oh bondage, up yours\" ] } })")]
    #[case("", "(filter: { line: { eq: \"Oh bondage, up yours\" } })")]
    #[case("", "(filter: { line: { notEq: \"The body is good business\", notIn: [ \"Oh bondage, up yours\" ] } })")]
    fn valid_filter_and_order_queries_pass(
        #[case] song_args: &str,
        #[case] lyric_args: &str,
        key_pair: KeyPair,
    ) {
        let song_args = song_args.to_string();
        let lyric_args = lyric_args.to_string();
        test_runner(move |mut node: TestNode| async move {
            // Publish some lyrics to the node.
            let (lyric_schema, view_ids) = here_be_some_lyrics(&mut node, &key_pair).await;

            // Publish some songs, which contain "title", "artist" and "lyrics" fields.
            let (song_schema, _) =
                here_be_some_karaoke_hits(&mut node, &view_ids, &lyric_schema, &key_pair).await;

            // Init a GraphQL client we'll use to query the node.
            let client = http_test_client(&node).await;

            // Perform a paginated collection query for the songs.
            query_songs(&client, song_schema.id(), &song_args, &lyric_args).await;
        })
    }

    #[rstest]
    fn take_me_to_the_karaoke(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            // Publish some lyrics to the node.
            let (lyric_schema, view_ids) = here_be_some_lyrics(&mut node, &key_pair).await;

            // Publish some songs, which contain "title", "artist" and "lyrics" fields.
            let (song_schema, _) =
                here_be_some_karaoke_hits(&mut node, &view_ids, &lyric_schema, &key_pair).await;

            // Init a GraphQL client we'll use to query the node.
            let client = http_test_client(&node).await;

            // Perform a paginated collection query for the songs on the node identified by the
            // schema id. We don't pass any arguments and so will get up to the default number of
            // items per page, which is 25.
            let data = query_songs(&client, song_schema.id(), "", "").await;

            // There are 3 songs on the node, so we get 3 back.
            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 3);
            // There are no next pages and the total number of songs is 3.
            assert_eq!(data["query"]["hasNextPage"], json!(false));
            assert_eq!(data["query"]["totalCount"], json!(3));

            // Let's just get the first two when the songs are sorted in alphanumeric order by their title.
            let data = query_songs(
                &client,
                song_schema.id(),
                "(first: 2, orderDirection: ASC, orderBy: title)",
                "",
            )
            .await;

            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 2);
            // Now there is a next page but the total count is still 3.
            assert_eq!(data["query"]["hasNextPage"], json!(true));
            assert_eq!(data["query"]["totalCount"], json!(3));

            // Check the first song out.
            let naturals_not_in = data["query"]["documents"][0].clone();
            assert_eq!(
                naturals_not_in["fields"]["title"],
                json!("Natural's Not In")
            );
            assert_eq!(naturals_not_in["fields"]["artist"], json!("Gang Of Four"));
            assert_eq!(naturals_not_in["fields"]["lyrics"]["totalCount"], json!(45));

            // And the next.
            let oh_bondage_up_yours = data["query"]["documents"][1].clone();
            assert_eq!(
                oh_bondage_up_yours["fields"]["title"],
                json!("Oh Bondage Up Yours!")
            );
            assert_eq!(oh_bondage_up_yours["fields"]["artist"], json!("X-ray Spex"));
            assert_eq!(
                oh_bondage_up_yours["fields"]["lyrics"]["totalCount"],
                json!(32)
            );

            // That's more my style, so let's get the lyrics for this song. But there are a lot,
            // so I'll just get the first 2 lines.

            // We can identify the song by its id and then paginate the lyrics field which is a
            // relation list of song lyric lines.
            let oh_bondage_up_yours_id =
                oh_bondage_up_yours["meta"]["documentId"].as_str().unwrap();
            let data = query_songs(
                &client,
                song_schema.id(),
                &format!("(meta: {{ documentId: {{ eq: \"{oh_bondage_up_yours_id}\" }} }})"),
                "(first: 2)", // args for the lyrics collection
            )
            .await;

            let lyrics = data["query"]["documents"][0]["fields"]["lyrics"].clone();
            assert_eq!(lyrics["documents"].as_array().unwrap().len(), 2);
            assert_eq!(lyrics["totalCount"], json!(32));
            assert_eq!(lyrics["hasNextPage"], json!(true));
            assert_eq!(
                lyrics["documents"][0]["fields"]["line"],
                json!("Bind me, tie me, chain me to the wall")
            );
            assert_eq!(
                lyrics["documents"][1]["fields"]["line"],
                json!("I wanna be a slave to you all")
            );

            // Nice, let's get the chorus too. We use the "endCursor" value to make a new request
            // for the next page of results.
            let end_cursor = lyrics["endCursor"].as_str().unwrap();
            let data = query_songs(
                &client,
                song_schema.id(),
                &format!("(meta: {{ documentId: {{ eq: \"{oh_bondage_up_yours_id}\" }} }})"),
                &format!("(after: \"{end_cursor}\", first: 2)"), // args for the lyrics collection
            )
            .await;

            let lyrics = data["query"]["documents"][0]["fields"]["lyrics"].clone();
            assert_eq!(
                lyrics["documents"][0]["fields"]["line"],
                json!("Oh bondage, up yours")
            );
            assert_eq!(
                lyrics["documents"][1]["fields"]["line"],
                json!("Oh bondage, no more")
            );

            // And how about doing a text search over all lyric lines in this song which include
            // the word "bondage".
            let data = query_songs(
                &client,
                song_schema.id(),
                &format!("(meta: {{ documentId: {{ eq: \"{oh_bondage_up_yours_id}\" }} }})"),
                "(filter: { line: { contains: \"bondage\" } })",
            )
            .await;

            let lyrics = data["query"]["documents"][0]["fields"]["lyrics"].clone();
            assert_eq!(lyrics["totalCount"], json!(22));
            assert_eq!(lyrics["documents"].as_array().unwrap().len(), 22);

            // We can perform many filters....

            // Songs released in 1977:
            let data = query_songs(
                &client,
                song_schema.id(),
                "(filter: { release_year: { eq: 1977 } })",
                "",
            )
            .await;

            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 2);
            assert_eq!(data["query"]["totalCount"], json!(2));

            // Songs not by David Bowie released in 1977:
            let data = query_songs(
                &client,
                song_schema.id(),
                "(filter: { release_year: { eq: 1977 }, artist: { notEq: \"David Bowie\" } })",
                "",
            )
            .await;

            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 1);
            assert_eq!(data["query"]["totalCount"], json!(1));

            // Songs released since 1977 which don't contain a "!" in the title:
            let data = query_songs(
                &client,
                song_schema.id(),
                "(filter: { release_year: { gte: 1977 }, title: { notContains: \"!\" } })",
                "",
            )
            .await;

            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 2);
            assert_eq!(data["query"]["totalCount"], json!(2));

            // Songs released in 1977 or 1979 not by David Bowie or Gang Of Four:
            let data = query_songs(
                &client,
                song_schema.id(),
                "(filter: { release_year: { in: [ 1977, 1979 ] }, artist: { notIn: [\"David Bowie\", \"Gang Of Four\"] } })",
                "",
            )
            .await;

            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 1);
            assert_eq!(data["query"]["totalCount"], json!(1));

            // Songs released between 1976 and 1978 but are by David Bowie but don't contain
            // the word "Speed" in the title:
            let data = query_songs(
                &client,
                song_schema.id(),
                "(filter: { release_year: { gte: 1976, lt: 1978 }, artist: { eq: \"David Bowie\" }, title: { notContains: \"Speed Of Life\" } })",
                "",
            )
            .await;

            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 0);
            assert_eq!(data["query"]["totalCount"], json!(0));

            let me = key_pair.public_key().to_string();

            // What about songs I've published to the network:
            let data = query_songs(
                &client,
                song_schema.id(),
                &format!("(meta: {{ owner: {{ eq: \"{me}\" }} }})"),
                "",
            )
            .await;

            assert_eq!(data["query"]["totalCount"], json!(3));

            // Oh yeh, i like that song lyric "This heaven gives me migraine"! I wonder if I can
            // find it....
            let data = query_lyrics(
                &client,
                lyric_schema.id(),
                "(filter: { line: { eq: \"This heaven gives me migraine\" } })",
            )
            .await;

            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 2); // The line is referenced twice in the songs lyrics
            assert_eq!(data["query"]["totalCount"], json!(2));

            // But what song is it from?
            //
            // I can find out using the id of this lyric.
            let lyric_id = data["query"]["documents"][0]["meta"]["documentId"].clone();

            // And filtering songs for ones which reference it in their "lyrics" field.
            let data = query_songs(
                &client,
                song_schema.id(),
                &format!("(filter: {{ lyrics: {{ in: [ {lyric_id} ] }} }})"),
                "",
            )
            .await;

            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 1);
            assert_eq!(data["query"]["totalCount"], json!(1));
            assert_eq!(
                data["query"]["documents"][0]["fields"]["title"],
                json!("Natural's Not In")
            );
        })
    }

    #[rstest]
    fn paginated_query_over_collection_with_application_fields_selected(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            // Publish some lyrics to the node.
            let (lyric_schema, view_ids) = here_be_some_lyrics(&mut node, &key_pair).await;

            // Publish some songs, which contain "title", "artist" and "lyrics" fields.
            let (song_schema, _) =
                here_be_some_karaoke_hits(&mut node, &view_ids, &lyric_schema, &key_pair).await;

            // Init a GraphQL client we'll use to query the node.
            let client = http_test_client(&node).await;

            let data = query_songs(&client, song_schema.id(), "(first: 4)", "").await;
            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 3);

            let data = query_songs(&client, song_schema.id(), "(first: 2)", "").await;
            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 2);
            let end_cursor = data["query"]["endCursor"].clone();

            let data = query_songs(
                &client,
                song_schema.id(),
                &format!("(first: 2, after: {end_cursor})"),
                "",
            )
            .await;
            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 1);
            let end_cursor = data["query"]["endCursor"].clone();

            let data = query_songs(
                &client,
                song_schema.id(),
                &format!("(first: 2, after: {end_cursor})"),
                "",
            )
            .await;
            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 0);
        })
    }

    #[rstest]
    fn paginated_query_over_collection_with_only_meta_fields_selected(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            // Publish some lyrics to the node.
            let (lyric_schema, view_ids) = here_be_some_lyrics(&mut node, &key_pair).await;

            // Publish some songs, which contain "title", "artist" and "lyrics" fields.
            let (song_schema, _) =
                here_be_some_karaoke_hits(&mut node, &view_ids, &lyric_schema, &key_pair).await;

            // Init a GraphQL client we'll use to query the node.
            let client = http_test_client(&node).await;

            let data = query_songs_meta_fields_only(&client, song_schema.id(), "(first: 4)").await;
            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 3);

            let data = query_songs_meta_fields_only(&client, song_schema.id(), "(first: 2)").await;
            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 2);
            let end_cursor = data["query"]["endCursor"].clone();

            let data = query_songs_meta_fields_only(
                &client,
                song_schema.id(),
                &format!("(first: 2, after: {end_cursor})"),
            )
            .await;

            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 1);
            let end_cursor = data["query"]["endCursor"].clone();

            let data = query_songs_meta_fields_only(
                &client,
                song_schema.id(),
                &format!("(first: 2, after: {end_cursor})"),
            )
            .await;
            assert_eq!(data["query"]["documents"].as_array().unwrap().len(), 0);
        })
    }
}
