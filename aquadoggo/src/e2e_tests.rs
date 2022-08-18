// SPDX-License-Identifier: AGPL-3.0-or-later

//! E2E test for aquadoggo.
use std::str::FromStr;
use std::time::Duration;

use async_graphql::Response as GqlResponse;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::entry::encode::sign_and_encode_entry;
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::{Author, KeyPair};
use p2panda_rs::operation::encode::encode_operation;
use p2panda_rs::operation::traits::Actionable;
use p2panda_rs::operation::{Operation, OperationAction, OperationBuilder, OperationValue};
use p2panda_rs::schema::{FieldType, Schema, SchemaId};
use reqwest::Client;
use serde_json::{json, Map, Value};

use crate::{Configuration, Node};

#[tokio::test]
async fn e2e() {
    // Node configuration.
    //
    // We mostly go for default configuration options here. The only thing we want to do change
    // is the database config. We want an in memory sqlite db for this test, the default id to use
    // a postgres db.

    let config = Configuration {
        database_url: Some("sqlite::memory:".to_string()),
        ..Default::default()
    };

    // Start the node.
    //
    // There's quite a lot happening under the hood here. We initialize the db, start a http server with
    // GraphQL endpoints, a service for materializing documents from entries and operation which arrive
    // at the node, and finally a replication service for communicating and replicating with other nodes.
    //
    // Nodes are the workhorses of the p2panda network, we thank you for all your efforts 🙏🏻.

    let aquadoggo = Node::start(config).await; // 🐬🐕

    // Create some authors.
    //
    // This is not a very "real world" step as you would usually have stored your key pair somewhere
    // safe and be loading it into an app. Probably not something you want to copy-paste into your hot
    // new app ;-p

    let panda = KeyPair::from_private_key_str(
        "ddcafe34db2625af34c8ba3cf35d46e23283d908c9848c8b43d1f5d0fde779ea",
    )
    .unwrap();

    // Create a GraphQL client.
    //
    // This is the client which will be communicating with the already running node. For this example we
    // are running a node and client in the same application, in a real world setting they might be on the
    // the same device, or equally they could be speaking across a network. This seperation is important
    // for p2panda, as it's what enables the possibility to build very lightweight clients communicating
    // with remote nodes who persist, replicate, materialize and serve data on the network.

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    // Creating schema.
    //
    // All data in the p2panda network adheres to pre-defined schema. "That's great!" I hear you say,
    // "but who defines the data defining schema?"🐣.... Good question. For that we have two hard-coded
    // p2panda "system schema" (more will appear in the future, but this is enough for now). These are
    // `schema_field_definition` and `schema_definition`. With these two anyone can publish their own
    // schema to a node.
    //
    // Creating schema isn't something you do everytime you run your app. Like creating key pairs
    // it's an initialization step which you (or someone else) does before you get going properly.
    // You could even re-use schema's someone else already published.
    //
    // For that reason the details of this process are hidden here in this handy method. You know
    // where to go if you want to know more.

    let cafe_schema_id = create_cafe_schema(&client).await;

    // Create a new document.
    //
    // Data in the p2panda network is represented by something called a `document`. Documents can
    // be CREATED, UPDATED and DELETED. We do this by publishing data mutations called `operations`
    // which perform these actions.
    //
    // Once again, there are a few steps happening in the background here, look into the respective
    // methods if you want to dig deeper.
    //
    // Here panda wants to create a new cafe called "Panda Cafe". In order to do this we construct
    // and publish a CREATE operation with the full fields defined by the `cafe_schema`. Doing this
    // will create a new document which is materialized on the node. It will only contain one
    // operation right now.

    let panda_cafe_operation = OperationBuilder::new(&cafe_schema_id)
        .action(OperationAction::Create)
        .fields(&[("name", OperationValue::String("Panda Cafe".to_string()))])
        .build()
        .unwrap();

    // Document id's
    //
    // Here we publish the operation which in turn creates the new document, well done panda!
    //
    // Every time a document is mutated it gets a new view id representing the current state
    // of the document. This id is globally unique and can be used to find an specific instance
    // of a document. The first such id, from when a CREATE operation was published is special.
    // It can be used as a general id for this document, often you will use this id when you
    // just want the most current state of the document.

    let panda_cafe_view_id = publish(&client, &panda, &panda_cafe_operation).await;
    let panda_cafe_id = panda_cafe_view_id.clone();

    // Update a document.
    //
    // Panda want's more impact for their new cafe name, they want to add some drama! This means
    // they need to update the document by publishing an UPDATE operation containing the change
    // to the field they want to update.
    //
    // In order to update the correct document, they include the current document view id in the
    // `previous_operations` for this operation. This means this operation will be applied at
    // the correct point in the document.

    let panda_cafe_operation = OperationBuilder::new(&cafe_schema_id)
        .action(OperationAction::Update)
        .fields(&[("name", OperationValue::String("Panda Cafe!".to_string()))])
        .previous_operations(&panda_cafe_view_id)
        .build()
        .unwrap();

    let panda_cafe_view_id = publish(&client, &panda, &panda_cafe_operation).await;

    // Materialization.
    //
    // When operations arrive on a node, a process called `materialization` kicks in which processes
    // operations and creates or updates documents, storing the result in the db for easy access.

    // Query a document.
    //
    // Now that the cafe has been created and updated we can query it from the client. We do can do
    // this using it's schema and document or view id.

    let panda_cafe = query(&client, &panda_cafe_view_id, &cafe_schema_id).await;

    // The fields should represent the current state of the document, or if you query by a specific
    // document view id, then the state at that point in the document. We also get a little meta
    // data with each document, more to come ;-p

    let panda_cafe_name = panda_cafe["fields"]["name"].as_str().unwrap();
    let current_panda_cafe_view_id = panda_cafe["meta"]["viewId"].as_str().unwrap();

    assert_eq!(panda_cafe_name, "Panda Cafe!");
    assert_eq!(current_panda_cafe_view_id, panda_cafe_view_id.to_string());

    // To be continued.........

    aquadoggo.shutdown().await;
}

async fn post(client: &Client, query_str: &str) -> GqlResponse {
    client
        .post("http://127.0.0.1:2020/graphql")
        .json(&json!({
            "query": query_str,
        }))
        .send()
        .await
        .expect("Send query to node")
        .json::<GqlResponse>()
        .await
        .expect("Parse GraphQL response")
}

async fn parse_next_args_response(
    response: GqlResponse,
    field: &str,
) -> (LogId, SeqNum, Option<Hash>, Option<Hash>) {
    let json_value = response.data.into_json().expect("Get response data field");

    let log_id = json_value[field]["logId"].as_str().unwrap();
    let seq_num = json_value[field]["seqNum"].as_str().unwrap();
    let backlink = json_value[field]["backlink"].as_str();
    let skiplink = json_value[field]["skiplink"].as_str();

    (
        LogId::from_str(log_id).unwrap(),
        SeqNum::from_str(seq_num).unwrap(),
        backlink.map(|hash| Hash::from_str(hash).unwrap()),
        skiplink.map(|hash| Hash::from_str(hash).unwrap()),
    )
}

async fn next_args(
    client: &Client,
    author: &Author,
    view_id: Option<&DocumentViewId>,
) -> (LogId, SeqNum, Option<Hash>, Option<Hash>) {
    // @TODO: Describe next args

    let args = match view_id {
        Some(id) => {
            format!("nextEntryArgs(publicKey: \"{author}\", documentViewId: \"{id}\")")
        }
        None => format!("nextEntryArgs(publicKey: \"{author}\")"),
    };

    let query_str = format!(
        "{{
            {args}
            {{
                logId,
                seqNum,
                backlink,
                skiplink
            }}
        }}"
    );

    let response = post(client, &query_str).await;
    parse_next_args_response(response, "nextEntryArgs").await
}

async fn publish(client: &Client, key_pair: &KeyPair, operation: &Operation) -> DocumentViewId {
    // @TODO: Describe entries and logs

    let document_view_id = operation.previous_operations();
    let next_args = next_args(
        &client,
        &Author::from(key_pair.public_key()),
        document_view_id,
    )
    .await;

    let encoded_operation = encode_operation(&operation).expect("Encode operation");
    let (log_id, seq_num, backlink, skiplink) = next_args;

    let encoded_entry = sign_and_encode_entry(
        &log_id,
        &seq_num,
        skiplink.as_ref(),
        backlink.as_ref(),
        &encoded_operation,
        &key_pair,
    )
    .expect("Encode entry");

    let query_str = format!(
        "mutation TestPublishEntry {{
            publishEntry(entry: \"{encoded_entry}\", operation: \"{encoded_operation}\") {{
                logId,
                seqNum,
                backlink,
                skiplink
            }}
        }}"
    );

    let _ = post(client, &query_str).await;

    // Wait a little time for materialization to happen.
    tokio::time::sleep(Duration::from_millis(200)).await;

    encoded_entry.hash().into()
}

async fn query(
    client: &Client,
    document_view_id: &DocumentViewId,
    schema_id: &SchemaId,
) -> Map<String, Value> {
    // @TODO: Describe querying

    let query_str = format!(
        r#"{{
            documentQuery: {schema_id}(viewId: "{document_view_id}") {{
                fields {{ name }}
                meta {{ 
                    viewId
                }}
            }},
        }}"#,
    );

    let response = post(client, &query_str).await;
    response.data.into_json().expect("Get response data field")["documentQuery"]
        .as_object()
        .expect("Unwrap object")
        .to_owned()
}

async fn create_cafe_schema(client: &Client) -> SchemaId {
    // @TODO: Describe schemas

    let shirokuma = KeyPair::new();

    let create_field_op = Schema::create_field("name", FieldType::String);
    let field_id = publish(&client, &shirokuma, &create_field_op).await;

    let create_schema_op = Schema::create("cafe", "A cafe", vec![field_id]);
    let schema_definition_id = publish(&client, &shirokuma, &create_schema_op).await;
    let cafe_schema_id = SchemaId::Application("cafe".to_string(), schema_definition_id);
    cafe_schema_id
}
