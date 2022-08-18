// SPDX-License-Identifier: AGPL-3.0-or-later

//! Integration tests for dynamic graphql schema generation and query resolution.
use std::convert::TryInto;
use std::str::FromStr;
use std::time::Duration;

use async_graphql::{value, Response as GqlResponse};
use async_graphql::{Request, Variables};
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::entry::encode::sign_and_encode_entry;
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::entry::{EntryBuilder, LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::{Author, KeyPair};
use p2panda_rs::operation::encode::encode_operation;
use p2panda_rs::operation::traits::Actionable;
use p2panda_rs::operation::{Operation, OperationAction, OperationBuilder, OperationValue};
use p2panda_rs::schema::{FieldType, Schema, SchemaId};
use reqwest::Client;
use reqwest::Response;
use serde_json::json;

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

    let _my_hero = Node::start(config).await; // 🐬🐕

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
    // Data in the p2panda network is encapsulated by something called a `document`. Documents can
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

    let panda_operation_1 = OperationBuilder::new(&cafe_schema_id)
        .action(OperationAction::Create)
        .fields(&[("name", OperationValue::String("Panda Cafe".to_string()))])
        .build()
        .unwrap();

    // Update a document.
    //
    // Panda want's more impact for their new cafe name, they want to add some drama!

    // @TOOD: Keep going with the story.

    let panda_cafe_view_id = publish(&client, &panda, &panda_operation_1).await;

    let panda_operation_2 = OperationBuilder::new(&cafe_schema_id)
        .action(OperationAction::Update)
        .fields(&[("name", OperationValue::String("Panda Cafe!".to_string()))])
        .previous_operations(&panda_cafe_view_id)
        .build()
        .unwrap();

    let _panda_cafe_view_id = publish(&client, &panda, &panda_operation_2).await;

    //     query(&client, &panda_cafe_view_id, &cafe_schema_id).await;
    //     query_view_id(&client, &panda_cafe_view_id, &cafe_schema_id).await;
    //
    //     let penguin = KeyPair::from_private_key_str(
    //         "1c86b2524b48f0ba86103cddc6bdfd87774ab77ab4c0ea989ed0eeab3d28827a",
    //     )
    //     .unwrap();
}

async fn parse_next_args_response(
    response: Response,
    field: &str,
) -> (LogId, SeqNum, Option<Hash>, Option<Hash>) {
    let gql_response = response
        .json::<GqlResponse>()
        .await
        .expect("Parse GraphQL response");

    let json_value = gql_response
        .data
        .into_json()
        .expect("Get response data field");

    println!("{json_value}");

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

    let response = client
        .post("http://127.0.0.1:2020/graphql")
        .json(&json!({
            "query": query_str,
        }))
        .send()
        .await
        .expect("Query node for next args");

    parse_next_args_response(response, "nextEntryArgs").await
}

async fn publish(client: &Client, key_pair: &KeyPair, operation: &Operation) -> DocumentViewId {
    // @TODO: Add description of entries and logs.

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

    let parameters = Variables::from_value(value!({
        "entry": encoded_entry.to_string(),
        "operation": encoded_operation.to_string(),
    }));

    let request = Request::new(
        r#"
    mutation TestPublishEntry($entry: String!, $operation: String!) {
        publishEntry(entry: $entry, operation: $operation) {
            logId,
            seqNum,
            backlink,
            skiplink
        }
    }"#,
    )
    .variables(parameters);

    let response = client
        .post("http://127.0.0.1:2020/graphql")
        .json(&json!({
          "query": request.query,
          "variables": request.variables
        }
        ))
        .send()
        .await
        .expect("Send entry and operation to node");

    // Wait a little time for materialization to happen.
    tokio::time::sleep(Duration::from_millis(200)).await;

    encoded_entry.hash().into()
}

// @TODO: This isn't working, I thought it would...
//
async fn query_view_id(client: &Client, document_view_id: &DocumentViewId, schema_id: &SchemaId) {
    let query = format!(
        r#"{{
        byViewId: {type_name}(viewId: "{view_id}") {{
            fields {{ name }}
        }},
        byDocumentId: {type_name}(id: "{document_id}") {{
            fields {{ name }}
        }}
    }}"#,
        type_name = schema_id.to_string(),
        view_id = document_view_id.to_string(),
        document_id = document_view_id.to_string()
    );

    println!("{query}");

    let response = client
        .post("http://127.0.0.1:2020/graphql")
        .json(&json!({
            "query": query,
        }))
        .send()
        .await
        .expect("Query newest document view id");

    let gql_response = response
        .json::<GqlResponse>()
        .await
        .expect("Parse GraphQL response");

    println!("{:#?}", gql_response);

    let json_value = gql_response
        .data
        .into_json()
        .expect("Get response data field");

    // let view_id = json_value["meta"]["viewId"].as_str().unwrap();

    // println!("{}", view_id)
}

async fn query(client: &Client, document_view_id: &DocumentViewId, schema_id: &SchemaId) {
    let query = format!(
        r#"{{
            collection: all_{type_name} {{
                fields {{ name }}
                meta {{ viewId }}
            }}
        }}"#,
        type_name = schema_id
    );

    let response = client
        .post("http://127.0.0.1:2020/graphql")
        .json(&json!({
            "query": query,
        }))
        .send()
        .await
        .expect("Query newest document view id");

    let gql_response = response
        .json::<GqlResponse>()
        .await
        .expect("Parse GraphQL response");

    println!("{:#?}", gql_response);

    let json_value = gql_response
        .data
        .into_json()
        .expect("Get response data field");

    //     let view_id = json_value["fields"]["name"].as_str().unwrap();
    //
    //     println!("{}", view_id)
}

async fn create_cafe_schema(client: &Client) -> SchemaId {
    let shirokuma = KeyPair::new();

    // @TODO: Finish this description of publishing a schema.

    // Create `cafe` schema.
    //
    // The schema we want to create is called `cafe` and it has one field `name`. To make this happen
    // we send an operation to our node.....

    let create_field_op = Schema::create_field("name", FieldType::String);
    let field_id = publish(&client, &shirokuma, &create_field_op).await;

    let create_schema_op = Schema::create("cafe", "A cafe", vec![field_id]);
    let schema_definition_id = publish(&client, &shirokuma, &create_schema_op).await;
    let cafe_schema_id = SchemaId::Application("cafe".to_string(), schema_definition_id);
    cafe_schema_id
}
