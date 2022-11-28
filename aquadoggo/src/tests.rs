// SPDX-License-Identifier: AGPL-3.0-or-later

//! E2E test and mini-tutorial for aquadoggo.
use std::str::FromStr;
use std::time::Duration;

use async_graphql::Response as GqlResponse;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::entry::encode::sign_and_encode_entry;
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::{KeyPair, PublicKey};
use p2panda_rs::operation::encode::encode_operation;
use p2panda_rs::operation::traits::Actionable;
use p2panda_rs::operation::{Operation, OperationAction, OperationBuilder};
use p2panda_rs::schema::{FieldType, Schema, SchemaId};
use reqwest::Client;
use serde_json::{json, Map, Value};

use crate::{Configuration, Node};

#[tokio::test]
async fn e2e() {
    // This is an aquadoggo E2E test.
    //
    // It is also an introduction to some key concepts and patterns you will come across when
    // developing with p2panda. Please enjoy.

    // Meet `aquadoggo`.
    //
    // `aquadoggo` is a Rust implementation of a node in the p2panda network. A node is the full
    // peer who interacts with other peers. They persist, replicate, materialise and serve data on
    // the network. A node can be thought of as the "backend" of the p2panda stack. Unlike a
    // "normal" backend though, they may be running inside another application, or on your own
    // device locally (laptop, mobile), or in fact somewhere else across a network. Nodes are
    // designed to be "local first" which means they are fine if there is currently no internet
    // connection on your computer.

    // Node configuration.
    //
    // Before even starting the node, we need to configure it a little. We mostly go for the
    // default options. The only thing we want to do change is the database config. We want an
    // in-memory sqlite database for this test.

    let config = Configuration {
        database_url: Some("sqlite::memory:".to_string()),
        ..Default::default()
    };

    // Start the node.
    //
    // There's quite a lot happening under the hood here. We initialize the database, start a http
    // server with GraphQL endpoints, a service for materialising documents from entries and
    // operation which arrive at the node, and finally a replication service for communicating and
    // replicating with other nodes.
    //
    // Nodes are the workhorses of the p2panda network, we thank you for all your efforts 🙏🏻.

    let aquadoggo = Node::start(config).await; // 🐬🐕

    // Create some authors.
    //
    // This is not a very "real world" step as you would usually have stored your key pair
    // somewhere safe and be loading it into an app. Probably not something you want to copy-paste
    // into your hot new app ;-p

    let panda = KeyPair::from_private_key_str(
        "ddcafe34db2625af34c8ba3cf35d46e23283d908c9848c8b43d1f5d0fde779ea",
    )
    .unwrap();

    // Create a GraphQL client.
    //
    // This is the client which will be communicating with the already running node. For this
    // example we are running a node and client in the same application, in other settings they
    // might be on the the same device, or equally they could be speaking across a network. This
    // separation is important for p2panda, as it's what enables the possibility to build very
    // lightweight clients communicating with nodes who persist, replicate, materialise and
    // serve the data.

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    // Creating schema.
    //
    // All data in the p2panda network adheres to pre-defined schemas. "That's great!" I hear you
    // say, "but who defines the data defining schemas?"🐣.... Good question. For that we have two
    // hard-coded p2panda "system schemas" (more will appear in the future, but this is enough for
    // now). These are `schema_field_definition` and `schema_definition`. With these two anyone can
    // publish their own schema to a node.
    //
    // Creating schemas isn't something you do everytime you run your app. Like creating key pairs
    // it's an initialisation step which you (or someone else) does before you get going properly.
    // You could even re-use schemas someone else already published.
    //
    // For the sake of brevity the details of this process are hidden here in this handy method.
    // You know where to go if you want to know more. In a nutshell: This is publishing data on the
    // network announcing a new schema for nodes to pick up.

    let cafe_schema_id =
        create_schema(&client, "cafe", "A cafe", vec![("name", FieldType::String)]).await;

    // Create a new document.
    //
    // Data in the p2panda network is represented by something called a "document". Documents can
    // be created, updated and deleted. We do this by publishing data mutations called "operations"
    // which perform these actions.
    //
    // Once again, there are a few steps happening in the background here, look into the respective
    // methods if you want to dig deeper.
    //
    // Here panda wants to create a new cafe called "Panda Cafe". In order to do this we construct
    // and publish a CREATE operation with the full fields defined by the `cafe_schema`. Doing this
    // will create a new document which is materialised on the node. It will only contain one
    // operation right now.

    let panda_cafe_operation = OperationBuilder::new(&cafe_schema_id)
        .action(OperationAction::Create)
        .fields(&[("name", "Panda Cafe".into())])
        .build()
        .unwrap();

    // Document id's.
    //
    // Here we publish the operation which in turn creates the new document, well done panda!
    //
    // Every time a document is mutated it gets a new view id representing the current state of the
    // document. This id is globally unique and can be used to find an specific instance of a
    // document. The first such id, from when a CREATE operation was published, is special. It can
    // be used as a general id for this document, often you will use this id when you just want the
    // most current state of the document.

    let panda_cafe_view_id = publish(&client, &panda, &panda_cafe_operation).await;
    let _panda_cafe_id = panda_cafe_view_id.clone();

    // Update a document.
    //
    // Panda wants more impact for their new cafe name, they want to add some drama! This means
    // they need to update the document by publishing an UPDATE operation containing the change to
    // the field they want to update.
    //
    // In order to update the correct document, they include the current document view id in the
    // `previous` for this operation. This means this operation will be applied at the
    // correct point in the document.

    let panda_cafe_operation = OperationBuilder::new(&cafe_schema_id)
        .action(OperationAction::Update)
        .fields(&[("name", "Panda Cafe!".into())])
        .previous(&panda_cafe_view_id)
        .build()
        .unwrap();

    let panda_cafe_view_id = publish(&client, &panda, &panda_cafe_operation).await;

    // Materialisation.
    //
    // When operations arrive on a node, a process called "materialisation" kicks in which
    // processes operations and creates or updates documents, storing the result in the db for easy
    // access.

    // Query a document.
    //
    // Now that the cafe has been created and updated we can query it from the client. We do can do
    // this using it's schema id and document or view id.

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

/// Publish an entry and it's operation to a node.
async fn publish(client: &Client, key_pair: &KeyPair, operation: &Operation) -> DocumentViewId {
    // Publishing operations.
    //
    // There are a few important topics we've glossed over so far. These are "entries" and "logs".
    // They form an important data structures for a p2p protocol such as p2panda. This is an
    // "append-only" log. This is way outside the scope of this example, but you can read more
    // about "Bamboo" (of course, the flavour of log a panda would use) here:
    // https://github.com/AljoschaMeyer/bamboo
    //
    // The main thing to know is that entries are the signed data type which are used to author and
    // verify operations.

    // Composing an entry.
    //
    // Every entry contains a `log_id` and `seq_num`. Both these u64 values must be strictly
    // monotonically incrementing per public_key. Entries and operations which are part of the same
    // document live on the same log and the seq number increases. When new documents are created,
    // the log id increments and the sequence number starts from 0.
    //
    // In order to compose an entry with the correct values, we need to ask our node for them,
    // that's what this method does.

    let next_args = next_args(client, &key_pair.public_key(), operation.previous()).await;

    // Encoding data.
    //
    // Once we have the correct entry aruments we are ready to go. First we encode our operation,
    // then we add the hash of this to an entry which we sign and encode.
    //
    // A LOT more could be said here, please check the specification for much more detail.

    let encoded_operation = encode_operation(operation).expect("Encode operation");
    let (log_id, seq_num, backlink, skiplink) = next_args;

    let encoded_entry = sign_and_encode_entry(
        &log_id,
        &seq_num,
        skiplink.as_ref(),
        backlink.as_ref(),
        &encoded_operation,
        key_pair,
    )
    .expect("Encode entry");

    // This is a plain old GraphQL query string.
    let query_str = format!(
        "mutation TestPublishEntry {{
            publish(entry: \"{encoded_entry}\", operation: \"{encoded_operation}\") {{
                logId,
                seqNum,
                backlink,
                skiplink
            }}
        }}"
    );

    // Which we post to the node.
    let _ = post(client, &query_str).await;

    encoded_entry.hash().into()
}

/// Create a schema.
async fn create_schema(
    client: &Client,
    name: &str,
    description: &str,
    fields: Vec<(&str, FieldType)>,
) -> SchemaId {
    // Schemas.
    //
    // As we learned before, all data on the p2panda network must follow pre-defined schemas. If an
    // application wants to use a new schema it must first be defined. This is the step we will
    // outline here. We assume some existing knowledge of "operations" and "documents".
    //
    // As a schema can be defined and subsequently used by any author, we generate a new key pair
    // here for this task only.

    let shirokuma = KeyPair::new();

    // Publish the schema fields.
    //
    // A schema is defined in two steps. First, we define each field in the schema, then we define
    // the whole schema made up of these parts. The available field types are:
    //
    // - Boolean
    // - Float
    // - Integer
    // - String
    // - Relation
    // - RelationList
    // - PinnedRelation
    // - PinnedRelationList
    //
    // (some of those types will be familiar, some... not so much. Check out the p2panda spec to
    // fill in the gaps).
    //
    // Here we publish each schema field that was passed into this method. They are published as
    // operations who's content follows the `schema_field_definition` schema. As with any other
    // data a new document is created by that.
    //
    // We collect the returned ids of these documents.

    let mut fields_ids = Vec::new();

    for (key, field_type) in fields {
        let create_field_operation = Schema::create_field(key, field_type);
        let field_id = publish(client, &shirokuma, &create_field_operation).await;
        fields_ids.push(field_id);
    }

    // Publish the schema.
    //
    // Now that we have our schema field ids we can publish the schema itself. We perform the same
    // process: create operation, publish and finally retrieve the document id. The operation
    // includes a name, description, and the field ids.
    //
    // The returned id is _important_. This is what we need to use everytime we want to publish
    // data following this schema. It can also be used to query all data from a specific schema, or
    // even set the rules for replication.

    let create_schema_operation = Schema::create(name, description, fields_ids);
    let schema_definition_id = publish(client, &shirokuma, &create_schema_operation).await;

    SchemaId::Application(name.to_string(), schema_definition_id)
}

/// Get the next args for a `public_key` and `document`.
async fn next_args(
    client: &Client,
    public_key: &PublicKey,
    view_id: Option<&DocumentViewId>,
) -> (LogId, SeqNum, Option<Hash>, Option<Hash>) {
    let args = match view_id {
        Some(id) => {
            format!("nextArgs(publicKey: \"{public_key}\", viewId: \"{id}\")")
        }
        None => format!("nextArgs(publicKey: \"{public_key}\")"),
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
    parse_next_args_response(response, "nextArgs").await
}

/// Query a materialised document.
async fn query(
    client: &Client,
    document_view_id: &DocumentViewId,
    schema_id: &SchemaId,
) -> Map<String, Value> {
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

/// Post a GraphQL query.
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

/// Parse next args response.
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
