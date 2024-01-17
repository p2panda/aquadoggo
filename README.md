<h1 align="center">aquadoggo</h1>

<div align="center">
    <img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/dolphin-left.gif" width="auto" height="30px">
    <strong>p2panda network node</strong>
    <img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/dolphin-right.gif" width="auto" height="30px">
</div>
<br />

<div align="center">
  <!-- CI status -->
  <a href="https://github.com/p2panda/aquadoggo/actions">
    <img src="https://img.shields.io/github/actions/workflow/status/p2panda/aquadoggo/tests.yml?branch=main&style=flat-square" alt="CI Status" />
  </a>
  <!-- Codecov report -->
  <a href="https://app.codecov.io/gh/p2panda/aquadoggo/">
    <img src="https://img.shields.io/codecov/c/gh/p2panda/aquadoggo?style=flat-square" alt="Codecov Report" />
  </a>
  <!-- Crates version -->
  <a href="https://crates.io/crates/aquadoggo">
    <img src="https://img.shields.io/crates/v/aquadoggo.svg?style=flat-square" alt="Crates.io version" />
  </a>
</div>

<div align="center">
  <h3>
    <a href="https://docs.rs/aquadoggo">
      Docs
    </a>
    <span> | </span>
    <a href="https://github.com/p2panda/aquadoggo/releases">
      Releases
    </a>
    <span> | </span>
    <a href="https://p2panda.org/about/contribute">
      Contribute
    </a>
    <span> | </span>
    <a href="https://p2panda.org">
      Website
    </a>
  </h3>
</div>

<br/>

`aquadoggo` is a reference node implementation for [p2panda](https://p2panda.org). It is a intended as a tool for making the design and build of local-first, collaborative p2p applications as simple as possible, and hopefully even a little fun!

`aquadoggo` can run both on your own device for local-first applications, or on a public server when acting as shared community infrastructure. Nodes like `aquadoggo` perform a number of tasks ranging from core p2panda data replication and validation, aiding the discovery and establishment of connections between edge peers, and exposing a developer friendly API used for building applications.

## Features

- Awaits signed operations from clients via GraphQL.
- Verifies the consistency, format and signature of operations and rejects invalid ones.
- Stores operations of the network in an SQL database of your choice (SQLite, PostgreSQL).
- Materializes views on top of the known data.
- Answers filtered, sorted and paginated data queries via GraphQL.
- Discovers other nodes in local network and internet.
- Establishes peer-to-peer connections via UDP holepunching or via relays.
- Replicates data efficiently with other nodes.

## Who is this for?

`aquadoggo` might be interesting for anyone who wants to participate in a p2p network. This could be as a node maintainer, an application developer or simply someone wanting to learn more about p2p networking in a hands-on fashion.

If you are familiar with (or are keen to learn) how to use command line interfaces then you're able to deploy a node on your own machine, you can then experiment with creating data schemas, publishing and replicating data, and then querying it again using the GraphQL playground. Check out the [resources](#Resources) section for ideas on next steps when you're ready.

## What can I build with this?

Many applications which rely on being able to store and retrieve data from a persistent store could likely be built using `aquadoggo` as their data layer. `aquadoggo` can be considered as a p2p "backend", which takes some of the complexity out of p2p development, leaving you to focus on building applications using your preferred tools.

> If you want to build a client application which communicates with an `aquadoggo` you will need to have some experience with web development or the Rust programming language. For writing an application using Rust you can import `aquadoggo` directly in your code. If building a TypeScript web frontend which will interface with a local or remote node, you can import the small TypeScript client library [`shirokuma`](https://github.com/p2panda/shirokuma) to your project. We have plans for making it easier to interact with `aquadoggo` using other languages in the future.

Some example applications which could be built on top of `aquadoggo` are:

- 🥄 **Community centre resource management:** Members of the centre want to manage some shared resources (table tennis, tools, cooking equipment), they each run an app ([Tauri](https://tauri.app/) desktop app with a bundled `aquadoggo` inside) on their own devices, where they can add resources, view availability and making loan requests. Discovery and syncing of data occurs automatically when member's devices are on the same local network.
    <details>
    <summary>See config</summary>
    <br>

    ```toml
    # Schemas needed for our resource management application
    allow_schema_ids = [
        "resource_0020c3accb0b0c8822ecc0309190e23de5f7f6c82f660ce08023a1d74e055a3d7c4d",
        "resource_booking_request_0020aaabb3edecb2e8b491b0c0cb6d7d175e4db0e9da6003b93de354feb9c52891d0",
        "resource_booking_accepted_00209a75d6f1440c188fa52555c8cdd60b3988e468e1db2e469b7d4425a225eba8ec",
    ]

    # Enable mDNS discovery to automatically find other nodes on the local network and share data with them
    mdns = true
    ```
    </details>
- 🐦 **Local ecology monitoring:** Village residents want to collect data on bird species which are sighted in their area over the year. They want anyone with the app to be able to upload a sighting. All the residents run a native Android app on their smartphone, and they make use of a number of relay nodes which enables discovery and p2p or relayed connection establishment.
    <details>
    <summary>See config</summary>
    <br>

    _app node config_
    ```toml
    # Schemas needed for our ecology monitoring application
    allow_schema_ids = [
        "bird_species_0020c3accb0b0c8822ecc0309190e23de5f7f6c82f660ce08023a1d74e055a3d7c4d",
        "bird_sighting_0020aaabb3edecb2e8b491b0c0cb6d7d175e4db0e9da6003b93de354feb9c52891d0",
    ]

    # Addresses of the relay nodes helping us to connect the residents over the internet
    relay_addresses = [
        "203.0.113.1:2022",
        "198.51.100.21:2022",
    ]
    ```

    _relay node config_
    ```toml
    # A relay doesn't need to support any schemas
    allow_schema_ids = []

    # Enable relay mode
    relay_mode = true
    ```
    </details>
- 🗞️ **Coop notice boards:** residents of a group of housing coops want to start a collaborative notice board. Each coop deploys a node on their local network and residents access a web-app to post and view ads or news. They're already using a shared VPN so nodes can connect directly, but only some coops are allowed to join the noticeboard network.
    <details>
    <summary>See config</summary>
    <br>

    ```toml
    # Schemas needed for our coop notice board application
    allow_schema_ids = [
        "notice_board_0020c3accb0b0c8822ecc0309190e23de5f7f6c82f660ce08023a1d74e055a3d7c4d",
        "notice_board_post_0020aaabb3edecb2e8b491b0c0cb6d7d175e4db0e9da6003b93de354feb9c52891d0",
    ]

    # Addresses of already known nodes we can connect directly to
    direct_node_addresses = [
        "192.0.2.78:2022",
        "198.51.100.22:2022",
        "192.0.2.211:2022",
        "203.0.114.123:2022",
    ]

    # Peer ids of allowed peers, these will be the expected identities for the nodes we are connecting
    # directly to
    allowed_peer_ids = [
        "12D3KooWP1ahRHeNp6s1M9qDJD2oyqRsYFeKLYjcjmFxrq6KM8xd",
        "12D3KooWPC9zdWXQ3aCEcxvuct9KUWU5tPsUT6KFo29Wf8jWRW24",
        "12D3KooWDNNSdY8vxYKYZBGdfDTg1ZafxEVuEmh49jtF8rUeMkq2",
        "12D3KooWMKiBvAxynLn7KmqbWdEzA8yq3of6yoLZF1cpmb4Z9fHf",
    ]
    ```
    </details>

We're excited to hear about your ideas! Join our [official chat](https://wald.liebechaos.org/) and reach out.

## Installation

### Command line application

Check out our [Releases](https://github.com/p2panda/aquadoggo/releases/) section where we publish binaries for Linux, RaspberryPi, MacOS and Windows or read [how you can compile](/aquadoggo_cli/README.md#Installation) `aquadoggo` yourself.

### Rust Crate

For using `aquadoggo` in your Rust project, you can add it as a dependency with the following command:

```bash
cargo add aquadoggo
```

## Usage

### Run node

You can also run the node simply as a [command line application](/aquadoggo_cli). `aquadoggo` can be configured in countless ways for your needs, read our [configuration](/aquadoggo_cli/README.md#Usage) section for more examples, usecases and an overview of configuration options.

```bash
# Start a local node on your machine, go to http://localhost:2020/graphql for using the GraphQL playground
aquadoggo

# Check out all configuration options
aquadoggo --help

# Enable logging
aquadoggo --log-level info
```

### Docker

For server deployments you might prefer using [Docker](https://hub.docker.com/r/p2panda/aquadoggo) to run `aquadoggo`.

```bash
docker run -p 2020:2020 -p 2022:2022 -e LOG_LEVEL=info p2panda/aquadoggo
```

### Embed node

Run the node directly next to the frontend you're building for full peer-to-peer applications by using the [`aquadoggo`](/aquadoggo) Rust crate. Check out our [Tauri](https://github.com/p2panda/tauri-example) example for writing a desktop app.

```rust
use aquadoggo::{Configuration, Node};
use p2panda_rs::identity::KeyPair;

let config = Configuration::default();
let key_pair = KeyPair::new();
let node = Node::start(key_pair, config).await;
```

### FFI bindings

If you are not working with Rust you can create FFI bindings from the `aquadoggo` crate into your preferred programming language. Dealing with FFI bindings can be a bit cumbersome and we do not have much prepared for you (yet), but check out our [Meli](https://github.com/p2panda/meli/) Android project as an example on how we dealt with FFI bindings for Dart / Flutter.

## Query API

As an application developer the interface you are likely to use the most is the GraphQL query API. For whichever schema your node supports a custom query API is generated, you use this to fetch data into your app. Results from a collection query can be paginated, sorted and filtered.

Fetch one "mushroom" by its id, returning values for only the selected fields:

```graphql
{
  mushroom: mushroom_0020c3accb0b0c8822ecc0309190e23de5f7f6c82f660ce08023a1d74e055a3d7c4d(
    id: "0020aaabb3edecb2e8b491b0c0cb6d7d175e4db0e9da6003b93de354feb9c52891d0"
  ) {
    fields {
      description
      edible
      latin
      title
    }
  }
}
```

<details>
<summary>Example query response</summary>
<br>

```json
{
  "mushroom": {
    "description": "Its scientific name rhacodes comes from the Greek word rhakos, which means a piece of cloth. It does often have a soft, ragged fabric-like appearance.",
    "edible": true,
    "latin": "Chlorophyllum rhacodes",
    "title": "Shaggy parasol"
  }
}
```
</details>

Fetch all "events" with ordering and filtering as well as selecting some meta fields. Here only events between the specified dates and with a title containing the string 'funtastic' will be returned, they will be arranged in ascending chronological order:

```graphql
{
  events: all_events_0020aaabb3edecb2e8b491b0c0cb6d7d175e4db0e9da6003b93de354feb9c52891d0(
    first: 20
    orderBy: "happening_at"
    orderDirection: ASC
    filter: {
      title: { contains: "funtastic" }
      happening_at: { gte: 1677676480, lte: 1696162480 }
    }
  ) {
    totalCount
    documents {
      meta {
        owner
        documentId
        viewId
      }
      fields {
        title
        happening_at
      }
    }
  }
}
```

<details>
<summary>Example query response</summary>
<br>

```json
{
  "events": {
    "totalCount": 2,
    "documents": [
      {
        "meta": {
          "owner": "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96",
          "documentId": "0020f3214a136fd6d0a649e14432409bb28a59a6caf723fa329129c404c92574cb41",
          "viewId": "00206e365e3a6a9b66dfe96ea4b3b3b7c61b250330a46b0c99134121603db5feef11"
        },
        "fields": {
          "title": "Try funtasticize!!",
          "happening_at": 1680264880
        }
      },
      {
        "meta": {
          "owner": "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96",
          "documentId": "002048a55d9265a16ba44b5f3be3e457238e02d3219ecca777d7b4edf28ba2f6d011",
          "viewId": "002048a55d9265a16ba44b5f3be3e457238e02d3219ecca777d7b4edf28ba2f6d011"
        },
        "fields": {
          "title": "Is funtastic even a real word?",
          "happening_at": 1693484080
        }
      }
    ]
  }
}
```
</details>

## Resources

- 🐬 Deploy your own `aquadoggo` following the [tutorial](https://p2panda.org/tutorials/aquadoggo)
- 🛠️ Create your own schemas using [`fishy`](https://github.com/p2panda/fishy)
- 🛼 Open the GraphQL playground in your browser, served under `http://localhost:2020/graphql`
- 📖 Try the [mushroom app tutorial](https://p2panda.org/tutorials/mushroom-app)
- 🔬 Manually publish data to a node [`send-to-node`](https://github.com/p2panda/send-to-node)
- 🐼 [Learn more](https://p2panda.org) about how p2panda works

## What shouldn't I do with `aquadoggo`?

`aquadoggo` is built using the [p2panda](https://p2panda.org) protocol which is in development and some planned features are still missing, the main ones being:

- **Capabilities:** Currently all data can be edited by any author who has access to the network. In many cases, permissions can be handled where needed on the client side (planned mid-2024).
- **Privacy:** While node communication is encrypted with TLS the data stored on nodes itself is not. Integration of [MLS](https://p2panda.org/specifications/aquadoggo/encryption/) is underway but not complete yet.
- **Deletion:** Network-wide purging of data is dependent on having a capabilities system already in place, so these two features will arrive together.
- **Anonymity:** Networking exposes sensitive data, we're waiting for [Arti](https://tpo.pages.torproject.net/core/arti/) supporting Onion Services to make this a configurable option.

As well as these yet-to-be implemented features, there are also general networking concerns (exposing your IP address, sharing data with untrusted peers) that you should take into account when participating in any network, and particularily in peer-to-peer networks.

So although `aquadoggo` is already very useful in many cases, there are others where it won't be a good fit yet or we would actively warn against use. For now, any uses which would be handling especially sensitive data are not recommended, and any users who have special network security requirements need to take extra precautions. Reach out on our [official chat](https://wald.liebechaos.org/) if you have any questions.

## License

GNU Affero General Public License v3.0 [`AGPL-3.0-or-later`](LICENSE)

## Supported by

<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/ngi-logo.png" width="auto" height="80px"><br />
<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/nlnet-logo.svg" width="auto" height="80px"><br />
<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/eu-flag-logo.png" width="auto" height="80px">

*This project has received funding from the European Union’s Horizon 2020
research and innovation programme within the framework of the NGI-POINTER
Project funded under grant agreement No 871528 and NGI-ASSURE No 957073*
