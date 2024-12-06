<h1 align="center">aquadoggo</h1>

<div align="center">
  <strong>Embeddable p2panda network node</strong>
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
    <a href="https://aquadoggo.p2panda.org/about/contribute/">
      Contributing
    </a>
    <span> | </span>
    <a href="https://aquadoggo.p2panda.org/">
      Website
    </a>
  </h3>
</div>

<br/>

`aquadoggo` is a reference node implementation for [p2panda](https://aquadoggo.p2panda.org/). It is a intended as a tool for making the design and build of local-first, collaborative p2p applications as simple as possible, and hopefully even a little fun!

`aquadoggo` can run both on your own device for local-first applications, or on a public server when acting as shared community infrastructure. Nodes like `aquadoggo` perform a number of tasks ranging from core p2panda data replication and validation, aiding the discovery and establishment of connections between edge peers, and exposing a developer friendly API used for building applications.

> 📖 Read more about nodes in our [learn](https://aquadoggo.p2panda.org/learn/networks) section<br>
> 🐬 Visit the main repo [README](https://github.com/p2panda/aquadoggo) for more general info 

## Features

- Awaits signed operations from clients via GraphQL.
- Verifies the consistency, format and signature of operations and rejects invalid ones.
- Stores operations of the network in an SQL database of your choice (SQLite, PostgreSQL).
- Materializes views on top of the known data.
- Answers filtered, sorted and paginated data queries via GraphQL.
- Discovers other nodes in local network and internet.
- Establishes peer-to-peer connections via UDP holepunching or via relays.
- Replicates data efficiently with other nodes.

## Installation

For using `aquadoggo` in your Rust project, you can add it as a dependency with the following command:

```bash
cargo add aquadoggo
```

## Example

Run the node directly next to the frontend you're building for full peer-to-peer applications. Check out our [Tauri](https://github.com/p2panda/tauri-example) example for writing a desktop app.

```rust,ignore
use aquadoggo::{Configuration, Node};
use p2panda_rs::identity::KeyPair;

let config = Configuration::default();
let key_pair = KeyPair::new();
let node = Node::start(key_pair, config).await;
```

### FFI bindings

If you are not working with Rust you can create FFI bindings from the `aquadoggo` crate into your preferred programming language. Dealing with FFI bindings can be a bit cumbersome and we do not have much prepared for you (yet), but check out our [Meli](https://github.com/p2panda/meli/) Android project as an example on how we dealt with FFI bindings for Dart / Flutter.

### Command line application

Check out our [Releases](/releases) section where we publish binaries for Linux, RaspberryPi, MacOS and Windows.

## Development

The Protocol Buffers compiler must be installed in order to compile aquadoggo.

On a Debian-based OS run:

```bash
$ sudo apt install -y protobuf-compiler
```

See the [installation documentation](https://grpc.io/docs/protoc-installation/) for more options.

## License

GNU Affero General Public License v3.0 [`AGPL-3.0-or-later`](LICENSE)

## Supported by

<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/ngi-logo.png" width="auto" height="80px"><br />
<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/nlnet-logo.svg" width="auto" height="80px"><br />
<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/eu-flag-logo.png" width="auto" height="80px">

*This project has received funding from the European Union’s Horizon 2020
research and innovation programme within the framework of the NGI-POINTER
Project funded under grant agreement No 871528 and NGI-ASSURE No 957073*
