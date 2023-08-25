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
    <a href="https://github.com/p2panda/handbook#how-to-contribute">
      Contributing
    </a>
    <span> | </span>
    <a href="https://p2panda.org">
      Website
    </a>
  </h3>
</div>

<br/>

Configurable node server implementation for the [`p2panda`] network which can
be embedded inside your Rust program.

> The core p2panda [`specification`] is fully functional but still under review so
> please be prepared for breaking API changes until we reach v1.0. Currently no
> p2panda implementation has recieved a security audit.

[`p2panda`]: https://p2panda.org
[`specification`]: https://p2panda.org/specification

## Features

- Awaits signed operations from clients via GraphQL.
- Verifies the consistency, format and signature of operations and rejects invalid ones.
- Stores operations of the network in an SQL database of your choice (SQLite, PostgreSQL).
- Materializes views on top of the known data.
- Answers filtered, sorted and paginated data queries via GraphQL.
- Discovers other nodes in local network and internet.
- Establishes direct peer-to-peer connections via UDP holepunching.
- Falls back to relays when direct connections are not possible.
- Replicates data efficiently with other nodes.

## Command line application

Configure and run your own `aquadoggo` instance.

```bash
aquadoggo -c config.toml
```

Read more about [how to use the application](/aquadoggo_cli).

## Rust Crate

### Example

Embed the node server in your Rust application or web container like [`Tauri`]:

```rust
use aquadoggo::{Configuration, Node};
use p2panda_rs::identity::KeyPair;

let config = Configuration::default();
let key_pair = KeyPair::new();
let node = Node::start(key_pair, config).await;
```

[`Tauri`]: https://tauri.studio

### Installation

With [`cargo-edit`](https://github.com/killercup/cargo-edit) installed run:

```bash
$ cargo add aquadoggo
```

[`cargo-edit`]: https://github.com/killercup/cargo-edit

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
<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/eu-flag-logo.png" width="auto" height="80px">

*This project has received funding from the European Union’s Horizon 2020
research and innovation programme within the framework of the NGI-POINTER
Project funded under grant agreement No 871528*
