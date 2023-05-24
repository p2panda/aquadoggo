<h1 align="center">aquadoggo</h1>

<div align="center">
  <strong>Embeddable p2p network node</strong>
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

Configurable node server implementation for the [`p2panda`] network running as a
[`command line application`] or embedded via the [`library`] inside your Rust program.

> The core p2panda [`specification`] is fully functional but still under review so
> please be prepared for breaking API changes until we reach v1.0. Currently no
> p2panda implementation has received a security audit.

[`command line application`]: /aquadoggo_cli
[`library`]: /aquadoggo
[`p2panda`]: https://p2panda.org/
[`specification`]: https://p2panda.org/specification

## Features

- Awaits signed operations from clients via GraphQL.
- Verifies the consistency, format and signature of operations and rejects invalid ones.
- Stores operations of the network in an SQL database of your choice (SQLite, PostgreSQL).
- Materializes views on top of the known data.
- Answers filterable and paginated data queries via GraphQL.
- Discovers other nodes in local network and internet.
- Replicates data with other nodes.

## Example

Embed the node server in your Rust application or web container like [`Tauri`]:

```rust
use aquadoggo::{Configuration, Node};

let config = Configuration::default();
let node = Node::start(config).await;
```

You can also run the node server simply as a [command line application][`command line application`]:

```bash
# Compile and run local node at http://localhost:2020/graphql
cargo run

# Enable logging
RUST_LOG=aquadoggo=info cargo run
```

.. or run it inside a [Docker](https://hub.docker.com/r/p2panda/aquadoggo) container:

```bash
docker run -p 2020:2020 -e RUST_LOG=aquadoggo=info p2panda/aquadoggo
```

[`Tauri`]: https://tauri.studio

## Installation

```sh
cargo add aquadoggo
```

## License

GNU Affero General Public License v3.0 [`AGPL-3.0-or-later`](LICENSE)

## Supported by

<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/ngi-logo.png" width="auto" height="80px"><br />
<img src="https://raw.githubusercontent.com/p2panda/handbook/main/website/static/images/nlnet-logo.svg" width="auto" height="80px"><br />
<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/eu-flag-logo.png" width="auto" height="80px">

*This project has received funding from the European Union’s Horizon 2020
research and innovation programme within the framework of the NGI-POINTER
Project funded under grant agreement No 871528*
