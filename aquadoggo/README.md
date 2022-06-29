<h1 align="center">aquadoggo</h1>

<div align="center">
  <strong>Embeddable p2panda network node</strong>
</div>

<br />

<div align="center">
  <!-- CI status -->
  <a href="https://github.com/p2panda/aquadoggo/actions">
    <img src="https://img.shields.io/github/workflow/status/p2panda/aquadoggo/tests?style=flat-square" alt="CI Status" />
  </a>
  <!-- Crates version -->
  <a href="https://crates.io/crates/aquadoggo">
    <img src="https://img.shields.io/crates/v/aquadoggo.svg?style=flat-square" alt="Crates.io version" />
  </a>
</div>

<div align="center">
  <h3>
    <a href="#installation">
      Installation
    </a>
    <span> | </span>
    <a href="https://docs.rs/aquadoggo">
      API
    </a>
    <span> | </span>
    <a href="https://github.com/p2panda/aquadoggo/releases">
      Releases
    </a>
    <span> | </span>
    <a href="https://github.com/p2panda/handbook#how-to-contribute">
      Contributing
    </a>
  </h3>
</div>

<br/>

Configurable node server implementation for the [`p2panda`] network which can be embedded inside your Rust program.

[`p2panda`]: https://github.com/p2panda/handbook

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

let config = Configuration::new(None)?;
let node = Node::start(config).await;
```

[`Tauri`]: https://tauri.studio

## Installation

With [`cargo-edit`](https://github.com/killercup/cargo-edit) installed run:

```sh
$ cargo add aquadoggo
```

[`cargo-edit`]: https://github.com/killercup/cargo-edit

## Development

### Regenerate graphql schema

When you update the graphql code you'll need to manually regenerate the graphql schema files.

In the `aquadoggo/aquadoggo` directory:

```sh
cargo build --bin dump_gql_schema
cargo run --bin dump_gql_schema > src/graphql/replication/client/schema.graphql
```

Note that the `cargo build` step is required first because the `>` in the run step truncates the schema file which is used for code generation.

## License

GNU Affero General Public License v3.0 [`AGPL-3.0-or-later`](LICENSE)

## Supported by

<img src="https://p2panda.org/images/ngi-logo.png" width="auto" height="80px"><br /><img src="https://p2panda.org/images/eu-flag-logo.png" width="auto" height="80px">

*This project has received funding from the European Union’s Horizon 2020 research and innovation programme within the framework of the NGI-POINTER Project funded under grant agreement No 871528*
