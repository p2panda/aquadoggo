<h1 align="center">p2panda-node</h1>

<div align="center">
  <strong>Embeddable p2p network node</strong>
</div>

<br />

<div align="center">
  <!-- CI status -->
  <a href="https://github.com/p2panda/node/actions">
    <img src="https://img.shields.io/github/workflow/status/p2panda/node/tests?style=flat-square" alt="CI Status" />
  </a>
  <!-- Crates version -->
  <a href="https://crates.io/crates/p2panda-node">
    <img src="https://img.shields.io/crates/v/p2panda-node.svg?style=flat-square" alt="Crates.io version" />
  </a>
</div>

<div align="center">
  <h3>
    <a href="#installation">
      Installation
    </a>
    <span> | </span>
    <a href="https://github.com/p2panda/node/releases">
      Releases
    </a>
    <span> | </span>
    <a href="https://github.com/p2panda/design-document#get-involved">
      Contributing
    </a>
  </h3>
</div>

<br/>

Configurable node server implementation for the [`p2panda`] network running as a [command line application](/node_cli) or embedded via the [library](/node) inside your Rust program.

[`p2panda`]: https://github.com/p2panda/design-document

## Features

- Awaits signed messages from clients via a JSON RPC API.
- Verifies the consistency, format and signature of messages and rejects invalid ones.
- Stores messages of the network in a SQL database of your choice (SQLite, PostgreSQL or MySQL).
- Materializes views on top of the known data.
- Answers filterable and paginated data queries.
- Discovers other nodes in local network and internet.
- Replicates data with other nodes.

## Example

Embed the node server in your Rust application or web container like [`Tauri`]:

```rust
use p2panda_node::{Configuration, Runtime};

let config = Configuration::new()?;
let node = Runtime::start(config).await;
```

You can also run the node server simply as a command line application:

```sh
$ p2panda --help

USAGE:
    p2panda [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -d, --data-dir <data-dir>    Path to data folder, $HOME/.local/share/p2panda by default on Linux
```

[`Tauri`]: https://tauri.studio

## Installation

With [cargo-edit](https://github.com/killercup/cargo-edit) installed run:

```sh
$ cargo add p2panda-node
```

[cargo-add]: https://github.com/killercup/cargo-edit

## License

GNU Affero General Public License v3.0 `AGPL-3.0`
