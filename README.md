<h1 align="center">aquadoggo</h1>

<div align="center">
  <strong>Embeddable p2p network node</strong>
</div>

<br />

<div align="center">
  <!-- CI status -->
  <a href="https://github.com/p2panda/aquadoggo/actions">
    <img src="https://img.shields.io/github/workflow/status/p2panda/aquadoggo/tests?style=flat-square" alt="CI Status" />
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

Configurable node server implementation for the [`p2panda`] network running as a [`command line application`] or embedded via the [`library`] inside your Rust program.

[`command line application`]: /aquadoggo_cli
[`library`]: /aquadoggo
[`p2panda`]: https://github.com/p2panda/handbook

## Features

- Awaits signed operations from clients via GraphQL.
- Verifies the consistency, format and signature of operations and rejects invalid ones.
- Stores operations of the network in a SQL database of your choice (SQLite, PostgreSQL or MySQL).
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

You can also run the node server simply as a command line application:

```
$ aquadoggo --help

USAGE:
    aquadoggo [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -d, --data-dir <data-dir>    Path to data folder, $HOME/.local/share/aquadoggo by default on Linux
```

[`Tauri`]: https://tauri.studio

## Installation

With [`cargo-edit`](https://github.com/killercup/cargo-edit) installed run:

```sh
$ cargo add aquadoggo
```

[`cargo-edit`]: https://github.com/killercup/cargo-edit

## License

GNU Affero General Public License v3.0 [`AGPL-3.0-or-later`](LICENSE)

## Supported by

<img src="https://p2panda.org/images/ngi-logo.png" width="auto" height="80px"><br /><img src="https://p2panda.org/images/eu-flag-logo.png" width="auto" height="80px">

*This project has received funding from the European Union’s Horizon 2020 research and innovation programme within the framework of the NGI-POINTER Project funded under grant agreement No 871528*
