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
    <a href="https://github.com/p2panda/aquadoggo/releases">
      Releases
    </a>
    <span> | </span>
    <a href="https://github.com/p2panda/design-document#how-to-contribute">
      Contributing
    </a>
  </h3>
</div>

<br/>

Configurable node server implementation for the [`p2panda`] network running as a [`command line application`] or embedded via the [`library`] inside your Rust program.

[`command line application`]: /aquadoggo_cli
[`library`]: /aquadoggo
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
use aquadoggo::{Configuration, Runtime};

let config = Configuration::new(None)?;
let node = Runtime::start(config).await;
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
