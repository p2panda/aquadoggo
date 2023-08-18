# aquadoggo CLI

Node server with GraphQL API for the p2panda network.

## Usage

```
  -d, --data-dir <DATA_DIR>
          Path to data folder, $HOME/.local/share/aquadoggo by default on Linux

  -P, --http-port <HTTP_PORT>
          Port for the http server, 2020 by default

  -q, --quic-port <QUIC_PORT>
          Port for the QUIC transport, 2022 by default for a relay/rendezvous node

  -r, --remote-node-addresses <REMOTE_NODE_ADDRESSES>
          URLs of remote nodes to replicate with

  -m, --mdns <MDNS>
          Enable mDNS for peer discovery over LAN (using port 5353), false by default

          [possible values: true, false]

      --enable-relay-server
          Enable relay server to facilitate peer connectivity, false by default

      --relay-addr <RELAY_ADDR>
          IP address for the relay peer.

          eg. --relay-addr "127.0.0.1"

      --relay-port <RELAY_PORT>
          Port for the relay peer, defaults to expected relay port 2022.

          eg. --relay-port "1234"

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

## Environment variables

* `RUST_LOG` Can be set to `warn`, `error`, `info`, `debug`, `trace` for logging.
* `DATABASE_URL` Database url (SQLite, PostgreSQL) (default `sqlite:<data-dir>/aquadoggo-node.sqlite3`).
* `DATABASE_MAX_CONNECTIONS` Maximum number of database connections in pool (default `32`).
* `HTTP_PORT` HTTP server port for GraphQL API (default `2020`).
* `WORKER_POOL_SIZE` Materializer worker pool size (default `16`).

**Example:**

```bash
# For all debug logs from `aquadoggo` and external crates
RUST_LOG=debug DATABASE_URL=postgres://postgres:postgres@localhost:5432/db cargo run

# For compact info logs, only directly coming from `aquadoggo`
RUST_LOG=aquadoggo=info DATABASE_URL=postgres://postgres:postgres@localhost:5432/db cargo run
```

## Configuring supported schema

If a `config.toml` file is present then `aquadoggo` will read `supported_schema_ids` at start-up
and be configured to only replicate and offer a query API for schema identified by the listed ids.

See `example_config.toml` for further instructions.

## Development

```bash
cargo run
cargo test
cargo build
```

## License

GNU Affero General Public License v3.0 [`AGPL-3.0-or-later`](LICENSE)

## Supported by

<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/ngi-logo.png" width="auto" height="80px"><br />
<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/eu-flag-logo.png" width="auto" height="80px">

*This project has received funding from the European Union’s Horizon 2020 research and innovation programme within the framework of the NGI-POINTER Project funded under grant agreement No 871528*
