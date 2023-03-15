# aquadoggo CLI

Node server with GraphQL API for the p2panda network.

## Usage

```
Options:
  -d, --data-dir <DATA_DIR>
          Path to data folder, $HOME/.local/share/aquadoggo by default on Linux

  -P, --http-port <HTTP_PORT>
          Port for the http server, 2020 by default

  -q, --quic-port <QUIC_PORT>
          Port for the QUIC transport, 2022 by default

  -r, --remote-node-addresses <REMOTE_NODE_ADDRESSES>
          URLs of remote nodes to replicate with

  -A <PUBLIC_KEYS_TO_REPLICATE>
          A collection of authors and their logs to replicate.

          eg. -A 123abc="1 2 345" -A 456def="6 7"
          .. adds the authors:
          - "123abc" with log_ids 1, 2, 345
          - "456def" with log_ids 6 7

  -m, --mdns <MDNS>
          Enable mDNS for peer discovery over LAN (using port 5353), true by default

          [possible values: true, false]

      --ping <PING>
          Enable ping for connected peers (send and receive ping packets), true by default

          [possible values: true, false]

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
