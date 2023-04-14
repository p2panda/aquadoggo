# aquadoggo CLI

Node server with GraphQL API for the p2panda network.

## Usage

When running the node as a rendezvous client (`--rendezvous-client true`) both the rendezvous address and peer ID must be provided.

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

  -m, --mdns <MDNS>
          Enable mDNS for peer discovery over LAN (using port 5353), true by default

          [possible values: true, false]

      --ping <PING>
          Enable ping for connected peers (send and receive ping packets), true by default

          [possible values: true, false]

  -C, --rendezvous-client <RENDEZVOUS_CLIENT>
          Enable rendezvous client to facilitate peer discovery via a rendezvous server, false by default

          [possible values: true, false]

  -S, --rendezvous-server <RENDEZVOUS_SERVER>
          Enable rendezvous server to facilitate peer discovery for remote peers, false by default

          [possible values: true, false]

      --rendezvous-address <RENDEZVOUS_ADDRESS>
          The IP address of a rendezvous server in the form of a multiaddress.

          eg. --rendezvous-address "/ip4/127.0.0.1/udp/12345/quic-v1"

      --rendezvous-peer-id <RENDEZVOUS_PEER_ID>
          The peer ID of a rendezvous server in the form of an Ed25519 key encoded as a raw base58btc multihash.

          eg. --rendezvous-peer-id "12D3KooWD3eckifWpRn9wQpMG9R9hX3sD158z7EqHWmweQAJU5SA"

      --relay-client <RELAY_CLIENT>
          Enable relay client to facilitate peer connectivity via a relay server, false by default

          [possible values: true, false]

      --relay-server <RELAY_SERVER>
          Enable relay server to facilitate peer connectivity, false by default

          [possible values: true, false]

      --relay-address <RELAY_ADDRESS>
          The IP address of a relay server in the form of a multiaddress.

          eg. --relay-address "/ip4/127.0.0.1/udp/12345/quic-v1"

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
