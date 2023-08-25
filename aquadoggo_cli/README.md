<h1 align="center">aquadoggo</h1>

<div align="center">
  <strong>Node for the p2panda network</strong>
</div>

<br />

<div align="center">
  <h3>
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

Configurable node implementation for the [`p2panda`] network.

## Install

### Pre-compiled binaries

Check out our [Releases](releases) section.

### Compile it yourself

For the following steps you need a
[Rust](https://www.rust-lang.org/learn/get-started) development environment on
your machine.

```bash
# Download source code
git clone https://github.com/p2panda/aquadoggo.git
cd aquadoggo

# Compile binary
cargo build --release

# Copy binary into your path (example)
cp ./target/release/aquadoggo ~/.local/bin
```

## Usage

`aquadoggo` is a powerful node implementation which can run in very different setups during development and in production. It can be configured through a `config.toml` file, environment variables and command line arguments, depending on your needs.

Check out the [`config.toml`](config.toml) file for all configurations and documentation or run `--help` to see all possible command line arguments. All values can also be defined as environment variables, written in CAMEL_CASE (for example `HTTP_PORT=3000`).

```
Usage: aquadoggo [OPTIONS]

Options:
  -c, --config <PATH>
          Path to an optional "config.toml" file for further configuration.

          When not set the program will try to find a `config.toml` file in the same folder the program is executed in and otherwise in the regarding operation systems XDG config directory ("$HOME/.config/aquadoggo/config.toml" on Linux).

  -s, --allow-schema-ids [<SCHEMA_ID>...]
          List of schema ids which a node will replicate, persist and expose on the GraphQL API. Separate multiple values with a whitespace. Defaults to allow _any_ schemas ("*").

          When allowing a schema you automatically opt into announcing, replicating and materializing documents connected to it, supporting applications and networks which are dependent on this data.

          It is recommended to set this list to all schema ids your own application should support, including all important system schemas.

          WARNING: When set to wildcard "*", your node will support _any_ schemas it will encounter on the network. This is useful for experimentation and local development but _not_ recommended for production settings.

  -d, --database-url <CONNECTION_STRING>
          URL / connection string to PostgreSQL or SQLite database. Defaults to an in-memory SQLite database.

          WARNING: By default your node will not persist anything after shutdown. Set a database connection url for production settings to not loose data.

  -p, --http-port <PORT>
          HTTP port for client-node communication, serving the GraphQL API. Defaults to 2020

  -q, --quic-port <PORT>
          QUIC port for node-node communication and data replication. Defaults to 2022

  -k, --private-key <PATH>
          Path to persist your ed25519 private key file. Defaults to an ephemeral key only for this current session.

          The key is used to identify you towards other nodes during network discovery and replication. This key is _not_ used to create and sign data.

          If a path is set, a key will be generated newly and stored under this path when node starts for the first time.

          When no path is set, your node will generate an ephemeral private key on every start up and _not_ persist it.

  -m, --mdns [<BOOL>]
          mDNS to discover other peers on the local network. Enabled by default

          [possible values: true, false]

  -n, --direct-node-addresses [<IP:PORT>...]
          List of known node addresses we want to connect to directly.

          Make sure that nodes mentioned in this list are directly reachable (they need to be hosted with a static IP Address). If you need to connect to nodes with changing, dynamic IP addresses or even with nodes behind a firewall or NAT, do not use this field but use at least one relay.

  -a, --allow-peer-ids [<PEER_ID>...]
          List of peers which are allowed to connect to your node.

          If set then only nodes (identified by their peer id) contained in this list will be able to connect to your node (via a relay or directly). When not set any other node can connect to yours.

          Peer IDs identify nodes by using their hashed public keys. They do _not_ represent authored data from clients and are only used to authenticate nodes towards each other during networking.

          Use this list for example for setups where the identifier of the nodes you want to form a network with is known but you still need to use relays as their IP addresses change dynamically.

  -b, --block-peer-ids [<PEER_ID>...]
          List of peers which will be blocked from connecting to your node.

          If set then any peers (identified by their peer id) contained in this list will be blocked from connecting to your node (via a relay or directly). When an empty list is provided then there are no restrictions on which nodes can connect to yours.

          Block lists and allow lists are exclusive, which means that you should _either_ use a block list _or_ an allow list depending on your setup.

          Use this list for example if you want to allow _any_ node to connect to yours _except_ of a known number of excluded nodes.

  -r, --relay-addresses [<IP:PORT>...]
          List of relay addresses.

          A relay helps discover other nodes on the internet (also known as "rendesvouz" or "bootstrap" server) and helps establishing direct p2p connections when node is behind a firewall or NAT (also known as "holepunching").

          WARNING: This will potentially expose your IP address on the network. Do only connect to trusted relays or make sure your IP address is hidden via a VPN or proxy if you're concerned about leaking your IP.

  -e, --relay-mode [<BOOL>]
          Enable if node should also function as a relay. Disabled by default.

          Other nodes can use relays to aid discovery and establishing connectivity.

          Relays _need_ to be hosted in a way where they can be reached directly, for example with a static IP address through an VPS.

          [possible values: true, false]

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

## Development

```bash
# Run node during development with logging enabled
RUST_LOG=aquadoggo=debug cargo run

# Run tests
cargo test
```

## License

GNU Affero General Public License v3.0 [`AGPL-3.0-or-later`](LICENSE)

## Supported by

<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/ngi-logo.png" width="auto" height="80px"><br />
<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/nlnet-logo.svg" width="auto" height="80px"><br />
<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/eu-flag-logo.png" width="auto" height="80px">

*This project has received funding from the European Union’s Horizon 2020
research and innovation programme within the framework of the NGI-POINTER
Project funded under grant agreement No 871528*
