# p2panda-node CLI

Node server with JSON RPC API for the p2panda network.

## Usage

```
USAGE:
    p2panda [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -d, --data-dir <data-dir>    Path to data folder, $HOME/.local/share/p2panda by default on Linux
```

## Environment variables

* `DATABASE_URL` Database url (SQLite, MySQL, PostgreSQL) (default `sqlite:<data-dir>/p2panda-node.sqlite3`).
* `DATABASE_MAX_CONNECTIONS` Maximum number of database connections in pool (default `32`).
* `HTTP_PORT` RPC API HTTP server port (default `2020`).
* `HTTP_THREADS` Number of HTTP server threads to run (default `4`).
* `RPC_MAX_PAYLOAD` Maximum size of RPC request body in bytes (default `512000`, 512kB).
* `WS_MAX_CONNECTIONS` Maximum number of connections for WebSocket RPC server (default `128`).
* `WS_PORT` RPC API WebSocket server port (default `2022`).

## Development

```
cargo run
cargo test
cargo build
```

## License

GNU Affero General Public License v3.0 `AGPL-3.0`
