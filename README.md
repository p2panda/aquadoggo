# node

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

## Configuration

Configure the application by creating a `config.toml` file in the data directory `~/.local/share/p2panda` (Linux), `~/Library/Application Support/p2panda` (MacOS) or `~\AppData\Roaming\p2panda\data` (Windows).

Possible values with the regarding defaults are:

```toml
[server]
# RPC API HTTP server port
http_port = 9123
# Number of HTTP server threads to run
http_threads = 4
# Maximum size of RPC request body in bytes (default is 512kB)
max_payload = 512000
# Maximum number of connections for WebSocket RPC server
ws_max_connections = 128
# RPC API WebSocket server port
ws_port = 9456
```

## Development

```
cargo run
cargo test
```

## License

GNU Affero General Public License v3.0 `AGPL-3.0`
