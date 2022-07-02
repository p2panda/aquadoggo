# aquadoggo CLI

Node server with GraphQL API for the p2panda network.

## Usage

```
USAGE:
    aquadoggo [OPTIONS]

FLAGS:
    -h, --help
            Prints help information

    -V, --version
            Prints version information

OPTIONS:
    -A <authors-to-replicate>...
            A collection of authors and their logs to replicate.

            eg. -A 123abc="1 2 345" -A 456def="6 7" .. adds the authors:
            - "123abc" with log_ids 1, 2, 345
            - "456def" with log_ids 6 7

    -d, --data-dir <data-dir>
            Path to data folder, $HOME/.local/share/aquadoggo by default on Linux

    -r, --remote-node-addresses <remote-node-addresses>...
            URLs of remote nodes to replicate with
```

## Environment variables

* `RUST_LOG` Can be set to `warn`, `error`, `info`, `debug`, `trace` for logging.
* `DATABASE_URL` Database url (SQLite, PostgreSQL) (default `sqlite:<data-dir>/aquadoggo-node.sqlite3`).
* `DATABASE_MAX_CONNECTIONS` Maximum number of database connections in pool (default `32`).
* `HTTP_PORT` HTTP server port for GraphQL API (default `2020`).
* `WORKER_POOL_SIZE` Materializer worker pool size (default `16`).

**Example:**

```
RUST_LOG=debug DATABASE_URL=postgres://postgres:postgres@localhost:5432/db ./aquadoggo
```

## Development

```
cargo run
cargo test
cargo build
```

## License

GNU Affero General Public License v3.0 [`AGPL-3.0-or-later`](LICENSE)

## Supported by

<img src="https://p2panda.org/images/ngi-logo.png" width="auto" height="80px"><br />
<img src="https://p2panda.org/images/eu-flag-logo.png" width="auto" height="80px">

*This project has received funding from the European Union’s Horizon 2020 research and innovation programme within the framework of the NGI-POINTER Project funded under grant agreement No 871528*
