# aquadoggo CLI

Configurable node for the p2panda network.

## Usage

`aquadoggo` is a powerful node implementation which can run in very different setups during development and in production. It can be configured through a `config.toml` file, environment variables and command line arguments, depending on your needs.

Check out the [`config.toml`](config.toml) file for all configurations and documentation.

```bash
# Show all possible command line arguments
aquadoggo --help

# Run aquadoggo in local development mode (default)
aquadoggo
```

## Development

```bash
# Run node during development with logging enabled
RUST_LOG=aquadoggo=debug cargo run

# Run all tests
cargo test

# Compile release binary
cargo build --release
```

## License

GNU Affero General Public License v3.0 [`AGPL-3.0-or-later`](LICENSE)

## Supported by

<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/ngi-logo.png" width="auto" height="80px"><br />
<img src="https://raw.githubusercontent.com/p2panda/.github/main/assets/eu-flag-logo.png" width="auto" height="80px">

*This project has received funding from the European Union’s Horizon 2020 research and innovation programme within the framework of the NGI-POINTER Project funded under grant agreement No 871528*
