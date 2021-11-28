# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Breaking change**: Replace schema logs with document logs, changing the behavior of *all* existing RPC methods  [#44](https://github.com/p2panda/aquadoggo/pull/44)
- Nicer looking `README.md` for crate [#42](https://github.com/p2panda/aquadoggo/42)

## [0.1.0]

Released on 2021-10-25: :package: [`crate`](https://crates.io/crates/aquadoggo/0.1.0) and 2021-10-26: 🐳 [`docker`](https://hub.docker.com/layers/p2panda/aquadoggo/v0.1.0/images/sha256-be4ba99ce47517dc99e42feda70dd452356190b5f86fcffea44b1bce1d4d315e?context=explore)

### Changed

- Use p2panda-rs 0.2.1 with fixed linter setting [#41](https://github.com/p2panda/aquadoggo/41)
- Use `tide` for HTTP server and `jsonrpc-v2` for JSON RPC [#29](https://github.com/p2panda/aquadoggo/29)

### Added

- `panda_queryEntries` RPC method [#23](https://github.com/p2panda/aquadoggo/pull/23)
- Docker support [#22](https://github.com/p2panda/aquadoggo/pull/22)
- `panda_publishEntry` RPC method [#21](https://github.com/p2panda/aquadoggo/pull/21)
- `panda_getEntryArguments` RPC method [#11](https://github.com/p2panda/aquadoggo/pull/11)
- SQL database persistence supporting PostgreSQL, MySQL and SQLite via `sqlx` [#9](https://github.com/p2panda/aquadoggo/pull/9)
- Server configuration via environment variables [#7](https://github.com/p2panda/aquadoggo/pull/7)
- JSON RPC HTTP and WebSocket API server via [#5](https://github.com/p2panda/aquadoggo/pull/5)

[Unreleased]: https://github.com/p2panda/aquadoggo/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/p2panda/aquadoggo/releases/tag/v0.1.0
