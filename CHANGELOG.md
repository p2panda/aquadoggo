# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- Introduce libp2p networking service and configuration [#282](https://github.com/p2panda/aquadoggo/pull/282)
- Rework test runner and test node population patterns and refactor test_utils [#277](https://github.com/p2panda/aquadoggo/pull/277)
- Implement API changes to p2panda-rs storage traits, new and breaking db migration [#268](https://github.com/p2panda/aquadoggo/pull/268)
- Move all test utils into one module [#275](https://github.com/p2panda/aquadoggo/pull/275)

### Fixed

- Fix race-condition of mutably shared static schema store during testing [#269](https://github.com/p2panda/aquadoggo/pull/269)

## [0.4.0]

### Added

- GraphQL replication service gets and verifies new entries and inserts them into the db [#137](https://github.com/p2panda/aquadoggo/pull/137)
- Dynamically generated GraphQL types and query fields for accessing materialised documents [#141](https://github.com/p2panda/aquadoggo/pull/141) 🥞
- `validation` and `domain` modules used for publish and next args API [#204](https://github.com/p2panda/aquadoggo/pull/204) 🥞
- Schema task and schema provider that update when new schema views are materialised [#166](https://github.com/p2panda/aquadoggo/pull/166) 🥞
- Service ready signal [#218](https://github.com/p2panda/aquadoggo/pull/218)
- Validate operations against their claimed schema [#245](https://github.com/p2panda/aquadoggo/pull/235)

### Changed

- Refactor scalars and replication API, replace `graphql-client` with `gql_client` [#184](https://github.com/p2panda/aquadoggo/pull/184)
- Give error types of worker a string for better debugging [#194](https://github.com/p2panda/aquadoggo/pull/194)
- Bump `p2panda-rs` which now supports log id's starting from `0` [#207](https://github.com/p2panda/aquadoggo/pull/207)
- Removed unused field `entry_hash` from operation data model [#221](https://github.com/p2panda/aquadoggo/pull/221)
- Detach test helpers from test storage provider implementation [#237](https://github.com/p2panda/aquadoggo/pull/237)
- Remove `Scalar` suffix from scalar types in GraphQL schema [#231](https://github.com/p2panda/aquadoggo/pull/231)
- Implement new API for untagged operations [#245](https://github.com/p2panda/aquadoggo/pull/235)
- Use `DocumentStore` trait from `p2panda_rs` [#249](https://github.com/p2panda/aquadoggo/pull/249)
- Increase broadcast channel sizes [#257](https://github.com/p2panda/aquadoggo/pull/257)
- Use transaction in `insert_document()` [#259](https://github.com/p2panda/aquadoggo/pull/259)
- Use `Human` display strings in info logging [#251](https://github.com/p2panda/aquadoggo/pull/251)
- Implement `p2panda-rs` API changes for `PublicKey` and `previous()` [#262](https://github.com/p2panda/aquadoggo/pull/262)

### Fixed

- Don't return errors from `SchemaStore` when a schema could not be constructed [#192](https://github.com/p2panda/aquadoggo/pull/192)
- Filter out deleted documents in `get_documents_by_schema` SQL query [#193](https://github.com/p2panda/aquadoggo/pull/193)
- Resolve implicit `__typename` field on dynamically generated GraphQL objects [#236](https://github.com/p2panda/aquadoggo/pull/236)
- Allow `Content-Type` header [#236](https://github.com/p2panda/aquadoggo/pull/236)
- Do not forget to register `DocumentIdScalar` [#252](https://github.com/p2panda/aquadoggo/pull/252)
- Fix pagination during replication [#199](https://github.com/p2panda/aquadoggo/pull/256)
- Load `DocumentId` to be able to resolve it in meta field query [#258](https://github.com/p2panda/aquadoggo/pull/258)

### Mushroom Time

- Write a lib-level doc string [#263](https://github.com/p2panda/aquadoggo/pull/263)

## [0.3.0]

Released on 2022-07-01: :package: [`crate`](https://crates.io/crates/aquadoggo/0.3.0)

### Added

- Introduce GraphQL endpoint [#81](https://github.com/p2panda/aquadoggo/pull/81)
- Generic task queue with worker pool [#82](https://github.com/p2panda/aquadoggo/pull/82)
- Service manager [#90](https://github.com/p2panda/aquadoggo/pull/90)
- Service error handling, refactor runtime [#92](https://github.com/p2panda/aquadoggo/pull/92)
- Refactor module structure, propagate errors in worker to service manager [#97](https://github.com/p2panda/aquadoggo/pull/97)
- Restructure storage modules and remove JSON RPC [#101](https://github.com/p2panda/aquadoggo/pull/101)
- Implement new methods required for replication defined by `EntryStore` trait [#102](https://github.com/p2panda/aquadoggo/pull/102)
- Implement SQL `OperationStore` [#103](https://github.com/p2panda/aquadoggo/pull/103)
- GraphQL client API with endpoint for retrieving next entry arguments [#119](https://github.com/p2panda/aquadoggo/pull/119)
- GraphQL endpoint for publishing entries [#123](https://github.com/p2panda/aquadoggo/pull/132)
- Implement SQL `DocumentStore` [#118](https://github.com/p2panda/aquadoggo/pull/118)
- Implement SQL `SchemaStore` [#130](https://github.com/p2panda/aquadoggo/pull/130)
- Reduce and dependency tasks [#144](https://github.com/p2panda/aquadoggo/pull/144)
- GraphQL endpoints for replication [#100](https://github.com/p2panda/aquadoggo/pull/100)
- Inform materialization service about new operations [#161](https://github.com/p2panda/aquadoggo/pull/161)
- e2e publish entry tests [#167](https://github.com/p2panda/aquadoggo/pull/167)
- Reschedule pending tasks on startup [#168](https://github.com/p2panda/aquadoggo/pull/168)
- Debug logging in reduce task [#175](https://github.com/p2panda/aquadoggo/pull/175)

### Changed

- Move to `tokio` async runtime [#75](https://github.com/p2panda/aquadoggo/pull/75)
- Implement SQL storage using `p2panda_rs` storage provider traits [#80](https://github.com/p2panda/aquadoggo/pull/80)
- Improve `Signal` efficiency in `ServiceManager` [#95](https://github.com/p2panda/aquadoggo/pull/95)
- `EntryStore` improvements [#123](https://github.com/p2panda/aquadoggo/pull/123)
- Improvements for log and entry table layout [#124](https://github.com/p2panda/aquadoggo/issues/122)
- Update `StorageProvider` API after `p2panda-rs` changes [#129](https://github.com/p2panda/aquadoggo/pull/129)
- Move `SqlStorage` into shared `Context` [#135](https://github.com/p2panda/aquadoggo/pull/135)
- Refactor tests to use fixtures exported from `p2panda-rs` [#147](https://github.com/p2panda/aquadoggo/pull/147)
- Use `DocumentViewId` for previous operations [#147](https://github.com/p2panda/aquadoggo/pull/147)
- Use `VerifiedOperation` [#158](https://github.com/p2panda/aquadoggo/pull/158)
- Refactor `test_db` helper method [#176](https://github.com/p2panda/aquadoggo/pull/176)
- Update `publishEntry` params and `nextEntryArgs` response fields [#181](https://github.com/p2panda/aquadoggo/pull/181)
- Improve test runtime by reducing selected tests' iteration count [#202](https://github.com/p2panda/aquadoggo/pull/202)

### Fixed

- Fix high CPU usage of idle workers [#136](https://github.com/p2panda/aquadoggo/pull/136)
- Improve CI, track test coverage [#139](https://github.com/p2panda/aquadoggo/pull/139)
- Fix compatibility with PostgreSQL, change sqlx runtime to `tokio` [#170](https://github.com/p2panda/aquadoggo/pull/170)
- Use UPSERT for inserting or updating documents [#173](https://github.com/p2panda/aquadoggo/pull/173)
- Don't critically fail reduce task when document missing [#177](https://github.com/p2panda/aquadoggo/pull/177)

## [0.2.0]

_Please note: `aquadoggo-rs` crate is not published yet, due to unpublished dependencies._

### Changed

- Replace schema logs with document logs, changing the behavior the `nextEntryArgs` and `publishEntry` RPC methods, invalidating and deleting all previously published entries [#44](https://github.com/p2panda/aquadoggo/pull/44)
- Rename `Message` to `Operation` everywhere [#48](https://github.com/p2panda/aquadoggo/pull/48)
- Make JSON RPC methods compatible with new document logs flow [#47](https://github.com/p2panda/aquadoggo/pull/47)
- Nicer looking `README.md` for crate [#42](https://github.com/p2panda/aquadoggo/42)
- Support u64 integers [#54](https://github.com/p2panda/aquadoggo/pull/54)

### Fixed

- Fixed bamboo log selection for authors with more than 10 documents [#66](https://github.com/p2panda/aquadoggo/pull/66)

## [0.1.0]

Released on 2021-10-25: :package: [`crate`](https://crates.io/crates/aquadoggo/0.1.0) and 2021-10-26: 🐳 [`docker`](https://hub.docker.com/layers/p2panda/aquadoggo/v0.1.0/images/sha256-be4ba99ce47517dc99e42feda70dd452356190b5f86fcffea44b1bce1d4d315e?context=explore)

### Added

- `panda_queryEntries` RPC method [#23](https://github.com/p2panda/aquadoggo/pull/23)
- Docker support [#22](https://github.com/p2panda/aquadoggo/pull/22)
- `panda_publishEntry` RPC method [#21](https://github.com/p2panda/aquadoggo/pull/21)
- `panda_getEntryArguments` RPC method [#11](https://github.com/p2panda/aquadoggo/pull/11)
- SQL database persistence supporting PostgreSQL, MySQL and SQLite via `sqlx` [#9](https://github.com/p2panda/aquadoggo/pull/9)
- Server configuration via environment variables [#7](https://github.com/p2panda/aquadoggo/pull/7)
- JSON RPC HTTP and WebSocket API server via [#5](https://github.com/p2panda/aquadoggo/pull/5)

### Changed

- Use p2panda-rs 0.2.1 with fixed linter setting [#41](https://github.com/p2panda/aquadoggo/41)
- Use `tide` for HTTP server and `jsonrpc-v2` for JSON RPC [#29](https://github.com/p2panda/aquadoggo/29)

[unreleased]: https://github.com/p2panda/aquadoggo/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/p2panda/aquadoggo/releases/tag/v0.4.0
[0.3.0]: https://github.com/p2panda/aquadoggo/releases/tag/v0.3.0
[0.2.0]: https://github.com/p2panda/aquadoggo/releases/tag/v0.2.0
[0.1.0]: https://github.com/p2panda/aquadoggo/releases/tag/v0.1.0
