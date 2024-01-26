# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.1]

### Added

- Export ConfigFile which can be de/serialized to and from a config file [#607](https://github.com/p2panda/aquadoggo/pull/607)

### Fixed

- Fix bug where known schemas are not replicated between nodes [#603](https://github.com/p2panda/aquadoggo/pull/603).

## [0.7.0]

### Added

- Introduce a `migrate` API on the Node to allow publishing data programmatically [#598](https://github.com/p2panda/aquadoggo/pull/598)

### Changed

- Update p2panda-rs to `v0.8.1` [#599](https://github.com/p2panda/aquadoggo/pull/599)

## [0.6.0]

### Added

- Serve static files from `blobs` directory [#480](https://github.com/p2panda/aquadoggo/pull/480) 🥞
- Add method to store for pruning document views [#491](https://github.com/p2panda/aquadoggo/pull/491)
- Introduce `BlobStore` [#484](https://github.com/p2panda/aquadoggo/pull/484)
- Task for automatic garbage collection of unused documents and views [#500](https://github.com/p2panda/aquadoggo/pull/500) 🥞
- Blobs directory configuration [#549](https://github.com/p2panda/aquadoggo/pull/549)
- Integrate `Bytes` operation value [#554](https://github.com/p2panda/aquadoggo/pull/554/)
- Implement dependency replication for `blob_v1` and `blob_piece_v1` documents [#514](https://github.com/p2panda/aquadoggo/pull/514)
- Remove deleted/unused blobs from the file system [#571](https://github.com/p2panda/aquadoggo/pull/571)

### Changed

- HTTP routes to serve files with correct content type headers [#544](https://github.com/p2panda/aquadoggo/pull/544)
- Build a byte buffer over paginated pieces when assembling blobs [#547](https://github.com/p2panda/aquadoggo/pull/547)
- Stream blob data in chunks to files to not occupy too much memory [#551](https://github.com/p2panda/aquadoggo/pull/551)
- Remove unused methods from `EntryStore` [#560](https://github.com/p2panda/aquadoggo/pull/560)
- Updates for new hash serialization in p2panda-rs [#569](https://github.com/p2panda/aquadoggo/pull/569)
- Use `libp2p` `0.5.3` [#570](https://github.com/p2panda/aquadoggo/pull/570)
- Optimize test data generation methods [#572](https://github.com/p2panda/aquadoggo/pull/572)
- Use `SocketAddr` in network config instead of `MultiAddr` [#576](https://github.com/p2panda/aquadoggo/pull/576)
- Update `p2panda-rs` to `0.8.0` [#585](https://github.com/p2panda/aquadoggo/pull/585)
- Update `libp2p` to `0.52.4` [#596](https://github.com/p2panda/aquadoggo/pull/596)

### Fixed

- Make sure temporary directory does not run out of scope [#557](https://github.com/p2panda/aquadoggo/pull/557)
- Deduplicate generated schema field by key in proptests [#558](https://github.com/p2panda/aquadoggo/pull/558)
- Do not panic when blob does not have all pieces yet [#563](https://github.com/p2panda/aquadoggo/pull/563)
- Fix `blob` tasks being triggered too often [#578](https://github.com/p2panda/aquadoggo/pull/578)
- Fix `schema` tasks being triggered too often [#581](https://github.com/p2panda/aquadoggo/pull/581)
- Fix blobs getting corrupted when written to the file system [#587](https://github.com/p2panda/aquadoggo/pull/587)
- Fix pagination bug when only one field is selected and sorted at the same time [#593](https://github.com/p2panda/aquadoggo/pull/593)
- Fix SQLite in-memory database overwrite by giving them each a random name [#595](https://github.com/p2panda/aquadoggo/pull/595)

## [0.5.0]

### Added

- Dial peers discovered via mDNS [#331](https://github.com/p2panda/aquadoggo/pull/331)
- Simplify network CLI options and configuration [#322](https://github.com/p2panda/aquadoggo/pull/322)
- Introduce `autonat` and `relay` network protocols [#314](https://github.com/p2panda/aquadoggo/pull/314)
- Introduce `identify` and `rendezvous` network protocols / behaviours [#304](https://github.com/p2panda/aquadoggo/pull/304)
- Introduce libp2p networking service and configuration [#282](https://github.com/p2panda/aquadoggo/pull/282)
- Create and validate abstract queries [#302](https://github.com/p2panda/aquadoggo/pull/302)
- Support paginated, ordered and filtered collection queries [#308](https://github.com/p2panda/aquadoggo/pull/308)
- SQL query for collections [#311](https://github.com/p2panda/aquadoggo/pull/311)
- Add custom validation to GraphQL scalar types [#318](https://github.com/p2panda/aquadoggo/pull/318)
- Introduce property tests for GraphQL query API with `proptest` [#338](https://github.com/p2panda/aquadoggo/pull/338)
- Introduce initial libp2p network behaviour for replication protocol [#365](https://github.com/p2panda/aquadoggo/pull/365)
- Replication protocol session manager [#363](https://github.com/p2panda/aquadoggo/pull/363)
- Replication message de- / serialization [#375](https://github.com/p2panda/aquadoggo/pull/375)
- Naive protocol replication [#380](https://github.com/p2panda/aquadoggo/pull/380)
- Integrate replication manager with networking stack [#387](https://github.com/p2panda/aquadoggo/pull/387) 🥞
- Reverse lookup for pinned relations in dependency task  [#434](https://github.com/p2panda/aquadoggo/pull/434)
- Persist and maintain index of operation's position in document [#438](https://github.com/p2panda/aquadoggo/pull/438)
- Introduce `dialer` behaviour with retry logic [#444](https://github.com/p2panda/aquadoggo/pull/444)
- Introduce peer sampling to the replication service [#463](https://github.com/p2panda/aquadoggo/pull/463)
- Only replicate and materialize configured "supported schema" [#569](https://github.com/p2panda/aquadoggo/pull/469)
- Parse supported schema ids from `config.toml` [#473](https://github.com/p2panda/aquadoggo/pull/473)
- Fix relayed connections, add DCUtR Holepunching and reduce CLI args [#502](https://github.com/p2panda/aquadoggo/pull/502)
- Announce supported schema ids in network before replication [#515](https://github.com/p2panda/aquadoggo/pull/515)
- Allow & block lists, direct dial known peers, connect to multiple relays [#542](https://github.com/p2panda/aquadoggo/pull/524)

### Changed

- Migrate CLI from `structopt` to `clap` [#289](https://github.com/p2panda/aquadoggo/pull/289)
- Rework test runner and test node population patterns and refactor test_utils [#277](https://github.com/p2panda/aquadoggo/pull/277)
- Implement API changes to p2panda-rs storage traits, new and breaking db migration [#268](https://github.com/p2panda/aquadoggo/pull/268)
- Move all test utils into one module [#275](https://github.com/p2panda/aquadoggo/pull/275)
- Use new version of `async-graphql` for dynamic schema generation [#287](https://github.com/p2panda/aquadoggo/pull/287)
- Restructure `graphql` module [#307](https://github.com/p2panda/aquadoggo/pull/307)
- Removed replication service for now, preparing for new replication protocol [#296](https://github.com/p2panda/aquadoggo/pull/296)
- Bring back e2e GraphQL API tests [#342](https://github.com/p2panda/aquadoggo/pull/342)
- Move GraphQL `types` into separate modules [#343](https://github.com/p2panda/aquadoggo/pull/343)
- Set default order for root queries to document id [#352](https://github.com/p2panda/aquadoggo/pull/352)
- Remove property tests again because of concurrency bug [#347](https://github.com/p2panda/aquadoggo/pull/347)
- Incrementally update documents in materializer [#280](https://github.com/p2panda/aquadoggo/pull/280)
- Decouple p2panda's authentication data types from libp2p's [#408](https://github.com/p2panda/aquadoggo/pull/408)
- Make `TaskInput` an enum and other minor clean ups in materialiser [#429](https://github.com/p2panda/aquadoggo/pull/429)
- Use `libp2p` `0.52.0` [#425](https://github.com/p2panda/aquadoggo/pull/425)
- Check for duplicate entries arriving to `Ingest` before consuming [#439](https://github.com/p2panda/aquadoggo/pull/439)
- Replicate entries in their topologically sorted document order [#442](https://github.com/p2panda/aquadoggo/pull/442)
- Remove "quick commit" from materialization service [#450](https://github.com/p2panda/aquadoggo/pull/450)
- Reduce WARN level logging in network and replication services [#467](https://github.com/p2panda/aquadoggo/pull/467)
- mDNS and AutoNAT disabled by default [#475](https://github.com/p2panda/aquadoggo/pull/475)
- By default, nodes support _any_ schema [#487](https://github.com/p2panda/aquadoggo/pull/487)
- Rework networking service [#502](https://github.com/p2panda/aquadoggo/pull/502)
- Deduplicate peer connections when initiating replication sessions [#525](https://github.com/p2panda/aquadoggo/pull/525)
- Improve consistency and documentation of configuration API [#528](https://github.com/p2panda/aquadoggo/pull/528)
- Improve log level config and user interface [#539](https://github.com/p2panda/aquadoggo/pull/539)

### Fixed

- Correct use of `sqlx` transactions [#285](https://github.com/p2panda/aquadoggo/pull/285)
- Fix race-condition of mutably shared static schema store during testing [#269](https://github.com/p2panda/aquadoggo/pull/269)
- Introduce flag to requeue tasks in worker queue, fixes race-condition in materialization logic [#286](https://github.com/p2panda/aquadoggo/pull/286)
- Update breaking API calls for new `p2panda-rs` 0.7.0 version [#293](https://github.com/p2panda/aquadoggo/pull/293)
- Handle decoding and parsing empty pinned relation lists [#336](https://github.com/p2panda/aquadoggo/pull/336)
- Make `value` on `QueryRow` an option [#339](https://github.com/p2panda/aquadoggo/pull/339)
- Fix race conditions in SQLite database test runner [#345](https://github.com/p2panda/aquadoggo/pull/345)
- Remove duplicate `Cursor` implementation, query code clean up [#346](https://github.com/p2panda/aquadoggo/pull/346)
- Fix SQL ordering to correctly assemble resulting rows to documents [#347](https://github.com/p2panda/aquadoggo/pull/347)
- Fix parsing lists arguments twice in GraphQL [#354](https://github.com/p2panda/aquadoggo/pull/354)
- Sort paginated query field rows by document view id [#354](https://github.com/p2panda/aquadoggo/pull/354)
- Fix missing field when filtering owner [#359](https://github.com/p2panda/aquadoggo/pull/359)
- Bind untrusted user filter arguments in SQL query [#358](https://github.com/p2panda/aquadoggo/pull/358)
- Reintroduce property tests for GraphQL query api [#362](https://github.com/p2panda/aquadoggo/pull/362)
- Fix cursor pagination over ordered queries [#412](https://github.com/p2panda/aquadoggo/pull/412)
- Fix issue where filtered queries fail on updated fields [#409](https://github.com/p2panda/aquadoggo/pull/409)
- Fix insertion of view before document is materialized [#413](https://github.com/p2panda/aquadoggo/pull/413)
- Handle duplicate document view insertions in reduce task [#410](https://github.com/p2panda/aquadoggo/pull/410)
- Fix race condition when check for existing view ids was too early [#420](https://github.com/p2panda/aquadoggo/pull/420)
- Use fork of `asynchronous-codec` to temporarily fix CBOR decoding bug [#440](https://github.com/p2panda/aquadoggo/pull/440)
- Fix composing of circuit relay address [#451](https://github.com/p2panda/aquadoggo/pull/451)
- Correct selection of comparison field during pagination [#471](https://github.com/p2panda/aquadoggo/pull/471)
- Don't check for `affected_rows` on task deletion [#461](https://github.com/p2panda/aquadoggo/pull/461)
- Do not critically fail when view does not exist due to race condition [#460](https://github.com/p2panda/aquadoggo/pull/460)
- Do nothing on log insertion conflict [#468](https://github.com/p2panda/aquadoggo/pull/468)
- Don't update or announce an update in schema provider if a schema with this id exists already [#472](https://github.com/p2panda/aquadoggo/pull/472)
- Do nothing on document_view insertion conflicts [#474](https://github.com/p2panda/aquadoggo/pull/474)
- Only over-write `http_port` when cli arg is passed [#489](https://github.com/p2panda/aquadoggo/pull/489)
- Move deserialization into PeerMessage to distinct variants correctly [#538](https://github.com/p2panda/aquadoggo/pull/538)

### Open Sauce

- CI: Temporary workaround for Rust compiler bug [#417](https://github.com/p2panda/aquadoggo/pull/417)
- CI: Update actions [#455](https://github.com/p2panda/aquadoggo/pull/455)

## [0.4.0]

### Added

- Import `publish` and `next_args` from `p2panda-rs` `api` module [#279](https://github.com/p2panda/aquadoggo/pull/279) `rs`
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

[unreleased]: https://github.com/p2panda/aquadoggo/compare/v0.7.1...HEAD
[0.7.1]: https://github.com/p2panda/aquadoggo/releases/tag/v0.7.1
[0.7.0]: https://github.com/p2panda/aquadoggo/releases/tag/v0.7.0
[0.6.0]: https://github.com/p2panda/aquadoggo/releases/tag/v0.6.0
[0.5.0]: https://github.com/p2panda/aquadoggo/releases/tag/v0.5.0
[0.4.0]: https://github.com/p2panda/aquadoggo/releases/tag/v0.4.0
[0.3.0]: https://github.com/p2panda/aquadoggo/releases/tag/v0.3.0
[0.2.0]: https://github.com/p2panda/aquadoggo/releases/tag/v0.2.0
[0.1.0]: https://github.com/p2panda/aquadoggo/releases/tag/v0.1.0
