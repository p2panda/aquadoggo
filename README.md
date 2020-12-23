# p2:panda_face: node

Configurable node server implementation for the p2panda network running as a [standalone process](/node_cli) or embedded as a [library](/node).

## Features

* Awaits signed messages from clients via a JSON RPC API
* Verifies the consistency, format and signature of messages and rejects invalid ones
* Stores messages of the network in a SQL database of your choice (SQLite, PostgreSQL or MySQL)
* Materializes views on top of the known data
* Answers filterable and paginated data queries
* Discovers other nodes in local network and internet
* Replicates data with other nodes

## License

GNU Affero General Public License v3.0 `AGPL-3.0`
