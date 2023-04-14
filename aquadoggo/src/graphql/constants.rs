// SPDX-License-Identifier: AGPL-3.0-or-later

//! String identifiers for graphql types and scalars registered to the root schema as well as fixed
//! query and argument names.

// Type and scalar identifiers.

/// GraphQL object representing a documents meta data.
pub const DOCUMENT_META: &str = "DocumentMeta";

/// GraphQL object representing next arguments data.
pub const NEXT_ARGS: &str = "NextArguments";

/// GraphQL scalar type representing a public key.
pub const PUBLIC_KEY: &str = "PublicKey";

/// GraphQL scalar representing a document id.
pub const DOCUMENT_ID: &str = "DocumentId";

/// GraphQL scalar representing a document view id.
pub const DOCUMENT_VIEW_ID: &str = "DocumentViewId";

// Query, field and argument names and pre-/suffixes.

/// Prefix for query name where all documents of a particular schema can be retrieved.
pub const QUERY_ALL_PREFIX: &str = "all_";

/// Name of query to fetch next entry arguments.
pub const NEXT_ARGS_QUERY: &str = "nextArgs";

/// Argument string used for passing a document id into a query.
pub const DOCUMENT_ID_ARG: &str = "id";

/// Argument string used for passing a public key into a query.
pub const PUBLIC_KEY_ARG: &str = "publicKey";

/// Argument string used for passing a document view id into a query.
pub const DOCUMENT_VIEW_ID_ARG: &str = "viewId";

/// Argument string used for passing a filter into a query.
pub const FILTER_ARG: &str = "filter";

/// Argument string used for passing a filter into a query.
pub const META_FILTER_ARG: &str = "meta";

/// Argument string used for passing a pagination cursor into a query.
pub const PAGINATION_AFTER_ARG: &str = "after";

/// Argument string used for passing number of paginated items requested to query.
pub const PAGINATION_FIRST_ARG: &str = "first";

/// Argument string used for passing field to order by to query.
pub const ORDER_BY_ARG: &str = "orderBy";

/// Argument string used for passing ordering direction to query.
pub const ORDER_DIRECTION_ARG: &str = "orderDirection";

/// Name of field where a collection of documents can be accessed.
pub const DOCUMENTS_FIELD: &str = "documents";

/// Name of field on a document where it's fields can be accessed.
pub const FIELDS_FIELD: &str = "fields";

/// Name of field on a document where it's meta data can be accessed.
pub const META_FIELD: &str = "meta";

/// Name of field on a document where pagination cursor can be accessed.
pub const CURSOR_FIELD: &str = "cursor";

/// Name of field on a paginated response which contains the total count.
pub const TOTAL_COUNT_FIELD: &str = "totalCount";

/// Name of field on a paginated response which shows if a next page exists.
pub const HAS_NEXT_PAGE_FIELD: &str = "hasNextPage";
