// SPDX-License-Identifier: AGPL-3.0-or-later

//! String identifiers for graphql types and scalars registered to the root schema as well as
//! fixed query and argument names.

// Type and scalar identifiers.

///  The name of the graphql object representing a documents meta data.
pub const DOCUMENT_META: &str = "DocumentMeta";

/// The name of the graphql object representing next arguments data.
pub const NEXT_ARGS: &str = "NextArguments";

/// The graphql scalar type representing a public key.
pub const PUBLIC_KEY: &str = "PublicKey";

///  The name of the graphql scalar representing a document id.
pub const DOCUMENT_ID: &str = "DocumentId";

///  The name of the graphql scalar representing a document view id.
pub const DOCUMENT_VIEW_ID: &str = "DocumentViewId";

// Query, field and argument names and pre/suf-fixes.

/// The prefix for query name where all documents of a particular schema can be retrieved.
pub const QUERY_ALL_PREFIX: &str = "all_";

/// The name of the next args query.
pub const NEXT_ARGS_QUERY: &str = "nextArgs";

/// The argument string used for passing a document id into a query.
pub const DOCUMENT_ID_ARG: &str = "id";

/// The argument string used for passing a public key into a query.
pub const PUBLIC_KEY_ARG: &str = "publicKey";

/// The argument string used for passing a document view id into a query.
pub const DOCUMENT_VIEW_ID_ARG: &str = "viewId";

/// The name of the field on a document where it's fields can be accessed.
pub const FIELDS_FIELD: &str = "fields";

/// The name of the field on a document where it's meta data can be accessed.
pub const META_FIELD: &str = "meta";
