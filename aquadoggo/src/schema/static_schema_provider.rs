// SPDX-License-Identifier: AGPL-3.0-or-later

//! Global provider for schemas which serves as a workaround to get dynamic schemas into the
//! `async-graphql` crate. Dynamic meaning that the GraphQL schema changes during runtime.
//!
//! The methods and structs provided here help us to fullfil the API of `async-graphql` which gives
//! us somewhat a path to introduce new GraphQL queries during runtime but only with some
//! limitations we need to work around with.
//!
//! Note that the `async-graphql` crate does officially not have support for dynamic schemas,
//! meaning it is designed to build one static GraphQL schema once in the beginning and then keep
//! it until the end of the program. See: <https://github.com/async-graphql/async-graphql/issues/495>
//!
//! Together with the `GraphQLSchemaManager` and these methods here we can still introduce new
//! GraphQL schemas during the programs runtime, this is why we call them "dynamic schemas".
//!
//! The limitations of `async-graphql` we need to get around are:
//!
//! 1. `OutputType::create_type_info` is an associated function which does not provide any access
//!    to `self`, so we are limited in what data we can provide from the "outside" (like p2panda
//!    schemas) to build new GraphQL types.
//!
//!    The solution here is to introduce a global storage we can both access from inside the
//!    `create_type_info` function to read from it and other places to write to it.
//!
//! 2. Fields in `MetaType::Object` like `description` require a 'static lifetime.
//!
//!    The solution here is to make use of `Box::leak` which removes the ownership over the inner
//!    value, making it a value with 'static lifetime. Together with the global storage we can
//!    bring data into these `async-graphql` functions AND give them a 'static lifetime.
//!
//!    This is a fairly hacky workaround and can cause memory leaks when not handled properly. This
//!    is why this module provides a `StaticLeak` struct which should help us to clean up after
//!    ourselves.
use std::sync::Mutex;

use once_cell::sync::Lazy;
use p2panda_rs::schema::Schema;

/// Global schema provider containing all application and system schemas which will be used to
/// build the next GraphQL schema.
///
/// This is similar to `SchemaProvider` though serving a slightly different purpose, as it is a
/// workaround for `async-graphql` (see module-level description for details), the contained data
/// should be identical though.
static SCHEMA_PROVIDER: Lazy<Mutex<Vec<Schema>>> = Lazy::new(|| Mutex::new(Vec::new()));

/// Replaces the current content of the global schema provider with new data.
pub fn save_static_schemas(data: &[Schema]) {
    let mut schemas = SCHEMA_PROVIDER
        .lock()
        .expect("Could not acquire mutex lock for static schema provider");

    schemas.clear();
    schemas.append(&mut data.to_vec());
}

/// Reads the current schemas of the global schema provider and returns the result in a 'static
/// lifetime.
///
/// Warning: The returned data needs to be manually freed when not being used anymore, otherwise it
/// will cause a memory leak.
///
/// @TODO: For now we're fine as we do not clean up any dynamically generated schemas (see
/// `GraphQLSchemaManager`). This means that the returned schema arrays here will anyhow life as
/// long as the whole program. As soon as we start removing "old" dynamically generated schemas
/// from the `GraphQLSchemaManager` we will have to also manually deallocate that used memory from
/// here. See `StaticLeak` struct below which might help us with this.
pub fn load_static_schemas() -> &'static Vec<Schema> {
    let data = SCHEMA_PROVIDER
        .lock()
        .expect("Could not acquire mutex lock for static schema provider")
        .to_vec();

    Box::leak(Box::new(data))
}
