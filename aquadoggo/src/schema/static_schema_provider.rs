// SPDX-License-Identifier: AGPL-3.0-or-later

//! Global provider for schemas which serves as a workaround to get "dynamic" schemas into the
//! `async-graphql` crate.
//!
//! The methods and structs provided here help us to fullfil the API of `async-graphql` which gives
//! us somewhat a path to introduce new GraphQL queries during runtime but only with some
//! limitations we need to work around with.
//!
//! Note that the `async-graphql` crate does officially not have support for "dynamic schemas",
//! meaning it is designed to build one static GraphQL schema once in the beginning and then keep
//! it until the end of the program. See: https://github.com/async-graphql/async-graphql/issues/495
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

/// Leaks an inner value T so we can get a 'static lifetime of it while also making sure the
/// allocated memory gets freed as soon as the struct gets dropped
///
/// Warning: This is a tool to allow developers to get back the control over unmanaged
/// memory. This hopefully helps you to prevent memory leaks, but it doesn't automatically
/// warn you from making mistakes! You can still try to access the borrowed value from
/// `unsafe_value` after dropping the struct and you will find only random garbage inside.
///
/// ## Example of what can go wrong
///
/// ```
/// // Allocate the value `100` on the heap and get a pointer to it without consuming the box yet
/// let owned = Box::new(100);
/// let ptr_owned: *const i32 = &*owned;
///
/// // Consume the box but keep the value in memory
/// let leaked = Box::leak(owned);
///
/// // The pointer is still pointing at the same address in memory
/// let ptr_leaked = leaked as *const i32;
/// assert_eq!(ptr_owned, ptr_leaked);
///
/// // Drop the reference to that value. Since we consumed the box already Rust doesn't manage the
/// // memory anymore for us, it is still there! We have a memory leak.
/// drop(leaked);
///
/// // Let's look into whats at that memory location, and indeed, we can still read the value even
/// // though all references are gone .. :-(
/// let points_at = unsafe { *ptr_leaked };
/// assert_eq!(points_at, 100);
/// ```
///
/// See `StaticLeak` tests for more examples!
pub struct StaticLeak<T: 'static> {
    inner: &'static T,
    _ptr: Box<T>,
}

#[allow(dead_code)]
impl<T> StaticLeak<T> {
    /// Leaks a value T while keeping the ownership intact to make sure all memory gets freed as
    /// soon as this gets dropped.
    ///
    /// With the help of unsafe code this circumvents borrow-checking as we "own an unowned value".
    #[allow(unsafe_code)]
    pub fn new(value: T) -> Self {
        // Leak the inner value, from now on the value is not owned anymore. If we loose the
        // reference, the value will still exist in memory, this could potentially cause a memory
        // leak!
        let inner = Box::leak(Box::new(value));

        // Without the knowledge of the borrow-checker we "reclaim" the ownership and keep it
        // inside the struct. Whenever the struct gets dropped, we can safely drop the Box and also
        // free the used memory.
        let _ptr = unsafe { Box::from_raw(inner) };

        Self { inner, _ptr }
    }

    /// Returns a reference to the value T with 'static lifetime.
    ///
    /// Warning: The borrow-checker "assumes" that this value will exist until the end of the
    /// program, but that might not be the case (see examples above)! Do only use it if you either:
    ///
    /// 1. Make sure this value will actually be used until the end of the program (it is truly
    ///    'static).
    ///
    /// 2. Make sure you do not refer to it anymore _after_ you've dropped the `StaticLeak`
    ///    instance.
    #[allow(unsafe_code)]
    pub unsafe fn unsafe_value(&self) -> &'static T {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use p2panda_rs::schema::{FieldType, Schema, SchemaId};
    use p2panda_rs::test_utils::fixtures::schema;

    use super::{load_static_schemas, save_static_schemas};

    #[rstest]
    fn save_and_load_schemas(#[from(schema)] schema_id: SchemaId) {
        // Create and store schemas in global storage
        let schema =
            Schema::new(&schema_id, "Test schema", vec![("name", FieldType::String)]).unwrap();
        let schemas = vec![schema];
        save_static_schemas(&schemas);

        // Load from global storage
        let from_store = load_static_schemas();
        assert_eq!(&schemas, from_store);
    }

    // #[test]
    // fn drops_static_value() {
    //     let managed_leak = StaticLeak::new("Hello".to_string());

    //     // So far, everything is fine. We have a value with a static lifetime, which is exactly
    //     // what we wanted to have.
    //     let value: &'static String = managed_leak.unsafe_value();
    //     assert_eq!(*value, "Hello".to_string());

    //     // We drop the struct before the program ended, this will free the used memory for the
    //     // struct and the inner value! We do not have a memory leak!
    //     drop(managed_leak);

    //     // Now it gets bad .. `value` still exists in scope and the borrow checker doesn't warn us
    //     // that it got dropped because it still assumes it has 'static lifetime. But actually it is
    //     // gone and you will probably not find the value `100` here anymore ..
    //     assert_ne!(*value, "Hello".to_string());
    // }
}
