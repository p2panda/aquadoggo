// SPDX-License-Identifier: AGPL-3.0-or-later

//! # aquadoggo
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unstable_features,
    unused_import_braces,
    unused_qualifications
)]

mod config;
mod db;
mod errors;
mod rpc;
mod runtime;
mod task;

#[cfg(test)]
mod test_helpers;

pub use config::Configuration;
pub use runtime::Runtime;

/// Init pretty_env_logger before the test suite runs to handle logging outputs.
///
/// Several of our dependencies (`sqlx`, `p2panda_rs`, `tide`) emmit log messages
/// which we can handle and print using `pretty_env_logger`. Logging ehaviour
/// can be customised at runtime. With eg. `RUST_LOG=p2panda_rs=info cargo t` or
/// `RUST_LOG=openmls=debug cargo t`.
///
/// The `ctor` crate is used to define a global constructor function. This method
/// will be run before any of the test suites.
#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
#[ctor::ctor]
fn init() {
    // If the `RUST_LOG` env var is not set skip initiation as we don't want
    // to see any logs.
    if std::env::var("RUST_LOG").is_ok() {
        pretty_env_logger::init();
    }
}
