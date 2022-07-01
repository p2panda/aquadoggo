// SPDX-License-Identifier: AGPL-3.0-or-later

use std::sync::Mutex;

use once_cell::sync::Lazy;
use p2panda_rs::schema::Schema;

static SCHEMA_PROVIDER: Lazy<Mutex<Vec<Schema>>> = Lazy::new(|| Mutex::new(Vec::new()));

pub struct StaticLeak<T: 'static> {
    inner: &'static T,

    /// Keeps a reference to the pointer. If it gets dropped together with `StaticLeak` the memory
    /// gets freed as well - even though it is 'static!
    _ptr: Box<T>,
}

#[allow(dead_code)]
impl<T> StaticLeak<T> {
    #[allow(unsafe_code)]
    pub fn new(value: T) -> Self {
        let inner = Box::leak(Box::new(value));
        let _ptr = unsafe { Box::from_raw(inner) };
        Self { inner, _ptr }
    }

    pub fn unsafe_value(&self) -> &'static T {
        self.inner
    }
}

pub fn save_static_schemas(data: &[Schema]) {
    let mut schemas = SCHEMA_PROVIDER
        .lock()
        .expect("Could not acquire mutex lock for static schema provider");

    schemas.clear();
    schemas.append(&mut data.to_vec());
}

pub fn load_static_schemas() -> &'static Vec<Schema> {
    let data = SCHEMA_PROVIDER
        .lock()
        .expect("Could not acquire mutex lock for static schema provider")
        .to_vec();

    // @TODO: This creates a memory leak! We could use this `StaticLeak` struct to manage the
    // memory and free it as soon as the schemas is not needed anymore, probably connected to the
    // `GraphQLSchemaManager`?
    Box::leak(Box::new(data))
}

#[cfg(test)]
mod tests {
    use super::StaticLeak;

    #[test]
    fn drops_static_value() {
        let value: String = "Test Value".into();
        let data = StaticLeak::new(value.clone());
        let unsafe_value = data.unsafe_value();
        assert_eq!(unsafe_value, &value);
        drop(data);
        assert_ne!(unsafe_value, &value);
    }
}
