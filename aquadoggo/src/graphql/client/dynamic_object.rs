// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::registry::{MetaField, MetaType};
use p2panda_rs::schema::Schema;

pub trait DynamicObject {
    fn new(schema: &'static Schema) -> Self;

    fn name(&self) -> String;

    fn schema(&self) -> &'static Schema;

    fn metafield(&self) -> MetaField;

    fn metatype(&self) -> MetaType;
}
