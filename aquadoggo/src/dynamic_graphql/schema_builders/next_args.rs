// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, InputValue, Object, TypeRef};
use dynamic_graphql::{FieldValue, ScalarValue};
use p2panda_rs::api;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::identity::PublicKey;

use crate::db::SqlStore;
use crate::dynamic_graphql::scalars::{DocumentViewIdScalar, PublicKeyScalar};
use crate::dynamic_graphql::types::NextArguments;

// Add next args to the query object.
pub fn build_next_args_query(query: Object) -> Object {
    query.field(
        Field::new("nextArgs", TypeRef::named("NextArguments"), |ctx| {
            FieldFuture::new(async move {
                let mut args = ctx.field().arguments()?.into_iter().map(|(_, value)| value);
                let store = ctx.data::<SqlStore>()?;

                // Convert and validate passed parameters.
                let public_key: PublicKey = PublicKeyScalar::from_value(args.next().unwrap())?.into();
                let document_view_id: Option<DocumentViewId> = match args.next() {
                    Some(value) => Some(DocumentViewIdScalar::from_value(value)?.into()),
                    None => None,
                };

                // Calculate next entry's arguments.
                let (backlink, skiplink, seq_num, log_id) =
                    api::next_args(store, &public_key, document_view_id.as_ref()).await?;

                let next_args = NextArguments {
                    log_id: log_id.into(),
                    seq_num: seq_num.into(),
                    backlink: backlink.map(|hash| hash.into()),
                    skiplink: skiplink.map(|hash| hash.into()),
                };

                Ok(Some(FieldValue::owned_any(next_args)))
            })
        })
        .argument(InputValue::new("publicKey", TypeRef::named_nn("PublicKey")))
        .argument(InputValue::new(
            "documentViewId",
            TypeRef::named("DocumentViewId"),
        ))
        .description("Gimme some sweet sweet next args!"),
    )
}
