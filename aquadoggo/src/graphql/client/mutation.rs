// SPDX-License-Identifier: AGPL-3.0-or-later

//! Mutation root.
use async_graphql::{Context, Object, Result};
use p2panda_rs::entry::decode::decode_entry;
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::operation::{EncodedOperation, Operation, OperationId};
use p2panda_rs::Validate;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::db::provider::SqlStorage;
use crate::domain::publish;
use crate::graphql::client::NextEntryArguments;
use crate::graphql::scalars;

/// GraphQL queries for the Client API.
#[derive(Default, Debug, Copy, Clone)]
pub struct ClientMutationRoot;

#[Object]
impl ClientMutationRoot {
    /// Publish an entry using parameters obtained through `nextEntryArgs` query.
    ///
    /// Returns arguments for publishing the next entry in the same log.
    async fn publish_entry(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "entry", desc = "Signed and encoded entry to publish")]
        entry: scalars::EntrySignedScalar,
        #[graphql(
            name = "operation",
            desc = "p2panda operation representing the entry payload."
        )]
        operation: scalars::EncodedOperationScalar,
    ) -> Result<NextEntryArguments> {
        let store = ctx.data::<SqlStorage>()?;
        let tx = ctx.data::<ServiceSender>()?;

        let entry_signed: EncodedEntry = entry.into();
        let encoded_operation: EncodedOperation = operation.into();

        // @TODO:
        // - decode operation
        // - get the schema
        // - pass all into `publish()`

        /////////////////////////////////////
        // PUBLISH THE ENTRY AND OPERATION //
        /////////////////////////////////////

        // let next_args = publish(store, &entry_signed, &encoded_operation).await?;

        ////////////////////////////////////////
        // SEND THE OPERATION TO MATERIALIZER //
        ////////////////////////////////////////

        // Send new operation on service communication bus, this will arrive eventually at
        // the materializer service

        let operation_id: OperationId = entry_signed.hash().into();

        if tx.send(ServiceMessage::NewOperation(operation_id)).is_err() {
            // Silently fail here as we don't mind if there are no subscribers. We have
            // tests in other places to check if messages arrive.
        }

        todo!()
        // Ok(next_args)
    }
}

// @TODO: bring back _all_ mutation tests
