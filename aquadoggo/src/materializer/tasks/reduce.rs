// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentBuilder;

use crate::context::Context;
use crate::materializer::worker::{TaskError, TaskResult};
use crate::materializer::TaskInput;

pub async fn reduce_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    // @TODO: Load operations from database
    let _document = DocumentBuilder::new(vec![])
        .build()
        .map_err(|_| TaskError::Failure)?;

    // @TODO: Store document view in database
    Ok(None)
}
