// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::context::Context;
use crate::materializer::worker::TaskResult;
use crate::materializer::TaskInput;

pub async fn schema_task(_context: Context, _input: TaskInput) -> TaskResult<TaskInput> {
    todo!()
}
