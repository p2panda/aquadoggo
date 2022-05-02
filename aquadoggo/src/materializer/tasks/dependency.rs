// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::context::Context;
use crate::materializer::worker::TaskResult;
use crate::materializer::Input;

pub async fn dependency_task(context: Context, input: Input) -> TaskResult<Input> {
    Ok(None)
}
