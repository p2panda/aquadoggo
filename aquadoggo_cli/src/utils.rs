// SPDX-License-Identifier: AGPL-3.0-or-later

use std::env;
use std::path::{Path, PathBuf};

use path_clean::PathClean;

/// Returns the absolute path of a file or directory.
pub fn absolute_path(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();

    if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .expect("Could not determine current directory")
            .join(path)
    }
    .clean()
}
