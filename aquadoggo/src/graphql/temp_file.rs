// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fs::File;
use std::io::BufReader;
use std::marker::PhantomData;
use std::path::PathBuf;

use log::{debug, info};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::graphql::error::TempFileError;
use crate::Configuration;

/// Returns a path for the given filename in an OS-specific default data directory
fn get_application_data_dir(fname: &str) -> PathBuf {
    let temp_config = Configuration::new(None).unwrap();
    let mut path = temp_config.base_path.unwrap();
    path.push(fname);
    path
}

/// This helper allows circumventing visibility limitations by storing/loading data in a temp file.
pub(crate) struct TempFile<Data>
where
    Data: Serialize + DeserializeOwned,
{
    /// Path of the temporary file.
    path: PathBuf,

    // Marker for generic type of stored data.
    data_type: PhantomData<Data>,
}

impl<Data: Serialize + DeserializeOwned> TempFile<Data> {
    /// Restore data given the temporary files path.
    pub fn load(fname: &str) -> Data {
        let path = get_application_data_dir(fname);
        debug!("Loading temp file from {:?}", path);
        let file = File::open(path).unwrap();
        let reader = BufReader::new(file);
        serde_json::from_reader(reader).unwrap()
    }

    /// Restore data and pin it to memory for the rest of the program's runtime.
    ///
    /// **Attention**: dropping the returned reference will leak memory.
    pub fn load_static(fname: &str) -> &'static Data {
        Box::leak(Box::new(Self::load(fname)))
    }

    /// Serialise data to a given path.
    ///
    /// Call `unlink()` on the returned instance to remove the temporary file.
    pub fn save(data: &Data, fname: &str) -> Result<Self, TempFileError> {
        let path = get_application_data_dir(fname);
        if path.exists() {
            return Err(TempFileError::ExistingTempFile);
        }

        debug!("Writing temp file to {:?}", path);
        let file = File::create(path.clone())?;
        serde_json::to_writer(file, &data)
            .map_err(|err| TempFileError::Serialisation(err.to_string()))?;
        Ok(Self {
            path,
            data_type: PhantomData,
        })
    }

    /// Unlink the temporary file from the file system.
    pub fn unlink(&self) {
        if self.path.exists() {
            info!("Unlinking temp file at {:?}", self.path);
            std::fs::remove_file(self.path.clone()).unwrap()
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use super::*;

    #[test]
    fn test_temp_file_helper() {
        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct MyData(String);

        let original = MyData("test".to_owned());

        let temp_file = TempFile::save(&original, "test.tmp.json");

        let restored = TempFile::load("test.tmp.json");
        let _restored_static: &'static MyData = TempFile::load_static("test.tmp.json");
        assert_eq!(original, restored);

        temp_file.unwrap().unlink();
        assert!(!Path::new("test.tmp.json").exists())
    }
}
