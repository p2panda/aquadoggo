// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fs::File;
use std::io::BufReader;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::Serialize;

/// This helper allows circumventing visibility limitations by storing/loading data in a temp file.
pub(crate) struct TempFile<T: Serialize + DeserializeOwned> {
    /// Path of the temporary file.
    path: String,

    // Marker for generic type of stored data.
    data_type: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> TempFile<T> {
    /// Restore data given the temporary files path.
    pub fn load(path: &str) -> T {
        let file = File::open(path).unwrap();
        let reader = BufReader::new(file);
        serde_json::from_reader(reader).unwrap()
    }

    /// Restore data and pin it to memory for the rest of the program's runtime.
    ///
    /// **Attention**: dropping the returned reference will leak memory.
    pub fn load_static(path: &str) -> &'static T {
        Box::leak(Box::new(Self::load(path)))
    }

    /// Serialise data to a given path.
    ///
    /// Call `unlink()` on the returned instance to remove the temporary file.
    pub fn save(data: &T, path: &str) -> Self {
        let file = File::create(path).unwrap();
        serde_json::to_writer(file, &data).unwrap();
        Self {
            path: path.to_owned(),
            data_type: PhantomData,
        }
    }

    /// Unlink the temporary file from the file system.
    pub fn unlink(&self) {
        std::fs::remove_file(&self.path).unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_temp_file_helper() {
        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct MyData(String);
        let original = MyData("test".to_owned());

        let temp_file = TempFile::save(&original, "./data.temp");
        let restored = TempFile::load("./data.temp");
        let _restored_static: &'static MyData = TempFile::load_static("./data.temp");
        assert_eq!(original, restored);

        temp_file.unlink();
        assert!(!Path::new("./data.temp").exists())
    }
}
