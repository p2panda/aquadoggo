// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::{anyhow, Result};
use p2panda_rs::schema::SchemaId;
use std::fs::File;
use std::io::Read;
use toml::Table;

pub fn read_schema_ids_from_file(file: &mut File) -> Result<Vec<SchemaId>> {
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    let table = buf.parse::<Table>().unwrap();
    let value = table
        .get("supported_schema_ids")
        .ok_or(anyhow!("No \"supported_schema_ids\" field found config file"))?;
    Ok(value.clone().try_into::<Vec<SchemaId>>()?)
}

#[cfg(test)]
mod tests {
    use std::fs::{create_dir_all, File};
    use std::io::Write;

    use tempfile::TempDir;

    use super::read_schema_ids_from_file;

    #[test]
    fn reads_schema_ids_from_file() {
        let tmp_dir = TempDir::new().unwrap();
        let mut tmp_path = tmp_dir.path().to_owned();
        tmp_path.push("schema.toml");

        create_dir_all(tmp_path.parent().unwrap()).unwrap();
        let mut file = File::create(&tmp_path).unwrap();
        file.write_all("schemas = [\"plant_0020c11ab63099193a8c8516cc00f88bfc8cdd657b94a99fef2fce86aaaede84f87d\"]".as_bytes()).unwrap();

        let result = read_schema_ids_from_file(&mut file);
        println!("{result:?}");
        assert!(result.is_ok());
    }
}
