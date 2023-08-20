// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fs::File;
use std::io::Read;

use anyhow::{anyhow, Result};
use p2panda_rs::schema::SchemaId;
use toml::Table;

pub fn read_schema_ids_from_file(file: &mut File) -> Result<Vec<SchemaId>> {
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    let table = buf.parse::<Table>().unwrap();
    let value = table.get("supported_schema_ids").ok_or(anyhow!(
        "No \"supported_schema_ids\" field found config file"
    ))?;
    Ok(value.clone().try_into::<Vec<SchemaId>>()?)
}
