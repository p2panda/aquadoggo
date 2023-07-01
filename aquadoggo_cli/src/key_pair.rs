// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use anyhow::Result;
use p2panda_rs::identity::KeyPair;

/// File of the name where the private key will be stored inside.
const KEY_PAIR_FILE_NAME: &str = "private-key";

/// Returns a new instance of `KeyPair` by either loading the private key from a path or generating
/// a new one and saving it in the file system.
pub fn generate_or_load_key_pair(base_path: PathBuf) -> Result<KeyPair> {
    let mut key_pair_path = base_path;
    key_pair_path.push(KEY_PAIR_FILE_NAME);

    let key_pair = if key_pair_path.is_file() {
        load_key_pair_from_file(key_pair_path)?
    } else {
        let key_pair = KeyPair::new();
        save_key_pair_to_file(&key_pair, key_pair_path)?;
        key_pair
    };

    Ok(key_pair)
}

/// Returns a new instance of `KeyPair` by generating a new key pair which is not persisted on the
/// file system.
///
/// This method is useful to run nodes for testing purposes.
#[allow(dead_code)]
pub fn generate_ephemeral_key_pair() -> KeyPair {
    KeyPair::new()
}

/// Saves human-readable (hex-encoded) private key string (ed25519) into a file at the given path.
///
/// This method automatically creates the required directories on that path and fixes the
/// permissions of the file (0600, read and write permissions only for the owner).
fn save_key_pair_to_file(key_pair: &KeyPair, path: PathBuf) -> Result<()> {
    let private_key_hex = hex::encode(key_pair.private_key().as_bytes());

    // Make sure that directories exist and write file into it
    fs::create_dir_all(path.parent().unwrap())?;
    let mut file = File::create(&path)?;
    file.write_all(private_key_hex.as_bytes())?;
    file.sync_all()?;

    // Set permission for sensitive information
    let mut permissions = file.metadata()?.permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(path, permissions)?;

    Ok(())
}

/// Loads a private key from a file at the given path and derives ed25519 key pair from it.
///
/// The private key in the file needs to be represented as a hex-encoded string.
fn load_key_pair_from_file(path: PathBuf) -> Result<KeyPair> {
    let mut file = File::open(path)?;
    let mut private_key_hex = String::new();
    file.read_to_string(&mut private_key_hex)?;
    let key_pair = KeyPair::from_private_key_str(&private_key_hex)?;
    Ok(key_pair)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::generate_or_load_key_pair;

    #[test]
    fn saves_and_loads_key_pair() {
        let tmp_dir = TempDir::new().unwrap();
        let mut tmp_path = tmp_dir.path().to_owned();
        tmp_path.push("private-key.txt");

        // Attempt to load the key pair from the temporary path
        // This should result in a new key pair being generated and written to file
        let key_pair_1 = generate_or_load_key_pair(tmp_path.clone());
        assert!(key_pair_1.is_ok(), "{:?}", key_pair_1.err());

        // Attempt to load the key pair from the same temporary path
        // This should result in the previously-generated key pair being loaded from file
        let key_pair_2 = generate_or_load_key_pair(tmp_path);
        assert!(key_pair_2.is_ok());

        // Ensure that both key pairs have the same public key
        assert_eq!(
            key_pair_1.unwrap().public_key(),
            key_pair_2.unwrap().public_key()
        );
    }
}
