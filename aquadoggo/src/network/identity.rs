// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use anyhow::Result;
use libp2p::identity::{ed25519, Keypair};
use libp2p::PeerId;

/// Utilities for dealing with the node identity in the form of an Ed25519 key pair.
pub trait Identity {
    fn new() -> Self
    where
        Self: Sized;

    fn peer_id(&self) -> PeerId;

    fn key_pair(&self) -> Keypair;

    fn save(&self, path: &Path) -> Result<()>;

    fn load(path: &Path) -> Result<Self>
    where
        Self: Sized;
}

// @TODO: This should use our p2panda `KeyPair` type and in general be handled outside the libp2p
// context. Related issue: https://github.com/p2panda/aquadoggo/issues/388
impl Identity for Keypair {
    /// Generate a new Ed25519 key pair.
    fn new() -> Self {
        Keypair::generate_ed25519()
    }

    /// Return the peer ID of a key pair.
    fn peer_id(&self) -> PeerId {
        PeerId::from(self.public())
    }

    /// Return the key pair.
    fn key_pair(&self) -> Keypair {
        self.clone()
    }

    /// Encode the private key as a hex string and save it to the given file path.
    // See: https://github.com/p2panda/aquadoggo/issues/295
    #[allow(deprecated)]
    fn save(&self, path: &Path) -> Result<()> {
        let private_key = match self {
            Keypair::Ed25519(key_pair) => key_pair.secret(),
        };
        let encoded_private_key = hex::encode(private_key);

        fs::create_dir_all(path.parent().unwrap())?;
        let mut file = File::create(path)?;
        file.write_all(encoded_private_key.as_bytes())?;
        file.sync_all()?;

        let mut permissions = file.metadata()?.permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(path, permissions)?;

        Ok(())
    }

    /// Load a key pair from file at the given path.
    // See: https://github.com/p2panda/aquadoggo/issues/295
    #[allow(deprecated)]
    fn load(path: &Path) -> Result<Self>
    where
        Self: Sized,
    {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let private_key_bytes = hex::decode(contents)?;
        let private_key = ed25519::SecretKey::from_bytes(private_key_bytes)?;
        let key_pair = Keypair::Ed25519(private_key.into());

        Ok(key_pair)
    }
}
