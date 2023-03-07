// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use anyhow::Result;
use libp2p::identity::{ed25519, Keypair};
use libp2p::PeerId;
use pem::Pem;

/// Utilities for dealing with a libp2p identity in the form of an ed25519 keypair.
pub trait Identity {
    fn new() -> Self
    where
        Self: Sized;

    fn id(&self) -> PeerId;

    fn keypair(&self) -> Keypair;

    fn encode_pem(&self) -> String;

    fn save(&self, path: &Path) -> Result<()>;

    fn load(path: &Path) -> Result<Self>
    where
        Self: Sized;
}

impl Identity for Keypair {
    /// Generate a random keypair.
    fn new() -> Self {
        Keypair::generate_ed25519()
    }

    /// Return the public key of a keypair.
    fn id(&self) -> PeerId {
        PeerId::from(self.public())
    }

    /// Return the keypair.
    fn keypair(&self) -> Keypair {
        self.clone()
    }

    /// Encode the keypair using the PEM (Privacy-Enhanced Mail) format.
    fn encode_pem(&self) -> String {
        let pem_data = match self {
            Keypair::Ed25519(keypair) => {
                let key = keypair.encode();

                // Write the ASN.1 header id-ed25519 to the buffer
                let mut buf: Vec<u8> = vec![
                    0x30, 0x53, 0x02, 0x01, 0x01, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x04,
                    0x22, 0x04, 0x20,
                ];
                // Extend with secret key
                buf.extend(key[..32].iter());
                // Extend with pubkey separator
                buf.extend([0xA1, 0x23, 0x03, 0x21, 0x00].iter());
                // Extend with public key
                buf.extend(key[32..].iter());

                Pem {
                    tag: "PRIVATE KEY".to_string(),
                    contents: buf,
                }
            }
        };

        pem::encode(&pem_data)
    }

    /// Encode the keypair as PEM and write it to the given path.
    fn save(&self, path: &Path) -> Result<()> {
        let pem = self.encode_pem();

        fs::create_dir_all(path.parent().unwrap())?;
        let mut file = File::create(path)?;
        file.write_all(pem.as_bytes())?;
        file.sync_all()?;

        let mut perms = file.metadata()?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(path, perms)?;

        Ok(())
    }

    /// Load a keypair from file at the given path.
    fn load(path: &Path) -> Result<Self>
    where
        Self: Sized,
    {
        // Read the keypair from file
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let parsed = pem::parse(contents)?;
        let keypair = match parsed.tag.as_str() {
            // PEM encoded ed25519 key
            "PRIVATE KEY" => {
                // Read the private key - offset 16; 32bytes long
                let sk_bytes = parsed.contents[16..48].to_vec();
                let secret = ed25519::SecretKey::from_bytes(sk_bytes)?;
                // Generate a keypair from the secret key
                Keypair::Ed25519(secret.into())
            }
            _ => panic!("Unsupported key type"),
        };

        Ok(keypair)
    }
}
