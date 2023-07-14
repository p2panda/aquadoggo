// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::identity::{KeyPair, PublicKey};

/// Helper method to convert p2panda `PublicKey` to libp2p `PeerId`.
///
/// Our specification and APIs use ed25519 public keys as client and node identifiers. Internally
/// libp2p handles that via "peer ids" (multihashes, base58-encoded).
pub fn to_libp2p_peer_id(public_key: &PublicKey) -> libp2p::PeerId {
    let bytes = public_key.to_bytes();
    // Unwrap here because we already checked the validity of this key
    let ed25519_public = libp2p::identity::ed25519::PublicKey::try_from_bytes(&bytes).unwrap();
    let libp2p_public = libp2p::identity::PublicKey::from(ed25519_public);
    libp2p_public.to_peer_id()
}

/// Helper method to convert p2panda `KeyPair` to the libp2p equivalent.
pub fn to_libp2p_key_pair(key_pair: &KeyPair) -> libp2p::identity::Keypair {
    let bytes = key_pair.private_key().as_bytes();
    // Unwrap here because we already validated this private key
    libp2p::identity::Keypair::ed25519_from_bytes(bytes.to_owned()).unwrap()
}

#[cfg(test)]
mod tests {
    use p2panda_rs::identity::KeyPair;

    use super::{to_libp2p_key_pair, to_libp2p_peer_id};

    #[test]
    fn peer_id_public_key_conversion() {
        let key_pair = KeyPair::from_private_key_str(
            "799faf7a1fa47c54fac705f30d3c4d80c40efb562da19315fabefcc995a228a1",
        )
        .unwrap();
        let public_key = key_pair.public_key();
        let peer_id = to_libp2p_peer_id(&public_key);
        assert_eq!(
            peer_id.to_string(),
            "12D3KooWCw68m5CRcV8vD9iuR325oKwJHLYqTYH5mYwD6k2QV4nm".to_string(),
        )
    }

    #[test]
    fn key_pair_conversion() {
        let key_pair = KeyPair::new();
        let key_pair_converted = to_libp2p_key_pair(&key_pair);
        assert_eq!(
            key_pair.public_key().to_bytes(),
            key_pair_converted
                .public()
                .try_into_ed25519()
                .unwrap()
                .to_bytes(),
        );
    }
}
