//! Deterministic key generation: master_seed + counter → privkey → secp256k1 pubkey → keccak256 → ETH address
//!
//! privkey_i = HMAC-SHA256(master_seed, counter_i.to_le_bytes())
//! addr = keccak256(secp256k1_pubkey_uncompressed[1..])[12..32]

use hmac::{Hmac, Mac};
use rayon::prelude::*;
use sha2::Sha256;
use tiny_keccak::{Hasher, Keccak};

type HmacSha256 = Hmac<Sha256>;

pub const ADDR_LEN: usize = 20;
pub type Address = [u8; ADDR_LEN];

/// A (counter, private_key_bytes, address) triple for hit verification.
pub struct KeyPair {
    pub counter: u64,
    pub privkey: [u8; 32],
    pub address: Address,
}

pub struct KeyGen {
    seed: [u8; 32],
    secp: secp256k1::Secp256k1<secp256k1::All>,
}

// secp256k1::Secp256k1 is Send+Sync, safe for rayon.
unsafe impl Send for KeyGen {}
unsafe impl Sync for KeyGen {}

impl KeyGen {
    pub fn new(seed: [u8; 32]) -> Self {
        Self {
            seed,
            secp: secp256k1::Secp256k1::new(),
        }
    }

    pub fn seed(&self) -> &[u8; 32] {
        &self.seed
    }

    #[inline]
    pub fn derive_privkey(&self, counter: u64) -> Option<secp256k1::SecretKey> {
        let mut mac = HmacSha256::new_from_slice(&self.seed).expect("HMAC key");
        mac.update(&counter.to_le_bytes());
        let result = mac.finalize().into_bytes();
        secp256k1::SecretKey::from_slice(&result).ok()
    }

    /// Counter → ETH address (20 bytes). Returns None if privkey invalid (near-zero probability).
    #[inline]
    pub fn derive_address(&self, counter: u64) -> Option<Address> {
        let sk = self.derive_privkey(counter)?;
        Some(pubkey_to_addr(&self.secp, &sk))
    }

    /// Counter → full keypair (privkey bytes + address). For hit verification & recovery.
    #[inline]
    pub fn derive_keypair(&self, counter: u64) -> Option<KeyPair> {
        let sk = self.derive_privkey(counter)?;
        let addr = pubkey_to_addr(&self.secp, &sk);
        Some(KeyPair {
            counter,
            privkey: sk.secret_bytes(),
            address: addr,
        })
    }

    /// Sequential batch: [start, end) → Vec<(counter, address)>.
    pub fn batch_addresses(&self, start: u64, end: u64) -> Vec<(u64, Address)> {
        (start..end)
            .filter_map(|c| self.derive_address(c).map(|a| (c, a)))
            .collect()
    }

    /// Parallel batch via rayon: [start, end) → Vec<(counter, address)>. Thread-safe.
    pub fn par_batch_addresses(&self, start: u64, end: u64) -> Vec<(u64, Address)> {
        (start..end)
            .into_par_iter()
            .filter_map(|c| self.derive_address(c).map(|a| (c, a)))
            .collect()
    }

    /// Parallel batch returning only address fingerprints (u64) for filter insertion.
    pub fn par_batch_fingerprints(&self, start: u64, end: u64) -> Vec<u64> {
        (start..end)
            .into_par_iter()
            .filter_map(|c| {
                self.derive_address(c)
                    .map(|a| crate::filter::addr_to_u64(&a))
            })
            .collect()
    }
}

/// secp256k1 secret key → uncompressed pubkey → keccak256 → last 20 bytes
#[inline]
fn pubkey_to_addr(
    secp: &secp256k1::Secp256k1<secp256k1::All>,
    sk: &secp256k1::SecretKey,
) -> Address {
    let pk = secp256k1::PublicKey::from_secret_key(secp, sk);
    let pk_bytes = pk.serialize_uncompressed();
    let mut hash = [0u8; 32];
    let mut keccak = Keccak::v256();
    keccak.update(&pk_bytes[1..]);
    keccak.finalize(&mut hash);
    let mut addr = [0u8; ADDR_LEN];
    addr.copy_from_slice(&hash[12..]);
    addr
}

/// Standalone: known privkey hex → ETH address hex. For test vector validation.
pub fn privkey_hex_to_addr_hex(privkey_hex: &str) -> anyhow::Result<String> {
    let bytes = hex::decode(privkey_hex)?;
    anyhow::ensure!(bytes.len() == 32, "privkey must be 32 bytes");
    let secp = secp256k1::Secp256k1::new();
    let sk = secp256k1::SecretKey::from_slice(&bytes)?;
    let addr = pubkey_to_addr(&secp, &sk);
    Ok(hex::encode(addr))
}

/// Generate or load master seed from file.
pub fn load_or_create_seed(path: &std::path::Path) -> anyhow::Result<[u8; 32]> {
    if path.exists() {
        let bytes = std::fs::read(path)?;
        anyhow::ensure!(
            bytes.len() == 32,
            "master_seed.key must be exactly 32 bytes, got {}",
            bytes.len()
        );
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&bytes);
        log::info!(
            "loaded master seed from {} (hash: {})",
            path.display(),
            seed_hash_id(&seed)
        );
        Ok(seed)
    } else {
        use rand::RngCore;
        let mut seed = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut seed);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, seed)?;
        log::info!(
            "created new master seed at {} (hash: {})",
            path.display(),
            seed_hash_id(&seed)
        );
        Ok(seed)
    }
}

/// SHA-256(seed) → first 16 hex chars for cursor identification.
pub fn seed_hash_id(seed: &[u8; 32]) -> String {
    use sha2::Digest;
    hex::encode(&Sha256::digest(seed)[..8])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_same_seed_same_counter() {
        let kg = KeyGen::new([0xABu8; 32]);
        assert_eq!(
            kg.derive_address(42).unwrap(),
            kg.derive_address(42).unwrap()
        );
    }

    #[test]
    fn different_counters_different_addresses() {
        let kg = KeyGen::new([0xCDu8; 32]);
        assert_ne!(kg.derive_address(0).unwrap(), kg.derive_address(1).unwrap());
    }

    #[test]
    fn batch_generation() {
        let kg = KeyGen::new([0x11u8; 32]);
        assert_eq!(kg.batch_addresses(0, 100).len(), 100);
    }

    #[test]
    fn par_batch_matches_sequential() {
        let kg = KeyGen::new([0x22u8; 32]);
        let seq = kg.batch_addresses(0, 500);
        let par = kg.par_batch_addresses(0, 500);
        assert_eq!(seq.len(), par.len());
        let mut par_sorted = par.clone();
        par_sorted.sort_by_key(|&(c, _)| c);
        assert_eq!(seq, par_sorted);
    }

    #[test]
    fn derive_keypair_consistency() {
        let kg = KeyGen::new([0x33u8; 32]);
        let kp = kg.derive_keypair(99).unwrap();
        let addr = kg.derive_address(99).unwrap();
        assert_eq!(kp.address, addr);
        assert_eq!(kp.counter, 99);
        assert_eq!(kp.privkey.len(), 32);
    }

    /// Known Ethereum test vector: privkey=1 → well-documented address.
    /// See: https://ethereum.stackexchange.com/questions/3542/how-are-ethereum-addresses-generated
    #[test]
    fn known_eth_test_vector_privkey_1() {
        let addr = privkey_hex_to_addr_hex(
            "0000000000000000000000000000000000000000000000000000000000000001",
        )
        .unwrap();
        assert_eq!(addr, "7e5f4552091a69125d5dfcb7b8c2659029395bdf");
    }

    /// Manual step-by-step verification: our derivation chain matches standard ETH flow.
    #[test]
    fn derivation_chain_consistency() {
        let privkey_hex = "4c0883a69102937d6231471b5dbb6204fe512961708279f1f3b3f3c48e4b5c57";
        let bytes = hex::decode(privkey_hex).unwrap();
        let secp = secp256k1::Secp256k1::new();
        let sk = secp256k1::SecretKey::from_slice(&bytes).unwrap();
        let pk = secp256k1::PublicKey::from_secret_key(&secp, &sk);
        let pk_bytes = pk.serialize_uncompressed();
        assert_eq!(pk_bytes[0], 0x04); // uncompressed prefix
        assert_eq!(pk_bytes.len(), 65);
        let mut hash = [0u8; 32];
        let mut keccak = Keccak::v256();
        keccak.update(&pk_bytes[1..]);
        keccak.finalize(&mut hash);
        let manual_addr = hex::encode(&hash[12..]);
        let fn_addr = privkey_hex_to_addr_hex(privkey_hex).unwrap();
        assert_eq!(
            manual_addr, fn_addr,
            "manual vs function derivation mismatch"
        );
    }

    #[test]
    fn seed_hash_deterministic() {
        let s = [0xFFu8; 32];
        assert_eq!(seed_hash_id(&s), seed_hash_id(&s));
        assert_eq!(seed_hash_id(&s).len(), 16);
    }

    #[test]
    fn par_fingerprints_count() {
        let kg = KeyGen::new([0x44u8; 32]);
        let fps = kg.par_batch_fingerprints(0, 200);
        assert_eq!(fps.len(), 200);
    }
}
