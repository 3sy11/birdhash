//! Filter abstraction over BinaryFuse8 (L1), BinaryFuse16 (fetch), and Bloom 10% (L2 archive).
//! Serialization uses bincode for compact binary format (critical: serde_json would 4x inflate).

use anyhow::Result;
use std::path::Path;

pub use xorf::{BinaryFuse16, BinaryFuse8};

/// Build a BinaryFuse8 filter from u64 fingerprints (L1 gen filter).
pub fn build_fuse8(keys: &[u64]) -> Result<BinaryFuse8> {
    BinaryFuse8::try_from(keys).map_err(|e| anyhow::anyhow!("BinaryFuse8 build failed: {:?}", e))
}

/// Build a BinaryFuse16 filter from u64 fingerprints (fetch filter).
pub fn build_fuse16(keys: &[u64]) -> Result<BinaryFuse16> {
    BinaryFuse16::try_from(keys).map_err(|e| anyhow::anyhow!("BinaryFuse16 build failed: {:?}", e))
}

/// Convert 20-byte ETH address to u64 fingerprint for filter insertion/query.
/// Uses SipHash-style mixing of all 20 bytes for better distribution.
#[inline]
pub fn addr_to_u64(addr: &[u8; 20]) -> u64 {
    let a = u64::from_le_bytes([
        addr[0], addr[1], addr[2], addr[3], addr[4], addr[5], addr[6], addr[7],
    ]);
    let b = u64::from_le_bytes([
        addr[8], addr[9], addr[10], addr[11], addr[12], addr[13], addr[14], addr[15],
    ]);
    let c = u32::from_le_bytes([addr[16], addr[17], addr[18], addr[19]]) as u64;
    // Finalizer mix: combine all bytes to avoid discarding entropy
    let mut h = a;
    h ^= b.wrapping_mul(0x517cc1b727220a95);
    h ^= c.wrapping_mul(0x6c62272e07bb0142);
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h
}

// ── Binary serialization (bincode) ──
// serde_json is 4x larger for Vec<u8> payloads; bincode writes raw bytes.

pub fn save_fuse8(filter: &BinaryFuse8, path: &Path) -> Result<()> {
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    }
    let data = bincode::serialize(filter)?;
    atomic_write(path, &data)
}

pub fn load_fuse8(path: &Path) -> Result<BinaryFuse8> {
    let data = std::fs::read(path)?;
    Ok(bincode::deserialize(&data)?)
}

pub fn save_fuse16(filter: &BinaryFuse16, path: &Path) -> Result<()> {
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    }
    let data = bincode::serialize(filter)?;
    atomic_write(path, &data)
}

pub fn load_fuse16(path: &Path) -> Result<BinaryFuse16> {
    let data = std::fs::read(path)?;
    Ok(bincode::deserialize(&data)?)
}

/// Write data to a temp file then rename for crash-safe atomicity.
fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

// ── L2 Bloom filter (10% FP) using bitvec ──

pub struct BloomFilter {
    bits: bitvec::vec::BitVec,
    num_hashes: u32,
    num_bits: u64,
}

impl BloomFilter {
    /// Create bloom for `n` elements at ~10% FP. m = -n·ln(p)/(ln2)², k = (m/n)·ln2
    pub fn new(n: usize) -> Self {
        let p: f64 = 0.10;
        let m = (-(n as f64) * p.ln() / (2.0_f64.ln().powi(2))).ceil() as u64;
        let k = ((m as f64 / n as f64) * 2.0_f64.ln()).round() as u32;
        Self {
            bits: bitvec::vec::BitVec::repeat(false, m as usize),
            num_hashes: k.max(1),
            num_bits: m,
        }
    }

    #[inline]
    pub fn insert(&mut self, key: u64) {
        for i in 0..self.num_hashes {
            let idx = self.hash_idx(key, i);
            self.bits.set(idx, true);
        }
    }

    #[inline]
    pub fn contains(&self, key: u64) -> bool {
        (0..self.num_hashes).all(|i| self.bits[self.hash_idx(key, i)])
    }

    #[inline]
    fn hash_idx(&self, key: u64, i: u32) -> usize {
        let mut h = key.wrapping_mul(0x517cc1b727220a95).wrapping_add(i as u64);
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h ^= h >> 33;
        (h % self.num_bits) as usize
    }

    #[allow(dead_code)]
    pub fn size_bytes(&self) -> usize {
        self.bits.len() / 8
    }
    #[allow(dead_code)]
    pub fn num_entries_capacity(&self) -> u64 {
        (self.num_bits as f64 / 4.79).round() as u64
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        }
        let raw = self.bits.as_raw_slice();
        let mut buf = Vec::with_capacity(12 + std::mem::size_of_val(raw));
        buf.extend_from_slice(&self.num_hashes.to_le_bytes());
        buf.extend_from_slice(&self.num_bits.to_le_bytes());
        for word in raw {
            buf.extend_from_slice(&word.to_le_bytes());
        }
        atomic_write(path, &buf)
    }

    pub fn load(path: &Path) -> Result<Self> {
        let data = std::fs::read(path)?;
        anyhow::ensure!(data.len() >= 12, "bloom file too short");
        let num_hashes = u32::from_le_bytes(data[0..4].try_into()?);
        let num_bits = u64::from_le_bytes(data[4..12].try_into()?);
        let word_size = std::mem::size_of::<usize>();
        let n_words = (data.len() - 12) / word_size;
        let mut raw = Vec::with_capacity(n_words);
        for i in 0..n_words {
            let off = 12 + i * word_size;
            let mut bytes = [0u8; std::mem::size_of::<usize>()];
            bytes.copy_from_slice(&data[off..off + word_size]);
            raw.push(usize::from_le_bytes(bytes));
        }
        Ok(Self {
            bits: bitvec::vec::BitVec::from_vec(raw),
            num_hashes,
            num_bits,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xorf::Filter;

    #[test]
    fn fuse8_build_and_query() {
        let keys: Vec<u64> = (0..10_000).collect();
        let f = build_fuse8(&keys).unwrap();
        for &k in &keys {
            assert!(f.contains(&k));
        }
    }

    #[test]
    fn fuse16_build_and_query() {
        let keys: Vec<u64> = (0..10_000).collect();
        let f = build_fuse16(&keys).unwrap();
        for &k in &keys {
            assert!(f.contains(&k));
        }
    }

    #[test]
    fn fuse8_round_trip_bincode() {
        let keys: Vec<u64> = (1000..2000).collect();
        let f = build_fuse8(&keys).unwrap();
        let tmp = std::env::temp_dir().join("birdhash_test_fuse8.bin");
        save_fuse8(&f, &tmp).unwrap();
        let f2 = load_fuse8(&tmp).unwrap();
        for &k in &keys {
            assert!(f2.contains(&k));
        }
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn fuse16_round_trip_bincode() {
        let keys: Vec<u64> = (2000..3000).collect();
        let f = build_fuse16(&keys).unwrap();
        let tmp = std::env::temp_dir().join("birdhash_test_fuse16.bin");
        save_fuse16(&f, &tmp).unwrap();
        let f2 = load_fuse16(&tmp).unwrap();
        for &k in &keys {
            assert!(f2.contains(&k));
        }
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn fuse8_fp_rate_within_bound() {
        let keys: Vec<u64> = (0..50_000).collect();
        let f = build_fuse8(&keys).unwrap();
        let test_range = 1_000_000..1_100_000u64;
        let fp = test_range.filter(|k| f.contains(k)).count();
        let fp_rate = fp as f64 / 100_000.0;
        // BinaryFuse8 theoretical FP = 1/256 ≈ 0.39%, allow up to 1%
        assert!(fp_rate < 0.01, "FP rate {:.4} exceeds 1%", fp_rate);
    }

    #[test]
    fn bloom_build_and_query() {
        let mut b = BloomFilter::new(10_000);
        for i in 0..10_000u64 {
            b.insert(i);
        }
        for i in 0..10_000u64 {
            assert!(b.contains(i));
        }
    }

    #[test]
    fn bloom_fp_rate_near_10pct() {
        let n = 10_000;
        let mut b = BloomFilter::new(n);
        for i in 0..n as u64 {
            b.insert(i);
        }
        let test_n = 100_000;
        let fp = (1_000_000u64..1_000_000 + test_n as u64)
            .filter(|k| b.contains(*k))
            .count();
        let fp_rate = fp as f64 / test_n as f64;
        assert!(fp_rate < 0.15, "Bloom FP {:.3} > 15%", fp_rate);
        assert!(fp_rate > 0.05, "Bloom FP {:.3} < 5%, suspicious", fp_rate);
    }

    #[test]
    fn bloom_round_trip() {
        let mut b = BloomFilter::new(1000);
        for i in 0..1000u64 {
            b.insert(i);
        }
        let tmp = std::env::temp_dir().join("birdhash_test_bloom.bin");
        b.save(&tmp).unwrap();
        let b2 = BloomFilter::load(&tmp).unwrap();
        for i in 0..1000u64 {
            assert!(b2.contains(i));
        }
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn addr_to_u64_uses_all_bytes() {
        let mut a = [0u8; 20];
        let mut b = [0u8; 20];
        a[19] = 1; // differ only in last byte
        assert_ne!(addr_to_u64(&a), addr_to_u64(&b));
        b[0] = 1; // differ only in first byte
        a[19] = 0;
        assert_ne!(addr_to_u64(&a), addr_to_u64(&b));
    }
}
