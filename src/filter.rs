//! BinaryFuse16 过滤器：序列化/反序列化 + 地址→u64 指纹转换。

use anyhow::Result;
use std::path::Path;

pub use xorf::BinaryFuse16;

pub fn build_fuse16(keys: &[u64]) -> Result<BinaryFuse16> {
    BinaryFuse16::try_from(keys).map_err(|e| anyhow::anyhow!("BinaryFuse16 build failed: {:?}", e))
}

#[inline]
pub fn addr_to_u64(addr: &[u8; 20]) -> u64 {
    let a = u64::from_le_bytes([addr[0], addr[1], addr[2], addr[3], addr[4], addr[5], addr[6], addr[7]]);
    let b = u64::from_le_bytes([addr[8], addr[9], addr[10], addr[11], addr[12], addr[13], addr[14], addr[15]]);
    let c = u32::from_le_bytes([addr[16], addr[17], addr[18], addr[19]]) as u64;
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

pub fn save_fuse16(filter: &BinaryFuse16, path: &Path) -> Result<()> {
    if let Some(p) = path.parent() { std::fs::create_dir_all(p)?; }
    let data = bincode::serialize(filter)?;
    atomic_write(path, &data)
}

pub fn load_fuse16(path: &Path) -> Result<BinaryFuse16> {
    let data = std::fs::read(path)?;
    Ok(bincode::deserialize(&data)?)
}

fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use xorf::Filter;

    #[test]
    fn fuse16_build_and_query() {
        let keys: Vec<u64> = (0..10_000).collect();
        let f = build_fuse16(&keys).unwrap();
        for &k in &keys { assert!(f.contains(&k)); }
    }

    #[test]
    fn fuse16_round_trip() {
        let keys: Vec<u64> = (2000..3000).collect();
        let f = build_fuse16(&keys).unwrap();
        let tmp = std::env::temp_dir().join("birdhash_test_fuse16.bin");
        save_fuse16(&f, &tmp).unwrap();
        let f2 = load_fuse16(&tmp).unwrap();
        for &k in &keys { assert!(f2.contains(&k)); }
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn addr_to_u64_uses_all_bytes() {
        let mut a = [0u8; 20];
        let mut b = [0u8; 20];
        a[19] = 1;
        assert_ne!(addr_to_u64(&a), addr_to_u64(&b));
        b[0] = 1;
        a[19] = 0;
        assert_ne!(addr_to_u64(&a), addr_to_u64(&b));
    }
}
