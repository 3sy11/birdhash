//! BinaryFuse16 过滤器：序列化/反序列化 + 地址→u64 指纹转换。
//!
//! ## 假阳性概率（单指纹 u64）
//! 链上地址数 U，生成地址数 G。BF 只存 u64 指纹，20 字节→8 字节为多对一，故：
//! - 单次查询假阳性概率 P(FP) ≈ U / 2^64（随机地址指纹落在 U 个指纹集合内）。
//! - 若 U = 5e8（5 亿），P ≈ 2.7e-11；生成 1 万亿地址时期望假阳性约 27。
//! - 若 U = 1e9，P ≈ 5.4e-11；生成 1 万亿时期望约 54。实测数百假阳性说明 U 或 G 较大。
//!
//! ## 双指纹（128 位）与三指纹（192 位）降假阳
//! 双指纹：fp1 && fp2 命中，P(FP) ≈ U/2^128；BinaryFuse16 固有 FP≈1/2^16，双滤叠加约 (1/2^16)^2。
//! 三指纹：fp1 && fp2 && fp3 命中，固有 FP 约 (1/2^16)^3 = 2^-48，3000 地址/秒时约 1e-11/天，可视为极低。

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

/// 与 addr_to_u64 独立的第二指纹（不同混合常数），用于双指纹 128 位降假阳
#[inline]
pub fn addr_to_u64_alt(addr: &[u8; 20]) -> u64 {
    let a = u64::from_le_bytes([addr[0], addr[1], addr[2], addr[3], addr[4], addr[5], addr[6], addr[7]]);
    let b = u64::from_le_bytes([addr[8], addr[9], addr[10], addr[11], addr[12], addr[13], addr[14], addr[15]]);
    let c = u32::from_le_bytes([addr[16], addr[17], addr[18], addr[19]]) as u64;
    let mut h = b;
    h ^= a.wrapping_mul(0x9e3779b97f4a7c15);
    h ^= c.wrapping_mul(0x6c62272e07bb0142);
    h ^= h >> 31;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h
}

/// 第三指纹（与 fp1/fp2 独立），用于三指纹 192 位进一步降假阳
#[inline]
pub fn addr_to_u64_alt2(addr: &[u8; 20]) -> u64 {
    let a = u64::from_le_bytes([addr[0], addr[1], addr[2], addr[3], addr[4], addr[5], addr[6], addr[7]]);
    let b = u64::from_le_bytes([addr[8], addr[9], addr[10], addr[11], addr[12], addr[13], addr[14], addr[15]]);
    let c = u32::from_le_bytes([addr[16], addr[17], addr[18], addr[19]]) as u64;
    let mut h = c;
    h ^= a.wrapping_mul(0x6c62272e07bb0142);
    h ^= b.wrapping_mul(0x9e3779b97f4a7c15);
    h ^= h >> 27;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 31;
    h = h.wrapping_mul(0x517cc1b727220a95);
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

    #[test]
    fn addr_to_u64_alt_independent() {
        let a = [1u8; 20];
        assert_ne!(addr_to_u64(&a), addr_to_u64_alt(&a));
        let mut b = [2u8; 20];
        b[0] = 1;
        assert_ne!(addr_to_u64_alt(&a), addr_to_u64_alt(&b));
    }
}
