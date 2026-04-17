//! 生成器工具函数：查询 ID 信息、导出派生 CSV。
//! 核心生成+碰撞逻辑已移至 collider.rs。

use anyhow::Result;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use tiny_keccak::{Hasher, Keccak};

use crate::config::AppConfig;
use crate::derivation::ACCOUNT_MAX;

type HmacSha256 = Hmac<Sha256>;
const ADDR_LEN: usize = 20;
const BIP44_PURPOSE: u32 = 44;
const BIP44_COIN_TYPE_ETH: u32 = 60;
const BIP44_CHANGE: u32 = 0;

fn id_to_entropy(seed_key: &[u8], id: u64) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(seed_key).expect("HMAC key");
    mac.update(&id.to_le_bytes());
    let h1 = mac.finalize().into_bytes();
    let mut mac = HmacSha256::new_from_slice(seed_key).expect("HMAC key");
    mac.update(&h1);
    mac.update(&(id.wrapping_add(1)).to_le_bytes());
    let h2 = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out[..16].copy_from_slice(&h1[..16]);
    out[16..].copy_from_slice(&h2[..16]);
    out
}

pub fn load_or_create_seed(path: &Path) -> Result<[u8; 32]> {
    crate::collider::load_or_create_seed(path)
}

fn id_to_mnemonic_and_seed(seed_key: &[u8], id: u64) -> Result<(String, [u8; 64])> {
    use bip32::{Language, Mnemonic};
    let entropy = id_to_entropy(seed_key, id);
    let m = Mnemonic::from_entropy(entropy, Language::English);
    let phrase = m.phrase().to_string();
    let seed = m.to_seed("");
    let mut out = [0u8; 64];
    out.copy_from_slice(seed.as_ref());
    Ok((phrase, out))
}

fn derive_eth_privkey_and_address(
    seed: &[u8; 64],
    account: u32,
    index: u32,
) -> Result<([u8; 32], [u8; ADDR_LEN])> {
    use bip32::{DerivationPath, XPrv};
    let path_str = format!(
        "m/{}'/{}'/{}'/{}'/{}",
        BIP44_PURPOSE, BIP44_COIN_TYPE_ETH, account, BIP44_CHANGE, index
    );
    let path: DerivationPath = path_str
        .parse()
        .map_err(|e| anyhow::anyhow!("path: {:?}", e))?;
    let xprv =
        XPrv::derive_from_path(seed, &path).map_err(|e| anyhow::anyhow!("derive: {:?}", e))?;
    let mut sk = [0u8; 32];
    sk.copy_from_slice(&xprv.to_bytes());
    let secp = secp256k1::Secp256k1::new();
    let pk = secp256k1::PublicKey::from_secret_key(
        &secp,
        &secp256k1::SecretKey::from_slice(&sk).map_err(|e| anyhow::anyhow!("{:?}", e))?,
    );
    let mut hash = [0u8; 32];
    let mut keccak = Keccak::v256();
    keccak.update(&pk.serialize_uncompressed()[1..]);
    keccak.finalize(&mut hash);
    let mut addr = [0u8; ADDR_LEN];
    addr.copy_from_slice(&hash[12..]);
    Ok((sk, addr))
}

pub fn load_derivation_candidates(path: &Path) -> Result<Vec<u32>> {
    anyhow::ensure!(path.exists(), "派生候选文件不存在: {}", path.display());
    let f = File::open(path)?;
    let mut out = Vec::new();
    for line in BufReader::new(f).lines() {
        let s = line?.trim().to_string();
        if s.is_empty() || s.starts_with('#') {
            continue;
        }
        if let Ok(n) = s.parse::<u32>() {
            out.push(n);
        }
    }
    anyhow::ensure!(!out.is_empty(), "派生候选为空");
    Ok(out)
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

pub fn export_id_all_derivations_to_csv(cfg: &AppConfig, id: u64, out_path: &Path) -> Result<()> {
    cfg.ensure_dirs()?;
    let seed_key = load_or_create_seed(&cfg.generator_seed_path())?;
    let candidates = load_derivation_candidates(&cfg.derivation_candidates_path())?;
    let (phrase, seed) = id_to_mnemonic_and_seed(&seed_key, id)?;
    let first3: String = phrase
        .split_whitespace()
        .take(3)
        .collect::<Vec<_>>()
        .join(" ");
    if let Some(p) = out_path.parent() {
        std::fs::create_dir_all(p)?;
    }
    let mut f = File::create(out_path)?;
    writeln!(
        f,
        "id,mnemonic_first3,account,index,derivation_path,privkey_hex,address_hex"
    )?;
    let mut rows = 0u64;
    for account in 0..=ACCOUNT_MAX {
        for &index in &candidates {
            let path_str = format!(
                "m/{}'/{}'/{}'/{}'/{}",
                BIP44_PURPOSE, BIP44_COIN_TYPE_ETH, account, BIP44_CHANGE, index
            );
            let (privkey, addr) = match derive_eth_privkey_and_address(&seed, account, index) {
                Ok(x) => x,
                Err(_) => continue,
            };
            writeln!(
                f,
                "{},{},{},{},{},{},0x{}",
                id,
                csv_escape(&first3),
                account,
                index,
                csv_escape(&path_str),
                hex::encode(privkey),
                hex::encode(addr)
            )?;
            rows += 1;
        }
    }
    println!("wrote {} rows -> {}", rows, out_path.display());
    Ok(())
}

pub fn print_id_details(cfg: &AppConfig, id: u64) -> Result<()> {
    cfg.ensure_dirs()?;
    let seed_key = load_or_create_seed(&cfg.generator_seed_path())?;
    let candidates = load_derivation_candidates(&cfg.derivation_candidates_path())?;
    let (phrase, seed) = id_to_mnemonic_and_seed(&seed_key, id)?;
    let account = 0u32;
    let index = *candidates.first().unwrap_or(&0);
    let path_str = format!(
        "m/{}'/{}'/{}'/{}'/{}",
        BIP44_PURPOSE, BIP44_COIN_TYPE_ETH, account, BIP44_CHANGE, index
    );
    let (privkey, addr) = derive_eth_privkey_and_address(&seed, account, index)?;
    let first3: String = phrase
        .split_whitespace()
        .take(3)
        .collect::<Vec<_>>()
        .join(" ");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  ID:           {}", id);
    println!("  助记词(前3):  {}", first3);
    println!("  助记词(完整): {}", phrase);
    println!("  示例路径:     {}  (account={} index={}, 共 {}×{} 条路径)", path_str, account, index, ACCOUNT_MAX + 1, candidates.len());
    println!("  私钥(hex):    {}", hex::encode(privkey));
    println!("  地址:         0x{}", hex::encode(addr));
    println!("═══════════════════════════════════════════════════════════════");
    Ok(())
}
