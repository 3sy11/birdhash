//! BTC 碰撞器：N 线程生成 BTC 地址(hash160) 并与 BF 碰撞。
//! BIP44(P2PKH) / BIP49(P2SH-P2WPKH) / BIP84(P2WPKH) 三 purpose。

use anyhow::Result;
use hmac::{Hmac, Mac};
use ripemd::Ripemd160;
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Instant;

use crate::collider;
use crate::config::AppConfig;
use crate::derivation::ACCOUNT_MAX;

type HmacSha256 = Hmac<Sha256>;
const ADDR_LEN: usize = 20;
const BIP44_COIN_BTC: u32 = 0;
const BIP44_CHANGE: u32 = 0;
const PURPOSES: [u32; 3] = [44, 49, 84];
const CHECKPOINT_INTERVAL_SECS: f64 = 2.0;
const BF_RELOAD_INTERVAL_SECS: u64 = 60;

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

/// compressed pubkey → SHA256 → RIPEMD160 → hash160
fn pubkey_to_hash160(compressed_pubkey: &[u8; 33]) -> [u8; ADDR_LEN] {
    let sha = Sha256::digest(compressed_pubkey);
    let rip = Ripemd160::digest(&sha);
    let mut out = [0u8; ADDR_LEN];
    out.copy_from_slice(&rip);
    out
}

fn derive_btc_hash160(seed: &[u8; 64], purpose: u32, account: u32, index: u32) -> Result<([u8; 32], [u8; ADDR_LEN])> {
    use bip32::{DerivationPath, XPrv};
    let path_str = format!("m/{}'/{}'/{}'/{}'/{}",
        purpose, BIP44_COIN_BTC, account, BIP44_CHANGE, index);
    let path: DerivationPath = path_str.parse().map_err(|e| anyhow::anyhow!("path: {:?}", e))?;
    let xprv = XPrv::derive_from_path(seed, &path).map_err(|e| anyhow::anyhow!("derive: {:?}", e))?;
    let mut sk = [0u8; 32];
    sk.copy_from_slice(&xprv.to_bytes());
    let secp = secp256k1::Secp256k1::new();
    let pk = secp256k1::PublicKey::from_secret_key(
        &secp,
        &secp256k1::SecretKey::from_slice(&sk).map_err(|e| anyhow::anyhow!("{:?}", e))?,
    );
    let compressed = pk.serialize();
    let hash160 = pubkey_to_hash160(&compressed);
    Ok((sk, hash160))
}

/// hash160 → 人可读地址
fn hash160_to_btc_address(hash160: &[u8; ADDR_LEN], purpose: u32) -> String {
    match purpose {
        44 => { // P2PKH: version 0x00
            let mut payload = vec![0x00];
            payload.extend_from_slice(hash160);
            bs58::encode(payload).with_check().into_string()
        }
        49 => { // P2SH-P2WPKH: version 0x05
            let mut redeem = vec![0x00, 0x14]; // OP_0 PUSH20
            redeem.extend_from_slice(hash160);
            let script_hash = Ripemd160::digest(&Sha256::digest(&redeem));
            let mut payload = vec![0x05];
            payload.extend_from_slice(&script_hash);
            bs58::encode(payload).with_check().into_string()
        }
        84 => { // P2WPKH: bech32 bc1q...
            bech32_encode_p2wpkh(hash160)
        }
        _ => hex::encode(hash160),
    }
}

fn bech32_encode_p2wpkh(hash160: &[u8; 20]) -> String {
    let mut data5 = vec![0u8]; // witness version 0
    // 8-bit → 5-bit
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    for &b in hash160.iter() {
        acc = (acc << 8) | b as u32;
        bits += 8;
        while bits >= 5 { bits -= 5; data5.push(((acc >> bits) & 31) as u8); }
    }
    if bits > 0 { data5.push(((acc << (5 - bits)) & 31) as u8); }
    // bech32 checksum (version 1 polymod)
    let hrp_expand: Vec<u8> = vec![3, 3, 0, 2, 3]; // "bc" → [3,3,0,2,3]
    let mut values = hrp_expand;
    values.extend_from_slice(&data5);
    values.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
    let polymod = bech32_polymod(&values) ^ 1;
    let mut checksum = Vec::with_capacity(6);
    for i in (0..6).rev() { checksum.push(((polymod >> (5 * i)) & 31) as u8); }
    const CHARSET: &[u8] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";
    let mut out = String::from("bc1");
    for &v in &data5 { out.push(CHARSET[v as usize] as char); }
    for &v in &checksum { out.push(CHARSET[v as usize] as char); }
    out
}

fn bech32_polymod(values: &[u8]) -> u32 {
    const GEN: [u32; 5] = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];
    let mut chk: u32 = 1;
    for &v in values {
        let b = chk >> 25;
        chk = ((chk & 0x1ffffff) << 5) ^ v as u32;
        for (i, &g) in GEN.iter().enumerate() {
            if (b >> i) & 1 != 0 { chk ^= g; }
        }
    }
    chk
}

// ── 候选路径计算 ──

fn paths_per_id(candidates: &[u32]) -> u64 {
    (ACCOUNT_MAX as u64 + 1) * candidates.len() as u64 * PURPOSES.len() as u64
}

fn path_index_to_parts(path_index: u64, candidates: &[u32]) -> (u32, u32, u32) {
    let purposes_len = PURPOSES.len() as u64;
    let cand_len = candidates.len() as u64;
    let purposes_x_cand = purposes_len * cand_len;
    let account = (path_index / purposes_x_cand) as u32;
    let rem = path_index % purposes_x_cand;
    let purpose_idx = (rem / cand_len) as usize;
    let cand_idx = (rem % cand_len) as usize;
    (
        PURPOSES[purpose_idx.min(PURPOSES.len() - 1)],
        account.min(ACCOUNT_MAX),
        candidates[cand_idx.min(candidates.len() - 1)],
    )
}

fn load_derivation_candidates(path: &Path) -> Result<Vec<u32>> {
    anyhow::ensure!(path.exists(), "派生候选文件不存在: {}", path.display());
    let f = File::open(path)?;
    let mut out = Vec::new();
    for line in BufReader::new(f).lines() {
        let s = line?.trim().to_string();
        if s.is_empty() || s.starts_with('#') { continue; }
        if let Ok(n) = s.parse::<u32>() { out.push(n); }
    }
    anyhow::ensure!(!out.is_empty(), "派生候选为空");
    Ok(out)
}

fn ensure_hits_csv(path: &Path) -> Result<()> {
    if path.exists() { return Ok(()); }
    if let Some(p) = path.parent() { std::fs::create_dir_all(p)?; }
    let mut f = File::create(path)?;
    writeln!(f, "btc_address,hash160,私钥,派生路径,助记词")?;
    Ok(())
}

fn append_hit(path: &Path, hash160: &[u8; ADDR_LEN], privkey: &[u8; 32], purpose: u32, deriv_path: &str, mnemonic: &str) -> Result<()> {
    ensure_hits_csv(path)?;
    let addr_str = hash160_to_btc_address(hash160, purpose);
    let mut f = OpenOptions::new().append(true).open(path)?;
    writeln!(f, "{},{},{},{},{}", addr_str, hex::encode(hash160), hex::encode(privkey), deriv_path, mnemonic)?;
    f.flush()?;
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct Checkpoint { next_address_index: u64 }

fn load_checkpoint(path: &Path) -> u64 {
    std::fs::read_to_string(path).ok()
        .and_then(|s| serde_json::from_str::<Checkpoint>(&s).ok())
        .map(|c| c.next_address_index).unwrap_or(0)
}

fn save_checkpoint(path: &Path, n: u64) -> Result<()> {
    let cp = Checkpoint { next_address_index: n };
    if let Some(p) = path.parent() { std::fs::create_dir_all(p)?; }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_string(&cp)?)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

// ── 主入口 ──

pub fn run_btc_collider(cfg: &AppConfig, num_threads: usize) -> Result<()> {
    cfg.ensure_btc_dirs()?;
    let seed_key = collider::load_or_create_seed(&cfg.generator_seed_path())?;
    let candidates = load_derivation_candidates(&cfg.derivation_candidates_path())?;
    let candidates = Arc::new(candidates);
    let paths_per = paths_per_id(&candidates);
    let checkpoint_path = cfg.btc_collider_cursor_path();
    let hits_csv = cfg.btc_hits_csv_path();
    ensure_hits_csv(&hits_csv)?;

    let bf_dir = cfg.btc_filter_dir();
    anyhow::ensure!(bf_dir.exists(), "BTC BF 目录不存在: {}，请先 fetch -B + build-filter -B", bf_dir.display());
    let bf_filters = collider::load_all_bf_pub(&bf_dir)?;
    let bf_count = bf_filters.len();
    anyhow::ensure!(!bf_filters.is_empty(), "BTC BF 为空，请先 fetch -B + build-filter -B");
    let bf = Arc::new(RwLock::new(bf_filters));

    let start_n = load_checkpoint(&checkpoint_path);
    let next_n = Arc::new(AtomicU64::new(start_n));
    let total_generated = Arc::new(AtomicU64::new(0));
    let total_hits = Arc::new(AtomicU64::new(0));

    println!("  BTC 碰撞器启动 | BF {} 个 | purpose {:?} | 候选 {} | 路径/ID {} | 线程 {} | 断点 N={}",
        bf_count, PURPOSES, candidates.len(), paths_per, num_threads, start_n);
    println!("  命中写入 {} | 检查点 {}", hits_csv.display(), checkpoint_path.display());

    // BF 热更新
    let bf_reload = Arc::clone(&bf);
    let bf_dir_clone = bf_dir.clone();
    thread::spawn(move || loop {
        thread::sleep(std::time::Duration::from_secs(BF_RELOAD_INTERVAL_SECS));
        if let Ok(new_f) = collider::load_all_bf_pub(&bf_dir_clone) {
            if !new_f.is_empty() {
                let n = new_f.len();
                *bf_reload.write().unwrap() = new_f;
                log::info!("BTC BF 热更新: {} 个", n);
            }
        }
    });

    for _ in 0..num_threads {
        let sk = seed_key;
        let cand = Arc::clone(&candidates);
        let bf = Arc::clone(&bf);
        let next = Arc::clone(&next_n);
        let tot = Arc::clone(&total_generated);
        let hits = Arc::clone(&total_hits);
        let csv_path = hits_csv.clone();
        thread::spawn(move || loop {
            let n = next.fetch_add(1, Ordering::SeqCst);
            let id = n / paths_per;
            let path_index = n % paths_per;
            let (purpose, account, index) = path_index_to_parts(path_index, &cand);
            let (phrase, seed) = match id_to_mnemonic_and_seed(&sk, id) {
                Ok(x) => x, Err(_) => continue,
            };
            let (privkey, hash160) = match derive_btc_hash160(&seed, purpose, account, index) {
                Ok(x) => x, Err(_) => continue,
            };
            tot.fetch_add(1, Ordering::Relaxed);
            let bf_guard = bf.read().unwrap();
            if collider::contains_bf_pub(&bf_guard, &hash160) {
                drop(bf_guard);
                let path_str = format!("m/{}'/{}'/{}'/{}'/{}",
                    purpose, BIP44_COIN_BTC, account, BIP44_CHANGE, index);
                let _ = append_hit(&csv_path, &hash160, &privkey, purpose, &path_str, &phrase);
                hits.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    let start = Instant::now();
    loop {
        thread::sleep(std::time::Duration::from_secs_f64(CHECKPOINT_INTERVAL_SECS));
        let n = next_n.load(Ordering::SeqCst);
        let _ = save_checkpoint(&checkpoint_path, n);
        let total = total_generated.load(Ordering::Relaxed);
        let hit_count = total_hits.load(Ordering::Relaxed);
        let elapsed = start.elapsed().as_secs_f64();
        let rate = total as f64 / elapsed.max(0.001);
        let current_id = n / paths_per;
        let bf_cnt = bf.read().map(|g| g.len()).unwrap_or(0);
        print!("\r  BTC N={} | ID={} | {:.0}/s | 已生成 {} | 命中 {} | BF {} 个  ",
            n, current_id, rate, total, hit_count, bf_cnt);
        let _ = std::io::stdout().flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash160_known_vector() {
        // compressed pubkey for privkey=1 (known Bitcoin testcase)
        let secp = secp256k1::Secp256k1::new();
        let mut sk_bytes = [0u8; 32];
        sk_bytes[31] = 1;
        let sk = secp256k1::SecretKey::from_slice(&sk_bytes).unwrap();
        let pk = secp256k1::PublicKey::from_secret_key(&secp, &sk);
        let compressed = pk.serialize();
        let h160 = pubkey_to_hash160(&compressed);
        // known: 0279BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798
        // → hash160 = 751e76e8199196d454941c45d1b3a323f1433bd6
        assert_eq!(hex::encode(h160), "751e76e8199196d454941c45d1b3a323f1433bd6");
    }

    #[test]
    fn p2pkh_address_from_hash160() {
        let h160 = hex::decode("751e76e8199196d454941c45d1b3a323f1433bd6").unwrap();
        let mut addr = [0u8; 20];
        addr.copy_from_slice(&h160);
        let btc_addr = hash160_to_btc_address(&addr, 44);
        assert_eq!(btc_addr, "1BgGZ9tcN4rm9KBzDn7KprQz87SZ26SAMH");
    }

    #[test]
    fn bech32_address_from_hash160() {
        let h160 = hex::decode("751e76e8199196d454941c45d1b3a323f1433bd6").unwrap();
        let mut addr = [0u8; 20];
        addr.copy_from_slice(&h160);
        let btc_addr = hash160_to_btc_address(&addr, 84);
        assert_eq!(btc_addr, "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4");
    }

    #[test]
    fn path_parts_round_trip() {
        let candidates = vec![0, 1, 2];
        let total = paths_per_id(&candidates);
        assert_eq!(total, 11 * 3 * 3); // ACCOUNT_MAX+1 * candidates * purposes
        let (purpose, account, index) = path_index_to_parts(0, &candidates);
        assert_eq!((purpose, account, index), (44, 0, 0));
        let (purpose, account, index) = path_index_to_parts(3, &candidates);
        assert_eq!((purpose, account, index), (49, 0, 0));
    }
}
