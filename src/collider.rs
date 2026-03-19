//! 碰撞器：单进程，N 线程生成地址并与 BF(BinaryFuse16) 碰撞。
//! 启动时加载所有 BF，每 60 秒热更新；命中写 hits.csv；检查点断点续碰。

use anyhow::{Context, Result};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Instant;
use tiny_keccak::{Hasher, Keccak};
use xorf::Filter;

use crate::config::AppConfig;
use crate::derivation::ACCOUNT_MAX;
use crate::filter::{self, BinaryFuse16};

type HmacSha256 = Hmac<Sha256>;
const ADDR_LEN: usize = 20;
const BIP44_PURPOSE: u32 = 44;
const BIP44_COIN_TYPE_ETH: u32 = 60;
const BIP44_CHANGE: u32 = 0;
const CHECKPOINT_INTERVAL_SECS: f64 = 2.0;
const BF_RELOAD_INTERVAL_SECS: u64 = 60;

// ── 密钥派生（与 generator 一致） ──

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

fn derive_eth_privkey_and_address(seed: &[u8; 64], account: u32, index: u32) -> Result<([u8; 32], [u8; ADDR_LEN])> {
    use bip32::{DerivationPath, XPrv};
    let path_str = format!("m/{}'/{}'/{}'/{}'/{}", BIP44_PURPOSE, BIP44_COIN_TYPE_ETH, account, BIP44_CHANGE, index);
    let path: DerivationPath = path_str.parse().map_err(|e| anyhow::anyhow!("path: {:?}", e))?;
    let xprv = XPrv::derive_from_path(seed, &path).map_err(|e| anyhow::anyhow!("derive: {:?}", e))?;
    let mut sk = [0u8; 32];
    sk.copy_from_slice(&xprv.to_bytes());
    let secp = secp256k1::Secp256k1::new();
    let pk = secp256k1::PublicKey::from_secret_key(&secp, &secp256k1::SecretKey::from_slice(&sk).map_err(|e| anyhow::anyhow!("{:?}", e))?);
    let mut hash = [0u8; 32];
    let mut keccak = Keccak::v256();
    keccak.update(&pk.serialize_uncompressed()[1..]);
    keccak.finalize(&mut hash);
    let mut addr = [0u8; ADDR_LEN];
    addr.copy_from_slice(&hash[12..]);
    Ok((sk, addr))
}

// ── BF 加载（支持三指纹：.bin + .alt.bin + .alt2.bin，无 alt/alt2 时退化为双指纹或单指纹） ──

struct BfTriple(pub BinaryFuse16, pub Option<BinaryFuse16>, pub Option<BinaryFuse16>);

fn load_all_bf(fetcher_dir: &Path) -> Result<Vec<BfTriple>> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(fetcher_dir).with_context(|| format!("read_dir {}", fetcher_dir.display()))?
        .filter_map(|e| {
            let p = e.ok()?.path();
            let n = p.file_name()?.to_string_lossy().to_string();
            if n.starts_with("filter.") && n.ends_with(".bin") && !n.contains(".alt") && n.contains('-') { Some(p) } else { None }
        })
        .collect();
    paths.sort();
    let mut triples = Vec::with_capacity(paths.len());
    for p in &paths {
        let f1 = filter::load_fuse16(p).with_context(|| format!("load {}", p.display()))?;
        let parent = p.parent().unwrap_or_else(|| Path::new("."));
        let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        let alt_path = parent.join(format!("{}.alt.bin", stem));
        let alt2_path = parent.join(format!("{}.alt2.bin", stem));
        let f2 = if alt_path.exists() { Some(filter::load_fuse16(&alt_path).with_context(|| format!("load alt {}", alt_path.display()))?) } else { None };
        let f3 = if alt2_path.exists() { Some(filter::load_fuse16(&alt2_path).with_context(|| format!("load alt2 {}", alt2_path.display()))?) } else { None };
        triples.push(BfTriple(f1, f2, f3));
    }
    Ok(triples)
}

fn contains_bf(triples: &[BfTriple], addr: &[u8; ADDR_LEN]) -> bool {
    let fp1 = filter::addr_to_u64(addr);
    let fp2 = filter::addr_to_u64_alt(addr);
    let fp3 = filter::addr_to_u64_alt2(addr);
    triples.iter().any(|BfTriple(f1, alt, alt2)| {
        if let (Some(f2), Some(f3)) = (alt, alt2) { f1.contains(&fp1) && f2.contains(&fp2) && f3.contains(&fp3) }
        else if let Some(f2) = alt { f1.contains(&fp1) && f2.contains(&fp2) }
        else { f1.contains(&fp1) }
    })
}

/// 供 CLI filter-query 与碰撞器：从 fetcher_dir 加载所有 BF（含 .alt/.alt2 三指纹），判断地址是否命中
pub fn bf_contains(fetcher_dir: &Path, addr: &[u8; ADDR_LEN]) -> Result<bool> {
    let triples = load_all_bf(fetcher_dir)?;
    Ok(!triples.is_empty() && contains_bf(&triples, addr))
}

// ── 派生候选 ──

fn load_derivation_candidates(path: &Path) -> Result<Vec<u32>> {
    anyhow::ensure!(path.exists(), "派生候选文件不存在: {}，请先运行 birdhash init", path.display());
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

fn paths_per_id(candidates: &[u32]) -> u64 { 112 * candidates.len() as u64 }

fn path_index_to_account_index(path_index: u64, candidates: &[u32]) -> (u32, u32) {
    let n = candidates.len() as u64;
    let account = (path_index / n) as u32;
    let idx_pos = (path_index % n) as usize;
    (account.min(ACCOUNT_MAX), candidates[idx_pos.min(candidates.len().saturating_sub(1))])
}

// ── 种子 ──

pub fn load_or_create_seed(path: &Path) -> Result<[u8; 32]> {
    if path.exists() {
        let b = std::fs::read(path).with_context(|| format!("read seed {}", path.display()))?;
        anyhow::ensure!(b.len() == 32, "seed 须为 32 字节");
        let mut out = [0u8; 32]; out.copy_from_slice(&b); return Ok(out);
    }
    write_new_seed(path)
}

/// 生成新的 32 字节随机种子并写入，覆盖已有文件（init 时调用）
pub fn write_new_seed(path: &Path) -> Result<[u8; 32]> {
    let mut out = [0u8; 32];
    use rand::RngCore; rand::thread_rng().fill_bytes(&mut out);
    if let Some(p) = path.parent() { std::fs::create_dir_all(p)?; }
    std::fs::write(path, &out)?;
    Ok(out)
}

// ── 检查点 ──

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

// ── CSV ──

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') { format!("\"{}\"", s.replace('"', "\"\"")) } else { s.to_string() }
}

fn ensure_hits_csv(path: &Path) -> Result<()> {
    if path.exists() { return Ok(()); }
    if let Some(p) = path.parent() { std::fs::create_dir_all(p)?; }
    let mut f = File::create(path)?;
    writeln!(f, "地址,私钥,派生路径,助记词")?;
    Ok(())
}

fn append_hit(path: &Path, addr: &[u8; ADDR_LEN], privkey: &[u8; 32], deriv_path: &str, mnemonic: &str) -> Result<()> {
    ensure_hits_csv(path)?;
    let mut f = OpenOptions::new().append(true).open(path)?;
    writeln!(f, "0x{},{},{},{}", hex::encode(addr), hex::encode(privkey), csv_escape(deriv_path), csv_escape(mnemonic))?;
    f.flush()?;
    Ok(())
}

// ── 主入口 ──

pub fn run_collider(cfg: &AppConfig, num_threads: usize) -> Result<()> {
    cfg.ensure_dirs()?;
    let seed_key = load_or_create_seed(&cfg.generator_seed_path())?;
    let candidates = load_derivation_candidates(&cfg.derivation_candidates_path())?;
    let candidates = Arc::new(candidates);
    let paths_per = paths_per_id(&candidates);
    let checkpoint_path = cfg.collider_cursor_path();
    let hits_csv = cfg.hits_bf_csv_path();
    let fetcher_dir = cfg.fetcher_dir();
    ensure_hits_csv(&hits_csv)?;

    let bf_filters = load_all_bf(&fetcher_dir)?;
    let bf_count = bf_filters.len();
    anyhow::ensure!(!bf_filters.is_empty(), "未找到 BF 过滤器，请先运行 birdhash fetch + build-filter");
    let bf = Arc::new(RwLock::new(bf_filters));

    let start_n = load_checkpoint(&checkpoint_path);
    let next_n = Arc::new(AtomicU64::new(start_n));
    let total_generated = Arc::new(AtomicU64::new(0));
    let total_hits = Arc::new(AtomicU64::new(0));

    println!("  碰撞器启动 | BF {} 个 | 种子 {} | 候选 {} | 路径/ID {} | 线程 {} | 断点 N={}",
        bf_count, cfg.generator_seed_path().display(), candidates.len(), paths_per, num_threads, start_n);
    println!("  命中写入 {} | 检查点 {}", hits_csv.display(), checkpoint_path.display());

    // BF 热更新线程
    let bf_reload = Arc::clone(&bf);
    let fetcher_dir_reload = fetcher_dir.clone();
    thread::spawn(move || {
        loop {
            thread::sleep(std::time::Duration::from_secs(BF_RELOAD_INTERVAL_SECS));
            match load_all_bf(&fetcher_dir_reload) {
                Ok(new_filters) if !new_filters.is_empty() => {
                    let n = new_filters.len();
                    *bf_reload.write().unwrap() = new_filters;
                    log::info!("BF 热更新: {} 个过滤器", n);
                }
                _ => {}
            }
        }
    });

    // N 个 worker 线程
    for _ in 0..num_threads {
        let sk = seed_key;
        let cand = Arc::clone(&candidates);
        let bf = Arc::clone(&bf);
        let next = Arc::clone(&next_n);
        let tot = Arc::clone(&total_generated);
        let hits = Arc::clone(&total_hits);
        let csv_path = hits_csv.clone();
        thread::spawn(move || {
            loop {
                let n = next.fetch_add(1, Ordering::SeqCst);
                let id = n / paths_per;
                let path_index = n % paths_per;
                let (account, index) = path_index_to_account_index(path_index, &cand);
                let (phrase, seed) = match id_to_mnemonic_and_seed(&sk, id) { Ok(x) => x, Err(_) => continue };
                let (privkey, addr) = match derive_eth_privkey_and_address(&seed, account, index) { Ok(x) => x, Err(_) => continue };
                tot.fetch_add(1, Ordering::Relaxed);
                let bf_guard = bf.read().unwrap();
                if contains_bf(&bf_guard, &addr) {
                    drop(bf_guard);
                    let path_str = format!("m/{}'/{}'/{}'/{}'/{}", BIP44_PURPOSE, BIP44_COIN_TYPE_ETH, account, BIP44_CHANGE, index);
                    let _ = append_hit(&csv_path, &addr, &privkey, &path_str, &phrase);
                    hits.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }

    // 主线程：保存检查点 + 刷新进度
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
        let bf_count = bf.read().map(|g| g.len()).unwrap_or(0);
        print!("\r  N={} | ID={} | 速度 {:.0}/s | 已生成 {} | 命中 {} | BF {} 个  ",
            n, current_id, rate, total, hit_count, bf_count);
        let _ = std::io::stdout().flush();
    }
}
