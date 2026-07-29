//! BTC 链上地址获取器：BTC Core JSON-RPC + 两层去重（BF + Bloom）+ addr parquet 落盘。

use anyhow::{Context, Result};
use std::io::Write;
use std::path::Path;

use crate::collider;
use crate::config::AppConfig;
use crate::dedup_bloom::DedupBloom;
use crate::fetcher;

const ADDR_LEN: usize = 20;
type Address = [u8; ADDR_LEN];
const SEGMENT_SIZE: u64 = 10;
const CHUNK_SIZE: u64 = 10;
const CHECKPOINT_INTERVAL: u64 = 5;
const DEDUP_CAPACITY_PER_SEGMENT: u64 = 500_000;

// ── BTC Core JSON-RPC ──

pub struct BtcRpc {
    url: String,
    user: String,
    password: String,
    agent: ureq::Agent,
    #[allow(dead_code)]
    timeout_secs: u64,
}

impl BtcRpc {
    pub fn new(url: &str, user: &str, password: &str, timeout_secs: u64) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(std::time::Duration::from_secs(10))
            .timeout_read(std::time::Duration::from_secs(timeout_secs.max(1)))
            .build();
        Self { url: url.to_string(), user: user.to_string(), password: password.to_string(), agent, timeout_secs }
    }

    fn call(&self, method: &str, params: &serde_json::Value) -> Result<serde_json::Value> {
        let body = serde_json::json!({"jsonrpc":"1.0","id":"birdhash","method":method,"params":params});
        let auth = format!("{}:{}", self.user, self.password);
        let encoded = base64_encode(auth.as_bytes());
        let resp = self.agent.post(&self.url)
            .set("Content-Type", "application/json")
            .set("Authorization", &format!("Basic {}", encoded))
            .send_json(body.clone())
            .map_err(|e| anyhow::anyhow!("BTC RPC {}: {}", method, e))?;
        let json: serde_json::Value = resp.into_json()?;
        if let Some(err) = json.get("error") {
            if !err.is_null() { anyhow::bail!("BTC RPC {}: {}", method, err); }
        }
        Ok(json["result"].clone())
    }

    pub fn get_block_count(&self) -> Result<u64> {
        let r = self.call("getblockcount", &serde_json::json!([]))?;
        r.as_u64().context("getblockcount: not u64")
    }

    pub fn get_block_hash(&self, height: u64) -> Result<String> {
        let r = self.call("getblockhash", &serde_json::json!([height]))?;
        r.as_str().map(|s| s.to_string()).context("getblockhash: not string")
    }

    pub fn get_block_verbose(&self, hash: &str) -> Result<serde_json::Value> {
        self.call("getblock", &serde_json::json!([hash, 2]))
    }
}

fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(CHARS[((n >> 18) & 63) as usize] as char);
        out.push(CHARS[((n >> 12) & 63) as usize] as char);
        if chunk.len() > 1 { out.push(CHARS[((n >> 6) & 63) as usize] as char); } else { out.push('='); }
        if chunk.len() > 2 { out.push(CHARS[(n & 63) as usize] as char); } else { out.push('='); }
    }
    out
}

// ── 地址解码 ──

/// Base58Check 地址 → hash160 (20 bytes)。支持 1xxx (P2PKH) 和 3xxx (P2SH)。
pub fn decode_base58_addr(addr: &str) -> Option<Address> {
    let decoded = bs58::decode(addr).with_check(None).into_vec().ok()?;
    if decoded.len() != 21 { return None; }
    let mut out = [0u8; ADDR_LEN];
    out.copy_from_slice(&decoded[1..21]);
    Some(out)
}

/// Bech32 地址 → witness program (20 bytes for P2WPKH)。手动解析 bc1q 地址。
pub fn decode_bech32_addr(addr: &str) -> Option<Address> {
    let addr_lower = addr.to_lowercase();
    if !addr_lower.starts_with("bc1q") { return None; }
    let data_part = &addr_lower[4..];
    const CHARSET: &str = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";
    let mut values = Vec::with_capacity(data_part.len());
    for ch in data_part.chars() {
        let pos = CHARSET.find(ch)? as u8;
        values.push(pos);
    }
    // bech32 checksum 为最后 6 个字符
    if values.len() < 7 { return None; }
    let data_values = &values[..values.len() - 6];
    // witness version = data_values[0], 对 P2WPKH 应为 0
    if data_values.is_empty() || data_values[0] != 0 { return None; }
    let prog_5bit = &data_values[1..];
    // 5-bit → 8-bit 转换
    let bytes = convert_bits(prog_5bit, 5, 8, false)?;
    if bytes.len() != 20 { return None; }
    let mut out = [0u8; ADDR_LEN];
    out.copy_from_slice(&bytes);
    Some(out)
}

fn convert_bits(data: &[u8], from_bits: u32, to_bits: u32, pad: bool) -> Option<Vec<u8>> {
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    let max_v = (1u32 << to_bits) - 1;
    let mut out = Vec::new();
    for &val in data {
        if (val as u32) >> from_bits != 0 { return None; }
        acc = (acc << from_bits) | val as u32;
        bits += from_bits;
        while bits >= to_bits {
            bits -= to_bits;
            out.push(((acc >> bits) & max_v) as u8);
        }
    }
    if pad && bits > 0 {
        out.push(((acc << (to_bits - bits)) & max_v) as u8);
    } else if bits >= from_bits || ((acc << (to_bits - bits)) & max_v) != 0 {
        if !pad && bits > 0 { /* 忽略剩余位 */ }
    }
    Some(out)
}

/// 从 BTC 区块 JSON 提取所有地址 hash160
pub fn extract_addresses_from_btc_block(block: &serde_json::Value) -> Vec<Address> {
    let mut addrs = Vec::new();
    let txs = match block["tx"].as_array() {
        Some(a) => a,
        None => return addrs,
    };
    for tx in txs {
        if let Some(vouts) = tx["vout"].as_array() {
            for vout in vouts {
                if let Some(addr_str) = vout["scriptPubKey"]["address"].as_str() {
                    if let Some(h) = decode_base58_addr(addr_str).or_else(|| decode_bech32_addr(addr_str)) {
                        addrs.push(h);
                    }
                }
                // 某些节点用 addresses 数组
                if let Some(arr) = vout["scriptPubKey"]["addresses"].as_array() {
                    for a in arr {
                        if let Some(s) = a.as_str() {
                            if let Some(h) = decode_base58_addr(s).or_else(|| decode_bech32_addr(s)) {
                                addrs.push(h);
                            }
                        }
                    }
                }
            }
        }
    }
    addrs
}

// ── 主入口 ──

pub fn run_btc_fetch(
    cfg: &AppConfig, batches: &[u64], rpc_url: Option<&str>,
) -> Result<()> {
    cfg.ensure_btc_dirs()?;
    let url = rpc_url.map(|s| s.to_string())
        .or_else(|| cfg.btc_rpc_url.clone())
        .context("需要 BTC RPC URL：--rpc 或 config.toml [btc_fetcher] rpc_url")?;
    let user = cfg.btc_rpc_user.clone().unwrap_or_default();
    let pass = cfg.btc_rpc_password.clone().unwrap_or_default();
    let rpc = BtcRpc::new(&url, &user, &pass, cfg.btc_rpc_timeout_secs);

    let block_count = rpc.get_block_count()?;
    let total_batches = (block_count + SEGMENT_SIZE - 1) / SEGMENT_SIZE;
    println!("  BTC block_count={} total_batches={}", block_count, total_batches);

    let batches: Vec<u64> = if batches.is_empty() {
        vec![total_batches]
    } else {
        batches.to_vec()
    };
    for &b in &batches {
        anyhow::ensure!(b >= 1 && b <= total_batches, "batch {} out of range 1..{}", b, total_batches);
    }

    // 第一层：加载已建 BF
    let bf_dir = cfg.btc_filter_dir();
    let bf_triples = if bf_dir.exists() {
        collider::load_all_bf_pub(&bf_dir).unwrap_or_default()
    } else { vec![] };
    let has_bf = !bf_triples.is_empty();
    if has_bf { println!("  第一层去重：已加载 {} 组 BF", bf_triples.len()); }

    // 第二层：从已有 parquet 重建 dedup Bloom
    let addr_root = cfg.btc_address_dir();
    let extra_cap = batches.len() as u64 * DEDUP_CAPACITY_PER_SEGMENT;
    let mut bloom = rebuild_bloom_from_dir(&addr_root, extra_cap)?;
    println!("  第二层去重：Bloom {} 已有地址, 容量 {:.0}MB", bloom.count(), bloom.memory_bytes() as f64 / 1_048_576.0);

    for &batch in &batches {
        let start_block = (batch - 1) * SEGMENT_SIZE + 1;
        let end_block = (batch * SEGMENT_SIZE).min(block_count);
        println!("  batch={} blocks {}..{}", batch, start_block, end_block);
        run_btc_segment(
            &rpc, &addr_root, batch, start_block, end_block,
            &bf_triples, &mut bloom, has_bf,
        )?;
    }
    Ok(())
}

fn rebuild_bloom_from_dir(addr_root: &Path, extra_capacity: u64) -> Result<DedupBloom> {
    let mut count = 0u64;
    if addr_root.exists() {
        if let Ok(entries) = std::fs::read_dir(addr_root) {
            for e in entries.flatten() {
                let dir = e.path();
                if !dir.is_dir() { continue; }
                for i in 0..100u32 {
                    let p = dir.join(format!("chunk_{:03}.parquet", i));
                    if p.exists() {
                        count += fetcher::read_addr_parquet(&p).map(|v| v.len() as u64).unwrap_or(0);
                    }
                }
            }
        }
    }
    let capacity = (count + extra_capacity).max(1_000_000);
    let mut bloom = DedupBloom::new(capacity, 1e-8);
    if count > 0 && addr_root.exists() {
        if let Ok(entries) = std::fs::read_dir(addr_root) {
            for e in entries.flatten() {
                let dir = e.path();
                if !dir.is_dir() { continue; }
                for i in 0..100u32 {
                    let p = dir.join(format!("chunk_{:03}.parquet", i));
                    if !p.exists() { continue; }
                    if let Ok(addrs) = fetcher::read_addr_parquet(&p) {
                        for a in &addrs { bloom.insert(a); }
                    }
                }
            }
        }
        println!("  Bloom 重建完成：{} 个已有地址", bloom.count());
    }
    Ok(bloom)
}

fn run_btc_segment(
    rpc: &BtcRpc, addr_root: &Path, _batch: u64,
    start_block: u64, end_block: u64,
    bf_triples: &[collider::BfTriple], bloom: &mut DedupBloom, has_bf: bool,
) -> Result<()> {
    let seg_s = fetcher::seg_start_for(start_block);
    let seg_name = fetcher::seg_dir_name(seg_s);
    let seg_dir = addr_root.join(&seg_name);
    std::fs::create_dir_all(&seg_dir)?;

    // checkpoint
    let ck_path = seg_dir.join("checkpoint.json");
    let existing_ck = fetcher::load_btc_checkpoint(&ck_path);
    let resume_from = if existing_ck > 0 { existing_ck + 1 } else { start_block };
    if resume_from > end_block {
        println!("    {} 已完成, 跳过", seg_name);
        return Ok(());
    }

    let start_chunk_idx = ((resume_from.saturating_sub(seg_s)) / CHUNK_SIZE) as u32;
    let mut cur_chunk = start_chunk_idx;
    let mut buf_bns: Vec<u64> = Vec::new();
    let mut buf_addrs: Vec<Address> = Vec::new();

    // 恢复已有 chunk
    let chunk_path = seg_dir.join(format!("chunk_{:03}.parquet", cur_chunk));
    if chunk_path.exists() {
        if let Ok(addrs) = fetcher::read_addr_parquet(&chunk_path) {
            buf_addrs = addrs;
            buf_bns = vec![0u64; buf_addrs.len()];
        }
    }

    let flush = |dir: &Path, idx: u32, bns: &[u64], addrs: &[Address]| -> Result<()> {
        if addrs.is_empty() { return Ok(()); }
        fetcher::write_addr_parquet(&dir.join(format!("chunk_{:03}.parquet", idx)), bns, addrs)
    };

    let start_time = std::time::Instant::now();
    let mut total_blocks = 0u64;
    let mut total_addrs = 0u64;
    let mut skipped_bf = 0u64;
    let mut skipped_bloom = 0u64;
    let mut last_block = resume_from.saturating_sub(1);

    for height in resume_from..=end_block {
        let hash = rpc.get_block_hash(height)?;
        let block = rpc.get_block_verbose(&hash)?;
        let addrs = extract_addresses_from_btc_block(&block);

        for addr in &addrs {
            if has_bf && collider::contains_bf_pub(bf_triples, addr) { skipped_bf += 1; continue; }
            if bloom.contains(addr) { skipped_bloom += 1; continue; }
            let chunk_idx = ((height - seg_s) / CHUNK_SIZE) as u32;
            if chunk_idx != cur_chunk {
                flush(&seg_dir, cur_chunk, &buf_bns, &buf_addrs)?;
                buf_bns.clear(); buf_addrs.clear();
                cur_chunk = chunk_idx;
            }
            buf_bns.push(height);
            buf_addrs.push(*addr);
            bloom.insert(addr);
            total_addrs += 1;
        }

        last_block = height;
        total_blocks += 1;

        if total_blocks % CHECKPOINT_INTERVAL == 0 {
            flush(&seg_dir, cur_chunk, &buf_bns, &buf_addrs)?;
            fetcher::save_btc_checkpoint(&ck_path, last_block)?;
            let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
            print!("\r    [btc] block={} addrs={} bf_skip={} bloom_skip={} ({:.0} blk/s)   ",
                last_block, total_addrs, skipped_bf, skipped_bloom, total_blocks as f64 / elapsed);
            let _ = std::io::stdout().flush();
        }
    }

    flush(&seg_dir, cur_chunk, &buf_bns, &buf_addrs)?;
    fetcher::save_btc_checkpoint(&ck_path, last_block)?;
    let elapsed = start_time.elapsed().as_secs_f64();
    println!("\n    {} done: {} blocks, {} new addrs, bf_skip={}, bloom_skip={} ({:.1}s)",
        seg_name, total_blocks, total_addrs, skipped_bf, skipped_bloom, elapsed);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_base58_p2pkh() {
        // 中本聪创世块地址
        let addr = decode_base58_addr("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa");
        assert!(addr.is_some());
        let h = addr.unwrap();
        assert_eq!(hex::encode(h), "62e907b15cbf27d5425399ebf6f0fb50ebb88f18");
    }

    #[test]
    fn decode_base58_invalid() {
        assert!(decode_base58_addr("").is_none());
        assert!(decode_base58_addr("xyz").is_none());
    }

    #[test]
    fn extract_from_btc_block_json() {
        let block = serde_json::json!({
            "tx": [{
                "vout": [
                    {"scriptPubKey": {"address": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"}},
                    {"scriptPubKey": {"address": "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy"}}
                ]
            }]
        });
        let addrs = extract_addresses_from_btc_block(&block);
        assert_eq!(addrs.len(), 2);
    }
}
