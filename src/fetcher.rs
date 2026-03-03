//! Fetcher: traverse Ethereum blocks via JSON-RPC, extract unique addresses,
//! archive block→addresses to segmented files, build BinaryFuse16 filter.
//!
//! Storage layout:
//!   archives/archive_{start}_{end}.bin — segmented block→address archives (BHFA format)
//!   all_addrs.bin    — append-only flat [u8;20] (for filter building)
//!   filter_fetch.bin — BinaryFuse16 built from all_addrs
//!   new_addrs.bin    — addresses added since last filter rebuild (export for scan)

use crate::archive::{ArchiveWriter, BlockAddresses};
use crate::config::AppConfig;
use crate::cursor::{self, FetcherCursor};
use crate::filter;
use crate::keygen::{Address, ADDR_LEN};
use anyhow::{Context, Result};
use chrono::Utc;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tiny_keccak::{Hasher, Keccak};
use xorf::Filter as XorFilter;

const CURSOR_SAVE_INTERVAL: u64 = 50;
const STATS_INTERVAL_SECS: u64 = 5;
const FILTER_REBUILD_INTERVAL: u64 = 10_000;

// ── AddressStore: append-only flat [u8;20] file ──

pub struct AddressStore {
    path: std::path::PathBuf,
    count: u64,
}

impl AddressStore {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let count = if path.exists() {
            let len = std::fs::metadata(path)?.len();
            anyhow::ensure!(len % ADDR_LEN as u64 == 0, "all_addrs.bin corrupt: size {} not multiple of {}", len, ADDR_LEN);
            len / ADDR_LEN as u64
        } else { 0 };
        Ok(Self { path: path.to_path_buf(), count })
    }
    pub fn count(&self) -> u64 { self.count }

    pub fn append(&mut self, addrs: &[Address]) -> Result<()> {
        if addrs.is_empty() { return Ok(()); }
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&self.path)?;
        for a in addrs { f.write_all(a)?; }
        self.count += addrs.len() as u64;
        Ok(())
    }

    pub fn read_all_addresses(&self) -> Result<Vec<Address>> {
        if self.count == 0 { return Ok(vec![]); }
        let data = std::fs::read(&self.path)?;
        let mut addrs = Vec::with_capacity(self.count as usize);
        for chunk in data.chunks_exact(ADDR_LEN) {
            let mut addr = [0u8; ADDR_LEN];
            addr.copy_from_slice(chunk);
            addrs.push(addr);
        }
        Ok(addrs)
    }

    pub fn read_all_fingerprints(&self) -> Result<Vec<u64>> {
        if self.count == 0 { return Ok(vec![]); }
        let data = std::fs::read(&self.path)?;
        let mut fps = Vec::with_capacity(self.count as usize);
        for chunk in data.chunks_exact(ADDR_LEN) {
            let mut addr = [0u8; ADDR_LEN];
            addr.copy_from_slice(chunk);
            fps.push(filter::addr_to_u64(&addr));
        }
        Ok(fps)
    }
}

// ── Export/Import new_addrs.bin ──

pub fn save_new_addrs(addrs: &[Address], path: &std::path::Path) -> Result<()> {
    if let Some(p) = path.parent() { std::fs::create_dir_all(p)?; }
    let mut buf = Vec::with_capacity(8 + addrs.len() * ADDR_LEN);
    buf.extend_from_slice(&(addrs.len() as u64).to_le_bytes());
    for a in addrs { buf.extend_from_slice(a); }
    let tmp = path.with_extension("bin.tmp");
    std::fs::write(&tmp, &buf)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

pub fn load_new_addrs(path: &std::path::Path) -> Result<Vec<Address>> {
    if !path.exists() { return Ok(vec![]); }
    let data = std::fs::read(path)?;
    anyhow::ensure!(data.len() >= 8, "new_addrs.bin too short");
    let count = u64::from_le_bytes(data[0..8].try_into()?) as usize;
    anyhow::ensure!(data.len() == 8 + count * ADDR_LEN, "new_addrs.bin size mismatch");
    let mut addrs = Vec::with_capacity(count);
    for i in 0..count {
        let off = 8 + i * ADDR_LEN;
        let mut a = [0u8; ADDR_LEN];
        a.copy_from_slice(&data[off..off + ADDR_LEN]);
        addrs.push(a);
    }
    Ok(addrs)
}

// ── Ethereum JSON-RPC client with retry ──

pub struct EthRpc {
    url: String,
    agent: ureq::Agent,
    retry_count: u32,
    retry_base_ms: u64,
}

impl EthRpc {
    /// timeout_read_secs: 单次请求读超时，超时即返回 Err
    pub fn new(url: &str, retry_count: u32, retry_base_ms: u64, timeout_read_secs: u64) -> Self {
        let read_secs = timeout_read_secs.max(1);
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(std::time::Duration::from_secs(10))
            .timeout_read(std::time::Duration::from_secs(read_secs))
            .build();
        Self { url: url.to_string(), agent, retry_count, retry_base_ms }
    }

    fn call_with_retry(&self, body: &serde_json::Value) -> Result<serde_json::Value> {
        let mut last_err = None;
        for attempt in 0..=self.retry_count {
            if attempt > 0 {
                let backoff = self.retry_base_ms * (1u64 << (attempt - 1).min(5));
                log::warn!("RPC retry {}/{} after {}ms", attempt, self.retry_count, backoff);
                std::thread::sleep(std::time::Duration::from_millis(backoff));
            }
            match self.agent.post(&self.url).set("Content-Type", "application/json").send_json(body.clone()) {
                Ok(resp) => match resp.into_json::<serde_json::Value>() {
                    Ok(v) => return Ok(v),
                    Err(e) => { last_err = Some(anyhow::anyhow!("JSON parse: {}", e)); }
                },
                Err(e) => { last_err = Some(anyhow::anyhow!("RPC request: {}", e)); }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("RPC failed after retries")))
    }

    pub fn get_latest_block_number(&self) -> Result<u64> {
        let resp = self.call_with_retry(&serde_json::json!({"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}))?;
        let hex = resp["result"].as_str().context("eth_blockNumber: no result")?;
        Ok(u64::from_str_radix(hex.trim_start_matches("0x"), 16)?)
    }

    pub fn get_blocks_with_txs(&self, start: u64, count: usize) -> Result<Vec<(u64, serde_json::Value)>> {
        let reqs: Vec<serde_json::Value> = (0..count).map(|i| {
            let bn = start + i as u64;
            serde_json::json!({"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[format!("0x{:x}", bn), true],"id": bn})
        }).collect();
        let resp = self.call_with_retry(&serde_json::json!(reqs))?;
        let arr = resp.as_array().context("batch response not array")?;
        let mut results = Vec::with_capacity(count);
        for item in arr {
            let block = &item["result"];
            if block.is_null() { continue; }
            let bn_hex = block["number"].as_str().unwrap_or("0x0");
            let bn = u64::from_str_radix(bn_hex.trim_start_matches("0x"), 16).unwrap_or(0);
            results.push((bn, block.clone()));
        }
        results.sort_by_key(|&(bn, _)| bn);
        Ok(results)
    }
}

// ── RPC 多 URL 降级池：失败一次或超时即换下一个，池内轮询 ──

pub struct RpcPool {
    urls: Vec<String>,
    current: usize,
    timeout_secs: u64,
}

impl RpcPool {
    /// 每个 URL 只试一次，超时 timeout_secs 或失败即切下一个并轮询
    pub fn new(urls: Vec<String>, timeout_secs: u64) -> Self {
        Self { urls, current: 0, timeout_secs }
    }
    pub fn url_count(&self) -> usize { self.urls.len() }

    fn with_rpc<F, T>(&mut self, f: F) -> Result<T>
    where
        F: Fn(&EthRpc) -> Result<T>,
    {
        let n = self.urls.len();
        anyhow::ensure!(n > 0, "no RPC URLs configured");
        for _ in 0..n {
            let url = &self.urls[self.current];
            let rpc = EthRpc::new(url, 0, 0, self.timeout_secs);
            match f(&rpc) {
                Ok(t) => return Ok(t),
                Err(e) => {
                    log::warn!("RPC[{}] {} failed (timeout/err): {}, try next", self.current, url, e);
                    self.current = (self.current + 1) % n;
                }
            }
        }
        Err(anyhow::anyhow!("all {} RPC URLs failed", n))
    }

    pub fn get_latest_block_number(&mut self) -> Result<u64> {
        self.with_rpc(|rpc| rpc.get_latest_block_number())
    }

    pub fn get_blocks_with_txs(&mut self, start: u64, count: usize) -> Result<Vec<(u64, serde_json::Value)>> {
        self.with_rpc(|rpc| rpc.get_blocks_with_txs(start, count))
    }
}

/// 拉取单个块并返回 (block_json, 提取的地址列表)，用于测试/调试
pub fn fetch_one_block(
    rpc_urls: &[String],
    block_number: u64,
    timeout_secs: u64,
) -> Result<(serde_json::Value, Vec<Address>)> {
    let mut pool = RpcPool::new(rpc_urls.to_vec(), timeout_secs);
    let blocks = pool.get_blocks_with_txs(block_number, 1)?;
    let (_bn, block_json) = blocks.into_iter().next().context("no block returned")?;
    let addrs = extract_addresses_from_block(&block_json);
    Ok((block_json, addrs))
}

/// CREATE 合约地址: keccak256(rlp([sender, nonce]))[12..32]
fn create_address(sender: &[u8; ADDR_LEN], nonce: u64) -> Address {
    let mut stream = rlp::RlpStream::new_list(2);
    stream.append(&sender.as_slice());
    stream.append(&nonce);
    let encoded = stream.out();
    let mut hash = [0u8; 32];
    let mut keccak = Keccak::v256();
    keccak.update(&encoded);
    keccak.finalize(&mut hash);
    let mut addr = [0u8; ADDR_LEN];
    addr.copy_from_slice(&hash[12..32]);
    addr
}

fn parse_hex_u64(s: &str) -> Option<u64> {
    let s = s.trim_start_matches("0x");
    if s.is_empty() { return Some(0); }
    u64::from_str_radix(s, 16).ok()
}

/// 从区块与交易中提取所有可由私钥/确定性推导的地址：from/to/miner/author/withdrawals/creates/CREATE(contract)
pub(crate) fn extract_addresses_from_block(block: &serde_json::Value) -> Vec<Address> {
    let mut addrs = Vec::new();
    // 块元信息: miner / author (共识层出块者)
    if let Some(miner) = block["miner"].as_str() {
        if let Some(a) = parse_hex_addr(miner) { addrs.push(a); }
    }
    if let Some(author) = block["author"].as_str() {
        if let Some(a) = parse_hex_addr(author) { addrs.push(a); }
    }
    // EIP-4895 提款: withdrawals[].address
    if let Some(withdrawals) = block["withdrawals"].as_array() {
        for w in withdrawals {
            if let Some(addr) = w["address"].as_str() {
                if let Some(a) = parse_hex_addr(addr) { addrs.push(a); }
            }
        }
    }
    // 交易: from, to, 合约创建(creates 或 CREATE(sender,nonce))
    if let Some(txs) = block["transactions"].as_array() {
        for tx in txs {
            let from = tx["from"].as_str().and_then(parse_hex_addr);
            if let Some(a) = from { addrs.push(a); }
            if let Some(to) = tx["to"].as_str() {
                if let Some(a) = parse_hex_addr(to) { addrs.push(a); }
            } else {
                // to 为空 = 合约创建
                if let Some(ca) = tx["creates"].as_str() {
                    if let Some(a) = parse_hex_addr(ca) { addrs.push(a); }
                } else if let Some(ref from_addr) = from {
                    let nonce = tx["nonce"].as_str().and_then(parse_hex_u64).unwrap_or(0);
                    addrs.push(create_address(from_addr, nonce));
                }
            }
        }
    }
    addrs
}

fn parse_hex_addr(s: &str) -> Option<Address> {
    let s = s.trim_start_matches("0x");
    if s.len() != 40 { return None; }
    let bytes = hex::decode(s).ok()?;
    if bytes.len() != ADDR_LEN { return None; }
    let mut addr = [0u8; ADDR_LEN];
    addr.copy_from_slice(&bytes);
    Some(addr)
}

// ── Fetcher ──

pub struct Fetcher {
    config: AppConfig,
    cursor: FetcherCursor,
    rpc: RpcPool,
    filter: Option<filter::BinaryFuse16>,
    addr_store: AddressStore,
    archive: ArchiveWriter,
    new_addrs: Vec<Address>,
    shutdown: Arc<AtomicBool>,
    start_time: Instant,
    last_stats: Instant,
    last_cursor_save: u64,
    session_blocks: u64,
    session_addrs: u64,
}

impl Fetcher {
    /// rpc_urls: 至少一个 URL，失败时按序降级
    pub fn new(config: AppConfig, cursor: FetcherCursor, rpc_urls: &[String]) -> Result<Self> {
        anyhow::ensure!(!rpc_urls.is_empty(), "fetcher requires at least one RPC URL");
        let addr_store = AddressStore::open(&config.all_addrs_path())?;
        let filter = if config.fetch_filter_path().exists() {
            Some(filter::load_fuse16(&config.fetch_filter_path())?)
        } else { None };
        let new_addrs = load_new_addrs(&config.new_addrs_path()).unwrap_or_default();
        let mut archive = ArchiveWriter::new(&config.archive_dir(), config.archive_segment);
        let head = cursor.historical_synced_up_to.max(cursor.realtime_synced_up_to).max(cursor.last_synced_block);
        if head > 0 { archive.resume(head + 1).ok(); }
        log::info!("fetcher init: {} stored addrs, {} new_addrs, filter={}, archive_seg={}, rpc_urls={}", addr_store.count(), new_addrs.len(), filter.is_some(), config.archive_segment, rpc_urls.len());
        Ok(Self {
            rpc: RpcPool::new(rpc_urls.to_vec(), config.rpc_timeout_secs),
            config, cursor, filter, addr_store, archive, new_addrs,
            shutdown: Arc::new(AtomicBool::new(false)),
            start_time: Instant::now(), last_stats: Instant::now(),
            last_cursor_save: 0, session_blocks: 0, session_addrs: 0,
        })
    }

    pub fn run(&mut self) -> Result<()> {
        self.setup_ctrlc();
        // 兼容旧游标: 若未设双游标则用 last_synced_block 初始化
        if self.cursor.realtime_synced_up_to == 0 && self.cursor.historical_synced_up_to == 0 && self.cursor.last_synced_block > 0 {
            self.cursor.historical_synced_up_to = self.cursor.last_synced_block;
            self.cursor.realtime_synced_up_to = self.cursor.last_synced_block;
        }
        let batch_size = self.config.rpc_batch_size;
        let latest = self.rpc.get_latest_block_number()?;
        if self.cursor.end_block < latest { self.cursor.end_block = latest; }
        // 从创世块 0 开始：首次同步(未同步过任何块)时从 0 拉取，否则从 historical_synced_up_to+1
        let mut block = if self.cursor.historical_synced_up_to == 0 && self.cursor.total_addresses == 0 {
            0
        } else {
            self.cursor.historical_synced_up_to + 1
        };
        println!("Fetcher started | from_block={} historical_up_to={} realtime_up_to={} latest={} rpc_urls={} batch={} poll_secs={}",
            block, self.cursor.historical_synced_up_to, self.cursor.realtime_synced_up_to, latest, self.rpc.url_count(), batch_size, self.config.poll_interval_secs);

        // 阶段1: 历史追赶 — 从 block(0 或断点) 拉到 latest
        while block <= self.cursor.end_block && !self.shutdown.load(Ordering::Relaxed) {
            let batch_end = (block + batch_size as u64).min(self.cursor.end_block + 1);
            let batch_count = (batch_end - block) as usize;
            let blocks = self.rpc.get_blocks_with_txs(block, batch_count)
                .with_context(|| format!("RPC batch at block {}", block))?;
            for (bn, block_json) in &blocks {
                let raw_addrs = extract_addresses_from_block(block_json);
                let unique = self.dedup_addresses(&raw_addrs);
                self.archive.write_block(&BlockAddresses { block_number: *bn, addresses: raw_addrs })?;
                if !unique.is_empty() {
                    self.addr_store.append(&unique)?;
                    self.new_addrs.extend_from_slice(&unique);
                    self.cursor.total_addresses += unique.len() as u64;
                    self.cursor.new_addrs_since_last_export += unique.len() as u64;
                    self.session_addrs += unique.len() as u64;
                }
                self.cursor.last_synced_block = *bn;
                self.cursor.historical_synced_up_to = *bn;
                self.cursor.realtime_synced_up_to = *bn;
                self.session_blocks += 1;
            }
            block = batch_end;
            if self.session_blocks > 0 && self.session_blocks.is_multiple_of(FILTER_REBUILD_INTERVAL) {
                self.archive.flush()?;
                self.rebuild_filter()?;
            }
            self.maybe_save_state()?;
            self.maybe_print_stats();
            if self.session_blocks.is_multiple_of(1000) {
                if let Ok(lat) = self.rpc.get_latest_block_number() {
                    if lat > self.cursor.end_block { self.cursor.end_block = lat; }
                }
            }
        }
        self.save_state()?;
        self.archive.flush()?;
        if !self.new_addrs.is_empty() { self.rebuild_filter()?; }
        self.print_stats_line();
        if self.config.poll_interval_secs == 0 {
            println!("\nFetcher stopped (no poll). historical={} realtime={} addrs={}", self.cursor.historical_synced_up_to, self.cursor.realtime_synced_up_to, self.cursor.total_addresses);
            return Ok(());
        }
        // 阶段2: 轮询最新块
        println!("\nFetcher entering poll loop (interval {}s)...", self.config.poll_interval_secs);
        while !self.shutdown.load(Ordering::Relaxed) {
            let latest = match self.rpc.get_latest_block_number() {
                Ok(l) => l,
                Err(e) => { log::warn!("poll get_latest failed: {}", e); std::thread::sleep(std::time::Duration::from_secs(self.config.poll_interval_secs)); continue; }
            };
            let from = self.cursor.realtime_synced_up_to + 1;
            if from <= latest {
                let count = ((latest - from + 1) as usize).min(batch_size * 10);
                match self.rpc.get_blocks_with_txs(from, count) {
                    Ok(blocks) => {
                        for (bn, block_json) in &blocks {
                            let raw_addrs = extract_addresses_from_block(block_json);
                            let unique = self.dedup_addresses(&raw_addrs);
                            self.archive.write_block(&BlockAddresses { block_number: *bn, addresses: raw_addrs })?;
                            if !unique.is_empty() {
                                self.addr_store.append(&unique)?;
                                self.new_addrs.extend_from_slice(&unique);
                                self.cursor.total_addresses += unique.len() as u64;
                                self.cursor.new_addrs_since_last_export += unique.len() as u64;
                                self.session_addrs += unique.len() as u64;
                            }
                            self.cursor.last_synced_block = *bn;
                            self.cursor.realtime_synced_up_to = *bn;
                            self.session_blocks += 1;
                        }
                        self.maybe_save_state()?;
                        if self.session_blocks.is_multiple_of(FILTER_REBUILD_INTERVAL) {
                            self.archive.flush()?;
                            self.rebuild_filter()?;
                        }
                    }
                    Err(e) => log::warn!("poll get_blocks failed: {}", e),
                }
            }
            self.print_stats_line();
            std::thread::sleep(std::time::Duration::from_secs(self.config.poll_interval_secs));
        }
        self.save_state()?;
        self.archive.flush()?;
        if !self.new_addrs.is_empty() { self.rebuild_filter()?; }
        println!("\nFetcher stopped. historical={} realtime={} addrs={}", self.cursor.historical_synced_up_to, self.cursor.realtime_synced_up_to, self.cursor.total_addresses);
        Ok(())
    }

    fn dedup_addresses(&self, addrs: &[Address]) -> Vec<Address> {
        let mut unique = Vec::new();
        for addr in addrs {
            let fp = filter::addr_to_u64(addr);
            let known = self.filter.as_ref().is_some_and(|f| f.contains(&fp));
            if !known { unique.push(*addr); }
        }
        unique
    }

    fn rebuild_filter(&mut self) -> Result<()> {
        let t = Instant::now();
        println!("\nRebuilding BinaryFuse16 filter ({} total addrs)...", self.addr_store.count());
        let mut fps = self.addr_store.read_all_fingerprints()?;
        fps.sort_unstable();
        fps.dedup();
        let f = filter::build_fuse16(&fps)?;
        filter::save_fuse16(&f, &self.config.fetch_filter_path())?;
        save_new_addrs(&self.new_addrs, &self.config.new_addrs_path())?;
        let old_new = self.new_addrs.len();
        self.new_addrs.clear();
        self.cursor.filter_version += 1;
        self.cursor.new_addrs_since_last_export = 0;
        self.filter = Some(f);
        println!("  filter rebuilt in {:.1}s ({} unique fps, exported {} new addrs)", t.elapsed().as_secs_f64(), fps.len(), old_new);
        Ok(())
    }

    fn setup_ctrlc(&self) {
        let flag = self.shutdown.clone();
        let _ = ctrlc::set_handler(move || {
            if flag.load(Ordering::Relaxed) { std::process::exit(1); }
            eprintln!("\nCtrl+C received, finishing current batch...");
            flag.store(true, Ordering::Relaxed);
        });
    }

    fn save_state(&mut self) -> Result<()> {
        self.cursor.last_updated = Utc::now();
        cursor::save_cursor(&self.cursor, &self.config.fetch_cursor_path())?;
        save_new_addrs(&self.new_addrs, &self.config.new_addrs_path())?;
        self.last_cursor_save = self.session_blocks;
        Ok(())
    }

    fn maybe_save_state(&mut self) -> Result<()> {
        if self.session_blocks - self.last_cursor_save >= CURSOR_SAVE_INTERVAL { self.save_state()?; }
        Ok(())
    }

    fn maybe_print_stats(&mut self) {
        if self.last_stats.elapsed().as_secs() >= STATS_INTERVAL_SECS {
            self.print_stats_line();
            self.last_stats = Instant::now();
        }
    }

    fn print_stats_line(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64().max(0.001);
        let bps = self.session_blocks as f64 / elapsed;
        let pct = if self.cursor.end_block > 0 { self.cursor.realtime_synced_up_to as f64 / self.cursor.end_block as f64 * 100.0 } else { 0.0 };
        print!("\r  [{:.0}s] hist={} realtime={} ({:.1}%) {:.0}blk/s addrs={} new_buf={}        ",
            elapsed, self.cursor.historical_synced_up_to, self.cursor.realtime_synced_up_to, pct, bps, self.cursor.total_addresses, self.new_addrs.len());
        let _ = std::io::stdout().flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("birdhash_fetch_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn address_store_append_and_read() {
        let dir = test_dir("store");
        let path = dir.join("all_addrs.bin");
        let mut store = AddressStore::open(&path).unwrap();
        assert_eq!(store.count(), 0);
        let a1: Address = [1u8; 20];
        let a2: Address = [2u8; 20];
        store.append(&[a1, a2]).unwrap();
        assert_eq!(store.count(), 2);
        let fps = store.read_all_fingerprints().unwrap();
        assert_eq!(fps.len(), 2);
        assert_eq!(fps[0], filter::addr_to_u64(&a1));
        assert_eq!(fps[1], filter::addr_to_u64(&a2));
        let store2 = AddressStore::open(&path).unwrap();
        assert_eq!(store2.count(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn new_addrs_round_trip() {
        let dir = test_dir("newaddrs");
        let path = dir.join("new_addrs.bin");
        let addrs: Vec<Address> = (0..100u8).map(|i| { let mut a = [0u8; 20]; a[0] = i; a }).collect();
        save_new_addrs(&addrs, &path).unwrap();
        let loaded = load_new_addrs(&path).unwrap();
        assert_eq!(addrs, loaded);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn parse_hex_addr_valid() {
        let addr = parse_hex_addr("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf").unwrap();
        assert_eq!(hex::encode(addr), "7e5f4552091a69125d5dfcb7b8c2659029395bdf");
    }

    #[test]
    fn parse_hex_addr_no_prefix() {
        let addr = parse_hex_addr("7E5F4552091A69125d5DfCb7b8C2659029395Bdf").unwrap();
        assert_eq!(hex::encode(addr), "7e5f4552091a69125d5dfcb7b8c2659029395bdf");
    }

    #[test]
    fn parse_hex_addr_invalid() {
        assert!(parse_hex_addr("0x1234").is_none());
        assert!(parse_hex_addr("").is_none());
        assert!(parse_hex_addr("0xGGGG").is_none());
    }

    #[test]
    fn extract_addresses_from_block_json() {
        let block = serde_json::json!({
            "number": "0x1",
            "miner": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "transactions": [
                {"from": "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf", "to": "0x2B5AD5c4795c026514f8317c7a215E218DcCD6cF"},
                {"from": "0x6813Eb9362372EEF6200f3b1dbC3f819671cBA69", "to": null}
            ]
        });
        let addrs = extract_addresses_from_block(&block);
        // miner + from1 + to1 + from2 + CREATE(from2, nonce=0)
        assert_eq!(addrs.len(), 5);
        assert_eq!(hex::encode(addrs[0]), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let from2: Address = parse_hex_addr("0x6813Eb9362372EEF6200f3b1dbC3f819671cBA69").unwrap();
        assert!(addrs.contains(&from2));
        assert!(addrs.contains(&create_address(&from2, 0)));
    }

    #[test]
    fn filter_rebuild_from_store() {
        let dir = test_dir("rebuild");
        let path = dir.join("all_addrs.bin");
        let filter_path = dir.join("filter_fetch.bin");
        let mut store = AddressStore::open(&path).unwrap();
        let addrs: Vec<Address> = (0u16..1000).flat_map(|i| {
            (0u8..5).map(move |j| { let mut a = [0u8; 20]; a[0] = (i & 0xFF) as u8; a[1] = (i >> 8) as u8; a[2] = j; a })
        }).collect();
        store.append(&addrs).unwrap();
        let mut fps = store.read_all_fingerprints().unwrap();
        fps.sort_unstable();
        fps.dedup();
        let f = filter::build_fuse16(&fps).unwrap();
        filter::save_fuse16(&f, &filter_path).unwrap();
        let f2 = filter::load_fuse16(&filter_path).unwrap();
        for a in &addrs { assert!(f2.contains(&filter::addr_to_u64(a))); }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn extract_contract_creation_address() {
        let block = serde_json::json!({
            "number": "0x2",
            "miner": "0x0000000000000000000000000000000000000000",
            "transactions": [
                {"from": "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf", "to": null, "creates": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
            ]
        });
        let addrs = extract_addresses_from_block(&block);
        assert_eq!(addrs.len(), 3); // miner + from + creates
        assert_eq!(hex::encode(addrs[2]), "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    }

    #[test]
    fn extract_withdrawals_and_author() {
        let block = serde_json::json!({
            "number": "0x3",
            "miner": "0x0000000000000000000000000000000000000001",
            "author": "0x0000000000000000000000000000000000000001",
            "withdrawals": [
                {"address": "0xcccccccccccccccccccccccccccccccccccccccc", "amount": "0x1"},
                {"address": "0xdddddddddddddddddddddddddddddddddddddddd", "amount": "0x2"}
            ],
            "transactions": []
        });
        let addrs = extract_addresses_from_block(&block);
        assert!(addrs.iter().any(|a| hex::encode(a) == "cccccccccccccccccccccccccccccccccccccccc"));
        assert!(addrs.iter().any(|a| hex::encode(a) == "dddddddddddddddddddddddddddddddddddddddd"));
        assert!(addrs.iter().any(|a| hex::encode(a) == "0000000000000000000000000000000000000001"));
    }

    #[test]
    fn create_address_deterministic() {
        let mut sender = [0u8; 20];
        sender[19] = 1;
        let a0 = create_address(&sender, 0);
        let a1 = create_address(&sender, 1);
        assert_ne!(a0, a1);
        let a0_again = create_address(&sender, 0);
        assert_eq!(a0, a0_again);
    }
}
