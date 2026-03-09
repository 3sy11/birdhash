//! Fetcher: 仅拉取原始块数据。按区间 [start_block, end_block] 写入 {start}-{end}/blocks.jsonl + checkpoint.json。
//! 供下游（碰撞器）读取 blocks.jsonl 解析地址。AddressStore/load_new_addrs 保留供 Collider/Scanner 消费已有数据。

use crate::filter;
use crate::keygen::{Address, ADDR_LEN};
use anyhow::{Context, Result};
use chrono::Utc;
use std::cell::RefCell;
use std::collections::HashSet;
use std::io::{BufRead, Write};
use std::path::Path;
use tiny_keccak::{Hasher, Keccak};

const CHECKPOINT_SAVE_INTERVAL: u64 = 50;

thread_local!(static FETCH_OUTPUT_PREFIX: RefCell<String> = RefCell::new(String::new()));
pub fn set_fetch_output_prefix(s: &str) { FETCH_OUTPUT_PREFIX.with(|c| *c.borrow_mut() = s.to_string()); }
fn fetch_prefix() -> String { FETCH_OUTPUT_PREFIX.with(|c| c.borrow().clone()) }

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
        if let Some(arr) = resp.as_array() {
            let mut results = Vec::with_capacity(count);
            for item in arr {
                let block = &item["result"];
                if block.is_null() { continue; }
                let bn_hex = block["number"].as_str().unwrap_or("0x0");
                let bn = u64::from_str_radix(bn_hex.trim_start_matches("0x"), 16).unwrap_or(0);
                results.push((bn, block.clone()));
            }
            results.sort_by_key(|&(bn, _)| bn);
            return Ok(results);
        }
        // 部分 RPC 不支持 batch 或返回单对象/error，降级为逐块请求
        let mut results = Vec::with_capacity(count);
        for bn in start..(start + count as u64) {
            let one = self.call_with_retry(&serde_json::json!({"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[format!("0x{:x}", bn), true],"id": bn}))?;
            let block = &one["result"];
            if block.is_null() { continue; }
            let n = block["number"].as_str().and_then(|h| u64::from_str_radix(h.trim_start_matches("0x"), 16).ok()).unwrap_or(bn);
            results.push((n, block.clone()));
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
        let mut last_err: Option<anyhow::Error> = None;
        for _ in 0..n {
            let url = &self.urls[self.current];
            let rpc = EthRpc::new(url, 0, 0, self.timeout_secs);
            match f(&rpc) {
                Ok(t) => return Ok(t),
                Err(e) => {
                    last_err = Some(e);
                    log::warn!("RPC[{}] {} failed (timeout/err): {}, try next", self.current, url, last_err.as_ref().unwrap());
                    self.current = (self.current + 1) % n;
                }
            }
        }
        let last_msg = last_err.as_ref().map(|e| e.to_string().replace('\n', " ")).unwrap_or_else(|| "unknown".into());
        eprintln!("{}Last RPC error: {}", fetch_prefix(), last_msg);
        Err(anyhow::anyhow!("all {} RPC URLs failed: {}", n, last_msg))
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

pub(crate) fn parse_hex_addr(s: &str) -> Option<Address> {
    let s = s.trim_start_matches("0x");
    if s.len() != 40 { return None; }
    let bytes = hex::decode(s).ok()?;
    if bytes.len() != ADDR_LEN { return None; }
    let mut addr = [0u8; ADDR_LEN];
    addr.copy_from_slice(&bytes);
    Some(addr)
}

// ── 区间拉取：仅写 blocks.jsonl + checkpoint.json ──

#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct FetchRangeCheckpoint {
    pub start_block: u64,
    pub end_block: u64,
    pub last_fetched_block: u64,
    pub status: String,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub updated_at: chrono::DateTime<Utc>,
}

const SEGMENT_SIZE: u64 = 100_000;
const CHUNK_SIZE: u64 = 1_000;
const CHUNK_COUNT: u32 = 100;
const REBUILD_FILTER_EVERY_N_BLOCKS: u64 = 100;

fn seg_start(block: u64) -> u64 {
    (block / SEGMENT_SIZE) * SEGMENT_SIZE
}

fn seg_dir_name(seg_start: u64) -> String {
    format!("{}-{}", seg_start, seg_start + SEGMENT_SIZE - 1)
}

/// 从 checkpoint 或 blocks.jsonl 行数得到该区间已拉到的最大块号。若该段在 "latest" 目录中则从 latest 读取。
fn segment_last_fetched(root: &Path, seg_start: u64) -> u64 {
    let name = seg_dir_name(seg_start);
    let range_dir = root.join(&name);
    let checkpoint_path = range_dir.join("checkpoint.json");
    if checkpoint_path.exists() {
        if let Ok(data) = std::fs::read_to_string(&checkpoint_path) {
            if let Ok(cp) = serde_json::from_str::<FetchRangeCheckpoint>(&data) {
                if cp.start_block == seg_start { return cp.last_fetched_block; }
            }
        }
    }
    let blocks_path = range_dir.join("blocks.jsonl");
    if blocks_path.exists() {
        if let Ok(f) = std::fs::File::open(&blocks_path) {
            let line_count = std::io::BufReader::new(f).lines().count();
            if line_count > 0 { return seg_start.saturating_add(line_count as u64).saturating_sub(1); }
        }
    }
    let latest_dir = root.join("latest");
    if latest_dir.join("checkpoint.json").exists() {
        if let Ok(data) = std::fs::read_to_string(latest_dir.join("checkpoint.json")) {
            if let Ok(cp) = serde_json::from_str::<FetchRangeCheckpoint>(&data) {
                if cp.start_block == seg_start { return cp.last_fetched_block; }
            }
        }
    }
    let mut total_lines: u64 = 0;
    for i in 0..CHUNK_COUNT {
        let p = latest_dir.join(format!("chunk_{:03}.jsonl", i));
        if !p.exists() { break; }
        if let Ok(f) = std::fs::File::open(&p) {
            total_lines += std::io::BufReader::new(f).lines().count() as u64;
        }
    }
    if total_lines > 0 { return seg_start.saturating_add(total_lines).saturating_sub(1); }
    seg_start.saturating_sub(1)
}

/// 确保 "latest" 目录对应 seg_start：若 latest 已满(10万块)则归档为块范围目录并新建 latest。返回 (latest_dir, checkpoint, ck_path)，调用方自行打开 blocks.jsonl。
fn ensure_latest_ready_for_segment(root: &Path, seg_start: u64) -> Result<(std::path::PathBuf, FetchRangeCheckpoint, std::path::PathBuf)> {
    let latest_dir = root.join("latest");
    if !latest_dir.exists() {
        std::fs::create_dir_all(&latest_dir)?;
        let cp = FetchRangeCheckpoint {
            start_block: seg_start,
            end_block: seg_start + SEGMENT_SIZE - 1,
            last_fetched_block: seg_start.saturating_sub(1),
            status: "running".into(),
            updated_at: Utc::now(),
        };
        let ck_path = latest_dir.join("checkpoint.json");
        save_checkpoint_static(&ck_path, &cp)?;
        return Ok((latest_dir, cp, ck_path));
    }
    let ck_path = latest_dir.join("checkpoint.json");
    let mut cp: FetchRangeCheckpoint = if ck_path.exists() {
        let data = std::fs::read_to_string(&ck_path)?;
        serde_json::from_str(&data).unwrap_or(FetchRangeCheckpoint {
            start_block: seg_start,
            end_block: seg_start + SEGMENT_SIZE - 1,
            last_fetched_block: seg_start.saturating_sub(1),
            status: "running".into(),
            updated_at: Utc::now(),
        })
    } else {
        FetchRangeCheckpoint {
            start_block: seg_start,
            end_block: seg_start + SEGMENT_SIZE - 1,
            last_fetched_block: seg_start.saturating_sub(1),
            status: "running".into(),
            updated_at: Utc::now(),
        }
    };
    if cp.start_block == seg_start {
        let mut total_lines: u64 = 0;
        for i in 0..CHUNK_COUNT {
            let p = latest_dir.join(format!("chunk_{:03}.jsonl", i));
            if !p.exists() { break; }
            total_lines += std::io::BufReader::new(std::fs::File::open(&p)?).lines().count() as u64;
        }
        if total_lines > 0 { cp.last_fetched_block = seg_start.saturating_add(total_lines).saturating_sub(1); }
        return Ok((latest_dir, cp, ck_path));
    }
    if cp.start_block < seg_start {
        let range_name = seg_dir_name(cp.start_block);
        let range_dir = root.join(&range_name);
        let _ = std::fs::rename(&latest_dir, &range_dir);
        std::fs::create_dir_all(&latest_dir)?;
        cp = FetchRangeCheckpoint {
            start_block: seg_start,
            end_block: seg_start + SEGMENT_SIZE - 1,
            last_fetched_block: seg_start.saturating_sub(1),
            status: "running".into(),
            updated_at: Utc::now(),
        };
        save_checkpoint_static(&ck_path, &cp)?;
        return Ok((latest_dir, cp, ck_path));
    }
    Ok((latest_dir, cp, ck_path))
}

fn save_checkpoint_static(path: &Path, cp: &FetchRangeCheckpoint) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_string_pretty(cp).unwrap())?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Progress: (batch_id, current, end, written, blk_s); None = single process
pub type FetchProgressSender = Option<(u64, std::sync::mpsc::Sender<(u64, u64, u64, u64, f64)>)>;

/// 拉取 [start_block, end_block]：启动时取最新高度；最新段按 1000 块/文件写 latest/chunk_*.jsonl；非最新段写范围目录 blocks.jsonl。
/// filter_out_dir: 仅拉取最新段时用，每 100 块重建当前 chunk 小 filter，满 10 万合并归档。
pub fn run_fetch_range(
    output_root: &Path,
    start_block: u64,
    end_block: u64,
    rpc_urls: &[String],
    timeout_secs: u64,
    batch_size: usize,
    output_prefix: Option<&str>,
    progress_tx: FetchProgressSender,
    filter_out_dir: Option<&Path>,
    poll_mode: bool,
) -> Result<()> {
    let p = output_prefix.unwrap_or("");
    let use_progress = progress_tx.is_some();
    if !p.is_empty() { set_fetch_output_prefix(p); } else { set_fetch_output_prefix(""); }
    anyhow::ensure!(!rpc_urls.is_empty(), "no RPC URLs");
    anyhow::ensure!(start_block <= end_block, "start_block > end_block");
    std::fs::create_dir_all(output_root)?;
    let mut pool = RpcPool::new(rpc_urls.to_vec(), timeout_secs);
    let latest = pool.get_latest_block_number()?;
    let end_block = end_block.min(latest);
    let batch_id = end_block / SEGMENT_SIZE + 1;
    if !use_progress && !poll_mode { print!("{}", p); println!("Fetch {}..{} (latest={}), 10w-step dirs under {}", start_block, end_block, latest, output_root.display()); }
    let mut next = end_block.saturating_add(1);
    let seg_min = seg_start(start_block);
    let seg_max = seg_start(end_block);
    let mut seg = seg_min;
    while seg <= seg_max {
        let last = segment_last_fetched(output_root, seg);
        let first_missing = last.saturating_add(1);
        let first_to_fetch = first_missing.max(start_block);
        if first_to_fetch <= end_block { next = next.min(first_to_fetch); }
        seg = seg.saturating_add(SEGMENT_SIZE);
    }
    if next > end_block {
        if !use_progress && !poll_mode { print!("{}", p); println!("Range {}..{} already present in files, skip fetch.", start_block, end_block); }
        if poll_mode { print!("\r  batch={} height={} blocks=0   ", batch_id, end_block); let _ = std::io::stdout().flush(); }
        set_fetch_output_prefix(""); return Ok(());
    }
    if !use_progress && !poll_mode && next > start_block { print!("{}", p); println!("Resume from block {} (already have {}..{})", next, start_block, next.saturating_sub(1)); }
    let already_have = next.saturating_sub(start_block);
    let mut total_written: u64 = already_have;
    let batch_size = batch_size.max(1);
    type SegKey = u64;
    let seg_head = seg_start(end_block);
    let mut segments: std::collections::HashMap<SegKey, (std::fs::File, FetchRangeCheckpoint, std::path::PathBuf)> = std::collections::HashMap::new();
    let open_seg = |seg_s: u64, root: &Path, end_blk: u64| -> Result<(std::fs::File, FetchRangeCheckpoint, std::path::PathBuf)> {
        let is_latest_seg = seg_s == seg_start(end_blk);
        if is_latest_seg {
            let (range_dir, cp, checkpoint_path) = ensure_latest_ready_for_segment(root, seg_s)?;
            let chunk0 = range_dir.join("chunk_000.jsonl");
            let f = std::fs::OpenOptions::new().create(true).append(true).open(&chunk0)?;
            return Ok((f, cp, checkpoint_path));
        }
        let name = seg_dir_name(seg_s);
        let range_dir = root.join(&name);
        std::fs::create_dir_all(&range_dir)?;
        let blocks_path = range_dir.join("blocks.jsonl");
        let checkpoint_path = range_dir.join("checkpoint.json");
        let mut cp = FetchRangeCheckpoint {
            start_block: seg_s,
            end_block: seg_s + SEGMENT_SIZE - 1,
            last_fetched_block: seg_s.saturating_sub(1),
            status: "running".into(),
            updated_at: Utc::now(),
        };
        if checkpoint_path.exists() {
            if let Ok(data) = std::fs::read_to_string(&checkpoint_path) {
                if let Ok(loaded) = serde_json::from_str::<FetchRangeCheckpoint>(&data) {
                    if loaded.start_block == seg_s { cp.last_fetched_block = loaded.last_fetched_block; }
                }
            }
        }
        if cp.last_fetched_block < seg_s && blocks_path.exists() {
            let line_count = std::io::BufReader::new(std::fs::File::open(&blocks_path)?).lines().count();
            if line_count > 0 { cp.last_fetched_block = seg_s.saturating_add(line_count as u64).saturating_sub(1); }
        }
        let f = std::fs::OpenOptions::new().create(true).append(true).open(&blocks_path)?;
        Ok((f, cp, checkpoint_path))
    };
    let save_checkpoint = |path: &Path, cp: &FetchRangeCheckpoint| -> Result<()> {
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_string_pretty(cp).unwrap())?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    };
    let start_time = std::time::Instant::now();
    let mut current_block: u64 = next.saturating_sub(1);
    if let Some((bid, ref tx)) = progress_tx { let _ = tx.send((bid, current_block, end_block, total_written, 0.0)); }
    while next <= end_block {
        let count = ((end_block - next + 1) as usize).min(batch_size);
        let blocks = pool.get_blocks_with_txs(next, count).with_context(|| format!("RPC block {}", next))?;
        for (_bn, block_json) in &blocks {
            let bn = block_json["number"].as_str().and_then(|h| u64::from_str_radix(h.trim_start_matches("0x"), 16).ok()).unwrap_or(0);
            let seg = seg_start(bn);
            let (file, cp, ck_path) = segments.entry(seg).or_insert_with(|| open_seg(seg, output_root, end_block).expect("open_seg"));
            if bn <= cp.last_fetched_block { continue; }
            let line = serde_json::to_string(block_json).context("block to json")?;
            if seg == seg_head {
                let latest_dir = ck_path.parent().unwrap();
                let chunk_idx = ((bn - cp.start_block) / CHUNK_SIZE) as u32;
                if chunk_idx == 0 { writeln!(file, "{}", line)?; } else {
                    let chunk_path = latest_dir.join(format!("chunk_{:03}.jsonl", chunk_idx));
                    let mut cf = std::fs::OpenOptions::new().create(true).append(true).open(&chunk_path)?;
                    writeln!(cf, "{}", line)?;
                }
            } else { writeln!(file, "{}", line)?; }
            cp.last_fetched_block = bn;
            cp.updated_at = Utc::now();
            total_written += 1;
            current_block = bn;
            if seg == seg_head {
                save_checkpoint(ck_path, cp)?;
                if let Some(ref fdir) = filter_out_dir {
                    let latest_dir = ck_path.parent().unwrap();
                    let batch_id = cp.start_block / SEGMENT_SIZE + 1;
                    let offset_in_seg = cp.last_fetched_block - cp.start_block;
                    let offset_in_chunk = offset_in_seg % CHUNK_SIZE;
                    if (offset_in_chunk + 1) % REBUILD_FILTER_EVERY_N_BLOCKS == 0 {
                        let chunk_idx = offset_in_seg / CHUNK_SIZE;
                        let chunk_path = latest_dir.join(format!("chunk_{:03}.jsonl", chunk_idx));
                        if chunk_path.exists() {
                            let filter_path = fdir.join(format!("filter.{}.{:02}.bin", batch_id, chunk_idx));
                            let _ = build_small_filter_for_chunk(&chunk_path, &filter_path);
                        }
                    }
                    if cp.last_fetched_block >= cp.start_block + SEGMENT_SIZE - 1 {
                        merge_and_archive_latest(output_root, cp.start_block, fdir)?;
                        segments.remove(&seg_head);
                        break;
                    }
                }
            }
            if seg != seg_head && total_written % CHECKPOINT_SAVE_INTERVAL == 0 { save_checkpoint(ck_path, cp)?; file.flush()?; }
            else if seg == seg_head { file.flush()?; }
        }
        if blocks.is_empty() { break; }
        next = blocks.last().map(|(b, _)| *b).unwrap_or(next).saturating_add(1);
        let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
        let new_written = total_written.saturating_sub(already_have);
        let blk_s = new_written as f64 / elapsed;
        if let Some((bid, ref tx)) = progress_tx { let _ = tx.send((bid, current_block, end_block, total_written, blk_s)); }
        else if poll_mode { print!("\r  batch={} height={} blocks={}   ", batch_id, current_block, total_written); let _ = std::io::stdout().flush(); }
        else { print!("{}\r  block={} | {}..{} written={} ({:.0} blk/s)   ", p, current_block, start_block, end_block, total_written, blk_s); let _ = std::io::stdout().flush(); }
    }
    for (_seg, (ref mut file, ref mut cp, ref ck_path)) in segments.iter_mut() {
        cp.status = "done".into();
        cp.updated_at = Utc::now();
        file.flush()?;
        save_checkpoint(ck_path, cp)?;
    }
    if poll_mode { print!("\r  batch={} height={} blocks={}   ", batch_id, end_block, total_written); let _ = std::io::stdout().flush(); }
    else if !use_progress { print!("{}", p); println!("\nDone. {} blocks written, {} segments in {:.1}s", total_written, segments.len(), start_time.elapsed().as_secs_f64()); }
    set_fetch_output_prefix("");
    Ok(())
}

/// 扫描 range_root 下所有有数据的批次：范围目录 blocks.jsonl + latest（若有）对应的 batch_id
pub fn list_batches_in_ranges(range_root: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    let entries = std::fs::read_dir(range_root).with_context(|| format!("read_dir {}", range_root.display()))?;
    for e in entries {
        let e = e?;
        let name = e.file_name().to_string_lossy().to_string();
        if name == "latest" {
            let ck = range_root.join("latest").join("checkpoint.json");
            if ck.exists() {
                let cp: FetchRangeCheckpoint = serde_json::from_str(&std::fs::read_to_string(&ck)?)?;
                let batch_id = cp.start_block / SEGMENT_SIZE + 1;
                ids.push(batch_id);
            }
            continue;
        }
        if let Some((a, b)) = name.split_once('-') {
            if let (Ok(start), Ok(_end)) = (a.parse::<u64>(), b.parse::<u64>()) {
                if start % SEGMENT_SIZE == 0 && range_root.join(&name).join("blocks.jsonl").exists() {
                    ids.push(start / SEGMENT_SIZE + 1);
                }
            }
        }
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

/// 从指定批次的块数据收集地址指纹（范围目录 blocks.jsonl 或 latest 下 chunk_*.jsonl）
fn collect_fingerprints_for_batch(range_root: &Path, batch_id: u64) -> Result<HashSet<u64>> {
    let seg_start = (batch_id.saturating_sub(1)) * SEGMENT_SIZE;
    let range_name = seg_dir_name(seg_start);
    let range_blocks = range_root.join(&range_name).join("blocks.jsonl");
    let mut fingerprints = HashSet::new();
    if range_blocks.exists() {
        let f = std::fs::File::open(&range_blocks)?;
        for line in std::io::BufReader::new(f).lines() {
            let line = line?;
            if line.trim().is_empty() { continue; }
            let block: serde_json::Value = serde_json::from_str(&line)?;
            for addr in extract_addresses_from_block(&block) { fingerprints.insert(filter::addr_to_u64(&addr)); }
        }
        return Ok(fingerprints);
    }
    let latest_dir = range_root.join("latest");
    if latest_dir.join("checkpoint.json").exists() {
        let cp: FetchRangeCheckpoint = serde_json::from_str(&std::fs::read_to_string(latest_dir.join("checkpoint.json"))?)?;
        if cp.start_block == seg_start {
            for i in 0..100u32 {
                let chunk = latest_dir.join(format!("chunk_{:03}.jsonl", i));
                if !chunk.exists() { break; }
                let f = std::fs::File::open(&chunk)?;
                for line in std::io::BufReader::new(f).lines() {
                    let line = line?;
                    if line.trim().is_empty() { continue; }
                    let block: serde_json::Value = serde_json::from_str(&line)?;
                    for addr in extract_addresses_from_block(&block) { fingerprints.insert(filter::addr_to_u64(&addr)); }
                }
            }
            return Ok(fingerprints);
        }
    }
    anyhow::bail!("no data for batch {}", batch_id);
}

/// 从已拉取的块数据构建 BinaryFuse16 过滤器。batches=None 时扫描全量批次并逐个构建；batches=Some(ids) 时合并构建一个 filter。输出命名 filter.{min_batch}-{max_batch}.bin，写入 filter_out_dir，并写元信息。
pub fn build_fetch_filter_from_ranges(range_root: &Path, batches: Option<&[u64]>, filter_out_dir: &Path) -> Result<(usize, u64)> {
    std::fs::create_dir_all(filter_out_dir)?;
    let batch_list: Vec<u64> = match batches {
        Some(ids) if !ids.is_empty() => ids.to_vec(),
        _ => list_batches_in_ranges(range_root)?,
    };
    anyhow::ensure!(!batch_list.is_empty(), "no batches to build");
    let mut total_entries: u64 = 0;
    let mut fingerprints = HashSet::new();
    for &batch_id in &batch_list {
        let fp = collect_fingerprints_for_batch(range_root, batch_id)?;
        for k in fp { fingerprints.insert(k); }
    }
    let keys: Vec<u64> = fingerprints.into_iter().collect();
    let entries = keys.len() as u64;
    let min_b = *batch_list.iter().min().unwrap();
    let max_b = *batch_list.iter().max().unwrap();
    let name = format!("filter.{}-{}.bin", min_b, max_b);
    let out_path = filter_out_dir.join(&name);
    let filter = filter::build_fuse16(&keys)?;
    filter::save_fuse16(&filter, &out_path)?;
    let max_block_height = max_b * SEGMENT_SIZE;
    save_fetch_filter_meta(&out_path, &batch_list, max_block_height)?;
    total_entries += entries;
    Ok((1, total_entries))
}

/// 无参全量：对每个批次单独构建 filter.{id}-{id}.bin，返回 (filter_count, total_entries_sum)
pub fn build_fetch_filter_all_batches(range_root: &Path, filter_out_dir: &Path) -> Result<(usize, u64)> {
    std::fs::create_dir_all(filter_out_dir)?;
    let batch_list = list_batches_in_ranges(range_root)?;
    anyhow::ensure!(!batch_list.is_empty(), "no batches found under {}", range_root.display());
    let mut count = 0usize;
    let mut total_entries: u64 = 0;
    for &batch_id in &batch_list {
        let fingerprints = collect_fingerprints_for_batch(range_root, batch_id)?;
        let keys: Vec<u64> = fingerprints.into_iter().collect();
        let entries = keys.len() as u64;
        if keys.is_empty() { continue; }
        let name = format!("filter.{}-{}.bin", batch_id, batch_id);
        let out_path = filter_out_dir.join(&name);
        let filter = filter::build_fuse16(&keys)?;
        filter::save_fuse16(&filter, &out_path)?;
        let max_block_height = batch_id * SEGMENT_SIZE;
        save_fetch_filter_meta(&out_path, &[batch_id], max_block_height)?;
        count += 1;
        total_entries += entries;
        println!("  {} batches=1 entries={}", name, entries);
    }
    Ok((count, total_entries))
}

/// 元信息：已加载批次与过滤器覆盖的块最高高度
#[derive(serde::Serialize, serde::Deserialize)]
pub struct FetchFilterMeta {
    pub loaded_batches: Vec<u64>,
    pub max_block_height: u64,
}

fn fetch_filter_meta_path(filter_path: &Path) -> std::path::PathBuf {
    let stem = filter_path.file_stem().and_then(|s| s.to_str()).unwrap_or("filter_fetch");
    filter_path.parent().unwrap_or_else(|| Path::new(".")).join(format!("{}_meta.json", stem))
}

/// 写入与 filter 同目录的 _meta.json，标识已加载批次和最高块高
pub fn save_fetch_filter_meta(filter_path: &Path, batches: &[u64], max_block_height: u64) -> Result<()> {
    let meta = FetchFilterMeta { loaded_batches: batches.to_vec(), max_block_height };
    let path = fetch_filter_meta_path(filter_path);
    if let Some(p) = path.parent() { std::fs::create_dir_all(p)?; }
    std::fs::write(&path, serde_json::to_string_pretty(&meta)?)?;
    Ok(())
}

/// 从单个 chunk 文件构建小过滤器并保存
fn build_small_filter_for_chunk(chunk_path: &Path, filter_out_path: &Path) -> Result<()> {
    let mut fingerprints = HashSet::new();
    let f = std::fs::File::open(chunk_path)?;
    for line in std::io::BufReader::new(f).lines() {
        let line = line?;
        if line.trim().is_empty() { continue; }
        let block: serde_json::Value = serde_json::from_str(&line)?;
        for addr in extract_addresses_from_block(&block) { fingerprints.insert(filter::addr_to_u64(&addr)); }
    }
    let keys: Vec<u64> = fingerprints.into_iter().collect();
    if keys.is_empty() { return Ok(()); }
    let filter = filter::build_fuse16(&keys)?;
    filter::save_fuse16(&filter, filter_out_path)?;
    Ok(())
}

/// 满 10 万：合并 latest 的 100 个 chunk 为 range 目录下一份 blocks.jsonl，构建 filter.{batch_id}-{batch_id}.bin，删小 filter 与 latest，新建空 latest
pub fn merge_and_archive_latest(range_root: &Path, seg_start: u64, filter_out_dir: &Path) -> Result<()> {
    let batch_id = seg_start / SEGMENT_SIZE + 1;
    let range_name = seg_dir_name(seg_start);
    let range_dir = range_root.join(&range_name);
    let latest_dir = range_root.join("latest");
    std::fs::create_dir_all(&range_dir)?;
    let merged = range_dir.join("blocks.jsonl");
    let mut out = std::fs::File::create(&merged)?;
    for i in 0..CHUNK_COUNT {
        let chunk = latest_dir.join(format!("chunk_{:03}.jsonl", i));
        if !chunk.exists() { break; }
        let f = std::fs::File::open(&chunk)?;
        for line in std::io::BufReader::new(f).lines() {
            let line = line?;
            writeln!(out, "{}", line)?;
        }
    }
    drop(out);
    let mut fingerprints = HashSet::new();
    let f = std::fs::File::open(&merged)?;
    for line in std::io::BufReader::new(f).lines() {
        let line = line?;
        if line.trim().is_empty() { continue; }
        let block: serde_json::Value = serde_json::from_str(&line)?;
        for addr in extract_addresses_from_block(&block) { fingerprints.insert(filter::addr_to_u64(&addr)); }
    }
    let keys: Vec<u64> = fingerprints.into_iter().collect();
    let filter_path = filter_out_dir.join(format!("filter.{}-{}.bin", batch_id, batch_id));
    let filter = filter::build_fuse16(&keys)?;
    filter::save_fuse16(&filter, &filter_path)?;
    save_fetch_filter_meta(&filter_path, &[batch_id], batch_id * SEGMENT_SIZE)?;
    for i in 0..CHUNK_COUNT {
        let _ = std::fs::remove_file(filter_out_dir.join(format!("filter.{}.{:02}.bin", batch_id, i)));
    }
    for i in 0..CHUNK_COUNT {
        let _ = std::fs::remove_file(latest_dir.join(format!("chunk_{:03}.jsonl", i)));
    }
    let _ = std::fs::remove_file(latest_dir.join("checkpoint.json"));
    let next_start = seg_start + SEGMENT_SIZE;
    std::fs::create_dir_all(&latest_dir)?;
    let cp = FetchRangeCheckpoint {
        start_block: next_start,
        end_block: next_start + SEGMENT_SIZE - 1,
        last_fetched_block: next_start.saturating_sub(1),
        status: "running".into(),
        updated_at: Utc::now(),
    };
    save_checkpoint_static(&latest_dir.join("checkpoint.json"), &cp)?;
    Ok(())
}

/// 读取过滤器元信息，文件不存在则返回 None
pub fn load_fetch_filter_meta(filter_path: &Path) -> Result<Option<FetchFilterMeta>> {
    let path = fetch_filter_meta_path(filter_path);
    if !path.exists() { return Ok(None); }
    let s = std::fs::read_to_string(&path)?;
    Ok(Some(serde_json::from_str(&s)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use xorf::Filter;

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
