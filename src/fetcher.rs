//! Fetcher: 拉取原始块数据，写 chunk_*.jsonl，只维护检查点 meta。

const ADDR_LEN: usize = 20;
type Address = [u8; ADDR_LEN];
use anyhow::{Context, Result};
use chrono::Utc;
use std::cell::RefCell;
use std::io::{BufRead, Write};
use std::path::Path;
use tiny_keccak::{Hasher, Keccak};

const CHECKPOINT_SAVE_INTERVAL: u64 = 50;

thread_local!(static FETCH_OUTPUT_PREFIX: RefCell<String> = RefCell::new(String::new()));
pub fn set_fetch_output_prefix(s: &str) { FETCH_OUTPUT_PREFIX.with(|c| *c.borrow_mut() = s.to_string()); }
fn fetch_prefix() -> String { FETCH_OUTPUT_PREFIX.with(|c| c.borrow().clone()) }

// ── Ethereum JSON-RPC client with retry ──

pub struct EthRpc {
    url: String,
    agent: ureq::Agent,
    retry_count: u32,
    retry_base_ms: u64,
}

impl EthRpc {
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

// ── RPC 多 URL 降级池 ──

pub struct RpcPool {
    urls: Vec<String>,
    current: usize,
    timeout_secs: u64,
}

impl RpcPool {
    pub fn new(urls: Vec<String>, timeout_secs: u64) -> Self {
        Self { urls, current: 0, timeout_secs }
    }
    fn with_rpc<F, T>(&mut self, f: F) -> Result<T>
    where F: Fn(&EthRpc) -> Result<T> {
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
                    log::warn!("RPC[{}] {} failed: {}, try next", self.current, url, last_err.as_ref().unwrap());
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

/// 拉取单个块并返回 (block_json, 提取的地址列表)
pub fn fetch_one_block(rpc_urls: &[String], block_number: u64, timeout_secs: u64) -> Result<(serde_json::Value, Vec<Address>)> {
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

/// 从区块与交易中提取所有可由私钥/确定性推导的地址
pub fn extract_addresses_from_block(block: &serde_json::Value) -> Vec<Address> {
    let mut addrs = Vec::new();
    if let Some(miner) = block["miner"].as_str() {
        if let Some(a) = parse_hex_addr(miner) { addrs.push(a); }
    }
    if let Some(author) = block["author"].as_str() {
        if let Some(a) = parse_hex_addr(author) { addrs.push(a); }
    }
    if let Some(withdrawals) = block["withdrawals"].as_array() {
        for w in withdrawals {
            if let Some(addr) = w["address"].as_str() {
                if let Some(a) = parse_hex_addr(addr) { addrs.push(a); }
            }
        }
    }
    if let Some(txs) = block["transactions"].as_array() {
        for tx in txs {
            let from = tx["from"].as_str().and_then(parse_hex_addr);
            if let Some(a) = from { addrs.push(a); }
            if let Some(to) = tx["to"].as_str() {
                if let Some(a) = parse_hex_addr(to) { addrs.push(a); }
            } else {
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

// ── 检查点与 Meta ──

#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct FetchRangeCheckpoint {
    pub start_block: u64,
    pub end_block: u64,
    pub last_fetched_block: u64,
    pub status: String,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub updated_at: chrono::DateTime<Utc>,
}

/// 获取器元信息：当前批次检查点，存于 fetcher_dir/meta.json
#[derive(serde::Serialize, serde::Deserialize, Default, Clone)]
pub struct FetchMeta {
    pub version: u32,
    pub current_batch: u64,
    pub current_batch_fetched_through_block: u64,
}

const META_VERSION: u32 = 1;
pub const SEGMENT_SIZE: u64 = 100_000;
const CHUNK_SIZE: u64 = 1_000;
const CHUNK_COUNT: u32 = 100;

fn seg_start(block: u64) -> u64 {
    (block / SEGMENT_SIZE) * SEGMENT_SIZE
}

pub fn seg_dir_name(seg_start: u64) -> String {
    format!("{}-{}", seg_start, seg_start + SEGMENT_SIZE - 1)
}

fn meta_path(range_root: &Path) -> std::path::PathBuf {
    range_root.parent().unwrap_or_else(|| range_root.as_ref()).join("meta.json")
}

#[allow(dead_code)]
pub fn load_meta(range_root: &Path) -> Result<FetchMeta> {
    let p = meta_path(range_root);
    if !p.exists() { return Ok(FetchMeta::default()); }
    let data = std::fs::read_to_string(&p).with_context(|| format!("read {}", p.display()))?;
    let m: FetchMeta = serde_json::from_str(&data).with_context(|| format!("parse {}", p.display()))?;
    Ok(m)
}

fn save_meta(range_root: &Path, meta: &FetchMeta) -> Result<()> {
    let p = meta_path(range_root);
    if let Some(parent) = p.parent() { std::fs::create_dir_all(parent)?; }
    let tmp = unique_tmp_path(&p);
    std::fs::write(&tmp, serde_json::to_string_pretty(meta)?)?;
    std::fs::rename(&tmp, &p)?;
    Ok(())
}

fn unique_tmp_path(path: &Path) -> std::path::PathBuf {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("tmp");
    let tid = format!("{:?}", std::thread::current().id()).replace(['(', ')', ' '], "");
    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    dir.join(format!(".{}.{}.{}.tmp", name, tid, ts))
}

/// 若 meta 不存在或 version<1 则从 ranges 目录推断当前批次并写 meta
fn ensure_meta_upgraded(range_root: &Path) -> Result<FetchMeta> {
    let p = meta_path(range_root);
    let mut meta = if p.exists() {
        let data = std::fs::read_to_string(&p)?;
        serde_json::from_str::<FetchMeta>(&data).unwrap_or_default()
    } else {
        FetchMeta::default()
    };
    if meta.version >= META_VERSION { return Ok(meta); }
    let mut last_done: u64 = 0;
    if let Ok(entries) = std::fs::read_dir(range_root) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if let Some((a, _)) = name.split_once('-') {
                if let Ok(start) = a.parse::<u64>() {
                    if start % SEGMENT_SIZE == 0 && range_root.join(&name).join("blocks.jsonl").exists() {
                        last_done = last_done.max(start / SEGMENT_SIZE + 1);
                    }
                }
            }
        }
    }
    meta.current_batch = last_done.saturating_add(1);
    meta.current_batch_fetched_through_block = if meta.current_batch <= 1 { 0 } else { (meta.current_batch - 1) * SEGMENT_SIZE - 1 };
    meta.version = META_VERSION;
    save_meta(range_root, &meta)?;
    Ok(meta)
}

fn segment_last_fetched(root: &Path, seg_s: u64) -> u64 {
    let name = seg_dir_name(seg_s);
    let range_dir = root.join(&name);
    let blocks_path = range_dir.join("blocks.jsonl");
    if blocks_path.exists() {
        if let Ok(f) = std::fs::File::open(&blocks_path) {
            let line_count = std::io::BufReader::new(f).lines().count();
            if line_count > 0 { return seg_s.saturating_add(line_count as u64).saturating_sub(1); }
        }
    }
    let mut total_lines: u64 = 0;
    for i in 0..CHUNK_COUNT {
        let p = range_dir.join(format!("chunk_{:03}.jsonl", i));
        if !p.exists() { break; }
        if let Ok(f) = std::fs::File::open(&p) {
            total_lines += std::io::BufReader::new(f).lines().count() as u64;
        }
    }
    if total_lines > 0 { return seg_s.saturating_add(total_lines).saturating_sub(1); }
    seg_s.saturating_sub(1)
}

fn block_number_from_json_line(line: &str) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_str(line.trim()).ok()?;
    let bn_hex = v["number"].as_str()?;
    u64::from_str_radix(bn_hex.trim_start_matches("0x"), 16).ok()
}

/// 仅统计 [span_lo, span_hi] 内实际出现的块号（并行多批共享同一 seg 目录时，行数推算会误判）
fn max_block_in_segment_span_parsed(range_root: &Path, seg_s: u64, span_lo: u64, span_hi: u64) -> Option<u64> {
    if span_lo > span_hi { return None; }
    let range_dir = range_root.join(seg_dir_name(seg_s));
    if !range_dir.is_dir() { return None; }
    let mut max_b: Option<u64> = None;
    let blocks_path = range_dir.join("blocks.jsonl");
    if blocks_path.exists() {
        if let Ok(f) = std::fs::File::open(&blocks_path) {
            for line in std::io::BufReader::new(f).lines().flatten() {
                if let Some(bn) = block_number_from_json_line(&line) {
                    if bn >= span_lo && bn <= span_hi { max_b = Some(max_b.map_or(bn, |m| m.max(bn))); }
                }
            }
        }
    }
    for i in 0..CHUNK_COUNT {
        let p = range_dir.join(format!("chunk_{:03}.jsonl", i));
        if !p.exists() { continue; }
        if let Ok(f) = std::fs::File::open(&p) {
            for line in std::io::BufReader::new(f).lines().flatten() {
                if let Some(bn) = block_number_from_json_line(&line) {
                    if bn >= span_lo && bn <= span_hi { max_b = Some(max_b.map_or(bn, |m| m.max(bn))); }
                }
            }
        }
    }
    max_b
}

/// 块号所属的批次号（与 main 中 batch 定义一致：batch 1 = 块 1..=100000）
pub fn batch_id_for_block(block: u64) -> u64 {
    if block == 0 { return 0; }
    (block - 1) / SEGMENT_SIZE + 1
}

/// 扫描 ranges 下各 10 万区间目录，取已写入的最大块号；空目录不计（segment_last_fetched 空时 last&lt;seg_s）
pub fn max_fetched_block_on_disk(range_root: &Path) -> Result<u64> {
    let mut max_b = 0u64;
    let entries = std::fs::read_dir(range_root).with_context(|| format!("read_dir {}", range_root.display()))?;
    for e in entries {
        let e = e?;
        let name = e.file_name().to_string_lossy().to_string();
        if let Some((a, _)) = name.split_once('-') {
            if let Ok(seg_s) = a.parse::<u64>() {
                if seg_s % SEGMENT_SIZE != 0 { continue; }
                let last = segment_last_fetched(range_root, seg_s);
                if last >= seg_s { max_b = max_b.max(last); }
            }
        }
    }
    Ok(max_b)
}

/// 在 [span_lo, span_hi] 内取已落盘的最大块号（按 JSON number 统计，与并行多批共享 seg 一致）
fn max_fetched_in_span(range_root: &Path, span_lo: u64, span_hi: u64) -> Option<u64> {
    if span_lo > span_hi { return None; }
    let mut max_b: Option<u64> = None;
    let mut seg = seg_start(span_lo);
    let end_seg = seg_start(span_hi);
    loop {
        let s_lo = span_lo.max(seg);
        let s_hi = span_hi.min(seg + SEGMENT_SIZE - 1);
        if s_lo <= s_hi {
            if let Some(m) = max_block_in_segment_span_parsed(range_root, seg, s_lo, s_hi) {
                max_b = Some(max_b.map_or(m, |x| x.max(m)));
            }
        }
        if seg >= end_seg { break; }
        seg = seg.saturating_add(SEGMENT_SIZE);
    }
    max_b
}

fn save_checkpoint_static(path: &Path, cp: &FetchRangeCheckpoint) -> Result<()> {
    let tmp = unique_tmp_path(path);
    std::fs::write(&tmp, serde_json::to_string_pretty(cp).unwrap())?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn ensure_range_ready_for_segment(root: &Path, seg_s: u64) -> Result<(std::path::PathBuf, FetchRangeCheckpoint, std::path::PathBuf)> {
    let name = seg_dir_name(seg_s);
    let range_dir = root.join(&name);
    std::fs::create_dir_all(&range_dir)?;
    let cp = FetchRangeCheckpoint {
        start_block: seg_s,
        end_block: seg_s + SEGMENT_SIZE - 1,
        last_fetched_block: seg_s.saturating_sub(1),
        status: "running".into(),
        updated_at: Utc::now(),
    };
    let ck_path = range_dir.join("checkpoint.json");
    Ok((range_dir, cp, ck_path))
}

/// Progress: (batch_id, current, end, written, blk_s)
pub type FetchProgressSender = Option<(u64, std::sync::mpsc::Sender<(u64, u64, u64, u64, f64)>)>;

/// 拉取 [start_block, end_block]：只写 chunk_*.jsonl 并更新检查点 meta，不做合并与 BF 生成。
pub fn run_fetch_range(
    output_root: &Path,
    start_block: u64,
    end_block: u64,
    rpc_urls: &[String],
    timeout_secs: u64,
    batch_size: usize,
    output_prefix: Option<&str>,
    progress_tx: FetchProgressSender,
    poll_mode: bool,
) -> Result<()> {
    let p = output_prefix.unwrap_or("");
    let use_progress = progress_tx.is_some();
    if !p.is_empty() { set_fetch_output_prefix(p); } else { set_fetch_output_prefix(""); }
    anyhow::ensure!(!rpc_urls.is_empty(), "no RPC URLs");
    anyhow::ensure!(start_block <= end_block, "start_block > end_block");
    std::fs::create_dir_all(output_root)?;
    let mut meta = ensure_meta_upgraded(output_root)?;
    let disk_through = max_fetched_block_on_disk(output_root)?;
    // 续传以 ranges 目录为准；meta 仅作对齐（避免此前只在 seg_head 写 meta 导致批次号虚高）
    meta.current_batch_fetched_through_block = disk_through;
    meta.current_batch = batch_id_for_block(disk_through);
    meta.version = META_VERSION;
    save_meta(output_root, &meta)?;
    let mut pool = RpcPool::new(rpc_urls.to_vec(), timeout_secs);
    let latest = pool.get_latest_block_number()?;
    let end_block = end_block.min(latest);
    let batch_id = batch_id_for_block(end_block);
    if !use_progress && !poll_mode { print!("{}", p); println!("Fetch disk_through={} meta aligned | current_batch={} through={} (latest={})", disk_through, meta.current_batch, meta.current_batch_fetched_through_block, latest); }
    // 续传只看 [start_block,end_block] 覆盖段内的落盘进度（main 有数据时传 resume_next 作 start_block，避免被「本批理论起点」抬高）
    let mut next = match max_fetched_in_span(output_root, start_block, end_block) {
        None => start_block,
        Some(h) => h.saturating_add(1),
    };
    if next > end_block {
        if !use_progress && !poll_mode { print!("{}", p); println!("Already have through {}, skip fetch.", meta.current_batch_fetched_through_block); }
        if poll_mode { print!("\r  batch={} height={}   ", batch_id, end_block); let _ = std::io::stdout().flush(); }
        set_fetch_output_prefix(""); return Ok(());
    }
    if !use_progress && !poll_mode { print!("{}", p); println!("Resume from block {} to {}", next, end_block); }
    let already_have = next.saturating_sub(start_block);
    let mut total_written: u64 = already_have;
    let batch_size = batch_size.max(1);
    type SegKey = u64;
    let mut segments: std::collections::HashMap<SegKey, (std::fs::File, FetchRangeCheckpoint, std::path::PathBuf)> = std::collections::HashMap::new();
    // 任意 10 万块区间均与拉最新批一致：chunk_000..chunk_099（每 chunk 1000 块），不写单文件 blocks.jsonl
    let open_seg = |seg_s: u64, root: &Path| -> Result<(std::fs::File, FetchRangeCheckpoint, std::path::PathBuf)> {
        let (range_dir, mut cp, checkpoint_path) = ensure_range_ready_for_segment(root, seg_s)?;
        let span_lo = start_block.max(seg_s);
        let span_hi = end_block.min(seg_s + SEGMENT_SIZE - 1);
        cp.last_fetched_block = max_block_in_segment_span_parsed(root, seg_s, span_lo, span_hi).unwrap_or(seg_s.saturating_sub(1));
        let chunk0 = range_dir.join("chunk_000.jsonl");
        let f = std::fs::OpenOptions::new().create(true).append(true).open(&chunk0)?;
        Ok((f, cp, checkpoint_path))
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
            let (file, cp, ck_path) = segments.entry(seg).or_insert_with(|| open_seg(seg, output_root).expect("open_seg"));
            if bn <= cp.last_fetched_block { continue; }
            let line = serde_json::to_string(block_json).context("block to json")?;
            let range_dir = ck_path.parent().unwrap();
            let chunk_idx = ((bn - cp.start_block) / CHUNK_SIZE) as u32;
            if chunk_idx == 0 { writeln!(file, "{}", line)?; } else {
                let chunk_path = range_dir.join(format!("chunk_{:03}.jsonl", chunk_idx));
                let mut cf = std::fs::OpenOptions::new().create(true).append(true).open(&chunk_path)?;
                writeln!(cf, "{}", line)?;
            }
            cp.last_fetched_block = bn;
            cp.updated_at = Utc::now();
            total_written += 1;
            current_block = bn;
            // 任意区间写入都更新 meta（同一批跨多个 10 万目录时不再只依赖 seg_head）
            meta.current_batch_fetched_through_block = bn;
            meta.current_batch = batch_id_for_block(bn);
            if total_written % CHECKPOINT_SAVE_INTERVAL == 0 {
                save_meta(output_root, &meta)?;
                save_checkpoint_static(ck_path, cp)?;
            }
            if chunk_idx == 0 { file.flush()?; }
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
        save_checkpoint_static(ck_path, cp)?;
    }
    let _ = save_meta(output_root, &meta);
    if poll_mode { print!("\r  batch={} height={} blocks={}   ", batch_id, end_block, total_written); let _ = std::io::stdout().flush(); }
    else if !use_progress { print!("{}", p); println!("\nDone. {} blocks written, {} segments in {:.1}s", total_written, segments.len(), start_time.elapsed().as_secs_f64()); }
    set_fetch_output_prefix("");
    Ok(())
}

/// 扫描 range_root 下所有有数据的批次（blocks.jsonl 或任意 chunk_*.jsonl）
pub fn list_batches_in_ranges(range_root: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    let entries = std::fs::read_dir(range_root).with_context(|| format!("read_dir {}", range_root.display()))?;
    for e in entries {
        let e = e?;
        let name = e.file_name().to_string_lossy().to_string();
        if let Some((a, b)) = name.split_once('-') {
            if let (Ok(start), Ok(_end)) = (a.parse::<u64>(), b.parse::<u64>()) {
                if start % SEGMENT_SIZE != 0 { continue; }
                let dir = range_root.join(&name);
                if dir.join("blocks.jsonl").exists() || (0..CHUNK_COUNT).any(|i| dir.join(format!("chunk_{:03}.jsonl", i)).exists()) {
                    ids.push(start / SEGMENT_SIZE + 1);
                }
            }
        }
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
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

/// 合并 range_dir 内 chunk_*.jsonl → blocks.jsonl，成功后删除 chunk 文件；返回合并后总行数
pub fn merge_range_dir(range_dir: &Path) -> Result<usize> {
    let mut lines: Vec<String> = Vec::new();
    let blocks_path = range_dir.join("blocks.jsonl");
    if blocks_path.exists() {
        let f = std::fs::File::open(&blocks_path)?;
        for line in std::io::BufReader::new(f).lines() { lines.push(line?); }
    }
    let mut merged_chunks = false;
    for i in 0..CHUNK_COUNT {
        let chunk = range_dir.join(format!("chunk_{:03}.jsonl", i));
        if !chunk.exists() { continue; }
        let f = std::fs::File::open(&chunk)?;
        for line in std::io::BufReader::new(f).lines() { lines.push(line?); }
        merged_chunks = true;
    }
    if !merged_chunks { return Ok(lines.len()); }
    let mut out = std::fs::File::create(&blocks_path)?;
    for line in &lines { writeln!(out, "{}", line)?; }
    drop(out);
    for i in 0..CHUNK_COUNT { let _ = std::fs::remove_file(range_dir.join(format!("chunk_{:03}.jsonl", i))); }
    Ok(lines.len())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(addrs.len(), 5);
        assert_eq!(hex::encode(addrs[0]), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let from2: Address = parse_hex_addr("0x6813Eb9362372EEF6200f3b1dbC3f819671cBA69").unwrap();
        assert!(addrs.contains(&from2));
        assert!(addrs.contains(&create_address(&from2, 0)));
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
        assert_eq!(addrs.len(), 3);
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
        assert_eq!(a0, create_address(&sender, 0));
    }
}
