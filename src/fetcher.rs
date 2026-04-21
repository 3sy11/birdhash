//! Fetcher: 拉取原始块数据，按 chunk 写入 parquet，维护检查点。

const ADDR_LEN: usize = 20;
type Address = [u8; ADDR_LEN];
use anyhow::{Context, Result};
use chrono::Utc;
use std::io::{BufRead, Write};
use std::path::Path;
use tiny_keccak::{Hasher, Keccak};

const CHECKPOINT_SAVE_INTERVAL: u64 = 50;

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
        Self {
            url: url.to_string(),
            agent,
            retry_count,
            retry_base_ms,
        }
    }

    fn call_with_retry(&self, body: &serde_json::Value) -> Result<serde_json::Value> {
        let mut last_err = None;
        for attempt in 0..=self.retry_count {
            if attempt > 0 {
                let backoff = self.retry_base_ms * (1u64 << (attempt - 1).min(5));
                log::warn!(
                    "RPC retry {}/{} after {}ms",
                    attempt,
                    self.retry_count,
                    backoff
                );
                std::thread::sleep(std::time::Duration::from_millis(backoff));
            }
            match self
                .agent
                .post(&self.url)
                .set("Content-Type", "application/json")
                .send_json(body.clone())
            {
                Ok(resp) => match resp.into_json::<serde_json::Value>() {
                    Ok(v) => return Ok(v),
                    Err(e) => {
                        last_err = Some(anyhow::anyhow!("JSON parse: {}", e));
                    }
                },
                Err(e) => {
                    last_err = Some(anyhow::anyhow!("RPC request: {}", e));
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("RPC failed after retries")))
    }

    pub fn get_latest_block_number(&self) -> Result<u64> {
        let resp = self.call_with_retry(
            &serde_json::json!({"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}),
        )?;
        let hex = resp["result"]
            .as_str()
            .context("eth_blockNumber: no result")?;
        Ok(u64::from_str_radix(hex.trim_start_matches("0x"), 16)?)
    }

    pub fn get_blocks_with_txs(
        &self,
        start: u64,
        count: usize,
    ) -> Result<Vec<(u64, serde_json::Value)>> {
        let reqs: Vec<serde_json::Value> = (0..count).map(|i| {
            let bn = start + i as u64;
            serde_json::json!({"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[format!("0x{:x}", bn), true],"id": bn})
        }).collect();
        let resp = self.call_with_retry(&serde_json::json!(reqs))?;
        if let Some(arr) = resp.as_array() {
            let mut results = Vec::with_capacity(count);
            for item in arr {
                let block = &item["result"];
                if block.is_null() {
                    continue;
                }
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
            if block.is_null() {
                continue;
            }
            let n = block["number"]
                .as_str()
                .and_then(|h| u64::from_str_radix(h.trim_start_matches("0x"), 16).ok())
                .unwrap_or(bn);
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
        Self {
            urls,
            current: 0,
            timeout_secs,
        }
    }
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
                    log::warn!(
                        "RPC[{}] {} failed: {}, try next",
                        self.current,
                        url,
                        last_err.as_ref().unwrap()
                    );
                    self.current = (self.current + 1) % n;
                }
            }
        }
        let last_msg = last_err
            .as_ref()
            .map(|e| e.to_string().replace('\n', " "))
            .unwrap_or_else(|| "unknown".into());
        eprintln!("Last RPC error: {}", last_msg);
        Err(anyhow::anyhow!("all {} RPC URLs failed: {}", n, last_msg))
    }

    pub fn get_latest_block_number(&mut self) -> Result<u64> {
        self.with_rpc(|rpc| rpc.get_latest_block_number())
    }

    pub fn get_blocks_with_txs(
        &mut self,
        start: u64,
        count: usize,
    ) -> Result<Vec<(u64, serde_json::Value)>> {
        self.with_rpc(|rpc| rpc.get_blocks_with_txs(start, count))
    }
}

/// 拉取单个块并返回 (block_json, 提取的地址列表)
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
    if s.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(s, 16).ok()
}

/// 从区块与交易中提取所有可由私钥/确定性推导的地址
pub fn extract_addresses_from_block(block: &serde_json::Value) -> Vec<Address> {
    let mut addrs = Vec::new();
    if let Some(miner) = block["miner"].as_str() {
        if let Some(a) = parse_hex_addr(miner) {
            addrs.push(a);
        }
    }
    if let Some(author) = block["author"].as_str() {
        if let Some(a) = parse_hex_addr(author) {
            addrs.push(a);
        }
    }
    if let Some(withdrawals) = block["withdrawals"].as_array() {
        for w in withdrawals {
            if let Some(addr) = w["address"].as_str() {
                if let Some(a) = parse_hex_addr(addr) {
                    addrs.push(a);
                }
            }
        }
    }
    if let Some(txs) = block["transactions"].as_array() {
        for tx in txs {
            let from = tx["from"].as_str().and_then(parse_hex_addr);
            if let Some(a) = from {
                addrs.push(a);
            }
            if let Some(to) = tx["to"].as_str() {
                if let Some(a) = parse_hex_addr(to) {
                    addrs.push(a);
                }
            } else {
                if let Some(ca) = tx["creates"].as_str() {
                    if let Some(a) = parse_hex_addr(ca) {
                        addrs.push(a);
                    }
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
    if s.len() != 40 {
        return None;
    }
    let bytes = hex::decode(s).ok()?;
    if bytes.len() != ADDR_LEN {
        return None;
    }
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

pub const SEGMENT_SIZE: u64 = 100_000;
const CHUNK_SIZE: u64 = 1_000;
const CHUNK_COUNT: u32 = 100;

fn seg_start(block: u64) -> u64 {
    (block / SEGMENT_SIZE) * SEGMENT_SIZE
}

pub fn seg_dir_name(seg_start: u64) -> String {
    format!("{}-{}", seg_start, seg_start + SEGMENT_SIZE - 1)
}

#[derive(serde::Serialize, serde::Deserialize, Default, Clone)]
pub struct FetchMeta {
    pub version: u32,
    pub current_batch: u64,
    pub current_batch_fetched_through_block: u64,
}

fn meta_path(range_root: &Path) -> std::path::PathBuf {
    range_root
        .parent()
        .unwrap_or_else(|| range_root.as_ref())
        .join("meta.json")
}

pub fn load_meta(range_root: &Path) -> Result<FetchMeta> {
    let p = meta_path(range_root);
    if !p.exists() {
        return Ok(FetchMeta::default());
    }
    let data = std::fs::read_to_string(&p).with_context(|| format!("read {}", p.display()))?;
    let m: FetchMeta =
        serde_json::from_str(&data).with_context(|| format!("parse {}", p.display()))?;
    Ok(m)
}

fn unique_tmp_path(path: &Path) -> std::path::PathBuf {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("tmp");
    let tid = format!("{:?}", std::thread::current().id()).replace(['(', ')', ' '], "");
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    dir.join(format!(".{}.{}.{}.tmp", name, tid, ts))
}

/// 块号所属的批次号（与 main 中 batch 定义一致：batch 1 = 块 1..=100000）
pub fn batch_id_for_block(block: u64) -> u64 {
    if block == 0 {
        return 0;
    }
    (block - 1) / SEGMENT_SIZE + 1
}

fn save_checkpoint_static(path: &Path, cp: &FetchRangeCheckpoint) -> Result<()> {
    let tmp = unique_tmp_path(path);
    std::fs::write(&tmp, serde_json::to_string_pretty(cp).unwrap())?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn load_checkpoint(range_dir: &Path) -> Option<FetchRangeCheckpoint> {
    let ck_path = range_dir.join("checkpoint.json");
    if !ck_path.exists() {
        return None;
    }
    let data = std::fs::read_to_string(&ck_path).ok()?;
    serde_json::from_str(&data).ok()
}

fn ensure_range_ready_for_segment(
    root: &Path,
    seg_s: u64,
) -> Result<(std::path::PathBuf, FetchRangeCheckpoint, std::path::PathBuf)> {
    let name = seg_dir_name(seg_s);
    let range_dir = root.join(&name);
    std::fs::create_dir_all(&range_dir)?;

    let (cp, ck_path) = if let Some(existing_cp) = load_checkpoint(&range_dir) {
        if existing_cp.status == "done"
            && existing_cp.last_fetched_block >= seg_s + SEGMENT_SIZE - 1
        {
            let cp = FetchRangeCheckpoint {
                start_block: seg_s,
                end_block: seg_s + SEGMENT_SIZE - 1,
                last_fetched_block: existing_cp.last_fetched_block,
                status: "done".into(),
                updated_at: Utc::now(),
            };
            return Ok((range_dir.clone(), cp, range_dir.join("checkpoint.json")));
        }
        (existing_cp, range_dir.join("checkpoint.json"))
    } else {
        let cp = FetchRangeCheckpoint {
            start_block: seg_s,
            end_block: seg_s + SEGMENT_SIZE - 1,
            last_fetched_block: seg_s.saturating_sub(1),
            status: "running".into(),
            updated_at: Utc::now(),
        };
        (cp, range_dir.join("checkpoint.json"))
    };

    let mut cp = cp;
    cp.status = "running".into();
    cp.updated_at = Utc::now();
    Ok((range_dir, cp, ck_path))
}

/// Progress: (batch_id, current, end, written, blk_s)
pub type FetchProgressSender = Option<(u64, std::sync::mpsc::Sender<(u64, u64, u64, u64, f64)>)>;

pub fn run_fetch_range(
    output_root: &Path, start_block: u64, end_block: u64,
    rpc_urls: &[String], timeout_secs: u64, batch_size: usize,
    output_prefix: Option<&str>, progress_tx: FetchProgressSender, poll_mode: bool,
) -> Result<()> {
    let p = output_prefix.unwrap_or("");
    anyhow::ensure!(!rpc_urls.is_empty(), "no RPC URLs");
    anyhow::ensure!(start_block <= end_block, "start_block > end_block");
    std::fs::create_dir_all(output_root)?;

    let mut pool = RpcPool::new(rpc_urls.to_vec(), timeout_secs);
    let latest = pool.get_latest_block_number()?;
    let end_block = end_block.min(latest);
    let batch_id = batch_id_for_block(end_block);
    let first_seg = seg_start(start_block);
    let last_seg = seg_start(end_block);

    let mut seg = first_seg;
    while seg <= last_seg {
        let seg_end = (seg + SEGMENT_SIZE - 1).min(end_block);
        let seg_start_b = if seg == first_seg { start_block } else { seg };
        let (_, cp, _) = ensure_range_ready_for_segment(output_root, seg)?;
        if cp.last_fetched_block >= seg_end {
            log::info!("{}Segment {} done, skip.", p, seg_dir_name(seg));
            seg = seg.saturating_add(SEGMENT_SIZE);
            continue;
        }
        let resume_from = (cp.last_fetched_block + 1).max(seg_start_b);
        println!("{}Segment {} resume {} (checkpoint {})", p, seg_dir_name(seg), resume_from, cp.last_fetched_block);
        let (range_dir, _, _) = ensure_range_ready_for_segment(output_root, seg)?;
        let is_single = first_seg == last_seg;
        run_single_segment(
            output_root, range_dir, seg, resume_from, seg_end,
            &mut pool, timeout_secs, batch_size, p,
            if is_single { progress_tx.clone() } else { None },
            poll_mode, batch_id, seg == last_seg,
        )?;
        seg = seg.saturating_add(SEGMENT_SIZE);
    }
    Ok(())
}

fn run_single_segment(
    _output_root: &Path, range_dir: std::path::PathBuf, seg_s: u64,
    start_block: u64, end_block: u64, pool: &mut RpcPool, _timeout_secs: u64,
    batch_size: usize, prefix: &str, progress_tx: FetchProgressSender,
    poll_mode: bool, batch_id: u64, is_last: bool,
) -> Result<()> {
    let mut cp = FetchRangeCheckpoint {
        start_block: seg_s, end_block: seg_s + SEGMENT_SIZE - 1,
        last_fetched_block: start_block.saturating_sub(1),
        status: "running".into(), updated_at: Utc::now(),
    };
    let ck_path = range_dir.join("checkpoint.json");

    // parquet 缓冲：(block_number, block_json_string)
    let start_chunk_idx = ((start_block.saturating_sub(seg_s)) / CHUNK_SIZE) as u32;
    let mut cur_chunk = start_chunk_idx;
    let mut buf_bns: Vec<u64> = Vec::new();
    let mut buf_jsons: Vec<String> = Vec::new();

    // 恢复：加载当前 chunk 已有数据
    let chunk_path = range_dir.join(format!("chunk_{:03}.parquet", cur_chunk));
    if chunk_path.exists() {
        if let Ok((bns, jsons)) = read_chunk_parquet(&chunk_path) {
            log::info!("{}resume chunk_{:03}.parquet ({} blocks)", prefix, cur_chunk, bns.len());
            buf_bns = bns;
            buf_jsons = jsons;
        }
    }

    let flush = |dir: &Path, idx: u32, bns: &[u64], jsons: &[String]| -> Result<()> {
        if jsons.is_empty() { return Ok(()); }
        write_chunk_parquet(&dir.join(format!("chunk_{:03}.parquet", idx)), bns, jsons)
    };

    let batch_size = batch_size.max(1);
    let start_time = std::time::Instant::now();
    let mut next = start_block;
    let mut total_written: u64 = 0;
    let mut current_block: u64 = start_block.saturating_sub(1);

    if let Some((bid, ref tx)) = progress_tx {
        let _ = tx.send((bid, current_block, end_block, 0, 0.0));
    }

    while next <= end_block {
        let count = ((end_block - next + 1) as usize).min(batch_size);
        let blocks = pool.get_blocks_with_txs(next, count).with_context(|| format!("RPC block {}", next))?;

        for (_bn, block_json) in &blocks {
            let bn = block_json["number"].as_str()
                .and_then(|h| u64::from_str_radix(h.trim_start_matches("0x"), 16).ok())
                .unwrap_or(0);
            if bn < start_block || bn <= cp.last_fetched_block { continue; }

            let chunk_idx = ((bn - seg_s) / CHUNK_SIZE) as u32;
            if chunk_idx != cur_chunk {
                flush(&range_dir, cur_chunk, &buf_bns, &buf_jsons)?;
                save_checkpoint_static(&ck_path, &cp)?;
                buf_bns.clear();
                buf_jsons.clear();
                cur_chunk = chunk_idx;
            }

            buf_bns.push(bn);
            buf_jsons.push(serde_json::to_string(block_json).context("block to json")?);

            cp.last_fetched_block = bn;
            cp.updated_at = Utc::now();
            total_written += 1;
            current_block = bn;

            if total_written % CHECKPOINT_SAVE_INTERVAL == 0 {
                flush(&range_dir, cur_chunk, &buf_bns, &buf_jsons)?;
                save_checkpoint_static(&ck_path, &cp)?;
            }
        }

        if blocks.is_empty() { break; }
        next = blocks.last().map(|(b, _)| *b).unwrap_or(next).saturating_add(1);

        let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
        let blk_s = total_written as f64 / elapsed;
        if let Some((bid, ref tx)) = progress_tx {
            let _ = tx.send((bid, current_block, end_block, total_written, blk_s));
        } else if poll_mode {
            print!("\r  batch={} height={} blocks={}   ", batch_id, current_block, total_written);
            let _ = std::io::stdout().flush();
        } else if is_last {
            print!("{}\r  block={} | {}..{} written={} ({:.0} blk/s)   ", prefix, current_block, start_block, end_block, total_written, blk_s);
            let _ = std::io::stdout().flush();
        }
    }

    flush(&range_dir, cur_chunk, &buf_bns, &buf_jsons)?;
    cp.status = "done".into();
    cp.updated_at = Utc::now();
    save_checkpoint_static(&ck_path, &cp)?;

    if poll_mode {
        print!("\r  batch={} height={} blocks={}   ", batch_id, end_block, total_written);
        let _ = std::io::stdout().flush();
    } else if progress_tx.is_none() && is_last {
        println!("{}\nDone. {} blocks (parquet) in {:.1}s", prefix, total_written, start_time.elapsed().as_secs_f64());
    }
    Ok(())
}

/// 扫描 range_root 下所有有数据的批次（blocks.jsonl 或任意 chunk_*.jsonl）
pub fn list_batches_in_ranges(range_root: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    let entries = std::fs::read_dir(range_root)
        .with_context(|| format!("read_dir {}", range_root.display()))?;
    for e in entries {
        let e = e?;
        let name = e.file_name().to_string_lossy().to_string();
        if let Some((a, b)) = name.split_once('-') {
            if let (Ok(start), Ok(_end)) = (a.parse::<u64>(), b.parse::<u64>()) {
                if start % SEGMENT_SIZE != 0 {
                    continue;
                }
                let dir = range_root.join(&name);
                if dir.join("blocks.jsonl").exists()
                    || (0..CHUNK_COUNT).any(|i| dir.join(format!("chunk_{:03}.jsonl", i)).exists())
                    || (0..CHUNK_COUNT).any(|i| dir.join(format!("chunk_{:03}.parquet", i)).exists())
                {
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
    let stem = filter_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("filter_fetch");
    filter_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("{}_meta.json", stem))
}

/// 写入与 filter 同目录的 _meta.json，标识已加载批次和最高块高
pub fn save_fetch_filter_meta(
    filter_path: &Path,
    batches: &[u64],
    max_block_height: u64,
) -> Result<()> {
    let meta = FetchFilterMeta {
        loaded_batches: batches.to_vec(),
        max_block_height,
    };
    let path = fetch_filter_meta_path(filter_path);
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    }
    std::fs::write(&path, serde_json::to_string_pretty(&meta)?)?;
    Ok(())
}

// ── Parquet 读写（存原始块 JSON，利用 snappy 压缩减小体积） ──

pub fn write_chunk_parquet(path: &Path, block_numbers: &[u64], block_jsons: &[String]) -> Result<()> {
    use arrow::array::{StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_json", DataType::Utf8, false),
    ]));
    let bn_array = Arc::new(UInt64Array::from(block_numbers.to_vec()));
    let json_array = Arc::new(StringArray::from(block_jsons.to_vec()));
    let batch = RecordBatch::try_new(schema.clone(), vec![bn_array, json_array])?;
    let tmp = unique_tmp_path(path);
    let file = std::fs::File::create(&tmp)?;
    let mut w = ArrowWriter::try_new(file, schema, None)?;
    w.write(&batch)?;
    w.close()?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// 读取 chunk parquet 返回 (block_numbers, block_json_strings)
pub fn read_chunk_parquet(path: &Path) -> Result<(Vec<u64>, Vec<String>)> {
    use arrow::array::{StringArray, UInt64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut bns = Vec::new();
    let mut jsons = Vec::new();
    for batch in reader {
        let batch = batch?;
        let bn_col = batch.column(0).as_any().downcast_ref::<UInt64Array>().context("cast bn")?;
        let js_col = batch.column(1).as_any().downcast_ref::<StringArray>().context("cast json")?;
        for i in 0..batch.num_rows() {
            bns.push(bn_col.value(i));
            jsons.push(js_col.value(i).to_string());
        }
    }
    Ok((bns, jsons))
}

/// 从 segment 目录读取所有地址（兼容 parquet + jsonl）
pub fn read_addresses_from_range_dir(range_dir: &Path) -> Result<Vec<Address>> {
    let mut addresses = Vec::new();
    // parquet 文件
    for i in 0..CHUNK_COUNT {
        let p = range_dir.join(format!("chunk_{:03}.parquet", i));
        if !p.exists() { continue; }
        let (_, jsons) = read_chunk_parquet(&p)?;
        for js in &jsons {
            let block: serde_json::Value = serde_json::from_str(js)?;
            addresses.extend(extract_addresses_from_block(&block));
        }
    }
    // blocks.jsonl（旧合并格式）
    let blocks_path = range_dir.join("blocks.jsonl");
    if blocks_path.exists() {
        let f = std::fs::File::open(&blocks_path)?;
        for line in std::io::BufReader::new(f).lines() {
            let line = line?;
            if line.trim().is_empty() { continue; }
            let block: serde_json::Value = serde_json::from_str(&line)?;
            addresses.extend(extract_addresses_from_block(&block));
        }
    }
    // chunk_*.jsonl（旧分块格式）
    for i in 0..CHUNK_COUNT {
        let p = range_dir.join(format!("chunk_{:03}.jsonl", i));
        if !p.exists() { continue; }
        let f = std::fs::File::open(&p)?;
        for line in std::io::BufReader::new(f).lines() {
            let line = line?;
            if line.trim().is_empty() { continue; }
            let block: serde_json::Value = serde_json::from_str(&line)?;
            addresses.extend(extract_addresses_from_block(&block));
        }
    }
    Ok(addresses)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hex_addr_valid() {
        let addr = parse_hex_addr("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf").unwrap();
        assert_eq!(
            hex::encode(addr),
            "7e5f4552091a69125d5dfcb7b8c2659029395bdf"
        );
    }

    #[test]
    fn parse_hex_addr_no_prefix() {
        let addr = parse_hex_addr("7E5F4552091A69125d5DfCb7b8C2659029395Bdf").unwrap();
        assert_eq!(
            hex::encode(addr),
            "7e5f4552091a69125d5dfcb7b8c2659029395bdf"
        );
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
        assert_eq!(
            hex::encode(addrs[0]),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
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
        assert_eq!(
            hex::encode(addrs[2]),
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
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
        assert!(addrs
            .iter()
            .any(|a| hex::encode(a) == "cccccccccccccccccccccccccccccccccccccccc"));
        assert!(addrs
            .iter()
            .any(|a| hex::encode(a) == "dddddddddddddddddddddddddddddddddddddddd"));
        assert!(addrs
            .iter()
            .any(|a| hex::encode(a) == "0000000000000000000000000000000000000001"));
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
