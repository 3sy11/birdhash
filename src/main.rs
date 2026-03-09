mod config;
mod derivation;
mod fetcher;
mod filter;
#[allow(dead_code)]
mod keygen;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::io::Write;
use xorf::Filter;

#[derive(Parser)]
#[command(name = "birdhash", version, about = "Ethereum block fetcher and address filter")]
struct Cli {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create data and fetcher directories
    Init,
    /// Fetch raw blocks by batch: 1=1..100000, 2=100001..200000, ... Omit = latest batch (with poll loop if poll_interval_secs>0).
    Fetch {
        /// Batch index (1-based). Multiple: --batch 1 2 3 4 or --batch 1,2,3,4. Single/omit = latest batch.
        #[arg(long, num_args(1..), value_delimiter(','))]
        batch: Option<Vec<u64>>,
        /// RPC URL (required or set [fetcher].rpc_url in config). Also accepted as --rpc-url.
        #[arg(short, long, alias = "rpc-url")]
        rpc: Option<String>,
        /// Output root dir (default: data/fetcher/ranges)
        #[arg(long)]
        output_dir: Option<String>,
    },
    /// 从已拉取的块数据构建地址过滤器（BinaryFuse16）。无参则扫描全量批次逐批构建；--batch 可多批合并为一个 filter。输出 filter.{id}-{id}.bin + 元信息。
    BuildFilter {
        /// 批 ID，与 Fetch 一致。可多个合并：--batch 1 2 3 4。不传则构建数据目录下全部批次（各一个 filter）
        #[arg(long, num_args(0..), value_delimiter(','))]
        batch: Option<Vec<u64>>,
        /// 过滤器输出目录（默认: data/fetcher）
        #[arg(long)]
        output: Option<String>,
    },
    /// 查询地址是否在获取器过滤器中：输入地址，输出 0=不存在 1=存在
    FilterQuery {
        /// 以太坊地址（0x 开头或 40 位十六进制）
        #[arg(required = true)]
        address: String,
        /// 过滤器文件路径（默认: data/fetcher/filter_fetch.bin）
        #[arg(long)]
        filter: Option<String>,
    },
    /// Fetch one block and print block info, transactions, extracted addresses
    FetchTest {
        #[arg(short, long)]
        rpc: Option<String>,
        #[arg(short, long)]
        block: Option<u64>,
    },
    /// 生成派生路径 index 候选并写入 assets/derivation_candidates/（account 取 0-111，index 取该文件所列值）
    GenDerivationCandidates {
        /// 输出文件路径（默认: assets/derivation_candidates/derivation_candidates.txt）
        #[arg(long)]
        output: Option<String>,
    },
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();
    let cfg = load_config(&cli);
    match cli.command {
        Commands::Init => cmd_init(cfg),
        Commands::Fetch { batch, rpc, output_dir } => cmd_fetch(cfg, batch.as_deref(), rpc, output_dir, &cli.config),
        Commands::BuildFilter { batch, output } => cmd_build_filter(cfg, batch.as_deref(), output.as_deref()),
        Commands::FilterQuery { address, filter } => cmd_filter_query(cfg, &address, filter.as_deref()),
        Commands::FetchTest { rpc, block } => cmd_fetch_test(cfg, rpc, block),
        Commands::GenDerivationCandidates { output } => cmd_gen_derivation_candidates(cfg, output.as_deref()),
    }
}

fn load_config(cli: &Cli) -> config::AppConfig {
    config::AppConfig::load(std::path::Path::new(&cli.config))
}

fn cmd_init(cfg: config::AppConfig) -> Result<()> {
    cfg.ensure_dirs()?;
    println!("birdhash init: data_dir={}", cfg.data_dir.display());
    Ok(())
}

const SEGMENT_SIZE: u64 = 100_000;

fn cmd_fetch(cfg: config::AppConfig, batches: Option<&[u64]>, rpc_cli: Option<String>, output_dir: Option<String>, _config_path: &str) -> Result<()> {
    let rpc_urls = resolve_rpc_urls(&cfg, rpc_cli.clone())?;
    cfg.ensure_dirs()?;
    let out_root = output_dir.clone().map(std::path::PathBuf::from).unwrap_or_else(|| cfg.fetcher_ranges_dir());
    std::fs::create_dir_all(&out_root)?;
    let mut pool = fetcher::RpcPool::new(rpc_urls.clone(), cfg.rpc_timeout_secs);
    let latest = pool.get_latest_block_number()?;
    let total_batches = (latest + SEGMENT_SIZE - 1) / SEGMENT_SIZE;
    anyhow::ensure!(total_batches >= 1, "chain has no blocks (latest=0)");
    let batches: Vec<u64> = match batches {
        None => vec![total_batches],
        Some(s) if s.is_empty() => vec![total_batches],
        Some(s) => s.to_vec(),
    };
    for &b in &batches { anyhow::ensure!(b >= 1 && b <= total_batches, "batch {} out of range 1..{}", b, total_batches); }
    if batches.len() > 1 {
        run_fetch_multi(&batches, latest, total_batches, &out_root, &rpc_urls, cfg.rpc_timeout_secs, cfg.rpc_batch_size, cfg.fetcher_dir().as_path())?;
        return Ok(());
    }
    let batch = batches[0];
    let prefix = std::env::var("BIRDHASH_BATCH").ok().map(|b| format!("[batch={}] ", b)).unwrap_or_default();
    println!("{}latest={} total_batches={} batch={}", prefix, latest, total_batches, batch);
    let start_block = (batch - 1) * SEGMENT_SIZE + 1;
    let mut end_block = (batch * SEGMENT_SIZE).min(latest);
    let fetcher_dir = cfg.fetcher_dir();
    let filter_dir = if batch == total_batches { Some(fetcher_dir.as_path()) } else { None };
    fetcher::run_fetch_range(&out_root, start_block, end_block, &rpc_urls, cfg.rpc_timeout_secs, cfg.rpc_batch_size, Some(&prefix), None, filter_dir, false)?;
    if batch == total_batches && cfg.poll_interval_secs > 0 {
        loop {
            print!("\r  batch={} height={} blocks={} (polling every {}s)   ", batch, end_block, end_block - (batch - 1) * SEGMENT_SIZE, cfg.poll_interval_secs);
            let _ = std::io::stdout().flush();
            std::thread::sleep(std::time::Duration::from_secs(cfg.poll_interval_secs));
            let new_latest = pool.get_latest_block_number()?;
            if new_latest <= end_block { continue; }
            fetcher::run_fetch_range(&out_root, end_block + 1, new_latest, &rpc_urls, cfg.rpc_timeout_secs, cfg.rpc_batch_size, Some(&prefix), None, filter_dir, true)?;
            end_block = new_latest;
        }
    }
    Ok(())
}

fn cmd_build_filter(cfg: config::AppConfig, batch: Option<&[u64]>, output: Option<&str>) -> Result<()> {
    let range_root = cfg.fetcher_ranges_dir();
    let out_dir = output.map(std::path::PathBuf::from).unwrap_or_else(|| cfg.fetcher_dir());
    let (count, entries) = match batch {
        Some(ids) if !ids.is_empty() => {
            let (c, e) = fetcher::build_fetch_filter_from_ranges(&range_root, Some(ids), &out_dir)?;
            println!("BuildFilter done: {} filter(s), batches={} entries={}", c, ids.len(), e);
            (c, e)
        }
        _ => {
            let (c, e) = fetcher::build_fetch_filter_all_batches(&range_root, &out_dir)?;
            println!("BuildFilter done: {} filter(s), total entries={}", c, e);
            (c, e)
        }
    };
    let _ = (count, entries);
    Ok(())
}

fn cmd_filter_query(cfg: config::AppConfig, address: &str, filter_path: Option<&str>) -> Result<()> {
    let addr = fetcher::parse_hex_addr(address.trim()).ok_or_else(|| anyhow::anyhow!("无效地址: {}", address))?;
    let fp = filter::addr_to_u64(&addr);
    let paths: Vec<std::path::PathBuf> = match filter_path {
        Some(s) => {
            let p = std::path::PathBuf::from(s);
            if p.is_dir() {
                let mut v: Vec<_> = std::fs::read_dir(&p)?.filter_map(|e| e.ok()).filter(|e| e.path().extension().map_or(false, |x| x == "bin")).map(|e| e.path()).collect();
                v.sort();
                v
            } else {
                if !p.exists() { anyhow::bail!("过滤器文件不存在: {}", p.display()); }
                vec![p]
            }
        }
        None => {
            let dir = cfg.fetcher_dir();
            if !dir.exists() { anyhow::bail!("fetcher 目录不存在: {}", dir.display()); }
            let mut v: Vec<_> = std::fs::read_dir(&dir)?.filter_map(|e| {
                let e = e.ok()?;
                let name = e.file_name();
                let n = name.to_string_lossy();
                if n.starts_with("filter.") && n.ends_with(".bin") { Some(e.path()) } else { None }
            }).collect();
            v.sort();
            v
        }
    };
    anyhow::ensure!(!paths.is_empty(), "未找到任何 filter.*.bin 文件");
    let mut filters: Vec<filter::BinaryFuse16> = Vec::with_capacity(paths.len());
    for p in &paths {
        let f = filter::load_fuse16(p)?;
        filters.push(f);
        eprintln!("loaded: {}", p.display());
    }
    let hit = filters.iter().any(|f| f.contains(&fp));
    println!("{}", if hit { 1 } else { 0 });
    Ok(())
}

fn run_fetch_multi(batches: &[u64], latest: u64, total_batches: u64, out_root: &std::path::Path, rpc_urls: &[String], timeout_secs: u64, batch_size: usize, fetcher_dir: &std::path::Path) -> Result<()> {
    use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
    use std::collections::HashMap;
    use std::sync::mpsc;
    use std::thread;
    let num_workers = batches.len();
    println!("latest={} total_batches={} spawning {} workers: batches {:?}", latest, total_batches, num_workers, batches);
    let (progress_tx, progress_rx) = mpsc::channel::<(u64, u64, u64, u64, f64)>();
    let multi = MultiProgress::new();
    let style = ProgressStyle::default_bar().template("{msg} {bar:40.cyan/blue} {pos:>7}/{len:7}").unwrap();
    let total_blocks: Vec<(u64, u64)> = batches.iter().map(|&b| { let start = (b - 1) * SEGMENT_SIZE + 1; let end = (b * SEGMENT_SIZE).min(latest); (start, end) }).collect();
    let batch_to_idx: HashMap<u64, usize> = batches.iter().enumerate().map(|(i, &b)| (b, i)).collect();
    let total_blocks_d = total_blocks.clone();
    let bars: Vec<_> = batches.iter().enumerate().map(|(i, &b)| {
        let (start, end) = total_blocks[i];
        let len = end - start + 1;
        let pb = multi.add(ProgressBar::new(len).with_style(style.clone()).with_message(format!("[batch={}] {}..{} | 0 blk/s", b, start, end)));
        (b, pb)
    }).collect();
    let display_handle = thread::spawn(move || {
        while let Ok((batch_id, _current, end, written, blk_s)) = progress_rx.recv() {
            if let Some(&idx) = batch_to_idx.get(&batch_id) {
                let (_, ref pb) = bars[idx];
                pb.set_position(written);
                let (start, _) = total_blocks_d[idx];
                pb.set_message(format!("[batch={}] {}..{} | {:.0} blk/s", batch_id, start, end, blk_s));
            }
        }
        for (_, pb) in &bars { pb.finish_with_message("done"); }
    });
    let mut handles = Vec::with_capacity(batches.len());
    for (i, &b) in batches.iter().enumerate() {
        let (start_block, end_block) = total_blocks[i];
        let out = out_root.to_path_buf();
        let urls = rpc_urls.to_vec();
        let tx_send = progress_tx.clone();
        let fdir = fetcher_dir.to_path_buf();
        let tot = total_batches;
        handles.push(thread::spawn(move || {
            let prog = Some((b, tx_send));
            let filter_dir = if b == tot { Some(fdir.as_path()) } else { None };
            fetcher::run_fetch_range(&out, start_block, end_block, &urls, timeout_secs, batch_size, None, prog, filter_dir, false)
        }));
    }
    drop(progress_tx);
    for (i, h) in handles.into_iter().enumerate() {
        let res = h.join().map_err(|_| anyhow::anyhow!("thread panicked"))?;
        res.with_context(|| format!("batch {} failed", batches[i]))?;
    }
    display_handle.join().map_err(|_| anyhow::anyhow!("display thread panicked"))?;
    println!("All {} batches done.", num_workers);
    Ok(())
}

fn resolve_rpc_urls(cfg: &config::AppConfig, rpc_cli: Option<String>) -> Result<Vec<String>> {
    let urls: Vec<String> = if let Some(u) = rpc_cli {
        vec![u]
    } else if !cfg.rpc_urls.is_empty() {
        cfg.rpc_urls.clone()
    } else if let Some(u) = &cfg.rpc_url {
        vec![u.clone()]
    } else {
        anyhow::bail!("no RPC URL. Use --rpc <URL> or set [fetcher].rpc_url / rpc_urls in config.toml");
    };
    Ok(urls)
}

fn cmd_gen_derivation_candidates(cfg: config::AppConfig, output: Option<&str>) -> Result<()> {
    let out_path = output.map(std::path::PathBuf::from).unwrap_or_else(|| cfg.derivation_candidates_path());
    let n = derivation::run_gen_derivation_candidates(&out_path)?;
    println!("wrote {} candidates -> {}", n, out_path.display());
    Ok(())
}

fn cmd_fetch_test(cfg: config::AppConfig, rpc_cli: Option<String>, block_arg: Option<u64>) -> Result<()> {
    let rpc_urls = resolve_rpc_urls(&cfg, rpc_cli)?;
    let block_number = if let Some(b) = block_arg {
        b
    } else {
        let mut pool = fetcher::RpcPool::new(rpc_urls.clone(), cfg.rpc_timeout_secs);
        pool.get_latest_block_number()?
    };
    println!("Fetching block {} ...", block_number);
    let (block, addrs) = fetcher::fetch_one_block(&rpc_urls, block_number, cfg.rpc_timeout_secs)?;
    let number = block["number"].as_str().unwrap_or("0x0");
    let hash = block["hash"].as_str().unwrap_or("—");
    let miner = block["miner"].as_str().unwrap_or("—");
    let author = block["author"].as_str().unwrap_or("—");
    let timestamp = block["timestamp"].as_str().unwrap_or("—");
    let txs = block["transactions"].as_array().map(|a| a.len()).unwrap_or(0);
    let withdrawals = block["withdrawals"].as_array().map(|a| a.len()).unwrap_or(0);
    println!("═══ Block {} ═══", block_number);
    println!("  number:    {}", number);
    println!("  hash:      {}", hash);
    println!("  miner:     {}", miner);
    println!("  author:    {}", author);
    println!("  timestamp: {}", timestamp);
    println!("  txs:       {}", txs);
    println!("  withdrawals: {}", withdrawals);
    if let Some(arr) = block["transactions"].as_array() {
        for (i, tx) in arr.iter().enumerate() {
            let from = tx["from"].as_str().unwrap_or("—");
            let to = tx["to"].as_str().unwrap_or("(contract creation)");
            let creates = tx["creates"].as_str();
            let nonce = tx["nonce"].as_str().unwrap_or("0x0");
            println!("  tx[{}] from={} to={} nonce={} creates={}", i, from, to, nonce, creates.unwrap_or("—"));
        }
    }
    let unique: std::collections::HashSet<_> = addrs.iter().map(|a| hex::encode(a)).collect();
    println!("═══ Extracted addresses ({} unique) ═══", unique.len());
    let mut sorted: Vec<_> = unique.into_iter().collect();
    sorted.sort();
    for (i, hex_addr) in sorted.into_iter().enumerate() {
        println!("  {}  0x{}", i + 1, hex_addr);
    }
    Ok(())
}
