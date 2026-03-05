mod archive;
mod collider;
mod config;
mod cursor;
mod fetcher;
mod filter;
mod generator;
#[allow(dead_code)]
mod keygen;
mod lifecycle;
mod scanner;
mod stats;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "birdhash",
    version,
    about = "Ethereum address collision detector with 3-tier storage"
)]
struct Cli {
    /// Path to config.toml (default: config.toml in current dir)
    #[arg(short, long, default_value = "config.toml")]
    config: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize master seed and data directories
    Init,
    /// Run deterministic key generator (seed+counter → BinaryFuse8 L1)
    Generate {
        #[arg(short, long)]
        threads: Option<usize>,
    },
    /// Fetch raw blocks by batch: 1=1..100000, 2=100001..200000, ... Multiple batches run in parallel (threads).
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
    /// Test fetcher: fetch one block and print block info, transactions, and extracted addresses
    FetchTest {
        #[arg(short, long)]
        rpc: Option<String>,
        /// Block number (default: latest)
        #[arg(short, long)]
        block: Option<u64>,
    },
    /// Run bidirectional collision (L1/L2 filter + batch regenerate verify)
    Collide {
        /// Local regenerate-range mode (L3): specify START END
        #[arg(long, num_args = 2, value_names = ["START", "END"])]
        regenerate_range: Option<Vec<u64>>,
    },
    /// Show 3-tier storage status, shard counts, disk usage
    Status,
    /// Manage disk lifecycle: L1→L2 downgrade (--archive) / L2→L3 purge
    Purge {
        /// Keep last N shards (default: 0 = purge all eligible)
        #[arg(long, default_value = "0")]
        keep_last: usize,
        /// L1→L2 archive mode (default: L2→L3 purge)
        #[arg(long)]
        archive: bool,
    },
    /// Distributed L3 scan: split counter range into task files
    ScanDistribute {
        #[arg(long, num_args = 2, value_names = ["START", "END"])]
        range: Vec<u64>,
        #[arg(long, default_value = "10000000000")]
        chunk_size: u64,
        #[arg(long)]
        fetch_addrs: String,
        #[arg(long, default_value = "data/tasks")]
        out: String,
    },
    /// Distributed L3 scan: worker processes a single task
    ScanWorker {
        #[arg(long)]
        task: String,
        #[arg(long, default_value = "data/tasks")]
        output: String,
    },
    /// Distributed L3 scan: collect and merge results
    ScanCollect {
        #[arg(long, default_value = "data/tasks")]
        results_dir: String,
    },
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();
    let cfg = load_config(&cli);
    match cli.command {
        Commands::Init => cmd_init(cfg),
        Commands::Status => cmd_status(cfg),
        Commands::Generate { threads } => cmd_generate(cfg, threads),
        Commands::Fetch { batch, rpc, output_dir } => cmd_fetch(cfg, batch.as_deref(), rpc, output_dir, &cli.config),
        Commands::FetchTest { rpc, block } => cmd_fetch_test(cfg, rpc, block),
        Commands::Collide { regenerate_range } => cmd_collide(cfg, regenerate_range),
        Commands::Purge { keep_last, archive } => cmd_purge(cfg, keep_last, archive),
        Commands::ScanDistribute {
            range,
            chunk_size,
            fetch_addrs,
            out,
        } => cmd_scan_distribute(cfg, &range, chunk_size, &fetch_addrs, &out),
        Commands::ScanWorker { task, output } => cmd_scan_worker(&task, &output),
        Commands::ScanCollect { results_dir } => cmd_scan_collect(cfg, &results_dir),
    }
}

fn load_config(cli: &Cli) -> config::AppConfig {
    let path = std::path::Path::new(&cli.config);
    let cfg = config::AppConfig::load(path);
    log::info!(
        "config: data_dir={} shard_size={} threads={}",
        cfg.data_dir.display(),
        cfg.shard_size,
        cfg.threads
    );
    cfg
}

fn cmd_init(cfg: config::AppConfig) -> Result<()> {
    cfg.ensure_dirs()?;
    let seed = keygen::load_or_create_seed(&cfg.master_seed_path())?;
    let seed_hash = keygen::seed_hash_id(&seed);
    let gen_path = cfg.gen_cursor_path();
    if !gen_path.exists() {
        let cur = cursor::GeneratorCursor {
            master_seed_hash: seed_hash.clone(),
            ..Default::default()
        };
        cursor::save_cursor(&cur, &gen_path)?;
    }
    println!("birdhash initialized");
    println!("  seed hash:  {}", seed_hash);
    println!("  data dir:   {}", cfg.data_dir.display());
    println!(
        "  shard size: {} ({:.0} 亿)",
        cfg.shard_size,
        cfg.shard_size as f64 / 1e8
    );
    println!("  threads:    {}", cfg.threads);
    Ok(())
}

fn cmd_generate(mut cfg: config::AppConfig, threads: Option<usize>) -> Result<()> {
    if let Some(t) = threads {
        cfg.threads = t;
    }
    rayon::ThreadPoolBuilder::new()
        .num_threads(cfg.threads)
        .build_global()
        .ok();
    cfg.ensure_dirs()?;
    let seed = keygen::load_or_create_seed(&cfg.master_seed_path())?;
    let seed_hash = keygen::seed_hash_id(&seed);
    let mut cur: cursor::GeneratorCursor = cursor::load_or_default(&cfg.gen_cursor_path());
    if cur.master_seed_hash.is_empty() {
        cur.master_seed_hash = seed_hash.clone();
    }
    anyhow::ensure!(
        cur.master_seed_hash == seed_hash,
        "seed hash mismatch: cursor={} vs seed={}",
        cur.master_seed_hash,
        seed_hash
    );
    let mut gen = generator::Generator::new(cfg, cur, seed);
    gen.load_fetch_filter()?;
    gen.run()
}

fn cmd_collide(cfg: config::AppConfig, regenerate_range: Option<Vec<u64>>) -> Result<()> {
    cfg.ensure_dirs()?;
    let seed = keygen::load_or_create_seed(&cfg.master_seed_path())?;
    let col_cur: cursor::ColliderCursor = cursor::load_or_default(&cfg.collider_cursor_path());
    let mut c = collider::Collider::new(cfg, col_cur, seed);
    if let Some(range) = regenerate_range {
        anyhow::ensure!(
            range.len() == 2,
            "regenerate-range needs exactly 2 values: START END"
        );
        c.regenerate_range(range[0], range[1])
    } else {
        c.run()
    }
}

fn cmd_purge(cfg: config::AppConfig, keep_last: usize, archive: bool) -> Result<()> {
    cfg.ensure_dirs()?;
    let seed = keygen::load_or_create_seed(&cfg.master_seed_path())?;
    if archive {
        lifecycle::downgrade_l1_to_l2(&cfg, &seed, keep_last)?;
    } else {
        lifecycle::purge_l2_to_l3(&cfg, keep_last)?;
    }
    let (l1, l2) = lifecycle::disk_usage(&cfg);
    println!(
        "Disk: L1={:.2} GB  L2={:.2} GB  total={:.2} GB",
        l1 as f64 / 1e9,
        l2 as f64 / 1e9,
        (l1 + l2) as f64 / 1e9
    );
    Ok(())
}

fn cmd_scan_distribute(
    cfg: config::AppConfig,
    range: &[u64],
    chunk_size: u64,
    fetch_addrs: &str,
    out: &str,
) -> Result<()> {
    anyhow::ensure!(range.len() == 2, "range needs exactly 2 values: START END");
    let seed = keygen::load_or_create_seed(&cfg.master_seed_path())?;
    let seed_hex = hex::encode(seed);
    let fetch_count = if std::path::Path::new(fetch_addrs).exists() {
        fetcher::load_new_addrs(std::path::Path::new(fetch_addrs))
            .map(|a| a.len() as u64)
            .unwrap_or(0)
    } else {
        0
    };
    scanner::distribute(
        range[0],
        range[1],
        chunk_size,
        &seed_hex,
        fetch_addrs,
        fetch_count,
        out,
    )?;
    Ok(())
}

fn cmd_scan_worker(task_path: &str, output: &str) -> Result<()> {
    scanner::worker(task_path, output)?;
    Ok(())
}

fn cmd_scan_collect(cfg: config::AppConfig, results_dir: &str) -> Result<()> {
    let hits = scanner::collect(results_dir)?;
    if !hits.is_empty() {
        cfg.ensure_dirs()?;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(cfg.hits_path())?;
        use std::io::Write;
        for (counter, info) in &hits {
            writeln!(f, "scan | counter={} | {}", counter, info)?;
        }
        println!(
            "Appended {} hits to {}",
            hits.len(),
            cfg.hits_path().display()
        );
    }
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
        run_fetch_multi(&batches, latest, total_batches, &out_root, &rpc_urls, cfg.rpc_timeout_secs, cfg.rpc_batch_size)?;
        return Ok(());
    }
    let batch = batches[0];
    let prefix = std::env::var("BIRDHASH_BATCH").ok().map(|b| format!("[batch={}] ", b)).unwrap_or_default();
    println!("{}latest={} total_batches={} batch={}", prefix, latest, total_batches, batch);
    let start_block = (batch - 1) * SEGMENT_SIZE + 1;
    let end_block = (batch * SEGMENT_SIZE).min(latest);
    fetcher::run_fetch_range(&out_root, start_block, end_block, &rpc_urls, cfg.rpc_timeout_secs, cfg.rpc_batch_size, Some(&prefix), None)
}

fn run_fetch_multi(batches: &[u64], latest: u64, total_batches: u64, out_root: &std::path::Path, rpc_urls: &[String], timeout_secs: u64, batch_size: usize) -> Result<()> {
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
        handles.push(thread::spawn(move || {
            let prog = Some((b, tx_send));
            fetcher::run_fetch_range(&out, start_block, end_block, &urls, timeout_secs, batch_size, None, prog)
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

fn cmd_status(cfg: config::AppConfig) -> Result<()> {
    let s = stats::gather_status(&cfg)?;
    stats::print_status(&s);
    Ok(())
}
