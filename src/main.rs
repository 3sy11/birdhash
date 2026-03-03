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

use anyhow::Result;
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
    /// Fetch on-chain addresses (block traversal → archive + BinaryFuse16)
    Fetch {
        /// RPC URL (overrides config.toml [fetcher].rpc_url)
        #[arg(short, long)]
        rpc: Option<String>,
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
        Commands::Fetch { rpc } => cmd_fetch(cfg, rpc),
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

fn cmd_fetch(cfg: config::AppConfig, rpc_cli: Option<String>) -> Result<()> {
    let rpc_urls = resolve_rpc_urls(&cfg, rpc_cli)?;
    cfg.ensure_dirs()?;
    let cur: cursor::FetcherCursor = cursor::load_or_default(&cfg.fetch_cursor_path());
    let mut f = fetcher::Fetcher::new(cfg, cur, &rpc_urls)?;
    f.run()
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
