mod collider;
mod config;
mod derivation;
mod fetcher;
mod filter;
mod generator;
mod gpu_collider;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::io::Write;
use xorf::Filter;

#[derive(Parser)]
#[command(
    name = "birdhash",
    version,
    about = "Ethereum block fetcher and address filter"
)]
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
        /// 链标识（默认 eth），影响落盘目录名：data/fetcher_<chain>/
        #[arg(long, default_value = "eth")]
        chain: String,
        /// 只保存块中提取的地址（写入 address/ 目录），不保存原始块 JSON
        #[arg(long)]
        addr_only: bool,
        #[arg(long, num_args(1..), value_delimiter(','))]
        batch: Option<Vec<u64>>,
        #[arg(short, long, alias = "rpc-url")]
        rpc: Option<String>,
        #[arg(long)]
        output_dir: Option<String>,
    },
    /// 从所有 fetcher 目录的块数据构建地址过滤器，输出到 data/filter/
    BuildFilter {
        /// 只处理指定批次（逗号分隔），省略则处理全部
        #[arg(long, num_args(0..), value_delimiter(','))]
        batch: Option<Vec<u64>>,
        /// 手动指定 source ranges 目录（覆盖自动扫描）
        #[arg(long)]
        source: Option<String>,
        /// 手动指定 BF 输出目录（默认 data/filter/）
        #[arg(long)]
        output: Option<String>,
    },
    /// 查询地址是否在 BF 过滤器中
    /// 查询地址是否在 BF 过滤器中
    FilterQuery {
        #[arg(required = true)]
        address: String,
        /// 手动指定 BF 文件或目录（默认 data/filter/）
        #[arg(long)]
        filter: Option<String>,
    },
    /// Fetch one block and print block info
    FetchTest {
        #[arg(short, long)]
        rpc: Option<String>,
        #[arg(short, long)]
        block: Option<u64>,
    },
    /// 碰撞器：N 线程生成地址，实时与 BF 碰撞，命中写 CSV，支持断点续碰
    Collide {
        /// worker 线程数（默认 4，--gpu 时为 CPU PBKDF2 线程数）
        #[arg(long, default_value = "4")]
        threads: usize,
        /// 使用 NVIDIA GPU（CUDA）加速派生，需要已安装 N 卡驱动
        #[arg(long)]
        gpu: bool,
    },
    /// 查询 ID 的助记词/地址/私钥信息，或导出全部派生 CSV
    IdInfo {
        /// ID 序号
        #[arg(required = true)]
        id: u64,
        /// 导出全部 (account×index) 派生为 CSV
        #[arg(long)]
        all: bool,
    },
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();
    let cfg = load_config(&cli);
    match cli.command {
        Commands::Init => cmd_init(cfg),
        Commands::Fetch { chain, addr_only, batch, rpc, output_dir } => cmd_fetch(cfg, &chain, addr_only, batch.as_deref(), rpc, output_dir, &cli.config),
        Commands::BuildFilter { batch, source, output } => cmd_build_filter(cfg, batch.as_deref(), source.as_deref(), output.as_deref()),
        Commands::FilterQuery { address, filter } => cmd_filter_query(cfg, &address, filter.as_deref()),
        Commands::FetchTest { rpc, block } => cmd_fetch_test(cfg, rpc, block),
        Commands::Collide { threads, gpu } => cmd_collide(cfg, threads, gpu),
        Commands::IdInfo { id, all } => cmd_id_info(cfg, id, all),
    }
}

fn load_config(cli: &Cli) -> config::AppConfig {
    config::AppConfig::load(std::path::Path::new(&cli.config))
}

fn cmd_init(cfg: config::AppConfig) -> Result<()> {
    cfg.ensure_chain_dirs("eth")?;
    collider::write_new_seed(&cfg.generator_seed_path())?;
    println!("birdhash init: data_dir={} | 已重新生成 {}", cfg.data_dir.display(), cfg.generator_seed_path().display());
    Ok(())
}

const SEGMENT_SIZE: u64 = 100_000;

fn cmd_fetch(
    cfg: config::AppConfig,
    chain: &str,
    addr_only: bool,
    batches: Option<&[u64]>,
    rpc_cli: Option<String>,
    output_dir: Option<String>,
    _config_path: &str,
) -> Result<()> {
    let rpc_urls = resolve_rpc_urls(&cfg, rpc_cli.clone())?;
    cfg.ensure_chain_dirs(chain)?;
    let out_root = output_dir.clone().map(std::path::PathBuf::from).unwrap_or_else(||
        if addr_only { cfg.fetcher_address_dir_for(chain) } else { cfg.fetcher_ranges_dir_for(chain) }
    );
    if addr_only { println!("  [addr-only] 只保存地址到 {}", out_root.display()); }
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
    for &b in &batches {
        anyhow::ensure!(
            b >= 1 && b <= total_batches,
            "batch {} out of range 1..{}",
            b,
            total_batches
        );
    }
    if batches.len() > 1 {
        run_fetch_multi(
            &batches,
            latest,
            total_batches,
            &out_root,
            &rpc_urls,
            cfg.rpc_timeout_secs,
            cfg.rpc_batch_size,
        )?;
        return Ok(());
    }
    let batch = batches[0];
    let prefix = std::env::var("BIRDHASH_BATCH")
        .ok()
        .map(|b| format!("[batch={}] ", b))
        .unwrap_or_default();
    println!(
        "{}latest={} total_batches={} batch={}",
        prefix, latest, total_batches, batch
    );
    let start_block = (batch - 1) * SEGMENT_SIZE + 1;
    let mut end_block = (batch * SEGMENT_SIZE).min(latest);
    let do_fetch = |root: &std::path::Path, s: u64, e: u64, pfx: Option<&str>, ptx: fetcher::FetchProgressSender, pm: bool| -> Result<()> {
        if addr_only {
            fetcher::run_fetch_range_addr_only(root, s, e, &rpc_urls, cfg.rpc_timeout_secs, cfg.rpc_batch_size, pfx, ptx, pm)
        } else {
            fetcher::run_fetch_range(root, s, e, &rpc_urls, cfg.rpc_timeout_secs, cfg.rpc_batch_size, pfx, ptx, pm)
        }
    };
    do_fetch(&out_root, start_block, end_block, Some(&prefix), None, false)?;
    if batch == total_batches && cfg.poll_interval_secs > 0 {
        loop {
            print!("\r  batch={} height={} blocks={} (polling every {}s)   ", batch, end_block, end_block - (batch - 1) * SEGMENT_SIZE, cfg.poll_interval_secs);
            let _ = std::io::stdout().flush();
            std::thread::sleep(std::time::Duration::from_secs(cfg.poll_interval_secs));
            let new_latest = pool.get_latest_block_number()?;
            if new_latest <= end_block { continue; }
            do_fetch(&out_root, end_block + 1, new_latest, Some(&prefix), None, true)?;
            end_block = new_latest;
        }
    }
    Ok(())
}

fn cmd_build_filter(
    cfg: config::AppConfig,
    batch: Option<&[u64]>,
    source: Option<&str>,
    output: Option<&str>,
) -> Result<()> {
    use crate::filter;
    cfg.ensure_dirs()?;
    let out_dir = output.map(std::path::PathBuf::from).unwrap_or_else(|| cfg.filter_dir());
    std::fs::create_dir_all(&out_dir)?;

    // 收集 ranges + address 目录
    let range_roots: Vec<std::path::PathBuf> = if let Some(s) = source {
        vec![std::path::PathBuf::from(s)]
    } else {
        cfg.all_fetcher_ranges_dirs()
    };
    let addr_roots: Vec<std::path::PathBuf> = if source.is_some() { vec![] } else { cfg.all_fetcher_address_dirs() };
    anyhow::ensure!(!range_roots.is_empty() || !addr_roots.is_empty(), "未找到任何 fetcher 的 ranges/address 目录，请先 birdhash fetch");
    if !range_roots.is_empty() {
        println!("  扫描 {} 个 ranges 目录:", range_roots.len());
        for r in &range_roots { println!("    {}", r.display()); }
    }
    if !addr_roots.is_empty() {
        println!("  扫描 {} 个 address 目录:", addr_roots.len());
        for r in &addr_roots { println!("    {}", r.display()); }
    }

    // 加载已有 BF（data/filter/ + 旧 data/fetcher/ 兼容），用于增量去重
    let mut existing_bf = collider::load_all_bf_pub(&out_dir).unwrap_or_default();
    // 兼容旧 data/fetcher 下的 BF
    let legacy_fetcher = cfg.data_dir.join("fetcher");
    if legacy_fetcher.exists() && legacy_fetcher != out_dir {
        existing_bf.extend(collider::load_all_bf_pub(&legacy_fetcher).unwrap_or_default());
    }
    let has_existing = !existing_bf.is_empty();
    if has_existing { println!("  已加载 {} 组已有 BF，增量构建：跳过已存在地址", existing_bf.len()); }

    let mut set1 = std::collections::HashSet::<u64>::new();
    let mut set2 = std::collections::HashSet::<u64>::new();
    let mut set3 = std::collections::HashSet::<u64>::new();
    let mut skipped_addrs = 0u64;
    let mut included_batches: Vec<u64> = Vec::new();

    for range_root in &range_roots {
        let meta = fetcher::load_meta(range_root).unwrap_or_default();
        let current_batch = meta.current_batch;
        let current_batch_through = meta.current_batch_fetched_through_block;
        let current_batch_seg_end = current_batch * fetcher::SEGMENT_SIZE - 1;
        let current_batch_done = current_batch_through >= current_batch_seg_end;

        let all_batches = match fetcher::list_batches_in_ranges(range_root) {
            Ok(b) if !b.is_empty() => b,
            _ => { println!("  {} 无可用批次，跳过", range_root.display()); continue; }
        };
        let batch_list: Vec<u64> = match batch {
            Some(ids) if !ids.is_empty() => ids.iter().copied().filter(|id| all_batches.contains(id)).collect(),
            _ => all_batches.clone(),
        };
        if batch_list.is_empty() { continue; }
        println!("  {} 发现 {} 个批次", range_root.display(), batch_list.len());

        for &bid in &batch_list {
            if bid == current_batch && !current_batch_done {
                println!("    batch={} 跳过（写入中 through={}/{}）", bid, current_batch_through, current_batch_seg_end);
                continue;
            }
            let seg_s = (bid.saturating_sub(1)) * fetcher::SEGMENT_SIZE;
            let range_dir = range_root.join(fetcher::seg_dir_name(seg_s));
            let addrs = fetcher::read_addresses_from_range_dir(&range_dir)?;
            if addrs.is_empty() { continue; }
            let mut batch_new = 0u64;
            for addr in &addrs {
                if has_existing && collider::contains_bf_pub(&existing_bf, addr) {
                    skipped_addrs += 1;
                    continue;
                }
                set1.insert(filter::addr_to_u64(addr));
                set2.insert(filter::addr_to_u64_alt(addr));
                set3.insert(filter::addr_to_u64_alt2(addr));
                batch_new += 1;
            }
            println!("    batch={} 读取 {} 个地址（新增 {}）", bid, addrs.len(), batch_new);
            included_batches.push(bid);
        }
    }

    // 扫描 address 目录（已提前提取地址，直接读 addr parquet）
    for addr_root in &addr_roots {
        let meta = fetcher::load_meta(addr_root).unwrap_or_default();
        let current_batch = meta.current_batch;
        let current_batch_through = meta.current_batch_fetched_through_block;
        let current_batch_seg_end = current_batch * fetcher::SEGMENT_SIZE - 1;
        let current_batch_done = current_batch_through >= current_batch_seg_end;
        let all_batches = match fetcher::list_batches_in_ranges(addr_root) {
            Ok(b) if !b.is_empty() => b,
            _ => { println!("  {} 无可用批次，跳过", addr_root.display()); continue; }
        };
        let batch_list: Vec<u64> = match batch {
            Some(ids) if !ids.is_empty() => ids.iter().copied().filter(|id| all_batches.contains(id)).collect(),
            _ => all_batches.clone(),
        };
        if batch_list.is_empty() { continue; }
        println!("  {} 发现 {} 个批次 (addr)", addr_root.display(), batch_list.len());
        for &bid in &batch_list {
            if bid == current_batch && !current_batch_done {
                println!("    batch={} 跳过（写入中）", bid);
                continue;
            }
            let seg_s = (bid.saturating_sub(1)) * fetcher::SEGMENT_SIZE;
            let seg_dir = addr_root.join(fetcher::seg_dir_name(seg_s));
            let addrs = fetcher::read_addresses_from_addr_dir(&seg_dir)?;
            if addrs.is_empty() { continue; }
            let mut batch_new = 0u64;
            for addr in &addrs {
                if has_existing && collider::contains_bf_pub(&existing_bf, addr) { skipped_addrs += 1; continue; }
                set1.insert(filter::addr_to_u64(addr));
                set2.insert(filter::addr_to_u64_alt(addr));
                set3.insert(filter::addr_to_u64_alt2(addr));
                batch_new += 1;
            }
            println!("    batch={} 读取 {} 个地址（新增 {}）[addr]", bid, addrs.len(), batch_new);
            included_batches.push(bid);
        }
    }

    if skipped_addrs > 0 { println!("  跳过已存在地址 {} 个", skipped_addrs); }
    if included_batches.is_empty() {
        println!("  没有新批次需要构建 BF");
        return Ok(());
    }

    let keys1: Vec<u64> = set1.into_iter().collect();
    let keys2: Vec<u64> = set2.into_iter().collect();
    let keys3: Vec<u64> = set3.into_iter().collect();
    let entries = keys1.len();
    if entries == 0 {
        println!("  所有地址已存在于已有 BF 中，无需生成新过滤器");
        return Ok(());
    }
    let min_b = *included_batches.iter().min().unwrap();
    let max_b = *included_batches.iter().max().unwrap();
    let contiguous = included_batches.len() == (max_b - min_b + 1) as usize;
    let base_name = if contiguous { format!("filter.{}-{}", min_b, max_b) }
    else { format!("filter.{}-{}-n{}", min_b, max_b, included_batches.len()) };
    let out_path = out_dir.join(format!("{}.bin", base_name));
    let out_alt = out_dir.join(format!("{}.alt.bin", base_name));
    let out_alt2 = out_dir.join(format!("{}.alt2.bin", base_name));
    // 已有同名 BF 则跳过
    if out_path.exists() {
        println!("  {} 已存在，跳过", out_path.display());
        return Ok(());
    }
    println!("  构建 BF: {} 个唯一指纹 ...", entries);
    let f1 = filter::build_fuse16(&keys1)?;
    let f2 = filter::build_fuse16(&keys2)?;
    let f3 = filter::build_fuse16(&keys3)?;
    filter::save_fuse16(&f1, &out_path)?;
    filter::save_fuse16(&f2, &out_alt)?;
    filter::save_fuse16(&f3, &out_alt2)?;
    fetcher::save_fetch_filter_meta(&out_path, &included_batches, max_b * fetcher::SEGMENT_SIZE)?;
    println!("  BuildFilter done: batches={:?} entries={} skipped={} -> {}", included_batches, entries, skipped_addrs, out_path.display());
    Ok(())
}

fn cmd_filter_query(
    cfg: config::AppConfig,
    address: &str,
    filter_path: Option<&str>,
) -> Result<()> {
    let addr = fetcher::parse_hex_addr(address.trim())
        .ok_or_else(|| anyhow::anyhow!("无效地址: {}", address))?;
    let resolve_dir = || -> Result<std::path::PathBuf> {
        let filter_dir = cfg.filter_dir();
        if filter_dir.exists() && collider::bf_count_in_dir(&filter_dir) > 0 { return Ok(filter_dir); }
        let legacy = cfg.data_dir.join("fetcher");
        if legacy.exists() && collider::bf_count_in_dir(&legacy) > 0 {
            println!("  data/filter 无 BF，回退到 {}", legacy.display());
            return Ok(legacy);
        }
        anyhow::bail!("未找到 BF 过滤器，已检查:\n  - {}\n  - {}", filter_dir.display(), legacy.display());
    };
    let hit = match filter_path {
        None => {
            let dir = resolve_dir()?;
            println!("  加载 BF 过滤器: {}", dir.display());
            let (hit, count) = collider::bf_contains_verbose(&dir, &addr)?;
            println!("  已加载 {} 组 BF 过滤器", count);
            hit
        }
        Some(s) => {
            let p = std::path::Path::new(s);
            if !p.exists() { anyhow::bail!("路径不存在: {}", p.display()); }
            if p.is_dir() {
                println!("  加载 BF 过滤器: {}", p.display());
                let (hit, count) = collider::bf_contains_verbose(p, &addr)?;
                println!("  已加载 {} 组 BF 过滤器", count);
                hit
            } else {
                println!("  加载单个 BF: {}", p.display());
                let f = filter::load_fuse16(p)?;
                f.contains(&filter::addr_to_u64(&addr))
            }
        }
    };
    println!("  查询地址: {}", address.trim());
    println!("  结果: {}", if hit { "命中" } else { "未命中" });
    println!("{}", if hit { 1 } else { 0 });
    Ok(())
}

fn run_fetch_multi(
    batches: &[u64],
    latest: u64,
    total_batches: u64,
    out_root: &std::path::Path,
    rpc_urls: &[String],
    timeout_secs: u64,
    batch_size: usize,
) -> Result<()> {
    use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
    use std::collections::HashMap;
    use std::sync::mpsc;
    use std::thread;
    let num_workers = batches.len();
    println!(
        "latest={} total_batches={} spawning {} workers: batches {:?}",
        latest, total_batches, num_workers, batches
    );
    let (progress_tx, progress_rx) = mpsc::channel::<(u64, u64, u64, u64, f64)>();
    let multi = MultiProgress::new();
    let style = ProgressStyle::default_bar()
        .template("{msg} {bar:40.cyan/blue} {pos:>7}/{len:7}")
        .unwrap();
    let total_blocks: Vec<(u64, u64)> = batches
        .iter()
        .map(|&b| {
            let start = (b - 1) * SEGMENT_SIZE + 1;
            let end = (b * SEGMENT_SIZE).min(latest);
            (start, end)
        })
        .collect();
    let batch_to_idx: HashMap<u64, usize> =
        batches.iter().enumerate().map(|(i, &b)| (b, i)).collect();
    let total_blocks_d = total_blocks.clone();
    let bars: Vec<_> = batches
        .iter()
        .enumerate()
        .map(|(i, &b)| {
            let (start, end) = total_blocks[i];
            let len = end - start + 1;
            let pb = multi.add(
                ProgressBar::new(len)
                    .with_style(style.clone())
                    .with_message(format!("[batch={}] {}..{} | 0 blk/s", b, start, end)),
            );
            (b, pb)
        })
        .collect();
    let display_handle = thread::spawn(move || {
        while let Ok((batch_id, _current, end, written, blk_s)) = progress_rx.recv() {
            if let Some(&idx) = batch_to_idx.get(&batch_id) {
                let (_, ref pb) = bars[idx];
                pb.set_position(written);
                let (start, _) = total_blocks_d[idx];
                pb.set_message(format!(
                    "[batch={}] {}..{} | {:.0} blk/s",
                    batch_id, start, end, blk_s
                ));
            }
        }
        for (_, pb) in &bars {
            pb.finish_with_message("done");
        }
    });
    let mut handles = Vec::with_capacity(batches.len());
    println!("spawning {} threads...", batches.len());
    for (i, &b) in batches.iter().enumerate() {
        let (start_block, end_block) = total_blocks[i];
        let out = out_root.to_path_buf();
        let urls = rpc_urls.to_vec();
        let tx_send = progress_tx.clone();
        println!(
            "spawning thread for batch {} ({}-{})",
            b, start_block, end_block
        );
        handles.push(thread::spawn(move || {
            let prog = Some((b, tx_send));
            println!("thread for batch {} started", b);
            fetcher::run_fetch_range(
                &out,
                start_block,
                end_block,
                &urls,
                timeout_secs,
                batch_size,
                None,
                prog,
                false,
            )
        }));
        println!("thread {} spawned, waiting for others...", i);
    }
    drop(progress_tx);
    println!("all threads spawned, waiting for completion...");
    for (i, h) in handles.into_iter().enumerate() {
        println!("waiting for thread {}...", i);
        let res = h.join().map_err(|_| anyhow::anyhow!("thread panicked"))?;
        println!("thread {} done", i);
        res.with_context(|| format!("batch {} failed", batches[i]))?;
    }
    display_handle
        .join()
        .map_err(|_| anyhow::anyhow!("display thread panicked"))?;
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
        anyhow::bail!(
            "no RPC URL. Use --rpc <URL> or set [fetcher].rpc_url / rpc_urls in config.toml"
        );
    };
    Ok(urls)
}

fn cmd_collide(cfg: config::AppConfig, threads: usize, gpu: bool) -> Result<()> {
    if gpu {
        log::info!("使用 GPU 模式，CPU PBKDF2 线程={}", threads);
        gpu_collider::run_gpu_collider(&cfg, threads)
    } else {
        collider::run_collider(&cfg, threads)
    }
}

fn cmd_id_info(cfg: config::AppConfig, id: u64, all: bool) -> Result<()> {
    if all {
        let out = cfg
            .generator_dir()
            .join(format!("export_id_{}_all.csv", id));
        generator::export_id_all_derivations_to_csv(&cfg, id, &out)
    } else {
        generator::print_id_details(&cfg, id)
    }
}

fn cmd_fetch_test(
    cfg: config::AppConfig,
    rpc_cli: Option<String>,
    block_arg: Option<u64>,
) -> Result<()> {
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
    let txs = block["transactions"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    let withdrawals = block["withdrawals"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
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
            println!(
                "  tx[{}] from={} to={} nonce={} creates={}",
                i,
                from,
                to,
                nonce,
                creates.unwrap_or("—")
            );
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
