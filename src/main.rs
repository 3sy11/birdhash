mod collider;
mod config;
mod derivation;
mod fetcher;
mod filter;
mod generator;
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
        #[arg(long, num_args(1..), value_delimiter(','))]
        batch: Option<Vec<u64>>,
        #[arg(short, long, alias = "rpc-url")]
        rpc: Option<String>,
        #[arg(long)]
        output_dir: Option<String>,
    },
    /// 从已拉取的块数据构建地址过滤器（BinaryFuse16）
    BuildFilter {
        #[arg(long, num_args(0..), value_delimiter(','))]
        batch: Option<Vec<u64>>,
        /// 指定 data 目录（默认用 config 的 data_dir），ranges=data/fetcher/ranges，BF 输出到 data/fetcher
        #[arg(long)]
        data_dir: Option<String>,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        output: Option<String>,
    },
    /// 查询地址是否在 BF 过滤器中
    FilterQuery {
        #[arg(required = true)]
        address: String,
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
        /// worker 线程数（默认 4）
        #[arg(long, default_value = "4")]
        threads: usize,
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
        Commands::Fetch {
            batch,
            rpc,
            output_dir,
        } => cmd_fetch(cfg, batch.as_deref(), rpc, output_dir, &cli.config),
        Commands::BuildFilter {
            batch,
            data_dir,
            source,
            output,
        } => cmd_build_filter(
            cfg,
            batch.as_deref(),
            data_dir.as_deref(),
            source.as_deref(),
            output.as_deref(),
        ),
        Commands::FilterQuery { address, filter } => {
            cmd_filter_query(cfg, &address, filter.as_deref())
        }
        Commands::FetchTest { rpc, block } => cmd_fetch_test(cfg, rpc, block),
        Commands::Collide { threads } => cmd_collide(cfg, threads),
        Commands::IdInfo { id, all } => cmd_id_info(cfg, id, all),
    }
}

fn load_config(cli: &Cli) -> config::AppConfig {
    config::AppConfig::load(std::path::Path::new(&cli.config))
}

fn cmd_init(cfg: config::AppConfig) -> Result<()> {
    cfg.ensure_dirs()?;
    collider::write_new_seed(&cfg.generator_seed_path())?;
    println!(
        "birdhash init: data_dir={} | 已重新生成 {}",
        cfg.data_dir.display(),
        cfg.generator_seed_path().display()
    );
    Ok(())
}

const SEGMENT_SIZE: u64 = 100_000;

fn cmd_fetch(
    cfg: config::AppConfig,
    batches: Option<&[u64]>,
    rpc_cli: Option<String>,
    output_dir: Option<String>,
    _config_path: &str,
) -> Result<()> {
    let rpc_urls = resolve_rpc_urls(&cfg, rpc_cli.clone())?;
    cfg.ensure_dirs()?;
    let out_root = output_dir
        .clone()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| cfg.fetcher_ranges_dir());
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
    fetcher::run_fetch_range(
        &out_root,
        start_block,
        end_block,
        &rpc_urls,
        cfg.rpc_timeout_secs,
        cfg.rpc_batch_size,
        Some(&prefix),
        None,
        false,
    )?;
    if batch == total_batches && cfg.poll_interval_secs > 0 {
        loop {
            print!(
                "\r  batch={} height={} blocks={} (polling every {}s)   ",
                batch,
                end_block,
                end_block - (batch - 1) * SEGMENT_SIZE,
                cfg.poll_interval_secs
            );
            let _ = std::io::stdout().flush();
            std::thread::sleep(std::time::Duration::from_secs(cfg.poll_interval_secs));
            let new_latest = pool.get_latest_block_number()?;
            if new_latest <= end_block {
                continue;
            }
            fetcher::run_fetch_range(
                &out_root,
                end_block + 1,
                new_latest,
                &rpc_urls,
                cfg.rpc_timeout_secs,
                cfg.rpc_batch_size,
                Some(&prefix),
                None,
                true,
            )?;
            end_block = new_latest;
        }
    }
    Ok(())
}

fn cmd_build_filter(
    cfg: config::AppConfig,
    batch: Option<&[u64]>,
    data_dir: Option<&str>,
    source: Option<&str>,
    output: Option<&str>,
) -> Result<()> {
    use crate::filter;
    use std::io::BufRead;
    let (range_root, out_dir) = if let Some(d) = data_dir {
        let base = std::path::PathBuf::from(d);
        (base.join("fetcher").join("ranges"), base.join("fetcher"))
    } else {
        (
            source
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| cfg.fetcher_ranges_dir()),
            output
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| cfg.fetcher_dir()),
        )
    };
    std::fs::create_dir_all(&out_dir)?;

    // 读取 meta，确认「当前正在写入」的批次，不合并该批次的 chunk
    let meta = fetcher::load_meta(&range_root).unwrap_or_default();
    let current_batch = meta.current_batch;
    let current_batch_through = meta.current_batch_fetched_through_block;
    let current_batch_seg_end = current_batch * fetcher::SEGMENT_SIZE - 1;
    // 当前批次未写满时不合并其 chunk
    let current_batch_done = current_batch_through >= current_batch_seg_end;

    // 确定要处理的批次列表
    let all_batches = fetcher::list_batches_in_ranges(&range_root)?;
    anyhow::ensure!(
        !all_batches.is_empty(),
        "no batches found under {}",
        range_root.display()
    );
    let batch_list: Vec<u64> = match batch {
        Some(ids) if !ids.is_empty() => {
            for &id in ids {
                anyhow::ensure!(
                    all_batches.contains(&id),
                    "batch {} not found in ranges",
                    id
                );
            }
            ids.to_vec()
        }
        _ => all_batches.clone(),
    };

    // 第一步：合并各批次的小文件（排除「当前写入中」的批次）
    let mut merged_count = 0usize;
    for &bid in &batch_list {
        let is_current = bid == current_batch && !current_batch_done;
        if is_current {
            println!(
                "  batch={} 跳过合并（当前写入中，through={}/{}）",
                bid, current_batch_through, current_batch_seg_end
            );
            continue;
        }
        let seg_s = (bid.saturating_sub(1)) * fetcher::SEGMENT_SIZE;
        let range_dir = range_root.join(fetcher::seg_dir_name(seg_s));
        let has_chunks =
            (0..100u32).any(|i| range_dir.join(format!("chunk_{:03}.jsonl", i)).exists());
        if has_chunks {
            let lines = fetcher::merge_range_dir(&range_dir)?;
            println!("  batch={} 合并完成，共 {} 行", bid, lines);
            merged_count += 1;
        }
    }
    if merged_count > 0 {
        println!("合并了 {} 个批次的小文件", merged_count);
    }

    // 第二步：从所有可读取的批次（只读 blocks.jsonl）收集指纹，生成 BF
    // 当前写入中的批次只有 chunk，不读入 BF（保持 BF 一致性）
    let mut set1 = std::collections::HashSet::<u64>::new();
    let mut set2 = std::collections::HashSet::<u64>::new();
    let mut set3 = std::collections::HashSet::<u64>::new();
    let mut included_batches: Vec<u64> = Vec::new();
    for &bid in &batch_list {
        let is_current_writing = bid == current_batch && !current_batch_done;
        if is_current_writing {
            continue;
        }
        let seg_s = (bid.saturating_sub(1)) * fetcher::SEGMENT_SIZE;
        let range_dir = range_root.join(fetcher::seg_dir_name(seg_s));
        let blocks_path = range_dir.join("blocks.jsonl");
        if !blocks_path.exists() {
            continue;
        }
        let f = std::fs::File::open(&blocks_path)?;
        for line in std::io::BufReader::new(f).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let block: serde_json::Value = serde_json::from_str(&line)?;
            for addr in fetcher::extract_addresses_from_block(&block) {
                set1.insert(filter::addr_to_u64(&addr));
                set2.insert(filter::addr_to_u64_alt(&addr));
                set3.insert(filter::addr_to_u64_alt2(&addr));
            }
        }
        included_batches.push(bid);
    }
    anyhow::ensure!(
        !included_batches.is_empty(),
        "没有可用于生成 BF 的批次（当前批次可能仍在写入中）"
    );

    let keys1: Vec<u64> = set1.into_iter().collect();
    let keys2: Vec<u64> = set2.into_iter().collect();
    let keys3: Vec<u64> = set3.into_iter().collect();
    let entries = keys1.len();
    let min_b = *included_batches.iter().min().unwrap();
    let max_b = *included_batches.iter().max().unwrap();
    // 连续批次用 filter.min-max；不连续时加 nN 表示实际批次数，避免与「连续区间」混淆
    let contiguous = included_batches.len() == (max_b - min_b + 1) as usize;
    let base_name = if contiguous {
        format!("filter.{}-{}", min_b, max_b)
    } else {
        format!("filter.{}-{}-n{}", min_b, max_b, included_batches.len())
    };
    let out_path = out_dir.join(format!("{}.bin", base_name));
    let out_alt = out_dir.join(format!("{}.alt.bin", base_name));
    let out_alt2 = out_dir.join(format!("{}.alt2.bin", base_name));
    let f1 = filter::build_fuse16(&keys1)?;
    let f2 = filter::build_fuse16(&keys2)?;
    let f3 = filter::build_fuse16(&keys3)?;
    filter::save_fuse16(&f1, &out_path)?;
    filter::save_fuse16(&f2, &out_alt)?;
    filter::save_fuse16(&f3, &out_alt2)?;
    fetcher::save_fetch_filter_meta(&out_path, &included_batches, max_b * fetcher::SEGMENT_SIZE)?;
    println!(
        "BuildFilter done: batches={:?} entries={} -> {}",
        included_batches,
        entries,
        out_path.display()
    );
    Ok(())
}

fn cmd_filter_query(
    cfg: config::AppConfig,
    address: &str,
    filter_path: Option<&str>,
) -> Result<()> {
    let addr = fetcher::parse_hex_addr(address.trim())
        .ok_or_else(|| anyhow::anyhow!("无效地址: {}", address))?;
    let hit = match filter_path {
        None => {
            let dir = cfg.fetcher_dir();
            if !dir.exists() {
                anyhow::bail!("fetcher 目录不存在: {}", dir.display());
            }
            collider::bf_contains(&dir, &addr)?
        }
        Some(s) => {
            let p = std::path::Path::new(s);
            if !p.exists() {
                anyhow::bail!("路径不存在: {}", p.display());
            }
            if p.is_dir() {
                collider::bf_contains(p, &addr)?
            } else {
                let f = filter::load_fuse16(p)?;
                f.contains(&filter::addr_to_u64(&addr))
            }
        }
    };
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

fn cmd_collide(cfg: config::AppConfig, threads: usize) -> Result<()> {
    collider::run_collider(&cfg, threads)
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
