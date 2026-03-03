//! Real-time statistics: generation speed, disk usage, shard counts, tier distribution.

use crate::config::AppConfig;
use crate::cursor;
use std::path::Path;

pub struct SystemStatus {
    pub total_generated: u64,
    pub l1_shards: u64,
    pub l2_shards: u64,
    pub l3_purged_ranges: Vec<(u64, u64)>,
    pub fetch_total_addrs: u64,
    #[allow(dead_code)]
    pub fetch_last_block: u64,
    pub fetch_historical_up_to: u64,
    pub fetch_realtime_up_to: u64,
    pub collider_hits: u64,
    pub l1_disk_bytes: u64,
    pub l2_disk_bytes: u64,
}

/// Gather system status from cursor files and disk.
pub fn gather_status(config: &AppConfig) -> anyhow::Result<SystemStatus> {
    let gen_cur: cursor::GeneratorCursor = cursor::load_or_default(&config.gen_cursor_path());
    let fetch_cur: cursor::FetcherCursor = cursor::load_or_default(&config.fetch_cursor_path());
    let col_cur: cursor::ColliderCursor = cursor::load_or_default(&config.collider_cursor_path());
    Ok(SystemStatus {
        total_generated: gen_cur.total_generated,
        l1_shards: gen_cur.l1_shard_count,
        l2_shards: gen_cur.l2_shard_count,
        l3_purged_ranges: gen_cur
            .purged_epochs
            .iter()
            .map(|e| e.counter_range)
            .collect(),
        fetch_total_addrs: fetch_cur.total_addresses,
        fetch_last_block: fetch_cur.last_synced_block,
        fetch_historical_up_to: fetch_cur.historical_synced_up_to,
        fetch_realtime_up_to: fetch_cur.realtime_synced_up_to,
        collider_hits: col_cur.hits,
        l1_disk_bytes: dir_size(&config.generator_dir(), "filter_gen_"),
        l2_disk_bytes: dir_size(&config.generator_dir(), "archive_gen_"),
    })
}

fn dir_size(dir: &Path, prefix: &str) -> u64 {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().starts_with(prefix))
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

pub fn print_status(s: &SystemStatus) {
    println!("═══ birdhash status ═══");
    println!(
        "Generated:     {} ({:.2}B)",
        s.total_generated,
        s.total_generated as f64 / 1e9
    );
    println!(
        "L1 shards:     {} ({:.2} GB)",
        s.l1_shards,
        s.l1_disk_bytes as f64 / 1e9
    );
    println!(
        "L2 archives:   {} ({:.2} GB)",
        s.l2_shards,
        s.l2_disk_bytes as f64 / 1e9
    );
    println!("L3 purged:     {} ranges", s.l3_purged_ranges.len());
    println!(
        "Fetch addrs:   {} (hist {} realtime {})",
        s.fetch_total_addrs, s.fetch_historical_up_to, s.fetch_realtime_up_to
    );
    println!("Hits:          {}", s.collider_hits);
}
