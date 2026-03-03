//! Generic JSON cursor read/write for all components.
//! Atomic save (write .tmp → rename) for crash-safe persistence.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;

// ── Generator Cursor ──

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PurgedEpoch {
    pub counter_range: (u64, u64),
    pub purged_at: DateTime<Utc>,
    pub collided_with_fetch_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GeneratorCursor {
    pub master_seed_hash: String,
    pub current_counter: u64,
    pub l1_shard_count: u64,
    pub l2_shard_count: u64,
    pub current_shard_entries: u64,
    pub total_generated: u64,
    pub purged_epochs: Vec<PurgedEpoch>,
    pub last_updated: DateTime<Utc>,
}

impl Default for GeneratorCursor {
    fn default() -> Self {
        Self {
            master_seed_hash: String::new(),
            current_counter: 0,
            l1_shard_count: 0,
            l2_shard_count: 0,
            current_shard_entries: 0,
            total_generated: 0,
            purged_epochs: vec![],
            last_updated: Utc::now(),
        }
    }
}

// ── Fetcher Cursor ──
// 双游标: historical = 从创世往前已同步到的块高; realtime = 从当前往后已同步到的块高(轮询头)

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FetcherCursor {
    pub start_block: u64,
    pub end_block: u64,
    pub last_synced_block: u64,
    /// 历史同步游标: 已同步 [0, historical_synced_up_to]
    #[serde(default)]
    pub historical_synced_up_to: u64,
    /// 实时同步游标: 已同步到该块，轮询时从 realtime_synced_up_to+1 拉到 latest
    #[serde(default)]
    pub realtime_synced_up_to: u64,
    pub total_addresses: u64,
    pub filter_version: u64,
    pub new_addrs_since_last_export: u64,
    pub last_updated: DateTime<Utc>,
}

impl Default for FetcherCursor {
    fn default() -> Self {
        Self {
            start_block: 0,
            end_block: 0,
            last_synced_block: 0,
            historical_synced_up_to: 0,
            realtime_synced_up_to: 0,
            total_addresses: 0,
            filter_version: 0,
            new_addrs_since_last_export: 0,
            last_updated: Utc::now(),
        }
    }
}

// ── Collider Cursor ──

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GenVsFetchProgress {
    pub last_gen_counter: u64,
    pub fetch_filter_version: u64,
    pub total_checked: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FetchVsGenL1Progress {
    pub completed_fetch_block_range: (u64, u64),
    pub current_fetch_offset: u64,
    pub gen_l1_shard_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FetchVsGenL2Progress {
    pub gen_l2_shard_version: u64,
    pub last_checked_l2_shard: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IncrementalQueue {
    pub pending_new_l1_shards: Vec<u64>,
    pub pending_new_l2_shards: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColliderCursor {
    pub gen_vs_fetch: GenVsFetchProgress,
    pub fetch_vs_gen_l1: FetchVsGenL1Progress,
    pub fetch_vs_gen_l2: FetchVsGenL2Progress,
    pub incremental: IncrementalQueue,
    pub hits: u64,
    pub last_updated: DateTime<Utc>,
}

impl Default for ColliderCursor {
    fn default() -> Self {
        Self {
            gen_vs_fetch: GenVsFetchProgress {
                last_gen_counter: 0,
                fetch_filter_version: 0,
                total_checked: 0,
            },
            fetch_vs_gen_l1: FetchVsGenL1Progress {
                completed_fetch_block_range: (0, 0),
                current_fetch_offset: 0,
                gen_l1_shard_version: 0,
            },
            fetch_vs_gen_l2: FetchVsGenL2Progress {
                gen_l2_shard_version: 0,
                last_checked_l2_shard: 0,
            },
            incremental: IncrementalQueue {
                pending_new_l1_shards: vec![],
                pending_new_l2_shards: vec![],
            },
            hits: 0,
            last_updated: Utc::now(),
        }
    }
}

// ── Scan Worker State ──

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScanTask {
    pub task_id: u64,
    pub master_seed: String,
    pub counter_start: u64,
    pub counter_end: u64,
    pub fetch_addresses_file: String,
    pub fetch_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScanWorkerState {
    pub task_id: u64,
    pub last_processed: u64,
    pub status: String,
    pub hits: Vec<(u64, String)>,
}

// ── Generic load/save with atomic write ──

pub fn load_cursor<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let data = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&data)?)
}

/// Atomic save: write to .tmp first, then rename. Prevents corruption on crash/kill.
pub fn save_cursor<T: Serialize>(cursor: &T, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_string_pretty(cursor)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &data)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

pub fn load_or_default<T: for<'de> Deserialize<'de> + Default>(path: &Path) -> T {
    load_cursor(path).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_cursor_round_trip() {
        let cur = GeneratorCursor {
            master_seed_hash: "abcd1234".into(),
            current_counter: 999,
            l1_shard_count: 5,
            l2_shard_count: 3,
            current_shard_entries: 42,
            total_generated: 999,
            purged_epochs: vec![PurgedEpoch {
                counter_range: (0, 500),
                purged_at: Utc::now(),
                collided_with_fetch_version: 2,
            }],
            last_updated: Utc::now(),
        };
        let tmp = std::env::temp_dir().join("birdhash_test_gen_cursor.json");
        save_cursor(&cur, &tmp).unwrap();
        let loaded: GeneratorCursor = load_cursor(&tmp).unwrap();
        assert_eq!(cur, loaded);
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn fetcher_cursor_round_trip() {
        let cur = FetcherCursor {
            start_block: 0,
            end_block: 19_000_000,
            last_synced_block: 1234,
            historical_synced_up_to: 1234,
            realtime_synced_up_to: 1234,
            total_addresses: 350_000_000,
            filter_version: 3,
            new_addrs_since_last_export: 100,
            last_updated: Utc::now(),
        };
        let tmp = std::env::temp_dir().join("birdhash_test_fetch_cursor.json");
        save_cursor(&cur, &tmp).unwrap();
        let loaded: FetcherCursor = load_cursor(&tmp).unwrap();
        assert_eq!(cur, loaded);
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn collider_cursor_round_trip() {
        let cur = ColliderCursor::default();
        let tmp = std::env::temp_dir().join("birdhash_test_col_cursor.json");
        save_cursor(&cur, &tmp).unwrap();
        let loaded: ColliderCursor = load_cursor(&tmp).unwrap();
        assert_eq!(cur, loaded);
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn scan_task_round_trip() {
        let task = ScanTask {
            task_id: 42,
            master_seed: "deadbeef".into(),
            counter_start: 100,
            counter_end: 200,
            fetch_addresses_file: "addrs.bin".into(),
            fetch_count: 50,
        };
        let tmp = std::env::temp_dir().join("birdhash_test_scan_task.json");
        save_cursor(&task, &tmp).unwrap();
        let loaded: ScanTask = load_cursor(&tmp).unwrap();
        assert_eq!(task, loaded);
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn load_or_default_missing_file() {
        let path = std::env::temp_dir().join("birdhash_nonexistent_cursor.json");
        let cur: GeneratorCursor = load_or_default(&path);
        assert_eq!(cur.current_counter, 0);
    }
}
