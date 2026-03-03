//! Disk lifecycle management: L1→L2 downgrade, L2→L3 purge.
//!
//! L1→L2: regenerate shard addresses → build Bloom 10% → save archive → delete L1 filter
//!         净省 41% (965 MB → 572 MB per shard)
//! L2→L3: delete archive → record purged_epoch in generator_cursor
//!         释放全部空间, 后续靠分布式回查

use crate::config::AppConfig;
use crate::cursor::{self, GeneratorCursor, PurgedEpoch};
use crate::filter;
use crate::keygen::KeyGen;
use anyhow::Result;
use chrono::Utc;
use std::path::Path;

/// Downgrade oldest L1 shards to L2 (Bloom 10%), keeping `keep_last` most recent L1 shards.
pub fn downgrade_l1_to_l2(config: &AppConfig, seed: &[u8; 32], keep_last: usize) -> Result<u64> {
    let mut gen_cur: GeneratorCursor = cursor::load_or_default(&config.gen_cursor_path());
    let total_l1 = gen_cur.l1_shard_count;
    if total_l1 <= keep_last as u64 {
        println!(
            "Nothing to downgrade: {} L1 shards, keep_last={}",
            total_l1, keep_last
        );
        return Ok(0);
    }
    let first_l1 = gen_cur.l2_shard_count; // L2 shards [0, l2_shard_count) already archived
    let last_to_downgrade = total_l1 - keep_last as u64;
    if first_l1 >= last_to_downgrade {
        println!(
            "No new shards to downgrade (already archived up to shard {})",
            first_l1
        );
        return Ok(0);
    }
    let kg = KeyGen::new(*seed);
    let mut downgraded = 0u64;
    for shard_id in first_l1..last_to_downgrade {
        let l1_path = config.l1_filter_path(shard_id);
        let l2_path = config.l2_archive_path(shard_id);
        if !l1_path.exists() {
            continue;
        }
        if l2_path.exists() {
            continue;
        } // already archived

        println!(
            "  downgrade shard {} : L1 → L2 (regenerate → Bloom 10%)...",
            shard_id
        );
        let t = std::time::Instant::now();

        // Regenerate shard addresses, build Bloom
        let shard_start = shard_id * config.shard_size;
        let shard_end = shard_start + config.shard_size;
        let mut bloom = filter::BloomFilter::new(config.shard_size as usize);
        for c in shard_start..shard_end {
            if let Some(a) = kg.derive_address(c) {
                bloom.insert(filter::addr_to_u64(&a));
            }
        }
        bloom.save(&l2_path)?;
        // Delete L1 filter
        std::fs::remove_file(&l1_path)?;
        downgraded += 1;
        gen_cur.l2_shard_count = shard_id + 1;
        gen_cur.last_updated = Utc::now();
        cursor::save_cursor(&gen_cur, &config.gen_cursor_path())?;
        let l2_size = std::fs::metadata(&l2_path).map(|m| m.len()).unwrap_or(0);
        println!(
            "    done in {:.1}s | L2={:.1} MB | freed ~{:.0} MB",
            t.elapsed().as_secs_f64(),
            l2_size as f64 / 1e6,
            965.0 - l2_size as f64 / 1e6
        );
    }
    println!("Downgraded {} shards L1→L2", downgraded);
    Ok(downgraded)
}

/// Purge oldest L2 shards to L3 (delete archive, record purged_epoch in cursor).
pub fn purge_l2_to_l3(config: &AppConfig, keep_last_l2: usize) -> Result<u64> {
    let mut gen_cur: GeneratorCursor = cursor::load_or_default(&config.gen_cursor_path());
    let total_l2 = gen_cur.l2_shard_count;
    // Find the range of L2 shards that still exist on disk
    let first_existing = gen_cur
        .purged_epochs
        .iter()
        .map(|e| e.counter_range.1 / config.shard_size)
        .max()
        .unwrap_or(0);
    let last_to_purge = total_l2.saturating_sub(keep_last_l2 as u64);
    if first_existing >= last_to_purge {
        println!(
            "Nothing to purge (already purged up to shard {})",
            first_existing
        );
        return Ok(0);
    }
    let mut purged = 0u64;
    for shard_id in first_existing..last_to_purge {
        let l2_path = config.l2_archive_path(shard_id);
        if !l2_path.exists() {
            continue;
        }
        println!(
            "  purge shard {} : L2 → L3 (delete archive, record epoch)...",
            shard_id
        );
        std::fs::remove_file(&l2_path)?;
        let counter_start = shard_id * config.shard_size;
        let counter_end = counter_start + config.shard_size;
        gen_cur.purged_epochs.push(PurgedEpoch {
            counter_range: (counter_start, counter_end),
            purged_at: Utc::now(),
            collided_with_fetch_version: 0, // to be updated by collider
        });
        purged += 1;
    }
    if purged > 0 {
        gen_cur.last_updated = Utc::now();
        cursor::save_cursor(&gen_cur, &config.gen_cursor_path())?;
    }
    println!("Purged {} shards L2→L3", purged);
    Ok(purged)
}

/// Calculate total disk usage of generator dir.
pub fn disk_usage(config: &AppConfig) -> (u64, u64) {
    let l1 = dir_prefix_size(&config.generator_dir(), "filter_gen_");
    let l2 = dir_prefix_size(&config.generator_dir(), "archive_gen_");
    (l1, l2)
}

fn dir_prefix_size(dir: &Path, prefix: &str) -> u64 {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keygen;

    fn test_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("birdhash_lc_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn downgrade_l1_to_l2_creates_bloom() {
        let dir = test_dir("downgrade");
        let seed = [0xCCu8; 32];
        let shard_size = 100u64;
        let cfg = AppConfig {
            data_dir: dir.clone(),
            shard_size,
            ..AppConfig::default()
        };
        cfg.ensure_dirs().unwrap();
        let kg = KeyGen::new(seed);

        // Create 3 L1 shards
        for sid in 0..3u64 {
            let fps: Vec<u64> = (sid * shard_size..(sid + 1) * shard_size)
                .filter_map(|c| kg.derive_address(c).map(|a| filter::addr_to_u64(&a)))
                .collect();
            let f = filter::build_fuse8(&fps).unwrap();
            filter::save_fuse8(&f, &cfg.l1_filter_path(sid)).unwrap();
        }
        let mut gen_cur = cursor::GeneratorCursor::default();
        gen_cur.l1_shard_count = 3;
        gen_cur.master_seed_hash = keygen::seed_hash_id(&seed);
        cursor::save_cursor(&gen_cur, &cfg.gen_cursor_path()).unwrap();

        // Downgrade keeping last 1
        let n = downgrade_l1_to_l2(&cfg, &seed, 1).unwrap();
        assert_eq!(n, 2); // shards 0,1 downgraded
        assert!(!cfg.l1_filter_path(0).exists()); // L1 deleted
        assert!(!cfg.l1_filter_path(1).exists());
        assert!(cfg.l1_filter_path(2).exists()); // kept
        assert!(cfg.l2_archive_path(0).exists()); // L2 created
        assert!(cfg.l2_archive_path(1).exists());

        // Verify L2 bloom contains the shard addresses
        let bloom = filter::BloomFilter::load(&cfg.l2_archive_path(0)).unwrap();
        let addr0 = kg.derive_address(0).unwrap();
        assert!(bloom.contains(filter::addr_to_u64(&addr0)));

        let gen_cur2: GeneratorCursor = cursor::load_cursor(&cfg.gen_cursor_path()).unwrap();
        assert_eq!(gen_cur2.l2_shard_count, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn purge_l2_to_l3_records_epoch() {
        let dir = test_dir("purge");
        let _seed = [0xDDu8; 32];
        let shard_size = 50u64;
        let cfg = AppConfig {
            data_dir: dir.clone(),
            shard_size,
            ..AppConfig::default()
        };
        cfg.ensure_dirs().unwrap();

        // Create 3 L2 archives
        for sid in 0..3u64 {
            let mut bloom = filter::BloomFilter::new(shard_size as usize);
            for i in 0..shard_size {
                bloom.insert(i);
            }
            bloom.save(&cfg.l2_archive_path(sid)).unwrap();
        }
        let mut gen_cur = cursor::GeneratorCursor::default();
        gen_cur.l1_shard_count = 3;
        gen_cur.l2_shard_count = 3;
        cursor::save_cursor(&gen_cur, &cfg.gen_cursor_path()).unwrap();

        // Purge keeping last 1
        let n = purge_l2_to_l3(&cfg, 1).unwrap();
        assert_eq!(n, 2);
        assert!(!cfg.l2_archive_path(0).exists());
        assert!(!cfg.l2_archive_path(1).exists());
        assert!(cfg.l2_archive_path(2).exists()); // kept

        let gen_cur2: GeneratorCursor = cursor::load_cursor(&cfg.gen_cursor_path()).unwrap();
        assert_eq!(gen_cur2.purged_epochs.len(), 2);
        assert_eq!(gen_cur2.purged_epochs[0].counter_range, (0, shard_size));
        assert_eq!(
            gen_cur2.purged_epochs[1].counter_range,
            (shard_size, shard_size * 2)
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disk_usage_counts_correctly() {
        let dir = test_dir("diskusage");
        let cfg = AppConfig {
            data_dir: dir.clone(),
            shard_size: 50,
            ..AppConfig::default()
        };
        cfg.ensure_dirs().unwrap();
        // Write some dummy filter files
        std::fs::write(cfg.l1_filter_path(0), &[0u8; 100]).unwrap();
        std::fs::write(cfg.l2_archive_path(0), &[0u8; 200]).unwrap();
        let (l1, l2) = disk_usage(&cfg);
        assert_eq!(l1, 100);
        assert_eq!(l2, 200);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
