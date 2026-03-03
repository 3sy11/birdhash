//! Collider: bidirectional collision across L1 (BinaryFuse8) / L2 (Bloom) tiers.
//!
//! For each gen shard:
//!   1. Load filter → scan fetch addresses → collect candidate HashSet<Address>
//!   2. Regenerate shard range (rayon parallel) → check candidates → exact hit
//!   3. Hit → derive_keypair → append hits.txt
//!
//! Also supports local regenerate-range (L3): pure computation, no filter on disk.

use crate::config::AppConfig;
use crate::cursor::{self, ColliderCursor, GeneratorCursor};
use crate::fetcher;
use crate::filter;
use crate::keygen::{Address, KeyGen};
use anyhow::{Context, Result};
use chrono::Utc;
use rayon::prelude::*;
use std::collections::HashSet;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use xorf::Filter as XorFilter;

pub struct Collider {
    config: AppConfig,
    cursor: ColliderCursor,
    keygen: KeyGen,
    shutdown: Arc<AtomicBool>,
    start_time: Instant,
}

impl Collider {
    pub fn new(config: AppConfig, cursor: ColliderCursor, seed: [u8; 32]) -> Self {
        Self {
            config,
            cursor,
            keygen: KeyGen::new(seed),
            shutdown: Arc::new(AtomicBool::new(false)),
            start_time: Instant::now(),
        }
    }

    /// Main L1+L2 collision loop.
    pub fn run(&mut self) -> Result<()> {
        self.setup_ctrlc();
        let gen_cur: GeneratorCursor = cursor::load_or_default(&self.config.gen_cursor_path());
        let fetch_addrs = self.load_fetch_addresses()?;
        if fetch_addrs.is_empty() {
            println!("No fetch addresses found. Run `birdhash fetch` first.");
            return Ok(());
        }
        println!(
            "Collider started | {} fetch addrs | L1 shards={} L2 shards={}",
            fetch_addrs.len(),
            gen_cur.l1_shard_count,
            gen_cur.l2_shard_count
        );

        // L1 碰撞
        let l1_start = self.cursor.fetch_vs_gen_l1.gen_l1_shard_version;
        for shard_id in l1_start..gen_cur.l1_shard_count {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            let path = self.config.l1_filter_path(shard_id);
            if !path.exists() {
                continue;
            }
            let hits = self.collide_fuse8_shard(shard_id, &fetch_addrs)?;
            self.cursor.fetch_vs_gen_l1.gen_l1_shard_version = shard_id + 1;
            self.cursor.hits += hits as u64;
            self.save_cursor()?;
        }

        // L2 碰撞
        let l2_start = self.cursor.fetch_vs_gen_l2.last_checked_l2_shard;
        for shard_id in l2_start..gen_cur.l2_shard_count {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            let path = self.config.l2_archive_path(shard_id);
            if !path.exists() {
                continue;
            }
            let hits = self.collide_bloom_shard(shard_id, &fetch_addrs)?;
            self.cursor.fetch_vs_gen_l2.last_checked_l2_shard = shard_id + 1;
            self.cursor.hits += hits as u64;
            self.save_cursor()?;
        }

        self.save_cursor()?;
        println!(
            "\nCollider finished. total_hits={} elapsed={:.1}s",
            self.cursor.hits,
            self.start_time.elapsed().as_secs_f64()
        );
        Ok(())
    }

    /// Local regenerate-range (L3): no filter, pure regeneration against fetch addrs.
    pub fn regenerate_range(&mut self, start: u64, end: u64) -> Result<()> {
        self.setup_ctrlc();
        let fetch_addrs = self.load_fetch_addresses()?;
        if fetch_addrs.is_empty() {
            println!("No fetch addresses found.");
            return Ok(());
        }
        let fetch_set: HashSet<Address> = fetch_addrs.into_iter().collect();
        println!(
            "Regenerate-range [{}, {}) | {} fetch addrs in set",
            start,
            end,
            fetch_set.len()
        );
        let t = Instant::now();
        let hits = self.scan_range_vs_set(start, end, &fetch_set)?;
        println!(
            "Regenerate-range done: {} hits in {:.1}s",
            hits,
            t.elapsed().as_secs_f64()
        );
        Ok(())
    }

    // ── L1: BinaryFuse8 shard collision ──

    fn collide_fuse8_shard(&self, shard_id: u64, fetch_addrs: &[Address]) -> Result<usize> {
        let t = Instant::now();
        let path = self.config.l1_filter_path(shard_id);
        let f = filter::load_fuse8(&path).with_context(|| format!("load L1 shard {}", shard_id))?;
        // Collect candidates: fetch addrs that pass the L1 filter
        let candidates: HashSet<Address> = fetch_addrs
            .iter()
            .filter(|a| f.contains(&filter::addr_to_u64(a)))
            .copied()
            .collect();
        let cand_count = candidates.len();
        if candidates.is_empty() {
            println!(
                "  L1 shard {} | 0 candidates | skip regen | {:.1}s",
                shard_id,
                t.elapsed().as_secs_f64()
            );
            return Ok(0);
        }
        // Regenerate shard and verify
        let shard_start = shard_id * self.config.shard_size;
        let shard_end = shard_start + self.config.shard_size;
        let hits = self.regenerate_and_verify(shard_start, shard_end, &candidates)?;
        println!(
            "  L1 shard {} | {} candidates → {} hits | {:.1}s",
            shard_id,
            cand_count,
            hits.len(),
            t.elapsed().as_secs_f64()
        );
        self.record_hits(&hits)?;
        Ok(hits.len())
    }

    // ── L2: Bloom shard collision ──

    fn collide_bloom_shard(&self, shard_id: u64, fetch_addrs: &[Address]) -> Result<usize> {
        let t = Instant::now();
        let path = self.config.l2_archive_path(shard_id);
        let bloom = filter::BloomFilter::load(&path)
            .with_context(|| format!("load L2 shard {}", shard_id))?;
        let candidates: HashSet<Address> = fetch_addrs
            .iter()
            .filter(|a| bloom.contains(filter::addr_to_u64(a)))
            .copied()
            .collect();
        let cand_count = candidates.len();
        if candidates.is_empty() {
            println!(
                "  L2 shard {} | 0 candidates | skip regen | {:.1}s",
                shard_id,
                t.elapsed().as_secs_f64()
            );
            return Ok(0);
        }
        let shard_start = shard_id * self.config.shard_size;
        let shard_end = shard_start + self.config.shard_size;
        let hits = self.regenerate_and_verify(shard_start, shard_end, &candidates)?;
        println!(
            "  L2 shard {} | {} candidates → {} hits | {:.1}s",
            shard_id,
            cand_count,
            hits.len(),
            t.elapsed().as_secs_f64()
        );
        self.record_hits(&hits)?;
        Ok(hits.len())
    }

    // ── Regenerate + verify ──

    fn regenerate_and_verify(
        &self,
        start: u64,
        end: u64,
        candidates: &HashSet<Address>,
    ) -> Result<Vec<HitRecord>> {
        let hits: Vec<HitRecord> = (start..end)
            .into_par_iter()
            .filter_map(|c| {
                let addr = self.keygen.derive_address(c)?;
                if candidates.contains(&addr) {
                    let kp = self.keygen.derive_keypair(c)?;
                    Some(HitRecord {
                        counter: c,
                        privkey: kp.privkey,
                        address: kp.address,
                    })
                } else {
                    None
                }
            })
            .collect();
        Ok(hits)
    }

    /// Scan a counter range against a full fetch address set (for L3 / regenerate-range).
    fn scan_range_vs_set(
        &self,
        start: u64,
        end: u64,
        fetch_set: &HashSet<Address>,
    ) -> Result<usize> {
        const CHUNK: u64 = 1_000_000;
        let mut total_hits = 0usize;
        let mut pos = start;
        while pos < end && !self.shutdown.load(Ordering::Relaxed) {
            let chunk_end = (pos + CHUNK).min(end);
            let hits: Vec<HitRecord> = (pos..chunk_end)
                .into_par_iter()
                .filter_map(|c| {
                    let addr = self.keygen.derive_address(c)?;
                    if fetch_set.contains(&addr) {
                        let kp = self.keygen.derive_keypair(c)?;
                        Some(HitRecord {
                            counter: c,
                            privkey: kp.privkey,
                            address: kp.address,
                        })
                    } else {
                        None
                    }
                })
                .collect();
            if !hits.is_empty() {
                self.record_hits(&hits)?;
                total_hits += hits.len();
            }
            pos = chunk_end;
            let pct = (pos - start) as f64 / (end - start) as f64 * 100.0;
            print!(
                "\r  regen [{}, {}) {:.1}% hits={}        ",
                start, end, pct, total_hits
            );
            let _ = std::io::stdout().flush();
        }
        println!();
        Ok(total_hits)
    }

    // ── Hit recording ──

    fn record_hits(&self, hits: &[HitRecord]) -> Result<()> {
        if hits.is_empty() {
            return Ok(());
        }
        let path = self.config.hits_path();
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let now = Utc::now().to_rfc3339();
        for h in hits {
            writeln!(
                f,
                "{} | counter={} | privkey={} | addr=0x{}",
                now,
                h.counter,
                hex::encode(h.privkey),
                hex::encode(h.address)
            )?;
            println!(
                "\n  *** HIT *** counter={} addr=0x{}",
                h.counter,
                hex::encode(h.address)
            );
        }
        Ok(())
    }

    // ── Fetch address loading ──

    fn load_fetch_addresses(&self) -> Result<Vec<Address>> {
        let store = fetcher::AddressStore::open(&self.config.all_addrs_path())?;
        if store.count() > 0 {
            println!(
                "Loading {} fetch addresses from all_addrs.bin...",
                store.count()
            );
            return store.read_all_addresses();
        }
        let addrs = fetcher::load_new_addrs(&self.config.new_addrs_path())?;
        if !addrs.is_empty() {
            println!(
                "Loading {} fetch addresses from new_addrs.bin...",
                addrs.len()
            );
        }
        Ok(addrs)
    }

    fn setup_ctrlc(&self) {
        let flag = self.shutdown.clone();
        let _ = ctrlc::set_handler(move || {
            if flag.load(Ordering::Relaxed) {
                std::process::exit(1);
            }
            eprintln!("\nCtrl+C received, finishing current shard...");
            flag.store(true, Ordering::Relaxed);
        });
    }

    fn save_cursor(&mut self) -> Result<()> {
        self.cursor.last_updated = Utc::now();
        cursor::save_cursor(&self.cursor, &self.config.collider_cursor_path())
    }
}

struct HitRecord {
    counter: u64,
    privkey: [u8; 32],
    address: Address,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keygen;
    use crate::cursor::GeneratorCursor;

    fn test_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("birdhash_col_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn setup_collider(
        dir: &std::path::Path,
        shard_size: u64,
        seed: [u8; 32],
    ) -> (AppConfig, KeyGen) {
        let cfg = AppConfig {
            data_dir: dir.to_path_buf(),
            shard_size,
            ..AppConfig::default()
        };
        cfg.ensure_dirs().unwrap();
        (cfg, KeyGen::new(seed))
    }

    #[test]
    fn collide_l1_finds_planted_hit() {
        let dir = test_dir("l1hit");
        let seed = [0x77u8; 32];
        let shard_size = 200u64;
        let (cfg, kg) = setup_collider(&dir, shard_size, seed);

        // Generate addresses for shard 0 [0, 200), build L1 filter
        let fps: Vec<u64> = (0..shard_size)
            .filter_map(|c| kg.derive_address(c).map(|a| filter::addr_to_u64(&a)))
            .collect();
        let f = filter::build_fuse8(&fps).unwrap();
        filter::save_fuse8(&f, &cfg.l1_filter_path(0)).unwrap();

        // Plant a hit: take addr at counter=42 as a "fetch address"
        let planted_addr = kg.derive_address(42).unwrap();
        let mut fake_addrs: Vec<Address> = (0..10)
            .map(|i| {
                let mut a = [0xFFu8; 20];
                a[0] = i;
                a
            })
            .collect();
        fake_addrs.push(planted_addr);
        // Save fetch addresses
        let store_path = cfg.all_addrs_path();
        let mut store = fetcher::AddressStore::open(&store_path).unwrap();
        store.append(&fake_addrs).unwrap();

        // Save gen cursor so collider knows shard count
        let mut gen_cur = GeneratorCursor::default();
        gen_cur.l1_shard_count = 1;
        gen_cur.master_seed_hash = keygen::seed_hash_id(&seed);
        cursor::save_cursor(&gen_cur, &cfg.gen_cursor_path()).unwrap();

        // Run collider
        let col_cur = ColliderCursor::default();
        let mut collider = Collider::new(cfg.clone(), col_cur, seed);
        collider.run().unwrap();

        assert_eq!(collider.cursor.hits, 1);
        assert!(cfg.hits_path().exists());
        let content = std::fs::read_to_string(cfg.hits_path()).unwrap();
        assert!(content.contains("counter=42"));
        assert!(content.contains(&hex::encode(planted_addr)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn collide_l1_no_hits() {
        let dir = test_dir("l1nohit");
        let seed = [0x88u8; 32];
        let shard_size = 100u64;
        let (cfg, kg) = setup_collider(&dir, shard_size, seed);

        let fps: Vec<u64> = (0..shard_size)
            .filter_map(|c| kg.derive_address(c).map(|a| filter::addr_to_u64(&a)))
            .collect();
        let f = filter::build_fuse8(&fps).unwrap();
        filter::save_fuse8(&f, &cfg.l1_filter_path(0)).unwrap();

        // Fetch addresses that are NOT in the gen shard (random bytes)
        let fake_addrs: Vec<Address> = (0..20)
            .map(|i| {
                let mut a = [0xAAu8; 20];
                a[0] = i;
                a[1] = 0xBB;
                a
            })
            .collect();
        let mut store = fetcher::AddressStore::open(&cfg.all_addrs_path()).unwrap();
        store.append(&fake_addrs).unwrap();

        let mut gen_cur = GeneratorCursor::default();
        gen_cur.l1_shard_count = 1;
        cursor::save_cursor(&gen_cur, &cfg.gen_cursor_path()).unwrap();

        let mut collider = Collider::new(cfg.clone(), ColliderCursor::default(), seed);
        collider.run().unwrap();
        assert_eq!(collider.cursor.hits, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn collide_l2_bloom_finds_planted_hit() {
        let dir = test_dir("l2hit");
        let seed = [0x99u8; 32];
        let shard_size = 150u64;
        let (cfg, kg) = setup_collider(&dir, shard_size, seed);

        // Build L2 Bloom filter for shard 0
        let mut bloom = filter::BloomFilter::new(shard_size as usize);
        for c in 0..shard_size {
            if let Some(a) = kg.derive_address(c) {
                bloom.insert(filter::addr_to_u64(&a));
            }
        }
        bloom.save(&cfg.l2_archive_path(0)).unwrap();

        let planted = kg.derive_address(77).unwrap();
        let mut fake: Vec<Address> = vec![[0xDD; 20], [0xEE; 20]];
        fake.push(planted);
        let mut store = fetcher::AddressStore::open(&cfg.all_addrs_path()).unwrap();
        store.append(&fake).unwrap();

        let mut gen_cur = GeneratorCursor::default();
        gen_cur.l2_shard_count = 1;
        cursor::save_cursor(&gen_cur, &cfg.gen_cursor_path()).unwrap();

        let mut collider = Collider::new(cfg.clone(), ColliderCursor::default(), seed);
        collider.run().unwrap();
        assert_eq!(collider.cursor.hits, 1);
        let content = std::fs::read_to_string(cfg.hits_path()).unwrap();
        assert!(content.contains("counter=77"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn regenerate_range_finds_planted_hit() {
        let dir = test_dir("regen");
        let seed = [0xAAu8; 32];
        let shard_size = 100u64;
        let (cfg, kg) = setup_collider(&dir, shard_size, seed);

        let planted = kg.derive_address(55).unwrap();
        let mut store = fetcher::AddressStore::open(&cfg.all_addrs_path()).unwrap();
        store.append(&[planted]).unwrap();

        let mut collider = Collider::new(cfg.clone(), ColliderCursor::default(), seed);
        collider.regenerate_range(0, 100).unwrap();
        assert!(cfg.hits_path().exists());
        let content = std::fs::read_to_string(cfg.hits_path()).unwrap();
        assert!(content.contains("counter=55"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn hit_record_format() {
        let dir = test_dir("hitfmt");
        let seed = [0xBBu8; 32];
        let (cfg, _) = setup_collider(&dir, 100, seed);
        let collider = Collider::new(cfg.clone(), ColliderCursor::default(), seed);
        let hit = HitRecord {
            counter: 999,
            privkey: [0x42u8; 32],
            address: [0x13u8; 20],
        };
        collider.record_hits(&[hit]).unwrap();
        let content = std::fs::read_to_string(cfg.hits_path()).unwrap();
        assert!(content.contains("counter=999"));
        assert!(content.contains("privkey=4242424242"));
        assert!(content.contains("addr=0x1313131313"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
