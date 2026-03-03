//! Generator: deterministic key generation → BinaryFuse8 L1 shards.
//!
//! Main loop: rayon parallel derive → accumulate u64 fingerprints → at shard_size build
//! BinaryFuse8 → flush to disk → update cursor. Supports Ctrl+C graceful shutdown and
//! resume from cursor (re-generates partial shard on restart).

use crate::config::AppConfig;
use crate::cursor::{self, GeneratorCursor};
use crate::filter;
use crate::keygen::KeyGen;
use anyhow::Result;
use chrono::Utc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use xorf::Filter as XorFilter;

const BATCH_SIZE: u64 = 100_000;
const CURSOR_SAVE_INTERVAL_SECS: u64 = 10;
const STATS_INTERVAL_SECS: u64 = 3;

pub struct Generator {
    config: AppConfig,
    cursor: GeneratorCursor,
    keygen: KeyGen,
    shard_fps: Vec<u64>,
    fetch_filter: Option<filter::BinaryFuse16>,
    shutdown: Arc<AtomicBool>,
    start_time: Instant,
    last_stats: Instant,
    last_cursor_save: Instant,
    session_generated: u64,
    gen_vs_fetch_candidates: u64,
}

impl Generator {
    pub fn new(config: AppConfig, cursor: GeneratorCursor, seed: [u8; 32]) -> Self {
        let cap = (config.shard_size as usize).min(64 * 1024 * 1024); // pre-alloc up to 512 MB
        Self {
            config,
            cursor,
            keygen: KeyGen::new(seed),
            shard_fps: Vec::with_capacity(cap),
            fetch_filter: None,
            shutdown: Arc::new(AtomicBool::new(false)),
            start_time: Instant::now(),
            last_stats: Instant::now(),
            last_cursor_save: Instant::now(),
            session_generated: 0,
            gen_vs_fetch_candidates: 0,
        }
    }

    /// Load fetch filter for real-time gen_vs_fetch collision (optional).
    pub fn load_fetch_filter(&mut self) -> Result<bool> {
        let path = self.config.fetch_filter_path();
        if path.exists() {
            self.fetch_filter = Some(filter::load_fuse16(&path)?);
            log::info!("loaded fetch filter from {}", path.display());
            Ok(true)
        } else {
            log::info!("no fetch filter found, gen_vs_fetch disabled");
            Ok(false)
        }
    }

    pub fn run(&mut self) -> Result<()> {
        self.setup_ctrlc();
        self.resume_partial_shard()?;
        println!(
            "Generator started | counter={} shard={} entries={}/{} threads={}",
            self.cursor.current_counter,
            self.cursor.l1_shard_count,
            self.cursor.current_shard_entries,
            self.config.shard_size,
            rayon::current_num_threads()
        );

        while !self.shutdown.load(Ordering::Relaxed) {
            let shard_remaining = self.config.shard_size - self.cursor.current_shard_entries;
            let batch = BATCH_SIZE.min(shard_remaining);
            let batch_start = self.cursor.current_counter;
            let batch_end = batch_start + batch;

            // Parallel generation → fingerprints
            let fps = self.keygen.par_batch_fingerprints(batch_start, batch_end);
            let count = fps.len() as u64;

            // Optional gen_vs_fetch: check each fingerprint against fetch filter
            if let Some(ref ff) = self.fetch_filter {
                for &fp in &fps {
                    if ff.contains(&fp) {
                        self.gen_vs_fetch_candidates += 1;
                        // Real verification deferred to Collider; log candidates periodically
                    }
                }
            }

            self.shard_fps.extend_from_slice(&fps);
            self.cursor.current_counter = batch_end;
            self.cursor.current_shard_entries += count;
            self.cursor.total_generated += count;
            self.session_generated += count;

            // Shard full → build BinaryFuse8, save, advance
            if self.cursor.current_shard_entries >= self.config.shard_size {
                self.flush_shard()?;
            }

            self.maybe_print_stats();
            self.maybe_save_cursor()?;
        }

        // Graceful shutdown: save cursor (partial shard fingerprints lost, re-generated on resume)
        self.save_cursor()?;
        self.print_stats_line();
        println!("Generator stopped gracefully.");
        Ok(())
    }

    /// On restart with current_shard_entries > 0, re-generate partial shard fingerprints.
    fn resume_partial_shard(&mut self) -> Result<()> {
        let partial = self.cursor.current_shard_entries;
        if partial == 0 {
            return Ok(());
        }
        let shard_start = self.cursor.current_counter - partial;
        println!(
            "Resuming: re-generating {} partial shard entries [{}, {})...",
            partial, shard_start, self.cursor.current_counter
        );
        let t = Instant::now();
        self.shard_fps = self
            .keygen
            .par_batch_fingerprints(shard_start, self.cursor.current_counter);
        println!(
            "Resumed {} entries in {:.1}s",
            self.shard_fps.len(),
            t.elapsed().as_secs_f64()
        );
        Ok(())
    }

    fn flush_shard(&mut self) -> Result<()> {
        let shard_id = self.cursor.l1_shard_count;
        let path = self.config.l1_filter_path(shard_id);
        let n = self.shard_fps.len();
        let t = Instant::now();
        println!("Building BinaryFuse8 shard {} ({} entries)...", shard_id, n);
        let f = filter::build_fuse8(&self.shard_fps)?;
        let build_ms = t.elapsed().as_millis();
        filter::save_fuse8(&f, &path)?;
        let save_ms = t.elapsed().as_millis() - build_ms;
        println!(
            "  shard {} saved: {} → build {:.1}s, write {:.1}s",
            shard_id,
            path.display(),
            build_ms as f64 / 1000.0,
            save_ms as f64 / 1000.0
        );
        self.shard_fps.clear();
        self.cursor.l1_shard_count += 1;
        self.cursor.current_shard_entries = 0;
        self.save_cursor()?;
        Ok(())
    }

    fn setup_ctrlc(&self) {
        let flag = self.shutdown.clone();
        let _ = ctrlc::set_handler(move || {
            if flag.load(Ordering::Relaxed) {
                std::process::exit(1);
            }
            eprintln!("\nCtrl+C received, finishing current batch...");
            flag.store(true, Ordering::Relaxed);
        });
    }

    fn save_cursor(&mut self) -> Result<()> {
        self.cursor.last_updated = Utc::now();
        cursor::save_cursor(&self.cursor, &self.config.gen_cursor_path())?;
        self.last_cursor_save = Instant::now();
        Ok(())
    }

    fn maybe_save_cursor(&mut self) -> Result<()> {
        if self.last_cursor_save.elapsed().as_secs() >= CURSOR_SAVE_INTERVAL_SECS {
            self.save_cursor()?;
        }
        Ok(())
    }

    fn maybe_print_stats(&mut self) {
        if self.last_stats.elapsed().as_secs() >= STATS_INTERVAL_SECS {
            self.print_stats_line();
            self.last_stats = Instant::now();
        }
    }

    fn print_stats_line(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64().max(0.001);
        let rate = self.session_generated as f64 / elapsed;
        let shard_pct =
            self.cursor.current_shard_entries as f64 / self.config.shard_size as f64 * 100.0;
        print!(
            "\r  [{:.0}s] total={:.2}B rate={:.1}M/s shard={} ({:.1}%) L1_files={}",
            elapsed,
            self.cursor.total_generated as f64 / 1e9,
            rate / 1e6,
            self.cursor.l1_shard_count,
            shard_pct,
            self.cursor.l1_shard_count
        );
        if self.gen_vs_fetch_candidates > 0 {
            print!(" gvf_candidates={}", self.gen_vs_fetch_candidates);
        }
        print!("        "); // clear trailing chars
        use std::io::Write;
        let _ = std::io::stdout().flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keygen;

    fn test_config(shard_size: u64, name: &str) -> (AppConfig, [u8; 32]) {
        let dir =
            std::env::temp_dir().join(format!("birdhash_gen_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir); // clean slate
        let cfg = AppConfig {
            data_dir: dir,
            shard_size,
            ..AppConfig::default()
        };
        cfg.ensure_dirs().unwrap();
        (cfg, [0x55u8; 32])
    }

    #[test]
    fn generator_produces_shard_file() {
        let (cfg, seed) = test_config(1000, "shard");
        let mut cur = GeneratorCursor::default();
        cur.master_seed_hash = keygen::seed_hash_id(&seed);
        let mut gen = Generator::new(cfg.clone(), cur, seed);
        gen.shutdown.store(false, Ordering::Relaxed);
        // Manually run one shard cycle
        let fps = gen.keygen.par_batch_fingerprints(0, 1000);
        gen.shard_fps = fps;
        gen.cursor.current_counter = 1000;
        gen.cursor.current_shard_entries = 1000;
        gen.cursor.total_generated = 1000;
        gen.flush_shard().unwrap();
        assert_eq!(gen.cursor.l1_shard_count, 1);
        assert_eq!(gen.cursor.current_shard_entries, 0);
        assert!(cfg.l1_filter_path(0).exists());
        // Verify the filter contains the generated fingerprints
        let f = filter::load_fuse8(&cfg.l1_filter_path(0)).unwrap();
        let kg = KeyGen::new(seed);
        let addr = kg.derive_address(0).unwrap();
        assert!(f.contains(&filter::addr_to_u64(&addr)));
        // Cleanup
        let _ = std::fs::remove_dir_all(&cfg.data_dir);
    }

    #[test]
    fn generator_resume_partial_shard() {
        let (cfg, seed) = test_config(500, "resume");
        let mut cur = GeneratorCursor::default();
        cur.master_seed_hash = keygen::seed_hash_id(&seed);
        cur.current_counter = 200;
        cur.current_shard_entries = 200;
        cur.total_generated = 200;
        let mut gen = Generator::new(cfg.clone(), cur, seed);
        gen.resume_partial_shard().unwrap();
        assert_eq!(gen.shard_fps.len(), 200);
        let _ = std::fs::remove_dir_all(&cfg.data_dir);
    }

    #[test]
    fn generator_full_run_tiny() {
        let (cfg, seed) = test_config(500, "fullrun");
        let mut cur = GeneratorCursor::default();
        cur.master_seed_hash = keygen::seed_hash_id(&seed);
        let mut gen = Generator::new(cfg.clone(), cur, seed);
        // Force stop after 1 shard worth
        let stop = gen.shutdown.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(500));
            stop.store(true, Ordering::Relaxed);
        });
        gen.run().unwrap();
        assert!(gen.cursor.total_generated >= 500);
        assert!(gen.cursor.l1_shard_count >= 1);
        assert!(cfg.l1_filter_path(0).exists());
        let _ = std::fs::remove_dir_all(&cfg.data_dir);
    }
}
