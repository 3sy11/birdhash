//! Distributed L3 scan: distribute / worker / collect.
//!
//! Coordinator splits purged counter ranges into task_*.json files.
//! Each task is self-contained: master_seed + counter range + fetch addresses path.
//! Worker processes a single task (stateless binary, resumable via worker state file).
//! Collector merges all worker results into final hits.

use crate::cursor::{self, ScanTask, ScanWorkerState};
use crate::fetcher;
use crate::keygen::{Address, KeyGen};
use anyhow::{Context, Result};
use rayon::prelude::*;
use std::collections::HashSet;
use std::io::Write;
use std::path::Path;

const WORKER_CHUNK: u64 = 500_000; // addresses per rayon batch in worker
const WORKER_SAVE_INTERVAL: u64 = 10_000_000; // save state every N counters

/// Split a counter range [start, end) into chunk-sized tasks, write task JSON files.
pub fn distribute(
    range_start: u64,
    range_end: u64,
    chunk_size: u64,
    master_seed_hex: &str,
    fetch_addrs_path: &str,
    fetch_count: u64,
    out_dir: &str,
) -> Result<u64> {
    std::fs::create_dir_all(out_dir)?;
    let mut task_id = 0u64;
    let mut pos = range_start;
    while pos < range_end {
        let end = (pos + chunk_size).min(range_end);
        let task = ScanTask {
            task_id,
            master_seed: master_seed_hex.to_string(),
            counter_start: pos,
            counter_end: end,
            fetch_addresses_file: fetch_addrs_path.to_string(),
            fetch_count,
        };
        let path = Path::new(out_dir).join(format!("task_{:05}.json", task_id));
        cursor::save_cursor(&task, &path)?;
        task_id += 1;
        pos = end;
    }
    println!("Distributed {} tasks to {}/", task_id, out_dir);
    Ok(task_id)
}

/// Process a single scan task. Worker is stateless + resumable.
pub fn worker(task_path: &str, output_dir: &str) -> Result<ScanWorkerState> {
    let task: ScanTask = cursor::load_cursor(Path::new(task_path))
        .with_context(|| format!("load task {}", task_path))?;
    let seed_bytes = hex::decode(&task.master_seed).with_context(|| "decode master_seed hex")?;
    anyhow::ensure!(seed_bytes.len() == 32, "master_seed must be 32 bytes");
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&seed_bytes);
    let kg = KeyGen::new(seed);

    // Load fetch addresses
    let fetch_addrs = load_fetch_addrs_for_scan(&task.fetch_addresses_file)?;
    let fetch_set: HashSet<Address> = fetch_addrs.into_iter().collect();
    anyhow::ensure!(!fetch_set.is_empty(), "no fetch addresses to scan against");

    // Resume from previous state if exists
    let state_path = Path::new(output_dir).join(format!("state_{:05}.json", task.task_id));
    let mut state: ScanWorkerState = if state_path.exists() {
        let s: ScanWorkerState = cursor::load_cursor(&state_path)?;
        println!(
            "Resuming task {} from counter {}",
            task.task_id, s.last_processed
        );
        s
    } else {
        ScanWorkerState {
            task_id: task.task_id,
            last_processed: task.counter_start,
            status: "running".into(),
            hits: vec![],
        }
    };

    std::fs::create_dir_all(output_dir)?;
    let t = std::time::Instant::now();
    let mut pos = state.last_processed;
    println!(
        "Worker task {} | range [{}, {}) | {} fetch addrs",
        task.task_id,
        pos,
        task.counter_end,
        fetch_set.len()
    );

    while pos < task.counter_end {
        let chunk_end = (pos + WORKER_CHUNK).min(task.counter_end);
        let hits: Vec<(u64, Address)> = (pos..chunk_end)
            .into_par_iter()
            .filter_map(|c| {
                let addr = kg.derive_address(c)?;
                if fetch_set.contains(&addr) {
                    Some((c, addr))
                } else {
                    None
                }
            })
            .collect();

        for (counter, addr) in &hits {
            let kp = kg
                .derive_keypair(*counter)
                .expect("keypair for hit counter");
            state.hits.push((
                *counter,
                format!("{}:{}", hex::encode(kp.privkey), hex::encode(addr)),
            ));
        }
        pos = chunk_end;
        state.last_processed = pos;

        // Periodic state save
        if (pos - task.counter_start) % WORKER_SAVE_INTERVAL < WORKER_CHUNK {
            state.status = "running".into();
            cursor::save_cursor(&state, &state_path)?;
            let pct = (pos - task.counter_start) as f64
                / (task.counter_end - task.counter_start) as f64
                * 100.0;
            let rate = (pos - task.counter_start) as f64 / t.elapsed().as_secs_f64().max(0.001);
            print!(
                "\r  task {} | {:.1}% | {:.1}M/s | {} hits        ",
                task.task_id,
                pct,
                rate / 1e6,
                state.hits.len()
            );
            let _ = std::io::stdout().flush();
        }
    }

    state.status = "completed".into();
    state.last_processed = task.counter_end;
    cursor::save_cursor(&state, &state_path)?;

    // Write final result
    let result_path = Path::new(output_dir).join(format!("result_{:05}.json", task.task_id));
    cursor::save_cursor(&state, &result_path)?;

    println!(
        "\n  task {} completed: {} hits in {:.1}s",
        task.task_id,
        state.hits.len(),
        t.elapsed().as_secs_f64()
    );
    Ok(state)
}

/// Collect and merge results from all worker result files.
pub fn collect(results_dir: &str) -> Result<Vec<(u64, String)>> {
    let mut all_hits: Vec<(u64, String)> = Vec::new();
    let mut completed = 0u64;
    let mut total_tasks = 0u64;

    let entries: Vec<_> = std::fs::read_dir(results_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("result_"))
        .collect();

    for entry in &entries {
        total_tasks += 1;
        let state: ScanWorkerState = cursor::load_cursor(&entry.path())
            .with_context(|| format!("load result {}", entry.path().display()))?;
        if state.status == "completed" {
            completed += 1;
        }
        all_hits.extend(state.hits.iter().cloned());
    }

    all_hits.sort_by_key(|(c, _)| *c);
    println!(
        "Collected {} results: {}/{} completed, {} total hits",
        entries.len(),
        completed,
        total_tasks,
        all_hits.len()
    );

    // Write merged hits to hits.txt in results_dir
    if !all_hits.is_empty() {
        let hits_path = Path::new(results_dir).join("scan_hits.txt");
        let mut f = std::fs::File::create(&hits_path)?;
        for (counter, info) in &all_hits {
            writeln!(f, "counter={} | {}", counter, info)?;
        }
        println!("Merged hits written to {}", hits_path.display());
    }
    Ok(all_hits)
}

/// Load fetch addresses from either new_addrs.bin format or flat binary file.
fn load_fetch_addrs_for_scan(path: &str) -> Result<Vec<Address>> {
    let p = Path::new(path);
    anyhow::ensure!(p.exists(), "fetch addresses file not found: {}", path);
    // Try new_addrs.bin format (8-byte header + flat addresses)
    if let Ok(addrs) = fetcher::load_new_addrs(p) {
        if !addrs.is_empty() {
            return Ok(addrs);
        }
    }
    // Fallback: try flat all_addrs.bin format (no header, pure [u8;20] concat)
    let store = fetcher::AddressStore::open(p)?;
    store.read_all_addresses()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("birdhash_scan_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn distribute_creates_task_files() {
        let dir = test_dir("dist");
        let out = dir.join("tasks");
        let n = distribute(
            0,
            100_000,
            30_000,
            "aa".repeat(32).as_str(),
            "addrs.bin",
            100,
            out.to_str().unwrap(),
        )
        .unwrap();
        assert_eq!(n, 4); // 0-30k, 30k-60k, 60k-90k, 90k-100k
        assert!(out.join("task_00000.json").exists());
        assert!(out.join("task_00003.json").exists());
        let task: ScanTask = cursor::load_cursor(&out.join("task_00000.json")).unwrap();
        assert_eq!(task.counter_start, 0);
        assert_eq!(task.counter_end, 30_000);
        let task3: ScanTask = cursor::load_cursor(&out.join("task_00003.json")).unwrap();
        assert_eq!(task3.counter_start, 90_000);
        assert_eq!(task3.counter_end, 100_000);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn worker_finds_planted_hit() {
        let dir = test_dir("worker");
        let seed = [0xEEu8; 32];
        let kg = KeyGen::new(seed);
        let planted = kg.derive_address(55).unwrap();

        // Write fetch addresses as new_addrs.bin
        let addrs_path = dir.join("fetch.bin");
        fetcher::save_new_addrs(&[planted], &addrs_path).unwrap();

        // Create task
        let task = ScanTask {
            task_id: 0,
            master_seed: hex::encode(seed),
            counter_start: 0,
            counter_end: 100,
            fetch_addresses_file: addrs_path.to_str().unwrap().to_string(),
            fetch_count: 1,
        };
        let task_path = dir.join("task.json");
        cursor::save_cursor(&task, &task_path).unwrap();

        let out_dir = dir.join("results");
        let state = worker(task_path.to_str().unwrap(), out_dir.to_str().unwrap()).unwrap();
        assert_eq!(state.status, "completed");
        assert_eq!(state.hits.len(), 1);
        assert_eq!(state.hits[0].0, 55);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn worker_resume_from_state() {
        let dir = test_dir("resume");
        let seed = [0xFFu8; 32];
        let kg = KeyGen::new(seed);
        let planted = kg.derive_address(80).unwrap();

        let addrs_path = dir.join("fetch.bin");
        fetcher::save_new_addrs(&[planted], &addrs_path).unwrap();

        let task = ScanTask {
            task_id: 0,
            master_seed: hex::encode(seed),
            counter_start: 0,
            counter_end: 100,
            fetch_addresses_file: addrs_path.to_str().unwrap().to_string(),
            fetch_count: 1,
        };
        let task_path = dir.join("task.json");
        cursor::save_cursor(&task, &task_path).unwrap();

        // Simulate partial completion: state says we processed up to 50
        let out_dir = dir.join("results");
        std::fs::create_dir_all(&out_dir).unwrap();
        let partial = ScanWorkerState {
            task_id: 0,
            last_processed: 50,
            status: "interrupted".into(),
            hits: vec![],
        };
        cursor::save_cursor(&partial, &out_dir.join("state_00000.json")).unwrap();

        let state = worker(task_path.to_str().unwrap(), out_dir.to_str().unwrap()).unwrap();
        assert_eq!(state.status, "completed");
        assert_eq!(state.hits.len(), 1); // should find hit at 80 (>50, so not skipped)
        assert_eq!(state.hits[0].0, 80);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn collect_merges_results() {
        let dir = test_dir("collect");
        // Write 2 result files
        let s1 = ScanWorkerState {
            task_id: 0,
            last_processed: 100,
            status: "completed".into(),
            hits: vec![(42, "pk1:addr1".into())],
        };
        let s2 = ScanWorkerState {
            task_id: 1,
            last_processed: 200,
            status: "completed".into(),
            hits: vec![(150, "pk2:addr2".into())],
        };
        cursor::save_cursor(&s1, &dir.join("result_00000.json")).unwrap();
        cursor::save_cursor(&s2, &dir.join("result_00001.json")).unwrap();

        let hits = collect(dir.to_str().unwrap()).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].0, 42); // sorted by counter
        assert_eq!(hits[1].0, 150);
        assert!(dir.join("scan_hits.txt").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
