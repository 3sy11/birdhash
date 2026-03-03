//! Configuration loading from config.toml with CLI override support.

use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub data_dir: PathBuf,
    pub shard_size: u64,
    pub l1_disk_ratio: f64,
    pub disk_watermark: f64,
    pub threads: usize,
    pub rpc_url: Option<String>,
    /// 多个 RPC 地址，失败时按序降级使用
    pub rpc_urls: Vec<String>,
    pub rpc_batch_size: usize,
    pub rpc_retry_count: u32,
    pub rpc_retry_base_ms: u64,
    /// 单次请求超时(秒)，超时或失败即换下一个 URL
    pub rpc_timeout_secs: u64,
    pub archive_segment: u64,
    /// 实时轮询间隔(秒)，0 表示只跑完历史后退出不轮询
    pub poll_interval_secs: u64,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            shard_size: 1_000_000_000,
            l1_disk_ratio: 0.6,
            disk_watermark: 0.8,
            threads: rayon::current_num_threads(),
            rpc_url: None,
            rpc_urls: vec![],
            rpc_batch_size: 10,
            rpc_retry_count: 5,
            rpc_retry_base_ms: 1000,
            rpc_timeout_secs: 30,
            archive_segment: 100_000,
            poll_interval_secs: 12,
        }
    }
}

#[derive(serde::Deserialize, Default)]
struct TomlConfig {
    general: Option<TomlGeneral>,
    generator: Option<TomlGenerator>,
    disk: Option<TomlDisk>,
    fetcher: Option<TomlFetcher>,
}
#[derive(serde::Deserialize, Default)]
struct TomlGeneral {
    data_dir: Option<String>,
    threads: Option<usize>,
}
#[derive(serde::Deserialize, Default)]
struct TomlGenerator {
    shard_size: Option<u64>,
}
#[derive(serde::Deserialize, Default)]
struct TomlDisk {
    l1_ratio: Option<f64>,
    watermark: Option<f64>,
}
#[derive(serde::Deserialize, Default)]
struct TomlFetcher {
    rpc_url: Option<String>,
    rpc_urls: Option<Vec<String>>,
    batch_size: Option<usize>,
    retry_count: Option<u32>,
    retry_base_ms: Option<u64>,
    timeout_secs: Option<u64>,
    archive_segment: Option<u64>,
    poll_interval_secs: Option<u64>,
}

impl AppConfig {
    pub fn master_seed_path(&self) -> PathBuf {
        self.data_dir.join("master_seed.key")
    }
    pub fn generator_dir(&self) -> PathBuf {
        self.data_dir.join("generator")
    }
    pub fn fetcher_dir(&self) -> PathBuf {
        self.data_dir.join("fetcher")
    }
    pub fn results_dir(&self) -> PathBuf {
        self.data_dir.join("results")
    }
    pub fn tasks_dir(&self) -> PathBuf {
        self.data_dir.join("tasks")
    }

    pub fn gen_cursor_path(&self) -> PathBuf {
        self.generator_dir().join("generator_cursor.json")
    }
    pub fn fetch_cursor_path(&self) -> PathBuf {
        self.fetcher_dir().join("fetcher_cursor.json")
    }
    pub fn collider_cursor_path(&self) -> PathBuf {
        self.results_dir().join("collider_cursor.json")
    }
    pub fn hits_path(&self) -> PathBuf {
        self.results_dir().join("hits.txt")
    }

    pub fn l1_filter_path(&self, shard_id: u64) -> PathBuf {
        self.generator_dir()
            .join(format!("filter_gen_{:08}.bin", shard_id))
    }
    pub fn l2_archive_path(&self, shard_id: u64) -> PathBuf {
        self.generator_dir()
            .join(format!("archive_gen_{:08}.bin", shard_id))
    }
    pub fn fetch_filter_path(&self) -> PathBuf {
        self.fetcher_dir().join("filter_fetch.bin")
    }
    pub fn new_addrs_path(&self) -> PathBuf {
        self.fetcher_dir().join("new_addrs.bin")
    }
    pub fn all_addrs_path(&self) -> PathBuf {
        self.fetcher_dir().join("all_addrs.bin")
    }
    pub fn archive_dir(&self) -> PathBuf {
        self.fetcher_dir().join("archives")
    }

    pub fn ensure_dirs(&self) -> anyhow::Result<()> {
        for d in [
            self.generator_dir(),
            self.fetcher_dir(),
            self.results_dir(),
            self.tasks_dir(),
            self.archive_dir(),
        ] {
            std::fs::create_dir_all(&d)?;
        }
        Ok(())
    }

    /// Load config from TOML file, falling back to defaults for missing fields.
    pub fn load(path: &Path) -> Self {
        let mut cfg = Self::default();
        let Ok(content) = std::fs::read_to_string(path) else {
            return cfg;
        };
        let Ok(toml): Result<TomlConfig, _> = toml::from_str(&content) else {
            log::warn!("failed to parse {}, using defaults", path.display());
            return cfg;
        };
        if let Some(g) = toml.general {
            if let Some(d) = g.data_dir {
                cfg.data_dir = PathBuf::from(d);
            }
            if let Some(t) = g.threads {
                if t > 0 {
                    cfg.threads = t;
                }
            }
        }
        if let Some(gen) = toml.generator {
            if let Some(s) = gen.shard_size {
                cfg.shard_size = s;
            }
        }
        if let Some(d) = toml.disk {
            if let Some(r) = d.l1_ratio { cfg.l1_disk_ratio = r; }
            if let Some(w) = d.watermark { cfg.disk_watermark = w; }
        }
        if let Some(f) = toml.fetcher {
            if let Some(ref u) = f.rpc_url { cfg.rpc_url = Some(u.clone()); }
            if let Some(ref urls) = f.rpc_urls { if !urls.is_empty() { cfg.rpc_urls = urls.clone(); } }
            if cfg.rpc_urls.is_empty() { if let Some(ref u) = f.rpc_url { cfg.rpc_urls = vec![u.clone()]; } }
            if let Some(b) = f.batch_size { if b > 0 { cfg.rpc_batch_size = b; } }
            if let Some(r) = f.retry_count { cfg.rpc_retry_count = r; }
            if let Some(ms) = f.retry_base_ms { cfg.rpc_retry_base_ms = ms; }
            if let Some(t) = f.timeout_secs { if t > 0 { cfg.rpc_timeout_secs = t; } }
            if let Some(s) = f.archive_segment { if s > 0 { cfg.archive_segment = s; } }
            if let Some(p) = f.poll_interval_secs { cfg.poll_interval_secs = p; }
        }
        cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_missing_file_returns_defaults() {
        let cfg = AppConfig::load(Path::new("/nonexistent/config.toml"));
        assert_eq!(cfg.shard_size, 1_000_000_000);
        assert_eq!(cfg.data_dir, PathBuf::from("data"));
    }

    #[test]
    fn load_partial_toml() {
        let dir = std::env::temp_dir().join(format!("birdhash_cfg_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            r#"
[general]
data_dir = "/tmp/mydata"
[generator]
shard_size = 500_000_000
"#,
        )
        .unwrap();
        let cfg = AppConfig::load(&path);
        assert_eq!(cfg.data_dir, PathBuf::from("/tmp/mydata"));
        assert_eq!(cfg.shard_size, 500_000_000);
        assert_eq!(cfg.l1_disk_ratio, 0.6); // default
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_fetcher_rpc_urls() {
        let dir = std::env::temp_dir().join(format!("birdhash_cfg_rpc_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            r#"
[fetcher]
rpc_urls = ["https://a.com", "https://b.com"]
poll_interval_secs = 30
"#,
        )
        .unwrap();
        let cfg = AppConfig::load(&path);
        assert_eq!(cfg.rpc_urls.len(), 2);
        assert_eq!(cfg.rpc_urls[0], "https://a.com");
        assert_eq!(cfg.poll_interval_secs, 30);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
