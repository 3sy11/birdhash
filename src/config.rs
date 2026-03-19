//! Configuration loading from config.toml with CLI override support.

use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub data_dir: PathBuf,
    pub assets_dir: PathBuf,
    pub threads: usize,
    pub rpc_url: Option<String>,
    pub rpc_urls: Vec<String>,
    pub rpc_batch_size: usize,
    pub rpc_timeout_secs: u64,
    pub poll_interval_secs: u64,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            assets_dir: PathBuf::from("assets"),
            threads: rayon::current_num_threads(),
            rpc_url: None,
            rpc_urls: vec![],
            rpc_batch_size: 10,
            rpc_timeout_secs: 30,
            poll_interval_secs: 12,
        }
    }
}

#[derive(serde::Deserialize, Default)]
struct TomlConfig {
    general: Option<TomlGeneral>,
    fetcher: Option<TomlFetcher>,
}
#[derive(serde::Deserialize, Default)]
struct TomlGeneral {
    data_dir: Option<String>,
    assets_dir: Option<String>,
    threads: Option<usize>,
}
#[derive(serde::Deserialize, Default)]
struct TomlFetcher {
    rpc_url: Option<String>,
    rpc_urls: Option<Vec<String>>,
    batch_size: Option<usize>,
    timeout_secs: Option<u64>,
    poll_interval_secs: Option<u64>,
}

impl AppConfig {
    pub fn generator_dir(&self) -> PathBuf { self.data_dir.join("generator") }
    pub fn fetcher_dir(&self) -> PathBuf { self.data_dir.join("fetcher") }
    pub fn results_dir(&self) -> PathBuf { self.data_dir.join("results") }
    pub fn collider_cursor_path(&self) -> PathBuf { self.results_dir().join("collider_cursor.json") }
    pub fn hits_bf_csv_path(&self) -> PathBuf { self.results_dir().join("hits_bf.csv") }
    pub fn fetcher_ranges_dir(&self) -> PathBuf { self.fetcher_dir().join("ranges") }
    pub fn derivation_candidates_path(&self) -> PathBuf { self.assets_dir.join("derivation_candidates").join("derivation_candidates.txt") }
    pub fn generator_seed_path(&self) -> PathBuf { self.assets_dir.join("generator_seed.key") }

    pub fn ensure_dirs(&self) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.data_dir)?;
        std::fs::create_dir_all(self.fetcher_dir())?;
        std::fs::create_dir_all(self.fetcher_ranges_dir())?;
        std::fs::create_dir_all(self.results_dir())?;
        Ok(())
    }

    pub fn load(path: &Path) -> Self {
        let mut cfg = Self::default();
        let Ok(content) = std::fs::read_to_string(path) else { return cfg; };
        let Ok(toml): Result<TomlConfig, _> = toml::from_str(&content) else {
            log::warn!("failed to parse {}, using defaults", path.display());
            return cfg;
        };
        if let Some(g) = toml.general {
            if let Some(d) = g.data_dir { cfg.data_dir = PathBuf::from(d); }
            if let Some(a) = g.assets_dir { cfg.assets_dir = PathBuf::from(a); }
            if let Some(t) = g.threads { if t > 0 { cfg.threads = t; } }
        }
        if let Some(f) = toml.fetcher {
            if let Some(ref u) = f.rpc_url { cfg.rpc_url = Some(u.clone()); }
            if let Some(ref urls) = f.rpc_urls { if !urls.is_empty() { cfg.rpc_urls = urls.clone(); } }
            if cfg.rpc_urls.is_empty() { if let Some(ref u) = f.rpc_url { cfg.rpc_urls = vec![u.clone()]; } }
            if let Some(b) = f.batch_size { if b > 0 { cfg.rpc_batch_size = b; } }
            if let Some(t) = f.timeout_secs { if t > 0 { cfg.rpc_timeout_secs = t; } }
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
        assert_eq!(cfg.data_dir, PathBuf::from("data"));
        assert_eq!(cfg.rpc_batch_size, 10);
    }

    #[test]
    fn load_partial_toml() {
        let dir = std::env::temp_dir().join(format!("birdhash_cfg_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("config.toml");
        std::fs::write(&path, r#"
[general]
data_dir = "/tmp/mydata"
"#).unwrap();
        let cfg = AppConfig::load(&path);
        assert_eq!(cfg.data_dir, PathBuf::from("/tmp/mydata"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_fetcher_rpc_urls() {
        let dir = std::env::temp_dir().join(format!("birdhash_cfg_rpc_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("config.toml");
        std::fs::write(&path, r#"
[fetcher]
rpc_urls = ["https://a.com", "https://b.com"]
poll_interval_secs = 30
"#).unwrap();
        let cfg = AppConfig::load(&path);
        assert_eq!(cfg.rpc_urls.len(), 2);
        assert_eq!(cfg.rpc_urls[0], "https://a.com");
        assert_eq!(cfg.poll_interval_secs, 30);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
