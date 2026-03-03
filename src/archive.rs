//! Block archive: segmented binary files storing block→addresses mapping.
//!
//! Format (per file `archive_{start:09}_{end:09}.bin`):
//!   Header (32 bytes): magic "BHFA" + version(u32) + start_block(u64) + end_block(u64) + total_addrs(u64)
//!   Records: [block_number(u64) + addr_count(u16) + addrs([u8;20] × count)] ...

use crate::keygen::{Address, ADDR_LEN};
use anyhow::{Context, Result};
use std::io::Write;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"BHFA";
const VERSION: u32 = 1;
const HEADER_SIZE: usize = 32;

/// One block's extracted addresses.
#[derive(Debug, Clone)]
pub struct BlockAddresses {
    pub block_number: u64,
    pub addresses: Vec<Address>,
}

/// Writer for the current archive segment (append-only).
pub struct ArchiveWriter {
    dir: PathBuf,
    segment_size: u64,
    seg_start: u64,
    seg_end: u64,
    total_addrs: u64,
    file: Option<std::fs::File>,
}

impl ArchiveWriter {
    pub fn new(dir: &Path, segment_size: u64) -> Self {
        std::fs::create_dir_all(dir).ok();
        Self { dir: dir.to_path_buf(), segment_size, seg_start: 0, seg_end: 0, total_addrs: 0, file: None }
    }

    /// Resume: open the segment that contains `block_number`, positioned at end for appending.
    pub fn resume(&mut self, block_number: u64) -> Result<()> {
        let seg_start = (block_number / self.segment_size) * self.segment_size;
        self.open_segment(seg_start)
    }

    fn segment_path(&self, start: u64) -> PathBuf {
        let end = start + self.segment_size - 1;
        self.dir.join(format!("archive_{:09}_{:09}.bin", start, end))
    }

    fn open_segment(&mut self, seg_start: u64) -> Result<()> {
        let seg_end = seg_start + self.segment_size - 1;
        let path = self.segment_path(seg_start);
        if path.exists() {
            let meta = std::fs::metadata(&path)?;
            let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&path)?;
            // Read header to get total_addrs
            if meta.len() >= HEADER_SIZE as u64 {
                let mut hdr = [0u8; HEADER_SIZE];
                std::io::Read::read_exact(&mut f, &mut hdr)?;
                self.total_addrs = u64::from_le_bytes(hdr[24..32].try_into()?);
            }
            // Seek to end for appending
            std::io::Seek::seek(&mut f, std::io::SeekFrom::End(0))?;
            self.file = Some(f);
        } else {
            let mut f = std::fs::File::create(&path)?;
            self.write_header(&mut f, seg_start, seg_end, 0)?;
            self.total_addrs = 0;
            self.file = Some(f);
        }
        self.seg_start = seg_start;
        self.seg_end = seg_end;
        Ok(())
    }

    fn write_header(&self, f: &mut std::fs::File, start: u64, end: u64, total: u64) -> Result<()> {
        let mut hdr = [0u8; HEADER_SIZE];
        hdr[0..4].copy_from_slice(MAGIC);
        hdr[4..8].copy_from_slice(&VERSION.to_le_bytes());
        hdr[8..16].copy_from_slice(&start.to_le_bytes());
        hdr[16..24].copy_from_slice(&end.to_le_bytes());
        hdr[24..32].copy_from_slice(&total.to_le_bytes());
        f.write_all(&hdr)?;
        Ok(())
    }

    fn update_header_total(&mut self) -> Result<()> {
        if let Some(ref mut f) = self.file {
            use std::io::Seek;
            let pos = f.stream_position()?;
            f.seek(std::io::SeekFrom::Start(24))?;
            f.write_all(&self.total_addrs.to_le_bytes())?;
            f.seek(std::io::SeekFrom::Start(pos))?;
        }
        Ok(())
    }

    /// Write a block's addresses to the archive. Automatically rolls to next segment if needed.
    pub fn write_block(&mut self, ba: &BlockAddresses) -> Result<()> {
        let target_seg = (ba.block_number / self.segment_size) * self.segment_size;
        if self.file.is_none() || target_seg != self.seg_start {
            // Flush & finalize current segment, open new
            self.flush()?;
            self.open_segment(target_seg)?;
        }
        let f = self.file.as_mut().context("no archive file open")?;
        f.write_all(&ba.block_number.to_le_bytes())?;
        let count = ba.addresses.len().min(u16::MAX as usize) as u16;
        f.write_all(&count.to_le_bytes())?;
        for addr in ba.addresses.iter().take(count as usize) {
            f.write_all(addr)?;
        }
        self.total_addrs += count as u64;
        Ok(())
    }

    /// Flush and update header total_addrs.
    pub fn flush(&mut self) -> Result<()> {
        self.update_header_total()?;
        if let Some(ref mut f) = self.file {
            f.flush()?;
        }
        Ok(())
    }
}

impl Drop for ArchiveWriter {
    fn drop(&mut self) { let _ = self.flush(); }
}

/// Read all addresses from a single archive file.
#[allow(dead_code)]
pub fn read_archive(path: &Path) -> Result<Vec<BlockAddresses>> {
    let data = std::fs::read(path).with_context(|| format!("read archive {}", path.display()))?;
    anyhow::ensure!(data.len() >= HEADER_SIZE, "archive file too short");
    anyhow::ensure!(&data[0..4] == MAGIC, "bad magic in archive {}", path.display());
    let total_expected = u64::from_le_bytes(data[24..32].try_into()?);
    let mut blocks = Vec::new();
    let mut pos = HEADER_SIZE;
    let mut total_read = 0u64;
    while pos + 10 <= data.len() {
        let bn = u64::from_le_bytes(data[pos..pos + 8].try_into()?);
        let count = u16::from_le_bytes(data[pos + 8..pos + 10].try_into()?) as usize;
        pos += 10;
        let need = count * ADDR_LEN;
        if pos + need > data.len() { break; }
        let mut addrs = Vec::with_capacity(count);
        for i in 0..count {
            let off = pos + i * ADDR_LEN;
            let mut a = [0u8; ADDR_LEN];
            a.copy_from_slice(&data[off..off + ADDR_LEN]);
            addrs.push(a);
        }
        pos += need;
        total_read += count as u64;
        blocks.push(BlockAddresses { block_number: bn, addresses: addrs });
    }
    log::debug!("read_archive {}: {} blocks, {} addrs (header says {})", path.display(), blocks.len(), total_read, total_expected);
    Ok(blocks)
}

/// Read all addresses (flat) from all archive files in a directory, sorted by block number.
#[allow(dead_code)]
pub fn read_all_archives(dir: &Path) -> Result<Vec<Address>> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("archive_") && e.file_name().to_string_lossy().ends_with(".bin"))
        .collect();
    entries.sort_by_key(|e| e.file_name().to_string_lossy().to_string());
    let mut all = Vec::new();
    for entry in entries {
        let blocks = read_archive(&entry.path())?;
        for ba in blocks { all.extend(ba.addresses); }
    }
    Ok(all)
}

/// List archive segments in a directory, returning (seg_start, seg_end, path).
#[allow(dead_code)]
pub fn list_segments(dir: &Path) -> Result<Vec<(u64, u64, PathBuf)>> {
    let mut segs = Vec::new();
    if !dir.exists() { return Ok(segs); }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with("archive_") || !name.ends_with(".bin") { continue; }
        // archive_000000000_000099999.bin
        let parts: Vec<&str> = name.trim_start_matches("archive_").trim_end_matches(".bin").split('_').collect();
        if parts.len() == 2 {
            if let (Ok(s), Ok(e)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
                segs.push((s, e, entry.path()));
            }
        }
    }
    segs.sort_by_key(|(s, _, _)| *s);
    Ok(segs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("birdhash_archive_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn archive_write_read_round_trip() {
        let dir = test_dir("roundtrip");
        let mut w = ArchiveWriter::new(&dir, 100); // 100 blocks per segment
        let ba1 = BlockAddresses { block_number: 0, addresses: vec![[1u8; 20], [2u8; 20]] };
        let ba2 = BlockAddresses { block_number: 50, addresses: vec![[3u8; 20]] };
        let ba3 = BlockAddresses { block_number: 99, addresses: vec![] };
        w.write_block(&ba1).unwrap();
        w.write_block(&ba2).unwrap();
        w.write_block(&ba3).unwrap();
        w.flush().unwrap();
        // Read back
        let path = dir.join("archive_000000000_000000099.bin");
        assert!(path.exists());
        let blocks = read_archive(&path).unwrap();
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0].block_number, 0);
        assert_eq!(blocks[0].addresses.len(), 2);
        assert_eq!(blocks[1].block_number, 50);
        assert_eq!(blocks[1].addresses.len(), 1);
        assert_eq!(blocks[2].block_number, 99);
        assert_eq!(blocks[2].addresses.len(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn archive_segment_rollover() {
        let dir = test_dir("rollover");
        let mut w = ArchiveWriter::new(&dir, 100);
        // Block 50 → segment 0
        w.write_block(&BlockAddresses { block_number: 50, addresses: vec![[1u8; 20]] }).unwrap();
        // Block 150 → segment 100
        w.write_block(&BlockAddresses { block_number: 150, addresses: vec![[2u8; 20]] }).unwrap();
        w.flush().unwrap();
        let segs = list_segments(&dir).unwrap();
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].0, 0);
        assert_eq!(segs[1].0, 100);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_all_archives_flat() {
        let dir = test_dir("all");
        let mut w = ArchiveWriter::new(&dir, 100);
        w.write_block(&BlockAddresses { block_number: 10, addresses: vec![[1u8; 20], [2u8; 20]] }).unwrap();
        w.write_block(&BlockAddresses { block_number: 110, addresses: vec![[3u8; 20]] }).unwrap();
        w.flush().unwrap();
        drop(w);
        let all = read_all_archives(&dir).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0], [1u8; 20]);
        assert_eq!(all[2], [3u8; 20]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn archive_resume_appends() {
        let dir = test_dir("resume");
        {
            let mut w = ArchiveWriter::new(&dir, 1000);
            w.write_block(&BlockAddresses { block_number: 0, addresses: vec![[1u8; 20]] }).unwrap();
            w.write_block(&BlockAddresses { block_number: 5, addresses: vec![[2u8; 20]] }).unwrap();
            w.flush().unwrap();
        }
        // Resume and append more
        {
            let mut w = ArchiveWriter::new(&dir, 1000);
            w.resume(10).unwrap();
            w.write_block(&BlockAddresses { block_number: 10, addresses: vec![[3u8; 20]] }).unwrap();
            w.flush().unwrap();
        }
        let path = dir.join("archive_000000000_000000999.bin");
        let blocks = read_archive(&path).unwrap();
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[2].block_number, 10);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
