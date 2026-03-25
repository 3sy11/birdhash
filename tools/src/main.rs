use anyhow::Result;
use std::collections::HashSet;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tiny_keccak::{Hasher, Keccak};

mod filter_mod {
    use anyhow::Result;
    use std::path::Path;
    pub use xorf::BinaryFuse16;

    pub fn addr_to_u64(addr: &[u8; 20]) -> u64 {
        let a = u64::from_le_bytes([
            addr[0], addr[1], addr[2], addr[3], addr[4], addr[5], addr[6], addr[7],
        ]);
        let b = u64::from_le_bytes([
            addr[8], addr[9], addr[10], addr[11], addr[12], addr[13], addr[14], addr[15],
        ]);
        let c = u32::from_le_bytes([addr[16], addr[17], addr[18], addr[19]]) as u64;
        let mut h = a;
        h ^= b.wrapping_mul(0x517cc1b727220a95);
        h ^= c.wrapping_mul(0x6c62272e07bb0142);
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h ^= h >> 33;
        h
    }

    pub fn build_fuse16(keys: &[u64]) -> Result<BinaryFuse16> {
        BinaryFuse16::try_from(keys)
            .map_err(|e| anyhow::anyhow!("BinaryFuse16 build failed: {:?}", e))
    }

    pub fn save_fuse16(filter: &BinaryFuse16, path: &Path) -> Result<()> {
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        }
        let data = bincode::serialize(filter)?;
        std::fs::write(path, data)?;
        Ok(())
    }
}

const ADDR_LEN: usize = 20;
type Address = [u8; ADDR_LEN];

fn parse_hex_addr(s: &str) -> Option<Address> {
    let s = s.trim_start_matches("0x");
    if s.len() != 40 {
        return None;
    }
    let bytes = hex::decode(s).ok()?;
    if bytes.len() != ADDR_LEN {
        return None;
    }
    let mut addr = [0u8; ADDR_LEN];
    addr.copy_from_slice(&bytes);
    Some(addr)
}

fn parse_hex_u64(s: &str) -> Option<u64> {
    let s = s.trim_start_matches("0x");
    if s.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(s, 16).ok()
}

fn create_address(sender: &[u8; ADDR_LEN], nonce: u64) -> Address {
    let mut stream = rlp::RlpStream::new_list(2);
    stream.append(&sender.as_slice());
    stream.append(&nonce);
    let encoded = stream.out();
    let mut hash = [0u8; 32];
    let mut keccak = Keccak::v256();
    keccak.update(&encoded);
    keccak.finalize(&mut hash);
    let mut addr = [0u8; ADDR_LEN];
    addr.copy_from_slice(&hash[12..32]);
    addr
}

fn extract_addresses_from_block(block: &serde_json::Value) -> Vec<Address> {
    let mut addrs = Vec::new();

    if let Some(miner) = block.get("miner").and_then(|v| v.as_str()) {
        if let Some(a) = parse_hex_addr(miner) {
            addrs.push(a);
        }
    }
    if let Some(author) = block.get("author").and_then(|v| v.as_str()) {
        if let Some(a) = parse_hex_addr(author) {
            addrs.push(a);
        }
    }

    if let Some(withdrawals) = block.get("withdrawals").and_then(|v| v.as_array()) {
        for w in withdrawals {
            if let Some(addr) = w.get("address").and_then(|v| v.as_str()) {
                if let Some(a) = parse_hex_addr(addr) {
                    addrs.push(a);
                }
            }
        }
    }

    if let Some(txs) = block.get("transactions").and_then(|v| v.as_array()) {
        for tx in txs {
            let from = tx
                .get("from")
                .and_then(|v| v.as_str())
                .and_then(parse_hex_addr);
            if let Some(a) = from {
                addrs.push(a);
            }

            if let Some(to) = tx.get("to").and_then(|v| v.as_str()) {
                if let Some(a) = parse_hex_addr(to) {
                    addrs.push(a);
                }
            } else if let Some(creates) = tx.get("creates").and_then(|v| v.as_str()) {
                if let Some(a) = parse_hex_addr(creates) {
                    addrs.push(a);
                }
            } else if let Some(ref from_addr) = from {
                let nonce = tx
                    .get("nonce")
                    .and_then(|v| v.as_str())
                    .and_then(parse_hex_u64)
                    .unwrap_or(0);
                addrs.push(create_address(from_addr, nonce));
            }
        }
    }
    addrs
}

fn main() -> Result<()> {
    let source_dir = Path::new("E:/data");
    let output_filter_path = Path::new("D:/Services/birdhash/data/fetcher/filter_fetch.bin");

    println!("Scanning {} directories...", source_dir.display());

    let mut addrs: HashSet<u64> = HashSet::new();
    let mut total_blocks = 0u64;

    for entry in std::fs::read_dir(source_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let blocks_file = path.join("blocks.jsonl");
        if !blocks_file.exists() {
            continue;
        }

        let dir_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        print!("Processing {} ... ", dir_name);

        let file = std::fs::File::open(&blocks_file)?;
        let reader = BufReader::new(file);

        let mut local_count = 0u64;
        for line_result in reader.lines() {
            let line = line_result?;
            total_blocks += 1;
            local_count += 1;

            if let Ok(block) = serde_json::from_str::<serde_json::Value>(&line) {
                let extracted = extract_addresses_from_block(&block);
                for addr in extracted {
                    addrs.insert(filter_mod::addr_to_u64(&addr));
                }
            }
        }
        println!("{} blocks", local_count);
    }

    println!("\nTotal blocks: {}", total_blocks);
    println!("Unique addresses: {}", addrs.len());

    let fps: Vec<u64> = addrs.into_iter().collect();
    println!("Building BinaryFuse16 filter...");

    let filter = filter_mod::build_fuse16(&fps)?;

    if let Some(p) = output_filter_path.parent() {
        std::fs::create_dir_all(p)?;
    }
    filter_mod::save_fuse16(&filter, output_filter_path)?;

    println!("Done! Filter saved to {}", output_filter_path.display());

    Ok(())
}
