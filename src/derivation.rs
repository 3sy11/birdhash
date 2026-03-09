//! 派生路径 account/index 候选生成。account 取值范围 0-111（含）；index 取值范围为本命令生成的 derivation_candidates.txt。

use anyhow::Result;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::Write;
use std::path::Path;

const FIRST_N: u64 = 10_000;
const LAST_N: u64 = 10_000;
const INDEX_MAX: u64 = 2_147_483_647;
const LO: u64 = 10_001;
const HI: u64 = INDEX_MAX - FIRST_N;

/// account 取值范围 [0, 111]（含 111）
pub const ACCOUNT_MAX: u32 = 111;

fn in_ranges(n: u64) -> bool {
    (n < FIRST_N) || (n >= LO && n <= HI) || (n > INDEX_MAX - LAST_N && n <= INDEX_MAX)
}

fn has_interrupted_digit(n: u64) -> bool {
    let s = n.to_string();
    let bytes = s.as_bytes();
    for d in b'0'..=b'9' {
        let pos: Vec<usize> = bytes.iter().enumerate().filter(|(_, &c)| c == d).map(|(i, _)| i).collect();
        if pos.is_empty() { continue; }
        let min_p = *pos.iter().min().unwrap();
        let max_p = *pos.iter().max().unwrap();
        if max_p - min_p + 1 != pos.len() { return true; }
    }
    false
}

/// 仅2种数码：对每对 (a,b) 生成所有长度 1..=10、首位非0（若含0）的数
fn gen_two_digits(out: &mut BTreeSet<u64>) {
    for a in 0u32..=9 {
        for b in (a + 1)..=9 {
            let (d0, d1) = (a as u64, b as u64);
            for len in 1..=10 {
                let first_opts: Vec<u64> = if a == 0 { vec![d0, d1] } else { vec![d0, d1] };
                for &first in &first_opts {
                    if first == 0 && len > 1 { continue; }
                    if len == 1 {
                        if in_ranges(first) { out.insert(first); }
                        continue;
                    }
                    let rest_len = len - 1;
                    for mask in 0u64..(1 << rest_len) {
                        let mut num = first;
                        for i in 0..rest_len {
                            let bit = (mask >> i) & 1;
                            num = num * 10 + if bit == 0 { d0 } else { d1 };
                        }
                        if in_ranges(num) { out.insert(num); }
                    }
                }
            }
        }
    }
}

pub fn run_gen_derivation_candidates(out_path: &Path) -> Result<u64> {
    let mut out: BTreeSet<u64> = BTreeSet::new();
    if in_ranges(0) { out.insert(0); }
    gen_two_digits(&mut out);
    for d in 1..=9u64 {
        let mut n = 0u64;
        for _ in 0..10 {
            n = n * 10 + d;
            if in_ranges(n) { out.insert(n); }
        }
    }
    let mut seen_blocks = std::collections::HashSet::new();
    for block_len in 1..=5 {
        let max_repeat = 10 / block_len;
        if max_repeat < 2 { continue; }
        let start = 10u64.pow(block_len - 1);
        let end = 10u64.pow(block_len);
        for block_val in start..end {
            let block_str = block_val.to_string();
            for repeat in 2..=max_repeat {
                let s: String = block_str.repeat(repeat as usize);
                if s.len() > 10 { continue; }
                if let Ok(n) = s.parse::<u64>() {
                    if in_ranges(n) && seen_blocks.insert(n) { out.insert(n); }
                }
            }
        }
    }
    for start in 0..=9 {
        for len in 2..=(10 - start) {
            if start == 0 && len > 1 { continue; }
            let mut n = 0u64;
            for i in 0..len { n = n * 10 + (start + i) as u64; }
            if in_ranges(n) { out.insert(n); }
        }
    }
    for start in (0..=9).rev() {
        for len in 2..=(start + 1) {
            if start < len - 1 { continue; }
            let mut n = 0u64;
            for i in 0..len { n = n * 10 + (start - i) as u64; }
            if (start >= len - 1) && in_ranges(n) { out.insert(n); }
        }
    }
    for a in 1..=9u64 {
        for b in 0..=9u64 {
            if a != b { out.insert(a * 1100 + b * 11); }
            out.insert((10 * a + b) * 101);
            if a != b { out.insert(1001 * a + 110 * b); }
        }
    }
    let filtered: BTreeSet<u64> = out.into_iter().filter(|&n| !has_interrupted_digit(n)).collect();
    let mut out = filtered;
    for n in 0..=1000 { out.insert(n); }
    for n in (INDEX_MAX - 99)..=INDEX_MAX { out.insert(n); }
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut f = File::create(out_path)?;
    writeln!(f, "# 派生路径 m/purpose'/coin_type'/account'/change/index")?;
    writeln!(f, "# account 取值范围: 0-111（含111）；index 取值范围: 本文件所列之值（由本命令生成）")?;
    writeln!(f, "# 生成规则: 特定条件数(2种数码/易记结构等) + 全范围0-1000 + 倒数100")?;
    writeln!(f, "# LO={} HI={} index_max={}", LO, HI, INDEX_MAX)?;
    for n in &out {
        writeln!(f, "{}", n)?;
    }
    Ok(out.len() as u64)
}
