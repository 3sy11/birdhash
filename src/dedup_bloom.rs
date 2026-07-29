//! 去重 Bloom Filter：用于 fetch 阶段过滤重复地址，减少落盘量。

use crate::filter;

const ADDR_LEN: usize = 20;

pub struct DedupBloom {
    bits: Vec<u64>,
    num_bits: u64,
    num_hashes: u32,
    count: u64,
}

impl DedupBloom {
    pub fn new(expected_items: u64, fpr: f64) -> Self {
        let fpr = fpr.max(1e-15);
        let bits_per = -(fpr.ln()) / (2.0_f64.ln().powi(2));
        let num_bits = ((expected_items as f64 * bits_per) as u64).max(64);
        let num_hashes = ((num_bits as f64 / expected_items.max(1) as f64) * 2.0_f64.ln()).ceil() as u32;
        let num_hashes = num_hashes.clamp(1, 30);
        let words = ((num_bits + 63) / 64) as usize;
        Self { bits: vec![0u64; words], num_bits, num_hashes, count: 0 }
    }

    #[inline]
    fn hash_indices(&self, addr: &[u8; ADDR_LEN]) -> impl Iterator<Item = u64> + '_ {
        let h1 = filter::addr_to_u64(addr);
        let h2 = filter::addr_to_u64_alt(addr);
        let n = self.num_bits;
        let k = self.num_hashes;
        (0..k).map(move |i| {
            let combined = h1.wrapping_add((i as u64).wrapping_mul(h2));
            combined % n
        })
    }

    pub fn insert(&mut self, addr: &[u8; ADDR_LEN]) {
        let indices: Vec<u64> = self.hash_indices(addr).collect();
        for idx in indices {
            let word = (idx / 64) as usize;
            let bit = idx % 64;
            self.bits[word] |= 1u64 << bit;
        }
        self.count += 1;
    }

    pub fn contains(&self, addr: &[u8; ADDR_LEN]) -> bool {
        self.hash_indices(addr).all(|idx| {
            let word = (idx / 64) as usize;
            let bit = idx % 64;
            (self.bits[word] >> bit) & 1 == 1
        })
    }

    pub fn count(&self) -> u64 { self.count }
    pub fn memory_bytes(&self) -> usize { self.bits.len() * 8 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_contains() {
        let mut bloom = DedupBloom::new(10_000, 0.001);
        let addr_a = [1u8; 20];
        let addr_b = [2u8; 20];
        assert!(!bloom.contains(&addr_a));
        bloom.insert(&addr_a);
        assert!(bloom.contains(&addr_a));
        assert!(!bloom.contains(&addr_b));
        assert_eq!(bloom.count(), 1);
    }

    #[test]
    fn low_false_positive_rate() {
        let n = 50_000u64;
        let mut bloom = DedupBloom::new(n, 0.001);
        for i in 0..n {
            let mut addr = [0u8; 20];
            addr[..8].copy_from_slice(&i.to_le_bytes());
            bloom.insert(&addr);
        }
        let mut fp = 0u64;
        let test_count = 50_000u64;
        for i in n..(n + test_count) {
            let mut addr = [0u8; 20];
            addr[..8].copy_from_slice(&i.to_le_bytes());
            if bloom.contains(&addr) { fp += 1; }
        }
        let fpr = fp as f64 / test_count as f64;
        assert!(fpr < 0.01, "FPR {:.4} too high (expected < 0.01)", fpr);
    }

    #[test]
    fn memory_scales_with_capacity() {
        let small = DedupBloom::new(1_000, 0.001);
        let large = DedupBloom::new(1_000_000, 0.001);
        assert!(large.memory_bytes() > small.memory_bytes() * 100);
    }
}
