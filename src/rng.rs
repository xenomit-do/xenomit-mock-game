//! Deterministic PRNG (xorshift64*) — the sim must be reproducible from
//! a seed, so no `rand` dependency and no OS entropy.

#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        // Never allow the zero state (xorshift gets stuck there).
        Self(seed | 1)
    }

    /// Mix a world seed with a per-session discriminator.
    pub fn for_session(seed: u64, user_id: i32) -> Self {
        Self::new(seed ^ (user_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform-ish `lo..=hi` integer.
    pub fn range(&mut self, lo: i32, hi: i32) -> i32 {
        if hi <= lo {
            return lo;
        }
        lo + (self.next_u64() % ((hi - lo + 1) as u64)) as i32
    }

    /// `chance(n, d)` → true with probability n/d.
    pub fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        self.next_u64() % denominator < numerator
    }

    pub fn pick<'a>(&mut self, items: &'a [&'a str]) -> &'a str {
        items[self.next_u64() as usize % items.len()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_sequence() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn range_bounds() {
        let mut r = Rng::new(7);
        for _ in 0..1000 {
            let v = r.range(5, 9);
            assert!((5..=9).contains(&v));
        }
    }
}
