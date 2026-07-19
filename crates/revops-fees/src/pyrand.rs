//! CPython `random` module parity RNG: hand-rolled MT19937 with CPython's
//! `seed(int)` (init_by_array), `random()` (genrand_res53), and `gauss()`
//! (Box–Muller with the cached second value) — bit-for-bit against
//! `random.Random(seed)`, pinned by `fixtures/fees/pyrand/sequences.json`.
//!
//! Sources: CPython `Modules/_randommodule.c` (the mt19937ar reference
//! implementation of Matsumoto & Nishimura) and `Lib/random.py::gauss`.
//! **No `rand` crate anywhere in this workspace** (Phase 4 Global
//! Constraints): distribution parity is not enough — sampling paths must
//! reproduce Python's exact draw stream under injected seeds.

const N: usize = 624;
const M: usize = 397;
const MATRIX_A: u32 = 0x9908_b0df;
const UPPER_MASK: u32 = 0x8000_0000;
const LOWER_MASK: u32 = 0x7fff_ffff;

/// A strict replay input could not be consumed at the requested semantic
/// decision boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionInputError {
    message: String,
}

impl DecisionInputError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn empty_label(input: &str) -> Self {
        Self::new(format!("{input} decision label must not be empty"))
    }
}

impl std::fmt::Display for DecisionInputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for DecisionInputError {}

fn require_label(label: &str, input: &str) -> Result<(), DecisionInputError> {
    if label.is_empty() {
        Err(DecisionInputError::empty_label(input))
    } else {
        Ok(())
    }
}

/// Replayable entropy consumed at semantically labeled decision boundaries.
pub trait DecisionEntropy {
    fn random(&mut self, label: &str) -> Result<f64, DecisionInputError>;

    fn gauss(&mut self, label: &str, mu: f64, sigma: f64) -> Result<f64, DecisionInputError>;
}

/// CPython-compatible `random.Random` (MT19937 core + `gauss` cache).
///
/// `gauss_next` mirrors Python's instance attribute: `gauss()` generates
/// Box–Muller pairs and caches the second value, which the NEXT `gauss()`
/// call consumes (scaled by the consuming call's `mu`/`sigma`) without
/// touching the underlying MT19937 stream. `random()` never touches the
/// cache.
pub struct PyRandom {
    mt: [u32; N],
    index: usize,
    gauss_next: Option<f64>,
}

impl PyRandom {
    /// CPython `random.seed(n)` for a non-negative int: `init_by_array`
    /// over the int's little-endian 32-bit words (`n == 0` => key `[0]`,
    /// matching `_PyLong_NumBits(0) == 0` => one key word).
    pub fn seed_from_u64(n: u64) -> Self {
        let lo = n as u32;
        let hi = (n >> 32) as u32;
        let key: &[u32] = if hi != 0 { &[lo, hi] } else { &[lo] };
        let mut rng = Self {
            mt: [0u32; N],
            index: N,
            gauss_next: None,
        };
        rng.init_by_array(key);
        rng
    }

    /// mt19937ar `init_genrand(s)`.
    fn init_genrand(&mut self, s: u32) {
        self.mt[0] = s;
        for i in 1..N {
            self.mt[i] = 1_812_433_253u32
                .wrapping_mul(self.mt[i - 1] ^ (self.mt[i - 1] >> 30))
                .wrapping_add(i as u32);
        }
        self.index = N;
    }

    /// mt19937ar `init_by_array(init_key, key_length)`.
    fn init_by_array(&mut self, key: &[u32]) {
        self.init_genrand(19_650_218);
        let mut i: usize = 1;
        let mut j: usize = 0;
        let mut k = N.max(key.len());
        while k > 0 {
            self.mt[i] = (self.mt[i]
                ^ (self.mt[i - 1] ^ (self.mt[i - 1] >> 30)).wrapping_mul(1_664_525))
            .wrapping_add(key[j])
            .wrapping_add(j as u32);
            i += 1;
            j += 1;
            if i >= N {
                self.mt[0] = self.mt[N - 1];
                i = 1;
            }
            if j >= key.len() {
                j = 0;
            }
            k -= 1;
        }
        k = N - 1;
        while k > 0 {
            self.mt[i] = (self.mt[i]
                ^ (self.mt[i - 1] ^ (self.mt[i - 1] >> 30)).wrapping_mul(1_566_083_941))
            .wrapping_sub(i as u32);
            i += 1;
            if i >= N {
                self.mt[0] = self.mt[N - 1];
                i = 1;
            }
            k -= 1;
        }
        self.mt[0] = 0x8000_0000;
        self.index = N;
    }

    /// mt19937ar `genrand_int32()`: block twist + tempering.
    fn genrand_u32(&mut self) -> u32 {
        if self.index >= N {
            for kk in 0..N {
                let y = (self.mt[kk] & UPPER_MASK) | (self.mt[(kk + 1) % N] & LOWER_MASK);
                let mag = if y & 1 == 1 { MATRIX_A } else { 0 };
                self.mt[kk] = self.mt[(kk + M) % N] ^ (y >> 1) ^ mag;
            }
            self.index = 0;
        }
        let mut y = self.mt[self.index];
        self.index += 1;
        y ^= y >> 11;
        y ^= (y << 7) & 0x9d2c_5680;
        y ^= (y << 15) & 0xefc6_0000;
        y ^ (y >> 18)
    }

    /// CPython `random_random` (`genrand_res53`): 53-bit resolution double
    /// in [0, 1): `((a>>5)*67108864.0 + (b>>6)) * (1.0/9007199254740992.0)`.
    pub fn random(&mut self) -> f64 {
        let a = self.genrand_u32() >> 5;
        let b = self.genrand_u32() >> 6;
        (f64::from(a) * 67_108_864.0 + f64::from(b)) * (1.0 / 9_007_199_254_740_992.0)
    }

    /// CPython `random.gauss(mu, sigma)` — Box–Muller with the cached
    /// second value (`Lib/random.py`):
    ///
    /// ```text
    /// z = self.gauss_next; self.gauss_next = None
    /// if z is None:
    ///     x2pi = random() * TWOPI
    ///     g2rad = sqrt(-2.0 * log(1.0 - random()))
    ///     z = cos(x2pi) * g2rad
    ///     self.gauss_next = sin(x2pi) * g2rad
    /// return mu + z * sigma
    /// ```
    ///
    /// (`TWOPI = 2.0 * pi` is exactly `f64::consts::TAU`.)
    pub fn gauss(&mut self, mu: f64, sigma: f64) -> f64 {
        if let Some(z) = self.gauss_next.take() {
            return mu + z * sigma;
        }
        let x2pi = self.random() * std::f64::consts::TAU;
        let g2rad = (-2.0 * (1.0 - self.random()).ln()).sqrt();
        let z = x2pi.cos() * g2rad;
        self.gauss_next = Some(x2pi.sin() * g2rad);
        mu + z * sigma
    }
}

impl DecisionEntropy for PyRandom {
    fn random(&mut self, label: &str) -> Result<f64, DecisionInputError> {
        require_label(label, "entropy")?;
        Ok(PyRandom::random(self))
    }

    fn gauss(&mut self, label: &str, mu: f64, sigma: f64) -> Result<f64, DecisionInputError> {
        require_label(label, "entropy")?;
        Ok(PyRandom::gauss(self, mu, sigma))
    }
}
