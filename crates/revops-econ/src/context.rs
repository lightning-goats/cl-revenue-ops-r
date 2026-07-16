//! Deterministic cycle context (Rust port of `modules/cycle_context.py`).
//!
//! Policies must not read the wall clock or unseeded randomness. A
//! `CycleContext` is constructed once per economic cycle by the
//! orchestrator and injected into every policy: `cycle_time` is the only
//! clock, and `derive_seed()` the only randomness source, so the same
//! context reproduces byte-identical decisions.
//!
//! `rng()` (Python's `random.Random(self.seed)`) is intentionally NOT
//! ported: nothing pinned in this phase consumes it, and there is no
//! byte-stable Rust equivalent to Python's Mersenne Twister worth carrying
//! across languages.

use sha2::{Digest, Sha256};

use crate::types::{EconError, EconResult, UnixTime, U63_MAX};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CycleContext {
    pub cycle_id: String,
    pub cycle_time: UnixTime,
    pub seed: i64,
    pub snapshot_id: String,
}

impl CycleContext {
    pub fn new(
        cycle_id: String,
        cycle_time: UnixTime,
        seed: i64,
        snapshot_id: String,
    ) -> EconResult<Self> {
        if cycle_id.is_empty() || snapshot_id.is_empty() || !(0..=U63_MAX).contains(&seed) {
            return Err(EconError {
                msg: format!("CycleContext invalid: {cycle_id:?}/{snapshot_id:?}/{seed}"),
            });
        }
        Ok(Self {
            cycle_id,
            cycle_time,
            seed,
            snapshot_id,
        })
    }

    /// Independent-but-deterministic seed for a named component: first 8
    /// bytes big-endian of `sha256("{seed}:{component}")`, masked to u63.
    /// A stable cross-language contract (pure sha256) — golden values are
    /// pinned against the Python reference implementation in the tests
    /// below.
    pub fn derive_seed(&self, component: &str) -> EconResult<i64> {
        if component.is_empty() {
            return Err(EconError {
                msg: format!("derive_seed component invalid: {component:?}"),
            });
        }
        let digest = Sha256::digest(format!("{}:{}", self.seed, component).as_bytes());
        let mut b = [0u8; 8];
        b.copy_from_slice(&digest[..8]);
        Ok((u64::from_be_bytes(b) & 0x7fff_ffff_ffff_ffff) as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(seed: i64) -> CycleContext {
        CycleContext::new(
            "c".to_string(),
            UnixTime::new(1_752_400_000).unwrap(),
            seed,
            "s".to_string(),
        )
        .unwrap()
    }

    #[test]
    fn new_rejects_empty_cycle_id() {
        assert!(
            CycleContext::new(String::new(), UnixTime::new(0).unwrap(), 0, "s".to_string())
                .is_err()
        );
    }

    #[test]
    fn new_rejects_empty_snapshot_id() {
        assert!(
            CycleContext::new("c".to_string(), UnixTime::new(0).unwrap(), 0, String::new())
                .is_err()
        );
    }

    #[test]
    fn new_rejects_negative_seed() {
        assert!(CycleContext::new(
            "c".to_string(),
            UnixTime::new(0).unwrap(),
            -1,
            "s".to_string()
        )
        .is_err());
    }

    #[test]
    fn derive_seed_rejects_empty_component() {
        assert!(ctx(0).derive_seed("").is_err());
    }

    /// Golden pairs generated once from the Python reference
    /// (`~/bin/cl_revenue_ops-port`, branch `port`):
    ///
    /// ```text
    /// python3 -c "
    /// from modules.cycle_context import CycleContext
    /// from modules.econ_types import UnixTime
    /// pairs = [
    ///     (0, 'econ-cycle'),
    ///     (0, 'rebalance-pair-0'),
    ///     (42, 'econ-cycle'),
    ///     (1752400000, 'shadow'),
    ///     (9223372036854775807, 'x'),
    /// ]
    /// for seed, component in pairs:
    ///     c = CycleContext(cycle_id='c', cycle_time=UnixTime(1752400000),
    ///                      seed=seed, snapshot_id='s')
    ///     print(seed, repr(component), c.derive_seed(component))
    /// "
    /// ```
    ///
    /// Output:
    /// ```text
    /// 0 'econ-cycle' 1161200426328304218
    /// 0 'rebalance-pair-0' 3812192950263033314
    /// 42 'econ-cycle' 1181443684567748569
    /// 1752400000 'shadow' 1581955701300553273
    /// 9223372036854775807 'x' 3840343568560440981
    /// ```
    #[test]
    fn derive_seed_matches_python_golden_pairs() {
        assert_eq!(
            ctx(0).derive_seed("econ-cycle").unwrap(),
            1161200426328304218
        );
        assert_eq!(
            ctx(0).derive_seed("rebalance-pair-0").unwrap(),
            3812192950263033314
        );
        assert_eq!(
            ctx(42).derive_seed("econ-cycle").unwrap(),
            1181443684567748569
        );
        assert_eq!(
            ctx(1_752_400_000).derive_seed("shadow").unwrap(),
            1581955701300553273
        );
        assert_eq!(ctx(U63_MAX).derive_seed("x").unwrap(), 3840343568560440981);
    }
}
