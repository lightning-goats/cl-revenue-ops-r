//! Self-contained on-chain address structural validation.
//!
//! Ports py `boltz_manager.py:56-232` verbatim (P1-006). Defensive-only:
//! rejects an obviously malformed or wrong-network withdrawal/refund/claim
//! destination before it is handed to boltzcli. Deliberately does not
//! attempt to enumerate every exotic encoding (see the confidential-Liquid
//! `blech32` structural-only check, ported as-is with its documented
//! checksum limitation).
//!
//! - bech32 / bech32m (BIP173/BIP350): full checksum + witness-program
//!   validation. Covers BTC (`bc`/`tb`/`bcrt`) and Liquid unconfidential
//!   segwit (`ex`/`tex`/`ert`).
//! - base58check: full double-SHA256 checksum + version-byte gating.
//!   Covers BTC legacy/P2SH (mainnet + test/regtest).
//! - Liquid confidential segwit (`lq`/`tlq`/`el`) uses a distinct checksum
//!   this port does not verify (matching Python); accepted on a strict
//!   prefix + charset + length structural check only.
//!
//! "network" here means the asset network (Bitcoin vs Liquid); mainnet/
//! testnet/regtest are all accepted for a given currency.

use sha2::{Digest, Sha256};

const BECH32_CHARSET: &str = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";
const B58_ALPHABET: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

const BTC_BECH32_HRPS: &[&str] = &["bc", "tb", "bcrt"];
const LBTC_BECH32_HRPS: &[&str] = &["ex", "tex", "ert"];
const LBTC_BLECH32_HRPS: &[&str] = &["lq", "tlq", "el"];
/// BTC base58 version bytes: P2PKH/P2SH for mainnet and test/regtest.
const BTC_BASE58_VERSIONS: &[u8] = &[0x00, 0x05, 0x6f, 0xc4];

/// py `_bech32_polymod` (boltz_manager.py:66-74).
fn bech32_polymod(values: &[u8]) -> u32 {
    const GENERATOR: [u32; 5] = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];
    let mut chk: u32 = 1;
    for &v in values {
        let top = chk >> 25;
        chk = ((chk & 0x1ffffff) << 5) ^ (v as u32);
        for (i, gen) in GENERATOR.iter().enumerate() {
            if (top >> i) & 1 == 1 {
                chk ^= gen;
            }
        }
    }
    chk
}

/// py `_bech32_hrp_expand` (boltz_manager.py:77-78).
fn bech32_hrp_expand(hrp: &str) -> Vec<u8> {
    let mut out: Vec<u8> = hrp.bytes().map(|b| b >> 5).collect();
    out.push(0);
    out.extend(hrp.bytes().map(|b| b & 31));
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bech32Spec {
    Bech32,
    Bech32m,
}

/// py `_bech32_decode` (boltz_manager.py:81-108). Returns
/// `(hrp, data_without_checksum, spec)` or `None`.
fn bech32_decode(addr: &str) -> Option<(String, Vec<u8>, Bech32Spec)> {
    if addr.chars().any(|c| (c as u32) < 33 || (c as u32) > 126) {
        return None;
    }
    let lower = addr.to_lowercase();
    let upper = addr.to_uppercase();
    if addr != lower && addr != upper {
        return None; // mixed case rejected per BIP173
    }
    let addr = lower;
    if addr.len() > 90 {
        return None;
    }
    let pos = addr.rfind('1')?;
    if pos < 1 || pos + 7 > addr.len() {
        return None;
    }
    let hrp = &addr[..pos];
    let mut data: Vec<u8> = Vec::with_capacity(addr.len() - pos - 1);
    for c in addr[pos + 1..].chars() {
        let idx = BECH32_CHARSET.find(c)?;
        data.push(idx as u8);
    }
    let mut poly_input = bech32_hrp_expand(hrp);
    poly_input.extend(&data);
    let const_ = bech32_polymod(&poly_input);
    let spec = if const_ == 1 {
        Bech32Spec::Bech32
    } else if const_ == 0x2bc830a3 {
        Bech32Spec::Bech32m
    } else {
        return None;
    };
    if data.len() < 6 {
        return None;
    }
    let payload = data[..data.len() - 6].to_vec();
    Some((hrp.to_string(), payload, spec))
}

/// py `_convertbits` (boltz_manager.py:111-130).
fn convertbits(data: &[u8], frombits: u32, tobits: u32, pad: bool) -> Option<Vec<u8>> {
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    let mut ret = Vec::new();
    let maxv: u32 = (1 << tobits) - 1;
    let max_acc: u32 = (1 << (frombits + tobits - 1)) - 1;
    for &value in data {
        if (value as u32) >> frombits != 0 {
            return None;
        }
        acc = ((acc << frombits) | (value as u32)) & max_acc;
        bits += frombits;
        while bits >= tobits {
            bits -= tobits;
            ret.push(((acc >> bits) & maxv) as u8);
        }
    }
    if pad {
        if bits > 0 {
            ret.push(((acc << (tobits - bits)) & maxv) as u8);
        }
    } else if bits >= frombits || ((acc << (tobits - bits)) & maxv) != 0 {
        return None;
    }
    Some(ret)
}

/// py `_valid_segwit_program` (boltz_manager.py:133-147). `data` is the
/// 5-bit payload (witver + program), checksum already stripped.
fn valid_segwit_program(data: &[u8]) -> bool {
    if data.is_empty() {
        return false;
    }
    let witver = data[0];
    if witver > 16 {
        return false;
    }
    let prog = match convertbits(&data[1..], 5, 8, false) {
        Some(p) => p,
        None => return false,
    };
    if prog.len() < 2 || prog.len() > 40 {
        return false;
    }
    if witver == 0 && prog.len() != 20 && prog.len() != 32 {
        return false;
    }
    true
}

/// py `_base58check_decode` (boltz_manager.py:150-168). Returns the
/// decoded payload (version byte + data, checksum stripped) or `None`.
fn base58check_decode(s: &str) -> Option<Vec<u8>> {
    if s.is_empty() {
        return None;
    }
    let mut num = num_bigint_free_zero();
    for ch in s.chars() {
        let idx = B58_ALPHABET.find(ch)?;
        num = big_mul_add(&num, 58, idx as u32);
    }
    let combined = big_to_be_bytes(&num);
    let n_pad = s.len() - s.trim_start_matches('1').len();
    let mut raw = vec![0u8; n_pad];
    raw.extend(combined);
    if raw.len() < 5 {
        return None;
    }
    let (payload, checksum) = raw.split_at(raw.len() - 4);
    let round1 = Sha256::digest(payload);
    let round2 = Sha256::digest(round1);
    if &round2[..4] != checksum {
        return None;
    }
    Some(payload.to_vec())
}

// --- minimal big-unsigned-int helpers (base58 needs base-256 conversion of
// an arbitrary-length base-58 number; addresses are short so a byte-vector
// "schoolbook" big integer is plenty and avoids pulling in a bignum crate
// for one call site). ---

fn num_bigint_free_zero() -> Vec<u8> {
    Vec::new()
}

/// `num = num * base + digit`, little bignum represented big-endian, no
/// leading zero bytes (matches Python's arbitrary-precision `int`).
fn big_mul_add(num: &[u8], base: u32, digit: u32) -> Vec<u8> {
    let mut carry: u64 = digit as u64;
    let mut out: Vec<u64> = num.iter().map(|&b| b as u64).collect();
    for b in out.iter_mut().rev() {
        let v = *b * base as u64 + carry;
        *b = v & 0xff;
        carry = v >> 8;
    }
    while carry > 0 {
        out.insert(0, carry & 0xff);
        carry >>= 8;
    }
    let bytes: Vec<u8> = out.into_iter().map(|v| v as u8).collect();
    let first_nonzero = bytes.iter().position(|&b| b != 0).unwrap_or(bytes.len());
    bytes[first_nonzero..].to_vec()
}

fn big_to_be_bytes(num: &[u8]) -> Vec<u8> {
    num.to_vec()
}

/// py `_validate_onchain_address` (boltz_manager.py:171-220).
pub fn validate_onchain_address(address: &str, currency: &str) -> bool {
    let addr = address.trim();
    if addr.is_empty() || addr.chars().any(|c| c.is_whitespace()) {
        return false;
    }

    let mut cur = currency.trim().to_uppercase();
    if cur == "L-BTC" || cur == "LBTC" {
        cur = "LBTC".to_string();
    }

    let lower = addr.to_lowercase();

    // Confidential Liquid segwit (blech32): strict structural check only.
    if cur == "LBTC" {
        for hrp in LBTC_BLECH32_HRPS {
            let prefix = format!("{hrp}1");
            if let Some(body) = lower.strip_prefix(&prefix) {
                return (6..=200).contains(&body.len())
                    && body.chars().all(|c| BECH32_CHARSET.contains(c));
            }
        }
    }

    // bech32 / bech32m segwit.
    if let Some((hrp, data, _spec)) = bech32_decode(addr) {
        if cur == "BTC" && !BTC_BECH32_HRPS.contains(&hrp.as_str()) {
            return false;
        }
        if cur == "LBTC" && !LBTC_BECH32_HRPS.contains(&hrp.as_str()) {
            return false;
        }
        if cur != "BTC" && cur != "LBTC" {
            return false;
        }
        return valid_segwit_program(&data);
    }

    // base58check legacy / P2SH.
    if let Some(payload) = base58check_decode(addr) {
        if !payload.is_empty() {
            let version = payload[0];
            if cur == "BTC" {
                return BTC_BASE58_VERSIONS.contains(&version);
            }
            if cur == "LBTC" {
                // Liquid uses distinct version bytes; a BTC-network version
                // byte here would be a wrong-network address.
                return !BTC_BASE58_VERSIONS.contains(&version);
            }
            return false;
        }
    }

    false
}

/// py `_validate_swap_destination` (boltz_manager.py:223-232). Validates a
/// swap destination address whose asset network is not known from the
/// call signature (refund/claim): accepts a structurally valid BTC or
/// Liquid address. The literal `"wallet"` keyword is handled by the
/// caller, not here.
pub fn validate_swap_destination(destination: &str) -> bool {
    validate_onchain_address(destination, "BTC") || validate_onchain_address(destination, "LBTC")
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test vectors below were generated by a standalone Python script
    // (`scratchpad/gen_addrs.py`) implementing the SAME algorithms as py
    // `boltz_manager.py` (base58check double-SHA256, bech32/bech32m
    // polymod) over deterministic byte payloads (`bytes(range(20))` /
    // `bytes(range(32))`) — not copied from any live wallet or exchange.

    const BTC_P2PKH: &str = "112D2adLM3UKy4Z4giRbReR6gjWuvHUqB";
    const BTC_P2SH: &str = "31h38a54tFMrR8kzBnP2241MFD2EUHtGha";
    const BTC_TESTNET_P2PKH: &str = "mfWyW5fc9NUj75YAnFgoRLrjxgLDn2MMth";
    const BTC_BECH32_V0: &str = "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345";
    const BTC_BECH32M_V1: &str = "bc1pqqqsyqcyq5rqwzqfpg9scrgwpugpzysnzs23v9ccrydpk8qarc0sg5tmnz";
    const BTC_TESTNET_BECH32: &str = "tb1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysnl25zw8";
    const LBTC_BECH32_EX: &str = "ex1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysnl9jfc5";

    #[test]
    fn btc_p2pkh_valid_for_btc() {
        assert!(validate_onchain_address(BTC_P2PKH, "BTC"));
    }

    #[test]
    fn btc_p2pkh_rejected_for_lbtc() {
        // Control: a valid BTC-network base58 version byte must be
        // rejected as a Liquid destination (wrong network).
        assert!(!validate_onchain_address(BTC_P2PKH, "LBTC"));
    }

    #[test]
    fn btc_p2sh_valid_for_btc() {
        assert!(validate_onchain_address(BTC_P2SH, "BTC"));
    }

    #[test]
    fn btc_testnet_p2pkh_accepted_for_btc_currency() {
        // "network" here means asset network, not mainnet/testnet/regtest.
        assert!(validate_onchain_address(BTC_TESTNET_P2PKH, "BTC"));
    }

    #[test]
    fn base58_bad_checksum_rejected() {
        // Flip the last character of a known-valid address: this breaks
        // the double-SHA256 checksum with overwhelming probability.
        let mut corrupted = BTC_P2PKH.to_string();
        corrupted.pop();
        corrupted.push(if BTC_P2PKH.ends_with('B') { 'C' } else { 'B' });
        assert!(!validate_onchain_address(&corrupted, "BTC"));
    }

    #[test]
    fn bech32_v0_valid_for_btc() {
        assert!(validate_onchain_address(BTC_BECH32_V0, "BTC"));
    }

    #[test]
    fn bech32m_v1_valid_for_btc() {
        assert!(validate_onchain_address(BTC_BECH32M_V1, "BTC"));
    }

    #[test]
    fn testnet_bech32_accepted_for_btc_currency() {
        assert!(validate_onchain_address(BTC_TESTNET_BECH32, "BTC"));
    }

    #[test]
    fn bech32_wrong_hrp_for_currency_rejected() {
        // A BTC bech32 address is not a valid LBTC destination.
        assert!(!validate_onchain_address(BTC_BECH32_V0, "LBTC"));
    }

    #[test]
    fn bech32_bad_checksum_rejected() {
        let mut corrupted = BTC_BECH32_V0.to_string();
        corrupted.pop();
        corrupted.push(if BTC_BECH32_V0.ends_with('5') {
            '4'
        } else {
            '5'
        });
        assert!(!validate_onchain_address(&corrupted, "BTC"));
    }

    #[test]
    fn bech32_mixed_case_rejected() {
        // BIP173: mixed-case bech32 is invalid regardless of checksum.
        let mixed = "bc1QQQQsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345";
        assert!(!validate_onchain_address(mixed, "BTC"));
    }

    #[test]
    fn lbtc_unconfidential_segwit_valid_for_lbtc() {
        assert!(validate_onchain_address(LBTC_BECH32_EX, "LBTC"));
    }

    #[test]
    fn lbtc_unconfidential_segwit_rejected_for_btc() {
        assert!(!validate_onchain_address(LBTC_BECH32_EX, "BTC"));
    }

    #[test]
    fn confidential_liquid_blech32_structural_only_accept() {
        // No checksum verification for blech32 (documented limitation) —
        // strict prefix + charset + length only.
        let addr = "lq1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq";
        assert!(validate_onchain_address(addr, "LBTC"));
    }

    #[test]
    fn confidential_liquid_blech32_rejects_bad_charset() {
        // Control: same prefix, but an invalid bech32-charset character
        // ('b' is not in the bech32 charset) must be rejected.
        let addr = "lq1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        assert!(!validate_onchain_address(addr, "LBTC"));
    }

    #[test]
    fn empty_and_whitespace_rejected() {
        assert!(!validate_onchain_address("", "BTC"));
        assert!(!validate_onchain_address("  ", "BTC"));
        assert!(!validate_onchain_address(
            "bc1 qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
            "BTC"
        ));
    }

    #[test]
    fn garbage_string_rejected() {
        assert!(!validate_onchain_address("not-an-address-at-all", "BTC"));
    }

    #[test]
    fn lbtc_currency_alias_normalized() {
        assert!(validate_onchain_address(LBTC_BECH32_EX, "L-BTC"));
        assert!(validate_onchain_address(LBTC_BECH32_EX, "lbtc"));
    }

    #[test]
    fn validate_swap_destination_accepts_either_network() {
        assert!(validate_swap_destination(BTC_P2PKH));
        assert!(validate_swap_destination(LBTC_BECH32_EX));
    }

    #[test]
    fn validate_swap_destination_rejects_garbage() {
        assert!(!validate_swap_destination("garbage"));
    }
}
