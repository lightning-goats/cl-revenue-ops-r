//! Pure input validation for every string LN+ (an untrusted external API)
//! hands back to us before it reaches a signing operation or a CLN RPC
//! call. Ports `lnplus_swaps.py:24-58` (constants + `_valid_pubkey` /
//! `_valid_connect_addr` / `_parse_ts`) and `_validate_challenge`
//! (py 136-148, gate 15).

use crate::error::LnPlusError;

pub const CHALLENGE_MAX_LEN: usize = 500;
pub const CHALLENGE_FORBIDDEN_PREFIXES: [&str; 5] = ["lnbc", "lntb", "lnbcrt", "cbor", "psbt"];
/// I3: sane host:port shape for a connect address sourced from the LN+
/// API — no shell metacharacters, no whitespace; length-capped separately.
pub const ADDR_MAX_LEN: usize = 300;

/// py 35-36 `_valid_pubkey`: `^0[23][0-9a-fA-F]{64}$`.
pub fn valid_pubkey(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 66 {
        return false;
    }
    if bytes[0] != b'0' || !(bytes[1] == b'2' || bytes[1] == b'3') {
        return false;
    }
    bytes[2..].iter().all(|b| b.is_ascii_hexdigit())
}

/// py 39-41 `_valid_connect_addr`: `^[A-Za-z0-9.\-_:\[\]]+$`, non-empty,
/// strictly under `ADDR_MAX_LEN`.
pub fn valid_connect_addr(value: &str) -> bool {
    if value.is_empty() || value.len() >= ADDR_MAX_LEN {
        return false;
    }
    value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | ':' | '[' | ']'))
}

/// One LN+ timestamp field as received from JSON: either a bare epoch
/// number or an ISO-8601 string. Mirrors Python's duck-typed
/// `Union[int, float, str]` input to `_parse_ts` (py 44-58).
#[derive(Debug, Clone, PartialEq)]
pub enum TsValue {
    Epoch(f64),
    Iso(String),
}

/// py 44-58 `_parse_ts`. B3: a TZ-less ISO string must NOT be interpreted
/// in the node's local timezone — LN+ deadlines/`ends` timestamps are UTC,
/// so a missing offset is treated as `+00:00`, never as local time.
///
/// This Rust port only needs to parse the exact subset of ISO-8601 LN+
/// actually emits: `YYYY-MM-DDTHH:MM:SS[.ffffff][+HH:MM|Z]`. A `Z` suffix
/// is normalized to `+00:00` first, exactly mirroring Python's
/// `value.replace("Z", "+00:00")`.
pub fn parse_ts(value: &TsValue) -> Option<i64> {
    match value {
        TsValue::Epoch(n) => {
            if *n > 0.0 {
                Some(*n as i64)
            } else {
                None
            }
        }
        TsValue::Iso(s) => parse_iso8601_utc(s),
    }
}

fn parse_iso8601_utc(raw: &str) -> Option<i64> {
    let s = raw.replace('Z', "+00:00");
    let (date_time, offset) = split_offset(&s)?;
    let (date, time) = date_time
        .split_once('T')
        .or_else(|| date_time.split_once(' '))?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: i64 = date_parts.next()?.parse().ok()?;
    let day: i64 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() {
        return None;
    }

    let time_main = time.split('.').next().unwrap_or(time);
    let mut time_parts = time_main.split(':');
    let hour: i64 = time_parts.next()?.parse().ok()?;
    let minute: i64 = time_parts.next()?.parse().ok()?;
    let second: i64 = time_parts.next().unwrap_or("0").parse().ok()?;
    if time_parts.next().is_some() {
        return None;
    }

    let offset_seconds = match offset {
        Some(off) => parse_offset_seconds(off)?,
        None => 0, // B3: no offset -> UTC, never local time.
    };

    let days = days_from_civil(year, month, day)?;
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(hour.checked_mul(3600)?)?
        .checked_add(minute.checked_mul(60)?)?
        .checked_add(second)?
        .checked_sub(offset_seconds)?;
    Some(seconds)
}

/// Split `2026-07-05T12:00:00+02:00` into (`2026-07-05T12:00:00`, `Some("+02:00")`),
/// or `2026-07-05T12:00:00` into (.., `None`). The date portion never
/// contains `+`/`-` after position 0 in a way that could be confused for
/// an offset here because we search from the `T`/space split point only.
fn split_offset(s: &str) -> Option<(&str, Option<&str>)> {
    let t_pos = s.find(['T', ' '])?;
    let (_, tail) = s.split_at(t_pos);
    // Search for a +HH:MM or -HH:MM offset marker within the time portion
    // (skip index 0, which is 'T').
    if let Some(rel) = tail[1..].find(['+', '-']) {
        let split_at = t_pos + 1 + rel;
        Some((&s[..split_at], Some(&s[split_at..])))
    } else {
        Some((s, None))
    }
}

fn parse_offset_seconds(off: &str) -> Option<i64> {
    if off == "+00:00" || off == "-00:00" {
        return Some(0);
    }
    let sign = if off.starts_with('-') { -1 } else { 1 };
    let rest = &off[1..];
    let (h, m) = rest.split_once(':')?;
    let hours: i64 = h.parse().ok()?;
    let minutes: i64 = m.parse().ok()?;
    Some(sign * (hours * 3600 + minutes * 60))
}

/// Days since the Unix epoch (1970-01-01) for a proleptic-Gregorian civil
/// date, via Howard Hinnant's `days_from_civil` algorithm.
fn days_from_civil(y: i64, m: i64, d: i64) -> Option<i64> {
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (m + 9) % 12; // [0, 11]
    let doy = (153 * mp + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    Some(era * 146_097 + doe - 719_468)
}

/// py 136-148 `_validate_challenge` — gate 15. Only sign strings that look
/// like a genuine LN+ auth challenge. Returns `Ok(())` or the exact
/// rejection reason as an [`LnPlusError`] (never actually signs anything —
/// signing is the caller's job via an injected RPC port).
pub fn validate_challenge(message: Option<&str>) -> Result<(), LnPlusError> {
    let message = match message {
        Some(m) if !m.is_empty() => m,
        _ => return Err(LnPlusError::new("LN+ challenge missing")),
    };
    if message.chars().count() > CHALLENGE_MAX_LEN {
        return Err(LnPlusError::new(
            "LN+ challenge suspiciously long — refusing to sign",
        ));
    }
    if !message.chars().all(is_printable) {
        return Err(LnPlusError::new(
            "LN+ challenge not printable — refusing to sign",
        ));
    }
    let lowered = message.to_ascii_lowercase();
    if CHALLENGE_FORBIDDEN_PREFIXES
        .iter()
        .any(|p| lowered.starts_with(p))
    {
        return Err(LnPlusError::new(
            "LN+ challenge looks like an invoice/PSBT — refusing to sign",
        ));
    }
    Ok(())
}

/// Approximation of Python `str.isprintable()`: no control characters
/// (categories Cc/Cf-ish) and not the empty check (handled by caller).
/// LN+ challenges are ASCII in practice; this errs toward rejecting any
/// C0/C1 control or DEL character, which is the property gate 15 cares
/// about (nothing that could smuggle terminal escapes or is otherwise
/// non-human-readable into something the operator's node signs).
fn is_printable(c: char) -> bool {
    !c.is_control()
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- valid_pubkey --

    #[test]
    fn pubkey_accepts_02_and_03_prefix() {
        let a = format!("02{}", "a".repeat(64));
        let b = format!("03{}", "b".repeat(64));
        assert!(valid_pubkey(&a));
        assert!(valid_pubkey(&b));
    }

    #[test]
    fn pubkey_rejects_wrong_prefix_length_or_chars() {
        assert!(!valid_pubkey(&format!("04{}", "a".repeat(64))));
        assert!(!valid_pubkey(&format!("02{}", "a".repeat(63))));
        assert!(!valid_pubkey(&format!("02{}", "z".repeat(64))));
        assert!(!valid_pubkey(""));
    }

    // -- valid_connect_addr --

    #[test]
    fn connect_addr_accepts_host_port_and_onion() {
        assert!(valid_connect_addr("203.0.113.5:9735"));
        assert!(valid_connect_addr("abcdefghijklmnop.onion:9735"));
        assert!(valid_connect_addr("[2001:db8::1]:9735"));
    }

    #[test]
    fn connect_addr_rejects_empty_too_long_or_bad_chars() {
        assert!(!valid_connect_addr(""));
        assert!(!valid_connect_addr(&"a".repeat(300)));
        assert!(!valid_connect_addr("host;rm -rf /"));
        assert!(!valid_connect_addr("host with space"));
    }

    // -- parse_ts --

    #[test]
    fn parse_ts_epoch_number() {
        assert_eq!(
            parse_ts(&TsValue::Epoch(1_700_000_000.0)),
            Some(1_700_000_000)
        );
        assert_eq!(parse_ts(&TsValue::Epoch(0.0)), None);
        assert_eq!(parse_ts(&TsValue::Epoch(-5.0)), None);
    }

    #[test]
    fn parse_ts_iso_with_z_is_utc() {
        // 2023-11-14T22:13:20Z == 1700000000
        assert_eq!(
            parse_ts(&TsValue::Iso("2023-11-14T22:13:20Z".to_string())),
            Some(1_700_000_000)
        );
    }

    #[test]
    fn parse_ts_iso_tz_less_string_is_treated_as_utc_not_local_b3() {
        // B3: a TZ-less ISO string must NOT be interpreted in the node's
        // local timezone. This is the control: if the fix regressed to
        // "naive local time", this would differ from the Z-suffixed
        // equivalent above (unless the host happens to run in UTC).
        assert_eq!(
            parse_ts(&TsValue::Iso("2023-11-14T22:13:20".to_string())),
            Some(1_700_000_000),
            "TZ-less ISO must parse identically to the Z-suffixed form"
        );
    }

    #[test]
    fn parse_ts_iso_with_explicit_offset() {
        // 2023-11-15T00:13:20+02:00 == same instant as 22:13:20Z
        assert_eq!(
            parse_ts(&TsValue::Iso("2023-11-15T00:13:20+02:00".to_string())),
            Some(1_700_000_000)
        );
    }

    #[test]
    fn parse_ts_invalid_string_is_none() {
        assert_eq!(parse_ts(&TsValue::Iso("not a date".to_string())), None);
    }

    // -- validate_challenge (gate 15) --

    #[test]
    fn challenge_accepts_plain_text() {
        assert!(validate_challenge(Some("Please sign this LN+ auth challenge: abc123")).is_ok());
    }

    #[test]
    fn challenge_rejects_missing_or_empty() {
        assert!(validate_challenge(None).is_err());
        assert!(validate_challenge(Some("")).is_err());
    }

    #[test]
    fn challenge_rejects_too_long() {
        let long = "a".repeat(CHALLENGE_MAX_LEN + 1);
        assert!(validate_challenge(Some(&long)).is_err());
    }

    #[test]
    fn challenge_at_max_len_is_ok() {
        let exact = "a".repeat(CHALLENGE_MAX_LEN);
        assert!(validate_challenge(Some(&exact)).is_ok());
    }

    #[test]
    fn challenge_rejects_non_printable() {
        assert!(validate_challenge(Some("hello\x00world")).is_err());
    }

    #[test]
    fn challenge_rejects_invoice_and_psbt_shaped_strings() {
        for bad in [
            "lnbc1000n1...",
            "lntb500u1...",
            "lnbcrt1u1...",
            "cbor:deadbeef",
            "psbt:cHNidP8",
        ] {
            assert!(
                validate_challenge(Some(bad)).is_err(),
                "expected rejection for {bad:?}"
            );
        }
    }

    #[test]
    fn challenge_prefix_check_is_case_insensitive() {
        assert!(validate_challenge(Some("LNBC1000n1...")).is_err());
    }
}
