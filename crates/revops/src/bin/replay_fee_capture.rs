#![forbid(unsafe_code)]

use std::env;
use std::fs::File;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::process::ExitCode;

use revops_fees::replay::{replay_fee_capture, ReplayError};
use revops_fees::replay_wire::{
    parse_fee_capture, validate_capture_manifest, FeeCaptureManifestV0, FeeCycleReplayV0,
    WireValue, MAX_REPLAY_ENVELOPE_BYTES,
};
use serde::Serialize;

const EXIT_EXACT: u8 = 0;
const EXIT_MISMATCH: u8 = 1;
const EXIT_INPUT: u8 = 2;
const MAX_REPLAY_WINDOW_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug)]
enum Mode {
    Capture(PathBuf),
    Manifest {
        manifest: PathBuf,
        capture_dir: PathBuf,
    },
}

#[derive(Debug, Serialize)]
struct FileResult {
    file: String,
    status: &'static str,
    capture_seq: Option<u64>,
    evaluated_channel_count: u64,
    adjustment_count: u64,
    mismatch_count: usize,
    error: Option<String>,
}

impl FileResult {
    fn input_error(path: &Path, error: impl Into<String>) -> Self {
        Self {
            file: path.display().to_string(),
            status: "error",
            capture_seq: None,
            evaluated_channel_count: 0,
            adjustment_count: 0,
            mismatch_count: 0,
            error: Some(error.into()),
        }
    }
}

#[derive(Debug, Default, Serialize)]
struct Verdict {
    commit: Option<String>,
    run_id: Option<String>,
    capture_count: usize,
    evaluated_channel_count: u64,
    adjustment_count: u64,
    mismatch_count: usize,
    results: Vec<FileResult>,
    error: Option<String>,
}

fn main() -> ExitCode {
    let (exit_code, verdict) = match parse_args(env::args().skip(1)) {
        Ok(mode) => run(mode),
        Err(error) => (
            EXIT_INPUT,
            Verdict {
                error: Some(error),
                ..Verdict::default()
            },
        ),
    };
    println!(
        "{}",
        serde_json::to_string(&verdict).expect("verdict contains only serializable values")
    );
    ExitCode::from(exit_code)
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Mode, String> {
    let mut capture = None;
    let mut manifest = None;
    let mut capture_dir = None;
    let mut args = args.peekable();

    while let Some(flag) = args.next() {
        let slot = match flag.as_str() {
            "--capture" => &mut capture,
            "--manifest" => &mut manifest,
            "--capture-dir" => &mut capture_dir,
            _ => return Err(format!("unknown argument {flag:?}")),
        };
        if slot.is_some() {
            return Err(format!("duplicate argument {flag:?}"));
        }
        let value = args
            .next()
            .filter(|value| !value.starts_with("--"))
            .ok_or_else(|| format!("{flag} requires one local path"))?;
        *slot = Some(PathBuf::from(value));
    }

    match (capture, manifest, capture_dir) {
        (Some(capture), None, None) => Ok(Mode::Capture(capture)),
        (None, Some(manifest), Some(capture_dir)) => Ok(Mode::Manifest {
            manifest,
            capture_dir,
        }),
        _ => Err(
            "expected exactly --capture <file> or --manifest <file> --capture-dir <dir>"
                .to_string(),
        ),
    }
}

fn run(mode: Mode) -> (u8, Verdict) {
    match mode {
        Mode::Capture(path) => run_capture(path),
        Mode::Manifest {
            manifest,
            capture_dir,
        } => run_manifest(&manifest, &capture_dir),
    }
}

fn run_capture(path: PathBuf) -> (u8, Verdict) {
    match read_capture(&path) {
        Ok((capture, _bytes)) => replay_captures(vec![(path, capture)], None),
        Err(error) => (
            EXIT_INPUT,
            Verdict {
                results: vec![FileResult::input_error(&path, &error)],
                error: Some(error),
                ..Verdict::default()
            },
        ),
    }
}

fn run_manifest(manifest_path: &Path, capture_dir: &Path) -> (u8, Verdict) {
    let manifest_bytes = match read_bounded(manifest_path) {
        Ok(bytes) => bytes,
        Err(error) => {
            return (
                EXIT_INPUT,
                Verdict {
                    results: vec![FileResult::input_error(manifest_path, &error)],
                    error: Some(error),
                    ..Verdict::default()
                },
            );
        }
    };
    let manifest: FeeCaptureManifestV0 = match serde_json::from_slice(&manifest_bytes) {
        Ok(manifest) => manifest,
        Err(error) => {
            let error = format!("cannot parse manifest {}: {error}", manifest_path.display());
            return (
                EXIT_INPUT,
                Verdict {
                    results: vec![FileResult::input_error(manifest_path, &error)],
                    error: Some(error),
                    ..Verdict::default()
                },
            );
        }
    };

    let canonical_dir = match capture_dir.canonicalize() {
        Ok(path) if path.is_dir() => path,
        Ok(_) => {
            return manifest_input_error(
                &manifest,
                format!(
                    "capture directory {} is not a directory",
                    capture_dir.display()
                ),
            );
        }
        Err(error) => {
            return manifest_input_error(
                &manifest,
                format!(
                    "cannot open capture directory {}: {error}",
                    capture_dir.display()
                ),
            );
        }
    };
    let mut capture_paths = Vec::with_capacity(manifest.attempts.len());
    let mut capture_values = Vec::with_capacity(manifest.attempts.len());
    let mut input_results = Vec::new();
    let mut window_bytes = match checked_window_bytes(0, manifest_bytes.len()) {
        Ok(bytes) => bytes,
        Err(error) => return manifest_input_error(&manifest, error),
    };
    for attempt in &manifest.attempts {
        let Some(filename) = attempt.filename.as_deref() else {
            let error = format!(
                "manifest attempt {} has no capture filename",
                attempt.capture_seq
            );
            return manifest_input_error(&manifest, error);
        };
        let path = match confined_capture_path(&canonical_dir, filename) {
            Ok(path) => path,
            Err(error) => {
                input_results.push(FileResult::input_error(&capture_dir.join(filename), error));
                continue;
            }
        };
        match read_capture(&path) {
            Ok((capture, bytes)) => match checked_window_bytes(window_bytes, bytes) {
                Ok(total) => {
                    window_bytes = total;
                    capture_paths.push(path);
                    capture_values.push(capture);
                }
                Err(error) => input_results.push(FileResult::input_error(&path, error)),
            },
            Err(error) => input_results.push(FileResult::input_error(&path, error)),
        }
    }
    if !input_results.is_empty() {
        return (
            EXIT_INPUT,
            Verdict {
                run_id: Some(manifest.capture_run_id),
                results: input_results,
                error: Some("one or more manifest captures could not be read".to_string()),
                ..Verdict::default()
            },
        );
    }

    if let Err(error) = validate_capture_manifest(&manifest, &capture_values) {
        return manifest_input_error(&manifest, error.to_string());
    }
    replay_captures(
        capture_paths.into_iter().zip(capture_values).collect(),
        Some(manifest.capture_run_id),
    )
}

fn manifest_input_error(manifest: &FeeCaptureManifestV0, error: String) -> (u8, Verdict) {
    (
        EXIT_INPUT,
        Verdict {
            run_id: Some(manifest.capture_run_id.clone()),
            error: Some(error),
            ..Verdict::default()
        },
    )
}

fn confined_capture_path(canonical_dir: &Path, filename: &str) -> Result<PathBuf, String> {
    let relative = Path::new(filename);
    let mut components = relative.components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
        return Err(format!(
            "capture filename {filename:?} must be one local basename"
        ));
    }
    let candidate = canonical_dir.join(relative);
    let canonical = candidate
        .canonicalize()
        .map_err(|error| format!("cannot open capture {}: {error}", candidate.display()))?;
    if !canonical.starts_with(canonical_dir) {
        return Err(format!(
            "capture {} escapes the explicit capture directory",
            candidate.display()
        ));
    }
    Ok(canonical)
}

fn read_to_limit(mut reader: impl Read, maximum: usize) -> Result<Vec<u8>, String> {
    let limit =
        u64::try_from(maximum).map_err(|_| "input byte limit does not fit u64".to_string())?;
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() > maximum {
        return Err(format!(
            "input is at least {} bytes; maximum is {maximum}",
            bytes.len()
        ));
    }
    Ok(bytes)
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, String> {
    let file =
        File::open(path).map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    read_to_limit(file, MAX_REPLAY_ENVELOPE_BYTES)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))
}

fn checked_window_bytes(current: u64, additional: usize) -> Result<u64, String> {
    let additional = u64::try_from(additional)
        .map_err(|_| "capture window byte count does not fit u64".to_string())?;
    let total = current
        .checked_add(additional)
        .ok_or_else(|| "capture window aggregate byte count overflow".to_string())?;
    if total > MAX_REPLAY_WINDOW_BYTES {
        return Err(format!(
            "capture window aggregate is {total} bytes; maximum is {MAX_REPLAY_WINDOW_BYTES}"
        ));
    }
    Ok(total)
}

fn read_capture(path: &Path) -> Result<(FeeCycleReplayV0, usize), String> {
    let bytes = read_bounded(path)?;
    let byte_count = bytes.len();
    parse_fee_capture(&bytes)
        .map(|capture| (capture, byte_count))
        .map_err(|error| format!("invalid capture {}: {error}", path.display()))
}

fn replay_captures(
    captures: Vec<(PathBuf, FeeCycleReplayV0)>,
    manifest_run_id: Option<String>,
) -> (u8, Verdict) {
    let mut verdict = Verdict {
        capture_count: captures.len(),
        run_id: manifest_run_id,
        ..Verdict::default()
    };
    let mut saw_input_error = false;

    for (path, capture) in captures {
        verdict.evaluated_channel_count = verdict
            .evaluated_channel_count
            .saturating_add(capture.completeness.evaluated_channels);
        let adjustments = adjustment_count(&capture);
        verdict.adjustment_count = verdict.adjustment_count.saturating_add(adjustments);

        if let Err(error) = merge_identity(&mut verdict, &capture) {
            saw_input_error = true;
            verdict.results.push(FileResult {
                file: path.display().to_string(),
                status: "error",
                capture_seq: Some(capture.capture_seq),
                evaluated_channel_count: capture.completeness.evaluated_channels,
                adjustment_count: adjustments,
                mismatch_count: 0,
                error: Some(error),
            });
            continue;
        }

        match replay_fee_capture(&capture) {
            Ok(_) => verdict.results.push(FileResult {
                file: path.display().to_string(),
                status: "exact",
                capture_seq: Some(capture.capture_seq),
                evaluated_channel_count: capture.completeness.evaluated_channels,
                adjustment_count: adjustments,
                mismatch_count: 0,
                error: None,
            }),
            Err(error) if replay_error_is_input(&error) => {
                saw_input_error = true;
                verdict.results.push(FileResult {
                    file: path.display().to_string(),
                    status: "error",
                    capture_seq: Some(capture.capture_seq),
                    evaluated_channel_count: capture.completeness.evaluated_channels,
                    adjustment_count: adjustments,
                    mismatch_count: 0,
                    error: Some(error.to_string()),
                });
            }
            Err(error) => {
                verdict.mismatch_count += 1;
                verdict.results.push(FileResult {
                    file: path.display().to_string(),
                    status: "mismatch",
                    capture_seq: Some(capture.capture_seq),
                    evaluated_channel_count: capture.completeness.evaluated_channels,
                    adjustment_count: adjustments,
                    mismatch_count: 1,
                    error: Some(error.to_string()),
                });
            }
        }
    }

    if saw_input_error {
        verdict.error = Some("one or more captures were malformed or inconsistent".to_string());
        (EXIT_INPUT, verdict)
    } else if verdict.mismatch_count > 0 {
        (EXIT_MISMATCH, verdict)
    } else {
        (EXIT_EXACT, verdict)
    }
}

fn merge_identity(verdict: &mut Verdict, capture: &FeeCycleReplayV0) -> Result<(), String> {
    match verdict.run_id.as_deref() {
        Some(run_id) if run_id != capture.capture_run_id => {
            return Err(format!(
                "capture run ID {:?} does not match selected run {:?}",
                capture.capture_run_id, run_id
            ));
        }
        None => verdict.run_id = Some(capture.capture_run_id.clone()),
        Some(_) => {}
    }

    let commit = capture
        .producer
        .get("python_commit")
        .and_then(|value| match value {
            WireValue::String(value) if !value.is_empty() => Some(value.clone()),
            _ => None,
        })
        .ok_or_else(|| "$.producer.python_commit must be a nonempty string".to_string())?;
    match verdict.commit.as_deref() {
        Some(expected) if expected != commit => Err(format!(
            "producer commit {commit:?} does not match selected commit {expected:?}"
        )),
        None => {
            verdict.commit = Some(commit);
            Ok(())
        }
        Some(_) => Ok(()),
    }
}

fn adjustment_count(capture: &FeeCycleReplayV0) -> u64 {
    let Some(WireValue::Array(outcomes)) = capture.expected.get("ordered_outcomes") else {
        return 0;
    };
    outcomes
        .iter()
        .filter(|outcome| {
            matches!(
                outcome,
                WireValue::Object(fields) if fields.contains_key("adjustment")
            )
        })
        .count() as u64
}

fn replay_error_is_input(error: &ReplayError) -> bool {
    match error {
        ReplayError::Shape { .. } | ReplayError::DecisionInput(_) => true,
        ReplayError::Transcript { actual, .. } => {
            actual.contains("unknown field")
                || actual.contains("missing field")
                || actual.contains("invalid:")
        }
        ReplayError::ValueMismatch { .. } => false,
    }
}

#[cfg(test)]
mod bounded_read_tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn bounded_reader_consumes_at_most_limit_plus_one() {
        let error = read_to_limit(Cursor::new(vec![0_u8; 9]), 8).unwrap_err();
        assert!(error.contains("maximum is 8"), "{error}");
    }

    #[test]
    fn aggregate_window_limit_rejects_overflow_and_excess() {
        assert_eq!(
            checked_window_bytes(MAX_REPLAY_WINDOW_BYTES - 1, 1).unwrap(),
            MAX_REPLAY_WINDOW_BYTES
        );
        assert!(checked_window_bytes(MAX_REPLAY_WINDOW_BYTES, 1).is_err());
        assert!(checked_window_bytes(u64::MAX, 1).is_err());
    }
}
