//! The §18 configuration keys the store consumes, with their declared
//! change classes:
//!
//! - `daemon.state_path` — restart-only (relocation is stop,
//!   move-or-rebuild, start).
//! - `cas.segment_size` — operational-live: applies to newly rolled
//!   segments only.
//! - `cas.cache_limit` — operational-live: eviction policy shifts; the
//!   observability rules (§13) are unaffected.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crate::db::Store;

/// Store-side configuration. Defaults match §18's example config.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// `daemon.state_path` — the `.distill/` directory.
    pub state_path: PathBuf,
    /// `cas.segment_size` — segments roll at this cap.
    pub segment_size: u64,
    /// `cas.cache_limit` — the eviction size cap.
    pub cache_limit: u64,
    /// `pipeline.parallelism` — operational-live.
    pub parallelism: usize,
    /// `pipeline.batch_reserved_workers` — operational-live reserved
    /// capacity for the oldest pending batch job.
    pub batch_reserved_workers: usize,
}

impl StoreConfig {
    pub fn new(state_path: impl AsRef<Path>) -> Self {
        StoreConfig {
            state_path: state_path.as_ref().to_path_buf(),
            segment_size: 256 * 1024 * 1024,
            cache_limit: 20 * 1024 * 1024 * 1024,
            parallelism: 8,
            batch_reserved_workers: 1,
        }
    }

    pub fn validate_scheduler(&self) -> Result<(), ConfigValidationError> {
        validate_scheduler(self.parallelism, self.batch_reserved_workers)
    }
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigValidationError {
    ParallelismZero,
    BatchReservationOutOfBounds { got: usize, max: usize },
    NonLoopbackAddress(SocketAddr),
}

impl std::fmt::Display for ConfigValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParallelismZero => f.write_str("pipeline.parallelism must be at least 1"),
            Self::BatchReservationOutOfBounds { got, max } => write!(
                f,
                "pipeline.batch_reserved_workers must be in 1..={max}, got {got}"
            ),
            Self::NonLoopbackAddress(addr) => {
                write!(f, "daemon.address must be loopback, got {addr}")
            }
        }
    }
}

impl std::error::Error for ConfigValidationError {}

fn validate_scheduler(parallelism: usize, reserved: usize) -> Result<(), ConfigValidationError> {
    if parallelism == 0 {
        return Err(ConfigValidationError::ParallelismZero);
    }
    let max = parallelism.saturating_sub(1).max(1);
    if reserved == 0 || reserved > max {
        return Err(ConfigValidationError::BatchReservationOutOfBounds { got: reserved, max });
    }
    Ok(())
}

/// Typed restart-only edits: values of the configuration file the running
/// process does not apply until it restarts. Nothing records them: the
/// process compares the values it started with against the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestartOnlyChange {
    StatePath(PathBuf),
    Address(SocketAddr),
    RsModPath(PathBuf),
    AutoCodegen(bool),
}

impl RestartOnlyChange {
    /// The validated `(key, value)` of each of `changes`, by key, one per
    /// key.
    pub fn key_values(
        changes: &[RestartOnlyChange],
    ) -> Result<Vec<(&'static str, String)>, ConfigValidationError> {
        let mut rows = changes
            .iter()
            .map(RestartOnlyChange::key_value)
            .collect::<Result<Vec<_>, _>>()?;
        rows.sort_by(|a, b| a.0.cmp(b.0));
        rows.dedup_by(|a, b| a.0 == b.0);
        Ok(rows)
    }

    fn key_value(&self) -> Result<(&'static str, String), ConfigValidationError> {
        Ok(match self {
            Self::StatePath(path) => ("daemon.state_path", path.to_string_lossy().into_owned()),
            Self::Address(address) => {
                if !address.ip().is_loopback() {
                    return Err(ConfigValidationError::NonLoopbackAddress(*address));
                }
                ("daemon.address", address.to_string())
            }
            Self::RsModPath(path) => ("codegen.rs_mod_path", path.to_string_lossy().into_owned()),
            Self::AutoCodegen(value) => ("codegen.auto_codegen", value.to_string()),
        })
    }
}

impl Store {
    /// Apply the operational-live subset of a validated candidate. Paths and
    /// other epoch/restart values are deliberately not copied here.
    pub fn apply_operational_config(
        &mut self,
        candidate: &StoreConfig,
    ) -> Result<(), ConfigValidationError> {
        candidate.validate_scheduler()?;
        let config = std::sync::Arc::make_mut(&mut self.read.config);
        config.segment_size = candidate.segment_size;
        config.cache_limit = candidate.cache_limit;
        config.parallelism = candidate.parallelism;
        config.batch_reserved_workers = candidate.batch_reserved_workers;
        Ok(())
    }
}

/// Error parsing a §18 byte-size literal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteSizeError {
    pub input: String,
}

impl std::fmt::Display for ByteSizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "malformed byte size `{}`: expected digits with an optional KiB/MiB/GiB/TiB suffix",
            self.input
        )
    }
}

impl std::error::Error for ByteSizeError {}

/// Parse §18's byte-size literal form (`"256MiB"`, `"20GiB"`): decimal
/// digits plus an optional binary-unit suffix. Only binary units are
/// pinned — `MB` is rejected, not guessed at.
pub fn parse_byte_size(s: &str) -> Result<u64, ByteSizeError> {
    let err = || ByteSizeError {
        input: s.to_owned(),
    };
    let (digits, multiplier) = match s.find(|c: char| !c.is_ascii_digit()) {
        None => (s, 1u64),
        Some(split) => {
            let mult = match &s[split..] {
                "KiB" => 1u64 << 10,
                "MiB" => 1u64 << 20,
                "GiB" => 1u64 << 30,
                "TiB" => 1u64 << 40,
                _ => return Err(err()),
            };
            (&s[..split], mult)
        }
    };
    if digits.is_empty() {
        return Err(err());
    }
    let n: u64 = digits.parse().map_err(|_| err())?;
    n.checked_mul(multiplier).ok_or_else(err)
}
