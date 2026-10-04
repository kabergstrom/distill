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


use crate::db::{meta_get_u64, meta_set_u64, InputTxn, Store, StoreReader};
use crate::error::StoreError;
use crate::state::{ConfigurationEpoch, ConfigurationState};

/// `store_meta` key of the active configuration generation: the last ready
/// configuration's, or the adopted restart's.
const CONFIGURATION_GENERATION: &str = "configuration_generation";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeClass {
    InputVersionedEpoch,
    OperationalLive,
    RestartOnly,
}

/// §18's total change-class table. Unknown keys return `None`: shipping
/// one without adding a row is a detectable spec/implementation defect.
pub fn change_class(key: &str) -> Option<ChangeClass> {
    Some(match key {
        "assets.roots"
        | "assets.schema_path"
        | "targets"
        | "modules.pipeline_dylib"
        | "tools" => ChangeClass::InputVersionedEpoch,
        "pipeline.parallelism"
        | "pipeline.max_dependency_depth"
        | "pipeline.batch_reserved_workers"
        | "cas.segment_size"
        | "cas.cache_limit" => ChangeClass::OperationalLive,
        "daemon.state_path" | "daemon.address" | "codegen.rs_mod_path" | "codegen.auto_codegen" => {
            ChangeClass::RestartOnly
        }
        _ => return None,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigValidationError {
    ParallelismZero,
    BatchReservationOutOfBounds { got: usize, max: usize },
    NonLoopbackAddress(SocketAddr),
    EmptyRestartChangeSet,
    Persistence(String),
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
            Self::EmptyRestartChangeSet => f.write_str("restart-only change set is empty"),
            Self::Persistence(error) => write!(f, "configuration state persistence: {error}"),
        }
    }
}

impl std::error::Error for ConfigValidationError {}

fn persistence(error: impl std::fmt::Display) -> ConfigValidationError {
    ConfigValidationError::Persistence(error.to_string())
}

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

/// Typed restart-only edits. Values validate before any pending generation
/// is recorded; active configuration and input version remain unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestartOnlyChange {
    StatePath(PathBuf),
    Address(SocketAddr),
    RsModPath(PathBuf),
    AutoCodegen(bool),
}

impl RestartOnlyChange {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRestart {
    pub generation: u64,
    pub keys: Vec<String>,
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

    /// Stage and validate restart-only changes without advancing the input
    /// version or replacing the active configuration generation.
    pub fn stage_pending_restart(
        &mut self,
        changes: &[RestartOnlyChange],
    ) -> Result<PendingRestart, ConfigValidationError> {
        if changes.is_empty() {
            return Err(ConfigValidationError::EmptyRestartChangeSet);
        }
        let mut rows = changes
            .iter()
            .map(RestartOnlyChange::key_value)
            .collect::<Result<Vec<_>, _>>()?;
        rows.sort_by(|a, b| a.0.cmp(b.0));
        rows.dedup_by(|a, b| a.0 == b.0);
        let generation = self
            .write_txn(|store| {
                let active = meta_get_u64(&store.conn, CONFIGURATION_GENERATION)?.unwrap_or(0);
                let prior = store.conn
                    .prepare_cached("SELECT COALESCE(MAX(generation), 0) FROM pending_restart")?
                    .query_row(
                        [],
                        |r| r.get::<_, i64>(0),
                    )? as u64;
                let generation = active.max(prior) + 1;
                store.conn
                    .prepare_cached("DELETE FROM pending_restart")?
                    .execute([])?;
                for (key, value) in &rows {
                    store.conn
                        .prepare_cached(
                            "INSERT INTO pending_restart(generation, config_key, config_value)
                         VALUES (?1, ?2, ?3)",
                        )?
                        .execute(rusqlite::params![generation as i64, key, value])?;
                }
                Ok(generation)
            })
            .map_err(persistence)?;
        Ok(PendingRestart {
            generation,
            keys: rows.into_iter().map(|(key, _)| key.to_owned()).collect(),
        })
    }

    /// Clear a staged restart candidate that has been edited back to the
    /// active startup values. This is not an input event.
    pub fn clear_pending_restart(&mut self) -> Result<(), StoreError> {
        self.write_txn(|store| {
            store.conn
                .prepare_cached("DELETE FROM pending_restart")?
                .execute([])?;
            Ok(())
        })
    }
}

impl StoreReader {

    pub fn operational_config(&self) -> StoreConfig {
        (*self.config).clone()
    }

    pub fn pending_restart(&self) -> Result<Option<PendingRestart>, StoreError> {
        let generation: Option<i64> =
            self.conn
                .query_row("SELECT MAX(generation) FROM pending_restart", [], |r| {
                    r.get(0)
                })?;
        let Some(generation) = generation else {
            return Ok(None);
        };
        let mut stmt = self.conn.prepare_cached(
            "SELECT config_key FROM pending_restart WHERE generation = ?1 ORDER BY config_key",
        )?;
        let keys = stmt
            .query_map([generation], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(PendingRestart {
            generation: generation as u64,
            keys,
        }))
    }

    /// The configuration status: the error the stored errors select (see
    /// [`StoreReader::configuration_error`]) over the active generation.
    pub fn configuration_state(&self) -> Result<ConfigurationState, StoreError> {
        let epoch = std::sync::Arc::new(ConfigurationEpoch {
            generation: self.configuration_generation()?,
        });
        Ok(match self.configuration_error()? {
            None => ConfigurationState::Ready(epoch),
            Some(reason) => ConfigurationState::Failed {
                reason,
                last_good: Some(epoch),
            },
        })
    }

    /// The active configuration generation (0 before any).
    pub fn configuration_generation(&self) -> Result<u64, StoreError> {
        Ok(meta_get_u64(&self.conn, CONFIGURATION_GENERATION)?.unwrap_or(0))
    }
}

impl InputTxn<'_> {
    /// Make `generation` the active configuration generation: written only
    /// when it changes.
    pub(crate) fn set_configuration_generation(&mut self, generation: u64) -> Result<(), StoreError> {
        if self.reader().configuration_generation()? != generation {
            meta_set_u64(&self.txn, CONFIGURATION_GENERATION, generation)?;
        }
        Ok(())
    }

    /// Startup adoption: pending values take effect and only now advance
    /// the input version.
    pub fn adopt_pending_restart(&mut self) -> Result<u64, StoreError> {
        let generation: Option<i64> =
            self.txn
                .query_row("SELECT MAX(generation) FROM pending_restart", [], |r| {
                    r.get(0)
                })?;
        let generation = generation.ok_or_else(|| StoreError::InvalidConfiguration {
            error: "no pending-restart generation to adopt".to_owned(),
        })?;
        self.set_configuration_generation(generation as u64)?;
        self.txn
            .prepare_cached("DELETE FROM pending_restart")?
            .execute([])?;
        Ok(generation as u64)
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
