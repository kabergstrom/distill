//! The §18 configuration keys the store consumes, with their declared
//! change classes:
//!
//! - `daemon.state_path` — restart-only (relocation is stop,
//!   move-or-rebuild, start).
//! - `daemon.displaced_retention_days` — operational-live: applies at the
//!   next retention sweep (§14).
//! - `cas.segment_size` — operational-live: applies to newly rolled
//!   segments only.
//! - `cas.cache_limit` — operational-live: eviction policy shifts; the
//!   observability rules (§13) are unaffected.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use rusqlite::OptionalExtension;

use crate::db::{InputTxn, Store, StoreReader};
use crate::error::StoreError;
use crate::state::{
    ConfigurationEpoch, ConfigurationPoison, ConfigurationPoisonCode, ConfigurationState, DscpV1,
};

type PersistedConfigurationRow = (
    i64,
    Option<i64>,
    Option<i64>,
    Option<Vec<u8>>,
    Option<Vec<u8>>,
    Option<String>,
);

/// Store-side configuration. Defaults match §18's example config.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// `daemon.state_path` — the `.distill/` directory.
    pub state_path: PathBuf,
    /// `daemon.displaced_retention_days` (§14 quarantine window).
    pub displaced_retention_days: u32,
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
            displaced_retention_days: 7,
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
        | "assets.lineage_manifest"
        | "targets"
        | "modules.pipeline_dylib"
        | "tools" => ChangeClass::InputVersionedEpoch,
        "pipeline.parallelism"
        | "pipeline.max_dependency_depth"
        | "pipeline.batch_reserved_workers"
        | "cas.segment_size"
        | "cas.cache_limit"
        | "daemon.displaced_retention_days" => ChangeClass::OperationalLive,
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
        config.displaced_retention_days = candidate.displaced_retention_days;
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
        let active: u64 = self
            .conn
            .query_row(
                "SELECT active_generation FROM configuration_state WHERE id = 0",
                [],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .map_err(persistence)?
            .unwrap_or(0) as u64;
        let prior: u64 = self
            .conn
            .query_row(
                "SELECT COALESCE(MAX(generation), 0) FROM pending_restart",
                [],
                |r| r.get::<_, i64>(0),
            )
            .map_err(persistence)? as u64;
        let generation = active.max(prior) + 1;
        let txn = self.read.conn.transaction().map_err(persistence)?;
        txn.execute("DELETE FROM pending_restart", [])
            .map_err(persistence)?;
        for (key, value) in &rows {
            txn.execute(
                "INSERT INTO pending_restart(generation, config_key, config_value)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![generation as i64, key, value],
            )
            .map_err(persistence)?;
        }
        txn.commit().map_err(persistence)?;
        Ok(PendingRestart {
            generation,
            keys: rows.into_iter().map(|(key, _)| key.to_owned()).collect(),
        })
    }

    /// Clear a staged restart candidate that has been edited back to the
    /// active startup values. This is not an input event.
    pub fn clear_pending_restart(&mut self) -> Result<(), StoreError> {
        self.conn.execute("DELETE FROM pending_restart", [])?;
        Ok(())
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
        let mut stmt = self.conn.prepare(
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

    pub fn configuration_state(&self) -> Result<ConfigurationState, StoreError> {
        read_configuration_state(&self.conn)
    }
}

pub(crate) fn read_configuration_state(
    conn: &rusqlite::Connection,
) -> Result<ConfigurationState, StoreError> {
    {
        let row: Option<PersistedConfigurationRow> = conn
            .query_row(
                "SELECT active_generation, poison_code, poison_detail_version, poison_detail,
                        poison_reason_hash, poison_message
                 FROM configuration_state WHERE id = 0",
                [],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                },
            )
            .optional()?;
        let (generation, code, detail_version, detail, reason_hash, message) =
            row.unwrap_or((0, None, None, None, None, None));
        let epoch = std::sync::Arc::new(ConfigurationEpoch {
            generation: generation as u64,
        });
        Ok(match (code, detail_version, detail, reason_hash, message) {
            (None, None, None, None, None) => ConfigurationState::Ready(epoch),
            (Some(code), Some(detail_version), Some(detail), Some(reason_hash), Some(message)) => {
                let code = u16::try_from(code)
                    .ok()
                    .and_then(|code| ConfigurationPoisonCode::try_from(code).ok())
                    .ok_or_else(|| StoreError::InvalidConfiguration {
                        error: format!("unknown persisted configuration poison code {code}"),
                    })?;
                let reason_hash: [u8; 32] = reason_hash.try_into().map_err(|bytes: Vec<u8>| {
                    StoreError::InvalidConfiguration {
                        error: format!(
                            "persisted configuration poison reason hash has length {}, expected 32",
                            bytes.len()
                        ),
                    }
                })?;
                let detail_version =
                    u8::try_from(detail_version).map_err(|_| StoreError::InvalidConfiguration {
                        error: format!(
                            "unknown persisted configuration poison detail version {detail_version}"
                        ),
                    })?;
                if detail_version != 1 {
                    return Err(StoreError::InvalidConfiguration {
                        error: format!(
                            "unknown persisted configuration poison detail version {detail_version}"
                        ),
                    });
                }
                let detail =
                    DscpV1::from_canonical_detail_bytes(code, &detail).map_err(|error| {
                        StoreError::InvalidConfiguration {
                            error: error.to_string(),
                        }
                    })?;
                let reason = ConfigurationPoison {
                    code,
                    reason_hash,
                    detail: Box::new(detail),
                    message,
                };
                reason
                    .validate()
                    .map_err(|error| StoreError::InvalidConfiguration {
                        error: error.to_string(),
                    })?;
                ConfigurationState::Poisoned {
                    reason,
                    last_good: Some(epoch),
                }
            }
            _ => {
                return Err(StoreError::InvalidConfiguration {
                    error: "persisted configuration poison fields are incomplete".to_owned(),
                });
            }
        })
    }
}

impl InputTxn<'_> {
    /// Publish a validated configuration candidate, healing any prior poison
    /// while retaining the active generation selected by the configuration
    /// coordinator.
    pub fn publish_configuration_ready(&mut self, generation: u64) -> Result<(), StoreError> {
        self.txn.execute(
            "INSERT INTO configuration_state(
                 id, active_generation, input_version,
                 poison_code, poison_detail_version, poison_detail,
                 poison_reason_hash, poison_message
             ) VALUES (0, ?1, ?2, NULL, NULL, NULL, NULL, NULL)
             ON CONFLICT(id) DO UPDATE SET active_generation = excluded.active_generation,
               input_version = excluded.input_version,
               poison_code = NULL,
               poison_detail_version = NULL,
               poison_detail = NULL,
               poison_reason_hash = NULL,
               poison_message = NULL",
            rusqlite::params![generation as i64, self.version().0 as i64],
        )?;
        Ok(())
    }

    pub fn publish_configuration_poison(
        &mut self,
        reason: &DscpV1,
        message: &str,
    ) -> Result<(), StoreError> {
        let poison = ConfigurationPoison::from_reason(reason, message);
        poison
            .validate()
            .map_err(|error| StoreError::InvalidConfiguration {
                error: error.to_string(),
            })?;
        let detail = poison.detail.canonical_detail_bytes();
        self.txn.execute(
            "INSERT INTO configuration_state(
                 id, active_generation, input_version,
                 poison_code, poison_detail_version, poison_detail,
                 poison_reason_hash, poison_message
             ) VALUES (0, 0, ?1, ?2, 1, ?3, ?4, ?5)
             ON CONFLICT(id) DO UPDATE SET input_version = excluded.input_version,
               poison_code = excluded.poison_code,
               poison_detail_version = excluded.poison_detail_version,
               poison_detail = excluded.poison_detail,
               poison_reason_hash = excluded.poison_reason_hash,
               poison_message = excluded.poison_message",
            rusqlite::params![
                self.version().0 as i64,
                poison.code as u16,
                detail,
                poison.reason_hash.as_slice(),
                poison.message,
            ],
        )?;
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
        self.txn.execute(
            "INSERT INTO configuration_state(
                 id, active_generation, input_version,
                 poison_code, poison_detail_version, poison_detail,
                 poison_reason_hash, poison_message
             ) VALUES (0, ?1, ?2, NULL, NULL, NULL, NULL, NULL)
             ON CONFLICT(id) DO UPDATE SET active_generation = excluded.active_generation,
               input_version = excluded.input_version,
               poison_code = NULL,
               poison_detail_version = NULL,
               poison_detail = NULL,
               poison_reason_hash = NULL,
               poison_message = NULL",
            rusqlite::params![generation, self.version().0 as i64],
        )?;
        self.txn.execute("DELETE FROM pending_restart", [])?;
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
