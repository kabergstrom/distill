//! Per-entity errors (LOCKLESS.md §4): the `errors` table holds one row per
//! current defect, scoped to the entity it is about, and a row goes away
//! when its entity heals. An error blocks what depends on its entity and
//! nothing else.
//!
//! Each producer owns a family of rows and replaces the whole family when
//! it publishes: namespace errors come from the scan (collisions, unreadable
//! skeletons and paths, unreadable subtrees).
//!
//! A scan that could not observe some subjects leaves a pending rejection:
//! its namespace errors, the configuration error it found (a directory
//! alias), and the subjects whose revalidation heals it. The configuration
//! source's own error (a rejected `distill.toml`) is a family of its own,
//! as is the pipeline candidate's failure.
//! Every writer leaves these rows alone except the scan and configuration
//! publications that change them, so a write elsewhere (an RPC authoring
//! write) cannot erase them and a restart keeps them.

use rusqlite::OptionalExtension;

use crate::db::{InputTxn, StoreReader};
use crate::error::StoreError;
use crate::state::{
    ConfigurationError, ConfigurationErrorCode, DscpV1, ErrorScope, NamespaceError, PipelineFailure,
};

/// The scan's namespace errors.
pub(crate) const NAMESPACE: i64 = 1;
/// The namespace errors of the pending scan rejection.
const SCAN_REJECTION: i64 = 2;
/// The configuration error of the pending scan rejection (at most one row).
const SCAN_REJECTION_CONFIGURATION: i64 = 3;
/// The configuration source's error (at most one row).
const CONFIGURATION_SOURCE: i64 = 4;
/// The pipeline candidate's failure (at most one row): `code` and
/// `identity` are the failure's, `record` its origin and cleanup (two
/// little-endian `u16`s).
const PIPELINE: i64 = 5;

/// A scan rejection waiting for its subjects to heal. `subjects` are the
/// rejected physical paths in the daemon's platform path encoding.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScanRejectionRecord {
    pub errors: Vec<NamespaceError>,
    pub configuration: Option<ConfigurationError>,
    pub subjects: Vec<Vec<u8>>,
}

impl InputTxn<'_> {
    /// Replace the namespace errors with `errors`, in canonical order
    /// (duplicate records collapse).
    pub fn set_namespace_errors(
        &mut self,
        errors: impl IntoIterator<Item = NamespaceError>,
    ) -> Result<Vec<NamespaceError>, StoreError> {
        self.replace_namespace_family(NAMESPACE, errors)
    }

    /// Replace the pending scan rejection; `None` heals it.
    pub fn set_scan_rejection(
        &mut self,
        rejection: Option<&ScanRejectionRecord>,
    ) -> Result<(), StoreError> {
        let empty = ScanRejectionRecord::default();
        let rejection = rejection.unwrap_or(&empty);
        self.replace_namespace_family(SCAN_REJECTION, rejection.errors.iter().cloned())?;
        self.replace_configuration_family(
            SCAN_REJECTION_CONFIGURATION,
            rejection.configuration.as_ref(),
        )?;
        self.txn
            .prepare_cached("DELETE FROM scan_rejection_subjects")?
            .execute([])?;
        for subject in &rejection.subjects {
            self.txn
                .prepare_cached("INSERT OR IGNORE INTO scan_rejection_subjects(path) VALUES (?1)")?
                .execute([subject])?;
        }
        Ok(())
    }

    /// Replace the configuration source's error; `None` heals it.
    pub fn set_configuration_source_error(
        &mut self,
        error: Option<&ConfigurationError>,
    ) -> Result<(), StoreError> {
        self.replace_configuration_family(CONFIGURATION_SOURCE, error)
    }

    /// The configuration status the stored errors select (see
    /// [`StoreReader::configuration_error`]); with none, `generation`
    /// becomes the active one. Returns the selected error.
    pub fn publish_configuration_status(
        &mut self,
        generation: u64,
    ) -> Result<Option<ConfigurationError>, StoreError> {
        let selected = self.reader().configuration_error()?;
        if selected.is_none() {
            self.set_configuration_generation(generation)?;
        }
        Ok(selected)
    }

    /// Replace the version's pipeline failure; `None` heals it.
    pub(crate) fn set_pipeline_failure(
        &mut self,
        failure: Option<&PipelineFailure>,
    ) -> Result<(), StoreError> {
        self.txn
            .prepare_cached("DELETE FROM errors WHERE family = ?1")?
            .execute([PIPELINE])?;
        let Some(failure) = failure else {
            return Ok(());
        };
        let mut record = (failure.origin as u16).to_le_bytes().to_vec();
        record.extend_from_slice(&(failure.cleanup as u16).to_le_bytes());
        let scope = ErrorScope::Pipeline;
        self.txn
            .prepare_cached(
                "INSERT INTO errors(family, scope_kind, scope_id, identity, code, record, message)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?
            .execute(rusqlite::params![
                PIPELINE,
                scope.kind(),
                scope.id(),
                failure.identity.as_slice(),
                failure.code as u16,
                record,
                failure.message,
            ])?;
        Ok(())
    }

    fn replace_configuration_family(
        &mut self,
        family: i64,
        error: Option<&ConfigurationError>,
    ) -> Result<(), StoreError> {
        self.txn
            .execute("DELETE FROM errors WHERE family = ?1", [family])?;
        let Some(error) = error else {
            return Ok(());
        };
        error
            .validate()
            .map_err(|error| StoreError::InvalidConfiguration {
                error: error.to_string(),
            })?;
        let scope = ErrorScope::Configuration;
        self.txn
            .prepare_cached(
                "INSERT INTO errors(family, scope_kind, scope_id, identity, code, record, message)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?
            .execute(rusqlite::params![
                family,
                scope.kind(),
                scope.id(),
                error.reason_hash.as_slice(),
                error.code as u16,
                error.detail.canonical_detail_bytes(),
                error.message,
            ])?;
        Ok(())
    }

    fn replace_namespace_family(
        &mut self,
        family: i64,
        errors: impl IntoIterator<Item = NamespaceError>,
    ) -> Result<Vec<NamespaceError>, StoreError> {
        let held = self
            .txn
            .prepare_cached("SELECT identity, record FROM errors WHERE family = ?1")?
            .query_map([family], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
        self.write_namespace_family(family, errors, held)
    }

    /// Make `family`, whose rows are `held` (identity to record), hold
    /// exactly `errors`. Only the rows that change are written: a family
    /// holds the namespace's defects, and most publications change none.
    pub(crate) fn write_namespace_family(
        &mut self,
        family: i64,
        errors: impl IntoIterator<Item = NamespaceError>,
        mut held: std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<Vec<NamespaceError>, StoreError> {
        let errors =
            NamespaceError::canonical_set(errors).map_err(StoreError::InvalidNamespaceError)?;
        for error in &errors {
            let record = error
                .persisted_bytes()
                .map_err(StoreError::InvalidNamespaceError)?;
            if held.remove(error.identity.as_slice()).as_ref() == Some(&record) {
                continue;
            }
            write_namespace_error(&self.txn, family, error, &record)?;
        }
        for identity in held.keys() {
            self.txn
                .prepare_cached("DELETE FROM errors WHERE family = ?1 AND identity = ?2")?
                .execute(rusqlite::params![family, identity])?;
        }
        Ok(errors)
    }
}

/// Write `error`, whose persisted bytes are `record`, as a row of `family`.
pub(crate) fn write_namespace_error(
    conn: &rusqlite::Connection,
    family: i64,
    error: &NamespaceError,
    record: &[u8],
) -> Result<(), StoreError> {
    let scope = error.scope();
    conn.prepare_cached(
        "INSERT OR REPLACE INTO errors(family, scope_kind, scope_id, identity, code, record, message)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?
    .execute(rusqlite::params![
        family,
        scope.kind(),
        scope.id(),
        error.identity.as_slice(),
        error.code as u16,
        record,
        error.message,
    ])?;
    Ok(())
}

impl StoreReader {
    /// Every namespace error, in canonical order: the scan's and the pending
    /// scan rejection's.
    pub fn namespace_errors(&self) -> Result<Vec<NamespaceError>, StoreError> {
        self.decode_errors(
            "SELECT record FROM errors WHERE family IN (?1, ?2)",
            rusqlite::params![NAMESPACE, SCAN_REJECTION],
        )
    }

    /// The pending scan rejection, if a scan left one.
    pub fn scan_rejection(&self) -> Result<Option<ScanRejectionRecord>, StoreError> {
        let errors = self.decode_errors(
            "SELECT record FROM errors WHERE family = ?1",
            rusqlite::params![SCAN_REJECTION],
        )?;
        let configuration = self.configuration_family(SCAN_REJECTION_CONFIGURATION)?;
        let subjects = self.query_rows(
            "SELECT path FROM scan_rejection_subjects ORDER BY path",
            [],
            |row| row.get::<_, Vec<u8>>(0),
        )?;
        if errors.is_empty() && configuration.is_none() && subjects.is_empty() {
            return Ok(None);
        }
        Ok(Some(ScanRejectionRecord {
            errors,
            configuration,
            subjects,
        }))
    }

    /// The configuration source's error, if its last observation was
    /// rejected.
    pub fn configuration_source_error(&self) -> Result<Option<ConfigurationError>, StoreError> {
        self.configuration_family(CONFIGURATION_SOURCE)
    }

    /// The configuration error the stored errors select: the canonical one
    /// of the source's error and the pending scan rejection's.
    pub fn configuration_error(&self) -> Result<Option<ConfigurationError>, StoreError> {
        let source = self.configuration_family(CONFIGURATION_SOURCE)?;
        let scan = self.configuration_family(SCAN_REJECTION_CONFIGURATION)?;
        ConfigurationError::select_canonical(source.into_iter().chain(scan)).map_err(|error| {
            StoreError::InvalidConfiguration {
                error: error.to_string(),
            }
        })
    }

    /// The version's pipeline failure, if its candidate failed.
    pub fn pipeline_failure(&self) -> Result<Option<PipelineFailure>, StoreError> {
        let row = self
            .conn
            .prepare_cached("SELECT code, identity, record, message FROM errors WHERE family = ?1")?
            .query_row([PIPELINE], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .optional()?;
        let Some((code, identity, record, message)) = row else {
            return Ok(None);
        };
        let invalid = |detail: &str| StoreError::InvalidPipelineState {
            detail: detail.to_owned(),
        };
        let code = u16::try_from(code).map_err(|_| invalid("stored pipeline failure code"))?;
        let identity: [u8; 32] = identity
            .try_into()
            .map_err(|_| invalid("stored pipeline failure identity is not 32 bytes"))?;
        let [origin_lo, origin_hi, cleanup_lo, cleanup_hi]: [u8; 4] = record
            .try_into()
            .map_err(|_| invalid("stored pipeline failure record is not 4 bytes"))?;
        PipelineFailure::from_wire(
            code,
            u16::from_le_bytes([origin_lo, origin_hi]),
            u16::from_le_bytes([cleanup_lo, cleanup_hi]),
            identity,
            message,
        )
        .map(Some)
        .map_err(StoreError::InvalidPipelineFailure)
    }

    fn configuration_family(&self, family: i64) -> Result<Option<ConfigurationError>, StoreError> {
        let row = self
            .conn
            .query_row(
                "SELECT code, identity, record, message FROM errors WHERE family = ?1",
                [family],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((code, reason_hash, detail, message)) = row else {
            return Ok(None);
        };
        let invalid = |error: String| StoreError::InvalidConfiguration { error };
        let code = u16::try_from(code)
            .ok()
            .and_then(|code| ConfigurationErrorCode::try_from(code).ok())
            .ok_or_else(|| invalid(format!("unknown stored configuration error code {code}")))?;
        let reason_hash: [u8; 32] = reason_hash.try_into().map_err(|_| {
            invalid("stored configuration error identity is not 32 bytes".to_owned())
        })?;
        let detail = DscpV1::from_canonical_detail_bytes(code, &detail)
            .map_err(|error| invalid(error.to_string()))?;
        let error = ConfigurationError {
            code,
            reason_hash,
            detail: Box::new(detail),
            message,
        };
        error
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        Ok(Some(error))
    }

    pub(crate) fn decode_errors(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
    ) -> Result<Vec<NamespaceError>, StoreError> {
        let records = self.query_rows(sql, params, |row| row.get::<_, Vec<u8>>(0))?;
        let errors = records
            .iter()
            .map(|bytes| {
                NamespaceError::from_persisted_bytes(bytes)
                    .map_err(StoreError::InvalidNamespaceError)
            })
            .collect::<Result<Vec<_>, _>>()?;
        NamespaceError::canonical_set(errors).map_err(StoreError::InvalidNamespaceError)
    }
}
