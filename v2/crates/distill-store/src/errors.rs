//! Per-entity errors (LOCKLESS.md §4): the `errors` table holds one row per
//! current defect, scoped to the entity it is about, and a row goes away
//! when its entity heals. An error blocks what depends on its entity and
//! nothing else.
//!
//! Each producer owns a family of rows and replaces the whole family when
//! it publishes: namespace errors come from the scan (collisions, unreadable
//! skeletons and paths, unreadable subtrees).

use crate::db::{InputTxn, StoreReader};
use crate::error::StoreError;
use crate::state::{ErrorScope, NamespaceError};

/// The scan's namespace errors.
const NAMESPACE: i64 = 1;

impl InputTxn<'_> {
    /// Replace the namespace errors with `errors`, in canonical order
    /// (duplicate records collapse).
    pub fn set_namespace_errors(
        &mut self,
        errors: impl IntoIterator<Item = NamespaceError>,
    ) -> Result<Vec<NamespaceError>, StoreError> {
        let errors =
            NamespaceError::canonical_set(errors).map_err(StoreError::InvalidNamespaceError)?;
        self.txn
            .execute("DELETE FROM errors WHERE family = ?1", [NAMESPACE])?;
        for error in &errors {
            let scope = error.scope();
            self.txn.execute(
                "INSERT INTO errors(family, scope_kind, scope_id, identity, code, record, message)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    NAMESPACE,
                    scope.kind(),
                    scope.id(),
                    error.identity.as_slice(),
                    error.code as u16,
                    error
                        .persisted_bytes()
                        .map_err(StoreError::InvalidNamespaceError)?,
                    error.message,
                ],
            )?;
        }
        Ok(errors)
    }
}

impl StoreReader {
    /// Every namespace error, in canonical order.
    pub fn namespace_errors(&self) -> Result<Vec<NamespaceError>, StoreError> {
        self.decode_errors(
            "SELECT record FROM errors WHERE family = ?1",
            rusqlite::params![NAMESPACE],
        )
    }

    /// The namespace errors about `scope`.
    pub fn namespace_errors_about(
        &self,
        scope: &ErrorScope,
    ) -> Result<Vec<NamespaceError>, StoreError> {
        self.decode_errors(
            "SELECT record FROM errors WHERE family = ?1 AND scope_kind = ?2 AND scope_id = ?3",
            rusqlite::params![NAMESPACE, scope.kind(), scope.id()],
        )
    }

    fn decode_errors(
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
