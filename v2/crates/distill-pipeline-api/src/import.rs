//! What an importer returns (§8).

use std::collections::BTreeMap;

use distill_core::id::TypeUuid;
use distill_json::AuthoredValue;

use crate::failure::StableFailureFingerprint;
use crate::query::{normalize_identifier, IntakeError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportError {
    pub fingerprint: StableFailureFingerprint,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ImportEntry {
    pub type_uuid: TypeUuid,
    pub value: AuthoredValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ImportOutput {
    entries: BTreeMap<String, ImportEntry>,
    primary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportOutputError {
    InvalidIdentifier(IntakeError),
    ReservedLocalId,
    DuplicateLocalId,
    PrimaryAlreadyDeclared,
}

impl ImportOutput {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            primary: None,
        }
    }

    pub fn entries(&self) -> &BTreeMap<String, ImportEntry> {
        &self.entries
    }

    pub fn entry(
        &mut self,
        local_id: &str,
        type_uuid: TypeUuid,
        value: AuthoredValue,
    ) -> Result<(), ImportOutputError> {
        let local_id =
            normalize_identifier(local_id).map_err(ImportOutputError::InvalidIdentifier)?;
        if local_id.starts_with('$') {
            return Err(ImportOutputError::ReservedLocalId);
        }
        if self.entries.contains_key(&local_id) {
            return Err(ImportOutputError::DuplicateLocalId);
        }
        self.entries
            .insert(local_id, ImportEntry { type_uuid, value });
        Ok(())
    }

    pub fn primary(&mut self, local_id: &str) -> Result<(), ImportOutputError> {
        if self.primary.is_some() {
            return Err(ImportOutputError::PrimaryAlreadyDeclared);
        }
        self.primary =
            Some(normalize_identifier(local_id).map_err(ImportOutputError::InvalidIdentifier)?);
        Ok(())
    }

    /// The entries by local id, and the declared primary local id.
    pub fn into_parts(self) -> (BTreeMap<String, ImportEntry>, Option<String>) {
        (self.entries, self.primary)
    }
}

impl Default for ImportOutput {
    fn default() -> Self {
        Self::new()
    }
}
