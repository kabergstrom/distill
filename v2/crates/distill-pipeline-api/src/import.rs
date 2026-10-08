//! What an importer returns (§8).

use std::collections::BTreeMap;

use distill_core::id::TypeUuid;
use distill_json::AuthoredValue;

use crate::failure::StableFailureFingerprint;
use crate::query::{normalize_identifier, FileQuery, IntakeError};

/// Stable authored identity of an import rule (§8): an `ImportRule.id` in a
/// rules bundle or a [`DefaultImportRule::id`]. Globally unique across both.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ImportRuleId(pub [u8; 16]);

/// How an import rule folds matched files into imports (§8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DirectoryGrouping {
    /// One import per matched file.
    PerFile,
    /// One import per (root, parent directory, stem).
    ByStem,
}

/// The importer id an authored rule names to claim the files it matches and
/// import nothing (§8 "Default imports"): a committed opt-out from the
/// default rules. No importer may register under it.
pub const NO_IMPORTER: &str = "none";

/// A default import rule (§8 "Default imports"), registered by a pipeline
/// module: the importer that imports a file no explicit import or authored
/// rule claims, chosen by path alone. Its imports use the importer's default
/// settings and record an ordinary directory origin under the root's
/// reserved default-rules bundle UUID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultImportRule {
    /// Recorded in every output's origin: kept for a compatible change of
    /// importer, minted anew to orphan the outputs instead.
    pub id: ImportRuleId,
    /// Path selector only, e.g. `**/*.{glb,gltf}`. No two default rules may
    /// match one file.
    pub matches: FileQuery,
    pub group: DirectoryGrouping,
    /// `Importer::ID`, registered by the same module.
    pub importer: String,
    /// Output template over `{stem}` / `{name}`, beside the sources:
    /// `"{name}.bundle"`.
    pub output: String,
}

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
