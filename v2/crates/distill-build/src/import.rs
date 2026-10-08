//! Authoring import contexts, outcome-bearing watched read-sets, and the
//! deterministic prior-bundle fold (§8).

use std::collections::BTreeMap;

use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};
use distill_json::AuthoredValue;

pub use distill_pipeline_api::import::{
    DefaultImportRule, ImportEntry, ImportError, ImportOutput, ImportOutputError, ImportRuleId,
    NO_IMPORTER,
};

use crate::dslf::DslfV1;
use crate::query::{
    file_query_result_hash, normalize_identifier, normalize_path, FileQuery, IntakeError, RootName,
    RootedPath,
};
use crate::trace::{
    local_failure_fingerprint, CapabilityKey, Observed, RawFileFailureClass, RawFileOp,
    RawFileSubject, StableFailureFingerprint,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DirectoryOrigin {
    pub rules_bundle: BundleUuid,
    pub rule: ImportRuleId,
    pub group: RootedPath,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileContentObservation {
    pub path: RootedPath,
    pub hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FileDep {
    Read {
        path: String,
        observed: Observed<FileContentObservation>,
    },
    Probe {
        path: String,
        observed: Observed<Option<RootName>>,
    },
    Listing {
        query: FileQuery,
        observed: Observed<[u8; 32]>,
    },
    Capability {
        key: CapabilityKey,
        observed: Observed<[u8; 32]>,
    },
}

pub trait ImportBackend {
    fn read(&mut self, path: &str) -> Result<(RootedPath, Vec<u8>), RawFileFailureClass>;
    fn probe(&mut self, path: &str) -> Result<Option<RootName>, RawFileFailureClass>;
    fn enumerate(&mut self, query: &FileQuery) -> Result<Vec<RootedPath>, RawFileFailureClass>;
    fn capability(&mut self, key: &CapabilityKey) -> Option<[u8; 32]>;
}

pub struct ImportContext<'a, B: ImportBackend + ?Sized> {
    importer_id: String,
    sources: Vec<RootedPath>,
    backend: &'a mut B,
    read_set: Vec<FileDep>,
}

impl<'a, B: ImportBackend + ?Sized> ImportContext<'a, B> {
    pub fn new(
        importer_id: &str,
        mut sources: Vec<RootedPath>,
        backend: &'a mut B,
    ) -> Result<Self, IntakeError> {
        let importer_id = normalize_identifier(importer_id)?;
        sources.sort_unstable();
        sources.dedup();
        Ok(Self {
            importer_id,
            sources,
            backend,
            read_set: Vec::new(),
        })
    }

    pub fn sources(&self) -> &[RootedPath] {
        &self.sources
    }
    pub fn read_set(&self) -> &[FileDep] {
        &self.read_set
    }
    pub fn into_read_set(self) -> Vec<FileDep> {
        self.read_set
    }

    pub fn read(&mut self, path: &str) -> Result<Vec<u8>, ImportError> {
        let path = normalize_path(path).map_err(|error| self.intake_failure(error))?;
        match self.backend.read(&path) {
            Ok((rooted, bytes)) => {
                let observed = FileContentObservation {
                    path: rooted,
                    hash: *blake3::hash(&bytes).as_bytes(),
                };
                self.read_set.push(FileDep::Read {
                    path,
                    observed: Observed::Ok(observed),
                });
                Ok(bytes)
            }
            Err(class) => {
                let failure =
                    raw_failure(RawFileOp::Read, RawFileSubject::Path(path.clone()), class);
                self.read_set.push(FileDep::Read {
                    path,
                    observed: Observed::Err(failure.clone()),
                });
                Err(ImportError {
                    fingerprint: failure,
                })
            }
        }
    }

    pub fn probe(&mut self, path: &str) -> Result<bool, ImportError> {
        let path = normalize_path(path).map_err(|error| self.intake_failure(error))?;
        match self.backend.probe(&path) {
            Ok(root) => {
                let exists = root.is_some();
                self.read_set.push(FileDep::Probe {
                    path,
                    observed: Observed::Ok(root),
                });
                Ok(exists)
            }
            Err(class) => {
                let failure =
                    raw_failure(RawFileOp::Probe, RawFileSubject::Path(path.clone()), class);
                self.read_set.push(FileDep::Probe {
                    path,
                    observed: Observed::Err(failure.clone()),
                });
                Err(ImportError {
                    fingerprint: failure,
                })
            }
        }
    }

    pub fn enumerate(&mut self, query: &FileQuery) -> Result<Vec<RootedPath>, ImportError> {
        match self.backend.enumerate(query) {
            Ok(mut paths) => {
                paths.sort_unstable();
                paths.dedup();
                let hash = file_query_result_hash(&paths);
                self.read_set.push(FileDep::Listing {
                    query: query.clone(),
                    observed: Observed::Ok(hash),
                });
                Ok(paths)
            }
            Err(class) => {
                let failure = raw_failure(
                    RawFileOp::Enumerate,
                    RawFileSubject::Query(query.clone()),
                    class,
                );
                self.read_set.push(FileDep::Listing {
                    query: query.clone(),
                    observed: Observed::Err(failure.clone()),
                });
                Err(ImportError {
                    fingerprint: failure,
                })
            }
        }
    }

    pub fn importer_capability(&mut self, id: &str) -> Result<[u8; 32], ImportError> {
        let id = normalize_identifier(id).map_err(|error| self.intake_failure(error))?;
        let key = CapabilityKey::Importer(id);
        match self.backend.capability(&key) {
            Some(hash) => {
                self.read_set.push(FileDep::Capability {
                    key,
                    observed: Observed::Ok(hash),
                });
                Ok(hash)
            }
            None => {
                let failure = StableFailureFingerprint::MissingCapability { key: key.clone() };
                self.read_set.push(FileDep::Capability {
                    key,
                    observed: Observed::Err(failure.clone()),
                });
                Err(ImportError {
                    fingerprint: failure,
                })
            }
        }
    }

    fn intake_failure(&self, error: IntakeError) -> ImportError {
        let error_code = match error {
            IntakeError::EmptyIdentifier => 1,
            IntakeError::IdentifierTooLong => 2,
            IntakeError::Nul => 3,
            IntakeError::InvalidPath => 4,
            IntakeError::EmptyQuery => 5,
            IntakeError::BundleRelativeWithoutOrigin => 6,
            IntakeError::InvalidGlob(_) => 7,
            IntakeError::AuthoringOnlyRestricted => 8,
        };
        let facts = DslfV1::Importer {
            importer_id: self.importer_id.clone(),
            importer_error_code: error_code,
            sources: self.sources.clone(),
        };
        ImportError {
            fingerprint: local_failure_fingerprint(&facts)
                .expect("ImportContext canonicalizes its source set"),
        }
    }
}

fn raw_failure(
    op: RawFileOp,
    subject: RawFileSubject,
    class: RawFileFailureClass,
) -> StableFailureFingerprint {
    StableFailureFingerprint::RawFile { op, subject, class }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ImportedEntry {
    pub uuid: AssetUuid,
    pub type_uuid: TypeUuid,
    pub value: AuthoredValue,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ImportRecord {
    pub importer: String,
    pub sources: Vec<RootedPath>,
    pub watch: bool,
    pub read_set: Vec<FileDep>,
    pub origin: Option<DirectoryOrigin>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ImportedBundle {
    pub bundle_uuid: BundleUuid,
    pub primary: Option<String>,
    pub entries: BTreeMap<String, ImportedEntry>,
    pub settings: AuthoredValue,
    pub record: ImportRecord,
}

pub struct FoldRequest {
    pub output: ImportOutput,
    /// Explicit import over an existing destination replaces settings;
    /// watched/reimport passes `None` to preserve the recorded value.
    pub explicit_settings: Option<AuthoredValue>,
    pub default_settings: AuthoredValue,
    pub importer: String,
    pub sources: Vec<RootedPath>,
    pub watch: bool,
    pub read_set: Vec<FileDep>,
    pub origin: Option<DirectoryOrigin>,
}

pub trait IdentitySource {
    fn next_asset(&mut self) -> AssetUuid;
    fn next_bundle(&mut self) -> BundleUuid;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoldError {
    DeclaredPrimaryMissing {
        local_id: String,
    },
    PriorPrimaryVanished {
        bundle: BundleUuid,
        local_id: String,
    },
    PrimaryRequired,
    InvalidImporter(IntakeError),
}

pub fn fold_import(
    prior: Option<&ImportedBundle>,
    request: FoldRequest,
    ids: &mut impl IdentitySource,
) -> Result<ImportedBundle, FoldError> {
    let importer = normalize_identifier(&request.importer).map_err(FoldError::InvalidImporter)?;
    let (output_entries, output_primary) = request.output.into_parts();
    let mut entries = BTreeMap::new();
    for (local_id, entry) in output_entries {
        let uuid = prior
            .and_then(|p| p.entries.get(&local_id))
            .map_or_else(|| ids.next_asset(), |entry| entry.uuid);
        entries.insert(
            local_id,
            ImportedEntry {
                uuid,
                type_uuid: entry.type_uuid,
                value: entry.value,
            },
        );
    }

    let primary = if let Some(declared) = output_primary {
        if !entries.contains_key(&declared) {
            return Err(FoldError::DeclaredPrimaryMissing { local_id: declared });
        }
        Some(declared)
    } else if let Some(prior_primary) = prior.and_then(|p| p.primary.as_ref()) {
        if !entries.contains_key(prior_primary) {
            return Err(FoldError::PriorPrimaryVanished {
                bundle: prior.expect("primary came from prior").bundle_uuid,
                local_id: prior_primary.clone(),
            });
        }
        Some(prior_primary.clone())
    } else {
        match entries.len() {
            0 => None,
            1 => entries.keys().next().cloned(),
            _ => return Err(FoldError::PrimaryRequired),
        }
    };

    let settings = request
        .explicit_settings
        .unwrap_or_else(|| prior.map_or(request.default_settings, |p| p.settings.clone()));
    Ok(ImportedBundle {
        bundle_uuid: prior.map_or_else(|| ids.next_bundle(), |p| p.bundle_uuid),
        primary,
        entries,
        settings,
        record: ImportRecord {
            importer,
            sources: request.sources,
            watch: request.watch,
            read_set: request.read_set,
            origin: request.origin,
        },
    })
}
