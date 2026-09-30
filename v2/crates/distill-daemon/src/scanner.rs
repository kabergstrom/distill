//! Filesystem-side authority for §14 scanning and §6 lineage repair.
//!
//! Logical paths are accepted only in their canonical root-relative form.
//! Physical traversal is confined to canonical configured roots, daemon-owned
//! paths are excluded, directory aliases/cycles are errors, and a file is
//! accepted only when its observation remains stable through the read.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, Metadata};
use std::io::Read;
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use distill_core::bootstrap::SCHEMA_LINEAGE_MANIFEST_TYPE_UUID;
use distill_core::id::{BundleFileHash, ContentHash};
use distill_rpc::{
    LineageManifestClaimant, LineageRepairDestination, OccupiedLineageDestinationKind,
};
use distill_store::db::StoreReader;
use distill_store::error::StoreError;
use distill_store::files::{
    FileKind, FileObservation, FileState, ObservedBundleFile, ObservedDiagnostic, ObservedDirectory,
    ObservedFile,
};
use distill_store::state::{PhysicalPathClaim, PhysicalPathFailureCode, PlatformPathBytes};
use unicode_normalization::{is_nfc, UnicodeNormalization};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetRoot {
    pub name: String,
    pub path: PathBuf,
    pub quarantine_dir: PathBuf,
}

impl AssetRoot {
    pub fn new(
        name: impl Into<String>,
        path: impl Into<PathBuf>,
        quarantine_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
            quarantine_dir: quarantine_dir.into(),
        }
    }
}

#[derive(Debug)]
pub enum ScanError {
    Multiple(Vec<ScanError>),
    InvalidRootName(String),
    DuplicateRootName(String),
    UnknownRoot(String),
    InvalidLogicalPath(String),
    InvalidPhysicalPath {
        root_name: String,
        raw_relative_path: PlatformPathBytes,
        failure: PhysicalPathFailureCode,
    },
    SameRootNormalizedPathCollision {
        root_name: String,
        normalized_path: String,
        claims: Vec<PhysicalPathClaim>,
    },
    RootUnavailable {
        root: String,
        path: PathBuf,
    },
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    SymlinkEscape {
        path: PathBuf,
        target: PathBuf,
    },
    DirectoryCycle {
        path: PathBuf,
    },
    DirectoryAlias {
        first_root: String,
        first: PathBuf,
        second_root: String,
        second: PathBuf,
    },
    DaemonOwnedDirectoryAlias {
        path: PathBuf,
        owned_path: PathBuf,
        kind: DaemonOwnedDirectoryKind,
    },
    FileIdentityChanged {
        path: PathBuf,
    },
    NonRegularFile {
        path: PathBuf,
    },
}

impl std::fmt::Display for ScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Multiple(errors) => write!(
                f,
                "{} canonical scan defects: {}",
                errors.len(),
                errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
            Self::InvalidRootName(name) => write!(f, "invalid canonical root name {name:?}"),
            Self::DuplicateRootName(name) => write!(f, "duplicate asset root {name:?}"),
            Self::UnknownRoot(name) => write!(f, "unknown asset root {name:?}"),
            Self::InvalidLogicalPath(path) => write!(f, "invalid canonical logical path {path:?}"),
            Self::InvalidPhysicalPath {
                root_name, failure, ..
            } => write!(
                f,
                "invalid physical path in root {root_name:?}: {failure:?}"
            ),
            Self::SameRootNormalizedPathCollision {
                root_name,
                normalized_path,
                ..
            } => write!(
                f,
                "distinct physical paths in root {root_name:?} normalize to {normalized_path:?}"
            ),
            Self::RootUnavailable { root, path } => {
                write!(
                    f,
                    "asset root {root:?} is unavailable at {}",
                    path.display()
                )
            }
            Self::Io { path, source } => write!(f, "scan I/O at {}: {source}", path.display()),
            Self::SymlinkEscape { path, target } => write!(
                f,
                "symlink {} escapes every configured root to {}",
                path.display(),
                target.display()
            ),
            Self::DirectoryCycle { path } => {
                write!(f, "directory cycle at {}", path.display())
            }
            Self::DirectoryAlias { first, second, .. } => write!(
                f,
                "one canonical directory is reachable as both {} and {}",
                first.display(),
                second.display()
            ),
            Self::DaemonOwnedDirectoryAlias {
                path,
                owned_path,
                kind,
                ..
            } => write!(
                f,
                "{} aliases {kind} directory {}",
                path.display(),
                owned_path.display()
            ),
            Self::FileIdentityChanged { path } => {
                write!(f, "file identity changed while reading {}", path.display())
            }
            Self::NonRegularFile { path } => {
                write!(
                    f,
                    "configured destination is not a regular file: {}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for ScanError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct CanonicalRoot {
    configured: AssetRoot,
    canonical_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct RootedScanner {
    roots: Arc<RwLock<BTreeMap<String, CanonicalRoot>>>,
    daemon_owned: Arc<RwLock<BTreeMap<PathBuf, DaemonOwnedDirectory>>>,
    revision: Arc<AtomicU64>,
}

/// A directory whose contents are produced or retained by the daemon and can
/// therefore never participate in the authored asset namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DaemonOwnedDirectoryKind {
    State,
    ModuleStaging,
    PackageOutput,
    CodegenOutput,
    Quarantine,
}

impl std::fmt::Display for DaemonOwnedDirectoryKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::State => "daemon state",
            Self::ModuleStaging => "pipeline module staging",
            Self::PackageOutput => "package output",
            Self::CodegenOutput => "codegen output",
            Self::Quarantine => "quarantine",
        };
        formatter.write_str(name)
    }
}

#[derive(Debug, Clone)]
struct DaemonOwnedDirectory {
    kind: DaemonOwnedDirectoryKind,
    path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScannedFileKind {
    File,
    Directory,
    Symlink,
}

/// One identity-checked raw-tree row. `content_hash` is present for every
/// regular-file observation, including a symlink whose admitted target is a
/// regular file; directories carry no byte identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedFile {
    pub root_name: String,
    pub normalized_path: String,
    pub kind: ScannedFileKind,
    pub modified_nanos: i64,
    pub size: u64,
    pub content_hash: Option<ContentHash>,
    raw_relative_path: PlatformPathBytes,
}

impl ScannedFile {
    /// The on-disk spelling of the path relative to its root, before
    /// normalization.
    pub fn raw_relative_path(&self) -> &PlatformPathBytes {
        &self.raw_relative_path
    }
}

/// A `.bundle` candidate remains in the report even when its envelope is
/// malformed. The coordinator, not traversal, decides whether the current
/// bytes yield a complete namespace skeleton or version-global poison.
#[derive(Debug, Clone)]
pub struct ScannedBundle {
    pub root_name: String,
    pub normalized_path: String,
    pub file_hash: BundleFileHash,
    pub bytes: Vec<u8>,
    pub parsed: Result<distill_bundle::Bundle, distill_bundle::BundleError>,
    pub namespace_skeleton: Option<distill_bundle::BundleNamespaceSkeleton>,
}

#[derive(Debug, Clone, Default)]
pub struct ScanSnapshot {
    pub files: BTreeMap<(String, String), ScannedFile>,
    logical_roots: BTreeMap<String, BTreeSet<String>>,
    pub bundles: BTreeMap<(String, String), Arc<ScannedBundle>>,
    directory_observations: BTreeMap<(String, String), DirectoryObservation>,
    directory_by_target: BTreeMap<PathBuf, (String, String, PathBuf)>,
    symlink_aliases: BTreeMap<(String, String), PathBuf>,
    aliases_by_target: BTreeMap<PathBuf, BTreeSet<(String, String)>>,
    diagnostics: BTreeMap<(String, String), ScanDiagnostic>,
}

/// A non-fatal filesystem observation that was excluded from the authored
/// namespace. Diagnostics are keyed by their canonical rooted path so an
/// incremental rescan replaces only the affected diagnostic rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanDiagnostic {
    DaemonOwnedDirectoryAlias {
        root_name: String,
        normalized_path: String,
        physical_path: PathBuf,
        owned_path: PathBuf,
        kind: DaemonOwnedDirectoryKind,
    },
    DirectoryCycle {
        root_name: String,
        normalized_path: String,
        path_chain: Vec<PathBuf>,
    },
}

impl std::fmt::Display for ScanDiagnostic {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DaemonOwnedDirectoryAlias {
                root_name,
                normalized_path,
                physical_path,
                owned_path,
                kind,
            } => write!(
                formatter,
                "daemon-owned-directory-alias: {root_name}/{normalized_path} ({}) aliases {kind} directory {}; subtree skipped",
                physical_path.display(),
                owned_path.display(),
            ),
            Self::DirectoryCycle {
                root_name,
                normalized_path,
                path_chain,
            } => write!(
                formatter,
                "directory-cycle: {root_name}/{normalized_path} revisits a canonical ancestor through {}; recursion edge skipped",
                path_chain
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join(" -> ")
            ),
        }
    }
}

/// The complete observation replacing a bounded set of logical path prefixes.
/// Applying a delta touches only keys below those prefixes; unrelated snapshot
/// rows and parsed bundle bytes are neither cloned nor enumerated.
#[derive(Debug, Clone)]
pub struct ScanDelta {
    affected: Vec<(String, String)>,
    observed: ScanSnapshot,
}

impl ScanSnapshot {
    /// Equality of filesystem authority, excluding parsed-object allocation
    /// details. Used to suppress watcher echoes of daemon-authored writes.
    pub fn same_observation(&self, other: &Self) -> bool {
        self.same_namespace_observation(other) && self.diagnostics == other.diagnostics
    }

    pub(crate) fn same_namespace_observation(&self, other: &Self) -> bool {
        self.files == other.files
            && self.bundles.len() == other.bundles.len()
            && self
                .bundles
                .values()
                .zip(other.bundles.values())
                .all(|(left, right)| {
                    left.root_name == right.root_name
                        && left.normalized_path == right.normalized_path
                        && left.file_hash == right.file_hash
                })
            && self.directory_observations == other.directory_observations
            && self.symlink_aliases == other.symlink_aliases
    }

    pub fn file_rows(&self) -> impl Iterator<Item = &ScannedFile> {
        self.files.values()
    }

    pub fn bundle_rows(&self) -> impl Iterator<Item = &ScannedBundle> {
        self.bundles.values().map(AsRef::as_ref)
    }

    pub fn diagnostic_rows(&self) -> impl Iterator<Item = &ScanDiagnostic> {
        self.diagnostics.values()
    }

    pub fn lineage_claimants(&self) -> Vec<LineageManifestClaimant> {
        let mut claimants = Vec::new();
        for bundle in self.bundles.values() {
            let Ok(parsed) = &bundle.parsed else {
                continue;
            };
            for (local_id, entry) in &parsed.assets {
                if entry.type_uuid == SCHEMA_LINEAGE_MANIFEST_TYPE_UUID {
                    claimants.push(LineageManifestClaimant {
                        root_name: bundle.root_name.clone(),
                        normalized_path: bundle.normalized_path.clone(),
                        bundle: parsed.uuid,
                        local_id: local_id.clone(),
                        asset: entry.uuid,
                        file_hash: bundle.file_hash,
                    });
                }
            }
        }
        claimants.sort();
        claimants.dedup();
        claimants
    }

    pub(crate) fn bundle_entries(
        &self,
    ) -> impl Iterator<Item = (&(String, String), &Arc<ScannedBundle>)> {
        self.bundles.iter()
    }

    pub(crate) fn bundle_at(&self, root: &str, path: &str) -> Option<Arc<ScannedBundle>> {
        self.bundles
            .get(&(root.to_owned(), path.to_owned()))
            .cloned()
    }

    pub fn apply_delta(&mut self, delta: ScanDelta) {
        for affected in &delta.affected {
            for key in matching_keys(&self.files, affected) {
                self.files.remove(&key);
                remove_logical_root(&mut self.logical_roots, &key.1, &key.0);
            }
            remove_matching(&mut self.bundles, affected);
            let removed_directories = matching_keys(&self.directory_observations, affected);
            for key in removed_directories {
                if let Some(observation) = self.directory_observations.remove(&key) {
                    self.directory_by_target.remove(&observation.canonical_path);
                }
            }
            let removed_aliases = matching_keys(&self.symlink_aliases, affected);
            for key in removed_aliases {
                if let Some(target) = self.symlink_aliases.remove(&key) {
                    remove_reverse_alias(&mut self.aliases_by_target, &target, &key);
                }
            }
            remove_matching(&mut self.diagnostics, affected);
        }
        for (key, file) in delta.observed.files {
            self.logical_roots
                .entry(key.1.clone())
                .or_default()
                .insert(key.0.clone());
            self.files.insert(key, file);
        }
        self.bundles.extend(delta.observed.bundles);
        for (key, observation) in delta.observed.directory_observations {
            self.directory_by_target.insert(
                observation.canonical_path.clone(),
                (
                    key.0.clone(),
                    key.1.clone(),
                    observation.physical_path.clone(),
                ),
            );
            self.directory_observations.insert(key, observation);
        }
        for (key, target) in delta.observed.symlink_aliases {
            self.aliases_by_target
                .entry(target.clone())
                .or_default()
                .insert(key.clone());
            self.symlink_aliases.insert(key, target);
        }
        self.diagnostics.extend(delta.observed.diagnostics);
    }
}

impl ScanDelta {
    pub fn affected_prefixes(&self) -> &[(String, String)] {
        &self.affected
    }

    pub fn observed_files(&self) -> impl Iterator<Item = &ScannedFile> {
        self.observed.files.values()
    }

    pub fn observed_bundles(&self) -> impl Iterator<Item = &ScannedBundle> {
        self.observed.bundles.values().map(AsRef::as_ref)
    }

    pub(crate) fn observed_bundle_entries(
        &self,
    ) -> impl Iterator<Item = (&(String, String), &Arc<ScannedBundle>)> {
        self.observed.bundles.iter()
    }

    pub fn is_same_observation(&self, baseline: &ScanSnapshot) -> bool {
        self.is_same_namespace_observation(baseline)
            && self.affected.iter().all(|affected| {
                matching_values(&baseline.diagnostics, affected)
                    .eq(matching_values(&self.observed.diagnostics, affected))
            })
    }

    pub(crate) fn is_same_namespace_observation(&self, baseline: &ScanSnapshot) -> bool {
        self.affected.iter().all(|affected| {
            matching_values(&baseline.files, affected)
                .eq(matching_values(&self.observed.files, affected))
                && matching_bundle_observations(&baseline.bundles, affected).eq(
                    matching_bundle_observations(&self.observed.bundles, affected),
                )
                && matching_values(&baseline.directory_observations, affected).eq(matching_values(
                    &self.observed.directory_observations,
                    affected,
                ))
                && matching_values(&baseline.symlink_aliases, affected)
                    .eq(matching_values(&self.observed.symlink_aliases, affected))
        })
    }
}

/// The last published observation an incremental scan is checked against:
/// either an in-memory [`ScanSnapshot`] or the store's scan tables
/// ([`StoredBaseline`]).
pub trait ScanBaseline {
    /// The on-disk spelling recorded for one rooted path.
    fn raw_relative_path(&self, root: &str, path: &str) -> Option<PlatformPathBytes>;
    /// The symlinked files whose canonical target lies at or below `canonical`.
    fn aliases_affected_by(&self, canonical: &Path) -> Vec<(String, String)>;
    /// The first traversed directory whose canonical path is `canonical`,
    /// with its physical path.
    fn directory_by_target(&self, canonical: &Path) -> Option<(String, String, PathBuf)>;
}

impl ScanBaseline for ScanSnapshot {
    fn raw_relative_path(&self, root: &str, path: &str) -> Option<PlatformPathBytes> {
        self.files
            .get(&(root.to_owned(), path.to_owned()))
            .map(|file| file.raw_relative_path.clone())
    }

    fn aliases_affected_by(&self, canonical: &Path) -> Vec<(String, String)> {
        aliases_affected_by(&self.aliases_by_target, canonical)
    }

    fn directory_by_target(&self, canonical: &Path) -> Option<(String, String, PathBuf)> {
        self.directory_by_target.get(canonical).cloned()
    }
}

/// [`ScanBaseline`] over the store's scan tables. A failed query answers as
/// if the row were absent and is kept for [`StoredBaseline::finish`], which
/// the caller checks before trusting the delta.
pub(crate) struct StoredBaseline<'a> {
    reader: &'a StoreReader,
    failure: RefCell<Option<StoreError>>,
}

impl<'a> StoredBaseline<'a> {
    pub(crate) fn new(reader: &'a StoreReader) -> Self {
        Self {
            reader,
            failure: RefCell::new(None),
        }
    }

    pub(crate) fn finish(self) -> Result<(), StoreError> {
        self.failure.into_inner().map_or(Ok(()), Err)
    }

    fn keep<T: Default>(&self, result: Result<T, StoreError>) -> T {
        result.unwrap_or_else(|error| {
            self.failure.borrow_mut().get_or_insert(error);
            T::default()
        })
    }
}

impl ScanBaseline for StoredBaseline<'_> {
    fn raw_relative_path(&self, root: &str, path: &str) -> Option<PlatformPathBytes> {
        self.keep(self.reader.observed_file(root, path))
            .map(|row| decode_raw_path(&row.file.raw_path))
    }

    fn aliases_affected_by(&self, canonical: &Path) -> Vec<(String, String)> {
        self.keep(self.reader.symlinks_targeting(&encode_path(canonical)))
            .into_iter()
            .filter(|row| {
                row.file
                    .symlink_target
                    .as_deref()
                    .is_some_and(|target| decode_path(target).starts_with(canonical))
            })
            .map(|row| (row.root_name, row.path))
            .collect()
    }

    fn directory_by_target(&self, canonical: &Path) -> Option<(String, String, PathBuf)> {
        self.keep(self.reader.directory_by_canonical(&encode_path(canonical)))
            .map(|row| (row.root_name, row.path, decode_path(&row.physical_path)))
    }
}

impl ScanSnapshot {
    /// The complete published observation, from the store's scan tables.
    pub(crate) fn load(reader: &StoreReader) -> Result<Self, StoreError> {
        Self::from_rows(
            reader.observed_files()?,
            reader.observed_directories()?,
            reader.scan_diagnostics()?,
            reader.bundle_files()?,
        )
    }

    /// The published observation at or below each of `prefixes`.
    pub(crate) fn load_under(
        reader: &StoreReader,
        prefixes: &[(String, String)],
    ) -> Result<Self, StoreError> {
        let (mut files, mut directories, mut diagnostics, mut bundles) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for (root, prefix) in prefixes {
            files.extend(reader.observed_files_under(root, prefix)?);
            directories.extend(reader.observed_directories_under(root, prefix)?);
            diagnostics.extend(reader.scan_diagnostics_under(root, prefix)?);
            bundles.extend(reader.bundle_files_under(root, prefix)?);
        }
        Self::from_rows(files, directories, diagnostics, bundles)
    }

    /// Only the diagnostic rows of the published observation.
    pub(crate) fn load_diagnostics(reader: &StoreReader) -> Result<Self, StoreError> {
        Self::from_rows(Vec::new(), Vec::new(), reader.scan_diagnostics()?, Vec::new())
    }

    /// Only the parsed `.bundle` rows of the published observation.
    pub(crate) fn load_bundles(reader: &StoreReader) -> Result<Self, StoreError> {
        Self::from_rows(Vec::new(), Vec::new(), Vec::new(), reader.bundle_files()?)
    }

    fn from_rows(
        files: Vec<ObservedFile>,
        directories: Vec<ObservedDirectory>,
        diagnostics: Vec<ObservedDiagnostic>,
        bundles: Vec<ObservedBundleFile>,
    ) -> Result<Self, StoreError> {
        let mut snapshot = Self::default();
        for row in files {
            let key = (row.root_name.clone(), row.path.clone());
            if let Some(target) = &row.file.symlink_target {
                snapshot
                    .symlink_aliases
                    .insert(key.clone(), decode_path(target));
            }
            snapshot.files.insert(
                key,
                ScannedFile {
                    root_name: row.root_name,
                    normalized_path: row.path,
                    kind: match row.file.state.kind {
                        FileKind::File => ScannedFileKind::File,
                        FileKind::Directory => ScannedFileKind::Directory,
                        FileKind::Symlink => ScannedFileKind::Symlink,
                    },
                    modified_nanos: row.file.state.mtime,
                    size: row.file.state.size,
                    content_hash: row.file.state.content_hash,
                    raw_relative_path: decode_raw_path(&row.file.raw_path),
                },
            );
        }
        for row in directories {
            snapshot.directory_observations.insert(
                (row.root_name, row.path),
                DirectoryObservation {
                    canonical_path: decode_path(&row.canonical_path),
                    physical_path: decode_path(&row.physical_path),
                },
            );
        }
        for row in diagnostics {
            let diagnostic = decode_diagnostic(&row.detail).ok_or_else(|| {
                StoreError::InvalidConfiguration {
                    error: format!(
                        "malformed scan diagnostic at {}/{}",
                        row.root_name, row.path
                    ),
                }
            })?;
            snapshot
                .diagnostics
                .insert((row.root_name, row.path), diagnostic);
        }
        for row in bundles {
            let bundle = scanned_bundle(&row.root_name, &row.path, row.bytes);
            snapshot
                .bundles
                .insert((row.root_name, row.path), Arc::new(bundle));
        }
        rebuild_reverse_indexes(&mut snapshot).map_err(|error| {
            StoreError::InvalidConfiguration {
                error: format!("published scan tables are inconsistent: {error}"),
            }
        })?;
        Ok(snapshot)
    }

    /// The `files` row recorded for one scanned path.
    pub(crate) fn file_observation(&self, key: &(String, String)) -> Option<FileObservation> {
        let file = self.files.get(key)?;
        Some(FileObservation {
            state: FileState {
                mtime: file.modified_nanos,
                size: file.size,
                kind: match file.kind {
                    ScannedFileKind::File => FileKind::File,
                    ScannedFileKind::Directory => FileKind::Directory,
                    ScannedFileKind::Symlink => FileKind::Symlink,
                },
                content_hash: file.content_hash,
            },
            raw_path: encode_raw_path(&file.raw_relative_path),
            symlink_target: self.symlink_aliases.get(key).map(|target| encode_path(target)),
        })
    }

    /// Every file row key with its `files` observation.
    pub(crate) fn file_observations(
        &self,
    ) -> impl Iterator<Item = (&(String, String), FileObservation)> {
        self.files.keys().map(|key| {
            (
                key,
                self.file_observation(key)
                    .expect("a file key has an observation"),
            )
        })
    }

    pub(crate) fn directory_rows(&self) -> Vec<ObservedDirectory> {
        self.directory_observations
            .iter()
            .map(|((root, path), observation)| ObservedDirectory {
                root_name: root.clone(),
                path: path.clone(),
                canonical_path: encode_path(&observation.canonical_path),
                physical_path: encode_path(&observation.physical_path),
            })
            .collect()
    }

    pub(crate) fn encoded_diagnostic_rows(&self) -> Vec<ObservedDiagnostic> {
        self.diagnostics
            .iter()
            .map(|((root, path), diagnostic)| ObservedDiagnostic {
                root_name: root.clone(),
                path: path.clone(),
                detail: encode_diagnostic(diagnostic),
            })
            .collect()
    }
}

impl ScanDelta {
    /// The fresh observation of the affected prefixes.
    pub(crate) fn observed(&self) -> &ScanSnapshot {
        &self.observed
    }
}

/// A path in the store's encoding: its platform bytes.
pub(crate) fn encode_path(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().to_vec()
}

pub(crate) fn decode_path(bytes: &[u8]) -> PathBuf {
    use std::os::unix::ffi::OsStringExt;
    PathBuf::from(OsString::from_vec(bytes.to_vec()))
}

fn encode_raw_path(raw: &PlatformPathBytes) -> Vec<u8> {
    match raw {
        PlatformPathBytes::Unix(bytes) => [&[0u8][..], bytes].concat(),
        PlatformPathBytes::Windows(units) => std::iter::once(1u8)
            .chain(units.iter().flat_map(|unit| unit.to_le_bytes()))
            .collect(),
    }
}

fn decode_raw_path(bytes: &[u8]) -> PlatformPathBytes {
    match bytes.split_first() {
        Some((1, units)) => PlatformPathBytes::Windows(
            units
                .chunks_exact(2)
                .map(|unit| u16::from_le_bytes([unit[0], unit[1]]))
                .collect(),
        ),
        Some((_, bytes)) => PlatformPathBytes::Unix(bytes.to_vec()),
        None => PlatformPathBytes::Unix(Vec::new()),
    }
}

fn put_field(out: &mut Vec<u8>, field: &[u8]) {
    out.extend_from_slice(&(field.len() as u32).to_le_bytes());
    out.extend_from_slice(field);
}

fn take_field<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
    let (length, rest) = input.split_first_chunk::<4>()?;
    let length = u32::from_le_bytes(*length) as usize;
    let field = rest.get(..length)?;
    *input = &rest[length..];
    Some(field)
}

fn encode_diagnostic(diagnostic: &ScanDiagnostic) -> Vec<u8> {
    let mut out = Vec::new();
    match diagnostic {
        ScanDiagnostic::DaemonOwnedDirectoryAlias {
            root_name,
            normalized_path,
            physical_path,
            owned_path,
            kind,
        } => {
            out.push(0);
            put_field(&mut out, root_name.as_bytes());
            put_field(&mut out, normalized_path.as_bytes());
            put_field(&mut out, &encode_path(physical_path));
            put_field(&mut out, &encode_path(owned_path));
            out.push(match kind {
                DaemonOwnedDirectoryKind::State => 0,
                DaemonOwnedDirectoryKind::ModuleStaging => 1,
                DaemonOwnedDirectoryKind::PackageOutput => 2,
                DaemonOwnedDirectoryKind::CodegenOutput => 3,
                DaemonOwnedDirectoryKind::Quarantine => 4,
            });
        }
        ScanDiagnostic::DirectoryCycle {
            root_name,
            normalized_path,
            path_chain,
        } => {
            out.push(1);
            put_field(&mut out, root_name.as_bytes());
            put_field(&mut out, normalized_path.as_bytes());
            for path in path_chain {
                put_field(&mut out, &encode_path(path));
            }
        }
    }
    out
}

fn decode_diagnostic(bytes: &[u8]) -> Option<ScanDiagnostic> {
    let (tag, mut input) = bytes.split_first()?;
    let text = |field: &[u8]| String::from_utf8(field.to_vec()).ok();
    let root_name = text(take_field(&mut input)?)?;
    let normalized_path = text(take_field(&mut input)?)?;
    match tag {
        0 => {
            let physical_path = decode_path(take_field(&mut input)?);
            let owned_path = decode_path(take_field(&mut input)?);
            let kind = match input {
                [0] => DaemonOwnedDirectoryKind::State,
                [1] => DaemonOwnedDirectoryKind::ModuleStaging,
                [2] => DaemonOwnedDirectoryKind::PackageOutput,
                [3] => DaemonOwnedDirectoryKind::CodegenOutput,
                [4] => DaemonOwnedDirectoryKind::Quarantine,
                _ => return None,
            };
            Some(ScanDiagnostic::DaemonOwnedDirectoryAlias {
                root_name,
                normalized_path,
                physical_path,
                owned_path,
                kind,
            })
        }
        1 => {
            let mut path_chain = Vec::new();
            while !input.is_empty() {
                path_chain.push(decode_path(take_field(&mut input)?));
            }
            Some(ScanDiagnostic::DirectoryCycle {
                root_name,
                normalized_path,
                path_chain,
            })
        }
        _ => None,
    }
}

pub(crate) fn scanned_bundle(root_name: &str, normalized_path: &str, bytes: Vec<u8>) -> ScannedBundle {
    let parsed = distill_bundle::parse_bundle(&bytes);
    let namespace_skeleton = parsed
        .is_err()
        .then(|| distill_bundle::extract_namespace_skeleton(&bytes).ok())
        .flatten();
    ScannedBundle {
        root_name: root_name.to_owned(),
        normalized_path: normalized_path.to_owned(),
        file_hash: BundleFileHash::of_observed_bytes(&bytes),
        parsed,
        namespace_skeleton,
        bytes,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectoryObservation {
    canonical_path: PathBuf,
    physical_path: PathBuf,
}

#[derive(Debug, Clone)]
struct PendingDirectory {
    root_name: String,
    physical_path: PathBuf,
    relative_components: Vec<String>,
    ancestry: BTreeSet<PathBuf>,
    path_chain: Vec<PathBuf>,
    entry_guards: Vec<EntryGuard>,
}

#[derive(Debug, Clone)]
struct EntryGuard {
    display_path: PathBuf,
    target_identity: FileIdentity,
    symlink_identity: Option<FileIdentity>,
}

#[derive(Debug)]
struct OpenedChild {
    file: File,
    metadata: Metadata,
    canonical_path: PathBuf,
    identity: FileIdentity,
    symlink_identity: Option<FileIdentity>,
    daemon_owned: Option<DaemonOwnedDirectory>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

impl RootedScanner {
    pub fn new(roots: impl IntoIterator<Item = AssetRoot>) -> Result<Self, ScanError> {
        let roots = roots.into_iter().collect::<Vec<_>>();
        let daemon_owned = Arc::new(RwLock::new(BTreeMap::new()));
        let scanner = Self {
            roots: Arc::new(RwLock::new(canonicalize_roots(roots.clone())?)),
            daemon_owned,
            revision: Arc::new(AtomicU64::new(0)),
        };
        for root in roots {
            if root.quarantine_dir.is_dir() {
                scanner.retain_daemon_owned_directory(
                    DaemonOwnedDirectoryKind::Quarantine,
                    root.quarantine_dir,
                )?;
            }
        }
        Ok(scanner)
    }

    pub(crate) fn candidate_with_roots(
        &self,
        roots: impl IntoIterator<Item = AssetRoot>,
    ) -> Result<Self, ScanError> {
        Ok(Self {
            roots: Arc::new(RwLock::new(canonicalize_roots(roots)?)),
            daemon_owned: Arc::clone(&self.daemon_owned),
            revision: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Record one daemon-owned canonical directory and exclude it
    /// from every current and future root candidate. Duplicate registrations
    /// converge on the canonical `(kind, path)` representative.
    pub(crate) fn retain_daemon_owned_directory(
        &self,
        kind: DaemonOwnedDirectoryKind,
        path: impl AsRef<Path>,
    ) -> Result<(), ScanError> {
        let path = fs::canonicalize(path.as_ref()).map_err(|source| ScanError::Io {
            path: path.as_ref().to_path_buf(),
            source,
        })?;
        let metadata = fs::metadata(&path).map_err(|source| ScanError::Io {
            path: path.clone(),
            source,
        })?;
        if !metadata.is_dir() {
            return Err(ScanError::NonRegularFile { path });
        }
        let retained = DaemonOwnedDirectory {
            kind,
            path: path.clone(),
        };
        let mut directories = self
            .daemon_owned
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match directories.entry(path) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(retained);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let current = entry.get();
                if (retained.kind, &retained.path) < (current.kind, &current.path) {
                    entry.insert(retained);
                }
            }
        }
        Ok(())
    }

    /// Validate a replacement root set completely before making it visible
    /// to any scanner clone. Watchers, authoring, and the coordinator all hold
    /// clones of this handle, so one swap changes their next pinned scan
    /// together without interrupting a traversal already in flight.
    pub fn replace_roots(
        &self,
        roots: impl IntoIterator<Item = AssetRoot>,
    ) -> Result<(), ScanError> {
        let roots = roots.into_iter().collect::<Vec<_>>();
        let replacement = self.candidate_with_roots(roots.clone())?;
        for root in roots {
            if root.quarantine_dir.is_dir() {
                self.retain_daemon_owned_directory(
                    DaemonOwnedDirectoryKind::Quarantine,
                    root.quarantine_dir,
                )?;
            }
        }
        self.replace_from(&replacement);
        Ok(())
    }

    pub(crate) fn replace_from(&self, replacement: &Self) {
        let replacement = replacement.root_snapshot();
        let mut roots = self
            .roots
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let watcher_roots_changed = roots.len() != replacement.len()
            || roots.iter().any(|(name, current)| {
                replacement.get(name).is_none_or(|next| {
                    current.configured != next.configured
                        || current.canonical_path != next.canonical_path
                })
            });
        *roots = replacement;
        if watcher_roots_changed {
            self.revision.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    pub(crate) fn has_same_roots(&self, other: &Self) -> bool {
        let left = self.root_snapshot();
        let right = other.root_snapshot();
        left.len() == right.len()
            && left.iter().all(|(name, root)| {
                right
                    .get(name)
                    .is_some_and(|candidate| root.configured == candidate.configured)
            })
    }

    /// Canonical configured roots to hand to the native watcher. Every
    /// delivered path is re-observed and checked against these roots.
    /// Native-watch roots and the daemon-owned quarantine prefixes nested
    /// beneath them. The watcher filters the latter before queue admission;
    /// scanner canonical containment remains the defense-in-depth boundary.
    pub(crate) fn watch_coverage(&self) -> (Vec<PathBuf>, Vec<PathBuf>) {
        let roots = self.root_snapshot();
        let watched = roots
            .values()
            .map(|root| root.canonical_path.clone())
            .collect();
        let excluded = roots
            .values()
            .map(|root| {
                root.configured
                    .quarantine_dir
                    .strip_prefix(&root.configured.path)
                    .map_or_else(
                        |_| root.configured.quarantine_dir.clone(),
                        |suffix| root.canonical_path.join(suffix),
                    )
            })
            .collect();
        (watched, excluded)
    }

    /// Translate one native invalidation path to the canonical rooted key
    /// used by the durable file tables. Native paths remain hints only; the
    /// scanner still reopens the path before any state is published.
    pub(crate) fn event_path_key(
        &self,
        path: &Path,
    ) -> Result<Option<(String, String)>, ScanError> {
        event_key(&self.root_snapshot(), path)
    }

    pub fn physical_path(&self, root: &str, path: &str) -> Result<PathBuf, ScanError> {
        let roots = self.root_snapshot();
        let root = roots
            .get(root)
            .ok_or_else(|| ScanError::UnknownRoot(root.to_owned()))?;
        let components = validate_logical_path(path)?;
        let mut physical = root.configured.path.clone();
        for component in components {
            physical.push(component);
        }
        Ok(physical)
    }

    fn root_snapshot(&self) -> BTreeMap<String, CanonicalRoot> {
        self.roots
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn daemon_owned_snapshot(&self) -> BTreeMap<PathBuf, DaemonOwnedDirectory> {
        self.daemon_owned
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Locate an observed physical error beneath its configured root and
    /// preserve the platform's exact relative path units for DSVP.
    pub fn scan_subject(&self, path: &Path) -> Option<distill_store::state::ScanSubject> {
        self.root_snapshot()
            .values()
            .filter_map(|root| {
                path.strip_prefix(&root.configured.path)
                    .or_else(|_| path.strip_prefix(&root.canonical_path))
                    .ok()
                    .map(|relative| (root, relative))
            })
            .max_by_key(|(root, _)| root.canonical_path.components().count())
            .map(|(root, relative)| {
                if relative.as_os_str().is_empty() {
                    distill_store::state::ScanSubject::Root {
                        root_name: root.configured.name.clone(),
                    }
                } else {
                    distill_store::state::ScanSubject::Subtree {
                        root_name: root.configured.name.clone(),
                        raw_relative_path: platform_path_bytes(relative),
                    }
                }
            })
    }

    pub fn first_root_subject(&self) -> Option<distill_store::state::ScanSubject> {
        self.root_snapshot()
            .keys()
            .next()
            .cloned()
            .map(|root_name| distill_store::state::ScanSubject::Root { root_name })
    }

    pub(crate) fn rejection_subjects(&self, error: &ScanError) -> Vec<PathBuf> {
        let rooted_raw = |root_name: &str, raw: &PlatformPathBytes| {
            let roots = self.root_snapshot();
            roots.get(root_name).and_then(|root| {
                platform_path(raw).map(|relative| root.canonical_path.join(relative))
            })
        };
        match error {
            ScanError::Multiple(errors) => errors
                .iter()
                .flat_map(|error| self.rejection_subjects(error))
                .collect(),
            ScanError::RootUnavailable { path, .. }
            | ScanError::Io { path, .. }
            | ScanError::SymlinkEscape { path, .. }
            | ScanError::DirectoryCycle { path }
            | ScanError::FileIdentityChanged { path }
            | ScanError::NonRegularFile { path } => vec![path.clone()],
            ScanError::DirectoryAlias { first, second, .. } => {
                vec![first.clone(), second.clone()]
            }
            ScanError::DaemonOwnedDirectoryAlias { path, .. } => vec![path.clone()],
            ScanError::InvalidPhysicalPath {
                root_name,
                raw_relative_path,
                ..
            } => rooted_raw(root_name, raw_relative_path)
                .into_iter()
                .collect(),
            ScanError::SameRootNormalizedPathCollision {
                root_name, claims, ..
            } => claims
                .iter()
                .filter_map(|claim| rooted_raw(root_name, &claim.raw_relative_path))
                .collect(),
            ScanError::InvalidRootName(_)
            | ScanError::DuplicateRootName(_)
            | ScanError::UnknownRoot(_)
            | ScanError::InvalidLogicalPath(_) => Vec::new(),
        }
    }

    pub fn normalized_observed_path(&self, root: &str, path: &Path) -> String {
        let relative = self
            .root_snapshot()
            .get(root)
            .and_then(|configured| path.strip_prefix(&configured.configured.path).ok())
            .map(|relative| relative.to_string_lossy().replace('\\', "/"))
            .unwrap_or_else(|| path.to_string_lossy().into_owned());
        if relative.is_empty() {
            root.to_owned()
        } else {
            format!("{root}/{relative}")
        }
    }

    pub fn normalized_observed_subject(&self, path: &Path) -> String {
        let roots = self.root_snapshot();
        roots
            .values()
            .filter_map(|root| {
                path.strip_prefix(&root.configured.path)
                    .or_else(|_| path.strip_prefix(&root.canonical_path))
                    .ok()
                    .map(|relative| (root, relative))
            })
            .max_by_key(|(root, _)| root.canonical_path.components().count())
            .map_or_else(
                || path.to_string_lossy().nfc().collect(),
                |(root, relative)| {
                    let relative = relative.to_string_lossy().replace('\\', "/");
                    if relative.is_empty() {
                        root.configured.name.clone()
                    } else {
                        format!("{}/{relative}", root.configured.name)
                    }
                },
            )
    }

    pub fn inspect_destination(
        &self,
        root: &str,
        path: &str,
    ) -> Result<LineageRepairDestination, ScanError> {
        let path = self.physical_path(root, path)?;
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(LineageRepairDestination::Absent)
            }
            Err(source) => Err(ScanError::Io { path, source }),
            Ok(metadata) if metadata.file_type().is_symlink() || metadata.is_file() => {
                let bytes = self.read_identity_checked(&path)?;
                let file_hash = BundleFileHash::of_observed_bytes(&bytes);
                let kind = distill_bundle::parse_bundle(&bytes)
                    .ok()
                    .and_then(|bundle| distill_bundle::write_bundle(&bundle).ok())
                    .filter(|canonical| canonical == &bytes)
                    .map_or(OccupiedLineageDestinationKind::Opaque, |_| {
                        OccupiedLineageDestinationKind::CanonicalBundle
                    });
                Ok(LineageRepairDestination::Occupied { file_hash, kind })
            }
            Ok(_) => Err(ScanError::NonRegularFile { path }),
        }
    }

    /// Enumerate the complete raw namespace in deterministic `(root, path)`
    /// order. This is the startup/recovery path; ordinary live watcher batches
    /// use incremental path/subtree observation instead.
    pub fn scan(&self) -> Result<ScanSnapshot, ScanError> {
        let roots = self.validated_root_snapshot()?;
        let daemon_owned = self.daemon_owned_snapshot();
        let mut snapshot = ScanSnapshot::default();
        let mut errors = Vec::new();
        for root in roots.values() {
            let pending = PendingDirectory {
                root_name: root.configured.name.clone(),
                physical_path: root.canonical_path.clone(),
                relative_components: Vec::new(),
                ancestry: BTreeSet::new(),
                path_chain: vec![root.canonical_path.clone()],
                entry_guards: Vec::new(),
            };
            match scan_pending(&roots, &daemon_owned, vec![pending]) {
                Ok(observed) => merge_scan_snapshot(&mut snapshot, observed),
                Err(ScanError::Multiple(mut nested)) => errors.append(&mut nested),
                Err(error) => errors.push(error),
            }
        }
        if errors.is_empty() {
            rebuild_reverse_indexes(&mut snapshot)?;
            Ok(snapshot)
        } else {
            Err(combine_scan_errors(errors))
        }
    }

    /// Re-observe only native-event paths and directory subtrees, merging the
    /// result into the last complete snapshot without touching unrelated disk
    /// state. `Ok(None)` means every event path was outside configured roots.
    pub fn scan_incremental_delta<B: ScanBaseline + ?Sized>(
        &self,
        baseline: &B,
        event_paths: &[PathBuf],
    ) -> Result<Option<ScanDelta>, ScanError> {
        let roots = self.validated_root_snapshot()?;
        let daemon_owned = self.daemon_owned_snapshot();
        // Keep the native physical spelling alongside its normalized logical
        // identity.  Reopening from the latter is incorrect on filesystems
        // where NFC-equivalent names are distinct.
        let mut affected = BTreeMap::<(String, String), BTreeSet<PathBuf>>::new();
        for event_path in event_paths {
            if let Some((root, path)) = event_key(&roots, event_path)? {
                let mut canonical_event = roots[&root].canonical_path.clone();
                for component in path.split('/').filter(|component| !component.is_empty()) {
                    canonical_event.push(component);
                }
                for alias in baseline.aliases_affected_by(&canonical_event) {
                    affected.entry(alias).or_default();
                }
                let key = (root, path);
                let physical = affected.entry(key.clone()).or_default();
                physical.insert(event_path.clone());
                if let Some(previous) = baseline.raw_relative_path(&key.0, &key.1) {
                    if let Some(relative) = platform_path(&previous) {
                        physical.insert(roots[&key.0].canonical_path.join(relative));
                    }
                }
            }
        }
        if affected.is_empty() {
            return Ok(None);
        }
        let physical = affected.clone();
        let affected = collapse_affected(affected.into_keys().collect());
        let mut observed = ScanSnapshot::default();
        let mut errors = Vec::new();
        for (root, path) in &affected {
            let event_paths = &physical[&(root.clone(), path.clone())];
            if event_paths.is_empty() {
                collect_incremental_observation(
                    scan_logical_path(&roots, &daemon_owned, root, path),
                    &mut observed,
                    &mut errors,
                );
            } else {
                for event_path in event_paths {
                    collect_incremental_observation(
                        scan_event_path(&roots, &daemon_owned, root, path, event_path),
                        &mut observed,
                        &mut errors,
                    );
                }
            }
        }
        if !errors.is_empty() {
            return Err(combine_scan_errors(errors));
        }
        rebuild_reverse_indexes(&mut observed)?;
        validate_incremental_directory_aliases(baseline, &affected, &observed)?;
        Ok(Some(ScanDelta { affected, observed }))
    }

    /// Convenience path for callers that require a standalone snapshot. Live
    /// watcher reconciliation uses `scan_incremental_delta` and applies only
    /// the committed delta in place.
    pub fn scan_incremental(
        &self,
        baseline: &ScanSnapshot,
        event_paths: &[PathBuf],
    ) -> Result<Option<ScanSnapshot>, ScanError> {
        let Some(delta) = self.scan_incremental_delta(baseline, event_paths)? else {
            return Ok(None);
        };
        let mut next = baseline.clone();
        next.apply_delta(delta);
        Ok(Some(next))
    }

    pub fn read_identity_checked(&self, path: &Path) -> Result<Vec<u8>, ScanError> {
        let roots = self.validated_root_snapshot()?;
        let daemon_owned = self.daemon_owned_snapshot();
        let Some(root) = roots
            .values()
            .filter_map(|root| {
                path.strip_prefix(&root.configured.path)
                    .ok()
                    .map(|relative| (root, relative))
            })
            .max_by_key(|(root, _)| root.configured.path.components().count())
        else {
            return Err(ScanError::InvalidLogicalPath(
                path.to_string_lossy().into_owned(),
            ));
        };
        let components = root
            .1
            .components()
            .map(|component| component.as_os_str().to_owned())
            .collect::<Vec<_>>();
        if components.is_empty() {
            return Err(ScanError::NonRegularFile {
                path: path.to_path_buf(),
            });
        }
        let mut display = root.0.configured.path.clone();
        let mut entry_guards = Vec::new();
        for (index, component) in components.iter().enumerate() {
            display.push(component);
            let opened = open_scanned_child(&display, &roots, &daemon_owned)?;
            reject_daemon_owned_access(&opened, &display)?;
            let guard = EntryGuard::new(&display, &opened);
            if index + 1 == components.len() {
                if !opened.metadata.is_file() {
                    return Err(ScanError::NonRegularFile { path: display });
                }
                let bytes = read_opened_file(opened.file, path, &opened.metadata)?;
                entry_guards.push(guard);
                revalidate_entry_guards(&entry_guards, &roots, &daemon_owned)?;
                return Ok(bytes);
            }
            if !opened.metadata.is_dir() {
                return Err(ScanError::NonRegularFile { path: display });
            }
            entry_guards.push(guard);
            drop(opened.file);
        }
        unreachable!("nonempty component walk returns at its final component")
    }

    fn validated_root_snapshot(&self) -> Result<BTreeMap<String, CanonicalRoot>, ScanError> {
        let roots = self.root_snapshot();
        let mut errors = Vec::new();
        for root in roots.values() {
            let result = (|| {
                let current_path = fs::canonicalize(&root.configured.path).map_err(|_| {
                    ScanError::RootUnavailable {
                        root: root.configured.name.clone(),
                        path: root.configured.path.clone(),
                    }
                })?;
                let current = fs::metadata(&current_path).map_err(|source| ScanError::Io {
                    path: current_path.clone(),
                    source,
                })?;
                if current_path != root.canonical_path || !current.is_dir() {
                    return Err(ScanError::RootUnavailable {
                        root: root.configured.name.clone(),
                        path: root.configured.path.clone(),
                    });
                }
                Ok(())
            })();
            if let Err(error) = result {
                errors.push(error);
            }
        }
        if errors.is_empty() {
            Ok(roots)
        } else if errors.len() == 1 {
            Err(errors.pop().expect("one root validation error"))
        } else {
            Err(ScanError::Multiple(errors))
        }
    }
}

fn scan_pending(
    roots: &BTreeMap<String, CanonicalRoot>,
    daemon_owned: &BTreeMap<PathBuf, DaemonOwnedDirectory>,
    mut stack: Vec<PendingDirectory>,
) -> Result<ScanSnapshot, ScanError> {
    let mut snapshot = ScanSnapshot::default();
    let mut errors = Vec::new();
    while let Some(pending) = stack.pop() {
        if let Err(error) = revalidate_entry_guards(&pending.entry_guards, roots, daemon_owned) {
            errors.push(error);
            continue;
        }
        let metadata = match directory_metadata(&pending.physical_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                errors.push(error);
                continue;
            }
        };
        if !metadata.is_dir() {
            errors.push(ScanError::NonRegularFile {
                path: pending.physical_path,
            });
            continue;
        }
        let canonical_path = match fs::canonicalize(&pending.physical_path) {
            Ok(path) => path,
            Err(source) => {
                errors.push(ScanError::Io {
                    path: pending.physical_path,
                    source,
                });
                continue;
            }
        };
        let identity = file_identity(&metadata);
        if let Some(retained) = daemon_owned.get(&canonical_path) {
            if retained.kind == DaemonOwnedDirectoryKind::Quarantine {
                continue;
            }
            record_daemon_owned_diagnostic(
                &mut snapshot,
                &pending.root_name,
                &pending.relative_components,
                &pending.physical_path,
                retained,
            );
            continue;
        }
        if pending.ancestry.contains(&canonical_path) {
            let normalized_path = pending.relative_components.join("/");
            snapshot.diagnostics.insert(
                (pending.root_name.clone(), normalized_path.clone()),
                ScanDiagnostic::DirectoryCycle {
                    root_name: pending.root_name,
                    normalized_path,
                    path_chain: pending.path_chain,
                },
            );
            continue;
        }
        let normalized_directory = pending.relative_components.join("/");
        snapshot.directory_observations.insert(
            (pending.root_name.clone(), normalized_directory.clone()),
            DirectoryObservation {
                canonical_path: canonical_path.clone(),
                physical_path: pending.physical_path.clone(),
            },
        );
        if !pending.relative_components.is_empty() {
            let file = ScannedFile {
                root_name: pending.root_name.clone(),
                normalized_path: normalized_directory,
                kind: ScannedFileKind::Directory,
                modified_nanos: modified_nanos(&metadata),
                size: metadata.len(),
                content_hash: None,
                raw_relative_path: raw_relative_path(
                    roots,
                    &pending.root_name,
                    &pending.physical_path,
                ),
            };
            snapshot
                .files
                .insert((file.root_name.clone(), file.normalized_path.clone()), file);
        }

        let mut entries = match read_directory_names(&pending.physical_path) {
            Ok(entries) => entries,
            Err(error) => {
                errors.push(error);
                continue;
            }
        };
        entries.sort_by_key(|entry| os_sort_key(entry));
        let after = match directory_metadata(&pending.physical_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                errors.push(error);
                continue;
            }
        };
        let after_canonical =
            fs::canonicalize(&pending.physical_path).map_err(|source| ScanError::Io {
                path: pending.physical_path.clone(),
                source,
            })?;
        if file_identity(&after) != identity || after_canonical != canonical_path {
            errors.push(ScanError::FileIdentityChanged {
                path: pending.physical_path,
            });
            continue;
        }
        if let Err(error) = revalidate_entry_guards(&pending.entry_guards, roots, daemon_owned) {
            errors.push(error);
            continue;
        }

        for entry in entries.into_iter().rev() {
            let physical = pending.physical_path.join(&entry);
            let component =
                match normalize_scanned_component(roots, &pending.root_name, &physical, &entry) {
                    Ok(component) => component,
                    Err(error) => {
                        errors.push(error);
                        continue;
                    }
                };
            let mut relative = pending.relative_components.clone();
            relative.push(component);
            let opened = match open_scanned_child(&physical, roots, daemon_owned) {
                Ok(opened) => opened,
                Err(error) => {
                    errors.push(error);
                    continue;
                }
            };
            let guard = EntryGuard::new(&physical, &opened);
            if opened.metadata.is_dir() {
                if let Some(retained) = &opened.daemon_owned {
                    if retained.kind == DaemonOwnedDirectoryKind::Quarantine {
                        continue;
                    }
                    record_daemon_owned_diagnostic(
                        &mut snapshot,
                        &pending.root_name,
                        &relative,
                        &physical,
                        retained,
                    );
                    continue;
                }
                let mut ancestry = pending.ancestry.clone();
                ancestry.insert(canonical_path.clone());
                let mut entry_guards = pending.entry_guards.clone();
                entry_guards.push(guard);
                let mut path_chain = pending.path_chain.clone();
                path_chain.push(physical.clone());
                drop(opened.file);
                stack.push(PendingDirectory {
                    root_name: pending.root_name.clone(),
                    physical_path: physical,
                    relative_components: relative,
                    ancestry,
                    path_chain,
                    entry_guards,
                });
            } else if opened.metadata.is_file() {
                if let Err(error) = observe_opened_file(
                    roots,
                    daemon_owned,
                    &mut snapshot,
                    &pending.root_name,
                    relative,
                    physical,
                    opened,
                    &pending.entry_guards,
                    guard,
                ) {
                    errors.push(error);
                }
            } else {
                errors.push(ScanError::NonRegularFile { path: physical });
            }
        }
    }
    if let Err(error) = rebuild_reverse_indexes(&mut snapshot) {
        errors.push(error);
    }
    if errors.is_empty() {
        Ok(snapshot)
    } else {
        Err(combine_scan_errors(errors))
    }
}

fn combine_scan_errors(errors: Vec<ScanError>) -> ScanError {
    let mut flattened = Vec::new();
    let mut pending = errors;
    while let Some(error) = pending.pop() {
        match error {
            ScanError::Multiple(nested) => pending.extend(nested),
            error => flattened.push(error),
        }
    }
    let mut collisions = BTreeMap::<(String, String), Vec<PhysicalPathClaim>>::new();
    let mut other = Vec::new();
    for error in flattened {
        match error {
            ScanError::SameRootNormalizedPathCollision {
                root_name,
                normalized_path,
                claims,
            } => collisions
                .entry((root_name, normalized_path))
                .or_default()
                .extend(claims),
            error => other.push(error),
        }
    }
    for ((root_name, normalized_path), mut claims) in collisions {
        claims.sort();
        claims.dedup();
        other.push(ScanError::SameRootNormalizedPathCollision {
            root_name,
            normalized_path,
            claims,
        });
    }
    if other.len() == 1 {
        other.pop().expect("one combined scan error")
    } else {
        ScanError::Multiple(other)
    }
}

fn collect_incremental_observation(
    result: Result<Option<ScanSnapshot>, ScanError>,
    observed: &mut ScanSnapshot,
    errors: &mut Vec<ScanError>,
) {
    match result {
        Ok(Some(partial)) => {
            for (key, current) in &partial.files {
                let Some(previous) = observed
                    .files
                    .get(key)
                    .filter(|previous| previous.raw_relative_path != current.raw_relative_path)
                else {
                    continue;
                };
                let (Some(previous_hash), Some(current_hash)) =
                    (previous.content_hash, current.content_hash)
                else {
                    continue;
                };
                errors.push(ScanError::SameRootNormalizedPathCollision {
                    root_name: key.0.clone(),
                    normalized_path: key.1.clone(),
                    claims: vec![
                        PhysicalPathClaim {
                            raw_relative_path: previous.raw_relative_path.clone(),
                            file_hash: BundleFileHash(previous_hash.0),
                        },
                        PhysicalPathClaim {
                            raw_relative_path: current.raw_relative_path.clone(),
                            file_hash: BundleFileHash(current_hash.0),
                        },
                    ],
                });
            }
            merge_scan_snapshot(observed, partial);
        }
        Ok(None) => {}
        Err(ScanError::Multiple(mut nested)) => errors.append(&mut nested),
        Err(error) => errors.push(error),
    }
}

fn merge_scan_snapshot(target: &mut ScanSnapshot, source: ScanSnapshot) {
    target.files.extend(source.files);
    target.bundles.extend(source.bundles);
    target
        .directory_observations
        .extend(source.directory_observations);
    target.symlink_aliases.extend(source.symlink_aliases);
    target.diagnostics.extend(source.diagnostics);
}

#[allow(clippy::too_many_arguments)]
fn observe_opened_file(
    roots: &BTreeMap<String, CanonicalRoot>,
    daemon_owned: &BTreeMap<PathBuf, DaemonOwnedDirectory>,
    snapshot: &mut ScanSnapshot,
    root_name: &str,
    relative: Vec<String>,
    physical: PathBuf,
    opened: OpenedChild,
    parent_guards: &[EntryGuard],
    guard: EntryGuard,
) -> Result<(), ScanError> {
    let metadata = opened.metadata;
    let is_symlink = opened.symlink_identity.is_some();
    let bytes = read_opened_file(opened.file, &physical, &metadata)?;
    revalidate_entry_guards(parent_guards, roots, daemon_owned)?;
    revalidate_entry_guards(std::slice::from_ref(&guard), roots, daemon_owned)?;
    let normalized_path = relative.join("/");
    let raw_relative_path = raw_relative_path(roots, root_name, &physical);
    let file_hash = BundleFileHash::of_observed_bytes(&bytes);
    if let Some(existing) = snapshot
        .files
        .get(&(root_name.to_owned(), normalized_path.clone()))
        .filter(|existing| existing.raw_relative_path != raw_relative_path)
    {
        let mut claims = vec![
            PhysicalPathClaim {
                raw_relative_path: existing.raw_relative_path.clone(),
                file_hash: BundleFileHash(existing.content_hash.expect("regular file hash").0),
            },
            PhysicalPathClaim {
                raw_relative_path,
                file_hash,
            },
        ];
        claims.sort();
        claims.dedup();
        return Err(ScanError::SameRootNormalizedPathCollision {
            root_name: root_name.to_owned(),
            normalized_path,
            claims,
        });
    }
    let file = ScannedFile {
        root_name: root_name.to_owned(),
        normalized_path: normalized_path.clone(),
        kind: if is_symlink {
            ScannedFileKind::Symlink
        } else {
            ScannedFileKind::File
        },
        modified_nanos: modified_nanos(&metadata),
        size: metadata.len(),
        content_hash: Some(ContentHash(*blake3::hash(&bytes).as_bytes())),
        raw_relative_path,
    };
    snapshot
        .files
        .insert((file.root_name.clone(), file.normalized_path.clone()), file);
    if is_symlink {
        let target = fs::canonicalize(&physical).map_err(|source| ScanError::Io {
            path: physical.clone(),
            source,
        })?;
        snapshot
            .symlink_aliases
            .insert((root_name.to_owned(), normalized_path.clone()), target);
    }
    if physical
        .extension()
        .and_then(|extension| extension.to_str())
        == Some("bundle")
    {
        let bundle = scanned_bundle(root_name, &normalized_path, bytes);
        snapshot
            .bundles
            .insert((root_name.to_owned(), normalized_path), Arc::new(bundle));
    }
    Ok(())
}

fn scan_logical_path(
    roots: &BTreeMap<String, CanonicalRoot>,
    daemon_owned: &BTreeMap<PathBuf, DaemonOwnedDirectory>,
    root_name: &str,
    path: &str,
) -> Result<Option<ScanSnapshot>, ScanError> {
    let root = roots
        .get(root_name)
        .ok_or_else(|| ScanError::UnknownRoot(root_name.to_owned()))?;
    if path.is_empty() {
        return scan_pending(
            roots,
            daemon_owned,
            vec![PendingDirectory {
                root_name: root_name.to_owned(),
                physical_path: root.canonical_path.clone(),
                relative_components: Vec::new(),
                ancestry: BTreeSet::new(),
                path_chain: vec![root.canonical_path.clone()],
                entry_guards: Vec::new(),
            }],
        )
        .map(Some);
    }

    let components = validate_logical_path(path)?
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let physical_components = components.iter().map(OsString::from).collect();
    scan_path_components(
        roots,
        daemon_owned,
        root_name,
        components,
        physical_components,
    )
}

fn scan_event_path(
    roots: &BTreeMap<String, CanonicalRoot>,
    daemon_owned: &BTreeMap<PathBuf, DaemonOwnedDirectory>,
    root_name: &str,
    normalized_path: &str,
    event_path: &Path,
) -> Result<Option<ScanSnapshot>, ScanError> {
    let root = roots
        .get(root_name)
        .ok_or_else(|| ScanError::UnknownRoot(root_name.to_owned()))?;
    let relative = event_path
        .strip_prefix(&root.canonical_path)
        .or_else(|_| event_path.strip_prefix(&root.configured.path))
        .map_err(|_| ScanError::InvalidLogicalPath(event_path.to_string_lossy().into_owned()))?;
    let physical_components = relative
        .components()
        .map(|component| component.as_os_str().to_owned())
        .collect::<Vec<_>>();
    let logical_components = physical_components
        .iter()
        .map(|component| {
            let physical = root.canonical_path.join(relative);
            normalize_scanned_component(roots, root_name, &physical, component)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if logical_components.join("/") != normalized_path {
        return Err(ScanError::InvalidLogicalPath(normalized_path.to_owned()));
    }
    if logical_components.is_empty() {
        return scan_logical_path(roots, daemon_owned, root_name, normalized_path);
    }
    scan_path_components(
        roots,
        daemon_owned,
        root_name,
        logical_components,
        physical_components,
    )
}

fn scan_path_components(
    roots: &BTreeMap<String, CanonicalRoot>,
    daemon_owned: &BTreeMap<PathBuf, DaemonOwnedDirectory>,
    root_name: &str,
    components: Vec<String>,
    physical_components: Vec<OsString>,
) -> Result<Option<ScanSnapshot>, ScanError> {
    let root = roots
        .get(root_name)
        .ok_or_else(|| ScanError::UnknownRoot(root_name.to_owned()))?;
    let mut physical = root.canonical_path.clone();
    let mut relative = Vec::new();
    let mut entry_guards = Vec::new();
    let mut ancestry = BTreeSet::new();
    let mut parent_canonical_path = root.canonical_path.clone();
    for (index, (component, physical_component)) in
        components.iter().zip(&physical_components).enumerate()
    {
        physical.push(physical_component);
        relative.push(component.clone());
        let opened = match open_scanned_child(&physical, roots, daemon_owned) {
            Ok(opened) => opened,
            Err(ScanError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None)
            }
            Err(error) => return Err(error),
        };
        let guard = EntryGuard::new(&physical, &opened);
        let final_component = index + 1 == physical_components.len();
        if final_component && opened.metadata.is_file() {
            let mut snapshot = ScanSnapshot::default();
            observe_opened_file(
                roots,
                daemon_owned,
                &mut snapshot,
                root_name,
                relative,
                physical,
                opened,
                &entry_guards,
                guard,
            )?;
            rebuild_reverse_indexes(&mut snapshot)?;
            return Ok(Some(snapshot));
        }
        if !opened.metadata.is_dir() {
            return Err(ScanError::NonRegularFile { path: physical });
        }
        if let Some(retained) = &opened.daemon_owned {
            let mut snapshot = ScanSnapshot::default();
            if retained.kind != DaemonOwnedDirectoryKind::Quarantine {
                record_daemon_owned_diagnostic(
                    &mut snapshot,
                    root_name,
                    &relative,
                    &physical,
                    retained,
                );
            }
            return Ok(Some(snapshot));
        }
        ancestry.insert(parent_canonical_path);
        parent_canonical_path = opened.canonical_path.clone();
        entry_guards.push(guard);
        drop(opened.file);
        if final_component {
            let mut path_chain = vec![root.canonical_path.clone()];
            path_chain.extend(entry_guards.iter().map(|guard| guard.display_path.clone()));
            return scan_pending(
                roots,
                daemon_owned,
                vec![PendingDirectory {
                    root_name: root_name.to_owned(),
                    physical_path: physical,
                    relative_components: relative,
                    ancestry,
                    path_chain,
                    entry_guards,
                }],
            )
            .map(Some);
        }
    }
    unreachable!("a validated nonempty logical path has a final component")
}

fn event_key(
    roots: &BTreeMap<String, CanonicalRoot>,
    event_path: &Path,
) -> Result<Option<(String, String)>, ScanError> {
    let Some((root, relative)) = roots
        .values()
        .filter_map(|root| {
            event_path
                .strip_prefix(&root.canonical_path)
                .or_else(|_| event_path.strip_prefix(&root.configured.path))
                .ok()
                .map(|relative| (root, relative))
        })
        .max_by_key(|(root, _)| root.canonical_path.components().count())
    else {
        return Ok(None);
    };
    let components = relative
        .components()
        .map(|component| {
            normalize_scanned_component(
                roots,
                &root.configured.name,
                event_path,
                component.as_os_str(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some((root.configured.name.clone(), components.join("/"))))
}

fn collapse_affected(paths: BTreeSet<(String, String)>) -> Vec<(String, String)> {
    let mut collapsed = Vec::<(String, String)>::new();
    for (root, path) in paths {
        if collapsed
            .iter()
            .any(|(prior_root, prior)| path_matches(prior_root, prior, &root, &path))
        {
            continue;
        }
        collapsed.push((root, path));
    }
    collapsed
}

fn path_matches(root: &str, prefix: &str, candidate_root: &str, candidate: &str) -> bool {
    root == candidate_root
        && (prefix.is_empty()
            || candidate == prefix
            || candidate
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('/')))
}

fn key_matches(prefix: &(String, String), candidate: &(String, String)) -> bool {
    path_matches(&prefix.0, &prefix.1, &candidate.0, &candidate.1)
}

fn matching_values<'a, V>(
    map: &'a BTreeMap<(String, String), V>,
    prefix: &(String, String),
) -> impl Iterator<Item = &'a V> {
    let start = prefix.clone();
    let prefix = prefix.clone();
    map.range(start..)
        .take_while(move |(key, _)| key_matches(&prefix, key))
        .map(|(_, value)| value)
}

fn matching_keys<V>(
    map: &BTreeMap<(String, String), V>,
    prefix: &(String, String),
) -> Vec<(String, String)> {
    let start = prefix.clone();
    map.range(start..)
        .take_while(|(key, _)| key_matches(prefix, key))
        .map(|(key, _)| key.clone())
        .collect()
}

fn remove_matching<V>(map: &mut BTreeMap<(String, String), V>, prefix: &(String, String)) {
    for key in matching_keys(map, prefix) {
        map.remove(&key);
    }
}

fn matching_bundle_observations<'a>(
    map: &'a BTreeMap<(String, String), Arc<ScannedBundle>>,
    prefix: &(String, String),
) -> impl Iterator<Item = (&'a str, &'a str, BundleFileHash)> {
    matching_values(map, prefix).map(|bundle| {
        (
            bundle.root_name.as_str(),
            bundle.normalized_path.as_str(),
            bundle.file_hash,
        )
    })
}

fn aliases_affected_by(
    aliases: &BTreeMap<PathBuf, BTreeSet<(String, String)>>,
    event: &Path,
) -> Vec<(String, String)> {
    aliases
        .range(event.to_path_buf()..)
        .take_while(|(target, _)| target.starts_with(event))
        .flat_map(|(_, aliases)| aliases.iter().cloned())
        .collect()
}

fn remove_reverse_alias(
    aliases: &mut BTreeMap<PathBuf, BTreeSet<(String, String)>>,
    target: &Path,
    alias: &(String, String),
) {
    let empty = aliases.get_mut(target).is_some_and(|entries| {
        entries.remove(alias);
        entries.is_empty()
    });
    if empty {
        aliases.remove(target);
    }
}

fn remove_logical_root(logical: &mut BTreeMap<String, BTreeSet<String>>, path: &str, root: &str) {
    let empty = logical.get_mut(path).is_some_and(|roots| {
        roots.remove(root);
        roots.is_empty()
    });
    if empty {
        logical.remove(path);
    }
}

fn validate_incremental_directory_aliases<B: ScanBaseline + ?Sized>(
    baseline: &B,
    affected: &[(String, String)],
    observed: &ScanSnapshot,
) -> Result<(), ScanError> {
    for ((root, path), observation) in &observed.directory_observations {
        let Some((first_root, first_path, first)) =
            baseline.directory_by_target(&observation.canonical_path)
        else {
            continue;
        };
        let first_key = (first_root.clone(), first_path.clone());
        if affected
            .iter()
            .any(|prefix| key_matches(prefix, &first_key))
        {
            continue;
        }
        if first_root != *root || first_path != *path {
            return Err(ScanError::DirectoryAlias {
                first_root: first_root.clone(),
                first: first.clone(),
                second_root: root.clone(),
                second: observation.physical_path.clone(),
            });
        }
    }
    Ok(())
}

fn rebuild_reverse_indexes(snapshot: &mut ScanSnapshot) -> Result<(), ScanError> {
    snapshot.logical_roots.clear();
    for (root, path) in snapshot.files.keys() {
        snapshot
            .logical_roots
            .entry(path.clone())
            .or_default()
            .insert(root.clone());
    }
    snapshot.directory_by_target.clear();
    let mut alias_errors = Vec::new();
    for ((root, path), observed) in &snapshot.directory_observations {
        if let Some((first_root, first_path, first)) = snapshot
            .directory_by_target
            .get(&observed.canonical_path)
            .cloned()
        {
            if first_root != *root || first_path != *path {
                alias_errors.push(ScanError::DirectoryAlias {
                    first_root,
                    first,
                    second_root: root.clone(),
                    second: observed.physical_path.clone(),
                });
            }
        } else {
            snapshot.directory_by_target.insert(
                observed.canonical_path.clone(),
                (root.clone(), path.clone(), observed.physical_path.clone()),
            );
        }
    }
    if !alias_errors.is_empty() {
        return Err(combine_scan_errors(alias_errors));
    }
    snapshot.aliases_by_target.clear();
    for (alias, target) in &snapshot.symlink_aliases {
        snapshot
            .aliases_by_target
            .entry(target.clone())
            .or_default()
            .insert(alias.clone());
    }
    Ok(())
}

fn directory_metadata(path: &Path) -> Result<Metadata, ScanError> {
    fs::metadata(path).map_err(|source| ScanError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn read_directory_names(path: &Path) -> Result<Vec<OsString>, ScanError> {
    fs::read_dir(path)
        .map_err(|source| ScanError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name())
                .map_err(|source| ScanError::Io {
                    path: path.to_path_buf(),
                    source,
                })
        })
        .collect()
}

fn open_scanned_child(
    physical: &Path,
    roots: &BTreeMap<String, CanonicalRoot>,
    daemon_owned: &BTreeMap<PathBuf, DaemonOwnedDirectory>,
) -> Result<OpenedChild, ScanError> {
    let path = physical.to_path_buf();
    let link_metadata = fs::symlink_metadata(&path).map_err(|source| ScanError::Io {
        path: physical.to_path_buf(),
        source,
    })?;
    let symlink_identity = link_metadata
        .file_type()
        .is_symlink()
        .then(|| file_identity(&link_metadata));
    let canonical_path = fs::canonicalize(&path).map_err(|source| ScanError::Io {
        path: physical.to_path_buf(),
        source,
    })?;
    let inside_root = roots
        .values()
        .any(|candidate| canonical_path.starts_with(&candidate.canonical_path));
    if !inside_root && !daemon_owned.contains_key(&canonical_path) {
        return Err(ScanError::SymlinkEscape {
            path: physical.to_path_buf(),
            target: canonical_path,
        });
    }
    let expected = fs::metadata(&canonical_path).map_err(|source| ScanError::Io {
        path: canonical_path.clone(),
        source,
    })?;
    let file = File::open(&path).map_err(|source| ScanError::Io {
        path: physical.to_path_buf(),
        source,
    })?;
    let metadata = file.metadata().map_err(|source| ScanError::Io {
        path: physical.to_path_buf(),
        source,
    })?;
    let identity = file_identity(&metadata);

    let current_link = fs::symlink_metadata(&path).map_err(|source| ScanError::Io {
        path: physical.to_path_buf(),
        source,
    })?;
    let current_symlink_identity = current_link
        .file_type()
        .is_symlink()
        .then(|| file_identity(&current_link));
    let current_canonical = fs::canonicalize(&path).map_err(|source| ScanError::Io {
        path: physical.to_path_buf(),
        source,
    })?;
    if current_symlink_identity != symlink_identity
        || current_canonical != canonical_path
        || file_identity(&expected) != identity
        || expected.is_dir() != metadata.is_dir()
        || expected.is_file() != metadata.is_file()
    {
        return Err(ScanError::FileIdentityChanged {
            path: physical.to_path_buf(),
        });
    }

    Ok(OpenedChild {
        file,
        daemon_owned: metadata
            .is_dir()
            .then(|| daemon_owned.get(&canonical_path).cloned())
            .flatten(),
        metadata,
        canonical_path,
        identity,
        symlink_identity,
    })
}

fn record_daemon_owned_diagnostic(
    snapshot: &mut ScanSnapshot,
    root_name: &str,
    relative_components: &[String],
    physical_path: &Path,
    retained: &DaemonOwnedDirectory,
) {
    let normalized_path = relative_components.join("/");
    snapshot.diagnostics.insert(
        (root_name.to_owned(), normalized_path.clone()),
        ScanDiagnostic::DaemonOwnedDirectoryAlias {
            root_name: root_name.to_owned(),
            normalized_path,
            physical_path: physical_path.to_path_buf(),
            owned_path: retained.path.clone(),
            kind: retained.kind,
        },
    );
}

fn daemon_owned_alias(path: &Path, retained: &DaemonOwnedDirectory) -> ScanError {
    ScanError::DaemonOwnedDirectoryAlias {
        path: path.to_path_buf(),
        owned_path: retained.path.clone(),
        kind: retained.kind,
    }
}

fn reject_daemon_owned_access(opened: &OpenedChild, path: &Path) -> Result<(), ScanError> {
    match &opened.daemon_owned {
        Some(retained) => Err(daemon_owned_alias(path, retained)),
        None => Ok(()),
    }
}

impl EntryGuard {
    fn new(display_path: &Path, opened: &OpenedChild) -> Self {
        Self {
            display_path: display_path.to_path_buf(),
            target_identity: opened.identity,
            symlink_identity: opened.symlink_identity,
        }
    }
}

fn revalidate_entry_guards(
    guards: &[EntryGuard],
    roots: &BTreeMap<String, CanonicalRoot>,
    daemon_owned: &BTreeMap<PathBuf, DaemonOwnedDirectory>,
) -> Result<(), ScanError> {
    for guard in guards {
        let current = open_scanned_child(&guard.display_path, roots, daemon_owned)?;
        reject_daemon_owned_access(&current, &guard.display_path)?;
        if current.identity != guard.target_identity
            || current.symlink_identity != guard.symlink_identity
        {
            return Err(ScanError::FileIdentityChanged {
                path: guard.display_path.clone(),
            });
        }
    }
    Ok(())
}

fn read_opened_file(mut file: File, path: &Path, opened: &Metadata) -> Result<Vec<u8>, ScanError> {
    let identity = file_identity_from_file(&file, opened).map_err(|source| ScanError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let opened_modified = modified_nanos(opened);
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| ScanError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let after = file.metadata().map_err(|source| ScanError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if file_identity_from_file(&file, &after).map_err(|source| ScanError::Io {
        path: path.to_path_buf(),
        source,
    })? != identity
        || opened.len() != bytes.len() as u64
        || after.len() != bytes.len() as u64
        || modified_nanos(&after) != opened_modified
    {
        return Err(ScanError::FileIdentityChanged {
            path: path.to_path_buf(),
        });
    }
    Ok(bytes)
}

fn canonicalize_roots(
    roots: impl IntoIterator<Item = AssetRoot>,
) -> Result<BTreeMap<String, CanonicalRoot>, ScanError> {
    let configured = roots.into_iter().collect::<Vec<_>>();
    let canonical_paths = configured
        .iter()
        .map(|root| {
            validate_root_name(&root.name)?;
            let canonical_path =
                fs::canonicalize(&root.path).map_err(|_| ScanError::RootUnavailable {
                    root: root.name.clone(),
                    path: root.path.clone(),
                })?;
            let metadata = fs::metadata(&canonical_path).map_err(|source| ScanError::Io {
                path: canonical_path.clone(),
                source,
            })?;
            if !metadata.is_dir() {
                return Err(ScanError::RootUnavailable {
                    root: root.name.clone(),
                    path: root.path.clone(),
                });
            }
            Ok((root.clone(), canonical_path))
        })
        .collect::<Result<Vec<_>, ScanError>>()?;

    let mut roots = BTreeMap::new();
    for (configured, canonical_path) in canonical_paths {
        let name = configured.name.clone();
        if roots
            .insert(
                name.clone(),
                CanonicalRoot {
                    configured,
                    canonical_path,
                },
            )
            .is_some()
        {
            return Err(ScanError::DuplicateRootName(name));
        }
    }
    Ok(roots)
}

fn platform_path_bytes(path: &Path) -> distill_store::state::PlatformPathBytes {
    use std::os::unix::ffi::OsStrExt;
    distill_store::state::PlatformPathBytes::Unix(path.as_os_str().as_bytes().to_vec())
}

fn raw_relative_path(
    roots: &BTreeMap<String, CanonicalRoot>,
    root_name: &str,
    physical: &Path,
) -> PlatformPathBytes {
    let root = &roots[root_name];
    let relative = physical
        .strip_prefix(&root.canonical_path)
        .or_else(|_| physical.strip_prefix(&root.configured.path))
        .expect("scanner paths remain beneath their configured root");
    platform_path_bytes(relative)
}

fn platform_path(raw: &PlatformPathBytes) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStringExt;
    match raw {
        PlatformPathBytes::Unix(bytes) => Some(PathBuf::from(OsString::from_vec(bytes.clone()))),
        PlatformPathBytes::Windows(_) => None,
    }
}

fn modified_nanos(metadata: &Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |duration| {
            i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
        })
}

fn validate_root_name(name: &str) -> Result<(), ScanError> {
    if name.is_empty()
        || !is_nfc(name)
        || name.contains(['/', '\\', '\0'])
        || matches!(name, "." | "..")
    {
        return Err(ScanError::InvalidRootName(name.to_owned()));
    }
    Ok(())
}

fn validate_logical_path(path: &str) -> Result<Vec<&str>, ScanError> {
    if path.is_empty() || path.starts_with('/') || path.ends_with('/') || !is_nfc(path) {
        return Err(ScanError::InvalidLogicalPath(path.to_owned()));
    }
    let components = path.split('/').collect::<Vec<_>>();
    if components.iter().any(|component| {
        component.is_empty() || matches!(*component, "." | "..") || component.contains(['\\', '\0'])
    }) {
        return Err(ScanError::InvalidLogicalPath(path.to_owned()));
    }
    Ok(components)
}

fn normalize_component(component: &std::ffi::OsStr) -> Result<String, ScanError> {
    let text = component
        .to_str()
        .ok_or_else(|| ScanError::InvalidLogicalPath(format!("{:?}", os_sort_key(component))))?;
    let normalized = text.nfc().collect::<String>();
    if normalized.is_empty()
        || matches!(normalized.as_str(), "." | "..")
        || normalized.contains(['/', '\\', '\0'])
    {
        return Err(ScanError::InvalidLogicalPath(normalized));
    }
    Ok(normalized)
}

fn normalize_scanned_component(
    roots: &BTreeMap<String, CanonicalRoot>,
    root_name: &str,
    physical: &Path,
    component: &OsStr,
) -> Result<String, ScanError> {
    normalize_component(component).map_err(|_| {
        let failure = if component.to_str().is_none() {
            PhysicalPathFailureCode::InvalidUnixUtf8
        } else {
            let text = component.to_string_lossy();
            if text.is_empty() {
                PhysicalPathFailureCode::EmptyComponent
            } else if text == "." {
                PhysicalPathFailureCode::DotComponent
            } else if text == ".." {
                PhysicalPathFailureCode::ParentComponent
            } else {
                PhysicalPathFailureCode::ForbiddenCharacter
            }
        };
        ScanError::InvalidPhysicalPath {
            root_name: root_name.to_owned(),
            raw_relative_path: raw_relative_path(roots, root_name, physical),
            failure,
        }
    })
}

fn os_sort_key(value: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    value.as_bytes().to_vec()
}

fn file_identity(metadata: &Metadata) -> FileIdentity {
    use std::os::unix::fs::MetadataExt;
    FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

fn file_identity_from_file(_file: &File, metadata: &Metadata) -> std::io::Result<FileIdentity> {
    Ok(file_identity(metadata))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_diagnostics_round_trip_through_their_table_encoding() {
        let diagnostics = [
            ScanDiagnostic::DaemonOwnedDirectoryAlias {
                root_name: "main".to_owned(),
                normalized_path: "gen/out".to_owned(),
                physical_path: PathBuf::from("/project/gen/out"),
                owned_path: PathBuf::from("/project/.distill/codegen"),
                kind: DaemonOwnedDirectoryKind::CodegenOutput,
            },
            ScanDiagnostic::DirectoryCycle {
                root_name: "main".to_owned(),
                normalized_path: "a/loop".to_owned(),
                path_chain: vec![PathBuf::from("/project/a"), PathBuf::from("/project/a/loop")],
            },
            ScanDiagnostic::DirectoryCycle {
                root_name: "main".to_owned(),
                normalized_path: String::new(),
                path_chain: Vec::new(),
            },
        ];
        for diagnostic in diagnostics {
            assert_eq!(
                decode_diagnostic(&encode_diagnostic(&diagnostic)),
                Some(diagnostic)
            );
        }
        assert_eq!(decode_diagnostic(&[7]), None);
    }

    #[test]
    fn raw_paths_round_trip_through_their_table_encoding() {
        for raw in [
            PlatformPathBytes::Unix(b"tex/Rock.bundle".to_vec()),
            PlatformPathBytes::Unix(Vec::new()),
            PlatformPathBytes::Windows(vec![0x74, 0x00e9, 0xd83d, 0xde00]),
        ] {
            assert_eq!(decode_raw_path(&encode_raw_path(&raw)), raw);
        }
    }
}
