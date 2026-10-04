//! Filesystem-side authority for scanning.
//!
//! Logical paths are accepted only in their canonical root-relative form.
//! Physical traversal is confined to canonical configured roots, daemon-owned
//! paths are excluded, directory aliases/cycles are errors, and a file is
//! accepted only when its observation remains stable through the read.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, Metadata};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use distill_core::id::{BundleFileHash, ContentHash};
use distill_store::db::StoreReader;
use distill_store::error::StoreError;
use distill_store::files::{FileKind, FileObservation, FileState, ObservedFile};
use distill_store::state::{PhysicalPathClaim, PhysicalPathFailureCode, PlatformPathBytes};
use distill_store::Current;
use unicode_normalization::{is_nfc, UnicodeNormalization};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetRoot {
    pub name: String,
    pub path: PathBuf,
}

impl AssetRoot {
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
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
    roots: Arc<Current<BTreeMap<String, CanonicalRoot>>>,
    daemon_owned: Arc<Current<BTreeMap<PathBuf, DaemonOwnedDirectory>>>,
}

/// A directory whose contents are produced or retained by the daemon and can
/// therefore never participate in the authored asset namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DaemonOwnedDirectoryKind {
    State,
    ModuleStaging,
    CodegenOutput,
}

impl std::fmt::Display for DaemonOwnedDirectoryKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::State => "daemon state",
            Self::ModuleStaging => "pipeline module staging",
            Self::CodegenOutput => "codegen output",
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
    /// A traversed directory's canonical path.
    canonical_path: Option<PathBuf>,
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
/// bytes yield a complete namespace skeleton or a namespace error.
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
/// namespace, keyed by its canonical rooted path. It is the scan's own
/// state: `doctor verify` reports the ones a fresh scan observes.
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
    /// What the baseline published under each affected prefix an event
    /// named, as the scan read it for the event's previous spelling.
    published: BTreeMap<(String, String), PublishedFiles>,
}

/// The files a baseline publishes at or below one rooted path, in path
/// order (the path's own row first), with each symlink alias's target.
#[derive(Debug, Clone, Default)]
pub struct PublishedFiles {
    files: Vec<ScannedFile>,
    aliases: Vec<((String, String), PathBuf)>,
}

impl PublishedFiles {
    fn read(reader: &StoreReader, root: &str, prefix: &str) -> Result<Self, StoreError> {
        let mut published = Self::default();
        for row in reader.observed_files_under(root, prefix)? {
            let key = (row.root_name.clone(), row.path.clone());
            let (file, alias) = scanned_file_row(row);
            published.aliases.extend(alias.map(|target| (key, target)));
            published.files.push(file);
        }
        Ok(published)
    }
}

impl ScanSnapshot {
    /// Equality of filesystem authority, excluding parsed-object allocation
    /// details and diagnostics: the oracle `matches_published` is tested
    /// against.
    #[cfg(test)]
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
            && self.symlink_aliases == other.symlink_aliases
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn file_rows(&self) -> impl Iterator<Item = &ScannedFile> {
        self.files.values()
    }

    pub fn bundle_rows(&self) -> impl Iterator<Item = &ScannedBundle> {
        self.bundles.values().map(AsRef::as_ref)
    }

    pub fn diagnostic_rows(&self) -> impl Iterator<Item = &ScanDiagnostic> {
        self.diagnostics.values()
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

    /// The bundle this delta read at `key`, if it observed one there.
    pub(crate) fn observed_bundle(&self, key: &(String, String)) -> Option<&Arc<ScannedBundle>> {
        self.observed.bundles.get(key)
    }

    pub(crate) fn observed_bundle_entries(
        &self,
    ) -> impl Iterator<Item = (&(String, String), &Arc<ScannedBundle>)> {
        self.observed.bundles.iter()
    }

    /// Whether a rename from `root`/`from` moves nothing: this delta covers
    /// the path and the store publishes nothing at or below it. The daemon's
    /// own atomic writes rename an unobserved temporary file onto their
    /// target; such a rename carries no identity.
    pub(crate) fn rename_moves_nothing(
        &self,
        published: &StoreReader,
        root: &str,
        from: &str,
    ) -> Result<bool, StoreError> {
        Ok(self
            .affected
            .iter()
            .any(|(prefix_root, prefix)| path_matches(prefix_root, prefix, root, from))
            && !published.observes_under(root, from)?)
    }

    /// Whether the store publishes, under every affected prefix, the
    /// namespace this delta observed there: its files, symlink targets and
    /// directories, and its bundle files' hashes (never their bytes).
    /// Diagnostics are scanner state and do not count. `published` is the
    /// store the delta's baseline read: a prefix an event named compares
    /// with the files that read returned.
    pub(crate) fn matches_published(&self, published: &StoreReader) -> Result<bool, StoreError> {
        for affected in &self.affected {
            let (root, prefix) = affected;
            let read;
            let PublishedFiles { files, aliases } = match self.published.get(affected) {
                Some(files) => files,
                None => {
                    read = PublishedFiles::read(published, root, prefix)?;
                    &read
                }
            };
            let bundles = published.bundle_file_hashes_under(root, prefix)?;
            let same = files
                .iter()
                .eq(matching_values(&self.observed.files, affected))
                && aliases
                    .iter()
                    .map(|(key, target)| (key, target))
                    .eq(subtree_entries(&self.observed.symlink_aliases, affected))
                && bundles
                    .iter()
                    .map(|(path, hash)| (root.as_str(), path.as_str(), BundleFileHash(*hash)))
                    .eq(matching_bundle_observations(
                        &self.observed.bundles,
                        affected,
                    ));
            if !same {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// The last published observation an incremental scan is checked against:
/// either an in-memory [`ScanSnapshot`] or the store's scan tables
/// ([`StoredBaseline`]).
pub trait ScanBaseline {
    /// The files published at or below one rooted path, its own first.
    fn files_under(&self, root: &str, path: &str) -> PublishedFiles;
    /// The symlinked files whose canonical target lies at or below `canonical`.
    fn aliases_affected_by(&self, canonical: &Path) -> Vec<(String, String)>;
    /// The first traversed directory whose canonical path is `canonical`,
    /// with its physical path.
    fn directory_by_target(&self, canonical: &Path) -> Option<(String, String, PathBuf)>;
}

impl ScanBaseline for ScanSnapshot {
    fn files_under(&self, root: &str, path: &str) -> PublishedFiles {
        let prefix = (root.to_owned(), path.to_owned());
        PublishedFiles {
            files: matching_values(&self.files, &prefix).cloned().collect(),
            aliases: subtree_entries(&self.symlink_aliases, &prefix)
                .map(|(key, target)| (key.clone(), target.clone()))
                .collect(),
        }
    }

    fn aliases_affected_by(&self, canonical: &Path) -> Vec<(String, String)> {
        aliases_affected_by(&self.aliases_by_target, canonical)
    }

    fn directory_by_target(&self, canonical: &Path) -> Option<(String, String, PathBuf)> {
        self.directory_by_target.get(canonical).cloned()
    }
}

/// [`ScanBaseline`] over the store's scan tables and `scanner`'s roots,
/// whose own directories are their configuration's. A failed query answers
/// as if the row were absent and is kept for [`StoredBaseline::finish`],
/// which the caller checks before trusting the delta.
pub(crate) struct StoredBaseline<'a> {
    reader: &'a StoreReader,
    roots: Arc<BTreeMap<String, CanonicalRoot>>,
    failure: RefCell<Option<StoreError>>,
}

impl<'a> StoredBaseline<'a> {
    pub(crate) fn new(reader: &'a StoreReader, scanner: &RootedScanner) -> Self {
        Self {
            reader,
            roots: scanner.root_snapshot(),
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
    fn files_under(&self, root: &str, path: &str) -> PublishedFiles {
        self.keep(PublishedFiles::read(self.reader, root, path))
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
        if let Some((name, root)) = self
            .roots
            .iter()
            .find(|(_, root)| root.canonical_path == canonical)
        {
            return Some((name.clone(), String::new(), root.canonical_path.clone()));
        }
        let row = self.keep(self.reader.directory_by_canonical(&encode_path(canonical)))?;
        // A traversed directory's physical path is its root's joined with
        // its on-disk spelling.
        let root = self.roots.get(&row.root_name)?;
        let relative = platform_path(&decode_raw_path(&row.file.raw_path))?;
        Some((row.root_name, row.path, root.canonical_path.join(relative)))
    }
}

impl ScanSnapshot {
    /// The complete published observation, from the store's scan tables:
    /// the oracle the bounded readers are compared with. No publication
    /// loads it. A bundle is its `files` row's hash: the store keeps no
    /// bytes, and the oracle compares only hashes.
    #[cfg(test)]
    pub(crate) fn load(reader: &StoreReader) -> Result<Self, StoreError> {
        let mut bundles = Vec::new();
        reader.for_each_bundle_file_hash(|root_name, path, hash| {
            bundles.push((root_name, path, hash));
            Ok(())
        })?;
        Self::from_rows(reader.observed_files()?, bundles)
    }

    #[cfg(test)]
    fn from_rows(
        files: Vec<ObservedFile>,
        bundles: Vec<(String, String, [u8; 32])>,
    ) -> Result<Self, StoreError> {
        let mut snapshot = Self::default();
        for row in files {
            let key = (row.root_name.clone(), row.path.clone());
            let (file, alias) = scanned_file_row(row);
            if let Some(target) = alias {
                snapshot.symlink_aliases.insert(key.clone(), target);
            }
            snapshot.files.insert(key, file);
        }
        for (root_name, path, hash) in bundles {
            let mut bundle = scanned_bundle(&root_name, &path, Vec::new());
            bundle.file_hash = BundleFileHash(hash);
            snapshot.bundles.insert((root_name, path), Arc::new(bundle));
        }
        rebuild_reverse_indexes(&mut snapshot).map_err(|error| {
            StoreError::InvalidConfiguration {
                error: format!("published scan tables are inconsistent: {error}"),
            }
        })?;
        Ok(snapshot)
    }

    /// Whether this scan observes what the store's scan tables publish:
    /// `self.same_namespace_observation(&ScanSnapshot::load(reader)?)`.
    /// The tables are streamed in key order and compared
    /// row by row; a bundle file is compared by its stored hash, so no
    /// bundle bytes are read or parsed.
    pub(crate) fn matches_published(&self, reader: &StoreReader) -> Result<bool, StoreError> {
        let mut same = true;

        let (mut files, mut aliases) = (self.files.iter(), self.symlink_aliases.iter());
        reader.for_each_observed_file(|row| {
            if same {
                let key = (row.root_name.clone(), row.path.clone());
                let (file, alias) = scanned_file_row(row);
                if let Some(target) = alias {
                    same = aliases
                        .next()
                        .is_some_and(|(left, right)| *left == key && *right == target);
                }
                same &= files
                    .next()
                    .is_some_and(|(left, right)| *left == key && *right == file);
            }
            Ok(())
        })?;
        same &= files.next().is_none() && aliases.next().is_none();

        let mut bundles = self.bundles.values();
        reader.for_each_bundle_file_hash(|root_name, path, hash| {
            if same {
                same = bundles.next().is_some_and(|bundle| {
                    bundle.root_name == root_name
                        && bundle.normalized_path == path
                        && bundle.file_hash == BundleFileHash(hash)
                });
            }
            Ok(())
        })?;
        same &= bundles.next().is_none();
        Ok(same)
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
            symlink_target: self
                .symlink_aliases
                .get(key)
                .map(|target| encode_path(target)),
            canonical_path: file.canonical_path.as_deref().map(encode_path),
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
}

impl ScanDelta {
    /// The fresh observation of the affected prefixes.
    pub(crate) fn observed(&self) -> &ScanSnapshot {
        &self.observed
    }
}

/// A path in the store's encoding: its platform bytes (on Windows, its
/// UTF-16 code units, little-endian).
#[cfg(unix)]
pub(crate) fn encode_path(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().to_vec()
}

#[cfg(unix)]
pub(crate) fn decode_path(bytes: &[u8]) -> PathBuf {
    use std::os::unix::ffi::OsStringExt;
    PathBuf::from(OsString::from_vec(bytes.to_vec()))
}

#[cfg(windows)]
pub(crate) fn encode_path(path: &Path) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt;
    path.as_os_str()
        .encode_wide()
        .flat_map(u16::to_le_bytes)
        .collect()
}

#[cfg(windows)]
pub(crate) fn decode_path(bytes: &[u8]) -> PathBuf {
    use std::os::windows::ffi::OsStringExt;
    let units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|unit| u16::from_le_bytes([unit[0], unit[1]]))
        .collect();
    PathBuf::from(OsString::from_wide(&units))
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

/// One `files` row as the scan file it records, with its symlink target.
fn scanned_file_row(row: ObservedFile) -> (ScannedFile, Option<PathBuf>) {
    let alias = row.file.symlink_target.as_deref().map(decode_path);
    let file = ScannedFile {
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
        canonical_path: row.file.canonical_path.as_deref().map(decode_path),
    };
    (file, alias)
}

pub(crate) fn scanned_bundle(
    root_name: &str,
    normalized_path: &str,
    bytes: Vec<u8>,
) -> ScannedBundle {
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
        let daemon_owned = Arc::new(Current::new(BTreeMap::new()));
        Ok(Self {
            roots: Arc::new(Current::new(canonicalize_roots(roots)?)),
            daemon_owned,
        })
    }

    pub(crate) fn candidate_with_roots(
        &self,
        roots: impl IntoIterator<Item = AssetRoot>,
    ) -> Result<Self, ScanError> {
        Ok(Self {
            roots: Arc::new(Current::new(canonicalize_roots(roots)?)),
            daemon_owned: Arc::clone(&self.daemon_owned),
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
        self.daemon_owned.update(|current| {
            let mut directories = BTreeMap::clone(current);
            match directories.entry(path.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(retained.clone());
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let current = entry.get();
                    if (retained.kind, &retained.path) < (current.kind, &current.path) {
                        entry.insert(retained.clone());
                    }
                }
            }
            directories
        });
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
        let replacement = self.candidate_with_roots(roots)?;
        self.replace_from(&replacement);
        Ok(())
    }

    pub(crate) fn replace_from(&self, replacement: &Self) {
        self.roots.store(replacement.root_snapshot());
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
    pub(crate) fn watch_coverage(&self) -> Vec<PathBuf> {
        self.root_snapshot()
            .values()
            .map(|root| root.canonical_path.clone())
            .collect()
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

    /// The configured directory of the root `path` (a [`Self::physical_path`])
    /// lies in: the tree whose staging directory a write to `path` stages in
    /// (`distill_store::atomic_file`).
    pub fn root_containing(&self, path: &Path) -> Result<PathBuf, ScanError> {
        self.root_snapshot()
            .values()
            .map(|root| &root.configured.path)
            .filter(|root| path.starts_with(root))
            .max_by_key(|root| root.components().count())
            .cloned()
            .ok_or_else(|| ScanError::UnknownRoot(path.display().to_string()))
    }

    fn root_snapshot(&self) -> Arc<BTreeMap<String, CanonicalRoot>> {
        self.roots.load()
    }

    fn daemon_owned_snapshot(&self) -> Arc<BTreeMap<PathBuf, DaemonOwnedDirectory>> {
        self.daemon_owned.load()
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
        let mut published = BTreeMap::new();
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
                let files = baseline.files_under(&key.0, &key.1);
                let physical = affected.entry(key.clone()).or_default();
                physical.insert(event_path.clone());
                if let Some(previous) = files
                    .files
                    .first()
                    .filter(|file| file.normalized_path == key.1)
                {
                    if let Some(relative) = platform_path(&previous.raw_relative_path) {
                        physical.insert(roots[&key.0].canonical_path.join(relative));
                    }
                }
                published.insert(key, files);
            }
        }
        if affected.is_empty() {
            return Ok(None);
        }
        let physical = affected.clone();
        let affected = collapse_affected(affected.into_keys().collect());
        published.retain(|key, _| affected.contains(key));
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
        Ok(Some(ScanDelta {
            affected,
            observed,
            published,
        }))
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

    /// The bundle at (`root`, `path`) as the reader's version observed it:
    /// the file's bytes, read through the scan's identity checks, when they
    /// hash to that version's `files` content hash. The store keeps no copy
    /// of a file's bytes. A file that is gone, unreadable or holds other
    /// bytes changed since the version observed it: `StoreError::Drifted`,
    /// which the watcher reports and the caller retries once published.
    pub(crate) fn read_published_bundle(
        &self,
        reader: &StoreReader,
        root: &str,
        path: &str,
    ) -> Result<ScannedBundle, StoreError> {
        let drifted = || StoreError::Drifted {
            root: root.to_owned(),
            path: path.to_owned(),
        };
        let expected = reader.file_content_hash(root, path)?.ok_or_else(drifted)?;
        let physical = self.physical_path(root, path).map_err(|_| drifted())?;
        let bytes = self
            .read_identity_checked(&physical)
            .map_err(|_| drifted())?;
        if ContentHash(*blake3::hash(&bytes).as_bytes()) != expected {
            return Err(drifted());
        }
        Ok(scanned_bundle(root, path, bytes))
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

    fn validated_root_snapshot(&self) -> Result<Arc<BTreeMap<String, CanonicalRoot>>, ScanError> {
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
                canonical_path: Some(canonical_path.clone()),
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
            // A staging directory holds uncommitted writes: nothing in it is
            // input.
            if distill_store::atomic_file::is_staging_name(&entry) {
                continue;
            }
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
        canonical_path: None,
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
            record_daemon_owned_diagnostic(
                &mut snapshot,
                root_name,
                &relative,
                &physical,
                retained,
            );
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
    // A staging directory holds uncommitted writes: nothing in it is input.
    if distill_store::atomic_file::in_staging(relative) {
        return Ok(None);
    }
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

/// The entries of `map` at or below `prefix` (every entry of its root when
/// the path is empty), in key order: the exact key, then the keys in
/// `[path/, path0)`. Siblings such as `path.txt` or `path-old/x` sort between
/// `path` and `path/`, so a walk from `path` that stopped at the first
/// non-matching key would never reach the subtree.
fn subtree_entries<'a, V>(
    map: &'a BTreeMap<(String, String), V>,
    prefix: &(String, String),
) -> Box<dyn Iterator<Item = (&'a (String, String), &'a V)> + 'a> {
    let (root, path) = prefix;
    if path.is_empty() {
        let root = root.clone();
        return Box::new(
            map.range((root.clone(), String::new())..)
                .take_while(move |(key, _)| key.0 == root),
        );
    }
    let below = map.range((root.clone(), format!("{path}/"))..(root.clone(), format!("{path}0")));
    Box::new(map.get_key_value(prefix).into_iter().chain(below))
}

fn matching_values<'a, V>(
    map: &'a BTreeMap<(String, String), V>,
    prefix: &(String, String),
) -> impl Iterator<Item = &'a V> {
    subtree_entries(map, prefix).map(|(_, value)| value)
}

fn matching_keys<V>(
    map: &BTreeMap<(String, String), V>,
    prefix: &(String, String),
) -> Vec<(String, String)> {
    subtree_entries(map, prefix)
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
    let file = open_entry(&path).map_err(|source| ScanError::Io {
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

/// Open a scanned file or directory for reading.
#[cfg(not(windows))]
fn open_entry(path: &Path) -> std::io::Result<File> {
    File::open(path)
}

/// Open a scanned file or directory for reading: a directory opens only
/// with backup semantics.
#[cfg(windows)]
fn open_entry(path: &Path) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
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

#[cfg(unix)]
fn platform_path_bytes(path: &Path) -> distill_store::state::PlatformPathBytes {
    use std::os::unix::ffi::OsStrExt;
    distill_store::state::PlatformPathBytes::Unix(path.as_os_str().as_bytes().to_vec())
}

#[cfg(windows)]
fn platform_path_bytes(path: &Path) -> distill_store::state::PlatformPathBytes {
    use std::os::windows::ffi::OsStrExt;
    distill_store::state::PlatformPathBytes::Windows(path.as_os_str().encode_wide().collect())
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

/// A raw path of this platform's kind; another platform's is `None`.
#[cfg(unix)]
fn platform_path(raw: &PlatformPathBytes) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStringExt;
    match raw {
        PlatformPathBytes::Unix(bytes) => Some(PathBuf::from(OsString::from_vec(bytes.clone()))),
        PlatformPathBytes::Windows(_) => None,
    }
}

#[cfg(windows)]
fn platform_path(raw: &PlatformPathBytes) -> Option<PathBuf> {
    use std::os::windows::ffi::OsStringExt;
    match raw {
        PlatformPathBytes::Unix(_) => None,
        PlatformPathBytes::Windows(units) => Some(PathBuf::from(OsString::from_wide(units))),
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

#[cfg(unix)]
fn os_sort_key(value: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    value.as_bytes().to_vec()
}

/// UTF-16 code units, big-endian, so the bytes order as the units do.
#[cfg(windows)]
fn os_sort_key(value: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt;
    value.encode_wide().flat_map(u16::to_be_bytes).collect()
}

#[cfg(unix)]
fn file_identity(metadata: &Metadata) -> FileIdentity {
    use std::os::unix::fs::MetadataExt;
    FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

/// Stable std has no file index on Windows (`windows_by_handle`); the
/// creation time stands in for it. A replacement within the tunneling window
/// can keep a name's creation time, so the length and modification-time
/// checks back it up where a read must not straddle a replacement.
#[cfg(windows)]
fn file_identity(metadata: &Metadata) -> FileIdentity {
    use std::os::windows::fs::MetadataExt;
    FileIdentity {
        device: 0,
        inode: metadata.creation_time(),
    }
}

fn file_identity_from_file(_file: &File, metadata: &Metadata) -> std::io::Result<FileIdentity> {
    Ok(file_identity(metadata))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scanned(path: &str, kind: ScannedFileKind, hash: u8) -> ((String, String), ScannedFile) {
        (
            ("main".to_owned(), path.to_owned()),
            ScannedFile {
                root_name: "main".to_owned(),
                normalized_path: path.to_owned(),
                kind,
                modified_nanos: 1,
                size: 1,
                content_hash: (kind != ScannedFileKind::Directory)
                    .then_some(ContentHash([hash; 32])),
                raw_relative_path: PlatformPathBytes::Unix(path.as_bytes().to_vec()),
                canonical_path: None,
            },
        )
    }

    fn snapshot(files: impl IntoIterator<Item = ((String, String), ScannedFile)>) -> ScanSnapshot {
        let mut snapshot = ScanSnapshot {
            files: files.into_iter().collect(),
            ..ScanSnapshot::default()
        };
        rebuild_reverse_indexes(&mut snapshot).unwrap();
        snapshot
    }

    /// An incremental pass's import overlay holds the rows its delta
    /// changes, not the subtree it rescanned: one edit under a root-wide
    /// prefix gives the same overlay at 100 files as at 3000.
    #[test]
    fn an_incremental_overlay_holds_only_the_changed_rows_at_any_size() {
        let overlay_len = |n: usize| {
            let files = (0..n)
                .map(|i| scanned(&format!("d{}/f{i}", i % 10), ScannedFileKind::File, 1))
                .collect::<Vec<_>>();
            let dir = tempfile::tempdir().unwrap();
            let mut store = distill_store::Store::open(distill_store::StoreConfig::new(
                dir.path().join(".distill"),
            ))
            .unwrap();
            let published = snapshot(files.clone());
            store
                .input_transaction(|txn| {
                    let version = txn.version();
                    let root = txn.intern_root("main")?;
                    for (key, file) in published.file_observations() {
                        txn.upsert_file(root, &key.1, &file, version)?;
                    }
                    Ok(())
                })
                .unwrap();
            let mut observed = files;
            observed[0] = scanned(&observed[0].0 .1.clone(), ScannedFileKind::File, 2);
            let delta = ScanDelta {
                affected: vec![("main".to_owned(), String::new())],
                observed: snapshot(observed),
                published: BTreeMap::new(),
            };
            crate::coordinator::incremental_overlay(&store.reader().unwrap(), &delta)
                .unwrap()
                .len()
        };
        assert_eq!(overlay_len(100), 1);
        assert_eq!(overlay_len(3000), 1);
    }

    /// `dir.txt` and `dir-old` sort between `dir` and `dir/child`; a subtree
    /// walk that stops at the first key outside `dir` never reaches the child.
    #[test]
    fn subtree_walks_reach_children_past_sorting_siblings() {
        let baseline = snapshot([
            scanned("dir", ScannedFileKind::Directory, 0),
            scanned("dir-old", ScannedFileKind::File, 1),
            scanned("dir-old/x", ScannedFileKind::File, 2),
            scanned("dir.txt", ScannedFileKind::File, 3),
            scanned("dir/child", ScannedFileKind::File, 4),
        ]);
        let affected = ("main".to_owned(), "dir".to_owned());
        assert_eq!(
            matching_keys(&baseline.files, &affected),
            [
                ("main".to_owned(), "dir".to_owned()),
                ("main".to_owned(), "dir/child".to_owned())
            ]
        );
        // The store publishing `baseline` answers for it.
        let dir = tempfile::tempdir().unwrap();
        let mut store = distill_store::Store::open(distill_store::StoreConfig::new(
            dir.path().join(".distill"),
        ))
        .unwrap();
        store
            .input_transaction(|txn| {
                let version = txn.version();
                let root = txn.intern_root("main")?;
                for (key, file) in baseline.file_observations() {
                    txn.upsert_file(root, &key.1, &file, version)?;
                }
                Ok(())
            })
            .unwrap();
        assert!(store.observes_under("main", "dir/child").unwrap());
        assert!(!store.observes_under("main", "di").unwrap());

        // The child changed: the delta is not the same observation.
        let changed = ScanDelta {
            affected: vec![affected.clone()],
            observed: snapshot([
                scanned("dir", ScannedFileKind::Directory, 0),
                scanned("dir/child", ScannedFileKind::File, 5),
            ]),
            published: BTreeMap::new(),
        };
        assert!(!changed.matches_published(&store).unwrap());
        let unchanged = ScanDelta {
            affected: vec![affected.clone()],
            observed: snapshot([
                scanned("dir", ScannedFileKind::Directory, 0),
                scanned("dir/child", ScannedFileKind::File, 4),
            ]),
            published: BTreeMap::new(),
        };
        assert!(unchanged.matches_published(&store).unwrap());
        assert!(!unchanged
            .rename_moves_nothing(&store, "main", "dir/child")
            .unwrap());
        assert!(unchanged
            .rename_moves_nothing(&store, "main", "dir/tmp")
            .unwrap());

        // Removing the child replaces the subtree and leaves the siblings.
        let mut applied = baseline.clone();
        applied.apply_delta(ScanDelta {
            affected: vec![affected],
            observed: snapshot([scanned("dir", ScannedFileKind::Directory, 0)]),
            published: BTreeMap::new(),
        });
        assert_eq!(
            applied
                .files
                .keys()
                .map(|key| key.1.as_str())
                .collect::<Vec<_>>(),
            ["dir", "dir-old", "dir-old/x", "dir.txt"]
        );
        assert_eq!(applied.logical_roots.get("dir/child"), None);
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

#[cfg(test)]
mod published_compare_tests {
    //! A scan compared row by row with the published tables answers what
    //! comparing it with the whole loaded snapshot answered, errors included.

    use super::*;
    use distill_store::{Store, StoreConfig};

    struct World {
        _dir: tempfile::TempDir,
        root: PathBuf,
        scanner: RootedScanner,
        store: Store,
    }

    fn new_world() -> World {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        fs::create_dir_all(root.join("dir/child")).unwrap();
        fs::write(root.join("dir/child/leaf.txt"), b"leaf").unwrap();
        fs::write(root.join("dir.txt"), b"sibling").unwrap();
        fs::write(root.join("a.bundle"), b"not a bundle").unwrap();
        fs::write(root.join("b.bundle"), b"also not a bundle").unwrap();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(root.join("dir.txt"), root.join("alias.txt")).unwrap();
            std::os::unix::fs::symlink(root.join("dir"), root.join("dir/child/loop")).unwrap();
        }
        let scanner = RootedScanner::new([AssetRoot::new("main", &root)]).unwrap();
        let store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
        World {
            _dir: dir,
            root,
            scanner,
            store,
        }
    }

    /// Publish `scan`'s rows as a complete publication writes them.
    fn publish(store: &mut Store, scan: &ScanSnapshot) {
        store
            .input_transaction(|txn| {
                let version = txn.version();
                for (key, file) in scan.file_observations() {
                    let root = txn.intern_root(&key.0)?;
                    txn.upsert_file(root, &key.1, &file, version)?;
                }
                Ok(())
            })
            .unwrap();
    }

    /// The streamed comparison and the loaded oracle's, which must agree;
    /// returns the agreed answer.
    fn compare(scan: &ScanSnapshot, store: &Store) -> Result<bool, String> {
        let streamed = scan
            .matches_published(store)
            .map_err(|error| error.to_string());
        let loaded = ScanSnapshot::load(store)
            .map(|published| scan.same_namespace_observation(&published))
            .map_err(|error| error.to_string());
        assert_eq!(streamed, loaded);
        streamed
    }

    #[test]
    fn an_unchanged_tree_matches_and_every_change_does_not() {
        type Change = fn(&Path);
        #[allow(unused_mut)]
        let mut changes: Vec<(&str, Change)> = vec![
            ("content", |root| {
                fs::write(root.join("dir.txt"), b"changed!").unwrap()
            }),
            ("added", |root| {
                fs::write(root.join("dir/new.txt"), b"new").unwrap()
            }),
            ("removed", |root| {
                fs::remove_file(root.join("dir/child/leaf.txt")).unwrap()
            }),
            ("bundle bytes", |root| {
                fs::write(root.join("b.bundle"), b"ALSO NOT A BUNDLE").unwrap()
            }),
            ("bundle removed", |root| {
                fs::remove_file(root.join("a.bundle")).unwrap()
            }),
            ("directory", |root| {
                fs::create_dir(root.join("empty")).unwrap()
            }),
        ];
        // The world has its symlinks only on Unix.
        #[cfg(unix)]
        changes.push(("symlink target", |root| {
            fs::remove_file(root.join("alias.txt")).unwrap();
            std::os::unix::fs::symlink(root.join("a.bundle"), root.join("alias.txt")).unwrap();
        }));
        for (name, change) in changes {
            let mut world = new_world();
            let published = world.scanner.scan().unwrap();
            publish(&mut world.store, &published);
            assert_eq!(compare(&published, &world.store), Ok(true));
            change(&world.root);
            let scan = world.scanner.scan().unwrap();
            assert_eq!(compare(&scan, &world.store), Ok(false), "{name}");
        }
    }

    /// The stored baseline finds each traversed directory, the roots' own
    /// included, where the scan that published it does: by its canonical
    /// path, with the physical path the scan traversed.
    #[test]
    fn stored_directories_are_found_as_the_scan_finds_them() {
        let mut world = new_world();
        let scan = world.scanner.scan().unwrap();
        publish(&mut world.store, &scan);
        let stored = StoredBaseline::new(&world.store, &world.scanner);
        assert!(scan.directory_observations.len() > 2);
        for observation in scan.directory_observations.values() {
            let canonical = &observation.canonical_path;
            assert_eq!(
                stored.directory_by_target(canonical),
                scan.directory_by_target(canonical),
                "{canonical:?}"
            );
            assert!(stored.directory_by_target(canonical).is_some());
        }
        assert_eq!(
            stored.directory_by_target(&world.root.join("nowhere")),
            None
        );
        stored.finish().unwrap();
    }

    #[test]
    fn inconsistent_tables_fail_as_loading_them_fails() {
        // Two traversed directories sharing a canonical path never reach
        // the tables: the unique index rejects the write.
        let mut world = new_world();
        let scan = world.scanner.scan().unwrap();
        publish(&mut world.store, &scan);
        let (_, directory) = scan
            .file_observations()
            .find(|(_, file)| file.canonical_path.is_some())
            .unwrap();
        assert!(world
            .store
            .input_transaction(|txn| {
                let root = txn.intern_root("main")?;
                txn.upsert_file(root, "elsewhere", &directory, txn.version())
            })
            .is_err());
        assert_eq!(compare(&scan, &world.store), Ok(true));
    }
}
