//! Filesystem-side authority for §14 scanning and §6 lineage repair.
//!
//! Logical paths are accepted only in their canonical root-relative form.
//! Physical traversal is identity checked, quarantine identities are excluded,
//! directory aliases/cycles are errors, and a file is accepted only when the
//! identity opened for reading still names the path after the read.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, Metadata};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use distill_core::attestation::SCHEMA_LINEAGE_MANIFEST_TYPE_UUID;
use distill_core::id::{BundleFileHash, ContentHash};
use distill_rpc::{
    LineageManifestClaimant, LineageRepairDestination, OccupiedLineageDestinationKind,
};
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
    InvalidRootName(String),
    DuplicateRootName(String),
    UnknownRoot(String),
    InvalidLogicalPath(String),
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
        identity: ObservedFileIdentity,
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
            Self::InvalidRootName(name) => write!(f, "invalid canonical root name {name:?}"),
            Self::DuplicateRootName(name) => write!(f, "duplicate asset root {name:?}"),
            Self::UnknownRoot(name) => write!(f, "unknown asset root {name:?}"),
            Self::InvalidLogicalPath(path) => write!(f, "invalid canonical logical path {path:?}"),
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
                write!(f, "directory identity cycle at {}", path.display())
            }
            Self::DirectoryAlias { first, second, .. } => write!(
                f,
                "one directory identity is reachable as both {} and {}",
                first.display(),
                second.display()
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
    quarantine_identity: Option<FileIdentity>,
}

#[derive(Debug, Clone)]
pub struct RootedScanner {
    roots: Arc<RwLock<BTreeMap<String, CanonicalRoot>>>,
    revision: Arc<AtomicU64>,
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
}

#[derive(Debug, Clone, Default)]
pub struct ScanSnapshot {
    pub files: Vec<ScannedFile>,
    pub bundles: Vec<ScannedBundle>,
}

#[derive(Debug, Clone)]
struct PendingDirectory {
    root_name: String,
    physical_path: PathBuf,
    relative_components: Vec<String>,
    ancestry: BTreeSet<FileIdentity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum FileIdentity {
    #[cfg(unix)]
    Unix { device: u64, inode: u64 },
    #[cfg(windows)]
    Windows {
        volume_serial: u64,
        file_id: [u8; 16],
    },
    #[cfg(not(any(unix, windows)))]
    Portable { len: u64, modified_nanos: u128 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservedFileIdentity {
    #[cfg(unix)]
    Unix { device: u64, inode: u64 },
    #[cfg(windows)]
    Windows {
        volume_serial: u64,
        file_id: [u8; 16],
    },
    #[cfg(not(any(unix, windows)))]
    Portable,
}

impl From<FileIdentity> for ObservedFileIdentity {
    fn from(identity: FileIdentity) -> Self {
        match identity {
            #[cfg(unix)]
            FileIdentity::Unix { device, inode } => Self::Unix { device, inode },
            #[cfg(windows)]
            FileIdentity::Windows {
                volume_serial,
                file_id,
            } => Self::Windows {
                volume_serial,
                file_id,
            },
            #[cfg(not(any(unix, windows)))]
            FileIdentity::Portable { .. } => Self::Portable,
        }
    }
}

impl RootedScanner {
    pub fn new(roots: impl IntoIterator<Item = AssetRoot>) -> Result<Self, ScanError> {
        Ok(Self {
            roots: Arc::new(RwLock::new(canonicalize_roots(roots)?)),
            revision: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Validate a replacement root set completely before making it visible
    /// to any scanner clone. Watchers, authoring, and the coordinator all hold
    /// clones of this handle, so one swap changes their next pinned scan
    /// together without interrupting a traversal already in flight.
    pub fn replace_roots(
        &self,
        roots: impl IntoIterator<Item = AssetRoot>,
    ) -> Result<(), ScanError> {
        let replacement = Self::new(roots)?;
        self.replace_from(&replacement);
        Ok(())
    }

    pub(crate) fn replace_from(&self, replacement: &Self) {
        *self
            .roots
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = replacement.root_snapshot();
        self.revision.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
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

    /// Locate an observed physical error beneath its configured root and
    /// preserve the platform's exact relative path units for DSVP.
    pub fn scan_subject(&self, path: &Path) -> Option<distill_store::state::ScanSubject> {
        self.root_snapshot().values().find_map(|root| {
            let relative = path.strip_prefix(&root.configured.path).ok()?;
            if relative.as_os_str().is_empty() {
                Some(distill_store::state::ScanSubject::Root {
                    root_name: root.configured.name.clone(),
                })
            } else {
                Some(distill_store::state::ScanSubject::Subtree {
                    root_name: root.configured.name.clone(),
                    raw_relative_path: platform_path_bytes(relative),
                })
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

    pub fn lineage_claimants(&self) -> Result<Vec<LineageManifestClaimant>, ScanError> {
        let mut claimants = Vec::new();
        for bundle in self.scan()?.bundles {
            let Ok(parsed) = bundle.parsed else {
                continue;
            };
            for (local_id, entry) in parsed.assets {
                if entry.type_uuid == SCHEMA_LINEAGE_MANIFEST_TYPE_UUID {
                    claimants.push(LineageManifestClaimant {
                        root_name: bundle.root_name.clone(),
                        normalized_path: bundle.normalized_path.clone(),
                        bundle: parsed.uuid,
                        local_id,
                        asset: entry.uuid,
                        file_hash: bundle.file_hash,
                    });
                }
            }
        }
        claimants.sort();
        claimants.dedup();
        Ok(claimants)
    }

    /// Enumerate the complete raw namespace in deterministic `(root, path)`
    /// order. Watchers arm outside this call; their generation-tagged event
    /// union is applied by the coordinator after this candidate commits.
    pub fn scan(&self) -> Result<ScanSnapshot, ScanError> {
        let roots = self.root_snapshot();
        let mut stack = roots
            .values()
            .rev()
            .map(|root| PendingDirectory {
                root_name: root.configured.name.clone(),
                physical_path: root.configured.path.clone(),
                relative_components: Vec::new(),
                ancestry: BTreeSet::new(),
            })
            .collect::<Vec<_>>();
        let mut identities = BTreeMap::<FileIdentity, (String, PathBuf)>::new();
        let mut snapshot = ScanSnapshot::default();

        while let Some(pending) = stack.pop() {
            let root = &roots[&pending.root_name];
            let metadata =
                fs::metadata(&pending.physical_path).map_err(|source| ScanError::Io {
                    path: pending.physical_path.clone(),
                    source,
                })?;
            if !metadata.is_dir() {
                return Err(ScanError::NonRegularFile {
                    path: pending.physical_path,
                });
            }
            let identity = file_identity(&metadata);
            let live_quarantine_identity = fs::metadata(&root.configured.quarantine_dir)
                .ok()
                .filter(|metadata| metadata.is_dir())
                .map(|metadata| file_identity(&metadata));
            if pending.physical_path == root.configured.quarantine_dir
                || root.quarantine_identity == Some(identity)
                || live_quarantine_identity == Some(identity)
            {
                continue;
            }
            if pending.ancestry.contains(&identity) {
                return Err(ScanError::DirectoryCycle {
                    path: pending.physical_path,
                });
            }
            if !pending.relative_components.is_empty() {
                snapshot.files.push(ScannedFile {
                    root_name: pending.root_name.clone(),
                    normalized_path: pending.relative_components.join("/"),
                    kind: ScannedFileKind::Directory,
                    modified_nanos: modified_nanos(&metadata),
                    size: metadata.len(),
                    content_hash: None,
                });
            }
            if let Some((first_root, first)) = identities.insert(
                identity,
                (pending.root_name.clone(), pending.physical_path.clone()),
            ) {
                if first_root != pending.root_name || first != pending.physical_path {
                    return Err(ScanError::DirectoryAlias {
                        first_root,
                        first,
                        second_root: pending.root_name,
                        second: pending.physical_path,
                        identity: identity.into(),
                    });
                }
            }

            let mut entries = fs::read_dir(&pending.physical_path)
                .map_err(|source| ScanError::Io {
                    path: pending.physical_path.clone(),
                    source,
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|source| ScanError::Io {
                    path: pending.physical_path.clone(),
                    source,
                })?;
            entries.sort_by_key(|entry| os_sort_key(&entry.file_name()));
            let after = fs::metadata(&pending.physical_path).map_err(|source| ScanError::Io {
                path: pending.physical_path.clone(),
                source,
            })?;
            if file_identity(&after) != identity {
                return Err(ScanError::FileIdentityChanged {
                    path: pending.physical_path,
                });
            }

            for entry in entries.into_iter().rev() {
                let component = normalize_component(&entry.file_name())?;
                let mut relative = pending.relative_components.clone();
                relative.push(component);
                let physical = entry.path();
                let link_metadata =
                    fs::symlink_metadata(&physical).map_err(|source| ScanError::Io {
                        path: physical.clone(),
                        source,
                    })?;
                let metadata = fs::metadata(&physical).map_err(|source| ScanError::Io {
                    path: physical.clone(),
                    source,
                })?;
                let is_symlink = link_metadata.file_type().is_symlink();
                if is_symlink {
                    let canonical =
                        fs::canonicalize(&physical).map_err(|source| ScanError::Io {
                            path: physical.clone(),
                            source,
                        })?;
                    if !roots
                        .values()
                        .any(|candidate| canonical.starts_with(&candidate.canonical_path))
                    {
                        return Err(ScanError::SymlinkEscape {
                            path: physical,
                            target: canonical,
                        });
                    }
                }
                if metadata.is_dir() {
                    let canonical =
                        fs::canonicalize(&physical).map_err(|source| ScanError::Io {
                            path: physical.clone(),
                            source,
                        })?;
                    if !roots
                        .values()
                        .any(|candidate| canonical.starts_with(&candidate.canonical_path))
                    {
                        return Err(ScanError::SymlinkEscape {
                            path: physical,
                            target: canonical,
                        });
                    }
                    let mut ancestry = pending.ancestry.clone();
                    ancestry.insert(identity);
                    stack.push(PendingDirectory {
                        root_name: pending.root_name.clone(),
                        physical_path: physical,
                        relative_components: relative,
                        ancestry,
                    });
                } else if metadata.is_file() {
                    let bytes = self.read_identity_checked(&physical)?;
                    let normalized_path = relative.join("/");
                    snapshot.files.push(ScannedFile {
                        root_name: pending.root_name.clone(),
                        normalized_path: normalized_path.clone(),
                        kind: if is_symlink {
                            ScannedFileKind::Symlink
                        } else {
                            ScannedFileKind::File
                        },
                        modified_nanos: modified_nanos(&metadata),
                        size: metadata.len(),
                        content_hash: Some(ContentHash(*blake3::hash(&bytes).as_bytes())),
                    });
                    if physical
                        .extension()
                        .and_then(|extension| extension.to_str())
                        == Some("bundle")
                    {
                        snapshot.bundles.push(ScannedBundle {
                            root_name: pending.root_name.clone(),
                            normalized_path,
                            file_hash: BundleFileHash::of_observed_bytes(&bytes),
                            parsed: distill_bundle::parse_bundle(&bytes),
                            bytes,
                        });
                    }
                } else {
                    return Err(ScanError::NonRegularFile { path: physical });
                }
            }
        }
        snapshot.files.sort_by(|left, right| {
            (&left.root_name, &left.normalized_path)
                .cmp(&(&right.root_name, &right.normalized_path))
        });
        snapshot.bundles.sort_by(|left, right| {
            (&left.root_name, &left.normalized_path)
                .cmp(&(&right.root_name, &right.normalized_path))
        });
        Ok(snapshot)
    }

    pub fn read_identity_checked(&self, path: &Path) -> Result<Vec<u8>, ScanError> {
        let mut file = File::open(path).map_err(|source| ScanError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let opened = file.metadata().map_err(|source| ScanError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if !opened.is_file() {
            return Err(ScanError::NonRegularFile {
                path: path.to_path_buf(),
            });
        }
        let identity = file_identity(&opened);
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|source| ScanError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        let current = fs::metadata(path).map_err(|source| ScanError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if file_identity(&current) != identity || current.len() != bytes.len() as u64 {
            return Err(ScanError::FileIdentityChanged {
                path: path.to_path_buf(),
            });
        }
        Ok(bytes)
    }
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
            let quarantine_identity = match fs::metadata(&root.quarantine_dir) {
                Ok(metadata) if metadata.is_dir() => Some(file_identity(&metadata)),
                Ok(_) => {
                    return Err(ScanError::NonRegularFile {
                        path: root.quarantine_dir.clone(),
                    })
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(source) => {
                    return Err(ScanError::Io {
                        path: root.quarantine_dir.clone(),
                        source,
                    })
                }
            };
            Ok((root.clone(), canonical_path, quarantine_identity))
        })
        .collect::<Result<Vec<_>, ScanError>>()?;

    let mut roots = BTreeMap::new();
    for (configured, canonical_path, quarantine_identity) in canonical_paths {
        let name = configured.name.clone();
        if roots
            .insert(
                name.clone(),
                CanonicalRoot {
                    configured,
                    canonical_path,
                    quarantine_identity,
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

#[cfg(not(any(unix, windows)))]
fn platform_path_bytes(path: &Path) -> distill_store::state::PlatformPathBytes {
    distill_store::state::PlatformPathBytes::Unix(path.to_string_lossy().as_bytes().to_vec())
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

#[cfg(unix)]
fn os_sort_key(value: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    value.as_bytes().to_vec()
}

#[cfg(windows)]
fn os_sort_key(value: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt;
    value.encode_wide().flat_map(u16::to_le_bytes).collect()
}

#[cfg(not(any(unix, windows)))]
fn os_sort_key(value: &std::ffi::OsStr) -> Vec<u8> {
    value.to_string_lossy().as_bytes().to_vec()
}

#[cfg(unix)]
fn file_identity(metadata: &Metadata) -> FileIdentity {
    use std::os::unix::fs::MetadataExt;
    FileIdentity::Unix {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

#[cfg(windows)]
fn file_identity(metadata: &Metadata) -> FileIdentity {
    use std::os::windows::fs::MetadataExt;
    let mut file_id = [0u8; 16];
    file_id[..8].copy_from_slice(&metadata.file_index().unwrap_or(0).to_le_bytes());
    FileIdentity::Windows {
        volume_serial: u64::from(metadata.volume_serial_number().unwrap_or(0)),
        file_id,
    }
}

#[cfg(not(any(unix, windows)))]
fn file_identity(metadata: &Metadata) -> FileIdentity {
    let modified_nanos = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |duration| duration.as_nanos());
    FileIdentity::Portable {
        len: metadata.len(),
        modified_nanos,
    }
}
