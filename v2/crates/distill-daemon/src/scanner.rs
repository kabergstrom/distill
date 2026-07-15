//! Filesystem-side authority for §14 scanning and §6 lineage repair.
//!
//! Logical paths are accepted only in their canonical root-relative form.
//! Physical traversal is identity checked, quarantine identities are excluded,
//! directory aliases/cycles are errors, and a file is accepted only when the
//! identity opened for reading still names the path after the read.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
#[cfg(unix)]
use std::ffi::{CStr, CString, OsString};
use std::fs::{self, File, Metadata};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use distill_core::bootstrap::SCHEMA_LINEAGE_MANIFEST_TYPE_UUID;
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
    directory: DirectoryCapability,
}

#[derive(Debug, Clone)]
struct DirectoryCapability {
    #[cfg(unix)]
    file: Arc<File>,
    #[cfg(not(unix))]
    path: PathBuf,
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
    directory: DirectoryCapability,
    relative_components: Vec<String>,
    ancestry: BTreeSet<FileIdentity>,
    entry_guards: Vec<EntryGuard>,
}

#[derive(Debug, Clone)]
struct EntryGuard {
    parent: DirectoryCapability,
    name: std::ffi::OsString,
    display_path: PathBuf,
    target_identity: FileIdentity,
    symlink_identity: Option<FileIdentity>,
}

#[derive(Debug)]
struct OpenedChild {
    file: File,
    metadata: Metadata,
    symlink_identity: Option<FileIdentity>,
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
                directory: root.directory.clone(),
                relative_components: Vec::new(),
                ancestry: BTreeSet::new(),
                entry_guards: Vec::new(),
            })
            .collect::<Vec<_>>();
        let mut identities = BTreeMap::<FileIdentity, (String, PathBuf)>::new();
        let mut snapshot = ScanSnapshot::default();

        while let Some(pending) = stack.pop() {
            let root = &roots[&pending.root_name];
            revalidate_entry_guards(&pending.entry_guards, &roots)?;
            let metadata = directory_metadata(&pending.directory, &pending.physical_path)?;
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

            let mut entries = read_directory_names(&pending.directory, &pending.physical_path)?;
            entries.sort_by_key(|entry| os_sort_key(entry));
            let after = directory_metadata(&pending.directory, &pending.physical_path)?;
            if file_identity(&after) != identity {
                return Err(ScanError::FileIdentityChanged {
                    path: pending.physical_path,
                });
            }
            revalidate_entry_guards(&pending.entry_guards, &roots)?;

            for entry in entries.into_iter().rev() {
                let component = normalize_component(&entry)?;
                let mut relative = pending.relative_components.clone();
                relative.push(component);
                let physical = pending.physical_path.join(&entry);
                let opened = open_scanned_child(&pending.directory, &entry, &physical, &roots)?;
                let guard = EntryGuard::new(&pending.directory, &entry, &physical, &opened);
                if opened.metadata.is_dir() {
                    let mut ancestry = pending.ancestry.clone();
                    ancestry.insert(identity);
                    let mut entry_guards = pending.entry_guards.clone();
                    entry_guards.push(guard);
                    let directory =
                        DirectoryCapability::from_open_directory(opened.file, &physical);
                    stack.push(PendingDirectory {
                        root_name: pending.root_name.clone(),
                        physical_path: physical,
                        directory,
                        relative_components: relative,
                        ancestry,
                        entry_guards,
                    });
                } else if opened.metadata.is_file() {
                    let metadata = opened.metadata;
                    let is_symlink = opened.symlink_identity.is_some();
                    let bytes = read_opened_file(opened.file, &physical, &metadata)?;
                    revalidate_entry_guards(&pending.entry_guards, &roots)?;
                    revalidate_entry_guards(std::slice::from_ref(&guard), &roots)?;
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
        let roots = self.root_snapshot();
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
        let mut directory = root.0.directory.clone();
        let mut display = root.0.configured.path.clone();
        let mut entry_guards = Vec::new();
        for (index, component) in components.iter().enumerate() {
            display.push(component);
            let opened = open_scanned_child(&directory, component, &display, &roots)?;
            let guard = EntryGuard::new(&directory, component, &display, &opened);
            if index + 1 == components.len() {
                if !opened.metadata.is_file() {
                    return Err(ScanError::NonRegularFile { path: display });
                }
                let bytes = read_opened_file(opened.file, path, &opened.metadata)?;
                entry_guards.push(guard);
                revalidate_entry_guards(&entry_guards, &roots)?;
                return Ok(bytes);
            }
            if !opened.metadata.is_dir() {
                return Err(ScanError::NonRegularFile { path: display });
            }
            entry_guards.push(guard);
            directory = DirectoryCapability::from_open_directory(opened.file, &display);
        }
        unreachable!("nonempty component walk returns at its final component")
    }
}

impl DirectoryCapability {
    fn open_root(path: &Path) -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let file = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(path)?;
            Ok(Self {
                file: Arc::new(file),
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {
                path: path.to_path_buf(),
            })
        }
    }

    fn from_open_directory(file: File, _path: &Path) -> Self {
        #[cfg(unix)]
        {
            Self {
                file: Arc::new(file),
            }
        }
        #[cfg(not(unix))]
        {
            drop(file);
            Self {
                path: _path.to_path_buf(),
            }
        }
    }
}

fn directory_metadata(
    directory: &DirectoryCapability,
    display_path: &Path,
) -> Result<Metadata, ScanError> {
    #[cfg(unix)]
    {
        directory.file.metadata().map_err(|source| ScanError::Io {
            path: display_path.to_path_buf(),
            source,
        })
    }
    #[cfg(not(unix))]
    {
        fs::metadata(&directory.path).map_err(|source| ScanError::Io {
            path: display_path.to_path_buf(),
            source,
        })
    }
}

#[cfg(unix)]
fn read_directory_names(
    directory: &DirectoryCapability,
    display_path: &Path,
) -> Result<Vec<OsString>, ScanError> {
    use std::os::fd::IntoRawFd;
    use std::os::unix::ffi::OsStringExt;

    struct Dir(*mut libc::DIR);
    impl Drop for Dir {
        fn drop(&mut self) {
            // SAFETY: fdopendir returned this sole owned DIR pointer.
            unsafe { libc::closedir(self.0) };
        }
    }

    // `dup` would share the retained descriptor's directory offset, causing
    // later or concurrent scans to start wherever an earlier scan stopped.
    // Opening `.` relative to the retained capability creates a fresh open
    // file description for this enumeration without consulting a pathname.
    let descriptor = open_at(directory, OsStr::new("."), false)
        .map_err(|source| ScanError::Io {
            path: display_path.to_path_buf(),
            source,
        })?
        .into_raw_fd();
    // SAFETY: fdopendir consumes the independently owned descriptor.
    let pointer = unsafe { libc::fdopendir(descriptor) };
    if pointer.is_null() {
        let source = std::io::Error::last_os_error();
        // SAFETY: fdopendir did not consume the descriptor on failure.
        unsafe { libc::close(descriptor) };
        return Err(ScanError::Io {
            path: display_path.to_path_buf(),
            source,
        });
    }
    let directory = Dir(pointer);
    let mut names = Vec::new();
    loop {
        set_errno(0);
        // SAFETY: the DIR remains live for this call; each returned entry is
        // copied before the next readdir invocation.
        let entry = unsafe { libc::readdir(directory.0) };
        if entry.is_null() {
            let error = errno();
            if error != 0 {
                return Err(ScanError::Io {
                    path: display_path.to_path_buf(),
                    source: std::io::Error::from_raw_os_error(error),
                });
            }
            break;
        }
        // SAFETY: POSIX dirent names are NUL terminated within d_name.
        let bytes = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if bytes != b"." && bytes != b".." {
            names.push(OsString::from_vec(bytes.to_vec()));
        }
    }
    Ok(names)
}

#[cfg(not(unix))]
fn read_directory_names(
    directory: &DirectoryCapability,
    display_path: &Path,
) -> Result<Vec<std::ffi::OsString>, ScanError> {
    fs::read_dir(&directory.path)
        .map_err(|source| ScanError::Io {
            path: display_path.to_path_buf(),
            source,
        })?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name())
                .map_err(|source| ScanError::Io {
                    path: display_path.to_path_buf(),
                    source,
                })
        })
        .collect()
}

#[cfg(unix)]
fn set_errno(value: libc::c_int) {
    // SAFETY: errno is thread-local and this function is used immediately
    // around readdir on the same thread.
    unsafe { *errno_pointer() = value };
}

#[cfg(unix)]
fn errno() -> libc::c_int {
    // SAFETY: errno_pointer returns this thread's live errno cell.
    unsafe { *errno_pointer() }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
unsafe fn errno_pointer() -> *mut libc::c_int {
    // SAFETY: delegated to the C runtime's thread-local errno accessor.
    unsafe { libc::__errno_location() }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
unsafe fn errno_pointer() -> *mut libc::c_int {
    // SAFETY: delegated to the C runtime's thread-local errno accessor.
    unsafe { libc::__error() }
}

#[cfg(unix)]
fn child_symlink_identity(
    directory: &DirectoryCapability,
    name: &OsStr,
) -> std::io::Result<Option<FileIdentity>> {
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;

    let name = CString::new(name.as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "NUL in filename"))?;
    // SAFETY: zeroed stat is an out parameter for fstatat; the directory
    // descriptor and C string remain live for the call.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    let status = unsafe {
        libc::fstatat(
            directory.file.as_raw_fd(),
            name.as_ptr(),
            &mut stat,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if status != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(
        ((stat.st_mode & libc::S_IFMT) == libc::S_IFLNK).then_some(FileIdentity::Unix {
            device: stat.st_dev as u64,
            inode: stat.st_ino as u64,
        }),
    )
}

#[cfg(unix)]
fn open_at(directory: &DirectoryCapability, name: &OsStr, follow: bool) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    let name = CString::new(name.as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "NUL in filename"))?;
    let mut flags = libc::O_RDONLY | libc::O_CLOEXEC;
    if !follow {
        flags |= libc::O_NOFOLLOW;
    }
    // SAFETY: openat receives a live directory descriptor and C string; on
    // success the returned descriptor is transferred exactly once to File.
    let descriptor = unsafe { libc::openat(directory.file.as_raw_fd(), name.as_ptr(), flags) };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn open_scanned_child(
    directory: &DirectoryCapability,
    name: &std::ffi::OsStr,
    physical: &Path,
    roots: &BTreeMap<String, CanonicalRoot>,
) -> Result<OpenedChild, ScanError> {
    #[cfg(unix)]
    {
        let symlink_identity =
            child_symlink_identity(directory, name).map_err(|source| ScanError::Io {
                path: physical.to_path_buf(),
                source,
            })?;
        let is_symlink = symlink_identity.is_some();
        let expected = if is_symlink {
            let canonical = fs::canonicalize(physical).map_err(|source| ScanError::Io {
                path: physical.to_path_buf(),
                source,
            })?;
            if !roots
                .values()
                .any(|candidate| canonical.starts_with(&candidate.canonical_path))
            {
                return Err(ScanError::SymlinkEscape {
                    path: physical.to_path_buf(),
                    target: canonical,
                });
            }
            Some(fs::metadata(&canonical).map_err(|source| ScanError::Io {
                path: canonical,
                source,
            })?)
        } else {
            None
        };
        let file = open_at(directory, name, is_symlink).map_err(|source| ScanError::Io {
            path: physical.to_path_buf(),
            source,
        })?;
        let metadata = file.metadata().map_err(|source| ScanError::Io {
            path: physical.to_path_buf(),
            source,
        })?;
        let current_symlink_identity =
            child_symlink_identity(directory, name).map_err(|source| ScanError::Io {
                path: physical.to_path_buf(),
                source,
            })?;
        if current_symlink_identity != symlink_identity
            || expected.as_ref().is_some_and(|expected| {
                file_identity(expected) != file_identity(&metadata)
                    || expected.is_dir() != metadata.is_dir()
                    || expected.is_file() != metadata.is_file()
            })
        {
            return Err(ScanError::FileIdentityChanged {
                path: physical.to_path_buf(),
            });
        }
        Ok(OpenedChild {
            file,
            metadata,
            symlink_identity,
        })
    }
    #[cfg(not(unix))]
    {
        let link_metadata = fs::symlink_metadata(physical).map_err(|source| ScanError::Io {
            path: physical.to_path_buf(),
            source,
        })?;
        let symlink_identity = link_metadata
            .file_type()
            .is_symlink()
            .then(|| file_identity(&link_metadata));
        let is_symlink = symlink_identity.is_some();
        if is_symlink {
            let canonical = fs::canonicalize(physical).map_err(|source| ScanError::Io {
                path: physical.to_path_buf(),
                source,
            })?;
            if !roots
                .values()
                .any(|candidate| canonical.starts_with(&candidate.canonical_path))
            {
                return Err(ScanError::SymlinkEscape {
                    path: physical.to_path_buf(),
                    target: canonical,
                });
            }
        }
        let file = File::open(physical).map_err(|source| ScanError::Io {
            path: physical.to_path_buf(),
            source,
        })?;
        let metadata = file.metadata().map_err(|source| ScanError::Io {
            path: physical.to_path_buf(),
            source,
        })?;
        let current_link_metadata =
            fs::symlink_metadata(physical).map_err(|source| ScanError::Io {
                path: physical.to_path_buf(),
                source,
            })?;
        let current_symlink_identity = current_link_metadata
            .file_type()
            .is_symlink()
            .then(|| file_identity(&current_link_metadata));
        if current_symlink_identity != symlink_identity {
            return Err(ScanError::FileIdentityChanged {
                path: physical.to_path_buf(),
            });
        }
        Ok(OpenedChild {
            file,
            metadata,
            symlink_identity,
        })
    }
}

impl EntryGuard {
    fn new(
        parent: &DirectoryCapability,
        name: &OsStr,
        display_path: &Path,
        opened: &OpenedChild,
    ) -> Self {
        Self {
            parent: parent.clone(),
            name: name.to_owned(),
            display_path: display_path.to_path_buf(),
            target_identity: file_identity(&opened.metadata),
            symlink_identity: opened.symlink_identity,
        }
    }
}

fn revalidate_entry_guards(
    guards: &[EntryGuard],
    roots: &BTreeMap<String, CanonicalRoot>,
) -> Result<(), ScanError> {
    for guard in guards {
        let current = open_scanned_child(&guard.parent, &guard.name, &guard.display_path, roots)?;
        if file_identity(&current.metadata) != guard.target_identity
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
    let identity = file_identity(opened);
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
    if file_identity(&after) != identity
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
            let directory = DirectoryCapability::open_root(&canonical_path).map_err(|source| {
                ScanError::Io {
                    path: canonical_path.clone(),
                    source,
                }
            })?;
            Ok((root.clone(), canonical_path, quarantine_identity, directory))
        })
        .collect::<Result<Vec<_>, ScanError>>()?;

    let mut roots = BTreeMap::new();
    for (configured, canonical_path, quarantine_identity, directory) in canonical_paths {
        let name = configured.name.clone();
        if roots
            .insert(
                name.clone(),
                CanonicalRoot {
                    configured,
                    canonical_path,
                    quarantine_identity,
                    directory,
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
