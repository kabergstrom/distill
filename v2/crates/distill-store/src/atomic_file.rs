//! The one way Distill writes a file that is not a CAS segment.
//!
//! A write stages its bytes as a temp file in a *staging directory*, syncs
//! the temp (and fails if the sync fails), and only then renames it over the
//! target, then syncs the target's directory: `fsync` on Unix,
//! `FlushFileBuffers` on a directory handle on Windows. A reader of the
//! target sees the old bytes or the new ones, never a mix.
//!
//! **Staging directories.** Every directory tree Distill writes into (each
//! asset root, the codegen output, the tool object store, a pack output)
//! owns one staging directory, [`STAGING_DIR`], directly inside it, so a
//! temp is on the same filesystem as its target and the rename is atomic. A
//! rename across filesystems fails (`EXDEV`, or `ERROR_NOT_SAME_DEVICE`)
//! before the target changes: such a write fails and changes nothing. A
//! file in a staging directory is uncommitted by definition, so the process
//! that owns the tree empties it when it opens the tree ([`open_staging`];
//! on Windows a file another process holds open is left for a later open);
//! nothing else ever deletes a temp. The scanner and the watcher ignore
//! every path through a directory named [`STAGING_DIR`].
//!
//! **What a failure means.** An `Err` from [`Staged::commit`],
//! [`write`] or [`remove`] means the target did not change. Once the rename
//! is done the change is visible, so a failure to sync the directory after
//! it is logged and the call succeeds: reporting it as a failure would tell
//! the caller a visible change did not happen.

use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use distill_core::id::ContentHash;

/// The staging directory's name, reserved in every tree Distill writes.
pub const STAGING_DIR: &str = ".distill-staging";

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Whether `name` is the reserved staging directory name.
pub fn is_staging_name(name: &OsStr) -> bool {
    name == STAGING_DIR
}

/// Whether `path` passes through a staging directory.
pub fn in_staging(path: &Path) -> bool {
    path.components()
        .any(|component| is_staging_name(component.as_os_str()))
}

/// `owner`'s staging directory.
pub fn staging_dir(owner: &Path) -> PathBuf {
    owner.join(STAGING_DIR)
}

/// Open `owner`'s staging directory for this process: delete what an
/// earlier process left in it (writes that never committed). Call it before
/// anything writes into `owner`. The directory itself is created by the
/// first write.
///
/// On Windows a file another process holds open (a virus scanner, an
/// indexer) cannot be deleted; it is logged and left for a later open. A
/// new temp never takes its name (see [`create_temp`]).
pub fn open_staging(owner: &Path) -> io::Result<()> {
    let staging = staging_dir(owner);
    let entries = match fs::read_dir(&staging) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    let mut removed = false;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let removal = if entry.file_type()?.is_dir() {
            fs::remove_dir_all(&path)
        } else {
            fs::remove_file(&path)
        };
        match removal {
            Ok(()) => removed = true,
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) if held_open(&error) => {
                tracing::warn!(
                    path = %path.display(),
                    %error,
                    "uncommitted temp is held open; left for a later open"
                );
            }
            Err(error) => return Err(error),
        }
    }
    if removed {
        sync_dir(&staging)?;
    }
    Ok(())
}

/// Whether `error` is Windows refusing to delete a file because another
/// handle holds it: a sharing or lock violation, access denied (a handle
/// without `FILE_SHARE_DELETE`, a delete already pending), or a directory
/// left non-empty by such a member.
fn held_open(error: &io::Error) -> bool {
    const ERROR_ACCESS_DENIED: i32 = 5;
    const ERROR_SHARING_VIOLATION: i32 = 32;
    const ERROR_LOCK_VIOLATION: i32 = 33;
    const ERROR_DIR_NOT_EMPTY: i32 = 145;
    cfg!(windows)
        && matches!(
            error.raw_os_error(),
            Some(
                ERROR_ACCESS_DENIED
                    | ERROR_SHARING_VIOLATION
                    | ERROR_LOCK_VIOLATION
                    | ERROR_DIR_NOT_EMPTY
            )
        )
}

/// Create a temp for `target` in `staging` with `create`, which fails with
/// `AlreadyExists` when the name is taken. A temp is named by the process
/// id and a sequence number; a name an earlier process of the same id left
/// behind (one [`open_staging`] could not delete) is passed over for the
/// next number.
fn create_temp<T>(
    staging: &Path,
    target: &Path,
    create: impl Fn(&Path) -> io::Result<T>,
) -> Result<(PathBuf, T), AtomicWriteError> {
    let name = target
        .file_name()
        .and_then(OsStr::to_str)
        .map(|name| truncated(name, 64))
        .unwrap_or("temp");
    loop {
        let temp = staging.join(format!(
            "{}-{}-{name}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        match create(&temp) {
            Ok(created) => return Ok((temp, created)),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(source) => return Err(AtomicWriteError::Io { path: temp, source }),
        }
    }
}

/// What the target must hold right before it is replaced or removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expected {
    /// No check.
    Any,
    /// The target must not exist.
    Absent,
    /// The target's bytes must hash to this.
    Hash(ContentHash),
}

impl From<Option<ContentHash>> for Expected {
    fn from(hash: Option<ContentHash>) -> Self {
        hash.map_or(Self::Absent, Self::Hash)
    }
}

#[derive(Debug)]
pub enum AtomicWriteError {
    /// The target changed since it was read.
    Conflict {
        path: PathBuf,
    },
    Io {
        path: PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for AtomicWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict { path } => write!(
                formatter,
                "conflict: {} changed on disk since it was read",
                path.display()
            ),
            Self::Io { path, source } => write!(formatter, "{}: {source}", path.display()),
        }
    }
}

impl std::error::Error for AtomicWriteError {}

fn io_at(path: &Path) -> impl Fn(io::Error) -> AtomicWriteError + '_ {
    move |source| AtomicWriteError::Io {
        path: path.to_path_buf(),
        source,
    }
}

pub fn content_hash(bytes: &[u8]) -> ContentHash {
    ContentHash(*blake3::hash(bytes).as_bytes())
}

/// The target's current hash, `None` when it does not exist.
pub fn current_hash(path: &Path) -> Result<Option<ContentHash>, AtomicWriteError> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(content_hash(&bytes))),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(source) => Err(AtomicWriteError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Fail with [`AtomicWriteError::Conflict`] unless `path` holds `expected`.
pub fn check(path: &Path, expected: Expected) -> Result<(), AtomicWriteError> {
    let matches = match expected {
        Expected::Any => return Ok(()),
        Expected::Absent => match fs::symlink_metadata(path) {
            Ok(_) => false,
            Err(error) if error.kind() == ErrorKind::NotFound => true,
            Err(source) => {
                return Err(AtomicWriteError::Io {
                    path: path.to_path_buf(),
                    source,
                })
            }
        },
        Expected::Hash(hash) => current_hash(path)? == Some(hash),
    };
    if matches {
        Ok(())
    } else {
        Err(AtomicWriteError::Conflict {
            path: path.to_path_buf(),
        })
    }
}

/// A synced temp file in a staging directory, not yet renamed over its
/// target. Dropping it deletes the temp.
#[derive(Debug)]
pub struct Staged {
    temp: PathBuf,
    target: PathBuf,
    /// Renamed over the target: no temp is left to delete.
    renamed: bool,
}

impl Staged {
    pub fn target(&self) -> &Path {
        &self.target
    }

    /// Rename the temp over the target if the target still holds
    /// `expected` (re-checked right before the rename).
    pub fn commit(mut self, expected: Expected) -> Result<(), AtomicWriteError> {
        check(&self.target, expected)?;
        let target = self.target.clone();
        let parent = parent(&target)?;
        create_dirs(parent).map_err(io_at(parent))?;
        replace(&self.temp, &target).map_err(io_at(&target))?;
        self.renamed = true;
        sync_after_rename(parent);
        Ok(())
    }

    /// Publish the temp under a content-addressed name, never replacing
    /// what is there. Returns whether this call created it; an existing file
    /// of that name is the caller's to verify.
    pub fn commit_new(self) -> Result<bool, AtomicWriteError> {
        let parent = parent(&self.target)?;
        create_dirs(parent).map_err(io_at(parent))?;
        let created = match fs::hard_link(&self.temp, &self.target) {
            Ok(()) => true,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => false,
            Err(source) => {
                return Err(AtomicWriteError::Io {
                    path: self.target.clone(),
                    source,
                })
            }
        };
        if created {
            sync_after_rename(parent);
        }
        // `self` drops here and deletes the temp's own name.
        Ok(created)
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        if !self.renamed {
            let _ = fs::remove_file(&self.temp);
        }
    }
}

/// Stage `bytes` for `target` in `owner`'s staging directory: write the
/// temp and sync it.
pub fn stage(owner: &Path, target: &Path, bytes: &[u8]) -> Result<Staged, AtomicWriteError> {
    stage_with(owner, target, bytes, |_| Ok(()))
}

/// [`stage`], running `prepare` on the written temp before it is synced
/// (to set its permissions, say).
pub fn stage_with(
    owner: &Path,
    target: &Path,
    bytes: &[u8],
    prepare: impl FnOnce(&File) -> io::Result<()>,
) -> Result<Staged, AtomicWriteError> {
    let staging = staging_dir(owner);
    create_dirs(&staging).map_err(io_at(&staging))?;
    let (temp, mut file) = create_temp(&staging, target, |temp| {
        OpenOptions::new().write(true).create_new(true).open(temp)
    })?;
    let staged = Staged {
        temp,
        target: target.to_path_buf(),
        renamed: false,
    };
    file.write_all(bytes)
        .and_then(|()| prepare(&file))
        .and_then(|()| file.sync_all())
        .map_err(io_at(&staged.temp))?;
    drop(file);
    Ok(staged)
}

/// A directory tree built in a staging directory, published whole under a
/// content-addressed name by one rename. Dropping it unpublished deletes
/// the tree.
#[derive(Debug)]
pub struct StagedDir {
    temp: PathBuf,
    target: PathBuf,
    published: bool,
}

/// Create an empty directory in `owner`'s staging directory, to become
/// `target` when published.
pub fn stage_dir(owner: &Path, target: &Path) -> Result<StagedDir, AtomicWriteError> {
    let staging = staging_dir(owner);
    create_dirs(&staging).map_err(io_at(&staging))?;
    let (temp, ()) = create_temp(&staging, target, |temp| fs::create_dir(temp))?;
    Ok(StagedDir {
        temp,
        target: target.to_path_buf(),
        published: false,
    })
}

impl StagedDir {
    /// Where the tree is built.
    pub fn path(&self) -> &Path {
        &self.temp
    }

    pub fn target(&self) -> &Path {
        &self.target
    }

    /// Write one member file of the tree, synced, with `prepare` run on it
    /// first (to set its permissions, say); its parent directories are
    /// created.
    pub fn write_member(
        &self,
        relative: &Path,
        bytes: &[u8],
        prepare: impl FnOnce(&File) -> io::Result<()>,
    ) -> Result<(), AtomicWriteError> {
        let path = self.temp.join(relative);
        let parent = parent(&path)?;
        create_dirs(parent).map_err(io_at(parent))?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(io_at(&path))?;
        file.write_all(bytes)
            .and_then(|()| prepare(&file))
            .and_then(|()| file.sync_all())
            .map_err(io_at(&path))?;
        sync_dir(parent).map_err(io_at(parent))
    }

    /// Publish the tree under its content-addressed target, never replacing
    /// what is there: one rename. Returns whether this call created it; an
    /// existing target is the caller's to verify, and this tree is deleted.
    pub fn publish_new(mut self) -> Result<bool, AtomicWriteError> {
        let target = self.target.clone();
        let parent = parent(&target)?;
        create_dirs(parent).map_err(io_at(parent))?;
        if fs::symlink_metadata(&target).is_ok() {
            return Ok(false);
        }
        match fs::rename(&self.temp, &target) {
            Ok(()) => {
                self.published = true;
                sync_after_rename(parent);
                Ok(true)
            }
            // Another writer published the same tree first: a rename onto
            // a non-empty directory fails.
            Err(_) if fs::symlink_metadata(&target).is_ok() => Ok(false),
            Err(source) => Err(AtomicWriteError::Io {
                path: target,
                source,
            }),
        }
    }
}

impl Drop for StagedDir {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_dir_all(&self.temp);
        }
    }
}

/// Replace `target` with `bytes` if it still holds `expected`.
pub fn write(
    owner: &Path,
    target: &Path,
    bytes: &[u8],
    expected: Expected,
) -> Result<(), AtomicWriteError> {
    stage(owner, target, bytes)?.commit(expected)
}

/// Remove `target` if it still holds `expected`. A missing target is not
/// an error.
pub fn remove(target: &Path, expected: Expected) -> Result<(), AtomicWriteError> {
    check(target, expected)?;
    match fs::remove_file(target) {
        Ok(()) => {
            sync_after_rename(parent(target)?);
            Ok(())
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(source) => Err(AtomicWriteError::Io {
            path: target.to_path_buf(),
            source,
        }),
    }
}

/// Move the file at `from`, which must hold `expected`, to `to`, which must
/// not exist: one rename, so the file is never at both paths or at neither.
pub fn move_file(from: &Path, expected: Expected, to: &Path) -> Result<(), AtomicWriteError> {
    check(from, expected)?;
    check(to, Expected::Absent)?;
    let to_parent = parent(to)?;
    create_dirs(to_parent).map_err(io_at(to_parent))?;
    replace(from, to).map_err(io_at(to))?;
    sync_after_rename(to_parent);
    if let Ok(from_parent) = parent(from) {
        if from_parent != to_parent {
            sync_after_rename(from_parent);
        }
    }
    Ok(())
}

fn parent(path: &Path) -> Result<&Path, AtomicWriteError> {
    path.parent().ok_or_else(|| AtomicWriteError::Io {
        path: path.to_path_buf(),
        source: io::Error::new(ErrorKind::InvalidInput, "path has no parent directory"),
    })
}

fn truncated(name: &str, max: usize) -> &str {
    if name.len() <= max {
        return name;
    }
    let mut end = max;
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    &name[..end]
}

/// Create `directory` and its missing ancestors, syncing each parent a
/// directory was created in.
fn create_dirs(directory: &Path) -> io::Result<()> {
    match fs::symlink_metadata(directory) {
        Ok(metadata) if metadata.is_dir() => return Ok(()),
        Ok(_) => return Err(io::Error::new(ErrorKind::AlreadyExists, "not a directory")),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let parent = directory
        .parent()
        .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "no existing ancestor"))?;
    create_dirs(parent)?;
    match fs::create_dir(directory) {
        Ok(()) => sync_dir(parent),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

/// After a rename the change is visible: a failed directory sync is logged,
/// not returned (see the module docs).
fn sync_after_rename(directory: &Path) {
    if let Err(error) = sync_dir(directory) {
        tracing::error!(
            directory = %directory.display(),
            %error,
            "directory sync after a rename failed; the change is visible but may not survive a power loss"
        );
    }
}

#[cfg(not(windows))]
fn replace(from: &Path, to: &Path) -> io::Result<()> {
    fs::rename(from, to)
}

/// `MoveFileExW` with `MOVEFILE_REPLACE_EXISTING`: an atomic replace on one
/// volume. Like a Unix rename it is durable only once its directory is
/// synced ([`sync_dir`]): `MOVEFILE_WRITE_THROUGH` waits only for the copy
/// of a move across volumes, which this never makes.
///
/// `MoveFileExW` refuses (`ERROR_ACCESS_DENIED`) to replace a target any
/// process holds open, even one opened with `FILE_SHARE_DELETE` (a reader
/// of the old bytes, the scanner hashing it). [`fs::rename`] then retries
/// as a POSIX-semantics rename (`FileRenameInfoEx`), which replaces the
/// name while such handles go on reading the old file. A holder without
/// `FILE_SHARE_DELETE` still blocks it: the call fails and the target is
/// unchanged.
#[cfg(windows)]
fn replace(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_REPLACE_EXISTING};
    let wide = |path: &Path| {
        path.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<u16>>()
    };
    let (from_wide, to_wide) = (wide(from), wide(to));
    // SAFETY: both arguments are NUL-terminated UTF-16 strings that outlive
    // the call.
    let moved =
        unsafe { MoveFileExW(from_wide.as_ptr(), to_wide.as_ptr(), MOVEFILE_REPLACE_EXISTING) };
    if moved != 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    const ERROR_ACCESS_DENIED: i32 = 5;
    if error.raw_os_error() == Some(ERROR_ACCESS_DENIED) {
        fs::rename(from, to)
    } else {
        Err(error)
    }
}

/// `FlushFileBuffers` on the directory, which writes its entries to disk.
/// Opening a directory takes `FILE_FLAG_BACKUP_SEMANTICS`, which needs no
/// privilege; the flush takes `FILE_WRITE_DATA` (`FILE_ADD_FILE` for a
/// directory), the right a rename into the directory already needed.
#[cfg(windows)]
fn sync_dir(path: &Path) -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, FILE_WRITE_DATA};
    OpenOptions::new()
        .access_mode(FILE_WRITE_DATA)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?
        .sync_all()
}

#[cfg(not(windows))]
fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(directory: &Path) -> Vec<String> {
        let mut names = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[test]
    fn replaces_creates_and_removes_with_a_preimage_check() {
        let temp = tempfile::tempdir().unwrap();
        let owner = temp.path();
        let target = owner.join("nested/file.bundle");
        write(owner, &target, b"one", Expected::Absent).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"one");
        assert!(matches!(
            write(owner, &target, b"two", Expected::Absent),
            Err(AtomicWriteError::Conflict { .. })
        ));
        assert!(matches!(
            write(owner, &target, b"two", Expected::Hash(content_hash(b"zzz"))),
            Err(AtomicWriteError::Conflict { .. })
        ));
        write(owner, &target, b"two", Expected::Hash(content_hash(b"one"))).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"two");
        assert!(matches!(
            remove(&target, Expected::Hash(content_hash(b"one"))),
            Err(AtomicWriteError::Conflict { .. })
        ));
        remove(&target, Expected::Hash(content_hash(b"two"))).unwrap();
        assert!(!target.exists());
        // Nothing is left behind, not even a temp.
        assert!(names(&owner.join("nested")).is_empty());
        assert!(names(&staging_dir(owner)).is_empty());
    }

    /// A crash after the temp is written and synced, before the rename:
    /// the target is untouched, the temp is only in the staging directory,
    /// and opening the tree deletes it.
    #[test]
    fn a_crash_between_the_temp_and_the_rename_leaves_the_target_and_only_a_staged_temp() {
        let temp = tempfile::tempdir().unwrap();
        let owner = temp.path();
        let target = owner.join("file.bundle");
        write(owner, &target, b"old", Expected::Absent).unwrap();

        let staged = stage(owner, &target, b"new").unwrap();
        // The process dies here: no rename, no cleanup.
        std::mem::forget(staged);
        assert_eq!(fs::read(&target).unwrap(), b"old");
        assert_eq!(names(owner), [STAGING_DIR, "file.bundle"]);
        assert_eq!(names(&staging_dir(owner)).len(), 1);

        open_staging(owner).unwrap();
        assert!(names(&staging_dir(owner)).is_empty());
        assert_eq!(fs::read(&target).unwrap(), b"old");
    }

    #[test]
    fn a_failed_commit_leaves_the_target_and_no_temp() {
        let temp = tempfile::tempdir().unwrap();
        let owner = temp.path();
        let target = owner.join("file.bundle");
        write(owner, &target, b"old", Expected::Absent).unwrap();
        let staged = stage(owner, &target, b"new").unwrap();
        fs::write(&target, b"edited").unwrap();
        assert!(matches!(
            staged.commit(Expected::Hash(content_hash(b"old"))),
            Err(AtomicWriteError::Conflict { .. })
        ));
        assert_eq!(fs::read(&target).unwrap(), b"edited");
        assert!(names(&staging_dir(owner)).is_empty());
    }

    #[test]
    fn content_addressed_publication_never_replaces() {
        let temp = tempfile::tempdir().unwrap();
        let owner = temp.path();
        let target = owner.join("objects/abc");
        assert!(stage(owner, &target, b"bytes")
            .unwrap()
            .commit_new()
            .unwrap());
        assert!(!stage(owner, &target, b"bytes")
            .unwrap()
            .commit_new()
            .unwrap());
        assert_eq!(fs::read(&target).unwrap(), b"bytes");
        assert!(names(&staging_dir(owner)).is_empty());
    }

    #[test]
    fn a_move_never_replaces_its_destination() {
        let temp = tempfile::tempdir().unwrap();
        let owner = temp.path();
        let from = owner.join("a.bundle");
        let to = owner.join("dir/b.bundle");
        write(owner, &from, b"a", Expected::Absent).unwrap();
        move_file(&from, Expected::Hash(content_hash(b"a")), &to).unwrap();
        assert!(!from.exists());
        assert_eq!(fs::read(&to).unwrap(), b"a");
        write(owner, &from, b"c", Expected::Absent).unwrap();
        assert!(matches!(
            move_file(&from, Expected::Any, &to),
            Err(AtomicWriteError::Conflict { .. })
        ));
        assert_eq!(fs::read(&to).unwrap(), b"a");
        assert_eq!(fs::read(&from).unwrap(), b"c");
    }

    /// A reader holding the target open (with `FILE_SHARE_DELETE`, as std
    /// opens files) does not block the replace, even on Windows, and goes
    /// on reading the old bytes.
    #[test]
    fn a_target_held_open_is_replaced_and_the_holder_reads_the_old_bytes() {
        use std::io::Read;

        let temp = tempfile::tempdir().unwrap();
        let owner = temp.path();
        let target = owner.join("file.bundle");
        write(owner, &target, b"old", Expected::Absent).unwrap();
        let mut held = File::open(&target).unwrap();
        write(owner, &target, b"new", Expected::Hash(content_hash(b"old"))).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"new");
        let mut read = Vec::new();
        held.read_to_end(&mut read).unwrap();
        assert_eq!(read, b"old");
        assert!(names(&staging_dir(owner)).is_empty());
    }

    #[test]
    fn staging_paths_are_recognised() {
        assert!(in_staging(Path::new("/root/.distill-staging/1-2-a.bundle")));
        assert!(!in_staging(Path::new("/root/a.bundle")));
        assert!(is_staging_name(OsStr::new(STAGING_DIR)));
    }

    /// Temps an earlier process of this process's id left behind (which
    /// `open_staging` could not delete) are passed over, not reused.
    #[test]
    fn a_leftover_temp_of_the_same_process_id_is_never_reused() {
        let temp = tempfile::tempdir().unwrap();
        let owner = temp.path();
        let target = owner.join("file.bundle");
        fs::create_dir(staging_dir(owner)).unwrap();
        let next = TEMP_SEQUENCE.load(Ordering::Relaxed);
        let leftovers = (next..next + 64)
            .map(|sequence| format!("{}-{sequence}-file.bundle", std::process::id()))
            .collect::<Vec<_>>();
        for name in &leftovers {
            fs::write(staging_dir(owner).join(name), b"leftover").unwrap();
        }
        write(owner, &target, b"new", Expected::Absent).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"new");
        let mut expected = leftovers;
        expected.sort();
        assert_eq!(names(&staging_dir(owner)), expected);
        for name in &expected {
            assert_eq!(fs::read(staging_dir(owner).join(name)).unwrap(), b"leftover");
        }
    }

    /// A temp another process holds open without `FILE_SHARE_DELETE`
    /// cannot be deleted: opening the tree leaves it, and writes go on.
    #[cfg(windows)]
    #[test]
    fn opening_the_tree_leaves_a_temp_held_open() {
        use std::os::windows::fs::OpenOptionsExt;

        let temp = tempfile::tempdir().unwrap();
        let owner = temp.path();
        let target = owner.join("file.bundle");
        std::mem::forget(stage(owner, &target, b"uncommitted").unwrap());
        let held_name = names(&staging_dir(owner)).remove(0);
        let held = OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(staging_dir(owner).join(&held_name))
            .unwrap();
        open_staging(owner).unwrap();
        assert_eq!(names(&staging_dir(owner)), [held_name.clone()]);
        write(owner, &target, b"new", Expected::Absent).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"new");
        drop(held);
        open_staging(owner).unwrap();
        assert!(names(&staging_dir(owner)).is_empty());
    }
}
