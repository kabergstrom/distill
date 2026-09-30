//! Crash-safe file publication into asset roots and codegen output.
//!
//! A write goes to a temp file in the target's directory, is synced, renamed
//! over the target, and the directory is synced. A delete is a plain
//! `remove_file` plus the directory sync. The file changes first and the
//! store rows follow; a crash between the two is healed by the next scan.

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use distill_core::id::ContentHash;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

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
    Conflict { path: PathBuf },
    Io { path: PathBuf, source: std::io::Error },
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

pub fn content_hash(bytes: &[u8]) -> ContentHash {
    ContentHash(*blake3::hash(bytes).as_bytes())
}

/// Replace `path` with `bytes`.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), AtomicWriteError> {
    atomic_write_expecting(path, bytes, Expected::Any)
}

/// Replace `path` with `bytes` if it still holds `expected`. The target is
/// re-hashed right before the rename.
pub fn atomic_write_expecting(
    path: &Path,
    bytes: &[u8],
    expected: Expected,
) -> Result<(), AtomicWriteError> {
    let io = |source| AtomicWriteError::Io {
        path: path.to_path_buf(),
        source,
    };
    let parent = parent(path)?;
    fs::create_dir_all(parent).map_err(io)?;
    let temp = temp_path(path).map_err(io)?;
    let written = write_temp(&temp, bytes)
        .and_then(|()| check(path, expected))
        .and_then(|()| fs::rename(&temp, path).map_err(io))
        .and_then(|()| sync_dir(parent));
    if written.is_err() {
        let _ = fs::remove_file(&temp);
    }
    written
}

/// Remove `path` if it still holds `expected`. A missing target is not an
/// error.
pub fn remove_expecting(path: &Path, expected: Expected) -> Result<(), AtomicWriteError> {
    check(path, expected)?;
    match fs::remove_file(path) {
        Ok(()) => sync_dir(parent(path)?),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(source) => Err(AtomicWriteError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
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

fn check(path: &Path, expected: Expected) -> Result<(), AtomicWriteError> {
    let matches = match expected {
        Expected::Any => return Ok(()),
        Expected::Absent => current_hash(path)?.is_none(),
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

fn parent(path: &Path) -> Result<&Path, AtomicWriteError> {
    path.parent().ok_or_else(|| AtomicWriteError::Io {
        path: path.to_path_buf(),
        source: std::io::Error::new(ErrorKind::InvalidInput, "path has no parent directory"),
    })
}

fn temp_path(target: &Path) -> std::io::Result<PathBuf> {
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file");
    for _ in 0..64 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temp = target.with_file_name(format!(
            ".{name}.distill-{}-{sequence}.tmp",
            std::process::id()
        ));
        match fs::symlink_metadata(&temp) {
            Ok(_) => continue,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(temp),
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::new(
        ErrorKind::AlreadyExists,
        "could not allocate a unique temp file",
    ))
}

fn write_temp(temp: &Path, bytes: &[u8]) -> Result<(), AtomicWriteError> {
    let io = |source| AtomicWriteError::Io {
        path: temp.to_path_buf(),
        source,
    };
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp)
        .map_err(io)?;
    file.write_all(bytes).map_err(io)?;
    file.sync_all().map_err(io)
}

fn sync_dir(path: &Path) -> Result<(), AtomicWriteError> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| AtomicWriteError::Io {
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_creates_and_removes_with_a_preimage_check() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("nested/file.bundle");
        atomic_write_expecting(&target, b"one", Expected::Absent).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"one");
        assert!(matches!(
            atomic_write_expecting(&target, b"two", Expected::Absent),
            Err(AtomicWriteError::Conflict { .. })
        ));
        assert!(matches!(
            atomic_write_expecting(&target, b"two", Expected::Hash(content_hash(b"zzz"))),
            Err(AtomicWriteError::Conflict { .. })
        ));
        atomic_write_expecting(&target, b"two", Expected::Hash(content_hash(b"one"))).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"two");
        assert!(matches!(
            remove_expecting(&target, Expected::Hash(content_hash(b"one"))),
            Err(AtomicWriteError::Conflict { .. })
        ));
        remove_expecting(&target, Expected::Hash(content_hash(b"two"))).unwrap();
        assert!(!target.exists());
        // Nothing is left behind, not even a temp.
        assert_eq!(fs::read_dir(target.parent().unwrap()).unwrap().count(), 0);
    }
}
