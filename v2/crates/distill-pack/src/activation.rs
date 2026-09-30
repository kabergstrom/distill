//! Durable `pack.current` activation (§16).

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub enum PointerError {
    Io(io::Error),
    Grammar,
    InvalidPack,
    ImmutableConflict(PathBuf),
}

impl From<io::Error> for PointerError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

pub fn pointer_bytes(hash: [u8; 32]) -> [u8; 65] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0u8; 65];
    for (i, byte) in hash.into_iter().enumerate() {
        out[i * 2] = HEX[(byte >> 4) as usize];
        out[i * 2 + 1] = HEX[(byte & 0x0f) as usize];
    }
    out[64] = b'\n';
    out
}

pub fn parse_pointer(bytes: &[u8]) -> Result<[u8; 32], PointerError> {
    if bytes.len() != 65 || bytes[64] != b'\n' {
        return Err(PointerError::Grammar);
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        let high = lower_hex(bytes[i * 2]).ok_or(PointerError::Grammar)?;
        let low = lower_hex(bytes[i * 2 + 1]).ok_or(PointerError::Grammar)?;
        out[i] = (high << 4) | low;
    }
    Ok(out)
}

fn lower_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

pub fn read_current(directory: &Path) -> Result<[u8; 32], PointerError> {
    parse_pointer(&fs::read(directory.join("pack.current"))?)
}

pub fn manifest_filename(hash: [u8; 32]) -> String {
    let pointer = pointer_bytes(hash);
    format!(
        "manifest-{}.dpk",
        std::str::from_utf8(&pointer[..64]).expect("hex")
    )
}

pub fn archive_filename(file_hash: [u8; 32]) -> String {
    let pointer = pointer_bytes(file_hash);
    format!(
        "archive-{}.dpk",
        std::str::from_utf8(&pointer[..64]).expect("hex")
    )
}

/// Publish an authenticated archive under its immutable content-addressed
/// physical name. A temporary file is fsynced, then hard-linked into place:
/// link creation is atomic and no-replace on the same filesystem. An existing
/// equal file is idempotent; different bytes under the same name are fatal.
pub fn publish_archive(directory: &Path, bytes: &[u8]) -> Result<[u8; 32], PointerError> {
    crate::archive::validate_archive(bytes).map_err(|_| PointerError::InvalidPack)?;
    let hash = *blake3::hash(bytes).as_bytes();
    publish_immutable(directory, &archive_filename(hash), bytes)?;
    Ok(hash)
}

pub fn publish_manifest(directory: &Path, bytes: &[u8]) -> Result<[u8; 32], PointerError> {
    crate::manifest::decode_manifest(bytes).map_err(|_| PointerError::InvalidPack)?;
    let hash = crate::manifest::manifest_hash(bytes);
    publish_immutable(directory, &manifest_filename(hash), bytes)?;
    Ok(hash)
}

fn publish_immutable(directory: &Path, name: &str, bytes: &[u8]) -> Result<(), PointerError> {
    let destination = directory.join(name);
    if destination.exists() {
        return verify_existing(&destination, bytes);
    }
    let temp = unique_named_temp(directory, name);
    let result = (|| -> Result<(), PointerError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        match fs::hard_link(&temp, &destination) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                verify_existing(&destination, bytes)?;
            }
            Err(error) => return Err(PointerError::Io(error)),
        }
        sync_directory(directory)?;
        fs::remove_file(&temp)?;
        sync_directory(directory)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// fsync a directory; a no-op on Windows, which has no directory fsync
/// (NTFS journals directory entries).
fn sync_directory(directory: &Path) -> io::Result<()> {
    if cfg!(windows) {
        return Ok(());
    }
    File::open(directory)?.sync_all()
}

fn verify_existing(path: &Path, bytes: &[u8]) -> Result<(), PointerError> {
    if fs::read(path)? == bytes {
        Ok(())
    } else {
        Err(PointerError::ImmutableConflict(path.to_owned()))
    }
}

/// The manifest/archives must already have been fsynced. This performs the
/// final no-replace-temp → file-fsync → rename → directory-fsync sequence.
pub fn activate(directory: &Path, hash: [u8; 32]) -> Result<(), PointerError> {
    let temp = unique_temp(directory);
    let result = (|| -> Result<(), PointerError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(&pointer_bytes(hash))?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp, directory.join("pack.current"))?;
        sync_directory(directory)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn unique_temp(directory: &Path) -> PathBuf {
    let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
    directory.join(format!(".pack.current.{}.{}.tmp", std::process::id(), id))
}

fn unique_named_temp(directory: &Path, name: &str) -> PathBuf {
    let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
    directory.join(format!(".{name}.{}.{}.tmp", std::process::id(), id))
}
