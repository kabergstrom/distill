//! Durable `pack.current` activation (§16).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use distill_store::atomic_file::{self, AtomicWriteError, Expected};

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

impl From<AtomicWriteError> for PointerError {
    fn from(value: AtomicWriteError) -> Self {
        match value {
            AtomicWriteError::Io { source, .. } => Self::Io(source),
            AtomicWriteError::Conflict { path } => Self::ImmutableConflict(path),
        }
    }
}

/// `pack.current`: the manifest hash as 32 lowercase hex chars and a newline.
pub fn pointer_bytes(hash: [u8; 16]) -> [u8; 33] {
    let mut out = [b'\n'; 33];
    out[..32].copy_from_slice(distill_core::hash::to_hex(&hash).as_bytes());
    out
}

pub fn parse_pointer(bytes: &[u8]) -> Result<[u8; 16], PointerError> {
    if bytes.len() != 33 || bytes[32] != b'\n' {
        return Err(PointerError::Grammar);
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
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

pub fn read_current(directory: &Path) -> Result<[u8; 16], PointerError> {
    parse_pointer(&fs::read(directory.join("pack.current"))?)
}

pub fn manifest_filename(hash: [u8; 16]) -> String {
    let pointer = pointer_bytes(hash);
    format!(
        "manifest-{}.dpk",
        std::str::from_utf8(&pointer[..32]).expect("hex")
    )
}

pub fn archive_filename(file_hash: [u8; 16]) -> String {
    let pointer = pointer_bytes(file_hash);
    format!(
        "archive-{}.dpk",
        std::str::from_utf8(&pointer[..32]).expect("hex")
    )
}

/// Publish an authenticated archive under its immutable content-addressed
/// physical name: an fsynced temp in the directory's staging directory is
/// hard-linked into place, which is atomic and no-replace. An existing
/// equal file is idempotent; different bytes under the same name are fatal.
pub fn publish_archive(directory: &Path, bytes: &[u8]) -> Result<[u8; 16], PointerError> {
    crate::archive::validate_archive(bytes).map_err(|_| PointerError::InvalidPack)?;
    let hash = distill_core::hash::hash(bytes);
    publish_immutable(directory, &archive_filename(hash), bytes)?;
    Ok(hash)
}

pub fn publish_manifest(directory: &Path, bytes: &[u8]) -> Result<[u8; 16], PointerError> {
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
    if !atomic_file::stage(directory, &destination, bytes)?.commit_new()? {
        verify_existing(&destination, bytes)?;
    }
    Ok(())
}

fn verify_existing(path: &Path, bytes: &[u8]) -> Result<(), PointerError> {
    if fs::read(path)? == bytes {
        Ok(())
    } else {
        Err(PointerError::ImmutableConflict(path.to_owned()))
    }
}

/// Empty `directory`'s staging directory: the temps an earlier pack command
/// left there never committed. Call it before publishing into `directory`.
pub fn open_pack_directory(directory: &Path) -> Result<(), PointerError> {
    atomic_file::open_staging(directory)?;
    Ok(())
}

/// The manifest/archives must already have been published. This atomically
/// replaces `pack.current` (fsynced temp, rename, directory fsync).
pub fn activate(directory: &Path, hash: [u8; 16]) -> Result<(), PointerError> {
    atomic_file::write(
        directory,
        &directory.join("pack.current"),
        &pointer_bytes(hash),
        Expected::Any,
    )?;
    Ok(())
}
