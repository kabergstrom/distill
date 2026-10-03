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
/// physical name: an fsynced temp in the directory's staging directory is
/// hard-linked into place, which is atomic and no-replace. An existing
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
pub fn activate(directory: &Path, hash: [u8; 32]) -> Result<(), PointerError> {
    atomic_file::write(
        directory,
        &directory.join("pack.current"),
        &pointer_bytes(hash),
        Expected::Any,
    )?;
    Ok(())
}
