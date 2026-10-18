//! Publishing a pack (§16): content-addressed archives and manifests, and
//! the durable `pack.current` activation.

use std::fs;
use std::path::Path;

use distill_pack::{archive_filename, manifest_filename, pointer_bytes, PointerError};
use distill_store::atomic_file::{self, AtomicWriteError, Expected};

fn pointer_error(error: AtomicWriteError) -> PointerError {
    match error {
        AtomicWriteError::Io { source, .. } => PointerError::Io(source),
        AtomicWriteError::Conflict { path } => PointerError::ImmutableConflict(path),
    }
}

/// Publish an authenticated archive under its immutable content-addressed
/// physical name: an fsynced temp in the directory's staging directory is
/// hard-linked into place, which is atomic and no-replace. An existing
/// equal file is idempotent; different bytes under the same name are fatal.
pub fn publish_archive(directory: &Path, bytes: &[u8]) -> Result<[u8; 16], PointerError> {
    distill_pack::validate_archive(bytes).map_err(|_| PointerError::InvalidPack)?;
    let hash = distill_core::hash::hash(bytes);
    publish_immutable(directory, &archive_filename(hash), bytes)?;
    Ok(hash)
}

pub fn publish_manifest(directory: &Path, bytes: &[u8]) -> Result<[u8; 16], PointerError> {
    distill_pack::decode_manifest(bytes).map_err(|_| PointerError::InvalidPack)?;
    let hash = distill_pack::manifest_hash(bytes);
    publish_immutable(directory, &manifest_filename(hash), bytes)?;
    Ok(hash)
}

fn publish_immutable(directory: &Path, name: &str, bytes: &[u8]) -> Result<(), PointerError> {
    let destination = directory.join(name);
    if destination.exists() {
        return verify_existing(&destination, bytes);
    }
    if !atomic_file::stage(directory, &destination, bytes)
        .and_then(|staged| staged.commit_new())
        .map_err(pointer_error)? {
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
    )
    .map_err(pointer_error)?;
    Ok(())
}
