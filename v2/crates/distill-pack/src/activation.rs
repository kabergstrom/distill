//! Reading the durable `pack.current` activation (§16); `distill-pack-build`
//! publishes and activates.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

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
