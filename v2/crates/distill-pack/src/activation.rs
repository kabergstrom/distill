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
