//! The shared file header (§5): every standalone binary file starts with
//! 4 magic bytes `NGPF`, the `u32` [`Domain`] discriminant naming its
//! format, and the `u32` version of that format, little-endian. Artifact
//! images, pack archives, pack manifests, bundle containers and schema
//! files share it; one reader refuses a wrong magic, domain or version by
//! name.

use std::fmt;

use crate::hash::Domain;

pub const MAGIC: [u8; 4] = *b"NGPF";

/// Header length in bytes.
pub const HEADER_LEN: usize = 12;

/// The header of a `domain` file at `version`.
pub fn header(domain: Domain, version: u32) -> [u8; HEADER_LEN] {
    let mut out = [0; HEADER_LEN];
    out[..4].copy_from_slice(&MAGIC);
    out[4..8].copy_from_slice(&(domain as u32).to_le_bytes());
    out[8..].copy_from_slice(&version.to_le_bytes());
    out
}

/// Append the header of a `domain` file at `version` to `out`.
pub fn write_header(out: &mut Vec<u8>, domain: Domain, version: u32) {
    out.extend_from_slice(&header(domain, version));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderError {
    /// Fewer than [`HEADER_LEN`] bytes.
    TooShort { len: usize },
    /// Not `NGPF`.
    BadMagic { found: [u8; 4] },
    /// Another format's file (`found`: the raw discriminant, which may name
    /// no known domain).
    WrongDomain { expected: Domain, found: u32 },
    /// The right format at a version this reader does not read.
    UnsupportedVersion {
        domain: Domain,
        expected: u32,
        found: u32,
    },
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HeaderError::TooShort { len } => {
                write!(f, "file of {len} bytes is shorter than its header")
            }
            HeaderError::BadMagic { found } => {
                write!(f, "bad file magic {found:02x?}, expected \"NGPF\"")
            }
            HeaderError::WrongDomain { expected, found } => match Domain::from_u32(*found) {
                Some(domain) => write!(f, "a {domain:?} file where a {expected:?} file belongs"),
                None => write!(f, "unknown file domain {found} where a {expected:?} file belongs"),
            },
            HeaderError::UnsupportedVersion {
                domain,
                expected,
                found,
            } => write!(
                f,
                "{domain:?} file version {found} is not the supported version {expected}"
            ),
        }
    }
}

impl std::error::Error for HeaderError {}

/// Check `bytes` start with the header of a `domain` file at `version` and
/// return what follows it.
pub fn read_header(bytes: &[u8], domain: Domain, version: u32) -> Result<&[u8], HeaderError> {
    if bytes.len() < HEADER_LEN {
        return Err(HeaderError::TooShort { len: bytes.len() });
    }
    let found: [u8; 4] = bytes[..4].try_into().expect("four bytes");
    if found != MAGIC {
        return Err(HeaderError::BadMagic { found });
    }
    let found_domain = u32::from_le_bytes(bytes[4..8].try_into().expect("four bytes"));
    if found_domain != domain as u32 {
        return Err(HeaderError::WrongDomain {
            expected: domain,
            found: found_domain,
        });
    }
    let found_version = u32::from_le_bytes(bytes[8..12].try_into().expect("four bytes"));
    if found_version != version {
        return Err(HeaderError::UnsupportedVersion {
            domain,
            expected: version,
            found: found_version,
        });
    }
    Ok(&bytes[HEADER_LEN..])
}

/// Whether `bytes` start with the magic (a header of any domain).
pub fn has_magic(bytes: &[u8]) -> bool {
    bytes.starts_with(&MAGIC)
}
