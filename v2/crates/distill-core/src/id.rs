//! Identity newtypes (§7). UUIDs display and parse in RFC 4122 hyphenated
//! form; hashes as 64 lowercase hex chars. Parsing rejects any other shape —
//! never truncates, never guesses.

use std::fmt;
use std::str::FromStr;

/// Error parsing an id or hash from text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdParseError;

impl fmt::Display for IdParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("malformed id: expected RFC 4122 hyphenated uuid or 64-char hex hash")
    }
}

impl std::error::Error for IdParseError {}

fn hex_val(c: u8) -> Result<u8, IdParseError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(IdParseError),
    }
}

fn parse_uuid(s: &str) -> Result<[u8; 16], IdParseError> {
    let b = s.as_bytes();
    if b.len() != 36 {
        return Err(IdParseError);
    }
    let mut out = [0u8; 16];
    let mut oi = 0;
    let mut i = 0;
    while i < 36 {
        if matches!(i, 8 | 13 | 18 | 23) {
            if b[i] != b'-' {
                return Err(IdParseError);
            }
            i += 1;
            continue;
        }
        let hi = hex_val(b[i])?;
        let lo = hex_val(b[i + 1])?;
        out[oi] = (hi << 4) | lo;
        oi += 1;
        i += 2;
    }
    Ok(out)
}

fn fmt_uuid(bytes: &[u8; 16], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    for (i, byte) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            f.write_str("-")?;
        }
        write!(f, "{:02x}", byte)?;
    }
    Ok(())
}

macro_rules! uuid_newtype {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub [u8; 16]);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt_uuid(&self.0, f)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self)
            }
        }

        impl FromStr for $name {
            type Err = IdParseError;
            fn from_str(s: &str) -> Result<Self, IdParseError> {
                parse_uuid(s).map(Self)
            }
        }

        id_serde!($name);
    };
}

/// Serde as the canonical text form (hyphenated uuid / 64-hex) — schema
/// JSON interchange wants readable ids, and the strict FromStr does the
/// validation.
#[cfg(feature = "serde")]
macro_rules! id_serde {
    ($name:ident) => {
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(self)
            }
        }
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let text = <std::borrow::Cow<'de, str>>::deserialize(d)?;
                text.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}
#[cfg(not(feature = "serde"))]
macro_rules! id_serde {
    ($name:ident) => {};
}

uuid_newtype! {
    /// A single asset's stable identity, explicit in its bundle (§7).
    AssetUuid
}
uuid_newtype! {
    /// A bundle file's own stable identity (§6).
    BundleUuid
}
uuid_newtype! {
    /// An asset type's stable identity — `#[asset(uuid = …)]` (§4, §5).
    TypeUuid
}

impl AssetUuid {
    /// RFC 4122 §4.3 name-based UUID (version 5, SHA-1): how derived
    /// outputs get their identity — `UUIDv5(parent uuid, output key)` (§9).
    pub fn v5(namespace: AssetUuid, name: &str) -> AssetUuid {
        let mut hasher = sha1_smol::Sha1::new();
        hasher.update(&namespace.0);
        hasher.update(name.as_bytes());
        let digest = hasher.digest().bytes();
        let mut out = [0u8; 16];
        out.copy_from_slice(&digest[..16]);
        out[6] = (out[6] & 0x0F) | 0x50; // version 5
        out[8] = (out[8] & 0x3F) | 0x80; // RFC 4122 variant
        AssetUuid(out)
    }
}

macro_rules! hash_newtype {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub [u8; 32]);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                for byte in &self.0 {
                    write!(f, "{:02x}", byte)?;
                }
                Ok(())
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self)
            }
        }

        impl FromStr for $name {
            type Err = IdParseError;
            fn from_str(s: &str) -> Result<Self, IdParseError> {
                let b = s.as_bytes();
                if b.len() != 64 {
                    return Err(IdParseError);
                }
                let mut out = [0u8; 32];
                for (i, chunk) in b.chunks_exact(2).enumerate() {
                    out[i] = (hex_val(chunk[0])? << 4) | hex_val(chunk[1])?;
                }
                Ok(Self(out))
            }
        }

        id_serde!($name);
    };
}

hash_newtype! {
    /// blake3 of an artifact's bytes — content identity: what traces
    /// record, clients fetch, and packs ship (§9, §12).
    ContentHash
}
hash_newtype! {
    /// Raw blake3 identity of the exact observed bundle-file bytes, whether
    /// valid or malformed. Parsing/canonicality is an independent gate (§6).
    BundleFileHash
}

impl BundleFileHash {
    pub fn of_observed_bytes(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }
}
hash_newtype! {
    /// blake3 of the DSLH encoding of a type's logical schema (§5).
    LogicalHash
}
hash_newtype! {
    /// blake3 of the DSWL encoding of a type's wire layout tree (§12).
    LayoutHash
}

/// The reserved rules-bundle UUID of the asset root `root`'s default import
/// layer (§8 "Default imports"): what a default import's `DirectoryOrigin`
/// names as its rules bundle. Derived from the root's name, never stored; no
/// bundle file may claim it.
pub fn default_rules_bundle(root: &str) -> BundleUuid {
    let digest = crate::canonical::domain_digest(*b"DSDR", 1, |encoder| encoder.str(root));
    let mut uuid = [0; 16];
    uuid.copy_from_slice(&digest[..16]);
    BundleUuid(uuid)
}
