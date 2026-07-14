//! §5 canonical record encoding: the one byte encoding for every composite
//! this project hashes or stores as a key, so no `‖` formula can
//! repartition variable-length fields.
//!
//! Rules (normative in DESIGN.md §5): fields in declaration order; integers
//! LE fixed-width; `str` as u32 byte length + NFC UTF-8 bytes; fixed byte
//! arrays raw; sets sorted by encoded bytes, deduplicated, u32 count +
//! elements; sequences u32 count + elements; `Option` as a u8 presence
//! marker + payload; enums as a u8 discriminant + payload. Digests are
//! domain-prefixed and versioned: `blake3(domain ‖ version ‖ fields)`.

use unicode_normalization::UnicodeNormalization;

/// `CompilationIdentity` digest (§5).
pub const DSCI: [u8; 4] = *b"DSCI";
/// Host-interface `ModuleAbiIdentity` digest (§3, §5).
pub const DSMA: [u8; 4] = *b"DSMA";
/// Target-definition hash (§18).
pub const DSTG: [u8; 4] = *b"DSTG";
/// Candidate target-set identity (§5, §13, §18).
pub const DSTS: [u8; 4] = *b"DSTS";
/// Tag-annotation epoch — the exact compiled `#[asset(tag)]` projection
/// (§5, §10).
pub const DSTA: [u8; 4] = *b"DSTA";
/// `StaticInputs` digest — the build-cache lookup key (§9).
pub const DSSI: [u8; 4] = *b"DSSI";
/// `TraceOp` sequences (§9).
pub const DSTR: [u8; 4] = *b"DSTR";
/// Schema-lineage chain digest — a type's ordered schema-digest history
/// (§6, §11, §13).
pub const DSSL: [u8; 4] = *b"DSSL";
/// Load-policy digest — sorted (type_uuid, build_only) pairs (§9, §13,
/// §16).
pub const DSLP: [u8; 4] = *b"DSLP";
/// Compiled full semantic attestation (§3, §5).
pub const DSCA: [u8; 4] = *b"DSCA";
/// One type's canonical RegistryExtras v1 row table (§3, §5).
pub const DSRE: [u8; 4] = *b"DSRE";
/// Typed local deterministic-failure detail (§5, §9).
pub const DSLF: [u8; 4] = *b"DSLF";
/// Typed configuration-poison reason facts (§5, §13, §17).
pub const DSCP: [u8; 4] = *b"DSCP";
/// Typed version-global poison identity (§7, §13, §17).
pub const DSVP: [u8; 4] = *b"DSVP";
/// Typed pipeline-poison identity (§3, §13, §17).
pub const DSPP: [u8; 4] = *b"DSPP";
/// Complete hermetic tool-execution capsule identity (§9, §13).
pub const DSCT: [u8; 4] = *b"DSCT";

/// Append-only encoder for the canonical record encoding. Composites encode
/// their fields in declaration order by calling these methods; there is no
/// decoder because the encoding exists to be hashed, not read back.
#[derive(Default)]
pub struct CanonicalEncoder {
    buf: Vec<u8>,
}

macro_rules! int_methods {
    ($($name:ident: $ty:ty),* $(,)?) => {
        $(pub fn $name(&mut self, v: $ty) {
            self.buf.extend_from_slice(&v.to_le_bytes());
        })*
    };
}

impl CanonicalEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    int_methods! {
        u8: u8, u16: u16, u32: u32, u64: u64, u128: u128,
        i8: i8, i16: i16, i32: i32, i64: i64, i128: i128,
    }

    pub fn bool(&mut self, v: bool) {
        self.u8(v as u8);
    }

    /// u32 byte length + NFC-normalized UTF-8 bytes.
    pub fn str(&mut self, s: &str) {
        let nfc: String = s.nfc().collect();
        let len = u32::try_from(nfc.len()).expect("canonical str exceeds u32 length");
        self.u32(len);
        self.buf.extend_from_slice(nfc.as_bytes());
    }

    /// Fixed-size byte arrays encode raw — the width is part of the record's
    /// declared shape, so no length prefix.
    pub fn raw(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// u8 presence marker (0/1) + payload when present.
    pub fn option<T>(&mut self, v: Option<T>, f: impl FnOnce(&mut Self, &T)) {
        match v {
            None => self.u8(0),
            Some(x) => {
                self.u8(1);
                f(self, &x);
            }
        }
    }

    /// u8 discriminant in declaration order; the caller encodes the payload
    /// after this call.
    pub fn enum_variant(&mut self, discriminant: u8) {
        self.u8(discriminant);
    }

    /// u32 count + elements in the given order.
    pub fn seq<T>(&mut self, items: &[T], f: impl Fn(&mut Self, &T)) {
        let len = u32::try_from(items.len()).expect("canonical seq exceeds u32 count");
        self.u32(len);
        for it in items {
            f(self, it);
        }
    }

    /// Sets: each element encoded standalone, the encodings sorted bytewise
    /// and deduplicated, then u32 (deduplicated) count + concatenation.
    pub fn set<'a, T: 'a>(
        &mut self,
        items: impl IntoIterator<Item = &'a T>,
        f: impl Fn(&mut Self, &T),
    ) {
        let mut encoded: Vec<Vec<u8>> = items
            .into_iter()
            .map(|it| {
                let mut e = Self::new();
                f(&mut e, it);
                e.into_bytes()
            })
            .collect();
        encoded.sort();
        encoded.dedup();
        let len = u32::try_from(encoded.len()).expect("canonical set exceeds u32 count");
        self.u32(len);
        for e in encoded {
            self.buf.extend_from_slice(&e);
        }
    }
}

/// `blake3(domain ‖ version ‖ canonical fields)` — the §5 digest form.
pub fn domain_digest(
    domain: [u8; 4],
    version: u8,
    f: impl FnOnce(&mut CanonicalEncoder),
) -> [u8; 32] {
    let mut e = CanonicalEncoder::new();
    f(&mut e);
    let mut hasher = blake3::Hasher::new();
    hasher.update(&domain);
    hasher.update(&[version]);
    hasher.update(&e.into_bytes());
    *hasher.finalize().as_bytes()
}
