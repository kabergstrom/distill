//! §5 the one encoding: the byte form of everything hashed, keyed, carried
//! or stored as binary data (fixed structs, schemas, layouts, values).
//!
//! Rules (normative in DESIGN.md §5):
//! - struct fields in declaration order; integers fixed-width
//!   little-endian; `f32`/`f64` IEEE bits little-endian; `bool` one byte
//!   (0 or 1);
//! - strings: `u32` byte length and the UTF-8 bytes as given. The encoder
//!   never normalizes: Unicode normalization belongs to intake (§10), so
//!   equal names are equal bytes before any encoder sees them;
//! - sequences and byte strings: `u32` count and items (a byte string over
//!   4 GiB is refused);
//! - fixed arrays: the items, no count;
//! - options: a `u8` presence byte and the payload;
//! - enums: a `u32` discriminant and the payload;
//! - sets and maps: `u32` count, elements (map entries by key) sorted
//!   bytewise by their encoding, no duplicates.
//!
//! [`Decoder`] is the strict inverse: every read is bounds-checked, counts
//! are checked against the bytes left, nesting is capped, a `bool` or
//! presence byte other than 0 or 1 is refused, and [`Decoder::finish`]
//! refuses trailing bytes, so one value has one byte string.

use std::fmt;

/// Append-only encoder. Composites encode their fields in declaration
/// order by calling these methods.
#[derive(Default, Clone)]
pub struct Encoder {
    buf: Vec<u8>,
}

macro_rules! put_int {
    ($($name:ident: $ty:ty),* $(,)?) => {
        $(pub fn $name(&mut self, v: $ty) {
            self.buf.extend_from_slice(&v.to_le_bytes());
        })*
    };
}

/// A count or byte length as the encoding's `u32`. Over 4 GiB of items is
/// refused: no encoded body holds one.
pub fn count_u32(len: usize) -> u32 {
    u32::try_from(len).expect("encoded count or length exceeds u32")
}

impl Encoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    put_int! {
        u8: u8, u16: u16, u32: u32, u64: u64, u128: u128,
        i8: i8, i16: i16, i32: i32, i64: i64, i128: i128,
    }

    pub fn f32(&mut self, v: f32) {
        self.u32(v.to_bits());
    }

    pub fn f64(&mut self, v: f64) {
        self.u64(v.to_bits());
    }

    pub fn bool(&mut self, v: bool) {
        self.u8(v as u8);
    }

    /// `u32` byte length and the UTF-8 bytes as given.
    pub fn str(&mut self, s: &str) {
        self.bytes(s.as_bytes());
    }

    /// A byte string: `u32` length and the bytes.
    pub fn bytes(&mut self, bytes: &[u8]) {
        self.u32(count_u32(bytes.len()));
        self.buf.extend_from_slice(bytes);
    }

    /// Fixed-size byte arrays encode raw — the width is part of the
    /// record's declared shape, so no length prefix.
    pub fn raw(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// `u8` presence marker (0/1) and the payload when present.
    pub fn option<T>(&mut self, v: Option<T>, f: impl FnOnce(&mut Self, &T)) {
        match v {
            None => self.u8(0),
            Some(x) => {
                self.u8(1);
                f(self, &x);
            }
        }
    }

    /// `u32` discriminant; the caller encodes the payload after this call.
    pub fn enum_variant(&mut self, discriminant: u32) {
        self.u32(discriminant);
    }

    /// `u32` count and the elements in the given order.
    pub fn seq<T>(&mut self, items: &[T], f: impl Fn(&mut Self, &T)) {
        self.u32(count_u32(items.len()));
        for it in items {
            f(self, it);
        }
    }

    /// Sets: each element encoded standalone, the encodings sorted
    /// bytewise and deduplicated, then `u32` (deduplicated) count and the
    /// concatenation.
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
        self.u32(count_u32(encoded.len()));
        for e in encoded {
            self.buf.extend_from_slice(&e);
        }
    }

    /// Maps: entries sorted bytewise by their keys' encodings, `u32` count,
    /// then each key and value. Two entries with one key encoding are a
    /// caller error (a map has no duplicate keys).
    pub fn map<'a, K: 'a, V: 'a>(
        &mut self,
        entries: impl IntoIterator<Item = (&'a K, &'a V)>,
        key: impl Fn(&mut Self, &K),
        value: impl Fn(&mut Self, &V),
    ) {
        let mut encoded: Vec<(Vec<u8>, &V)> = entries
            .into_iter()
            .map(|(k, v)| {
                let mut e = Self::new();
                key(&mut e, k);
                (e.into_bytes(), v)
            })
            .collect();
        encoded.sort_by(|a, b| a.0.cmp(&b.0));
        assert!(
            encoded.windows(2).all(|pair| pair[0].0 != pair[1].0),
            "a map holds one entry per key"
        );
        self.u32(count_u32(encoded.len()));
        for (k, v) in encoded {
            self.buf.extend_from_slice(&k);
            value(self, v);
        }
    }
}

/// Nesting cap of every strict decoder (§5 "Strict decoding").
pub const MAX_DEPTH: usize = 256;

/// Count cap for zero-size elements, which the bytes left cannot bound.
pub const MAX_ZERO_SIZE_COUNT: usize = 1 << 20;

/// Why strict decoding refused its input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeErrorKind {
    /// Fewer bytes than the read needs.
    Truncated,
    /// Bytes left after the body: the sign of a body read against the
    /// wrong grammar.
    TrailingBytes,
    /// A `bool` or presence byte other than 0 or 1.
    InvalidFlag(u8),
    /// A count larger than the bytes left allow at the element's minimum
    /// size.
    CountTooLarge(u32),
    /// More than [`MAX_DEPTH`] nested frames.
    DepthExceeded,
    /// An enum discriminant, field index or tag out of range.
    VariantOutOfRange(u32),
    /// Keys or indices not strictly increasing.
    NotStrictlyIncreasing,
    InvalidUtf8,
    /// A `u32` that is not a Unicode scalar value.
    InvalidChar(u32),
    /// A grammar-specific refusal the primitives cannot name.
    Invalid(&'static str),
}

/// A strict decoding refusal: what, and at which byte offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError {
    pub offset: usize,
    pub kind: DecodeErrorKind,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "decode error at byte {}: {:?}", self.offset, self.kind)
    }
}

impl std::error::Error for DecodeError {}

/// Strict reader of the one encoding over a borrowed body.
pub struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
    depth: usize,
}

macro_rules! get_int {
    ($($name:ident: $ty:ty),* $(,)?) => {
        $(pub fn $name(&mut self) -> Result<$ty, DecodeError> {
            Ok(<$ty>::from_le_bytes(self.array()?))
        })*
    };
}

impl<'a> Decoder<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            position: 0,
            depth: 0,
        }
    }

    /// The byte offset of the next read.
    pub fn position(&self) -> usize {
        self.position
    }

    pub fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// An error of `kind` at the current offset.
    pub fn error(&self, kind: DecodeErrorKind) -> DecodeError {
        DecodeError {
            offset: self.position,
            kind,
        }
    }

    pub fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        let end = self
            .position
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| self.error(DecodeErrorKind::Truncated))?;
        let out = &self.bytes[self.position..end];
        self.position = end;
        Ok(out)
    }

    pub fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        Ok(self.take(N)?.try_into().expect("take returned N bytes"))
    }

    get_int! {
        u8: u8, u16: u16, u32: u32, u64: u64, u128: u128,
        i8: i8, i16: i16, i32: i32, i64: i64, i128: i128,
    }

    pub fn f32(&mut self) -> Result<f32, DecodeError> {
        Ok(f32::from_bits(self.u32()?))
    }

    pub fn f64(&mut self) -> Result<f64, DecodeError> {
        Ok(f64::from_bits(self.u64()?))
    }

    /// A `bool` or presence byte: 0 or 1, nothing else.
    pub fn bool(&mut self) -> Result<bool, DecodeError> {
        let at = self.position;
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(DecodeError {
                offset: at,
                kind: DecodeErrorKind::InvalidFlag(other),
            }),
        }
    }

    /// A `char`: a `u32` Unicode scalar value.
    pub fn char(&mut self) -> Result<char, DecodeError> {
        let at = self.position;
        let value = self.u32()?;
        char::from_u32(value).ok_or(DecodeError {
            offset: at,
            kind: DecodeErrorKind::InvalidChar(value),
        })
    }

    /// A count of elements at least `min_size` bytes each, checked against
    /// the bytes left (zero-size elements: at most
    /// [`MAX_ZERO_SIZE_COUNT`]).
    pub fn count(&mut self, min_size: usize) -> Result<usize, DecodeError> {
        let at = self.position;
        let count = self.u32()?;
        let limit = match min_size {
            0 => MAX_ZERO_SIZE_COUNT,
            size => self.remaining() / size,
        };
        if count as usize > limit {
            return Err(DecodeError {
                offset: at,
                kind: DecodeErrorKind::CountTooLarge(count),
            });
        }
        Ok(count as usize)
    }

    /// A byte string: `u32` length and the bytes.
    pub fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = self.count(1)?;
        self.take(len)
    }

    /// A string: `u32` length and UTF-8 bytes, as given (no normalization
    /// check: intake normalizes, §10).
    pub fn str(&mut self) -> Result<&'a str, DecodeError> {
        let at = self.position;
        let bytes = self.bytes()?;
        std::str::from_utf8(bytes).map_err(|_| DecodeError {
            offset: at,
            kind: DecodeErrorKind::InvalidUtf8,
        })
    }

    /// A presence byte, then the payload when present.
    pub fn option<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, DecodeError>,
    ) -> Result<Option<T>, DecodeError> {
        if self.bool()? {
            f(self).map(Some)
        } else {
            Ok(None)
        }
    }

    /// A `u32` discriminant below `count`.
    pub fn enum_variant(&mut self, count: u32) -> Result<u32, DecodeError> {
        let at = self.position;
        let discriminant = self.u32()?;
        if discriminant >= count {
            return Err(DecodeError {
                offset: at,
                kind: DecodeErrorKind::VariantOutOfRange(discriminant),
            });
        }
        Ok(discriminant)
    }

    /// A set: `u32` count and elements whose encodings strictly increase
    /// bytewise (sorted, no duplicates; §5).
    pub fn set<T>(
        &mut self,
        min_size: usize,
        mut element: impl FnMut(&mut Self) -> Result<T, DecodeError>,
    ) -> Result<Vec<T>, DecodeError> {
        let count = self.count(min_size)?;
        let mut out = Vec::with_capacity(count.min(4096));
        let mut previous: Option<&'a [u8]> = None;
        for _ in 0..count {
            let start = self.position;
            out.push(element(self)?);
            let encoded = &self.bytes[start..self.position];
            if previous.is_some_and(|previous| previous >= encoded) {
                return Err(DecodeError {
                    offset: start,
                    kind: DecodeErrorKind::NotStrictlyIncreasing,
                });
            }
            previous = Some(encoded);
        }
        Ok(out)
    }

    /// A map: `u32` count and entries whose key encodings strictly increase
    /// bytewise (sorted, one entry per key; §5).
    pub fn map<K, V>(
        &mut self,
        min_size: usize,
        mut key: impl FnMut(&mut Self) -> Result<K, DecodeError>,
        mut value: impl FnMut(&mut Self, &K) -> Result<V, DecodeError>,
    ) -> Result<Vec<(K, V)>, DecodeError> {
        let count = self.count(min_size)?;
        let mut out = Vec::with_capacity(count.min(4096));
        let mut previous: Option<&'a [u8]> = None;
        for _ in 0..count {
            let start = self.position;
            let k = key(self)?;
            let encoded = &self.bytes[start..self.position];
            if previous.is_some_and(|previous| previous >= encoded) {
                return Err(DecodeError {
                    offset: start,
                    kind: DecodeErrorKind::NotStrictlyIncreasing,
                });
            }
            previous = Some(encoded);
            let v = value(self, &k)?;
            out.push((k, v));
        }
        Ok(out)
    }

    /// Enter a nested frame; refused past [`MAX_DEPTH`]. Pair with
    /// [`Decoder::leave`].
    pub fn enter(&mut self) -> Result<(), DecodeError> {
        if self.depth >= MAX_DEPTH {
            return Err(self.error(DecodeErrorKind::DepthExceeded));
        }
        self.depth += 1;
        Ok(())
    }

    pub fn leave(&mut self) {
        self.depth -= 1;
    }

    /// The end of the body: trailing bytes are refused.
    pub fn finish(self) -> Result<(), DecodeError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(self.error(DecodeErrorKind::TrailingBytes))
        }
    }
}
