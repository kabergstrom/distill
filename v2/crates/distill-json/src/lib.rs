//! §6 canonical JSON and the `AuthoredValue` bundle data model.
//!
//! The writer emits exactly one form per value (canonical bytes require a
//! canonical writer — input hashes hash whole-file bytes); the parser is a
//! strict JSON reader that additionally rejects duplicate keys and enforces
//! a nesting-depth cap, so malformed input is a definite error, never a
//! guess.

use std::collections::BTreeMap;
use std::fmt;

mod parse;
mod write;

pub use parse::parse;
pub use write::{write, write_f32};

/// The bundle data model (§6): canonical JSON plus blob bytes. This is the
/// value type `load_current` yields (§11), migration functions transform
/// (§11), and importers emit (§8).
#[derive(Clone, Debug, PartialEq)]
pub enum AuthoredValue {
    /// `Option::None`; `Some(x)` encodes as `x` (§5).
    Null,
    Bool(bool),
    /// Negative integers. Non-negative integers parse as `UInt` — the
    /// variant split is pinned so parsing is deterministic; schema-directed
    /// decode range-checks either variant into the leaf type.
    Int(i128),
    UInt(u128),
    /// Finite only: NaN/Inf are rejected in authored data and -0.0
    /// normalizes to +0.0 (§6).
    Float(f64),
    Str(String),
    /// Vec/arrays/sets; non-string-key maps as [k, v] pairs (§6).
    Array(Vec<AuthoredValue>),
    /// Structs; enums as `{ "Variant": … }`; string-key maps.
    Object(BTreeMap<String, AuthoredValue>),
    /// `#[asset(blob)]` bytes — container-chunk backed, never JSON text:
    /// writing one through the JSON writer is an error (§6).
    Blob(Vec<u8>),
}

/// Nesting-depth cap for parsing (containers). Deep input is a definite
/// error, not a stack overflow — the §12 cap discipline applied to text.
pub const MAX_DEPTH: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseErrorKind {
    /// An object spelled the same key twice — §6: parse error, never
    /// last-write-wins.
    DuplicateKey,
    /// Valid value followed by non-whitespace.
    TrailingContent,
    /// Malformed or out-of-range number (beyond u128/i128, or a float
    /// overflowing to infinity).
    Number,
    /// The grammar expected something else here.
    Expected,
    UnexpectedEof,
    /// Malformed escape sequence.
    Escape,
    /// A UTF-16 surrogate half without its pair.
    LoneSurrogate,
    /// Raw control character inside a string.
    ControlChar,
    /// Nesting beyond MAX_DEPTH.
    DepthLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseError {
    pub kind: ParseErrorKind,
    /// Byte offset into the input where the error was detected.
    pub offset: usize,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JSON parse error at byte {}: {:?}",
            self.offset, self.kind
        )
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteError {
    /// NaN or ±Inf in authored data (§6).
    NonFiniteFloat,
    /// Blobs are container-chunk data; they have no JSON text form (§6).
    Blob,
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteError::NonFiniteFloat => f.write_str("NaN/Inf is not authorable (§6)"),
            WriteError::Blob => f.write_str("blobs have no JSON text form (§6)"),
        }
    }
}

impl std::error::Error for WriteError {}
