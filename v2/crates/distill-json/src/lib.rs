//! The JSON lexer and canonical scalar writers (§6) that ngp-schema's text
//! codec, the bundle envelope and the schema snapshot codec build on.
//!
//! [`Lexer`] is a strict pull lexer read by a caller that knows the shape
//! it expects; there is no value tree (the bundle data model is
//! `ngp_schema::Value`). [`write_f32`], [`write_f64`] and [`write_str`]
//! emit the one canonical text of a float or string leaf.

use std::fmt;

mod lex;
mod write;

pub use lex::{Kind, Lexer, Number, Open};
pub use write::{write_f32, write_f64, write_str};

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
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteError::NonFiniteFloat => f.write_str("NaN/Inf is not authorable (§6)"),
        }
    }
}

impl std::error::Error for WriteError {}
