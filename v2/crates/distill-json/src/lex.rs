//! The pull lexer: strict RFC 8259 tokens, read by a caller that knows the
//! shape it expects (a schema-directed parser, the bundle envelope, the
//! snapshot codec). There is no value tree: the caller asks for the next
//! token kind, reads scalars, and steps through arrays and objects; a value
//! it wants later is skipped as a byte span and lexed again from there.
//!
//! Beyond RFC 8259: nesting is capped at [`MAX_DEPTH`], integers keep full
//! u128/i128 range (overflow is an error, never a silent float), floats
//! overflowing to infinity are errors, and -0.0 normalizes. Duplicate keys
//! are the caller's to reject: it knows which keys it has seen
//! ([`ParseErrorKind::DuplicateKey`]).

use std::borrow::Cow;
use std::ops::Range;

use crate::{ParseError, ParseErrorKind as K, MAX_DEPTH};

/// What the next value is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Null,
    Bool,
    Number,
    String,
    Array,
    Object,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Kind::Null => "null",
            Kind::Bool => "bool",
            Kind::Number => "number",
            Kind::String => "string",
            Kind::Array => "array",
            Kind::Object => "object",
        }
    }
}

/// A number token: its validated text. `integer`: no fraction, no exponent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Number<'a> {
    pub text: &'a str,
    pub integer: bool,
    pub offset: usize,
}

impl Number<'_> {
    pub fn negative(&self) -> bool {
        self.text.starts_with('-')
    }

    /// The number as a binary64: finite, -0.0 normalized.
    pub fn f64(&self) -> Result<f64, ParseError> {
        let value: f64 = self.text.parse().map_err(|_| self.error())?;
        if !value.is_finite() {
            return Err(self.error());
        }
        Ok(if value == 0.0 { 0.0 } else { value })
    }

    /// The number rounded once to the nearest binary32: finite, -0.0
    /// normalized.
    pub fn f32(&self) -> Result<f32, ParseError> {
        let value: f32 = self.text.parse().map_err(|_| self.error())?;
        if !value.is_finite() {
            return Err(self.error());
        }
        Ok(if value == 0.0 { 0.0 } else { value })
    }

    /// An integer token as u128 (`-0` reads as 0); `None` when out of
    /// range or not an integer.
    pub fn u128(&self) -> Option<u128> {
        if !self.integer {
            return None;
        }
        match self.text.strip_prefix('-') {
            Some(digits) => digits.bytes().all(|b| b == b'0').then_some(0),
            None => self.text.parse().ok(),
        }
    }

    /// An integer token as i128; `None` when out of range or not an
    /// integer.
    pub fn i128(&self) -> Option<i128> {
        if !self.integer {
            return None;
        }
        self.text.parse().ok()
    }

    fn error(&self) -> ParseError {
        ParseError { kind: K::Number, offset: self.offset }
    }
}

/// An open array or object: tracks whether an element was read, so
/// separators are checked.
#[derive(Debug)]
pub struct Open {
    first: bool,
}

#[derive(Debug, Clone)]
pub struct Lexer<'a> {
    text: &'a str,
    bytes: &'a [u8],
    pos: usize,
    end: usize,
    depth: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(text: &'a str) -> Self {
        Self { text, bytes: text.as_bytes(), pos: 0, end: text.len(), depth: 0 }
    }

    /// A lexer over `span` of `text` (a value an earlier pass skipped), at
    /// nesting `depth` (the depth the skipped value sat at).
    pub fn span(text: &'a str, span: Range<usize>, depth: usize) -> Self {
        Self { text, bytes: text.as_bytes(), pos: span.start, end: span.end, depth }
    }

    /// The text being lexed (whole input, not just a span).
    pub fn text(&self) -> &'a str {
        self.text
    }

    pub fn offset(&self) -> usize {
        self.pos
    }

    /// Current container nesting.
    pub fn depth(&self) -> usize {
        self.depth
    }

    pub fn error(&self, kind: K) -> ParseError {
        ParseError { kind, offset: self.pos }
    }

    fn at(&self) -> Option<u8> {
        if self.pos < self.end {
            Some(self.bytes[self.pos])
        } else {
            None
        }
    }

    fn skip_ws(&mut self) {
        while matches!(self.at(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn expect(&mut self, b: u8) -> Result<(), ParseError> {
        match self.at() {
            Some(c) if c == b => {
                self.pos += 1;
                Ok(())
            }
            None => Err(self.error(K::UnexpectedEof)),
            Some(_) => Err(self.error(K::Expected)),
        }
    }

    /// The kind of the next value (skipping whitespace).
    pub fn peek(&mut self) -> Result<Kind, ParseError> {
        self.skip_ws();
        match self.at() {
            None => Err(self.error(K::UnexpectedEof)),
            Some(b'n') => Ok(Kind::Null),
            Some(b't' | b'f') => Ok(Kind::Bool),
            Some(b'"') => Ok(Kind::String),
            Some(b'[') => Ok(Kind::Array),
            Some(b'{') => Ok(Kind::Object),
            Some(b'-' | b'0'..=b'9') => Ok(Kind::Number),
            Some(_) => Err(self.error(K::Expected)),
        }
    }

    fn literal(&mut self, word: &str) -> Result<(), ParseError> {
        if self.bytes[self.pos..self.end].starts_with(word.as_bytes()) {
            self.pos += word.len();
            Ok(())
        } else {
            Err(self.error(K::Expected))
        }
    }

    pub fn null(&mut self) -> Result<(), ParseError> {
        self.skip_ws();
        self.literal("null")
    }

    pub fn bool(&mut self) -> Result<bool, ParseError> {
        self.skip_ws();
        match self.at() {
            Some(b't') => self.literal("true").map(|()| true),
            Some(b'f') => self.literal("false").map(|()| false),
            None => Err(self.error(K::UnexpectedEof)),
            Some(_) => Err(self.error(K::Expected)),
        }
    }

    /// The end of the input (or span): only whitespace may remain.
    pub fn finish(&mut self) -> Result<(), ParseError> {
        self.skip_ws();
        if self.pos < self.end {
            return Err(self.error(K::TrailingContent));
        }
        Ok(())
    }

    /// Open an array (`[`).
    pub fn array(&mut self) -> Result<Open, ParseError> {
        self.skip_ws();
        self.open(b'[')
    }

    /// Open an object (`{`).
    pub fn object(&mut self) -> Result<Open, ParseError> {
        self.skip_ws();
        self.open(b'{')
    }

    fn open(&mut self, b: u8) -> Result<Open, ParseError> {
        if self.depth >= MAX_DEPTH {
            return Err(self.error(K::DepthLimit));
        }
        self.expect(b)?;
        self.depth += 1;
        Ok(Open { first: true })
    }

    /// Whether another array element follows (the caller then reads it);
    /// `false` consumed the closing `]`.
    pub fn next_item(&mut self, open: &mut Open) -> Result<bool, ParseError> {
        self.skip_ws();
        if self.at() == Some(b']') {
            if !open.first && self.bytes[..self.pos].last().is_some_and(|&b| b == b',') {
                // Unreachable through this API (a `,` is consumed only when
                // an element follows), kept as a definite error.
                return Err(self.error(K::Expected));
            }
            self.pos += 1;
            self.depth -= 1;
            return Ok(false);
        }
        if !open.first {
            self.expect(b',')?;
            self.skip_ws();
            if self.at() == Some(b']') {
                return Err(self.error(K::Expected));
            }
        }
        if self.at().is_none() {
            return Err(self.error(K::UnexpectedEof));
        }
        open.first = false;
        Ok(true)
    }

    /// The next object key with its `:` consumed (the caller then reads
    /// the value), or `None` after consuming the closing `}`. Also returns
    /// the key's byte offset, for duplicate-key errors.
    pub fn next_key(&mut self, open: &mut Open) -> Result<Option<(Cow<'a, str>, usize)>, ParseError> {
        self.skip_ws();
        if self.at() == Some(b'}') {
            self.pos += 1;
            self.depth -= 1;
            return Ok(None);
        }
        if !open.first {
            self.expect(b',')?;
            self.skip_ws();
        }
        let offset = self.pos;
        match self.at() {
            Some(b'"') => {}
            None => return Err(self.error(K::UnexpectedEof)),
            Some(_) => return Err(self.error(K::Expected)),
        }
        let key = self.string()?;
        self.skip_ws();
        self.expect(b':')?;
        self.skip_ws();
        if self.at().is_none() {
            return Err(self.error(K::UnexpectedEof));
        }
        open.first = false;
        Ok(Some((key, offset)))
    }

    /// Skip one value, returning its byte span. Its syntax is checked in
    /// full (duplicate keys excepted: a skipped value is lexed again by
    /// whoever reads it).
    pub fn skip(&mut self) -> Result<Range<usize>, ParseError> {
        let kind = self.peek()?;
        let start = self.pos;
        match kind {
            Kind::Null => self.null()?,
            Kind::Bool => {
                self.bool()?;
            }
            Kind::Number => {
                self.number()?;
            }
            Kind::String => {
                self.string()?;
            }
            Kind::Array => {
                let mut open = self.array()?;
                while self.next_item(&mut open)? {
                    self.skip()?;
                }
            }
            Kind::Object => {
                let mut open = self.object()?;
                while self.next_key(&mut open)?.is_some() {
                    self.skip()?;
                }
            }
        }
        Ok(start..self.pos)
    }

    pub fn string(&mut self) -> Result<Cow<'a, str>, ParseError> {
        self.skip_ws();
        self.expect(b'"')?;
        let start = self.pos;
        // Fast path: no escapes.
        loop {
            match self.at() {
                None => return Err(self.error(K::UnexpectedEof)),
                Some(b'"') => {
                    let s = &self.text[start..self.pos];
                    self.pos += 1;
                    return Ok(Cow::Borrowed(s));
                }
                Some(b'\\') => break,
                Some(c) if c < 0x20 => return Err(self.error(K::ControlChar)),
                Some(_) => self.pos += 1,
            }
        }
        let mut out = String::from(&self.text[start..self.pos]);
        loop {
            let run = self.pos;
            match self.at() {
                None => return Err(self.error(K::UnexpectedEof)),
                Some(b'"') => {
                    self.pos += 1;
                    return Ok(Cow::Owned(out));
                }
                Some(b'\\') => {
                    self.pos += 1;
                    out.push(self.escape()?);
                }
                Some(c) if c < 0x20 => return Err(self.error(K::ControlChar)),
                Some(_) => {
                    while let Some(c) = self.at() {
                        if c == b'"' || c == b'\\' || c < 0x20 {
                            break;
                        }
                        self.pos += 1;
                    }
                    // Boundaries sit at ASCII bytes: char boundaries.
                    out.push_str(&self.text[run..self.pos]);
                }
            }
        }
    }

    fn escape(&mut self) -> Result<char, ParseError> {
        let esc = self.pos - 1;
        let c = self.at().ok_or_else(|| self.error(K::UnexpectedEof))?;
        self.pos += 1;
        let at = |kind| ParseError { kind, offset: esc };
        Ok(match c {
            b'"' => '"',
            b'\\' => '\\',
            b'/' => '/',
            b'b' => '\u{8}',
            b'f' => '\u{c}',
            b'n' => '\n',
            b'r' => '\r',
            b't' => '\t',
            b'u' => {
                let hi = self.hex4()?;
                if (0xDC00..=0xDFFF).contains(&hi) {
                    return Err(at(K::LoneSurrogate));
                }
                if (0xD800..=0xDBFF).contains(&hi) {
                    if self.at() == Some(b'\\') && self.pos + 1 < self.end && self.bytes[self.pos + 1] == b'u' {
                        self.pos += 2;
                        let lo = self.hex4()?;
                        if !(0xDC00..=0xDFFF).contains(&lo) {
                            return Err(at(K::LoneSurrogate));
                        }
                        let combined = 0x10000 + (((hi - 0xD800) as u32) << 10) + (lo - 0xDC00) as u32;
                        char::from_u32(combined).ok_or_else(|| at(K::Escape))?
                    } else {
                        return Err(at(K::LoneSurrogate));
                    }
                } else {
                    char::from_u32(hi as u32).ok_or_else(|| at(K::Escape))?
                }
            }
            _ => return Err(at(K::Escape)),
        })
    }

    fn hex4(&mut self) -> Result<u16, ParseError> {
        let mut v: u16 = 0;
        for _ in 0..4 {
            let c = self.at().ok_or_else(|| self.error(K::Escape))?;
            let d = match c {
                b'0'..=b'9' => c - b'0',
                b'a'..=b'f' => c - b'a' + 10,
                b'A'..=b'F' => c - b'A' + 10,
                _ => return Err(self.error(K::Escape)),
            };
            v = (v << 4) | d as u16;
            self.pos += 1;
        }
        Ok(v)
    }

    /// A number token, syntax-checked; integers are range-checked against
    /// u128/i128 here so overflow is a definite lexical error.
    pub fn number(&mut self) -> Result<Number<'a>, ParseError> {
        self.skip_ws();
        let start = self.pos;
        let bad = |_: &Self| ParseError { kind: K::Number, offset: start };
        if self.at() == Some(b'-') {
            self.pos += 1;
        }
        match self.at() {
            Some(b'0') => {
                self.pos += 1;
                if matches!(self.at(), Some(b'0'..=b'9')) {
                    return Err(bad(self));
                }
            }
            Some(b'1'..=b'9') => {
                while matches!(self.at(), Some(b'0'..=b'9')) {
                    self.pos += 1;
                }
            }
            _ => return Err(bad(self)),
        }
        let mut integer = true;
        if self.at() == Some(b'.') {
            integer = false;
            self.pos += 1;
            if !matches!(self.at(), Some(b'0'..=b'9')) {
                return Err(bad(self));
            }
            while matches!(self.at(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }
        if matches!(self.at(), Some(b'e' | b'E')) {
            integer = false;
            self.pos += 1;
            if matches!(self.at(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !matches!(self.at(), Some(b'0'..=b'9')) {
                return Err(bad(self));
            }
            while matches!(self.at(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }
        let number = Number { text: &self.text[start..self.pos], integer, offset: start };
        let ok = if integer {
            if number.negative() {
                number.text.parse::<i128>().is_ok()
            } else {
                number.text.parse::<u128>().is_ok()
            }
        } else {
            number.f64().is_ok()
        };
        if !ok {
            return Err(bad(self));
        }
        Ok(number)
    }
}
