//! Strict JSON parser producing `AuthoredValue`. Beyond RFC 8259: duplicate
//! object keys are errors, integers keep full i128/u128 range (overflow is
//! an error, never a silent float), floats overflowing to infinity are
//! errors, -0.0 normalizes, and nesting is capped at `MAX_DEPTH`.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

use crate::{AuthoredValue, ParseError, ParseErrorKind as K, MAX_DEPTH};

pub fn parse(text: &str) -> Result<AuthoredValue, ParseError> {
    let mut p = Parser {
        bytes: text.as_bytes(),
        pos: 0,
    };
    p.skip_ws();
    let v = p.value(0)?;
    p.skip_ws();
    if p.pos < p.bytes.len() {
        return Err(p.err(K::TrailingContent));
    }
    Ok(v)
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn err(&self, kind: K) -> ParseError {
        ParseError {
            kind,
            offset: self.pos,
        }
    }

    fn err_at(&self, kind: K, offset: usize) -> ParseError {
        ParseError { kind, offset }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn expect(&mut self, b: u8) -> Result<(), ParseError> {
        if self.peek() == Some(b) {
            self.pos += 1;
            Ok(())
        } else if self.peek().is_none() {
            Err(self.err(K::UnexpectedEof))
        } else {
            Err(self.err(K::Expected))
        }
    }

    fn value(&mut self, depth: usize) -> Result<AuthoredValue, ParseError> {
        match self.peek() {
            None => Err(self.err(K::UnexpectedEof)),
            Some(b'n') => self.literal("null", AuthoredValue::Null),
            Some(b't') => self.literal("true", AuthoredValue::Bool(true)),
            Some(b'f') => self.literal("false", AuthoredValue::Bool(false)),
            Some(b'"') => Ok(AuthoredValue::Str(self.string()?)),
            Some(b'[') => self.array(depth),
            Some(b'{') => self.object(depth),
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(_) => Err(self.err(K::Expected)),
        }
    }

    fn literal(&mut self, text: &str, v: AuthoredValue) -> Result<AuthoredValue, ParseError> {
        if self.bytes[self.pos..].starts_with(text.as_bytes()) {
            self.pos += text.len();
            Ok(v)
        } else {
            Err(self.err(K::Expected))
        }
    }

    fn array(&mut self, depth: usize) -> Result<AuthoredValue, ParseError> {
        if depth >= MAX_DEPTH {
            return Err(self.err(K::DepthLimit));
        }
        self.expect(b'[')?;
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(AuthoredValue::Array(items));
        }
        loop {
            self.skip_ws();
            items.push(self.value(depth + 1)?);
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    return Ok(AuthoredValue::Array(items));
                }
                None => return Err(self.err(K::UnexpectedEof)),
                Some(_) => return Err(self.err(K::Expected)),
            }
        }
    }

    fn object(&mut self, depth: usize) -> Result<AuthoredValue, ParseError> {
        if depth >= MAX_DEPTH {
            return Err(self.err(K::DepthLimit));
        }
        self.expect(b'{')?;
        let mut map = BTreeMap::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(AuthoredValue::Object(map));
        }
        loop {
            self.skip_ws();
            let key_offset = self.pos;
            if self.peek() != Some(b'"') {
                return Err(match self.peek() {
                    None => self.err(K::UnexpectedEof),
                    Some(_) => self.err(K::Expected),
                });
            }
            let key = self.string()?;
            self.skip_ws();
            self.expect(b':')?;
            self.skip_ws();
            let value = self.value(depth + 1)?;
            match map.entry(key) {
                Entry::Vacant(e) => {
                    e.insert(value);
                }
                Entry::Occupied(_) => return Err(self.err_at(K::DuplicateKey, key_offset)),
            }
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(AuthoredValue::Object(map));
                }
                None => return Err(self.err(K::UnexpectedEof)),
                Some(_) => return Err(self.err(K::Expected)),
            }
        }
    }

    fn string(&mut self) -> Result<String, ParseError> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            let start = self.pos;
            match self.peek() {
                None => return Err(self.err(K::UnexpectedEof)),
                Some(b'"') => {
                    self.pos += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.pos += 1;
                    out.push(self.escape()?);
                }
                Some(c) if c < 0x20 => return Err(self.err(K::ControlChar)),
                Some(_) => {
                    // Run of plain bytes: input is &str, so UTF-8 holds.
                    while let Some(c) = self.peek() {
                        if c == b'"' || c == b'\\' || c < 0x20 {
                            break;
                        }
                        self.pos += 1;
                    }
                    // SAFETY of slicing: boundaries are at ASCII bytes,
                    // which are always char boundaries.
                    out.push_str(
                        std::str::from_utf8(&self.bytes[start..self.pos]).expect("input was UTF-8"),
                    );
                }
            }
        }
    }

    fn escape(&mut self) -> Result<char, ParseError> {
        let esc_offset = self.pos - 1;
        let c = self.peek().ok_or_else(|| self.err(K::UnexpectedEof))?;
        self.pos += 1;
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
                    return Err(self.err_at(K::LoneSurrogate, esc_offset));
                }
                if (0xD800..=0xDBFF).contains(&hi) {
                    // Must be followed by \uDC00..=\uDFFF.
                    if self.peek() == Some(b'\\') && self.bytes.get(self.pos + 1) == Some(&b'u') {
                        self.pos += 2;
                        let lo = self.hex4()?;
                        if !(0xDC00..=0xDFFF).contains(&lo) {
                            return Err(self.err_at(K::LoneSurrogate, esc_offset));
                        }
                        let combined =
                            0x10000 + (((hi - 0xD800) as u32) << 10) + (lo - 0xDC00) as u32;
                        char::from_u32(combined)
                            .ok_or_else(|| self.err_at(K::Escape, esc_offset))?
                    } else {
                        return Err(self.err_at(K::LoneSurrogate, esc_offset));
                    }
                } else {
                    char::from_u32(hi as u32).ok_or_else(|| self.err_at(K::Escape, esc_offset))?
                }
            }
            _ => return Err(self.err_at(K::Escape, esc_offset)),
        })
    }

    fn hex4(&mut self) -> Result<u16, ParseError> {
        let mut v: u16 = 0;
        for _ in 0..4 {
            let c = self.peek().ok_or_else(|| self.err(K::Escape))?;
            let d = match c {
                b'0'..=b'9' => c - b'0',
                b'a'..=b'f' => c - b'a' + 10,
                b'A'..=b'F' => c - b'A' + 10,
                _ => return Err(self.err(K::Escape)),
            };
            v = (v << 4) | d as u16;
            self.pos += 1;
        }
        Ok(v)
    }

    fn number(&mut self) -> Result<AuthoredValue, ParseError> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        // Integer part: '0' or [1-9][0-9]*. A leading zero followed by a
        // digit is an error.
        match self.peek() {
            Some(b'0') => {
                self.pos += 1;
                if matches!(self.peek(), Some(b'0'..=b'9')) {
                    return Err(self.err_at(K::Number, start));
                }
            }
            Some(b'1'..=b'9') => {
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.pos += 1;
                }
            }
            _ => return Err(self.err_at(K::Number, start)),
        }
        let mut is_float = false;
        if self.peek() == Some(b'.') {
            is_float = true;
            self.pos += 1;
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(self.err_at(K::Number, start));
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            is_float = true;
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(self.err_at(K::Number, start));
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }
        let text = std::str::from_utf8(&self.bytes[start..self.pos]).expect("ascii");
        if is_float {
            let f: f64 = text.parse().map_err(|_| self.err_at(K::Number, start))?;
            if !f.is_finite() {
                return Err(self.err_at(K::Number, start));
            }
            // -0.0 normalized (§6).
            let f = if f == 0.0 { 0.0 } else { f };
            Ok(AuthoredValue::Float(f))
        } else if text.starts_with('-') {
            let i: i128 = text.parse().map_err(|_| self.err_at(K::Number, start))?;
            Ok(AuthoredValue::Int(i))
        } else {
            let u: u128 = text.parse().map_err(|_| self.err_at(K::Number, start))?;
            Ok(AuthoredValue::UInt(u))
        }
    }
}
