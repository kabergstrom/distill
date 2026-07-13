//! The canonical writer: exactly one text form per value (§6, RFC 8785
//! reference). No insignificant whitespace anywhere — bare `,` and `:`
//! separators, no indentation; keys in BTreeMap (UTF-8 byte) order;
//! minimal escaping; floats by ECMAScript Number-to-string (8785's
//! shortest-round-trip rules, exponent form included). The §6 whole-file
//! trailing `\n` is the FILE writer's byte, not this value serializer's.

use crate::{AuthoredValue, WriteError};

pub fn write(v: &AuthoredValue) -> Result<String, WriteError> {
    let mut out = String::new();
    write_value(v, &mut out)?;
    Ok(out)
}

/// Canonical decimal for an IEEE-754 binary32 value. This is the
/// schema-directed `f32` leaf formatter: shortest digits that reparse to
/// the same binary32 bits, re-notated with the same ECMAScript exponent
/// thresholds as the ordinary JSON number writer.
pub fn write_f32(value: f32) -> Result<String, WriteError> {
    if !value.is_finite() {
        return Err(WriteError::NonFiniteFloat);
    }
    let value = if value == 0.0 { 0.0 } else { value };
    let mut out = String::new();
    let mut buf = ryu::Buffer::new();
    let text = buf.format_finite(value.abs());
    write_ecmascript_digits(value < 0.0, text, &mut out);
    Ok(out)
}

fn write_value(v: &AuthoredValue, out: &mut String) -> Result<(), WriteError> {
    match v {
        AuthoredValue::Null => out.push_str("null"),
        AuthoredValue::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        AuthoredValue::Int(i) => out.push_str(&i.to_string()),
        AuthoredValue::UInt(u) => out.push_str(&u.to_string()),
        AuthoredValue::Float(f) => {
            if !f.is_finite() {
                return Err(WriteError::NonFiniteFloat);
            }
            // -0.0 normalized (§6): -0.0 == 0.0, so this catches exactly it.
            let f = if *f == 0.0 { 0.0 } else { *f };
            write_ecmascript_number(f, out);
        }
        AuthoredValue::Str(s) => write_string(s, out),
        AuthoredValue::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(item, out)?;
            }
            out.push(']');
        }
        AuthoredValue::Object(map) => {
            out.push('{');
            for (i, (k, val)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(k, out);
                out.push(':');
                write_value(val, out)?;
            }
            out.push('}');
        }
        AuthoredValue::Blob(_) => return Err(WriteError::Blob),
    }
    Ok(())
}

/// ECMAScript `Number::toString` (ECMA-262 §6.1.6.1.20, the form RFC 8785
/// pins): shortest round-trip digits, decimal notation for exponents in
/// (-6, 21], signed-exponent scientific form outside. Built on ryu's
/// shortest digits, re-notated per the ES thresholds — ryu alone switches
/// notation at different magnitudes and prints integral doubles as "2.0".
fn write_ecmascript_number(f: f64, out: &mut String) {
    debug_assert!(f.is_finite());
    let mut buf = ryu::Buffer::new();
    let ryu_text = buf.format_finite(f.abs());
    write_ecmascript_digits(f < 0.0, ryu_text, out);
}

fn write_ecmascript_digits(negative: bool, ryu_text: &str, out: &mut String) {
    if negative {
        out.push('-');
    }

    // Decompose ryu's output ("ddd.ddd" or "d.ddde±xx") into the shortest
    // digit string and its decimal exponent n, where value = 0.digits × 10ⁿ.
    let (mantissa, e10) = match ryu_text.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i32>().expect("ryu exponent is an integer")),
        None => (ryu_text, 0),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((i, fr)) => (i, fr),
        None => (mantissa, ""),
    };
    let mut digits: String = format!("{int_part}{frac_part}");
    let mut n = int_part.len() as i32 + e10;
    // Strip leading zeros (adjusting n) and trailing zeros (shortest form).
    let lead = digits.len() - digits.trim_start_matches('0').len();
    digits.drain(..lead);
    n -= lead as i32;
    digits.truncate(digits.trim_end_matches('0').len());
    if digits.is_empty() {
        // Zero has no nonzero digits; ES prints "0".
        out.push('0');
        return;
    }
    let k = digits.len() as i32;

    if k <= n && n <= 21 {
        // Integer: digits followed by n-k zeros.
        out.push_str(&digits);
        for _ in 0..(n - k) {
            out.push('0');
        }
    } else if 0 < n && n <= 21 {
        // Decimal point inside the digits.
        out.push_str(&digits[..n as usize]);
        out.push('.');
        out.push_str(&digits[n as usize..]);
    } else if -6 < n && n <= 0 {
        // Leading "0." then -n zeros then the digits.
        out.push_str("0.");
        for _ in 0..(-n) {
            out.push('0');
        }
        out.push_str(&digits);
    } else {
        // Scientific: d[.ddd]e±(n-1), exponent sign mandatory.
        out.push_str(&digits[..1]);
        if k > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        let exp = n - 1;
        if exp >= 0 {
            out.push('+');
        }
        out.push_str(&exp.to_string());
    }
}

/// Minimal escaping (§6): only mandatory escapes — quote, backslash, and
/// controls (< 0x20, shorthand where JSON has one); printable characters,
/// non-ASCII included, stay raw. `/` is not escaped.
fn write_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}
