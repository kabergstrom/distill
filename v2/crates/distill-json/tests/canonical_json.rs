//! §6 "Canonical JSON is fully pinned": the lexer reads strict RFC 8259
//! (plus a depth cap and full-range integers), the scalar writers emit one
//! form per leaf: minimal escaping (only mandatory escapes; no \uXXXX for
//! printable characters), shortest-round-trip float formatting, NaN/Inf
//! rejected, -0.0 normalized. Duplicate keys and key order are the
//! schema-directed reader's and writer's (ngp-schema's text codec).

use distill_json::{
    write_f32, write_f64, write_str, Kind, Lexer, ParseError, ParseErrorKind, WriteError,
};

/// One whole value, syntax-checked.
fn lex(text: &str) -> Result<(), ParseError> {
    let mut lexer = Lexer::new(text);
    lexer.skip()?;
    lexer.finish()
}

fn kind(text: &str) -> ParseErrorKind {
    lex(text).unwrap_err().kind
}

fn f64_text(value: f64) -> String {
    let mut out = String::new();
    write_f64(value, &mut out).unwrap();
    out
}

fn str_text(value: &str) -> String {
    let mut out = String::new();
    write_str(value, &mut out);
    out
}

// --- scalar writers ---

#[test]
fn string_writer_escapes_minimally() {
    assert_eq!(str_text(""), "\"\"");
    // Printable non-ASCII stays raw — no \uXXXX for printable characters.
    assert_eq!(str_text("é😀"), "\"é😀\"");
    // Mandatory escapes only: quote, backslash, controls.
    assert_eq!(
        str_text("a\"b\\c\nd\re\tf\u{8}g\u{c}h"),
        "\"a\\\"b\\\\c\\nd\\re\\tf\\bg\\fh\""
    );
    // Controls without shorthand use lowercase \u00xx.
    assert_eq!(str_text("\u{1}\u{1f}"), "\"\\u0001\\u001f\"");
    // Forward slash is NOT escaped (not mandatory).
    assert_eq!(str_text("a/b"), "\"a/b\"");
}

#[test]
fn binary64_writer_is_ecmascript_number_to_string() {
    // §6: float leaves print by 8785's shortest-round-trip rules —
    // ECMAScript Number-to-string, exponent form included.
    assert_eq!(f64_text(0.1), "0.1");
    assert_eq!(f64_text(-2.5), "-2.5");
    // Integral doubles print as integers (no ".0").
    assert_eq!(f64_text(2.0), "2");
    assert_eq!(f64_text(-4.0), "-4");
    // Exponent form begins at 1e21 and below 1e-6, with a signed exponent.
    assert_eq!(f64_text(1e21), "1e+21");
    assert_eq!(f64_text(1e20), "100000000000000000000");
    assert_eq!(f64_text(1.5e22), "1.5e+22");
    assert_eq!(f64_text(1e300), "1e+300");
    assert_eq!(f64_text(1e-6), "0.000001");
    assert_eq!(f64_text(1e-7), "1e-7");
    assert_eq!(f64_text(5e-324), "5e-324"); // min subnormal
    // 8785's own example.
    assert_eq!(f64_text(333333333.3333333), "333333333.3333333");
}

#[test]
fn binary64_writer_normalizes_negative_zero() {
    assert_eq!(f64_text(-0.0), "0");
}

#[test]
fn binary64_writer_rejects_nan_and_inf() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(
            write_f64(value, &mut String::new()),
            Err(WriteError::NonFiniteFloat)
        );
    }
}

#[test]
fn binary32_writer_uses_shortest_roundtrip_decimal() {
    assert_eq!(write_f32(0.1).unwrap(), "0.1");
    assert_eq!(write_f32(f32::from_bits(1)).unwrap(), "1e-45");
    assert_eq!(write_f32(1.0e20).unwrap(), "100000000000000000000");
    assert_eq!(write_f32(f32::MAX).unwrap(), "3.4028235e+38");
    assert_eq!(write_f32(-0.0).unwrap(), "0");
}

#[test]
fn binary32_writer_rejects_non_finite_values() {
    assert_eq!(write_f32(f32::NAN), Err(WriteError::NonFiniteFloat));
    assert_eq!(write_f32(f32::INFINITY), Err(WriteError::NonFiniteFloat));
    assert_eq!(
        write_f32(f32::NEG_INFINITY),
        Err(WriteError::NonFiniteFloat)
    );
}

// --- lexer: happy paths ---

#[test]
fn lexer_reads_scalars() {
    let mut lexer = Lexer::new("null");
    assert_eq!(lexer.peek().unwrap(), Kind::Null);
    lexer.null().unwrap();
    lexer.finish().unwrap();
    assert!(Lexer::new("true").bool().unwrap());
    assert!(!Lexer::new("false").bool().unwrap());
    assert_eq!(Lexer::new("\"hi\"").string().unwrap(), "hi");
    let mut lexer = Lexer::new("  1  ");
    assert_eq!(lexer.number().unwrap().u128(), Some(1));
    lexer.finish().unwrap();
}

#[test]
fn lexer_numbers_keep_full_integer_range() {
    fn number(text: &str) -> distill_json::Number<'_> {
        Lexer::new(text).number().unwrap()
    }
    assert_eq!(number("0").u128(), Some(0));
    assert_eq!(number("-1").i128(), Some(-1));
    assert_eq!(number("-0").u128(), Some(0));
    assert!(!number("2.0").integer);
    assert_eq!(number("2.0").f64().unwrap(), 2.0);
    assert_eq!(number("2e1").f64().unwrap(), 20.0);
    assert_eq!(number("-2.5e-1").f64().unwrap(), -0.25);
    let (max, min) = (u128::MAX.to_string(), i128::MIN.to_string());
    assert_eq!(number(&max).u128(), Some(u128::MAX));
    assert_eq!(number(&min).i128(), Some(i128::MIN));
}

#[test]
fn lexer_negative_zero_float_normalizes() {
    let value = Lexer::new("-0.0").number().unwrap().f64().unwrap();
    assert_eq!(value, 0.0);
    assert!(value.is_sign_positive(), "-0.0 must normalize to +0.0");
}

#[test]
fn lexer_string_escapes() {
    assert_eq!(
        Lexer::new(r#""a\"b\\c\/d\ne\rf\tg\bh\fi""#).string().unwrap(),
        "a\"b\\c/d\ne\rf\tg\u{8}h\u{c}i"
    );
    assert_eq!(Lexer::new(r#""\u00e9""#).string().unwrap(), "é");
    assert_eq!(Lexer::new(r#""é""#).string().unwrap(), "é");
    // Surrogate pair.
    assert_eq!(Lexer::new(r#""\ud83d\ude00""#).string().unwrap(), "😀");
}

#[test]
fn lexer_steps_through_containers() {
    let mut lexer = Lexer::new("[1, [2], {\"b\": 3, \"a\": 4}]");
    let mut array = lexer.array().unwrap();
    assert!(lexer.next_item(&mut array).unwrap());
    assert_eq!(lexer.number().unwrap().u128(), Some(1));
    assert!(lexer.next_item(&mut array).unwrap());
    assert_eq!(lexer.skip().unwrap(), 4..7);
    assert!(lexer.next_item(&mut array).unwrap());
    let mut object = lexer.object().unwrap();
    // Keys come in text order; ordering and duplicates are the reader's.
    let (key, offset) = lexer.next_key(&mut object).unwrap().unwrap();
    assert_eq!((&*key, offset), ("b", 10));
    lexer.skip().unwrap();
    let (key, _) = lexer.next_key(&mut object).unwrap().unwrap();
    assert_eq!(key, "a");
    lexer.skip().unwrap();
    assert!(lexer.next_key(&mut object).unwrap().is_none());
    assert!(!lexer.next_item(&mut array).unwrap());
    assert_eq!(lexer.depth(), 0);
    lexer.finish().unwrap();
    lex("[]").unwrap();
    lex("{}").unwrap();
    // Insignificant whitespace is fine.
    lex("{\n  \"a\": [0.1, -7, 18446744073709551616],\n  \"z\": \"é\"\n}").unwrap();
}

#[test]
fn a_skipped_span_lexes_again() {
    let text = "{\"a\": [1, 2]}";
    let mut lexer = Lexer::new(text);
    let mut object = lexer.object().unwrap();
    lexer.next_key(&mut object).unwrap().unwrap();
    let span = lexer.skip().unwrap();
    let mut again = Lexer::span(text, span, lexer.depth());
    let mut array = again.array().unwrap();
    assert!(again.next_item(&mut array).unwrap());
    assert_eq!(again.number().unwrap().u128(), Some(1));
}

// --- lexer: negatives ---

#[test]
fn lexer_rejects_trailing_content() {
    assert!(matches!(kind("1 2"), ParseErrorKind::TrailingContent));
    assert!(matches!(kind("{} x"), ParseErrorKind::TrailingContent));
}

#[test]
fn lexer_rejects_malformed_numbers() {
    for t in ["01", "1.", ".5", "1e", "+1", "-", "0x1", "1e+"] {
        assert!(
            matches!(
                kind(t),
                ParseErrorKind::Number | ParseErrorKind::Expected | ParseErrorKind::TrailingContent
            ),
            "{t:?} must fail"
        );
    }
    // Overflow beyond u128 / below i128 is an error, never a silent float.
    let too_big = format!("{}0", u128::MAX);
    assert!(matches!(kind(&too_big), ParseErrorKind::Number));
    let too_small = format!("{}0", i128::MIN);
    assert!(matches!(kind(&too_small), ParseErrorKind::Number));
    // Float overflow to infinity is an error.
    assert!(matches!(kind("1e999"), ParseErrorKind::Number));
}

#[test]
fn lexer_rejects_bad_strings() {
    assert!(matches!(kind("\"a"), ParseErrorKind::UnexpectedEof));
    assert!(matches!(kind("\"\\x\""), ParseErrorKind::Escape));
    assert!(matches!(kind("\"\\u12\""), ParseErrorKind::Escape));
    // Lone surrogates, both halves.
    assert!(matches!(kind("\"\\uD800\""), ParseErrorKind::LoneSurrogate));
    assert!(matches!(kind("\"\\uDC00\""), ParseErrorKind::LoneSurrogate));
    assert!(matches!(
        kind("\"\\uD800x\""),
        ParseErrorKind::LoneSurrogate
    ));
    // Raw control characters are invalid inside strings.
    assert!(matches!(kind("\"a\u{1}b\""), ParseErrorKind::ControlChar));
}

#[test]
fn lexer_rejects_structural_garbage() {
    for t in [
        "",
        "  ",
        "[1,]",
        "{\"a\":}",
        "{\"a\" 1}",
        "[1 2]",
        "{1: 2}",
        "nul",
        "tru",
        "{",
    ] {
        assert!(lex(t).is_err(), "{t:?} must fail");
    }
}

#[test]
fn lexer_depth_cap_is_enforced() {
    // §12's cap discipline applied to parsing: deep nesting is a definite
    // error, not a stack overflow. Cap pinned at 512.
    let deep_ok = format!("{}1{}", "[".repeat(512), "]".repeat(512));
    assert!(lex(&deep_ok).is_ok());
    let deep_bad = format!("{}1{}", "[".repeat(513), "]".repeat(513));
    assert!(matches!(kind(&deep_bad), ParseErrorKind::DepthLimit));
}

#[test]
fn lexer_errors_carry_byte_offsets() {
    let err = lex("{\"a\": nul}").unwrap_err();
    assert_eq!(err.offset, 6);
}
