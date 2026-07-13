//! §6 "Canonical JSON is fully pinned": keys sorted by UTF-8 byte order,
//! duplicate keys are parse errors, minimal escaping (only mandatory
//! escapes; no \uXXXX for printable characters), shortest-round-trip float
//! formatting, NaN/Inf rejected in authored data, -0.0 normalized.
//!
//! `AuthoredValue` is the bundle data model (§6): canonical JSON plus blob
//! bytes. Blobs never appear in JSON text (the container encoding rewrites
//! blob fields as offset/len), so writing a Blob through the JSON writer is
//! an error, never an improvised encoding.

use std::collections::BTreeMap;

use distill_json::{parse, write, write_f32, AuthoredValue as V, ParseErrorKind, WriteError};

fn obj(entries: &[(&str, V)]) -> V {
    V::Object(
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect::<BTreeMap<_, _>>(),
    )
}

// --- writer: pinned canonical form (§6: RFC 8785 reference, named
// deviations; NO insignificant whitespace anywhere — bare `,` and `:`
// separators, no indentation; the single trailing `\n` is a whole-FILE
// byte owned by the file writer, not this value serializer) ---

#[test]
fn writer_pins_compact_form_with_sorted_keys() {
    let v = obj(&[
        ("b", V::UInt(1)),
        (
            "a",
            V::Array(vec![V::Bool(true), V::Null, V::Str("x".into())]),
        ),
    ]);
    assert_eq!(write(&v).unwrap(), r#"{"a":[true,null,"x"],"b":1}"#);
}

#[test]
fn writer_emits_no_whitespace_and_no_trailing_newline() {
    let v = obj(&[
        ("k", obj(&[("x", V::UInt(1))])),
        ("l", V::Array(vec![V::UInt(2)])),
    ]);
    let text = write(&v).unwrap();
    assert_eq!(text, r#"{"k":{"x":1},"l":[2]}"#);
    assert!(!text.contains(' ') && !text.contains('\n'));
}

#[test]
fn writer_empty_containers() {
    assert_eq!(write(&V::Array(vec![])).unwrap(), "[]");
    assert_eq!(write(&obj(&[])).unwrap(), "{}");
    let nested = obj(&[("k", obj(&[])), ("l", V::Array(vec![]))]);
    assert_eq!(write(&nested).unwrap(), r#"{"k":{},"l":[]}"#);
}

#[test]
fn writer_scalars() {
    assert_eq!(write(&V::Null).unwrap(), "null");
    assert_eq!(write(&V::Bool(false)).unwrap(), "false");
    assert_eq!(write(&V::UInt(0)).unwrap(), "0");
    assert_eq!(write(&V::Int(-3)).unwrap(), "-3");
    assert_eq!(write(&V::UInt(u128::MAX)).unwrap(), u128::MAX.to_string());
    assert_eq!(write(&V::Int(i128::MIN)).unwrap(), i128::MIN.to_string());
    assert_eq!(write(&V::Str(String::new())).unwrap(), "\"\"");
}

#[test]
fn writer_minimal_escaping() {
    // Printable non-ASCII stays raw — no \uXXXX for printable characters.
    assert_eq!(write(&V::Str("é😀".into())).unwrap(), "\"é😀\"");
    // Mandatory escapes only: quote, backslash, controls.
    assert_eq!(
        write(&V::Str("a\"b\\c\nd\re\tf\u{8}g\u{c}h".into())).unwrap(),
        "\"a\\\"b\\\\c\\nd\\re\\tf\\bg\\fh\""
    );
    // Controls without shorthand use lowercase \u00xx.
    assert_eq!(
        write(&V::Str("\u{1}\u{1f}".into())).unwrap(),
        "\"\\u0001\\u001f\""
    );
    // Forward slash is NOT escaped (not mandatory).
    assert_eq!(write(&V::Str("a/b".into())).unwrap(), "\"a/b\"");
}

#[test]
fn writer_floats_are_ecmascript_number_to_string() {
    // §6: float leaves print by 8785's shortest-round-trip rules —
    // ECMAScript Number-to-string, exponent form included.
    assert_eq!(write(&V::Float(0.1)).unwrap(), "0.1");
    assert_eq!(write(&V::Float(-2.5)).unwrap(), "-2.5");
    // Integral doubles print as integers (no ".0").
    assert_eq!(write(&V::Float(2.0)).unwrap(), "2");
    assert_eq!(write(&V::Float(-4.0)).unwrap(), "-4");
    // Exponent form begins at 1e21 and below 1e-6, with a signed exponent.
    assert_eq!(write(&V::Float(1e21)).unwrap(), "1e+21");
    assert_eq!(write(&V::Float(1e20)).unwrap(), "100000000000000000000");
    assert_eq!(write(&V::Float(1.5e22)).unwrap(), "1.5e+22");
    assert_eq!(write(&V::Float(1e300)).unwrap(), "1e+300");
    assert_eq!(write(&V::Float(1e-6)).unwrap(), "0.000001");
    assert_eq!(write(&V::Float(1e-7)).unwrap(), "1e-7");
    assert_eq!(write(&V::Float(5e-324)).unwrap(), "5e-324"); // min subnormal
                                                             // 8785's own example.
    assert_eq!(
        write(&V::Float(333333333.3333333)).unwrap(),
        "333333333.3333333"
    );
}

#[test]
fn writer_normalizes_negative_zero() {
    assert_eq!(write(&V::Float(-0.0)).unwrap(), "0");
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

#[test]
fn integral_float_reparses_as_uint_by_design() {
    // The text form is canonical, not the enum variant: Float(2.0) prints
    // "2", which parse's pinned variant split reads back as UInt(2).
    // Schema-directed decode range-converts either variant into the leaf
    // type (§5/§6) — the FILE bytes stay unambiguous and stable.
    let reparsed = parse(&write(&V::Float(2.0)).unwrap()).unwrap();
    assert_eq!(reparsed, V::UInt(2));
}

#[test]
fn writer_rejects_nan_and_inf() {
    assert!(matches!(
        write(&V::Float(f64::NAN)),
        Err(WriteError::NonFiniteFloat)
    ));
    assert!(matches!(
        write(&V::Float(f64::INFINITY)),
        Err(WriteError::NonFiniteFloat)
    ));
    assert!(matches!(
        write(&V::Float(f64::NEG_INFINITY)),
        Err(WriteError::NonFiniteFloat)
    ));
    // Nested occurrences are found too.
    let v = obj(&[("x", V::Array(vec![V::Float(f64::NAN)]))]);
    assert!(matches!(write(&v), Err(WriteError::NonFiniteFloat)));
}

#[test]
fn writer_rejects_blobs() {
    assert!(matches!(write(&V::Blob(vec![1, 2])), Err(WriteError::Blob)));
}

// --- parser: happy paths ---

#[test]
fn parse_scalars() {
    assert_eq!(parse("null").unwrap(), V::Null);
    assert_eq!(parse("true").unwrap(), V::Bool(true));
    assert_eq!(parse("false").unwrap(), V::Bool(false));
    assert_eq!(parse("\"hi\"").unwrap(), V::Str("hi".into()));
    assert_eq!(parse("  1  ").unwrap(), V::UInt(1));
}

#[test]
fn parse_numbers_pin_variant_choice() {
    // Non-negative integers are UInt; negative are Int; anything with a
    // fraction or exponent is Float.
    assert_eq!(parse("0").unwrap(), V::UInt(0));
    assert_eq!(parse("-1").unwrap(), V::Int(-1));
    assert_eq!(parse("-0").unwrap(), V::Int(0));
    assert_eq!(parse("2.0").unwrap(), V::Float(2.0));
    assert_eq!(parse("2e1").unwrap(), V::Float(20.0));
    assert_eq!(parse("-2.5e-1").unwrap(), V::Float(-0.25));
    assert_eq!(parse(&u128::MAX.to_string()).unwrap(), V::UInt(u128::MAX));
    assert_eq!(parse(&i128::MIN.to_string()).unwrap(), V::Int(i128::MIN));
}

#[test]
fn parse_negative_zero_float_normalizes() {
    let v = parse("-0.0").unwrap();
    match v {
        V::Float(f) => {
            assert_eq!(f, 0.0);
            assert!(f.is_sign_positive(), "-0.0 must normalize to +0.0");
        }
        other => panic!("expected float, got {:?}", other),
    }
}

#[test]
fn parse_string_escapes() {
    assert_eq!(
        parse(r#""a\"b\\c\/d\ne\rf\tg\bh\fi""#).unwrap(),
        V::Str("a\"b\\c/d\ne\rf\tg\u{8}h\u{c}i".into())
    );
    assert_eq!(parse(r#""é""#).unwrap(), V::Str("é".into()));
    assert_eq!(parse(r#""é""#).unwrap(), V::Str("é".into()));
    // Surrogate pair.
    assert_eq!(parse(r#""😀""#).unwrap(), V::Str("😀".into()));
}

#[test]
fn parse_containers() {
    assert_eq!(parse("[]").unwrap(), V::Array(vec![]));
    assert_eq!(parse("{}").unwrap(), obj(&[]));
    assert_eq!(
        parse("[1, [2], {\"a\": 3}]").unwrap(),
        V::Array(vec![
            V::UInt(1),
            V::Array(vec![V::UInt(2)]),
            obj(&[("a", V::UInt(3))])
        ])
    );
    // Key order in the text is not required to be canonical — the writer
    // canonicalizes; the parser only rejects duplicates.
    assert_eq!(
        parse("{\"b\": 1, \"a\": 2}").unwrap(),
        obj(&[("a", V::UInt(2)), ("b", V::UInt(1))])
    );
}

#[test]
fn round_trip_is_identity_on_parsed_values() {
    let text = "{\"a\":[0.1,-7,18446744073709551616],\"z\":\"é\"}";
    let v = parse(text).unwrap();
    assert_eq!(write(&v).unwrap(), text);
    assert_eq!(parse(&write(&v).unwrap()).unwrap(), v);
    // Non-canonical whitespace parses fine; the writer canonicalizes.
    let pretty = "{\n  \"a\": [0.1, -7, 18446744073709551616],\n  \"z\": \"é\"\n}";
    assert_eq!(write(&parse(pretty).unwrap()).unwrap(), text);
}

// --- parser: negatives ---

fn kind(text: &str) -> ParseErrorKind {
    parse(text).unwrap_err().kind
}

#[test]
fn parse_rejects_duplicate_keys() {
    assert!(matches!(
        kind("{\"a\":1,\"a\":2}"),
        ParseErrorKind::DuplicateKey
    ));
    // Also when spelled via distinct escapes of the same key.
    assert!(matches!(
        kind("{\"a\":1,\"\\u0061\":2}"),
        ParseErrorKind::DuplicateKey
    ));
}

#[test]
fn parse_rejects_trailing_content() {
    assert!(matches!(kind("1 2"), ParseErrorKind::TrailingContent));
    assert!(matches!(kind("{} x"), ParseErrorKind::TrailingContent));
}

#[test]
fn parse_rejects_malformed_numbers() {
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
fn parse_rejects_bad_strings() {
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
fn parse_rejects_structural_garbage() {
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
        assert!(parse(t).is_err(), "{t:?} must fail");
    }
}

#[test]
fn parse_depth_cap_is_enforced() {
    // §12's cap discipline applied to parsing: deep nesting is a definite
    // error, not a stack overflow. Cap pinned at 512.
    let deep_ok = format!("{}1{}", "[".repeat(512), "]".repeat(512));
    assert!(parse(&deep_ok).is_ok());
    let deep_bad = format!("{}1{}", "[".repeat(513), "]".repeat(513));
    assert!(matches!(kind(&deep_bad), ParseErrorKind::DepthLimit));
}

#[test]
fn parse_errors_carry_byte_offsets() {
    let err = parse("{\"a\": nul}").unwrap_err();
    assert_eq!(err.offset, 6);
}
