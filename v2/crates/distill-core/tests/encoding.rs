//! §5 "One encoding": fields in order; integers LE fixed-width; floats as
//! IEEE bits; bool one byte; strings `u32` length + UTF-8 bytes as given;
//! sequences and byte lengths `u32`; options a `u8` presence byte; enums a
//! `u32` discriminant; sets and maps sorted bytewise by encoding. The
//! decoder refuses every non-canonical form by kind and offset.

use distill_core::encoding::{DecodeError, DecodeErrorKind, Decoder, Encoder, MAX_DEPTH};

fn enc(f: impl FnOnce(&mut Encoder)) -> Vec<u8> {
    let mut e = Encoder::new();
    f(&mut e);
    e.into_bytes()
}

fn kind<T: std::fmt::Debug>(result: Result<T, DecodeError>) -> DecodeErrorKind {
    result.unwrap_err().kind
}

#[test]
fn integers_are_le_fixed_width() {
    assert_eq!(enc(|e| e.u8(1)), vec![1]);
    assert_eq!(enc(|e| e.u16(1)), vec![1, 0]);
    assert_eq!(enc(|e| e.u32(1)), vec![1, 0, 0, 0]);
    assert_eq!(enc(|e| e.u64(1)), vec![1, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(enc(|e| e.u128(1)).len(), 16);
    assert_eq!(enc(|e| e.i64(-1)), vec![0xFF; 8]);
    assert_eq!(enc(|e| e.i128(-1)).len(), 16);
}

#[test]
fn floats_are_ieee_bits() {
    assert_eq!(enc(|e| e.f32(-0.0)), (-0.0f32).to_bits().to_le_bytes().to_vec());
    let nan = f64::from_bits(0x7ff8_0000_0000_0001);
    let bytes = enc(|e| e.f64(nan));
    let mut d = Decoder::new(&bytes);
    assert_eq!(d.f64().unwrap().to_bits(), 0x7ff8_0000_0000_0001);
    d.finish().unwrap();
}

#[test]
fn bool_is_one_byte_and_the_decoder_refuses_other_values() {
    assert_eq!(enc(|e| e.bool(false)), vec![0]);
    assert_eq!(enc(|e| e.bool(true)), vec![1]);
    assert_eq!(kind(Decoder::new(&[2]).bool()), DecodeErrorKind::InvalidFlag(2));
}

#[test]
fn str_is_u32_length_plus_raw_utf8() {
    assert_eq!(enc(|e| e.str("ab")), vec![2, 0, 0, 0, b'a', b'b']);
    let bytes = enc(|e| e.str("é"));
    assert_eq!(&bytes[..4], &[2, 0, 0, 0]);
}

#[test]
fn str_is_not_normalized() {
    // Normalization belongs to intake (§10): precomposed and decomposed
    // forms are different bytes, and both decode as given.
    let precomposed = enc(|e| e.str("\u{00E9}"));
    let decomposed = enc(|e| e.str("e\u{0301}"));
    assert_ne!(precomposed, decomposed);
    assert_eq!(Decoder::new(&decomposed).str().unwrap(), "e\u{0301}");
}

#[test]
fn strings_cannot_repartition() {
    let a = enc(|e| {
        e.str("ab");
        e.str("c");
    });
    let b = enc(|e| {
        e.str("a");
        e.str("bc");
    });
    assert_ne!(a, b);
}

#[test]
fn option_is_presence_byte_plus_payload() {
    assert_eq!(enc(|e| e.option(None::<u8>, |e, v| e.u8(*v))), vec![0]);
    assert_eq!(enc(|e| e.option(Some(7u8), |e, v| e.u8(*v))), vec![1, 7]);
    assert_eq!(
        kind(Decoder::new(&[2, 7]).option(|d| d.u8())),
        DecodeErrorKind::InvalidFlag(2)
    );
}

#[test]
fn enum_discriminant_is_u32_plus_payload() {
    let v = enc(|e| {
        e.enum_variant(3);
        e.u32(9);
    });
    assert_eq!(v, vec![3, 0, 0, 0, 9, 0, 0, 0]);
    assert_eq!(Decoder::new(&v).enum_variant(4).unwrap(), 3);
    assert_eq!(
        kind(Decoder::new(&v).enum_variant(3)),
        DecodeErrorKind::VariantOutOfRange(3)
    );
}

#[test]
fn sequences_are_u32_count_plus_elements() {
    let v = enc(|e| e.seq(&[1u8, 2, 3], |e, x| e.u8(*x)));
    assert_eq!(v, vec![3, 0, 0, 0, 1, 2, 3]);
    assert_eq!(enc(|e| e.seq(&[] as &[u8], |e, x| e.u8(*x))), vec![0, 0, 0, 0]);
}

#[test]
fn bytes_are_u32_length_plus_bytes() {
    assert_eq!(enc(|e| e.bytes(&[9, 8])), vec![2, 0, 0, 0, 9, 8]);
}

#[test]
fn sets_are_sorted_and_deduplicated() {
    let v = enc(|e| e.set(&[3u8, 1, 2, 1], |e, x| e.u8(*x)));
    assert_eq!(v, vec![3, 0, 0, 0, 1, 2, 3]);
    let a = enc(|e| e.set(&["b", "a"], |e, s| e.str(s)));
    let b = enc(|e| e.set(&["a", "b"], |e, s| e.str(s)));
    assert_eq!(a, b);
    // Sorted by encoded bytes: "b" ([1,0,0,0,b]) before "aa" ([2,0,0,0,a,a]).
    let expected = enc(|e| {
        e.u32(2);
        e.str("b");
        e.str("aa");
    });
    assert_eq!(enc(|e| e.set(&["aa", "b"], |e, s| e.str(s))), expected);
}

#[test]
fn maps_are_sorted_by_key_encoding() {
    let entries = [(2u8, "two"), (1u8, "one")];
    let v = enc(|e| e.map(entries.iter().map(|(k, v)| (k, v)), |e, k| e.u8(*k), |e, v| e.str(v)));
    let expected = enc(|e| {
        e.u32(2);
        e.u8(1);
        e.str("one");
        e.u8(2);
        e.str("two");
    });
    assert_eq!(v, expected);
    let mut d = Decoder::new(&v);
    let decoded = d.map(1, |d| d.u8(), |d, _| d.str().map(str::to_owned)).unwrap();
    d.finish().unwrap();
    assert_eq!(decoded, vec![(1, "one".to_owned()), (2, "two".to_owned())]);
}

#[test]
fn the_decoder_refuses_unsorted_or_duplicate_set_and_map_entries() {
    let unsorted = [2, 0, 0, 0, 2, 1];
    assert_eq!(
        kind(Decoder::new(&unsorted).set(1, |d| d.u8())),
        DecodeErrorKind::NotStrictlyIncreasing
    );
    let duplicate = [2, 0, 0, 0, 1, 1];
    assert_eq!(
        kind(Decoder::new(&duplicate).set(1, |d| d.u8())),
        DecodeErrorKind::NotStrictlyIncreasing
    );
    let duplicate_key = [2, 0, 0, 0, 1, 7, 1, 8];
    assert_eq!(
        kind(Decoder::new(&duplicate_key).map(2, |d| d.u8(), |d, _| d.u8())),
        DecodeErrorKind::NotStrictlyIncreasing
    );
}

#[test]
fn the_decoder_refuses_truncation_trailing_bytes_and_oversized_counts() {
    assert_eq!(kind(Decoder::new(&[1, 0]).u32()), DecodeErrorKind::Truncated);
    let mut d = Decoder::new(&[1, 2]);
    d.u8().unwrap();
    assert_eq!(kind(d.finish()), DecodeErrorKind::TrailingBytes);
    // A length past the bytes left is refused before any allocation.
    assert_eq!(
        kind(Decoder::new(&[0xff, 0xff, 0xff, 0xff, 1]).bytes()),
        DecodeErrorKind::CountTooLarge(u32::MAX)
    );
    assert_eq!(
        kind(Decoder::new(&[3, 0, 0, 0, 1, 2]).count(1)),
        DecodeErrorKind::CountTooLarge(3)
    );
}

#[test]
fn the_decoder_refuses_invalid_utf8_and_chars() {
    assert_eq!(kind(Decoder::new(&[1, 0, 0, 0, 0xff]).str()), DecodeErrorKind::InvalidUtf8);
    let surrogate = 0xD800u32.to_le_bytes();
    assert_eq!(kind(Decoder::new(&surrogate).char()), DecodeErrorKind::InvalidChar(0xD800));
}

#[test]
fn the_decoder_bounds_nesting_depth() {
    let mut d = Decoder::new(&[]);
    for _ in 0..MAX_DEPTH {
        d.enter().unwrap();
    }
    assert_eq!(kind(d.enter()), DecodeErrorKind::DepthExceeded);
}

#[test]
fn errors_carry_their_offset() {
    let mut d = Decoder::new(&[0, 0, 0, 0, 5]);
    d.u32().unwrap();
    let error = d.bool().unwrap_err();
    assert_eq!(error.offset, 4);
    assert_eq!(error.kind, DecodeErrorKind::InvalidFlag(5));
}
