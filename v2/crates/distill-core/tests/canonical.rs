//! §5 "Identity records hash and compare by a canonical record encoding":
//! fields in declaration order; integers LE fixed-width; str as u32 length +
//! NFC UTF-8 bytes; fixed byte arrays raw; sets sorted, deduplicated, u32
//! count + elements; sequences u32 count + elements; Option as u8 presence
//! marker + payload; enums as u8 discriminant + payload; digests
//! domain-prefixed and versioned.

use distill_core::canonical::{
    domain_digest, CanonicalEncoder, DSCP, DSCT, DSLF, DSLI, DSPP, DSSI, DSTG, DSTR, DSVP,
};

fn enc(f: impl FnOnce(&mut CanonicalEncoder)) -> Vec<u8> {
    let mut e = CanonicalEncoder::new();
    f(&mut e);
    e.into_bytes()
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
fn bool_encodes_as_single_byte() {
    assert_eq!(enc(|e| e.bool(false)), vec![0]);
    assert_eq!(enc(|e| e.bool(true)), vec![1]);
}

#[test]
fn str_is_u32_length_plus_utf8() {
    assert_eq!(enc(|e| e.str("ab")), vec![2, 0, 0, 0, b'a', b'b']);
    // Length counts bytes, not chars.
    let bytes = enc(|e| e.str("é")); // U+00E9 is 2 bytes in UTF-8
    assert_eq!(&bytes[..4], &[2, 0, 0, 0]);
}

#[test]
fn str_is_nfc_normalized() {
    // U+00E9 (precomposed) vs U+0065 U+0301 (decomposed) must encode
    // identically: NFC normalization is part of the encoding.
    let precomposed = enc(|e| e.str("\u{00E9}"));
    let decomposed = enc(|e| e.str("e\u{0301}"));
    assert_eq!(precomposed, decomposed);
}

#[test]
fn strings_cannot_repartition() {
    // §5: "no ‖ formula in this document can repartition variable-length
    // fields." ("ab","c") and ("a","bc") concatenate identically unframed.
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
fn option_is_presence_marker_plus_payload() {
    let none = enc(|e| e.option(None::<u8>, |e, v| e.u8(*v)));
    assert_eq!(none, vec![0]);
    let some = enc(|e| e.option(Some(7u8), |e, v| e.u8(*v)));
    assert_eq!(some, vec![1, 7]);
    // Framing: None followed by a 1-byte field is distinct from Some.
    let none_then_byte = enc(|e| {
        e.option(None::<u8>, |e, v| e.u8(*v));
        e.u8(7);
    });
    assert_ne!(some, none_then_byte[..].to_vec().split_off(0)); // different lengths anyway
    assert_eq!(none_then_byte, vec![0, 7]);
    assert_ne!(some, none_then_byte); // [1,7] vs [0,7]
}

#[test]
fn enum_discriminant_is_u8_plus_payload() {
    let v = enc(|e| {
        e.enum_variant(3);
        e.u32(9);
    });
    assert_eq!(v, vec![3, 9, 0, 0, 0]);
}

#[test]
fn sequences_are_u32_count_plus_elements() {
    let v = enc(|e| e.seq(&[1u8, 2, 3], |e, x| e.u8(*x)));
    assert_eq!(v, vec![3, 0, 0, 0, 1, 2, 3]);
    let empty = enc(|e| e.seq(&[] as &[u8], |e, x| e.u8(*x)));
    assert_eq!(empty, vec![0, 0, 0, 0]);
}

#[test]
fn sets_are_sorted_and_deduplicated() {
    // Encoded element bytes are sorted; duplicates collapse; the count is
    // the deduplicated count.
    let v = enc(|e| e.set(&[3u8, 1, 2, 1], |e, x| e.u8(*x)));
    assert_eq!(v, vec![3, 0, 0, 0, 1, 2, 3]);
    // Order-insensitive by construction.
    let a = enc(|e| e.set(&["b", "a"], |e, s| e.str(s)));
    let b = enc(|e| e.set(&["a", "b"], |e, s| e.str(s)));
    assert_eq!(a, b);
}

#[test]
fn set_sorts_by_encoded_bytes_not_input_order() {
    // Strings sort by their encoded form (length-prefixed bytes): "b" < "aa"
    // bytewise on the length prefix? No — equal-length prefixes for 1-char
    // strings; here we pin that sorting is over the *encoded* bytes.
    let v = enc(|e| e.set(&["aa", "b"], |e, s| e.str(s)));
    // "aa" encodes [2,0,0,0,a,a]; "b" encodes [1,0,0,0,b]. [1,..] < [2,..]
    // so "b" comes first.
    let expected = enc(|e| {
        e.u32(2);
        e.str("b");
        e.str("aa");
    });
    assert_eq!(v, expected);
}

#[test]
fn fixed_arrays_encode_raw() {
    let v = enc(|e| e.raw(&[9u8; 32]));
    assert_eq!(v, vec![9u8; 32]);
}

#[test]
fn domain_digests_are_domain_and_version_separated() {
    let payload = |e: &mut CanonicalEncoder| e.str("x");
    let a = domain_digest(DSLI, 1, payload);
    let b = domain_digest(DSTG, 1, payload);
    let c = domain_digest(DSLI, 2, payload);
    assert_ne!(a, b, "different domain, same payload");
    assert_ne!(a, c, "same domain, different version");
    // Deterministic.
    assert_eq!(a, domain_digest(DSLI, 1, payload));
    // 32-byte blake3.
    assert_eq!(a.len(), 32);
}

#[test]
fn all_domains_are_distinct() {
    let ds = [DSLI, DSTG, DSSI, DSTR, DSLF, DSCP, DSVP, DSPP, DSCT];
    for (i, a) in ds.iter().enumerate() {
        for b in &ds[i + 1..] {
            assert_ne!(a, b);
        }
    }
    assert_eq!(&DSLI, b"DSLI");
    assert_eq!(&DSTG, b"DSTG");
    assert_eq!(&DSSI, b"DSSI");
    assert_eq!(&DSTR, b"DSTR");
    assert_eq!(&DSLF, b"DSLF");
    assert_eq!(&DSCP, b"DSCP");
}
