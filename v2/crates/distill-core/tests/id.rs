//! Identity newtypes (§7): 16-byte UUIDs for assets/bundles/types, 32-byte
//! hashes (ContentHash of artifact bytes; LogicalHash/LayoutHash of the two
//! schema projections). Display/parse round-trips; wrong shapes are errors,
//! never truncated.

use std::str::FromStr;

use distill_core::id::{
    AssetUuid, BundleFileHash, BundleUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid,
};

#[test]
fn uuid_display_parses_back() {
    let u = AssetUuid([0xAB; 16]);
    let s = u.to_string();
    assert_eq!(s, "abababab-abab-abab-abab-abababababab");
    assert_eq!(AssetUuid::from_str(&s).unwrap(), u);
}

#[test]
fn uuid_parse_accepts_hyphenated_lowercase_and_uppercase() {
    let s = "00112233-4455-6677-8899-AABBCCDDEEFF";
    let u = TypeUuid::from_str(s).unwrap();
    assert_eq!(
        u.0,
        [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
            0xEE, 0xFF
        ]
    );
}

#[test]
fn uuid_parse_rejects_bad_shapes() {
    assert!(BundleUuid::from_str("").is_err());
    assert!(BundleUuid::from_str("0011223344556677").is_err());
    assert!(BundleUuid::from_str("00112233-4455-6677-8899-aabbccddeeff0").is_err());
    assert!(BundleUuid::from_str("g0112233-4455-6677-8899-aabbccddeeff").is_err());
    // Right length, hyphens misplaced.
    assert!(BundleUuid::from_str("001122334-455-6677-8899-aabbccddeeff").is_err());
}

#[test]
fn hash_display_is_64_hex_and_parses_back() {
    let h = ContentHash([0x0F; 32]);
    let s = h.to_string();
    assert_eq!(s.len(), 64);
    assert_eq!(ContentHash::from_str(&s).unwrap(), h);
    assert!(ContentHash::from_str(&s[..63]).is_err());
    assert!(ContentHash::from_str(&format!("{}0", s)).is_err());
}

#[test]
fn bundle_file_hash_accepts_exact_malformed_observed_bytes() {
    let malformed = b"{ not a canonical bundle\xff";
    assert_eq!(
        BundleFileHash::of_observed_bytes(malformed),
        BundleFileHash(*blake3::hash(malformed).as_bytes())
    );
}

#[test]
fn hash_types_are_distinct_types() {
    // Compile-time property really; pin the constructors exist and carry 32 bytes.
    let _l = LogicalHash([1; 32]);
    let _y = LayoutHash([2; 32]);
    let _c = ContentHash([3; 32]);
}

#[test]
fn uuid_v5_children_derive_from_parent_and_key() {
    // §9: extra outputs get UUIDv5(parent uuid, output key) — RFC 4122 §4.3
    // name-based UUID with SHA-1, name = the output key's UTF-8 bytes.
    let parent = AssetUuid([7; 16]);
    let a = AssetUuid::v5(parent, "reflection");
    let b = AssetUuid::v5(parent, "reflection");
    let c = AssetUuid::v5(parent, "thumbnail");
    let d = AssetUuid::v5(AssetUuid([8; 16]), "reflection");
    assert_eq!(a, b, "deterministic");
    assert_ne!(a, c, "key participates");
    assert_ne!(a, d, "parent participates");
    // Version and variant bits per RFC 4122.
    assert_eq!(a.0[6] >> 4, 5, "version nibble is 5");
    assert_eq!(a.0[8] >> 6, 0b10, "variant is RFC 4122");
}
