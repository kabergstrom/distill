//! §5 hash: XXH3-128, unseeded, 16 little-endian output bytes; digests are
//! `XXH3-128(u32 domain ‖ u32 version ‖ body)`.

use distill_core::hash::{digest, digest_bytes, digest_prefix, hash, to_hex, Domain, Hasher};

/// Reference values from `xxhsum -H2` (xxHash 0.8), which prints the
/// 128-bit value big-endian.
const VECTORS: &[(&[u8], u128)] = &[
    (b"", 0x99aa06d3014798d86001c324468d497f),
    (b"abc", 0x06b05ab6733a618578af5f94892f3950),
    (
        b"The quick brown fox jumps over the lazy dog",
        0xddd650205ca3e7fa24a1cc2e3a8a7651,
    ),
];

#[test]
fn hash_matches_the_xxh3_reference_vectors() {
    for (input, expected) in VECTORS {
        assert_eq!(hash(input), expected.to_le_bytes(), "{input:?}");
    }
    // 300 bytes: XXH3's long-input path.
    let long = vec![b'a'; 300];
    assert_eq!(hash(&long), 0x5e563f2cb28d2005c1d17581d8eb7e21u128.to_le_bytes());
}

#[test]
fn streaming_equals_one_shot() {
    let long: Vec<u8> = (0..10_000u32).map(|i| (i * 31 % 251) as u8).collect();
    let mut hasher = Hasher::new();
    for chunk in long.chunks(7) {
        hasher.update(chunk);
    }
    assert_eq!(hasher.finish(), hash(&long));
    let mut short = Hasher::new();
    short.update(b"a").update(&[b'a'; 299]);
    assert_eq!(short.finish(), hash(&[b'a'; 300]));
}

#[test]
fn digest_is_the_hash_of_domain_version_and_body() {
    let body = b"body";
    let mut preimage = Vec::new();
    preimage.extend_from_slice(&(Domain::Schema as u32).to_le_bytes());
    preimage.extend_from_slice(&7u32.to_le_bytes());
    preimage.extend_from_slice(body);
    assert_eq!(digest_bytes(Domain::Schema, 7, body), hash(&preimage));
    assert_eq!(&digest_prefix(Domain::Schema, 7)[..], &preimage[..8]);
    assert_eq!(digest(Domain::Schema, 7, |e| e.raw(body)), hash(&preimage));
}

#[test]
fn digests_are_domain_and_version_separated() {
    let payload = |e: &mut distill_core::encoding::Encoder| e.str("x");
    let a = digest(Domain::Schema, 1, payload);
    assert_ne!(a, digest(Domain::Layout, 1, payload), "different domain, same payload");
    assert_ne!(a, digest(Domain::Schema, 2, payload), "same domain, different version");
    assert_eq!(a, digest(Domain::Schema, 1, payload));
}

#[test]
fn domain_discriminants_are_pinned_and_distinct() {
    for (index, domain) in Domain::ALL.iter().enumerate() {
        assert_eq!(*domain as u32, index as u32 + 1, "{domain:?}");
        assert_eq!(Domain::from_u32(*domain as u32), Some(*domain));
    }
    assert_eq!(Domain::from_u32(0), None);
    assert_eq!(Domain::from_u32(Domain::ALL.len() as u32 + 1), None);
}

#[test]
fn hex_is_32_lowercase_chars_in_stored_byte_order() {
    let mut bytes = [0u8; 16];
    bytes[0] = 0xab;
    bytes[15] = 0x01;
    assert_eq!(to_hex(&bytes), "ab000000000000000000000000000001");
}
