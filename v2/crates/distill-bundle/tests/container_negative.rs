//! §6 container corruptions — each one a distinct, precisely named error,
//! detected before any large allocation and never a panic.

mod common;

use common::*;
use distill_bundle::{parse_bundle, write_bundle, BundleError as E};
use distill_core::hash::Domain;
use distill_core::header::HeaderError;
use ngp_schema::SchemaNode as N;

/// A canonical container with multiple blobs and inter-blob gaps.
fn base_container() -> Vec<u8> {
    let sc = schema(st(&[("first", N::Blob), ("second", N::Blob)]));
    let data = obj(&[
        ("first", blob(b"12345678")), // 8 bytes -> gap 8..16
        ("second", blob(b"abcdefgh")),
    ]);
    let b = bundle(&[&sc], vec![("x", entry(UUID_A, &sc, data))], None);
    write_bundle(&b).unwrap()
}

#[test]
fn too_short_for_header() {
    let base = base_container();
    let err = parse_bundle(&base[..20]).unwrap_err();
    assert!(
        matches!(err, E::TooShortForHeader { len: 20 }),
        "got {err:?}"
    );
}

#[test]
fn without_the_magic_bytes_are_not_a_container() {
    // The magic dispatches a container; without it the bytes are read as
    // the plain encoding's text and refused there, never as a container.
    let mut bytes = base_container();
    bytes[1] = b'X';
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(!matches!(err, E::Header(_)), "got {err:?}");
}

#[test]
fn another_formats_header_is_refused_by_name() {
    let mut bytes = base_container();
    bytes[4..8].copy_from_slice(&(Domain::PackArchive as u32).to_le_bytes());
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(
            err,
            E::Header(HeaderError::WrongDomain { expected: Domain::Bundle, found })
                if found == Domain::PackArchive as u32
        ),
        "got {err:?}"
    );
}

#[test]
fn unsupported_container_version() {
    let mut bytes = base_container();
    bytes[8..12].copy_from_slice(&1u32.to_le_bytes());
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(
            err,
            E::Header(HeaderError::UnsupportedVersion { domain: Domain::Bundle, expected: 2, found: 1 })
        ),
        "got {err:?}"
    );
}

#[test]
fn truncated_file() {
    let base = base_container();
    let err = parse_bundle(&base[..base.len() - 1]).unwrap_err();
    assert!(
        matches!(err, E::Truncated { expected, actual } if actual + 1 == expected),
        "got {err:?}"
    );
}

#[test]
fn file_too_long() {
    let mut bytes = base_container();
    bytes.push(0);
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(err, E::TrailingBytes { expected, actual } if expected + 1 == actual),
        "got {err:?}"
    );
}

#[test]
fn json_len_larger_than_file_errors_before_allocating() {
    let mut bytes = base_container();
    bytes[12..20].copy_from_slice(&(1u64 << 62).to_le_bytes());
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(err, E::Truncated { expected, .. } if expected > (1u64 << 62)),
        "got {err:?}"
    );
}

#[test]
fn header_length_overflow_is_checked() {
    let mut bytes = base_container();
    bytes[12..20].copy_from_slice(&u64::MAX.to_le_bytes());
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(matches!(err, E::HeaderOverflow { .. }), "got {err:?}");
}

#[test]
fn nonzero_pad_byte() {
    let mut bytes = base_container();
    let json_len = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let json_end = CONTAINER_HEADER_LEN as u64 + json_len;
    let pad = align16(json_end) - json_end;
    assert!(
        pad > 0,
        "fixture must have pad bytes (json_len = {json_len})"
    );
    bytes[json_end as usize] = 1;
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(err, E::NonzeroPadByte { file_offset } if file_offset == json_end),
        "got {err:?}"
    );
}

#[test]
fn nonzero_inter_blob_gap_byte() {
    let mut bytes = base_container();
    let json_len = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let chunk_start = align16(CONTAINER_HEADER_LEN as u64 + json_len) as usize;
    // First blob is 8 bytes; 8..16 is the alignment gap before the second.
    bytes[chunk_start + 10] = 7;
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(err, E::NonzeroGapByte { chunk_offset: 10 }),
        "got {err:?}"
    );
}

/// Build a container whose envelope carries hand-picked {"len","offset"}
/// blob objects over an arbitrary chunk.
fn placed(fields: &[(&str, u64, u64)], chunk: &[u8]) -> Vec<u8> {
    let sc = schema(st(&fields
        .iter()
        .map(|(n, _, _)| (*n, N::Blob))
        .collect::<Vec<_>>()));
    let data = obj(&fields
        .iter()
        .map(|(n, len, offset)| {
            (
                *n,
                obj(&[("len", u(*len as u128)), ("offset", u(*offset as u128))]),
            )
        })
        .collect::<Vec<_>>());
    build_container(&raw_plain(&sc, "x", data), chunk)
}

#[test]
fn blob_range_out_of_bounds() {
    let bytes = placed(&[("a1", 100, 0)], &[0u8; 16]);
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(
            &err,
            E::BlobOutOfBounds { local_id, path, offset: 0, len: 100, blob_len: 16 }
                if local_id == "x" && path.contains("a1")
        ),
        "got {err:?}"
    );
}

#[test]
fn blob_offset_plus_len_overflow_is_checked() {
    let bytes = placed(&[("a1", u64::MAX, 16)], &[0u8; 16]);
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(matches!(err, E::BlobOutOfBounds { .. }), "got {err:?}");
}

#[test]
fn misaligned_blob_offset() {
    let mut chunk = vec![0u8; 16];
    chunk[8..12].copy_from_slice(b"DATA");
    let bytes = placed(&[("a1", 4, 8)], &chunk);
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(&err, E::MisalignedBlob { offset: 8, path, .. } if path.contains("a1")),
        "got {err:?}"
    );
}

#[test]
fn overlapping_blobs() {
    // Canonical path order is a1 then a2; both claim offset 0.
    let bytes = placed(&[("a1", 8, 0), ("a2", 8, 0)], &[0u8; 8]);
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(
            &err,
            E::OverlappingBlobs { offset: 0, prev_end: 8, path, .. } if path.contains("a2")
        ),
        "got {err:?}"
    );
}

#[test]
fn blob_not_at_canonical_offset() {
    // a2 sits at 32; canonical layout demands align16(8) = 16. Aligned and
    // non-overlapping, but not the deterministic writer's placement.
    let mut chunk = vec![0u8; 40];
    chunk[..8].copy_from_slice(b"11111111");
    chunk[32..40].copy_from_slice(b"22222222");
    let bytes = placed(&[("a1", 8, 0), ("a2", 8, 32)], &chunk);
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(
            &err,
            E::BlobNotAtCanonicalOffset { expected: 16, actual: 32, path, .. }
                if path.contains("a2")
        ),
        "got {err:?}"
    );
}

#[test]
fn blob_chunk_trailing_space_rejected() {
    // blob_len must equal the end of the last blob exactly.
    let bytes = placed(&[("a1", 8, 0)], &[0u8; 24]);
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(
            err,
            E::BlobChunkLength {
                expected: 8,
                actual: 24
            }
        ),
        "got {err:?}"
    );
}

#[test]
fn zero_blob_container_with_nonempty_chunk_rejected() {
    let sc = simple_schema();
    let b = bundle(
        &[&sc],
        vec![(
            "a",
            entry(UUID_A, &sc, obj(&[("count", u(1)), ("name", s("n"))])),
        )],
        None,
    );
    let bytes = build_container(&plain_bytes(&b), &[0u8; 16]);
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(
            err,
            E::BlobChunkLength {
                expected: 0,
                actual: 16
            }
        ),
        "got {err:?}"
    );
}

#[test]
fn container_json_chunk_must_be_utf8() {
    let bytes = build_container(&[0xFF, 0xFE, b'{'], &[]);
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(matches!(err, E::NotUtf8 { .. }), "got {err:?}");
}
