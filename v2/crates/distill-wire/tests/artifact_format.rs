//! DSTL artifact container (§12): pinned layout, canonical writer,
//! strict reader — negatives for every validation rule.

use distill_bundle::PathComponent;
use distill_core::id::{AssetUuid, LayoutHash, LogicalHash, TypeUuid};
use distill_wire::artifact::{
    artifact_header_layout_hash, assemble_artifact, canonical_blob_order, content_hash,
    parse_artifact, parse_artifact_parts, split_artifact, write_artifact, ArtifactError,
    ArtifactHeader, ARTIFACT_FORMAT_VERSION, ARTIFACT_MAGIC,
};

fn header() -> ArtifactHeader {
    ArtifactHeader {
        asset_uuid: AssetUuid([0x11; 16]),
        authored_type: TypeUuid([0x22; 16]),
        terminal_type: TypeUuid([0x33; 16]),
        encoded_type: TypeUuid([0x44; 16]),
        logical_hash: LogicalHash([0x55; 32]),
        layout_hash: LayoutHash([0x66; 32]),
    }
}

fn write_simple(
    deps: &[AssetUuid],
    fixed: &[u8],
    variable: &[u8],
    blobs: &[(Vec<PathComponent>, &[u8])],
) -> Vec<u8> {
    write_artifact(&header(), deps, fixed, variable, blobs).expect("write")
}

fn field(name: &str) -> PathComponent {
    PathComponent::Field(name.to_string())
}

// --- pinned byte layout -----------------------------------------------------

#[test]
fn minimal_artifact_bytes_pinned() {
    let bytes = write_simple(&[], &[0xAA, 0xAA, 0xAA, 0xAA], &[], &[]);

    let mut expected = Vec::new();
    expected.extend_from_slice(&[0x89, 0x44, 0x53, 0x54, 0x4C, 0x0D, 0x1A, 0x0A]); // magic
    expected.extend_from_slice(&1u32.to_le_bytes()); // version
    expected.extend_from_slice(&[0x11; 16]); // asset_uuid
    expected.extend_from_slice(&[0x22; 16]); // authored_type
    expected.extend_from_slice(&[0x33; 16]); // terminal_type
    expected.extend_from_slice(&[0x44; 16]); // encoded_type
    expected.extend_from_slice(&[0x55; 32]); // logical_hash
    expected.extend_from_slice(&[0x66; 32]); // layout_hash
    expected.extend_from_slice(&0u32.to_le_bytes()); // dep_count
    expected.extend_from_slice(&0u32.to_le_bytes()); // blob_count
    expected.extend_from_slice(&4u32.to_le_bytes()); // fixed_len
    expected.extend_from_slice(&0u64.to_le_bytes()); // var_len
    assert_eq!(expected.len(), 160); // header size, pinned
    expected.extend_from_slice(&[0xAA, 0xAA, 0xAA, 0xAA]); // fixed
    expected.extend_from_slice(&[0u8; 12]); // pad to 16

    assert_eq!(bytes, expected);
}

#[test]
fn magic_constant_is_pinned() {
    assert_eq!(
        ARTIFACT_MAGIC,
        [0x89, 0x44, 0x53, 0x54, 0x4C, 0x0D, 0x1A, 0x0A]
    );
    assert_eq!(ARTIFACT_FORMAT_VERSION, 1);
}

#[test]
fn content_hash_is_blake3_of_raw_bytes() {
    let bytes = write_simple(&[], &[1, 2, 3], &[], &[]);
    let expected = blake3::hash(&bytes);
    assert_eq!(content_hash(&bytes).0, *expected.as_bytes());
}

// --- roundtrips --------------------------------------------------------------

#[test]
fn roundtrip_sections_and_header() {
    let deps = [AssetUuid([9; 16]), AssetUuid([3; 16])];
    let fixed = [1u8, 2, 3, 4, 5];
    let variable = [7u8, 8, 9];
    let blob_a: &[u8] = &[0xB0; 5];
    let blob_b: &[u8] = &[0xB1; 17];
    let blobs = vec![(vec![field("a")], blob_a), (vec![field("b")], blob_b)];
    let bytes = write_simple(&deps, &fixed, &variable, &blobs);

    let view = parse_artifact(&bytes).expect("parse");
    assert_eq!(view.version, ARTIFACT_FORMAT_VERSION);
    assert_eq!(view.asset_uuid, AssetUuid([0x11; 16]));
    assert_eq!(view.authored_type, TypeUuid([0x22; 16]));
    assert_eq!(view.terminal_type, TypeUuid([0x33; 16]));
    assert_eq!(view.encoded_type, TypeUuid([0x44; 16]));
    assert_eq!(view.logical_hash, LogicalHash([0x55; 32]));
    assert_eq!(view.layout_hash, LayoutHash([0x66; 32]));
    // Sorted lexicographically by the writer.
    assert_eq!(view.load_deps, vec![AssetUuid([3; 16]), AssetUuid([9; 16])]);
    assert_eq!(view.fixed, &fixed);
    assert_eq!(view.variable, &variable);
    assert_eq!(view.blob(0), Some(blob_a));
    assert_eq!(view.blob(1), Some(blob_b));
    assert_eq!(view.blob(2), None);
}

#[test]
fn split_transport_reconstructs_metadata_and_complete_content_identity() {
    let a: &[u8] = &[0xA; 5];
    let b: &[u8] = &[0xB; 17];
    let bytes = write_simple(
        &[AssetUuid([9; 16])],
        &[1, 2, 3, 4],
        &[5, 6],
        &[(vec![field("a")], a), (vec![field("b")], b)],
    );
    let complete = parse_artifact(&bytes).unwrap();
    let structural_len = bytes.len() - complete.blob_section.len();
    let blobs: Vec<_> = (0..complete.blob_table.len())
        .map(|index| complete.blob(index as u32).unwrap())
        .collect();
    let split = parse_artifact_parts(&bytes[..structural_len], &blobs).unwrap();
    assert_eq!(split.asset_uuid, complete.asset_uuid);
    assert_eq!(split.terminal_type, complete.terminal_type);
    assert_eq!(split.layout_hash, complete.layout_hash);
    assert_eq!(split.load_deps, complete.load_deps);
    assert_eq!(split.fixed, complete.fixed);
    assert_eq!(split.variable, complete.variable);
    assert_eq!(split.content_hash, content_hash(&bytes));
}

#[test]
fn header_layout_hash_reads_the_header_alone() {
    let bytes = write_simple(&[AssetUuid([9; 16])], &[1, 2, 3, 4], &[5, 6], &[]);
    let header_len = 8 + 4 + 4 * 16 + 32 + 32;
    assert_eq!(
        artifact_header_layout_hash(&bytes).unwrap(),
        header().layout_hash
    );
    assert_eq!(
        artifact_header_layout_hash(&bytes[..header_len]).unwrap(),
        header().layout_hash
    );
    assert!(matches!(
        artifact_header_layout_hash(&bytes[..header_len - 1]),
        Err(ArtifactError::Truncated { .. })
    ));
    let mut bad = bytes.clone();
    bad[0] ^= 1;
    assert!(matches!(
        artifact_header_layout_hash(&bad),
        Err(ArtifactError::BadMagic)
    ));
}

#[test]
fn split_and_assemble_are_inverse() {
    let a: &[u8] = &[0xA; 5];
    let b: &[u8] = &[0xB; 17];
    let c: &[u8] = &[];
    let bytes = write_simple(
        &[AssetUuid([9; 16])],
        &[1, 2, 3, 4],
        &[5, 6],
        &[
            (vec![field("a")], a),
            (vec![field("b")], b),
            (vec![field("c")], c),
        ],
    );
    let (structural, blobs) = split_artifact(&bytes).unwrap();
    assert_eq!(blobs.len(), 3);
    assert_eq!(
        parse_artifact_parts(structural, &blobs)
            .unwrap()
            .content_hash,
        content_hash(&bytes)
    );
    assert_eq!(assemble_artifact(structural, &blobs), bytes);
}

#[test]
fn split_transport_rejects_missing_wrong_sized_and_structurally_appended_blobs() {
    let blob: &[u8] = &[1, 2, 3];
    let bytes = write_simple(&[], &[9], &[], &[(vec![field("a")], blob)]);
    let complete = parse_artifact(&bytes).unwrap();
    let structural_len = bytes.len() - complete.blob_section.len();
    let structural = &bytes[..structural_len];
    assert!(matches!(
        parse_artifact_parts(structural, &[]),
        Err(ArtifactError::BlobCountMismatch { .. })
    ));
    assert!(matches!(
        parse_artifact_parts(structural, &[&blob[..2]]),
        Err(ArtifactError::BlobLengthMismatch { .. })
    ));
    let mut appended = structural.to_vec();
    appended.push(0);
    assert!(matches!(
        parse_artifact_parts(&appended, &[blob]),
        Err(ArtifactError::TrailingBytes { .. })
    ));
}

#[test]
fn writer_sorts_and_dedups_load_deps() {
    let deps = [
        AssetUuid([7; 16]),
        AssetUuid([1; 16]),
        AssetUuid([7; 16]), // duplicate — canonicalized away
        AssetUuid([4; 16]),
    ];
    let bytes = write_simple(&deps, &[], &[], &[]);
    let view = parse_artifact(&bytes).expect("parse");
    assert_eq!(
        view.load_deps,
        vec![AssetUuid([1; 16]), AssetUuid([4; 16]), AssetUuid([7; 16])]
    );
}

#[test]
fn empty_artifact_roundtrips() {
    let bytes = write_simple(&[], &[], &[], &[]);
    let view = parse_artifact(&bytes).expect("parse");
    assert!(view.load_deps.is_empty());
    assert!(view.fixed.is_empty());
    assert!(view.variable.is_empty());
    assert_eq!(view.blob_table.len(), 0);
    assert_eq!(bytes.len(), 160); // header only, no padding needed
}

// --- blob table: structural-path order, offsets, gaps ------------------------

#[test]
fn blob_table_orders_by_encoded_structural_path() {
    // "b" sorts after "a.c" component-wise by encoded bytes; give them out
    // of order and check the table order follows the canonical path order.
    let first: &[u8] = &[1];
    let second: &[u8] = &[2, 2];
    let blobs = vec![
        (vec![field("b")], second),
        (vec![field("a"), field("c")], first),
    ];
    let bytes = write_simple(&[], &[], &[], &blobs);
    let view = parse_artifact(&bytes).expect("parse");
    assert_eq!(view.blob(0), Some(first));
    assert_eq!(view.blob(1), Some(second));
}

#[test]
fn canonical_blob_order_matches_writer_assignment() {
    let paths = vec![
        vec![field("b")],
        vec![field("a"), field("c")],
        vec![PathComponent::Index(2)],
        vec![PathComponent::Index(10)],
    ];
    // Encoded-bytes order: field("a")... < field("b") < index(2) < index(10)
    // (index payloads are big-endian, so byte order == numeric order; the
    // field tag 0x01 sorts before the index tag 0x03).
    let order = canonical_blob_order(&paths).expect("order");
    assert_eq!(order, vec![1, 0, 2, 3]);
}

#[test]
fn duplicate_blob_path_is_an_error() {
    let err = canonical_blob_order(&[vec![field("x")], vec![field("x")]]).unwrap_err();
    assert!(matches!(err, ArtifactError::DuplicateBlobPath { .. }));

    let b: &[u8] = &[1];
    let err = write_artifact(
        &header(),
        &[],
        &[],
        &[],
        &[(vec![field("x")], b), (vec![field("x")], b)],
    )
    .unwrap_err();
    assert!(matches!(err, ArtifactError::DuplicateBlobPath { .. }));
}

#[test]
fn blob_offsets_are_ascending_16_aligned_with_alignment_gaps_only() {
    let a: &[u8] = &[0xA; 5]; // ends at 5, next must start at 16
    let b: &[u8] = &[0xB; 16]; // ends at 32, next starts at 32
    let c: &[u8] = &[0xC; 1];
    let blobs = vec![
        (vec![field("a")], a),
        (vec![field("b")], b),
        (vec![field("c")], c),
    ];
    let bytes = write_simple(&[], &[], &[], &blobs);
    let view = parse_artifact(&bytes).expect("parse");
    assert_eq!(view.blob_table, vec![(0, 5), (16, 16), (32, 1)]);
    // File ends exactly at the last blob's end.
    assert_eq!(bytes.len() as u64, 160 + 3 * 16 + 32 + 1);
}

#[test]
fn zero_length_blobs_are_legal() {
    let empty: &[u8] = &[];
    let one: &[u8] = &[1];
    let blobs = vec![(vec![field("a")], empty), (vec![field("b")], one)];
    let bytes = write_simple(&[], &[], &[], &blobs);
    let view = parse_artifact(&bytes).expect("parse");
    assert_eq!(view.blob_table, vec![(0, 0), (0, 1)]);
    assert_eq!(view.blob(0), Some(empty));
    assert_eq!(view.blob(1), Some(one));
}

// --- reader negatives ---------------------------------------------------------

#[test]
fn rejects_bad_magic() {
    let mut bytes = write_simple(&[], &[], &[], &[]);
    bytes[0] = 0x88;
    assert!(matches!(
        parse_artifact(&bytes),
        Err(ArtifactError::BadMagic)
    ));
}

#[test]
fn rejects_unsupported_version() {
    let mut bytes = write_simple(&[], &[], &[], &[]);
    bytes[8..12].copy_from_slice(&2u32.to_le_bytes());
    assert!(matches!(
        parse_artifact(&bytes),
        Err(ArtifactError::UnsupportedVersion { got: 2 })
    ));
}

#[test]
fn rejects_truncation_at_every_length() {
    let blob: &[u8] = &[0xB; 5];
    let bytes = write_simple(
        &[AssetUuid([1; 16])],
        &[1, 2, 3],
        &[4, 5],
        &[(vec![field("a")], blob)],
    );
    for len in 0..bytes.len() {
        assert!(
            parse_artifact(&bytes[..len]).is_err(),
            "truncated to {len} bytes must not parse"
        );
    }
    assert!(parse_artifact(&bytes).is_ok());
}

#[test]
fn rejects_trailing_bytes() {
    let mut bytes = write_simple(&[], &[1], &[], &[]);
    bytes.push(0);
    assert!(matches!(
        parse_artifact(&bytes),
        Err(ArtifactError::TrailingBytes { .. })
    ));
}

#[test]
fn rejects_unsorted_load_deps() {
    let bytes = write_simple(&[AssetUuid([1; 16]), AssetUuid([2; 16])], &[], &[], &[]);
    let mut swapped = bytes.clone();
    // deps live at 160..192; swap the two entries
    let (a, b) = (160, 176);
    for i in 0..16 {
        swapped.swap(a + i, b + i);
    }
    assert!(matches!(
        parse_artifact(&swapped),
        Err(ArtifactError::DepsNotCanonical { .. })
    ));
}

#[test]
fn rejects_duplicate_load_deps() {
    let bytes = write_simple(&[AssetUuid([1; 16]), AssetUuid([2; 16])], &[], &[], &[]);
    let mut dup = bytes.clone();
    let (a, b) = (160, 176);
    for i in 0..16 {
        dup[b + i] = dup[a + i];
    }
    assert!(matches!(
        parse_artifact(&dup),
        Err(ArtifactError::DepsNotCanonical { .. })
    ));
}

#[test]
fn rejects_nonzero_padding_after_fixed() {
    let mut bytes = write_simple(&[], &[1, 2, 3], &[], &[]);
    // fixed at 160..163, pad at 163..176
    bytes[165] = 1;
    assert!(matches!(
        parse_artifact(&bytes),
        Err(ArtifactError::NonzeroPadding { .. })
    ));
}

#[test]
fn rejects_nonzero_padding_after_variable() {
    let mut bytes = write_simple(&[], &[], &[1, 2, 3], &[]);
    // variable at 160..163, pad at 163..176
    bytes[175] = 7;
    assert!(matches!(
        parse_artifact(&bytes),
        Err(ArtifactError::NonzeroPadding { .. })
    ));
}

#[test]
fn rejects_nonzero_blob_gap() {
    let a: &[u8] = &[0xA; 5];
    let b: &[u8] = &[0xB; 1];
    let blobs = vec![(vec![field("a")], a), (vec![field("b")], b)];
    let mut bytes = write_simple(&[], &[], &[], &blobs);
    // blob section starts at 160 + 2*16 = 192; gap is 192+5 .. 192+16
    bytes[192 + 9] = 0xFF;
    assert!(matches!(
        parse_artifact(&bytes),
        Err(ArtifactError::NonzeroPadding { .. })
    ));
}

#[test]
fn rejects_var_len_at_or_above_2_pow_32() {
    let mut bytes = write_simple(&[], &[], &[], &[]);
    bytes[152..160].copy_from_slice(&(1u64 << 32).to_le_bytes());
    assert!(matches!(
        parse_artifact(&bytes),
        Err(ArtifactError::VarLenTooLarge { .. })
    ));
}

#[test]
fn rejects_huge_dep_count_without_overflow_panic() {
    let mut bytes = write_simple(&[], &[], &[], &[]);
    bytes[140..144].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(parse_artifact(&bytes).is_err());
}

#[test]
fn rejects_huge_blob_count_without_overflow_panic() {
    let mut bytes = write_simple(&[], &[], &[], &[]);
    bytes[144..148].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(parse_artifact(&bytes).is_err());
}

fn patch_blob_table(bytes: &mut [u8], entry: usize, offset: u64, len: u64) {
    let base = 160 + entry * 16;
    bytes[base..base + 8].copy_from_slice(&offset.to_le_bytes());
    bytes[base + 8..base + 16].copy_from_slice(&len.to_le_bytes());
}

#[test]
fn rejects_blob_table_first_offset_nonzero() {
    // One blob of len 16: table (0,16), section 16 bytes at 192.
    let b: &[u8] = &[0xB; 16];
    let mut bytes = write_simple(&[], &[], &[], &[(vec![field("a")], b)]);
    patch_blob_table(&mut bytes, 0, 16, 16);
    assert!(matches!(
        parse_artifact(&bytes),
        Err(ArtifactError::BlobTableInvalid { index: 0, .. })
    ));
}

#[test]
fn rejects_blob_table_misaligned_or_overlapping_offset() {
    let a: &[u8] = &[0xA; 16];
    let b: &[u8] = &[0xB; 16];
    let blobs = vec![(vec![field("a")], a), (vec![field("b")], b)];
    let base = write_simple(&[], &[], &[], &blobs);

    // Overlap: second entry points back into the first.
    let mut overlap = base.clone();
    patch_blob_table(&mut overlap, 1, 0, 16);
    assert!(matches!(
        parse_artifact(&overlap),
        Err(ArtifactError::BlobTableInvalid { index: 1, .. })
    ));

    // Misaligned: second entry at offset 17 (also not the exact expected 16).
    let mut misaligned = base.clone();
    patch_blob_table(&mut misaligned, 1, 17, 15);
    assert!(matches!(
        parse_artifact(&misaligned),
        Err(ArtifactError::BlobTableInvalid { index: 1, .. })
    ));

    // Oversized gap: second entry at offset 32 leaves a 16-byte gap.
    let mut gapped = base.clone();
    patch_blob_table(&mut gapped, 1, 32, 0);
    assert!(matches!(
        parse_artifact(&gapped),
        Err(ArtifactError::BlobTableInvalid { index: 1, .. })
    ));
}

#[test]
fn rejects_blob_table_length_overflow() {
    let b: &[u8] = &[0xB; 16];
    let mut bytes = write_simple(&[], &[], &[], &[(vec![field("a")], b)]);
    patch_blob_table(&mut bytes, 0, 0, u64::MAX);
    // Checked arithmetic: error, never a wrapping panic.
    assert!(parse_artifact(&bytes).is_err());
}
