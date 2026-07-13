use distill_core::id::ContentHash;
use distill_pack::archive::*;

#[test]
fn structural_bytes_use_fixed_chunks_and_blobs_remain_contiguous() {
    let structural = vec![0xAB; STRUCTURAL_CHUNK_SIZE + 17];
    let blob = vec![0xCD; STRUCTURAL_CHUNK_SIZE + 99];
    let artifact = ArtifactPayload {
        content_hash: ContentHash([1; 32]),
        structural: structural.clone(),
        blobs: vec![blob.clone()],
    };
    let built = encode_archive(7, "zstd-test", 3, &[artifact]).unwrap();
    let encoding = &built.encodings[&ContentHash([1; 32])];
    assert_eq!(encoding.blocks.len(), 2);
    assert_eq!(encoding.blobs.len(), 1);
    let decoded = decode_archive(&built.bytes).unwrap();
    let blocks: Vec<_> = encoding
        .blocks
        .iter()
        .map(|key| decoded.objects[key].raw.clone())
        .collect();
    assert_eq!(
        [blocks[0].as_slice(), blocks[1].as_slice()].concat(),
        structural
    );
    assert_eq!(decoded.objects[&encoding.blobs[0]].raw, blob);
    assert_eq!(
        decoded.objects[&encoding.blobs[0]].kind,
        ArchiveObjectKind::Blob
    );
}

#[test]
fn ekey_hashes_stored_payload_only_and_index_points_at_exact_payload() {
    let payload = ArtifactPayload {
        content_hash: ContentHash([2; 32]),
        structural: b"same".to_vec(),
        blobs: vec![],
    };
    let a = encode_archive(1, "encoder-a", 1, std::slice::from_ref(&payload)).unwrap();
    let b = encode_archive(99, "encoder-b", 1, &[payload]).unwrap();
    let ka = a.encodings[&ContentHash([2; 32])].blocks[0];
    let kb = b.encodings[&ContentHash([2; 32])].blocks[0];
    assert_eq!(ka, kb);
    let loc = a.index[&ka];
    let stored = &a.bytes[loc.offset as usize..(loc.offset + loc.len) as usize];
    assert_eq!(ekey(stored), ka);
}

#[test]
fn archive_rejects_bad_trailer_crc_ekey_and_truncation() {
    let payload = ArtifactPayload {
        content_hash: ContentHash([3; 32]),
        structural: b"hello".to_vec(),
        blobs: vec![],
    };
    let built = encode_archive(1, "test", 1, &[payload]).unwrap();
    for len in 0..built.bytes.len() {
        assert!(
            decode_archive(&built.bytes[..len]).is_err(),
            "accepted truncation {len}"
        );
    }
    let mut bad = built.bytes.clone();
    bad[0] ^= 1;
    assert!(matches!(decode_archive(&bad), Err(ArchiveError::FileHash)));

    let key = built.encodings[&ContentHash([3; 32])].blocks[0];
    let loc = built.index[&key];
    let mut bad = built.bytes.clone();
    bad[loc.offset as usize] ^= 1;
    resign(&mut bad);
    assert!(matches!(
        decode_archive(&bad),
        Err(ArchiveError::Crc) | Err(ArchiveError::EKey)
    ));
}

fn resign(bytes: &mut [u8]) {
    let body = bytes.len() - 32;
    let hash = *blake3::hash(&bytes[..body]).as_bytes();
    bytes[body..].copy_from_slice(&hash);
}
