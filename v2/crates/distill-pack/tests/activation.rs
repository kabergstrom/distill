use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use distill_core::id::ContentHash;
use distill_pack::activation::*;
use distill_pack::archive::{encode_archive, ArtifactPayload};

#[test]
fn pointer_grammar_is_exactly_lowercase_hex_plus_newline() {
    let hash = [0xAB; 32];
    let bytes = pointer_bytes(hash);
    assert_eq!(bytes.len(), 65);
    assert_eq!(&bytes[..4], b"abab");
    assert_eq!(bytes[64], b'\n');
    assert_eq!(parse_pointer(&bytes).unwrap(), hash);
    assert!(parse_pointer(&bytes[..64]).is_err());
    let mut upper = bytes;
    upper[0] = b'A';
    assert!(parse_pointer(&upper).is_err());
}

#[test]
fn activation_atomically_replaces_pointer_and_leaves_no_temp() {
    let dir = std::env::temp_dir().join(format!(
        "distill-pack-activate-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&dir).unwrap();
    activate(&dir, [1; 32]).unwrap();
    assert_eq!(read_current(&dir).unwrap(), [1; 32]);
    activate(&dir, [2; 32]).unwrap();
    assert_eq!(read_current(&dir).unwrap(), [2; 32]);
    assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn archive_names_are_hash_addressed_and_publication_is_no_replace() {
    let dir = std::env::temp_dir().join(format!(
        "distill-pack-archive-publish-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&dir).unwrap();
    let archive = encode_archive(
        1,
        "test",
        1,
        &[ArtifactPayload {
            content_hash: ContentHash([1; 32]),
            structural: vec![1, 2, 3],
            blobs: vec![],
        }],
    )
    .unwrap();
    let hash = publish_archive(&dir, &archive.bytes).unwrap();
    assert_eq!(hash, *blake3::hash(&archive.bytes).as_bytes());
    assert_ne!(hash.as_slice(), &archive.bytes[archive.bytes.len() - 32..]);
    let expected = archive_filename(hash);
    assert!(expected.starts_with("archive-"));
    assert_eq!(expected.len(), "archive-".len() + 64 + ".dpk".len());
    assert_eq!(fs::read(dir.join(&expected)).unwrap(), archive.bytes);
    publish_archive(&dir, &archive.bytes).unwrap();
    assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);

    fs::remove_file(dir.join(&expected)).unwrap();
    fs::write(dir.join(&expected), b"different immutable bytes").unwrap();
    assert!(matches!(
        publish_archive(&dir, &archive.bytes),
        Err(PointerError::ImmutableConflict(_))
    ));
    fs::remove_dir_all(dir).unwrap();
}
