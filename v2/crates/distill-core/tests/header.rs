//! §5 shared file header: `NGPF`, `u32` Domain, `u32` version; one reader
//! refuses a wrong magic, domain or version by name.

use distill_core::hash::Domain;
use distill_core::header::{header, read_header, write_header, HeaderError, HEADER_LEN};

#[test]
fn header_is_magic_domain_version() {
    let bytes = header(Domain::PackArchive, 4);
    assert_eq!(&bytes[..4], b"NGPF");
    assert_eq!(&bytes[4..8], &(Domain::PackArchive as u32).to_le_bytes());
    assert_eq!(&bytes[8..12], &4u32.to_le_bytes());
    assert_eq!(bytes.len(), HEADER_LEN);
}

#[test]
fn reader_returns_the_body() {
    let mut file = Vec::new();
    write_header(&mut file, Domain::Bundle, 2);
    file.extend_from_slice(b"body");
    assert_eq!(read_header(&file, Domain::Bundle, 2).unwrap(), b"body");
}

#[test]
fn reader_refuses_wrong_magic_domain_and_version_by_name() {
    let file = header(Domain::ArtifactImage, 1);
    assert_eq!(
        read_header(&file[..11], Domain::ArtifactImage, 1),
        Err(HeaderError::TooShort { len: 11 })
    );
    let mut bad = file;
    bad[0] = b'X';
    assert_eq!(
        read_header(&bad, Domain::ArtifactImage, 1),
        Err(HeaderError::BadMagic { found: *b"XGPF" })
    );
    let error = read_header(&file, Domain::PackManifest, 1).unwrap_err();
    assert_eq!(
        error,
        HeaderError::WrongDomain {
            expected: Domain::PackManifest,
            found: Domain::ArtifactImage as u32,
        }
    );
    assert_eq!(
        error.to_string(),
        "a ArtifactImage file where a PackManifest file belongs"
    );
    let error = read_header(&file, Domain::ArtifactImage, 2).unwrap_err();
    assert_eq!(
        error,
        HeaderError::UnsupportedVersion {
            domain: Domain::ArtifactImage,
            expected: 2,
            found: 1,
        }
    );
    assert_eq!(
        error.to_string(),
        "ArtifactImage file version 1 is not the supported version 2"
    );
}
