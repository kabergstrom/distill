use distill_core::id::LogicalHash;
use distill_core::lineage::{
    AcceptedSchemaEpoch, EntryLineageCodecError, EntryLineageV1, LineageStamp,
};

fn manifest() -> EntryLineageV1 {
    EntryLineageV1::Manifest(LineageStamp {
        epochs: vec![
            AcceptedSchemaEpoch {
                digest: LogicalHash([0x11; 32]),
                forward_parent: None,
            },
            AcceptedSchemaEpoch {
                digest: LogicalHash([0x22; 32]),
                forward_parent: Some(0),
            },
        ],
        cursor: 1,
        chain: [0x33; 32],
    })
}

#[test]
fn entry_lineage_record_bytes_are_pinned_and_roundtrip() {
    let encoded = manifest().encode_record().unwrap();
    let mut expected = vec![1, 1];
    expected.extend_from_slice(&2_u32.to_le_bytes());
    expected.extend_from_slice(&[0x11; 32]);
    expected.push(0);
    expected.extend_from_slice(&[0x22; 32]);
    expected.push(1);
    expected.extend_from_slice(&0_u32.to_le_bytes());
    expected.extend_from_slice(&1_u32.to_le_bytes());
    expected.extend_from_slice(&[0x33; 32]);
    assert_eq!(encoded, expected);
    assert_eq!(EntryLineageV1::decode_record(&encoded).unwrap(), manifest());

    let bootstrap = EntryLineageV1::Bootstrap {
        bundle_format_version: 1,
    };
    assert_eq!(bootstrap.encode_record().unwrap(), [1, 2, 1, 0, 0, 0]);
    assert_eq!(
        EntryLineageV1::decode_record(&[1, 2, 1, 0, 0, 0]).unwrap(),
        bootstrap
    );
}

#[test]
fn entry_lineage_decoder_is_strict() {
    assert_eq!(
        EntryLineageV1::decode_record(&[]),
        Err(EntryLineageCodecError::Truncated)
    );
    assert_eq!(
        EntryLineageV1::decode_record(&[2, 2, 1, 0, 0, 0]),
        Err(EntryLineageCodecError::UnsupportedVersion(2))
    );
    assert_eq!(
        EntryLineageV1::decode_record(&[1, 9]),
        Err(EntryLineageCodecError::UnknownTag(9))
    );
    assert_eq!(
        EntryLineageV1::decode_record(&[1, 2, 1, 0, 0, 0, 0]),
        Err(EntryLineageCodecError::TrailingBytes)
    );

    let mut bad_option = manifest().encode_record().unwrap();
    bad_option[2 + 4 + 32] = 2;
    assert_eq!(
        EntryLineageV1::decode_record(&bad_option),
        Err(EntryLineageCodecError::InvalidOptionTag(2))
    );
}
