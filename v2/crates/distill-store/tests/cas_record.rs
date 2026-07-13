//! §13's pinned CAS record frame and the result-payload grammar: byte
//! layout pinning, roundtrips for every kind, and decode negatives —
//! malformed input is a definite error, checked before allocation.

use distill_core::canonical::{domain_digest, DSTR};
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, TypeUuid};
use distill_store::cas::record::{
    decode_record, encode_record, AuxRow, CapabilityKey, FailureCause, FailureFingerprint, KeyKind,
    LocalFailureClass, OutputRow, Record, RecordKind, ResultOutcome, ResultPayload, ToolErrorClass,
    RECORD_HEADER_LEN,
};
use distill_store::StoreError;

fn sample(kind: RecordKind) -> Record {
    Record {
        kind,
        asset_uuid: AssetUuid([7u8; 16]),
        static_input_key: vec![0xAA; 32],
        output_key: "normals".to_owned(),
        payload: b"payload bytes".to_vec(),
    }
}

// ---- frame layout pinning ----

#[test]
fn header_layout_is_pinned() {
    let rec = sample(RecordKind::ProcessorOutput);
    let bytes = encode_record(&rec);

    assert_eq!(&bytes[0..4], b"DSR1", "magic");
    assert_eq!(bytes[4], 1, "version");
    assert_eq!(bytes[5], 1, "kind: processor output = 1");
    assert_eq!(u16::from_le_bytes([bytes[6], bytes[7]]), 32, "key_len");
    assert_eq!(u16::from_le_bytes([bytes[8], bytes[9]]), 7, "out_key_len");
    assert_eq!(&bytes[10..26], &[7u8; 16], "asset_uuid");
    assert_eq!(
        u64::from_le_bytes(bytes[26..34].try_into().unwrap()),
        13,
        "payload_len"
    );
    assert_eq!(
        &bytes[34..66],
        blake3::hash(b"payload bytes").as_bytes(),
        "content_hash = blake3(payload)"
    );
    // crc32c at [66..70]; variable sections follow.
    assert_eq!(RECORD_HEADER_LEN, 70);
    assert_eq!(&bytes[70..102], &[0xAA; 32], "static_input_key");
    assert_eq!(&bytes[102..109], b"normals", "output_key");
    assert_eq!(&bytes[109..122], b"payload bytes", "payload");
}

#[test]
fn crc_coverage_is_the_exact_two_disjoint_ranges() {
    // R22/L14: exclude magic, version, the CRC field, and alignment pad;
    // cover kind..content_hash plus key/output-key/payload.
    let bytes = encode_record(&sample(RecordKind::ProcessorOutput));
    let stored = u32::from_le_bytes(bytes[66..70].try_into().unwrap());
    let content_len = RECORD_HEADER_LEN + 32 + "normals".len() + b"payload bytes".len();
    let mut covered = Vec::new();
    covered.extend_from_slice(&bytes[5..66]);
    covered.extend_from_slice(&bytes[RECORD_HEADER_LEN..content_len]);
    assert_eq!(stored, crc32c(&covered));

    let mut self_referential = covered;
    self_referential.extend_from_slice(&bytes[66..70]);
    assert_ne!(
        stored,
        crc32c(&self_referential),
        "CRC does not cover itself"
    );
}

#[test]
fn records_pad_with_zeros_to_16_byte_alignment() {
    for payload_len in [0usize, 1, 15, 16, 17, 100] {
        let rec = Record {
            kind: RecordKind::Debug,
            asset_uuid: AssetUuid([0u8; 16]),
            static_input_key: Vec::new(),
            output_key: String::new(),
            payload: vec![0xCC; payload_len],
        };
        let bytes = encode_record(&rec);
        assert_eq!(bytes.len() % 16, 0, "aligned for payload_len {payload_len}");
        let content_end = RECORD_HEADER_LEN + payload_len;
        assert!(bytes[content_end..].iter().all(|&b| b == 0), "pad is zeros");
    }
}

#[test]
fn kind_bytes_are_pinned() {
    for (kind, byte) in [
        (RecordKind::ImportEncoding, 0u8),
        (RecordKind::ProcessorOutput, 1),
        (RecordKind::Debug, 2),
        (RecordKind::WireTree, 3),
        (RecordKind::Result, 4),
    ] {
        let bytes = encode_record(&sample(kind));
        assert_eq!(bytes[5], byte);
        assert_eq!(kind.is_payload(), byte != 4);
    }
}

// ---- roundtrips ----

#[test]
fn every_kind_roundtrips() {
    for kind in [
        RecordKind::ImportEncoding,
        RecordKind::ProcessorOutput,
        RecordKind::Debug,
        RecordKind::WireTree,
        RecordKind::Result,
    ] {
        let rec = sample(kind);
        let bytes = encode_record(&rec);
        let decoded = decode_record(&bytes, 0, 0).unwrap();
        assert_eq!(decoded.record, rec);
        assert_eq!(decoded.encoded_len as usize, bytes.len());
        assert_eq!(decoded.content_hash, *blake3::hash(&rec.payload).as_bytes());
    }
}

#[test]
fn empty_keys_and_payload_roundtrip() {
    // The primary output's key is the empty string (§13).
    let rec = Record {
        kind: RecordKind::ProcessorOutput,
        asset_uuid: AssetUuid([1u8; 16]),
        static_input_key: Vec::new(),
        output_key: String::new(),
        payload: Vec::new(),
    };
    let bytes = encode_record(&rec);
    let decoded = decode_record(&bytes, 0, 0).unwrap();
    assert_eq!(decoded.record, rec);
}

#[test]
fn a_wire_tree_record_hash_is_its_layout_hash() {
    // §13: a wire tree's blake3 IS the LayoutHash.
    let tree_bytes = b"canonical DSWL serialization";
    let rec = Record {
        kind: RecordKind::WireTree,
        asset_uuid: AssetUuid([0u8; 16]),
        static_input_key: Vec::new(),
        output_key: String::new(),
        payload: tree_bytes.to_vec(),
    };
    let bytes = encode_record(&rec);
    let decoded = decode_record(&bytes, 0, 0).unwrap();
    assert_eq!(decoded.content_hash, *blake3::hash(tree_bytes).as_bytes());
}

// ---- decode negatives ----

fn expect_bad(bytes: &[u8], needle: &str) {
    match decode_record(bytes, 3, 160) {
        Err(StoreError::BadRecord {
            segment,
            offset,
            detail,
        }) => {
            assert_eq!(segment, 3);
            assert_eq!(offset, 160);
            assert!(
                detail.contains(needle),
                "detail `{detail}` missing `{needle}`"
            );
        }
        other => panic!("expected BadRecord({needle}), got {other:?}"),
    }
}

#[test]
fn truncated_header_is_a_definite_error() {
    let bytes = encode_record(&sample(RecordKind::Debug));
    expect_bad(&bytes[..RECORD_HEADER_LEN - 1], "header");
    expect_bad(&[], "header");
}

#[test]
fn bad_magic_is_rejected() {
    let mut bytes = encode_record(&sample(RecordKind::Debug));
    bytes[0] = b'X';
    expect_bad(&bytes, "magic");
}

#[test]
fn unsupported_version_is_rejected() {
    let mut bytes = encode_record(&sample(RecordKind::Debug));
    bytes[4] = 2;
    expect_bad(&bytes, "version");
}

#[test]
fn unknown_kind_is_rejected() {
    let mut bytes = encode_record(&sample(RecordKind::Debug));
    bytes[5] = 9;
    expect_bad(&bytes, "kind");
}

#[test]
fn oversized_payload_len_errors_before_allocation() {
    // §13: all lengths are checked before allocation.
    let mut bytes = encode_record(&sample(RecordKind::Debug));
    bytes[26..34].copy_from_slice(&u64::MAX.to_le_bytes());
    match decode_record(&bytes, 0, 0) {
        Err(StoreError::OversizedRecord { payload_len, .. }) => {
            assert_eq!(payload_len, u64::MAX);
        }
        other => panic!("expected OversizedRecord, got {other:?}"),
    }
}

#[test]
fn declared_lengths_beyond_the_buffer_are_truncation() {
    let mut bytes = encode_record(&sample(RecordKind::Debug));
    // Claim a slightly longer payload than the buffer holds.
    let actual = u64::from_le_bytes(bytes[26..34].try_into().unwrap());
    bytes[26..34].copy_from_slice(&(actual + 1000).to_le_bytes());
    match decode_record(&bytes, 0, 0) {
        Err(StoreError::OversizedRecord { .. }) => {}
        other => panic!("expected OversizedRecord, got {other:?}"),
    }
}

#[test]
fn crc_corruption_is_detected() {
    let mut bytes = encode_record(&sample(RecordKind::Debug));
    // Flip a payload byte: crc32c over kind…payload no longer matches.
    let n = bytes.len();
    bytes[RECORD_HEADER_LEN + 5] ^= 0xFF;
    let _ = n;
    expect_bad(&bytes, "crc");
}

#[test]
fn content_hash_is_verified_not_just_crc() {
    // §13 recovery verifies each payload against its stored blake3, not
    // just CRC — so a record whose CRC was recomputed over corrupt bytes
    // still fails.
    let rec = sample(RecordKind::ProcessorOutput);
    let mut bytes = encode_record(&rec);
    bytes[RECORD_HEADER_LEN + 32 + 7] ^= 0x01; // flip a payload byte
                                               // Recompute the CRC field so the CRC check passes.
    let crc = distill_bundle_crc(&bytes);
    bytes[66..70].copy_from_slice(&crc.to_le_bytes());
    expect_bad(&bytes, "blake3");
}

/// CRC-32C over the frame's covered range (kind…payload, crc field
/// skipped), recomputed the way the encoder pins it.
fn distill_bundle_crc(bytes: &[u8]) -> u32 {
    let key_len = u16::from_le_bytes([bytes[6], bytes[7]]) as usize;
    let out_len = u16::from_le_bytes([bytes[8], bytes[9]]) as usize;
    let payload_len = u64::from_le_bytes(bytes[26..34].try_into().unwrap()) as usize;
    let end = RECORD_HEADER_LEN + key_len + out_len + payload_len;
    let mut covered = Vec::new();
    covered.extend_from_slice(&bytes[5..66]);
    covered.extend_from_slice(&bytes[RECORD_HEADER_LEN..end]);
    crc32c(&covered)
}

/// Reference CRC-32C (Castagnoli), matching distill-bundle's pinned
/// implementation.
fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[test]
fn nonzero_pad_bytes_are_rejected() {
    let mut bytes = encode_record(&sample(RecordKind::Debug));
    let last = bytes.len() - 1;
    let content_end = RECORD_HEADER_LEN + 32 + 7 + 13;
    assert!(last >= content_end, "sample has pad bytes");
    bytes[last] = 0x55;
    expect_bad(&bytes, "pad");
}

#[test]
fn invalid_utf8_output_key_is_rejected() {
    let mut bytes = encode_record(&sample(RecordKind::ProcessorOutput));
    bytes[RECORD_HEADER_LEN + 32] = 0xFF; // first output_key byte
    let crc = distill_bundle_crc(&bytes);
    bytes[66..70].copy_from_slice(&crc.to_le_bytes());
    expect_bad(&bytes, "UTF-8");
}

// ---- result payload grammar ----

fn success_payload(key_kind: KeyKind) -> ResultPayload {
    ResultPayload {
        key_kind,
        static_inputs_canonical: vec![1, 2, 3, 4],
        trace: b"canonical trace ops".to_vec(),
        outcome: ResultOutcome::Success {
            outputs: vec![
                OutputRow {
                    output_key: String::new(),
                    type_uuids: vec![TypeUuid([1u8; 16]), TypeUuid([2u8; 16])],
                    content_hash: ContentHash([3u8; 32]),
                },
                OutputRow {
                    output_key: "normals".to_owned(),
                    type_uuids: vec![TypeUuid([4u8; 16])],
                    content_hash: ContentHash([5u8; 32]),
                },
            ],
            aux: vec![AuxRow {
                debug_key: "atlas-debug".to_owned(),
                content_hash: ContentHash([6u8; 32]),
            }],
        },
    }
}

#[test]
fn result_payload_success_roundtrips() {
    for key_kind in [KeyKind::Processor, KeyKind::BuildImport] {
        let payload = success_payload(key_kind);
        let bytes = payload.encode();
        let decoded = ResultPayload::decode(&bytes).unwrap();
        assert_eq!(decoded, payload);
    }
}

#[test]
fn key_kind_bytes_are_pinned() {
    // §13: result records are tagged by key kind — a key_kind byte in
    // the result payload. DSSI (processor) = 0, DSBI (build import) = 1.
    let bytes = success_payload(KeyKind::Processor).encode();
    assert_eq!(bytes[0], 0);
    let bytes = success_payload(KeyKind::BuildImport).encode();
    assert_eq!(bytes[0], 1);
}

#[test]
fn failure_outcomes_roundtrip_with_every_fingerprint() {
    // §9/§13 (R21): a failure record is a dependency trace (possibly
    // empty) plus a terminal FailureCause — FailureCause::Op when the
    // trace's own terminal entry (an Observed::Err op) is the cause, or
    // FailureCause::Local(fingerprint) for deterministic local failures
    // no operation produced. No output rows either way.
    let fingerprints = vec![
        FailureFingerprint::Ambiguous {
            conflicting: vec![AssetUuid([1u8; 16]), AssetUuid([2u8; 16])],
        },
        FailureFingerprint::Poisoned {
            bundle: BundleUuid([3u8; 16]),
        },
        FailureFingerprint::MissingRef {
            query: b"canonical ASTQ bytes".to_vec(),
            expected_terminal: TypeUuid([4u8; 16]),
        },
        FailureFingerprint::Descendant {
            asset: AssetUuid([5u8; 16]),
            fingerprint: Box::new(FailureFingerprint::ToolLaunch {
                id: "shaderc".to_owned(),
                class: ToolErrorClass::MissingInterpreter,
            }),
        },
        FailureFingerprint::ToolLaunch {
            id: "dxc".to_owned(),
            class: ToolErrorClass::SpawnDenied,
        },
        FailureFingerprint::MissingCapability {
            key: CapabilityKey::MigrationFn("legacy-v2-to-v3".to_owned()),
        },
        FailureFingerprint::MissingCapability {
            key: CapabilityKey::DefaultTable(TypeUuid([6u8; 16])),
        },
        FailureFingerprint::MissingCapability {
            key: CapabilityKey::Importer("gltf".to_owned()),
        },
        FailureFingerprint::MissingCapability {
            key: CapabilityKey::Processor {
                input: TypeUuid([7u8; 16]),
            },
        },
        FailureFingerprint::Local {
            class: LocalFailureClass::Validator,
            detail: [8u8; 32],
        },
        FailureFingerprint::Local {
            class: LocalFailureClass::MigrationPlan,
            detail: [9u8; 32],
        },
        FailureFingerprint::Local {
            class: LocalFailureClass::Processor,
            detail: [10u8; 32],
        },
    ];
    for fp in fingerprints {
        let payload = ResultPayload {
            key_kind: KeyKind::Processor,
            static_inputs_canonical: vec![9],
            trace: b"trace up to the failure".to_vec(),
            outcome: ResultOutcome::Failure {
                cause: FailureCause::Local(fp.clone()),
            },
        };
        let decoded = ResultPayload::decode(&payload.encode()).unwrap();
        assert_eq!(decoded, payload);
        match decoded.outcome {
            ResultOutcome::Failure {
                cause: FailureCause::Local(fingerprint),
            } => {
                assert_eq!(fingerprint, fp)
            }
            _ => panic!("failure expected"),
        }
    }
}

#[test]
fn op_caused_failures_carry_no_standalone_fingerprint() {
    // §13: for a failing context operation the trace ends in its
    // Observed::Err entry — the cause is FailureCause::Op, and the
    // record invents no synthetic fingerprint beside the trace.
    let payload = ResultPayload {
        key_kind: KeyKind::Processor,
        static_inputs_canonical: vec![1],
        trace: b"trace ending in the failing Observed::Err op".to_vec(),
        outcome: ResultOutcome::Failure {
            cause: FailureCause::Op,
        },
    };
    let decoded = ResultPayload::decode(&payload.encode()).unwrap();
    assert_eq!(decoded, payload);
}

#[test]
fn local_failures_memoize_with_an_empty_trace() {
    // §9 (R21/M13): validator diagnostics, migration-plan validation,
    // and processor BuildErrors arise from no context operation — the
    // trace may be EMPTY, and nothing invents a synthetic op for it.
    let payload = ResultPayload {
        key_kind: KeyKind::Processor,
        static_inputs_canonical: vec![2],
        trace: Vec::new(),
        outcome: ResultOutcome::Failure {
            cause: FailureCause::Local(FailureFingerprint::Local {
                class: LocalFailureClass::Validator,
                detail: *blake3::hash(b"typed diagnostic").as_bytes(),
            }),
        },
    };
    let decoded = ResultPayload::decode(&payload.encode()).unwrap();
    assert_eq!(decoded, payload);
}

#[test]
fn failure_cause_and_fingerprint_tag_bytes_are_pinned() {
    // The failure arm's grammar, byte by byte: outcome tag, cause tag
    // (0 = Op, 1 = Local), fingerprint tag (5 = MissingCapability,
    // 6 = Local), capability-key tag, class byte.
    let op = ResultPayload {
        key_kind: KeyKind::Processor,
        static_inputs_canonical: Vec::new(),
        trace: Vec::new(),
        outcome: ResultOutcome::Failure {
            cause: FailureCause::Op,
        },
    };
    let bytes = op.encode();
    assert_eq!(bytes[1], 1, "outcome tag: failure");
    // key_kind, outcome, static_inputs len(4), trace len(4) → cause tag.
    assert_eq!(bytes[10], 0, "cause tag: Op");
    assert_eq!(bytes.len(), 11, "nothing follows an Op cause");

    let cap = ResultPayload {
        key_kind: KeyKind::Processor,
        static_inputs_canonical: Vec::new(),
        trace: Vec::new(),
        outcome: ResultOutcome::Failure {
            cause: FailureCause::Local(FailureFingerprint::MissingCapability {
                key: CapabilityKey::Processor {
                    input: TypeUuid([7u8; 16]),
                },
            }),
        },
    };
    let bytes = cap.encode();
    assert_eq!(bytes[10], 1, "cause tag: Local");
    assert_eq!(bytes[11], 5, "fingerprint tag: MissingCapability");
    assert_eq!(bytes[12], 3, "capability-key tag: Processor");
    assert_eq!(&bytes[13..29], &[7u8; 16], "the requested input type uuid");

    let local = ResultPayload {
        key_kind: KeyKind::Processor,
        static_inputs_canonical: Vec::new(),
        trace: Vec::new(),
        outcome: ResultOutcome::Failure {
            cause: FailureCause::Local(FailureFingerprint::Local {
                class: LocalFailureClass::MigrationPlan,
                detail: [3u8; 32],
            }),
        },
    };
    let bytes = local.encode();
    assert_eq!(bytes[11], 6, "fingerprint tag: Local");
    assert_eq!(bytes[12], 1, "class byte: MigrationPlan");
    assert_eq!(&bytes[13..45], &[3u8; 32], "the stable diagnostic hash");
}

#[test]
fn trace_digest_is_the_dstr_domain_digest() {
    let payload = success_payload(KeyKind::Processor);
    let expected = domain_digest(DSTR, 1, |e| e.raw(&payload.trace));
    assert_eq!(payload.trace_digest(), expected);
}

#[test]
fn result_payload_decode_negatives() {
    let good = success_payload(KeyKind::Processor).encode();

    // Truncations at every boundary are definite errors.
    for cut in [0, 1, 2, 5, good.len() - 1] {
        assert!(
            matches!(
                ResultPayload::decode(&good[..cut]),
                Err(StoreError::BadResultPayload { .. })
            ),
            "cut at {cut}"
        );
    }

    // Unknown key_kind byte.
    let mut bad = good.clone();
    bad[0] = 7;
    assert!(matches!(
        ResultPayload::decode(&bad),
        Err(StoreError::BadResultPayload { .. })
    ));

    // Unknown outcome tag.
    let mut bad = good.clone();
    bad[1] = 9;
    assert!(matches!(
        ResultPayload::decode(&bad),
        Err(StoreError::BadResultPayload { .. })
    ));

    // Trailing garbage is rejected — the grammar is exact.
    let mut bad = good.clone();
    bad.push(0);
    assert!(matches!(
        ResultPayload::decode(&bad),
        Err(StoreError::BadResultPayload { .. })
    ));

    // An absurd count fails before allocation.
    let mut bad = good;
    let count_at = 2 + 4 + 4 + 4 + b"canonical trace ops".len();
    bad[count_at..count_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        ResultPayload::decode(&bad),
        Err(StoreError::BadResultPayload { .. })
    ));
}

#[test]
fn hostile_descendant_nesting_is_capped() {
    // A corrupt payload cannot recurse the decoder off the stack.
    let mut fp = FailureFingerprint::Poisoned {
        bundle: BundleUuid([0u8; 16]),
    };
    for _ in 0..10_000 {
        fp = FailureFingerprint::Descendant {
            asset: AssetUuid([1u8; 16]),
            fingerprint: Box::new(fp),
        };
    }
    let payload = ResultPayload {
        key_kind: KeyKind::Processor,
        static_inputs_canonical: Vec::new(),
        trace: Vec::new(),
        outcome: ResultOutcome::Failure {
            cause: FailureCause::Local(fp),
        },
    };
    let bytes = payload.encode();
    assert!(matches!(
        ResultPayload::decode(&bytes),
        Err(StoreError::BadResultPayload { .. })
    ));
}
