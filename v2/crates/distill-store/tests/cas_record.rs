//! §13's result row: the pinned encoding of a deterministic failure's
//! cause (`results.failure`) — roundtrips for every fingerprint, tag
//! bytes, and decode negatives (malformed input is a definite error,
//! checked before allocation) — and the trace digest.

use distill_core::hash::digest;
use distill_core::hash::Domain;
use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};
use distill_store::cas::record::{
    trace_digest, CapabilityKey, EntryRole, FailureCause, FailureFingerprint, LocalFailureClass,
};
use distill_store::StoreError;

fn bad(bytes: &[u8]) -> bool {
    matches!(
        FailureCause::decode(bytes),
        Err(StoreError::BadResultPayload { .. })
    )
}

#[test]
fn failure_causes_roundtrip_with_every_fingerprint() {
    // §9/§13 (R21): a failure record is a dependency trace (possibly
    // empty) plus a terminal FailureCause — FailureCause::Op when the
    // trace's own terminal entry (an Observed::Err op) is the cause, or
    // FailureCause::Local(fingerprint) for deterministic local failures
    // no operation produced.
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
        FailureFingerprint::RoleIneligible {
            asset: AssetUuid([11u8; 16]),
            observed_role: EntryRole::AuthoringOnly,
        },
        FailureFingerprint::Descendant {
            asset: AssetUuid([5u8; 16]),
            fingerprint: Box::new(FailureFingerprint::MissingCapability {
                key: CapabilityKey::Tool("shaderc".to_owned()),
            }),
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
        FailureFingerprint::MissingCapability {
            key: CapabilityKey::Tool("shaderc".to_owned()),
        },
        FailureFingerprint::Local {
            class: LocalFailureClass::Validator,
            detail: [8u8; 16],
        },
        FailureFingerprint::Local {
            class: LocalFailureClass::MigrationPlan,
            detail: [9u8; 16],
        },
        FailureFingerprint::Local {
            class: LocalFailureClass::Processor,
            detail: [10u8; 16],
        },
        FailureFingerprint::Local {
            class: LocalFailureClass::MigrationFunction,
            detail: [11u8; 16],
        },
        FailureFingerprint::Local {
            class: LocalFailureClass::OutputBinding,
            detail: [12u8; 16],
        },
        FailureFingerprint::Local {
            class: LocalFailureClass::Importer,
            detail: [13u8; 16],
        },
        FailureFingerprint::Local {
            class: LocalFailureClass::ImportIntake,
            detail: [14u8; 16],
        },
        FailureFingerprint::Local {
            class: LocalFailureClass::ArtifactEncoding,
            detail: [15u8; 16],
        },
    ];
    for fingerprint in fingerprints {
        let cause = FailureCause::Local(fingerprint);
        assert_eq!(FailureCause::decode(&cause.encode()).unwrap(), cause);
    }
    assert_eq!(
        FailureCause::decode(&FailureCause::Op.encode()).unwrap(),
        FailureCause::Op
    );
}

#[test]
fn failure_cause_and_fingerprint_tag_bytes_are_pinned() {
    // The grammar, byte by byte: every tag a u32 (§5): cause tag (0 = Op,
    // 1 = Local), fingerprint tag (5 = MissingCapability, 6 = Local,
    // 7 = RoleIneligible), capability-key tag, class, role.
    let tag = |bytes: &[u8], at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
    assert_eq!(
        FailureCause::Op.encode(),
        [0, 0, 0, 0],
        "nothing follows an Op cause"
    );

    let bytes = FailureCause::Local(FailureFingerprint::MissingCapability {
        key: CapabilityKey::Processor {
            input: TypeUuid([7u8; 16]),
        },
    })
    .encode();
    assert_eq!(tag(&bytes, 0), 1, "cause tag: Local");
    assert_eq!(tag(&bytes, 4), 5, "fingerprint tag: MissingCapability");
    assert_eq!(tag(&bytes, 8), 4, "capability-key tag: Processor");
    assert_eq!(&bytes[12..28], &[7u8; 16], "the requested input type uuid");
    assert_eq!(bytes.len(), 28);

    let mut reserved = bytes;
    reserved[4] = 4;
    assert!(matches!(
        FailureCause::decode(&reserved),
        Err(StoreError::BadResultPayload { detail })
            if detail == "fingerprint tag 4 is permanently reserved"
    ));

    let bytes = FailureCause::Local(FailureFingerprint::Local {
        class: LocalFailureClass::MigrationPlan,
        detail: [3u8; 16],
    })
    .encode();
    assert_eq!(tag(&bytes, 4), 6, "fingerprint tag: Local");
    assert_eq!(tag(&bytes, 8), 2, "class: MigrationPlan");
    assert_eq!(&bytes[12..28], &[3u8; 16], "the stable diagnostic hash");
    assert_eq!(bytes.len(), 28);

    let bytes = FailureCause::Local(FailureFingerprint::RoleIneligible {
        asset: AssetUuid([9; 16]),
        observed_role: EntryRole::AuthoringOnly,
    })
    .encode();
    assert_eq!(tag(&bytes, 4), 7, "fingerprint tag: RoleIneligible");
    assert_eq!(&bytes[8..24], &[9; 16]);
    assert_eq!(tag(&bytes, 24), 1, "role: AuthoringOnly");
}

#[test]
fn trace_digest_is_the_trace_domain_digest() {
    let trace = b"canonical trace ops";
    assert_eq!(
        trace_digest(trace),
        digest(Domain::Trace, 1, |e| e.raw(trace))
    );
}

#[test]
fn failure_cause_decode_negatives() {
    let good = FailureCause::Local(FailureFingerprint::Ambiguous {
        conflicting: vec![AssetUuid([1u8; 16]), AssetUuid([2u8; 16])],
    })
    .encode();

    // Truncations at every boundary are definite errors.
    for cut in [0, 1, 4, 7, 8, 11, good.len() - 1] {
        assert!(bad(&good[..cut]), "cut at {cut}");
    }

    // Unknown cause tag.
    let mut tag = good.clone();
    tag[0] = 9;
    assert!(bad(&tag));

    // Trailing garbage is rejected — the grammar is exact.
    let mut trailing = good.clone();
    trailing.push(0);
    assert!(bad(&trailing));

    // An absurd count fails before allocation.
    let mut count = good;
    count[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(bad(&count));
}

#[test]
fn hostile_descendant_nesting_is_capped() {
    // A corrupt row cannot recurse the decoder off the stack.
    let mut fp = FailureFingerprint::Poisoned {
        bundle: BundleUuid([0u8; 16]),
    };
    for _ in 0..10_000 {
        fp = FailureFingerprint::Descendant {
            asset: AssetUuid([1u8; 16]),
            fingerprint: Box::new(fp),
        };
    }
    assert!(bad(&FailureCause::Local(fp).encode()));
}
