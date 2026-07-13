//! §13 log-structured CAS behavior: append/commit/read roundtrips, the
//! result record as commit marker, candidate buckets that append and
//! never overwrite, derived-output assertion verification, wire trees as
//! first-class records, and segment rolling.

use distill_core::id::{AssetUuid, BundleFileHash, ContentHash};
use distill_store::cas::manifest::{read_current, write_current, GenerationManifest, SegmentKind};
use distill_store::cas::record::{
    decode_record, CapabilityKey, FailureCause, FailureFingerprint, KeyKind, LocalFailureClass,
    ResultOutcome,
};
use distill_store::cas::{AuxSpec, BuildCommit, CommitOutcome, OutputSpec, PayloadKind};
use distill_store::state::{
    ReadableBundleSource, SkeletonFailureCode, VersionPoison, VersionPoisonV1,
};
use distill_store::{Store, StoreConfig, StoreError};

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, s)
}

const PARENT: AssetUuid = AssetUuid([7u8; 16]);

fn version_poison(message: &str) -> VersionPoison {
    VersionPoison::new(
        VersionPoisonV1::IncompleteSkeleton {
            source: ReadableBundleSource {
                root_name: "main".into(),
                normalized_path: "broken.bundle".into(),
                file_hash: BundleFileHash([9; 32]),
            },
            failure: SkeletonFailureCode::IncompleteAssetIdentity,
        },
        message,
    )
    .unwrap()
}

fn success_commit(static_key: [u8; 32], trace: &[u8]) -> BuildCommit {
    BuildCommit {
        key_kind: KeyKind::Processor,
        static_input_key: static_key,
        asset_uuid: PARENT,
        static_inputs_canonical: b"canonical static inputs".to_vec(),
        trace: trace.to_vec(),
        outcome: CommitOutcome::Success {
            payload_kind: PayloadKind::ProcessorOutput,
            outputs: vec![
                OutputSpec {
                    output_key: String::new(),
                    type_uuids: vec![],
                    bytes: b"primary artifact bytes".to_vec(),
                },
                OutputSpec {
                    output_key: "normals".to_owned(),
                    type_uuids: vec![],
                    bytes: b"normals artifact bytes".to_vec(),
                },
            ],
            aux: vec![AuxSpec {
                debug_key: "debug-dump".to_owned(),
                bytes: b"debug bytes".to_vec(),
            }],
        },
    }
}

fn declare_child(store: &mut Store, parent: AssetUuid, key: &str) -> AssetUuid {
    let child = AssetUuid::v5(parent, key);
    store
        .input_transaction(|txn| txn.set_derived_output(child, parent, key))
        .unwrap();
    child
}

// ---- wire trees ----

#[test]
fn wire_trees_are_first_class_cas_records() {
    // §13: the payload is the canonical DSWL serialization — its blake3
    // IS the LayoutHash.
    let (_d, mut store) = store();
    let tree = b"canonical DSWL bytes";
    let hash = store.put_wire_tree(tree).unwrap();
    assert_eq!(hash.0, *blake3::hash(tree).as_bytes());
    assert_eq!(store.cas_read(&hash.0).unwrap(), tree);
}

#[test]
fn wire_tree_writes_are_idempotent() {
    let (_d, mut store) = store();
    let a = store.put_wire_tree(b"same tree").unwrap();
    let b = store.put_wire_tree(b"same tree").unwrap();
    assert_eq!(a, b);
    assert_eq!(store.cas_read(&a.0).unwrap(), b"same tree");
}

// ---- commit + read roundtrip ----

#[test]
fn a_successful_commit_publishes_outputs_and_advances_only_the_memo_seq() {
    let (_d, mut store) = store();
    declare_child(&mut store, PARENT, "normals");
    let input_version_before = store.input_version();

    let receipt = store
        .commit_build(success_commit([1u8; 32], b"trace"))
        .unwrap();
    assert_eq!(receipt.memo_seq.0, 1);
    assert_eq!(store.memo_seq().0, 1);
    assert_eq!(
        store.input_version(),
        input_version_before,
        "build results attach to an input basis without advancing any input version (§13)"
    );

    // The receipt names every output's content hash.
    assert_eq!(receipt.outputs.len(), 2);
    let primary = receipt.outputs.iter().find(|(k, _)| k.is_empty()).unwrap();
    assert_eq!(
        primary.1,
        ContentHash(*blake3::hash(b"primary artifact bytes").as_bytes())
    );

    // Every output and aux payload reads back by content hash.
    for (_, hash) in &receipt.outputs {
        assert!(!store.cas_read(&hash.0).unwrap().is_empty());
    }
    assert_eq!(receipt.aux.len(), 1);
    assert_eq!(
        store.cas_read(&receipt.aux[0].1 .0).unwrap(),
        b"debug bytes"
    );
    assert!(receipt.unverified_assertions.is_empty());
}

#[test]
fn lookup_finds_the_committed_candidate() {
    let (_d, mut store) = store();
    declare_child(&mut store, PARENT, "normals");
    store
        .commit_build(success_commit([1u8; 32], b"trace"))
        .unwrap();

    let candidates = store
        .lookup_candidates(KeyKind::Processor, &[1u8; 32])
        .unwrap();
    assert_eq!(candidates.len(), 1);
    let c = &candidates[0];
    assert_eq!(c.asset_uuid, PARENT);
    assert_eq!(c.payload.trace, b"trace");
    assert_eq!(
        c.payload.static_inputs_canonical,
        b"canonical static inputs"
    );
    match &c.payload.outcome {
        ResultOutcome::Success { outputs, aux } => {
            assert_eq!(outputs.len(), 2);
            assert_eq!(aux.len(), 1);
            assert_eq!(aux[0].debug_key, "debug-dump");
        }
        other => panic!("expected success, got {other:?}"),
    }

    // A different key kind or key misses — the two key grammars never mix.
    assert!(store
        .lookup_candidates(KeyKind::BuildImport, &[1u8; 32])
        .unwrap()
        .is_empty());
    assert!(store
        .lookup_candidates(KeyKind::Processor, &[2u8; 32])
        .unwrap()
        .is_empty());
}

#[test]
fn commits_append_to_the_bucket_never_overwrite() {
    // §13/§9: two snapshots alternating over one key each keep hitting
    // their own candidate instead of rebuilding each other's away;
    // revalidation is most-recently-committed-first.
    let (_d, mut store) = store();
    declare_child(&mut store, PARENT, "normals");
    store
        .commit_build(success_commit([1u8; 32], b"trace-A"))
        .unwrap();
    store
        .commit_build(success_commit([1u8; 32], b"trace-B"))
        .unwrap();

    let candidates = store
        .lookup_candidates(KeyKind::Processor, &[1u8; 32])
        .unwrap();
    assert_eq!(candidates.len(), 2, "the bucket holds both");
    assert_eq!(candidates[0].payload.trace, b"trace-B", "most recent first");
    assert_eq!(candidates[1].payload.trace, b"trace-A");

    // Recommitting an existing (key, trace) refreshes recency, never
    // duplicates.
    store
        .commit_build(success_commit([1u8; 32], b"trace-A"))
        .unwrap();
    let candidates = store
        .lookup_candidates(KeyKind::Processor, &[1u8; 32])
        .unwrap();
    assert_eq!(candidates.len(), 2);
    assert_eq!(
        candidates[0].payload.trace, b"trace-A",
        "refreshed to front"
    );
}

#[test]
fn cas_read_of_an_unknown_hash_is_not_found() {
    let (_d, store) = store();
    match store.cas_read(&[9u8; 32]) {
        Err(StoreError::NotFound { hash }) => assert_eq!(hash, [9u8; 32]),
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[test]
fn corrupted_segment_bytes_fail_the_read() {
    // Reads verify against the requested hash — corruption is caught,
    // never returned.
    let (dir, mut store) = store();
    declare_child(&mut store, PARENT, "normals");
    let receipt = store
        .commit_build(success_commit([1u8; 32], b"trace"))
        .unwrap();
    let hash = receipt.outputs[0].1;

    // Flip one byte of every segment file's payload region.
    let cas_dir = dir.path().join(".distill/cas");
    for entry in std::fs::read_dir(&cas_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().map(|e| e == "dsr").unwrap_or(false) {
            let mut bytes = std::fs::read(&path).unwrap();
            for b in bytes.iter_mut() {
                *b ^= 0xFF;
            }
            std::fs::write(&path, bytes).unwrap();
        }
    }
    assert!(matches!(
        store.cas_read(&hash.0),
        Err(StoreError::CorruptExtent { .. })
    ));
}

// ---- build-import results (§13, R20/M18) ----

#[test]
fn build_import_results_carry_exactly_one_output_row() {
    let (_d, mut store) = store();
    let mut commit = success_commit([2u8; 32], b"bi-trace");
    commit.key_kind = KeyKind::BuildImport;
    commit.static_inputs_canonical = Vec::new();
    // Two outputs: arity error.
    let err = store.commit_build(commit).unwrap_err();
    match err {
        StoreError::BuildImportOutputArity { got } => assert_eq!(got, 2),
        other => panic!("expected BuildImportOutputArity, got {other:?}"),
    }

    // Exactly one output row commits and is bucket-indexed by DSBI digest.
    let commit = BuildCommit {
        key_kind: KeyKind::BuildImport,
        static_input_key: [2u8; 32],
        asset_uuid: PARENT,
        static_inputs_canonical: Vec::new(),
        trace: b"bi-trace".to_vec(),
        outcome: CommitOutcome::Success {
            payload_kind: PayloadKind::ImportEncoding,
            outputs: vec![OutputSpec {
                output_key: String::new(),
                type_uuids: vec![],
                bytes: b"import encoding".to_vec(),
            }],
            aux: vec![],
        },
    };
    store.commit_build(commit).unwrap();
    let candidates = store
        .lookup_candidates(KeyKind::BuildImport, &[2u8; 32])
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].payload.trace, b"bi-trace");
}

// ---- failure records (§13, R21/H4+M13) ----

#[test]
fn failure_records_commit_index_and_carry_no_outputs() {
    // An op-caused failure: the trace ends in the failing Observed::Err
    // entry, the cause is Op, and no standalone fingerprint rides beside
    // the trace.
    let (_d, mut store) = store();
    let commit = BuildCommit {
        key_kind: KeyKind::Processor,
        static_input_key: [3u8; 32],
        asset_uuid: PARENT,
        static_inputs_canonical: b"si".to_vec(),
        trace: b"trace incl failing op".to_vec(),
        outcome: CommitOutcome::Failure {
            cause: FailureCause::Op,
        },
    };
    let receipt = store.commit_build(commit).unwrap();
    assert!(receipt.outputs.is_empty());
    assert!(receipt.aux.is_empty());

    let candidates = store
        .lookup_candidates(KeyKind::Processor, &[3u8; 32])
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].payload.trace, b"trace incl failing op");
    assert!(matches!(
        candidates[0].payload.outcome,
        ResultOutcome::Failure {
            cause: FailureCause::Op
        }
    ));
}

#[test]
fn a_capability_miss_memoizes_with_its_requested_key() {
    // §8/§9/§13 (R21/H4): a missing MigrationFn or default-table entry
    // fails before any code runs — the failure record carries the
    // requested CapabilityKey so revalidation against a later pipeline
    // epoch can heal it on the first epoch that supplies the
    // registration.
    let (_d, mut store) = store();
    let key = CapabilityKey::MigrationFn("v2-to-v3".to_owned());
    let commit = BuildCommit {
        key_kind: KeyKind::Processor,
        static_input_key: [4u8; 32],
        asset_uuid: PARENT,
        static_inputs_canonical: b"si".to_vec(),
        // The trace ends in the TraceOp::Capability miss (§9); its bytes
        // are opaque to the store.
        trace: b"...capability(v2-to-v3) -> Err(MissingCapability)".to_vec(),
        outcome: CommitOutcome::Failure {
            cause: FailureCause::Local(FailureFingerprint::MissingCapability { key: key.clone() }),
        },
    };
    store.commit_build(commit).unwrap();
    let candidates = store
        .lookup_candidates(KeyKind::Processor, &[4u8; 32])
        .unwrap();
    assert_eq!(candidates.len(), 1);
    match &candidates[0].payload.outcome {
        ResultOutcome::Failure {
            cause: FailureCause::Local(FailureFingerprint::MissingCapability { key: got }),
        } => assert_eq!(got, &key),
        other => panic!("expected a MissingCapability failure, got {other:?}"),
    }
}

#[test]
fn a_deterministic_local_failure_memoizes_with_an_empty_trace() {
    // §9/§13 (R21/M13): a validator diagnostic arises from no context
    // operation — trace empty, cause Local, memoized like any failure.
    let (_d, mut store) = store();
    let commit = BuildCommit {
        key_kind: KeyKind::Processor,
        static_input_key: [5u8; 32],
        asset_uuid: PARENT,
        static_inputs_canonical: b"si".to_vec(),
        trace: Vec::new(),
        outcome: CommitOutcome::Failure {
            cause: FailureCause::Local(FailureFingerprint::Local {
                class: LocalFailureClass::Validator,
                detail: *blake3::hash(b"normal map length != 1").as_bytes(),
            }),
        },
    };
    store.commit_build(commit).unwrap();
    let candidates = store
        .lookup_candidates(KeyKind::Processor, &[5u8; 32])
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert!(candidates[0].payload.trace.is_empty());
}

// ---- derived-output namespace + assertions (§9, §13) ----

#[test]
fn derived_output_namespace_resolves_children() {
    let (_d, mut store) = store();
    let child = declare_child(&mut store, PARENT, "normals");
    let (parent, key) = store.resolve_child(child).unwrap().expect("derived");
    assert_eq!(parent, PARENT);
    assert_eq!(key, "normals");
    assert!(store.resolve_child(AssetUuid([9u8; 16])).unwrap().is_none());
}

#[test]
fn commit_assertions_verify_against_the_namespace() {
    // §9: commit rows are memo data verified against the input-versioned
    // namespace index, never a namespace claim of their own.
    let (_d, mut store) = store();
    // No namespace row for "normals": the assertion does not verify.
    let receipt = store
        .commit_build(success_commit([1u8; 32], b"trace"))
        .unwrap();
    assert_eq!(receipt.unverified_assertions.len(), 1);
    assert_eq!(receipt.unverified_assertions[0].1, "normals");

    // With the namespace row present, the assertion verifies.
    declare_child(&mut store, PARENT, "normals");
    let receipt = store
        .commit_build(success_commit([1u8; 32], b"trace-2"))
        .unwrap();
    assert!(receipt.unverified_assertions.is_empty());
}

#[test]
fn the_namespace_is_the_only_authority_for_child_resolution() {
    // §9/§13: a retired child UUID is never resurrected by an old record.
    let (_d, mut store) = store();
    let child = declare_child(&mut store, PARENT, "normals");
    store
        .commit_build(success_commit([1u8; 32], b"trace"))
        .unwrap();
    assert!(store.resolve_child(child).unwrap().is_some());

    // The namespace retires the key at a later input version; the memo
    // rows (result record, assertion) still exist — resolution must miss.
    store
        .input_transaction(|txn| txn.remove_derived_output(child))
        .unwrap();
    assert!(store.resolve_child(child).unwrap().is_none());
}

#[test]
fn resolve_child_is_namespace_facing_under_version_poison() {
    let (_d, mut store) = store();
    let child = declare_child(&mut store, PARENT, "normals");
    store
        .input_transaction(|txn| {
            let poison = version_poison("collision");
            txn.set_version_poison(Some(&poison))
        })
        .unwrap();
    assert!(matches!(
        store.resolve_child(child),
        Err(StoreError::Poisoned { .. })
    ));
}

// ---- segment rolling + persistence ----

#[test]
fn segments_roll_at_the_size_cap() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StoreConfig::new(dir.path().join(".distill"));
    config.segment_size = 512; // tiny cap: every commit rolls
    let mut store = Store::open(config).unwrap();

    for i in 0..4u8 {
        let commit = BuildCommit {
            key_kind: KeyKind::Processor,
            static_input_key: [i; 32],
            asset_uuid: PARENT,
            static_inputs_canonical: vec![],
            trace: vec![i],
            outcome: CommitOutcome::Success {
                payload_kind: PayloadKind::ProcessorOutput,
                outputs: vec![OutputSpec {
                    output_key: String::new(),
                    type_uuids: vec![],
                    bytes: vec![i; 400],
                }],
                aux: vec![],
            },
        };
        store.commit_build(commit).unwrap();
    }

    let segments: Vec<_> = std::fs::read_dir(dir.path().join(".distill/cas"))
        .unwrap()
        .filter_map(|e| {
            let name = e.unwrap().file_name().to_string_lossy().into_owned();
            name.ends_with(".dsr").then_some(name)
        })
        .collect();
    assert!(
        segments.len() >= 2,
        "expected rolled segments, got {segments:?}"
    );

    // Everything still reads.
    for i in 0..4u8 {
        let hash = *blake3::hash(&vec![i; 400]).as_bytes();
        assert_eq!(store.cas_read(&hash).unwrap(), vec![i; 400]);
    }
}

#[test]
fn a_record_larger_than_the_cap_gets_one_typed_dedicated_oversize_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StoreConfig::new(dir.path().join(".distill"));
    config.segment_size = 512;
    let mut store = Store::open(config.clone()).unwrap();
    let payload = vec![0x5au8; 4096];
    let receipt = store
        .commit_build(BuildCommit {
            key_kind: KeyKind::Processor,
            static_input_key: [0x33; 32],
            asset_uuid: PARENT,
            static_inputs_canonical: vec![],
            trace: b"oversize".to_vec(),
            outcome: CommitOutcome::Success {
                payload_kind: PayloadKind::ProcessorOutput,
                outputs: vec![OutputSpec {
                    output_key: String::new(),
                    type_uuids: vec![],
                    bytes: payload.clone(),
                }],
                aux: vec![],
            },
        })
        .unwrap();

    let current = read_current(&config.state_path.join("cas")).unwrap();
    let oversized: Vec<_> = current
        .segments
        .iter()
        .filter(|s| s.kind == SegmentKind::Oversize)
        .collect();
    assert_eq!(oversized.len(), 1);
    let bytes = std::fs::read(config.state_path.join("cas").join(&oversized[0].name)).unwrap();
    let decoded = decode_record(&bytes, 0, 0).unwrap();
    assert_eq!(
        decoded.encoded_len as usize,
        bytes.len(),
        "exactly one record per file"
    );
    assert!(decoded.encoded_len > config.segment_size);
    assert_eq!(store.cas_read(&receipt.outputs[0].1 .0).unwrap(), payload);

    store.compact().unwrap();
    let compacted = read_current(&config.state_path.join("cas")).unwrap();
    assert_eq!(
        compacted
            .segments
            .iter()
            .filter(|s| s.kind == SegmentKind::Oversize)
            .count(),
        1,
        "compaction preserves dedicated oversize typing"
    );

    // Force startup's full segment-scan path, including the oversize
    // payload followed by its result record in another segment.
    write_current(
        &config.state_path.join("cas"),
        &GenerationManifest {
            generation: compacted.generation + 1,
            segments: compacted.segments,
        },
    )
    .unwrap();

    drop(store);
    let mut reopened = Store::open(config).unwrap();
    assert!(reopened.recovery_report().rebuilt_index);
    assert_eq!(
        reopened
            .lookup_candidates(KeyKind::Processor, &[0x33; 32])
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        reopened.cas_read(&receipt.outputs[0].1 .0).unwrap(),
        payload
    );
}

#[test]
fn commits_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig::new(dir.path().join(".distill"));
    let mut store = Store::open(config.clone()).unwrap();
    let receipt = store
        .commit_build(success_commit([1u8; 32], b"trace"))
        .unwrap();
    let hash = receipt.outputs[0].1;
    drop(store);

    let mut store = Store::open(config).unwrap();
    assert_eq!(store.cas_read(&hash.0).unwrap(), b"primary artifact bytes");
    let candidates = store
        .lookup_candidates(KeyKind::Processor, &[1u8; 32])
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(store.memo_seq().0, 1, "memo counter persisted");
}
