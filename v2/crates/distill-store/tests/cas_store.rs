//! §13 log-structured CAS behavior: append/commit/read roundtrips,
//! candidate buckets that append and never overwrite, wire trees as
//! first-class extents, and segment rolling.

use distill_core::id::{AssetUuid, BundleFileHash, ContentHash};
use distill_store::cas::record::{
    CapabilityKey, FailureCause, FailureFingerprint, KeyKind, LocalFailureClass, ResultOutcome,
};
use distill_store::cas::{AuxSpec, BuildCommit, CommitOutcome, OutputSpec};
use distill_store::claims::{DerivedOutputClaim, SourceClaim, SourceClaims};
use distill_store::state::{
    NamespaceError, NamespaceErrorV1, ReadableBundleSource, SkeletonFailureCode,
};
use distill_store::{Store, StoreConfig, StoreError};
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::wire::WireNode;

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, s)
}

const PARENT: AssetUuid = AssetUuid([7u8; 16]);

fn namespace_error(message: &str) -> NamespaceError {
    NamespaceError::new(
        NamespaceErrorV1::IncompleteSkeleton {
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
        wire_trees: Vec::new(),
        key_kind: KeyKind::Processor,
        static_input_key: static_key,
        asset_uuid: PARENT,
        trace: trace.to_vec(),
        outcome: CommitOutcome::Success {
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

/// The claims of a source at `path` deriving `key` from `parent`.
fn deriving_source(path: &str, parent: AssetUuid, key: &str) -> SourceClaims {
    SourceClaims {
        root_name: "main".to_owned(),
        path: path.to_owned(),
        claims: vec![SourceClaim::DerivedOutput {
            child: AssetUuid::v5(parent, key),
            output: DerivedOutputClaim {
                parent,
                output_key: key.to_owned(),
                terminal_type: distill_core::id::TypeUuid([0x51; 16]),
            },
        }],
    }
}

fn under(path: &str) -> Vec<(String, String)> {
    vec![("main".to_owned(), path.to_owned())]
}

/// A source `p.bundle` derives `key` from `parent`.
fn declare_child(store: &mut Store, parent: AssetUuid, key: &str) -> AssetUuid {
    store
        .input_transaction(|txn| {
            txn.replace_source_claims(
                Some(&under("p.bundle")),
                &[deriving_source("p.bundle", parent, key)],
            )
        })
        .unwrap();
    AssetUuid::v5(parent, key)
}

// ---- wire trees ----

#[test]
fn wire_trees_are_first_class_cas_records() {
    // §13: LayoutHash is the domain-separated digest of the canonical DSWL
    // body, while typed reads return that body without the CAS preimage.
    let (_d, mut store) = store();
    let root = WireNode::Unit { offset: 0 };
    let tree = dswl_bytes(&root).unwrap();
    let hash = store.put_wire_tree(&tree).unwrap();
    assert_eq!(hash, dswl_hash(&root).unwrap());
    assert_eq!(store.wire_tree_read(hash).unwrap(), tree);
}

#[test]
fn wire_tree_writes_are_idempotent() {
    let (_d, mut store) = store();
    let tree = dswl_bytes(&WireNode::Unit { offset: 0 }).unwrap();
    let a = store.put_wire_tree(&tree).unwrap();
    let b = store.put_wire_tree(&tree).unwrap();
    assert_eq!(a, b);
    assert_eq!(store.wire_tree_read(a).unwrap(), tree);
}

#[test]
fn malformed_wire_tree_bodies_are_rejected_before_append() {
    let (_d, mut store) = store();
    assert!(matches!(
        store.put_wire_tree(b"not DSWL"),
        Err(StoreError::InvalidWireTree { .. })
    ));
}

// ---- commit + read roundtrip ----

#[test]
fn a_successful_commit_publishes_outputs_and_advances_only_the_memo_seq() {
    let (_d, mut store) = store();
    declare_child(&mut store, PARENT, "normals");
    let input_version_before = store.input_version().unwrap();

    let receipt = store
        .commit_build(success_commit([1u8; 32], b"trace"))
        .unwrap();
    assert_eq!(receipt.memo_seq.0, 1);
    assert_eq!(store.memo_seq().unwrap().0, 1);
    assert_eq!(
        store.input_version().unwrap(),
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
fn a_commit_naming_payloads_already_in_the_cas_appends_nothing() {
    // A node result names the bytes its last stage committed: the second
    // commit indexes a new candidate without appending anything.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    declare_child(&mut store, PARENT, "normals");
    let segment_bytes = || -> u64 {
        std::fs::read_dir(dir.path().join(".distill/cas"))
            .unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len())
            .sum()
    };
    let before = segment_bytes();
    store
        .commit_build(success_commit([1u8; 32], b"trace"))
        .unwrap();
    let first = segment_bytes() - before;
    let before = segment_bytes();
    let mut node = success_commit([2u8; 32], b"trace");
    node.key_kind = KeyKind::Node;
    store.commit_build(node).unwrap();
    let appended = segment_bytes() - before;
    assert!(first > 0);
    assert_eq!(appended, 0, "a segment holds only the bytes");
    let candidates = store.lookup_candidates(KeyKind::Node, &[2u8; 32]).unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].payload.key_kind, KeyKind::Node);
    let ResultOutcome::Success { outputs, .. } = &candidates[0].payload.outcome else {
        panic!("node result succeeded");
    };
    for output in outputs {
        assert!(store.cas_read(&output.content_hash.0).is_ok());
    }
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
    // Two outputs: arity error.
    let err = store.commit_build(commit).unwrap_err();
    match err {
        StoreError::BuildImportOutputArity { got } => assert_eq!(got, 2),
        other => panic!("expected BuildImportOutputArity, got {other:?}"),
    }

    // Exactly one output row commits and is bucket-indexed by DSBI digest.
    let commit = BuildCommit {
        wire_trees: Vec::new(),
        key_kind: KeyKind::BuildImport,
        static_input_key: [2u8; 32],
        asset_uuid: PARENT,
        trace: b"bi-trace".to_vec(),
        outcome: CommitOutcome::Success {
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
        wire_trees: Vec::new(),
        key_kind: KeyKind::Processor,
        static_input_key: [3u8; 32],
        asset_uuid: PARENT,
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
        wire_trees: Vec::new(),
        key_kind: KeyKind::Processor,
        static_input_key: [4u8; 32],
        asset_uuid: PARENT,
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
        wire_trees: Vec::new(),
        key_kind: KeyKind::Processor,
        static_input_key: [5u8; 32],
        asset_uuid: PARENT,
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

// ---- derived-output namespace (§9, §13) ----

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
fn derived_output_namespace_replacement_is_atomic_and_complete() {
    let (_d, mut store) = store();
    let old = declare_child(&mut store, PARENT, "normals");
    let next_parent = AssetUuid([8; 16]);
    let next = AssetUuid::v5(next_parent, "meshlets");
    store
        .input_transaction(|txn| {
            txn.replace_source_claims(
                None,
                &[deriving_source("q.bundle", next_parent, "meshlets")],
            )
        })
        .unwrap();
    assert!(store.resolve_child(old).unwrap().is_none());
    assert_eq!(
        store.resolve_child(next).unwrap(),
        Some((next_parent, "meshlets".to_owned()))
    );
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
    // row (the `results` row) still exists — resolution must miss.
    store
        .input_transaction(|txn| txn.replace_source_claims(Some(&under("p.bundle")), &[]))
        .unwrap();
    assert!(store.resolve_child(child).unwrap().is_none());
}

#[test]
fn resolve_child_ignores_namespace_errors_elsewhere() {
    let (_d, mut store) = store();
    let child = declare_child(&mut store, PARENT, "normals");
    store
        .input_transaction(|txn| txn.set_namespace_errors([namespace_error("collision")]))
        .unwrap();
    assert_eq!(
        store.resolve_child(child).unwrap(),
        Some((PARENT, "normals".to_owned()))
    );
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
            wire_trees: Vec::new(),
            key_kind: KeyKind::Processor,
            static_input_key: [i; 32],
            asset_uuid: PARENT,
            trace: vec![i],
            outcome: CommitOutcome::Success {
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

fn oversize_files(config: &StoreConfig) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(config.state_path.join("cas"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("oversize-"))
        })
        .collect()
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
            wire_trees: Vec::new(),
            key_kind: KeyKind::Processor,
            static_input_key: [0x33; 32],
            asset_uuid: PARENT,
            trace: b"oversize".to_vec(),
            outcome: CommitOutcome::Success {
                outputs: vec![OutputSpec {
                    output_key: String::new(),
                    type_uuids: vec![],
                    bytes: payload.clone(),
                }],
                aux: vec![],
            },
        })
        .unwrap();

    let oversized = oversize_files(&config);
    assert_eq!(oversized.len(), 1);
    let bytes = std::fs::read(&oversized[0]).unwrap();
    assert_eq!(bytes, payload, "exactly the one extent");
    assert!(bytes.len() as u64 > config.segment_size);
    assert_eq!(store.cas_read(&receipt.outputs[0].1 .0).unwrap(), payload);

    store.compact().unwrap();
    assert_eq!(
        oversize_files(&config).len(),
        1,
        "compaction leaves a live oversize segment alone"
    );

    // A reopen keeps the oversize payload in a segment of its own.
    drop(store);
    let (reopened, recovery) = Store::open_with_recovery(config).unwrap();
    assert_eq!(recovery, distill_store::cas::RecoveryReport::default());
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

    let store = Store::open(config).unwrap();
    assert_eq!(store.cas_read(&hash.0).unwrap(), b"primary artifact bytes");
    let candidates = store
        .lookup_candidates(KeyKind::Processor, &[1u8; 32])
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(store.memo_seq().unwrap().0, 1, "memo counter persisted");
}
