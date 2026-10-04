//! §13's consistency contract, cross-cutting: the two sequencing
//! domains stay independent, multi-table input transactions are
//! all-or-nothing, WAL readers only ever observe complete input
//! versions, and a namespace error gates only its entity.

use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use distill_core::tool::ToolCwdPolicy;
use distill_store::bundles::{AssetRecord, BundleMeta};
use distill_store::cas::record::KeyKind;
use distill_store::cas::{BuildCommit, CommitOutcome, OutputSpec};
use distill_store::claims::{SourceClaim, SourceClaims};
use distill_store::pipeline::{ResolvedToolPackageFile, ResolvedToolSourceV2, ToolRegistrationV2};
use distill_store::state::{
    NamespaceError, NamespaceErrorV1, ReadableBundleSource, SkeletonFailureCode,
};
use distill_store::{Store, StoreConfig, StoreError};

fn cfg(dir: &tempfile::TempDir) -> StoreConfig {
    StoreConfig::new(dir.path().join(".distill"))
}

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

fn commit(store: &mut Store, key: u8) {
    store
        .commit_build(BuildCommit {
            wire_trees: Vec::new(),
            key_kind: KeyKind::Processor,
            static_input_key: [key; 32],
            asset_uuid: AssetUuid([7u8; 16]),
            trace: vec![key],
            outcome: CommitOutcome::Success {
                outputs: vec![OutputSpec {
                    output_key: String::new(),
                    type_uuids: vec![],
                    bytes: vec![key; 64],
                }],
                aux: vec![],
            },
        })
        .unwrap();
}

#[test]
fn the_two_sequencing_domains_are_independent() {
    // §13: watcher/authoring transactions advance the input version;
    // build results commit on a separate memo sequence, attaching
    // outputs to an input basis without advancing any input version.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();

    store.input_transaction(|_| Ok(())).unwrap();
    commit(&mut store, 1);
    commit(&mut store, 2);
    store.input_transaction(|_| Ok(())).unwrap();
    commit(&mut store, 3);

    assert_eq!(store.input_version().unwrap().0, 2, "two input events");
    assert_eq!(store.memo_seq().unwrap().0, 3, "three memo commits");

    // The memo committed under version 1 is readable at version 2 — an
    // old basis's memo read by a newer version is the memoization
    // semantic, not a leak.
    assert_eq!(
        store
            .lookup_candidates(KeyKind::Processor, &[1u8; 32])
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn multi_table_input_transactions_are_all_or_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();

    let err = store
        .input_transaction::<(), _>(|txn| {
            let root = txn.intern_root("main")?;
            txn.upsert_file(
                root,
                "a.bundle",
                &distill_store::files::FileState {
                    mtime: 1,
                    size: 1,
                    kind: distill_store::files::FileKind::File,
                    content_hash: None,
                }
                .into(),
                distill_store::state::InputVersion(1),
            )?;
            txn.upsert_bundle(&BundleMeta {
                bundle: BundleUuid([1u8; 16]),
                root,
                path: "a.bundle".into(),
                format_version: 1,
                content_hash: ContentHash([1u8; 32]),
                origin: None,
                import_watched: false,
            })?;
            txn.upsert_asset(&AssetRecord {
                asset: AssetUuid([2u8; 16]),
                bundle: BundleUuid([1u8; 16]),
                local_id: "e".into(),
                type_uuid: TypeUuid([3u8; 16]),
                logical_hash: LogicalHash([4u8; 32]),
                authoring_only: false,
                terminal_type: None,
                tags: std::collections::BTreeMap::from([("t".into(), None)]),
            })?;
            txn.register_tool(
                "tool",
                ToolRegistrationV2 {
                    source: ResolvedToolSourceV2::Package {
                        launcher: "bin/tool".into(),
                        files: vec![ResolvedToolPackageFile {
                            path: "bin/tool".into(),
                            executable: true,
                            bytes: b"tool bytes".to_vec(),
                        }],
                    },
                    environment: vec![],
                    cwd_policy: ToolCwdPolicy::EmptyScratch,
                },
            )?;
            Err(StoreError::InvalidConfiguration {
                error: "abort everything".into(),
            })
        })
        .unwrap_err();
    assert!(matches!(err, StoreError::InvalidConfiguration { .. }));

    // None of it published — readers observe all metadata for a given
    // tree state, or none of it.
    assert_eq!(store.input_version().unwrap().0, 0);
    assert!(store.entry(AssetUuid([2u8; 16])).unwrap().is_none());
    assert!(store.bundle(BundleUuid([1u8; 16])).unwrap().is_none());
    assert!(store.tool("tool").unwrap().is_none());
}

#[test]
fn wal_readers_only_observe_complete_input_versions() {
    // §13 concurrency: one writer, many snapshot readers. A WAL read
    // transaction opened before an input transaction commits never sees
    // its partial writes.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    store
        .input_transaction(|txn| {
            let root = txn.intern_root("main")?;
            txn.upsert_file(
                root,
                "a.bundle",
                &distill_store::files::FileState {
                    mtime: 1,
                    size: 1,
                    kind: distill_store::files::FileKind::File,
                    content_hash: None,
                }
                .into(),
                distill_store::state::InputVersion(1),
            )
        })
        .unwrap();

    // Reader pins a snapshot (BEGIN starts the read view at first read).
    let reader = rusqlite::Connection::open(dir.path().join(".distill/meta.sqlite")).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    let count_at_snapshot: i64 = reader
        .query_row("SELECT COUNT(*) FROM files", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count_at_snapshot, 1);

    // Writer publishes a second version.
    store
        .input_transaction(|txn| {
            let root = txn.intern_root("main")?;
            txn.upsert_file(
                root,
                "b.bundle",
                &distill_store::files::FileState {
                    mtime: 2,
                    size: 2,
                    kind: distill_store::files::FileKind::File,
                    content_hash: None,
                }
                .into(),
                distill_store::state::InputVersion(2),
            )
        })
        .unwrap();

    // The pinned reader still sees exactly its version.
    let count_pinned: i64 = reader
        .query_row("SELECT COUNT(*) FROM files", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count_pinned, 1, "the snapshot is immutable");
    reader.execute_batch("COMMIT").unwrap();

    // A fresh read view sees the complete new version.
    let count_fresh: i64 = reader
        .query_row("SELECT COUNT(*) FROM files", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count_fresh, 2);
}

#[test]
fn a_namespace_error_does_not_gate_the_namespace() {
    // A namespace error (LOCKLESS.md §4) is about one entity: the rest of
    // the namespace stays readable.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    store
        .input_transaction(|txn| {
            txn.replace_source_claims(
                None,
                &[SourceClaims {
                    root_name: "main".into(),
                    path: "broken.bundle".into(),
                    claims: vec![SourceClaim::Malformed(namespace_error("unreadable"))],
                }],
            )?;
            Ok(())
        })
        .unwrap();

    assert_eq!(
        store.namespace_errors().unwrap(),
        vec![namespace_error("unreadable")]
    );
    assert!(store.path_assets("x").unwrap().is_empty());
    assert!(store.entry(AssetUuid([1u8; 16])).unwrap().is_none());
    // CAS reads are pure metadata.
    commit(&mut store, 3);
    assert_eq!(
        store.cas_read(blake3::hash(&[3u8; 64]).as_bytes()).unwrap(),
        vec![3u8; 64]
    );
}

#[test]
fn memo_state_is_monotone_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    commit(&mut store, 1);
    let seq_before = store.memo_seq().unwrap();
    drop(store);
    let mut store = Store::open(cfg(&dir)).unwrap();
    assert_eq!(store.memo_seq().unwrap(), seq_before);
    commit(&mut store, 2);
    assert!(
        store.memo_seq().unwrap() > seq_before,
        "the memo sequence never rewinds"
    );
}
