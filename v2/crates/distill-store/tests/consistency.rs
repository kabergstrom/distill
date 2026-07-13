//! §13's consistency contract, cross-cutting: the two sequencing
//! domains stay independent, multi-table input transactions are
//! all-or-nothing, WAL readers only ever observe complete input
//! versions, and the poison classifications compose.

use distill_core::attestation::{
    CompiledTypeRow, CompiledTypeTable, RegistryExtraFact, RegistryExtraRow, RegistryExtrasV1,
    SchemaNodeId,
};
use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use distill_core::target_set::CanonicalTargetSet;
use distill_core::tool::{
    ToolCapsuleFileRole, ToolCwdPolicy, ToolLaunchMetadataV1, ToolPlatformBinding,
};
use distill_schema::bootstrap_gen_v1::consumer_bootstrap_authority_v1;
use distill_store::bundles::{AssetRecord, BundleMeta};
use distill_store::cas::record::KeyKind;
use distill_store::cas::{BuildCommit, CommitOutcome, OutputSpec, PayloadKind};
use distill_store::pipeline::{
    AcceptedSchemaEpoch, AcceptedTypeLineage, SchemaLineageManifest, TypeAuthorityState,
    ValidatedPipelineEpoch, VerifiedSchemaLineageManifest,
};
use distill_store::pipeline::{ResolvedToolCapsuleFile, ToolCapsuleRegistrationV1};
use distill_store::state::{
    load_policy_digest, CleanupDisposition, PipelineEpoch, PipelinePoison, PipelinePoisonCode,
    PipelinePoisonOrigin, PipelineState, ReadableBundleSource, SkeletonFailureCode, VersionPoison,
    VersionPoisonV1,
};
use distill_store::{Store, StoreConfig, StoreError};

fn cfg(dir: &tempfile::TempDir) -> StoreConfig {
    StoreConfig::new(dir.path().join(".distill"))
}

fn validated_epoch(
    dylib_hash: [u8; 32],
    custom: Option<(TypeUuid, LogicalHash)>,
) -> ValidatedPipelineEpoch {
    let authority = consumer_bootstrap_authority_v1().unwrap();
    let mut rows = authority.rows().to_vec();
    if let Some((type_uuid, logical_hash)) = custom {
        rows.push(
            CompiledTypeRow::new(
                type_uuid,
                logical_hash,
                [4; 32],
                false,
                RegistryExtrasV1::canonical(vec![RegistryExtraRow {
                    node: SchemaNodeId(0),
                    path: vec![],
                    fact: RegistryExtraFact::BuildOnly(false),
                }])
                .unwrap(),
            )
            .unwrap(),
        );
    }
    let table = CompiledTypeTable::canonical(rows).unwrap();
    let policy = table
        .rows
        .iter()
        .map(|row| (row.type_uuid, row.build_only))
        .collect::<Vec<_>>();
    let epoch = PipelineEpoch {
        dylib_hash,
        load_policy_digest: load_policy_digest(&policy),
        compiled_types: table.digest,
        target_set: CanonicalTargetSet::canonical(vec![]).unwrap(),
        schema_registry: table
            .rows
            .iter()
            .map(|row| (row.type_uuid, row.logical_hash))
            .collect(),
        registrations: vec![],
    };
    ValidatedPipelineEpoch::validate(epoch, &table, authority).unwrap()
}

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

fn pipeline_poison(message: &str) -> PipelinePoison {
    PipelinePoison::new(
        PipelinePoisonCode::CandidateRegistration,
        PipelinePoisonOrigin::CandidateOpen,
        CleanupDisposition::CleanedAndClosed,
        message,
    )
    .unwrap()
}

fn commit(store: &mut Store, key: u8) {
    store
        .commit_build(BuildCommit {
            key_kind: KeyKind::Processor,
            static_input_key: [key; 32],
            asset_uuid: AssetUuid([7u8; 16]),
            static_inputs_canonical: vec![],
            trace: vec![key],
            outcome: CommitOutcome::Success {
                payload_kind: PayloadKind::ProcessorOutput,
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

    assert_eq!(store.input_version().0, 2, "two input events");
    assert_eq!(store.memo_seq().0, 3, "three memo commits");

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
                },
            )?;
            txn.upsert_bundle(&BundleMeta {
                bundle: BundleUuid([1u8; 16]),
                root,
                path: "a.bundle".into(),
                format_version: 1,
                content_hash: ContentHash([1u8; 32]),
                origin: None,
            })?;
            txn.upsert_asset(&AssetRecord {
                asset: AssetUuid([2u8; 16]),
                bundle: BundleUuid([1u8; 16]),
                local_id: "e".into(),
                type_uuid: TypeUuid([3u8; 16]),
                logical_hash: LogicalHash([4u8; 32]),
                authoring_only: false,
                tags: vec!["t".into()],
            })?;
            txn.project_verified_lineage_manifest(
                &VerifiedSchemaLineageManifest::from_verified_source(
                    ContentHash([10u8; 32]),
                    SchemaLineageManifest {
                        types: [(
                            TypeUuid([3u8; 16]),
                            AcceptedTypeLineage {
                                epochs: vec![AcceptedSchemaEpoch {
                                    digest: LogicalHash([5u8; 32]),
                                    forward_parent: None,
                                }],
                                current: 0,
                                authority: TypeAuthorityState::Active,
                            },
                        )]
                        .into_iter()
                        .collect(),
                    },
                ),
            )?;
            txn.stage_tool(
                "tool",
                ToolCapsuleRegistrationV1 {
                    files: vec![ResolvedToolCapsuleFile {
                        path: "bin/tool".into(),
                        role: ToolCapsuleFileRole::Launcher,
                        executable: true,
                        bytes: b"tool bytes".to_vec(),
                    }],
                    resolved_interpreter: None,
                    launch: ToolLaunchMetadataV1 {
                        argv0: "bin/tool".into(),
                        interpreter_args: vec![],
                    },
                    environment: vec![],
                    cwd_policy: ToolCwdPolicy::EmptyScratch,
                    platform: ToolPlatformBinding::ExplicitResidual {
                        platform_id: "test-platform".into(),
                        system_runtime_class: "test-runtime".into(),
                    },
                },
            )?;
            txn.publish_pipeline_epoch(&validated_epoch(
                [6u8; 32],
                Some((TypeUuid([3u8; 16]), LogicalHash([5u8; 32]))),
            ))?;
            Err(StoreError::Poisoned {
                error: "abort everything".into(),
            })
        })
        .unwrap_err();
    assert!(matches!(err, StoreError::Poisoned { .. }));

    // None of it published — readers observe all metadata for a given
    // tree state, or none of it.
    assert_eq!(store.input_version().0, 0);
    assert!(store.entry(AssetUuid([2u8; 16])).unwrap().is_none());
    assert!(store.bundle(BundleUuid([1u8; 16])).unwrap().is_none());
    assert!(store.lineage(TypeUuid([3u8; 16])).unwrap().is_empty());
    assert!(store.tool("tool").unwrap().is_none());
    assert!(store.pipeline_state().unwrap().is_none());
    assert_eq!(
        store.logical_path("a.bundle").unwrap(),
        distill_store::files::LogicalPathState::Missing
    );
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
                },
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
                },
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
fn pure_metadata_reads_survive_a_pipeline_poison() {
    // §13: the pipeline poison gates pipeline-dependent operations;
    // path-index and CAS reads remain valid.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    commit(&mut store, 1);
    let hash = *blake3::hash(&[1u8; 64]).as_bytes();
    store
        .input_transaction(|txn| {
            let root = txn.intern_root("main")?;
            txn.set_path_entry("a.bundle", root, AssetUuid([2u8; 16]))?;
            txn.publish_pipeline_poison(&pipeline_poison("candidate rejected: duplicate type uuid"))
        })
        .unwrap();

    let state = store.pipeline_state().unwrap().expect("published");
    assert!(matches!(state, PipelineState::Poisoned { .. }));
    assert!(state.epoch().is_err());

    // CAS reads and path resolution still answer.
    assert_eq!(store.cas_read(&hash).unwrap(), vec![1u8; 64]);
    assert_eq!(
        store.resolve_path("a.bundle").unwrap(),
        Some(AssetUuid([2u8; 16]))
    );
    let _ = store.input_version();
}

#[test]
fn version_poison_and_pipeline_poison_are_distinct_gates() {
    // The version-global poison (§7) blocks the asset namespace even
    // when the pipeline is healthy — and vice versa.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    store
        .input_transaction(|txn| {
            txn.project_verified_lineage_manifest(
                &VerifiedSchemaLineageManifest::from_verified_source(
                    ContentHash([11u8; 32]),
                    SchemaLineageManifest::default(),
                ),
            )?;
            txn.publish_pipeline_epoch(&validated_epoch([1u8; 32], None))?;
            let poison = version_poison("identity collision");
            txn.set_version_poison(Some(&poison))
        })
        .unwrap();

    // Pipeline healthy…
    assert!(store.pipeline_state().unwrap().unwrap().epoch().is_ok());
    // …but the namespace is uniformly poisoned.
    assert!(matches!(
        store.resolve_path("x"),
        Err(StoreError::Poisoned { .. })
    ));
    assert!(matches!(
        store.entry(AssetUuid([1u8; 16])),
        Err(StoreError::Poisoned { .. })
    ));
    // CAS reads are pure metadata: valid under both poisons.
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
    let seq_before = store.memo_seq();
    drop(store);
    let mut store = Store::open(cfg(&dir)).unwrap();
    assert_eq!(store.memo_seq(), seq_before);
    commit(&mut store, 2);
    assert!(
        store.memo_seq() > seq_before,
        "the memo sequence never rewinds"
    );
}
