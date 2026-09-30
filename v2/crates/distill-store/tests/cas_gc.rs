//! §13 GC: eviction deletes index rows only and is always safe (everything
//! rebuildable); `cas_refs` holds a result's whole output table (aux and
//! wire trees included) as one unit; compaction repoints the index and
//! leaves dead segment files to the sweeper.

use std::time::Duration;

use distill_core::id::{AssetUuid, LogicalHash, TypeUuid};
use distill_store::cas::record::KeyKind;
use distill_store::cas::{
    AuxSpec, BuildCommit, CommitOutcome, OutputSpec, PayloadKind, SegmentSweeper,
};
use distill_store::{Store, StoreConfig, StoreError};
use distill_wire::artifact::{write_artifact, ArtifactHeader};
use distill_wire::dswl::dswl_bytes;
use distill_wire::wire::WireNode;

const PARENT: AssetUuid = AssetUuid([7u8; 16]);

fn cfg(dir: &tempfile::TempDir) -> StoreConfig {
    StoreConfig::new(dir.path().join(".distill"))
}

fn commit_with_aux(
    store: &mut Store,
    key: u8,
    bytes: &[u8],
    aux: &[u8],
) -> ([u8; 32], [u8; 32], [u8; 32]) {
    let receipt = store
        .commit_build(BuildCommit {
            wire_trees: Vec::new(),
            key_kind: KeyKind::Processor,
            static_input_key: [key; 32],
            asset_uuid: PARENT,
            static_inputs_canonical: vec![],
            trace: vec![key],
            outcome: CommitOutcome::Success {
                payload_kind: PayloadKind::ProcessorOutput,
                outputs: vec![OutputSpec {
                    output_key: String::new(),
                    type_uuids: vec![],
                    bytes: bytes.to_vec(),
                }],
                aux: vec![AuxSpec {
                    debug_key: "dbg".to_owned(),
                    bytes: aux.to_vec(),
                }],
            },
        })
        .unwrap();
    (
        receipt.outputs[0].1 .0,
        receipt.aux[0].1 .0,
        receipt.trace_digest,
    )
}

fn commit_artifact(
    store: &mut Store,
    key: u8,
    artifact: &[u8],
    wire_trees: Vec<Vec<u8>>,
) -> ([u8; 32], [u8; 32]) {
    let receipt = store
        .commit_build(BuildCommit {
            wire_trees,
            key_kind: KeyKind::Processor,
            static_input_key: [key; 32],
            asset_uuid: PARENT,
            static_inputs_canonical: vec![],
            trace: vec![key],
            outcome: CommitOutcome::Success {
                payload_kind: PayloadKind::ProcessorOutput,
                outputs: vec![OutputSpec {
                    output_key: String::new(),
                    type_uuids: vec![],
                    bytes: artifact.to_vec(),
                }],
                aux: vec![],
            },
        })
        .unwrap();
    (receipt.outputs[0].1 .0, receipt.trace_digest)
}

// ---- eviction ----

#[test]
fn evicting_an_unpinned_result_removes_the_whole_unit() {
    let (dir, mut store) = {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(cfg(&dir)).unwrap();
        (dir, s)
    };
    let _ = dir;
    let (out, aux, trace_digest) = commit_with_aux(&mut store, 1, b"artifact", b"debug");

    assert!(store
        .evict_result(KeyKind::Processor, &[1u8; 32], &trace_digest)
        .unwrap());
    assert!(store
        .lookup_candidates(KeyKind::Processor, &[1u8; 32])
        .unwrap()
        .is_empty());
    assert!(matches!(
        store.cas_read(&out),
        Err(StoreError::NotFound { .. })
    ));
    assert!(
        matches!(store.cas_read(&aux), Err(StoreError::NotFound { .. })),
        "aux payloads pin and evict with the result (§13)"
    );

    // Evicting an absent candidate is Ok(false), not an error.
    assert!(!store
        .evict_result(KeyKind::Processor, &[1u8; 32], &trace_digest)
        .unwrap());
}

#[test]
fn a_shared_extent_survives_until_its_last_referencing_result_is_evicted() {
    // Distinct inputs routinely produce identical bytes (§9): the extent
    // row must outlive any single result that references it.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (out1, _, digest1) = commit_with_aux(&mut store, 1, b"same bytes", b"dbg-1");
    let (out2, _, digest2) = commit_with_aux(&mut store, 2, b"same bytes", b"dbg-2");
    assert_eq!(out1, out2, "content-addressed collapse");

    assert!(store
        .evict_result(KeyKind::Processor, &[1u8; 32], &digest1)
        .unwrap());
    assert_eq!(
        store.cas_read(&out1).unwrap(),
        b"same bytes",
        "still referenced by the second result"
    );
    assert!(store
        .evict_result(KeyKind::Processor, &[2u8; 32], &digest2)
        .unwrap());
    assert!(matches!(
        store.cas_read(&out1),
        Err(StoreError::NotFound { .. })
    ));
}

fn unit_artifact(layout: distill_core::id::LayoutHash) -> Vec<u8> {
    write_artifact(
        &ArtifactHeader {
            asset_uuid: PARENT,
            authored_type: TypeUuid([1; 16]),
            terminal_type: TypeUuid([2; 16]),
            encoded_type: TypeUuid([3; 16]),
            logical_hash: LogicalHash([4; 32]),
            layout_hash: layout,
        },
        &[],
        &[],
        &[],
        &[],
    )
    .unwrap()
}

fn unit_layout() -> (Vec<u8>, distill_core::id::LayoutHash) {
    let wire_bytes = dswl_bytes(&WireNode::Unit { offset: 0 }).unwrap();
    let hash = distill_wire::dswl::dswl_hash(&WireNode::Unit { offset: 0 }).unwrap();
    (wire_bytes, hash)
}

#[test]
fn a_wire_tree_is_part_of_every_result_that_names_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (wire_bytes, layout) = unit_layout();
    let artifact = unit_artifact(layout);
    let (_, digest1) = commit_artifact(&mut store, 1, &artifact, vec![wire_bytes.clone()]);
    // The second commit finds the tree already in the CAS.
    let (_, digest2) = commit_artifact(&mut store, 2, &artifact, Vec::new());

    assert!(store
        .evict_result(KeyKind::Processor, &[1; 32], &digest1)
        .unwrap());
    assert_eq!(
        store.wire_tree_read(layout).unwrap(),
        wire_bytes,
        "the second result still references the shared layout"
    );
    assert!(store
        .evict_result(KeyKind::Processor, &[2; 32], &digest2)
        .unwrap());
    assert!(matches!(
        store.wire_tree_read(layout),
        Err(StoreError::NotFound { .. })
    ));
}

#[test]
fn a_commit_naming_an_absent_wire_tree_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (_, layout) = unit_layout();
    let result = store.commit_build(BuildCommit {
        wire_trees: Vec::new(),
        key_kind: KeyKind::Processor,
        static_input_key: [1; 32],
        asset_uuid: PARENT,
        static_inputs_canonical: vec![],
        trace: vec![1],
        outcome: CommitOutcome::Success {
            payload_kind: PayloadKind::ProcessorOutput,
            outputs: vec![OutputSpec {
                output_key: String::new(),
                type_uuids: vec![],
                bytes: unit_artifact(layout),
            }],
            aux: vec![],
        },
    });
    assert!(matches!(result, Err(StoreError::MissingWireTree { hash }) if hash == layout.0));
    assert!(store
        .lookup_candidates(KeyKind::Processor, &[1; 32])
        .unwrap()
        .is_empty());
}

// ---- cache-limit sweep ----

#[test]
fn the_cache_limit_sweep_evicts_whole_units_to_the_cap() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = cfg(&dir);
    config.cache_limit = 3000; // three ~1.2kB results exceed this
    let mut store = Store::open(config).unwrap();
    let (out1, _, _) = commit_with_aux(&mut store, 1, &[1u8; 1200], b"a");
    let (out2, _, _) = commit_with_aux(&mut store, 2, &[2u8; 1200], b"b");
    let (out3, _, _) = commit_with_aux(&mut store, 3, &[3u8; 1200], b"c");

    let report = store.enforce_cache_limit().unwrap();
    assert_eq!(report.evicted, 1, "one eviction reaches the cap");
    assert!(report.live_bytes <= 3000, "{report:?}");
    let gone = [out1, out2, out3]
        .iter()
        .filter(|hash| matches!(store.cas_read(hash), Err(StoreError::NotFound { .. })))
        .count();
    assert_eq!(gone, 1);
}

#[test]
fn an_installed_wire_tree_is_a_unit_of_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (wire_bytes, _) = unit_layout();
    let layout = store.put_wire_tree(&wire_bytes).unwrap();

    let report = store.enforce_cache_limit().unwrap();
    assert_eq!(report.evicted, 0);
    assert_eq!(store.wire_tree_read(layout).unwrap(), wire_bytes);
    store.evict_installed(&layout.0).unwrap();
    assert!(matches!(
        store.wire_tree_read(layout),
        Err(StoreError::NotFound { .. })
    ));
}

// ---- compaction ----

#[test]
fn compaction_repoints_live_records_and_the_sweeper_deletes_the_dead_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (survivor, _, _) = commit_with_aux(&mut store, 1, b"surviving artifact", b"dbg-s");
    let (_, _, dead_digest) = commit_with_aux(&mut store, 2, &[9u8; 4096], b"dbg-d");
    assert!(store
        .evict_result(KeyKind::Processor, &[2u8; 32], &dead_digest)
        .unwrap());

    let size_before: u64 = segment_bytes(&dir);
    let report = store.compact().unwrap();
    assert_eq!(report.dead_segments.len(), 1, "{report:?}");
    assert!(report.reclaimed_bytes > 4096, "{report:?}");
    // The dead file stays until the sweeper's grace has passed.
    assert_eq!(
        SegmentSweeper::new(Duration::from_secs(3600))
            .sweep(&mut store)
            .unwrap(),
        0
    );
    assert!(segment_bytes(&dir) > size_before);
    assert_eq!(SegmentSweeper::new(Duration::ZERO).sweep(&mut store).unwrap(), 1);
    let size_after: u64 = segment_bytes(&dir);
    assert!(
        size_after < size_before,
        "dead bytes reclaimed: {size_before} -> {size_after}"
    );

    // Everything live still reads; the bucket still resolves.
    assert_eq!(store.cas_read(&survivor).unwrap(), b"surviving artifact");
    let candidates = store
        .lookup_candidates(KeyKind::Processor, &[1u8; 32])
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert!(store
        .lookup_candidates(KeyKind::Processor, &[2u8; 32])
        .unwrap()
        .is_empty());

    // The index flip was transactional: a reopen does not rebuild.
    drop(store);
    let (store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    assert!(!recovery.rebuilt_index);
    assert_eq!(store.cas_read(&survivor).unwrap(), b"surviving artifact");
}

#[test]
fn a_snapshot_reads_its_blobs_until_the_dead_segment_is_swept() {
    // A reader that looked a location up before compaction and eviction
    // still reads it: the dead segment's file stays for the grace period.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (kept, _, _) = commit_with_aux(&mut store, 1, b"kept artifact", b"dbg-k");
    let (evicted, _, digest) = commit_with_aux(&mut store, 2, &[5u8; 4096], b"dbg-e");

    let view = store.reader().unwrap().begin_snapshot().unwrap();
    assert!(store
        .evict_result(KeyKind::Processor, &[2u8; 32], &digest)
        .unwrap());
    assert!(!store.compact().unwrap().dead_segments.is_empty());
    let mut sweeper = SegmentSweeper::new(Duration::from_millis(200));
    assert_eq!(sweeper.sweep(&mut store).unwrap(), 0);

    assert_eq!(view.cas_read(&kept).unwrap(), b"kept artifact");
    assert_eq!(view.cas_read(&evicted).unwrap(), vec![5u8; 4096]);
    assert!(matches!(store.cas_read(&evicted), Err(StoreError::NotFound { .. })));

    std::thread::sleep(Duration::from_millis(250));
    assert_eq!(sweeper.sweep(&mut store).unwrap(), 1);
    // Past the bound the old location is a cache miss, never wrong bytes.
    assert!(matches!(view.cas_read(&evicted), Err(StoreError::NotFound { .. })));
    let reader = view.into_reader().unwrap();
    assert_eq!(reader.cas_read(&kept).unwrap(), b"kept artifact");
}

#[test]
fn compacted_duplicate_payload_precedes_every_surviving_result() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (hash, _, first_digest) =
        commit_with_aux(&mut store, 1, b"shared artifact", b"first debug");
    let (duplicate, _, second_digest) =
        commit_with_aux(&mut store, 2, b"shared artifact", &[3u8; 4096]);
    assert_eq!(hash, duplicate);
    assert!(store
        .evict_result(KeyKind::Processor, &[2; 32], &second_digest)
        .unwrap());

    assert!(!store.compact().unwrap().dead_segments.is_empty());
    SegmentSweeper::new(Duration::ZERO).sweep(&mut store).unwrap();
    drop(store);

    // Force the rebuild path, which must reconstruct the complete index
    // from only the compacted log.
    let conn = rusqlite::Connection::open(dir.path().join(".distill/meta.sqlite")).unwrap();
    conn.execute("UPDATE cas_segments SET indexed_len = indexed_len + 1000000", [])
        .unwrap();
    drop(conn);

    let (store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    assert!(recovery.rebuilt_index);
    assert_eq!(store.cas_read(&hash).unwrap(), b"shared artifact");
    let candidates = store
        .lookup_candidates(KeyKind::Processor, &[1; 32])
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].trace_digest, first_digest);
}

#[test]
fn compaction_preserves_failure_records() {
    // Failure records memoize at their basis (§13) — compaction must
    // carry them like any committed result.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    store
        .commit_build(BuildCommit {
            wire_trees: Vec::new(),
            key_kind: KeyKind::Processor,
            static_input_key: [5u8; 32],
            asset_uuid: PARENT,
            static_inputs_canonical: vec![],
            trace: b"failing trace".to_vec(),
            outcome: CommitOutcome::Failure {
                cause: distill_store::cas::record::FailureCause::Local(
                    distill_store::cas::record::FailureFingerprint::Poisoned {
                        bundle: distill_core::id::BundleUuid([1u8; 16]),
                    },
                ),
            },
        })
        .unwrap();
    let (_, _, digest) = commit_with_aux(&mut store, 6, &[6u8; 4096], b"dbg");
    assert!(store
        .evict_result(KeyKind::Processor, &[6u8; 32], &digest)
        .unwrap());
    assert!(store.compact().unwrap().records_copied > 0);
    let candidates = store
        .lookup_candidates(KeyKind::Processor, &[5u8; 32])
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].payload.trace, b"failing trace");
}

fn segment_bytes(dir: &tempfile::TempDir) -> u64 {
    std::fs::read_dir(dir.path().join(".distill/cas"))
        .unwrap()
        .filter_map(|e| {
            let e = e.unwrap();
            let p = e.path();
            (p.extension().map(|x| x == "dsr"))
                .unwrap_or(false)
                .then(|| e.metadata().unwrap().len())
        })
        .sum()
}
