//! §13 GC: Bitcask-style compaction driven by cache policy — eviction is
//! always safe (everything rebuildable) but never observable: no pinned
//! ContentHash may be removed, pins hold a result's whole output table
//! as one unit, and the check runs inside the same transaction that
//! deletes the index rows.

use distill_core::id::{AssetUuid, LogicalHash, TypeUuid};
use distill_store::artifacts::PinKind;
use distill_store::cas::record::KeyKind;
use distill_store::cas::{AuxSpec, BuildCommit, CommitOutcome, OutputSpec, PayloadKind};
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

fn commit_artifact(store: &mut Store, key: u8, artifact: &[u8]) -> ([u8; 32], [u8; 32]) {
    let receipt = store
        .commit_build(BuildCommit {
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
fn a_pinned_output_blocks_eviction_of_the_whole_result() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (out, _aux, trace_digest) = commit_with_aux(&mut store, 1, b"artifact", b"debug");

    store.pin(PinKind::Lease, "client-42", &[out]).unwrap();
    match store.evict_result(KeyKind::Processor, &[1u8; 32], &trace_digest) {
        Err(StoreError::Pinned { hash }) => assert_eq!(hash, out),
        other => panic!("expected Pinned, got {other:?}"),
    }
    // Still fully readable.
    assert_eq!(store.cas_read(&out).unwrap(), b"artifact");

    // Releasing the pin unblocks eviction.
    store.unpin_holder(PinKind::Lease, "client-42").unwrap();
    assert!(store
        .evict_result(KeyKind::Processor, &[1u8; 32], &trace_digest)
        .unwrap());
}

#[test]
fn pinning_the_aux_hash_also_blocks_eviction() {
    // Pins hold a build result's whole output table as one unit (§13) —
    // aux payloads included.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (_out, aux, trace_digest) = commit_with_aux(&mut store, 1, b"artifact", b"debug");
    store.pin(PinKind::PackSession, "pack-1", &[aux]).unwrap();
    assert!(matches!(
        store.evict_result(KeyKind::Processor, &[1u8; 32], &trace_digest),
        Err(StoreError::Pinned { .. })
    ));
}

#[test]
fn ephemeral_pins_die_with_the_process_manifest_pins_persist() {
    // §13: ephemeral state (lease tables) carries no version and
    // rebuilds from scratch; manifest entries persist.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (out, _aux, trace_digest) = commit_with_aux(&mut store, 1, b"artifact", b"debug");
    store.pin(PinKind::Lease, "client-42", &[out]).unwrap();
    store.pin(PinKind::Manifest, "current", &[out]).unwrap();
    drop(store);

    let mut store = Store::open(cfg(&dir)).unwrap();
    // The manifest pin still blocks.
    assert!(matches!(
        store.evict_result(KeyKind::Processor, &[1u8; 32], &trace_digest),
        Err(StoreError::Pinned { .. })
    ));
    store.unpin_holder(PinKind::Manifest, "current").unwrap();
    // The lease pin evaporated with the process.
    assert!(store
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

#[test]
fn a_wire_tree_is_part_of_every_result_that_names_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let wire_bytes = dswl_bytes(&WireNode::Unit { offset: 0 }).unwrap();
    let layout = store.put_wire_tree(&wire_bytes).unwrap();
    let artifact = write_artifact(
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
    .unwrap();
    let (_, digest1) = commit_artifact(&mut store, 1, &artifact);
    let (_, digest2) = commit_artifact(&mut store, 2, &artifact);

    assert!(store
        .evict_result(KeyKind::Processor, &[1; 32], &digest1)
        .unwrap());
    assert_eq!(
        store.wire_tree_read(layout).unwrap(),
        wire_bytes,
        "the second result still references the shared layout"
    );

    store
        .pin(PinKind::Manifest, "current", &[layout.0])
        .unwrap();
    assert!(matches!(
        store.evict_result(KeyKind::Processor, &[2; 32], &digest2),
        Err(StoreError::Pinned { hash }) if hash == layout.0
    ));
    store.unpin_holder(PinKind::Manifest, "current").unwrap();
    assert!(store
        .evict_result(KeyKind::Processor, &[2; 32], &digest2)
        .unwrap());
    assert!(matches!(
        store.wire_tree_read(layout),
        Err(StoreError::NotFound { .. })
    ));
}

// ---- cache-limit sweep ----

#[test]
fn the_cache_limit_sweep_evicts_lru_first_and_skips_pinned() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = cfg(&dir);
    config.cache_limit = 3000; // three ~1.2kB results exceed this
    let mut store = Store::open(config).unwrap();
    // ~1.2kB per result. Commit three; touch #1 so #2 becomes LRU; pin #3.
    let (out1, _, _) = commit_with_aux(&mut store, 1, &[1u8; 1200], b"a");
    let (out2, _, _) = commit_with_aux(&mut store, 2, &[2u8; 1200], b"b");
    let (out3, _, _) = commit_with_aux(&mut store, 3, &[3u8; 1200], b"c");
    std::thread::sleep(std::time::Duration::from_millis(5));
    let _ = store
        .lookup_candidates(KeyKind::Processor, &[1u8; 32])
        .unwrap(); // touch #1
    store.pin(PinKind::Manifest, "current", &[out3]).unwrap();

    let report = store.enforce_cache_limit().unwrap();
    assert!(report.evicted >= 1);
    assert!(report.live_bytes <= 3000, "{report:?}");
    // #2 (LRU, unpinned) went first; #1 was touched, #3 is pinned.
    assert!(matches!(
        store.cas_read(&out2),
        Err(StoreError::NotFound { .. })
    ));
    assert_eq!(store.cas_read(&out1).unwrap(), vec![1u8; 1200]);
    assert_eq!(store.cas_read(&out3).unwrap(), vec![3u8; 1200]);
}

#[test]
fn an_unowned_wire_tree_is_collected_even_below_the_cache_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let wire_bytes = dswl_bytes(&WireNode::Unit { offset: 0 }).unwrap();
    let layout = store.put_wire_tree(&wire_bytes).unwrap();
    assert_eq!(store.wire_tree_read(layout).unwrap(), wire_bytes);

    let report = store.enforce_cache_limit().unwrap();
    assert_eq!(report.evicted, 0);
    assert!(matches!(
        store.wire_tree_read(layout),
        Err(StoreError::NotFound { .. })
    ));
}

// ---- compaction ----

#[test]
fn compaction_reclaims_dead_bytes_and_flips_the_generation() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (survivor, _, _) = commit_with_aux(&mut store, 1, b"surviving artifact", b"dbg-s");
    let (_, _, dead_digest) = commit_with_aux(&mut store, 2, b"dead artifact", b"dbg-d");
    let wire_bytes = dswl_bytes(&WireNode::Unit { offset: 0 }).unwrap();
    let wire = store.put_wire_tree(&wire_bytes).unwrap();
    assert!(store
        .evict_result(KeyKind::Processor, &[2u8; 32], &dead_digest)
        .unwrap());

    let size_before: u64 = segment_bytes(&dir);
    let report = store.compact().unwrap();
    let size_after: u64 = segment_bytes(&dir);
    assert!(
        size_after < size_before,
        "dead bytes reclaimed: {size_before} -> {size_after}"
    );
    assert!(report.new_generation > report.old_generation);

    // Everything live still reads; the bucket still resolves.
    assert_eq!(store.cas_read(&survivor).unwrap(), b"surviving artifact");
    assert!(matches!(
        store.wire_tree_read(wire),
        Err(StoreError::NotFound { .. })
    ));
    let candidates = store
        .lookup_candidates(KeyKind::Processor, &[1u8; 32])
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert!(store
        .lookup_candidates(KeyKind::Processor, &[2u8; 32])
        .unwrap()
        .is_empty());

    // The generation flip was transactional: a reopen sees agreement and
    // does not rebuild.
    drop(store);
    let (store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    assert!(!recovery.rebuilt_index);
    assert_eq!(store.cas_read(&survivor).unwrap(), b"surviving artifact");
}

#[test]
fn compacted_duplicate_payload_precedes_every_surviving_result() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (hash, _, first_digest) =
        commit_with_aux(&mut store, 1, b"shared artifact", b"first debug");
    let (duplicate, _, second_digest) =
        commit_with_aux(&mut store, 2, b"shared artifact", b"second debug");
    assert_eq!(hash, duplicate);
    assert!(store
        .evict_result(KeyKind::Processor, &[2; 32], &second_digest)
        .unwrap());

    store.compact().unwrap();
    drop(store);

    // Force the generation-mismatch recovery path that must be able to
    // reconstruct the complete index from only the compacted log.
    let current_path = dir.path().join(".distill/cas/CURRENT");
    let current = std::fs::read_to_string(&current_path).unwrap();
    let mut lines: Vec<_> = current.lines().map(str::to_owned).collect();
    let generation = lines[0]
        .strip_prefix("generation ")
        .unwrap()
        .parse::<u64>()
        .unwrap();
    lines[0] = format!("generation {}", generation + 1);
    std::fs::write(&current_path, lines.join("\n") + "\n").unwrap();

    let (mut store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
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
    store.compact().unwrap();
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
