//! §13 crash safety: the transaction that indexes a record group is its
//! commit. Recovery cuts bytes past the index (rolled back or interrupted:
//! uncommitted), reports a lost tail without changing anything, and
//! deletes dead segments — each asserted through the typed
//! `RecoveryReport` and post-recovery reads. A segment file no row names
//! is past `next_segment_id`, uncommitted, and truncated by the next
//! allocation of its id.

use distill_core::id::AssetUuid;
use distill_store::cas::record::{encode_record, KeyKind, Record, RecordKind};
use distill_store::cas::{BuildCommit, CommitOutcome, OutputSpec, PayloadKind, RecoveryReport};
use distill_store::{Store, StoreConfig, StoreError};

const PARENT: AssetUuid = AssetUuid([7u8; 16]);

fn cfg(dir: &tempfile::TempDir) -> StoreConfig {
    StoreConfig::new(dir.path().join(".distill"))
}

fn commit(store: &mut Store, key: u8, bytes: &[u8]) -> [u8; 32] {
    store
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
                aux: vec![],
            },
        })
        .unwrap();
    *blake3::hash(bytes).as_bytes()
}

fn segment_files(dir: &tempfile::TempDir) -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = std::fs::read_dir(dir.path().join(".distill/cas"))
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            (p.extension().map(|x| x == "dsr"))
                .unwrap_or(false)
                .then_some(p)
        })
        .collect();
    v.sort();
    v
}

#[test]
fn a_clean_reopen_recovers_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    commit(&mut store, 1, b"artifact");
    drop(store);
    let (_store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    assert_eq!(recovery, RecoveryReport::default());
}

#[test]
fn bytes_past_the_index_are_cut_off_and_publish_nothing() {
    // What a rolled-back or interrupted transaction appended: here a whole
    // valid group (payload and result record) and a torn record after it.
    // None of it committed, so none of it is adopted.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let committed = commit(&mut store, 1, b"committed artifact");
    drop(store);

    let payload = b"uncommitted artifact".to_vec();
    let payload_record = encode_record(&Record {
        kind: RecordKind::ProcessorOutput,
        asset_uuid: PARENT,
        static_input_key: vec![],
        output_key: String::new(),
        payload: payload.clone(),
    });
    let result = distill_store::cas::record::ResultPayload {
        key_kind: KeyKind::Processor,
        static_inputs_canonical: vec![],
        trace: b"uncommitted trace".to_vec(),
        outcome: distill_store::cas::record::ResultOutcome::Success {
            outputs: vec![distill_store::cas::record::OutputRow {
                output_key: String::new(),
                type_uuids: vec![],
                content_hash: distill_core::id::ContentHash(*blake3::hash(&payload).as_bytes()),
            }],
            aux: vec![],
        },
    };
    let result_record = encode_record(&Record {
        kind: RecordKind::Result,
        asset_uuid: PARENT,
        static_input_key: vec![9u8; 32],
        output_key: String::new(),
        payload: result.encode(),
    });
    let seg = segment_files(&dir).pop().unwrap();
    let indexed = std::fs::metadata(&seg).unwrap().len();
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().append(true).open(&seg).unwrap();
    f.write_all(&payload_record).unwrap();
    f.write_all(&result_record).unwrap();
    f.write_all(&payload_record[..20]).unwrap();
    f.sync_all().unwrap();
    drop(f);

    let (store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    assert_eq!(recovery.truncated_tails, [(0, indexed)], "{recovery:?}");
    assert_eq!(std::fs::metadata(&seg).unwrap().len(), indexed);
    assert!(matches!(
        store.cas_read(blake3::hash(&payload).as_bytes()),
        Err(StoreError::NotFound { .. })
    ));
    assert!(store.lookup_candidates(KeyKind::Processor, &[9u8; 32]).unwrap().is_empty());
    assert_eq!(store.cas_read(&committed).unwrap(), b"committed artifact");
    drop(store);
    assert_eq!(
        Store::open_with_recovery(cfg(&dir)).unwrap().1,
        RecoveryReport::default()
    );
}

#[test]
fn a_lost_tail_is_reported_and_changes_nothing() {
    // A segment file shorter than its index (an external truncation, a
    // lying fsync) broke the filesystem's contract. Recovery reports it on
    // every open and repairs nothing: the lost result's rows stay, a read
    // of its bytes fails, and everything in other segments reads as
    // before.
    use distill_core::id::{ContentHash, TypeUuid};
    let dir = tempfile::tempdir().unwrap();
    let mut config = cfg(&dir);
    config.segment_size = 256;
    let mut store = Store::open(config.clone()).unwrap();
    let edges = [(AssetUuid([9; 16]), TypeUuid([1; 16]))];
    let installed = store.put_artifact(PARENT, &[5u8; 150], &edges).unwrap();
    let first = commit(&mut store, 1, b"first artifact");
    commit(&mut store, 2, b"second artifact");
    drop(store);

    let files = segment_files(&dir);
    assert!(files.len() >= 2, "the install and the results are in separate segments");
    let last = files.last().unwrap();
    let full = std::fs::metadata(last).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(last)
        .unwrap()
        .set_len(full - 7)
        .unwrap();

    for _ in 0..2 {
        let (store, recovery) = Store::open_with_recovery(config.clone()).unwrap();
        assert_eq!(recovery.lost_tails.len(), 1, "{recovery:?}");
        assert_eq!(recovery.lost_tails[0].1, full - 7);
        assert_eq!(recovery.truncated_tails, []);
        assert_eq!(std::fs::metadata(last).unwrap().len(), full - 7, "nothing was cut or grown");
        assert!(store.lookup_candidates(KeyKind::Processor, &[2u8; 32]).is_err(), "the lost result record reads as an error");
        assert_eq!(store.cas_read(&first).unwrap(), b"first artifact");
        assert_eq!(store.lookup_candidates(KeyKind::Processor, &[1u8; 32]).unwrap().len(), 1);
        assert_eq!(store.cas_read(&installed.0).unwrap(), [5u8; 150]);
        assert_eq!(store.artifact_load_edges(ContentHash(installed.0)).unwrap(), edges);
    }
}

#[test]
fn a_segment_file_no_row_names_is_left_for_the_allocation_of_its_id() {
    // A rolled-back allocation's file: created, its row and
    // `next_segment_id` never committed. Recovery does not look for it;
    // the next allocation of its id truncates it.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    commit(&mut store, 1, b"artifact");
    drop(store);

    let cas_dir = dir.path().join(".distill/cas");
    let next = segment_files(&dir).len();
    let uncommitted = cas_dir.join(format!("seg-{next:016x}.dsr"));
    std::fs::write(&uncommitted, b"garbage from an allocation that rolled back").unwrap();

    let (mut store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    assert_eq!(recovery, RecoveryReport::default());
    assert!(uncommitted.exists());
    // Recovery sealed the earlier writer's segment: this writer allocates
    // the next id.
    commit(&mut store, 2, b"after");
    assert_eq!(store.cas_read(blake3::hash(b"after").as_bytes()).unwrap(), b"after");
    assert_eq!(store.cas_read(blake3::hash(b"artifact").as_bytes()).unwrap(), b"artifact");
    let bytes = std::fs::read(&uncommitted).unwrap();
    assert!(!bytes.starts_with(b"garbage"), "the allocation truncated the uncommitted file");
}

#[test]
fn dead_segments_are_deleted_at_startup() {
    // The state lock guarantees no other process reads the store, so a
    // dead segment needs no grace period at open.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (_, digest) = {
        let hash = commit(&mut store, 1, &[1u8; 4096]);
        let candidates = store.lookup_candidates(KeyKind::Processor, &[1; 32]).unwrap();
        (hash, candidates[0].trace_digest)
    };
    assert!(store
        .evict_result(KeyKind::Processor, &[1; 32], &digest)
        .unwrap());
    let dead = store.compact().unwrap().dead_segments;
    assert_eq!(dead.len(), 1);
    let files = segment_files(&dir);
    drop(store);

    let (_store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    assert_eq!(recovery.removed_dead_segments, dead);
    assert_eq!(segment_files(&dir).len(), files.len() - 1);
}

/// A node publication as the daemon's flush writes it: one transaction
/// holding the node's result and its artifact install with load edges.
fn node_publication(key: u8) -> (BuildCommit, Vec<u8>) {
    use distill_core::id::{LogicalHash, TypeUuid};
    use distill_wire::artifact::{write_artifact, ArtifactHeader};
    use distill_wire::dswl::{dswl_bytes, dswl_hash};
    use distill_wire::wire::WireNode;
    let node = WireNode::Unit { offset: 0 };
    let artifact = write_artifact(
        &ArtifactHeader {
            asset_uuid: AssetUuid([key; 16]),
            authored_type: TypeUuid([1; 16]),
            terminal_type: TypeUuid([2; 16]),
            encoded_type: TypeUuid([3; 16]),
            logical_hash: LogicalHash([4; 32]),
            layout_hash: dswl_hash(&node).unwrap(),
        },
        &[AssetUuid([9; 16])],
        &[],
        &[],
        &[],
    )
    .unwrap();
    let commit = BuildCommit {
        wire_trees: vec![dswl_bytes(&node).unwrap()],
        key_kind: KeyKind::Node,
        static_input_key: [key; 32],
        asset_uuid: AssetUuid([key; 16]),
        static_inputs_canonical: vec![],
        trace: vec![key],
        outcome: CommitOutcome::Success {
            payload_kind: PayloadKind::ProcessorOutput,
            outputs: vec![OutputSpec {
                output_key: String::new(),
                type_uuids: vec![],
                bytes: artifact.clone(),
            }],
            aux: vec![],
        },
    };
    (commit, artifact)
}

#[test]
fn a_rolled_back_publication_stays_rolled_back_across_a_restart() {
    // The transaction that indexes a group is its commit: a node flush
    // that rolls back after appending its result (here a later write
    // fails; a crash before COMMIT is the same) publishes nothing, then
    // or after a restart. Adopting the group would serve the node as
    // built without the load edges its rolled-back install carried.
    use distill_core::id::{ContentHash, TypeUuid};
    let dir = tempfile::tempdir().unwrap();
    let edges = [(AssetUuid([9; 16]), TypeUuid([2; 16]))];
    let mut store = Store::open(cfg(&dir)).unwrap();
    let (first, first_bytes) = node_publication(1);
    store
        .write_transaction(|store| {
            store.commit_build(first)?;
            store.put_artifact(AssetUuid([1; 16]), &first_bytes, &edges)?;
            Ok(())
        })
        .unwrap();
    let (second, second_bytes) = node_publication(2);
    let rolled_back = store.write_transaction(|store| {
        store.commit_build(second)?;
        Err::<(), _>(StoreError::Rejected {
            detail: "a later write of the flush failed".to_owned(),
        })
    });
    assert!(rolled_back.is_err());
    assert!(store.candidate_rows(KeyKind::Node, &[2; 32]).unwrap().is_empty());
    drop(store);

    let (store, _) = Store::open_with_recovery(cfg(&dir)).unwrap();
    let hash = ContentHash(*blake3::hash(&second_bytes).as_bytes());
    assert!(
        store.candidate_rows(KeyKind::Node, &[2; 32]).unwrap().is_empty(),
        "recovery adopted a rolled-back node result; its load edges: {:?}",
        store.artifact_load_edges(hash).unwrap()
    );
    assert!(matches!(store.cas_read(&hash.0), Err(StoreError::NotFound { .. })));
    // The committed publication is intact.
    assert_eq!(store.candidate_rows(KeyKind::Node, &[1; 32]).unwrap().len(), 1);
    let first_hash = ContentHash(*blake3::hash(&first_bytes).as_bytes());
    assert_eq!(store.artifact_load_edges(first_hash).unwrap(), edges);
}
