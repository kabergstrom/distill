//! §13 crash-safety: torn-write detection and tail truncation, the
//! result record as commit marker (uncovered payloads publish nothing),
//! index-drift rebuild, duplicate classification, and
//! stray- and dead-segment cleanup — each asserted through the typed
//! `RecoveryReport` and post-recovery reads.

use distill_core::id::AssetUuid;
use distill_store::cas::record::{encode_record, KeyKind, Record, RecordKind};
use distill_store::cas::{BuildCommit, CommitOutcome, OutputSpec, PayloadKind};
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

/// Make the index claim more of every segment than its file holds, which
/// forces the full rebuild on the next open.
fn force_rebuild(dir: &tempfile::TempDir) {
    let conn = rusqlite::Connection::open(dir.path().join(".distill/meta.sqlite")).unwrap();
    conn.execute("UPDATE cas_segments SET indexed_len = indexed_len + 1000000", [])
        .unwrap();
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
    let report = &recovery;
    assert!(!report.rebuilt_index);
    assert_eq!(report.adopted_results, 0);
    assert_eq!(report.orphaned_payloads, 0);
    assert!(report.truncated_tails.is_empty());
    assert!(report.removed_stray_segments.is_empty());
}

#[test]
fn a_torn_tail_is_truncated_and_the_data_before_it_survives() {
    // Truncate an archive mid-record: recovery classifies the tail as
    // torn, truncates to the last valid boundary, and everything already
    // committed still reads.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let hash1 = commit(&mut store, 1, b"first artifact");
    let hash2 = commit(&mut store, 2, b"second artifact");
    drop(store);

    let seg = &segment_files(&dir)[0];
    let full = std::fs::metadata(seg).unwrap().len();
    // Chop mid-way through the last record.
    std::fs::OpenOptions::new()
        .write(true)
        .open(seg)
        .unwrap()
        .set_len(full - 7)
        .unwrap();

    // The SQLite index still references the (now missing) tail: reopening
    // must reconcile rather than serve dangling extents. The recorded
    // indexed_len exceeds the file: the segment rescans from scratch.
    let (store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    let report = recovery.clone();
    assert!(
        !report.truncated_tails.is_empty(),
        "the torn tail was classified: {report:?}"
    );
    // The first commit fully survives.
    assert_eq!(store.cas_read(&hash1).unwrap(), b"first artifact");
    let c1 = store
        .lookup_candidates(KeyKind::Processor, &[1u8; 32])
        .unwrap();
    assert_eq!(c1.len(), 1);
    // The second commit's result record was torn: it publishes nothing.
    assert!(store.cas_read(&hash2).is_err());
    assert!(store
        .lookup_candidates(KeyKind::Processor, &[2u8; 32])
        .unwrap()
        .is_empty());

    // The file was physically truncated to a valid boundary: a further
    // reopen is clean.
    drop(store);
    let (store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    assert!(recovery.truncated_tails.is_empty());
    assert_eq!(store.cas_read(&hash1).unwrap(), b"first artifact");
}

#[test]
fn payload_records_without_a_result_record_publish_nothing() {
    // §13: the result record is the commit marker — a crash after
    // output 2 of 3 publishes nothing.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let committed = commit(&mut store, 1, b"committed artifact");
    drop(store);

    // Simulate the crash: append two valid payload records with no
    // covering result record, then reopen.
    let orphan_a = encode_record(&Record {
        kind: RecordKind::ProcessorOutput,
        asset_uuid: PARENT,
        static_input_key: vec![],
        output_key: String::new(),
        payload: b"orphan output A".to_vec(),
    });
    let orphan_b = encode_record(&Record {
        kind: RecordKind::Debug,
        asset_uuid: PARENT,
        static_input_key: vec![],
        output_key: "dbg".to_owned(),
        payload: b"orphan debug B".to_vec(),
    });
    let seg = segment_files(&dir).pop().unwrap();
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().append(true).open(&seg).unwrap();
    f.write_all(&orphan_a).unwrap();
    f.write_all(&orphan_b).unwrap();
    f.sync_all().unwrap();
    drop(f);

    let (store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    let report = &recovery;
    assert_eq!(report.orphaned_payloads, 2, "{report:?}");
    assert_eq!(report.adopted_results, 0);
    // The orphans are not readable — never indexed.
    assert!(matches!(
        store.cas_read(blake3::hash(b"orphan output A").as_bytes()),
        Err(StoreError::NotFound { .. })
    ));
    // Prior committed state is untouched.
    assert_eq!(store.cas_read(&committed).unwrap(), b"committed artifact");
}

#[test]
fn an_unindexed_committed_group_is_adopted_on_reopen() {
    // A crash after the segment fsync but before the SQLite index rows:
    // the segments are the durable record, so recovery adopts the
    // committed group from the scan.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    commit(&mut store, 1, b"indexed artifact");
    drop(store);

    // Write a full valid group (payload + result) directly to the
    // segment, bypassing SQLite — exactly the crash window.
    let payload_bytes = b"adopted artifact".to_vec();
    let payload_rec = encode_record(&Record {
        kind: RecordKind::ProcessorOutput,
        asset_uuid: PARENT,
        static_input_key: vec![],
        output_key: String::new(),
        payload: payload_bytes.clone(),
    });
    let result_payload = distill_store::cas::record::ResultPayload {
        key_kind: KeyKind::Processor,
        static_inputs_canonical: vec![],
        trace: b"adopted trace".to_vec(),
        outcome: distill_store::cas::record::ResultOutcome::Success {
            outputs: vec![distill_store::cas::record::OutputRow {
                output_key: String::new(),
                type_uuids: vec![],
                content_hash: distill_core::id::ContentHash(
                    *blake3::hash(&payload_bytes).as_bytes(),
                ),
            }],
            aux: vec![],
        },
    };
    let result_rec = encode_record(&Record {
        kind: RecordKind::Result,
        asset_uuid: PARENT,
        static_input_key: vec![9u8; 32],
        output_key: String::new(),
        payload: result_payload.encode(),
    });
    let seg = segment_files(&dir).pop().unwrap();
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().append(true).open(&seg).unwrap();
    f.write_all(&payload_rec).unwrap();
    f.write_all(&result_rec).unwrap();
    f.sync_all().unwrap();
    drop(f);

    let (store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    let report = &recovery;
    assert_eq!(report.adopted_results, 1, "{report:?}");
    assert_eq!(report.orphaned_payloads, 0);
    assert_eq!(
        store
            .cas_read(blake3::hash(b"adopted artifact").as_bytes())
            .unwrap(),
        b"adopted artifact"
    );
    let candidates = store
        .lookup_candidates(KeyKind::Processor, &[9u8; 32])
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].payload.trace, b"adopted trace");
    assert!(
        store.memo_seq().unwrap().0 >= 2,
        "adoption advanced the memo sequence"
    );
}

#[test]
fn recovery_checkpoints_cross_segment_groups_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = cfg(&dir);
    config.segment_size = 256;
    let mut store = Store::open(config.clone()).unwrap();
    let bytes = vec![0x5a; 1024];
    let hash = commit(&mut store, 11, &bytes);
    drop(store);

    let database_path = dir.path().join(".distill/meta.sqlite");
    let connection = rusqlite::Connection::open(&database_path).unwrap();
    connection
        .execute_batch(
            "DELETE FROM cas_refs;
             DELETE FROM cas_extents;
             DELETE FROM result_candidates;
             DELETE FROM derived_assertions;
             UPDATE cas_segments SET indexed_len = 0;",
        )
        .unwrap();
    let segment_ids: Vec<i64> = {
        let mut statement = connection
            .prepare("SELECT segment_id FROM cas_segments ORDER BY segment_id")
            .unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    assert!(
        segment_ids.len() >= 2,
        "fixture must cross a segment boundary"
    );
    let fail_segment = segment_ids[1];
    connection
        .execute_batch(&format!(
            "CREATE TRIGGER fail_recovery_checkpoint
             BEFORE UPDATE OF indexed_len ON cas_segments
             WHEN NEW.segment_id = {fail_segment} AND NEW.indexed_len > 0
             BEGIN SELECT RAISE(ABORT, 'injected recovery crash'); END;"
        ))
        .unwrap();
    drop(connection);

    assert!(Store::open(config.clone()).is_err());

    let connection = rusqlite::Connection::open(&database_path).unwrap();
    let advanced: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM cas_segments WHERE indexed_len != 0",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        advanced, 0,
        "a failed recovery must not checkpoint an earlier payload segment"
    );
    connection
        .execute_batch("DROP TRIGGER fail_recovery_checkpoint")
        .unwrap();
    drop(connection);

    let store = Store::open(config).unwrap();
    assert_eq!(store.cas_read(&hash).unwrap(), bytes);
    assert_eq!(
        store
            .lookup_candidates(KeyKind::Processor, &[11; 32])
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn index_drift_discards_and_rebuilds_the_index() {
    // An index that claims more of a segment than its file holds is
    // discarded and rebuilt from the segments before any read.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let hash1 = commit(&mut store, 1, b"first artifact");
    let hash2 = commit(&mut store, 2, b"second artifact");
    drop(store);
    force_rebuild(&dir);

    let (store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    let report = &recovery;
    assert!(report.rebuilt_index, "{report:?}");
    assert_eq!(report.adopted_results, 2, "both groups rescanned");
    // Everything reads and both buckets resolve after the rebuild.
    assert_eq!(store.cas_read(&hash1).unwrap(), b"first artifact");
    assert_eq!(store.cas_read(&hash2).unwrap(), b"second artifact");
    assert_eq!(
        store
            .lookup_candidates(KeyKind::Processor, &[1u8; 32])
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .lookup_candidates(KeyKind::Processor, &[2u8; 32])
            .unwrap()
            .len(),
        1
    );

    // And the drift healed: next open is quiet.
    drop(store);
    assert!(
        !Store::open_with_recovery(cfg(&dir))
            .unwrap()
            .1
            .rebuilt_index
    );
}

#[test]
fn duplicate_content_hashes_keep_the_last_and_count_the_rest_garbage() {
    // §13: duplicate content hashes are byte-identical by definition —
    // recovery keeps the last and marks the rest garbage.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    commit(&mut store, 1, b"same bytes");
    // A commit skips payloads the index already holds; forget the extent so
    // the second commit appends the same bytes again, as two writers racing
    // past that hint would.
    let conn = rusqlite::Connection::open(dir.path().join(".distill/meta.sqlite")).unwrap();
    for table in ["cas_refs", "cas_extents"] {
        conn.execute(
            &format!("DELETE FROM {table} WHERE content_hash = ?1"),
            [blake3::hash(b"same bytes").as_bytes().as_slice()],
        )
        .unwrap();
    }
    commit(&mut store, 2, b"same bytes"); // same content, different key
    drop(store);

    // Force a full rebuild so the scan sees both occurrences.
    force_rebuild(&dir);

    let (store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    let report = &recovery;
    assert!(report.rebuilt_index);
    assert_eq!(report.duplicate_payloads, 1, "{report:?}");
    assert_eq!(
        store
            .cas_read(blake3::hash(b"same bytes").as_bytes())
            .unwrap(),
        b"same bytes"
    );
}

#[test]
fn stray_segment_files_are_removed() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    commit(&mut store, 1, b"artifact");
    drop(store);

    // Drop a segment-shaped stray (a rolled-back allocation: file created,
    // row never committed).
    let cas_dir = dir.path().join(".distill/cas");
    std::fs::write(cas_dir.join("seg-00000000000000ff.dsr"), b"garbage").unwrap();

    let (_store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    assert_eq!(
        recovery.removed_stray_segments,
        vec!["seg-00000000000000ff.dsr".to_owned()]
    );
    assert!(!cas_dir.join("seg-00000000000000ff.dsr").exists());
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

#[test]
fn corruption_inside_the_unindexed_tail_truncates_from_the_corrupt_record() {
    // Bit-flip the first byte of an appended (unindexed) record: the
    // scan stops there and truncates; earlier indexed data is untouched.
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(cfg(&dir)).unwrap();
    let hash1 = commit(&mut store, 1, b"good artifact");
    drop(store);

    let seg = segment_files(&dir).pop().unwrap();
    let valid_len = std::fs::metadata(&seg).unwrap().len();
    let mut garbage = encode_record(&Record {
        kind: RecordKind::ProcessorOutput,
        asset_uuid: PARENT,
        static_input_key: vec![],
        output_key: String::new(),
        payload: b"will be corrupted".to_vec(),
    });
    garbage[40] ^= 0xA5; // corrupt inside the header
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().append(true).open(&seg).unwrap();
    f.write_all(&garbage).unwrap();
    f.sync_all().unwrap();
    drop(f);

    let (store, recovery) = Store::open_with_recovery(cfg(&dir)).unwrap();
    let report = &recovery;
    assert_eq!(report.truncated_tails.len(), 1);
    assert_eq!(
        report.truncated_tails[0].1, valid_len,
        "truncated at the last valid boundary"
    );
    assert_eq!(std::fs::metadata(&seg).unwrap().len(), valid_len);
    assert_eq!(store.cas_read(&hash1).unwrap(), b"good artifact");
}
