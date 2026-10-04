//! §13 metadata-layer DDL pinning: every table the section defines
//! exists with the key shape the spec calls out. Two implementers
//! reading only these tests must agree on the schema.

use std::collections::BTreeSet;

use distill_store::{Store, StoreConfig};

fn open_conn(dir: &tempfile::TempDir) -> rusqlite::Connection {
    let config = StoreConfig::new(dir.path().join(".distill"));
    drop(Store::open(config.clone()).unwrap());
    rusqlite::Connection::open(config.state_path.join("meta.sqlite")).unwrap()
}

fn tables(conn: &rusqlite::Connection) -> BTreeSet<String> {
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
        .unwrap();
    stmt.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn columns(conn: &rusqlite::Connection, table: &str) -> Vec<String> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap();
    stmt.query_map([], |r| r.get::<_, String>(1))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn pk_columns(conn: &rusqlite::Connection, table: &str) -> Vec<String> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap();
    let mut cols: Vec<(i64, String)> = stmt
        .query_map([], |r| Ok((r.get::<_, i64>(5)?, r.get::<_, String>(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .filter(|(pk, _)| *pk > 0)
        .collect();
    cols.sort();
    cols.into_iter().map(|(_, name)| name).collect()
}

#[test]
fn every_section_13_table_exists() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    let got = tables(&conn);
    // The §13 table inventory, plus the store-internal tables §13/§14
    // require: store_meta (instance id, counters, watermark, poisons,
    // CAS segment ids), asset_tags (the `assets` search tags),
    // results / result_outputs / derived_outputs /
    // cas_extents / cas_segments (the three roles of §13's `artifacts`
    // row), cas_refs (what keeps each extent indexed), bundle_path_refs
    // (the reference fields a rename rewrites), and
    // codegen_outputs (§20's daemon-owned expected-preimage authority).
    let expected: BTreeSet<String> = [
        "files",
        "source_claims",
        "import_keys",
        "file_work",
        "bundles",
        "bundle_path_refs",
        "assets",
        "asset_tags",
        "tag_epochs",
        "results",
        "result_outputs",
        "derived_outputs",
        "cas_extents",
        "cas_segments",
        "cas_refs",
        "pending_restart",
        "tools",
        "roots",
        "store_meta",
        "errors",
        "scan_rejection_subjects",
        "codegen_outputs",
        "watched_import_failures",
        "change_log",
        "rpc_targets",
        "artifact_load_edges",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    assert_eq!(got, expected);
}

#[test]
fn codegen_outputs_are_exact_preimage_authority() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "codegen_outputs"),
        ["relative_path", "content_hash"]
    );
}

#[test]
fn watched_import_failures_are_memo_state_keyed_by_bundle() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "watched_import_failures"),
        [
            "bundle_uuid",
            "attempted_input_version",
            "basis",
            "terminal_kind",
            "terminal_code",
            "message",
            "memo_seq"
        ]
    );
    assert_eq!(
        pk_columns(&conn, "watched_import_failures"),
        ["bundle_uuid"]
    );
}

#[test]
fn files_is_keyed_per_root() {
    // §13: "(root id, normalized root-relative path) → mtime, size,
    // kind, content hash — physical tracking is per root".
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "files"),
        [
            "root_id",
            "path",
            "mtime",
            "size",
            "kind",
            "content_hash",
            "observation",
            "raw_path",
            "symlink_target",
            "canonical_path"
        ]
    );
    assert_eq!(pk_columns(&conn, "files"), ["root_id", "path"]);
}

#[test]
fn bundles_carry_the_physical_key_poison_and_directory_origin() {
    // §13: bundle uuid → (root id, normalized path), format version,
    // content hash; per-file indexing failures are rows, not absences —
    // the bundle-scoped poison row; directory-import ownership derives
    // at scan from generated bundles' DirectoryOrigin records (§8) into
    // the origin columns (never precious — rebuilt from the files).
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "bundles"),
        [
            "bundle_uuid",
            "root_id",
            "path",
            "format_version",
            "content_hash",
            "poison",
            "origin_rules_bundle",
            "origin_rule",
            "origin_group_root",
            "origin_group_path",
            "import_watched",
            "primary_asset"
        ]
    );
    assert_eq!(pk_columns(&conn, "bundles"), ["bundle_uuid"]);
}

#[test]
fn assets_row_shape() {
    // §13: asset uuid → bundle uuid, local_id, type_uuid, explicit
    // authoring/runtime role, logical hash, and search tags (tags
    // normalized into asset_tags).
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "assets"),
        [
            "asset_uuid",
            "bundle_uuid",
            "local_id",
            "type_uuid",
            "authoring_only",
            "logical_hash",
            "terminal_type",
            "tag_poison",
            "tag_module"
        ]
    );
    assert_eq!(columns(&conn, "asset_tags"), ["asset_uuid", "tag", "value"]);
}

#[test]
fn tools_table_is_the_tool_epoch() {
    // §13: tool key → (verified identity object, aggregate DSCT hash) — input-versioned.
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "tools"),
        [
            "tool_key",
            "present",
            "identity_object",
            "tool_hash",
            "input_version"
        ]
    );
    assert_eq!(pk_columns(&conn, "tools"), ["tool_key", "input_version"]);
}

#[test]
fn pending_restart_is_representable() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "pending_restart"),
        ["generation", "config_key", "config_value"]
    );
    assert_eq!(
        pk_columns(&conn, "pending_restart"),
        ["generation", "config_key"]
    );
}

#[test]
fn candidate_buckets_are_keyed_by_key_kind_static_key_and_trace_digest() {
    // §13/§9: static-input-key digest → candidate bucket, keyed
    // secondarily by trace digest; a result row is tagged by key kind and
    // its outputs are rows of their own, naming content hashes.
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        pk_columns(&conn, "results"),
        ["key_kind", "static_key", "trace_digest"]
    );
    assert_eq!(
        pk_columns(&conn, "result_outputs"),
        ["key_kind", "static_key", "trace_digest", "role", "name"]
    );
    assert_eq!(
        columns(&conn, "result_outputs"),
        ["key_kind", "static_key", "trace_digest", "role", "name", "types", "content_hash"]
    );
    let cols = columns(&conn, "results");
    for required in [
        "key_kind",
        "static_key",
        "trace_digest",
        "memo_seq",
        "asset_uuid",
        "trace",
        "failure",
    ] {
        assert!(
            cols.iter().any(|c| c == required),
            "missing column {required}"
        );
    }
}

#[test]
fn extent_index_holds_the_only_physical_location() {
    // §13: ContentHash → segment, offset, len — the CAS extent index.
    // Physical placement lives here and only here; result tables and
    // derived rows reference hashes, never offsets.
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "cas_extents"),
        ["content_hash", "segment", "offset", "len"]
    );
    assert_eq!(pk_columns(&conn, "cas_extents"), ["content_hash"]);
    // And no other artifact table sneaks a physical location in.
    for table in ["results", "result_outputs", "derived_outputs", "assets", "bundles"] {
        let cols = columns(&conn, table);
        assert!(
            !cols.iter().any(|c| c == "offset" || c == "segment"),
            "{table} must not embed physical placement"
        );
    }
}

#[test]
fn cas_segments_are_typed_regular_or_oversize() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "cas_segments"),
        ["segment_id", "file_name", "segment_kind", "indexed_len", "state", "owner"]
    );
}

#[test]
fn derived_output_namespace_is_keyed_by_child() {
    // §9/§13: the input-versioned namespace index is the only authority
    // for child resolution.
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "derived_outputs"),
        ["child_uuid", "parent_uuid", "output_key", "terminal_type"]
    );
    assert_eq!(pk_columns(&conn, "derived_outputs"), ["child_uuid"]);
}

#[test]
fn the_work_queue_holds_changed_paths_and_renames_in_one_order() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "file_work"),
        ["seq", "kind", "root_id", "path", "to_path", "observation"]
    );
    assert_eq!(pk_columns(&conn, "file_work"), ["seq"]);
}
