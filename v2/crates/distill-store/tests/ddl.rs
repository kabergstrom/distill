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
    // indexed CAS generation), asset_tags (the `assets` search tags),
    // result_candidates / derived_outputs / derived_assertions /
    // cas_extents / cas_segments (the three roles of §13's `artifacts`
    // row), registrations (the pipeline_state registration list), pins
    // (the eviction observability rule), write_intents + displaced
    // (§14's journal and quarantine, which live in daemon state), and
    // codegen_outputs (§20's daemon-owned expected-preimage authority).
    let expected: BTreeSet<String> = [
        "files",
        "dirty_files",
        "rename_events",
        "bundles",
        "assets",
        "asset_tags",
        "asset_tag_index",
        "path_index",
        "deps",
        "schemas",
        "result_candidates",
        "derived_outputs",
        "derived_assertions",
        "cas_extents",
        "cas_segments",
        "pipeline_state",
        "pipeline_schema_registry",
        "pipeline_candidate_schema_registry",
        "pipeline_target_set",
        "pipeline_candidate_target_set",
        "configuration_state",
        "pending_restart",
        "registrations",
        "tools",
        "schema_lineage",
        "schema_lineage_current",
        "schema_lineage_state",
        "roots",
        "store_meta",
        "pins",
        "write_intents",
        "displaced",
        "publication_groups",
        "publication_group_children",
        "codegen_outputs",
        "watched_import_failures",
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
        ["root_id", "path", "mtime", "size", "kind", "content_hash"]
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
            "origin_group_path"
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
            "logical_hash"
        ]
    );
    assert_eq!(columns(&conn, "asset_tags"), ["asset_uuid", "tag", "value"]);
    assert_eq!(
        columns(&conn, "asset_tag_index"),
        [
            "asset_uuid",
            "tag_epoch",
            "planner_version",
            "dylib_hash",
            "trace",
            "poison"
        ]
    );
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
fn schema_lineage_records_the_chain_with_generations_and_digests() {
    // §13: append-only accepted history and the independently movable
    // current cursor are different tables. A manifest-availability row
    // distinguishes an empty manifest from missing authority.
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "schema_lineage"),
        [
            "type_uuid",
            "generation",
            "schema_hash",
            "forward_parent",
            "input_version"
        ]
    );
    assert_eq!(
        pk_columns(&conn, "schema_lineage"),
        ["type_uuid", "generation"]
    );
    assert_eq!(
        columns(&conn, "schema_lineage_current"),
        [
            "type_uuid",
            "current_cursor",
            "chain_digest",
            "authority",
            "retired_from",
            "input_version"
        ]
    );
    assert_eq!(pk_columns(&conn, "schema_lineage_current"), ["type_uuid"]);
    assert_eq!(
        columns(&conn, "schema_lineage_state"),
        ["id", "input_version", "manifest_hash"]
    );
}

#[test]
fn pipeline_state_row_shape() {
    // §13: module content identity plus typed poison/acceptance state.
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "pipeline_state"),
        [
            "id",
            "dylib_hash",
            "input_version",
            "poison_code",
            "poison_origin",
            "poison_cleanup",
            "poison_identity",
            "poison_message",
            "acceptance_candidate_dylib_hash",
            "acceptance_manifest_hash"
        ]
    );
    assert_eq!(
        columns(&conn, "registrations"),
        ["kind", "reg_id", "version"]
    );
    assert_eq!(
        columns(&conn, "pipeline_schema_registry"),
        ["type_uuid", "logical_hash"]
    );
    assert_eq!(
        columns(&conn, "pipeline_candidate_schema_registry"),
        ["type_uuid", "logical_hash"]
    );
    assert_eq!(
        columns(&conn, "pipeline_target_set"),
        ["name", "target_definition_hash"]
    );
    assert_eq!(
        columns(&conn, "pipeline_candidate_target_set"),
        ["name", "target_definition_hash"]
    );
}

#[test]
fn configuration_state_and_pending_restart_are_representable() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "configuration_state"),
        [
            "id",
            "active_generation",
            "input_version",
            "poison_code",
            "poison_detail_version",
            "poison_detail",
            "poison_reason_hash",
            "poison_message"
        ]
    );
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
    // secondarily by trace digest; commits append candidates, never
    // overwrite; result records are tagged by key kind.
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        pk_columns(&conn, "result_candidates"),
        ["key_kind", "static_key", "trace_digest"]
    );
    let cols = columns(&conn, "result_candidates");
    for required in [
        "key_kind",
        "static_key",
        "trace_digest",
        "memo_seq",
        "segment",
        "offset",
        "len",
        "last_used",
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
    for table in ["derived_outputs", "derived_assertions", "assets", "bundles"] {
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
        ["segment_id", "file_name", "segment_kind", "indexed_len"]
    );
}

#[test]
fn derived_output_namespace_and_assertions_are_separate_tables() {
    // §9/§13: the input-versioned namespace index is the only authority
    // for child resolution; per-result assertion rows are memo data.
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "derived_outputs"),
        ["child_uuid", "parent_uuid", "output_key"]
    );
    assert_eq!(pk_columns(&conn, "derived_outputs"), ["child_uuid"]);
    assert_eq!(
        columns(&conn, "derived_assertions"),
        ["child_uuid", "parent_uuid", "output_key", "memo_seq"]
    );
}

#[test]
fn dirty_queue_and_rename_log_are_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "dirty_files"),
        ["seq", "root_id", "path", "exists_flag"]
    );
    assert_eq!(
        columns(&conn, "rename_events"),
        ["seq", "root_id", "from_path", "to_path"]
    );
}

#[test]
fn write_intent_journal_shape() {
    // §14: target path, temp path, conflict path, expected pre-image
    // hash and proposed content hash. Physical quarantine locations are
    // one-to-many rows keyed by intent identity, so swap-back objects
    // cannot alias the original displaced inode.
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "write_intents"),
        [
            "intent_id",
            "target_path",
            "temp_path",
            "conflict_path",
            "pre_image_hash",
            "proposed_hash",
            "rename_aside_state",
            "retired"
        ]
    );
    assert_eq!(
        columns(&conn, "displaced"),
        [
            "displacement_id",
            "intent_id",
            "ordinal",
            "content_hash",
            "origin_path",
            "quarantine_path",
            "quarantined_at",
            "restored",
            "cleaned_at",
            "cleanup_reason"
        ]
    );
}

#[test]
fn multi_path_publication_parent_names_its_basis_and_children() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open_conn(&dir);
    assert_eq!(
        columns(&conn, "publication_groups"),
        ["group_id", "kind", "basis", "retired"]
    );
    assert_eq!(
        columns(&conn, "publication_group_children"),
        ["group_id", "ordinal", "intent_id"]
    );
    assert_eq!(
        pk_columns(&conn, "publication_group_children"),
        ["group_id", "ordinal"]
    );
}
