//! The plans and costs of the namespace's selective reads. Every statement
//! those readers issue is captured from SQLite's trace and explained: none
//! may scan a namespace table. And over a large namespace, a narrow read
//! touches a small, fixed number of pages where the whole-table read it
//! replaced touches them all.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use rusqlite::Connection;

use crate::bundles::{
    AssetFilter, AssetRecord, BundleMeta, NamespaceSkeleton, SkeletonEntry, TagIndexUpdate,
};
use crate::db::{ReaderConn, StoreReader};
use crate::files::{path_name, FileKind, FileObservation, FileState, PathSelection};
use crate::{Store, StoreConfig};

const SCHEMA: LogicalHash = LogicalHash([0x5c; 32]);
const RUNTIME_TYPE: TypeUuid = TypeUuid([0x71; 16]);
const RECORD_TYPE: TypeUuid = TypeUuid([0x72; 16]);
/// The bundle every hundredth bundle references.
const REFERENCED: &str = "d00/b00000.bundle";

fn bundle_uuid(index: u32) -> BundleUuid {
    let mut bytes = [0x10; 16];
    bytes[12..].copy_from_slice(&index.to_be_bytes());
    BundleUuid(bytes)
}

fn asset_uuid(index: u32, entry: u8) -> AssetUuid {
    let mut bytes = [0x20; 16];
    bytes[0] = entry;
    bytes[12..].copy_from_slice(&index.to_be_bytes());
    AssetUuid(bytes)
}

fn bundle_path(index: u32) -> String {
    format!("d{:02}/b{index:05}.bundle", index % 50)
}

/// A namespace of `count` bundles across two roots: each with a served
/// runtime entry (tagged; one in a thousand rarely), a scanned file row and
/// its bytes, and the runtime entry as its primary; one in a hundred referencing
/// [`REFERENCED`], one in a thousand with an authoring-only `$record` entry
/// and beside a `.png` file, and one in five hundred poisoned instead.
fn populate(store: &mut Store, count: u32) {
    store
        .input_transaction(|txn| {
            let version = txn.version();
            let roots = [txn.intern_root("main")?, txn.intern_root("alt")?];
            for index in 0..count {
                let root = roots[(index % 2) as usize];
                let path = bundle_path(index);
                let bundle = bundle_uuid(index);
                let bytes = format!("bundle {index}").into_bytes();
                txn.upsert_file(
                    root,
                    &path,
                    &FileObservation::from(FileState {
                        mtime: i64::from(index),
                        size: bytes.len() as u64,
                        kind: FileKind::File,
                        content_hash: Some(ContentHash(*blake3::hash(&bytes).as_bytes())),
                    }),
                    version,
                )?;
                if index % 1000 == 3 {
                    let png = format!("d{:02}/t{index:05}.png", index % 50);
                    txn.upsert_file(
                        root,
                        &png,
                        &FileObservation::from(FileState {
                            mtime: 0,
                            size: 0,
                            kind: FileKind::File,
                            content_hash: Some(ContentHash([0; 32])),
                        }),
                        version,
                    )?;
                }
                let runtime = asset_uuid(index, 1);
                if index % 500 == 199 {
                    txn.poison_bundle(
                        &NamespaceSkeleton {
                            bundle,
                            root,
                            path,
                            format_version: 1,
                            content_hash: ContentHash(*blake3::hash(&bytes).as_bytes()),
                            entries: vec![SkeletonEntry {
                                asset: runtime,
                                local_id: "main".into(),
                                type_uuid: RUNTIME_TYPE,
                                authoring_only: false,
                                tags: BTreeMap::new(),
                            }],
                        },
                        "malformed",
                    )?;
                    continue;
                }
                txn.upsert_bundle(&BundleMeta {
                    bundle,
                    root,
                    path: path.clone(),
                    format_version: 1,
                    content_hash: ContentHash(*blake3::hash(&bytes).as_bytes()),
                    origin: None,
                    import_watched: index % 1000 == 7,
                })?;
                let mut tags = BTreeMap::from([(
                    "kind".to_owned(),
                    Some(if index % 3 == 0 { "mesh" } else { "texture" }.to_owned()),
                )]);
                if index % 1000 == 7 {
                    tags.insert("rare".into(), Some("yes".into()));
                }
                txn.upsert_asset(&AssetRecord {
                    asset: runtime,
                    bundle,
                    local_id: "main".into(),
                    type_uuid: RUNTIME_TYPE,
                    logical_hash: SCHEMA,
                    authoring_only: false,
                    tags,
                    terminal_type: Some(RUNTIME_TYPE),
                })?;
                if index % 1000 == 3 {
                    txn.upsert_asset(&AssetRecord {
                        asset: asset_uuid(index, 2),
                        bundle,
                        local_id: "$record".into(),
                        type_uuid: RECORD_TYPE,
                        logical_hash: SCHEMA,
                        authoring_only: true,
                        tags: BTreeMap::new(),
                        terminal_type: None,
                    })?;
                }
                if index % 100 == 1 {
                    txn.set_bundle_path_refs(bundle, [REFERENCED])?;
                }
                txn.set_primary_asset(bundle, runtime)?;
            }
            Ok(())
        })
        .unwrap();
}

fn store_with(count: u32) -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    populate(&mut store, count);
    (dir, store)
}

/// A filter selecting `path`'s runtime entries.
fn at_path(path: &str) -> AssetFilter {
    AssetFilter {
        bundle_path: Some(path.into()),
        ..AssetFilter::default()
    }
}

fn rare() -> AssetFilter {
    AssetFilter {
        tag: Some(("rare".into(), Some("yes".into()))),
        authoring_only: Some(false),
        ..AssetFilter::default()
    }
}

/// Run every selective read the converted call sites issue.
fn selective_reads(reader: &StoreReader) {
    let path = bundle_path(42);
    reader.bundles_at_path(&path).unwrap();
    reader.poisoned_bundles().unwrap();
    reader.generated_bundles().unwrap();
    reader.import_watched_bundles().unwrap();
    reader.bundles_referencing_path(REFERENCED).unwrap();
    reader
        .observed_files_in(PathSelection::Subtree("d07"))
        .unwrap();
    reader
        .observed_files_in(PathSelection::Prefix("d07/b000"))
        .unwrap();
    reader
        .observed_files_in(PathSelection::Name("b00042.bundle"))
        .unwrap();
    reader
        .observed_files_in(PathSelection::Extension("png"))
        .unwrap();
    // Every shape but the whole read, which has no index to search.
    let whole = AssetFilter {
        authoring_only: Some(false),
        ..AssetFilter::default()
    };
    for (filter, _, _) in filter_shapes()
        .into_iter()
        .filter(|(filter, _, _)| *filter != whole)
    {
        let _ = reader.served_assets_matching(&filter, |_| true).unwrap();
        let _ = reader.namespace_assets_matching(&filter, |_| true).unwrap();
    }
    for filter in [
        at_path(&path),
        rare(),
        AssetFilter {
            asset: Some(asset_uuid(42, 1)),
            ..AssetFilter::default()
        },
        AssetFilter {
            bundle: Some(bundle_uuid(42)),
            local_id: Some("main".into()),
            ..AssetFilter::default()
        },
        AssetFilter {
            path_prefixes: vec!["d07/b000".into()],
            authoring_only: Some(false),
            ..AssetFilter::default()
        },
        AssetFilter {
            tag: Some(("rare".into(), None)),
            path_prefixes: vec!["d07/".into()],
            ..AssetFilter::default()
        },
    ] {
        let _ = reader.served_assets_matching(&filter, |_| true).unwrap();
        let _ = reader.namespace_assets_matching(&filter, |_| true).unwrap();
    }
}

static TRACED: Mutex<Vec<String>> = Mutex::new(Vec::new());
/// Held by each test that traces, so their statements do not mix.
static TRACING: Mutex<()> = Mutex::new(());

fn trace(sql: &str) {
    TRACED.lock().unwrap().push(sql.to_owned());
}

fn connection(reader: &mut StoreReader) -> &mut Connection {
    match &mut reader.conn {
        ReaderConn::Owned(conn) => conn,
        ReaderConn::Lent(_) => unreachable!("a store's reader owns its connection"),
    }
}

fn explain(conn: &Connection, sql: &str) -> Vec<String> {
    let mut statement = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    statement
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// The tables a selective read must never scan.
const NAMESPACE_TABLES: [&str; 5] = [
    "assets",
    "bundles",
    "files",
    "asset_tags",
    "bundle_path_refs",
];

/// Partial indexes: walking one visits only the rows it was declared for.
const PARTIAL_INDEXES: [&str; 7] = [
    "bundles_poisoned",
    "bundles_import_watched",
    "assets_authoring",
    "files_by_ext",
    "assets_tag_poisoned",
    "assets_tag_poisoned_by_type",
    "assets_by_terminal_type",
];

/// Whether one `EXPLAIN QUERY PLAN` step walks a whole namespace table or
/// one of its full indexes. Queries alias their tables by one letter, and
/// those count as namespace tables too.
fn scans_namespace(step: &str) -> bool {
    let Some(rest) = step.strip_prefix("SCAN ") else {
        return false;
    };
    let mut words = rest.split_whitespace();
    let table = words.next().unwrap_or_default();
    let words = words.collect::<Vec<_>>();
    let partial = words
        .last()
        .is_some_and(|index| PARTIAL_INDEXES.contains(index));
    (NAMESPACE_TABLES.contains(&table) || table.len() == 1) && !partial
}

#[test]
fn selective_namespace_reads_search_indexes() {
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, store) = store_with(200);
    let mut reader = store.reader().unwrap();
    connection(&mut reader).trace(Some(trace));
    selective_reads(&reader);
    connection(&mut reader).trace(None);
    let statements = std::mem::take(&mut *TRACED.lock().unwrap());
    assert!(statements.len() >= 20, "{statements:?}");
    let conn = connection(&mut reader);
    for sql in statements {
        let plan = explain(conn, &sql);
        println!("{sql}\n  {}\n", plan.join("\n  "));
        assert!(
            !plan.iter().any(|step| scans_namespace(step)),
            "{sql}\nscans a namespace table: {plan:?}"
        );
    }
}

/// One filter per driving shape, each beside the broader selectors it must
/// not be driven by, with the exact plan of its rows statement and, for a
/// tag query, of its tag-poison statement.
fn filter_shapes() -> Vec<(
    AssetFilter,
    &'static [&'static str],
    &'static [&'static str],
)> {
    let name = |index: u32| path_name(&bundle_path(index)).to_owned();
    let kind_mesh = Some(("kind".to_owned(), Some("mesh".to_owned())));
    vec![
        (
            AssetFilter {
                asset: Some(asset_uuid(42, 1)),
                authored_type: Some(RUNTIME_TYPE),
                ..AssetFilter::default()
            },
            &[
                "SEARCH a USING INDEX sqlite_autoindex_assets_1 (asset_uuid=?)",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ],
            &[],
        ),
        (
            AssetFilter {
                bundle: Some(bundle_uuid(42)),
                local_id: Some("main".into()),
                tag: kind_mesh.clone(),
                ..AssetFilter::default()
            },
            &[
                "SEARCH a USING INDEX assets_by_bundle (bundle_uuid=? AND local_id=?)",
                "CORRELATED SCALAR SUBQUERY 1",
                "SEARCH t USING INDEX sqlite_autoindex_asset_tags_1 (asset_uuid=? AND tag=?)",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ],
            &[
                "SEARCH a USING INDEX assets_by_bundle (bundle_uuid=? AND local_id=?)",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ],
        ),
        (
            AssetFilter {
                bundle_path: Some(bundle_path(42)),
                local_id: Some("main".into()),
                ..AssetFilter::default()
            },
            &[
                "SEARCH b USING INDEX bundles_by_path (path=?)",
                "SEARCH a USING INDEX assets_by_bundle (bundle_uuid=? AND local_id=?)",
                "USE TEMP B-TREE FOR ORDER BY",
            ],
            &[],
        ),
        (
            AssetFilter {
                bundle_name: Some(name(42)),
                authored_type: Some(RUNTIME_TYPE),
                path_prefixes: vec!["d42/".into()],
                tag: kind_mesh.clone(),
                ..AssetFilter::default()
            },
            &[
                "SEARCH b USING INDEX bundles_by_name (name=?)",
                "SEARCH a USING INDEX assets_by_bundle (bundle_uuid=?)",
                "CORRELATED SCALAR SUBQUERY 1",
                "SEARCH t USING INDEX sqlite_autoindex_asset_tags_1 (asset_uuid=? AND tag=?)",
                "USE TEMP B-TREE FOR ORDER BY",
            ],
            &[
                "SEARCH b USING INDEX bundles_by_name (name=?)",
                "SEARCH a USING INDEX assets_by_bundle (bundle_uuid=?)",
            ],
        ),
        (
            AssetFilter {
                local_id: Some("$record".into()),
                authored_type: Some(RECORD_TYPE),
                authoring_only: Some(true),
                ..AssetFilter::default()
            },
            &[
                "SEARCH a USING INDEX assets_by_local_id (local_id=?)",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
                "USE TEMP B-TREE FOR ORDER BY",
            ],
            &[],
        ),
        (
            AssetFilter {
                tag: kind_mesh,
                authored_type: Some(RUNTIME_TYPE),
                path_prefixes: vec!["d07/".into()],
                ..AssetFilter::default()
            },
            &[
                "SEARCH a USING INDEX sqlite_autoindex_assets_1 (asset_uuid=?)",
                "LIST SUBQUERY 1",
                "SEARCH t USING INDEX asset_tags_by_tag (tag=? AND value=?)",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ],
            &[
                "SEARCH a USING INDEX assets_tag_poisoned_by_type (type_uuid=?)",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ],
        ),
        (
            AssetFilter {
                authored_type: Some(RUNTIME_TYPE),
                terminal_type: Some(RUNTIME_TYPE),
                path_prefixes: vec!["d07/".into()],
                tag: Some(("kind".into(), None)),
                ..AssetFilter::default()
            },
            &[
                "SEARCH a USING INDEX assets_by_type (type_uuid=?)",
                "CORRELATED SCALAR SUBQUERY 1",
                "SEARCH t USING INDEX sqlite_autoindex_asset_tags_1 (asset_uuid=? AND tag=?)",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
                "USE TEMP B-TREE FOR ORDER BY",
            ],
            &[
                "SEARCH a USING INDEX assets_tag_poisoned_by_type (type_uuid=?)",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ],
        ),
        (
            AssetFilter {
                path_prefixes: vec!["d07/".into(), "d07/b000".into()],
                terminal_type: Some(RUNTIME_TYPE),
                tag: Some(("kind".into(), None)),
                authoring_only: Some(false),
                ..AssetFilter::default()
            },
            &[
                "SEARCH b USING INDEX bundles_by_path (path>? AND path<?)",
                "SEARCH a USING INDEX assets_by_bundle (bundle_uuid=?)",
                "CORRELATED SCALAR SUBQUERY 1",
                "SEARCH t USING INDEX sqlite_autoindex_asset_tags_1 (asset_uuid=? AND tag=?)",
                "USE TEMP B-TREE FOR ORDER BY",
            ],
            &[
                "SCAN a USING INDEX assets_tag_poisoned",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ],
        ),
        (
            AssetFilter {
                authored_type_in: Some(vec![RUNTIME_TYPE, RECORD_TYPE]),
                terminal_type: Some(RUNTIME_TYPE),
                tag: Some(("kind".into(), None)),
                authoring_only: Some(false),
                ..AssetFilter::default()
            },
            &[
                "SEARCH a USING INDEX assets_by_type (type_uuid=?)",
                "CORRELATED SCALAR SUBQUERY 1",
                "SEARCH t USING INDEX sqlite_autoindex_asset_tags_1 (asset_uuid=? AND tag=?)",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
                "USE TEMP B-TREE FOR ORDER BY",
            ],
            &[
                "SEARCH a USING INDEX assets_tag_poisoned_by_type (type_uuid=?)",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ],
        ),
        (
            AssetFilter {
                terminal_type: Some(RUNTIME_TYPE),
                tag: Some(("kind".into(), None)),
                ..AssetFilter::default()
            },
            &[
                "SEARCH a USING INDEX assets_by_terminal_type (terminal_type=?)",
                "CORRELATED SCALAR SUBQUERY 1",
                "SEARCH t USING INDEX sqlite_autoindex_asset_tags_1 (asset_uuid=? AND tag=?)",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
                "USE TEMP B-TREE FOR ORDER BY",
            ],
            &[
                "SCAN a USING INDEX assets_tag_poisoned",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ],
        ),
        (
            AssetFilter {
                tag: Some(("rare".into(), None)),
                authoring_only: Some(false),
                ..AssetFilter::default()
            },
            &[
                "SEARCH a USING INDEX sqlite_autoindex_assets_1 (asset_uuid=?)",
                "LIST SUBQUERY 1",
                "SEARCH t USING INDEX asset_tags_by_tag (tag=?)",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ],
            &[
                "SCAN a USING INDEX assets_tag_poisoned",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ],
        ),
        (
            AssetFilter {
                authoring_only: Some(true),
                ..AssetFilter::default()
            },
            &[
                "SCAN a USING INDEX assets_authoring",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ],
            &[],
        ),
        // No indexed selector: one streamed statement over every asset.
        (
            AssetFilter {
                authoring_only: Some(false),
                ..AssetFilter::default()
            },
            &[
                "SCAN a USING INDEX sqlite_autoindex_assets_1",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ],
            &[],
        ),
    ]
}

/// Each filter's statements are driven by the index of its most selective
/// selector, whatever else it names (SQLite has no statistics to choose
/// by), and are exactly the statements the query runs: the rows, and for a
/// tag query first the poisoned tag-index rows it could select.
#[test]
fn filters_are_driven_by_their_most_selective_index() {
    use crate::bundles::NAMESPACE_ROWS;
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, store) = store_with(200);
    let mut reader = store.reader().unwrap();
    let mut mismatches = Vec::new();
    for (filter, rows_plan, poisons_plan) in filter_shapes() {
        for rows in [NAMESPACE_ROWS, crate::served::SERVED_ENTRY_WHERE] {
            let mut expected = Vec::new();
            if filter.tag.is_some() {
                expected.push(poisons_plan);
            }
            expected.push(rows_plan);
            connection(&mut reader).trace(Some(trace));
            reader
                .assets_matching(&filter, rows, |_| true)
                .unwrap()
                .ok();
            connection(&mut reader).trace(None);
            let statements = std::mem::take(&mut *TRACED.lock().unwrap());
            assert_eq!(
                statements.len(),
                expected.len(),
                "{filter:?}: {statements:#?}"
            );
            for (sql, plan) in statements.iter().zip(expected) {
                // Whether an index covers a step depends on the columns the rows
                // need, not on which index drives the read.
                let actual: Vec<String> = explain(connection(&mut reader), &sql)
                    .into_iter()
                    .map(|step| step.replace("COVERING ", ""))
                    .collect();
                if actual != plan {
                    mismatches.push(format!("{filter:?}\n{sql}\n{actual:#?}"));
                }
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n\n"));
}

/// Pages fetched by `read`, by this reader's page-cache counters.
fn pages(reader: &StoreReader, read: impl FnOnce()) -> u64 {
    let before = reader.pages_fetched().unwrap();
    read();
    reader.pages_fetched().unwrap() - before
}

/// Pages a typed tag query of the records fetches over `count` bundles,
/// with every runtime asset's tag index `pending` or not.
fn typed_tag_query_pages(count: u32, pending: bool) -> u64 {
    let (_dir, mut store) = store_with(count);
    if pending {
        store
            .input_transaction(|txn| {
                for index in (0..count).filter(|index| index % 500 != 199) {
                    txn.set_tag_index_pending(asset_uuid(index, 1))?;
                }
                Ok(())
            })
            .unwrap();
    }
    let reader = store.reader().unwrap();
    let poisoned = reader.tag_poisoned_assets().unwrap().len() as u32;
    assert_eq!(poisoned, if pending { count - count / 500 } else { 0 });
    let records = AssetFilter {
        authored_type: Some(RECORD_TYPE),
        tag: Some(("kind".into(), None)),
        ..AssetFilter::default()
    };
    pages(&reader, || {
        reader
            .namespace_assets_matching(&records, |_| true)
            .unwrap()
            .unwrap();
    })
}

/// A typed query's tag-poison check walks the poisoned rows of its own
/// types (`assets_tag_poisoned_by_type`): thousands of pending rows of
/// another type cost it nothing.
#[test]
fn a_typed_tag_query_checks_only_its_types_poisons() {
    let clean = typed_tag_query_pages(8_000, false);
    let pending = typed_tag_query_pages(8_000, true);
    // The one page is the partial index's root, empty when nothing is
    // pending.
    assert!(pending <= clean + 1, "{clean} {pending}");
}

/// Pages the rules bundles generated bundles name cost to find, over
/// `generated` bundles generated by two rules bundles (one removed), in a
/// namespace of 2 000.
fn generating_rules_pages(generated: u32) -> u64 {
    use crate::bundles::{DirectoryOrigin, DirectoryRuleId};
    let (_dir, mut store) = store_with(2_000);
    store
        .input_transaction(|txn| {
            let root = txn.intern_root("main")?;
            for index in 0..generated {
                txn.upsert_bundle(&BundleMeta {
                    bundle: bundle_uuid(100_000 + index),
                    root,
                    path: format!("gen/{index}.bundle"),
                    format_version: 1,
                    content_hash: ContentHash([0; 32]),
                    origin: Some(DirectoryOrigin {
                        rules_bundle: bundle_uuid(if index % 2 == 0 { 2 } else { 99_999 }),
                        rule: DirectoryRuleId([1; 16]),
                        group_root: "main".to_owned(),
                        group_path: format!("gen/{index}"),
                    }),
                    import_watched: false,
                })?;
            }
            Ok(())
        })
        .unwrap();
    let reader = store.reader().unwrap();
    pages(&reader, || {
        assert_eq!(reader.generating_rules_bundles().unwrap().len(), 2);
    })
}

/// Pages a derived child's and a missing asset's resolution fetch over
/// `count` claiming sources, each authoring one asset with one derived
/// output; every tenth source shares its bundle UUID with the next, so
/// bundle collisions are common.
fn derived_resolution_pages(count: u32) -> u64 {
    use crate::claims::{DerivedOutputClaim, SourceClaim, SourceClaims};
    use crate::state::{AssetClaimant, ReadableBundleSource};
    use distill_core::id::BundleFileHash;
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    let sources = (0..count)
        .map(|index| {
            let path = bundle_path(index);
            let bundle = bundle_uuid(if index % 10 == 0 { index + 1 } else { index });
            let readable = ReadableBundleSource {
                root_name: "main".to_owned(),
                normalized_path: path.clone(),
                file_hash: BundleFileHash([0; 32]),
            };
            let parent = asset_uuid(index, 1);
            SourceClaims {
                root_name: "main".to_owned(),
                path,
                claims: vec![
                    SourceClaim::Bundle {
                        bundle,
                        source: readable.clone(),
                    },
                    SourceClaim::Authored {
                        asset: parent,
                        claimant: AssetClaimant::Authored {
                            source: readable,
                            bundle,
                            local_id: "main".to_owned(),
                        },
                    },
                    SourceClaim::DerivedOutput {
                        child: AssetUuid::v5(parent, "meta"),
                        output: DerivedOutputClaim {
                            parent,
                            output_key: "meta".to_owned(),
                            terminal_type: RUNTIME_TYPE,
                        },
                    },
                ],
            }
        })
        .collect::<Vec<_>>();
    store
        .input_transaction(|txn| txn.replace_source_claims(None, &sources))
        .unwrap();
    let reader = store.reader().unwrap();
    let served = AssetUuid::v5(asset_uuid(42, 1), "meta");
    // Source 41 shares its bundle UUID with source 40: its child is withheld.
    let withheld = AssetUuid::v5(asset_uuid(41, 1), "meta");
    if count <= 1_000 {
        // The withheld set is what point resolution withholds: the assets
        // of the sources sharing a bundle UUID.
        let expected = (0..count)
            .map(|index| asset_uuid(index, 1))
            .filter(|asset| reader.withholding(*asset).unwrap().is_some())
            .collect::<BTreeSet<_>>();
        assert_eq!(expected.len() as u32, count / 10 * 2);
        assert_eq!(reader.withheld_assets().unwrap(), expected);
    }
    pages(&reader, || {
        assert!(reader.derived_output(served).unwrap().is_some());
        assert!(reader.derived_output(withheld).unwrap().is_none());
        assert!(reader
            .asset_resolution(asset_uuid(count + 7, 1))
            .unwrap()
            .is_none());
    })
}

/// A derived child resolves by point reads of its claims and the collisions
/// that would withhold it and its parent, however many sources claim.
#[test]
fn a_derived_child_resolves_by_point_reads() {
    let few = derived_resolution_pages(1_000);
    let many = derived_resolution_pages(20_000);
    // Index searches into `source_claims`, each at most an index level or
    // two deeper (74 and 104 pages when measured); a scan would fetch
    // thousands.
    assert!(many <= few + 36, "{few} {many}");
}

/// A transaction reads each `store_meta` counter once, sees its own
/// writes, and keeps nothing past its end.
#[test]
fn a_transaction_reads_each_counter_once() {
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, mut store) = store_with(0);
    TRACED.lock().unwrap().clear();
    store.read.conn.trace(Some(trace));
    let ((), version) = store
        .input_transaction(|txn| {
            for _ in 0..3 {
                txn.reader().input_version()?;
                txn.reader().compiled_version()?;
            }
            txn.mark_compiled()?;
            assert_eq!(txn.reader().compiled_version()?, Some(txn.version()));
            Ok(())
        })
        .unwrap();
    store.read.conn.trace(None);
    let statements = std::mem::take(&mut *TRACED.lock().unwrap());
    for key in ["input_version", "compiled_version"] {
        let reads = statements
            .iter()
            .filter(|sql| sql.contains(&format!("FROM store_meta WHERE key = '{key}'")))
            .count();
        assert_eq!(reads, 1, "{key}: {statements:#?}");
    }
    // Outside a transaction each read goes to the database.
    assert_eq!(store.input_version().unwrap(), version);
    assert_eq!(store.compiled_version().unwrap(), Some(version));
    let other = store.reader().unwrap();
    store.input_transaction(|_| Ok(())).unwrap();
    assert_eq!(
        other.input_version().unwrap(),
        crate::state::InputVersion(version.0 + 1)
    );
    assert_eq!(
        store.input_version().unwrap(),
        crate::state::InputVersion(version.0 + 1)
    );
}

/// A rewritten bundle's assets are read by bundle, and each vanished one
/// (and its tags) is dropped by key.
#[test]
fn a_rewritten_bundle_drops_its_vanished_assets_by_key() {
    let (_dir, store) = store_with(0);
    for (sql, expected) in [
        (
            "SELECT asset_uuid FROM assets WHERE bundle_uuid = ?1",
            vec!["SEARCH assets USING COVERING INDEX assets_by_bundle (bundle_uuid=?)"],
        ),
        (
            "DELETE FROM asset_tags WHERE asset_uuid = ?1",
            vec!["SEARCH asset_tags USING INDEX sqlite_autoindex_asset_tags_1 (asset_uuid=?)"],
        ),
        (
            "DELETE FROM assets WHERE asset_uuid = ?1",
            vec!["SEARCH assets USING INDEX sqlite_autoindex_assets_1 (asset_uuid=?)"],
        ),
    ] {
        assert_eq!(store.query_plan_details(sql).unwrap(), expected, "{sql}");
    }
}

/// A tool epoch reads each key's last row at its base in one pass over the
/// primary key: no sort, no per-key subquery.
#[test]
fn a_tool_epoch_reads_the_published_tools_in_one_pass() {
    let (_dir, store) = store_with(0);
    assert_eq!(
        store
            .query_plan_details(crate::pipeline::PUBLISHED_TOOLS)
            .unwrap(),
        ["SCAN tools USING INDEX sqlite_autoindex_tools_1"]
    );
}

/// Which rules bundles generated bundles name is found once per edit of a
/// bundle source: one seek per rules bundle, however many bundles each
/// generated.
#[test]
fn the_generating_rules_bundles_cost_one_seek_each() {
    let few = generating_rules_pages(4);
    let many = generating_rules_pages(4_000);
    // One more index level at most.
    assert!(many <= few + 3, "{few} {many}");
}

#[test]
fn narrow_reads_of_a_large_namespace_touch_few_pages() {
    let (_dir, store) = store_with(20_000);
    let reader = store.reader().unwrap();
    let path = bundle_path(12_345);
    let count = |rows: usize, expected: usize| {
        assert_eq!(rows, expected);
        rows as u64
    };
    let mut rows = 0;
    // (read, rows it returns, pages it fetched, pages of the whole-table
    // read it replaced)
    let costs = [
        (
            "bundle at a path",
            pages(&reader, || {
                rows = count(reader.bundles_at_path(&path).unwrap().len(), 1)
            }),
            rows,
            pages(&reader, || drop(reader.all_bundles().unwrap())),
        ),
        (
            "file prefix",
            pages(&reader, || {
                let selected = reader.observed_files_in(PathSelection::Prefix("d07/b000"));
                rows = count(selected.unwrap().len(), 2);
            }),
            rows,
            pages(&reader, || drop(reader.observed_files().unwrap())),
        ),
        (
            "file subtree",
            pages(&reader, || {
                let selected = reader.observed_files_in(PathSelection::Subtree("d07"));
                rows = count(selected.unwrap().len(), 400);
            }),
            rows,
            pages(&reader, || drop(reader.observed_files().unwrap())),
        ),
        (
            "served entries at a path",
            pages(&reader, || {
                rows = count(
                    reader
                        .served_assets_matching(&at_path(&path), |_| true)
                        .unwrap()
                        .unwrap()
                        .len(),
                    1,
                );
            }),
            rows,
            pages(&reader, || drop(reader.served_entries().unwrap())),
        ),
        (
            "rarely tagged entries",
            pages(&reader, || {
                rows = count(
                    reader
                        .namespace_assets_matching(&rare(), |_| true)
                        .unwrap()
                        .unwrap()
                        .len(),
                    20,
                );
            }),
            rows,
            pages(&reader, || {
                drop(reader.all_bundles().unwrap());
                drop(reader.served_entries().unwrap());
            }),
        ),
        (
            "a query reaching a poisoned bundle",
            pages(&reader, || {
                // Bundle 12 199 is poisoned: its skeleton row answers, as a
                // failure naming it.
                let poisoned = reader
                    .served_assets_matching(&at_path(&bundle_path(12_199)), |_| true)
                    .unwrap();
                assert_eq!(poisoned, Err(vec![bundle_uuid(12_199)]));
                rows = 1;
            }),
            rows,
            pages(&reader, || drop(reader.served_entries().unwrap())),
        ),
        (
            "entries at a bundle name, beside a broad type",
            pages(&reader, || {
                let filter = AssetFilter {
                    bundle_name: Some(path_name(&path).to_owned()),
                    authored_type: Some(RUNTIME_TYPE),
                    ..AssetFilter::default()
                };
                rows = count(
                    reader
                        .served_assets_matching(&filter, |_| true)
                        .unwrap()
                        .unwrap()
                        .len(),
                    1,
                );
            }),
            rows,
            pages(&reader, || drop(reader.served_entries().unwrap())),
        ),
        (
            "entries of a local id",
            pages(&reader, || {
                let filter = AssetFilter {
                    local_id: Some("$record".into()),
                    ..AssetFilter::default()
                };
                rows = count(
                    reader
                        .namespace_assets_matching(&filter, |_| true)
                        .unwrap()
                        .unwrap()
                        .len(),
                    20,
                );
            }),
            rows,
            pages(&reader, || drop(reader.all_asset_bundles().unwrap())),
        ),
        (
            "authoring-only entries",
            pages(&reader, || {
                let filter = AssetFilter {
                    authoring_only: Some(true),
                    ..AssetFilter::default()
                };
                rows = count(
                    reader
                        .namespace_assets_matching(&filter, |_| true)
                        .unwrap()
                        .unwrap()
                        .len(),
                    20,
                );
            }),
            rows,
            pages(&reader, || drop(reader.all_asset_bundles().unwrap())),
        ),
        (
            "file name",
            pages(&reader, || {
                let selected = reader.observed_files_in(PathSelection::Name(path_name(&path)));
                rows = count(selected.unwrap().len(), 1);
            }),
            rows,
            pages(&reader, || drop(reader.observed_files().unwrap())),
        ),
        (
            "file extension",
            pages(&reader, || {
                let selected = reader.observed_files_in(PathSelection::Extension("png"));
                rows = count(selected.unwrap().len(), 20);
            }),
            rows,
            pages(&reader, || drop(reader.observed_files().unwrap())),
        ),
        (
            "bundles referencing a path",
            pages(&reader, || {
                rows = count(
                    reader.bundles_referencing_path(REFERENCED).unwrap().len(),
                    200,
                );
            }),
            rows,
            pages(&reader, || drop(reader.all_bundles().unwrap())),
        ),
    ];
    for (read, narrow, rows, full) in costs {
        println!("{read}: {rows} rows in {narrow} pages (whole-table read: {full} pages)");
        assert!(
            narrow <= 16 + 8 * rows,
            "{read} fetched {narrow} pages for {rows} rows"
        );
        assert!(full >= 4 * narrow, "{read}: {narrow} pages against {full}");
    }
}

/// The bulk (bundle, asset) walk a complete publication merges against
/// reads `assets_by_bundle` alone, in index order; local ids lead asset
/// UUIDs in that index, so only each bundle's own rows are sorted, never
/// the whole table.
#[test]
fn the_bundle_asset_walk_sorts_one_bundle_at_a_time() {
    let (_dir, store) = store_with(10);
    let mut reader = store.reader().unwrap();
    let conn = connection(&mut reader);
    assert_eq!(
        explain(
            conn,
            "SELECT bundle_uuid, asset_uuid FROM assets ORDER BY bundle_uuid, asset_uuid"
        ),
        [
            "SCAN assets USING COVERING INDEX assets_by_bundle",
            "USE TEMP B-TREE FOR LAST TERM OF ORDER BY",
        ]
    );
}

/// A directory row and claim rows under every scanned bundle's directory,
/// and a symlink beside every hundredth bundle, for the subtree reads.
fn populate_scan_structure(store: &mut Store, count: u32) {
    let claims = populate_scan_structure_claims(count);
    store
        .input_transaction(|txn| {
            let version = txn.version();
            for index in 0..count {
                let root = txn.intern_root(["main", "alt"][(index % 2) as usize])?;
                txn.upsert_file(
                    root,
                    &format!("{}.d", bundle_path(index)),
                    &FileObservation {
                        canonical_path: Some(format!("/c/{index}").into_bytes()),
                        ..FileObservation::from(FileState {
                            mtime: 0,
                            size: 0,
                            kind: FileKind::Directory,
                            content_hash: None,
                        })
                    },
                    version,
                )?;
            }
            txn.replace_source_claims(None, &claims)?;
            let root = txn.intern_root("main")?;
            for index in (0..count).step_by(100) {
                txn.upsert_file(
                    root,
                    &format!("links/l{index:05}"),
                    &FileObservation {
                        state: FileState {
                            mtime: 0,
                            size: 0,
                            kind: FileKind::Symlink,
                            content_hash: None,
                        },
                        raw_path: Vec::new(),
                        symlink_target: Some(
                            format!("/t/d{:02}/x{index}", index % 50).into_bytes(),
                        ),
                        canonical_path: None,
                    },
                    version,
                )?;
            }
            Ok(())
        })
        .unwrap();
}

/// The claims [`populate_scan_structure`] writes: each bundle's primary path.
fn populate_scan_structure_claims(count: u32) -> Vec<crate::claims::SourceClaims> {
    (0..count)
        .map(|index| {
            let path = bundle_path(index);
            crate::claims::SourceClaims {
                root_name: ["main", "alt"][(index % 2) as usize].to_owned(),
                path: path.clone(),
                claims: vec![crate::claims::SourceClaim::PrimaryPath {
                    path,
                    asset: asset_uuid(index, 1),
                }],
            }
        })
        .collect()
}

/// The statements `run` issues that read a subtree of one root (they join
/// `roots` by name), each with its plan.
fn subtree_plans(store: &mut Store, run: impl FnOnce(&mut Store)) -> Vec<(String, Vec<String>)> {
    store.read.conn.trace(Some(trace));
    run(store);
    store.read.conn.trace(None);
    let statements = std::mem::take(&mut *TRACED.lock().unwrap());
    statements
        .into_iter()
        .filter(|sql| {
            sql.contains("JOIN roots r USING (root_id)") || sql.contains("symlink_target >=")
        })
        .map(|sql| {
            let plan = explain(&store.read.conn, &sql);
            (sql, plan)
        })
        .collect()
}

/// A subtree read (the scan baseline of a watcher batch or an RPC write, the
/// structure and claims it replaces, a pass's file overlay) is one range
/// search of its table's `(root_id, path)` key; a whole-root read is one
/// search of the root's rows. Symlinks targeting a path are one range of
/// their target index.
#[test]
fn subtree_reads_search_one_key_range() {
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, mut store) = store_with(200);
    populate_scan_structure(&mut store, 200);
    let roots = "SEARCH r USING COVERING INDEX sqlite_autoindex_roots_1 (name=?)";
    for (prefix, key) in [
        ("d07", "root_id=? AND path>? AND path<?"),
        ("", "root_id=?"),
    ] {
        let search = |table: &str, covering: bool| {
            let index = if covering { "COVERING INDEX" } else { "INDEX" };
            format!("SEARCH t USING {index} sqlite_autoindex_{table}_1 ({key})")
        };
        let read = |table: &str| vec![roots.to_owned(), search(table, false)];
        let delete = |table: &str| {
            vec![
                format!("SEARCH {table} USING INTEGER PRIMARY KEY (rowid=?)"),
                "LIST SUBQUERY 1".to_owned(),
                roots.to_owned(),
                search(table, true),
            ]
        };
        let reads = subtree_plans(&mut store, |store| {
            store.observed_files_under("main", prefix).unwrap();
            store.bundle_file_hashes_under("main", prefix).unwrap();
        });
        let plans = reads
            .iter()
            .map(|(_, plan)| plan.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            plans,
            ["files", "files"].map(read),
            "{prefix:?}: {reads:#?}"
        );
        let exists = subtree_plans(&mut store, |store| {
            store.observes_under("main", prefix).unwrap();
        });
        assert_eq!(exists.len(), 1);
        assert_eq!(
            exists[0].1,
            [
                "SCAN CONSTANT ROW".to_owned(),
                "SCALAR SUBQUERY 1".to_owned(),
                roots.to_owned(),
                search("files", true),
            ],
            "{prefix:?}: {exists:#?}"
        );
        let under = [("main".to_owned(), prefix.to_owned())];
        let writes = subtree_plans(&mut store, |store| {
            store
                .input_transaction(|txn| txn.replace_source_claims(Some(&under), &[]))
                .unwrap();
        });
        let plans = writes
            .iter()
            .map(|(_, plan)| plan.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            plans,
            [
                vec![roots.to_owned(), search("source_claims", true)],
                delete("source_claims"),
            ],
            "{prefix:?}: {writes:#?}"
        );
    }
    let links = subtree_plans(&mut store, |store| {
        let rows = store.symlinks_targeting(b"/t/d07").unwrap();
        assert_eq!(rows.len(), 0);
        let rows = store.symlinks_targeting(b"/t/d00").unwrap();
        assert_eq!(rows.len(), 2);
    });
    for (sql, plan) in links {
        assert_eq!(
            plan,
            [
                "SEARCH t USING INDEX files_by_symlink_target (symlink_target>? AND symlink_target<?)",
                "SEARCH r USING INTEGER PRIMARY KEY (rowid=?)",
            ],
            "{sql}"
        );
    }
    // The incremental scan's alias check: one probe of the unique index.
    let canonical = subtree_plans(&mut store, |store| {
        assert!(store.directory_by_canonical(b"/c/7").unwrap().is_some());
    });
    assert_eq!(canonical.len(), 1);
    assert_eq!(
        canonical[0].1,
        [
            "SEARCH t USING INDEX files_by_canonical (canonical_path=?)",
            "SEARCH r USING INTEGER PRIMARY KEY (rowid=?)",
        ],
        "{canonical:#?}"
    );
}

/// Reading one subtree of a large root fetches pages for its rows, not the
/// root's.
#[test]
fn a_subtree_read_of_a_large_root_touches_its_rows() {
    let (_dir, mut store) = store_with(20_000);
    populate_scan_structure(&mut store, 20_000);
    let reader = store.reader().unwrap();
    // "d08" holds every 50th bundle, all even, so all "main"'s: 400 of its
    // 10,000, each with a directory row beside it (whose table rows were
    // written apart from the bundles', so no table page holds two).
    let mut rows = 0;
    let narrow = pages(&reader, || {
        rows = reader.observed_files_under("main", "d08").unwrap().len();
        rows += reader
            .bundle_file_hashes_under("main", "d08")
            .unwrap()
            .len();
    });
    let whole = pages(&reader, || {
        drop(reader.observed_files_under("main", "").unwrap());
        drop(reader.bundle_file_hashes_under("main", "").unwrap());
    });
    println!("subtree: {rows} rows in {narrow} pages (whole root: {whole} pages)");
    assert_eq!(rows, 3 * 400);
    assert!(
        narrow <= 32 + 2 * rows as u64,
        "{narrow} pages for {rows} rows"
    );
    assert!(whole >= 10 * narrow, "{narrow} pages against {whole}");
}

/// A complete scan's old bundle summaries are one streamed join of the
/// bundle rows with their roots, never a root-name lookup per bundle.
#[test]
fn bundles_with_root_names_are_one_join() {
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, mut store) = store_with(200);
    let plans = subtree_plans(&mut store, |store| {
        let mut count = 0;
        store
            .for_each_bundle_with_root_name(|root_name, _| {
                assert!(root_name == "main" || root_name == "alt");
                count += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(count, 200);
    });
    assert_eq!(plans.len(), 1, "{plans:#?}");
    assert_eq!(
        plans[0].1,
        [
            "SCAN bundles USING INDEX sqlite_autoindex_bundles_1",
            "SEARCH r USING INTEGER PRIMARY KEY (rowid=?)",
        ],
        "{plans:#?}"
    );
}

/// A refinement finds the tag rows it must redo by one search of an index
/// per kind of staleness (poisoned, among them pending; another module's
/// migration), never by walking every row; and finds exactly those.
#[test]
fn stale_tag_rows_are_index_searches() {
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, mut store) = store_with(200);
    let (module, old_module) = ([9; 32], [8; 32]);
    let updates = (0..200)
        .map(|index| {
            let (dylib_hash, poison) = match index % 50 {
                0 => (None, Some("tag indexing pending".to_owned())),
                2 => (Some(old_module), None),
                3 => (Some(module), None),
                _ => (None, None),
            };
            TagIndexUpdate {
                asset: asset_uuid(index, 1),
                tags: BTreeMap::new(),
                dylib_hash,
                poison,
            }
        })
        .collect::<Vec<_>>();
    store.refine_unpublished_tag_index(&updates).unwrap();
    let stale = |module| {
        store
            .stale_tag_index_assets(module)
            .unwrap()
            .into_iter()
            .map(|(asset, bundle)| {
                let index = (0..200)
                    .find(|index| asset_uuid(*index, 1) == asset)
                    .unwrap();
                assert_eq!(bundle, bundle_uuid(index));
                index % 50
            })
            .collect::<Vec<_>>()
    };
    let mut kinds = stale(Some(module));
    kinds.sort_unstable();
    assert_eq!(kinds, [0, 0, 0, 0, 2, 2, 2, 2]);
    let mut kinds = stale(None);
    kinds.sort_unstable();
    assert_eq!(kinds, [0, 0, 0, 0, 2, 2, 2, 2, 3, 3, 3, 3]);

    store.read.conn.trace(Some(trace));
    store.stale_tag_index_assets(Some(module)).unwrap();
    store.read.conn.trace(None);
    let statements = std::mem::take(&mut *TRACED.lock().unwrap());
    let sql = statements
        .iter()
        .find(|sql| sql.contains("INDEXED BY assets_tag_poisoned"))
        .unwrap();
    let plan = explain(&store.read.conn, sql);
    assert_eq!(
        plan,
        [
            "COMPOUND QUERY",
            "LEFT-MOST SUBQUERY",
            "SCAN assets USING INDEX assets_tag_poisoned",
            "UNION USING TEMP B-TREE",
            "SEARCH assets USING INDEX assets_tag_migrated (tag_module<?)",
            "UNION USING TEMP B-TREE",
            "SEARCH assets USING INDEX assets_tag_migrated (tag_module>?)",
        ],
        "{sql}"
    );
}

/// The statements a configuration change issues, with their plans.
fn configuration_plans(
    store: &mut Store,
    run: impl FnOnce(&mut Store),
) -> Vec<(String, Vec<String>)> {
    store.read.conn.trace(Some(trace));
    run(store);
    store.read.conn.trace(None);
    let statements = std::mem::take(&mut *TRACED.lock().unwrap());
    statements
        .into_iter()
        .filter(|sql| !sql.starts_with("SAVEPOINT") && !sql.starts_with("RELEASE"))
        .map(|sql| {
            let plan = explain(&store.read.conn, &sql);
            (sql, plan)
        })
        .filter(|(_, plan)| !plan.is_empty())
        .collect()
}

/// The import index is one table keyed by bundle: a source's rows are
/// cleared through its bundle, a watched import is found by the key its
/// basis reads, a rules source by its listing directory or its bundle's
/// source, and the rules bundles generated bundles name are one seek of
/// `bundles_by_origin` each.
#[test]
fn import_index_statements_search_their_keys() {
    use crate::bundles::{DirectoryOrigin, DirectoryRuleId};
    use crate::imports::{ImportIndexSource, ImportReadKey, WatchedImport};
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, mut store) = store_with(20);
    // Two bundles generated by rules bundle 2 and one by a removed one.
    store
        .input_transaction(|txn| {
            let root = txn.intern_root("main")?;
            for (index, rules) in [
                (100, bundle_uuid(2)),
                (101, bundle_uuid(2)),
                (102, bundle_uuid(99)),
            ] {
                txn.upsert_bundle(&BundleMeta {
                    bundle: bundle_uuid(index),
                    root,
                    path: format!("gen/{index}.bundle"),
                    format_version: 1,
                    content_hash: ContentHash([0; 32]),
                    origin: Some(DirectoryOrigin {
                        rules_bundle: rules,
                        rule: DirectoryRuleId([1; 16]),
                        group_root: "main".to_owned(),
                        group_path: format!("gen/{index}"),
                    }),
                    import_watched: false,
                })?;
            }
            Ok(())
        })
        .unwrap();
    let row = |index: u32| ImportIndexSource {
        root_name: "main".to_owned(),
        path: bundle_path(index),
        bundle: bundle_uuid(index),
        watched: Some(WatchedImport {
            record: asset_uuid(index, 2),
            reads: vec![
                ImportReadKey::Path("tex/a.png".to_owned()),
                ImportReadKey::Listing,
            ],
        }),
        directory_rules: vec![(asset_uuid(index, 3), "d02/".to_owned())],
    };
    let plans = configuration_plans(&mut store, |store| {
        store
            .replace_import_index(&[("main".to_owned(), bundle_path(2))], &[row(2)])
            .unwrap();
        // A row of a bundle at a source not cleared.
        store.replace_import_index(&[], &[row(4)]).unwrap();
        assert_eq!(
            store.watched_imports().unwrap(),
            [bundle_uuid(2), bundle_uuid(4)]
        );
        assert_eq!(
            store.watched_imports_reading(["tex/a.png"], true).unwrap(),
            [bundle_uuid(2), bundle_uuid(4)]
        );
        assert_eq!(store.directory_rule_sources().unwrap().len(), 2);
        assert_eq!(rules_at(store, &bundle_path(2)).len(), 1);
        let generating = store.generating_rules_bundles().unwrap();
        assert_eq!(
            generating,
            [
                (
                    bundle_uuid(2),
                    Some((crate::files::RootId(1), bundle_path(2)))
                ),
                (bundle_uuid(99), None),
            ]
        );
    });
    let plans = plans
        .into_iter()
        .filter(|(sql, _)| !sql.contains("store_meta"))
        .map(|(sql, plan)| (sql.split_whitespace().collect::<Vec<_>>().join(" "), plan))
        .collect::<BTreeMap<_, _>>();
    let by_root = "SEARCH r USING INTEGER PRIMARY KEY (rowid=?)";
    let bundle_key = "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)";
    let expected: BTreeMap<String, Vec<String>> = [
        (
            "DELETE FROM import_keys WHERE bundle_uuid = x'10101010101010101010101000000004'",
            vec!["SEARCH import_keys USING PRIMARY KEY (bundle_uuid=?)"],
        ),
        (
            "DELETE FROM import_keys WHERE bundle_uuid IN ( SELECT b.bundle_uuid FROM bundles b JOIN roots r USING (root_id) WHERE r.name = 'main' AND b.path = 'd02/b00002.bundle')",
            vec![
                "SEARCH import_keys USING PRIMARY KEY (bundle_uuid=?)",
                "LIST SUBQUERY 1",
                "SEARCH r USING COVERING INDEX sqlite_autoindex_roots_1 (name=?)",
                "SEARCH b USING INDEX bundles_by_path (path=? AND root_id=?)",
            ],
        ),
        (
            "SELECT DISTINCT bundle_uuid FROM import_keys WHERE kind < 3 ORDER BY bundle_uuid",
            vec!["SCAN import_keys"],
        ),
        (
            "SELECT bundle_uuid FROM import_keys WHERE kind = 0 AND key = 'tex/a.png'",
            vec!["SEARCH import_keys USING COVERING INDEX import_keys_by_key (kind=? AND key=?)"],
        ),
        (
            "SELECT bundle_uuid FROM import_keys WHERE kind = 1 OR (kind = 2 AND 1)",
            vec![
                "MULTI-INDEX OR",
                "INDEX 1",
                "SEARCH import_keys USING COVERING INDEX import_keys_by_key (kind=?)",
                "INDEX 2",
                "SEARCH import_keys USING COVERING INDEX import_keys_by_key (kind=?)",
            ],
        ),
        (
            "SELECT r.name, b.path, k.bundle_uuid, k.asset_uuid FROM import_keys k JOIN bundles b USING (bundle_uuid) JOIN roots r ON r.root_id = b.root_id WHERE k.kind = 3 ORDER BY r.name, b.path, k.bundle_uuid, k.asset_uuid",
            vec![
                "SEARCH k USING COVERING INDEX import_keys_by_key (kind=?)",
                bundle_key,
                by_root,
                "USE TEMP B-TREE FOR ORDER BY",
            ],
        ),
    ]
    .into_iter()
    .chain(
        // From the start, then past each rules bundle found.
        ["zeroblob(0)", "x'10101010101010101010101000000002'", "x'10101010101010101010101000000063'"].map(|after| {
            (
                Box::leak(format!("SELECT g.origin_rules_bundle, r.root_id, r.path FROM bundles g INDEXED BY bundles_by_origin LEFT JOIN bundles r ON r.bundle_uuid = g.origin_rules_bundle WHERE g.origin_rules_bundle IS NOT NULL AND g.origin_rules_bundle > {after} ORDER BY g.origin_rules_bundle LIMIT 1").into_boxed_str()) as &str,
                vec![
                    "SEARCH g USING COVERING INDEX bundles_by_origin (origin_rules_bundle>?)",
                    "SEARCH r USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?) LEFT-JOIN",
                ],
            )
        }),
    )
    .map(|(sql, plan)| (sql.to_owned(), plan.into_iter().map(str::to_owned).collect()))
    .collect();
    assert_eq!(plans, expected, "{plans:#?}");
}

/// An asset's tag state is two columns of its row: marking it pending,
/// refining it and reading it are searches of the asset's primary key, and
/// the poisoned rows are a walk of their partial index.
#[test]
fn tag_state_statements_search_the_asset_key() {
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, mut store) = store_with(20);
    let asset = asset_uuid(3, 1);
    let plans = configuration_plans(&mut store, |store| {
        store
            .input_transaction(|txn| txn.set_tag_index_pending(asset))
            .unwrap();
        store
            .refine_unpublished_tag_index(&[TagIndexUpdate {
                asset,
                tags: BTreeMap::new(),
                dylib_hash: Some([9; 32]),
                poison: None,
            }])
            .unwrap();
        store.tag_index_state(asset).unwrap();
        store.tag_poisoned_assets().unwrap();
    });
    let by_key = "SEARCH assets USING INDEX sqlite_autoindex_assets_1 (asset_uuid=?)";
    let plans = plans
        .iter()
        .filter(|(sql, _)| !sql.contains("store_meta") && !sql.contains("asset_tags"))
        .map(|(sql, plan)| {
            (
                sql.split_whitespace().take(4).collect::<Vec<_>>().join(" "),
                plan.clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        plans,
        [
            (
                "UPDATE assets SET tag_poison".to_owned(),
                vec![by_key.to_owned()]
            ),
            (
                "UPDATE assets SET tag_poison".to_owned(),
                vec![by_key.to_owned()]
            ),
            (
                "SELECT tag_module, tag_poison FROM".to_owned(),
                vec![by_key.to_owned()]
            ),
            (
                "SELECT asset_uuid, bundle_uuid FROM".to_owned(),
                vec!["SCAN assets USING INDEX assets_tag_poisoned".to_owned()],
            ),
        ],
        "{plans:#?}"
    );
}

/// A bundle's primary asset is a column of its row: setting it searches
/// the bundle and the asset by key, a refusal reads the asset's role, and a
/// path's primaries are a search of `bundles_by_path` (the walk of every
/// primary, for a full publication, is that index in order).
#[test]
fn primary_asset_statements_search_their_keys() {
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, mut store) = store_with(20);
    let plans = configuration_plans(&mut store, |store| {
        store
            .input_transaction(|txn| txn.set_primary_asset(bundle_uuid(3), asset_uuid(3, 1)))
            .unwrap();
        store
            .input_transaction(|txn| txn.set_primary_asset(bundle_uuid(3), asset_uuid(3, 2)))
            .unwrap_err();
        assert_eq!(
            store.path_assets(&bundle_path(3)).unwrap(),
            BTreeSet::from([asset_uuid(3, 1)])
        );
        assert_eq!(store.all_path_entries().unwrap().len(), 20);
    });
    let by_key = "SEARCH assets USING INDEX sqlite_autoindex_assets_1 (asset_uuid=?)";
    let bundle_key = "SEARCH bundles USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)";
    let plans = plans
        .iter()
        .filter(|(sql, _)| !sql.contains("store_meta"))
        .map(|(sql, plan)| {
            (
                sql.split_whitespace().take(4).collect::<Vec<_>>().join(" "),
                plan.clone(),
            )
        })
        .collect::<Vec<_>>();
    let set = (
        "UPDATE bundles SET primary_asset".to_owned(),
        vec![
            bundle_key.to_owned(),
            "SCALAR SUBQUERY 1".to_owned(),
            by_key.to_owned(),
        ],
    );
    assert_eq!(
        plans,
        [
            set.clone(),
            set,
            (
                "SELECT authoring_only FROM assets".to_owned(),
                vec![by_key.to_owned()]
            ),
            (
                "SELECT primary_asset FROM bundles".to_owned(),
                vec!["SEARCH bundles USING INDEX bundles_by_path (path=?)".to_owned()],
            ),
            (
                "SELECT path, root_id, primary_asset".to_owned(),
                vec!["SCAN bundles USING INDEX bundles_by_path".to_owned()],
            ),
        ],
        "{plans:#?}"
    );
}

/// A type whose tag epoch changes has exactly its rows marked pending, by
/// one search of `assets_by_type`; an unchanged epoch marks nothing; the
/// epochs are one read of the per-type table.
#[test]
fn a_changed_tag_epoch_marks_only_its_types_rows() {
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, mut store) = store_with(200);
    let refined = |store: &mut Store| {
        let updates = [asset_uuid(3, 1), asset_uuid(3, 2), asset_uuid(4, 1)]
            .into_iter()
            .map(|asset| TagIndexUpdate {
                asset,
                tags: BTreeMap::new(),
                dylib_hash: None,
                poison: None,
            })
            .collect::<Vec<_>>();
        store.refine_unpublished_tag_index(&updates).unwrap();
    };
    let epochs =
        |record: u8| BTreeMap::from([(RUNTIME_TYPE, [1; 32]), (RECORD_TYPE, [record; 32])]);
    let pending = |store: &Store| {
        [asset_uuid(3, 1), asset_uuid(3, 2), asset_uuid(4, 1)]
            .into_iter()
            .filter(|asset| {
                store
                    .stale_tag_index_assets(None)
                    .unwrap()
                    .contains_key(asset)
            })
            .collect::<Vec<_>>()
    };
    let mark = |store: &mut Store, epochs: &BTreeMap<TypeUuid, [u8; 32]>| {
        store.replace_tag_epochs(epochs).unwrap()
    };
    refined(&mut store);
    assert!(pending(&store).is_empty());
    // Every type is new to an empty table.
    assert_eq!(
        mark(&mut store, &epochs(2)),
        std::collections::BTreeSet::from([RUNTIME_TYPE, RECORD_TYPE])
    );
    assert_eq!(pending(&store).len(), 3);
    refined(&mut store);
    assert!(mark(&mut store, &epochs(2)).is_empty());
    assert!(pending(&store).is_empty());

    let plans = configuration_plans(&mut store, |store| {
        assert_eq!(
            store.replace_tag_epochs(&epochs(3)).unwrap(),
            std::collections::BTreeSet::from([RECORD_TYPE])
        );
    });
    assert_eq!(pending(&store), [asset_uuid(3, 2)]);
    let plans = plans
        .iter()
        .map(|(_, plan)| plan.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        plans,
        [
            vec!["SCAN tag_epochs".to_owned()],
            vec!["SEARCH assets USING INDEX assets_by_type (type_uuid=?)".to_owned()],
            vec!["SEARCH tag_epochs USING PRIMARY KEY (type_uuid=?)".to_owned()],
        ],
        "{plans:#?}"
    );
}

/// A configuration change finds the sources it claims again by searches:
/// the bundles of a type (or only its poisoned ones) through
/// `assets_by_type`, and the malformed and colliding sources through
/// `source_claims_by_subject`: the colliding ones by a walk of its bundle
/// and asset claims, the index alone (it covers the claimant).
#[test]
fn reconfigured_sources_are_index_searches() {
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, mut store) = store_with(1000);
    let plans = configuration_plans(&mut store, |store| {
        let records = store.bundle_sources_of_type(RECORD_TYPE, false).unwrap();
        assert_eq!(records.len(), 1);
        assert!(records.iter().all(|(_, path)| *path == bundle_path(3)));
        let poisoned = store.bundle_sources_of_type(RUNTIME_TYPE, true).unwrap();
        assert_eq!(
            poisoned
                .into_iter()
                .map(|(_, path)| path)
                .collect::<Vec<_>>(),
            [bundle_path(199), bundle_path(699)]
        );
        store.unpublished_claim_sources().unwrap();
    });
    let plans = plans
        .iter()
        .map(|(_, plan)| plan.clone())
        .collect::<Vec<_>>();
    let source_type = [
        "SEARCH a USING INDEX assets_by_type (type_uuid=?)",
        "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
        "SEARCH r USING INTEGER PRIMARY KEY (rowid=?)",
    ];
    assert_eq!(
        plans,
        [
            source_type.to_vec(),
            source_type.to_vec(),
            vec![
                "COMPOUND QUERY",
                "LEFT-MOST SUBQUERY",
                "SEARCH t USING INDEX source_claims_by_subject (kind=?)",
                "SEARCH r USING INTEGER PRIMARY KEY (rowid=?)",
                "UNION USING TEMP B-TREE",
                // The collisions: a walk of the bundle and asset claims.
                "CO-ROUTINE c",
                "SEARCH source_claims USING COVERING INDEX source_claims_by_subject (kind=?)",
                "USE TEMP B-TREE FOR GROUP BY",
                "USE TEMP B-TREE FOR count(DISTINCT)",
                "SCAN c",
                "SEARCH t USING INDEX source_claims_by_subject (kind=? AND subject=?)",
                "SEARCH r USING INTEGER PRIMARY KEY (rowid=?)",
            ],
        ],
        "{plans:#?}"
    );
}

/// Reads of rare rows walk their partial index, whatever order they answer
/// in: the generated bundles (`bundles_by_origin`), the poisoned ones
/// (`bundles_poisoned`), the watched imports' (`bundles_import_watched`).
#[test]
fn rare_row_reads_walk_their_partial_index() {
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, store) = store_with(200);
    let mut reader = store.reader().unwrap();
    let cases: [(&dyn Fn(&StoreReader), &[&str]); 3] = [
        (
            &|reader| drop(reader.generated_bundles().unwrap()),
            &[
                "SEARCH bundles USING INDEX bundles_by_origin (origin_rules_bundle>?)",
                "USE TEMP B-TREE FOR ORDER BY",
            ],
        ),
        (
            &|reader| drop(reader.poisoned_bundles().unwrap()),
            &["SCAN bundles USING INDEX bundles_poisoned"],
        ),
        (
            &|reader| drop(reader.import_watched_bundles().unwrap()),
            &["SCAN bundles USING INDEX bundles_import_watched"],
        ),
    ];
    for (read, expected) in cases {
        connection(&mut reader).trace(Some(trace));
        read(&reader);
        connection(&mut reader).trace(None);
        let statements = std::mem::take(&mut *TRACED.lock().unwrap());
        assert_eq!(statements.len(), 1, "{statements:?}");
        assert_eq!(explain(connection(&mut reader), &statements[0]), expected);
    }
}

/// Doctor verification names every served runtime entry by one statement
/// of three columns, whatever the namespace size: no per-entry tag, schema
/// or value read.
#[test]
fn runtime_entry_types_are_one_statement() {
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut answered = Vec::new();
    for count in [50, 500] {
        let (_dir, store) = store_with(count);
        let mut reader = store.reader().unwrap();
        connection(&mut reader).trace(Some(trace));
        answered.push(reader.served_runtime_entry_types().unwrap().len());
        connection(&mut reader).trace(None);
        let statements = std::mem::take(&mut *TRACED.lock().unwrap());
        assert_eq!(statements.len(), 1, "{statements:?}");
        assert_eq!(
            explain(connection(&mut reader), &statements[0]),
            [
                "SCAN a USING INDEX sqlite_autoindex_assets_1",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ]
        );
    }
    assert!(
        answered[1] > 9 * answered[0] && answered[0] > 0,
        "{answered:?}"
    );
}

/// A candidate bucket's rows are one search of its primary key, and a
/// candidate with what it names one search of each table's.
#[test]
fn candidate_rows_search_their_bucket() {
    let (_dir, store) = store_with(10);
    assert_eq!(
        store
            .query_plan_details(crate::cas::store::CANDIDATE_ROWS)
            .unwrap(),
        [
            "SEARCH results USING INDEX sqlite_autoindex_results_1 (key_kind=? AND static_key=?)",
            "USE TEMP B-TREE FOR ORDER BY",
        ]
    );
    assert_eq!(
        store.query_plan_details(crate::cas::store::CANDIDATE).unwrap(),
        [
            "SEARCH r USING INDEX sqlite_autoindex_results_1 (key_kind=? AND static_key=? AND trace_digest=?)",
            "SEARCH o USING PRIMARY KEY (key_kind=? AND static_key=? AND trace_digest=?) LEFT-JOIN",
        ]
    );
}

/// The statements a pass's bookkeeping issues per edit, each with its exact
/// plan: the import index refresh (by source, its dropped rules returned by
/// the delete), the directory rules a dirty path's directories select, the
/// pending work and its acknowledgement by path, the sources an asset's
/// collision change makes pending (by claimant), the namespace error
/// family's diff, the pipeline failure and the configuration status,
/// and a root id.
#[test]
fn pass_bookkeeping_statements_search_their_indexes() {
    use crate::imports::ImportIndexSource;
    use crate::state::{AssetClaimant, ReadableBundleSource};
    use distill_core::id::BundleFileHash;
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, mut store) = store_with(200);
    populate_scan_structure(&mut store, 200);
    let rules = |index: u32| ImportIndexSource {
        root_name: "main".to_owned(),
        path: bundle_path(index),
        bundle: bundle_uuid(index),
        watched: None,
        directory_rules: vec![(asset_uuid(index, 3), format!("d{:02}/", index % 50))],
    };
    let sources = (0..200)
        .step_by(2)
        .map(|index| ("main".to_owned(), bundle_path(index)))
        .collect::<Vec<_>>();
    let rows = (0..200).step_by(2).map(rules).collect::<Vec<_>>();
    store.replace_import_index(&sources, &rows).unwrap();
    store
        .input_transaction(|txn| {
            let root = txn.intern_root("main")?;
            let version = txn.version();
            txn.push_dirty(root, "main", "gone.png", false, version)
        })
        .unwrap();
    // Two sources claim one asset: a collision the edit below resolves.
    let authored = |path: &str| crate::claims::SourceClaims {
        root_name: "main".to_owned(),
        path: path.to_owned(),
        claims: vec![
            crate::claims::SourceClaim::Bundle {
                bundle: bundle_uuid(42),
                source: ReadableBundleSource {
                    root_name: "main".to_owned(),
                    normalized_path: path.to_owned(),
                    file_hash: BundleFileHash([1; 32]),
                },
            },
            crate::claims::SourceClaim::Authored {
                asset: asset_uuid(42, 1),
                claimant: AssetClaimant::Authored {
                    source: ReadableBundleSource {
                        root_name: "main".to_owned(),
                        normalized_path: path.to_owned(),
                        file_hash: BundleFileHash([1; 32]),
                    },
                    bundle: bundle_uuid(42),
                    local_id: "main".to_owned(),
                },
            },
        ],
    };
    let under = [("main".to_owned(), "x/b.bundle".to_owned())];
    store
        .input_transaction(|txn| {
            txn.replace_source_claims(
                Some(&[("main".to_owned(), "x/a.bundle".to_owned())]),
                &[authored("x/a.bundle")],
            )?;
            txn.replace_source_claims(Some(&under), &[authored("x/b.bundle")])
        })
        .unwrap();
    let plans = configuration_plans(&mut store, |store| {
        store.path_claims(&bundle_path(4)).unwrap();
        let work = store.pending_file_work().unwrap();
        store.acknowledge_file_work(&work).unwrap();
        store
            .replace_import_index(&sources[..1], &rows[..1])
            .unwrap();
        store.directory_rule_sources_listing(["", "d04/"]).unwrap();
        // The collision's diagnostics and what it withholds.
        store.namespace_errors().unwrap();
        assert!(store.withholding(asset_uuid(42, 1)).unwrap().is_some());
        store
            .input_transaction(|txn| {
                txn.intern_root("main")?;
                txn.replace_source_claims(Some(&under), &[]).map(drop)
            })
            .unwrap();
        // Resolved: nothing withholds the asset.
        assert!(store.withholding(asset_uuid(42, 1)).unwrap().is_none());
        // A full replacement that drops one source's claims.
        let mut kept = populate_scan_structure_claims(200);
        kept.pop();
        store
            .input_transaction(|txn| txn.replace_source_claims(None, &kept))
            .unwrap();
    });
    let plan = |prefix: &str| {
        let found = plans
            .iter()
            .filter(|(sql, _)| {
                sql.split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .starts_with(prefix)
            })
            .map(|(_, plan)| plan.clone())
            .collect::<Vec<_>>();
        assert!(
            !found.is_empty(),
            "no statement starts with {prefix:?}: {plans:#?}"
        );
        found
    };
    let by_root = "SEARCH r USING INTEGER PRIMARY KEY (rowid=?)";
    let by_subject =
        ["SEARCH source_claims USING COVERING INDEX source_claims_by_subject (kind=? AND subject=?)"];
    let distinct = ["SEARCH source_claims USING COVERING INDEX source_claims_by_subject (kind=? AND subject=?)"];
    let cases: [(&str, &[&str]); 16] = [
        // A full replacement streams the claims (a whole-namespace pass)
        // and deletes the stale ones by key.
        ("SELECT root_id, path, kind, subject, claimant, detail FROM source_claims", &["SCAN source_claims"]),
        (
            "DELETE FROM source_claims WHERE root_id",
            &["SEARCH source_claims USING INDEX sqlite_autoindex_source_claims_1 (root_id=? AND path=? AND kind=? AND subject=? AND claimant=?)"],
        ),
        ("SELECT DISTINCT claimant FROM source_claims WHERE kind = 3", &distinct),
        // A subject's claimants, whatever index orders claimants.
        ("SELECT DISTINCT claimant FROM source_claims WHERE kind IN (0)", &distinct),
        (
            "SELECT DISTINCT claimant FROM source_claims WHERE kind IN (1, 2)",
            &[
                "SEARCH source_claims USING COVERING INDEX source_claims_by_subject (kind=? AND subject=?)",
                "USE TEMP B-TREE FOR DISTINCT",
            ],
        ),
        // Whether an asset subject collides, before and after a replacement.
        (
            "SELECT COUNT(DISTINCT claimant) > 1 FROM source_claims WHERE kind IN (1, 2)",
            &[
                "USE TEMP B-TREE FOR count(DISTINCT)",
                "SEARCH source_claims USING COVERING INDEX source_claims_by_subject (kind=? AND subject=?)",
            ],
        ),
        // The pending work: a pass's whole queue.
        ("SELECT w.seq", &["SCAN w", by_root]),
        // Acknowledging it: the captured range, and what was queued since.
        (
            "DELETE FROM file_work WHERE seq <=",
            &["SEARCH file_work USING INTEGER PRIMARY KEY (rowid<?)"],
        ),
        (
            "SELECT root_id, path FROM file_work WHERE seq >",
            &["SEARCH file_work USING INTEGER PRIMARY KEY (rowid>?)"],
        ),
        (
            "SELECT root_id FROM roots",
            &["SEARCH roots USING COVERING INDEX sqlite_autoindex_roots_1 (name=?)"],
        ),
        (
            "DELETE FROM import_keys WHERE bundle_uuid IN",
            &[
                "SEARCH import_keys USING PRIMARY KEY (bundle_uuid=?)",
                "LIST SUBQUERY 1",
                "SEARCH r USING COVERING INDEX sqlite_autoindex_roots_1 (name=?)",
                "SEARCH b USING INDEX bundles_by_path (path=? AND root_id=?)",
            ],
        ),
        (
            "SELECT r.name, b.path, k.bundle_uuid, k.asset_uuid FROM import_keys k JOIN bundles b USING (bundle_uuid) JOIN roots r ON r.root_id = b.root_id WHERE k.kind = 3 AND k.key",
            &[
                "SEARCH k USING COVERING INDEX import_keys_by_key (kind=? AND key=?)",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
                by_root,
            ],
        ),
        ("SELECT claimant FROM source_claims WHERE kind = 1", &by_subject),
        ("SELECT claimant FROM source_claims WHERE kind = 5", &["SEARCH source_claims USING COVERING INDEX source_claims_by_subject (kind=?)"]),
        (
            "SELECT subject FROM source_claims WHERE kind = ",
            &["SEARCH source_claims USING COVERING INDEX source_claims_by_claimant (claimant=? AND kind=?)"],
        ),
        // The namespace's collisions: a walk of the claim index's
        // bundle and asset ranges (the listing is the whole namespace's).
        (
            "SELECT CASE kind WHEN 0 THEN 0 ELSE 1 END AS g",
            &[
                "SEARCH source_claims USING COVERING INDEX source_claims_by_subject (kind=?)",
                "USE TEMP B-TREE FOR GROUP BY",
                "USE TEMP B-TREE FOR count(DISTINCT)",
            ],
        ),
    ];
    for (prefix, expected) in cases {
        let found = plan(prefix);
        assert!(!found.is_empty(), "{prefix} was not run");
        for found in found {
            assert_eq!(found, expected, "{prefix}");
        }
    }
    assert_eq!(
        plan("SELECT value FROM store_meta")[0],
        ["SEARCH store_meta USING INDEX sqlite_autoindex_store_meta_1 (key=?)"]
    );
}

/// The CAS's statements, each with its exact plan: every one searches an
/// index for the rows it answers, except the whole-index reads named here.
#[test]
fn cas_statements_search_their_indexes() {
    use crate::cas::gc::{
        COMPACTION_CANDIDATES, EVICT_RESULT_ROW, LIVE_BYTES, RELEASE_EXTENT, RESULT_EXTENTS,
        SEGMENTS_IN_STATE, SEGMENT_EXTENTS,
    };
    use crate::cas::store::{ACTIVE_SEGMENT, SEAL_ACTIVE_SEGMENT, SEAL_SEGMENT};
    use crate::served::DELETE_LOAD_EDGES;
    let (_dir, store) = store_with(10);
    let cases: &[(&str, &[&str])] = &[
        // One open regular segment per writer: `cas_segments_open` is
        // unique on the owner.
        (
            SEAL_ACTIVE_SEGMENT,
            &["SEARCH cas_segments USING INDEX cas_segments_open (owner=?)"],
        ),
        (SEAL_SEGMENT, &["SEARCH cas_segments USING INTEGER PRIMARY KEY (rowid=?)"]),
        // A result goes with its `result_outputs` rows (the cascade
        // searches their primary key), whose extents it reads first.
        (
            EVICT_RESULT_ROW,
            &["SEARCH results USING INDEX sqlite_autoindex_results_1 (key_kind=? AND static_key=? AND trace_digest=?)", "SEARCH result_outputs USING PRIMARY KEY (key_kind=? AND static_key=? AND trace_digest=?)"],
        ),
        (
            RESULT_EXTENTS,
            &["SEARCH result_outputs USING PRIMARY KEY (key_kind=? AND static_key=? AND trace_digest=?)"],
        ),
        // An extent goes when no holder names it: one search of each
        // holder table's hash index, then the foreign keys' (the load
        // edges cascade).
        (
            RELEASE_EXTENT,
            &[
                "SEARCH cas_extents USING INDEX sqlite_autoindex_cas_extents_1 (content_hash=?)",
                "SCALAR SUBQUERY 1",
                "SEARCH cas_refs USING COVERING INDEX cas_refs_by_hash (content_hash=?)",
                "SCALAR SUBQUERY 2",
                "SEARCH result_outputs USING COVERING INDEX result_outputs_by_hash (content_hash=?)",
                "SEARCH artifact_load_edges USING COVERING INDEX sqlite_autoindex_artifact_load_edges_1 (content_hash=?)",
                "SEARCH cas_refs USING COVERING INDEX cas_refs_by_hash (content_hash=?)",
                "SEARCH result_outputs USING COVERING INDEX result_outputs_by_hash (content_hash=?)",
            ],
        ),
        // The CAS's live bytes: a sum over the covering index, never a
        // table page.
        (LIVE_BYTES, &["SCAN cas_extents USING COVERING INDEX cas_extents_by_segment"]),
        // Each sealed segment (and this writer's open one) with its live
        // bytes: a covering-index range sum per segment.
        (
            COMPACTION_CANDIDATES,
            &[
                "MERGE (UNION ALL)",
                "LEFT",
                "SEARCH s USING INDEX cas_segments_by_state (state=?)",
                "CORRELATED SCALAR SUBQUERY 1",
                "SEARCH cas_extents USING COVERING INDEX cas_extents_by_segment (segment=?)",
                "RIGHT",
                "SEARCH s USING INDEX cas_segments_open (owner=?)",
                "CORRELATED SCALAR SUBQUERY 3",
                "SEARCH cas_extents USING COVERING INDEX cas_extents_by_segment (segment=?)",
            ],
        ),
        // A compacted segment's extents.
        (
            SEGMENT_EXTENTS,
            &["SEARCH cas_extents USING INDEX cas_extents_by_segment (segment=?)"],
        ),
        (
            SEGMENTS_IN_STATE,
            &["SEARCH cas_segments USING INDEX cas_segments_by_state (state=?)"],
        ),
        (
            ACTIVE_SEGMENT,
            &["SEARCH cas_segments USING COVERING INDEX cas_segments_open (owner=?)"],
        ),
        (
            DELETE_LOAD_EDGES,
            &["SEARCH artifact_load_edges USING COVERING INDEX sqlite_autoindex_artifact_load_edges_1 (content_hash=?)"],
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(&store.query_plan_details(sql).unwrap(), expected, "{sql}");
    }
}

/// Doctor's CAS verification runs one statement per segment, never one
/// per extent, each a search of the segment's extents.
#[test]
fn cas_verification_reads_per_segment() {
    use crate::cas::store::{VERIFY_SEGMENTS, VERIFY_SEGMENT_EXTENTS};
    use crate::cas::{BuildCommit, CommitOutcome, OutputSpec};
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let mut config = crate::StoreConfig::new(dir.path().join("state"));
    config.segment_size = 512;
    let mut store = Store::open(config).unwrap();
    for index in 0..300u32 {
        let mut key = [0u8; 32];
        key[..4].copy_from_slice(&index.to_le_bytes());
        store
            .commit_build(BuildCommit {
                wire_trees: Vec::new(),
                key_kind: crate::cas::record::KeyKind::Processor,
                static_input_key: key,
                asset_uuid: distill_core::id::AssetUuid([7; 16]),
                trace: vec![1],
                outcome: CommitOutcome::Success {
                    outputs: vec![OutputSpec {
                        output_key: String::new(),
                        type_uuids: vec![],
                        bytes: format!("output {index}").into_bytes(),
                    }],
                    aux: vec![],
                },
            })
            .unwrap();
    }
    let segments: usize = store
        .conn
        .query_row("SELECT COUNT(*) FROM cas_segments", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap() as usize;
    assert!(segments > 1 && segments < 100, "{segments}");
    let mut reader = store.reader().unwrap();
    connection(&mut reader).trace(Some(trace));
    assert_eq!(reader.verify_all_cas_extents().unwrap(), 300);
    connection(&mut reader).trace(None);
    let statements = std::mem::take(&mut *TRACED.lock().unwrap());
    assert_eq!(statements.len(), 1 + segments, "{statements:?}");
    assert_eq!(
        store.query_plan_details(VERIFY_SEGMENTS).unwrap(),
        ["SCAN cas_segments"]
    );
    assert_eq!(
        store.query_plan_details(VERIFY_SEGMENT_EXTENTS).unwrap(),
        ["SEARCH cas_extents USING INDEX cas_extents_by_segment (segment=?)"]
    );
}

/// A target replacement writes each target by its key; a pipeline fence
/// row is one insert.
#[test]
fn a_target_replacement_searches_its_key() {
    let (_dir, store) = store_with(1);
    assert_eq!(
        store
            .query_plan_details("DELETE FROM rpc_targets WHERE name = ?1")
            .unwrap(),
        ["SEARCH rpc_targets USING INDEX sqlite_autoindex_rpc_targets_1 (name=?)"]
    );
    assert!(store
        .query_plan_details(crate::served::SET_RPC_TARGET)
        .unwrap()
        .is_empty());
    assert!(store
        .query_plan_details(crate::served::APPEND_RECONNECT_ALL)
        .unwrap()
        .is_empty());
}

/// Pages a subscriber's history read fetches: one subscribed asset and
/// one subscribed path among `unrelated` other changes in the window.
fn history_pages(unrelated: u32) -> u64 {
    use crate::served::{Change, ServedWrite};
    use crate::state::InputVersion;
    use distill_core::id::AssetUuid;
    use std::collections::BTreeSet;
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(crate::StoreConfig::new(dir.path().join("state"))).unwrap();
    store
        .input_transaction(|txn| {
            let version = txn.version();
            for index in 0..unrelated {
                let mut uuid = [0u8; 16];
                uuid[..4].copy_from_slice(&index.to_le_bytes());
                txn.append_change(
                    version,
                    &Change::Asset {
                        asset: AssetUuid(uuid),
                        state: 1,
                    },
                )?;
                txn.append_change(
                    version,
                    &Change::Path {
                        path: format!("other/{index}"),
                    },
                )?;
            }
            txn.append_change(
                version,
                &Change::Asset {
                    asset: AssetUuid([0xff; 16]),
                    state: 1,
                },
            )?;
            txn.append_change(
                version,
                &Change::Path {
                    path: "watched".into(),
                },
            )
        })
        .unwrap();
    let reader = store.reader().unwrap();
    let assets = BTreeSet::from([AssetUuid([0xff; 16])]);
    let paths = BTreeSet::from(["watched".to_owned()]);
    pages(&reader, || {
        let history = reader
            .change_log_history(InputVersion(0), InputVersion(1), &assets, &paths)
            .unwrap();
        assert_eq!(history.len(), 2, "{history:?}");
    })
}

/// A subscriber's history costs its subscriptions, not the window: two
/// subjects are two index descents and two row lookups, whose depth grows
/// with the log of the table and nothing else. (The window read fetched
/// 90 pages at 5000 unrelated changes.)
#[test]
fn subscription_history_reads_the_subscribed_subjects() {
    let small = history_pages(10);
    let large = history_pages(5000);
    println!("history pages: {small} at 10 unrelated changes, {large} at 5000");
    assert!(
        large <= 16,
        "history pages: {small} at 10 unrelated changes, {large} at 5000"
    );
}

#[test]
fn subscription_history_searches_one_subject() {
    let (_dir, store) = store_with(1);
    assert_eq!(
        store.query_plan_details(crate::served::ASSET_HISTORY).unwrap(),
        ["SEARCH change_log USING INDEX change_log_assets (asset_uuid=? AND version>? AND version<?)"]
    );
    assert_eq!(
        store.query_plan_details(crate::served::PATH_HISTORY).unwrap(),
        ["SEARCH change_log USING INDEX change_log_paths (subject=? AND version>? AND version<?)"]
    );
}

/// A build node's entry and owning bundle are one statement of key
/// searches: the asset, its bundle, its tags.
#[test]
fn an_entry_and_its_bundle_are_one_statement() {
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, store) = store_with(1000);
    let mut reader = store.reader().unwrap();
    connection(&mut reader).trace(Some(trace));
    let (entry, bundle) = reader.entry_with_bundle(asset_uuid(7, 1)).unwrap().unwrap();
    let untagged = reader.entry(asset_uuid(8, 1)).unwrap().unwrap();
    let poisoned = reader.entry(asset_uuid(199, 1));
    connection(&mut reader).trace(None);
    let statements = std::mem::take(&mut *TRACED.lock().unwrap());
    assert_eq!(statements.len(), 3, "{statements:?}");
    assert_eq!(bundle.map(|bundle| bundle.bundle), Some(bundle_uuid(7)));
    assert_eq!(entry.bundle, bundle_uuid(7));
    assert_eq!(
        entry.tags,
        BTreeMap::from([
            ("kind".to_owned(), Some("texture".to_owned())),
            ("rare".to_owned(), Some("yes".to_owned())),
        ])
    );
    assert_eq!(untagged.tags.len(), 1);
    assert!(
        matches!(poisoned, Err(crate::StoreError::BundlePoisoned { .. })),
        "{poisoned:?}"
    );
    assert_eq!(
        store.query_plan_details(crate::bundles::ENTRY).unwrap(),
        [
            "SEARCH assets USING INDEX sqlite_autoindex_assets_1 (asset_uuid=?)",
            "SEARCH bundles USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?) LEFT-JOIN",
            "SEARCH asset_tags USING INDEX sqlite_autoindex_asset_tags_1 (asset_uuid=?) LEFT-JOIN",
        ]
    );
}

/// A traced statement with its literals replaced by `?`: statements that
/// differ only in their parameters have one shape.
fn statement_shape(sql: &str) -> String {
    let mut shape = String::new();
    let mut chars = sql.chars().peekable();
    let mut previous = ' ';
    while let Some(c) = chars.next() {
        let blob = (c == 'X' || c == 'x') && chars.peek() == Some(&'\'');
        let word = previous.is_alphanumeric() || previous == '_';
        if c == '\'' || (blob && !word) {
            if blob {
                chars.next();
            }
            for c in chars.by_ref() {
                if c == '\'' {
                    break;
                }
            }
            shape.push('?');
        } else if c.is_ascii_digit() && !word {
            while chars.peek().is_some_and(char::is_ascii_digit) {
                chars.next();
            }
            shape.push('?');
        } else {
            shape.push(c);
        }
        previous = c;
    }
    shape.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The point statements the CAS (an install, a build commit, its
/// lookup, reads, evictions, a compaction that copies live records) and
/// the served reads issue, each with its exact plan: every one searches the
/// key it is given, except the whole-table reads named here.
#[test]
fn cas_and_served_point_statements_search_their_keys() {
    use crate::cas::{BuildCommit, CommitOutcome, OutputSpec};
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let mut config = crate::StoreConfig::new(dir.path().join(".distill"));
    config.segment_size = 4096;
    let mut store = Store::open(config).unwrap();
    populate(&mut store, 50);
    let asset = asset_uuid(42, 1);
    let edges = [(asset_uuid(43, 1), RUNTIME_TYPE)];
    store.read.conn.trace(Some(trace));
    let installed = (0..12u8)
        .map(|index| store.put_artifact(&[index; 1000], &edges).unwrap())
        .collect::<Vec<_>>();
    store.put_artifact(&[0; 1000], &edges).unwrap();
    let key = [3; 32];
    store
        .commit_build(BuildCommit {
            wire_trees: Vec::new(),
            key_kind: crate::cas::record::KeyKind::Processor,
            static_input_key: key,
            asset_uuid: asset,
            trace: vec![1],
            outcome: CommitOutcome::Success {
                outputs: vec![OutputSpec {
                    output_key: String::new(),
                    type_uuids: vec![],
                    bytes: b"output".to_vec(),
                }],
                aux: vec![],
            },
        })
        .unwrap();
    let candidates = store
        .lookup_candidates(crate::cas::record::KeyKind::Processor, &key)
        .unwrap();
    let hash = installed[11];
    store.cas_read(&hash.0).unwrap();
    store.artifact_load_edges(hash).unwrap();
    store.served_entry_meta(asset).unwrap();
    // An inspection reads its bundle file at the published hash.
    store.bundle(BundleUuid([42; 16])).unwrap();
    store.root_name(crate::files::RootId(1)).unwrap();
    store.asset_resolution(asset).unwrap();
    // A missing asset reads its collisions too.
    store.asset_resolution(asset_uuid(200, 1)).unwrap();
    store.derived_output(asset).unwrap();
    store.served_path_candidates(&bundle_path(42)).unwrap();
    store
        .served_named_candidates(&bundle_path(42), "main")
        .unwrap();
    store.rpc_target("pc").unwrap();
    store.rpc_targets().unwrap();
    store.change_log_head().unwrap();
    store.change_log_after(0).unwrap();
    store.resolve_child(asset).unwrap();
    store
        .evict_result(
            crate::cas::record::KeyKind::Processor,
            &key,
            &candidates[0].trace_digest,
        )
        .unwrap();
    for hash in installed.iter().step_by(2) {
        store.evict_installed(&hash.0).unwrap();
    }
    let compaction = store.compact().unwrap();
    store.enforce_cache_limit().unwrap();
    store.read.conn.trace(None);
    assert!(compaction.extents_copied > 0, "{compaction:?}");
    let statements = std::mem::take(&mut *TRACED.lock().unwrap());
    let mut plans = BTreeMap::new();
    for sql in statements {
        // Trigger bodies are traced as comments.
        if sql.starts_with("--") {
            continue;
        }
        let plan = explain(&store.read.conn, &sql);
        if !plan.is_empty() {
            plans.entry(statement_shape(&sql)).or_insert(plan);
        }
    }
    // The whole-table reads: the live-bytes sum and the compaction
    // candidates are one row per segment; the RPC target set is a handful.
    let expected: &[(&str, &[&str])] = &[
        (
            "DELETE FROM artifact_load_edges WHERE content_hash = ?",
            &["SEARCH artifact_load_edges USING COVERING INDEX sqlite_autoindex_artifact_load_edges_1 (content_hash=?)"],
        ),
        (
            "SELECT ? FROM cas_extents WHERE content_hash = ?",
            &["SEARCH cas_extents USING COVERING INDEX sqlite_autoindex_cas_extents_1 (content_hash=?)"],
        ),
        (
            "SELECT COALESCE(MAX(seq), ?) FROM change_log",
            &["SEARCH change_log"],
        ),
        (
            "SELECT bundle_uuid, root_id, path, format_version, content_hash, origin_rules_bundle, origin_rule, origin_group_root, origin_group_path, import_watched FROM bundles WHERE bundle_uuid = ?",
            &["SEARCH bundles USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)"],
        ),
        (
            "SELECT name FROM roots WHERE root_id = ?",
            &["SEARCH roots USING INTEGER PRIMARY KEY (rowid=?)"],
        ),
        (
            "SELECT a.asset_uuid FROM assets a JOIN bundles b ON b.bundle_uuid = a.bundle_uuid WHERE a.terminal_type IS NOT NULL AND a.logical_hash IS NOT NULL AND b.poison IS NULL AND b.path = ? AND a.local_id = ? AND a.authoring_only = ?",
            &["SEARCH a USING INDEX assets_by_local_id (local_id=?)", "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)"],
        ),
        (
            "SELECT a.asset_uuid, a.bundle_uuid, a.local_id, b.path, a.type_uuid, a.terminal_type, a.logical_hash, a.authoring_only FROM assets a JOIN bundles b ON b.bundle_uuid = a.bundle_uuid WHERE a.terminal_type IS NOT NULL AND a.logical_hash IS NOT NULL AND b.poison IS NULL AND a.asset_uuid = ?",
            &["SEARCH a USING INDEX sqlite_autoindex_assets_1 (asset_uuid=?)", "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)"],
        ),
        (
            "SELECT primary_asset FROM bundles WHERE path = ? AND primary_asset IS NOT NULL",
            &["SEARCH bundles USING INDEX bundles_by_path (path=?)"],
        ),
        (
            "SELECT asset_uuid, expected_terminal FROM artifact_load_edges WHERE content_hash = ? ORDER BY asset_uuid",
            &["SEARCH artifact_load_edges USING INDEX sqlite_autoindex_artifact_load_edges_1 (content_hash=?)"],
        ),
        (
            "SELECT file_name FROM cas_segments WHERE segment_id = ?",
            &["SEARCH cas_segments USING INTEGER PRIMARY KEY (rowid=?)"],
        ),
        (
            "SELECT b.poison FROM assets a JOIN bundles b ON b.bundle_uuid = a.bundle_uuid WHERE a.asset_uuid = ?",
            &[
                "SEARCH a USING INDEX sqlite_autoindex_assets_1 (asset_uuid=?)",
                "SEARCH b USING INDEX sqlite_autoindex_bundles_1 (bundle_uuid=?)",
            ],
        ),
        // What withholds an asset: its own collision, then its bundle's.
        (
            "SELECT COUNT(DISTINCT claimant) > ? FROM source_claims WHERE kind IN (?, ?) AND subject = ?",
            &[
                "USE TEMP B-TREE FOR count(DISTINCT)",
                "SEARCH source_claims USING COVERING INDEX source_claims_by_subject (kind=? AND subject=?)",
            ],
        ),
        (
            "SELECT b.subject FROM source_claims a CROSS JOIN source_claims b ON b.root_id = a.root_id AND b.path = a.path AND b.kind = ? WHERE a.kind = ? AND a.subject = ? AND (SELECT COUNT(DISTINCT c.claimant) FROM source_claims c WHERE c.kind = ? AND c.subject = b.subject) > ? LIMIT ?",
            &[
                "SEARCH a USING INDEX source_claims_by_subject (kind=? AND subject=?)",
                "SEARCH b USING COVERING INDEX sqlite_autoindex_source_claims_1 (root_id=? AND path=? AND kind=?)",
                "CORRELATED SCALAR SUBQUERY 1",
                "SEARCH c USING COVERING INDEX source_claims_by_subject (kind=? AND subject=?)",
            ],
        ),
        (
            "SELECT name, definition_hash FROM rpc_targets ORDER BY name",
            &["SCAN rpc_targets USING INDEX sqlite_autoindex_rpc_targets_1"],
        ),
        (
            "SELECT name, definition_hash FROM rpc_targets WHERE name = ?",
            &["SEARCH rpc_targets USING INDEX sqlite_autoindex_rpc_targets_1 (name=?)"],
        ),
        // A derived child: its claims (then its and its parent's collisions).
        (
            "SELECT DISTINCT claimant, detail FROM source_claims WHERE kind = ? AND subject = ?",
            &[
                "SEARCH source_claims USING INDEX source_claims_by_subject (kind=? AND subject=?)",
                "USE TEMP B-TREE FOR DISTINCT",
            ],
        ),
        (
            "SELECT segment, offset, len FROM cas_extents WHERE content_hash = ?",
            &["SEARCH cas_extents USING INDEX sqlite_autoindex_cas_extents_1 (content_hash=?)"],
        ),
        (
            "SELECT segment_id FROM cas_segments WHERE owner = ? AND state = ? AND segment_kind = ?",
            &["SEARCH cas_segments USING COVERING INDEX cas_segments_open (owner=?)"],
        ),
        (
            "SELECT seq, version, kind, asset_uuid, state, subject FROM change_log WHERE seq > ? ORDER BY seq",
            &["SEARCH change_log USING INTEGER PRIMARY KEY (rowid>?)"],
        ),
        (
            "SELECT tag, value FROM asset_tags WHERE asset_uuid = ? ORDER BY tag",
            &["SEARCH asset_tags USING INDEX sqlite_autoindex_asset_tags_1 (asset_uuid=?)"],
        ),
        (
            "SELECT value FROM store_meta WHERE key = ?",
            &["SEARCH store_meta USING INDEX sqlite_autoindex_store_meta_1 (key=?)"],
        ),
        (
            "UPDATE cas_extents SET segment = ?, offset = ? WHERE content_hash = ? AND segment = ? AND offset = ?",
            &["SEARCH cas_extents USING INDEX sqlite_autoindex_cas_extents_1 (content_hash=?)"],
        ),
        (
            "UPDATE cas_segments SET indexed_len = ? WHERE segment_id = ?",
            &["SEARCH cas_segments USING INTEGER PRIMARY KEY (rowid=?)"],
        ),
        (
            "UPDATE cas_segments SET indexed_len = ?, state = CASE WHEN segment_kind = ? THEN ? ELSE state END WHERE segment_id = ?",
            &["SEARCH cas_segments USING INTEGER PRIMARY KEY (rowid=?)"],
        ),
        (
            "UPDATE cas_segments SET state = ? WHERE segment_id = ?",
            &["SEARCH cas_segments USING INTEGER PRIMARY KEY (rowid=?)"],
        ),
        // A result goes with its outputs (the cascade's search), an
        // install with its references, and an extent once neither holder
        // table names it (the foreign keys' searches).
        (
            "DELETE FROM cas_extents WHERE content_hash = ? AND NOT EXISTS (SELECT ? FROM cas_refs WHERE content_hash = ?) AND NOT EXISTS (SELECT ? FROM result_outputs WHERE content_hash = ?) RETURNING len",
            &[
                "SEARCH cas_extents USING INDEX sqlite_autoindex_cas_extents_1 (content_hash=?)",
                "SCALAR SUBQUERY 1",
                "SEARCH cas_refs USING COVERING INDEX cas_refs_by_hash (content_hash=?)",
                "SCALAR SUBQUERY 2",
                "SEARCH result_outputs USING COVERING INDEX result_outputs_by_hash (content_hash=?)",
                "SEARCH artifact_load_edges USING COVERING INDEX sqlite_autoindex_artifact_load_edges_1 (content_hash=?)",
                "SEARCH cas_refs USING COVERING INDEX cas_refs_by_hash (content_hash=?)",
                "SEARCH result_outputs USING COVERING INDEX result_outputs_by_hash (content_hash=?)",
            ],
        ),
        (
            "DELETE FROM cas_refs WHERE holder = ? RETURNING content_hash",
            &["SEARCH cas_refs USING PRIMARY KEY (holder=?)"],
        ),
        (
            "DELETE FROM results WHERE key_kind = ? AND static_key = ? AND trace_digest = ? RETURNING ?",
            &[
                "SEARCH results USING INDEX sqlite_autoindex_results_1 (key_kind=? AND static_key=? AND trace_digest=?)",
                "SEARCH result_outputs USING PRIMARY KEY (key_kind=? AND static_key=? AND trace_digest=?)",
            ],
        ),
        (
            "SELECT COALESCE(SUM(len), ?) FROM cas_extents",
            &["SCAN cas_extents USING COVERING INDEX cas_extents_by_segment"],
        ),
        (
            "SELECT content_hash FROM result_outputs WHERE key_kind = ? AND static_key = ? AND trace_digest = ?",
            &["SEARCH result_outputs USING PRIMARY KEY (key_kind=? AND static_key=? AND trace_digest=?)"],
        ),
        (
            "SELECT content_hash, offset, len FROM cas_extents WHERE segment = ?",
            &["SEARCH cas_extents USING INDEX cas_extents_by_segment (segment=?)"],
        ),
        // A candidate and what it names: one search of each primary key.
        (
            "SELECT r.asset_uuid, r.trace, r.failure, o.role, o.name, o.types, o.content_hash FROM results r LEFT JOIN result_outputs o ON o.key_kind = r.key_kind AND o.static_key = r.static_key AND o.trace_digest = r.trace_digest WHERE r.key_kind = ? AND r.static_key = ? AND r.trace_digest = ? ORDER BY o.role, o.name",
            &[
                "SEARCH r USING INDEX sqlite_autoindex_results_1 (key_kind=? AND static_key=? AND trace_digest=?)",
                "SEARCH o USING PRIMARY KEY (key_kind=? AND static_key=? AND trace_digest=?) LEFT-JOIN",
            ],
        ),
        (
            "SELECT segment_id, file_name, segment_kind, indexed_len, (SELECT COALESCE(SUM(len), ?) FROM cas_extents WHERE segment = s.segment_id) FROM cas_segments s WHERE state = ? UNION ALL SELECT segment_id, file_name, segment_kind, indexed_len, (SELECT COALESCE(SUM(len), ?) FROM cas_extents WHERE segment = s.segment_id) FROM cas_segments s WHERE owner = ? AND state = ? AND segment_kind = ? ORDER BY segment_id",
            &[
                "MERGE (UNION ALL)",
                "LEFT",
                "SEARCH s USING INDEX cas_segments_by_state (state=?)",
                "CORRELATED SCALAR SUBQUERY 1",
                "SEARCH cas_extents USING COVERING INDEX cas_extents_by_segment (segment=?)",
                "RIGHT",
                "SEARCH s USING INDEX cas_segments_open (owner=?)",
                "CORRELATED SCALAR SUBQUERY 3",
                "SEARCH cas_extents USING COVERING INDEX cas_extents_by_segment (segment=?)",
            ],
        ),
        (
            "SELECT trace_digest, memo_seq FROM results WHERE key_kind = ? AND static_key = ? ORDER BY memo_seq DESC",
            &[
                "SEARCH results USING INDEX sqlite_autoindex_results_1 (key_kind=? AND static_key=?)",
                "USE TEMP B-TREE FOR ORDER BY",
            ],
        ),
    ];
    let expected = expected
        .iter()
        .map(|(sql, plan)| {
            (
                sql.to_string(),
                plan.iter().map(|step| step.to_string()).collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(plans, expected);
}

/// A file's content hash, what a write receipt is waited on by, is one
/// search of the root name's index and one of the files primary key.
#[test]
fn a_file_content_hash_searches_two_keys() {
    let (_dir, store) = store_with(10);
    assert_eq!(
        store
            .query_plan_details(crate::files::FILE_CONTENT_HASH)
            .unwrap(),
        [
            "SEARCH r USING COVERING INDEX sqlite_autoindex_roots_1 (name=?)",
            "SEARCH t USING INDEX sqlite_autoindex_files_1 (root_id=? AND path=?)",
        ]
    );
    let reader = store.reader().unwrap();
    let path = bundle_path(0);
    assert_eq!(
        reader.file_content_hash("main", &path).unwrap(),
        Some(ContentHash(*blake3::hash(b"bundle 0").as_bytes()))
    );
    assert_eq!(reader.file_content_hash("alt", &path).unwrap(), None);
    assert_eq!(reader.file_content_hash("main", "missing").unwrap(), None);
}

/// The complete published observation's bundle hashes (the full scan's
/// comparison of its candidate with the store) are the `files` rows of
/// `.bundle` files, read through their extension index; the store keeps no
/// bundle bytes.
#[test]
fn every_bundle_file_hash_is_one_extension_search() {
    let _tracing = TRACING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, mut store) = store_with(200);
    let plans = subtree_plans(&mut store, |store| {
        let mut count = 0;
        store
            .for_each_bundle_file_hash(|_, _, _| {
                count += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(count, 200);
    });
    assert_eq!(plans.len(), 1, "{plans:#?}");
    assert_eq!(
        plans[0].1,
        [
            "SEARCH t USING INDEX files_by_ext (ext=?)",
            "SEARCH r USING INTEGER PRIMARY KEY (rowid=?)",
            "USE TEMP B-TREE FOR ORDER BY",
        ],
        "{plans:#?}"
    );
}

/// The directory-import rules the source `path` of the root "main" holds.
fn rules_at(store: &StoreReader, path: &str) -> Vec<crate::imports::DirectoryRuleSource> {
    store
        .directory_rule_sources()
        .unwrap()
        .into_iter()
        .filter(|source| source.root_name == "main" && source.path == path)
        .collect()
}

/// The assets the old namespace's collisions withhold are found from the
/// claims: the grouped asset claims, and per colliding bundle subject its
/// claims and their sources' authored claims, by key.
#[test]
fn the_withheld_assets_are_searched_from_the_collisions() {
    let (_dir, store) = store_with(0);
    assert_eq!(
        store
            .query_plan_details(crate::served::WITHHELD_ASSETS)
            .unwrap(),
        [
            "COMPOUND QUERY",
            "LEFT-MOST SUBQUERY",
            "SEARCH source_claims USING COVERING INDEX source_claims_by_subject (kind=?)",
            "USE TEMP B-TREE FOR GROUP BY",
            "USE TEMP B-TREE FOR count(DISTINCT)",
            "UNION USING TEMP B-TREE",
            "CO-ROUTINE c",
            "SEARCH source_claims USING COVERING INDEX source_claims_by_subject (kind=?)",
            "SCAN c",
            "SEARCH b USING INDEX source_claims_by_subject (kind=? AND subject=?)",
            "SEARCH a USING COVERING INDEX sqlite_autoindex_source_claims_1 \
             (root_id=? AND path=? AND kind=?)",
        ]
    );
}

/// A new bundle's row and its assets' rows are plain inserts: nothing is
/// searched or cleared.
#[test]
fn a_new_bundle_is_inserted_without_a_search() {
    let (_dir, store) = store_with(0);
    for sql in [
        crate::bundles::INSERT_BUNDLE,
        crate::bundles::INSERT_ASSET,
        "INSERT INTO asset_tags(asset_uuid, tag, value) VALUES (?1, ?2, ?3)",
    ] {
        assert_eq!(store.query_plan_details(sql).unwrap(), [""; 0], "{sql}");
    }
}
