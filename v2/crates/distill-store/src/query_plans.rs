//! The plans and costs of the namespace's selective reads. Every statement
//! those readers issue is captured from SQLite's trace and explained: none
//! may scan a namespace table. And over a large namespace, a narrow read
//! touches a small, fixed number of pages where the whole-table read it
//! replaced touches them all.

use std::collections::BTreeMap;
use std::sync::Mutex;

use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use rusqlite::Connection;

use crate::bundles::{
    AssetFilter, AssetRecord, BundleMeta, NamespaceSkeleton, ServedAuthoring, SkeletonEntry,
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
/// its bytes, and a path index entry; one in a hundred referencing
/// [`REFERENCED`], one in a thousand with an authoring-only `$record` entry
/// and beside a `.png` file, and one in five hundred poisoned instead.
fn populate(store: &mut Store, count: u32) {
    store
        .input_transaction(|txn| {
            let version = txn.version();
            let roots = [txn.intern_root("main")?, txn.intern_root("alt")?];
            txn.put_schema(SCHEMA, "{}")?;
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
                txn.set_bundle_file(root, &path, &bytes)?;
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
                    served: Some(ServedAuthoring {
                        authored_value: Vec::new(),
                        terminal_type: RUNTIME_TYPE,
                    }),
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
                        served: None,
                    })?;
                }
                if index % 100 == 1 {
                    txn.set_bundle_path_refs(bundle, [REFERENCED])?;
                }
                txn.set_path_entry(&path, root, runtime)?;
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
    reader.bundles_with_reserved_entry("$record").unwrap();
    reader.bundles_referencing_path(REFERENCED).unwrap();
    reader.asset_exists(asset_uuid(42, 1)).unwrap();
    reader.check_entries().unwrap_err();
    reader.check_tag_selector("rare", Some("yes"), false).unwrap();
    reader.check_tag_selector("kind", None, true).unwrap();
    reader.observed_files_in(PathSelection::Subtree("d07")).unwrap();
    reader.observed_files_in(PathSelection::Prefix("d07/b000")).unwrap();
    reader.observed_files_in(PathSelection::Name("b00042.bundle")).unwrap();
    reader.observed_files_in(PathSelection::Extension("png")).unwrap();
    for (filter, _) in filter_shapes() {
        reader.served_assets_matching(&filter).unwrap();
        reader.namespace_assets_matching(&filter).unwrap();
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
        reader.served_assets_matching(&filter).unwrap();
        reader.namespace_assets_matching(&filter).unwrap();
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
const NAMESPACE_TABLES: [&str; 8] = [
    "assets",
    "bundles",
    "files",
    "asset_tags",
    "asset_tag_index",
    "bundle_path_refs",
    "path_index",
    "directories",
];

/// Partial indexes: walking one visits only the rows it was declared for.
const PARTIAL_INDEXES: [&str; 6] = [
    "bundles_poisoned",
    "assets_unhashed",
    "assets_authoring",
    "files_by_ext",
    "asset_tag_index_poisoned",
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
    let _tracing = TRACING.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
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
/// not be driven by, with the index that must drive it.
fn filter_shapes() -> Vec<(AssetFilter, &'static str)> {
    let name = |index: u32| path_name(&bundle_path(index)).to_owned();
    let kind_mesh = Some(("kind".to_owned(), Some("mesh".to_owned())));
    vec![
        (
            AssetFilter {
                asset: Some(asset_uuid(42, 1)),
                authored_type: Some(RUNTIME_TYPE),
                ..AssetFilter::default()
            },
            "SEARCH a USING INDEX sqlite_autoindex_assets_1 (asset_uuid=?)",
        ),
        (
            AssetFilter {
                bundle: Some(bundle_uuid(42)),
                local_id: Some("main".into()),
                tag: kind_mesh.clone(),
                ..AssetFilter::default()
            },
            "SEARCH a USING INDEX assets_by_bundle (bundle_uuid=? AND local_id=?)",
        ),
        (
            AssetFilter {
                bundle_path: Some(bundle_path(42)),
                local_id: Some("main".into()),
                ..AssetFilter::default()
            },
            "SEARCH b USING INDEX bundles_by_path (path=?)",
        ),
        (
            AssetFilter {
                bundle_name: Some(name(42)),
                authored_type: Some(RUNTIME_TYPE),
                path_prefixes: vec!["d42/".into()],
                tag: kind_mesh.clone(),
                ..AssetFilter::default()
            },
            "SEARCH b USING INDEX bundles_by_name (name=?)",
        ),
        (
            AssetFilter {
                local_id: Some("$record".into()),
                authored_type: Some(RECORD_TYPE),
                authoring_only: Some(true),
                ..AssetFilter::default()
            },
            "SEARCH a USING INDEX assets_by_local_id (local_id=?)",
        ),
        (
            AssetFilter {
                tag_index_poisoned: true,
                path_prefixes: vec!["d07/".into()],
                authoring_only: Some(false),
                ..AssetFilter::default()
            },
            "SEARCH a USING INDEX sqlite_autoindex_assets_1 (asset_uuid=?)",
        ),
        (
            AssetFilter {
                tag: kind_mesh,
                authored_type: Some(RUNTIME_TYPE),
                path_prefixes: vec!["d07/".into()],
                ..AssetFilter::default()
            },
            "SEARCH a USING INDEX sqlite_autoindex_assets_1 (asset_uuid=?)",
        ),
        (
            AssetFilter {
                authored_type: Some(RUNTIME_TYPE),
                terminal_type: Some(RUNTIME_TYPE),
                path_prefixes: vec!["d07/".into()],
                tag: Some(("kind".into(), None)),
                ..AssetFilter::default()
            },
            "SEARCH a USING INDEX assets_by_type (type_uuid=?)",
        ),
        (
            AssetFilter {
                path_prefixes: vec!["d07/b000".into()],
                terminal_type: Some(RUNTIME_TYPE),
                tag: Some(("kind".into(), None)),
                authoring_only: Some(false),
                ..AssetFilter::default()
            },
            "SEARCH b USING INDEX bundles_by_path (path>? AND path<?)",
        ),
        (
            AssetFilter {
                terminal_type: Some(RUNTIME_TYPE),
                tag: Some(("kind".into(), None)),
                ..AssetFilter::default()
            },
            "SEARCH a USING INDEX assets_by_terminal_type (terminal_type=?)",
        ),
        (
            AssetFilter {
                tag: Some(("rare".into(), None)),
                authoring_only: Some(false),
                ..AssetFilter::default()
            },
            "SEARCH a USING INDEX sqlite_autoindex_assets_1 (asset_uuid=?)",
        ),
        (
            AssetFilter {
                authoring_only: Some(true),
                ..AssetFilter::default()
            },
            "SCAN a USING INDEX assets_authoring",
        ),
    ]
}

/// Each filter's query is driven by the index of its most selective
/// selector, whatever else it names: SQLite has no statistics to choose by.
#[test]
fn filters_are_driven_by_their_most_selective_index() {
    let _tracing = TRACING.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, store) = store_with(200);
    let mut reader = store.reader().unwrap();
    for (filter, driver) in filter_shapes() {
        connection(&mut reader).trace(Some(trace));
        reader.namespace_assets_matching(&filter).unwrap();
        reader.served_assets_matching(&filter).unwrap();
        connection(&mut reader).trace(None);
        let statements = std::mem::take(&mut *TRACED.lock().unwrap());
        assert_eq!(statements.len(), 2, "{statements:?}");
        for sql in statements {
            let plan = explain(connection(&mut reader), &sql);
            // Whether the driving index also covers the read is beside the point.
            let first = plan.first().map(|step| step.replace("COVERING INDEX", "INDEX"));
            assert_eq!(first.as_deref(), Some(driver), "{filter:?}\n{sql}\n{plan:?}");
        }
    }
}

/// Pages fetched by `read`, by this reader's page-cache counters.
fn pages(reader: &StoreReader, read: impl FnOnce()) -> u64 {
    let before = reader.pages_fetched().unwrap();
    read();
    reader.pages_fetched().unwrap() - before
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
            pages(&reader, || rows = count(reader.bundles_at_path(&path).unwrap().len(), 1)),
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
                rows = count(reader.served_assets_matching(&at_path(&path)).unwrap().len(), 1);
            }),
            rows,
            pages(&reader, || drop(reader.served_entries().unwrap())),
        ),
        (
            "rarely tagged entries",
            pages(&reader, || {
                rows = count(reader.namespace_assets_matching(&rare()).unwrap().len(), 20);
            }),
            rows,
            pages(&reader, || {
                drop(reader.all_bundles().unwrap());
                drop(reader.assets_by_tag_value("rare", Some("yes")).unwrap());
            }),
        ),
        (
            "unreadable entries",
            pages(&reader, || {
                reader.check_entries().unwrap_err();
                // It visits each poisoned bundle's skeleton rows: one in 500.
                rows = 40;
            }),
            rows,
            pages(&reader, || {
                for asset in reader.all_asset_ids().unwrap() {
                    if reader.entry(asset).is_err() {
                        break;
                    }
                }
            }),
        ),
        (
            "entries at a bundle name, beside a broad type",
            pages(&reader, || {
                let filter = AssetFilter {
                    bundle_name: Some(path_name(&path).to_owned()),
                    authored_type: Some(RUNTIME_TYPE),
                    ..AssetFilter::default()
                };
                rows = count(reader.served_assets_matching(&filter).unwrap().len(), 1);
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
                rows = count(reader.namespace_assets_matching(&filter).unwrap().len(), 20);
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
                rows = count(reader.namespace_assets_matching(&filter).unwrap().len(), 20);
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
                rows = count(reader.bundles_referencing_path(REFERENCED).unwrap().len(), 200);
            }),
            rows,
            pages(&reader, || drop(reader.all_bundles().unwrap())),
        ),
    ];
    for (read, narrow, rows, full) in costs {
        println!("{read}: {rows} rows in {narrow} pages (whole-table read: {full} pages)");
        assert!(narrow <= 16 + 8 * rows, "{read} fetched {narrow} pages for {rows} rows");
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

/// A bundle file's hash precedes its bytes in its row, so reading every
/// hash reads the rows' first pages, never the overflow pages that hold
/// the bytes of a bundle larger than a page. SQLite reads overflow pages
/// around its page cache, so this counts the bytes the reading thread
/// read from files.
#[cfg(target_os = "linux")]
#[test]
fn reading_bundle_file_hashes_skips_their_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    store
        .input_transaction(|txn| {
            let root = txn.intern_root("main")?;
            for index in 0..200u32 {
                let bytes = vec![index as u8; 16 * 1024];
                txn.set_bundle_file(root, &bundle_path(index), &bytes)?;
            }
            Ok(())
        })
        .unwrap();
    // Bytes this thread has read from files.
    fn read_bytes() -> u64 {
        let io = std::fs::read_to_string("/proc/thread-self/io").unwrap();
        let line = io.lines().find(|line| line.starts_with("rchar:")).unwrap();
        line["rchar:".len()..].trim().parse().unwrap()
    }
    fn read(read: impl FnOnce()) -> u64 {
        let before = read_bytes();
        read();
        read_bytes() - before
    }
    let reader = store.reader().unwrap();
    let mut hashes = 0;
    let hash_read = read(|| {
        reader
            .for_each_bundle_file_hash(|_, _, _| {
                hashes += 1;
                Ok(())
            })
            .unwrap();
    });
    let bytes_read = read(|| drop(reader.bundle_files().unwrap()));
    assert_eq!(hashes, 200);
    println!("hashes: {hash_read} bytes read; bundle bytes: {bytes_read}");
    assert!(hash_read <= 512 * 1024, "{hash_read} bytes read");
    assert!(bytes_read >= 20 * hash_read, "{hash_read} against {bytes_read} bytes read");
}

/// Scan-structure and claim rows under every scanned bundle's directory,
/// and a symlink beside every hundredth bundle, for the subtree reads.
fn populate_scan_structure(store: &mut Store, count: u32) {
    let mut directories = Vec::new();
    let mut diagnostics = Vec::new();
    let mut claims = Vec::new();
    for index in 0..count {
        let root = ["main", "alt"][(index % 2) as usize].to_owned();
        let path = bundle_path(index);
        directories.push(crate::files::ObservedDirectory {
            root_name: root.clone(),
            path: format!("{path}.d"),
            canonical_path: format!("/c/{index}").into_bytes(),
            physical_path: format!("/p/{index}").into_bytes(),
        });
        diagnostics.push(crate::files::ObservedDiagnostic {
            root_name: root.clone(),
            path: format!("{path}.x"),
            detail: Vec::new(),
        });
        claims.push(crate::claims::SourceClaims {
            root_name: root,
            path: path.clone(),
            claims: vec![crate::claims::SourceClaim::PrimaryPath {
                path,
                asset: asset_uuid(index, 1),
            }],
        });
    }
    store
        .input_transaction(|txn| {
            let version = txn.version();
            txn.replace_scan_structure(None, &directories, &diagnostics)?;
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
                        symlink_target: Some(format!("/t/d{:02}/x{index}", index % 50).into_bytes()),
                    },
                    version,
                )?;
            }
            Ok(())
        })
        .unwrap();
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
        .filter(|sql| sql.contains("JOIN roots r USING (root_id)") || sql.contains("symlink_target >="))
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
    let _tracing = TRACING.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, mut store) = store_with(200);
    populate_scan_structure(&mut store, 200);
    let roots = "SEARCH r USING COVERING INDEX sqlite_autoindex_roots_1 (name=?)";
    for (prefix, key) in [("d07", "root_id=? AND path>? AND path<?"), ("", "root_id=?")] {
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
            store.observed_directories_under("main", prefix).unwrap();
            store.scan_diagnostics_under("main", prefix).unwrap();
            store.bundle_file_hashes_under("main", prefix).unwrap();
        });
        let plans = reads.iter().map(|(_, plan)| plan.clone()).collect::<Vec<_>>();
        assert_eq!(
            plans,
            ["files", "directories", "scan_diagnostics", "bundle_files"].map(read),
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
                "SCALAR SUBQUERY 2".to_owned(),
                roots.to_owned(),
                search("directories", true),
            ],
            "{prefix:?}: {exists:#?}"
        );
        let under = [("main".to_owned(), prefix.to_owned())];
        let writes = subtree_plans(&mut store, |store| {
            store.replace_scan_diagnostics(Some(&under), &[]).unwrap();
            store
                .input_transaction(|txn| {
                    txn.replace_scan_structure(Some(&under), &[], &[])?;
                    txn.replace_source_claims(Some(&under), &[])
                })
                .unwrap();
        });
        let plans = writes.iter().map(|(_, plan)| plan.clone()).collect::<Vec<_>>();
        assert_eq!(
            plans,
            [
                delete("scan_diagnostics"),
                delete("directories"),
                delete("scan_diagnostics"),
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
            "SEARCH t USING INDEX directories_by_canonical (canonical_path=?)",
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
    // 10,000, each with a directory and a diagnostic row below it.
    let mut rows = 0;
    let narrow = pages(&reader, || {
        rows = reader.observed_files_under("main", "d08").unwrap().len();
        rows += reader.observed_directories_under("main", "d08").unwrap().len();
        rows += reader.scan_diagnostics_under("main", "d08").unwrap().len();
        rows += reader.bundle_file_hashes_under("main", "d08").unwrap().len();
    });
    let whole = pages(&reader, || {
        drop(reader.observed_files_under("main", "").unwrap());
        drop(reader.observed_directories_under("main", "").unwrap());
        drop(reader.scan_diagnostics_under("main", "").unwrap());
        drop(reader.bundle_file_hashes_under("main", "").unwrap());
    });
    println!("subtree: {rows} rows in {narrow} pages (whole root: {whole} pages)");
    assert_eq!(rows, 4 * 400);
    assert!(narrow <= 16 + 2 * rows as u64, "{narrow} pages for {rows} rows");
    assert!(whole >= 10 * narrow, "{narrow} pages against {whole}");
}
