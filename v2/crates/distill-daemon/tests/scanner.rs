use std::collections::BTreeMap;

use distill_bundle::{AssetEntry, Bundle, EntryLineageV1};
use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};
use distill_core::lineage::{lineage_chain_digest, AcceptedSchemaEpoch, LineageStamp};
use distill_daemon::scanner::{AssetRoot, RootedScanner, ScanError, ScannedFileKind};
use distill_json::AuthoredValue;
use distill_rpc::{LineageRepairDestination, OccupiedLineageDestinationKind};
use distill_schema::ngp_schema::{node_hash, LogicalSchema, PrimitiveKind, SchemaNode};

fn ordinary_bundle() -> Vec<u8> {
    let type_uuid = TypeUuid([71; 16]);
    let schema = LogicalSchema {
        root: SchemaNode::Primitive(PrimitiveKind::U8),
    };
    let schema_hash = node_hash(&schema.root).unwrap();
    let epochs = vec![AcceptedSchemaEpoch {
        digest: schema_hash,
        forward_parent: None,
    }];
    let entry = AssetEntry {
        uuid: AssetUuid([72; 16]),
        type_uuid,
        schema_hash,
        lineage: EntryLineageV1::Manifest(LineageStamp {
            chain: lineage_chain_digest(type_uuid, &epochs, 0),
            epochs,
            cursor: 0,
        }),
        authoring_only: false,
        data: AuthoredValue::UInt(7),
    };
    distill_bundle::write_bundle(&Bundle {
        format_version: 1,
        uuid: BundleUuid([73; 16]),
        primary: Some("entry".into()),
        schemas: BTreeMap::from([(schema_hash, schema)]),
        assets: BTreeMap::from([("entry".into(), entry)]),
    })
    .unwrap()
}

fn scanner(temp: &tempfile::TempDir) -> RootedScanner {
    let root = temp.path().join("assets");
    std::fs::create_dir_all(&root).unwrap();
    RootedScanner::new([AssetRoot::new(
        "main",
        &root,
        root.join(".distill-displaced"),
    )])
    .unwrap()
}

#[test]
fn full_scan_reports_raw_files_and_keeps_malformed_bundle_candidates() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir_all(root.join("nested")).unwrap();
    std::fs::write(root.join("nested/good.bundle"), ordinary_bundle()).unwrap();
    std::fs::write(root.join("bad.bundle"), b"not a bundle").unwrap();
    std::fs::write(root.join("source.png"), b"raw source").unwrap();
    let scanner = scanner(&temp);

    let scan = scanner.scan().unwrap();
    assert_eq!(
        scan.files
            .iter()
            .map(|file| (file.normalized_path.as_str(), file.kind))
            .collect::<Vec<_>>(),
        [
            ("bad.bundle", ScannedFileKind::File),
            ("nested", ScannedFileKind::Directory),
            ("nested/good.bundle", ScannedFileKind::File),
            ("source.png", ScannedFileKind::File),
        ]
    );
    assert_eq!(scan.bundles.len(), 2);
    assert!(scan.bundles[0].parsed.is_err());
    assert!(scan.bundles[1].parsed.is_ok());
    assert!(scan
        .files
        .iter()
        .filter(|file| file.kind == ScannedFileKind::File)
        .all(|file| file.content_hash.is_some()));
}

#[test]
fn incremental_scan_reobserves_only_named_paths() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir_all(&root).unwrap();
    let changed = root.join("changed.txt");
    let unrelated = root.join("unrelated.txt");
    std::fs::write(&changed, b"before").unwrap();
    std::fs::write(&unrelated, b"retained").unwrap();
    let scanner = scanner(&temp);
    let baseline = scanner.scan().unwrap();

    std::fs::write(&changed, b"after").unwrap();
    std::fs::remove_file(&unrelated).unwrap();
    let partial = scanner
        .scan_incremental(&baseline, std::slice::from_ref(&changed))
        .unwrap()
        .unwrap();

    assert_eq!(partial.files.len(), 2);
    assert!(partial
        .files
        .iter()
        .any(|file| file.normalized_path == "unrelated.txt"));
    assert_ne!(
        partial
            .files
            .iter()
            .find(|file| file.normalized_path == "changed.txt")
            .unwrap()
            .content_hash,
        baseline
            .files
            .iter()
            .find(|file| file.normalized_path == "changed.txt")
            .unwrap()
            .content_hash
    );

    let healed = scanner
        .scan_incremental(&partial, std::slice::from_ref(&unrelated))
        .unwrap()
        .unwrap();
    assert_eq!(healed.files.len(), 1);
    assert!(!healed
        .files
        .iter()
        .any(|file| file.normalized_path == "unrelated.txt"));
}

#[test]
fn incremental_directory_create_enumerates_only_that_subtree() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("stable.txt"), b"stable").unwrap();
    let scanner = scanner(&temp);
    let baseline = scanner.scan().unwrap();

    let subtree = root.join("new");
    std::fs::create_dir_all(subtree.join("nested")).unwrap();
    std::fs::write(subtree.join("nested/source.txt"), b"source").unwrap();
    let updated = scanner
        .scan_incremental(&baseline, std::slice::from_ref(&subtree))
        .unwrap()
        .unwrap();

    assert_eq!(
        updated
            .files
            .iter()
            .map(|file| file.normalized_path.as_str())
            .collect::<Vec<_>>(),
        ["new", "new/nested", "new/nested/source.txt", "stable.txt"]
    );
}

#[test]
fn destination_basis_distinguishes_absent_opaque_and_exact_canonical_bundle() {
    let temp = tempfile::tempdir().unwrap();
    let scanner = scanner(&temp);
    assert_eq!(
        scanner
            .inspect_destination("main", "control/lineage.bundle")
            .unwrap(),
        LineageRepairDestination::Absent
    );

    let control = temp.path().join("assets/control");
    std::fs::create_dir_all(&control).unwrap();
    let destination = control.join("lineage.bundle");
    std::fs::write(&destination, b"not a bundle").unwrap();
    assert!(matches!(
        scanner
            .inspect_destination("main", "control/lineage.bundle")
            .unwrap(),
        LineageRepairDestination::Occupied {
            kind: OccupiedLineageDestinationKind::Opaque,
            ..
        }
    ));

    std::fs::write(&destination, ordinary_bundle()).unwrap();
    assert!(matches!(
        scanner
            .inspect_destination("main", "control/lineage.bundle")
            .unwrap(),
        LineageRepairDestination::Occupied {
            kind: OccupiedLineageDestinationKind::CanonicalBundle,
            ..
        }
    ));
}

#[test]
fn rooted_paths_reject_noncanonical_or_traversing_input() {
    let temp = tempfile::tempdir().unwrap();
    let scanner = scanner(&temp);
    for path in ["", "/absolute.bundle", "../escape.bundle", "a//b.bundle"] {
        assert!(matches!(
            scanner.physical_path("main", path),
            Err(ScanError::InvalidLogicalPath(_))
        ));
    }
    assert!(matches!(
        scanner.physical_path("unknown", "a.bundle"),
        Err(ScanError::UnknownRoot(_))
    ));
}

#[test]
fn replacement_roots_are_shared_by_existing_scanner_clones() {
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    std::fs::create_dir_all(&first).unwrap();
    std::fs::create_dir_all(&second).unwrap();
    std::fs::write(first.join("old.txt"), b"old").unwrap();
    std::fs::write(second.join("new.txt"), b"new").unwrap();

    let scanner = RootedScanner::new([AssetRoot::new(
        "main",
        &first,
        first.join(".distill-displaced"),
    )])
    .unwrap();
    let watcher_view = scanner.clone();
    scanner
        .replace_roots([AssetRoot::new(
            "main",
            &second,
            second.join(".distill-displaced"),
        )])
        .unwrap();

    let scan = watcher_view.scan().unwrap();
    assert_eq!(scan.files.len(), 1);
    assert_eq!(scan.files[0].normalized_path, "new.txt");
    assert_eq!(
        watcher_view.physical_path("main", "new.txt").unwrap(),
        second.join("new.txt")
    );
}

#[test]
fn same_directory_identity_under_two_roots_is_never_tiebroken() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir_all(&root).unwrap();
    let scanner = RootedScanner::new([
        AssetRoot::new("first", &root, root.join(".q1")),
        AssetRoot::new("second", &root, root.join(".q2")),
    ])
    .unwrap();

    assert!(matches!(
        scanner.lineage_claimants(),
        Err(ScanError::DirectoryAlias { .. })
    ));
}

#[cfg(unix)]
#[test]
fn symlinked_directory_cannot_escape_configured_roots() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let scanner = scanner(&temp);
    symlink(outside.path(), temp.path().join("assets/escape")).unwrap();

    assert!(matches!(
        scanner.lineage_claimants(),
        Err(ScanError::SymlinkEscape { .. })
    ));
}

#[cfg(unix)]
#[test]
fn in_root_file_symlinks_are_identity_checked_and_reported() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("source.txt"), b"trusted").unwrap();
    symlink("source.txt", root.join("alias.txt")).unwrap();
    let scanner = scanner(&temp);

    let scan = scanner.scan().unwrap();
    let alias = scan
        .files
        .iter()
        .find(|file| file.normalized_path == "alias.txt")
        .unwrap();
    assert_eq!(alias.kind, ScannedFileKind::Symlink);
    assert_eq!(
        alias.content_hash,
        Some(distill_core::id::ContentHash(
            *blake3::hash(b"trusted").as_bytes()
        ))
    );
}

#[cfg(unix)]
#[test]
fn retained_root_capability_cannot_be_redirected_by_path_replacement() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    let retained = temp.path().join("retained-assets");
    let outside = temp.path().join("outside");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(root.join("source.txt"), b"trusted").unwrap();
    std::fs::write(outside.join("source.txt"), b"redirected").unwrap();
    let scanner = scanner(&temp);

    std::fs::rename(&root, &retained).unwrap();
    symlink(&outside, &root).unwrap();

    assert_eq!(
        scanner
            .read_identity_checked(&root.join("source.txt"))
            .unwrap(),
        b"trusted"
    );
    let scan = scanner.scan().unwrap();
    let source = scan
        .files
        .iter()
        .find(|file| file.normalized_path == "source.txt")
        .unwrap();
    assert_eq!(
        source.content_hash,
        Some(distill_core::id::ContentHash(
            *blake3::hash(b"trusted").as_bytes()
        ))
    );
}
