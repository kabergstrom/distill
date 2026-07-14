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
