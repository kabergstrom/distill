use std::collections::BTreeMap;

use distill_bundle::{AssetEntry, Bundle};
use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};
use distill_daemon::scanner::{
    AssetRoot, RootedScanner, ScanDiagnostic, ScanError, ScannedFileKind,
};
use distill_json::AuthoredValue;
use distill_schema::ngp_schema::{node_hash, LogicalSchema, PrimitiveKind, SchemaNode};
use distill_store::state::{PlatformPathBytes, ScanSubject};

fn ordinary_bundle() -> Vec<u8> {
    let type_uuid = TypeUuid([71; 16]);
    let schema = LogicalSchema {
        root: SchemaNode::Primitive(PrimitiveKind::U8),
    };
    let schema_hash = node_hash(&schema.root).unwrap();
    let entry = AssetEntry {
        uuid: AssetUuid([72; 16]),
        type_uuid,
        schema_hash,
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
        scan.file_rows()
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
    assert!(scan.bundle_rows().next().unwrap().parsed.is_err());
    assert!(scan.bundle_rows().nth(1).unwrap().parsed.is_ok());
    assert!(scan
        .file_rows()
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
        .file_rows()
        .any(|file| file.normalized_path == "unrelated.txt"));
    assert_ne!(
        partial
            .file_rows()
            .find(|file| file.normalized_path == "changed.txt")
            .unwrap()
            .content_hash,
        baseline
            .file_rows()
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
        .file_rows()
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
            .file_rows()
            .map(|file| file.normalized_path.as_str())
            .collect::<Vec<_>>(),
        ["new", "new/nested", "new/nested/source.txt", "stable.txt"]
    );
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
    assert_eq!(scan.file_rows().next().unwrap().normalized_path, "new.txt");
    assert_eq!(
        watcher_view.physical_path("main", "new.txt").unwrap(),
        second.join("new.txt")
    );
}

#[test]
fn same_canonical_directory_under_two_roots_is_never_tiebroken() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir_all(&root).unwrap();
    let scanner = RootedScanner::new([
        AssetRoot::new("first", &root, root.join(".q1")),
        AssetRoot::new("second", &root, root.join(".q2")),
    ])
    .unwrap();

    assert!(matches!(
        scanner.scan(),
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
        scanner.scan(),
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
        .file_rows()
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
fn configured_root_revalidation_rejects_path_replacement() {
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

    assert!(matches!(
        scanner.read_identity_checked(&root.join("source.txt")),
        Err(ScanError::RootUnavailable { .. })
    ));
    assert!(matches!(
        scanner.scan(),
        Err(ScanError::RootUnavailable { .. })
    ));
}

#[cfg(unix)]
#[test]
fn incremental_target_edit_reobserves_file_symlink_alias() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir_all(&root).unwrap();
    let target = root.join("source.txt");
    let alias = root.join("alias.txt");
    std::fs::write(&target, b"first").unwrap();
    symlink(&target, &alias).unwrap();
    let scanner = scanner(&temp);
    let baseline = scanner.scan().unwrap();

    std::fs::write(&target, b"second").unwrap();
    let next = scanner
        .scan_incremental(&baseline, std::slice::from_ref(&target))
        .unwrap()
        .unwrap();
    let expected = Some(distill_core::id::ContentHash(
        *blake3::hash(b"second").as_bytes(),
    ));
    assert_eq!(
        next.file_rows()
            .find(|file| file.normalized_path == "source.txt")
            .unwrap()
            .content_hash,
        expected
    );
    assert_eq!(
        next.file_rows()
            .find(|file| file.normalized_path == "alias.txt")
            .unwrap()
            .content_hash,
        expected
    );
}

#[cfg(unix)]
#[test]
fn incremental_event_preserves_the_native_decomposed_filename() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir_all(&root).unwrap();
    let decomposed = root.join("cafe\u{301}.txt");
    std::fs::write(&decomposed, b"before").unwrap();
    let scanner = scanner(&temp);
    let baseline = scanner.scan().unwrap();

    std::fs::write(&decomposed, b"after").unwrap();
    let updated = scanner
        .scan_incremental(&baseline, std::slice::from_ref(&decomposed))
        .unwrap()
        .unwrap();

    let normalized = "caf\u{e9}.txt";
    let before = baseline
        .file_rows()
        .find(|file| file.normalized_path == normalized)
        .unwrap();
    let after = updated
        .file_rows()
        .find(|file| file.normalized_path == normalized)
        .unwrap();
    assert_ne!(after.content_hash, before.content_hash);
    assert_eq!(updated.files.len(), baseline.files.len());
}

#[cfg(target_os = "linux")]
#[test]
fn distinct_native_names_that_normalize_together_are_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("caf\u{e9}.txt"), b"precomposed").unwrap();
    std::fs::write(root.join("cafe\u{301}.txt"), b"decomposed").unwrap();
    let scanner = scanner(&temp);

    assert!(matches!(
        scanner.scan(),
        Err(ScanError::SameRootNormalizedPathCollision {
            normalized_path,
            claims,
            ..
        }) if normalized_path == "caf\u{e9}.txt" && claims.len() == 2
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn incremental_native_spelling_rename_replaces_the_old_claim() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir_all(&root).unwrap();
    let decomposed = root.join("cafe\u{301}.txt");
    let precomposed = root.join("caf\u{e9}.txt");
    std::fs::write(&decomposed, b"contents").unwrap();
    let scanner = scanner(&temp);
    let baseline = scanner.scan().unwrap();

    std::fs::rename(&decomposed, &precomposed).unwrap();
    let updated = scanner
        .scan_incremental(&baseline, &[decomposed, precomposed])
        .unwrap()
        .unwrap();

    let rows = updated.file_rows().collect::<Vec<_>>();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].normalized_path, "caf\u{e9}.txt");
    assert_eq!(
        *rows[0].raw_relative_path(),
        PlatformPathBytes::Unix("caf\u{e9}.txt".as_bytes().to_vec())
    );
}

#[cfg(target_os = "linux")]
#[test]
fn incremental_new_normalized_claim_reopens_the_baseline_spelling() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir_all(&root).unwrap();
    let decomposed = root.join("cafe\u{301}.txt");
    let precomposed = root.join("caf\u{e9}.txt");
    std::fs::write(&decomposed, b"decomposed").unwrap();
    let scanner = scanner(&temp);
    let baseline = scanner.scan().unwrap();

    std::fs::write(&precomposed, b"precomposed").unwrap();
    assert!(matches!(
        scanner.scan_incremental(&baseline, &[precomposed]),
        Err(ScanError::SameRootNormalizedPathCollision { claims, .. })
            if claims.len() == 2
    ));
}

#[cfg(unix)]
#[test]
fn canonical_error_path_maps_to_exact_subject_under_symlinked_root() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let real = temp.path().join("real-assets");
    let configured = temp.path().join("assets");
    std::fs::create_dir_all(real.join("nested")).unwrap();
    symlink(&real, &configured).unwrap();
    let scanner = RootedScanner::new([AssetRoot::new(
        "main",
        &configured,
        real.join(".distill-displaced"),
    )])
    .unwrap();
    let canonical = std::fs::canonicalize(&real).unwrap();

    assert_eq!(
        scanner.scan_subject(&canonical.join("nested/unreadable")),
        Some(ScanSubject::Subtree {
            root_name: "main".into(),
            raw_relative_path: PlatformPathBytes::Unix(b"nested/unreadable".to_vec()),
        })
    );
}

#[cfg(target_os = "linux")]
#[test]
fn invalid_native_filename_is_a_typed_physical_path_defect() {
    use std::os::unix::ffi::OsStringExt;

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    std::fs::create_dir_all(&root).unwrap();
    let invalid = root.join(std::ffi::OsString::from_vec(vec![b'b', 0xff]));
    std::fs::write(invalid, b"invalid name").unwrap();
    let scanner = scanner(&temp);

    assert!(matches!(
        scanner.scan(),
        Err(ScanError::InvalidPhysicalPath {
            failure: distill_store::state::PhysicalPathFailureCode::InvalidUnixUtf8,
            ..
        })
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn full_scan_aggregates_independent_root_defects() {
    use std::os::unix::ffi::OsStringExt;

    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    std::fs::create_dir_all(&first).unwrap();
    std::fs::create_dir_all(&second).unwrap();
    std::fs::write(
        first.join(std::ffi::OsString::from_vec(vec![0xfe])),
        b"first",
    )
    .unwrap();
    std::fs::write(
        second.join(std::ffi::OsString::from_vec(vec![0xff])),
        b"second",
    )
    .unwrap();
    let scanner = RootedScanner::new([
        AssetRoot::new("first", &first, first.join(".q")),
        AssetRoot::new("second", &second, second.join(".q")),
    ])
    .unwrap();

    assert!(matches!(
        scanner.scan(),
        Err(ScanError::Multiple(errors)) if errors.len() == 2
    ));
}

#[cfg(unix)]
#[test]
fn ancestor_symlink_cycle_is_excluded_with_a_diagnostic() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("assets");
    let nested = root.join("nested");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(nested.join("kept.txt"), b"kept").unwrap();
    symlink(&root, nested.join("back-to-root")).unwrap();
    let scanner = scanner(&temp);

    let scan = scanner.scan().unwrap();
    assert!(scan
        .file_rows()
        .any(|file| file.normalized_path == "nested/kept.txt"));
    assert!(scan.diagnostic_rows().any(|diagnostic| matches!(
        diagnostic,
        ScanDiagnostic::DirectoryCycle {
            normalized_path,
            path_chain,
            ..
        } if normalized_path == "nested/back-to-root" && path_chain.len() >= 3
    )));
}
