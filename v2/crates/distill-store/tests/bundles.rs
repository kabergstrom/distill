//! §13 asset-namespace tables: bundles (physical key + poison rows),
//! assets + search tags, the path/primary resolution index, dependency
//! records with selector indexes, the schema cache — and the two poison
//! shapes: bundle-scoped rows and the version-global poison.

use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use distill_store::bundles::{
    AssetRecord, BundleMeta, DepKind, DirectoryOrigin, DirectoryRuleId, NamespaceSkeleton,
    SkeletonEntry,
};
use distill_store::files::RootId;
use distill_store::state::{
    ReadableBundleSource, SkeletonFailureCode, VersionPoison, VersionPoisonV1,
};
use distill_store::{Store, StoreConfig, StoreError};

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path().join(".distill"))).unwrap();
    (dir, s)
}

fn version_poison(message: &str) -> VersionPoison {
    VersionPoison::new(
        VersionPoisonV1::IncompleteSkeleton {
            source: ReadableBundleSource {
                root_name: "main".into(),
                normalized_path: "broken.bundle".into(),
                file_hash: BundleFileHash([9; 32]),
            },
            failure: SkeletonFailureCode::IncompleteAssetIdentity,
        },
        message,
    )
    .unwrap()
}

fn bundle_meta(root: RootId, n: u8) -> BundleMeta {
    BundleMeta {
        bundle: BundleUuid([n; 16]),
        root,
        path: format!("tex/{n}.bundle"),
        format_version: 1,
        content_hash: ContentHash([n; 32]),
        origin: None,
    }
}

fn asset_record(asset_n: u8, bundle_n: u8, tags: &[&str]) -> AssetRecord {
    AssetRecord {
        asset: AssetUuid([asset_n; 16]),
        bundle: BundleUuid([bundle_n; 16]),
        local_id: format!("entry-{asset_n}"),
        type_uuid: TypeUuid([9u8; 16]),
        logical_hash: LogicalHash([8u8; 32]),
        authoring_only: false,
        tags: tags.iter().map(|s| s.to_string()).collect(),
    }
}

fn seed(store: &mut Store) -> RootId {
    let (root, _) = store
        .input_transaction(|txn| {
            let root = txn.intern_root("main")?;
            txn.upsert_bundle(&bundle_meta(root, 1))?;
            txn.upsert_asset(&asset_record(10, 1, &["hero", "texture"]))?;
            txn.set_path_entry("tex/1.bundle", root, AssetUuid([10u8; 16]))?;
            Ok(root)
        })
        .unwrap();
    root
}

// ---- bundles + assets roundtrip ----

#[test]
fn bundle_and_asset_rows_roundtrip() {
    let (_d, mut store) = store();
    let root = seed(&mut store);

    let bundle = store.bundle(BundleUuid([1u8; 16])).unwrap().unwrap();
    assert_eq!(bundle.root, root);
    assert_eq!(bundle.path, "tex/1.bundle");
    assert_eq!(bundle.content_hash, ContentHash([1u8; 32]));

    let entry = store.entry(AssetUuid([10u8; 16])).unwrap().unwrap();
    assert_eq!(entry.bundle, BundleUuid([1u8; 16]));
    assert_eq!(entry.local_id, "entry-10");
    assert_eq!(entry.type_uuid, TypeUuid([9u8; 16]));
    assert_eq!(entry.logical_hash, LogicalHash([8u8; 32]));
    let mut tags = entry.tags.clone();
    tags.sort();
    assert_eq!(tags, ["hero", "texture"]);

    assert!(store.entry(AssetUuid([99u8; 16])).unwrap().is_none());
    assert!(store.bundle(BundleUuid([99u8; 16])).unwrap().is_none());
}

// ---- directory-import ownership (§2, §8, §13) ----

#[test]
fn directory_origin_rides_in_the_bundle_row() {
    // §13: directory-import ownership derives at scan from generated
    // bundles' DirectoryOrigin records (§8), never from precious rows —
    // the row here is the scan-derived index of the record riding in
    // the bundle's `$record` entry. Absence means explicit import.
    let (_d, mut store) = store();
    let root = seed(&mut store);
    let origin = DirectoryOrigin {
        rules_bundle: BundleUuid([50u8; 16]),
        rule: DirectoryRuleId([2u8; 16]),
        // §8: persisted records carry the normalized root NAME, never a
        // numeric ordinal.
        group_root: "main".to_owned(),
        group_path: "textures/hero.png".to_owned(),
    };
    let mut generated = bundle_meta(root, 3);
    generated.origin = Some(origin.clone());
    store
        .input_transaction(|txn| txn.upsert_bundle(&generated))
        .unwrap();

    let row = store.bundle(BundleUuid([3u8; 16])).unwrap().unwrap();
    assert_eq!(row.origin, Some(origin));
    // The explicit import from seed() carries no origin.
    let explicit = store.bundle(BundleUuid([1u8; 16])).unwrap().unwrap();
    assert_eq!(explicit.origin, None);
}

#[test]
fn ownership_reconstruction_queries_by_rules_bundle() {
    // §2/§8: after daemon-state loss, orphan tracking re-derives
    // ownership from the DirectoryOrigin records — the daemon can always
    // distinguish generated output from an equivalent explicit import.
    let (_d, mut store) = store();
    let root = seed(&mut store);
    let rules = BundleUuid([50u8; 16]);
    store
        .input_transaction(|txn| {
            for n in [3u8, 4u8] {
                let mut meta = bundle_meta(root, n);
                meta.origin = Some(DirectoryOrigin {
                    rules_bundle: rules,
                    rule: DirectoryRuleId([n; 16]),
                    group_root: "main".to_owned(),
                    group_path: format!("textures/{n}.png"),
                });
                txn.upsert_bundle(&meta)?;
            }
            // An explicit import in the same tree.
            txn.upsert_bundle(&bundle_meta(root, 5))?;
            Ok(())
        })
        .unwrap();
    let mut owned = store.bundles_owned_by(rules).unwrap();
    owned.sort();
    assert_eq!(owned, [BundleUuid([3u8; 16]), BundleUuid([4u8; 16])]);
    assert!(store
        .bundles_owned_by(BundleUuid([9u8; 16]))
        .unwrap()
        .is_empty());
}

#[test]
fn directory_rule_identity_is_uuid_style_and_not_a_mutable_index() {
    let (_d, mut store) = store();
    let root = seed(&mut store);
    let rule = DirectoryRuleId([0xabu8; 16]);
    let mut generated = bundle_meta(root, 6);
    generated.origin = Some(DirectoryOrigin {
        rules_bundle: BundleUuid([50u8; 16]),
        rule,
        group_root: "main".to_owned(),
        group_path: "textures/hero.png".to_owned(),
    });
    store
        .input_transaction(|txn| txn.upsert_bundle(&generated))
        .unwrap();

    let restored = store
        .bundle(generated.bundle)
        .unwrap()
        .unwrap()
        .origin
        .unwrap();
    assert_eq!(restored.rule, rule);
    assert_eq!(restored.rule.0.len(), 16);
    assert_eq!(
        restored.rule.to_string(),
        "abababab-abab-abab-abab-abababababab",
        "stable rule ids use UUID text shape, never list positions"
    );
}

#[test]
fn upsert_asset_replaces_tags_wholesale() {
    let (_d, mut store) = store();
    seed(&mut store);
    store
        .input_transaction(|txn| txn.upsert_asset(&asset_record(10, 1, &["updated"])))
        .unwrap();
    let entry = store.entry(AssetUuid([10u8; 16])).unwrap().unwrap();
    assert_eq!(entry.tags, ["updated"]);
}

#[test]
fn authoring_only_entries_are_visible_to_tooling_but_ineligible_at_runtime() {
    let (_d, mut store) = store();
    let root = seed(&mut store);
    let mut control = asset_record(11, 1, &["control"]);
    control.authoring_only = true;
    store
        .input_transaction(|txn| txn.upsert_asset(&control))
        .unwrap();

    let metadata = store.entry(control.asset).unwrap().unwrap();
    assert!(metadata.authoring_only);
    assert!(matches!(
        store.runtime_entry(control.asset).unwrap_err(),
        StoreError::RoleIneligible { asset } if asset == control.asset
    ));
    assert!(store.assets_by_tag("control").unwrap().is_empty());
    assert_eq!(
        store.authoring_assets_by_tag("control").unwrap(),
        [control.asset]
    );

    let err = store
        .input_transaction(|txn| txn.set_path_entry("tex/control.bundle", root, control.asset))
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::RoleIneligible { asset } if asset == control.asset
    ));
    assert_eq!(store.resolve_path("tex/control.bundle").unwrap(), None);

    let runtime = store.runtime_entry(AssetUuid([10u8; 16])).unwrap().unwrap();
    assert!(!runtime.authoring_only);
}

#[test]
fn remove_bundle_cascades_to_assets_and_tags() {
    let (_d, mut store) = store();
    seed(&mut store);
    store
        .input_transaction(|txn| {
            assert!(txn.remove_bundle(BundleUuid([1u8; 16]))?);
            Ok(())
        })
        .unwrap();
    assert!(store.bundle(BundleUuid([1u8; 16])).unwrap().is_none());
    assert!(store.entry(AssetUuid([10u8; 16])).unwrap().is_none());
    assert!(store.assets_by_tag("hero").unwrap().is_empty());
}

// ---- path resolution (§13 MetadataSnapshot::resolve_path semantics) ----

#[test]
fn resolve_path_misses_are_first_class() {
    let (_d, mut store) = store();
    seed(&mut store);
    assert_eq!(store.resolve_path("absent/path.bundle").unwrap(), None);
    assert_eq!(
        store.resolve_path("tex/1.bundle").unwrap(),
        Some(AssetUuid([10u8; 16]))
    );
}

#[test]
fn resolve_path_ambiguity_is_an_error_never_a_tiebreak() {
    // §13: "a path resolvable in more than one asset root is Err (§18),
    // never a tiebreak".
    let (_d, mut store) = store();
    seed(&mut store);
    store
        .input_transaction(|txn| {
            let engine = txn.intern_root("engine")?;
            txn.set_path_entry("tex/1.bundle", engine, AssetUuid([20u8; 16]))?;
            Ok(())
        })
        .unwrap();
    let err = store.resolve_path("tex/1.bundle").unwrap_err();
    match err {
        StoreError::AmbiguousPath { path, roots } => {
            assert_eq!(path, "tex/1.bundle");
            let mut roots = roots;
            roots.sort();
            assert_eq!(roots, ["engine", "main"]);
        }
        other => panic!("expected AmbiguousPath, got {other:?}"),
    }
}

// ---- deps + selector indexes ----

#[test]
fn dep_records_roundtrip_by_selector() {
    let (_d, mut store) = store();
    seed(&mut store);
    store
        .input_transaction(|txn| {
            txn.record_dep(
                AssetUuid([10u8; 16]),
                DepKind::Resolution,
                "path:tex/2.bundle",
            )?;
            txn.record_dep(AssetUuid([10u8; 16]), DepKind::Query, "tag:hero")?;
            txn.record_dep(AssetUuid([11u8; 16]), DepKind::Query, "tag:hero")?;
            Ok(())
        })
        .unwrap();

    let mut dependents = store.deps_on(DepKind::Query, "tag:hero").unwrap();
    dependents.sort();
    assert_eq!(dependents, [AssetUuid([10u8; 16]), AssetUuid([11u8; 16])]);
    assert_eq!(
        store
            .deps_on(DepKind::Resolution, "path:tex/2.bundle")
            .unwrap(),
        [AssetUuid([10u8; 16])]
    );
    assert!(store
        .deps_on(DepKind::Content, "path:tex/2.bundle")
        .unwrap()
        .is_empty());

    // Re-recording clears wholesale per source.
    store
        .input_transaction(|txn| {
            txn.clear_deps(AssetUuid([10u8; 16]))?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        store.deps_on(DepKind::Query, "tag:hero").unwrap(),
        [AssetUuid([11u8; 16])]
    );
}

// ---- schema cache ----

#[test]
fn schema_cache_roundtrips() {
    let (_d, mut store) = store();
    let hash = LogicalHash([5u8; 32]);
    store
        .input_transaction(|txn| txn.put_schema(hash, "{\"kind\":\"struct\"}"))
        .unwrap();
    assert_eq!(
        store.schema(hash).unwrap().as_deref(),
        Some("{\"kind\":\"struct\"}")
    );
    assert!(store.schema(LogicalHash([6u8; 32])).unwrap().is_none());
}

// ---- bundle-scoped poison rows (§7, §13) ----

fn skeleton(root: RootId, entries: Vec<SkeletonEntry>) -> NamespaceSkeleton {
    NamespaceSkeleton {
        bundle: BundleUuid([1u8; 16]),
        root,
        path: "tex/1.bundle".to_owned(),
        format_version: 1,
        content_hash: ContentHash([0xEE; 32]),
        entries,
    }
}

fn skeleton_entry(asset_n: u8, tags: &[&str]) -> SkeletonEntry {
    SkeletonEntry {
        asset: AssetUuid([asset_n; 16]),
        local_id: format!("entry-{asset_n}"),
        type_uuid: TypeUuid([9u8; 16]),
        authoring_only: false,
        tags: tags.iter().map(|s| s.to_string()).collect(),
    }
}

#[test]
fn poisoning_a_bundle_fails_resolves_against_its_uuids() {
    // §13: resolves against the poisoned UUIDs return a stable Failed
    // naming the parse or index error. Scope comes from the validated
    // skeleton parsed out of the CURRENT malformed bytes.
    let (_d, mut store) = store();
    let root = seed(&mut store);
    store
        .input_transaction(|txn| {
            txn.poison_bundle(
                &skeleton(root, vec![skeleton_entry(10, &["hero", "texture"])]),
                "malformed JSON at byte 12",
            )
        })
        .unwrap();

    let err = store.entry(AssetUuid([10u8; 16])).unwrap_err();
    match err {
        StoreError::BundlePoisoned { bundle, error } => {
            assert_eq!(bundle, BundleUuid([1u8; 16]));
            assert!(error.contains("malformed JSON at byte 12"));
        }
        other => panic!("expected BundlePoisoned, got {other:?}"),
    }

    // Path resolution reaching into the poisoned bundle fails the same way.
    assert!(matches!(
        store.resolve_path("tex/1.bundle").unwrap_err(),
        StoreError::BundlePoisoned { .. }
    ));

    // Queries whose selectors could match the file's entries fail naming
    // the poisoned bundle — the same shape as §10's tag poisoning.
    assert!(matches!(
        store.assets_by_tag("hero").unwrap_err(),
        StoreError::BundlePoisoned { .. }
    ));
    // A selector that cannot match the poisoned file's entries still works.
    assert!(store.assets_by_tag("unrelated-tag").unwrap().is_empty());
}

#[test]
fn poison_scope_is_proved_by_the_current_bytes_not_prior_rows() {
    // §7/§13 (R21): a malformed edit can introduce a new UUID, type,
    // local_id, or tag before the syntax error. The skeleton parsed from
    // the CURRENT bytes carries those new facts, so queries matching
    // them fail conservatively instead of returning Missing or shrunken
    // results — the prior row's facts are replaced, never consulted.
    let (_d, mut store) = store();
    let root = seed(&mut store); // prior version: asset 10, tags hero+texture
    store
        .input_transaction(|txn| {
            txn.poison_bundle(
                &skeleton(
                    root,
                    vec![
                        skeleton_entry(10, &["hero"]),
                        // The malformed edit introduced asset 33 with a
                        // brand-new tag before the syntax error.
                        skeleton_entry(33, &["brand-new-tag"]),
                    ],
                ),
                "unexpected token at byte 512",
            )
        })
        .unwrap();

    // The NEW uuid is addressable and fails, never Missing.
    assert!(matches!(
        store.entry(AssetUuid([33u8; 16])).unwrap_err(),
        StoreError::BundlePoisoned { .. }
    ));
    // A query matching the NEW tag fails naming the poisoned bundle.
    assert!(matches!(
        store.assets_by_tag("brand-new-tag").unwrap_err(),
        StoreError::BundlePoisoned { .. }
    ));
    // A fact only the OLD bytes claimed is gone — replaced, not
    // retained: the selector no longer matches anything.
    assert!(store.assets_by_tag("texture").unwrap().is_empty());
}

#[test]
fn bundle_scoped_poison_needs_no_prior_row() {
    // §13 (R21): scope is decided by the validated skeleton alone —
    // regardless of what prior metadata exists. A file malformed on
    // FIRST sight still poisons bundle-scoped when its current bytes
    // yield the complete namespace skeleton.
    let (_d, mut store) = store();
    let root = seed(&mut store);
    let fresh = NamespaceSkeleton {
        bundle: BundleUuid([77u8; 16]),
        root,
        path: "tex/77.bundle".to_owned(),
        format_version: 1,
        content_hash: ContentHash([0x77; 32]),
        entries: vec![skeleton_entry(78, &["fresh-tag"])],
    };
    store
        .input_transaction(|txn| txn.poison_bundle(&fresh, "truncated container"))
        .unwrap();
    assert!(matches!(
        store.entry(AssetUuid([78u8; 16])).unwrap_err(),
        StoreError::BundlePoisoned { bundle, .. } if bundle == BundleUuid([77u8; 16])
    ));
    assert!(matches!(
        store.assets_by_tag("fresh-tag").unwrap_err(),
        StoreError::BundlePoisoned { .. }
    ));
    // The unrelated seeded bundle still answers.
    assert!(store.entry(AssetUuid([10u8; 16])).unwrap().is_some());
}

#[test]
fn tooling_authoring_query_propagates_matching_bundle_poison() {
    let (_d, mut store) = store();
    let root = seed(&mut store);
    let mut control = skeleton_entry(78, &["control"]);
    control.authoring_only = true;
    let malformed = NamespaceSkeleton {
        bundle: BundleUuid([77u8; 16]),
        root,
        path: "tex/77.bundle".to_owned(),
        format_version: 1,
        content_hash: ContentHash([0x77; 32]),
        entries: vec![control],
    };
    store
        .input_transaction(|txn| txn.poison_bundle(&malformed, "truncated control entry"))
        .unwrap();

    assert!(matches!(
        store.authoring_assets_by_tag("control").unwrap_err(),
        StoreError::BundlePoisoned { bundle, .. } if bundle == BundleUuid([77u8; 16])
    ));
    assert!(store.assets_by_tag("control").unwrap().is_empty());
}

#[test]
fn fixing_the_file_heals_on_the_next_version() {
    let (_d, mut store) = store();
    let root = seed(&mut store);
    store
        .input_transaction(|txn| {
            txn.poison_bundle(
                &skeleton(
                    root,
                    vec![
                        skeleton_entry(10, &["hero"]),
                        skeleton_entry(33, &["stray"]),
                    ],
                ),
                "bad body",
            )
        })
        .unwrap();
    // The next indexed version republishes the bundle wholesale.
    store
        .input_transaction(|txn| {
            txn.upsert_bundle(&bundle_meta(root, 1))?;
            txn.upsert_asset(&asset_record(10, 1, &["hero"]))?;
            Ok(())
        })
        .unwrap();
    assert!(store.entry(AssetUuid([10u8; 16])).unwrap().is_some());
    assert_eq!(
        store.assets_by_tag("hero").unwrap(),
        [AssetUuid([10u8; 16])]
    );
    // The skeleton-only entry died with the poison: healing republishes
    // wholesale, and stale skeleton facts never outlive it.
    assert!(store.entry(AssetUuid([33u8; 16])).unwrap().is_none());
    assert!(store.assets_by_tag("stray").unwrap().is_empty());
}

// ---- version-global poison (§7, §13) ----

#[test]
fn version_poison_is_global_and_uniform() {
    // §13: every namespace-facing operation fails with this same error —
    // never one surviving duplicate, never last-good metadata from a
    // projection that happens not to touch the colliding rows.
    let (_d, mut store) = store();
    seed(&mut store);
    store
        .input_transaction(|txn| {
            let poison = version_poison("uuid 0a… authored twice: tex/1.bundle, tex/9.bundle");
            txn.set_version_poison(Some(&poison))
        })
        .unwrap();

    assert!(store.version_poison().unwrap().is_some());
    for err in [
        store.entry(AssetUuid([10u8; 16])).unwrap_err(),
        store.resolve_path("tex/1.bundle").unwrap_err(),
        store.assets_by_tag("hero").unwrap_err(),
        store.assets_by_tag("no-such-tag").unwrap_err(),
    ] {
        match err {
            StoreError::Poisoned { error } => assert!(error.contains("authored twice")),
            other => panic!("expected the uniform version poison, got {other:?}"),
        }
    }

    // Consumers that merely need the version number still can (§13).
    let _ = store.input_version();
    let _ = store.stamp();

    // The next version heals.
    store
        .input_transaction(|txn| txn.set_version_poison(None))
        .unwrap();
    assert!(store.version_poison().unwrap().is_none());
    assert!(store.entry(AssetUuid([10u8; 16])).unwrap().is_some());
}
