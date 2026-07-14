use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use distill_build::query::AssetQuery as BuildAssetQuery;
use distill_build::trace::PackDefinitionControlValue;
use distill_bundle::PathComponent;
use distill_core::attestation::{CompiledTypeRow, CompiledTypeTable, RegistryExtrasV1};
use distill_core::id::{AssetUuid, BundleUuid, LogicalHash, TypeUuid};
use distill_pack::builder::{build_pack, PackBuildError, PackBuildTarget};
use distill_pack::{PackTarget, PackfileIO, RuntimeAttestation};
use distill_rpc::{
    ArtifactPayload, AssetDeltaState, AssetMutation, AuthoringEntry, AuthoringEntryRole,
    AuthoringMutation, AuthoringValue, Commit, ConnectOutcome, ConnectRequest, GameModuleEpoch,
    LoadPolicyEntry, PathMutation, ServedClosureRow, ServedLoadEdge, Server, StoreInstanceId,
    StoredResolve, TargetDefinition, TargetDefinitionHash,
};
use distill_schema::bootstrap_gen_v1::consumer_bootstrap_authority_v1;
use distill_schema::ngp_schema::{node_hash, SchemaNode};
use distill_wire::artifact::{content_hash, write_artifact, ArtifactHeader};
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::wire::WireNode;

const TARGET_HASH: [u8; 32] = [7; 32];

struct Fixture {
    compiled: CompiledTypeTable,
    hub: distill_rpc::Hub,
    snapshot: distill_rpc::Snapshot,
    root: AssetUuid,
    child: AssetUuid,
    target: PackTarget,
}

fn fixture() -> Fixture {
    fixture_with_build_only(false)
}

fn fixture_with_build_only(build_only: bool) -> Fixture {
    let runtime_type = TypeUuid([21; 16]);
    let logical_hash = LogicalHash([31; 32]);
    let runtime_row = CompiledTypeRow::new(
        runtime_type,
        logical_hash,
        [41; 32],
        build_only,
        RegistryExtrasV1::default(),
    )
    .unwrap();
    let bootstrap = consumer_bootstrap_authority_v1().unwrap();
    let mut rows = bootstrap.rows().to_vec();
    rows.push(runtime_row);
    let compiled = CompiledTypeTable::canonical(rows).unwrap();
    let policy = compiled
        .rows
        .iter()
        .map(|row| LoadPolicyEntry {
            type_uuid: row.type_uuid,
            build_only: row.build_only,
        })
        .collect::<Vec<_>>();
    let target_definition = TargetDefinition::canonical(
        "dev",
        TargetDefinitionHash(TARGET_HASH),
        compiled.rows.clone(),
        policy.clone(),
    )
    .unwrap();
    let server = Server::new(StoreInstanceId([9; 16]), vec![target_definition]).unwrap();

    let root = AssetUuid([1; 16]);
    let child = AssetUuid([2; 16]);
    let wire = WireNode::Unit { offset: 0 };
    let layout_hash = dswl_hash(&wire).unwrap();
    server
        .install_wire_tree(layout_hash, Arc::from(dswl_bytes(&wire).unwrap()))
        .unwrap();

    let child_row = artifact_row(
        child,
        runtime_type,
        logical_hash,
        layout_hash,
        Vec::new(),
        Some(Arc::from(&b""[..])),
    );
    let root_row = artifact_row(
        root,
        runtime_type,
        logical_hash,
        layout_hash,
        vec![ServedLoadEdge {
            asset: child,
            expected_terminal: runtime_type,
        }],
        None,
    );
    let root_closure = vec![root_row.1.clone(), child_row.1.clone()];
    server
        .install_artifact(
            child_row.0,
            ArtifactPayload {
                closure_rows: vec![child_row.1.clone()],
                ..child_row.2
            },
        )
        .unwrap();
    server
        .install_artifact(
            root_row.0,
            ArtifactPayload {
                closure_rows: root_closure,
                ..root_row.2
            },
        )
        .unwrap();

    server
        .commit(Commit {
            authoring: vec![
                AuthoringMutation::Set(entry(root, runtime_type, "assets/root.bundle")),
                AuthoringMutation::Set(entry(child, runtime_type, "assets/child.bundle")),
            ],
            assets: vec![
                AssetMutation::Set {
                    uuid: root,
                    resolution: StoredResolve::Built {
                        content_hash: root_row.0,
                    },
                    delta: AssetDeltaState::Changed,
                },
                AssetMutation::Set {
                    uuid: child,
                    resolution: StoredResolve::Built {
                        content_hash: child_row.0,
                    },
                    delta: AssetDeltaState::Changed,
                },
            ],
            paths: vec![
                PathMutation::Set {
                    path: "assets/root.bundle".into(),
                    candidates: BTreeSet::from([root]),
                },
                PathMutation::Set {
                    path: "assets/child.bundle".into(),
                    candidates: BTreeSet::from([child]),
                },
            ],
            ..Commit::default()
        })
        .unwrap();

    let request = ConnectRequest::canonical(
        GameModuleEpoch(1),
        "dev",
        TargetDefinitionHash(TARGET_HASH),
        compiled.rows.clone(),
        policy,
    )
    .unwrap();
    let hub = match server.root().connect(request) {
        ConnectOutcome::Connected(connected) => connected.hub,
        other => panic!("connection failed: {other:?}"),
    };
    let snapshot = hub.snapshot().success().unwrap();
    Fixture {
        compiled,
        hub,
        snapshot,
        root,
        child,
        target: PackTarget {
            os: 1,
            arch: 2,
            apis: vec![3],
            options: BTreeMap::new(),
        },
    }
}

fn artifact_row(
    asset: AssetUuid,
    runtime_type: TypeUuid,
    logical_hash: LogicalHash,
    layout_hash: distill_core::id::LayoutHash,
    load_edges: Vec<ServedLoadEdge>,
    blob: Option<Arc<[u8]>>,
) -> (
    distill_core::id::ContentHash,
    ServedClosureRow,
    ArtifactPayload,
) {
    let load_deps = load_edges.iter().map(|edge| edge.asset).collect::<Vec<_>>();
    let blob_inputs = blob
        .iter()
        .map(|bytes| (vec![PathComponent::Field("blob".into())], bytes.as_ref()))
        .collect::<Vec<_>>();
    let complete = write_artifact(
        &ArtifactHeader {
            asset_uuid: asset,
            authored_type: runtime_type,
            terminal_type: runtime_type,
            encoded_type: runtime_type,
            logical_hash,
            layout_hash,
        },
        &load_deps,
        &[],
        &[],
        &blob_inputs,
    )
    .unwrap();
    let parsed = distill_wire::artifact::parse_artifact(&complete).unwrap();
    let structural_len = complete.len() - parsed.blob_section.len();
    let hash = content_hash(&complete);
    let row = ServedClosureRow {
        asset,
        content_hash: hash,
        authored_type: runtime_type,
        encoded_type: runtime_type,
        terminal_type: runtime_type,
        load_edges,
    };
    (
        hash,
        row,
        ArtifactPayload {
            structural: Arc::from(complete[..structural_len].to_vec()),
            blobs: blob.into_iter().collect(),
            encoded_type: runtime_type,
            terminal_type: runtime_type,
            closure_rows: Vec::new(),
        },
    )
}

fn entry(uuid: AssetUuid, runtime_type: TypeUuid, path: &str) -> AuthoringEntry {
    let schema_hash = node_hash(&SchemaNode::Blob).unwrap();
    AuthoringEntry {
        uuid,
        bundle: BundleUuid(uuid.0),
        local_id: "main".into(),
        normalized_path: path.into(),
        type_uuid: runtime_type,
        terminal_type: runtime_type,
        schema_hash,
        logical_schema: Arc::from(&b"\"blob\""[..]),
        role: AuthoringEntryRole::Runtime,
        tags: BTreeMap::new(),
        value: AuthoringValue {
            canonical_value: Arc::from(&b"{\"$distill_blob\":0}"[..]),
            blobs: vec![Arc::from(&b"source"[..])],
        },
    }
}

fn definition(root: AssetUuid) -> PackDefinitionControlValue {
    PackDefinitionControlValue {
        roots: vec![BuildAssetQuery {
            uuid: Some(root),
            ..BuildAssetQuery::default()
        }],
        target: "dev".into(),
        zstd_level: 3,
        include_path_table: true,
    }
}

#[test]
fn build_pack_pulls_the_typed_closure_and_emits_mountable_files() {
    let fixture = fixture();
    let output = build_pack(
        &definition(fixture.root),
        &PackBuildTarget {
            name: "dev".into(),
            manifest: fixture.target.clone(),
            definition_hash: TARGET_HASH,
        },
        &fixture.compiled,
        "zstd-test",
        &fixture.snapshot,
        &fixture.hub,
    )
    .unwrap();

    assert_eq!(output.manifest.assets.len(), 2);
    assert_eq!(output.manifest.assets[0].asset_uuid, fixture.root);
    assert_eq!(
        output.manifest.assets[0].load_deps[0].asset_uuid,
        fixture.child
    );
    assert_eq!(output.manifest.paths.as_ref().unwrap().len(), 2);
    assert_eq!(
        output.archive_file_hash,
        *blake3::hash(&output.archive_bytes).as_bytes()
    );

    PackfileIO::mount(
        &output.manifest_bytes,
        vec![output.archive_bytes],
        &RuntimeAttestation {
            target: fixture.target,
            target_def_hash: TARGET_HASH,
            compiled_types: fixture.compiled,
            bootstrap_authority: consumer_bootstrap_authority_v1().unwrap(),
        },
    )
    .unwrap();
}

#[test]
fn build_pack_rejects_an_empty_root_selection() {
    let fixture = fixture();
    assert!(matches!(
        build_pack(
            &definition(AssetUuid([99; 16])),
            &PackBuildTarget {
                name: "dev".into(),
                manifest: fixture.target,
                definition_hash: TARGET_HASH,
            },
            &fixture.compiled,
            "zstd-test",
            &fixture.snapshot,
            &fixture.hub,
        ),
        Err(PackBuildError::EmptyRoot { index: 0 })
    ));
}

#[test]
fn build_pack_rejects_runtime_build_only_types() {
    let fixture = fixture_with_build_only(true);
    assert!(matches!(
        build_pack(
            &definition(fixture.root),
            &PackBuildTarget {
                name: "dev".into(),
                manifest: fixture.target,
                definition_hash: TARGET_HASH,
            },
            &fixture.compiled,
            "zstd-test",
            &fixture.snapshot,
            &fixture.hub,
        ),
        Err(PackBuildError::BuildOnlyType(type_uuid)) if type_uuid == TypeUuid([21; 16])
    ));
}

#[test]
fn build_pack_closes_roots_before_rpc_evaluation() {
    let fixture = fixture();
    let mut definition = definition(fixture.root);
    definition.roots[0].authoring_only = Some(true);
    assert!(matches!(
        build_pack(
            &definition,
            &PackBuildTarget {
                name: "dev".into(),
                manifest: fixture.target,
                definition_hash: TARGET_HASH,
            },
            &fixture.compiled,
            "zstd-test",
            &fixture.snapshot,
            &fixture.hub,
        ),
        Err(PackBuildError::InvalidRoot {
            index: 0,
            error: distill_build::query::IntakeError::AuthoringOnlyRestricted,
        })
    ));
}
