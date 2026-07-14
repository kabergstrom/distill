use std::collections::BTreeSet;
use std::sync::Arc;

use distill_core::attestation::{CompiledTypeRow, CompiledTypeTable, RegistryExtrasV1};
use distill_core::id::{AssetUuid, LogicalHash, TypeUuid};
use distill_loader::{IoBasis, IoEvent, LoaderIO, ReqId, ResolveResult, RpcIo};
use distill_rpc::{
    ArtifactPayload, AssetDeltaState, AssetMutation, Commit, ConnectOutcome, ConnectRequest,
    GameModuleEpoch, LoadPolicyEntry, PathMutation, ServedClosureRow, Server, StoreInstanceId,
    StoredResolve, TargetDefinition, TargetDefinitionHash,
};
use distill_schema::bootstrap_gen_v1::consumer_bootstrap_authority_v1;
use distill_wire::artifact::{content_hash, parse_artifact, write_artifact, ArtifactHeader};
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::wire::WireNode;

const TARGET_HASH: [u8; 32] = [7; 32];

#[test]
fn rpc_io_resolves_fetches_paths_and_live_deltas_under_authenticated_bases() {
    let asset = AssetUuid([1; 16]);
    let type_uuid = TypeUuid([2; 16]);
    let logical_hash = LogicalHash([3; 32]);
    let runtime_row = CompiledTypeRow::new(
        type_uuid,
        logical_hash,
        [4; 32],
        false,
        RegistryExtrasV1::default(),
    )
    .unwrap();
    let mut rows = consumer_bootstrap_authority_v1().unwrap().rows().to_vec();
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
    let target = TargetDefinition::canonical(
        "dev",
        TargetDefinitionHash(TARGET_HASH),
        compiled.rows.clone(),
        policy.clone(),
    )
    .unwrap();
    let server = Server::new(StoreInstanceId([5; 16]), vec![target]).unwrap();

    let wire = WireNode::Unit { offset: 0 };
    let layout_hash = dswl_hash(&wire).unwrap();
    server
        .install_wire_tree(layout_hash, Arc::from(dswl_bytes(&wire).unwrap()))
        .unwrap();
    let complete = write_artifact(
        &ArtifactHeader {
            asset_uuid: asset,
            authored_type: type_uuid,
            terminal_type: type_uuid,
            encoded_type: type_uuid,
            logical_hash,
            layout_hash,
        },
        &[],
        &[],
        &[],
        &[(Vec::new(), &[][..])],
    )
    .unwrap();
    let parsed = parse_artifact(&complete).unwrap();
    let structural_len = complete.len() - parsed.blob_section.len();
    let hash = content_hash(&complete);
    server
        .install_artifact(
            hash,
            ArtifactPayload {
                structural: Arc::from(complete[..structural_len].to_vec()),
                blobs: vec![Arc::from(&b""[..])],
                encoded_type: type_uuid,
                terminal_type: type_uuid,
                closure_rows: vec![ServedClosureRow {
                    asset,
                    content_hash: hash,
                    authored_type: type_uuid,
                    encoded_type: type_uuid,
                    terminal_type: type_uuid,
                    load_edges: Vec::new(),
                }],
            },
        )
        .unwrap();
    server
        .commit(Commit {
            assets: vec![AssetMutation::Set {
                uuid: asset,
                resolution: StoredResolve::Built { content_hash: hash },
                delta: AssetDeltaState::Changed,
            }],
            paths: vec![PathMutation::Set {
                path: "assets/a.bundle".into(),
                candidates: BTreeSet::from([asset]),
            }],
            ..Commit::default()
        })
        .unwrap();

    let request = ConnectRequest::canonical(
        GameModuleEpoch(1),
        "dev",
        TargetDefinitionHash(TARGET_HASH),
        compiled.rows,
        policy,
    )
    .unwrap();
    let hub = match server.root().connect(request) {
        ConnectOutcome::Connected(connected) => connected.hub,
        other => panic!("connect failed: {other:?}"),
    };
    let mut io = RpcIo::new(hub).unwrap();
    let basis = io.begin_sweep();
    assert!(matches!(
        &basis,
        IoBasis::Rpc {
            policy_generation: 0,
            target_generation: 0,
            attestation_generation: 0,
            ..
        }
    ));

    io.resolve(ReqId(1), asset, &basis);
    io.fetch(ReqId(2), hash, &basis);
    io.resolve_path(ReqId(3), "assets/a.bundle", &basis);
    let events = io.poll();
    assert!(matches!(
        &events[0],
        IoEvent::Resolved {
            req: ReqId(1),
            result: ResolveResult::Built { content_hash },
            basis: event_basis,
            ..
        } if *content_hash == hash && event_basis == &basis
    ));
    assert!(matches!(
        &events[1],
        IoEvent::Fetched {
            req: ReqId(2),
            content_hash,
            artifact,
            basis: event_basis,
        } if *content_hash == hash
            && artifact.blobs.len() == 1
            && artifact.blobs[0].is_empty()
            && event_basis == &basis
    ));
    assert!(matches!(
        &events[2],
        IoEvent::PathResolved {
            req: ReqId(3),
            result: distill_loader::PathResolveResult::Resolved(found),
            basis: event_basis,
            ..
        } if *found == asset && event_basis == &basis
    ));

    io.subscribe(asset);
    assert!(io.poll().is_empty());
    server
        .commit(Commit {
            assets: vec![AssetMutation::Set {
                uuid: asset,
                resolution: StoredResolve::Built { content_hash: hash },
                delta: AssetDeltaState::Changed,
            }],
            ..Commit::default()
        })
        .unwrap();
    assert!(matches!(
        io.poll().as_slice(),
        [IoEvent::Delta { assets, .. }]
            if assets == &vec![(asset, distill_loader::AssetDeltaState::Changed)]
    ));
}
