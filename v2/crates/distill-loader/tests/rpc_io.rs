use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use distill_core::id::{AssetUuid, LogicalHash, TypeUuid};
use distill_loader::{
    IoBasis, IoEvent, LoaderIO, ReqId, ResolveResult, RpcIo, RpcIoConfig, RuntimeTarget,
};
use distill_rpc::capnp_transport::StagedListener;
use distill_rpc::{
    ArtifactPayload, AssetDeltaState, AssetMutation, Commit, ConnectRequest, PathMutation, Server,
    StoreInstanceId, StoredResolve, TargetDefinition, TargetDefinitionHash,
};
use distill_wire::artifact::{content_hash, parse_artifact, write_artifact, ArtifactHeader};
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::wire::WireNode;

const TARGET_HASH: [u8; 32] = [7; 32];

struct Fixture {
    server: Server,
    request: ConnectRequest,
    asset: AssetUuid,
    hash: distill_core::id::ContentHash,
}

fn fixture() -> Fixture {
    let asset = AssetUuid([1; 16]);
    let type_uuid = TypeUuid([2; 16]);
    let logical_hash = LogicalHash([3; 32]);
    let target = TargetDefinition::new("dev", TargetDefinitionHash(TARGET_HASH));
    let server = Server::new(StoreInstanceId([5; 16]), vec![target]).unwrap();

    let wire = WireNode::Unit { offset: 0 };
    let layout_hash = dswl_hash(&wire).unwrap();
    server
        .install_wire_tree(layout_hash, Arc::from(dswl_bytes(&wire).unwrap()))
        .unwrap();
    let blob = vec![0x5a; 64];
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
        &[(Vec::new(), blob.as_slice())],
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
                blobs: vec![Arc::from(blob)],
                load_edges: Vec::new(),
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

    let request = ConnectRequest::new("dev", TargetDefinitionHash(TARGET_HASH));
    Fixture {
        server,
        request,
        asset,
        hash,
    }
}

#[test]
fn rpc_io_drives_the_same_loader_boundary_on_its_own_capnp_thread() {
    let Fixture {
        server,
        request,
        asset,
        hash,
    } = fixture();
    let (address_tx, address_rx) = std::sync::mpsc::sync_channel(1);
    let root = server.root();
    let server_thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let listener = StagedListener::bind(root, "127.0.0.1:0").await.unwrap();
            address_tx.send(listener.local_addr().unwrap()).unwrap();
            let first = listener.accept_one().await.unwrap();
            let second = listener.accept_one().await.unwrap();
            first.await.unwrap().unwrap();
            second.await.unwrap().unwrap();
        });
    });
    let address = address_rx.recv().unwrap();
    let target = RuntimeTarget {
        epoch: distill_loader::GameModuleEpoch(1),
        target_definition_hash: request.target_definition_hash.0,
    };
    let spool = tempfile::tempdir().unwrap();
    let mut io = RpcIo::connect_with_config(
        address,
        request,
        RpcIoConfig {
            fetch_memory_budget: 8,
            spool_threshold: 1,
            spool_directory: Some(spool.path().to_owned()),
        },
    )
    .unwrap();
    io.bind_target(target.clone());
    assert!(matches!(
        poll_until(&mut io, 1).as_slice(),
        [IoEvent::TargetBound {
            target: bound,
            basis: IoBasis::Rpc { .. },
        }] if bound == &target
    ));
    let basis = io.begin_sweep();
    assert!(matches!(&basis, IoBasis::Rpc { .. }));

    io.fetch(ReqId(10), hash, &basis);
    io.fetch(ReqId(11), hash, &basis);
    std::thread::sleep(Duration::from_millis(100));
    let first_fetch = io.poll();
    assert_eq!(
        first_fetch
            .iter()
            .filter(|event| matches!(event, IoEvent::Fetched { .. }))
            .count(),
        1,
        "the first queued payload must retain its exclusive permit until consumed"
    );
    drop(first_fetch);
    assert!(poll_until(&mut io, 1)
        .iter()
        .any(|event| matches!(event, IoEvent::Fetched { .. })));

    io.resolve(ReqId(1), asset, &basis);
    io.fetch(ReqId(2), hash, &basis);
    io.resolve_path(ReqId(3), "assets/a.bundle", &basis);
    let events = poll_until(&mut io, 3);
    assert!(events.iter().any(|event| matches!(
        event,
        IoEvent::Resolved {
            req: ReqId(1),
            result: ResolveResult::Built { content_hash },
            basis: event_basis,
            ..
        } if *content_hash == hash && event_basis == &basis
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        IoEvent::Fetched {
            req: ReqId(2),
            artifact,
            basis: event_basis,
            ..
        } if artifact.blobs.len() == 1
            && artifact.blobs[0].as_bytes() == [0x5a; 64]
            && event_basis == &basis
    )));
    assert_eq!(std::fs::read_dir(spool.path()).unwrap().count(), 1);
    assert!(events.iter().any(|event| matches!(
        event,
        IoEvent::PathResolved {
            req: ReqId(3),
            result: distill_loader::PathResolveResult::Resolved(found),
            basis: event_basis,
            ..
        } if *found == asset && event_basis == &basis
    )));

    io.subscribe(asset);
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
    let deltas = poll_until(&mut io, 1);
    assert!(deltas.iter().any(|event| matches!(
        event,
        IoEvent::Delta { assets, .. }
            if assets == &vec![(asset, distill_loader::AssetDeltaState::Changed)]
    )));

    drop(events);
    assert_eq!(std::fs::read_dir(spool.path()).unwrap().count(), 0);
    drop(io);
    server_thread.join().unwrap();
}

fn poll_until(io: &mut RpcIo, minimum: usize) -> Vec<IoEvent> {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut events = Vec::new();
    while events.len() < minimum && Instant::now() < deadline {
        events.extend(io.poll());
        if events.len() < minimum {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    assert!(
        events.len() >= minimum,
        "RPC IO events timed out: {events:?}"
    );
    events
}
