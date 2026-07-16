use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use distill_core::id::{AssetUuid, LogicalHash, TypeUuid};
use distill_loader::{
    IoBasis, IoEvent, LoaderIO, ManifestHash, ReqId, ResolveResult, RpcIo, RpcIoConfig,
    RuntimeTarget,
};
use distill_rpc::capnp_transport::StagedListener;
use distill_rpc::{
    ArtifactPayload, AssetDeltaState, AssetMutation, Commit, ConnectRequest, LeasePolicy,
    PathMutation, Server, StoreInstanceId, StoredResolve, TargetDefinition, TargetDefinitionHash,
};
use distill_wire::artifact::{content_hash, parse_artifact, write_artifact, ArtifactHeader};
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::wire::{WireField, WireNode};

const TARGET_HASH: [u8; 32] = [7; 32];

struct Fixture {
    server: Server,
    request: ConnectRequest,
    asset: AssetUuid,
    hash: distill_core::id::ContentHash,
    artifact_bytes: usize,
    wire_bytes: usize,
}

fn fixture() -> Fixture {
    let asset = AssetUuid([1; 16]);
    let type_uuid = TypeUuid([2; 16]);
    let logical_hash = LogicalHash([3; 32]);
    let target = TargetDefinition::new("dev", TargetDefinitionHash(TARGET_HASH));
    let server = Server::new(StoreInstanceId([5; 16]), vec![target]).unwrap();

    let wire = WireNode::Struct {
        offset: 0,
        size: 0,
        align: 1,
        fields: (0..64)
            .map(|index| WireField {
                name: format!("field-{index:02}"),
                declaration_index: index,
                node: WireNode::Unit { offset: 0 },
            })
            .collect(),
    };
    let layout_hash = dswl_hash(&wire).unwrap();
    let wire_bytes = dswl_bytes(&wire).unwrap();
    server
        .install_wire_tree(layout_hash, Arc::from(wire_bytes.clone()))
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
    let artifact_bytes = complete.len();
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
        artifact_bytes,
        wire_bytes: wire_bytes.len(),
    }
}

#[test]
fn rpc_io_drives_the_same_loader_boundary_on_its_own_capnp_thread() {
    let Fixture {
        server,
        request,
        asset,
        hash,
        artifact_bytes,
        wire_bytes,
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
    let budget = artifact_bytes * 2;
    assert!(wire_bytes > budget);
    let mut io = RpcIo::connect_with_config(
        address,
        request,
        RpcIoConfig {
            fetch_memory_budget: budget,
            spool_threshold: budget,
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
    assert_eq!(
        io.begin_sweep(),
        basis,
        "control commands must bypass a queued fetch waiting for residency admission"
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

#[test]
fn rpc_io_accepts_multiple_asset_subscriptions_on_one_delta_stream() {
    let Fixture {
        server,
        request,
        asset: first,
        ..
    } = fixture();
    let second = AssetUuid([9; 16]);
    server
        .commit(Commit {
            assets: vec![AssetMutation::Set {
                uuid: second,
                resolution: StoredResolve::Deleted,
                delta: AssetDeltaState::Deleted,
            }],
            ..Commit::default()
        })
        .unwrap();

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

    let target = RuntimeTarget {
        epoch: distill_loader::GameModuleEpoch(1),
        target_definition_hash: request.target_definition_hash.0,
    };
    let mut io = RpcIo::connect(address_rx.recv().unwrap(), request).unwrap();
    io.bind_target(target);
    assert!(poll_until(&mut io, 1)
        .iter()
        .any(|event| matches!(event, IoEvent::TargetBound { .. })));

    io.subscribe(first);
    io.subscribe(second);
    // This synchronous command is queued after both subscriptions and proves
    // that both installs completed before the live commit below.
    io.begin_sweep();
    server
        .commit(Commit {
            assets: vec![
                AssetMutation::Set {
                    uuid: first,
                    resolution: StoredResolve::Deleted,
                    delta: AssetDeltaState::Changed,
                },
                AssetMutation::Set {
                    uuid: second,
                    resolution: StoredResolve::Deleted,
                    delta: AssetDeltaState::Restored,
                },
            ],
            ..Commit::default()
        })
        .unwrap();

    let events = poll_until(&mut io, 1);
    assert!(events.iter().any(|event| matches!(
        event,
        IoEvent::Delta { assets, .. }
            if assets == &vec![
                (first, distill_loader::AssetDeltaState::Changed),
                (second, distill_loader::AssetDeltaState::Restored),
            ]
    )));
    assert!(!events
        .iter()
        .any(|event| matches!(event, IoEvent::ConnectionError { .. })));

    drop(io);
    server_thread.join().unwrap();
}

#[test]
fn rpc_io_reports_lease_expiry_for_rebind_and_restores_subscriptions() {
    let Fixture {
        server,
        request,
        asset,
        ..
    } = fixture();
    server
        .install_lease_policy(LeasePolicy {
            ttl: Duration::from_millis(150),
            max_snapshot_leases: 8,
            max_connections: 8,
        })
        .unwrap();
    let (address_tx, address_rx) = std::sync::mpsc::sync_channel(1);
    let (stalled_ready_tx, stalled_ready_rx) = std::sync::mpsc::sync_channel(1);
    let (release_stall_tx, release_stall_rx) = std::sync::mpsc::channel();
    let root = server.root();
    let server_thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let listener = StagedListener::bind(root.clone(), "127.0.0.1:0")
                .await
                .unwrap();
            let address = listener.local_addr().unwrap();
            address_tx.send(address).unwrap();
            let first = listener.accept_one().await.unwrap();
            let second = listener.accept_one().await.unwrap();
            drop(listener);

            let failed_listener = tokio::net::TcpListener::bind(address).await.unwrap();
            stalled_ready_tx.send(()).unwrap();
            let (stalled, _) = failed_listener.accept().await.unwrap();
            release_stall_rx.recv().unwrap();
            drop(stalled);
            drop(failed_listener);

            let listener = StagedListener::bind(root, &address.to_string())
                .await
                .unwrap();
            let third = listener.accept_one().await.unwrap();
            let connections = [first, second, third];
            for connection in connections {
                connection.await.unwrap().unwrap();
            }
        });
    });
    let target = RuntimeTarget {
        epoch: distill_loader::GameModuleEpoch(1),
        target_definition_hash: request.target_definition_hash.0,
    };
    let mut io = RpcIo::connect(address_rx.recv().unwrap(), request).unwrap();
    io.bind_target(target.clone());
    assert!(poll_until(&mut io, 1)
        .iter()
        .any(|event| matches!(event, IoEvent::TargetBound { .. })));
    io.subscribe(asset);

    assert!(poll_until(&mut io, 1).iter().any(|event| matches!(
        event,
        IoEvent::ReconnectRequired {
            reason: distill_loader::ReconnectReason::LeaseExpired
        }
    )));
    stalled_ready_rx.recv().unwrap();
    io.bind_target(target);
    let deadline = Instant::now() + Duration::from_secs(4);
    let mut reconnect_events = Vec::new();
    let mut release_stall_tx = Some(release_stall_tx);
    while !reconnect_events
        .iter()
        .any(|event| matches!(event, IoEvent::TargetBound { .. }))
    {
        reconnect_events.extend(io.poll());
        if reconnect_events
            .iter()
            .any(|event| matches!(event, IoEvent::TargetRejected { .. }))
        {
            if let Some(release) = release_stall_tx.take() {
                release.send(()).unwrap();
            }
        }
        assert!(
            Instant::now() < deadline,
            "RpcIO did not retry its failed target bind: {reconnect_events:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(reconnect_events
        .iter()
        .any(|event| matches!(event, IoEvent::TargetRejected { .. })));

    server
        .commit(Commit {
            assets: vec![AssetMutation::Set {
                uuid: asset,
                resolution: StoredResolve::Failed {
                    error: "changed after reconnect".into(),
                },
                delta: AssetDeltaState::Changed,
            }],
            ..Commit::default()
        })
        .unwrap();
    assert!(poll_until(&mut io, 1).iter().any(|event| matches!(
        event,
        IoEvent::Delta { assets, .. }
            if assets == &vec![(asset, distill_loader::AssetDeltaState::Changed)]
    )));

    drop(io);
    server_thread.join().unwrap();
}

#[test]
fn rpc_io_drop_interrupts_a_stalled_reconnect() {
    let Fixture {
        server, request, ..
    } = fixture();
    let (address_tx, address_rx) = std::sync::mpsc::sync_channel(1);
    let (stalled_ready_tx, stalled_ready_rx) = std::sync::mpsc::sync_channel(1);
    let (stalled_tx, stalled_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let root = server.root();
    let server_thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let listener = StagedListener::bind(root, "127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            address_tx.send(address).unwrap();
            let initial = listener.accept_one().await.unwrap();
            let first_rebind = listener.accept_one().await.unwrap();
            drop(listener);

            let stalled_listener = tokio::net::TcpListener::bind(address).await.unwrap();
            stalled_ready_tx.send(()).unwrap();
            let (stalled, _) = stalled_listener.accept().await.unwrap();
            stalled_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(stalled);
            drop(stalled_listener);

            initial.await.unwrap().unwrap();
            first_rebind.await.unwrap().unwrap();
        });
    });
    let target = RuntimeTarget {
        epoch: distill_loader::GameModuleEpoch(1),
        target_definition_hash: request.target_definition_hash.0,
    };
    let mut io = RpcIo::connect(address_rx.recv().unwrap(), request).unwrap();
    io.bind_target(target.clone());
    assert!(poll_until(&mut io, 1)
        .iter()
        .any(|event| matches!(event, IoEvent::TargetBound { .. })));

    stalled_ready_rx.recv().unwrap();
    io.bind_target(target);
    stalled_rx.recv().unwrap();
    let (dropped_tx, dropped_rx) = std::sync::mpsc::sync_channel(1);
    let drop_thread = std::thread::spawn(move || {
        drop(io);
        dropped_tx.send(()).unwrap();
    });
    let dropped_promptly = dropped_rx.recv_timeout(Duration::from_millis(500)).is_ok();

    release_tx.send(()).unwrap();
    drop_thread.join().unwrap();
    server_thread.join().unwrap();
    assert!(
        dropped_promptly,
        "RpcIO drop waited for the stalled reconnect timeout"
    );
}

#[test]
fn rpc_io_full_completion_channel_does_not_cancel_candidate_publication() {
    let Fixture {
        server,
        request,
        asset,
        ..
    } = fixture();
    let (address_tx, address_rx) = std::sync::mpsc::sync_channel(1);
    let (candidate_connected_tx, candidate_connected_rx) = std::sync::mpsc::sync_channel(1);
    let root = server.root();
    let server_thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let listener = StagedListener::bind(root, "127.0.0.1:0").await.unwrap();
            address_tx.send(listener.local_addr().unwrap()).unwrap();
            let initial = listener.accept_one().await.unwrap();
            let first_bind = listener.accept_one().await.unwrap();
            let candidate = listener.accept_one().await.unwrap();
            candidate_connected_tx.send(()).unwrap();
            for connection in [initial, first_bind, candidate] {
                connection.await.unwrap().unwrap();
            }
        });
    });
    let first_target = RuntimeTarget {
        epoch: distill_loader::GameModuleEpoch(1),
        target_definition_hash: request.target_definition_hash.0,
    };
    let mut io = RpcIo::connect(address_rx.recv().unwrap(), request).unwrap();
    io.bind_target(first_target);
    assert!(poll_until(&mut io, 1)
        .iter()
        .any(|event| matches!(event, IoEvent::TargetBound { .. })));

    let stale_basis = IoBasis::Pack {
        manifest: ManifestHash([0; 32]),
    };
    for request_id in 0..256 {
        io.resolve(ReqId(request_id), asset, &stale_basis);
    }
    let candidate_target = RuntimeTarget {
        epoch: distill_loader::GameModuleEpoch(2),
        target_definition_hash: TARGET_HASH,
    };
    io.bind_target(candidate_target.clone());
    candidate_connected_rx.recv().unwrap();

    // Leave the candidate's TargetBound blocked longer than the peer timeout.
    // The old ordering committed the candidate inside that timeout, canceled
    // this acknowledgment, cleared rebind, and therefore never retried it.
    std::thread::sleep(Duration::from_millis(2_200));
    let mut published = io.poll();
    assert_eq!(
        published
            .iter()
            .filter(|event| matches!(event, IoEvent::RequestError { .. }))
            .count(),
        256
    );
    assert!(!published
        .iter()
        .any(|event| matches!(event, IoEvent::TargetRejected { .. })));
    if !published.iter().any(|event| {
        matches!(
            event,
            IoEvent::TargetBound { target, .. } if target == &candidate_target
        )
    }) {
        published.extend(poll_until(&mut io, 1));
    }
    assert!(published.iter().any(|event| matches!(
        event,
        IoEvent::TargetBound { target, .. } if target == &candidate_target
    )));
    assert!(!published
        .iter()
        .any(|event| matches!(event, IoEvent::TargetRejected { .. })));

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
