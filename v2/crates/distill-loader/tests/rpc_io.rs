use std::sync::Arc;
use std::time::{Duration, Instant};

use distill_core::id::{AssetUuid, BundleUuid, LogicalHash, TypeUuid};
use distill_loader::{
    AssetPath, IoBasis, IoEvent, LoaderIO, ManifestHash, ReqId, ResolveResult, RpcIo, RpcIoConfig,
    RuntimeTarget,
};
use distill_rpc::capnp_transport::StagedListener;
use distill_rpc::{
    ArtifactPayload, BuildAnswer, ConnectRequest, Server, SnapshotPolicy, TargetDefinition,
    TargetDefinitionHash,
};
use distill_test_project::{Asset, TestBuilds, TestProject};
use distill_wire::artifact::{content_hash, parse_artifact, write_artifact, ArtifactHeader};
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::wire::{WireField, WireNode};

const TARGET_HASH: [u8; 32] = [7; 32];
const TYPE: TypeUuid = TypeUuid([2; 16]);
/// The bundle file holding the fixture's asset.
const PATH: &str = "assets/a.bundle";

/// A project whose daemon serves `asset` from the bundle file [`PATH`] (its
/// primary); the build backend builds it to the installed artifact `hash`.
struct Fixture {
    project: TestProject,
    server: Server,
    request: ConnectRequest,
    asset: AssetUuid,
    hash: distill_core::id::ContentHash,
    artifact_bytes: usize,
    wire_bytes: usize,
}

/// Author the bundle file `path` holding `asset` alone, as its primary,
/// with the blob `[value]`; a different `value` changes the asset.
fn write_asset(project: &mut TestProject, path: &str, asset: AssetUuid, value: u8) {
    project.write_bundle(
        path,
        BundleUuid([asset.0[0].wrapping_add(100); 16]),
        Some("a"),
        &[Asset::blob("a", asset, TYPE, &[value])],
    );
}

fn fixture() -> Fixture {
    let asset = AssetUuid([1; 16]);
    let type_uuid = TYPE;
    let logical_hash = LogicalHash([3; 32]);
    let target = TargetDefinition::new("dev", TargetDefinitionHash(TARGET_HASH));
    let mut project = TestProject::new(vec![target]);
    let server = project.server();

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
    assert_eq!(distill_test_project::put_wire_tree(&server.handle(), &wire_bytes), layout_hash);
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
    assert_eq!(
        distill_test_project::put_artifact(
            &server.handle(),
            &ArtifactPayload {
                structural: Arc::from(complete[..structural_len].to_vec()),
                blobs: vec![Arc::from(blob)],
                load_edges: Vec::new(),
            }
        ),
        hash
    );
    TestBuilds::install(&server)
        .answer(asset, Ok(BuildAnswer::Built { content_hash: hash }));
    write_asset(&mut project, PATH, asset, 0);
    project.publish();

    let request = ConnectRequest::new("dev", TargetDefinitionHash(TARGET_HASH));
    Fixture {
        project,
        server,
        request,
        asset,
        hash,
        artifact_bytes,
        wire_bytes: wire_bytes.len(),
    }
}

#[test]
fn rpc_io_drives_the_same_loader_boundary_on_the_callers_thread() {
    let Fixture {
        mut project,
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
    let budget = artifact_bytes * 2;
    assert!(wire_bytes > budget);
    let mut io = RpcIo::connect_with_config(
        address,
        request,
        RpcIoConfig {
            fetch_memory_budget: budget,
            ..RpcIoConfig::default()
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
    // The DSWL trees overflow the budget: growing an admitted payload never
    // waits, and both payloads stay in memory until `poll` hands them over.
    let both = poll_until(&mut io, 2);
    assert_eq!(
        both.iter()
            .filter(|event| matches!(event, IoEvent::Fetched { .. }))
            .count(),
        2,
        "{both:?}"
    );
    assert_eq!(io.stats().resident_fetch_bytes, 0);
    drop(both);

    io.resolve(ReqId(1), asset, &basis);
    io.fetch(ReqId(2), hash, &basis);
    io.resolve_path(ReqId(3), &AssetPath::from(PATH), &basis);
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
    write_asset(&mut project, PATH, asset, 1);
    project.publish();
    let deltas = poll_until(&mut io, 1);
    assert!(deltas.iter().any(|event| matches!(
        event,
        IoEvent::Delta { assets, .. }
            if assets == &vec![(asset, distill_loader::AssetDeltaState::Changed)]
    )));

    drop(events);
    drop(io);
    server_thread.join().unwrap();
}

#[test]
fn rpc_io_accepts_multiple_asset_subscriptions_on_one_delta_stream() {
    let Fixture {
        mut project,
        server,
        request,
        asset: first,
        ..
    } = fixture();
    // The second asset was authored and deleted.
    let second = AssetUuid([9; 16]);
    write_asset(&mut project, "assets/b.bundle", second, 0);
    project.publish();
    project.remove("assets/b.bundle");
    project.publish();

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
    // Both subscribe calls have answered once only the delta stream and the
    // maintenance task remain.
    poll_while(&mut io, |io| io.stats().control_tasks > 2);
    // One publication changes the first and authors the second again: the
    // daemon reports a returning asset as changed.
    write_asset(&mut project, PATH, first, 1);
    write_asset(&mut project, "assets/b.bundle", second, 0);
    project.publish();

    let events = poll_until(&mut io, 1);
    assert!(
        events.iter().any(|event| matches!(
            event,
            IoEvent::Delta { assets, .. }
                if assets == &vec![
                    (first, distill_loader::AssetDeltaState::Changed),
                    (second, distill_loader::AssetDeltaState::Changed),
                ]
        )),
        "{events:?}"
    );
    assert!(!events
        .iter()
        .any(|event| matches!(event, IoEvent::ConnectionError { .. })));

    drop(io);
    server_thread.join().unwrap();
}

#[test]
fn rpc_io_reports_an_expired_snapshot_and_the_next_round_reads_a_new_one() {
    let Fixture {
        project: _project,
        server,
        request,
        asset,
        hash,
        ..
    } = fixture();
    let (address_tx, address_rx) = std::sync::mpsc::sync_channel(1);
    let handle = server.handle();
    let server_thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let front = Server::open(&handle);
            front
                .install_snapshot_policy(SnapshotPolicy {
                    ttl: Duration::from_millis(100),
                    ..SnapshotPolicy::default()
                })
                .unwrap();
            let listener = StagedListener::bind(front.root(), "127.0.0.1:0")
                .await
                .unwrap();
            address_tx.send(listener.local_addr().unwrap()).unwrap();
            listener.accept_one().await.unwrap().await.unwrap().unwrap();
        });
    });
    let mut io = RpcIo::connect(address_rx.recv().unwrap(), request).unwrap();
    let basis = io.begin_sweep();
    std::thread::sleep(Duration::from_millis(250));
    io.resolve(ReqId(1), asset, &basis);
    assert!(matches!(
        poll_until(&mut io, 1).as_slice(),
        [IoEvent::SnapshotExpired { req: ReqId(1), basis: expired }] if expired == &basis
    ));

    // Nothing was published: the new round's snapshot has the same basis.
    let basis = io.begin_sweep();
    io.resolve(ReqId(2), asset, &basis);
    assert!(matches!(
        poll_until(&mut io, 1).as_slice(),
        [IoEvent::Resolved {
            req: ReqId(2),
            result: ResolveResult::Built { content_hash },
            ..
        }] if *content_hash == hash
    ));

    drop(io);
    server_thread.join().unwrap();
}

#[test]
fn rpc_io_reconnects_a_closed_connection_and_restores_subscriptions() {
    let Fixture {
        mut project,
        server,
        request,
        asset,
        ..
    } = fixture();
    let (address_tx, address_rx) = std::sync::mpsc::sync_channel(1);
    let (close_tx, close_rx) = tokio::sync::oneshot::channel::<()>();
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
            // The daemon closes the connections: their threads drop the
            // RPC systems and the sockets with them.
            close_rx.await.unwrap();
            first.close();
            second.close();

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
    // Report the first failed rebind: the stalled attempt is released on it.
    let mut io = RpcIo::connect_with_config(
        address_rx.recv().unwrap(),
        request,
        RpcIoConfig {
            target_rejection_after: 1,
            ..RpcIoConfig::default()
        },
    )
    .unwrap();
    io.bind_target(target.clone());
    assert!(poll_until(&mut io, 1)
        .iter()
        .any(|event| matches!(event, IoEvent::TargetBound { .. })));
    io.subscribe(asset);
    poll_for(&mut io, Duration::from_millis(100));
    close_tx.send(()).unwrap();

    assert!(poll_until(&mut io, 1).iter().any(|event| matches!(
        event,
        IoEvent::ReconnectRequired {
            reason: distill_loader::ReconnectReason::ConnectionLost
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

    // A second bundle file claims the asset: the daemon withholds it, failed.
    project.write_bundle(
        "assets/dup.bundle",
        BundleUuid([42; 16]),
        None,
        &[Asset::blob("dup", asset, TYPE, &[9])],
    );
    project.publish();
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
        project: _project,
        server,
        request,
        ..
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
    // RpcIO is driven by the thread that owns it; this one plays the engine.
    let (dropped_tx, dropped_rx) = std::sync::mpsc::sync_channel(1);
    let engine = std::thread::spawn(move || {
        let mut io = RpcIo::connect(address_rx.recv().unwrap(), request).unwrap();
        io.bind_target(target.clone());
        assert!(poll_until(&mut io, 1)
            .iter()
            .any(|event| matches!(event, IoEvent::TargetBound { .. })));

        stalled_ready_rx.recv().unwrap();
        io.bind_target(target);
        // Step until the rebind's connection is accepted and stalls.
        while stalled_rx.try_recv().is_err() {
            io.poll();
            std::thread::sleep(Duration::from_millis(2));
        }
        let started = Instant::now();
        drop(io);
        dropped_tx.send(started.elapsed()).unwrap();
    });
    let elapsed = dropped_rx.recv_timeout(Duration::from_secs(5));

    release_tx.send(()).unwrap();
    engine.join().unwrap();
    server_thread.join().unwrap();
    let elapsed = elapsed.expect("RpcIO drop never returned");
    assert!(
        elapsed < Duration::from_millis(100),
        "RpcIO drop waited for the stalled reconnect: {elapsed:?}"
    );
}

#[test]
fn rpc_io_rebind_fences_everything_of_the_old_connection() {
    let Fixture {
        project: _project,
        server,
        request,
        asset,
        ..
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
            let initial = listener.accept_one().await.unwrap();
            let first_bind = listener.accept_one().await.unwrap();
            let candidate = listener.accept_one().await.unwrap();
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

    // Requests of the old connection, some answered and undelivered, some
    // queued, and some failed on a foreign basis.
    let basis = io.begin_sweep();
    for request_id in 0..300 {
        io.resolve(ReqId(request_id), asset, &basis);
    }
    let stale_basis = IoBasis::Pack {
        manifest: ManifestHash([0; 32]),
    };
    io.resolve(ReqId(1_000), asset, &stale_basis);
    io.step();
    io.step();
    let candidate_target = RuntimeTarget {
        epoch: distill_loader::GameModuleEpoch(2),
        target_definition_hash: TARGET_HASH,
    };
    io.bind_target(candidate_target.clone());
    // A second report of the same lost connection keeps the rebind underway.
    io.bind_target(candidate_target.clone());
    let published = poll_until(&mut io, 1);
    assert!(
        matches!(
            published.as_slice(),
            [IoEvent::TargetBound { target, .. }] if target == &candidate_target
        ),
        "only the candidate's binding may follow a rebind: {published:?}"
    );
    let stats = io.stats();
    assert_eq!(
        (stats.queued_requests, stats.in_flight_requests),
        (0, 0),
        "{stats:?}"
    );
    poll_for(&mut io, Duration::from_millis(100));

    drop(io);
    server_thread.join().unwrap();
}

#[test]
fn rpc_io_polls_import_failures_and_reports_only_changes() {
    let Fixture {
        project: _project,
        server,
        request,
        ..
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
            listener.accept_one().await.unwrap().await.unwrap().unwrap();
        });
    });
    let mut io = RpcIo::connect(address_rx.recv().unwrap(), request).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    let first = loop {
        io.poll();
        if let Some(failures) = io.take_import_failures() {
            break failures;
        }
        assert!(Instant::now() < deadline, "import failures were never polled");
        std::thread::sleep(Duration::from_millis(5));
    };
    assert!(first.is_empty());
    // The next poll returns the same list: not a change.
    poll_for(&mut io, Duration::from_millis(1200));
    assert_eq!(io.take_import_failures(), None);
    drop(io);
    server_thread.join().unwrap();
}

fn poll_until(io: &mut RpcIo, minimum: usize) -> Vec<IoEvent> {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut events = Vec::new();
    while events.len() < minimum && Instant::now() < deadline {
        events.extend(io.poll());
        if events.len() < minimum {
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    assert!(
        events.len() >= minimum,
        "RPC IO events timed out: {events:?}"
    );
    events
}

/// Step `io` for `duration`, as frames would.
fn poll_for(io: &mut RpcIo, duration: Duration) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        assert!(io.poll().is_empty());
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn poll_while(io: &mut RpcIo, mut condition: impl FnMut(&RpcIo) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while condition(io) {
        assert!(io.poll().is_empty());
        assert!(Instant::now() < deadline, "RPC IO never settled: {:?}", io.stats());
        std::thread::sleep(Duration::from_millis(2));
    }
}
