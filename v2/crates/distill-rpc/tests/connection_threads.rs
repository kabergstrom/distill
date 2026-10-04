//! One thread per RPC connection: isolation between connections, the
//! connection and snapshot bounds, delta and fence fan-out, cleanup when a
//! client vanishes, prompt shutdown, panic isolation, a concurrent stress
//! run, and writers: each connection writes on a writer of its own. Every
//! test runs under a watchdog.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use distill_rpc::capnp_loader::{RemoteCall, RemoteHub, RemoteSnapshot};
use distill_rpc::capnp_transport::{
    schema, CapnpClient, ConnectionHandle, RemoteConnectOutcome, StagedListener,
};
use distill_rpc::*;
use distill_test_project::{Asset, TestProject};

const TARGET_HASH: TargetDefinitionHash = TargetDefinitionHash([7; 32]);
/// `runtime_type_policy` of this type blocks until the test releases it.
const STALL: TypeUuid = TypeUuid([0xee; 16]);
/// `runtime_type_policy` of this type panics.
const PANIC: TypeUuid = TypeUuid([0xdd; 16]);
const ASSET: AssetUuid = AssetUuid([44; 16]);
const LARGE: AssetUuid = AssetUuid([45; 16]);

fn request() -> ConnectRequest {
    ConnectRequest::new("dev", TARGET_HASH)
}

// ---------------------------------------------------------------------------
// Harness

/// Run `body` on its own thread and fail the test if it outlives `limit`.
fn watchdog<T: Send + 'static>(limit: Duration, body: impl FnOnce() -> T + Send + 'static) -> T {
    let (done_tx, done_rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let _ = done_tx.send(body());
    });
    match done_rx.recv_timeout(limit) {
        Ok(value) => {
            thread.join().unwrap();
            value
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => match thread.join() {
            Err(panic) => std::panic::resume_unwind(panic),
            Ok(()) => unreachable!("the body either answers or panics"),
        },
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("test exceeded its {limit:?} watchdog"),
    }
}

/// Run a `!Send` future to completion on a thread of its own, the way an
/// engine client runs: its own runtime and `LocalSet`.
fn on_thread<T, F, Fut>(make: F) -> std::thread::JoinHandle<T>
where
    T: Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
{
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, make())
    })
}

/// Wait until `condition` holds, polling; false if `limit` passes first.
fn eventually(limit: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    loop {
        if condition() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Await `future` from a plain thread, failing after `limit`.
fn wait_for<T: Send + 'static>(
    limit: Duration,
    future: impl Future<Output = T> + Send + 'static,
) -> Option<T> {
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = done_tx.send(futures::executor::block_on(future));
    });
    done_rx.recv_timeout(limit).ok()
}

/// A rendezvous the stalling backend and the test share.
#[derive(Default)]
struct Gate {
    state: Mutex<(usize, bool)>,
    changed: Condvar,
}

impl Gate {
    /// Called by the backend: count one stalled call, wait for release.
    fn stall(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 += 1;
        self.changed.notify_all();
        let (state, _) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(30), |state| !state.1)
            .unwrap();
        drop(state);
    }

    fn wait_stalled(&self, count: usize, limit: Duration) -> bool {
        let state = self.state.lock().unwrap();
        let (state, _) = self
            .changed
            .wait_timeout_while(state, limit, |state| state.0 < count)
            .unwrap();
        state.0 >= count
    }

    fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}

/// Answers each build with the artifact installed for its asset (a build
/// of `ASSET` at a version in `by_version` with that version's), and
/// stalls or panics on the runtime policy of `STALL` or `PANIC`.
struct TestBackend {
    gate: Arc<Gate>,
    built: BTreeMap<AssetUuid, ContentHash>,
    by_version: Mutex<BTreeMap<InputVersion, ContentHash>>,
}

impl BuildBackend for TestBackend {
    fn start(&self, view: BuildView<'_>, request: &BuildRequest) -> BuildStart {
        let at_version = (request.requested_asset == ASSET)
            .then(|| self.by_version.lock().unwrap().get(&view.stamp.version).copied())
            .flatten();
        BuildStart::Answered(
            match at_version.or_else(|| self.built.get(&request.requested_asset).copied()) {
                Some(content_hash) => Ok(BuildAnswer::Built { content_hash }),
                None => Ok(BuildAnswer::Drifted {
                    input: request.drifted_input.clone(),
                }),
            },
        )
    }

    fn runtime_type_policy(
        &self,
        _snapshot: &distill_store::StoreReader,
        request: &RuntimeTypePolicyRequest,
    ) -> Result<RuntimeTypePolicy, RpcFailure> {
        if request.type_uuid == STALL {
            self.gate.stall();
        }
        if request.type_uuid == PANIC {
            panic!("the test backend panics on request");
        }
        Ok(RuntimeTypePolicy { build_only: false })
    }
}

fn artifact(asset: AssetUuid, fixed: &[u8]) -> (ContentHash, ArtifactPayload) {
    let wire = distill_wire::wire::WireNode::Unit { offset: 0 };
    let layout_hash = distill_wire::dswl::dswl_hash(&wire).unwrap();
    let complete = distill_wire::artifact::write_artifact(
        &distill_wire::artifact::ArtifactHeader {
            asset_uuid: asset,
            authored_type: TypeUuid([1; 16]),
            terminal_type: TypeUuid([1; 16]),
            encoded_type: TypeUuid([1; 16]),
            logical_hash: LogicalHash([77; 32]),
            layout_hash,
        },
        &[],
        fixed,
        &[],
        &[],
    )
    .unwrap();
    let hash = distill_wire::artifact::content_hash(&complete);
    (
        hash,
        ArtifactPayload {
            structural: complete.into(),
            blobs: Vec::new(),
            load_edges: Vec::new(),
        },
    )
}

/// The type of every authored asset and artifact here.
const TYPE: TypeUuid = TypeUuid([1; 16]);
/// The bundle file holding `ASSET`.
const ASSET_FILE: &str = "asset.bundle";
/// A second bundle file claiming `ASSET`, which withholds it.
const DUPLICATE_FILE: &str = "duplicate.bundle";

/// An empty project whose daemon serves the target "dev".
fn project() -> TestProject {
    TestProject::new(vec![TargetDefinition::new("dev", TARGET_HASH)])
}

/// Author `ASSET` holding `value`, as the primary of its own bundle file.
fn write_asset(project: &mut TestProject, value: u8) {
    project.write_bundle(
        ASSET_FILE,
        BundleUuid([44; 16]),
        Some("asset"),
        &[Asset::uint("asset", ASSET, TYPE, value)],
    );
}

/// Author a second bundle file claiming `ASSET`: the daemon withholds it,
/// and serves it failed with [`collision_error`].
fn write_duplicate(project: &mut TestProject) {
    project.write_bundle(
        DUPLICATE_FILE,
        BundleUuid([46; 16]),
        None,
        &[Asset::uint("duplicate", ASSET, TYPE, 0)],
    );
}

/// What a resolve of `ASSET` answers while two bundle files claim it.
fn collision_error() -> String {
    format!("duplicate asset UUID {ASSET}")
}

struct Fixture {
    // The daemon and its files, held for the fixture's lifetime.
    project: TestProject,
    server: Server,
    backend: Arc<TestBackend>,
    gate: Arc<Gate>,
    hash: ContentHash,
    large: ContentHash,
}

/// A daemon serving `ASSET` and `LARGE`, each authored in its own bundle
/// file; their builds answer the installed artifacts, `LARGE`'s 8 MiB.
fn fixture() -> Fixture {
    let mut project = project();
    let server = project.server();
    let gate = Arc::new(Gate::default());
    let (hash, payload) = artifact(ASSET, &[7, 8, 9]);
    server.install_artifact(hash, payload).unwrap();
    let (large, payload) = artifact(LARGE, &vec![0x5a; 8 << 20]);
    server.install_artifact(large, payload).unwrap();
    let backend = Arc::new(TestBackend {
        gate: Arc::clone(&gate),
        built: BTreeMap::from([(ASSET, hash), (LARGE, large)]),
        by_version: Mutex::new(BTreeMap::new()),
    });
    server.install_build_backend(backend.clone());
    write_asset(&mut project, 1);
    project.write_bundle(
        "large.bundle",
        BundleUuid([45; 16]),
        Some("large"),
        &[Asset::uint("large", LARGE, TYPE, 1)],
    );
    project.publish();
    Fixture {
        project,
        server,
        backend,
        gate,
        hash,
        large,
    }
}

enum Command {
    Connections(mpsc::Sender<usize>),
    Shutdown(Duration, mpsc::Sender<(usize, Duration)>),
}

/// A listener on a thread of its own, as the daemon runs it. Accepted
/// connections are handed to the test.
struct Daemon {
    address: SocketAddr,
    commands: tokio::sync::mpsc::UnboundedSender<Command>,
    accepted: mpsc::Receiver<ConnectionHandle>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Daemon {
    fn start(root: Root) -> Self {
        let (address_tx, address_rx) = mpsc::channel();
        let (commands, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
        let (accepted_tx, accepted) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let listener = StagedListener::bind(root, "127.0.0.1:0").await.unwrap();
                address_tx.send(listener.local_addr().unwrap()).unwrap();
                loop {
                    tokio::select! {
                        accepted = listener.accept_one() => {
                            if let Ok(connection) = accepted {
                                let _ = accepted_tx.send(connection);
                            }
                        }
                        command = command_rx.recv() => match command {
                            Some(Command::Connections(reply)) => {
                                let _ = reply.send(listener.connections());
                            }
                            Some(Command::Shutdown(grace, reply)) => {
                                let started = Instant::now();
                                let remaining = listener.shutdown(grace).await;
                                let _ = reply.send((remaining, started.elapsed()));
                                return;
                            }
                            None => return,
                        },
                    }
                }
            });
        });
        Self {
            address: address_rx.recv().unwrap(),
            commands,
            accepted,
            thread: Some(thread),
        }
    }

    fn connections(&self) -> usize {
        let (reply, answer) = mpsc::channel();
        self.commands.send(Command::Connections(reply)).unwrap();
        answer.recv().unwrap()
    }

    fn next_connection(&self) -> ConnectionHandle {
        self.accepted
            .recv_timeout(Duration::from_secs(5))
            .expect("the listener accepted no connection")
    }

    /// Shut the listener down; how many threads outlived `grace`, and how
    /// long shutdown took.
    fn shutdown(mut self, grace: Duration) -> (usize, Duration) {
        let (reply, answer) = mpsc::channel();
        self.commands.send(Command::Shutdown(grace, reply)).unwrap();
        let result = answer.recv().unwrap();
        self.thread.take().unwrap().join().unwrap();
        result
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            let (reply, _answer) = mpsc::channel();
            let _ = self
                .commands
                .send(Command::Shutdown(Duration::from_millis(500), reply));
            let _ = thread.join();
        }
    }
}

async fn connect(address: SocketAddr) -> (CapnpClient, RemoteHub) {
    let client = CapnpClient::connect_local(address).await.unwrap();
    let hub = RemoteHub::connected(client.connect(&request()).await.unwrap()).unwrap();
    (client, hub)
}

async fn snapshot(hub: &RemoteHub) -> RemoteSnapshot {
    match hub.snapshot().await.unwrap() {
        RemoteCall::Success(snapshot) => snapshot,
        other => panic!("expected a snapshot, got {other:?}"),
    }
}

async fn resolve_built(snapshot: &RemoteSnapshot, expected: ContentHash) {
    match snapshot.resolve(ASSET).await.unwrap() {
        RemoteCall::Success(terminal) => assert_eq!(
            terminal.value,
            ResolveResult::Built {
                content_hash: expected
            }
        ),
        other => panic!("expected a resolve, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 1. Isolation

/// A request stalled inside the server on one connection must not delay
/// another connection's calls, nor a new connection. On the single-thread
/// server every connection waited for the stall.
#[test]
fn a_stalled_request_on_one_connection_does_not_delay_another() {
    watchdog(Duration::from_secs(60), || {
        let Fixture {
            project: _project,
            server,
            gate,
            hash,
            ..
        } = fixture();
        let (address_tx, address_rx) = mpsc::channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let root = server.root();
        let daemon = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                let listener = StagedListener::bind(root, "127.0.0.1:0").await.unwrap();
                address_tx.send(listener.local_addr().unwrap()).unwrap();
                listener
                    .serve_until(async move {
                        let _ = stop_rx.await;
                    })
                    .await
                    .unwrap();
            });
        });
        let address: SocketAddr = address_rx.recv().unwrap();

        // B connects first and makes one round trip.
        let (round_tx, round_rx) = mpsc::channel::<()>();
        let (took_tx, took_rx) = mpsc::channel::<Duration>();
        let b = on_thread(move || async move {
            let (_client, hub) = connect(address).await;
            while round_rx.recv().is_ok() {
                let started = Instant::now();
                let snapshot = snapshot(&hub).await;
                resolve_built(&snapshot, hash).await;
                took_tx.send(started.elapsed()).unwrap();
            }
        });
        round_tx.send(()).unwrap();
        took_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("connection B's first round trip");

        // A stalls inside the server.
        let a = on_thread(move || async move {
            let (_client, hub) = connect(address).await;
            let snapshot = snapshot(&hub).await;
            snapshot.runtime_type_policy(STALL).await.unwrap().success()
        });
        assert!(
            gate.wait_stalled(1, Duration::from_secs(10)),
            "connection A never reached the stalling call"
        );

        // While A is stalled, B and a new connection C are served.
        round_tx.send(()).unwrap();
        let during_b = took_rx.recv_timeout(Duration::from_secs(3));
        let c = on_thread(move || async move {
            let started = Instant::now();
            let (_client, hub) = connect(address).await;
            let snapshot = snapshot(&hub).await;
            resolve_built(&snapshot, hash).await;
            started.elapsed()
        });
        let (c_tx, c_rx) = mpsc::channel();
        let c_waiter = std::thread::spawn(move || {
            let _ = c_tx.send(c.join().unwrap());
        });
        let during_c = c_rx.recv_timeout(Duration::from_secs(3));

        gate.release();
        assert!(a.join().unwrap().is_some(), "A's stalled call completes");
        drop(round_tx);
        b.join().unwrap();
        c_waiter.join().unwrap();
        stop_tx.send(()).unwrap();
        daemon.join().unwrap();

        let during_b =
            during_b.expect("connection B's call waited for connection A's stalled request");
        let during_c =
            during_c.expect("a new connection waited for connection A's stalled request");
        assert!(during_b < Duration::from_secs(2), "B took {during_b:?}");
        assert!(during_c < Duration::from_secs(2), "C took {during_c:?}");
    });
}

// ---------------------------------------------------------------------------
// 2. Limits

#[test]
fn connections_up_to_the_bound_are_served_at_once_and_one_more_is_refused() {
    watchdog(Duration::from_secs(60), || {
        let Fixture { project: _project, server, hash, .. } = fixture();
        server
            .install_snapshot_policy(SnapshotPolicy {
                max_connections: 4,
                ..SnapshotPolicy::default()
            })
            .unwrap();
        let daemon = Daemon::start(server.root());
        let address = daemon.address;

        // Four clients connect, all hold a snapshot, then all resolve.
        let (opened_tx, opened_rx) = mpsc::channel::<()>();
        let mut releases = Vec::new();
        let mut clients = Vec::new();
        for _ in 0..4 {
            let opened_tx = opened_tx.clone();
            let (release_tx, release_rx) = mpsc::channel::<()>();
            releases.push(release_tx);
            clients.push(on_thread(move || async move {
                let (_client, hub) = connect(address).await;
                let snapshot = snapshot(&hub).await;
                opened_tx.send(()).unwrap();
                let _ = release_rx.recv();
                resolve_built(&snapshot, hash).await;
                // Hold the connection until told to go.
                let _ = release_rx.recv();
            }));
        }
        for _ in 0..4 {
            opened_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("a connection under the bound was not served");
        }
        assert_eq!(daemon.connections(), 4);
        assert_eq!(server.handle().open_connections(), 4);
        for release in &releases {
            release.send(()).unwrap();
        }

        // A fifth is closed unserved: its bootstrap call fails.
        let refused = on_thread(move || async move {
            let client = CapnpClient::connect_local(address).await.unwrap();
            client.connect(&request()).await.is_err()
        });
        assert!(refused.join().unwrap(), "a connection past the bound was served");
        assert_eq!(daemon.connections(), 4);

        // When one leaves, a new one is admitted.
        releases.remove(0).send(()).unwrap();
        clients.remove(0).join().unwrap();
        assert!(eventually(Duration::from_secs(5), || daemon.connections() == 3));
        let admitted = on_thread(move || async move {
            let (_client, hub) = connect(address).await;
            resolve_built(&snapshot(&hub).await, hash).await;
        });
        admitted.join().unwrap();

        drop(releases);
        for client in clients {
            client.join().unwrap();
        }
        assert!(eventually(Duration::from_secs(5), || daemon.connections() == 0));
        assert!(eventually(Duration::from_secs(5), || {
            server.handle().open_connections() == 0 && server.open_snapshots() == 0
        }));
    });
}

// ---------------------------------------------------------------------------
// 3. Fan-out

#[test]
fn deltas_and_fences_reach_every_connection() {
    watchdog(Duration::from_secs(60), || {
        let Fixture { mut project, server, .. } = fixture();
        let daemon = Daemon::start(server.root());
        let address = daemon.address;
        let since = server.current_stamp().unwrap().version;

        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        let (event_tx, event_rx) = mpsc::channel::<(usize, StreamEvent)>();
        let (fenced_tx, fenced_rx) = mpsc::channel::<bool>();
        let mut clients = Vec::new();
        for index in 0..4 {
            let ready_tx = ready_tx.clone();
            let event_tx = event_tx.clone();
            let fenced_tx = fenced_tx.clone();
            clients.push(on_thread(move || async move {
                let (_client, hub) = connect(address).await;
                let mut subscription = match hub.subscribe(since, vec![ASSET], vec![]).await.unwrap() {
                    RemoteCall::Success(subscription) => subscription,
                    other => panic!("subscribe failed: {other:?}"),
                };
                assert!(matches!(
                    subscription.next().await.unwrap(),
                    Some(StreamEvent::InitialDelta { .. })
                ));
                ready_tx.send(()).unwrap();
                for _ in 0..2 {
                    let event = subscription.next().await.unwrap().expect("the stream ended");
                    event_tx.send((index, event)).unwrap();
                }
                fenced_tx
                    .send(matches!(
                        hub.snapshot().await.unwrap(),
                        RemoteCall::ReconnectRequired(ReconnectReason::TargetDefinitionChanged)
                    ))
                    .unwrap();
            }));
        }
        for _ in 0..4 {
            ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        }

        // `ASSET` changes: its bundle file now holds another value.
        write_asset(&mut project, 2);
        let changed = project.publish();
        let mut deltas = BTreeMap::new();
        for _ in 0..4 {
            let (index, event) = event_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            deltas.insert(index, event);
        }
        for event in deltas.values() {
            assert!(matches!(
                event,
                StreamEvent::Delta(Delta { basis, assets, .. })
                    if basis.snapshot == changed
                        && assets == &vec![(ASSET, AssetDeltaState::Changed)]
            ));
        }
        assert_eq!(deltas.len(), 4);

        server
            .replace_target(TargetDefinition::new("dev", TargetDefinitionHash([8; 32])))
            .unwrap();
        let mut fences = BTreeMap::new();
        for _ in 0..4 {
            let (index, event) = event_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            fences.insert(index, event);
        }
        for event in fences.values() {
            assert!(matches!(
                event,
                StreamEvent::Asset {
                    event: AssetEvent::ReconnectRequired {
                        reason: ReconnectReason::TargetDefinitionChanged
                    },
                    ..
                }
            ));
        }
        assert_eq!(fences.len(), 4);
        for _ in 0..4 {
            assert!(fenced_rx.recv_timeout(Duration::from_secs(5)).unwrap());
        }
        for client in clients {
            client.join().unwrap();
        }
    });
}

// ---------------------------------------------------------------------------
// 4. Snapshot bound and TTL

#[test]
fn the_snapshot_bound_and_ttl_hold_across_connection_threads() {
    watchdog(Duration::from_secs(60), || {
        let Fixture { project: _project, server, hash, .. } = fixture();
        server
            .install_snapshot_policy(SnapshotPolicy {
                ttl: Duration::from_millis(1500),
                max_snapshots: 2,
                max_connections: 8,
            })
            .unwrap();
        let daemon = Daemon::start(server.root());
        let address = daemon.address;

        // A holds the whole bound.
        let (a_step_tx, a_step_rx) = mpsc::channel::<()>();
        let (a_done_tx, a_done_rx) = mpsc::channel::<bool>();
        let a = on_thread(move || async move {
            let (_client, hub) = connect(address).await;
            let first = snapshot(&hub).await;
            let _second = snapshot(&hub).await;
            a_done_tx.send(true).unwrap();
            // A third releases A's own oldest.
            a_step_rx.recv().unwrap();
            let _third = snapshot(&hub).await;
            a_done_tx
                .send(matches!(first.resolve(ASSET).await.unwrap(), RemoteCall::SnapshotExpired))
                .unwrap();
            // Past the TTL every snapshot of A is gone.
            a_step_rx.recv().unwrap();
            a_done_tx
                .send(matches!(_second.resolve(ASSET).await.unwrap(), RemoteCall::SnapshotExpired))
                .unwrap();
            let _ = a_step_rx.recv();
        });
        assert!(a_done_rx.recv_timeout(Duration::from_secs(10)).unwrap());
        assert_eq!(server.open_snapshots(), 2);

        // B holds none, so it is refused rather than evicting A's.
        let (b_step_tx, b_step_rx) = mpsc::channel::<()>();
        let (b_done_tx, b_done_rx) = mpsc::channel::<String>();
        let b = on_thread(move || async move {
            let (_client, hub) = connect(address).await;
            loop {
                let outcome = match hub.snapshot().await.unwrap() {
                    RemoteCall::Success(snapshot) => {
                        resolve_built(&snapshot, hash).await;
                        "opened".to_owned()
                    }
                    RemoteCall::Error(error) => error.message,
                    other => format!("{other:?}"),
                };
                b_done_tx.send(outcome).unwrap();
                if b_step_rx.recv().is_err() {
                    return;
                }
            }
        });
        let refused = b_done_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(refused.contains("open snapshots"), "B was not refused: {refused}");
        assert_eq!(server.open_snapshots(), 2);

        a_step_tx.send(()).unwrap();
        assert!(a_done_rx.recv_timeout(Duration::from_secs(10)).unwrap());
        assert_eq!(server.open_snapshots(), 2);

        // The TTL expires A's snapshots on A's thread; then B opens one.
        assert!(eventually(Duration::from_secs(5), || server.open_snapshots() == 0));
        a_step_tx.send(()).unwrap();
        assert!(a_done_rx.recv_timeout(Duration::from_secs(10)).unwrap());
        b_step_tx.send(()).unwrap();
        assert_eq!(b_done_rx.recv_timeout(Duration::from_secs(10)).unwrap(), "opened");

        drop((a_step_tx, b_step_tx));
        a.join().unwrap();
        b.join().unwrap();
        assert!(eventually(Duration::from_secs(5), || server.open_snapshots() == 0));
    });
}

// ---------------------------------------------------------------------------
// 5. A client vanishing mid-fetch

#[test]
fn a_connection_dropped_mid_fetch_releases_everything() {
    watchdog(Duration::from_secs(60), || {
        let Fixture {
            project: _project,
            server,
            hash,
            large,
            ..
        } = fixture();
        let daemon = Daemon::start(server.root());
        let address = daemon.address;

        let client = on_thread(move || async move {
            let (client, hub) = connect(address).await;
            let snapshot = snapshot(&hub).await;
            let mut fetched = match snapshot.fetch(large).await.unwrap() {
                RemoteCall::Success(terminal) => terminal.value,
                other => panic!("fetch failed: {other:?}"),
            };
            assert!(fetched.total_bytes() > 8 << 20);
            for _ in 0..3 {
                assert!(fetched.next_chunk().await.unwrap().is_some());
            }
            // Vanish with the fetch half done.
            drop(client);
        });
        let connection = daemon.next_connection();
        client.join().unwrap();
        let outcome = wait_for(Duration::from_secs(5), connection)
            .expect("the connection thread outlived its client");
        assert!(outcome.is_ok(), "the connection thread panicked: {outcome:?}");
        assert_eq!(daemon.connections(), 0);
        assert_eq!(server.open_snapshots(), 0);
        assert_eq!(server.handle().open_connections(), 0);

        // The server serves the next client as before.
        on_thread(move || async move {
            let (_client, hub) = connect(address).await;
            resolve_built(&snapshot(&hub).await, hash).await;
        })
        .join()
        .unwrap();
    });
}

// ---------------------------------------------------------------------------
// 6. Shutdown

#[test]
fn shutdown_is_prompt_with_stuck_clients() {
    watchdog(Duration::from_secs(60), || {
        let Fixture { project: _project, server, gate, .. } = fixture();
        let daemon = Daemon::start(server.root());
        let address = daemon.address;

        // A client that connects and never speaks.
        let mut silent = std::net::TcpStream::connect(address).unwrap();
        let silent_connection = daemon.next_connection();
        // A client whose request is stuck inside the server.
        let stalled = on_thread(move || async move {
            let (_client, hub) = connect(address).await;
            let snapshot = snapshot(&hub).await;
            let _ = snapshot.runtime_type_policy(STALL).await;
        });
        let stalled_connection = daemon.next_connection();
        assert!(gate.wait_stalled(1, Duration::from_secs(10)));
        // An idle client waiting on its delta stream.
        let (idle_ready_tx, idle_ready_rx) = mpsc::channel();
        let idle = on_thread(move || async move {
            let (_client, hub) = connect(address).await;
            let since = InputVersion(0);
            let mut subscription = match hub.subscribe(since, vec![ASSET], vec![]).await.unwrap() {
                RemoteCall::Success(subscription) => subscription,
                other => panic!("subscribe failed: {other:?}"),
            };
            let _ = subscription.next().await;
            idle_ready_tx.send(()).unwrap();
            // The daemon closes the connection under the waiting call.
            subscription.next().await.is_err()
        });
        let idle_connection = daemon.next_connection();
        idle_ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();

        let (remaining, took) = daemon.shutdown(Duration::from_millis(500));
        assert!(took < Duration::from_secs(2), "shutdown took {took:?}");
        assert_eq!(remaining, 1, "only the thread inside the stalled call remains");
        assert!(idle.join().unwrap(), "the idle client's connection was not closed");
        assert!(wait_for(Duration::from_secs(2), idle_connection).is_some());
        assert!(wait_for(Duration::from_secs(2), silent_connection).is_some());
        silent
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut byte = [0; 1];
        assert!(
            matches!(std::io::Read::read(&mut silent, &mut byte), Ok(0) | Err(_)),
            "the silent client's socket stayed open"
        );

        // The stalled thread exits once its call returns.
        gate.release();
        assert!(wait_for(Duration::from_secs(5), stalled_connection).is_some());
        stalled.join().unwrap();
        assert!(
            eventually(Duration::from_secs(5), || {
                server.open_snapshots() == 0 && server.handle().open_connections() == 0
            }),
            "snapshots {} connections {}",
            server.open_snapshots(),
            server.handle().open_connections()
        );
    });
}

// ---------------------------------------------------------------------------
// 7. Panic isolation

#[test]
fn a_panic_ends_only_its_own_connection() {
    watchdog(Duration::from_secs(60), || {
        let Fixture { project: _project, server, hash, .. } = fixture();
        let daemon = Daemon::start(server.root());
        let address = daemon.address;

        let (b_step_tx, b_step_rx) = mpsc::channel::<()>();
        let (b_done_tx, b_done_rx) = mpsc::channel::<()>();
        let b = on_thread(move || async move {
            let (_client, hub) = connect(address).await;
            let snapshot = snapshot(&hub).await;
            resolve_built(&snapshot, hash).await;
            b_done_tx.send(()).unwrap();
            b_step_rx.recv().unwrap();
            resolve_built(&snapshot, hash).await;
            resolve_built(&self::snapshot(&hub).await, hash).await;
            b_done_tx.send(()).unwrap();
        });
        let _b_connection = daemon.next_connection();
        b_done_rx.recv_timeout(Duration::from_secs(10)).unwrap();

        let a = on_thread(move || async move {
            let (_client, hub) = connect(address).await;
            let snapshot = snapshot(&hub).await;
            snapshot.runtime_type_policy(PANIC).await.is_err()
        });
        let a_connection = daemon.next_connection();
        assert!(a.join().unwrap(), "the panicking call answered");
        let outcome = wait_for(Duration::from_secs(5), a_connection).expect("A's thread lingered");
        assert!(matches!(outcome, Err(capnp_transport::ConnectionPanicked(ref message)) if message.contains("panics on request")));

        // B and the listener carry on, and A's capabilities are released.
        b_step_tx.send(()).unwrap();
        b_done_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(eventually(Duration::from_secs(5), || daemon.connections() == 1));
        assert!(
            eventually(Duration::from_secs(5), || server.handle().open_connections() == 1),
            "the panicked connection kept its admission: {}",
            server.handle().open_connections()
        );
        assert!(
            eventually(Duration::from_secs(5), || server.open_snapshots() == 1),
            "A's snapshot or B's dropped one is still claimed: {}",
            server.open_snapshots()
        );
        on_thread(move || async move {
            let (_client, hub) = connect(address).await;
            resolve_built(&snapshot(&hub).await, hash).await;
        })
        .join()
        .unwrap();
        b.join().unwrap();
    });
}

// ---------------------------------------------------------------------------
// 8. Stress

/// Clients resolve, fetch, refresh, follow their delta streams, and
/// reconnect while the daemon publishes. Every answer must match the
/// version it is stamped with, and everything is released at the end.
#[test]
fn concurrent_snapshots_resolves_fetches_and_commits_stay_consistent() {
    watchdog(Duration::from_secs(120), || {
        let Fixture {
            mut project,
            server,
            backend,
            hash,
            ..
        } = fixture();
        let daemon = Daemon::start(server.root());
        let address = daemon.address;
        let base = server.current_stamp().unwrap().version;
        const CLIENTS: usize = 6;
        const COMMITS: u64 = 120;

        // Each even generation's build of `ASSET` answers an artifact of its
        // own, installed before any client resolves.
        let by_version = (1..=COMMITS)
            .filter(|generation| generation % 2 == 0)
            .map(|generation| {
                let (content_hash, payload) = artifact(ASSET, &generation.to_le_bytes());
                server.install_artifact(content_hash, payload).unwrap();
                (InputVersion(base.0 + generation), content_hash)
            })
            .collect::<BTreeMap<_, _>>();
        *backend.by_version.lock().unwrap() = by_version.clone();

        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut clients = Vec::new();
        for index in 0..CLIENTS {
            let running = Arc::clone(&running);
            clients.push(on_thread(move || async move {
                let mut observed = Vec::new();
                let mut last_delta = base;
                let mut round = 0usize;
                while running.load(std::sync::atomic::Ordering::Acquire) || round == 0 {
                    // Reconnect every few rounds: thread churn under load.
                    let (_client, hub) = connect(address).await;
                    let mut subscription =
                        match hub.subscribe(base, vec![ASSET], vec![]).await.unwrap() {
                            RemoteCall::Success(subscription) => subscription,
                            other => panic!("subscribe failed: {other:?}"),
                        };
                    let mut snapshot = snapshot(&hub).await;
                    for _ in 0..(4 + index) {
                        round += 1;
                        let stamp = snapshot.basis().snapshot.version;
                        match snapshot.resolve(ASSET).await.unwrap() {
                            RemoteCall::Success(terminal) => {
                                assert_eq!(terminal.basis.snapshot.version, stamp);
                                if let ResolveResult::Built { content_hash } = &terminal.value {
                                    let mut fetched = match snapshot.fetch(*content_hash).await.unwrap() {
                                        RemoteCall::Success(terminal) => terminal.value,
                                        other => panic!("fetch failed: {other:?}"),
                                    };
                                    let mut bytes = Vec::new();
                                    while let Some(chunk) = fetched.next_chunk().await.unwrap() {
                                        bytes.extend_from_slice(&chunk.bytes);
                                    }
                                    assert_eq!(distill_wire::artifact::content_hash(&bytes), *content_hash);
                                }
                                observed.push((stamp, terminal.value));
                            }
                            other => panic!("resolve failed: {other:?}"),
                        }
                        snapshot = match snapshot.refresh().await.unwrap() {
                            RemoteCall::Success(refreshed) => refreshed,
                            other => panic!("refresh failed: {other:?}"),
                        };
                        // Drain what the stream has without waiting long.
                        while let Ok(Ok(Some(event))) = tokio::time::timeout(
                            Duration::from_millis(1),
                            subscription.next(),
                        )
                        .await
                        {
                            let version = event.basis().snapshot.version;
                            assert!(version >= last_delta.min(version));
                            if let StreamEvent::Delta(_) = event {
                                last_delta = version;
                            }
                        }
                    }
                }
                observed
            }));
        }

        // Publish while the clients run: odd generations withhold `ASSET`
        // behind a second bundle file claiming it, even ones remove that
        // file, and its build at that version answers the generation's own
        // artifact.
        let mut generations = BTreeMap::new();
        for generation in 1..=COMMITS {
            if generation % 2 == 0 {
                project.remove(DUPLICATE_FILE);
            } else {
                write_duplicate(&mut project);
            }
            let stamp = project.publish();
            assert_eq!(stamp.version, InputVersion(base.0 + generation));
            generations.insert(stamp.version, generation);
            std::thread::sleep(Duration::from_millis(2));
        }
        running.store(false, std::sync::atomic::Ordering::Release);

        let mut checked = 0;
        for client in clients {
            for (version, value) in client.join().unwrap() {
                let expected = match generations.get(&version) {
                    None => {
                        assert_eq!(version, base);
                        ResolveResult::Built { content_hash: hash }
                    }
                    Some(generation) if generation % 2 == 0 => ResolveResult::Built {
                        content_hash: by_version[&version],
                    },
                    Some(_) => ResolveResult::Failed {
                        error: collision_error(),
                    },
                };
                assert_eq!(value, expected, "resolve at {version:?}");
                checked += 1;
            }
        }
        assert!(checked >= CLIENTS * 4);
        assert!(eventually(Duration::from_secs(5), || daemon.connections() == 0));
        assert!(eventually(Duration::from_secs(5), || {
            server.open_snapshots() == 0 && server.handle().open_connections() == 0
        }));
    });
}

// ---------------------------------------------------------------------------
// 9. Writers: every connection writes through a writer of its own

/// Writes no file: answers a receipt, after writing a row of its own (the
/// clean watermark) through the writer it is given, inside the input the
/// server rolls back. Records each base, the segment size its writer saw,
/// and the most writes it ever saw running at once.
#[derive(Default)]
struct LedgerBackend {
    bases: Mutex<Vec<InputVersion>>,
    segment_sizes: Mutex<Vec<u64>>,
    running: Mutex<(usize, usize)>,
    /// Hold each write open this long, so writes on two connections
    /// overlap.
    hold: Duration,
}

fn store_failure(error: distill_store::StoreError) -> RpcFailure {
    RpcFailure::InvalidAuthoringRequest {
        detail: error.to_string(),
    }
}

/// The served projection of the version `next`: `ASSET` fails with a
/// message naming it.
fn ledger_commit(next: InputVersion) -> Commit {
    Commit {
        assets: vec![AssetMutation::Set {
            uuid: ASSET,
            resolution: StoredResolve::Failed {
                error: format!("v{}", next.0),
            },
            delta: AssetDeltaState::Changed,
        }],
        ..Commit::default()
    }
}

/// The backend's row for the version `next` (a root named for it), written
/// through `store`.
fn ledger_row(store: &mut distill_store::Store, next: InputVersion) -> Result<(), distill_store::StoreError> {
    store
        .input_transaction(|transaction| transaction.intern_root(&ledger_root(next)))
        .map(|_| ())
}

fn ledger_root(version: InputVersion) -> String {
    format!("ledger-v{}", version.0)
}

impl AuthoringBackend for LedgerBackend {
    fn write_files(
        &self,
        store: &mut distill_store::Store,
        base: InputVersion,
        _operations: &[AuthoringOp],
        _force_lossy: bool,
    ) -> Result<Option<WriteReceipt>, RpcFailure> {
        {
            let mut running = self.running.lock().unwrap();
            running.0 += 1;
            running.1 = running.1.max(running.0);
        }
        ledger_row(store, InputVersion(base.0 + 1)).map_err(store_failure)?;
        std::thread::sleep(self.hold);
        self.bases.lock().unwrap().push(base);
        self.segment_sizes
            .lock()
            .unwrap()
            .push(store.config().segment_size);
        self.running.lock().unwrap().0 -= 1;
        Ok(Some(WriteReceipt::default()))
    }

    fn prepare_import(
        &self,
        _store: &mut distill_store::Store,
        _base: InputVersion,
        _request: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        unreachable!("the ledger backend only writes")
    }

    fn prepare_reimport(
        &self,
        _store: &mut distill_store::Store,
        _base: InputVersion,
        _bundle: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        unreachable!("the ledger backend only writes")
    }

    fn prepare_operation(
        &self,
        _store: &mut distill_store::Store,
        _base: InputVersion,
        _operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure> {
        unreachable!("the ledger backend only writes")
    }
}

/// A server of its own over a fresh project's store, writing through
/// `backend`; the project holds the daemon and its files.
fn ledger_server(backend: Arc<LedgerBackend>) -> (TestProject, Server) {
    let project = project();
    let handle = ServerHandle::open(backend, Arc::clone(project.coordinator().opener()));
    let server = Server::open(&handle);
    (project, server)
}

/// Connect, keeping the raw hub for the authoring calls `RemoteHub` does
/// not wrap.
async fn connect_writer(address: SocketAddr) -> (CapnpClient, RemoteHub, schema::hub::Client) {
    let client = CapnpClient::connect_local(address).await.unwrap();
    let raw = match client.connect(&request()).await.unwrap() {
        RemoteConnectOutcome::Connected { hub, instance } => (hub, instance),
        _ => panic!("expected a connected hub"),
    };
    let hub = RemoteHub::connected(RemoteConnectOutcome::Connected {
        hub: raw.0.clone(),
        instance: raw.1,
    })
    .unwrap();
    (client, hub, raw.0)
}

/// One write at `base`: its receipt, or the error code.
async fn write_at(hub: &schema::hub::Client, base: InputVersion) -> Result<WriteReceipt, u16> {
    let mut call = hub.write_request();
    {
        let mut params = call.get();
        params.set_base(base.0);
        let mut ops = params.reborrow().init_ops(1);
        ops.reborrow().get(0).init_remove().set_bytes(&ASSET.0);
    }
    let response = call.send().promise.await.unwrap();
    match response.get().unwrap().get_result().unwrap().which().unwrap() {
        schema::data_call::Which::Success(bytes) => {
            Ok(WriteReceipt::decode(bytes.unwrap()).unwrap())
        }
        schema::data_call::Which::Error(error) => Err(error.unwrap().get_code()),
        _ => panic!("unexpected write outcome"),
    }
}

/// One write at the current version, which succeeds.
async fn write_once(hub: &RemoteHub, raw: &schema::hub::Client) -> WriteReceipt {
    let base = snapshot(hub).await.basis().snapshot.version;
    write_at(raw, base).await.unwrap()
}

/// Two connections write at once, each on its own writer. SQLite's write
/// lock orders them: no two writes ever run at once, and none commits
/// anything (the backend's row in each input is rolled back).
#[test]
fn two_connections_writing_concurrently_are_serialized_and_commit_nothing() {
    watchdog(Duration::from_secs(60), || {
        let backend = Arc::new(LedgerBackend {
            hold: Duration::from_millis(2),
            ..LedgerBackend::default()
        });
        let (_project, server) = ledger_server(Arc::clone(&backend));
        let daemon = Daemon::start(server.root());
        let address = daemon.address;
        let start = server.current_stamp().unwrap().version;
        const WRITES: u64 = 15;

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let writers = (0..2)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                on_thread(move || async move {
                    let (_client, hub, raw) = connect_writer(address).await;
                    barrier.wait();
                    for _ in 0..WRITES {
                        assert_eq!(write_once(&hub, &raw).await, WriteReceipt::default());
                    }
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap();
        }
        assert_eq!(
            backend.running.lock().unwrap().1,
            1,
            "no two writes overlapped"
        );
        assert_eq!(
            *backend.bases.lock().unwrap(),
            vec![start; 2 * WRITES as usize]
        );
        let reader = server.handle().opener().open_reader().unwrap();
        assert_eq!(reader.input_version().unwrap(), start);
        assert_eq!(reader.root_id(&ledger_root(InputVersion(start.0 + 1))).unwrap(), None);
    });
}

/// A coordinated commit's durable step (the backend's row) and the served
/// projection it returns land in one input: a reader's snapshot sees both
/// or neither, at every version.
#[test]
fn a_coordinated_commits_backend_row_and_served_projection_land_together() {
    watchdog(Duration::from_secs(60), || {
        let (_project, server) = ledger_server(Arc::new(LedgerBackend::default()));
        let handle = server.handle();
        let start = server.current_stamp().unwrap().version;
        const COMMITS: u64 = 150;

        let committer = {
            let handle = Arc::clone(&handle);
            std::thread::spawn(move || {
                // An owner of its own: this thread's writer.
                let admin = Server::open(&handle);
                for _ in 0..COMMITS {
                    let base = admin.current_stamp().unwrap().version;
                    let next = InputVersion(base.0 + 1);
                    admin
                        .coordinated_commit(base, |store| {
                            ledger_row(store, next).map_err(|error| error.to_string())?;
                            Ok(ledger_commit(next))
                        })
                        .unwrap();
                }
            })
        };
        let mut seen = std::collections::BTreeSet::new();
        let last = InputVersion(start.0 + COMMITS);
        loop {
            let snapshot = handle
                .opener()
                .open_reader()
                .unwrap()
                .begin_snapshot()
                .unwrap();
            let version = snapshot.input_version().unwrap();
            let ledger = snapshot.root_id(&ledger_root(version)).unwrap();
            let next_ledger = snapshot.root_id(&ledger_root(InputVersion(version.0 + 1))).unwrap();
            let served = snapshot.asset_resolution(ASSET).unwrap();
            assert_eq!(next_ledger, None, "no later backend row at {version:?}");
            if version != start {
                assert!(ledger.is_some(), "backend row at {version:?}");
                assert_eq!(
                    served,
                    Some(distill_store::served::ResolutionRow::Failed(format!(
                        "v{}",
                        version.0
                    ))),
                    "served projection at {version:?}"
                );
            }
            seen.insert(version);
            if version == last {
                break;
            }
        }
        committer.join().unwrap();
        assert!(seen.len() > 2, "the reader saw versions while commits ran: {seen:?}");
    });
}

/// A connection whose write waits on SQLite's write lock blocks only
/// itself: another connection snapshots, resolves and subscribes
/// meanwhile, and the write answers once the lock is free.
#[test]
fn a_connection_blocked_on_the_write_lock_does_not_stall_another() {
    watchdog(Duration::from_secs(60), || {
        let (mut project, server) = ledger_server(Arc::new(LedgerBackend::default()));
        // `ASSET` is served failed: two bundle files claim it.
        write_asset(&mut project, 1);
        write_duplicate(&mut project);
        let start = project.publish().version;
        let daemon = Daemon::start(server.root());
        let address = daemon.address;

        // Another owner holds the write lock.
        let mut holder = server.handle().opener().open_writer().unwrap();
        holder.open_input().unwrap();

        let (written_tx, written_rx) = mpsc::channel();
        let writer = on_thread(move || async move {
            let (_client, _hub, raw) = connect_writer(address).await;
            written_tx.send(write_at(&raw, start).await).unwrap();
        });
        std::thread::sleep(Duration::from_millis(200));
        assert!(written_rx.try_recv().is_err(), "the write waits on the lock");

        let (ready_tx, ready_rx) = mpsc::channel();
        let reader = on_thread(move || async move {
            let started = Instant::now();
            let (_client, hub) = connect(address).await;
            let snapshot = snapshot(&hub).await;
            assert_eq!(snapshot.basis().snapshot.version, start);
            assert!(matches!(
                snapshot.resolve(ASSET).await.unwrap(),
                RemoteCall::Success(_)
            ));
            let mut subscription = match hub.subscribe(start, vec![ASSET], vec![]).await.unwrap() {
                RemoteCall::Success(subscription) => subscription,
                other => panic!("subscribe failed: {other:?}"),
            };
            assert!(matches!(
                subscription.next().await.unwrap(),
                Some(StreamEvent::InitialDelta { .. })
            ));
            ready_tx.send(started.elapsed()).unwrap();
        });
        let elapsed = ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(
            elapsed < Duration::from_secs(5),
            "the other connection was served at once, took {elapsed:?}"
        );
        assert!(
            written_rx.try_recv().is_err(),
            "the write still waits on the lock"
        );

        holder.finish_input(false).unwrap();
        assert_eq!(
            written_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            Ok(WriteReceipt::default())
        );
        assert_eq!(server.current_stamp().unwrap().version, start);
        writer.join().unwrap();
        reader.join().unwrap();
    });
}

/// An operational configuration change reaches a connection's writer that
/// was opened before it, from its next transaction on.
#[test]
fn a_configuration_change_reaches_open_writers() {
    watchdog(Duration::from_secs(60), || {
        let backend = Arc::new(LedgerBackend::default());
        let (_project, server) = ledger_server(Arc::clone(&backend));
        let daemon = Daemon::start(server.root());
        let address = daemon.address;
        let handle = server.handle();
        let before = handle.opener().config().segment_size;

        let (step_tx, step_rx) = mpsc::channel::<()>();
        let (done_tx, done_rx) = mpsc::channel();
        let writer = on_thread(move || async move {
            let (_client, hub, raw) = connect_writer(address).await;
            write_once(&hub, &raw).await;
            done_tx.send(()).unwrap();
            step_rx.recv().unwrap();
            write_once(&hub, &raw).await;
        });
        done_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let mut changed = distill_store::StoreConfig::clone(&handle.opener().config());
        changed.segment_size = before * 2;
        handle.opener().apply_operational_config(&changed).unwrap();
        step_tx.send(()).unwrap();
        writer.join().unwrap();
        assert_eq!(*backend.segment_sizes.lock().unwrap(), [before, before * 2]);
    });
}
