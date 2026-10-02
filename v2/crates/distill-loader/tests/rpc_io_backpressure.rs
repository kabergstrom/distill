//! Regression tests for the RPC loader IO under load: the real `RpcIo`
//! against an in-process daemon, every scenario under a watchdog.
//!
//! Each test names the hazard of the former threaded design it guards
//! against (H1-H12 in the loader deadlock diagnosis): there, these
//! scenarios hung the engine thread, starved fetches, or leaked superseded
//! work. A test that hangs fails at its watchdog instead of hanging the
//! suite.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use distill_asset::{AssetType, ErasedValue, ModuleEpochToken};
use distill_core::id::{AssetUuid, ContentHash, LayoutHash, TypeUuid};
use distill_loader::{
    AdoptionId, AssetStorage, GameModuleEpoch, HandleId, IoBasis, IoEvent, LoadStatus, Loader,
    LoaderIO, ManifestHash, PendingState, PendingToken, ReqId, RpcIo, RpcIoConfig, RpcIoStats,
    RuntimeTarget, StorageError, UpdateResult,
};
use distill_rpc::capnp_transport::StagedListener;
use distill_rpc::{
    ArtifactPayload, AssetDeltaState, AssetMutation, Commit, ConnectRequest, Server,
    StoreInstanceId, StoredResolve, TargetDefinition, TargetDefinitionHash,
};
use distill_wire::artifact::{content_hash, parse_artifact, write_artifact, ArtifactHeader};
use distill_wire::dswl::{dswl_bytes, dswl_hash};
use distill_wire::native::CallbackPanic;
use distill_wire::wire::WireNode;

#[distill_asset::asset(uuid = "51112233-4455-6677-8899-aabbccddeeff")]
struct A;

const TARGET_HASH: [u8; 32] = [7; 32];

fn target() -> RuntimeTarget {
    RuntimeTarget::new(GameModuleEpoch(1), TARGET_HASH)
}

fn request() -> ConnectRequest {
    ConnectRequest::new("dev", TargetDefinitionHash(TARGET_HASH))
}

fn asset(seed: u16) -> AssetUuid {
    let mut bytes = [0x20; 16];
    bytes[..2].copy_from_slice(&seed.to_le_bytes());
    AssetUuid(bytes)
}

/// Run `body` on its own thread (RpcIo lives on the thread that drives it);
/// fail when it has not finished within `limit`. A hung body is leaked.
fn watchdog<T: Send + 'static>(limit: Duration, body: impl FnOnce() -> T + Send + 'static) -> T {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let value = body();
        let _ = done_tx.send(value);
    });
    match done_rx.recv_timeout(limit) {
        Ok(value) => value,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("the scenario did not finish within {limit:?}: the caller was blocked")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => panic!("the scenario panicked"),
    }
}

/// A daemon serving `count` built `A` assets (each with one blob of
/// `blob_len` bytes when nonzero), accepting connections until the process
/// exits.
struct Daemon {
    server: Server,
    assets: Vec<(AssetUuid, ContentHash)>,
    address: SocketAddr,
}

fn serve(count: u16, blob_len: usize) -> Daemon {
    serve_sized(&vec![blob_len; usize::from(count)])
}

/// A daemon serving one built `A` asset per entry of `blob_lens`, with one
/// blob of that many bytes when nonzero.
fn serve_sized(blob_lens: &[usize]) -> Daemon {
    let target = TargetDefinition::new("dev", TargetDefinitionHash(TARGET_HASH));
    let server = Server::new(StoreInstanceId([6; 16]), vec![target]).unwrap();
    let wire = WireNode::Struct {
        offset: 0,
        size: 0,
        align: 1,
        fields: Vec::new(),
    };
    let layout_hash = dswl_hash(&wire).unwrap();
    server
        .install_wire_tree(layout_hash, Arc::from(dswl_bytes(&wire).unwrap()))
        .unwrap();
    let mut assets = Vec::new();
    let mut mutations = Vec::new();
    for (seed, &blob_len) in blob_lens.iter().enumerate() {
        let uuid = asset(u16::try_from(seed).unwrap());
        let blob = vec![0x5a; blob_len];
        let blobs = if blob_len == 0 {
            Vec::new()
        } else {
            vec![(Vec::new(), blob.as_slice())]
        };
        let bytes = write_artifact(
            &ArtifactHeader {
                asset_uuid: uuid,
                authored_type: A::TYPE_UUID,
                terminal_type: A::TYPE_UUID,
                encoded_type: A::TYPE_UUID,
                logical_hash: A::descriptor().logical_hash,
                layout_hash: LayoutHash(layout_hash.0),
            },
            &[],
            &[],
            &[],
            &blobs,
        )
        .unwrap();
        let structural_len = bytes.len() - parse_artifact(&bytes).unwrap().blob_section.len();
        let hash = content_hash(&bytes);
        server
            .install_artifact(
                hash,
                ArtifactPayload {
                    structural: Arc::from(bytes[..structural_len].to_vec()),
                    blobs: if blob_len == 0 {
                        Vec::new()
                    } else {
                        vec![Arc::from(blob.clone())]
                    },
                    load_edges: Vec::new(),
                },
            )
            .unwrap();
        mutations.push(AssetMutation::Set {
            uuid,
            resolution: StoredResolve::Built { content_hash: hash },
            delta: AssetDeltaState::Changed,
        });
        assets.push((uuid, hash));
    }
    server
        .commit(Commit {
            assets: mutations,
            ..Commit::default()
        })
        .unwrap();
    let (address_tx, address_rx) = std::sync::mpsc::sync_channel(1);
    let root = server.root();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let listener = StagedListener::bind(root, "127.0.0.1:0").await.unwrap();
            address_tx.send(listener.local_addr().unwrap()).unwrap();
            loop {
                let connection = listener.accept_one().await.unwrap();
                tokio::task::spawn_local(connection);
            }
        });
    });
    Daemon {
        server,
        assets,
        address: address_rx.recv().unwrap(),
    }
}

impl Daemon {
    /// Re-mark `uuid` changed, as a reimport commit does.
    fn touch(&self, uuid: AssetUuid, hash: ContentHash) {
        self.server
            .commit(Commit {
                assets: vec![AssetMutation::Set {
                    uuid,
                    resolution: StoredResolve::Built { content_hash: hash },
                    delta: AssetDeltaState::Changed,
                }],
                ..Commit::default()
            })
            .unwrap();
    }
}

/// A TCP relay in front of the daemon that can stall (stop forwarding,
/// keep sockets open) or cut (close) its connections.
struct Relay {
    address: SocketAddr,
    stalled: Arc<AtomicBool>,
    accepted: Arc<AtomicUsize>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
}

impl Relay {
    fn new(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let relay = Self {
            address: listener.local_addr().unwrap(),
            stalled: Arc::new(AtomicBool::new(false)),
            accepted: Arc::new(AtomicUsize::new(0)),
            sockets: Arc::new(Mutex::new(Vec::new())),
        };
        let stalled = Arc::clone(&relay.stalled);
        let accepted = Arc::clone(&relay.accepted);
        let sockets = Arc::clone(&relay.sockets);
        std::thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { return };
                accepted.fetch_add(1, Ordering::SeqCst);
                let server = TcpStream::connect(upstream).unwrap();
                sockets.lock().unwrap().push(client.try_clone().unwrap());
                sockets.lock().unwrap().push(server.try_clone().unwrap());
                for (from, to) in [
                    (client.try_clone().unwrap(), server.try_clone().unwrap()),
                    (server, client),
                ] {
                    let stalled = Arc::clone(&stalled);
                    std::thread::spawn(move || pump(from, to, stalled));
                }
            }
        });
        relay
    }

    fn stall(&self) {
        self.stalled.store(true, Ordering::SeqCst);
    }

    fn resume(&self) {
        self.stalled.store(false, Ordering::SeqCst);
    }

    /// Close every relayed connection.
    fn cut(&self) {
        for socket in self.sockets.lock().unwrap().drain(..) {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

fn pump(mut from: TcpStream, mut to: TcpStream, stalled: Arc<AtomicBool>) {
    from.set_read_timeout(Some(Duration::from_millis(5))).unwrap();
    let mut buffer = [0u8; 16 * 1024];
    loop {
        if stalled.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(5));
            continue;
        }
        match from.read(&mut buffer) {
            Ok(0) => {
                let _ = to.shutdown(Shutdown::Write);
                return;
            }
            Ok(read) => {
                if to.write_all(&buffer[..read]).is_err() {
                    return;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => {
                let _ = to.shutdown(Shutdown::Write);
                return;
            }
        }
    }
}

/// Connect, bind the target, and return the bound IO with its sweep basis.
fn bound(address: SocketAddr, config: RpcIoConfig) -> (RpcIo, IoBasis) {
    let mut io = RpcIo::connect_with_config(address, request(), config).unwrap();
    io.bind_target(target());
    let deadline = Instant::now() + Duration::from_secs(3);
    while !io
        .poll()
        .iter()
        .any(|event| matches!(event, IoEvent::TargetBound { .. }))
    {
        assert!(Instant::now() < deadline, "target never bound");
        std::thread::sleep(Duration::from_millis(1));
    }
    let basis = io.begin_sweep();
    (io, basis)
}

/// Poll as frames would until `done` holds over everything delivered.
fn frames_until(
    io: &mut RpcIo,
    limit: Duration,
    mut done: impl FnMut(&[IoEvent]) -> bool,
) -> (Vec<IoEvent>, usize) {
    let deadline = Instant::now() + limit;
    let mut events = Vec::new();
    let mut frames = 0;
    while !done(&events) {
        assert!(
            Instant::now() < deadline,
            "not done after {frames} frames: {} events",
            events.len()
        );
        events.extend(io.poll());
        frames += 1;
        std::thread::sleep(Duration::from_millis(1));
    }
    (events, frames)
}

fn count(events: &[IoEvent], kind: fn(&IoEvent) -> bool) -> usize {
    events.iter().filter(|event| kind(event)).count()
}

fn is_fetched(event: &IoEvent) -> bool {
    matches!(event, IoEvent::Fetched { .. })
}

fn is_resolved(event: &IoEvent) -> bool {
    matches!(event, IoEvent::Resolved { .. })
}

fn is_request_error(event: &IoEvent) -> bool {
    matches!(event, IoEvent::RequestError { .. })
}

fn stale() -> IoBasis {
    IoBasis::Pack {
        manifest: ManifestHash([0; 32]),
    }
}

fn with_budget(fetch_memory_budget: usize) -> RpcIoConfig {
    RpcIoConfig {
        fetch_memory_budget,
        ..RpcIoConfig::default()
    }
}

/// The position of `req`'s `Fetched` event in `events`.
fn fetched_at(events: &[IoEvent], req: ReqId) -> Option<usize> {
    events
        .iter()
        .position(|event| matches!(event, IoEvent::Fetched { req: fetched, .. } if *fetched == req))
}

/// Step without taking anything (the engine is busy elsewhere) until
/// `done` holds over the IO's stats.
fn step_until(io: &mut RpcIo, limit: Duration, mut done: impl FnMut(RpcIoStats) -> bool) {
    let deadline = Instant::now() + limit;
    while !done(io.stats()) {
        assert!(Instant::now() < deadline, "not reached: {:?}", io.stats());
        api::step(io);
        std::thread::sleep(Duration::from_millis(1));
    }
}

// H1, H7: a backlog of superseded fetches never blocks the next sweep, and
// ending the old sweep frees the IO for the new one.
#[test]
fn a_superseded_fetch_backlog_never_blocks_the_next_sweep() {
    watchdog(Duration::from_secs(20), || {
        let daemon = serve(1, 0);
        let (mut io, basis) = bound(daemon.address, RpcIoConfig::default());
        let hash = daemon.assets[0].1;
        for req in 0..300 {
            io.fetch(ReqId(req), hash, &basis);
        }
        std::thread::sleep(Duration::from_millis(300));
        drop(io.poll());
        api::end_sweep(&mut io, &basis);
        let next = io.begin_sweep();
        io.fetch(ReqId(1_000), hash, &next);
        let (events, _) = frames_until(&mut io, Duration::from_secs(5), |events| {
            events
                .iter()
                .any(|event| matches!(event, IoEvent::Fetched { req: ReqId(1_000), .. }))
        });
        assert!(
            !events.iter().any(|event| matches!(
                event,
                IoEvent::Fetched { req, .. } | IoEvent::RequestError { req, .. } if req.0 < 300
            )),
            "an ended sweep's fetches were still delivered"
        );
    });
}

// H2, H3, H4, H9: no number of requests blocks the caller, every request is
// answered, and the IO starts only a bounded number at once.
#[test]
fn request_floods_never_block_the_caller_and_are_all_answered() {
    watchdog(Duration::from_secs(30), || {
        let daemon = serve(1, 0);
        let (mut io, basis) = bound(daemon.address, RpcIoConfig::default());
        let uuid = daemon.assets[0].0;
        for req in 0..1_000 {
            io.resolve(ReqId(req), uuid, &stale());
        }
        for req in 1_000..1_600 {
            io.resolve(ReqId(req), uuid, &basis);
        }
        for req in 1_600..1_900 {
            io.fetch(ReqId(req), daemon.assets[0].1, &basis);
        }
        let _ = io.begin_sweep();
        api::step(&mut io);
        let running = api::running(&io);
        assert!(running <= 256, "{running} requests started at once");
        let (events, _) = frames_until(&mut io, Duration::from_secs(20), |events| {
            events.len() >= 1_900
        });
        assert_eq!(count(&events, is_request_error), 1_000);
        assert_eq!(count(&events, is_resolved), 600);
        assert_eq!(count(&events, is_fetched), 300);
    });
}

// H5: the engine's frame never waits on the daemon. With a deep backlog in
// flight, every `poll` returns within the step budget plus one turn.
#[test]
fn poll_stays_within_its_step_budget_under_a_backlog() {
    watchdog(Duration::from_secs(30), || {
        let daemon = serve(1, 256 * 1024);
        let (mut io, basis) = bound(daemon.address, RpcIoConfig::default());
        let (uuid, hash) = daemon.assets[0];
        for req in 0..400 {
            io.fetch(ReqId(req), hash, &basis);
            io.resolve(ReqId(10_000 + req), uuid, &basis);
        }
        let mut slowest = Duration::ZERO;
        let mut delivered = 0;
        let deadline = Instant::now() + Duration::from_secs(20);
        while delivered < 800 {
            assert!(Instant::now() < deadline, "only {delivered}/800 delivered");
            let started = Instant::now();
            delivered += io.poll().len();
            slowest = slowest.max(started.elapsed());
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            slowest < Duration::from_millis(40),
            "a poll took {slowest:?} with a 2 ms step budget"
        );
    });
}

// H6: fetches are answered as fast as the daemon serves them, not one per
// two frames.
#[test]
fn fetch_throughput_is_not_one_per_two_frames() {
    watchdog(Duration::from_secs(30), || {
        let daemon = serve(1, 0);
        let (mut io, basis) = bound(daemon.address, RpcIoConfig::default());
        let hash = daemon.assets[0].1;
        for req in 0..200 {
            io.fetch(ReqId(req), hash, &basis);
        }
        let (events, frames) = frames_until(&mut io, Duration::from_secs(20), |events| {
            count(events, is_fetched) == 200
        });
        assert_eq!(events.len(), 200);
        assert!(
            frames < 100,
            "200 fetches took {frames} frames; one per two frames would take 400"
        );
    });
}

// H7: ending a sweep cancels its queued and running requests: nothing more
// is delivered for them and the payloads, admission waits, and reservations
// they held are released.
#[test]
fn ending_a_sweep_cancels_its_requests_and_releases_what_they_held() {
    watchdog(Duration::from_secs(20), || {
        let daemon = serve(1, 64 * 1024);
        // Room for one payload: the other running fetches wait for admission.
        let (mut io, basis) = bound(daemon.address, with_budget(100 * 1024));
        let (uuid, hash) = daemon.assets[0];
        for req in 0..300 {
            io.fetch(ReqId(req), hash, &basis);
            io.resolve(ReqId(1_000 + req), uuid, &basis);
        }
        // Some answered and undelivered, some running, some queued.
        for _ in 0..5 {
            api::step(&mut io);
            std::thread::sleep(Duration::from_millis(2));
        }
        api::end_sweep(&mut io, &basis);
        let deadline = Instant::now() + Duration::from_millis(500);
        while Instant::now() < deadline {
            let events = io.poll();
            assert!(
                events.is_empty(),
                "events delivered for an ended sweep: {}",
                events.len()
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(api::in_flight(&io), 0);
        assert_eq!(api::resident(&io), 0);
        assert_eq!(io.stats().waiting_fetches, 0, "a cancelled fetch kept its admission wait");
        // The IO still serves the next sweep.
        let next = io.begin_sweep();
        io.fetch(ReqId(5_000), hash, &next);
        frames_until(&mut io, Duration::from_secs(5), |events| {
            count(events, is_fetched) == 1
        });
    });
}

// H11: a daemon that accepts but never answers cannot hang startup.
#[test]
fn connect_to_a_stalled_daemon_times_out() {
    watchdog(Duration::from_secs(10), || {
        let daemon = serve(1, 0);
        let relay = Relay::new(daemon.address);
        relay.stall();
        let started = Instant::now();
        let connected = RpcIo::connect_with_config(
            relay.address,
            request(),
            api::with_connect_timeout(Duration::from_millis(500)),
        );
        assert!(connected.is_err(), "connected through a stalled relay");
        assert!(started.elapsed() < Duration::from_secs(3));
    });
}

// H12: repeated reports of one lost connection do not restart the rebind:
// attempts keep their backoff and failure count, so the rejection is
// reported after `target_rejection_after` failed attempts.
#[test]
fn repeated_binds_to_one_target_do_not_storm_the_daemon() {
    watchdog(Duration::from_secs(30), || {
        let daemon = serve(1, 0);
        let relay = Relay::new(daemon.address);
        let (mut io, _) = bound(
            relay.address,
            RpcIoConfig {
                target_rejection_after: 3,
                ..RpcIoConfig::default()
            },
        );
        let before = relay.accepted();
        relay.stall();
        // Three 2 s attempts with backoff between them.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut rejected = false;
        while !rejected {
            assert!(
                Instant::now() < deadline,
                "a rebind stalled by the daemon was never reported"
            );
            // The loader rebinds once per ReconnectRequired it sees.
            io.bind_target(target());
            rejected = io
                .poll()
                .iter()
                .any(|event| matches!(event, IoEvent::TargetRejected { .. }));
            std::thread::sleep(Duration::from_millis(2));
        }
        let attempts = relay.accepted() - before;
        assert!(attempts <= 4, "{attempts} connection attempts for one rebind");
        relay.resume();
        frames_until(&mut io, Duration::from_secs(5), |events| {
            events
                .iter()
                .any(|event| matches!(event, IoEvent::TargetBound { .. }))
        });
    });
}

// H10: dropping the IO never waits for the daemon, whatever is in flight.
#[test]
fn drop_is_prompt_with_a_stalled_daemon_and_a_backlog() {
    let elapsed = watchdog(Duration::from_secs(20), || {
        let daemon = serve(1, 0);
        let relay = Relay::new(daemon.address);
        let (mut io, basis) = bound(relay.address, RpcIoConfig::default());
        let (uuid, hash) = daemon.assets[0];
        for req in 0..300 {
            io.fetch(ReqId(req), hash, &basis);
            io.resolve(ReqId(1_000 + req), uuid, &basis);
        }
        relay.stall();
        io.subscribe(uuid);
        for _ in 0..20 {
            io.poll();
            std::thread::sleep(Duration::from_millis(2));
        }
        let started = Instant::now();
        drop(io);
        started.elapsed()
    });
    assert!(
        elapsed < Duration::from_millis(200),
        "dropping RpcIO took {elapsed:?}"
    );
}

// Fetch admission ---------------------------------------------------------
//
// A fetch reserves its payload's bytes before reading its stream, and waits
// (reading nothing) while they do not fit; payloads live only in memory.

/// Poll as frames would until `count` fetches are delivered, recording the
/// most payload bytes held and whether any fetch waited for admission.
fn fetch_frames(io: &mut RpcIo, count: usize, limit: Duration) -> (Vec<IoEvent>, usize, bool) {
    let deadline = Instant::now() + limit;
    let mut events = Vec::new();
    let mut peak = 0;
    let mut waited = false;
    while self::count(&events, is_fetched) < count {
        assert!(
            Instant::now() < deadline,
            "{}/{count} fetched: {:?}",
            self::count(&events, is_fetched),
            io.stats()
        );
        // Measure before `poll` takes (and releases) what was fetched.
        api::step(io);
        let stats = io.stats();
        peak = peak.max(stats.resident_fetch_bytes);
        waited |= stats.waiting_fetches > 0;
        events.extend(io.poll());
        std::thread::sleep(Duration::from_millis(1));
    }
    (events, peak, waited)
}

fn assert_blobs(events: &[IoEvent], len: usize) {
    for event in events {
        if let IoEvent::Fetched { artifact, .. } = event {
            assert_eq!(artifact.blobs.len(), 1);
            assert!(artifact.blobs[0].as_bytes() == vec![0x5a; len].as_slice());
        }
    }
}

/// Slack over the budget for the DSWL trees of admitted payloads, which
/// grow a reservation without waiting.
const DSWL_SLACK: usize = 4 * 1024;

#[test]
fn concurrent_small_fetches_under_a_tiny_budget_wait_their_turn_and_all_complete() {
    watchdog(Duration::from_secs(30), || {
        const BLOB: usize = 16 * 1024;
        // Room for two payloads; 32 fetches run at once.
        let budget = 40 * 1024;
        let daemon = serve(8, BLOB);
        let (mut io, basis) = bound(daemon.address, with_budget(budget));
        for req in 0..200 {
            io.fetch(ReqId(req), daemon.assets[req as usize % 8].1, &basis);
        }
        let (events, peak, waited) = fetch_frames(&mut io, 200, Duration::from_secs(20));
        assert_eq!(events.len(), 200, "{events:?}");
        assert!(waited, "no fetch ever waited for admission");
        assert!(
            peak <= budget + DSWL_SLACK,
            "{peak} payload bytes held with a {budget} byte budget"
        );
        assert_blobs(&events, BLOB);
        assert_eq!(api::resident(&io), 0);
        assert_eq!(io.stats().waiting_fetches, 0);
    });
}

#[test]
fn a_payload_larger_than_the_whole_budget_is_fetched_alone() {
    watchdog(Duration::from_secs(30), || {
        const BLOB: usize = 256 * 1024;
        let budget = 64 * 1024;
        let daemon = serve(1, BLOB);
        let (mut io, basis) = bound(daemon.address, with_budget(budget));
        let hash = daemon.assets[0].1;
        for req in 0..4 {
            io.fetch(ReqId(req), hash, &basis);
        }
        let (events, peak, waited) = fetch_frames(&mut io, 4, Duration::from_secs(20));
        assert_eq!(events.len(), 4, "{events:?}");
        assert!(waited);
        assert!(
            peak >= BLOB && peak <= BLOB + DSWL_SLACK,
            "{peak} payload bytes held: oversized payloads must be held one at a time"
        );
        assert_blobs(&events, BLOB);
        assert_eq!(api::resident(&io), 0);
    });
}

#[test]
fn a_fetch_waiting_for_admission_resumes_once_poll_takes_the_held_payload() {
    watchdog(Duration::from_secs(20), || {
        let daemon = serve(2, 64 * 1024);
        // Room for one payload.
        let (mut io, basis) = bound(daemon.address, with_budget(100 * 1024));
        io.fetch(ReqId(1), daemon.assets[0].1, &basis);
        step_until(&mut io, Duration::from_secs(5), |stats| {
            stats.undelivered_events == 1
        });
        io.fetch(ReqId(2), daemon.assets[1].1, &basis);
        step_until(&mut io, Duration::from_secs(5), |stats| {
            stats.waiting_fetches == 1
        });
        // However long the engine leaves the first payload untaken, the
        // second fetch stays parked: nothing on the IO side frees memory.
        for _ in 0..50 {
            api::step(&mut io);
            std::thread::sleep(Duration::from_millis(1));
        }
        let stats = io.stats();
        assert_eq!(stats.waiting_fetches, 1, "{stats:?}");
        assert_eq!(stats.undelivered_events, 1, "{stats:?}");
        assert_eq!(stats.in_flight_fetches, 1, "{stats:?}");
        let first = io.poll();
        assert_eq!(first.len(), 1, "{first:?}");
        assert!(fetched_at(&first, ReqId(1)).is_some());
        // Taking it released the budget: the waiter resumes.
        assert_eq!(io.stats().waiting_fetches, 0);
        frames_until(&mut io, Duration::from_secs(5), |events| {
            fetched_at(events, ReqId(2)).is_some()
        });
        assert_eq!(api::resident(&io), 0);
    });
}

#[test]
fn ending_a_sweep_cancels_a_fetch_waiting_for_admission() {
    watchdog(Duration::from_secs(20), || {
        let daemon = serve(2, 64 * 1024);
        let (mut io, basis) = bound(daemon.address, with_budget(100 * 1024));
        io.fetch(ReqId(1), daemon.assets[0].1, &basis);
        step_until(&mut io, Duration::from_secs(5), |stats| {
            stats.undelivered_events == 1
        });
        io.fetch(ReqId(2), daemon.assets[1].1, &basis);
        step_until(&mut io, Duration::from_secs(5), |stats| {
            stats.waiting_fetches == 1
        });
        api::end_sweep(&mut io, &basis);
        let deadline = Instant::now() + Duration::from_millis(200);
        while Instant::now() < deadline {
            let events = io.poll();
            assert!(events.is_empty(), "delivered for an ended sweep: {events:?}");
            std::thread::sleep(Duration::from_millis(2));
        }
        let stats = io.stats();
        assert_eq!(stats.waiting_fetches, 0, "{stats:?}");
        assert_eq!(stats.in_flight_fetches, 0, "{stats:?}");
        assert_eq!(api::in_flight(&io), 0);
        assert_eq!(api::resident(&io), 0);
        let next = io.begin_sweep();
        io.fetch(ReqId(3), daemon.assets[1].1, &next);
        frames_until(&mut io, Duration::from_secs(5), |events| {
            fetched_at(events, ReqId(3)).is_some()
        });
    });
}

#[test]
fn dropping_the_io_with_fetches_waiting_for_admission_is_prompt() {
    let elapsed = watchdog(Duration::from_secs(20), || {
        let daemon = serve(1, 64 * 1024);
        let (mut io, basis) = bound(daemon.address, with_budget(100 * 1024));
        for req in 0..40 {
            io.fetch(ReqId(req), daemon.assets[0].1, &basis);
        }
        step_until(&mut io, Duration::from_secs(5), |stats| {
            stats.waiting_fetches >= 2
        });
        let started = Instant::now();
        drop(io);
        started.elapsed()
    });
    assert!(
        elapsed < Duration::from_millis(200),
        "dropping RpcIO took {elapsed:?}"
    );
}

#[test]
fn a_large_waiting_payload_is_admitted_before_later_small_ones() {
    watchdog(Duration::from_secs(20), || {
        const KIB: usize = 1024;
        let daemon = serve_sized(&[40 * KIB, 90 * KIB, 20 * KIB, 20 * KIB]);
        let (mut io, basis) = bound(daemon.address, with_budget(100 * KIB));
        io.fetch(ReqId(1), daemon.assets[0].1, &basis);
        step_until(&mut io, Duration::from_secs(5), |stats| {
            stats.undelivered_events == 1
        });
        io.fetch(ReqId(2), daemon.assets[1].1, &basis);
        step_until(&mut io, Duration::from_secs(5), |stats| {
            stats.waiting_fetches == 1
        });
        io.fetch(ReqId(3), daemon.assets[2].1, &basis);
        io.fetch(ReqId(4), daemon.assets[3].1, &basis);
        // The small payloads would fit beside the held 40 KiB, but they
        // queue behind the large one.
        step_until(&mut io, Duration::from_secs(5), |stats| {
            stats.waiting_fetches == 3
        });
        for _ in 0..20 {
            api::step(&mut io);
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(io.stats().waiting_fetches, 3);
        let (events, _) = frames_until(&mut io, Duration::from_secs(10), |events| {
            count(events, is_fetched) == 4
        });
        let large = fetched_at(&events, ReqId(2)).unwrap();
        assert!(fetched_at(&events, ReqId(1)).unwrap() < large);
        assert!(large < fetched_at(&events, ReqId(3)).unwrap(), "{events:?}");
        assert!(large < fetched_at(&events, ReqId(4)).unwrap(), "{events:?}");
    });
}

// The loader end to end ---------------------------------------------------

#[derive(Default)]
struct Storage {
    values: BTreeMap<(HandleId, AdoptionId), ErasedValue>,
}

impl AssetStorage for Storage {
    fn update(
        &mut self,
        _type_uuid: TypeUuid,
        handle: HandleId,
        value: ErasedValue,
        adoption: AdoptionId,
    ) -> Result<UpdateResult, StorageError> {
        self.values.insert((handle, adoption), value);
        Ok(UpdateResult::Ready)
    }

    fn poll(&mut self, _token: PendingToken) -> PendingState {
        PendingState::Ready
    }

    fn commit(&mut self, _type_uuid: TypeUuid, _handle: HandleId, _adoption: AdoptionId) {}

    fn free(
        &mut self,
        _type_uuid: TypeUuid,
        handle: HandleId,
        adoption: AdoptionId,
    ) -> Result<(), CallbackPanic> {
        if let Some(value) = self.values.remove(&(handle, adoption)) {
            value.destroy()?;
        }
        Ok(())
    }
}

struct Game {
    daemon: Daemon,
    loader: Loader<RpcIo>,
    handles: Vec<distill_loader::Handle<A>>,
    storage: Storage,
    slowest: Duration,
}

impl Game {
    fn new(daemon: Daemon, address: SocketAddr) -> Self {
        Self::with_config(daemon, address, RpcIoConfig::default())
    }

    fn with_config(daemon: Daemon, address: SocketAddr, config: RpcIoConfig) -> Self {
        let io = RpcIo::connect_with_config(address, request(), config).unwrap();
        let mut loader = Loader::new(io);
        loader
            .register_types(
                GameModuleEpoch(1),
                ModuleEpochToken::new(1),
                TARGET_HASH,
                &[A::descriptor()],
            )
            .unwrap();
        let handles = daemon
            .assets
            .iter()
            .map(|(uuid, _)| loader.add_ref::<A>(*uuid).unwrap())
            .collect();
        Self {
            daemon,
            loader,
            handles,
            storage: Storage::default(),
            slowest: Duration::ZERO,
        }
    }

    fn frame(&mut self) {
        let started = Instant::now();
        self.loader.process(&mut self.storage).unwrap();
        self.slowest = self.slowest.max(started.elapsed());
        self.loader.take_diagnostics();
        std::thread::sleep(Duration::from_millis(2));
    }

    fn loaded(&self) -> usize {
        self.handles
            .iter()
            .filter(|handle| self.loader.status(handle) == LoadStatus::Loaded)
            .count()
    }

    /// Frames until every handle is loaded.
    fn settle(&mut self, limit: Duration) {
        let deadline = Instant::now() + limit;
        while self.loaded() < self.handles.len() {
            assert!(
                Instant::now() < deadline,
                "{}/{} loaded",
                self.loaded(),
                self.handles.len()
            );
            self.frame();
        }
    }

    /// `frames` frames with a reimport commit every `period` frames.
    fn churn(&mut self, frames: usize, period: usize) {
        for frame in 0..frames {
            if frame % period == 0 {
                let (uuid, hash) = self.daemon.assets[frame % self.daemon.assets.len()];
                self.daemon.touch(uuid, hash);
            }
            self.frame();
        }
    }
}

// H1 through the orchestrator, then H5: commit churn (a mass reimport)
// never stalls a frame, and once it stops everything loads.
#[test]
fn loader_frames_stay_short_under_commit_churn_and_everything_loads_after() {
    let slowest = watchdog(Duration::from_secs(60), || {
        let daemon = serve(12, 0);
        let address = daemon.address;
        let mut game = Game::new(daemon, address);
        game.churn(600, 2);
        game.settle(Duration::from_secs(10));
        game.slowest
    });
    assert!(
        slowest < Duration::from_millis(50),
        "a frame's process() took {slowest:?}"
    );
}

#[test]
fn loader_loads_everything_cold_without_churn() {
    watchdog(Duration::from_secs(30), || {
        let daemon = serve(64, 0);
        let address = daemon.address;
        let mut game = Game::new(daemon, address);
        game.settle(Duration::from_secs(10));
    });
}

// Admission through the orchestrator: with room for about one payload at a
// time, fetches wait for the payloads the loader adopts and everything loads.
#[test]
fn loader_loads_everything_cold_with_a_tiny_fetch_budget() {
    watchdog(Duration::from_secs(30), || {
        let daemon = serve(64, 32 * 1024);
        let address = daemon.address;
        let mut game = Game::with_config(daemon, address, with_budget(40 * 1024));
        game.settle(Duration::from_secs(20));
    });
}

// Reconnect: the daemon connection closes mid-session; the loader rebinds,
// reloads, and still follows later changes.
#[test]
fn loader_recovers_from_a_lost_connection() {
    watchdog(Duration::from_secs(30), || {
        let daemon = serve(12, 0);
        let relay = Relay::new(daemon.address);
        let mut game = Game::new(daemon, relay.address);
        game.settle(Duration::from_secs(10));
        let before = relay.accepted();
        relay.cut();
        let deadline = Instant::now() + Duration::from_secs(10);
        while relay.accepted() == before {
            assert!(Instant::now() < deadline, "the loader never reconnected");
            game.frame();
        }
        game.churn(40, 4);
        game.settle(Duration::from_secs(10));
    });
}

/// What these tests read beyond the `LoaderIO` boundary.
mod api {
    use super::*;

    pub fn end_sweep(io: &mut RpcIo, basis: &IoBasis) {
        io.end_sweep(basis);
    }

    pub fn step(io: &mut RpcIo) {
        io.step();
    }

    pub fn in_flight(io: &RpcIo) -> usize {
        let stats = io.stats();
        stats.queued_requests + stats.in_flight_requests
    }

    pub fn running(io: &RpcIo) -> usize {
        io.stats().in_flight_requests
    }

    pub fn resident(io: &RpcIo) -> usize {
        io.stats().resident_fetch_bytes
    }

    pub fn with_connect_timeout(timeout: Duration) -> RpcIoConfig {
        RpcIoConfig {
            connect_timeout: timeout,
            ..RpcIoConfig::default()
        }
    }
}
