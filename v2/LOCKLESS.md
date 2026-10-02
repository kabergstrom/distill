# Lockless, database-first Distill daemon

Status: plan of record for branch `lockless`. It replaces the current
shared-memory architecture. The inventory in §2 comes from reading the code
at f10f599, not from the older design documents.

## 1. Principles

1. **Zero `Mutex` / `RwLock` / `Condvar`** in `distill-store`,
   `distill-rpc`, and `distill-daemon`.
   - Threads communicate by message passing (std/tokio channels).
   - Atomics are allowed only for process shutdown and for FFI panic
     latches.
2. **SQLite is the only authoritative state.** Every value in memory is one
   of three kinds:
   - an *immutable content-addressed* value (keyed by its hash, shared by
     `Arc`; for example parsed bundles by file hash, loaded pipeline epochs
     by dylib hash);
   - *owned by exactly one thread* and fully reconstructible from the
     database;
   - *ephemeral runtime*, which has no meaning after a restart: sockets,
     RPC subscriptions, dlopen handles, lease timers.

   There are no materialized views of database content in memory.
3. **One SQLite connection per thread.** Exactly one thread, the
   *authority*, owns the write connection and the CAS append state. Every
   other thread opens its own read-only connection (WAL).
4. **One state change = one transaction.** Validation and every fallible
   step run *before* `COMMIT`. After a commit, the only work left is
   infallible: sending notifications.
5. **A snapshot handle is an open read transaction.**
   - A client's snapshot lease owns its own read-only connection with a
     `BEGIN` + first read, so it sees one committed `input_version` for as
     long as the lease exists. When the lease ends, the transaction ends.
   - Tables stay current-state. There are no versioned rows and no way to
     address an arbitrary historical version.
   - A snapshot is a **metadata** snapshot. It does not pin file contents
     or build inputs. A cook or load that finds its data changed (and no CAS
     blob for it) fails as `Drifted`, and the client retries with a new
     snapshot. A build is a pure function of its inputs, not of a
     snapshot: it is keyed by its static inputs and shared by every
     requester of that key, and each requester gets its answer at its own
     snapshot, `Drifted` where the build's traced inputs differ there
     (§3 Builds).
   - Clients normally hold one snapshot, but nothing enforces it; the lease
     cap bounds open connections.
6. **Publication is a transaction.** It bumps `input_version`, writes the
   current-state rows, and appends `change_log` rows. The authority then
   broadcasts the new change-log sequence (`tokio::sync::watch`). Each
   subscriber catches up by reading `change_log WHERE seq > cursor` on its
   front end's own connection. `change_log` is trimmed by count; the oldest
   available cursor lives in `store_meta`, and a cursor older than it gets a
   reconnect event.

## 2. What exists today, and where it goes

### 2.1 Locks

There are about 45 distinct lock fields (67 lock-typed struct fields). The
contended core:

| Lock | Holders | Replacement |
|---|---|---|
| `Server.inner: Mutex<ServerState>` (the "publication lock") | RPC thread, `spawn_blocking` resolves, coordinator, lease-expiry thread, pack/doctor | The authority's write transaction (publication) plus per-thread reads; see §2.2 |
| `Arc<Mutex<Store>>` (one connection for everyone, in 10 structs) | all threads | `StoreWriter` on the authority, a `StoreReader` per thread |
| `scan_snapshot`, `scan_projection` | coordinator, RPC (writes/imports), build pool (import reads) | tables, §2.3 |
| `import_watch_index` | coordinator, RPC | tables, §2.3 |
| `pipeline: Mutex<CoordinatedPipelineRuntime>` | coordinator, RPC, pool | immutable `Arc<PipelineRuntime>` owned by the authority and passed along with each job |
| `operational: Mutex` + `operational_wake: Condvar` | RPC, pool | the authority owns the scheduler; the pool is fed by a channel |
| `ConnectionState` ×6 copies, `ViewLease.*`, `LeaseExpiryScheduler` + thread | RPC, expiry thread, publishers | RPC-thread-owned (`Rc<RefCell<…>>` on the `LocalSet`) plus `tokio::time` |
| epoch `lifecycle` / `registration_arena` / `module` | all | frozen, immutable registry after `register`; unload on the last `Arc` drop, routed to the authority |
| `WatcherQueue` | watcher, coordinator | the watcher sends batches to the authority inbox |
| `active_builds: Mutex<usize>` | RPC | the authority knows its outstanding jobs, so GC and compaction are scheduled by it |

There are also known live lock-order inversions today. These go away with
the locks:

- `reconcile_incremental` takes scan state before the server lock. A
  stopgap fix is on `reconcile-lock-order` and cherry-picked here.
- `publish_configuration_candidate` holds `pipeline` and then takes the
  server lock, while an RPC `write` under the server lock takes `pipeline`
  via `pipeline_snapshot()`.
- `directory_import_tasks_inner` locks store → import index → scan
  snapshot, while `execute_import` locks store → scan snapshot.

### 2.2 RPC `ServerState` / `VersionView`

Today `VersionView` is cloned whole on every commit, and old versions
survive only in lease `Arc`s. The `pipeline` field is an
`Arc<RwLock<PipelineDiagnostic>>` that is mutated in place, so a poison
leaks into old snapshots.

Every field is served from current-state tables, read inside the lease's
read transaction:

| Field | Destination |
|---|---|
| `current` | `store_meta.input_version`. `advance_empty_version` also becomes a real transaction. |
| `views[*].assets` (Built/Drifted/Failed/Deleted{at}) | An `assets` row → `Drifted{Asset}`; a derived output → `Drifted{Asset(parent)}`; otherwise a row in new table `asset_resolutions(asset, kind, content_hash, error, deleted_at)`; no row → Missing. |
| `views[*].authoring` | `bundles` / `assets` / `asset_tags` / `schemas`. New `assets` columns hold the encoded authored value (canonical JSON + blobs) and `terminal_type`, so a snapshot never reads a file. |
| `views[*].paths` | `path_index`; several roots at one path is the ambiguity. |
| `views[*].derived_outputs` | `derived_outputs` (+ `terminal_type` column) |
| `tag_poisons`, `version_poison`, `configuration`, `pipeline` | `asset_tag_index.poison`, the persisted version poison, `configuration_state`, `pipeline_state` (then §4's `errors`) |
| `lineage_repair` | new persisted repair-state columns next to `configuration_state` |
| `history`, `oldest_available_cursor` | `change_log(seq, version, kind, subject)` trimmed to 4096 rows; the oldest cursor in `store_meta` |
| `build_results` (cleared on every commit) | `resolutions(version, target, asset, outcome)`, written by the authority after a build, pruned on publication |
| `artifacts` (hash → asset, layout, load edges) | The DSTL bytes in the CAS already carry everything but `ServedLoadEdge.expected_terminal`, which goes in `artifact_load_edges(content_hash, asset, expected_terminal)`, written in the CAS index transaction. |
| `wire_trees` | presence in `cas_extents` |
| `pipeline_generation`, `targets[*].target_generation`, `protocol_epoch` | `store_meta` / `pipeline_target_set` columns |
| `restart_required_keys` | `pending_restart` |
| `connections`, subscriptions, queues, `ViewLease`, pack sessions | ephemeral, owned by the RPC front end. Durable pins stay in `pins`. |

`Commit` survives for now as the typed publication delta. It is applied to
these tables inside the publishing `input_transaction`, before `COMMIT`,
which removes the commit-then-fallible sites in `commit_locked`. The
store-less `Server::new` test mode becomes an embedded mode: the server
owns a tempdir store and `server.commit(Commit)` maps onto the same
tables, so the ~50 rpc test call sites keep working.

### 2.3 Daemon state

| State | Today | Destination |
|---|---|---|
| `ScanSnapshot.files` | memory, partly mirrored in `files` | `files` (+ `raw_path` bytes, `symlink_target`) |
| `ScanSnapshot.bundles` (full bytes + parsed `Bundle`) | memory | read from disk, verify `bundles.content_hash`, parse through the content-addressed cache |
| directory observations, symlink aliases, scan diagnostics | memory only | tables `directories`, `symlink_aliases`, and `errors` (scope = file) |
| `ScanProjectionIndex` (claimants, collisions, pending sets) | memory only; only the outcome reaches SQLite | `asset_claims(asset_uuid, bundle_uuid)` with no uniqueness constraint. Collisions are queries (`GROUP BY … HAVING count > 1`) that produce per-asset error rows, and the pending sets disappear (one transaction). |
| `ImportWatchIndex` | memory, lazily built from bundle files | `import_records(bundle_uuid, importer, settings, watch)` + `import_reads(bundle_uuid, root, path, observed)` indexed by path. Dirty files join against `import_reads`. |
| `PipelineProjection`, `RegisteredImporters` | memory | part of the immutable `PipelineRuntime`. The projection's durable consequence goes in a `type_projection` table. |
| `schema_authority`, `build_targets`, `lineage_destination`, roots, quarantine | `RwLock`s filled from config | immutable `Arc<ActiveConfig>` owned by the authority and passed with jobs. Its hash and generation live in `configuration_state`. Done for the schema authority, build targets, roots, scanner, projection, pipeline importers and pipeline snapshot: one `Compiled` entry per `store_meta.compiled_version` (§6.1). |
| `configuration_poison`, `scan_rejection` | memory only (and `PendingScanRejection.subjects` is lost on restart) | `errors`. Done: `errors` families 2–4 and `scan_rejection_subjects` (§6.1). |
| `scan_initialized` / `scan_healthy` | atomics | authority-local. Done: `scan_initialized` is set only after a commit; healthy is "no stored rejection". |
| `Store.input_version` / `memo_seq` | cached copies | read from `store_meta` in the transaction |
| `Store.cas: CasInner` | memory | owned by the authority (the writer). Readers resolve segment names from `cas_segments`, not from `CasInner`. |
| `Store.last_recovery` | memory | returned from `open` and logged |
| `last_background_error` | memory | `errors` (scope = daemon) |

### 2.4 Commit-then-fallible sites to eliminate

These are sites where a durable commit is followed by fallible in-memory
work. If that work fails, SQLite and memory diverge:

- In `commit_locked`, `validate_commit` and `validate_lineage_repair_configuration` run after the caller's transaction committed.
- `coordinator.rs`:
  - 757–880: the config candidate installs and `.expect`s after the transaction.
  - 995–1060: the pipeline candidate does the same.
  - 1236–1286: the schema transition.
  - 1351–1366: the pipeline rejection.
  - 1602–1626: a checkpoint restore rolls memory back but not SQLite.
  - 1705–1720 and 1842–1845.
  - 3611 and 4179: a fallible `pipeline_state()?` after the commit.
  - 4349–4374.
  - 446–461 and 469–472.
- `refine_unpublished_tag_index` is a second transaction inside every publication, and its failure is swallowed as a poison.
- `importer.rs`: `clear_watched_import_failure` (1292), `record_directory_orphans*` (633, 695), and `acknowledge_file_work` are separate commits. Done for the process loop: they join the pass's one input (§6.1).
- `retire_publication_group` does check-then-act as three autocommits.
- `lookup_candidates` was a "read" that wrote `last_used`. Done: lookups are pure and the cache-limit sweep evicts at random, so a hit never writes (LRU drops to zero hits once the working set exceeds the cap).

## 3. Target thread model

As built in phase 11. There is no authority thread: each thread writes on
a connection of its own, and SQLite's write lock orders the writers.

```
 notify ──▶ watcher ──WatcherEvent──▶ process loop ──(own writer)──▶ SQLite
 rpc listener ──▶ one thread per connection (LocalSet front end: snapshots, subscriptions)
    ├─ reads: own StoreReader, one read transaction per snapshot
    ├─ importer runs: blocking worker (the connection keeps serving)
    ├─ builds: submit or join a build cell, await its ticket on the LocalSet
    └─ writes, import publications, operations: inline ──(own writer)──▶ SQLite
 scheduler: build cells (key → Queued | Running | Done); lends each job a writer ──▶ worker ──▶ SQLite
```

- **Writers.** Each owner opens a writer of its own from the daemon's
  `StoreOpener` (config, instance id, CAS directory and the state-dir
  lock; immutable but for the operational configuration, which a writer
  rereads as each transaction begins): the process loop, every RPC
  connection (lazily, on its first write), and each build job, which the
  scheduler lends one of its writers. The owner passes it as `&mut Store`
  to whatever writes on its behalf (authoring, importer, build,
  operations, coordinator, codegen); nothing finds a writer through
  thread-local or shared state. Every write transaction takes the lock up
  front (`BEGIN IMMEDIATE`), so a transaction reads the state it writes
  over; nested calls join that transaction (`write_transaction_with`).
  Reads inside an open input go through its writer; an import run
  outside any write reads a snapshot of its own. A writer's CAS segment
  is sealed when its owner closes it.
- **The process loop** owns the `WatcherQueue`, `ConfigWatch` and codegen.
  It reconciles watcher events once the filesystem has settled (each
  event restarts a trailing quiet window, `watch.quiet_ms`), publishes
  scans and imports, and runs startup. Each pass (`coordinator/pass.rs`)
  publishes one input version: the scan, directory and watched imports,
  the dirty-queue acknowledgement and the runtime pipeline-failure sync.
  Its scan state (the pending rejection and health) sits in a `Mutex`
  other publications read.
- **watcher** forwards `notify` events to the loop and holds no queue
  state. It stops on its command channel.
- **Imports and authoring calls** run on the calling thread: the process
  loop's, or the RPC connection's own thread, which blocks only that
  connection. A publication reads its base inside its own transaction and
  is refused if the base moved. An RPC import runs its importer on a
  blocking worker before it opens the input; a process-loop pass plans in
  a rolled-back write, runs its importers in parallel outside any write
  against an overlay of the planned file state, and applies them all in
  one coordinated input (`Stale`, and rerun whole, if the base moved).
- **Builds** are build cells (DESIGN.md §13 Build cells). A resolve, on
  its connection thread, computes the node's key (static inputs only) at
  its snapshot and looks it up in the node cache: a cached node whose
  trace holds there answers at once. Otherwise it submits the key's cell,
  or joins the one in flight, and awaits the ticket (a `Send` future; a
  dropped ticket withdraws its interest, and a queued cell nobody wants
  never runs). The scheduler runs a cell at the highest class among its
  waiters. Its worker builds at a snapshot of its own, looks up stage and
  node results on a latest reader, and, for a dependency, steals a
  queued cell, waits on a running one unless that closes a cycle of
  waiting workers, or builds it privately; one worker suffices for any
  chain. It publishes each node's stage rows, wire trees, artifacts with
  `artifact_load_edges`, and node row in one transaction on its own
  writer, then completes the cell. Each waiter revalidates the node's
  trace at its own snapshot: `Built` where it holds, `Drifted` where not.
  Traces are revalidated by indexed point queries in the snapshot's read
  transaction, answers kept per build; nothing loads the whole project.
  Inline builds (doctor verification, tag-index refinement) take an
  `OpenInput` proof: they run only on a writer inside an open input, and
  nothing on the resolve path has one.
- **rpc front ends** are cheap, `!Send` values, one per client
  connection, each on that connection's own thread (`Rc<RefCell<…>>` state
  on its `LocalSet`). Each owns its reader, subscriptions, delta queue,
  change-log cursor and snapshots; no front end reaches another. A
  snapshot is a read transaction; the capnp transport expires it after a
  fixed TTL (30 s by default, `SnapshotPolicy`), with a `spawn_local`
  timer. The number open across connections, and the number of
  connections, are capped by atomic counters on the `ServerHandle`. A call on an expired snapshot fails with
  `SnapshotExpired`, and the client retries on a new one. Nothing pins
  artifacts or CAS segments.
- **CAS reclamation** deletes a retired segment once no read transaction
  can still see it: the daemon waits the snapshot TTL plus a margin.
- **Module epochs** unload when the last clone of their token drops.
- **Pipeline swaps** use New Game Plus's reload gate
  (`ngp_module_host::SourceGate`). A candidate module whose source hash is
  the watched schema's loads. One whose source is ahead of the schema but
  whose layout hash (`__ngp_layout_hash`: the crate with non-const fn items
  stripped) equals the schema's `layout_hashes` entry loads ahead of
  source-walk. Any other mismatch leaves the candidate pending while a Ready
  epoch keeps serving: no input version, no pipeline generation bump, and
  the next schema or module write retries it. Without a Ready epoch, or for
  a crate the schema lacks, it is a `CandidateAttestation` failure.
  A schema write that leaves the Ready epoch's version key
  (`ModuleReloadIdentity::version_key`: the module's own source hash, the
  other crates' source hashes, the layout hashes, plus the rest of the
  schema) unchanged, as source-walk catching up after an ahead-of-walk
  adoption does, is observed without republishing, as is a pipeline write
  of the bytes the Ready epoch was staged from. A missing pipeline module
  (cargo removes the dylib before linking the new one) leaves the Ready
  epoch serving until it reappears.
- **rebuild** (a serving `distilld`) runs the configuration's `[[rebuild]]`
  jobs: it watches each job's dep-info inputs with its own `notify` watcher
  and runs the job's steps as child processes, one at a time (in a process
  group on Unix, a kill-on-close job object on Windows), once its inputs
  have settled (the same `watch.quiet_ms` trailing window as the process
  loop). The process loop sends it each accepted configuration's jobs and
  window over its channel; it touches no store state: the daemon adopts
  the pipeline module and schema the steps write through the ordinary
  watch.
- The only atomics are ID and temp-name sequences, and the RPC server's
  admission counters and snapshot policy.

## 4. Error model: per-entity rows instead of poisons

Today the model has `VersionPoison`, `PipelinePoison`, `ConfigurationPoison`
and `ScopedBundlePoison` (about 290 mentions). A single `VersionPoison`
freezes *every* bundle, asset and path update and blocks all resolves.

The replacement is one table, `errors(scope_kind, scope_id, code, message)`
(current state; cleared when the entity heals), where scope is one of file, bundle, asset, target,
pipeline, config, or daemon.

- An asset collision fails the colliding assets only. Everything else
  keeps publishing.
- A pipeline or config error blocks what depends on it. It does not
  freeze the namespace.
- A resolve returns the asset's own error rows plus any pipeline or target
  row that actually blocks it.

## 5. Other fixes in scope

1. Imports and builds leave RPC calls: they become jobs (§3).
2. **Thin plugin API crate** `distill-pipeline-api`.
   - Today plugins link all of `distill-daemon` (a 260 MB .so), and the
     ABI fingerprint hashes the daemon's entire source tree, so any
     daemon edit breaks every plugin.
   - The new crate holds:
     - the table and export macro;
     - the registration arena, behind an opaque host vtable;
     - descriptors and the callback and context traits;
     - product and error types;
     - leaf types split out of `distill-build` (`Target*`, `TargetSelector`,
       `OutputDecls`, `ImportOutput`/`ImportError`, `AssetQuery`, `RootedPath`,
       `StableFailureFingerprint` closure, `GeneratedFile`, `Tool*`), so the
       API does not pull in `distill-store` / rusqlite.
   - The fingerprint covers this crate's closure only.
3. Per-entity errors (§4).
4. Nested builds recurse inline on the worker instead of blocking other
   threads. The depth limit stays.
5. **Bootstrap.**
   - `distilld init` / `distilld import <path>`.
   - `source-walk` emits the pipeline schema.
   - `start-engine.sh` gets `--distill-*` args.
   - The `deferred-ngp` harness shrinks to a config file.
6. **Logging.** `tracing` in daemon, rpc and store; `RUST_LOG` is honored by
   `distilld`. (Started in phase 0.)
7. **Journal, quarantine, lineage: decided (phase 10).**
   - The journal and quarantine are gone. Every write into an asset root
     or codegen output is `atomic_write`: temp file in the target's
     directory, fsync, rename over the target, fsync the directory. The
     target is re-hashed right before the rename (conflict check). A
     delete is a plain `remove_file`. The file is written first, the
     store rows follow; a crash between the two is healed by the scanner,
     which is disk truth.
   - Migration happens on read only. A bundle write never drops data: the
     automatic plan from the stored entry's schema to the written one may
     drop only default-valued fields, and a refused plan needs a
     registered migration function. `force_lossy` overrides.
   - Renames are `#[asset(renamed_from = "old")]`. Anything the planner
     refuses goes to a migration function registered by (type, from, to);
     without one, that asset's read fails with a per-asset error.
   - Schema lineage (manifest, seed, repair, `distilld init`) is deleted.
8. The coordinator's 40 ms sleep-poll and the codegen run on every loop go
   away. Codegen runs when a publication changed an input it read.
9. Subscriptions come from `change_log` (§1.6).
10. The `scanner` test target compiles again (phase 0).
11. Engine side (newgameplus): `AssetHandle` becomes GC-tracked and gets a
    `Default`. Done (newgameplus `58e5fce`, deferred-ngp `dcb81ed`): the GC
    releases requests no traced root reaches; storage still frees their GPU
    objects; `release_asset` is gone.

## 6. Phases

Each phase ends with `cargo test --workspace` green (§7 lists the known
baseline failures) and the deferred-ngp hot-reload scenario still working
(shader edit → adoption).

0. **Groundwork.**
   - tracing;
   - scanner test fix;
   - lock-order stopgap cherry-picked;
   - sibling worktree set `/data/Projects/lockless/{distill,newgameplus,…}`.
1. **Store split.**
   - `Store` → `StoreWriter` (connection + `CasInner`) and `StoreReader`
     (a read-only connection with `&self` reads).
   - Drop the cached `input_version` / `memo_seq`.
   - Readers use `cas_segments` for file names.
   - `lookup_candidates` becomes pure (done; eviction is random).
   - Merge `refine_unpublished_tag_index` and the other separate commits
     into their parent transactions.
   - `Arc<Mutex<Store>>` still wraps the writer during this phase. Readers
     are opened per thread wherever code only reads (build pool, trace
     source, codegen context, RPC fetch).
2. **Current-state tables and change log.**
   - Add the `assets` value/terminal columns, `derived_outputs.terminal_type`,
     `asset_resolutions`, `change_log`, `resolutions`,
     `artifact_load_edges`, lineage repair state, and the generation /
     epoch fences.
   - Every publication writes them in its existing transaction (applying
     `Commit` before `COMMIT`).
3. **RPC reads the database.**
   - Replace `VersionView` and `ServerState` with queries; a snapshot lease
     is a read transaction on its own connection.
   - Subscriptions read `change_log`.
   - Delete `commit_locked` and the history deque.
   - Connection and lease state moves to per-thread front ends; the expiry
     thread is deleted.
4. **Authority thread.**
   - The coordinator loop becomes the authority. The watcher feeds it
     through a channel.
   - RPC authoring calls become `Command`s with oneshot replies.
   - `Server.inner` is deleted. The publication lock no longer exists.
5. **Scan and import state into tables.**
   - Covers `files` extensions, `directories`, `symlink_aliases`,
     `asset_claims`, `import_records`, and `import_reads`.
   - Delete `ScanSnapshot`, `ScanProjectionIndex`, and `ImportWatchIndex`.
   - Imports become jobs.
6. **Builds as jobs.**
   - The authority does the CAS append and commit.
   - Cross-request dedupe on the RPC thread.
   - Inline dependency builds.
   - Delete the `operational` Mutex and Condvar.
7. **Per-entity errors.** Delete the poison types and gates.
8. **`distill-pipeline-api`.** Port the fixture plugin and deferred-ngp's
   `tools/distill/pipeline`.
9. **Bootstrap CLI and harness removal.**
10. **Journal, quarantine, and lineage** (§5.7): replaced by atomic writes,
    migrate on read and `renamed_from`.

11. **No authority, no leases, no drain atomics.**
    - Each thread writes on its own connection with `BEGIN IMMEDIATE`.
    - Snapshot capabilities expire at a fixed TTL; nothing pins.
    - Module epochs unload on the last clone's drop.
    - CAS files are reclaimed by a read bound.
    - Owning-thread state is plain fields or `OnceLock`s. The only atomics
      left are ID and temp-name sequences.

A grep for `Mutex|RwLock|Condvar` over `distill-{store,rpc,daemon}/src`
should reach zero by the end of phase 6.

### 6.1 Progress

- **Phases 1–2:** done (commits up to `738b556`).
- **Phase 3:** done.
  - **Server layout.**
    - `ServerHandle` (`Arc`, Send + Sync) holds config, instance, the
      published-version `watch`, the backends and the writer.
    - `Server` is a per-thread `Rc` front end, found through a thread-local
      registry via `Server::attach`.
    - A snapshot is an `Rc<SnapshotTxn>` over a pooled `StoreReader`.
    - Connections, leases and cached build results live on the front end.
    - Lease expiry is lazy, plus `Server::sweep_expired`, which the capnp
      transport calls every second. The expiry thread is gone.
  - **Writers.**
    - Embedded mode owns its `Store` on a writer thread (`Writer::Embedded`)
      and takes jobs over a channel.
    - The daemon passes its store through `ExternalStore`, which also
      provides the publication guard.
    - distill-rpc and distill-store hold no `Mutex`, `RwLock` or `Condvar`;
      the transport's stream services use `RefCell`.
  - **Resolve split.**
    - `Snapshot::resolve_prepare` returns `Done` or a Send `PendingBuild`.
    - The transport runs `PendingBuild::run` in `spawn_blocking`, then calls
      `resolve_finish`.
  - **Artifacts.**
    - Artifacts are CAS blobs from `assemble_artifact`, with their load edges
      in `artifact_load_edges`.
    - A read checks those edges against the parsed dependencies.
    - `ArtifactPayloadBackend` is deleted.
  - **Known gap (closed in phase 6, `e3fcf76`).** The daemon committed its namespace in
    its own input transaction. The RPC `Delta` for that commit is applied
    afterwards, in a second served transaction (`apply_commit_served`), on
    the same authority step.
    - Readers can see the new version before its served rows.
    - A snapshot is not serialized behind an in-flight publication, so a
      resolve against a just-superseded version returns `Drifted`, and the
      client must take a fresh snapshot.
    - `game_assets_e2e::resolved_hash` retries for this reason.
    - This closes once the daemon computes the `Commit` before its own
      `COMMIT`, which is phase 5's rewrite of the scan/import state.
- **Phase 4:** done.
  - **The authority** (`distill-daemon/src/authority.rs`) is one thread per
    coordinator, blocking on an mpsc inbox. Its messages are `Run(job)`,
    `Watch(event)`, `Attach(driver factory)`, `Detach` and `Shutdown`.
  - **Publishing runs on the authority.**
    - `ExternalStore` exposes `on_authority` and `execute` instead of a
      publication guard.
    - `ServerHandle::on_authority` runs a borrowed step there. It is inline
      when embedded or when already on the authority, and otherwise sends a
      scoped job and blocks until it has run.
    - `Server::coordinated_*` assert that they run on the authority.
    - Hub write / import / reimport, operation and schema-transition
      completions, and lineage repair do their gate checks on the RPC thread,
      then send the backend call plus the publication to the authority, which
      rechecks the base.
    - Every publishing `DaemonCoordinator` entry point wraps its body in
      `on_authority`. Tests use `DaemonCoordinator::coordinated_commit`.
  - **The process loop** is the authority's `Driver`.
    - It owns the `WatcherQueue`, `ConfigWatch` and codegen.
    - Startup (config, full scan, imports, retention) runs in the driver
      factory on the authority, so watcher events that arrive meanwhile wait
      in the inbox.
    - The background error is a `tokio::sync::watch`.
  - **The watcher** sends `WatcherEvent`s through a sink into the inbox. Its
    coverage is owned by its monitor thread; native callbacks forward raw
    events there. `watcher.rs` and `process.rs` hold no locks.
  - **Still polled until phase 6:**
    - The driver ticks every 40 ms (the debounce) to check for a runtime
      pipeline poison and to reap drained retired epochs. Both become
      messages once builds are jobs.
    - The watcher monitor polled `scanner.revision()`. Phase 5 removed that.
  - **Still blocking:** an RPC authoring call blocks the RPC thread until the
    authority replies, as the publication mutex did before. Replies become
    awaited oneshots with the build jobs (phase 6).
- **Phase 5:** done (commits `0f545da`–`411edc9`).
  - **Scan observation in tables.**
    - `files` gains `raw_path` and `symlink_target`.
    - `bundle_files` holds the bytes each bundle was read as.
    - `directories` and `scan_diagnostics` hold the traversal state.
    - Incremental scans check against `StoredBaseline`, a `ScanBaseline`
      over those tables, and load only the affected prefixes.
    - `ScanSnapshot` remains only as the scanner's result type and as a
      loaded view for full republication and doctor verify.
  - **Claims in tables** (`claims.rs`, schema 30).
    - `source_claims` holds one row per claim per source. Claim kinds are
      bundle, authored, derived, primary path, lineage and malformed.
    - `claim_collisions` and `claim_pending` are kept by
      `InputTxn::replace_source_claims`, for the touched subjects only.
    - An incremental scan writes its files, structure and claims, then
      plans against `transaction.reader()`, all in one input transaction.
    - `ScanProjectionIndex` and its checkpoint/restore logic are deleted.
  - **Import index in tables** (`imports.rs`, schema 31).
    - `import_records` and `import_reads(kind, key)` store the watched read
      sets. Dirty paths join against them.
    - `directory_rule_sources` stores directory rules.
    - `ImportWatchIndex` is deleted. A readiness `AtomicBool` replaces its
      mutex.
  - **Imports as jobs.**
    - An import is a run (`run_import`: execute the importer, write nothing)
      plus a publish (`publish_import`: fold, revalidate the read set,
      write the bundle).
    - RPC import and reimport:
      - `Hub::import_prepare` returns a Send `PendingImport`.
      - The transport runs it under `spawn_blocking`
        (`AuthoringBackend::run_import`, which takes `Arc<Self>`).
      - `Hub::import_finish` publishes on the authority, only while still at
        the request's base.
    - Watched reimports and directory imports:
      - They run in parallel (rayon) at one version.
      - They publish in order on the authority, each as its own version.
      - A run from an older version publishes when its destination row and
        read set are unchanged. Anything else reruns on the authority.
  - **Watcher roots by command.** The driver sends the scanner's coverage
    (`ReplaceAssetRoots`) after each configuration reconcile, and the
    monitor blocks on its channel.
  - **Deferred to phase 6: the `Delta` gap above.**
    - Tag-index refinement (`refine_published_tag_index[_incremental]`)
      runs builds through the store mutex against the committed namespace,
      after the input transaction, and then mutates the `Commit`.
    - So the served `Delta` cannot be written in the daemon's transaction
      until refinement becomes a build job that reads the uncommitted view.
- **Phase 6:** done (commits `6f46c9f`–`dd34248`).
  - **The store belongs to the authority** (`store_cell.rs`).
    - `AuthorityStore` holds the writer in an `UnsafeCell`. `write()`
      panics off the authority. `write_with` sends the write there.
    - `read()` borrows the writer on the authority. Elsewhere it opens a
      read transaction on a per-thread cached `StoreReader`.
    - A thread the authority lends itself to (`AuthoritySender::lend`,
      used by `run_scheduled`) counts as the authority while the authority
      waits on it.
  - **Builds.** Build commits, wire-tree puts, pins and lease releases go
    through `write_with`. The eviction/compaction gate is an `AtomicUsize`
    with a maintenance bit, not a lock.
  - **No locks left.** `Mutex`, `RwLock` and `Condvar` are gone from
    distill-store, distill-rpc and distill-daemon.
    - Configuration that is replaced whole and read anywhere is published
      through `arc-swap`. This covers the authoring roots, quarantine,
      lineage backend, importers and pipeline projection; the scanner
      roots and daemon-owned directories; and the coordinator's lineage
      destination, schema authority and build targets.
    - Authority-only state lives in an `AuthorityCell` (panics off the
      authority): the pipeline runtime, scan rejection and configuration
      poison. Dropping the pipeline guard publishes the host's
      `PipelineSnapshot` for other threads.
    - The scheduler is an actor thread (`ScheduledPool`) that admits jobs
      onto the rayon pool and receives completions as messages.
    - An epoch's lifecycle is atomics plus a `OnceLock` for the first
      runtime error. Unloading takes the epoch through `Arc::get_mut`, so
      an epoch that is still shared is retained (poisoned) rather than
      unloaded.
  - **One input per coordinated publication (closes the `Delta` gap).**
    - Every store write is a savepoint. `Store::arm_input` makes the next
      input transaction begin one outer transaction (`BEGIN IMMEDIATE`)
      that later writes join: the daemon's namespace, tag refinement's
      build and tag-index writes, and the server's served `Delta`.
      `finish_input` commits or rolls it back.
    - `Server::coordinated_*` arms it around the daemon's step and
      notifies subscribers once it commits. A reader on another connection
      sees the old version until then
      (`a_coordinated_publication_is_invisible_until_it_commits_whole`).
    - Journal writes that must be durable ahead of their filesystem change
      (record group, journaled moves) run before the input begins and
      panic after it. Retire and abort may join.
    - A rollback restores the `cas_segments` rows of segments created
      inside the input; their unindexed bytes are dead space.
  - The driver is event-driven. Watcher events, pokes (an epoch poisoned or
    drained) and completed jobs that published schedule its next pass;
    there is no fixed tick.
  - The build gate waits on the authority: the maintenance bit is set and
    cleared inside the sweep's authority job, so `enter_build` just queues
    behind it. There is no back-off sleep.
  - RPC authoring calls (write, import publish, lineage repair, progress
    completion) check their gates on the RPC thread. They then hand an
    `AuthorityCall` to the transport, which waits on it with
    `spawn_blocking`. The capnp driver never blocks on the authority.
  - Phase 6 is done: no `Mutex`, `RwLock`, `Condvar` or polling sleep is
    left in distill-store, distill-rpc or distill-daemon. The dev
    supervisor's sleeps go away with it in phase 9.

- **Phase 7:** done (commits `2505ff3`, `2f915c3`).
  - Namespace errors are per-entity. The `errors` table (store `errors.rs`)
    holds every current scan error, each scoped to its file, bundle or
    asset. `store_meta.version_poison` and the gates that made every
    namespace read fail are gone: `entry`, `resolve_path`, tag queries,
    `resolve_child`, and the RPC `versionPoisoned` result arms (wire
    protocol 5).
  - Only what collides is withheld. The coordinator (`Withheld`) holds
    back a bundle UUID more than one file claims, with its assets, and an
    asset UUID claimed more than once. Everything else publishes. A
    withheld asset resolves `Failed` with its error, once. A bundle whose
    asset set changes republishes even when its bytes did not.
  - Claims mark an asset's claimant bundles, derived output and paths
    pending when it starts or stops colliding, so the survivor of a
    healed collision publishes it again.
  - A scan the filesystem refused (unreadable subtree, path collision,
    invalid path) keeps the namespace it had and publishes the errors.
  - Metadata diagnostics list every namespace error.
  - Deviation from §4: only namespace errors live in `errors`. The
    configuration error and the pipeline failure keep their single-row
    state and their own gates, since each already scopes to one entity
    (the configuration, the pipeline epoch): a pipeline failure blocks
    builds against the target, not the namespace or metadata reads.
    Bundle skeleton errors (`bundles.poison`) and tag-index errors stay
    as columns on their own rows, for the same reason.
  - The vocabulary is renamed: `VersionPoison` → `NamespaceError`,
    `ConfigurationPoison` → `ConfigurationError`, `PipelinePoison` →
    `PipelineFailure` (the `Poisoned` status arms became `Failed`). The
    persisted bytes and SQL columns are unchanged. "Poison" now means only
    a bundle's or tag entry's own error and a module epoch token.

- **Phase 8:** done (commits `85afc7d`, `f43efca`, `7be33d8`; deferred-ngp
  `f148cda`).
  - **`distill-pipeline-api`** holds everything a module links: the callback
    traits and descriptors, products and errors, the importer interface,
    the erasure and call thunks, the registration arena, the module table,
    probe, ABI identity and `export_pipeline_module_v2!`, and the leaf types
    split out of `distill-build` (query grammar, `OutputDecls`, `Target*`,
    the `StableFailureFingerprint` closure, `ImportOutput`/`ImportError`,
    `GeneratedFile`/`CodegenFailure`, `ToolOutput`/`ToolRunError`). The
    daemon and `distill-build` re-export them at their old paths.
    `CallbackPanic` moved to `distill-core`; `distill-wire` re-exports it.
  - **Closure:** distill-core, distill-json, distill-migrate, ngp-schema,
    ngp-source-hash (plus blake3, globset, unicode-normalization, serde).
    No distill-store, rusqlite, distill-build or distill-daemon.
  - **Registration.** `register` receives the API's `RegistrationArena`,
    which wraps a `&mut dyn RegistrationHost`. Its generic `register_*`
    run in the module and erase the callback there; the host's single
    `install_callback` wraps it in a capsule owned by the candidate's own
    epoch and installs it as before, under `HostCallbackBoundary`. The
    payload is consumed on entry. The daemon-side arena (`install`,
    `from_raw`, `owner_pin`) is unchanged for host tests;
    `CandidateRegistrationArena::registrar()` gives the module view.
  - **Fingerprint.** `build.rs` moved to the API crate and hashes only
    that closure. The v2 `Cargo.toml`/`Cargo.lock` are no longer hashed
    (a plugin workspace resolves its own lock). Daemon, build and store
    edits leave built modules valid. The table ABI version stays 2: an old
    module fails the identity check before `register`.
  - **Ports.** The two test fixtures and deferred-ngp's
    `tools/distill/pipeline` depend on the API crate. The deferred-ngp
    pipeline's debug `.so` went from 286 MB to 104 MB (the rest is
    rafx-shader-processor).

- **Phase 9:** done (commits `a8cf6d5`, `f5bfd2f`, `67dabbb`; newgameplus
  `187b47b`, `1578a9f`, `a870c2b`; deferred-ngp `bb12d86`, `b8eeee1`).
  - **`distill-daemon/src/bootstrap.rs`**, shared by the CLI and the tests:
    - `init` writes the schema-lineage manifest (every project type active
      at one epoch, its current logical hash) and the schema seed (one
      authoring-only entry per project type holding its zero value, derived
      from the logical schema) beside `assets.lineage_manifest`, only when
      the bytes change. Identities derive from the lineage destination, so
      reruns are byte-identical. It refuses to replace a manifest that
      recorded schema transitions (more than one epoch for a type).
    - `import` imports through a running daemon's RPC hub
      (`RemoteHub::import`, new in distill-rpc), retrying while the daemon
      starts, its pipeline is unavailable or the base moved.
    - `engine_args` gives `--distill-rpc --distill-target
      --distill-target-hash` for a target (the configured address, so a
      fixed port).
  - **CLI:** `distilld init <config>`, `distilld import <config> <source>
    <dest> --importer <id> --settings <json> [--root] [--target]
    [--no-watch] [--if-missing] [--wait]`, `distilld engine-args <config>
    [target]`. `--settings` is required: the RPC import takes explicit
    settings and has no "importer default" spelling.
  - `game_assets_e2e` bootstraps through `init` (a rerun writes nothing)
    and imports through the RPC hub.
  - **source-walk `--pipeline-manifest <crate>`** walks the pipeline crate
    plus each path dependency that declares `#[asset(uuid)]` types (depends
    on distill-asset and its sources spell `asset(uuid`), and writes
    `<target-dir>/distill-pipeline-schema.json`: real layouts under the
    rustc identity, `source_hashes` with the pipeline crate's hash. For
    that, deferred-ngp's importer settings and imported source became
    `#[distill_asset::asset]` types (`GlslSettings`, `GlslSource`, same
    TYPE_UUID bytes); they existed only in the harness's schema before.
  - **start-engine.sh** `--distill-config <toml> --distill-pipeline <crate>
    [--distill-imports <file>]`: builds the pipeline cdylib, runs
    source-walk into the pipeline's target dir, `distilld init`, starts
    `distilld <config>` in the background (log, pid file, stopped by the
    next launch or a failed one), imports listed sources without a bundle,
    and appends `distilld engine-args`. Linux only.
  - **deferred-ngp:** `tools/distill/dev` is deleted. `tools/distill/`
    holds `distill.toml` (paths relative to it; the loader already resolved
    them against the config's directory) and `imports` (tonemap.comp,
    lighting.comp).
  - **The dev supervisor is deleted** (`dev.rs`, `distilld dev`,
    `DevLaunchConfig`, 8 tests). The daemon watches its schema file and
    pipeline module itself (`ConfigWatch`) and adopts new ones. The
    supervisor was the only thing that rebuilt on source change: it ran
    source-walk in watch mode and `cargo watch` over the pipeline and
    gameplay packages. Now a pipeline edit needs `cargo build` plus a
    source-walk rerun (the schema's source hash must match the module),
    and the game module is rebuilt by the user's cargo as without the
    supervisor.
  - **Headless check:** pipeline build, source-walk (5 s), `init`, daemon
    on 127.0.0.1:9910, `import` of tonemap.comp and lighting.comp; an RPC
    client resolving the bundles as target `dev` saw `Built`, then a new
    content hash within ~0.6 s of appending a line to tonemap.comp and to
    lighting.comp's include `gbuffer_common.glsl`.
  - **Deferred:** the imports list is a start-engine.sh file format, not
    daemon config. A daemon-side "importer default settings" import is not
    in the protocol.
- **Phase 10:** done (commits `a213e28`, `51d1c85`, `d855121`, `d439f55`;
  newgameplus `7631c8f`, `79d1883`; deferred-ngp `8a16b50`). distill
  from `503d889`: 92 files, +2004 −22547.
  - **Lineage, schema transitions, migration controls** (`a213e28`,
    −15.6k lines): the lineage manifest, seed, repair and `distilld init`
    are deleted, with `DiskMigration`, schema transitions, MigrationV1
    controls and doctor schema repair. Build migration: a migration
    function registered for (type, from, to) wins, else the automatic
    plan; a refusal is that asset's error. A migrated asset's build key
    includes the pipeline dylib hash.
  - **Atomic writes** (`51d1c85`, −6.9k lines): `atomic.rs`
    (`atomic_write_expecting`, `remove_expecting`, `Expected::{Any,
    Absent, Hash}`). The journal, quarantine, `PublicationGroupKind`,
    `.distill-displaced`, `quarantine_dir` and
    `displaced_retention_days` are gone (SCHEMA_VERSION 34; the four
    journal tables dropped). Authoring writes the bundle, then commits
    rows under the store guard; codegen writes changed files, then
    `commit_codegen_outputs`. Crash-heal tests: a rewritten bundle whose
    rows never committed is adopted on restart; codegen files written
    before a crash are adopted by the next publication. `DoctorRequest::
    Clean` is removed (PROTOCOL_VERSION 7).
  - **Lossless writes** (`d855121`): `write` carries `forceLossy`
    (PROTOCOL_VERSION 8). A write replacing an entry stored under another
    schema is refused with `RpcFailure::LossyWrite { type_uuid, asset,
    fields, detail }` when a `DropField` hits a non-default value
    (`distill_migrate::lossy_drops`, paths like `$.items[].gone`), or when
    the planner refuses and no migration function is registered.
  - **`renamed_from`** (`d439f55`, newgameplus `7631c8f`): source-walk
    parses `#[asset(renamed_from = "old")]` into `FieldAttrs`; it is never
    hashed. `ngp_schema::project_with_renames` keys each rename by the
    planner's display path; `ProjectTypeAuthority::renamed_from` carries
    them; `plan_automatic_renamed` matches by name, then by rename.
  - **Tests:** 1042 passed, 1 failed (the known
    `tool_output_is_drained_while_large_stdin_is_written`), from 1144/1:
    lineage, journal and quarantine tests went with their code.
  - **Headless check:** pipeline build, source-walk, daemon (no `init`),
    import of tonemap.comp and lighting.comp; the RPC client saw
    lighting.comp's new content hash ~0.4 s after an edit to
    `gbuffer_common.glsl`. No `.distill-displaced` anywhere.
  - **Deviations:**
    - The attribute is `#[asset(renamed_from)]`, not `#[distill(...)]`:
      every field attr (`blob`, `skip`, `tag`, `rev`) is on `asset`.
    - A rename is a `CopyField` or `Widen` from the old name. A rename
      whose value also changed shape is refused (nested ops read and write
      one path), so it needs a migration function.
    - Renames live beside the logical schema (`Renames`), not in
      `SchemaNode`: a snapshot holds exactly what its hash covers. The
      write check takes them from the current schema authority when the
      write is under the current hash.
    - Codegen crash heal: if inputs changed between the crash and the
      restart, a file matching neither the recorded nor the proposed
      hash is reported as edited outside distill.
    - Configs naming `lineage_manifest` or `displaced_retention_days` are
      rejected (`deny_unknown_fields`). A store from before SCHEMA_VERSION
      34 is refused; delete the state directory.
    - The scan diagnostic tag for quarantine no longer decodes.

- **Phase 11:** done (commits `833a4ea`, `d614d5d`, `ac0896f`, `2753221`,
  `e3d495d`; newgameplus `aef3393`). distill from `0c3f6d9`: 60 files,
  +4368 −5256.
  - **Epochs** (`833a4ea`): a `PipelineEpoch` is an `Arc`; the last drop
    cleans registrations up, unloads and closes the library. The drain
    atomics, arena fence, `EpochWake`, the retired list and reap polling
    are gone.
  - **CAS** (`d614d5d`): a read of CAS bytes finishes within the snapshot
    TTL of its index lookup. Eviction deletes index rows; compaction
    copies live records and repoints the index in one transaction; the
    loop's `SegmentSweeper` deletes a dead segment's file after the TTL
    plus `CAS_DELETE_MARGIN` (10 s). `cas_segments` replaces `CURRENT`;
    each writer appends to its own segment; `cas_refs` makes pruning one
    statement. The pins table, `CAS_MAINTENANCE` and build gating are
    gone (SCHEMA_VERSION 35).
  - **Writers** (`ac0896f`): the authority thread is gone. `SharedStore`
    gives each thread a writer (see §3); RPC publications run as
    `WriteCall`s on `spawn_blocking`; the scan/publish loop is its own
    thread.
  - **Snapshots** (`2753221`): a snapshot capability holds its read
    transaction until dropped, its connection closes, or the TTL passes
    (`SnapshotPolicy`, default 30 s, a `spawn_local` timer armed by the
    capnp transport). Use does not extend it. Calls on an expired one
    answer `snapshotExpired`; a fetch whose blob left the CAS answers
    `ARTIFACT_NOT_FOUND`; a connection closed by the connection bound
    answers `CONNECTION_CLOSED`. The loader retries the round at a new
    snapshot on either, up to 3 times; pack builds retry the same way.
    Artifact and pack-session pins, `ArtifactLeaseBackend`, connection
    deadlines and the lease sweep are gone (PROTOCOL_VERSION 9).
  - **Atomics** (`e3d495d`): `scan_initialized` and the import index flag
    are `OnceLock`s, scan health sits with the pending scan rejection, the
    watcher stops on its command channel, and a module epoch token latches
    its poison cause in a `OnceLock`. Left outside tests: scheduler
    `next_id`, `TEMP_SEQUENCE` (atomic.rs), pack activation `TEMP_ID`,
    `NEXT_SHARED_ID`, and `NEXT_HANDLE_ID` plus the embedded state-dir
    `NEXT` in server.rs.
  - **Mutexes added:**
    - `SharedStore::idle`: the idle writer pool, shared by every thread;
      held only to push or pop.
    - `PipelineState::runtime`: the module host; taken to prepare,
      install or fail an epoch, never while waiting on the write lock.
    - `DaemonCoordinator::scan` (pending rejection and health) and
      `configuration_error`: written by the loop, read by publications on
      other threads.
    - `Current<T>` (distill-store), a `Mutex<Arc<T>>` held only to clone
      or swap the `Arc`, replaces `arc-swap` for configuration replaced
      whole: the store config, scanner roots and daemon-owned
      directories, authoring roots, importers and pipeline projection,
      build targets and the published `PipelineSnapshot`. The schema
      authority is a `Mutex<Option<Arc<_>>>`.
  - **Tests:** 1044 passed, 1 failed (the known
    `tool_output_is_drained_while_large_stdin_is_written`); newgameplus
    lib 61 passed.
  - **Headless check:** pipeline build, source-walk, daemon on
    127.0.0.1:9910, import of tonemap.comp and lighting.comp; the RPC
    client saw lighting.comp's new content hash ~0.4 s after an edit to
    `gbuffer_common.glsl`, tonemap.comp's unchanged.
  - **Deviations:**
    - Tag-index refinement, build commits and `DeferredOperation`
      completion run inside the input transaction. The front end's
      build-result cache stays.
    - `distilld pack` is an RPC client of the running daemon (fixed
      `daemon.address`, as `import`); with no daemon it says to start
      one. It reads the PackDefinition on the metadata hub and builds on
      the definition's target hub: `resolve` with `batch` set,
      `runtimeTypePolicy`, `query`, `entry` (PROTOCOL_VERSION 10). The
      in-process pack path is gone. The daemon does not own the output
      directory, so it must lie outside every asset root.
    - Snapshots of one version on one front end share a `SnapshotTxn`;
      each capability has its own expiry.
    - Only the capnp transport arms expiry. In-process callers (tests)
      release on drop.
    - The snapshot policy is per front end: a test installs it on the
      serving thread's front end.
    - Hub connections no longer time out; the connection bound closes
      the oldest. The loader's reconnect path stays for transport loss.
    - Resolve still checks that the artifact is in the CAS, without
      pinning it.
    - The import index flag is per process. A forced rebuild that fails
      rolls back and leaves the flag set, so the old index stands until
      the next full import pass. (Superseded: the flag is now a
      `store_meta` marker, see "Compiled state by store version".)
    - A stopping watcher delivers the events queued before `Stop`.
- **One pass, one input version.** A process-loop pass
  (`coordinator/pass.rs`) plans in a write that always rolls back, runs
  importers in parallel outside any write against a `FileOverlay` of the
  planned file state, then applies inside one `coordinated_maybe_commit`:
  the runtime pipeline-failure sync first, the scan step with its tag
  refinement, every import matched to its run, and the dirty-queue
  acknowledgement. The merged `Commit`s publish as one version. An outside
  write between plan and apply makes the pass `Stale` and it reruns whole.
  An import whose read set drifted, or that the plan did not run, is
  skipped; its paths stay queued and the loop retries. The acknowledgement
  is per path: a path with newer work stays queued instead of failing the
  pass, so the background error clears on the next good pass. Kept out of
  the single input: configuration/schema/pipeline candidate publications
  (their own versions, before the pass), codegen, chained imports (the
  next pass) and import output disk writes. The watcher's echo of a
  pass's own bundle writes publishes nothing: a rename from a path never
  observed (the atomic write's temporary file) moves no identity. The
  import index flag was an `AtomicBool` the pass restored when its plan
  rolled back; it is now a `store_meta` marker (below).
- **After phase 11: one owner per writer.** RPC connections each run on a
  thread of their own and share only the `ServerHandle`. `SharedStore`,
  its thread-local `HELD`/`READERS`/`OPEN_GUARDS`, `WriteGuard` and
  `ReadGuard`, `WriteCall` with its `spawn_blocking` hop, and
  `FRONT_ENDS`/`Server::attach` are gone: owners open writers from a
  `StoreOpener` and pass them explicitly (§3). `SharedStore::idle` and
  `NEXT_SHARED_ID` went with it. Doctor verification rebuilds inline on
  the writer whose input it completes in, as the old in-transaction path
  did.
- **Build cells.** A build's identity is its static inputs, never the
  requester's snapshot: `BuildRequest` lost its `basis`, and the drift a
  build reported when the store had moved past the requester's basis is
  gone. `BuildBackend::build` (blocking, returning a publication the server
  installed) became `BuildBackend::start`, which answers at once or hands
  back a `BuildTicket`; the finished build answers each waiter at its own
  `BuildView`. The transport awaits the ticket on the connection's
  `LocalSet` instead of `spawn_blocking`, and `run_scheduled` is test-only.
  The wire protocol is unchanged. Node results are cached under
  `KeyKind::Node` with their traces. Tag-index refinement keeps running
  inline, now with an `OpenInput` proof, in a pass's apply input and never
  in its rolled-back plan.
- **Lazy trace reads.** `StoreTraceSource::capture`, which loaded every
  entry, bundle, path, derived output, tag poison and tool into maps for
  each build, is gone, and so is `PinnedToolEpoch`'s copy of the tools
  table. Each trace question is a point query in the snapshot's read
  transaction (`distill-store` `trace_reads`), kept in the build's
  `TraceAnswers`; tools are read per id at the build's tool version. New
  indexes `bundles_by_path`, `assets_by_type`, the partial
  `asset_tag_index_poisoned`, and `assets_by_bundle` keyed by bundle and
  local id (SCHEMA_VERSION 36 on that branch; the merged schema is 37,
  below). A store failure fails
  only the questions that reach it, where the capture failed every build.
- **Compiled state by store version.** SQLite is the only source of truth
  for what the daemon compiled; memory holds derivations of it, each keyed
  by the version it derives from.
  - `crate::compiled`: the schema authority, build targets, pipeline
    projection, pipeline importers, pipeline snapshot, scanner and roots
    are one immutable `Compiled` entry in a `CompiledRegistry` keyed by
    `store_meta.compiled_version`, which every compiled publication (a
    configuration candidate, a pipeline rejection) writes in its own input.
    `DaemonCoordinator::compiled_at(reader)` is the only lookup, an exact
    one: the process loop, RPC readers and writers (authoring, importers,
    operations), codegen, build `NodeEnv` capture (the requester's
    snapshot in `start`, the worker's view in `build_cell`, the input's
    rows inline) and `runtime_type_policy` (given the requester's
    snapshot) resolve the state their own transaction sees. A key with no
    entry is `CompiledLookupError` (`NotLoaded`, or `Superseded`, which RPC
    answers as `SnapshotExpired`), never another version's state. A process
    opening a store marked by an earlier process registers no entry for
    that key; its loop first publishes "no pipeline epoch has been
    published" so the store says what it holds.
  - A publication stages its entry under the version it will publish
    (invisible: no reader sees that key before the commit), confirms it
    after the commit, and drops it on failure, so a failed publication
    changes nothing. The module host, the watcher's roots and
    `scan_initialized` change only after the commit. Superseded entries
    live while held; the newest 4 stay for 120 s.
  - The pending scan rejection (its namespace errors, its configuration
    error, its subjects) and the configuration source's error are rows
    (`errors` families 2–4, `scan_rejection_subjects`); healthy is "no
    stored rejection", and the configuration status is selected from them
    inside each publishing input. Only scan and configuration publications
    write them, so an RPC write keeps them (`publish_incremental_paths` no
    longer replaces the rejection's namespace errors) and a restart keeps
    them with their subjects.
  - The import index's built flag is `store_meta.import_index_built`,
    written in the savepoint that writes the index rows, so a rolled-back
    plan leaves it unset.
  - `ServerHandle::replace_target` writes only the served target hash;
    builds take targets from the compiled entry, so it installs nothing in
    memory.
  - The node key (`DSNK` v2) includes each named type's build-only policy,
    so a policy change cannot serve a cached node.
  - The unused `publish_pipeline_candidate` is gone.
- **Queries, not table loads** (schema 36). Readers that loaded a whole
  table and filtered it in Rust ask SQLite instead, on the caller's
  transaction, with the same results, order and errors. Each statement is
  checked by `EXPLAIN QUERY PLAN` (`distill-store/src/query_plans.rs`), and
  the narrow ones are pinned by the pages they fetch from a 20 000-entry
  store (`StoreReader::pages_fetched`).
  - RPC `query`/`queryAssets` and codegen queries: one `assets ⋈ bundles`
    query per selector set (`AssetFilter`); a glob narrows by its keys
    (below) and is matched on the returned rows only. Doctor build
    verification reads the runtime entries in one query.
  - Import destinations look bundles up by path (`bundles_by_path`);
    enumeration and the pass overlay read the prefix subtree or the glob's
    literal-prefix range of `files` (or its keys, below); the
    watched-fixpoint check reads only bundles with a `$record` row
    (`assets_by_local_id`) or a poison
    (`bundles_poisoned`); directory orphans read only generated bundles.
  - Rename-with-fixups reads only the moving bundle, the bundles whose
    reference fields name its path (`bundle_path_refs`, written with each
    bundle's rows at publication) and the poisoned ones.
  - The full step's no-change check and doctor verify compare the scan with
    the scan tables row by row; bundle files compare by
    `bundle_files.hash`, so no bundle bytes are loaded.
  - A complete publication diffs `files`, the per-bundle asset sets, the
    asset deletions and the path index by ordered merges of streamed rows,
    holding only the differences.
  - Still whole: `published_scan` and the no-coordinator fallback of
    `publish_incremental_paths` republish the complete namespace, so they
    load it; `ensure_import_index` rebuilds every row. A failed complete
    tag refinement (a full rescan or configuration publication) reads
    `all_asset_bundles` inside its open input to poison every asset: a
    bulk operation of a bulk publication, read only on that failure.
- **Merged schema 37.** The build-cells, db-truth and db-queries branches each
  defined a schema 36; the merge is one SCHEMA_VERSION 37 with the union of
  their tables and one copy of each index. `assets_by_bundle` is
  `(bundle_uuid, local_id, asset_uuid)`: it answers a trace's (bundle, local
  id) lookup and covers the (bundle, asset) walk of a complete publication,
  which sorts one bundle's rows at a time (local ids are unique within a
  bundle). `bundles_by_path` is `(path, root_id)`. `bundle_files` stores
  `hash` before `bytes`, so the hash-only walk never reads the bytes'
  overflow pages (a test counts the bytes the reading thread reads).
- **Query drivers.** Schema 37 also adds `assets_by_local_id` (replacing the
  partial `assets_reserved`), the partial `assets_authoring`, and a
  generated final-segment `name` column on `bundles` and `files` and an
  `ext` column on `files`, each indexed. `GlobKeys` reads a glob's literal
  prefix, the final segment its literal tail names (`**/name.ext`) and the
  extension (`*.ext`), in `globset`'s or the RPC's dialect. `AssetFilter`
  ranks its selectors (identity or exact name, poisoned tag rows, tag value,
  authored type, path prefix, terminal type, bare tag, authoring-only) and
  names the driver's index with `INDEXED BY` and the join order with `CROSS
  JOIN`; build traces pick their candidates in the same order and file
  enumeration takes a name, then a subtree or prefix, then an extension.
  A bare `*` or `**`, or `authoring_only = false` alone, is a whole read.
  The shapes callers issue: build traces (closed queries: uuid, bundle and
  local id, bundle path and local id, type, tag, prefix and glob), codegen
  (any selector set, always runtime-only), RPC (any selector set, always with
  a role) and pure metadata (uuid, bundle, type, prefix, role), pack roots
  (any selector set, through the RPC), directory-import listings
  (`*.ext` globs over `files`) and the reserved-entry lookup (`$record` by
  local id). None needs a composite index beyond `assets_by_bundle`: each
  has one selective driver, and the rest filter its rows.
- **Chained imports in one pass.** A pass runs imports in levels: after a
  level runs, the rolled-back plan input finds the imports that read a
  changed output (over the overlay plus those outputs) and runs them next,
  upstream first, at most 8 levels per pass; a cycle is cut and reported.
  The apply checks each level against the outputs before it.
- **The scheduler thread is joined on drop.** It owns the opener and the
  idle writers, which hold the state directory's lock; dropping the pool
  now waits for it (except on a pool worker running a job, which the
  scheduler waits for, and on the scheduler thread, which drops queued
  jobs as it stops), so a coordinator reopened at once finds the lock
  free.
- **Loader step counters.** `RpcIo::last_step()` reports a step's turns,
  task polls and most polls in one turn; the backpressure tests assert
  those counts, not wall time.

## 7. Test baseline

Recorded at the start of phase 0; see `git log` for updates.

Phase 0 (after the tracing and scanner fixes): `cargo test --workspace` →
1128 passed, 1 failed. The failure is
`distill-build --test tool::tool_output_is_drained_while_large_stdin_is_written`
(EPIPE while writing the tool's stdin). It already failed before phase 0 and
is unrelated to this work.

End of phase 3: `cargo test --workspace --no-fail-fast` → only the same
`tool_output_is_drained_while_large_stdin_is_written` failure.

End of phase 4: the same single failure.

End of phase 8: 1151 passed, the same single failure.

End of phase 9: 1144 passed (the supervisor's 8 tests removed, 1 bootstrap
test added), the same single failure.

Build cells, on top of the one-input pass: 1151 passed, no failures (the
stdin-drain test now passes).

Lazy trace reads: 1157 passed, no failures (6 new: the store's plan and
prefix-bound tests, and the lazy-against-eager equivalence, revalidation,
poisoned-bundle and 20 000-asset scale tests).
Compiled state by store version: 1162 passed, no failures. (One earlier
full run failed the timing-sensitive `distill-loader --test
rpc_io_backpressure fetch_throughput_is_not_one_per_two_frames` once
under load; it passed on every rerun.)
Queries, not table loads (schema 36), on top of build cells and the scanner
sibling fix: 1170 passed, no failures.
Merge of db-truth and db-queries (schema 37), with the tag-refinement
fallback read only on failure, chained imports in one pass, counted loader
steps, the scheduler join and the query drivers: 1196 passed, no failures.
