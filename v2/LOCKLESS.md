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
     snapshot. Builds always run against current state; a mismatch with the
     snapshot's recorded inputs is `Drifted`.
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
| `schema_authority`, `build_targets`, `lineage_destination`, roots, quarantine | `RwLock`s filled from config | immutable `Arc<ActiveConfig>` owned by the authority and passed with jobs. Its hash and generation live in `configuration_state`. |
| `configuration_poison`, `scan_rejection` | memory only (and `PendingScanRejection.subjects` is lost on restart) | `errors` |
| `scan_initialized` / `scan_healthy` | atomics | authority-local |
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
- `importer.rs`: `clear_watched_import_failure` (1292), `record_directory_orphans*` (633, 695), and `acknowledge_file_work` are separate commits.
- `retire_publication_group` does check-then-act as three autocommits.
- `lookup_candidates` was a "read" that wrote `last_used`. Done: lookups are pure and the cache-limit sweep evicts at random, so a hit never writes (LRU drops to zero hits once the working set exceeds the cap).

## 3. Target thread model

```
 notify ──FsBatch──▶ ┌────────────┐ ──Job──▶ pool worker ×N (StoreReader each)
                     │ authority  │ ◀─JobDone─┘
 rpc ────Command───▶ │ StoreWriter│
    ◀──Published(v)──│ + CAS write│
                     └────────────┘
 rpc (LocalSet, StoreReader, owns connections/leases/subscriptions)
    ──BuildJob──▶ pool ──JobDone──▶ authority ──reply(hash)──▶ rpc
```

- **authority** (one OS thread) runs a blocking `recv` on its inbox. It never
  sleep-polls; it debounces file events with `recv_timeout` against a
  deadline. Messages:
  - `FsBatch`, the watcher's raw events;
  - `Command(AuthoringCommand, reply)`: write, import, reimport, operation,
    lineage repair, admin;
  - `JobDone(result)`: import or build result;
  - `ConfigChanged`;
  - `Shutdown`.

  For each message the authority:
  1. reads what it needs through its own connection,
  2. plans,
  3. dispatches jobs or performs journaled file publications,
  4. runs one transaction,
  5. sends notifications.
- **Imports are jobs.** The authority sends `ImportJob{version, bundle,
  importer, config, pipeline}`. A worker runs the importer against the
  filesystem plus its `StoreReader` at `version` and returns the bundle
  bytes plus the read set. The authority then:
  1. revalidates the read set against `files` (an optimistic base check),
  2. performs the journaled bundle-file publication,
  3. commits namespace rows, `import_reads`, and the change log in one
     transaction.

  If the base drifted, it re-queues the job. No importer runs under any
  lock or on the RPC thread.
- **Builds are jobs.**
  - RPC resolve sends `BuildJob{version, target, asset}`. The RPC thread
    keeps an ephemeral in-flight map, which gives cross-request dedupe
    (missing today).
  - A worker does memo lookup and trace revalidation against its reader.
    It builds dependency reads *inline* on the same worker with a per-job
    content-addressed memo, and encodes the artifacts.
  - It returns the encoded records.
  - The authority appends to the CAS, indexes, and writes
    `artifact_load_edges` and `resolutions` in one transaction, then
    replies with the hash. If the build's input version is not the
    snapshot's and its trace disagrees with the snapshot, the reply is
    `Drifted` and the client retries on a fresh snapshot.
  - A trace read no longer snapshots the whole project
    (`StoreTraceSource::capture` is O(project) per callback today). It is
    a point query on the worker's reader.
- **rpc front ends** are cheap, `!Send`, per-thread values
  (`Rc<RefCell<…>>` state on a `LocalSet`). Each owns its connections,
  subscriptions, lease timers, pack sessions, and in-flight build map, a
  `StoreReader` for current-state and fence reads, and one read-transaction
  connection per snapshot lease. Authoring and build requests go to the
  authority over a channel with oneshot replies. Several front ends can
  coexist (the daemon's RPC thread, a test thread). On a change-log `watch`
  tick they read `change_log` and fan deltas out. Lease expiry is checked
  lazily on access plus `tokio::time`, and the `distill-rpc-lease-expiry`
  thread is deleted.
- **watcher** forwards `notify` events to the authority inbox. It holds no
  queue state.
- **CAS compaction** is authority-only. Retired segment files are deleted
  only after the jobs dispatched before the retirement have completed and
  no snapshot lease predates the retirement. Readers resolve segment names from
  `cas_segments`.

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
7. **Re-evaluate** journal (3.5k lines), quarantine, lineage_repair and
   migration_control (about 6k lines).
   - The write-intent journal stays; it is what makes writes into user
     asset roots crash-safe. It becomes authority-only.
   - The rest is judged after phases 1–5, once the single writer removes
     the concurrency it defends against.
8. The coordinator's 40 ms sleep-poll and the codegen run on every loop go
   away. Codegen runs when a publication changed an input it read.
9. Subscriptions come from `change_log` (§1.6).
10. The `scanner` test target compiles again (phase 0).
11. Engine side (newgameplus): `AssetHandle` becomes GC-tracked and gets a
    `Default`.

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
10. **Journal, quarantine, and lineage review** (§5.7).

A grep for `Mutex|RwLock|Condvar` over `distill-{store,rpc,daemon}/src`
should reach zero by the end of phase 6.

## 7. Test baseline

Recorded at the start of phase 0; see `git log` for updates.

Phase 0 (after the tracing and scanner fixes): `cargo test --workspace` →
1128 passed, 1 failed. The failure is
`distill-build --test tool::tool_output_is_drained_while_large_stdin_is_written`
(EPIPE while writing the tool's stdin). It already failed before phase 0 and
is unrelated to this work.
