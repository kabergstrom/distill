# Distill v2 — Asset Pipeline for newgameplus

## 1. Overview

Distill is a long-lived asset daemon that watches an asset tree, imports and
cooks content, and serves it to game processes and editor tools. It is built on
newgameplus's existing schema infrastructure: `source-walk` extracts schemas
from plain Rust structs, `ngp-schema` computes migration plans, and pipeline
extensions load as a cdylib the same way game modules do.

This document supersedes the v1 design. It is a rebuild of distill's concepts
(UUID-addressed assets, daemon, content-addressed caching, load/build
dependency separation, the loader/handle state machine) on newgameplus's
foundations — not an incremental improvement of the old implementation.

### Goals

1. **Plain Rust structs are the asset model.** One attribute, no serde derives,
   no parallel source-type/cooked-type hierarchy. Schema comes from source
   analysis, so editors and validation work without the game compiling.
2. **Content iteration decoupled from compilation.** The daemon outlives game
   rebuilds; schema-driven imports need no game code at all.
3. **Deterministic, cacheable builds.** All impurity happens at authoring time
   and is checkpointed into the bundle. The build pipeline is a pure function
   of bundle bytes, schemas, and code versions.
4. **Git-friendly sources.** Self-contained bundle files carry their own
   identity and schemas. Nothing outside the asset tree is precious.
5. **Schema evolution as routine workflow.** Automatic migration of source
   files on disk; custom migrations are themselves assets.
6. **A real processing (cook) stage** — typed transforms over a pull-based
   incremental build graph. (v1 distill stubbed this.)
7. **Content hot-reload** for newgameplus, complementing its code hot-reload.
8. **Packfile shipping** with incremental patching.

### Non-goals

- Cross-compiler-version ABI stability (the module host requires same rustc).
- Arbitrary module unload without the host protocol: residency, contained
  cleanup, and leak-on-failure are enforced by §3's owner-token state machine.
- Watching external raw files **by default**. Import is an explicit authoring
  action; re-import is explicit. A per-import `watch` opt-in (§8) re-runs an
  import when its recorded read-set invalidates — for high-iteration sources
  like shader text, where edit-to-reload matters.
- Arbitrary code predicates in asset queries (see §10).

### Normative compatibility model (R34)

This section is the current authority for module/schema pairing and runtime
asset compatibility. It supersedes every older clause in this document that
requires `CompiledTypeRow`, `DSCA`, `DSLP`, `DSRE`, `RegistryExtrasV1`, a
target-native bootstrap table, an accepted runtime type set, attestation
expansion, in-place Hub reattestation, or the binary-local `DSFT` summary.
Those names remain only in superseded historical clauses and the review
ledger; the current implementation contains none of their codecs or protocol
fields.

There are three independent boundaries:

1. **Pipeline module opening.** The staged module's New Game Plus source
   identity must equal the source identity recorded for that module crate in
   the watched schema. The check uses the shared `ngp-source-hash` export/read
   implementation also used by New Game Plus. `ModuleAbiIdentity` separately
   gates the Rust host interface before the first Rust-ABI call. Candidate
   opening does not compare native asset layouts or export a compiled-type
   table: pipeline values cross into the daemon as the neutral reflection /
   authored-value stream and are encoded from the paired schema.
2. **Runtime artifact loading.** The authenticated artifact header's terminal
   `TypeUuid` and `LogicalHash` must equal the selected live runtime descriptor;
   `build_only` is rejected from that local descriptor. The artifact's DSWL
   bytes must hash to its `LayoutHash`, and the loader must successfully compile
   a fixup plan from that producer wire tree to the exact live
   `NativeLayoutNode`. Execution uses the size, alignment, ctor, drop, and skip
   tables from that same descriptor. Different producer and consumer native
   layouts are supported; DSNL equality is neither required nor sufficient.
   There is no cross-epoch fixup-plan cache, so a second digest over local table
   assignments adds no safety and is removed. A future cache must be scoped to
   one live module epoch and keyed by `(TypeUuid, LayoutHash)`.
3. **Target selection.** `DSTG` remains the compact commitment to one complete
   target definition and crosses RPC/build/pack boundaries. A candidate target
   set is stored and compared as its canonical sorted `(name, DSTG)` rows;
   `DSTS`, a second digest stored beside those rows, adds no authority and is
   removed.

RPC validates each fetched artifact at the load boundary above. Artifact fetch
exists only on a pinned `Snapshot` capability, so resolve and fetch return the
same `SnapshotStamp`; there is no target-bound “fetch latest” Hub bypass. A
module epoch change advances the connection fence and returns
`PipelineEpochChanged`; clients reconnect and resubscribe instead of mutating
an existing Hub with `reattest`. Packs carry their target/DSTG,
artifact content hashes, authenticated artifact bytes, wire trees, and typed
dependency edges; mount/load applies the same live-descriptor checks. Pack
construction enforces build-only policy, and runtime loading independently
rejects a local build-only descriptor. There is no DSCA/DSLP projection at
either boundary.

The bundle-format bootstrap remains one checked-in logical control spec with
exactly five TypeUuids and schemas. It is validated by the bundle decoder and
does not have per-target native rows or a consumer bootstrap brand. Tag
annotations, reference/blob metadata, control roles, and build-only policy are
read from the schema/descriptor that owns them; they are not duplicated into a
generic registry-extras attestation grammar.

Every live descriptor must be locally sound. In particular, generic `Option<T>`
uses a typed contained construction thunk and never guesses a niche layout.
That local validation/construction boundary is what made removal of the
cross-binary DSNL comparison safe.

## 2. Source-of-Truth Hierarchy

This invariant governs the whole design:

1. **Authored data** — bundle files in the asset tree: the `assets` map,
   import-settings entries (with their recorded read-sets, §8), primary
   selection, the UUIDs and schema snapshots written into them, and the
   source-controlled `SchemaLineageManifest` (§6, §11). The lineage manifest
   authority contents are updated only by explicit schema-acceptance commands
   and record every accepted type epoch even if no ordinary bundle is
   rewritten; §6's narrow local repair command may create an absent manifest or
   remove duplicate claimants, but cannot edit a unique valid authority.
   Human/tool-owned, version-controlled, sacred.
2. **Code** — the asset-types crate, the pipeline module, registered
   processors/importers/migration functions, and source-walk output.
3. **Daemon state** — SQLite databases, the artifact CAS, dependency records,
   file-tracking state. Fully derived from (1) + (2). Never committed to
   version control. `rm -rf .distill/` costs a rebuild, never content or
   migration direction: the durable lineage manifest is rescanned before any
   schema-dependent work. Non-bootstrap bundle lineage stamps are independently
   verifiable snapshots of an accepted manifest prefix, useful for detecting
   corruption and diagnosing stale data, but they are not authority for
   accepting a new epoch. The exact five format-bootstrap control types use
   their sealed `EntryLineageV1::Bootstrap` arm instead (§6). If the manifest
   is absent after state loss, no observed bundle,
   registry head, or matching hash constitutes forward proof; the type
   hard-stops until its accepted history is restored through the explicit
   repair/acceptance controls.
   Authority and executable code meet only by exact equality over **active**
   logical authority plus the closed bundle-format bootstrap exception (§3): a
   `Ready(PipelineEpoch)` exists iff every non-bootstrap candidate schema row
   has one `Active` manifest row, every `Active` row has one candidate row, and
   each candidate DSLH equals that row's selected `current` digest. Every
   bootstrap-control row instead equals
   its format-derived logical row under the separate bootstrap-authority gate.
   No other exception exists. `Retired` rows preserve accepted history but are
   not active registry authority. A mismatch is `SchemaAcceptanceRequired`,
   never an implicit forward step or implicit retirement/reactivation.

There is no machine-owned section in committed files. Everything the daemon
needs to persist beyond what bundles carry lives in daemon state and must be
reconstructible by scanning the tree and re-running pure work —
directory-import ownership included: a generated bundle's own `$record`
carries its `DirectoryOrigin` (§8), so ownership reconstructs by scan. Two records
in daemon state protect authored data rather than deriving from it: §14's
**write-intent journal**, which names whose bytes sit where while a
daemon-authored file replacement or deletion is in flight, and §14's
**displaced-inode quarantine** — per-filesystem, daemon-owned quarantine
directories (one per watched root, one beside each daemon-owned output
directory), entries named by the journal's unique **intent IDs**;
`.distill/` holds only the journal and metadata, which reference the
physical locations — which retains
every inode a rewrite, deletion, or conflict restoration displaces for a
configured window — bytes a
concurrent in-place writer may still be writing through an open
descriptor. Their loss never loses
content — recovery without them degrades to §14's conservative rule (nothing
unclassified is ever deleted; unexplained files are preserved under named
conflict or quarantine paths) — but while publication is in flight the
journal is the crash-recovery record, fsynced before the first rename,
and until retention expires the quarantined inode is the last holder of
any edit that raced publication.

## 3. Architecture

```
┌───────────────────────────────────────────────────────────────┐
│ Game workspace                                                │
│                                                               │
│  asset-types crate          pipeline crate      game module   │
│  #[asset(uuid=…)] structs   importers,          gameplay      │
│        │                    processors,         code          │
│        │                    migration fns          │          │
│        ▼                        │                  │          │
│  source-walk ──► schema.json    ▼                  ▼          │
│                             pipeline.cdylib   game.cdylib     │
└──────────┬──────────────────────┬──────────────────┬──────────┘
           │                      │                  │
           ▼                      ▼                  │
┌───────────────────────────────────────────┐        │
│ Distill Daemon (long-lived)               │        │
│                                           │        │
│  File Tracker ─► Import ─► Process ─► CAS │        │
│  Schema Registry   Migration   SQLite     │        │
│                                           │        │
│  RPC ◄────────────────────────────────────┼──┐     │
└───────────────────────────────────────────┘  │     │
                                               ▼     ▼
                                          ┌────────────────┐
                                          │ Editor │ Game  │
                                          └────────────────┘
```

### Processes

| Process | Role | Lifetime |
|---------|------|----------|
| **Daemon** | Watches the asset tree, imports, cooks, migrates, serves, and loads the pipeline cdylib. In authoring `dev` mode its outer supervisor also owns schema/module build-process lifetimes. | Long-lived, survives game rebuilds |
| **Game** | Loads assets via RPC (dev) or packfile (release). | Rebuilt frequently |
| **Editor** | Schema-driven UI; talks to daemon via RPC. | Independent |
| **source-walk** | Extracts schemas from the asset-types crate via Rust Analyzer and owns incremental workspace-source observation. | Supervised by `distilld dev` during authoring builds |

### Development build supervision

`distilld` has a production `serve` mode, which consumes already-produced
artifacts, and an optional authoring `dev` mode. `dev` contains a small outer
supervisor that exists before the asset daemon opens a pipeline epoch. It
starts and supervises exactly the project's `source-walk` watch process and
Cargo watch build for the configured pipeline and gameplay-module packages,
including process-group teardown, bounded crash-loop backoff, and diagnostic
forwarding. It is pipeline-independent control-plane code: running Cargo or
`source-walk` never requires a loaded pipeline module.

On a clean checkout with no artifacts, the supervisor starts both producers
and waits for the first readable schema and pipeline cdylib before starting
the serving daemon. With prior artifacts it may start the serving daemon
immediately while producers catch up. Thereafter the daemon's existing exact
artifact watches and candidate gate are authoritative: a transient
schema/module source-identity mismatch rejects only that candidate and keeps
the prior epoch pinned. Build failure is a supervisor diagnostic, not an input
version or pipeline poison by itself; only an observed output-artifact change
enters candidate staging.

This moves **process ownership**, not source analysis, into Distill.
`source-walk` still owns the source-directory/VFS watch and the exact source
snapshot it hashes; Cargo watch still owns Rust build invalidation. A Cargo
manifest/feature-graph change makes `source-walk` request restart so the
supervisor can reconstruct its Rust Analyzer workspace. The daemon does not
crawl or hash the Cargo workspace and does not add a second source-staleness
model. The gameplay cdylib is a supervised build output only: the game engine,
not the daemon, stages, migrates, and commits that module because its live
state and callbacks are engine-owned.

The supervisor launch description (workspace root, source-walk launcher and
arguments, Cargo launcher, pipeline/gameplay packages, target directory, and
profile) is startup-only launcher configuration, separate from the watched
§18 daemon TOML. Changing it restarts the supervisor and has no direct content
identity; the resulting schema and module bytes already enter the ordinary
input-version and module-epoch paths. This is deliberately not a generic task
runner or a pipeline tool registration surface.

### Modules

The daemon loads a **pipeline cdylib**, separate from the game module. Both
link a shared **asset-types crate** that defines the `#[asset]` structs;
source-walk runs over that crate. Heavy toolchain dependencies (shader
compilers, image codecs, mesh optimizers) live only in the pipeline module.
Rust is a required tool for developer workflows; the daemon is built from the
project workspace, not installed as a foreign binary.

Module hosting is **shared code, not a parallel implementation**: the
host newgameplus already runs for game modules (`module_state.rs` —
`libloading`, audited function tables, explicit resource cleanup on
unload) is factored into a shared module-host crate that both the engine
(game modules) and the daemon (the pipeline module) consume. One
implementation carries the invariants — same rustc, same features on
shared deps, clean unload — and a fix or hardening lands in both hosts
at once; the code is never duplicated. There is no off-the-shelf host —
this is bespoke code with known invariants, owned once. The
same-rustc host-interface invariant is checked, not assumed: the module exports
its `ModuleAbiIdentity` (§3) and shared New Game Plus source identity in its
C-ABI prefix, and the daemon refuses registration unless the ABI identity
matches the host and the source identity exactly matches the watched schema's
entry for that module crate.
No authored asset instance participates in candidate opening: the expected
rows come entirely from the watched source-walk schema artifact, and authored
bundles are examined only after a module epoch is ready. Loading never touches
the artifact path directly: each epoch
copies the dylib into daemon state, hashes the copy, and `dlopen`s the
copy — the hashed bytes are exactly the loaded bytes, so a supervisor
rebuild landing between hash and load can never run one build's code
under another's identity (the same staging rule covers package tools, while
ambient tools are explicitly unstaged, §9). Reloads use a **module epoch**, and rotation is **staged, never in
place**: a reload first opens a *candidate* epoch — copy, hash, `dlopen`,
the identity checks below, a host-minted unpublished `ModuleEpochToken`,
`register` into that token's status-bearing **registration arena**, and
pipeline-map construction
validated against the candidate's target configuration (§18: module,
schemas, and target set are ONE candidate — a target edit stages
through this same mechanism) — while the prior epoch remains
fully authoritative; only a candidate that passes every check becomes
current (published as an input event, §13). A candidate that fails —
identity mismatch, duplicate registration, a module that will not open —
**never opens and never half-opens**: the daemon publishes the new input
version as a **pipeline-poisoned version** (§13) carrying the named
error — its snapshots hold `PipelineState::Poisoned` and a fallible
`epoch()` (§13), so pure-metadata reads keep answering while anything
needing pipeline code fails naming the error — the prior epoch keeps
serving exactly the snapshots that pin it
(never silently standing in as the new version's code), and the next
good artifact swap heals. New jobs enter the current epoch; the daemon
drains old-epoch jobs at a barrier before `dlclose`, then the staged
candidate's registrations take effect. Registered identifiers are
deep-copied into daemon-owned
memory at registration, and every module-provided object is dropped at
the epoch barrier, before `dlclose`. Job *payloads* are serialized
artifacts, but the registration and context surface — `Registry`,
`Outputs`, `AuthoredValue`, errors — crosses by value under the Rust
ABI, so that surface carries its own check: daemon and module each embed
a **`ModuleAbiIdentity`** at build (declared below) — rustc identity,
the interface crate's source fingerprint, panic strategy, global-
allocator contract — and registration compares them **daemon-side**
before any Rust-ABI call. The asset-layout identity (§5) says nothing
about the daemon's own interface ABI — a long-lived daemon and a freshly
rebuilt module can agree on asset layouts while disagreeing on what
`Registry` compiles to — so it can never stand in for this. Values
allocated in the module may be dropped by the daemon and vice versa; the
matched `ModuleAbiIdentity` (one workspace, one std, one allocator —
checked, not assumed) is what makes that sound. Panic containment
lives **inside the module**: every exported entry point is a generated
wrapper running the real body under `catch_unwind` and returning an error
status — a panic never unwinds across the module boundary, which would
abort — and both sides build with `panic = "unwind"`: a panic in pipeline
code fails that job, not the daemon. The same containment rule covers
every **generated function-pointer table** — `DefaultTable` writers,
skip-writers, ctor tables (§12), per-type encode visitors (§4),
`MigrationFn` — whose entries are
macro-generated catch_unwind thunks: value producers return an error
status on panic (never unwind across the table), and drop entries catch,
**leak the value, and report** — a drop that unwound mid-rollback would
abort. Ownership obeys the same rule: no raw `Box<dyn Any>` — whose
destructor is bare module drop glue that automatic Rust drop would run
unwrapped — ever crosses an unloadable-module boundary; type-erased
values cross as `ErasedValue` (§4), constructed and destroyed only
through generated status thunks with automatic drop suppressed, and a
drop failure poisons the owning module epoch rather than trusting a
half-torn-down module (§15). This is the second, runtime poison entry
point, distinct from a candidate that fails to open: an epoch that was
already published atomically flips its shared epoch token to poisoned,
fences all new work through every snapshot that pins it, and can never
finish draining. Existing cleanup may continue only through contained
status thunks; `dlclose` is forbidden forever and the host deliberately
leaks the library. No later successful candidate retroactively makes that
epoch unloadable (§13's `PipelineState`).

The unpublished candidate follows the same ownership rule, with no implicit
Rust-destruction escape hatch. Its registration arena owns **every** module
object installed before validation finishes and records installation order.
The ownership handoff begins before registration can fail: each generated
registration shim wraps a module-owned object in an
`ErasedRegistrationCapsule` whose payload has automatic drop suppressed,
owner token is the candidate token, and destroy entry is a contained status
thunk. Ownership transfers to the host callback **on entry on every outcome**;
the generated capsule already owns a module-allocated intrusive arena node, so
the host never needs to allocate to establish ownership. The host thunk's
**first instruction inside its panic boundary** pure-moves the capsule into an
allocation-free callback-local guard, then pointer-links that node into the
candidate arena before duplicate checking, allocation, deep-copying, or any
other fallible action. Link/invariant failure leaves the local guard as sole
owner, latches the candidate's cleanup/leak disposition, and returns
`Consumed`; it can neither unwind into module frames nor return ownership.
Every later returned status also says `Consumed` explicitly. The host latches
the first rejected or panicking registration independently of the module's
return value, so ignoring a diagnostic `RegistrationStatus` can never publish
a partial candidate. Thus neither a normal rejection nor a host panic can
leave the caller believing it still owns a value, and reverse intrusive-list
cleanup is the sole destructor path — no raw/module automatic drop is
reachable.
Any post-open failure first fences the candidate token, then destroys arena
members in defined reverse installation order exclusively through contained,
status-returning module thunks; only after registration cleanup succeeds may
the host call the module's `unload`, and only after both steps succeed, the
token remains unpoisoned, and no token pin remains may it call `dlclose`. A
panic, error status, or leaked pin during either step publishes §13's typed
`CandidateOpen`-origin, `CandidateCleanup`-code poison with the exact failing
cleanup disposition and deliberately leaks the complete arena and library
forever. No automatic/raw Rust drop can run after `dlclose`, and a failed
candidate never borrows the published-epoch drain barrier it did not reach.

Containment is symmetric. Every call from module code back into a host-owned
trait object or registry method is a daemon-generated **host-side
`catch_unwind` STATUS thunk**. `EncodeSink`, `Registry`, `ProcessContext`, and
future reverse callbacks return explicit callback status; no callback whose
implementation belongs to the host has a bare `()` return across the module
ABI. The ABI audit enumerates both directions for every table revision:
module-owned call targets need module-side thunks, and host-owned call targets
need host-side thunks. A host callback panic becomes a typed job/registration
error and never unwinds through module frames.

What that does **not** cover — aborts,
allocator corruption, native-library crashes — kills the daemon; daemon
state is disposable (§2), so recovery is restart plus rebuild, and process
isolation stays descoped (§22). External toolchain binaries (dxc, texconv)
run as subprocesses as they do today. Pipeline-only native dependencies
(shaderc, spirv-cross) must be **statically linked** into the pipeline
cdylib — the dylib hash is the module's code identity and covers only
that one file, so a dependent `.so`/`.dylib`/DLL loaded at runtime could
change while every cache key stayed put. This is a pipeline build/link
contract checked by the pipeline build, not by teaching the daemon to parse
platform executable formats or reproduce the OS loader. Runtime `dlopen` by
pipeline code is therefore **banned outright**: there is deliberately no
staged-library resolution/open API. Anything dynamic is a §9 tool subprocess
invoked through `run_tool`, registered either as a complete hashed package
directory or as an explicitly ambient toolchain (§9). (The daemon module
host's own staged `dlopen` of the pipeline cdylib above is the hosting boundary,
not pipeline code.) The system runtime (libc, libSystem)
is acknowledged as part of `LayoutIdentity`'s (target, rustc) pair
(§5) and outside the dylib hash; system-runtime drift is not tracked
(§22).

### Module surface

The audited function table, and the registration API it is handed:

```rust
#[repr(C)]
pub struct PipelineModuleTableV2 {
    /// Checked first — cross-ABI-stable, at a #[repr(C)] prefix.
    pub abi_version: u32,
    /// ABI bootstrap: one of the calls made before the identity checks pass,
    /// so it is C-ABI — no Rust type crosses the boundary until the same-rustc
    /// host-interface contract is *verified*, not assumed. Writes the canonical
    /// ModuleAbiIdentity encoding into the caller's buffer;
    /// returns 0 with *len set, the required capacity if cap is too small,
    /// or <0 on error — a contained panic is a status, never an unwind.
    pub module_abi: unsafe extern "C" fn(buf: *mut u8, cap: u32, len: *mut u32) -> i32,
    /// Everything below is Rust-ABI under the now-checked same-rustc
    /// contract (the module_state.rs pattern). Every exported fn is a
    /// generated wrapper: panics are caught inside the module and returned
    /// as errors, never unwound across the boundary.
    pub register: unsafe fn(
        targets: &[TargetDefinition],
        arena: &mut CandidateRegistrationArena,
    ) -> Result<BTreeSet<String>, ModuleCallError>,
    /// Explicit cleanup before dlclose. An error (a contained panic
    /// included) means cleanup cannot be trusted: the daemon reports and
    /// **leaks the module** — never dlclosing corrupt state.
    pub unload: unsafe fn() -> Result<(), ModuleCallError>,
}

/// Embedded in daemon and module alike at build; compared daemon-side at
/// registration before any Rust-ABI use. This covers only the host interface —
/// what Registry, Outputs, and every by-value boundary type compile to, and
/// whose allocations cross ownership at the boundary. Asset data does not
/// cross here in native form; runtime compatibility is checked while loading
/// each authenticated artifact against its live descriptor.
pub struct ModuleAbiIdentity {
    pub rustc: String,                    // must equal the daemon's exactly
    pub interface_fingerprint: [u8; 32],  // shared host-interface build
                                          // fingerprint, computed identically
                                          // by both workspace builds
    pub measured_interface: [u8; 32],     // measured layout digest (§5) over
                                          // the boundary types themselves —
                                          // Registry, Outputs, AuthoredValue,
                                          // error types — the backstop no
                                          // declared-input hash can give
    pub panic_strategy: String,           // both sides "unwind" (§3)
    pub allocator: String,                // must be "system": both sides link
                                          // std::alloc::System, so cross-module
                                          // alloc/free is one malloc — a module
                                          // #[global_allocator] fails the check
}

/// The bundle decoder's sealed logical bootstrap. It has exactly five rows:
/// SchemaLineageManifest, Migration, ImportRecord, DirectoryImportRules, and
/// PackDefinition. Each row is `(symbol, TypeUuid, canonical logical-schema
/// bytes, DSLH)` and rows are strictly TypeUuid-sorted. The checked-in byte
/// resource is parsed and rehashed by every bundle consumer; it contains no
/// native layout, build-policy projection, or consumer-specific brand.
pub struct BootstrapControlSpecV1(pub [BootstrapControlSpecRowV1; 5]);

pub enum HostCallbackError {
    Panic(CallbackPanic),
    Rejected(CallbackRejection),
}

/// Generated at the module side of every object-bearing Registry call.
/// The module allocates the intrusive node together with the suppressed-drop
/// payload. Ownership transfers to the host callback on entry; linking the
/// node into the arena is allocation-free.
pub struct ErasedRegistrationCapsule {
    node: NonNull<RegistrationIngressNode>,
}
pub struct RegistrationIngressNode {
    prev: Option<NonNull<RegistrationIngressNode>>,
    next: Option<NonNull<RegistrationIngressNode>>,
    installation_seq: u64,
    payload: ManuallyDrop<ErasedRegistrationPayload>,
    owner: ModuleEpochToken,
    destroy: unsafe fn(*mut u8) -> Result<(), CallbackPanic>,
}
pub struct ErasedRegistrationPayload { pub ptr: *mut u8, pub kind: u32 }

/// Stack-owned by the host thunk from its first instruction inside
/// catch_unwind until the intrusive node is linked. No construction or move
/// allocates. If linking fails, this guard invokes contained cleanup or
/// deliberately leaks and latches CandidateOpen-origin DSPP poison.
pub struct RegistrationIngressGuard {
    capsule: ManuallyDrop<ErasedRegistrationCapsule>,
    linked: bool,
}

/// Every registration callback returns this disposition on both success and
/// failure. The generated ergonomic wrapper exposes the nested Result only
/// after observing `Consumed`; there is no "returned to caller" arm. The host
/// enforces rejection through its independent latch; must_use is diagnostic.
#[must_use = "registration status is diagnostic; the host already latched failure"]
pub struct RegistrationStatus {
    pub disposition: RegistrationDisposition,
    pub result: Result<(), HostCallbackError>,
}
pub enum RegistrationDisposition { Consumed }

pub struct CallbackRejection {
    pub code: u32,
    pub message: String,
}

pub enum EncodeError {
    Module(CallbackPanic),
    Host(HostCallbackError),
}

/// Every single-owner key — importer ID, tool id, migration-fn key, one
/// DefaultTable and one schema per type UUID, one processor per (input
/// type, target) (§9's overlap rule) — registered twice is an
/// epoch-opening error naming both registrants: the epoch never opens,
/// nothing last-write-wins. Only validators are deliberately additive
/// (§9: multiple per type all run).
impl Registry {
    pub fn importer<I: Importer>(&mut self, i: I) -> RegistrationStatus; // §8
    pub fn processor<P: Processor>(&mut self, p: P) -> RegistrationStatus; // §9; interface from P::outputs()/
                                                                // P::targets(), instance only for
                                                                // process-time state
    pub fn validator<V: Validator>(&mut self, v: V) -> RegistrationStatus; // §9
    pub fn migration_fn(&mut self, key: &str, f: MigrationFn) -> RegistrationStatus; // §11
    /// Default materialization for adoption (§6) and the *automatic*
    /// migration segment's default ops (§11 — custom edges instead carry
    /// literal values materialized at edge-authoring time, and never
    /// consult this table): registers the #[asset]-generated
    /// DefaultTable<T>. An op needing an entry the table lacks fails
    /// hard (§11); defaults are never fabricated.
    /// Implemented as `RegistrationArena::register_asset_defaults::<T>()`
    /// (`distill-pipeline-api/src/asset_defaults.rs`): a `PipelineDefaults`
    /// over T's table. Field defaults come from the entry at the container
    /// node's `Field` (or `Variant`+`Field`) path, parent defaults from the
    /// container type's own writer.
    pub fn defaults<T: AssetType>(&mut self, table: &'static DefaultTable<T>)
        -> RegistrationStatus;
    /// Tool registry behind ctx.run_tool (§9). Registration is an input
    /// event: the daemon snapshots a complete package directory or records an
    /// explicitly ambient toolchain identity, then publishes (id → execution
    /// identity + DSCT hash) as input-versioned state (§9, §13). Jobs resolve
    /// the tool through their pinned snapshot; an ambient registration without
    /// a trusted fingerprint makes every calling attempt nonmemoizable.
    pub fn tool(&mut self, id: &str, spec: ToolRegistration) -> RegistrationStatus;
}

/// Generated by #[asset], never hand-written. `parent` is present iff
/// T: Default held at macro expansion — a non-Default parent still
/// registers its field defaults, which no trait bound on the registration
/// call could express. All writers produce AuthoredValue.
pub struct DefaultTable<T: AssetType> {
    pub parent: Option<DefaultWriter>,
    /// Every defaultable node in T's finite canonical schema graph. The key
    /// is `(SchemaNodeId, node-local typed path)`, never an unrolled path from
    /// the root: recursive back-references reuse their first node id and
    /// therefore cannot make this table infinite. Within a node, PathStep is
    /// injective for variants and container/map positions. A migration or
    /// sparse-adoption target is first resolved to this key and succeeds iff
    /// the node type implements Default — detected by the generated
    /// compile-time autoref-specialization probe, never guessed.
    pub nodes: &'static [(SchemaNodeId, &'static [PathStep], DefaultWriter)],
    /// One row per assigned SchemaNodeId: the node type's canonical logical
    /// bytes and its own writer. The migration provider matches a frame's
    /// to-schema container node against these bytes to find its id.
    pub types: &'static [DefaultNodeType],
    pub _marker: PhantomData<fn() -> T>,
}

/// Assigned deterministically during the canonical first-expansion walk of
/// the logical schema graph. Fields and variants use the §5 canonical order;
/// a repeated/recursive type emits a back-reference to the assigned id.
pub struct SchemaNodeId(pub u32);

/// A generated catch_unwind thunk (§3's table rule): a panicking user
/// Default is an error status, never an unwind across the boundary.
pub type DefaultWriter = fn() -> Result<AuthoredValue, CallbackPanic>;

pub enum PathStep {                       // the static-table form of PathSeg (§9)
    Field(&'static str),
    Variant(&'static str),
    Elem,                                 // Vec/array/set/Option element
    MapKey,
    MapValue,
}

/// A loaded module at a fixed epoch: the registrations above plus the
/// dylib content hash. Jobs hold one for their whole run (epochs, above).
/// On successful candidate publication the registration arena moves intact
/// into this handle; epoch retirement uses the same reverse-order status
/// cleanup before `unload` and `dlclose`.
pub struct ModuleHandle { /* registration_arena, dylib_hash, epoch */ }

/// Host-owned state created before the candidate can become a PipelineEpoch.
/// Entries own every installed module object through contained status-drop
/// thunks and are destroyed in reverse installation order. Automatic Rust
/// drop is suppressed; the arena and library leak together if cleanup,
/// unload, token poison, or a remaining pin makes dlclose unsafe.
pub struct CandidateRegistrationArena {
    owner: ModuleEpochToken,
    head: Option<NonNull<RegistrationIngressNode>>,
    tail: Option<NonNull<RegistrationIngressNode>>,
    /// First rejection/panic/link failure wins; checked after register even
    /// when module code ignored every returned RegistrationStatus.
    latched_failure: Option<HostCallbackError>,
    cleanup_disposition: CandidateCleanupState,
}
pub enum CandidateCleanupState {
    Clean,
    MustCleanup,
    /// Exact DSPP disposition, restricted here to
    /// RegistrationCleanupFailed..DlcloseFailed (§13).
    MustLeak(CleanupDisposition),
}

/// One validated publication unit: the schema registry, module handle,
/// pipeline map, planner version, AND the target configuration (§18)
/// that passed the §5 identity checks
/// *together* at epoch open — the pipeline map is constructed and
/// validated against exactly this epoch's target set, so a snapshot can
/// never pair a target definition with a map built for a different one.
/// Everything downstream — build jobs,
/// LoadContext (§11) — borrows one epoch, never fields from two, so a
/// fresh registry can never pair with a stale module's tables.
pub struct PipelineEpoch { /* schemas, module, pipeline_map, planner_version, targets */ }
```

## 4. Asset Type System

```rust
#[asset(uuid = "d4079e74-3ec9-4ebc-9b77-a87cafdfdada")]
pub struct HpBarConfig {
    pub color_curve: AssetRef<ColorCurve>,
    pub max_hp: f32,

    #[asset(skip)]
    pub cached_mesh: Option<GpuMeshHandle>,
}
```

Source identity is not another function-table entry. The module emits New Game
Plus's existing four data symbols—`__NGP_SOURCE_HASH_PTR`,
`__NGP_SOURCE_HASH_LEN`, `__NGP_CRATE_NAME_PTR`, and
`__NGP_CRATE_NAME_LEN`—through the focused `NgpSourceIdentity` derive. The
daemon reads them through `ngp-module-host::read_source_identity`, exactly as
New Game Plus does, and compares the copied `(crate_name, source_hash)` with the
watched schema before calling `register`.

| Attribute | Scope | Meaning |
|-----------|-------|---------|
| `#[asset(uuid = "…")]` | struct | Asset type with a stable 128-bit type UUID |
| `#[asset(skip)]` | field | Excluded from schema, sources, artifacts; `Default` on load |
| `#[asset(blob)]` | field | Stored in the bundle's binary blob chunk; runtime type is `Blob`, an `Arc`-backed byte range — pack loads borrow the mmap, never copy |
| `#[asset(tag)]` | field | String-typed field indexed for queries (§10); re-indexed whenever the bundle is dirtied |
| `#[asset(rev = N)]` | struct, enum variant, or field | Semantic revision: bumped when meaning changes without shape (metres → centimetres). Participates in the logical hash, so the bump is migratable like any structural change — and only by custom edge: the automatic planner hard-stops on a rev mismatch (§11). A variant's rev is carried by its own `EnumVariant` TypeDef's `attrs.rev` (§5) — exactly one declared carrier |

The type UUID is stable across renames and refactors, and distinct from schema
hashes.

An item carrying the root `#[asset(uuid = ...)]` attribute MUST NOT declare
generic type, lifetime, or const parameters. Both macro expansion and
source-walk extraction reject such a root before emitting a descriptor or
schema; there is no open-ended family behind one TypeUuid. Concrete
monomorphizations remain legal **inside** the reachable schema graph (for
example, a non-generic asset field of type `Curve<f32>`), where the complete
arguments are part of the nested nominal key (§5).

Identity and hash types, used throughout:

```rust
pub struct AssetUuid(pub [u8; 16]);   // minted at authoring; UUIDv5 for derived outputs (§7, §9)
pub struct BundleUuid(pub [u8; 16]);
pub struct TypeUuid(pub [u8; 16]);    // #[asset(uuid = "…")]
pub struct LogicalHash(pub [u8; 32]); // blake3 over the §5 grammar
pub struct LayoutHash(pub [u8; 32]);  // blake3 over the §12 DSWL grammar
pub struct ContentHash(pub [u8; 32]);        // blake3 of artifact bytes (§12)
/// Raw blake3 of the exact observed bundle-file bytes, valid or malformed.
/// This is byte identity only; canonical bundle validation is a separate gate.
pub struct BundleFileHash(pub [u8; 32]);

/// Implemented by #[asset]; never hand-written.
pub trait AssetType: 'static {
    const TYPE_UUID: TypeUuid;
    /// The consuming binary's generated runtime descriptor — an associated
    /// fn, not a const, because the tables it borrows are statics, which
    /// consts cannot refer to (the §9 Processor precedent).
    fn descriptor() -> &'static AssetRuntimeDescriptor;
}

/// Everything a consuming binary needs to receive an artifact of this type:
/// generated by #[asset] alongside the tables it points into. The loader
/// resolves a fetched artifact's `TypeUuid` to one of these (§15) — without
/// it, a vector, blob, or skipped field in a fetched artifact has no
/// declared path to its constructor, drop entry, or skip-default writer.
/// Digests alone are not enough: plan compilation needs the measured
/// native tree itself (destination offsets, tag writes, skip positions),
/// and `AssetStorage::update` needs an owned type-erased value — both are
/// generated facts only the descriptor can supply.
pub struct AssetRuntimeDescriptor {
    pub type_uuid: TypeUuid,
    /// Checked against the authenticated artifact terminal DSLH. A mismatch
    /// is registry disagreement, never a migration trigger.
    pub logical_hash: LogicalHash,
    /// Local runtime policy. A loader rejects this descriptor before plan
    /// compilation; pack construction separately rejects build-only authored
    /// closure members from the watched schema.
    pub build_only: bool,
    /// The measured native layout tree (§12), table ids annotated in
    /// place — the native-side input to fixup-plan compilation.
    pub native_layout: &'static NativeLayoutNode,
    pub size: usize,
    pub align: usize,
    pub ctors: &'static CtorTable,               // §12
    pub drops: &'static DropTable,               // §12
    pub skip_writers: &'static SkipWriterTable,  // §12
    /// Move a completed fixed-up value out of its plan-execution buffer
    /// into the owned, type-erased form `AssetStorage::update` consumes
    /// (§15). The buffer must hold a complete `T`; afterwards the buffer
    /// is uninitialized again. No-unwind (§3). `ErasedValue` (below),
    /// never a bare `Box<dyn Any>`: a raw box's destructor is module
    /// drop glue that automatic Rust drop would run unwrapped across
    /// the unloadable-module boundary — the erased owner suppresses
    /// automatic drop and destroys only through its status thunk. No
    /// `Send + Sync` bounds, deliberately: the engine model is
    /// single-threaded (§15) and `AssetType` requires only `'static` —
    /// undeclared auto-trait bounds here would silently exclude assets
    /// with thread-affine skipped/GPU fields.
    /// The host passes the owning registered `ModuleEpochToken` explicitly;
    /// the thunk embeds that exact token in the returned `ErasedValue`.
    /// Ambient or tokenless ownership inference is forbidden.
    pub finalize: unsafe fn(src: *mut u8, owner_epoch: ModuleEpochToken)
        -> Result<ErasedValue, CallbackPanic>,
    /// Lower a typed value into the neutral wire-builder vocabulary —
    /// the only form in which a processor output crosses the module
    /// boundary (§9's result binding, §12's encoder). A no-unwind
    /// generated visitor (§3's thunk rule): it walks the value and
    /// emits flat bytes, container begin/push/finish events in
    /// canonical order — maps and sets iterated in their deterministic
    /// §5 serialization order (encoded key/element bytes), never native
    /// iteration order — blob byte handoffs, and typed reference
    /// emissions (strong/weak + target uuid). Without it,
    /// `AssetRuntimeDescriptor` could construct and drop values but
    /// never deterministically read one back out: raw layout cannot
    /// iterate a HashMap, extract Blob backings, or tell a strong
    /// reference from a weak one.
    pub encode: unsafe fn(value_ptr: *const u8, sink: &mut dyn EncodeSink)
        -> Result<(), EncodeError>,
}

/// The owned, type-erased form asset values take across the module
/// boundary — game-side storage (§15) holds exactly this, never a
/// `Box<dyn Any>`. Construction (`finalize`, placeholder thunks §15)
/// and destruction both go through generated no-unwind STATUS thunks —
/// §12's ctor/drop-table vocabulary extended with a status return,
/// because the ownership boundary must OBSERVE a drop failure.
/// Automatic Rust drop is suppressed (ManuallyDrop semantics); the
/// value is destroyed only by calling `drop_thunk`, which runs the real
/// drop under catch_unwind inside the owning module and returns a
/// status. On Err the value is deliberately leaked and the owning
/// module epoch is poisoned (§15): a Drop that panicked mid-teardown
/// cannot be trusted, so `drain_complete` never reports true and the
/// host leaks the module rather than dlclosing corrupt state (§3's
/// failed-unload rule).
pub struct ErasedValue {
    ptr: *mut u8,                    // the value; ManuallyDrop by construction
    type_uuid: TypeUuid,
    drop_thunk: unsafe fn(*mut u8) -> Result<(), CallbackPanic>,
    /// Shared poison/residency token of the module epoch whose code owns
    /// `drop_thunk` and the value. Every status-returning destruction path
    /// reports through this token; it outlives the erased value and makes
    /// the published-epoch poison transition observable to the host (§3,
    /// §13, §15).
    owner_epoch: ModuleEpochToken,
}

pub struct ModuleEpochToken { /* epoch id + shared poison/residency state */ }

/// The daemon-side consumer of a generated encode visitor's events —
/// the neutral vocabulary is the whole interface, so no typed Rust
/// value ever crosses the module boundary on the output path. §12's
/// encoder builds wire bytes from the events; §9's result binding
/// consumes the reference emissions.
pub trait EncodeSink {
    /// Flat value bytes (scalars, flat structs) at the current position.
    fn flat(&mut self, bytes: &[u8]) -> Result<(), HostCallbackError>;
    /// Container events, in canonical order (§5): the visitor — not the
    /// sink — owns iteration order, and emits map/set members sorted by
    /// their encoded bytes.
    fn begin(&mut self, kind: EncodeContainer, len: u32) -> Result<(), HostCallbackError>;
    fn push(&mut self) -> Result<(), HostCallbackError>;
    fn finish(&mut self) -> Result<(), HostCallbackError>;
    /// #[asset(blob)] payload handoff — the bytes, never a slot encoding.
    fn blob(&mut self, bytes: &[u8]) -> Result<(), HostCallbackError>;
    /// Typed reference emission: §9's result binding validates each one
    /// against the job's snapshot and records the TraceOp::RefCheck.
    fn reference(&mut self, strong: bool, target: AssetUuid, expected_terminal: TypeUuid)
        -> Result<(), HostCallbackError>;
}

pub enum EncodeContainer { Vec, Array, Set, Map, Option, Box, Arc, Str,
                           Struct, Variant(u32) }
```

### References are queries

An `AssetRef<T>` in source data is an `AssetQuery` (§10) that must resolve to
exactly one asset. The serialized forms are query encodings — none is more
canonical than another:

- `{ "path": "characters/hero.bundle" }` — by path, resolves to the target bundle's primary
- `"characters/hero.bundle"` — shorthand by path when the string is not UUID-shaped
- `{ "path": "characters/hero.bundle", "asset": "mesh/body" }` — by path + local id
- `{ "asset": "mesh/body" }` — by local id within the same bundle
- `"91a2500a-…"` — by UUID

String parsing has one deterministic precedence rule: a bare string that is
syntactically a UUID is **always** a UUID selector, even if a bundle path with
the same bytes exists. A UUID-shaped path MUST use the explicit
`{ "path": "..." }` object form (with optional `asset`); resolution never
falls back from a failed UUID lookup to path lookup.

Resolution failure (zero results) and ambiguity (multiple results) are build
errors carrying diagnostics; the daemon never rewrites authored references.
Authoring tools prefer writing the **UUID form** for cross-bundle references —
UUID references survive renames with no fixup at all; path forms remain
first-class for hand-authoring, and local-id forms for same-bundle
references. Where path references do require fixup, a rename through the
editor RPC runs as an explicit **long-running operation** (§17) with
progress and a summary — never a silent side effect. In binary artifacts,
every reference is a resolved 16-byte UUID — and a reference a *processor*
emits into an artifact is validated at result binding exactly like an
authored one: resolved as (uuid, expected terminal type) against the
job's snapshot and recorded as a revalidated trace entry (§9's
`TraceOp::RefCheck`), so deleting or retyping the target invalidates
every cached artifact that carries a typed reference to it.

`WeakAssetRef<T>` is the non-gating variant: the same query encodings,
resolved during the build with the same recorded dependencies, encoded in
artifacts as the same 16-byte UUID — but it never enters load-dependency
lists. It resolves against the loaded set after load and yields nothing
when the target is absent; intentionally cyclic data uses it to break
cycles (§9). The reference forms above are exact-match `AssetQuery`
selectors (§10) — uuid, bundle path (+ primary), (bundle path, local id),
same-bundle local id — each with its own invalidation index.

Native forms, defined by the asset-types crate:

```rust
/// 16 bytes native and wire (§12): the resolved UUID. The query encodings of
/// this section exist only in bundle JSON; build import resolves them.
#[repr(transparent)]
pub struct AssetRef<T: AssetType>(AssetUuid, PhantomData<fn() -> T>);
#[repr(transparent)]
pub struct WeakAssetRef<T: AssetType>(AssetUuid, PhantomData<fn() -> T>);

impl<T: AssetType> AssetRef<T> {       // same for WeakAssetRef
    pub fn new(uuid: AssetUuid) -> Self;  // typed by declaration; the pipeline
    pub fn uuid(&self) -> AssetUuid;      // map validates T at build/read time
}

/// #[asset(blob)] runtime type: an Arc-backed borrowed byte range — a pack
/// mmap extent, a range of a fetched artifact buffer, or a bundle blob
/// chunk. Never a copy (§12, §16).
pub struct Blob { backing: Arc<dyn AsRef<[u8]> + Send + Sync>, offset: usize, len: usize }
impl Deref for Blob { type Target = [u8]; }
```

## 5. Schema System

source-walk analyzes the asset-types crate and emits schema JSON (reusing
`ngp-schema`'s `Schema`/`TypeDef`/`Field` model). The daemon watches this file
and reloads on change. `source-walk` owns workspace-source observation and
emits the schema artifact atomically; the daemon neither crawls the Cargo
workspace nor independently recomputes source freshness. The schema's shared
New Game Plus source identity (`ngp-source-hash`) pairs that emitted artifact
with the pipeline module compiled from the same source state. An edited source
tree that has not produced a new schema or module artifact is not a daemon
input, and an independently emitted schema/module mismatch rejects only that
candidate while the last accepted epoch remains pinned by existing snapshots.
The source identity is deliberately not an ABI or safety attestation and does
not reconstruct a Cargo dependency closure. The recorded schema lineage (§6,
§11, §13) remains the authority for authored-data ancestry: data whose stamped
cursor is not a proved forward ancestor of the registry cursor refuses
schema-dependent builds with a staleness error naming both hashes, rather than
being automatically diffed backward into older semantics. Every physical
layout table carries a small
**`LayoutIdentity`** used only to select and bind the table: exact target
triple, rustc identity, and source-walk extraction/layout algorithm version.
It is not exported by the pipeline module and does not claim to enumerate the
sources, features, cfgs, manifests, lock nodes, build-script inputs, or other
causes that produced the layout.

In authoring mode §3's daemon-resident development supervisor owns the
`source-walk` and Cargo child-process lifetimes. That does not change this
input boundary: workspace source paths are inputs to those producer processes,
not daemon asset inputs; only their atomically published schema/module
artifacts are admitted by the daemon watcher and candidate gate.

Agreement is checked at the boundary that consumes it. An opened but
unpublished pipeline module exports the same New Game Plus source identity
that source-walk records in the schema. Candidate acceptance requires equality
before any Rust-ABI registration call; the staged dylib hash identifies
pipeline behavior and `ModuleAbiIdentity` separately covers the host interface.
Runtime consumers do not need the pipeline module's native layout: each
authenticated artifact carries its terminal `TypeUuid`, `LogicalHash`, and
DSWL tree, and the loader compiles that tree against its own live descriptor.

Each target resolves to a bound `LayoutIdentity` (§18) that must match the
layout table used to encode its artifacts. Actual output keys additionally
bind their logical and DSWL layout hashes (§9), so a changed table cannot reuse
old artifacts merely because it was produced by the same target/toolchain.
Until per-target layout emission lands (§22), the only layout table is the
host's, so buildable targets degenerate to host-identity targets.

```rust
pub struct LayoutIdentity {
    pub target_triple: String,          // exact, e.g. "aarch64-apple-darwin"
    pub rustc: String,                  // rustc -vV verbose version, commit hash included
    pub algorithm_version: u32,         // source-walk extraction + layout algorithm
}
```

Identity records hash and compare by a **canonical record encoding**,
normative here: fields in declaration order; integers LE fixed-width;
`str` as `u32` length + NFC UTF-8 bytes; `[u8; 32]` raw; sets sorted,
deduplicated, and encoded as `u32` count + elements; sequences as `u32`
count + elements; `Option` as a `u8` presence marker + payload; enums as
a `u8` discriminant in declaration order + payload; domain-prefixed and
versioned — `blake3("DSLI" ‖ version:u8 ‖ fields)` for
`LayoutIdentity`, `blake3("DSMA" ‖ version:u8 ‖ fields)` for
`ModuleAbiIdentity` (distinct meanings never share a domain — the two
records answer different questions and must never alias), `blake3("DSTG" ‖
version:u8 ‖ fields)` for the target-definition hash (§18). Two builds
of any component can never disagree on an identity's bytes. The same
encoding serializes **every composite this document hashes or stores as
a key** — `StaticInputs` (domain `"DSSI"`), `TraceOp` sequences (`"DSTR"`),
the `AssetQuery` inside a trace entry, output tables — so no `‖` formula
in this document can repartition variable-length fields.

Every **semantic or composite** hash in this document is domain-prefixed;
the domain strings form one table, no two of which may ever prefix the
same byte meaning. The rule is total over that class: every semantic or
composite hash construction must appear here; an unregistered one is a
spec defect, since that is how two meanings come to share bytes:

| Domain | Hashes |
|---|---|
| `"DSLI"` | `LayoutIdentity` records (§5) |
| `"DSMA"` | `ModuleAbiIdentity` records (§3, §5) — the host-interface identity, never sharing `LayoutIdentity`'s domain |
| `"DSTG"` | target-definition hash (§18) |
| `"DSSL"` | schema-lineage chain digest — a type's accepted epoch records, explicit parent links, and current cursor (§6, §11, §13) |
| `"DSLH"` | logical hash — the schema AST grammar (§5) |
| `"DSCT"` | tool-execution identity — complete packaged-directory bytes or explicit ambient toolchain identity, plus sanitized environment and cwd policy (§9, §13) |
| `"DSWL"` | layout hash — the wire tree (§12) |
| `"DSSI"` | static-input key (§9) |
| `"DSIH"` | full input hash — the witnessed all-inputs composite a validated hit implies (§9); never stored as an index |
| `"DSTR"` | dependency-trace digest (§9) |
| `"DSBI"` | build-import pre-key (§8) |
| `"ASTQ"` / `"FILQ"` | asset-namespace / raw-file query result hashes (§10, §8) |
| `"DSEK"` | pack encoding key (§16) |
| `"DSPM"` | pack manifest hash (§16) |
| `"DSLF"` | canonical local deterministic-failure detail (§9) |
| `"DSCP"` | canonical configuration-poison reason (§13, §17, §18) |
| `"DSVP"` | canonical version-global poison identity (§7, §13, §17) |
| `"DSPP"` | canonical pipeline-poison identity (§3, §13, §17) |

`DSLF` and `DSCP` are declared tagged-record grammars encoded only through the
canonical record codec above. Their v1 declarations are exhaustive:

```rust
#[repr(u16)]
pub enum LocalFailureClass {
    Validator = 1, MigrationPlan = 2, Processor = 3,
    MigrationFunction = 4, OutputBinding = 5, Importer = 6,
    ImportIntake = 7, ArtifactEncoding = 8,
}
pub enum DslfV1 { // tag is the LocalFailureClass value
    Validator {
        asset: AssetUuid, type_uuid: TypeUuid,
        /// Error FieldPaths sorted by canonical path bytes; multiplicity is
        /// retained. Warning rows and presentation messages are not failure.
        error_paths: Vec<FieldPath>,
    },
    MigrationPlan {
        type_uuid: TypeUuid, from: LogicalHash, to: LogicalHash,
        failure: MigrationPlanFailureV1,
    },
    Processor {
        asset: AssetUuid, processor_id: String, processor_version: u32,
        stage: u16, build_error_code: u32,
    },
    MigrationFunction {
        asset: AssetUuid, type_uuid: TypeUuid,
        from: LogicalHash, to: LogicalHash,
        function_key: String, migration_error_code: u32,
    },
    OutputBinding {
        asset: AssetUuid, processor_id: String, processor_version: u32,
        stage: u16, failure: OutputBindingFailureV1,
    },
    Importer {
        importer_id: String, importer_error_code: u32,
        /// Canonical rooted source set, root-name/path sorted.
        sources: Vec<RootedPath>,
    },
    ImportIntake {
        importer_id: String, failure: ImportIntakeFailureV1,
    },
    ArtifactEncoding {
        asset: AssetUuid, encoded_type: TypeUuid,
        failure: ArtifactEncodingFailureV1,
    },
}
#[repr(u16)]
pub enum MigrationPlanFailureV1 {
    MissingPath { path: FieldPath } = 1,
    UnwrittenDestination { path: FieldPath } = 2,
    DuplicateDestination { path: FieldPath } = 3,
    RevisionMismatch { path: FieldPath,
                       expected_revision: u32,
                       observed_revision: u32 } = 4,
    /// Both vectors sort/deduplicate raw Migration AssetUuid bytes.
    AmbiguousEdge { conflicting_edges: Vec<AssetUuid> } = 5,
    Cycle { cycle_edges: Vec<AssetUuid> } = 6,
    MissingReverseEdge { missing_from: LogicalHash,
                         missing_to: LogicalHash } = 7,
    NonConformingOutput { edge: AssetUuid, path: FieldPath } = 8,
    MapKeyCollision { path: FieldPath } = 9,
    SetElementCollision { path: FieldPath } = 10,
    MissingDefault { path: FieldPath } = 11,
}
#[repr(u16)]
pub enum OutputBindingSlotV1 {
    Primary = 1,
    Extra { output_key: String } = 2,
}
#[repr(u16)]
pub enum OutputBindingFailureV1 {
    MissingPrimary = 1,
    DuplicatePrimary { observed_count: u32 } = 2,
    MissingExtra { output_key: String,
                   expected_type: TypeUuid } = 3,
    DuplicateExtra { output_key: String,
                     expected_type: TypeUuid,
                     observed_count: u32 } = 4,
    UndeclaredExtra { output_key: String,
                      observed_type: TypeUuid } = 5,
    TypeMismatch { slot: OutputBindingSlotV1,
                   expected_type: TypeUuid,
                   observed_type: TypeUuid } = 6,
    InvalidOutputKey { output_key: String } = 7,
    DuplicateDebugKey { debug_key: String } = 8,
    EncodeRejected { slot: OutputBindingSlotV1,
                     encoded_type: TypeUuid,
                     failure: ArtifactEncodingFailureV1 } = 9,
}
#[repr(u16)]
pub enum ImportIntakeFailureV1 {
    DuplicateLocalId { local_id: String } = 1,
    ReservedLocalId { local_id: String } = 2,
    PrimaryAlreadyDeclared { first_local_id: String,
                             attempted_local_id: String } = 3,
    PrimaryMissing { local_id: String } = 4,
    VanishedPriorPrimary { bundle: BundleUuid,
                           local_id: String } = 5,
    RoleViolation { local_id: String,
                    expected_role: ImportEntryRoleV1,
                    observed_role: ImportEntryRoleV1 } = 6,
    TypeMismatch { local_id: String,
                   expected_type: TypeUuid,
                   observed_type: TypeUuid } = 7,
    NonCanonicalValue { local_id: String, type_uuid: TypeUuid,
                        path: FieldPath } = 8,
    SettingsInvalid { settings_type: TypeUuid,
                      path: FieldPath } = 9,
}
#[repr(u8)]
pub enum ImportEntryRoleV1 { Runtime = 1, AuthoringOnly = 2 }
#[repr(u16)]
pub enum ReferenceObservationV1 {
    Missing = 1,
    Found { observed_terminal: TypeUuid } = 2,
}
#[repr(u16)]
pub enum ArtifactEncodingFailureV1 {
    SizeLimit { limit: u64, observed: u64 } = 1,
    NonCanonicalOrder { path: FieldPath } = 2,
    ValueSchemaMismatch { path: FieldPath,
                          expected_type: TypeUuid,
                          observed_type: TypeUuid } = 3,
    LayoutUnavailable = 4,
    InvalidScalar { path: FieldPath } = 5,
    InvalidReference { path: FieldPath,
                       expected_terminal: TypeUuid,
                       observed: ReferenceObservationV1 } = 6,
}

#[repr(u16)]
pub enum ConfigurationPoisonCode {
    MalformedConfiguration = 1,
    NonLoopbackAddress = 2,
    DuplicateRootName = 3,
    InvalidPath = 4,
    OwnedPathOverlap = 5,
    EmptyTargetApis = 6,
    InvalidParallelism = 7,
    InvalidBatchReservation = 8,
    DirectoryAlias = 9,
    MissingLineageManifest = 10,
    DuplicateLineageManifest = 11,
    UnsupportedTargetIdentity = 12,
    DuplicateTargetName = 13,
    ConfigurationSourceUnavailable = 14,
}
pub enum DscpV1 { // tag is the ConfigurationPoisonCode value
    MalformedConfiguration { file_hash: [u8; 32] },
    NonLoopbackAddress { address: String },
    DuplicateRootName { normalized_name: String },
    InvalidPath { key: ConfigurationPathKey,
                  normalized_or_raw_path: String },
    OwnedPathOverlap { first: OwnedPathSide, second: OwnedPathSide },
    EmptyTargetApis { target: String },
    InvalidParallelism { value: u32 },
    InvalidBatchReservation { parallelism: u32, reservation: u32 },
    DirectoryAlias { first: DirectoryAliasSide, second: DirectoryAliasSide },
    MissingLineageManifest,
    DuplicateLineageManifest { claimants: Vec<LineageManifestClaimant> },
    UnsupportedTargetIdentity { target: String,
                                expected: LayoutIdentity,
                                observed: LayoutIdentity },
    DuplicateTargetName { normalized_name: String },
    ConfigurationSourceUnavailable {
        path: ConfigurationSourcePathBytes,
        failure: ConfigurationSourceFailure,
    },
}
#[repr(u8)]
pub enum ConfigurationPathKey {
    AssetRoot = 1, StatePath = 2, SchemaPath = 3,
    PipelineDylib = 4, CodegenRsModPath = 5, LineageManifest = 6,
}
#[repr(u8)]
pub enum OwnedPathKind {
    AssetRoot = 1, DaemonState = 2, SchemaArtifact = 3,
    PipelineModule = 4, CodegenOutput = 5, Quarantine = 6,
}
pub struct OwnedPathSide { pub kind: OwnedPathKind, pub path: String }
pub struct DirectoryAliasSide {
    pub normalized_path: String,
}
#[repr(u8)]
pub enum ConfigurationSourcePathBytes {
    Unix(Vec<u8>) = 1,
    Windows(Vec<u16>) = 2,
}
#[repr(u16)]
pub enum ConfigurationSourceFailure {
    Missing = 1,
    PermissionDenied = 2,
    InvalidFileType = 3,
    IoDataLoss = 4,
}
pub struct ConfigurationPoison {
    pub code: ConfigurationPoisonCode,
    /// Exact same-code canonical DSCP v1 payload. This is the typed reason;
    /// `message` is never allowed to substitute for it.
    pub detail: DscpV1,
    pub reason_hash: [u8; 32],
    pub message: String, // presentation only; never hashed
}
```

`DSLF v1 = blake3("DSLF" || 0x01 || LocalFailureClass:u16 || variant
fields)` and `DSCP v1 = blake3("DSCP" || 0x01 ||
ConfigurationPoisonCode:u16 || variant fields)`. Fields are encoded in the
orders above. Every nested `*FailureV1`/`*SlotV1`/`ReferenceObservationV1`
variant begins with its pinned `u16` tag shown above; `ImportEntryRoleV1` uses
its pinned `u8` tag. Each is followed by exactly that variant's required
fields: absent data is represented by a different variant, never by an
optional catch-all field. Strings/paths are normalized and
length-framed, and every unordered vector is sorted as its comment states
with duplicates rejected unless multiplicity is explicitly retained.
`DuplicateLineageManifest.claimants` sorts and deduplicates the complete
injective `(root_name, normalized_path, bundle, normalized local_id, asset,
file_hash)` rows, so two entries in one bundle remain distinct. Every
semantically symmetric
pair is canonically ordered before encoding: `ConfigurationPathKey` and
`OwnedPathKind` are their fixed `u8` values above (unknown values reject),
`OwnedPathOverlap` sorts the two `(kind:u8, path)` records by canonical bytes,
and `DirectoryAlias` sorts its two `normalized_path` records the same way. A new symmetric
variant must state its pair ordering explicitly.
`DuplicateTargetName` is code 13 everywhere (candidate validation, persisted
configuration state, RPC `ConfigurationPoison.code`, and diagnostic decode)
and carries exactly the one NFC-normalized colliding name. It is the required
mapping for canonical target-row duplicate-name rejection and is never reported as
`DuplicateRootName` or a parser catch-all.
`ConfigurationSourceUnavailable` is code 14 everywhere and carries the exact
raw configured path as Unix bytes or Windows UTF-16 code units plus exactly one
fixed failure code. A watched configuration file that is deleted, becomes
unreadable, changes to a disallowed file type, or cannot be read without data
loss publishes this poison; the daemon never silently reverts to defaults.

Configuration validation enumerates the complete canonical defect set before
choosing authority. Candidate DSCP values are canonical-encoded, deduplicated,
and sorted by `(ConfigurationPoisonCode:u16, detail bytes)`; the first row is
the sole persisted/snapshot/RPC `ConfigurationPoison`, while `doctor` may show
the complete sorted set. Parser, watcher, and validation completion order can
never select the winner. The lineage-repair bootstrap is enabled only when
that authoritative first defect is `MissingLineageManifest` or
`DuplicateLineageManifest`; the presence of either defect later in the
doctor-only set grants no repair capability.
Presentation messages, backtraces, OS prose, and parser prose are excluded.
`ConfigurationPoison` persistence stores the explicit DSCP version plus the
complete canonical detail bytes, never only the code/digest. Decode requires
the detail variant's tag to equal `code`, recomputes `reason_hash`, and rejects
an unknown version/tag, a wrong code/detail pair, noncanonical detail, or a
digest mismatch.
An unknown version/code is a hard failure on persisted or RPC decode; a new
variant requires a grammar version bump unless it consumes a previously
reserved discriminant whose optional-field framing was already pinned. These
grammars are the sole inputs to `StableFailureFingerprint::Local.detail` and
`ConfigurationPoison.reason_hash`. The DSLF class list is total over every
memoizable local producer named by this design: importer-returned failures,
daemon import-intake/fold failures, migration-function returns, output
binding, and artifact encoding join validators, migration planning, and
processor returns. Callback panics and transient infrastructure failures stay
outside DSLF because they poison or do not memoize; another deterministic
local producer requires a class and exact v1 arm before it may memoize.

Candidate target sets are canonical rows, not a digest plus rows. Names are
NFC-normalized, rows sort by name bytes, duplicate normalized names reject,
and each row is `(name:str, target_definition_hash:[u8;32])` with a recomputed
DSTG (§18). Candidate staging, schema acceptance, and commit compare the exact
rows transactionally. Persisting or transmitting a second hash beside the
complete rows adds no stale-work or atomicity guarantee.

Tag annotations are read directly from the watched shared schema. The daemon
uses the authenticated schema file's source hash as a conservative local
tag-index generation: any schema edit invalidates the projection, while tag
facts are never copied into a second cross-binary registry grammar.

The explicit exception class is **byte-identity digests**. These are raw
`blake3` over exactly the bytes their name identifies, deliberately
domainless because their meaning is byte equality rather than a semantic
record: `ContentHash` over complete DSTL artifact bytes; tracked raw-file and
`BundleFileHash` values over the exact observed file bytes, whether valid or
malformed (canonical bundle validation is independent); staged pipeline-
dylib hashes over the executable bytes; each `ToolPackageFile.bytes_hash` over
that exact packaged member's bytes (the DSCT aggregate remains semantic); CAS record
`content_hash` values over payload bytes; and pack/archive per-file
trailers and archive-reference file hashes over the preceding/full named
file bytes as §16 specifies. This list is closed: a new domainless digest
must first be added to this exception class. `DSEK` and `DSPM` remain
semantic/composite hashes and therefore remain domain-prefixed.

**Local layout soundness.** Source-walk emits the authenticated DSWL producer
tree used to encode artifacts. Each consuming binary independently generates
the exact `NativeLayoutNode`, constructor, drop, and skip tables used to load
into its own Rust type. The loader authenticates DSWL, checks terminal
TypeUuid/DSLH against the local descriptor, and compiles a plan from the wire
tree to that descriptor. A successful plan is the compatibility proof; no
cross-binary native-layout equality is required.

Plans are ephemeral to one sweep. They are never persisted or shared across
module epochs, so binary-local table identifiers need no digest. If an
epoch-local plan cache is introduced later, its complete authority is the
resident epoch plus `(TypeUuid, LayoutHash)`; retirement destroys it before
module cleanup.

### One model, split — not a second one

All schema code is **shared code in `ngp-schema` and source-walk,
refactored in place** — the module-hosting rule (§3) applied to schemas:
distill contains no schema-model code. The refactor, normative here:

```rust
// ngp-schema, refactored: logical facts and measured layout become
// separate records — the two projections this section hashes stop being
// conventions and become types.
pub struct Schema {
    pub source_hashes: BTreeMap<String, String>,
    pub type_ops_hash: String,        // paired generated module TypeOps table
    pub layout_hashes: BTreeMap<String, String>, // NGP module reload metadata
    pub rustc_version: String,        // producer metadata, not logical identity
    pub types: Vec<TypeDef>,          // logical facts + native matching metadata
    pub layouts: Vec<SchemaLayouts>,  // ≥ 1, one per compilation identity
}
pub struct TypeDef {                  // logical: target-free
    pub id: SchemaTypeId,
    pub kind: PrimitiveType,          // gains I128 (§19)
    pub path: TypePath,               // diagnostics + nominal fallback; never hashed
    pub uuid: Option<TypeUuid>,       // #[asset(uuid = …)] — stable identity (§7)
    pub attrs: TypeAttrs,             // the attribute channel (§19)
    pub fields: Vec<Field>,
    pub generic_parameters: Vec<String>,
    pub generic_argument_ids: Vec<SchemaTypeId>,
    pub generic_const_arguments: Vec<String>, // native instantiation matching only
    pub has_default: bool,
    pub has_explicit_discriminants: bool, // enum validation, not a hash gate
}
pub struct Field {                    // logical only: today's offset,
    pub id: FieldIdentifier,          // field_size, and FieldShape leave —
    pub type_id: SchemaTypeId,        // layout moves to FieldLayout, shape
    pub attrs: FieldAttrs,            // to identity classification (below)
}
pub struct TypeAttrs  {               // #[asset(rev = N)], 0 absent; type or variant
    pub rev: u32,
    pub build_only: bool,             // #[asset(build_only)] — load-closure
                                      // policy (§4), never hashed: policy
                                      // changes must not mint migrations
}
pub struct FieldAttrs {
    pub rev: u32, pub skip: bool, pub blob: bool,
    pub tag: bool,                    // #[asset(tag)] — indexing only, never
                                      // hashed (§10's tag-annotation epoch
                                      // is its change-tracking); the live
                                      // registry derives the type→tag-field
                                      // map from this bit
}
pub struct SchemaLayouts {            // physical: measured, per identity
    pub identity: LayoutIdentity,
    pub layouts: Vec<TypeLayout>,     // parallel to types, by SchemaTypeId
}
pub struct TypeLayout {
    pub size: Option<u64>, pub align: Option<u64>,
    pub layout_complete: bool,
    pub tag_encoding: Option<TagEncoding>,  // extended per §19; Option<T>'s
                                            // discriminant (FieldShape's
                                            // OptionDiscriminant today) folds in
    /// Positionally 1:1 with the TypeDef's `fields` — index i describes
    /// fields[i], `#[asset(skip)]` fields INCLUDED: the logical projection
    /// is what drops them (they are schema-invisible), but their slots
    /// must be placeable for skip-default writes (§12).
    pub fields: Vec<FieldLayout>,
}
pub struct FieldLayout { pub offset: Option<u64>, pub field_size: Option<u64> }
```

`Schema::merge` dedups **logical** records and attaches layout tables per
`LayoutIdentity`. Distinct compilation identities retain distinct tables;
logical disagreement under one native matching key, or conflicting
measurements under the same layout identity, is an explicit error rather
than a silent winner. Existing layout consumers (the
engine GC, `ngp-reflect`, `migrate.rs`) select the table matching the
running binary and zip by `SchemaTypeId` — no behavior change, since one
host table is all that exists until per-target emission lands (§22).
Container and framework types are classified by **type identity, not by
`FieldShape`** (Serializability, below); the shared classifier replaces
the old shape-driven traversal rather than introducing Distill's own model.

Source-walk normalizes repeated analyzer observations before publication.
Identified records coalesce only when their logical facts and measured
layouts agree, with references remapped on both sides. Native matching
distinguishes `usize`/`isize` from fixed-width integers and includes concrete
const arguments; lifetimes do not distinguish native types. Unresolved
const arguments and opaque observations do not acquire global matching
keys. Corresponding opaque fields of an identified parent may agree by
their modeled facts and same-identity measurements without globally
interning those opaque records. These are schema merge rules, not new
module-acceptance fingerprints or a dependency-closure gate.

**Asset shapes are target-invariant, by rule.** Every layout table is
positionally parallel to the one `types` vector, which is only coherent
if every configured target agrees on the logical projection — a
`#[cfg(windows)]` field or variant inside an asset type would give one
target a logical shape the shared `TypeDef` cannot represent, and the
host pipeline module could not even construct the foreign field it
doesn't have. So per-target layout emission (§22) must re-run extraction
under each configured target's cfg evaluation and verify that every
asset-reachable type produces an identical **registry projection**: the
logical AST node for node (equal DSLH hashes), the stable type UUID,
and the full attribute channel *including the bits the hash deliberately
excludes* (`tag`, `build_only`) — equality over everything the shared
`TypeDef` represents, not merely what migration identity retains, since
a `cfg_attr`-varied UUID or policy bit would poison the pipeline map,
tag indexing, and load-closure validation even under identical shapes.
Any divergence is an **extraction error**
naming the type, the two targets, and the first differing fact —
never a per-target logical schema. Logical identity stays one; only
layout varies by target. cfg-dependent data belongs behind processors
(§9), not inside asset shapes.

An enum's fields reference `EnumVariant` TypeDefs. A variant's
`#[asset(rev = N)]` has exactly one carrier: that variant TypeDef's
`attrs.rev`, not the enclosing enum field's `FieldAttrs.rev`. Both the
source-walk emitter and logical projection use this existing attribute
channel; no separate variant-attribute record is needed. Native matching
metadata and the operational Schema metadata above do not enter DSLH or
DSWL grammar bytes.

From these records, two distinct schemas per type are derived:

### Logical schema — identity

Field names, structural shape, container kinds, enum variant names
and shapes — a projection of the `types` records alone. **No offsets,
sizes, discriminant encodings — and no type paths: renames must not
change identity.** The **logical
hash** over this is the identity used for:

- `schema_hash` in bundle asset entries
- migration matching (automatic and custom)
- artifact headers (compatibility check)

### Layout schema — encoding

Offsets, sizes, alignment, discriminant encodings — the `TypeLayout`
record for the target's compilation identity.
The **layout hash** over this is used only for:

- artifact input hashes (a relayout must re-encode artifacts)
- the loader's zero-copy fast path check

A recompile or compiler upgrade that shuffles `repr(Rust)` layout changes
layout hashes — invalidating cached artifacts — but never touches logical
identity, so no bundle files churn and no migrations are minted.

### Logical hash, precisely

`blake3` over a canonical byte form defined by these rules:

- **Structural, not nominal.** Nested types expand inline; the hash covers
  shape, never type names (field and variant *names* do count — they are
  the JSON encoding). Type renames and moves never change the hash. Type
  identity for matching lives in the stable type UUID, carried beside the
  hash, never inside it.
- **Transparent wrappers.** `Box` and `Arc` hash as their inner type —
  logically they do not exist (JSON cannot see them), so `Vec<Box<T>>` →
  `Vec<T>` is not a migration. (`Rc` is not a serializable kind; `Arc`
  covers shared ownership in asset data.)
- **Order-insensitive where data is.** Struct fields and enum variants hash
  sorted by name: declaration reordering is not a data-format change (layout
  cares; the layout hash covers it).
- **Skip-invisible.** `#[asset(skip)]` fields are absent from the logical
  form. `#[asset(blob)]` is hashed (it changes encoding); `#[asset(tag)]`
  is not (it changes indexing, not interpretation — the tag-annotation
  epoch covers it, §10); `#[asset(build_only)]` is not (it is load-closure
  policy, §4 — toggling it must not mint migrations; the load-policy
  digest covers it, §9, §13); `#[asset(rev = N)]`
  is hashed (it declares an interpretation change with no shape change).
- **Canonical leaves.** Primitives by canonical name; containers as
  (kind, element forms): `vec`, `array(n)`, `option`, `map(key, value)`.
  The `f32`/`f64` distinction also directs canonical JSON emission: an
  `f32` leaf is parsed by IEEE-754 round-to-nearest-ties-even to binary32;
  canonicality is byte equality with re-emission of the shortest decimal
  that reparses, under that same rule, to the same binary32 bits (§6).
  Ordinary values such as `0.1` are therefore canonical.
  `AssetRef<T>` hashes as (`assetref`, T's stable type UUID) and
  `WeakAssetRef<T>` as (`weakref`, T's UUID) — the data is a UUID either
  way, but retargeting a reference changes meaning and must be visible to
  migration.
- **Recursion by back-reference.** When expansion re-enters a type on the
  current expansion path, emit (`backref`, distance) instead of recursing —
  recursive types hash well-defined and position-independent.
- **Generics by monomorphization.** `Foo<u32>` expands as its concrete
  shape; generic parameter names never appear. This rule applies only to a
  fully concrete nested type reachable from a non-generic asset root. A root
  item carrying `#[asset]` with any generic parameters is rejected by both
  macro expansion and extraction (§4), rather than assigning one TypeUuid to
  an unbounded family.
- **Versioned binary encoding.** The hash input is the binary grammar
  below — JSON is never the hash input.

#### Normative hash grammar (v1)

All integers little-endian, fixed width. `str` = `u32` byte length + UTF-8
bytes (NFC-normalized). Every node begins with a `u8` kind tag:

```
0x01 primitive   name:str            canonical names: bool, u8…u128,
                                     i8…i128, f32, f64, char
0x02 struct      rev:u32  field_count:u32
                 fields sorted by name: (name:str, rev:u32, node)
                 tuples/newtypes use decimal index names "0", "1", …
0x03 enum        rev:u32  variant_count:u32
                 variants sorted by name: (name:str, rev:u32, node)
                 payload is a struct node; unit variants are
                 zero-field structs
0x04 vec         node
0x05 array       len:u64  node
0x06 option      node
0x07 map         key:node  value:node
0x08 string
0x09 assetref    type_uuid:[u8;16]
0x0A weakref     type_uuid:[u8;16]
0x0B blob
0x0C backref     distance:u32        counts expanded struct/enum type
                                     frames on the current expansion path,
                                     0 = innermost; wrapper and container
                                     nodes do not count
0x0D unit                            `()`; encodes as JSON null; distinct
                                     from a zero-field struct (`{}`) —
                                     Option of a null-encoding type is
                                     unserializable (Serializability)
0x0E set        node                 HashSet/BTreeSet; JSON: array sorted
                                     by element's encoded bytes (§6);
                                     duplicate elements are a parse
                                     error, never a silent dedup
```

`rev` is the `#[asset(rev = N)]` value, 0 when absent. The logical hash is
`blake3("DSLH" ‖ 0x01 ‖ root node)` — domain-prefixed and versioned. It
is computed by a **hand-written walk over the `types` records directly**
— `ngp_schema::logical_hash(&Schema, SchemaTypeId) -> [u8; 32]` — never
by serializing the model structs: the grammar above is the pinned spec of
the bytes that walk emits, so refactoring `ngp-schema`'s own types can
never move an asset's identity. A
leaf the schema model cannot yet round-trip (`i128` today; `ngp-schema`
has `Unit` but no `I128`) is an **extraction error**, never a silent
lowering — adding the missing leaves is part of the §19 model changes.

The bundle `schemas` section stores this same AST as canonical JSON (one
tree per type, backrefs for recursion); re-deriving a hash re-encodes the
decoded AST to this grammar, so verification needs no other input — and a
snapshot's content is **exactly** what its hash covers, nothing unhashed
rides along (tag markers live in the current registry, never in snapshots,
§10; `EntryLineageV1` is entry metadata *beside* `schema_hash`, §6 —
the manifest arm carries user-lineage direction while the closed bootstrap arm
binds format control facts, neither entering snapshot content). The
snapshot is the *projection*, not the model record: embedding
`TypeDef`s would carry paths, layout tables, and walk-scoped
`SchemaTypeId`s — bytes the hash deliberately excludes, riding along
unverifiable. Hash → content stays unique, which §11's repair guarantee depends
on.

The decoded AST — the snapshot form bundles store, produced from `types`
by a projection function living in `ngp-schema` beside the walk and the
codec (shared code, §19); the daemon consumes it:

```rust
pub struct LogicalSchema { pub root: SchemaNode }

impl SchemaRegistry {                        // the daemon's live registry (§11)
    pub fn current(&self, t: TypeUuid) -> Option<(&LogicalSchema, LogicalHash)>;
    pub fn archived(&self, h: LogicalHash) -> Option<&LogicalSchema>;  // accelerator, rebuildable
}

pub enum SchemaNode {                        // mirrors the byte grammar 1:1
    Primitive(PrimitiveKind),                // 0x01 — bool, u8…u128, i8…i128, f32, f64, char
    Struct { rev: u32, fields: Vec<(String, u32, SchemaNode)> },    // 0x02, name-sorted
    Enum   { rev: u32, variants: Vec<(String, u32, SchemaNode)> },  // 0x03, name-sorted
    Vec(Box<SchemaNode>),                    // 0x04
    Array { len: u64, elem: Box<SchemaNode> },                      // 0x05
    Option(Box<SchemaNode>),                 // 0x06
    Map { key: Box<SchemaNode>, value: Box<SchemaNode> },           // 0x07
    String,                                  // 0x08
    AssetRef(TypeUuid),                      // 0x09
    WeakRef(TypeUuid),                       // 0x0A
    Blob,                                    // 0x0B
    BackRef(u32),                            // 0x0C
    Unit,                                    // 0x0D
    Set(Box<SchemaNode>),                    // 0x0E
}
```

### Serializability

Classification is by **type identity, never by `FieldShape`**: a closed
well-known table matches std containers by path, aliases included (the
`known_container_layout` precedent) — `Vec`, arrays, `Box`, `Arc`,
`String`, `HashMap`/`BTreeMap` (one map node — identity is structural,
the concrete container is the binary's own business), `HashSet`/`BTreeSet`
(one set node, likewise), `Option` — and distill's framework types match
by their attributed type UUIDs: `AssetRef<T>`/`WeakAssetRef<T>` are
ordinary support-crate structs whose generic argument names `T` (its
type UUID fills the grammar leaf), and blob slots are `#[asset(blob)]`
field attributes. **Nothing asset-specific enters `ngp-schema`.** A field
is serializable unless marked `#[asset(skip)]` or its type is outside the
classifier — any unrecognized smart container, `GpuRef`, `FnPtr` — which
is an extraction error, never a guess. Serializable
hash-based maps and sets require a
**deterministic `BuildHasher`** — the asset-types support crate's
fixed-seed state; a std `RandomState` map is unserializable (extraction
error). Decoding constructs through the map's real hasher (§12), so a
random seed would make two decodes of one verified artifact iterate
differently — framework-injected entropy no input hash covers (§9): a
processor innocently iterating a map input into a `Vec` would emit
different artifacts under one input hash. **Blobs are barred beneath map
keys and set elements**: canonical ordering for sets and non-string-key
maps is by *encoded* element/key bytes (§6, §12), while a blob's encoding
holds an offset or index assigned from that same final order — computing
the order would need the bytes that need the order. The circularity is
broken by construction: extraction rejects a `#[asset(blob)]`-bearing
type anywhere in the subtree of a map **key** or set **element** with a
typed extraction error naming the path; map **values** may contain blobs
freely (values never participate in key ordering). Directly nested `Option<Option<T>>`
is unserializable: the JSON encoding is `None` = `null`, `Some(x)` = `x`'s
encoding, which cannot distinguish the inner levels — wrap the inner
option in a struct. The same rule generalizes: `Option<T>` is
unserializable whenever `T`'s normalized encoding can itself be `null` —
after erasing transparent wrappers, that is unit and options
(`Option<()>`, `Option<Box<()>>`) — since `None` and `Some` would alias.

## 6. Bundle File Format

A bundle is one self-contained file holding one or more assets. There is no
sidecar file and no machine-owned `_distill` section; committed content is
authored data plus deterministic snapshots (UUIDs, schemas) written by tools.

### Two physical encodings, one extension

The parser dispatches on the first bytes of the file:

- **Plain JSON** — used whenever the bundle has no blobs. Text: diffable,
  hand-editable, merge-friendly. This is the common case for hand-authored
  assets.
- **Container** — used whenever blobs are present:

```
magic     [u8; 8]  = 89 44 53 42 0D 0A 1A 0A   ("\x89DSB\r\n\x1a\n")
version   u32 LE
json_len  u64 LE
blob_len  u64 LE
json_crc  u32 LE
blob_crc  u32 LE
json      [u8; json_len]      UTF-8, identical structure to plain JSON;
                              blob fields become { "offset": n, "len": n }
pad       zeros to 16-byte alignment
blob      [u8; blob_len]      individual blobs 16-byte aligned
```

The PNG-style magic (`\x89` high-bit byte, `\r\n`, `\x1a`, `\n`) is invalid as
UTF-8/JSON at byte zero and detects line-ending mangling loudly at open time.
Projects should still set `*.<ext> -text` in `.gitattributes` and may install a
`textconv` driver that prints the JSON chunk.

Integrity and parsing rules, pinned:

- CRCs are CRC-32C (Castagnoli). File size must equal header + `json_len` +
  pad + `blob_len` exactly; all offset/length arithmetic is checked, and
  `json_len`/`blob_len` are validated against the file size before any
  allocation.
- Blob offsets are relative to the start of the blob chunk. Ranges must be
  in-bounds, non-overlapping, 16-byte aligned, and sorted ascending in
  canonical structural-path order (below); zero-length blobs are legal. All padding bytes must
  be zero and are verified.
- Writers use temp-file + fsync + atomic rename + directory fsync. Header
  `version` covers the container encoding; the JSON `format_version` covers
  envelope semantics; a parser accepts a file only when it supports both.

**Writer determinism is part of the spec**: canonical JSON serialization, zero
padding, blobs laid out in sorted structural-path order. Input hashes hash whole-file
bytes, so canonical bytes require a canonical writer — every byte choice
is normative, none left to a library. Canonical JSON is fully pinned,
with **RFC 8785 (JCS) as the reference serialization** and these named
deviations and completions controlling where they differ: keys sort by
**UTF-8 byte order** (never 8785's UTF-16 code-unit order); duplicate
keys are parse errors; escaping is minimal (only the mandatory `\"`,
`\\`, and control-character escapes, in 8785's short forms; no `\uXXXX`
for printable characters); **no insignificant whitespace anywhere** —
bare `,` and `:` separators, no indentation — and the document ends with
**exactly one trailing `\n`** (the one whole-file byte 8785 leaves
unstated); integer leaves (`Int`/`UInt`) print as minimal decimal — no
leading zeros, no `+`, never a `.` or exponent; float leaves print by
8785's shortest-round-trip rules (ECMAScript Number-to-string, exponent
form included), NaN/Inf rejected in
authored data, `-0.0` normalized — and float canonicalization is
**schema-directed**: an `f32` leaf's value is rounded to its nearest
binary32 value by IEEE-754 round-to-nearest-ties-even *before*
shortest-round-trip decimal emission (the
schema, not the in-memory `f64`, decides the width — §5's float leaf
kinds), so a writer that parsed into a double and one that quantized
first cannot emit different canonical bytes for one value. Adoption
and validation enforce canonicality by re-emitting the shortest decimal
that reparses, under the same ties-even rule, to the same binary32 bits
and requiring byte equality with the authored token; `0.1` is canonical.
Maps with non-string keys
encode as an array of `[key, value]` pairs sorted by the key's encoded
bytes; sets as an array sorted by the element's encoded bytes, duplicate
elements a parse error (§5). Key and set-element encodings are blob-free
by §5's serializability rule, so both orderings are well-defined before
any blob is placed. Ordering is pinned at every level: assets by `local_id`, fields by
name, blobs by (local_id, **canonical structural path**). The structural
path is the injective blob key: the component sequence from the entry
root to the blob — `field(name)`, `variant(name)`, `index(u64)`,
`mapkey(canonical encoded key bytes)` — encoded per component as a
component-kind tag byte plus a length-framed payload, blobs ordered by
the path's encoded bytes. A leaf-name key (`(local_id, field name)`)
would alias two blobs under one name in different nested structs,
variants, array elements, or map values — `AuthoredValue` permits all
of those shapes — letting two writers produce different bytes (and
ContentHashes) for one value; the full path names each blob uniquely,
so ordering is total by construction. The same key orders the artifact
blob table (§12). The magic `DSB` is distinct from the
artifact format's `DSTL` (§12).

### Envelope

| Field | Type | Description |
|-------|------|-------------|
| `format_version` | number | Bundle file format version |
| `uuid` | string | The bundle's own stable UUID |
| `primary` | string? | Local ID resolved by plain-path references. Optional; inference is over runtime content entries only — every `authoring_only` entry is ineligible, including daemon-owned `$` entries (§8): defaults to the sole eligible entry; multiple eligible entries with no declaration and no prior primary are an error (§8's fold rule). |
| `schemas` | object | `logical_hash → LogicalSchema` for every logical hash the bundle references — entry `schema_hash`es, migration endpoints (§11). Bundles are **schema-closed**: any hash mentioned in the file resolves within the file. |
| `assets` | object | `local_id → asset entry` |

### Asset entry

| Field | Type | Description |
|-------|------|-------------|
| `uuid` | string | This asset's stable UUID, explicit in the file |
| `type_uuid` | string | Asset type UUID |
| `schema_hash` | string | Logical hash the entry was last written with |
| `lineage` | object | The closed `EntryLineageV1` sum below. Non-bootstrap entries carry a verifiable manifest stamp; the exact five format-bootstrap control types carry the sealed format-version arm. |
| `authoring_only` | bool | Per-entry role. `true` entries are visible only to an explicit authoring-tool query, never eligible as `primary`, runtime query results, processor inputs, references, pack roots, or shipped pack members. |
| `data` | object | Field values (JSON mapping as in v1: primitives, arrays, objects, enum `{ "Variant": {…} }`, refs per §4) |
| `blobs` | — | `#[asset(blob)]` fields; only in container encoding |

Parsed in memory:

```rust
pub struct Bundle {
    pub format_version: u32,
    pub uuid: BundleUuid,
    pub primary: Option<String>,
    pub schemas: BTreeMap<LogicalHash, LogicalSchema>,
    pub assets: BTreeMap<String, AssetEntry>,          // local_id → entry
}

pub struct AssetEntry {
    pub uuid: AssetUuid,
    pub type_uuid: TypeUuid,
    pub schema_hash: LogicalHash,
    pub lineage: EntryLineageV1,
    pub authoring_only: bool,
    pub data: AuthoredValue,
}

/// Closed entry-lineage carrier. Its canonical record encoding is
/// `version:u8=1 || tag:u8 || payload`, with `Manifest=1` followed by the
/// canonical LineageStamp record and `Bootstrap=2` followed by
/// `bundle_format_version:u32-LE`; unknown versions/tags and trailing bytes
/// reject. Canonical JSON is exactly the one-member object
/// `{ "manifest": <LineageStamp> }` or
/// `{ "bootstrap": { "bundle_format_version": 1 } }`; those lowercase keys,
/// the nested key, and the numeric value are literal, and aliases, additional
/// members, or the wrong arm reject.
#[repr(u8)]
pub enum EntryLineageV1 {
    Manifest(LineageStamp) = 1,
    Bootstrap { bundle_format_version: u32 } = 2,
}

`EntryLineageV1::Bootstrap` is legal and mandatory iff `type_uuid` is one of
the five closed `BootstrapControlSpecV1` TypeUuids: `SchemaLineageManifest`,
`Migration`, `ImportRecord`, `DirectoryImportRules`, or `PackDefinition`.
It requires both the enclosing bundle and arm to say format version 1,
`schema_hash` to equal that type's sealed table-row DSLH, and the bundle's
`schemas[schema_hash]` canonical logical graph to equal the DSB-sealed logical
graph, not merely hash to caller-supplied bytes. It also enforces that type's
closed control role and namespace rules (including `authoring_only = true`,
`$record` only for `ImportRecord`, and every type-specific control-value
invariant). Bootstrap entries never consult `SchemaLineageManifest`, never
become its type rows, and never enter user lineage comparison or migration.

`EntryLineageV1::Manifest` is legal and mandatory for every other TypeUuid.
Its stamp must validate as the exact accepted manifest prefix, parent graph,
cursor, schema equality, and DSSL commitment described below. Entry decode,
bundle schema-closure validation, scanner publication, adoption, importer and
editor/RPC writes, disk migration, repair validation, and every `ControlRead`
all call this same arm/type validator; none may accept a bootstrap type with a
manifest stamp or a non-bootstrap type with the bootstrap arm.

/// A verifiable snapshot beside every `schema_hash` (§11). `epochs` is an
/// exact prefix of the durable manifest's append-only epoch vector, including
/// each epoch's explicit forward parent. `cursor` selects the writing epoch;
/// it need not be the final vector element after an accepted rollback.
/// `schema_hash == epochs[cursor].digest` is mandatory. `chain` is
/// `blake3("DSSL" ‖ version:u8 ‖ type_uuid ‖ count:u32 ‖
/// each(digest ‖ parent:Option<u32>) ‖ cursor:u32)`, recomputed on read.
/// A stamp whose prefix, parents, cursor, or commitment disagrees with the
/// source-controlled manifest is malformed. A valid stamp proves only what
/// the manifest already accepted; it can never replace a missing manifest or
/// authorize a new epoch after daemon-state loss.
pub struct LineageStamp {
    pub epochs: Vec<AcceptedSchemaEpoch>,
    pub cursor: u32,
    pub chain: [u8; 32],
}

pub struct AcceptedSchemaEpoch {
    pub digest: LogicalHash,
    /// `None` only for the first accepted epoch of a type. Otherwise points
    /// to the epoch that this schema was accepted as a forward successor of.
    pub forward_parent: Option<u32>,
}

pub struct AcceptedTypeLineage {
    pub epochs: Vec<AcceptedSchemaEpoch>, // append-only accepted history
    pub current: u32,                     // independently movable cursor
    pub authority: TypeAuthorityState,    // explicit; never inferred from
                                          // candidate presence/absence
}

pub enum TypeAuthorityState {
    Active,
    /// History remains authoritative and verifiable, but this TypeUuid is
    /// excluded from the exact active-registry equality gate.
    Retired { retired_from: u32 },
}

/// A unique, source-controlled, built-in authoring asset. It is parsed by
/// bundle `format_version` bootstrap code and is itself never migrated through
/// the user migration graph. Only the explicit schema-acceptance command may
/// rewrite the unique valid manifest's authority contents. The narrow local
/// repair command below may create an absent manifest or remove duplicate
/// claimants, but cannot edit a unique valid manifest; source-walk, module
/// reload, import, migration, and ordinary bundle writes are readers. Every
/// accepted epoch is recorded even when no data bundle happens to be written
/// in that epoch.
pub struct SchemaLineageManifest {
    pub types: BTreeMap<TypeUuid, AcceptedTypeLineage>,
}

Exactly one `SchemaLineageManifest` entry MUST exist in the configured asset
tree, with `authoring_only = true`; missing or duplicate manifests publish a
typed configuration poison and no schema-dependent service starts. The
explicit acceptance/rollback command for an existing unique manifest rewrites
it through §14's journaled
swap protocol. Its request carries the base manifest `BundleFileHash`, every
affected type's base cursor, and the staged candidate epoch identity. The
base hash is only the exact observed-byte CAS precondition; manifest decode and
canonical validation are separate and mandatory before mutation. The
decoder and coordinator compare that identity's exact canonical target rows
(§5); caller-supplied ordering or duplicate names are never trusted. The
transaction refuses unless that base still matches **and** each digest it
would select equals the corresponding candidate schema row's `logical_hash`;
rollback additionally completes all reverse-edge coverage validation before
the cursor transaction begins. Commit rewrites the manifest and promotes that
exact retained `StagedPipelineEpoch` to `Ready` as one coordinator operation;
an identity mismatch, a stale base/cursor, or a different staged candidate
leaves the manifest untouched. Thus two acceptors cannot lose an epoch or
cursor move, and an acceptor cannot select a schema different from the code it
validated. Bootstrap is non-circular: the closed five-row
`BootstrapControlSpecV1` (§3) is authority of bundle `format_version` and is
validated independently before the manifest is decoded. The manifest's
`types` map MUST NOT contain any bootstrap-control TypeUuid and is never
consulted to parse or validate those entries. All other compiled types MUST
match its Active rows exactly; there is no role-, plugin-, or name-based
exception.

Missing/duplicate recovery is a separate, deliberately tiny control plane:

```rust
pub struct LineageManifestClaimant {
    pub asset: AssetUuid,
    pub bundle: BundleUuid,
    pub local_id: String,
    pub root_name: RootName,
    pub normalized_path: String,
    /// Raw blake3 of the exact physical preimage bytes (§5), not a canonicality
    /// assertion.
    pub file_hash: BundleFileHash,
}
#[repr(u8)]
pub enum OccupiedLineageDestinationKind {
    CanonicalBundle = 1,
    Opaque = 2,
}
pub enum LineageRepairDestination {
    Absent,
    Occupied {
        file_hash: BundleFileHash,
        kind: OccupiedLineageDestinationKind,
    },
}
pub enum LineageRepairState {
    Missing {
        configured_path: RootedPath,
        destination: LineageRepairDestination,
    },
    Duplicate { claimants: Vec<LineageManifestClaimant> },
}
pub struct LineageRepairInspection {
    pub instance: StoreInstanceId,
    pub stamp: SnapshotStamp,
    pub state: LineageRepairState,
}
```

`Root.lineageRepair` (§17) is unbound and is usable only by the explicit local
repair command while the **current** DSCP detail is exactly
`MissingLineageManifest` or `DuplicateLineageManifest`. It is not
`MetadataHub`, never consults `last_good`, and exposes no query, inspection of
ordinary authored values, build, resolve, import, general write, acceptance,
rollback, retirement, or reactivation capability. `inspect` returns the
current StoreInstanceId and exact SnapshotStamp plus either `Missing` at the
normalized `assets.lineage_manifest` rooted destination (§18), including the
destination's exact `Absent` or occupied `{ file_hash,
CanonicalBundle|Opaque }` repair basis, or every
duplicate physical claimant strictly sorted and deduplicated by
`(root_name, normalized_path, raw BundleUuid bytes, normalized local_id, raw
AssetUuid bytes, file_hash)` canonical bytes. That tuple is injective even when
one physical bundle contains multiple manifest entries; DSCP detail, repair
inspection, stale comparison, and survivor selection all carry these exact
rows.

`createMissing` accepts that complete `Missing` inspection as its exact
destination-aware basis plus proposed canonical bundle bytes. Inside one
coordinator action
it requires StoreInstanceId/stamp/configured path and missing DSCP to remain
exact, revalidates and reopens the destination under its configured root, requires its
`Absent|Occupied { file_hash, kind }` state to match byte-for-byte, and checks
that no manifest claimant has appeared. It validates the proposed bytes as one
canonical bundle containing a built-in `SchemaLineageManifest` entry marked
`authoring_only` whose lineage is the required
`EntryLineageV1::Bootstrap { bundle_format_version: 1 }`; the entry's sealed
schema facts/control role and the manifest's canonical non-bootstrap type map
and lineage records must validate, and bootstrap-control TypeUuids remain
forbidden from that `types` map. Thus the first manifest is installable while
authority is absent without inventing a self-referential user-lineage stamp.

For an `Absent` basis it journals an atomic **no-replace** creation. For an
occupied canonical bundle it requires the replacement to preserve every
non-manifest entry byte-for-byte and to differ only by adding exactly one
canonical manifest entry; its exact preimage is retained in quarantine and the
replacement is installed with the same rename-aside/no-clobber protocol as §14.
For an occupied opaque file it first renames the exact hashed raw preimage
no-replace into per-filesystem quarantine, preserving those bytes under a
recoverable journal name, and only then installs the proposed canonical bundle
no-replace at the configured destination. In all three cases the destination
basis CAS repeats immediately before mutation; the durable parent intent,
file/quarantine/directory fsyncs, restart roll-forward-or-back, and final rescan
are mandatory. The input version advances only after the rescan proves one
unique valid manifest. A destination appearance, byte/kind/state drift, or
basis drift is typed stale/conflict; no occupied preimage is overwritten or
made unrecoverable.

`resolveDuplicate` likewise accepts the complete `Duplicate` inspection and
one explicit user-selected survivor whose entire claimant tuple must be an
exact member. The coordinator reopens every named physical file, validates
all exact `BundleFileHash` preimages and the survivor's canonical built-in,
authoring-only manifest with required format-v1 bootstrap lineage and a
bootstrap-free `types` map, and stales without mutation on any
instance/stamp/state/claimant/byte drift. It then journals removal of every
non-surviving manifest claimant: a file containing no retained entries is
renamed no-replace into its per-filesystem quarantine, while a file sharing
unrelated or surviving entries is atomically replaced by its canonical form
with only non-surviving manifest entries removed and its exact preimage retained
in quarantine. The multi-path journal and file/quarantine/directory fsyncs make
restart recovery resume or roll back before publication; no scan order chooses
a winner. Only after a rescan proves the selected AssetUuid at its selected
root/path—under either its unchanged preimage hash or the journal-derived
canonical replacement hash when co-located entries were removed—and no other
manifest claimant does the coordinator publish the healing input version.
Until either operation
finishes durably, normal target `Root.connect` remains
`ConfigurationPoisoned`; the repair surface never serves prior authority.

Type removal and return are explicit authority transitions, never side effects
of a candidate's set difference. Retirement requires the same stale manifest
hash/cursor and exact candidate-identity checks as acceptance, requires the
candidate to omit the TypeUuid, and proves before the transaction that no live
authored entry has that type and no live migration endpoint requires it. The
request carries the exact metadata/`ControlSnapshot` `SnapshotStamp` that
produced this negative proof; the coordinator rechecks that stamp and every
role-inclusive entry/endpoint index inside the manifest transaction, so a
concurrent scan or authoring write cannot land between proof and retirement.
It then preserves `epochs`/`current` and changes only `authority` to `Retired`.
Reactivation requires candidate inclusion and explicitly selects that
candidate row's exact logical digest: an already accepted digest selects its
existing epoch under the rollback/forward rules, while a genuinely new digest
appends under the ordinary acceptance rule. No load, scan, module staging, or
candidate replacement can retire or reactivate a row implicitly.

Retirement's negative condition remains an invariant after commit:
`Retired(type) => no live entry of type and no live Migration endpoint for
type`. Bundle scanning, import/adoption folds, editor/RPC CRUD, disk migration,
and manual-filesystem reconciliation all check it before their input version
publishes. Current bytes that violate it are not omitted or accepted under a
nominally Ready registry; the version publishes typed
`RetiredTypeReferenced`/reactivation-required authority poison naming the type
and entry/endpoint identities. Explicit reactivation alone may admit such
bytes, atomically restoring `Active`, selecting the candidate's exact digest,
and publishing their metadata in the same stale-base checked coordinator
transaction.

/// The bundle data model: canonical JSON plus blob bytes. This is the value
/// type load_current yields (§11), migration functions transform (§11), and
/// importers emit (§8).
pub enum AuthoredValue {
    Null,                                     // Option::None; Some(x) encodes as x (§5)
    Bool(bool),
    Int(i128), UInt(u128), Float(f64),        // NaN/Inf rejected, -0.0 normalized
    Str(String),
    Array(Vec<AuthoredValue>),                // Vec/arrays/sets; non-string-key maps as [k, v] pairs
    Object(BTreeMap<String, AuthoredValue>),  // structs; enums as { "Variant": … }
    Blob(Vec<u8>),                            // #[asset(blob)] — container chunk bytes
}
```

### Import settings are assets

Importer options are human decisions, so they are ordinary asset entries — a
schema-described struct (e.g. `TextureImportSettings { srgb,
generate_mips, … }`) living in the same bundle as the imported content. They
get editor UI, validation, and migration like any other asset. Beside the
settings entry the daemon writes a built-in **`ImportRecord`** entry
(declared in §8): the importer id, the **weak** source paths — provenance
plus the target of explicit re-import, never a build dependency — the
`watch` flag, the import's recorded **read-set** (§8), and, for
directory-generated bundles, the **`DirectoryOrigin`** record (§8)
naming the owning rules bundle, rule, and group. Daemon-owned
entries live under **reserved local_ids** — `$settings`, `$record`; the
`$` prefix is invalid in importer output (§8) — so role is carried by
both the namespace and the mandatory per-entry `authoring_only` bit, and a
fold can never confuse importer content with
the entries it must preserve. The reservation is enforced at **every
boundary that can put an entry in a bundle**, not just importer output:
the bundle parser, adoption, editor CRUD, and the RPC write API all
reject a `$`-prefixed `local_id` other than the two recognized ids as
an integrity error, and a hand-authored `$settings` or `$record` is
accepted only carrying exactly its built-in role and type — a spoofed
`$record` can otherwise impersonate daemon-owned reimport state. With
`watch = true` the read-set is live — invalidation re-runs the import —
and with the default `watch = false` it is provenance only. Everything
re-import needs is in the committed file, so it survives daemon-state
loss with no importer cooperation.

All reserved metadata/control entries are `authoring_only = true`: `$settings`,
`$record`, `Migration`, `DirectoryImportRules`, `PackDefinition`, and the
`SchemaLineageManifest`. Parser, adoption, importer fold, editor CRUD, and RPC
write validation reject a recognized control type with `authoring_only = false`
or any attempt to select an authoring-only entry as `primary`. Ordinary query
surfaces exclude these rows by default; the explicit tooling-only query
mode in §10 may select them for human inspection. That mode is rejected from processor
`ProcessContext`, authored references, runtime snapshots, and pack roots, and
authoring-only entries are never shippable even if reachable through a bad
configuration. UUID is not a role bypass: runtime `Snapshot.resolve(uuid)`,
loader resolve, and every dependency/closure expansion reject such an entry
with typed `RoleIneligible` before build import or processor lookup. Tooling
value inspection uses the pinned authoring-inspection capability (§17), whose
result cannot be fed to resolve, processing, dependency tracing, or packing.

The daemon's own control plane is separate from that tooling mode. A private,
capability-typed `ControlSnapshot`/`ControlQuery` surface may select
authoring-only `Migration` entries solely for migration planning and rollback
validation, and has distinct non-artifact readers for `PackDefinition`,
directory-import rules/settings, and the lineage manifest. Autonomous control
classes use private role-inclusive enumeration (directory rules are the first),
so discovery never falls back to ordinary role-filtered `AssetQuery`. Reads
return only the closed branded `ControlValue` sum — including a fully decoded
Migration endpoint/kind record — never raw `AuthoredValue`. Its authority
tokens and query types are coordinator-private and cannot be constructed by
`ProcessContext`, authored references, runtime RPC, pack roots, or ordinary
`AssetQuery`; every control read is stamped to the underlying metadata basis,
creates its `ControlRead` observation before decode/validation, records and
revalidates success or stable failure, and parses the built-in control value
directly without building, processing, loading, or shipping the control asset
itself.

### Adoption

Humans may hand-write bundles without UUIDs or schema snapshots. On first
sight, the daemon performs an authoring-time normalization: mint missing UUIDs,
write the schema snapshot, canonicalize formatting, and rewrite the file. Like
any authoring action, this edits a committed file; the build never does this.
Authors may also write **sparse**: omitted fields are materialized as
explicit values during adoption (defaults via the pipeline module's default
materializer), so the canonical on-disk form is always total and the build
never fills in a default. A later change to a type's `Default` therefore
affects only newly adopted assets, never silently reinterpreting old ones.

## 7. Identity

- Bundle and asset UUIDs are minted at authoring time (import, editor create,
  or adoption) and live in the file as content. Identity survives file renames
  and moves trivially because it travels inside the file — and survives daemon
  downtime, git operations, and daemon-state loss for the same reason.
- Processing preserves identity: the primary output carries the parent
  asset's UUID. Extra derived outputs (§9) get `UUIDv5(parent_uuid,
  output_key)` — keys are statically declared and unique across the type's
  whole chain (§9), so the mapping is collision-free and deterministic, no
  authoring state involved — and are
  reachable only through built artifacts, never from authored data.
  The UUIDv5 name bytes are pinned: the output key's **NFC-normalized**
  UTF-8 (§10's identifier rule) — key uniqueness is checked over the same
  normalized bytes the hash consumes, so two spellings of one key can
  neither pass uniqueness nor mint different children.
- Codegen identity is global even though `local_id` is bundle-local:
  generated Rust filenames and module identifiers use only the asset's fixed
  full lowercase `AssetUuid` hex (`sp_<32hex>.rs` / `sp_<32hex>`, §20).
  A bounded human slug may appear only as non-authoritative display metadata.
- Rename detection (§14) matches deleted+created paths by bundle UUID during
  scan reconciliation, which is robust where v1's mtime/length heuristics were
  not (e.g. `git checkout` while the daemon runs or while it is down).
- Uniqueness is checked per identity domain, never assumed. Two files
  claiming one bundle UUID, or two entries anywhere in the tree claiming
  one asset UUID (file copies either way), are integrity errors surfaced
  with every authored claimant; derived collisions additionally name each
  `(parent, output_key)` claimant. Nothing resolves a collision for the
  user: `doctor` reports it, and re-minting identity in an authored copy
  is an ordinary authoring write (`Hub.write`) to the copy the user
  designates — never a silent resolution of a derived claimant. Two
  registered types claiming one
  type UUID is a schema-load error. Derived identities are validated at
  **publication**, not first use: every input-version publication checks
  the union of authored UUIDs and the version's precomputed derived UUIDs
  (§9) — child vs. authored and child vs. child — atomically before the
  version becomes queryable, so an authored file claiming a remembered
  derived child's UUID is rejected loudly at scan; a derived-output
  commit whose row disagrees with the precomputed index fails the
  commit. UUIDv5 collisions are checked, never trusted to probability.
  **Rejection still publishes.** A filesystem state that fails identity
  validation cannot simply be dropped: the daemon would keep serving the
  prior version while `current` never advances, and a snapshot-pinned,
  retry-refreshed client (§15) whose resolve observes drifted bytes would
  refresh into the same version and spin forever at quiescence. Instead
  the daemon publishes a **poisoned version**: it advances `current`,
  carries the validation error, and resolves against it fail
  deterministically naming the collision (a stable `Failed`, not
  `Drifted`); subscribers receive its invalidation events like any
  version's; last-good artifacts stay fetchable by ContentHash, and the
  next consistent filesystem state publishes normally over it.
  Identity collision is not the special case: **every per-file indexing
  failure publishes** — malformed JSON or container bytes, a
  schema-closure failure (§6), an unsupported format version. Scope
  follows what the **current bytes prove**, never what prior bytes
  claimed. Bundle-scoped poison requires the malformed file itself to
  yield a fully validated, **complete namespace skeleton**: the outer
  envelope parses, and every asset UUID, `local_id`, type UUID, and tag
  value is extracted and validated even though the file as a whole
  fails — only then can a poison row bound exactly the queries the file
  could match. When it does, the failure publishes as a
  **bundle-scoped poison row** (§13)
  in the new version: the file's prior rows are replaced by the poison
  row — never silently retained (a quiescent snapshot would spin on
  `Drifted` against the changed bytes) and never silently dropped
  (query semantics would change invisibly) — resolves against the
  file's UUIDs return a stable `Failed` naming the parse or index
  error, and queries whose selectors could match its entries fail
  naming the poisoned bundle (the same shape as §10's tag poisoning).
  Anything less than the complete skeleton poisons conservatively
  **version-global**, exactly as for identity collisions, naming the
  unreadable path — *regardless of what prior metadata exists*: a prior
  indexed row identifies what the *old* bytes claimed, not what the
  malformed bytes might claim — a malformed edit can introduce a new
  UUID, type, `local_id`, or tag before the syntax error, so scoping by
  the old row would let queries matching the new facts return `Missing`
  or shrunken results instead of failing. The conservative principle,
  stated: a poison is scoped only by facts validated from the bytes
  being poisoned; everything else indicts the version.
  Fixing the file heals on the next version. The cross-file identity
  collision keeps the version-global poison above — a UUID claimed
  twice indicts the namespace, not one file — but the publication rule
  is uniform: rejection, at any scope, still publishes.

Version-global poison has one closed, stable identity; it is never a prose
string and never impersonates a bundle-scoped failure sentinel:

```rust
#[repr(u16)]
pub enum VersionPoisonCode {
    DuplicateAssetUuid = 1,
    DuplicateBundleUuid = 2,
    SameRootNormalizedPathCollision = 3,
    IncompleteSkeleton = 4,
    UnreadableGlobalBundlePath = 5,
    InvalidPhysicalPath = 6,
    UnreadableScanSubtree = 7,
}
pub struct ReadableBundleSource {
    pub root_name: RootName,
    pub normalized_path: String,
    /// Exact observed file bytes; readability/canonicality are separate facts.
    pub file_hash: BundleFileHash,
}
/// Exact claimant identity for an AssetUuid collision. Variant tags are the
/// fixed, nonzero u8 values below; local_id/output_key are NFC-normalized by
/// the §10 identifier grammar before comparison or encoding.
#[repr(u8)]
pub enum AssetClaimant {
    Authored {
        source: ReadableBundleSource,
        bundle: BundleUuid,
        local_id: String,
    } = 1,
    Derived {
        parent: AssetUuid,
        output_key: String,
    } = 2,
}
/// The scanner's lossless platform/path byte grammar. Unix carries the exact
/// root-relative pathname bytes as tag 1 || len:u32-LE || bytes. Windows
/// carries the exact root-relative UTF-16 code-unit sequence as tag 2 ||
/// len:u32-LE || each unit:u16-LE. Neither arm normalizes, case-folds, repairs,
/// or converts through Unicode, so NFC and case aliases remain distinct.
#[repr(u8)]
pub enum PlatformPathBytes {
    Unix(Vec<u8>) = 1,
    Windows(Vec<u16>) = 2,
}
pub struct PhysicalPathClaim {
    pub raw_relative_path: PlatformPathBytes,
    /// Exact observed file bytes, not proof that the bundle is canonical.
    pub file_hash: BundleFileHash,
}
#[repr(u16)]
pub enum SkeletonFailureCode {
    EnvelopeMalformed = 1,
    MissingFormatVersion = 2,
    InvalidBundleUuid = 3,
    IncompleteAssetIdentity = 4,
    IncompleteTypeIdentity = 5,
    IncompleteTagIdentity = 6,
}
#[repr(u16)]
pub enum GlobalBundleReadFailureCode {
    PermissionDenied = 1,
    InvalidFileType = 2,
    SymlinkIdentityChanged = 3,
    IoDataLoss = 4,
}
#[repr(u16)]
pub enum PhysicalPathFailureCode {
    InvalidUnixUtf8 = 1,
    UnpairedWindowsUtf16 = 2,
    Absolute = 3,
    EmptyComponent = 4,
    DotComponent = 5,
    ParentComponent = 6,
    ForbiddenCharacter = 7, // slash/backslash/NUL as applicable
}
#[repr(u8)]
pub enum ScanSubject {
    Root { root_name: RootName } = 1,
    Subtree {
        root_name: RootName,
        raw_relative_path: PlatformPathBytes,
    } = 2,
}
#[repr(u16)]
pub enum ScanFailureCode {
    PermissionDenied = 1,
    NotFound = 2,
    InvalidFileType = 3,
    SymlinkIdentityChanged = 4,
    IoDataLoss = 5,
}
pub enum VersionPoisonV1 {
    DuplicateAssetUuid {
        asset: AssetUuid,
        claimants: Vec<AssetClaimant>,
    },
    DuplicateBundleUuid {
        bundle: BundleUuid,
        sources: Vec<ReadableBundleSource>,
    },
    SameRootNormalizedPathCollision {
        root_name: RootName,
        normalized_path: String,
        claims: Vec<PhysicalPathClaim>,
    },
    IncompleteSkeleton {
        source: ReadableBundleSource,
        failure: SkeletonFailureCode,
    },
    UnreadableGlobalBundlePath {
        root_name: RootName,
        normalized_path: String,
        failure: GlobalBundleReadFailureCode,
    },
    /// A physical pathname rejected before a normalized logical-path String
    /// exists. The raw platform arm is complete and lossless.
    InvalidPhysicalPath {
        root_name: RootName,
        raw_relative_path: PlatformPathBytes,
        failure: PhysicalPathFailureCode,
    },
    UnreadableScanSubtree {
        subject: ScanSubject,
        failure: ScanFailureCode,
    },
}
pub struct VersionPoison {
    pub code: VersionPoisonCode,
    pub identity: [u8; 32],
    pub detail: VersionPoisonV1,
    pub message: String, // presentation only
}
```

`VersionPoison.identity = blake3("DSVP" || 0x01 || code:u16-LE || exact
variant fields)`. A code admits only its same-named payload variant. A
`DuplicateAssetUuid` has at least two strictly sorted, deduplicated tagged
`AssetClaimant` rows ordered by their complete canonical bytes; this represents
authored/authored, authored/derived, and derived/derived collisions even when
multiple claimants share one bundle or source file. `DuplicateBundleUuid`
source rows sort by `(root_name, normalized_path, file_hash)` canonical bytes,
reject duplicates, and require at least two rows. A
`SameRootNormalizedPathCollision` is exactly one root and normalized logical
path with at least two strictly sorted/deduplicated `PhysicalPathClaim` rows,
ordered by `(encoded raw_relative_path, file_hash)`; the raw paths must be
distinct. It is the scan-time integrity error for two physical names within
one root that collapse under §10 NFC normalization. The lossless claim keeps
the original case bytes/code units as well as distinguishing NFC aliases; no
case fold is introduced into §10's case-sensitive logical path key. The same normalized
logical path in two different roots is never DSVP: it remains the ordinary
`Ambiguous(roots)` multimap state and only path-dependent operations fail.
Root names, normalized paths, local ids, and output keys use §10 normalization;
raw physical names use the lossless platform/path grammar above. An
`InvalidPhysicalPath` uses no normalized path: it hashes the exact raw platform
arm followed by its fixed `PhysicalPathFailureCode`. The scanner publishes it
for intake-invalid physical names, including invalid Unix UTF-8, unpaired
Windows UTF-16, absolute names, empty/`.`/`..` components, and the applicable
forbidden character; healing compares that complete raw identity. The decoder
recomputes the canonical reason from raw units using lowest-code precedence.
On Unix: invalid UTF-8 wins first; a leading slash is `Absolute`; after that,
the empty whole path, trailing slash, or doubled slash is `EmptyComponent`;
an exact decoded `.` component is `DotComponent`, then exact `..` is
`ParentComponent`; NUL or backslash is `ForbiddenCharacter`. On Windows:
unpaired UTF-16 wins first; `Absolute` covers leading slash or backslash,
UNC/device forms, and any ASCII-letter-plus-colon prefix including
drive-relative forms; both slash and backslash delimit components;
empty/trailing/doubled separators after the absolute test are
`EmptyComponent`; exact decoded UTF-16 `.` and `..` components follow; then
NUL, any U+0000..U+001F control, or `< > : " | ? *` is
`ForbiddenCharacter`. No platform API's lossy normalization or error order may
choose a different code.
`InvalidUnixUtf8` admits only the Unix arm and `UnpairedWindowsUtf16` only the
Windows arm; the remaining reasons admit either platform arm only when the raw
sequence exhibits the named defect. A wrong arm/reason or bytes that do not
prove the reason reject as noncanonical on persistence and RPC decode.
Enum values
and claimant/path tags are the fixed values above. Equality and healing compare the complete canonical detail
(the digest is only its compact key), so hash equality never authorizes a
different payload. Unknown codes, wrong code/payload pairs, noncanonical
ordering, or invalid cardinality reject on persistence and RPC decode.

`UnreadableScanSubtree` represents enumeration failure before the scanner can
prove a complete file set. Its `ScanSubject` is either the configured root or
one exact lossless raw relative subtree path, with fixed tags `Root=1` and
`Subtree=2`; its failure is exactly `PermissionDenied`, `NotFound`,
`InvalidFileType`, `SymlinkIdentityChanged`, or `IoDataLoss` with codes 1..5.
The scanner canonically enumerates all such failures alongside every other
DSVP candidate, deduplicates exact `(subject, failure)` rows, and includes them
in the ordinary `(code, detail bytes)` authority sort. It never publishes an
apparently complete version after silently skipping a root or subtree.

The same `VersionPoison` value is stored on the published input version,
returned by `MetadataSnapshot::poisoned`, carried by
`QueryError::VersionPoisoned` and §17's typed resolve/metadata/query result
arms, and emitted in invalidation diagnostics. It is
version-global: every namespace-facing query, entry/path lookup, authoring
inspection, and resolve at that stamp returns that identical canonical value.
Version number/state diagnostics and immutable ContentHash fetch remain
usable. A version poison is never inserted into the bundle-failure table,
never represented as `StableFailureFingerprint::Poisoned { bundle }`, and
never memoized in a build trace; healing is publication of a later input
version whose global validation succeeds.

One published header still carries exactly one `VersionPoison`, but selection
is not scan-order-dependent. Before publication the scanner enumerates the
complete candidate set of version-global defects, canonical-encodes each
`(code:u16-LE, exact VersionPoisonV1 detail)`, sorts by `(code, detail bytes)`,
deduplicates identical encodings, and publishes the first row. Presentation messages
never participate. `doctor` may retain and display the complete sorted set as
non-authoritative diagnostics, but persistence, snapshots, invalidations, RPC,
equality, and healing use only the selected first value; no filesystem walk or
thread completion order is a tiebreak.

## 8. Import Pipeline

Two sharply separated phases:

### Authoring import (impure)

Triggered by RPC ("import this external file") or by a watched read-set
(below). The import is a **fold over the prior bundle**, not a pure function
of sources:

```
import(source bytes, settings, Option<prior bundle>) → bundle
```

Content flows from the sources; identity and decisions flow from the prior
bundle — the bundle UUID, entry UUIDs (matched by `local_id`), primary
selection, and the import-settings entry itself. Fresh UUIDs are minted only
for entries with no `local_id` match: the single nondeterministic step.
Everything else is deterministic, which makes re-import a **fixpoint**: a
bundle is in sync exactly when re-running its import over its recorded
read-set reproduces it byte-identically. `doctor` and CI verify that
fixpoint for every watched bundle, so the
committed-the-source-but-not-the-bundle case fails loudly instead of
churning working trees after checkout.
Publication records each bundle's watch flag in its `bundles` row
(`import_watched`, set when the bundle's `$record` is watched). `doctor`
finds the watched bundles through that row's partial index, without parsing
a bundle. A poisoned bundle's row cannot record the flag, so its file is read
instead.

The matching importer from the pipeline module (or a built-in) parses the
external format and returns canonical asset values keyed by `local_id`; the
daemon copies **all** data into the managed bundle — the imported
representation is canonical from then on. Hand edits to imported *content*
do not survive re-import (sources are authoritative for what they produce);
settings and identity do. Re-import — explicit or watched — always reuses
existing entry UUIDs. Settings survive every *recorded-settings* run —
watched re-imports and `reimport` (§17) consume the bundle's own settings
entry; an explicit `import` request naming an existing destination is
different: it supplies settings deliberately, and they replace the
recorded ones.

After import, a texture-from-PNG, a texture-from-PSD, and a hand-authored
texture are indistinguishable downstream: intermediate representations are
unified in the bundle.

```rust
pub trait Importer: Send + Sync {
    const ID: &'static str;                 // stable id; referenced by rules bundles
    /// Schema-described settings entry living in the imported bundle (§6).
    type Settings: AssetType;
    /// Parse sources into canonical values keyed by local_id. All filesystem
    /// access goes through ctx and is recorded as the read-set. The daemon —
    /// never the importer — folds the result over the prior bundle: UUID
    /// reuse, primary selection, settings survival.
    fn import(&self, ctx: &mut ImportContext, settings: &Self::Settings)
        -> Result<ImportOutput, ImportError>;
}

/// Importer-authored stable failure. `code = 0` is reserved; meaning is stable
/// within Importer::ID and the staged dylib identity observed by the attempt.
/// The message is presentation-only; DSLF::Importer carries the code/id and
/// canonical source set. Daemon fold/intake failures instead use the fixed
/// per-code ImportIntakeFailureV1 variants (§5).
pub struct ImportError { pub code: u32, pub message: String }

impl ImportContext {
    /// The fold's matched source paths: the explicit import's file, or the
    /// directory-import group (below). Rooted: which physical root supplied
    /// each source is part of the group's identity (output lands beside it).
    pub fn sources(&self) -> &[RootedPath];
    /// Outcome-bearing content dep: success records the rooted path and
    /// blake3(raw bytes); stable failure (including NotFound) records an
    /// Observed::Err entry before returning Err. Paths are root-relative
    /// and normalized (§10 path rules).
    pub fn read(&mut self, path: &str) -> Result<Vec<u8>, ImportError>;
    /// Outcome-bearing resolution dep: exists | NONE on success — misses
    /// first-class — or a stable observed failure. A path readable in more
    /// than one asset root is Err (§18), never a tiebreak.
    pub fn probe(&mut self, path: &str) -> Result<bool, ImportError>;
    /// Outcome-bearing FileQuery dep: sorted normalized rooted matches, or
    /// a stable listing-failure observation; a failed listing is never an
    /// empty or partial success.
    pub fn enumerate(&mut self, query: &FileQuery) -> Result<Vec<RootedPath>, ImportError>;
}

impl ImportOutput {
    /// A canonical entry in the §6 data model; references in any §4 query
    /// encoding — build import resolves them. The Settings entry and the
    /// ImportRecord envelope are daemon-written, never importer-written —
    /// and enforceably so: local_ids beginning with `$` are **reserved**
    /// (the daemon writes `$settings` and `$record`), so an importer
    /// cannot collide with a daemon-owned entry by construction.
    /// A duplicate `local_id` is an error, never last-write-wins; a
    /// reserved-prefix `local_id` is an error at the call.
    pub fn entry(&mut self, local_id: &str, type_uuid: TypeUuid, value: AuthoredValue)
        -> Result<(), ImportOutputError>;      // DuplicateLocalId | ReservedLocalId
    /// At most one declaration; a second call is an error. Must name a
    /// local_id present in the output (checked at fold).
    pub fn primary(&mut self, local_id: &str)
        -> Result<(), ImportOutputError>;      // ImportOutputError::PrimaryAlreadyDeclared
}
```

The output is a **total replacement set** for importer-produced content:
after the fold, the bundle's content entries are exactly the returned
`local_id`s — an entry present in the prior bundle but absent from the
new output is deleted, its UUID retired (references to it now dangle,
which is the §4 resolution error, never a silent hold-over). Daemon-owned
entries — the Settings entry, the ImportRecord envelope — persist across
the fold by role, not by being re-emitted. Primary selection: an explicit
`primary()` declaration wins; with no declaration, the prior primary
carries over by `local_id` match; if the prior primary's `local_id` is
absent from the new output and the importer declared no replacement, the
fold fails with `ImportIntakeFailureV1::VanishedPriorPrimary` naming the bundle and the vanished
`local_id` — path references resolve to the primary (§4), so guessing
one would silently retarget them. All primary inference is over
**importer-produced content entries only** — the daemon-owned
`$settings` and `$record` entries never count (every imported bundle
contains them, so counting them would leave a normal first import with
no inferable primary). If exactly one content entry exists and there is
no prior primary, it is the primary; otherwise — multiple content
entries, no declaration, no prior — the fold fails asking for an
explicit `primary()`, never guesses.

Importer-returned deterministic failures use the importer `code` and DSLF
Importer arm; daemon-owned output intake/fold checks use the fixed
`ImportIntakeFailureV1` variants (§5), including duplicate/reserved local ids,
primary violations, role/type mismatch, noncanonical values, and invalid
settings. Stable failure messages are presentation-only. A newly introduced
memoizable intake failure is not eligible for attempted-basis persistence
until it has an exact code/field mapping in DSLF v1 (or a bumped grammar).

### Read-sets and watched imports

An importer runs against a context that records every filesystem interaction
as a dependency in the §10 vocabulary, evaluated over the **raw-file
namespace**: reading a file records a content dep (path → hash of the raw
bytes), probing records a resolution dep (path → exists | NONE, misses
first-class), and enumerating records a **`FileQuery`** dep, resolved
against the tracked file table — the same dependency kinds and
invalidation machinery as build deps, driven by file-tracker events (§14)
instead of asset events, with domain-separated result hashes so a
file-namespace result can never alias an asset-namespace result (§10).
File deps stay root-relative, but they evaluate over the **derived
logical path index** (§13: `Missing` / `Unique(root)` / `Ambiguous`),
never a single-root row — so a recorded `Unique` result is invalidated
by a *transition into ambiguity* (a same-path file appearing in a second
root) exactly as by a content change, and the re-run then fails with the
ambiguity error (§18) instead of silently reading whichever root the
old row happened to name; a transition back out re-validates on the
surviving root's content.
The recorded
**read-set** is written into the bundle's `ImportRecord` entry (§6,
declared below), so staleness detection is pure and survives daemon-state
loss.

```rust
/// A configured asset root's persistent identity (§13, §18): its
/// **normalized root name** (§18's named roots, NFC per §10's
/// identifier rule), never a numeric ordinal — an ordinal in committed
/// records would let reordering or adding roots in configuration
/// reinterpret every committed record as naming a different physical
/// root. This is the type every bundle-visible or persisted record
/// declares and serializes — `RootedPath.root`, `FileDep::Probe`'s
/// observation, everything in `ImportRecord` — and the form FILQ
/// result hashes frame (length-prefixed name bytes): the generic
/// schema codec sees a name, never an integer whose meaning lives in
/// process state.
pub struct RootName(pub String);
/// Process-local interning of a `RootName`, used only inside daemon
/// metadata (§13's `files`/`bundles` rows, in-memory indexes). Never
/// serialized, never hashed, never in a bundle-visible type: the
/// RootName ↔ RootId conversion is explicit at the persistence
/// boundary — parse interns, write names.
pub struct RootId(pub u32);
/// The canonical physical file key: which root, plus the normalized
/// root-relative path (§10 path rules). Multi-root identity is never
/// erased — the same path in two roots is two `RootedPath`s.
pub struct RootedPath { pub root: RootName, pub path: String }

pub struct FileQuery {                 // path selectors only (§10), raw-file namespace
    pub path_prefix: Option<String>,
    pub path_glob: Option<String>,     // globset syntax over normalized paths
}
// ≥1 selector; result: sorted (root, normalized path) pairs — sorted
// by root name bytes then path bytes — recorded as
// blake3("FILQ" ‖ version:u8 ‖ count:u32 ‖
//        (root_name: len:u32+bytes ‖ path: len:u32+bytes)*) —
// length-framed, so ["a","bc"] and ["ab","c"] can never alias;
// root-framed, so identical bytes moving between roots at the same
// logical path change the result; framed by root NAME, never ordinal,
// so reordering or adding configured roots can never reinterpret a
// committed hash — and never aliasing an "ASTQ" hash (§10)

#[asset(uuid = "…")]                   // built-in type; daemon-written (§6)
pub struct ImportRecord {
    pub importer: String,              // Importer::ID — which importer re-runs
    pub sources: Vec<RootedPath>,      // weak source paths: provenance + re-import target
    pub watch: bool,
    pub read_set: Vec<FileDep>,        // rediscovered on every run
    pub settings: String,              // local_id of the importer's Settings entry
    /// Present iff this bundle was generated by a directory import
    /// (below): the owning rules bundle, the matching rule, and the
    /// group key. Ownership reconstruction after daemon-state loss
    /// reads this (§2); absence means explicit import.
    pub origin: Option<DirectoryOrigin>,
}

/// A generated bundle's directory-import provenance (§2, §13): who
/// owns it and which group produced it. `group` is the fold's group
/// key — the matched file for `PerFile`; `(root, parent directory,
/// stem)` rendered as a rooted path for `ByStem`.
pub struct DirectoryOrigin {
    pub rules_bundle: BundleUuid,
    pub rule: ImportRuleId,            // stable authored rule id, NEVER a list index
    pub group: RootedPath,
}

pub struct FileContentObservation {
    pub path: RootedPath,              // the root that satisfied the read
    pub hash: [u8; 32],                // raw byte-identity digest (§5)
}

pub enum RawFileOp { Read, Probe, Enumerate }
pub enum RawFileFailureClass { NotFound, PermissionDenied, ListingFailed, OtherStable }
pub enum RawFileSubject { Path(String), Query(FileQuery) }

pub enum FileDep {                     // outcome-bearing raw-file basis
    /// A same-bytes file appearing at the same logical path in another
    /// root is a different observation and invalidates.
    Read { path: String, observed: Observed<FileContentObservation> },
    /// Ok(None) is a first-class miss. Revalidation compares the logical
    /// index verdict; Ambiguous/Missing/a different root, or a changed
    /// stable error fingerprint, invalidates.
    Probe { path: String, observed: Observed<Option<RootName>> },
    /// Ok is the "FILQ" result hash (§10); Err includes stable listing
    /// failure. A failed enumeration therefore has the operation that
    /// heals it in its basis.
    Listing { query: FileQuery, observed: Observed<[u8; 32]> },
    /// Importer resolution against the snapshot's pipeline epoch, hit and
    /// miss alike. A missing importer heals when a later epoch registers it.
    Capability { key: CapabilityKey, observed: Observed<[u8; 32]> },
}
```

An import request may set `watch = true` (an `ImportRequest` field, §17,
recorded into the `ImportRecord`): the daemon re-runs the import
whenever any read-set entry invalidates. Coverage is by construction, not
declaration — the read-set is rediscovered on every run, so transitively
included files, files appearing at previously probed paths, files newly
matching an enumerated glob, and includes added by editing an
already-included file all re-trigger. Importer capability resolution is
part of the same authoring-import basis: lookup of `Importer::ID` records
a `FileDep::Capability` hit or miss before importer code can run. A
watched import whose source disappears surfaces an import error; the
bundle keeps its last-written content. A failed run leaves an
**outcome-bearing attempted-basis record**: the rediscovered read-set up
to and including the failing read, probe, enumeration, or capability
lookup — including `NotFound` and listing-failure `Observed::Err`
fingerprints — plus its terminal `FailureCause`, is recorded in daemon
state as the failure's basis (the bundle's committed
`ImportRecord` is never rewritten by a failed run), and the import
revalidates the terminal failure exactly like a build trace and re-runs
exactly when *that* outcome changes — healing a missing file, listing, or
importer wakes it; unchanged failures never continuously retry and can
never be suppressed forever. The default
remains `watch = false`: explicit re-import, read-set
as provenance only. Read-set entries are authoring-side dependencies — they
gate re-*import*, never enter build input hashes, and leave build purity
untouched.

**Publication revalidates the read-set.** An import's reads and its
commit are not one atomic step: a source event can be consumed by the
coordinator after the importer read the old bytes but before the new
read-set is installed — and once a stale result installed its
dependencies, that event would never re-fire and nothing else would
wake the import: a lost wakeup leaving the watched bundle stale
indefinitely. Import results therefore commit under the same
**attempted-basis validation** the build memoization already uses (§9,
§13): inside the committing coordinator transaction, the complete
rediscovered read-set is revalidated against the current raw-file
index (§13's `files` rows and derived logical path index) — every
read, probe, and listing `Observed` outcome plus every importer-
capability hit/miss must still match; for a failed attempt the terminal
failure is revalidated under the same `FailureCause` rule as §9 — and
the bundle write and its dependency rows install
atomically only if every entry holds. On any mismatch the result is
discarded and the import re-enqueued against the newer state, so the
committed `ImportRecord` always describes a read-set that was current
at its own installation.

A **directory import** scales the fold without breaking it: an authored
rules bundle owns the listing query as *its* read-set, and applying the
rules is a batch of **independent single-bundle folds** — one per matched
group, each with its own read-set, settings, and prior. No fold ever
writes another fold's bundle; multi-bundle application follows the
long-running-operation rules (§17): per-bundle atomic, loudly reported,
never transactional across files. When the listing loses a group, the
generated bundle is **orphaned, never auto-deleted** — authored files are
sacred (§2): it enters the same error state as a watched import whose
source vanished, is listed by `doctor`, and is deleted only by explicit
user action. Output paths are deterministic (`<stem>.bundle`,
`<name>.glsl.bundle`, beside their sources); a naming collision is a rules
error, never a tiebreak. Every generated bundle also persists its
**directory origin** in the daemon-owned `$record` entry
(`ImportRecord.origin`, above): the rules bundle's UUID, the matching
rule's stable `ImportRuleId` (never its vector position), and the group
key. Reordering or inserting rules therefore cannot reinterpret an
existing origin; deleting the referenced rule id puts every bundle it
generated into the defined orphan state above. Ownership is thereby
reconstructible from the tree alone (§2): after daemon-state loss, the
rebuild scan reclaims each generated bundle for its rules bundle from
the record — orphan detection and the listing-loss error state
re-derive instead of vanishing — and a bundle with no origin record is
an explicit import, never adopted by any rules bundle.

```rust
#[asset(uuid = "…")]                    // built-in type
pub struct DirectoryImportRules {
    /// The listing is this rules bundle's own read-set.
    pub listing: FileQuery,
    /// First matching rule wins per file; a file matching no rule is ignored.
    pub rules: Vec<ImportRule>,
}

pub struct ImportRule {
    /// Stable authored UUID-style identity, globally unique across rules.
    /// Reordering the vector never changes ownership; duplicates are a
    /// rules-bundle validation error before any fold runs.
    pub id: ImportRuleId,
    pub matches: FileQuery,
    pub group: Grouping,                // one fold per group
    pub importer: String,               // Importer::ID
    /// Settings template, validated against the importer's Settings schema.
    pub settings: AuthoredValue,
    /// Naming template over {stem} / {name}: "{stem}.bundle",
    /// "{name}.glsl.bundle" — output lands beside its sources.
    pub output: String,
}

pub struct ImportRuleId(pub [u8; 16]);

pub enum Grouping { PerFile, ByStem }   // ByStem: group key is
                                        // (root, parent directory, stem) —
                                        // never the bare stem: output lands
                                        // beside its sources, and a group
                                        // spanning directories would have no
                                        // unique "beside". foo.vert/foo.frag
                                        // in two directories are two groups.
```

### Build import (pure)

For every bundle in the tree, the daemon validates each entry against its
schema, resolves references to UUIDs (recording resolution deps, §10), and
encodes each entry into the binary artifact format. No pipeline code runs
for schema-described data — with three keyed exceptions: custom migration
function edges, default materialization in the final automatic migration
segment (`WriteFieldDefault`/`WriteParentDefault`, §11 — custom edges
carry literal values and run no default code), and validators registered
for the type. Whenever any of them executes, the pipeline dylib hash joins
the input hash and the module-epoch rules (§3) apply to the job. And
whenever any of them is *looked up* — a `MigrationFn` key, a
`DefaultTable` entry — the resolution records a `TraceOp::Capability`
observation against the snapshot's pipeline epoch, **hit and miss
alike** (§9): a missing registration fails before any code runs, so
without the recorded miss the failure's memo would carry no module
dependency at all, and registering the missing capability could never
heal it.

Build import is **per entry** — each entry gets its own artifact under
its own key, and the key follows the processor model (§9): a **static
pre-key** plus a **discovered-trace bucket**. The pre-key —
`blake3("DSBI" ‖ version ‖ entry AssetUuid ‖
bundle uuid ‖ local_id ‖ entry type uuid ‖ the entry's terminal type
uuid (pipeline map, §9) ‖ canonical bundle bytes ‖ the entry's current
logical hash ‖ layout hash ‖
artifact-format version)` — §5's canonical record encoding — carries
everything computable before work: the bundle
bytes already include the source content, blob bytes, settings entries,
the `ImportRecord`'s importer id, and embedded schemas; the entry
identity fields pin *which* entry, without which two same-typed sibling
entries (several `ShaderStage`s in one shader bundle, §20) would supply
identical key inputs and resolving B could return A's artifact
wholesale. What only execution can discover goes in the trace bucket:
the reference resolutions the **migrated** value produces. A
`MigrationFn` or default materializer can synthesize references no
static scan of the bundle bytes can enumerate, so discovering them
means running `load_current` (§11) — acknowledged as execution, not
lookup: each resolution records the resolved UUID and the referenced
asset's *observed terminal type* (misses first-class) as trace entries,
memoized and revalidated under exactly the processor rules — a bucket
of candidates per pre-key, keyed secondarily by trace digest (`"DSTR"`),
revalidated most-recent-first on lookup (§9, §13).
**Pipeline-map retyping is therefore visible per entry**: a module
reload that retypes a referenced asset from `T` to `U` fails exactly
the referencing entries' trace revalidation (the bundle bytes and
resolved UUID alone would
be unchanged, and a stale artifact would otherwise hit, bypassing §9's
reference re-validation); the entry's own terminal-type pre-key term
re-keys the entry when its own chain retypes, since the header records
`terminal_type`. There is deliberately no whole-pipeline-map hash term:
that would invalidate every cached import on any map change —
over-invalidation the per-entry terms make unnecessary. Publication and
every cache hit additionally verify the artifact header's `asset_uuid`
against the requested entry and its `authored_type`/`terminal_type`
against the current snapshot's pipeline map (§12).
There is no target term: artifacts carry no target field
(§12), and for build imports the layout hash is the only target-varying
input, so layout-identical targets share artifacts and cache entries. When
a migration chain applies (§11), the chain joins the key — each applied
migration bundle's bytes, the planner version, and the dylib hash under the
rule above — and plan *selection* records a query dependency over the
migration edges applicable to (type, from-hash), positive and negative, so
authoring a competing edge invalidates a cached result into the ambiguity
error rather than silently keeping the old chain.

## 9. Processing Pipeline

Processing adapts imported assets to their runtime-compatible form. Processors
are typed transforms registered in the pipeline module:

```rust
pub trait Processor: Send + Sync {
    const ID: &'static str;      // stable processor id (input hash, below)
    const VERSION: u32;          // manual bump; the dylib hash backstops staleness
    type Input: AssetType;
    /// Which targets this processor serves (pipeline map, below). Two
    /// processors for one input type with overlapping selectors are a
    /// registration error; the selector's canonical encoding joins
    /// pipeline-map identity.
    fn targets() -> TargetSelector;
    /// The closed output declaration (below): primary type plus every
    /// extra key with its type. Associated fns, deliberately: `const ID`
    /// plus single-owner registration (§3) already pin one registration
    /// per processor *type*, so a `&self` here could never vary — the
    /// interface is a per-type constant, like the signature of a fn. An
    /// associated `const` can't allocate the Vec on stable Rust; the
    /// no-arg associated fn is that const's idiom. The registered
    /// *instance* exists for `process` state (scratch, pools), never for
    /// interface facts.
    fn outputs() -> OutputDecls;
    fn process(&self, input: Self::Input, ctx: &mut ProcessContext)
        -> Result<Outputs, BuildError>;
}

/// Processor-visible failure payload. `code = 0` is reserved; every nonzero
/// code's meaning is stable within `(Processor::ID, Processor::VERSION)`.
/// Reusing a code for a different meaning requires a VERSION bump. `message`
/// is presentation-only and is excluded from DSLF; the Processor DSLF row
/// carries this `code` as `build_error_code` together with id/version/stage.
pub struct BuildError { pub code: u32, pub message: String }

pub struct OutputDecls {
    pub primary: TypeUuid,
    pub extras: Vec<(String, TypeUuid)>,   // chain-unique keys (below); owned
                                           // strings so keys may be computed
                                           // (format!("lod{i}")) — evaluated
                                           // once at registration, frozen
}

pub struct TargetSelector {          // present fields AND; all-None = every target
    pub os: Option<BTreeSet<TargetOs>>,      // sets — the selector's canonical
    pub apis: Option<BTreeSet<GraphicsApi>>, // encoding joins pipeline-map identity
}
// Match, normatively: per present field the target must be COVERED —
// target.os ∈ os, target.apis ⊆ apis. A selector declares what the
// processor supports; a target is served only by a processor supporting
// its entire enabled API set. Overlap (the per-input-type registration
// error): every present field pair intersects (None is universal) —
// exactly the condition under which some target could match both.
// Empty sets are rejected at both ends: a present selector field must
// be non-empty (registration error) and Target.apis must be non-empty
// (config error, §18) — an empty target API set would vacuously match
// every selector and void overlap detection.

impl Outputs {
    // No public constructor: obtained only through ctx.outputs(), which
    // binds (parent uuid, declared output set) at creation — the binding
    // that lets extra() mint its UUIDv5 refs immediately. At result
    // binding, each bound value is lowered through its type's generated
    // encode visitor (§4's `encode` entry) into the neutral wire-builder
    // vocabulary; that event stream — flat bytes, canonical-order
    // container events, blob handoffs, typed reference emissions — is
    // everything the daemon consumes: no typed value crosses the module
    // boundary.
    /// Must bind exactly the declared set: the primary once, every extra
    /// once. `extra` returns the child's typed reference —
    /// UUIDv5(parent, key) — for embedding in the primary value: extras
    /// are reachable only through built artifacts (§7), so the returned
    /// ref is the one way a processor can make its extra reachable at all.
    pub fn primary<T: AssetType>(&mut self, value: T)
        -> Result<(), HostCallbackError>;
    pub fn extra<T: AssetType>(&mut self, key: &str, value: T)
        -> Result<AssetRef<T>, HostCallbackError>;
    /// Cache-internal debug payload (§13's auxiliary table): dumpable,
    /// never pullable, never in manifests. Keys obey the output-key
    /// bound (1–255 UTF-8 bytes, below) and are unique per job; a
    /// violation fails result binding naming the key — debug keys are
    /// runtime-chosen, so the check that registration does for declared
    /// keys happens at binding — never truncated, never last-write-wins.
    pub fn debug(&mut self, key: &str, bytes: Vec<u8>)
        -> Result<(), HostCallbackError>;
}
```

Every deterministic daemon-side rejection while binding that closed table —
missing/duplicate primary or extra, undeclared key, type mismatch, invalid
debug/output key, or deterministic encode rejection — maps to the fixed
`OutputBindingFailureV1` variant and DSLF arm (§5). A processor's own returned
`BuildError` remains the Processor arm; a callback panic is epoch poison, not a
local memo. The same totality rule maps deterministic artifact-encoder
rejections to `ArtifactEncodingFailureV1` rather than ad-hoc prose.

- **Type-changing**: input and output types differ freely
  (`ShaderSource → CookedShaderPackage`).
- **Identity**: the primary output inherits the parent asset's UUID —
  processing is invisible to the runtime, which only ever requests source
  identities. Extra named outputs get `UUIDv5(parent_uuid, output_key)`.
- **Outputs are declared, closed, and static.** Registration declares the
  complete output-key set and each output's type; at runtime `Outputs`
  must bind exactly that set — data-dependent output names or types are a
  build error. Extra outputs are terminal by declaration (chains continue
  only through the primary): each extra is encoded directly as its
  declared type, with `encoded_type = terminal_type = declared type`,
  even when that type is a registered processor input. Only primaries
  ever traverse processor chains. A parent type's extra key set is
  **target-invariant like its terminal type**: chains may differ per
  target, but every target's chain must declare the same extra keys with
  the same types — a pipeline-map construction error otherwise. (Without
  this, §13's target-free derived index would either claim children some
  target cannot produce — breaking "remembered children always
  resolve" — or silently become target-scoped. A Vulkan-only "reflection"
  extra is not expressible; declare it on every chain or on none.)
  Keys are unique across the parent type's **entire
  chain** — enforced when the pipeline map is constructed — so
  `UUIDv5(parent, key)` cannot collide and stays stable even when an
  output moves between stages. This is also what makes the static-input key's
  output hashes computable before any work runs, and what lets §13's
  derived-output index be derived from assets × pipeline map with no
  build ever run — the whole reason a remembered child UUID resolves
  under laziness. A value-dependent output *set* would force builds to
  answer namespace questions, and is the bug class the input-versioned
  index exists to kill (§13, §22). Identifier bounds are
  registration-checked: processor and importer IDs, output keys, and
  `debug` keys (at result binding — they are runtime-chosen) are
  1–255 UTF-8 bytes — the empty key is the primary's reserved encoding
  in output tables and CAS records (§13), so an empty extra key is
  rejected at registration and at result binding, never aliased — a
  processor declares ≤ 256 outputs, and a chain has ≤ 65,535 stages
  (`StaticInputs::stage` is `u16`). Serialized static-input keys are unbounded
  composites, so the CAS never stores them raw: records key on the
  32-byte `"DSSI"` digest (§13). Overlong identifiers are registration
  errors, and the store rejects rather than truncates, so framing limits
  can never alias two keys.
- **1→N is reachable only through the parent.** The build is pull-based: work
  happens because a source identity was demanded (by the loader or a pack
  build). Derived outputs are not in the authoring namespace — source data
  cannot reference them and queries do not enumerate them. They are reached
  exclusively through references embedded in built artifacts, starting from
  the primary output's load dependencies; pack reachability follows the same
  edges.
- **Data-dependent 1→N is an *import* concern, never a processor
  concern.** A source that splits into N pieces — a glTF's meshes, a
  shader stem's stages (§20) — splits at authoring import into N sibling
  *entries* of a bundle: authored namespace, each with its own UUID and
  chain, enumerable and queryable with no build run, N free to vary with
  the data because the entries *are* data. Processor extras are the
  complement: per-**type** companions (reflection, collision hulls)
  whose key set is as fixed as the output type itself. Between the two,
  data-dependent multiplicity always has a home — authored entries, or a
  collection inside one value (a package keyed by entry point) — and the
  derived namespace never depends on what any build produced.
- **Reading other assets**: `ctx.read::<Skeleton>(ref)` returns another
  asset's built artifact, building it inline if stale. The processor callback
  API is synchronous, so nested reads use ordinary native recursion on the
  current build worker. Host-amplified nesting is therefore bounded by a
  mandatory `max_dependency_depth` in `1..=64` (default 32), and build
  workers use explicit 8 MiB stacks. Module callbacks must not place large
  buffers on the native stack. Exceeding the cap returns a named chain error to
  the request; it is never memoized, trace-bearing, or poisoning, so a later
  shallower request can succeed. A cache consult counts as one live frame, not
  its recorded subtree. The request-local visit set rejects cycles.
  Concurrent identical requests may execute the same deterministic stage; they
  never wait on one another, and their serialized CAS commits converge on the
  same static-key/trace candidate.

- **Path lookups**: `ctx.read_path::<T>("lighting/brdf.glsl")` resolves then
  reads, recording both a resolution dep and a content dep. Misses are
  recordable (negative deps) so include-search-path probing invalidates
  correctly.
- **Set inputs**: `ctx.query(AssetQuery) → Vec<AssetUuid>` for aggregate
  assets (atlases, shader databases, level indexes). See §10.
- **Target platform**: `ctx.target`; artifacts are cached per target and
  shipped per target (§16).
- **Load dependencies**: discovered automatically, never declared —
  the type's generated encode visitor (§4's `encode` entry) emits every
  reference the output value carries as a typed reference event
  (strong/weak + target uuid), so `AssetRef` fields reach the daemon
  through the neutral event vocabulary and processors never enumerate
  them manually. Discovery is also
  **verification**: at result binding, every reference the visitor
  emits — `AssetRef<T>` and `WeakAssetRef<T>` alike, whatever UUID the
  processor constructed it from — is resolved against the job's snapshot
  as `(uuid, expected terminal type)` and recorded as a
  `TraceOp::RefCheck` entry, revalidated on every cache lookup like any
  trace entry. A strong ref whose UUID is absent at the snapshot, or
  whose pipeline-map terminal type differs from the declared `T`, fails
  result binding with a build error naming the field path (§4's
  resolution error); a weak ref may observe absence (`observed: None` —
  legal, it never gates load). Either way the observation is in the
  trace, so deleting or retyping a referenced asset invalidates the
  cached result instead of leaving a valid-looking artifact holding a
  now-invalid typed reference — the sorted `load_deps` table (§12)
  deliberately stores bare UUIDs, so the trace entry is where the
  expected-type edge lives. Load-dep graphs must be DAGs —
  but commit-time enforcement is impossible under laziness: committing
  `A → B` cannot see an unbuilt `B → A`. The check lives in every closure
  walk instead: `ctx.read` recursion already errors on build cycles, and
  the loader sweep and pack builds maintain a visit stack while expanding
  load deps, erroring on any cycle with its members named (v1's loader
  instead deadlocks on A↔B, and only waits for deps without verifying
  versions). Concurrent jobs never join or wait on one another; each expands
  its own graph until its local visit stack names the cycle. Intentionally
  cyclic data uses weak references, which resolve after load and do not gate
  completion.

The context those bullets describe:

```rust
impl ProcessContext {
    /// Content dep; builds the target recursively (cycles error). T is a
    /// terminal type per the pipeline map's typing rules.
    pub fn read<T: AssetType>(&mut self, r: AssetRef<T>) -> Result<Artifact<T>, BuildError>;
    /// Resolution dep (misses first-class) + content dep.
    pub fn read_path<T: AssetType>(&mut self, path: &str) -> Result<Artifact<T>, BuildError>;
    /// Query dep over the asset namespace.
    pub fn query(&mut self, q: &AssetQuery) -> Result<Vec<AssetUuid>, BuildError>;
    pub fn target(&self) -> Result<&Target, HostCallbackError>;       // §18
    /// The only way to obtain an Outputs: bound at creation to this
    /// job's (parent uuid, declared output set).
    pub fn outputs(&self) -> Result<Outputs, HostCallbackError>;
    /// The only subprocess path: registry-resolved, trace-recorded (below).
    pub fn run_tool(&mut self, id: &str, args: &[&str], stdin: &[u8])
        -> Result<ToolOutput, BuildError>;
}

/// A fetched, ContentHash-verified, fixed-up terminal artifact (§12).
pub struct Artifact<T: AssetType> { /* structural buffer + constructed value */ }
impl<T: AssetType> Deref for Artifact<T> { type Target = T; }

pub struct ToolOutput { pub status: i32, pub stdout: Vec<u8>, pub stderr: Vec<u8> }
```

`run_tool` is the **only** dynamic-execution carrier available to
pipeline code. Runtime `dlopen` is forbidden in importers, processors,
validators, migrations, and default materializers, and no API resolves a
tool entry to a library path or handle; code that must vary outside the
statically linked pipeline cdylib runs as a §9 subprocess.

Tool identity has two deliberately simple modes:

```rust
pub struct ToolRegistration {
    pub source: ToolSource,
    pub environment: Vec<(String, String)>,
    pub cwd_policy: ToolCwdPolicy,
}
pub enum ToolSource {
    Package { root: PathBuf, launcher: String },
    Ambient {
        launcher: PathBuf,
        toolchain_id: String,
        trusted_fingerprint: Option<[u8; 32]>,
    },
}
pub struct ToolExecutionIdentityV2 {
    pub source: ToolSourceIdentityV2,
    pub environment: Vec<(String, String)>,
    pub cwd_policy: ToolCwdPolicy,
}
pub enum ToolSourceIdentityV2 {
    Package { launcher: String, files: Vec<ToolPackageFile> } = 1,
    Ambient {
        launcher: String,
        toolchain_id: String,
        trusted_fingerprint: Option<[u8; 32]>,
    } = 2,
}
pub struct ToolPackageFile {
    pub path: String,
    pub executable: bool,
    pub len: u64,
    pub bytes_hash: [u8; 32],
}
#[repr(u8)]
pub enum ToolCwdPolicy {
    EmptyScratch = 1,
    ReadOnlyPackageRoot = 2,
    ReadOnlyPackageSubdir(String) = 3,
}
```

`Package` is the cacheable hermetic mode. Registration recursively snapshots
every regular file below `root`, rejects symlinks and special files, requires
`launcher` to be a canonical relative path naming an executable package file,
and stages the complete directory no-replace. Package paths are NFC UTF-8 with
`/`, relative, traversal-free, unique, and raw-byte sorted; each `bytes_hash` is
raw blake3 of the staged bytes and `len` is checked. Distill deliberately does
not parse ELF, Mach-O, PE, shebangs, or native loader search metadata. Producing
a self-contained package for the target host is the responsibility of the tool
package/build system; all packaged bytes, including any bundled interpreters,
plugins, resources, or libraries, are already covered by the directory snapshot.

`Ambient` names one canonical absolute executable path plus a nonempty NFC
`toolchain_id`. It is not copied and Distill does not inspect its adjacent files
or native dependencies. With `trusted_fingerprint: Some`, the caller asserts a
stable toolchain/package identity and the invocation may memoize; the fingerprint
and toolchain ID both enter DSCT. With `None`, the tool may run but every build
attempt that calls it is nonmemoizable: no success or failure record from that
attempt enters a candidate bucket. This makes local SDK discovery usable without
pretending that an ambient installation is reproducible.

Environment keys/values are NFC strings, keys are unique and raw-byte sorted,
and the published vector is the complete sanitized environment used at launch:
ambient variables including `PATH`, locale, home, and temp are cleared unless
present literally. `ReadOnlyPackageRoot` and `ReadOnlyPackageSubdir` are valid
only for package tools; ambient tools use `EmptyScratch`.

`DSCT v2 = blake3("DSCT" || 0x02 || canonical ToolExecutionIdentityV2)` using
§5's record codec and the fixed tags above. A package DSCT commits every package
file row plus launch/environment/cwd metadata. An ambient DSCT commits the exact
launcher path, toolchain ID, optional trusted fingerprint, environment, and cwd
policy. Registration publishes the identity and DSCT hash as snapshot input.
Packaged files are reopened and verified at launch; an unavailable package or
ambient executable yields only the transient nonmemoized launch outcome below.

### Pipeline map

Processor registration induces, per target, a static mapping from authored
type to terminal type:

- At most one processor per `(input type, target)`: processors declare
  their targets (`Processor::targets()`), and two selectors overlapping on
  the same input type are a registration error.
- Chains follow output types to a fixpoint: if a processor's output type has
  its own processor, it runs next. A cycle in the type graph is a
  registration error.
- A type with no processor is its own terminal type (its import encoding
  ships as-is).
- Chains may differ per target, but the **terminal type must be
  target-invariant** (registration error otherwise). Per-target variation
  lives in artifact content, not in the type — e.g. one
  `CookedShaderPackage` type whose bytes differ per backend.
- The map is published in the metadata store and the pack manifest; editors,
  the loader, and reference validation all consult the same table.

| Authored type | Chain (e.g. target = vulkan) | Terminal type | Identity |
|---|---|---|---|
| `ShaderSource` | ShaderCook | `CookedShaderPackage` | parent UUID |
| `TextureSource` | TextureCompress | `Bc7Texture` | parent UUID |
| `ShaderInclude` | — | `ShaderInclude` (build-only) | parent UUID |
| `HpBarConfig` | — | `HpBarConfig` | parent UUID |
| extra outputs | any | per output declaration | `UUIDv5(parent, key)` |

Typing rules that follow:

- `AssetRef<T>` means *"an asset whose terminal type is `T`"*. Referencing a
  `ShaderSource` bundle from an `AssetRef<CookedShaderPackage>` field
  validates through the map. Game code is typed by what it consumes;
  authored data references whatever produces it.
- Loader handles are typed by terminal type. The metadata store and manifests
  carry both authored and terminal type UUIDs per asset.
- `ctx.read::<T>` reads terminal artifacts. A processor that needs
  pre-cook data models it as a type with no processor (`ShaderInclude`), not
  by reaching into the middle of another chain.
- Terminal type does not make an asset a runtime concept. Runtime existence
  is decided by load-dep reachability alone: processor reads are build deps
  and never enter pack manifests, so a `ShaderInclude` is baked into the
  cooked package and ships nowhere. `#[asset(build_only)]` marks types for
  which appearing in any load-dep closure is a validation error. The
  check is judged **per closure node by that node's own type**: an
  authored asset whose *authored* type is build-only is unreachable at
  runtime through every stage of its chain (processing `A → B` does not
  launder a build-only `A` into a shippable `B` — the closure node is
  still the `A` asset); a derived extra is judged by its own declared
  output type's bit, never its parent's. Both facts are explicit
  metadata — the authored entry's type and the declared output type — so no
  implementation has room to judge reachability two ways. Each process uses
  the `build_only` bit generated into its own current descriptor. The daemon
  rejects forbidden pack/build closures; the loader repeats the check while
  adopting a closure. A module edit that changes the bit is an ordinary
  PipelineEpoch change: affected live RPC clients reconnect and rebuild their
  closures. `build_only` is deliberately absent from DSLH, so toggling a
  deployment policy never invents a schema migration.

### Input hashes

Every hash is named by what it hashes. Four identifiers, three of which
are lookup keys:

| Identifier | Hash of | Lookup key into |
|---|---|---|
| **Content hash** (`ContentHash`) | raw blake3 of an artifact's *bytes* — §5's domainless byte-identity exception | the CAS extent index: `ContentHash → segment, offset, len` (§13) |
| **Static-input key** (`StaticInputs` digest, `"DSSI"`) | a processor run's *static* inputs, computable before work | the `artifacts` table: digest → (trace, output table) (§13) |
| **Build-import input hash** (`"DSBI"`, §8) | a build import's *static pre-key* — the reference resolutions the migrated value produces are discovered by execution and live in the trace bucket (§8) | the `artifacts` table: digest → candidate bucket (§13) |
| **Full input hash** | *all* processor inputs, discovered trace included | nothing — never stored as an index; it is what a validated hit *witnesses* (below) |

The content hash identifies what came out; the input hashes identify what
went in. The full input hash is
`blake3("DSIH" ‖ version:u8 ‖ input artifact ContentHash ‖ dependency
trace ‖ processor id + version ‖
pipeline dylib hash ‖ target-definition hash ‖ output logical + layout
hashes ‖ artifact-format version)` — domain-registered like every hash
construction (§5's table) even though it is never stored as an index. The target-definition hash is the
canonical encoding of the full target struct (§18) — os, arch, API set,
options — never a config name. The dylib hash means a manually unbumped
processor version can never serve a stale artifact — the worst case is
over-invalidation after a rebuild, never wrong reuse. The
**dependency trace** is the
canonically ordered sequence of every context operation *with its label
and its observed outcome*: each entry records
`Observed<T> = Ok(T) | Err(StableFailureFingerprint)` (declared below) —
`read(uuid) → ContentHash`, `resolve(path) → uuid | NONE`,
`query(selector) → result-set hash`, `tool(id) → DSCT execution-identity hash` on
success; on failure a typed, content-derived fingerprint (ambiguity: the
sorted conflicting ids; poison: the poison row's identity; a missing
strong reference: the query and expected terminal; a descendant build
failure: the child's own fingerprint; an absent ToolEpoch entry:
`MissingCapability { Tool(id) }`). A failure record therefore carries
a trace (possibly empty) plus a terminal **`FailureCause`** (declared
below) — the failing op's own `Observed::Err` entry when a context
operation failed, or `FailureCause::Local` for deterministic local
failures no operation produced (validator diagnostics, migration-plan
validation, a processor `BuildError`) — and revalidates exactly like a
success (§13); transient infrastructure failures remain traceless and
unmemoized. External tool
binaries invoked as subprocesses are trace operations, never static
inputs (which tool runs can depend on inputs, and the dylib hash cannot
see them), revalidated on every lookup like all trace entries.
Pipeline-dylib hashes are §5 byte-identity digests over the staged dylib.
Tool identity is instead the domain-separated semantic/composite DSCT aggregate
above; a launcher byte hash alone is never a trace identity.
Subprocesses launch only through `ctx.run_tool(id, args)`, and the tool
a job runs is a **snapshot input**: registering or replacing a tool
(`Registry::tool`, §3) snapshots a package directory or records an explicit
ambient toolchain and publishes the (tool key → execution identity + DSCT hash)
mapping as
input-versioned **ToolEpoch** state (§13) — a registration change
advances the input version like any other input event. A job resolves
`id` through its pinned snapshot. Package tools launch only the staged package
the published DSCT names; ambient tools launch their explicit path, and only a
trusted fingerprint makes that call memoizable. A swap mid-epoch invalidates
traces into rebuilds that run the new registration. Staged package
versions coexist; `DriftedInput::Tool` (§15) covers an old basis whose
package has been evicted.
If `id` is absent from that snapshot's ToolEpoch, `run_tool` records the
terminal `TraceOp::Tool` miss and `MissingCapability { Tool(id) }` specified
below; it does not attempt a launch and cannot use `ToolLaunch` as an alias.
PATH lookup and symlink retargeting are never registration mechanisms. Distill
constructs a package launch only from snapshotted members and uses the staged
package as its cwd; the package author remains responsible for the process's OS
access. Ambient launchers are explicitly unstaged and are nonmemoizable without
a trusted fingerprint. Hashing
labeled operations rather than a sorted multiset of
content hashes means two dependencies that swap contents change the key.

The input hash and the content hash are deliberately distinct
identifiers. Purity
makes the input hash a unique *determinant* of the artifact — it answers
"has this exact build been done?", with the static-input key as the
stored index and the trace revalidation supplying the rest of the
answer. The content hash is the artifact's
*identity* — what dep traces record, clients fetch, and packs ship. The map
is many-to-one (distinct inputs routinely produce identical bytes), and that
collapse is what stops invalidation cascades: a rebuild that yields the same
content hash leaves every downstream trace valid.

**Lookup uses the static-input key.** The full input hash contains the trace,
which only execution discovers — it identifies a completed build but cannot
be computed before one. Cache lookup therefore keys on
the **`StaticInputs` digest** (declared below) — **every static
determinant of the
full input hash** (output types come from the pipeline map, their hashes
from the current registries), all computable before any work, leaving the
trace as the only discovered part. One static-input key does not determine
one result: two snapshots can share every static determinant while a path
resolves or a query answers differently, yielding distinct valid traces —
so the store maps static-input key → a **bucket of result candidates**,
each secondarily identified by its trace digest (`"DSTR"`, §5), and a
lookup revalidates candidates most-recently-committed-first against the
current snapshot (re-resolve the path, re-hash the query result set,
compare the read content hashes, re-check emitted-reference
observations), hitting on the first whose every entry
still holds — the verifying-trace discipline. Committing a new candidate
under an occupied key adds to the bucket, never overwrites: memos stay
monotone (§13), and two snapshots alternating over one key each keep
hitting their own candidate instead of rebuilding each other's away.
Bucket growth is bounded by CAS eviction (§13), which retires candidates
with their artifacts — a policy decision, distinct from correctness. Because statics live in the key and the trace
is revalidated, a hit *implies* the full input hash matches: an output
relayout, schema revision, or format bump can never reuse a stale
artifact. Concurrent requests do not share an in-memory flight: each validates
durable candidates against its own snapshot and may execute duplicate pure
work. The single-writer CAS commit path deduplicates identical candidates and
preserves distinct valid trace buckets. This avoids cross-job blocking without
changing cache authority.

```rust
pub struct StaticInputs {
    pub asset: AssetUuid,
    pub stage: u16,                        // index in the type's chain (pipeline map)
    pub input_hash: ContentHash,       // the input artifact's content hash
    pub target_def_hash: [u8; 32],         // canonical Target ‖ bound identity (§18)
    pub processor: (&'static str, u32),    // Processor::ID, Processor::VERSION
    pub dylib_hash: [u8; 32],
    /// Per declared output key: (logical, layout) hashes from the current
    /// registries, in key order.
    pub output_hashes: Vec<(String, LogicalHash, LayoutHash)>,
    pub artifact_format_version: u32,
}

/// What an op observed — success or a stable failure. The Err arm is
/// what lets a failure record's trace carry its failing terminal op
/// (§13) and revalidate it like any entry; transient infrastructure
/// failures never mint a fingerprint and never memoize (§9, §13).
pub enum Observed<T> {
    Ok(T),
    Err(StableFailureFingerprint),
}

/// A typed, content-derived encoding of a deterministic failure —
/// stable across runs, so revalidation compares it like a result hash.
pub enum StableFailureFingerprint {
    /// Ambiguous resolution or query: the sorted conflicting ids.
    Ambiguous  { conflicting: Vec<AssetUuid> },
    /// A poisoned bundle or version (§7, §13): the poison row's identity.
    Poisoned   { bundle: BundleUuid },
    /// A strong reference that failed to resolve: the query and the
    /// expected terminal type.
    MissingRef { query: AssetQuery, expected_terminal: TypeUuid },
    /// Exact runtime/reference resolution found the UUID, but its entry role
    /// is not eligible for the attempted carrier. Revalidation consults the
    /// role index, so an explicit role edit wakes the memo.
    RoleIneligible { asset: AssetUuid, observed_role: EntryRole },
    /// Coordinator-private control query/read failure. Subject, fixed u16
    /// code, and entries obey §9's exact sorted/deduplicated per-code DSTR
    /// grammar; presentation text is excluded.
    Control { subject: ControlFailureSubject, code: ControlFailureCode,
              entries: Vec<AssetUuid> },
    /// A descendant build failed: the child's own failure fingerprint.
    Descendant { asset: AssetUuid, fingerprint: Box<StableFailureFingerprint> },
    /// Authoring-import raw-file failure (§8): the exact attempted
    /// operation and canonical path/query plus a stable failure class.
    /// Revalidation compares the outcome, so NotFound or a failed
    /// listing wakes as soon as it heals.
    RawFile { op: RawFileOp, subject: RawFileSubject,
              class: RawFileFailureClass },
    /// A required pipeline or ToolEpoch capability was not registered. A
    /// pipeline miss terminates TraceOp::Capability; Tool(id) terminates
    /// TraceOp::Tool. Revalidation heals on the first snapshot epoch that
    /// supplies the registration.
    MissingCapability { key: CapabilityKey },
    /// A deterministic LOCAL failure that arose from no context
    /// operation (FailureCause::Local, below): fingerprinted by class
    /// plus `blake3("DSLF" || v1 canonical local-failure detail)` (§5).
    Local { class: LocalFailureClass, detail: [u8; 32] },
}
pub enum ToolErrorClass {
    NotExecutable, SpawnDenied, PackageUnavailable, AmbientUnavailable,
}
/// A post-lookup execution outcome. It is returned only to the attempted
/// caller and never enters StableFailureFingerprint, DSTR, or a memo bucket.
pub struct TransientToolLaunchFailure {
    pub id: String,
    pub tool_hash: [u8; 32],
    pub class: ToolErrorClass,
}
// LocalFailureClass and its exact DSLF payload grammar are declared in §5.

/// What a capability lookup asked the pipeline or ToolEpoch for — the
/// identity a recorded miss carries. Tags are fixed by the DSTR grammar.
#[repr(u8)]
pub enum CapabilityKey {
    MigrationFn(String) = 1,
    DefaultTable(TypeUuid) = 2,
    Importer(String) = 3,
    Processor { input: TypeUuid } = 4,
    Tool(String) = 5,
}

/// A failure record's terminal cause (§13): a failure memoizes as a
/// dependency trace (possibly empty) plus exactly one FailureCause.
/// Either the trace's own terminal entry is the failing operation (an
/// Observed::Err — the record ends in it), or the failure is LOCAL and
/// deterministic — validator error diagnostics, migration-plan
/// validation, a processor returning BuildError — and carries its
/// fingerprint here, with no synthetic trace op invented for it. Both
/// kinds memoize and revalidate under the same rules.
pub enum FailureCause {
    /// The trace's terminal entry (an Observed::Err op) is the cause.
    Op,
    Local(StableFailureFingerprint),
}

pub enum TraceOp {                         // the labeled, outcome-bearing trace
    /// Authoring-service source read. The whole owning bundle is the atomic
    /// authored file; Ok(None) is a first-class miss. Pipeline build code
    /// cannot construct this operation.
    AuthoringRead { asset: AssetUuid,
                    observed: Observed<Option<BundleFileHash>> },
    Read    { asset: AssetUuid, observed: Observed<ContentHash> },
    Resolve { path: String, observed: Observed<Option<AssetUuid>> },
                                           // Ok(None) is a first-class miss,
                                           // never a failure
    Query   { query: AssetQuery, observed: Observed<[u8; 32]> },  // domain-prefixed (§10)
    /// The tool key and aggregate DSCT execution hash the snapshot's ToolEpoch
    /// (§13) resolved it to — snapshot-pinned, never a live path. A miss is
    /// Observed::Err(MissingCapability { key: CapabilityKey::Tool(id) }).
    Tool    { id: String, observed: Observed<[u8; 32]> },
    /// Pipeline capability resolution (§8, §11): every lookup of a
    /// registered capability — MigrationFn key, DefaultTable, importer,
    /// processor for (input type, target) — records what the snapshot's
    /// epoch supplied, HIT AND MISS alike: Ok(the epoch's dylib hash)
    /// on hit, Err(MissingCapability) on miss. Without the recorded
    /// miss, a failure minted before any code ran would carry no module
    /// dependency and could never heal when the registration lands.
    Capability { key: CapabilityKey, observed: Observed<[u8; 32]> },
    /// Emitted-reference validation at result binding (load-dependency
    /// bullet, above): the reference's target UUID, the declared T's
    /// terminal type, and the snapshot's observed terminal type —
    /// Ok(None) = absent at the snapshot (legal only for weak refs; a
    /// strong ref's absence or retype is the failing terminal entry of
    /// a failure record, fingerprinted as MissingRef).
    RefCheck { asset: AssetUuid, expected_terminal: TypeUuid,
               observed: Observed<Option<TypeUuid>> },
    /// Role eligibility is observed separately from existence/type. A strong
    /// or direct internal reference to AuthoringOnly terminates with
    /// StableFailureFingerprint::RoleIneligible, never MissingRef; Runtime
    /// then proceeds to RefCheck. Normal public queries still filter roles.
    RoleCheck { asset: AssetUuid,
                observed: Observed<Option<EntryRole>> },
    /// Coordinator-private control-plane query (§6, §10). It is canonical
    /// trace data but its type cannot be constructed by pipeline/runtime code.
    Control { query: ControlQuery, observed: Observed<[u8; 32]> },
    /// Non-artifact control value read. The canonical subject plus observed
    /// whole-bundle byte identity revalidates at the attempted basis.
    ControlRead { subject: ControlSubject,
                  observed: Observed<BundleFileHash> },
}
```

The dependency digest has one exact v1 wire grammar:
`DSTR v1 = blake3("DSTR" || 0x01 || op_count:u32 || ops...)`. Operations
remain in observation order (they are a sequence, not a set), with `u8` tags
`Read=1`, `Resolve=2`, `Query=3`, reserved `ToolLaunch=4`, `Capability=5`,
`RefCheck=6`, `RoleCheck=7`, `Control=8`, `ControlRead=9`, and `Tool=10`; each
payload is encoded in the
field order declared above by the §5 canonical record codec. `Observed` uses
`Ok=1` and `Err=2`, followed by its declared payload. Repeated operations are
retained because multiplicity and order are part of the witnessed attempt.
`AuthoringRead=11` follows the same record rules and encodes its optional
`BundleFileHash` with the canonical `Option` tag. It is available only to
daemon authoring services such as codegen: ordinary processor `Read` remains
the ContentHash-bearing artifact operation and the two meanings never share a
tag.

`CapabilityKey` has its own fixed `u8` tags:
`MigrationFn=1`, `DefaultTable=2`, `Importer=3`, `Processor=4`, and `Tool=5`,
followed by the declared NFC string or raw TypeUuid fields. A ToolEpoch lookup
miss records `TraceOp::Tool { id, observed:
Err(MissingCapability { key: Tool(id) }) }` as the terminal operation and sets
`FailureCause::Op`; revalidation against a later snapshot heals when `id`
resolves to a verified tool identity. A lookup hit records
`Observed::Ok(tool_hash)` before execution. If that exact registration
then yields `NotExecutable`, `SpawnDenied`, `PackageUnavailable`, or
`AmbientUnavailable`, the whole
attempted result and its trace are discarded and the caller receives
`TransientToolLaunchFailure { id, tool_hash, class }`; it is never memoized.
An unfingerprinted ambient hit may return a successful `ToolOutput`, but it marks
the complete calling processor attempt nonmemoizable, so neither success nor a
later deterministic failure from that attempt enters a candidate bucket.
DSTR v1 tag 4 is permanently reserved under the historical name `ToolLaunch`
and every decoder MUST reject it. It has no payload grammar and can never be an
alias for a ToolEpoch miss, a successful `Tool` observation, or a transient
execution outcome.

The control subgrammar is fixed rather than inheriting declaration order.
`ControlQuery` uses the `ControlQueryTag` `u8` above followed by
`MigrationEdges(type_uuid:[u8;16], from_hash:[u8;32])` or no payload for
`DirectoryImportRuleSet`. `ControlSubject` similarly uses its fixed `u8` tag
followed by: `Migration(asset:[u8;16])`, `PackDefinition(asset:[u8;16])`,
`DirectoryImportRules(asset:[u8;16])`,
`ImportSettings(bundle:[u8;16], local_id:str)`, or no payload for
`SchemaLineageManifest`. A control `Observed::Err` encodes
`StableFailureFingerprint::Control` as
`ControlFailureSubjectTag:u8 || subject || ControlFailureCode:u16-LE ||
entry_count:u32 || entries`; entries are raw AssetUuid bytes sorted and
deduplicated, with the per-code cardinalities declared on
`StableControlError`. A noncanonical order, duplicate, wrong cardinality,
unknown tag, or unknown numeric failure code rejects at persisted-trace and
RPC decode. This grammar is shared by trace creation and revalidation; no
presentation message enters DSTR.

**A build result is a named output table, committed atomically.** The unit
the cache stores is not one ContentHash but the whole result: `output_key →
(type uuids, ContentHash)` — the primary output under a reserved key plus every
extra output — together with the discovered trace. Each output is its own
DSTL artifact with its own ContentHash and load-dep list. One coordinator
transaction commits the CAS index rows for every output and the
static-input-key → (trace, result) row. It writes no child row: an extra
output's `child uuid → (parent uuid, output_key)` lives only in the
input-versioned namespace index (below), never as a claim of the
commit's own. UUIDv5 is one-way,
so that index is how `resolve(child, at)` finds the parent — and it cannot
miss: a client can only name a child it read out of a fetched parent
artifact, and that artifact's commit wrote the index row first. Child
resolution is then parent resolution: resolve the parent at the same
snapshot (memoized or built) and read the producing stage's table. A
result pins and evicts as one unit — all outputs or none (§13). Chains
aggregate: the **chain result** for (asset, target) is the terminal
stage's primary plus the union of every stage's extras (chain-unique keys,
above, make the union well-formed). The derived-output row is immutable
and stage-free — `child uuid → (parent uuid, output_key)`, write-once,
identical whichever stage or target produced it — because stages are
neither stable (an output may move between stages) nor global (chains
differ per target, though every target's chain declares the same extra
key set, §9 — what makes a target-free row well-defined at all): the
producing stage derives at resolution from the
snapshot's pipeline map, whose static output declarations name which
stage of the parent's chain for the connection target declares
`output_key`, unambiguous by chain-uniqueness. And because declarations
are static, the index never depends on builds having run: it is
**input-versioned** — derived for each published input version from that
version's assets × its pinned pipeline map (§13), so creating or
deleting a bundle updates it under unchanged code and an epoch rotation
updates it for unchanged assets — and a client holding a child UUID
across eviction, daemon restart, or `.distill` loss can always resolve
it (UUIDv5 is one-way; without this, a remembered child would be
permanently orphaned). This snapshot-scoped index is the **only
authority** for child resolution: anything recovered from historical
CAS results is memo data, never a namespace claim — a child UUID resolves at a snapshot iff that
snapshot derives it, so a retired output key's UUID can later be minted
for authored data without a stale result resurrecting the old
claim. The index is validated against authored UUIDs at
publication (§7) — collisions surface before the version is queryable,
never as ordering-dependent resolution. No artifact is created —
laziness intact. Anything that pins a
parent's terminal artifact pins the whole chain
result, earlier-stage extras included, so a child UUID read out of any
held artifact is always resolvable for as long as that artifact is held.

**Invariant: the build never writes to source files.** Authoring writes
bundles; building writes only daemon state.

### Validators

```rust
pub trait Validator: Send + Sync {
    type Asset: AssetType;
    fn validate(&self, asset: &Self::Asset, diag: &mut Diagnostics)
        -> Result<(), CallbackPanic>;
}

impl Diagnostics {
    pub fn error(&mut self, path: FieldPath, message: impl Display)
        -> Result<(), HostCallbackError>;  // blocks publish
    pub fn warn(&mut self, path: FieldPath, message: impl Display)
        -> Result<(), HostCallbackError>;
}

/// Field-path addressing into an asset value: `stages[2].entry_point`.
pub struct FieldPath(pub Vec<PathSeg>);
pub enum PathSeg {
    Field(String),
    Index(u64),
    Key(String),        // map entry (by the key's) or set element (by its
                        // own) canonical JSON encoding
    Variant(String),
}
```

Registered per asset type in the pipeline module, like processors; multiple
validators per type all run (read-only, so order is irrelevant). The input
is the typed value from the shared bundle-load path (§11) — **single-asset
by construction**: no `ctx.read`, no queries, so the build-import
dependency set is unchanged by validation — and bound by the determinism
contract below like all pipeline code. Cross-asset invariants are not
validator territory — they belong to processors (which record what they
read) or to authoring-time lint tooling over queries. Output is
field-path-addressed diagnostics at two severities: `diag.error` and
`diag.warn`.

Invoked at two points:

1. **Authoring time** — at the end of import, adoption, and editor writes.
   Diagnostics return with the operation result and are stored in metadata;
   the file write never blocks — authored files may be committed
   work-in-progress, and git is the net.
2. **Build import** — after migration, before the artifact commits. Any
   `error` fails that asset's build: the manifest entry goes
   `StaleLastGood` (or stays `Missing`), the error is surfaced, and a pack
   build fails on any `error` in its closure. This is the publishability
   gate.

A validator decides publishability, so it is part of build identity: when
validators are registered for a type, the dylib hash joins that type's
build-import key (§8).

### The determinism contract

Pipeline code — importers, processors, validators, migration functions,
default materializers — is **trusted, not sandboxed**. The dylib hash
establishes code identity, not purity: native code can always reach
`std::fs`, the clock, or the environment behind the context's back. The
contract is: *all inputs through the context; determinism over those
inputs.* A violation is a bug whose blast radius is unbounded staleness —
the cache cannot see what it was never told about. Every reproducibility
claim in this document ("content hashes are determined by snapshot inputs",
"checkout + rebuild is byte-identical") holds under this contract, not
unconditionally. Mitigations, not enforcement: the optional double-run
determinism check, `doctor verify` (rebuild-and-compare), and process
isolation remaining available as future hardening (§22). `doctor verify`
is a read-only report on one read snapshot: it holds no write lock,
publishes no version and repairs nothing. It rescans every root against
the published input, reruns every watched import against its bundle,
rebuilds every runtime entry twice without a cache and compares both with
each other and with the cached result, and checks every CAS extent's bytes
against its hash; each difference is a finding.

## 10. Dependency & Invalidation Model

Everything a build step consumes is recorded as one of three dependency kinds:

| Kind | Recorded as | Invalidated by |
|------|-------------|----------------|
| **Content** | asset UUID → artifact ContentHash read | that asset's artifact changing |
| **Resolution** | path → resolved UUID *or NONE* | the path's mapping changing (create, delete, rename, primary change). Negative results are first-class: a file appearing at a previously-missed path invalidates. |
| **Query** | structured query → `blake3(sorted result UUIDs)` | the result set changing membership |

Rule of thumb: *content deps for what you read, resolution deps for how you
found it, query deps for sets you enumerated.* Reference resolution (§4) is
the cardinality-1 case of a query dep, so references record exactly these
dependencies with no extra machinery.

`AssetQuery` is structured — by type UUID, by search tag, by path prefix/glob —
never an arbitrary predicate, so the daemon can index queries by their
selectors (type → queries, tag → queries, path prefix trie; exact-path index
for resolutions) and re-evaluate only the queries an asset event could affect.
Content changes within a query's result set are already covered by the per-
member content deps. Processors needing finer filtering query broad and filter
after reading; correctness holds via the content deps.

Queries come in two domains: `AssetQuery` over the asset namespace (result:
sorted UUIDs) and `FileQuery` over the raw-file namespace (§8; result:
sorted rooted normalized paths). Their result hashes are domain-separated and
length-framed — `blake3("ASTQ" ‖ version:u8 ‖ count:u32 ‖ uuids)` vs
`blake3("FILQ" ‖ version:u8 ‖ count:u32 ‖ (root_name: len:u32+bytes ‖
path: len:u32+bytes)*)` — root names, never ordinals (§8) — so
identical selectors over the two namespaces can never alias, and
variable-length path lists cannot collide by re-splitting
(fixed 16-byte UUIDs need only the count). Query results
are returned to callers already in canonical order, so a caller can never
observe ordering the recorded result hash doesn't cover. And recorded dependencies are **labeled** — each trace entry
keeps its operation and argument rather than folding into an unlabeled hash
set (§9, input hash).

### AssetQuery, precisely

A conjunction of optional selectors, all indexable:

| Selector | Matches | Invalidation index |
|---|---|---|
| `uuid` | the entry with that exact UUID (existence + type validation) | uuid → queries |
| `bundle_path` | the primary entry of the bundle at that path | exact-path index |
| `bundle_path` + `local_id` | that entry in that bundle | exact-path index |
| `local_id` | that entry in the referencing bundle itself | (bundle uuid, local_id) → queries |
| `bundle_uuid` | entries in the bundle with that UUID (with `local_id`: that entry) | bundle uuid → queries |
| `authored_type` | entries of that authored type | type → queries |
| `terminal_type` | entries whose pipeline-map terminal is that type (well-defined: terminals are target-invariant, §9) | same, via pipeline map; a pipeline-map change re-evaluates every `terminal_type` query and re-validates typed references |
| `tag` / `tag=value` | entries whose `#[asset(tag)]` fields contain it | tag → queries; a tag change invalidates under both old and new values |
| `path_prefix` | all entries in bundles under a root-relative directory | prefix trie; a move invalidates under both old and new prefixes |
| `path_glob` | entries in bundles whose path matches | trie on the glob's longest literal prefix |
| `authoring_only` | `true` selects only authoring/control entries; `false` only runtime entries | entry-role index; `true` is legal only on the explicit tooling query surface |

The first four rows are the reference forms of §4 — exact-match selectors,
cardinality 1. A bare `local_id` (no `bundle_path` or `bundle_uuid`) is
**bundle-relative**, legal only where an origin bundle exists — reference
resolution and same-bundle lookups (§20) — and the daemon **closes** it
over that origin (writing `bundle_uuid` = the referencing bundle) before
evaluating or recording it, so every evaluated, traced, or revalidated
query is self-contained: a `TraceOp::Query` replays identically with no
ambient context. Context-free surfaces — `MetadataSnapshot::query`, RPC
`Snapshot.query`, pack roots (§16) — reject unclosed bundle-relative
queries as errors. Present selectors AND together; the result is the set of
matching entry UUIDs, canonically sorted. A query must contain at least one
selector — whole-tree enumeration is a tooling RPC, never a recordable
dependency. Editor queries (§17) use the same type; there is one query
language — and the same language, restricted to path selectors, describes
importer read-sets over the raw-file namespace (§8).

Absent `authoring_only` means `false`: ordinary queries uniformly exclude
authoring/control rows. `Some(true)` is accepted only by the explicit pinned
authoring-tooling query RPC. `ProcessContext::query`, authored references,
runtime `Snapshot.query`, and pack-root validation reject it before evaluation;
no ordinary dependency trace or shipping closure can opt authoring-only
entries in. The coordinator-private `ControlQuery` above is not an
`AssetQuery` mode and cannot be deserialized from any public surface; its one
migration-edge selector records `TraceOp::Control` and is the only control
lookup allowed to join a migration/build attempted basis (§6, §11).
Direct UUID resolution is governed by the same role index even though it is
not query syntax: runtime `Snapshot.resolve`, loader pulls, and closure
expansion return `RoleIneligible` for an authoring-only UUID. Internal strong
reference resolution records `RoleCheck`; finding AuthoringOnly returns the
same typed `RoleIneligible` fingerprint rather than collapsing it to a miss,
and a role edit invalidates that observation. Only §17's
separate inspection API can read its authored value, and that API has no
build/process/pack carrier.

```rust
pub struct AssetQuery {                    // present selectors AND together; ≥1 required
    pub uuid: Option<AssetUuid>,
    pub bundle_path: Option<String>,
    pub local_id: Option<String>,
    pub bundle_uuid: Option<BundleUuid>,
    pub authored_type: Option<TypeUuid>,
    pub terminal_type: Option<TypeUuid>,
    pub tag: Option<TagSelector>,
    pub path_prefix: Option<String>,
    pub path_glob: Option<String>,
    pub authoring_only: Option<bool>,
}
pub struct TagSelector { pub tag: String, pub value: Option<String> }
// result: matching entry UUIDs, canonically sorted, recorded as
// blake3("ASTQ" ‖ version:u8 ‖ count:u32 ‖ uuids)

/// Coordinator-private control authority. Constructors and the token type are
/// not exported through Registry, ProcessContext, MetadataSnapshot's public
/// surface, RPC, or pack code.
pub struct ControlSnapshot<'a> {
    snapshot: &'a MetadataSnapshot,
    authority: ControlAuthorityToken,
}
struct ControlAuthorityToken { /* coordinator-minted, basis-bound */ }

/// The only authoring-control query whose result participates in build/migrate
/// dependency traces. Its result UUIDs use the ASTQ sorted-set hash, while the
/// TraceOp::Control label and this canonical argument distinguish its meaning.
pub enum ControlQuery {
    MigrationEdges { type_uuid: TypeUuid, from_hash: LogicalHash },
    /// Role-inclusive enumeration for the autonomous directory-rule service;
    /// result is the complete sorted DirectoryImportRules AssetUuid set.
    DirectoryImportRuleSet,
}
#[repr(u8)]
pub enum ControlQueryTag {
    MigrationEdges = 1,
    DirectoryImportRuleSet = 2,
}
pub enum ControlSubject {
    Migration(AssetUuid),
    PackDefinition(AssetUuid),
    DirectoryImportRules(AssetUuid),
    ImportSettings { bundle: BundleUuid, local_id: String },
    SchemaLineageManifest,
}
#[repr(u8)]
pub enum ControlSubjectTag {
    Migration = 1,
    PackDefinition = 2,
    DirectoryImportRules = 3,
    ImportSettings = 4,
    SchemaLineageManifest = 5,
}
/// Private closed decoded carrier. There is deliberately no AuthoredValue arm
/// and no conversion into a build/process/encode value.
pub enum ControlValue {
    Migration(MigrationControlValue),
    PackDefinition(PackDefinition),
    DirectoryImportRules(DirectoryImportRules),
    ImportSettings(ValidatedImportSettings),
    Lineage(SchemaLineageManifest),
}
pub struct MigrationControlValue {
    pub asset: AssetUuid,
    pub target_type_uuid: TypeUuid,
    pub from_hash: LogicalHash,
    pub to_hash: LogicalHash,
    pub from_schema: LogicalSchema,
    pub to_schema: LogicalSchema,
    pub from_lineage: LineageStamp,
    pub to_lineage: LineageStamp,
    pub kind: MigrationControlKind,
}
pub enum MigrationControlKind {
    Ops(Vec<MigrationOp>),
    Function { key: String },
}
/// Branded, schema-validated settings bytes. Only the authoring-import bridge
/// can decode them into the registered Importer::Settings type; there is no
/// generic AuthoredValue accessor or artifact encoder implementation.
pub struct ValidatedImportSettings {
    pub settings_type: TypeUuid,
    pub schema_hash: LogicalHash,
    canonical_bytes: Vec<u8>,
}
pub enum ControlFailureSubject {
    Query(ControlQuery),
    Read(ControlSubject),
}
#[repr(u8)]
pub enum ControlFailureSubjectTag { Query = 1, Read = 2 }
/// A recorded control operation. `trace` has the same success/failure polarity
/// as `outcome`: success commits the sorted-set/value byte identity, and a
/// StableControlError maps exactly to StableFailureFingerprint::Control.
/// The basis and trace therefore survive deterministic failure just as they do
/// a hit or an empty successful query.
pub struct ControlAttempt<T> {
    pub outcome: Result<T, StableControlError>,
    pub basis: SnapshotStamp,
    pub trace: TraceOp,                 // TraceOp::Control or ControlRead
}
pub struct StableControlError {
    pub code: ControlFailureCode,
    /// Sorted/deduplicated raw AssetUuid bytes. RoleViolation and Poisoned
    /// carry one or more offending entries; Ambiguous carries at least two
    /// conflicts; every other code requires this vector to be empty.
    pub entries: Vec<AssetUuid>,
    pub message: String,                // presentation only; never hashed
}
#[repr(u16)]
pub enum ControlFailureCode {
    RoleViolation = 1, Missing = 2, Ambiguous = 3, Poisoned = 4,
    Malformed = 5, WrongBuiltInType = 6, WrongRole = 7,
    UnsupportedFormat = 8, SchemaClosure = 9,
}
pub struct StaleControlBasis;

impl<'a> ControlSnapshot<'a> {
    fn stamp(&self) -> SnapshotStamp; // exact negative-proof/trace CAS basis
    /// Returns sorted Migration AssetUuids and appends TraceOp::Control to the
    /// caller's attempted basis. Hit, empty set, and stable failure all return
    /// a ControlAttempt; only a basis race returns the unrecorded retry arm.
    fn query_migrations(&self, type_uuid: TypeUuid, from_hash: LogicalHash)
        -> Result<ControlAttempt<Vec<AssetUuid>>, StaleControlBasis>;
    /// Complete autonomous DirectoryImportRules discovery. Empty, hit, and
    /// stable failure all record TraceOp::Control at this basis.
    fn enumerate_directory_rules(&self)
        -> Result<ControlAttempt<Vec<AssetUuid>>, StaleControlBasis>;
    /// Closed built-in decode only: no raw AuthoredValue, artifact, processor,
    /// runtime handle, or shippable dependency token can be produced. Its
    /// ControlRead trace is created before decode/validation and retained on
    /// every success or stable failure. The subject/value mapping is exact:
    /// Migration→Migration, PackDefinition→PackDefinition,
    /// DirectoryImportRules→DirectoryImportRules,
    /// ImportSettings→ImportSettings, and SchemaLineageManifest→Lineage;
    /// any other decoded kind is WrongBuiltInType, never a coercion.
    fn read(&self, subject: ControlSubject)
        -> Result<ControlAttempt<ControlValue>, StaleControlBasis>;
}
```

Path rules are pinned, and **total**: a normalized path is a `/`-separated
sequence of one or more components, each a non-empty NFC-normalized UTF-8
string that is not `.` or `..` and contains no `/`, `\`, or NUL; no
leading, trailing, or doubled separators. Anything else — absolute paths,
traversal steps, empty components — is **rejected at intake** (import
context calls, query construction, scan): containment in the root is
lexical, by construction — and the lexical rule governs path *strings* only.
What the daemon physically opens is additionally governed by §14's canonical
root containment and observe/revalidate/retry rules.
For a physical scan name rejected before this normalized String exists, the
scanner publishes §7's `InvalidPhysicalPath` DSVP using the complete lossless
Unix-byte or Windows-UTF-16 arm and exact failure code; it never repairs the
name or invents a lossy `normalized_path`.
Comparison is byte-exact after NFC normalization (case-sensitive; `doctor`
flags trees differing only by case — a hazard on case-insensitive
filesystems). Two physical directory entries in one root that normalize to
the **same** NFC key publish §7's
`SameRootNormalizedPathCollision` version poison with one root/key and at
least two strictly ordered lossless raw-path/file-hash claims — rejected
before either row is inserted, never
last-write-wins into the `files` table (§13), whose key is the normalized
path. Identical normalized paths in different roots do not take this branch;
they remain `Ambiguous(roots)`. The glob grammar is `globset` syntax (`*`, `**`, `?`, `[…]`,
`{…}`) over normalized root-relative paths.

The same normalization boundary governs **every string identifier**, not
just paths: `local_id`s, derived-output keys, and tag names/values are
NFC-normalized at intake (importer output calls, `#[asset(tag)]`
extraction, query construction), uniqueness checks run **after**
normalization, and `UUIDv5(parent, key)` hashes exactly those normalized
UTF-8 bytes (§7, §9). One rule, no raw/normalized seams: precomposed and
decomposed spellings of one key are one key everywhere — they cannot pass
a raw uniqueness check yet collide in an index, and they cannot mint
distinct child UUIDs for what serialization treats as one name. Codegen uses
only the fixed full `AssetUuid` hex for physical filenames/module identifiers
(§20); normalized `local_id` is display metadata, never global identity.

### Tag indexing under lazy migration

Tags are extracted at the **current** schema: when a bundle is dirtied, the
indexer runs the shared bundle-load path (`load_current`, §11) — the same
implementation build import uses, never an indexer-specific loader — and
reads tag fields from the result; data is never read against a schema other
than its own. Fields the migration chain *defaulted* index at their
defaulted values, exactly as loaded: `load_current`'s result is the value
disk migration would write (§11), so rewriting a file on disk never
changes a query result, and query semantics always equal loaded-value
semantics — a processor querying `tag(category=enemy)` sees every asset
whose loaded value carries the tag, authored or defaulted. Op provenance
(`Loaded::defaulted`) is diagnostic metadata, never query semantics. Each index entry records
**every input of its `load_current` run**: the applied migration bundles'
hashes, the plan-selection deps (§11's per-visited-node edge-set queries),
the capability lookups — hits and misses, §9's `TraceOp::Capability` —
the planner version, the dylib hash when code ran, and the tag-annotation
epoch — so editing even a data-driven migration bundle re-extracts
affected tags exactly as a pipeline reload does. If `load_current` fails
during indexing, the entry's identity metadata (uuid, type, path) still
commits but the entry is **tag-poisoned**: any tag query whose selectors
could match it (its type, or a type-unconstrained tag query) **fails with
an error naming the poisoned bundles** instead of returning an
under-approximation — a processor can never bake an incomplete atlas out
of a silently shrunken result set. Fixing the bundle or the migration
unblocks; the failure also surfaces as a bundle-level indexing
error. Tag markers are **not** stored in schema snapshots — a snapshot's bytes
are exactly what its logical hash covers (§5), keeping hash → content unique.
The current (type → tag-field set) map comes directly from the authenticated
watched shared schema. Its source hash is the conservative local
tag-annotation epoch: any schema edit re-extracts tags for affected entries —
metadata-only work, no artifacts touched.

The epoch is kept per authored type, not for the schema as a whole: a
digest of the type's logical hash and of every schema type its value
reaches, attributes included (tag markers, renames), numbered in the order
the walk reaches them, so an edit elsewhere in the schema leaves it
unchanged. `tag_epochs` holds, per type, the epoch its rows were refined
under; a complete step or a configuration candidate records the current
epochs, and each type whose epoch changed has its rows marked pending, by
one search of `assets_by_type`, in that input. Refinement then redoes
exactly the stale rows: those a publication or an epoch change marked
pending (a search of the partial `assets_tag_poisoned`), and the rows
whose recorded module hash (`assets.tag_module`) differs from the current
module's (a search of the partial `assets_tag_migrated` over the rows where
code ran), never
a walk of every asset. Each bundle is read and parsed once per refinement,
however many of its entries it refines. A refinement that fails writes
poison rows for exactly the entries it was refining, in the same input; the
in-memory commit alone would not reach a store that applies deltas.
Those rows are the tag poisons §13's asset queries fail on. A pending row
counts as poisoned too, but the input that marks it also refines it, so a
published version holds one only where that refinement failed.

**A configuration change publishes its difference.** A candidate whose
roots are unchanged republishes, through the incremental publication a
watcher edit uses, only the sources its difference from the published
state reaches: every bundle holding an asset of a type whose pipeline
interface (terminal type, derived outputs) changed; the poisoned bundles
holding an asset of a type whose tag epoch changed, since the authority
validates their skeleton; and, when either set is non-empty, the malformed
sources and those of colliding claims, whose claims publish nothing to
compare. A pipeline-module-only change reaches no source; its migrated tag
rows are refinement's. Only a root replacement, or a process that has not
yet observed the roots or compiled the store's version, scans and
publishes completely; a complete publication too republishes the
poisoned bundles of the types whose tag epoch changed.

Recorded dependencies live in daemon state and double as the reverse indexes
for change propagation (v1's `reverse_path_refs` table was the precursor).
This replaces v1's stubbed transitive build-dependency propagation: deps are
discovered from what was actually read, so they are never wrong.

## 11. Migration System

### Inputs

- The **old** logical schema: embedded in the bundle being migrated.
- The **new** logical schema: from source-walk.
- **Custom migrations: they are bundles.** A `Migration` asset declares
  `{ target_type_uuid, from_hash, to_hash, kind }` where `kind` is either
  data-driven field ops or the key of a function registered in the pipeline
  module. **The migration bundle embeds both endpoint schemas** in its
  `schemas` section (schema-closure, §6): they are guaranteed available when
  the migration is authored, and embedding them means an edge can never
  outlive the schemas needed to use it — even after every asset bundle has
  migrated past `from_hash`. Migration assets live in the tree, version with
  the code and content they describe, and are discovered only through §10's
  coordinator-private `ControlSnapshot::query_migrations`: their
  `authoring_only` role keeps them out of normal/runtime queries without
  making the mandatory migration graph invisible. Every returned UUID is then
  read as `ControlSubject::Migration` into the closed
  `MigrationControlValue`. Its `ControlRead` observation is installed before
  bootstrap decode or plan validation can fail; every success and stable
  failure joins the attempted basis. A read/decode failure hard-stops — it is
  never reinterpreted as no edge and can never expose the trailing automatic
  diff.

```rust
pub struct Migration {                  // schema pinned by format_version (bootstrap, below)
    pub target_type_uuid: TypeUuid,     // with from_hash: the built-in indexed
    pub from_hash: LogicalHash,         // fields behind ControlQuery::MigrationEdges (§10)
    pub to_hash: LogicalHash,
    /// Complete predecessor records for the embedded endpoint snapshots;
    /// each list ends in its corresponding hash and verifies its DSSL
    /// commitment (§6). Reconstruction includes both lists (§11).
    pub from_lineage: LineageStamp,
    pub to_lineage: LineageStamp,
    pub kind: MigrationKind,
}

pub enum MigrationKind {
    Ops(Vec<MigrationOp>),              // data-driven, in the logical-plan vocabulary below
    Function(String),                   // key of a MigrationFn registered in the module (§3)
}

/// Deterministic, value-to-value, no I/O (determinism contract, §9); runs
/// inside load_current whenever the selected chain contains a function
/// edge. Constructed only by the module-side `migration_fn!` macro, whose
/// generated wrapper runs the body under catch_unwind (§3): a bare fn is
/// not registrable, so panic containment cannot be forgotten.
pub struct MigrationFn(fn(AuthoredValue) -> Result<AuthoredValue, MigrationError>);
/// `code = 0` is reserved; meaning is stable within the registered function
/// key and its staged dylib identity. Message is presentation-only; DSLF's
/// MigrationFunction arm carries the code and exact edge identity.
pub struct MigrationError { pub code: u32, pub message: String }
```

### Plan computation

Automatic plans come from `ngp-schema`'s field matching, refactored into two
stages. Matching emits a **logical migration plan** — the path-addressed,
value-level `MigrationOp` vocabulary declared below. `WriteFieldDefault`
and `WriteParentDefault` are distinct sources — the field type's default
versus the field's value taken from the parent type's `Default` — and can
produce different values; an unmatched enum variant carrying data is an
error requiring a custom edge, as is a post-migration map-key collision.
A name-matched struct field or enum variant whose `rev` differs (§4, §5)
is an automatic-plan **hard stop**: the plan refuses, naming the path
and both revs, and demands a custom migration edge. Neither automatic
copy nor drop-plus-default across a rev change is ever legal — `rev`
declares the same shape with new meaning (metres → centimetres, §4), so
copying silently reinterprets the value and defaulting silently discards
it: both are exactly the defect the revision exists to prevent.
The logical plan is the shared artifact. distill's executor interprets it
over `AuthoredValue` directly and must **fail hard** where a default is
unavailable — integrity (below) forbids fabricated values. `FieldOp` and
its offset-based in-memory executor stay **engine-owned** (hot-reload
machinery, §19): the engine may lower the same logical plan to `FieldOp`s
for live-value migration, but that lowering is newgameplus's concern —
distill never extends, invokes, or depends on it. What is shared from
`migrate.rs` is the matching/planning core and the primitive widening
rules. The two matchings differ by what identity is *available*: the
disk planner diffs two **snapshot ASTs** (the embedded historical
`SchemaNode` tree against the current type's projection), which carry no
nominal types by design — so it matches **structurally**: struct fields
and enum variants by name at each node, containers by position, exactly
the identity the logical hash defines; the only nominal input is picking
the root (the entry's `type_uuid` selects which current schema to diff
against). `TypeDef`-level matching on stable type UUIDs (crate + name
fallback) is the *engine* planner's business, where live `TypeDef`s
exist on both sides. The disk-side default ops — legal only in the
automatic segment (below) — materialize values by
calling into the
pipeline module (the dylib hash joins the input hash when they run, §8).

```rust
pub enum MigrationOp {                                 // path-addressed, value-level
    CopyField         { from: FieldPath, to: FieldPath },
    Widen             { from: FieldPath, to: FieldPath },
    /// A literal, materialized at edge-AUTHORING time — the only
    /// default-shaped op legal in custom edges (below).
    WriteValue        { to: FieldPath, value: AuthoredValue },
    WriteFieldDefault { to: FieldPath },               // the field type's default —
                                                       // automatic segment only (below)
    WriteParentDefault{ to: FieldPath },               // from the parent type's Default —
                                                       // automatic segment only (below)
    WriteNone         { to: FieldPath },
    DropField         { at: FieldPath },
    MapVariant        { at: FieldPath, from: String, to: String,
                        payload: Vec<MigrationOp> },   // unmatched + data = error
    MigrateElements   { at: FieldPath, element: Vec<MigrationOp> },  // Vec/array/Option/map values/
                                                                     // set elements — sets rebuild
                                                                     // through real equality; a
                                                                     // post-migration duplicate
                                                                     // element is an error, exactly
                                                                     // the map-key collision rule
    MigrateMapKeys    { at: FieldPath, key: Vec<MigrationOp> },      // key collision = error
    MigrateInline     { at: FieldPath, ops: Vec<MigrationOp> },
}
// There is deliberately no per-op custom-function variant: an op-level
// fn hook would carry no input path, no output path, and no endpoint
// schemas, so it could not participate in the total-and-disjoint output
// rule below. Custom logic has exactly one carrier — the whole-edge
// MigrationKind::Function, whose endpoints the migration bundle embeds.
```

**Default ops are automatic-segment-only; custom edges carry literals.**
`WriteFieldDefault`/`WriteParentDefault` consult the live module's
`DefaultTable<T>`, which exists only for the *current* schema — a
historical edge consulting a later module's table would be unexecutable
(the field is gone) or silently change meaning (the default moved). So a
custom `MigrationKind::Ops` edge may not contain them: **edge authoring
materializes** the then-current defaults into literal `WriteValue` ops
(the `migration new` scaffold and every authoring surface do this), so a
custom edge replays byte-identically forever from its bundle alone; a
`Migration` bundle carrying a default-table op is rejected at bundle
validation and indexing, naming the edge. Dynamic default materialization
is confined to the final automatic diff into the current schema, whose
target fields exist in the current module by construction (the dylib hash
joins the input hash when they run, §8).

**Execution semantics are normative, not executor-defined.** Every edge —
`Ops` or `Function` — executes value-to-value: ops **read from the edge's
immutable input value** (the value at `from_hash`) and **write into a
fresh output value**; no op ever observes another op's writes, so op
order can never make two conforming executors produce different authored
values, and `CopyField` **copies** — the source stays readable to later
ops; a move is spelled copy plus an explicit `DropField`. Plans are
validated before execution, against the edge's embedded endpoint
schemas: every `from` path must resolve in the `from_hash` schema and
every `to` path in the `to_hash` schema — a missing path is a typed
migration error naming the path and the edge — and plans are **total and
disjoint over the output**: every serializable leaf of the `to_hash`
schema is written by exactly one op (an unwritten or doubly written
destination is a plan-validation error naming the path; `DropField`
documents an intentionally unmatched *input* path and writes nothing).
Nested op lists (`MapVariant`, `MigrateElements`, `MigrateMapKeys`,
`MigrateInline`) apply the same model recursively — input element in,
fresh output element out. After every edge, function edges included, the
output value is **validated against the edge's `to_hash` endpoint
schema** (embedded in the migration bundle, §6): a `MigrationFn` result
that does not conform to `to_hash` is a migration error naming the edge,
surfaced before tag extraction, validators, or artifact encoding can
consume the value; the final automatic diff validates against the
current schema the same way.
`MigrationFn`'s own deterministic `Err` uses its nonzero stable code and the
DSLF MigrationFunction arm; endpoint nonconformance remains the fixed
MigrationPlan `NonConformingOutput` code. Presentation messages never enter
either fingerprint.

The migration graph is constructed, not heuristic. Nodes are logical
hashes; edges are **custom migration bundles only** — automatic diffs are
never edges between arbitrary historical schemas. Plan selection is a
**greedy walk in which custom edges are mandatory**: starting at `from`,
every step first tests `node == target_current` (the entry's type's
current logical hash) **before querying outgoing edges**, and terminates
immediately on equality — the walk never consults or follows an edge out
of the current schema, so an old `A→B` edge can never transform data
already at current `A`, and a future `B→C` edge can never make an `A→B`
migration overshoot current `B` into a backward automatic diff. Only for
a non-current node are outgoing custom edges queried through the pinned
`ControlSnapshot`: if one exists,
follow it — a custom
edge was authored for data at exactly that schema and is never bypassed. Every
UUID returned by the query is control-read and decoded before the set is used;
all those reads remain in the attempted basis even when planning later reports
ambiguity, a cycle, malformed endpoints, or invalid ops, so an edit to any
examined edge heals the memoized failure. A stable read/decode failure
hard-stops at that node;
two outgoing custom edges from one node, or a revisited node, is an
ambiguity error. When a non-current node has no outgoing edge, apply
**exactly one** automatic diff from that node to current — legal only
when explicit manifest ancestry proves the node is **directionally behind**
current. For type lineage `L`, start at `L.current` and follow
`forward_parent` links; the source stamp's selected epoch must occur on that
chain and its epoch prefix/parent records must exactly match `L.epochs`.
This test is evaluated only through a `Ready` epoch whose compiled registry
hash is exactly `L.epochs[L.current].digest`; `SchemaAcceptanceRequired`
cannot create a `LoadContext` or run an automatic segment. Only then is the
trailing automatic diff authorized. A source selected after
current, on another accepted branch, absent from the manifest, or carrying an
unknown/empty/malformed stamp supplies no proof and hard-stops with a
staleness/divergence error. Vector order and opaque `DSSL` equality never
establish ancestry.

Acceptance and selection are separate operations over the source-controlled
`SchemaLineageManifest` (§6). Both commands carry the stale-base manifest
hash/current cursors and candidate epoch identity described there; the
coordinator compares and commits them in one transaction. The ordinary
explicit schema-accept command for
a genuinely new digest appends exactly one epoch with
`forward_parent = Some(old_current)` (the first accepted epoch alone uses
`None`) and advances `current` to the appended index. It records the accepted
epoch even if no bundle is written. An existing digest is **never** silently
appended or selected by module/schema staging. Deliberate rollback is a
separate explicit command that moves only `current`, leaving `epochs`
append-only, and is allowed only after complete validation of authored custom
reverse edges from the old current and every live stamped data or migration
endpoint schema that is not a forward ancestor of the requested cursor. A
missing, ambiguous, non-total, or invalid reverse path rejects the cursor move;
normal automatic diffs are never used backward. Even a valid reverse graph
cannot select a cursor whose digest differs from the named candidate registry
row; the command leaves the manifest untouched in that case.

Retirement and reactivation are two more explicit manifest commands, not
special cases of staging. Retirement succeeds only when the stale manifest
base/cursors and exact candidate dylib plus canonical target rows still match, the
candidate omits the type, and a control-snapshot scan proves no live authored
entry or live migration endpoint requires it. The request carries that scan's
exact metadata/control `SnapshotStamp`, and the coordinator revalidates the
negative proof inside the manifest CAS; it preserves history and marks the row
Retired only if the basis still matches. Every later scan/import/CRUD/manual
publication maintains the same negative invariant. A current entry or
Migration endpoint for a Retired type publishes typed
`RetiredTypeReferenced` authority poison rather than entering a Ready index.
Reactivation requires candidate inclusion and selects exactly that candidate
row's digest, reusing or appending history under the normal forward/rollback
rules; it may admit waiting bytes only atomically with the Active transition.
`Ready` separately validates DSLI-keyed bootstrap authority rows, then
compares the candidate's remaining set exactly with Active rows; Retired rows remain
verifiable authority but are excluded from that set.

Daemon startup requires the manifest before it derives the SQLite lineage
cache. Bundle stamps and embedded migration endpoints are verified against it
but are never unioned to invent accepted history; after state loss, absence of
the manifest is never forward proof even if all observed lists happen to be
prefix-comparable. **No automatic migration occurs without this explicit
manifest ancestry proof.** Without it, a
code rollback — or §5's deliberate staleness window, where an older
registry keeps serving — could automatically project newer data into
older semantics, backward semantic revisions included. A destructive
direct automatic diff can therefore never
shadow a data-preserving custom edge, and partial custom chains are always
honored. Plan selection records one canonical `TraceOp::Control` migration-
edge query dep (§10) per
node whose edges it queried — each such node's outgoing-edge set,
including the *empty* set of the non-current node that ended a custom
chain — so
authoring an edge anywhere on the walk invalidates into re-planning (or
the ambiguity error), while edges at unvisited nodes are correctly
irrelevant. The terminating current node records **no** edge dep: its
edges are never consulted, so authoring an edge out of the current
schema neither affects nor invalidates data already at current. Every queried
edge that is examined additionally records `TraceOp::ControlRead`, including
stable decode/validation failure; query and read traces are revalidated as one
attempted-basis prefix. Intermediate schemas come from the migration
bundles themselves — every edge carries its endpoints — so path chaining
depends only on assets in the tree. The daemon's schema archive is a pure
accelerator, rebuilt by scanning all bundles' `schemas` sections (migration
bundles included), never precious. The `Migration` type itself never
migrates through this machinery: its schema is pinned by the bundle
`format_version` and evolved by daemon code — the bootstrap is closed.

Ambiguity is an error, never a guess: duplicate custom edges for the same
`(type, from, to)` (e.g. authored on parallel branches and merged), a
branching or cyclic walk, a bundle whose `type_uuid` no longer exists in code, and
cross-type migrations all halt with diagnostics naming the conflicting
migration assets. Resolution is a human edit, not a tie-break heuristic.

### Execution: lazy, at load

Migration is a load-time concern, and there is exactly **one bundle-load
implementation** — `load_current(entry)`: deserialize with the embedded old
schema, apply the plan chain in memory (running function edges when the
chain contains them; the dylib hash is recorded whenever code ran), and
yield the value at the current schema. Every consumer shares it: build
import loads then encodes the artifact (§8); tag indexing loads then
extracts tag fields (§10); validators receive its result (§9); disk
migration loads then rewrites the file. Bundles on disk are never rewritten by
the build and may lag indefinitely — self-description makes a mixed-schema
tree fully consistent, every file individually interpretable. The applied
chain joins the input hash (§8).

```rust
/// THE bundle-load path. Deserialize with the entry's embedded schema, walk
/// the migration graph, apply the plan chain, yield the current-schema value
/// plus everything the run consumed. Everything it reads comes through ctx —
/// a pinned snapshot, never live state — so LoadInputs records exactly what
/// a re-run would consume.
pub fn load_current(entry: &AssetEntry, bundle: &Bundle, ctx: &LoadContext)
    -> Result<Loaded, LoadError>;

pub struct LoadContext<'a> {
    /// Pinned metadata snapshot (§13): ordinary data reads resolve here.
    pub snapshot: &'a MetadataSnapshot,
    /// Coordinator-private authority over the same snapshot basis. Plan
    /// selection's authoring-only Migration queries resolve only here and
    /// land in LoadInputs as TraceOp::Control; pipeline code cannot obtain it.
    control: ControlSnapshot<'a>,
    /// The validated-together code epoch (§3), pinned by the snapshot
    /// itself (§13). Private, set only from snapshot.epoch() — fallible
    /// under pipeline poison (§13), so a LoadContext cannot exist at a
    /// poisoned version — a fresh
    /// schema registry can never pair with a stale module's migration
    /// functions or default tables, nor either with a foreign snapshot.
    epoch: &'a PipelineEpoch,
}

impl<'a> LoadContext<'a> {
    pub fn schemas(&self) -> &SchemaRegistry;  // the epoch's registry + archive (§5)
    /// Migration functions and default tables; the dylib hash lands in
    /// LoadInputs whenever either runs.
    pub fn module(&self) -> &ModuleHandle;
    pub fn planner_version(&self) -> u32;
}

pub struct Loaded {
    pub value: AuthoredValue,          // at the current logical schema
    pub defaulted: Vec<FieldPath>,     // op provenance — diagnostics; tags index the loaded value (§10)
    pub inputs: LoadInputs,            // consumed inputs, for input hashes and tag indexes
}

pub struct LoadInputs {
    /// The sole dependency authority: every Read, Control query, ControlRead,
    /// and Capability lookup in exact observation order across planning,
    /// decode, migration, and defaulting. Applied migration bundle identities
    /// are represented by their ControlRead subjects/outcomes; module uses by
    /// Capability outcomes. Derived typed views may filter this vector for
    /// diagnostics but are never persisted, hashed, or revalidated as
    /// independent authorities. On failure, the terminal Observed::Err op is
    /// the final element and no operation follows it.
    pub trace: Vec<TraceOp>,
    pub planner_version: u32,
    pub dylib_hash: Option<[u8; 32]>,  // present iff pipeline code ran
}
```

Appending to `LoadInputs.trace` happens at observation time, before decoding
or invoking code. DSTR hashes this one vector directly. Implementations MUST
reject a serialized `LoadInputs` that also supplies legacy split migration,
plan-selection, control-read, or capability vectors, whose interleaving could
not be reconstructed; convenience accessors are projections over `trace` only.

Disk migration exists only as an explicit authoring command
(`distill migrate`), run as a long-running operation (§17): useful for
retiring very old schemas or tidying diffs, never required for correctness.
It rewrites per bundle independently — a failure partway leaves a valid
mixed-schema tree, reported per bundle, with no rollback needed.

### Integrity, not degradation

Data is never read against any schema other than the one whose hash it
declares — there is no inference fallback. Consequences:

- An entry whose `schema_hash` has no matching snapshot fails schema-closure
  (§6): an integrity error, the same class as malformed JSON. The file stays
  intact and uninterpreted; nothing is ever lossily re-read.
- Because the hash pins the schema's content, a missing snapshot is
  restorable from **any** holder of the same hash — another bundle, a
  migration bundle's endpoints, the archive cache, git history — and the
  restoration provably cannot guess wrong. It is an ordinary authoring
  write of the snapshot into the bundle, made when the user asks;
  `doctor` only reports the missing snapshot.
- Hand-authored files without machine fields are not an integrity failure:
  the author is by definition writing against the current schema, so
  adoption (§6) **validates** against it and stamps hash + snapshot.
  Validation errors surface to the author; nothing silently drops.
- Only if a hash is itself mangled, or a schema exists in no reachable
  holder, does an entry stay unreadable — reported with exactly what is
  missing, preserved rather than interpreted.

## 12. Binary Artifact Format

Artifacts are memory-layout encoded. The schema system *guarantees* exact
layout knowledge (offsets, sizes, alignment, niche/tag encodings) per target
— the same guarantee newgameplus's reflection, GC, and hot-reload machinery
already stand on. Layout encoding does **not** mean casting bytes to Rust
values: flat data is copied; every indirection is constructed.

Layout, pinned (magic `89 44 53 54 4C 0D 1A 0A`, `"\x89DSTL\r\x1a\n"`):

```
magic         [u8; 8]
version       u32 LE      artifact format version (a input-hash input)
asset_uuid    [u8; 16]
authored_type [u8; 16]
terminal_type [u8; 16]
encoded_type  [u8; 16]
logical_hash  [u8; 32]
layout_hash   [u8; 32]
dep_count     u32 LE
blob_count    u32 LE
fixed_len     u32 LE
var_len       u64 LE
load_deps     [u8; 16] × dep_count
blob_table    (offset u64, len u64) × blob_count — into the blob section
pad           zeros to 16-byte alignment
fixed         [u8; fixed_len]
pad           zeros to 16-byte alignment
variable      [u8; var_len]
pad           zeros to 16-byte alignment
blobs         blob section; each blob 16-byte aligned
```

Total size must match the header exactly; all arithmetic is checked;
padding must be zero. `var_len` must be `< 2^32` (`VarRef` offsets are
`u32`); a larger variable section is a build error — bulk data belongs in
`#[asset(blob)]` fields, and the cap is revisable with a format-version
bump. Unordered source data is canonicalized: `load_deps` are sorted
lexicographically and deduplicated — bare UUIDs by design; the expected
terminal type of every emitted reference lives in the producing job's
trace as a revalidated `RefCheck` entry (§9) — and map entries flatten into the
variable section sorted by canonical encoded key bytes (blob-free by
§5's serializability rule) — a `HashMap`'s
iteration order never reaches the artifact, so identical values produce
identical bytes and one ContentHash (the collapse §9's early cutoff rests on).
The encoder's input for a processor output is the type's generated
encode visitor's event stream (§4's `encode` entry): flat bytes,
container events already in canonical §5 order, blob handoffs, and
resolved reference emissions — never a typed value read through raw
layout; build imports encode from `AuthoredValue`, whose canonical form
carries the same ordering. Either way the encoder consumes an already
canonically ordered vocabulary, so no native iteration order exists for
it to leak.
`encoded_type` names the value actually in the file: a mid-chain
artifact encodes an intermediate type (`B` in `A → B → C`) that neither
the authored nor the terminal UUID identifies — and logical/layout
hashes cannot substitute, since same-shaped types with different
TypeUuids deliberately share them — so fixup plan selection and cache
validation key on `encoded_type`; for terminal artifacts it equals
`terminal_type`. For a **derived extra output** (§9) the triple is
pinned too: the child has no authored bundle entry, so `authored_type =
terminal_type = encoded_type =` the declared extra output type. Extras
are encoded directly and remain terminal even when their declared type
is a registered processor input; only primaries traverse processor
chains — one rule, so cache-hit validation and loader header checks
cannot be implemented two ways. The
child's provenance (parent, output key) lives in the derived-output
index (§13), never in the header.
No CRCs: an artifact's identity **is** its blake3
(the ContentHash), and every fetch verifies the payload against the ContentHash it asked
for. `ContentHash` is the named §5 byte-identity exception — raw blake3
over the complete DSTL bytes, deliberately domainless. There is deliberately **no target field**: the layout hash is the only
target-dependent property of an encoded artifact, so layout-identical
targets share artifacts and cache entries (§8); which target an artifact
was built for is manifest metadata, not artifact content.

The blob section is the pack split point (§16): structural bytes (header
through the variable section's trailing pad, so the blob section begins
where they end) and each blob are separable, and the ContentHash is blake3 over
the whole raw file — a pack loader verifies without materializing it by
hashing the structural buffer, then each gap and extent in blob-table
order, synthesizing the zero padding between extents from the table's own
offsets. Table offsets must be ascending, 16-aligned, non-overlapping,
and gaps only ever alignment-sized; anything else — or a nonzero stored
gap — is an integrity error. Blob-table entry order is the **canonical
structural path** key the bundle format pins (§6): tagged, length-framed
`field`/`variant`/`index`/`mapkey` components from the artifact root,
entries sorted by the path's encoded bytes and `BlobRef.index` assigned
in that order — a leaf-name key would alias two blobs under one name in
different nested structs, variants, or elements, letting two writers
emit different bytes (and ContentHashes) for one value. `fetch` returns the split form: structural
storage plus one backing per blob-table entry — ranges of a single buffer
over RpcIO; a buffer plus mmap extents from a pack.

- **Fixed section** — a byte image of the struct per the target's **wire
  layout**: derived deterministically from the native layout by the layout
  schema, identical except where indirection or niche encoding forces
  divergence. Pointer-containing fields (`Vec`, `String`, maps, sets,
  `Box`, `Arc`) hold `VarRef { offset: u32, len: u32 }` — 8 bytes, zeroing
  any remaining slot bytes; `offset` is relative to the variable-section
  start; `len` is the element count (`Vec`, maps, sets), byte count
  (`String`), or flattened byte size (`Box`, `Arc`), validated against the
  layout schema. `#[asset(blob)]` slots hold `BlobRef { index: u32,
  zero: u32 }` into the blob table. Enums with integer discriminants keep them in place; **an
  enum whose native discriminant is niche-encoded gets an explicit `u32`
  wire tag instead** — wire bytes are never interpreted through native
  niches, because a `VarRef` bit pattern can collide with a niche's
  reserved value (`Some(Box::new(()))` is `VarRef{0,0}`, all zeros — the
  exact bytes the null-pointer niche calls `None`). Where wire tags change
  sizes, the wire layout recomputes offsets, and the **layout hash hashes
  the wire layout**, so every divergence-affecting native change
  propagates. `#[asset(skip)]` slots are not encoded.
- **Variable section** — element data, recursively flattened, elements
  aligned per the layout schema.
- **Fixup plan** — compiled once per (type, wire layout hash, **native
  layout identity**): the wire side comes from the artifact's layout
  hash, the native destinations (offsets, skip-field placement, tag
  writes) from the consumer's own layout table, and the plan cache is
  keyed on both — DSWL deliberately hashes only the wire tree, so a consumer
  relayout that leaves wire bytes untouched (an enlarged `#[asset(skip)]`
  field shifting later native offsets; a niche flipping under a rebuilt
  module) must drop cached plans by the native key: the consumer's
  **fixup-table identity** (§5) — the measured layout digest extended
  with the binary's generated-table assignment, every plan-interpreted
  index (`CtorId`, `DropId`, `SkipDefaultId`) paired with the nominal
  monomorphized key of what it constructs, drops, or writes. Plans name
  slots in those tables, so a rebuild that reorders them without moving
  any layout byte, or swaps two DSWL-identical types' entries, changes
  the native key and the stale plans are simply never looked up again —
  the binary-local key for the binary-local fact (cross-binary
  comparisons use the layout-only digest, §5). The plan is
  an ordered op list over a `MaybeUninit` destination, beginning
  with flat-copy ops for the maximal runs where wire and native offsets
  coincide (whole-section memcpy is invalid once any wire tag shifts an
  enclosing struct's offsets — flat data scatter-copies to native offsets),
  then `ptr::write`s a properly constructed owned value over every
  pointer-shaped slot; every op carries its (wire source range, native
  destination offset), and nothing reads the destination before writing
  it. Construction is typed, never offset-blind: each construct op names
  a `CtorId` into the consuming binary's generated constructor table
  (declared below), whose drop entries are also what the rollback stack
  pushes. Ops: `FlatCopy`; `ConstructVec`; `ConstructString` (UTF-8 validated);
  `ConstructMap` (entries flattened as key/value pairs in the variable
  section, built by real insertion through the ctor entry — the concrete
  map type and its BuildHasher (deterministic by §5's serializability
  rule) are the consuming binary's; a
  duplicate key is an integrity error); `ConstructSet` (elements in the
  variable section, built by real insertion — a duplicate element is an
  integrity error, mirroring the map rule); `ConstructBox`; `ConstructArc`;
  `ConstructBlob` (resolves its `BlobRef` through the blob table to the
  blob's backing — a stored pack extent (§16) or a range of the fetched
  buffer — an `Arc`-backed borrow, never a copy); `WriteSkipDefault` for `#[asset(skip)]`
  slots, from the type's #[asset]-generated skip-writer table — skipped
  fields have no schema presence, so their defaults come from the
  asset-types crate, with `Default` on the field type checked at macro
  expansion (distinct from the migration ops of §11); `ValidateScalar`
  (every `bool` and `char` inside a flat-copied range is checked for a
  valid bit pattern before the destination is treated as initialized — an
  invalid pattern is an integrity error, the same class as a bad
  discriminant, and unchecked it is UB, not just bad data);
  `SwitchVariant` (read the explicit wire tag — or the in-place integer
  discriminant — select that variant's sub-plan, and write the native value
  with its proper encoding: niches are *written*, never read; an invalid
  tag is an integrity error). Plan shape is pinned: every non-single-
  variant enum compiles to an enum-rooted plan whose sole op is
  `SwitchVariant`; its variant sub-plans are enum-relative and have
  `whole_drop: None`, and only after the selected payload completes and
  the native tag is written does the enum plan push the enum's whole-drop
  entry. Single-variant enums need no switch. Then recurse-into-element for nested structs, arrays,
  and container elements (ZSTs are no-ops). `Arc` aliasing is not preserved
  (each reference constructs fresh). Failure mid-fixup unwinds a
  **framed constructed-value stack**: beginning an aggregate (struct,
  enum payload, container) opens a frame; every completed construction
  pushes (pointer, its generated no-unwind drop entry — the ctor table's
  for containers, the skip entry's paired drop for `WriteSkipDefault`,
  per-type drop glue otherwise, mirroring `ngp-drops`); **completing an
  aggregate disarms its frame's entries and pushes exactly one entry for
  the whole value** — its full drop glue (fields plus any custom
  `Drop`), named by the plan's `whole_drop` id into the generated drop
  table (declared below) — so ownership transfers into the enclosing value exactly
  once: keeping the child entries armed would double-drop through the
  aggregate's glue, while dropping only children would skip a custom
  `Drop`. Failure pops and drops in reverse — correct under
  arbitrary nesting, where a linear prefix is not (a partially filled
  map is aborted through its cursor as the partial state it is). Validation before and during
  construction covers everything an op names: `VarRef` bounds and
  alignment with checked arithmetic, exact Box/Arc pointee lengths, the
  blob pad word, UTF-8, discriminants, scalar bit patterns
  (`ValidateScalar`), and configurable allocation-count and recursion-
  depth caps. The cap carrier is an `ExecLimits`-style parameter; defaults
  are `max_depth = 128` and `max_allocations = 2^32`. The checks are
  normative, while those defaults are deployment policy. Slot padding and
  structural gaps are not described by fixup ops and therefore are not an
  executor obligation: the reader that holds and validates the DSWL wire
  tree verifies them as zero before execution. The
  executor is **new work patterned on** newgameplus's offset-directed
  default/drop tables (`ngp-defaults`, `ngp-drops`) — those tables exist;
  the transactional constructor does not yet. "Zero-copy" means exactly one
  thing here: memcpy of the maximal runs where wire and native layouts
  coincide — the plan constructs diverging regions field-wise.

```rust
pub struct FixupPlanArena {     // keyed by (type, wire layout hash, fixup-table identity §5)
    pub plans: Vec<FixupPlan>,  // index 0 = root; PlanId indexes the arena, and
                                // an op may reference any plan, ancestors
                                // included — recursive types close cycles
                                // through indirection ops, finite because
                                // element counts and presence are data
}
pub struct FixupPlan {
    pub ops: Vec<FixupOp>,
    /// The framed-rollback entry pushed when this plan's aggregate
    /// completes — the whole value's drop glue (fields plus any custom
    /// Drop), from the generated drop table below. None ⇔ the type has
    /// no drop glue: nothing is pushed, the frame just disarms.
    pub whole_drop: Option<DropId>,
}
pub struct PlanId(pub u32);

/// Whole-value drop glue for plan-constructed aggregates (structs, enum
/// payloads): #[asset] generates one no-unwind entry (§3's thunk rule —
/// a panicking Drop leaks and reports) per aggregate node in the
/// consuming binary, keyed by the same fixup-table identity as the
/// plans. Containers roll back through their CtorEntry's drop_in_place
/// and skip slots through their SkipEntry's; this table covers what
/// neither can name — a completed nested aggregate with drop glue of
/// its own, which a later failure (UTF-8, ValidateScalar) must unwind
/// as one value. The plan compiler never guesses entries positionally:
/// every aggregate node in the native layout tree carries its own
/// `whole_drop: Option<DropId>` annotation, and `ConstructString`/
/// `ConstructBlob` values roll back through the executor's **built-in**
/// drop for the value it itself constructed (it built the String/blob
/// vec, it can free them — normative, not table-supplied).
pub struct DropId(pub u32);
pub struct DropTable {
    pub entries: &'static [unsafe fn(ptr: *mut u8) -> Result<(), CallbackPanic>],
}

/// Every primitive leaf the native tree can hold — the tree must describe
/// the whole value, not just the validated corners. `Bool` and `Char` are
/// the restricted-bit-pattern kinds (`ValidateScalar` applies to exactly
/// those); the rest are unrestricted and flat-copy freely.
pub enum ScalarKind { Bool, Char, U8, U16, U32, U64, U128,
                      I8, I16, I32, I64, I128, F32, F64 }

/// Index into the consuming binary's #[asset]-generated constructor
/// table: per monomorphized container instantiation, the typed alloc/
/// insert/finish/drop entry points fixup needs — a HashMap's
/// representation and BuildHasher, an Arc's allocation layout, are that
/// binary's private business, so ops never construct from offsets alone.
/// Drop entries are what the rollback stack pushes. The table is keyed
/// by the same fixup-table identity as the plans that index into it.
pub struct CtorId(pub u32);

/// The generated table's entry shape — §3's thunk rule applies to every
/// fn here: a panic is caught and returned (or, for drop, leaked and
/// reported), never unwound.
pub struct CtorTable { pub entries: &'static [CtorEntry] }  // indexed by CtorId

/// push's disposition: Duplicate is a *data* verdict (malformed artifact —
/// a set/map element already present), Panic a *callback* failure. The
/// executor maps Duplicate to the §12 integrity error naming the
/// container; both consume the element either way.
pub enum PushError { Duplicate, Panic(CallbackPanic) }

pub struct CtorEntry {
    /// Begin a container of len elements/entries; dst is the MaybeUninit
    /// native slot, untouched on Err. Scalars/Box/Arc use len = 1.
    pub begin: unsafe fn(dst: *mut u8, len: u32) -> Result<CtorCursor, CallbackPanic>,
    /// CONSUMES the element slot on entry, Ok or Err: a panicking user
    /// Hash/Eq mid-insertion may already have dropped or captured the
    /// moved value, which no thunk can restore — so the caller never
    /// rolls elem back after push; the cursor owns all partial state.
    /// For maps, `elem` points to a (K, V) temp the executor constructed
    /// at `key_offset`/`value_offset` (below); both move on entry. A real
    /// insertion reports duplication: `Duplicate` is the artifact
    /// integrity error §12 promises for sets and maps — distinct from
    /// `Panic`, which is callback failure, never data judgement.
    pub push: unsafe fn(cur: &mut CtorCursor, elem: *mut u8) -> Result<(), PushError>,
    /// Element temp layout the executor must provide to `push`: the
    /// element type for vec/set (key_offset = 0, value_offset unused),
    /// the monomorphized (K, V) pair for maps — this binary's layout of
    /// that pair, which no plan could otherwise know.
    pub elem_size: u32,
    pub elem_align: u32,
    pub key_offset: u32,
    pub value_offset: u32,
    /// Complete the container into dst. On Ok the cursor is spent (abort
    /// becomes a no-op); on Err dst is untouched and the cursor still
    /// owns the partial value — abort it. &mut, not by value: a caller
    /// cannot be asked to abort a cursor it no longer has.
    pub finish: unsafe fn(cur: &mut CtorCursor, dst: *mut u8) -> Result<(), CallbackPanic>,
    /// No-unwind disposal of a partial container (begin without finish,
    /// or failed finish); panicking element Drops leak-and-report (§3).
    pub abort: unsafe fn(cur: CtorCursor) -> Result<(), CallbackPanic>,
    /// Rollback drop for a completed value at ptr — never unwinds: a
    /// panicking Drop leaks the value and reports (§3).
    pub drop_in_place: unsafe fn(ptr: *mut u8) -> Result<(), CallbackPanic>,
}

/// The skip-writer table's entry (SkipDefaultId): write is paired with
/// its drop so a skip default constructed before a later failure can be
/// rolled back — the skipped field's type is schema-invisible, so no
/// other table could supply it. Both no-unwind (§3); Err leaves the
/// slot untouched.
pub struct SkipEntry {
    pub write: unsafe fn(dst: *mut u8) -> Result<(), CallbackPanic>,
    pub drop_in_place: unsafe fn(ptr: *mut u8) -> Result<(), CallbackPanic>,
}

pub struct SkipWriterTable { pub entries: &'static [SkipEntry] }  // indexed by SkipDefaultId

/// Fixup execution receives the descriptor's owning ModuleEpochToken
/// (§4). Every Err from DropTable, CtorEntry::abort/drop_in_place, or
/// SkipEntry::drop_in_place is propagated as callback failure and
/// atomically poisons that token; rollback never converts it to success.
/// The value/cursor is consumed on either status and is leaked on Err.

/// The native layout tree, generated as statics by #[asset] in every consuming
/// binary: the binary-local input to fixup-plan compilation — destination
/// offsets, tag writes, skip-slot positions, and generated table ids. Offsets
/// are frame-relative, as in the wire tree; skip slots appear with their
/// writers (they are schema-invisible but plan-visible). Generated statics
/// cannot hold heap collections, so every aggregate is a `&'static` slice.
pub enum NativeLayoutNode {
    Scalar  { offset: u32, size: u32, align: u32, kind: ScalarKind },
    /// `fields` in **physical order**: sorted by (offset ascending,
    /// declaration index ascending) — deterministic under field
    /// reordering and under equal-offset ZSTs, where "native order"
    /// alone names nothing. `whole_drop` is the completed aggregate's
    /// rollback entry (None ⇔ no drop glue) — the plan compiler reads
    /// it from here; no positional guessing against the drop table.
    Struct  { offset: u32, size: u32, align: u32,
              whole_drop: Option<DropId>,
              fields: &'static [NativeField] },   // skip slots included
    /// `variants` are self-contained records (declared below): name,
    /// declaration index, payload node, and that variant's own tag info
    /// in one place — a raw discriminant associates with its variant
    /// through the record's fields, never positionally against a
    /// separate declaration-order value list, and wire tables re-sort
    /// the records by name (§5's order) without ambiguity.
    Enum    { offset: u32, size: u32, align: u32, tag: NativeTagEncoding,
              whole_drop: Option<DropId>,
              variants: &'static [NativeVariant] },
    Array   { offset: u32, size: u32, align: u32, len: u32, stride: u32,
              elem: &'static NativeLayoutNode },
    Vec     { offset: u32, size: u32, align: u32,
              elem: &'static NativeLayoutNode, ctor: CtorId },
    Set     { offset: u32, size: u32, align: u32,
              elem: &'static NativeLayoutNode, ctor: CtorId },
    Map     { offset: u32, size: u32, align: u32,
              key: &'static NativeLayoutNode,
              value: &'static NativeLayoutNode, ctor: CtorId },
    BoxPtr  { offset: u32, size: u32, align: u32,
              inner: &'static NativeLayoutNode, ctor: CtorId },
    ArcPtr  { offset: u32, size: u32, align: u32,
              inner: &'static NativeLayoutNode, ctor: CtorId },
    Str     { offset: u32, size: u32, align: u32 },  // ConstructString/rollback built in
    Blob    { offset: u32, size: u32, align: u32 },  // the §4 Blob native form
    /// Skipped slots are schema-invisible but plan-visible: alignment is
    /// measured like every slot's; skipped-slot geometry is never inferred.
    Skip    { offset: u32, size: u32, align: u32, writer: SkipDefaultId },
    /// §5-style frame distance plus this slot's own frame-relative
    /// origin; size and align are the referenced frame's, by rule —
    /// stated as inferred, never re-stated (they could only disagree).
    BackRef { distance: u32, offset: u32 },
    /// Size 0, align 1 — stated normatively, so the node carries a
    /// well-defined position like every other node.
    Unit    { offset: u32 },
}

pub struct NativeField {
    pub name: &'static str,                       // decimal for tuples (§5)
    /// The source declaration index: the native/DSWL physical order is
    /// (offset ascending, declaration index ascending), and after wire
    /// repacking this is the only tie-break for equal offsets — native
    /// slice order alone cannot supply it.
    pub declaration_index: u32,
    pub node: NativeLayoutNode,
}

/// One enum variant, self-contained: the name, the source declaration
/// index (the order rustc states raw discriminants in), the payload
/// node, and this variant's own tag info. Discriminant ↔ name
/// association is by construction — the value sits in the same record
/// as the name it belongs to — so no implementation can pair a
/// declaration-order value list against a differently ordered name
/// list; wire tables derive their name-sorted order (§5) by re-sorting
/// these records.
pub struct NativeVariant {
    pub name: &'static str,
    pub declaration_index: u32,
    pub node: &'static NativeLayoutNode,
    pub tag: NativeVariantTag,
}

/// Per-variant tag info, carried in the variant record (never a
/// parallel array). Values are raw bits per NativeTagEncoding's rule.
pub enum NativeVariantTag {
    Direct   { value: u128 },   // raw bits at tag width, zero-extended
    Niche    { index: u32 },    // discriminant = niche_start + index, at tag width
    Untagged,                   // the niche encoding's untagged variant
    Single,                     // single-variant enums: nothing to read or write
}

/// Const-constructible mirror of the §5 layout record's tag encoding —
/// generated statics cannot hold the model type's `Vec`s. The encoding
/// carries the tag's geometry only; per-variant values live in the
/// `NativeVariant` records, where they cannot detach from their names.
///
/// **Discriminants are raw bits, everywhere**: the native discriminant
/// truncated to the tag's byte width (two's complement for signed reprs),
/// zero-extended to `u128`. Comparison and writing operate on raw bits at
/// tag width, so signedness never enters and `repr(u128)`/`repr(i128)`
/// are fully representable — an `i128`-valued carrier would lose the top
/// half of the `u128` discriminant space.
pub enum NativeTagEncoding {
    Direct { offset: u32, size: u8 },
    Niche  { offset: u32, size: u8, niche_start: u128 },
    Single,
}

pub enum FixupOp {
    FlatCopy        { wire: Range<u32>, native: u32 },
    ConstructVec    { wire_slot: u32, native: u32, elem: PlanId, ctor: CtorId },
    ConstructString { wire_slot: u32, native: u32 },
    ConstructMap    { wire_slot: u32, native: u32, key: PlanId, value: PlanId, ctor: CtorId },
    ConstructSet    { wire_slot: u32, native: u32, elem: PlanId, ctor: CtorId },
    ConstructBox    { wire_slot: u32, native: u32, inner: PlanId, ctor: CtorId },
    ConstructArc    { wire_slot: u32, native: u32, inner: PlanId, ctor: CtorId },
    ConstructBlob   { wire_slot: u32, native: u32 },   // BlobRef → blob table
    WriteSkipDefault{ native: u32, writer: SkipDefaultId },
    ValidateScalar  { wire: u32, kind: ScalarKind },   // bool/char bit patterns
    SwitchVariant   { wire_tag: WireTagRead, native: u32,
                      variants: Vec<(NativeTagWrite, PlanId)> },  // name-sorted order (§5)
    Recurse         { wire: u32, native: u32, plan: PlanId },
}

/// How the wire tag is READ — from the *wire* layout tree, a separate
/// axis from NativeTagWrite (the destination encoding): a wire produced
/// under a fully-flat encoding may fix into a native niche and vice
/// versa. Wire enums have exactly three forms (wire rules above):
/// single-variant carries no tag (no SwitchVariant op is emitted at
/// all), fully-flat carries the producing layout's in-place integer
/// discriminant, everything else the canonical u32 name-sorted index. A
/// read value matching no variant is an integrity error (§12).
pub enum WireTagRead {
    CanonicalU32 { offset: u32 },      // name-sorted variant index (§5)
    Direct { offset: u32, size: u8,
             values: Vec<u128> },      // the wire's discriminant per variant,
                                       // name-sorted order (§5); raw bits at
                                       // tag width zero-extended — matching is
                                       // raw-bit equality, signedness never enters
}

/// How a variant's native discriminant is written — from the layout
/// schema's tag encoding, never read back from wire bytes. Values are
/// raw bits at tag width (see NativeTagEncoding).
pub enum NativeTagWrite {
    Direct { offset: u32, size: u8, value: u128 },
    Niche  { offset: u32, size: u8, value: u128 },  // the niche's reserved value
    PayloadImplied,                                 // the niche lives inside payload
                                                    // bytes: writing the payload writes
                                                    // the tag (Option<Box<T>>'s Some)
    None,                                           // single-variant: nothing to write
}

/// Index into the enclosing type's skip-writer table, generated by
/// #[asset] in the asset-types crate (fixup runs game-side, §15): one
/// native default constructor per #[asset(skip)] field. Skipped fields
/// have no schema presence and no asset TypeUuid, so they key
/// positionally per type; a skipped field whose type lacks Default is a
/// compile error at #[asset] expansion.
pub struct SkipDefaultId(pub u32);
```

### Local native-layout contract

`NativeLayoutNode`, constructor/drop tables, and skip writers are generated
inside each consuming binary and are intentionally binary-local. They have no
stable cross-binary serialization or digest. Registration validates their
internal indexes, geometry, and callback ownership; fixup-plan compilation
matches an authenticated DSWL wire tree directly against the current
descriptor. Plans and every referenced thunk are scoped to that descriptor's
resident game-module epoch and are discarded during drain.

### Wire layout, precisely

Derived recursively from the native layout; these rules are normative:

- Primitives and flat structs/arrays of them: wire = native bytes (LE),
  identical offsets — the flat-copy case.
- Indirection slots (`Vec`, `String`, maps, sets, `Box`, `Arc`): wire size
  and alignment equal native; the first 8 bytes are the `VarRef`, the rest
  zero. For `Box` and `Arc`, `VarRef.len` must equal the pointee's wire
  size exactly; any other length is an integrity error, never ignored
  trailing data or a short read.
- Sequence storage (arrays and the flattened elements of vec/set
  indirections) uses `element_stride = align_up(element_wire_size,
  element_wire_align)` exactly. Map entries use one pinned pair layout:
  key offset `0`; value offset `align_up(key_wire_size,
  value_wire_align)`; pair alignment `max(key_wire_align,
  value_wire_align)`; pair stride `align_up(value_offset +
  value_wire_size, pair_alignment)`. The variable section advances by
  those strides, including their zero padding.
- Enums, three cases. **Single-variant**: wire = the sole variant's
  payload, no tag. **Fully flat** (integer discriminant and every variant
  payload wire == native): wire = native byte image, tag bytes in place.
  **Everything else** — niche-encoded, or any variant payload diverging in
  size *or alignment* — takes the canonical tagged form: `tag: u32` at
  wire offset 0 holding the variant's index in name-sorted order (the
  order §5 hashes); payload union at `align_up(4, max variant wire
  align)`; wire alignment = `max(4, max variant wire align)`; wire size =
  payload offset + max variant wire size, rounded up to alignment.
- Structs (and payload unions) containing any size- **or alignment-**
  diverging member: field order preserved (the native layout's field
  order), offsets recomputed by the same align-and-pack algorithm over
  wire sizes and alignments.
- **All wire padding is zero** — struct gaps, union tails, slot
  remainders — and the wire-tree-holding reader verifies it before fixup;
  the executor validates only bytes its ops name, as pinned above.
  Flat-copy runs cover initialized
  value bytes only: encoders write field-by-field and zero the gaps,
  never memcpy'ing native structs whose padding is undefined.
- **u32 bounds are enforced where u32 grammars begin.** Every
  fixed-layout size, offset, stride, array length, and plan index must
  fit `u32` — the fixed section is capped at 4 GiB exactly as the
  variable section is by `VarRef`'s u32 offsets. Source-walk extraction
  and `#[asset]` generation **reject** a type exceeding a bound with a
  typed error naming the type — never truncate, never wrap: a wrapped
  offset is a memory-safety bug, not a data bug. The model's `u64`
  fields (§5's `TypeLayout`) deliberately stay `u64` — the schema model
  describes what Rust can express; the bound is enforced at the
  boundary where the u32 grammars (`NativeLayoutNode`, wire trees,
  fixup plans, the DSTL header) begin.
- The **layout hash** is blake3 over the DSWL serialization of the wire
  tree: `"DSWL" ‖ version:u8 ‖ root`, nodes depth-first in **physical
  field order** — (wire offset ascending, then the field's own
  `declaration_index`, §12's `NativeField`), the
  same total order the native tree pins (equal-offset ZSTs and
  equal-offset fields after wire repacking tie-break by that recorded
  index; "native order" alone names nothing deterministic
  under reordering, and repacked wire offsets no longer follow native
  slice order at all) — integers LE; each node is `kind:u8, wire_offset:u32,
  wire_size:u32, wire_align:u32` plus kind payload. **Node payloads carry
  semantic identity, not just geometry** — two same-shaped layouts whose
  names or native encodings differ must never hash equal, or a cached
  artifact could be flat-copied into a reordered struct with its fields
  silently swapped. Payloads: primitive: id `u8`; indirection slot: slot
  kind `u8` **plus the pointee's node** (`vec`/`set`/`box`/`arc`: the element
  node; `map`: key node then value node; `string`/`blob`: none — the set
  element node is mandatory for exactly the reason vec's is: a set-element
  relayout under an unchanged logical hash must change the layout hash, or
  a stale artifact would fix up through the new element layout with fields
  silently swapped), offsets
  frame-relative to the element — a relayout anywhere beneath an
  indirection (a `Vec` element struct changing stride) changes the hash
  even though the slot itself stays 8 opaque bytes; struct: field count
  `u32` + per-field (`name:str`, node) in
  physical order (as above); enum: tag-form `u8`, then for the fully-flat form the
  native tag offset `u32`, tag size `u8`, **variant count `u32`**, and per-variant (`name:str`,
  native discriminant raw bits `u128` LE — tag-width bits zero-extended,
  as everywhere, node), and for the other forms
  **variant count `u32`** followed by per-variant (`name:str`, node) in
  name-sorted order (`str` as in §5). DSWL always carries the count; without
  it the enum grammar is not injective.
  Recursion terminates exactly as §5's grammar does: re-entering a type
  on the current expansion path emits a back-reference node (its own
  kind tag) carrying the frame distance instead of recursing — recursive
  asset types hash finite and position-independent.
  Reordering two same-typed fields or reassigning explicit discriminants
  therefore changes the layout hash even when every size and offset stays
  put. source-walk
  emits native layouts today, and its `TagEncoding` records sizes but not
  direct discriminant values, tag offsets, or niche encodings — extending
  it to carry them is part of the §19 model changes; the wire tree then
  derives deterministically in the encoder, no new extraction.

Hashes in the header:

- `logical_hash` — the schema the artifact was encoded at, always current at
  build time (migration runs inside the build, §11); a loader mismatch
  therefore means client and daemon disagree on the schema registry — an
  error, not a migration trigger.
- `layout_hash` — per (target, toolchain) wire-layout identity. **A hash
  is not the tree**: plan compilation needs the producing wire tree
  itself, so the daemon persists every wire tree it encodes under its
  DSWL hash (§13), serves it over RPC (§17), and packs embed a
  wire-tree table (§16). A consumer recomputes the DSWL hash of any
  tree it receives and requires it to equal the artifact header's
  `layout_hash` before compiling a plan — trees authenticate by
  recomputation, never by trust. Artifacts
  are encoded against the layout schema of the target they are built
  **for**, never the daemon host's; cross-target cooking requires
  source-walk to emit layout tables per configured target. Until that
  lands (§22), the enforceable rule is strict: the module's attested
  identity must match the schema record (the host check, §5), and a target
  is buildable only if its bound identity matches the identity of the
  layout table encoding it — today the host's, so non-host targets are
  refused rather than assumed layout-equal (§5's split enforcement).

## 13. Daemon State & Storage

All daemon state is disposable (§2) and lives under `.distill/` (gitignored).

### SQLite (metadata layer)

| Table | Contents |
|-------|----------|
| `files` | **(root id, normalized root-relative path)** → mtime, size, kind, content hash — last-known tree state. Physical tracking is per root: multiple roots form one *logical* namespace (§18), and a single-path key could hold only one of two same-path observations, silently choosing a root. The logical path index derives as a multimap with three states — `Missing`, `Unique(root)`, `Ambiguous(roots)` — and ambiguity is representable, not pre-collapsed |
| `file_work` | the watcher work no pass has consumed, in one order: changed paths (root id, path, exists/deleted, observation) and renames. A transaction queues its work in memory and writes, at its commit, only what no pass consumed in it; a pass acknowledges exactly the rows it captured, by sequence range |
| `import_keys` | the import index, one row per (bundle, asset, kind, key) indexed by (kind, key): what each watched import's basis reads (a path, a listing, an importer capability) under its `$record` entry, and each directory-import rules asset by the directory of its listing's literal prefix, so a changed path finds the rules that may list it by its ancestor directories. A row's source is its bundle's row, and its rows go with that row (a foreign key); the basis itself is read when needed, from the failure memo or the `$record`. Derived from the committed bundles and kept by source: every bundle publication queues its paths in `file_work`, and an import pass reindexes the dirty bundle sources, parsing each once, before it acknowledges that work, so the index is current but for pending work. It is never rebuilt whole; a store starts with no bundles and an empty index |
| `directories` | **(root id, path)** → the directory's canonical path, unique across roots (`directories_by_canonical`): two observed directories with one canonical path are an inconsistent table, not an alias to choose between |
| `bundles` | bundle uuid → **(root id, normalized path)**, format version, content hash, primary asset (the runtime entry its path resolves to; a path's candidates are the primaries of its bundles in every root, a search of `bundles_by_path`) — the physical key, matching `files`: UUID-based access must reach the owning file without a logical-index round trip that could turn ambiguous under a same-path file in a second root; path-query ambiguity is derived separately. Directory-import ownership derives at scan from generated bundles' `DirectoryOrigin` records (§8), whose `rule` is the authored stable `ImportRuleId`, never a vector index; deleting that id re-derives the orphan state, never reassigns ownership |
| `bundle_path_refs` | bundle uuid → each logical path its entries' asset/weak reference fields name, written with the bundle's rows at publication. Rename-with-fixups (§4) reads only the bundles that reference the moving path, plus the poisoned ones, whose references are unknown |
| `assets` | asset uuid → bundle uuid, local_id, type_uuid, logical hash, search tags, terminal type, and tag state: `tag_poison` (pending or the refinement's error) and `tag_module` (the module hash when code ran). A pipeline-map change republishes the rows of every bundle holding an asset whose terminal type it changes, whether or not the bundle changed |
| `schemas` | logical hash → schema JSON (cache, rebuilt from bundle snapshots) |
| `artifacts` | `results` + `result_outputs`: static-input-key digest → candidate bucket: (trace digest → trace + output table, whose rows are the pins), revalidated most-recent-first on lookup (§9 — the build-cache lookup; the full input hash is never stored, and commits append candidates, never overwrite); derived-output: child uuid → (parent uuid, output key) — input-versioned, derived per published version from its assets × pinned pipeline map (§9), the only authority for child resolution, commit rows verified against it; ContentHash → segment, offset, len (the CAS extent index) |
| `pipeline_state` | importer/processor registrations and versions; the pipeline dylib content hash; the exact canonical target rows; and the candidate `(TypeUuid, DSLH)` schema map. Publication compares the schema map with source-controlled active lineage and makes the whole PipelineEpoch an input-hash input wherever pipeline code or policy runs |
| `tools` | **ToolEpoch** state (§9): tool key → (`ToolExecutionIdentityV2`, DSCT hash, optional staged package root) — input-versioned. Package registrations snapshot and stage their complete directory no-replace; ambient registrations retain an explicit absolute launcher and toolchain identity, and are nonmemoizable unless they carry a trusted fingerprint. No row resolves to a library handle: runtime `dlopen` in pipeline code and a staged-library API are banned (§3, §9) |
| `schema_lineage` | disposable projection of the source-controlled `SchemaLineageManifest`: per type, the append-only accepted epoch vector `(digest, forward_parent)`, current cursor, explicit `Active \| Retired` authority state, and verified `DSSL` (§5, §6, §11). Startup rebuilds it only from that manifest; non-bootstrap `EntryLineageV1::Manifest` stamps and migration-endpoint stamps are checked against it but never unioned into authority. Forward ancestry follows parent links from current. Explicit rollback moves the cursor only after complete reverse-edge validation; ordinary acceptance appends and advances; retire/reactivate preserve history and require exact stale-base candidate checks including the metadata/control SnapshotStamp used for retirement's negative proof. Every input publication enforces that Retired types have no live entries/endpoints; violation publishes typed RetiredTypeReferenced authority poison. A missing/duplicate manifest leaves this table unavailable and hard-stops schema-dependent work; only §6's unbound exact-basis local repair surface remains mutable until one valid authority publishes |
| `codegen_outputs` | daemon-owned Rust codegen namespace → the exact `ContentHash` of each previously published file. This memo-side table is the expected-preimage authority for §20 publication and recovery; replacing the complete map never advances the input version |

**Evaluating a query.** An `AssetQuery` (a build trace's, codegen's, the
RPC's) or a `FileQuery` enumeration is answered by SQL driven from the index
of its most selective selector, the others checked on the rows it finds.
SQLite keeps no statistics here, so the daemon ranks them: an identity or
exact name (uuid, bundle, bundle path, a glob's literal final segment, local
id), then a tag value, an authored type, a path prefix (or a glob's literal
prefix), a terminal type, a bare tag, and the authoring-only rows. A glob
whose last segment is literal (`**/name.ext`) is found by the final-segment
`name` column of `bundles` and `files`; one whose literal tail holds a `.`
(`*.ext`) by `files.ext`. A wildcard may match `/`, so a literal stem after
one (`**/name.*`) narrows nothing. A bare `*` or `**`, or `authoring_only =
false` alone, selects every runtime entry (or every file): the only
queries that read the whole namespace.

Each published input-version header stores either no global poison or the exact
canonical `VersionPoisonV1` code/detail plus recomputed DSVP identity (§7).
If validation discovers more than one global defect, this is §7's first
canonical `(code, detail-bytes)` row after complete-set enumeration, never the
first filesystem/scanner result; the remaining sorted rows are doctor-only
diagnostics and have no persistence, snapshot, equality, healing, or RPC
authority.
Startup rejects a noncanonical stored tuple rather than synthesizing a bundle
row. Query/RPC projections decode that same record and never rebuild identity
from presentation text.

Per-file indexing failures are **rows, not absences** (§7): a file that
cannot be indexed — malformed container or JSON, a schema-closure
failure (§6), an unsupported format version — publishes a
**bundle-scoped poison row** in the new version *only when the current
malformed bytes themselves yield a fully validated, complete namespace
skeleton* (§7): the outer envelope parses and every asset UUID,
`local_id`, type UUID, and tag value extracts and validates even
though the file as a whole fails. The poison row then replaces every
prior `bundles`/`assets` row for that file's UUIDs. Prior rows are never
silently retained (a quiescent current snapshot would spin on `Drifted`
against the changed bytes) and never silently dropped (query semantics
would change invisibly): resolves against the poisoned UUIDs return a
stable `Failed` naming the parse or index error, and queries whose
selectors could match the file's entries fail naming the poisoned
bundle — the same shape as §10's tag poisoning. Anything less than the
complete skeleton publishes §7's conservative **version-global** poison
instead — *regardless of what prior metadata exists*: a prior indexed
row identifies what the old bytes claimed, not what the malformed
bytes might claim (a malformed edit can introduce a new UUID, type, or
tag before the syntax error), so a bundle-scoped row would need exactly
the facts the file refuses to yield, and answering queries from the
remaining index would silently under-approximate — a poison is scoped
only by facts validated from the bytes being poisoned (§7's stated
principle). Fixing the file heals
on the next version; cross-file identity collisions keep §7's
version-global poison.

#### Asset queries

Every asset query — a build trace's `TraceOp::Query`, a codegen's query, the
RPC's `query_assets` and pure-metadata query — is one `AssetFilter` answered
by `StoreReader::namespace_assets_matching` (or `served_assets_matching` over
the served rows). Each caller only translates its query: a build's
target-dependent `terminal_type` becomes the set of authored types whose
chains end there on its target (plus the types whose chains fail, so an
asset of one fails the build rather than vanishing); a glob contributes its
keys (literal prefix and literal final segment, one shared cutter,
`files::GlobKeys`) and is matched on the streamed rows. The filter is driven
by its most selective indexed selector and runs as at most two statements
whatever the namespace size — a filter with no indexed selector is one
streamed statement over every row, never a read per row. Those filters ask
for the whole namespace by nature: a bare `**`, a glob with neither a
literal prefix nor a literal final segment (bundle paths share their
extension, so it keys nothing), and the runtime role alone (most rows are
runtime rows; the authoring role has its partial index). A tag query is
driven by `asset_tags`, and its poison check by the poisoned rows' partial
index, which holds no rows while no refinement is pending or failed.

One poison semantics holds for every caller. A query answers the
non-poisoned rows of its role that match every selector, glob included. It
**fails, naming the poisoned bundles** (sorted, deduplicated):

- when a poisoned bundle's skeleton row (§7, above) matches every selector
  the skeleton carries — tag included; a skeleton has no served terminal
  type, so a served `terminal_type` selector never reaches one; or
- for a tag query, when a non-poisoned row matching every selector but the
  tag has a poisoned tag index (§10): its asset row's `tag_poison` is set,
  by publication (pending) or by the failed refinement of exactly that row.

A query that cannot reach a poison answers normally: one malformed file
fails only the questions whose selectors could match its entries. The trace
records the failure as `Observed::Err(Poisoned { bundle })` naming the least
bundle (it revalidates like any answer); codegen fails the run with the
bundles named; the RPC answers `TagIndexPoisoned { bundles }` for either
kind of poison. A reference resolved through a query that reaches a poison
fails the build.

### Log-structured CAS (artifact store)

Append-only segment files of content-addressed bytes and nothing else: an
extent is the raw bytes whose blake3 is its `ContentHash` (§5 byte-identity
exception), with no frame, header, checksum or padding. What an extent is
lives in SQLite: `cas_extents` maps `ContentHash → segment, offset, len`,
and the rows that hold an extent say what it is for. Reads check the bytes
against the hash.

A build result is SQLite rows, not a segment record. `results` is the
**candidate bucket**: `(key_kind, static_input_key, trace digest) →`
memo sequence, parent asset UUID, serialized dependency trace, and either
nothing (a success) or the encoded terminal `FailureCause` (a failure).
`result_outputs` holds its tables, one row per entry, each naming a
`ContentHash` (a foreign key to `cas_extents`): role 0 the typed output
table (`output_key → type uuids, ContentHash`; the primary's key is the
empty string, so children derive as `UUIDv5(asset_uuid, output_key)`),
role 1 the **auxiliary-payload table** (`debug key → ContentHash`, §16's
intermediates), role 2 the wire trees the outputs' headers name. The rows
are the pins: an extent is held exactly while some `result_outputs` row or
install's `cas_refs` row names it, and deleting a `results` row takes its
outputs with it (`ON DELETE CASCADE`).
Results are **tagged by key kind**: the two lookup keys have different
shapes, and neither is shoehorned into the other's grammar. A
**build-import result** (`key_kind = DSBI`) is keyed by the §8 `"DSBI"`
pre-key digest and carries the entry's UUID, its discovered trace — the
reference resolutions and plan-selection queries `load_current` produced
(§8, §11) — and its single output row. A **processor result**
(`key_kind = DSSI`) is keyed by the `StaticInputs`' 32-byte `"DSSI"`
digest (§5's canonical encoding); the raw `StaticInputs` are not stored —
nothing reads them back.
Auxiliary payloads pin and evict with the result exactly like outputs but
never appear in derived-output rows, manifests, or load-dep closures.
**Wire trees are first-class CAS extents** (§12's persistence promise
made concrete): every DSWL tree the daemon encodes against commits as an
extent whose bytes are the canonical DSWL serialization — its blake3 *is*
the LayoutHash — committed in the same transaction as any result whose
output headers name that `layout_hash`. Results **pin their wire trees
exactly like outputs** (role 2): the pin/evict unit extends to every wire
tree any output's header references, and eviction may retire a wire-tree
extent only when no current or last-good manifest entry, live lease,
in-flight build, or open pack-build session references its hash (the
observability rule below). This is what makes `Hub.wireTree` (§17) and
the pack wire-tree table (§16) unconditionally serviceable: any artifact
fetchable by ContentHash has its producing tree fetchable by LayoutHash,
across eviction, restart, and epoch retirement.
A result may instead carry a **failure outcome** — the discovered trace
(possibly empty) and the terminal **`FailureCause`** (§9): for a failing
context operation the trace ends in its `Observed::Err` entry; for a
deterministic local failure — validator diagnostics, migration-plan
validation, a processor `BuildError` — the cause is `Local(fingerprint)`
with no synthetic op invented. Either way it is revalidated on lookup
exactly like a success (a `Capability` miss revalidates against the
snapshot's pipeline epoch, so a failure minted by a missing registration
heals on the first epoch that supplies it), with no output rows:
deterministic build failures memoize at their basis (§15's quiescence —
an unchanged basis answers from the row instead of rebuilding once per
client), while transient infrastructure errors (I/O outside the tree, a
module crash) are typed apart and never memoized.

**The write transaction that indexes a group is its commit**: the bytes
not yet in the CAS are appended first, one fsync per touched segment,
then the extent rows, the `results` row and its `result_outputs` rows,
all in one SQLite write transaction (the caller's, when a node publishes
its result with its artifact installs and their load edges). A re-commit
of the same key and trace replaces the earlier result in that
transaction. A group whose transaction rolls back, or that a crash
interrupts before COMMIT, publishes nothing — a crash after output 2 of 3
publishes nothing — and its bytes past the segment's indexed length are
dead. A lookup reads one indexed bucket: the key's `results` rows,
most-recent-first, then the chosen candidate's row and its outputs by
primary key. The derived-output *namespace* is deliberately not derived
from results: historical results are memos, and child resolution
consults only the current version's derived index (§9) — a retired child
UUID is never resurrected by an old result. Segments roll at a size cap;
an extent larger than the cap is written instead as a dedicated
**oversize segment** — one extent per file, named and typed as oversize
in `cas_segments`, indexed, compacted, and evicted exactly like any
segment — so an oversized artifact (blobs are unbounded, §4, §16) is
representable, never rejected or truncated. Reads mmap and slice. Write
order: append, fsync the segment, then insert the index rows; segment
creation and deletion also fsync the directory. **SQLite is the
authority** on what committed and on which segments exist
(`cas_segments`, each writer's segment found by its row; one open regular
segment per writer is a unique index, not a sweep). Recovery truncates
each segment to its indexed length. A segment file shorter than its
indexed length lost committed bytes to something outside the store (an
external truncation, a lying fsync): recovery reports it and changes
nothing, and reads of the lost extents fail. Allocation's commit point is
`next_segment_id`: a file at or past it belongs to an allocation that
never committed, and the next allocation of its id truncates it, so
nothing scans for unnamed files.
Compaction is one SQLite write transaction: it allocates its destination
segments sealed, copies the live extents, fsyncs them, repoints
`cas_extents` and marks the old segments dead; their files are deleted
only after a grace period past the read bound. Evicting a result deletes
its `results` row (its outputs cascade) and, in the same transaction,
each extent no other row holds; a release of an install does the same for
its `cas_refs`. Foreign keys refuse to drop a held extent, so there is
nothing to prune. Live bytes, whole and per segment, are sums over the
covering `(segment, len)` index on `cas_extents`. GC is Bitcask-style
compaction driven by the size cap (random eviction, each victim one
sampled index probe; the pass runs only after the CAS index changed) — everything in the CAS
is rebuildable, so eviction is always safe. But never observable: eviction
may not remove a ContentHash referenced by any current or last-good manifest
entry, live snapshot lease, in-flight build, or open pack-build session —
checked inside the same transaction that deletes the index row; pins hold
a build result's whole output table as one unit, never single outputs — and
`resolve` pins its returned ContentHash to the caller's lease *before* the
response is sent, so a fetch following a resolve cannot lose a race with
the evictor. The CAS holds only derived
artifacts: import encodings and processor outputs. Source bytes are never
copied into the CAS — the tree is the only live source state; a pinned
resolve whose source inputs have drifted fails rather than reading a stashed
copy (§15).

Version control remains the durable archive: reproducing an arbitrary
committed state wholesale is a checkout plus a rebuild, guaranteed
byte-identical by build purity under the determinism contract (§9).

### Consistency contract

The metadata store is a deterministic function of (filesystem snapshot,
code). Two sequencing domains, deliberately distinct: watcher batches and
authoring operations apply as atomic transactions that advance the
**input version** — the thing snapshots pin and `Drifted` compares
against; build results commit as atomic **memo transactions** on a
separate memo sequence, attaching outputs to an input basis without
advancing any input version (which is precisely what lets a snapshot
acquire memoized outputs without changing, below). Readers only ever
observe a complete input version — all metadata for a given tree state, or
none of it. The store partitions by authority: **input-versioned** state
(bundle metadata, search tags, authored dependency records, and the
derived-output **namespace index**, §9) moves only
with input versions; **memo** state (build results, dependency traces,
which never define names) is monotone and keyed
by input basis, never
versioned — an old snapshot reading a newer memo is the memoization
semantic below, not a leak; **ephemeral** state (watch cursors, lease
tables) carries no version and rebuilds from scratch. The coordinator
orders writes so a memo row never becomes readable before the input
version it attaches to is. Code and configuration are inputs like any
other: a module or schema swap — or an edit to the daemon's own
configuration (§18) — lands as a watcher event, advances the input
version, and rotates the `PipelineEpoch` (§3) or configuration epoch
(§18) — restart-only keys excepted: a valid restart-only edit records
a pending-restart configuration generation and surfaces
`RestartRequired`, and the input version advances at restart, when the
value takes effect (§18); module, schemas, and target configuration validate as **one
candidate** (§3, §18), the candidate's pipeline map constructed against
the candidate's target set, so no published version pairs a target
definition with a map built for a different one — each snapshot pins
the epochs current
at its version, and a resolve whose pinned epoch has been drained (the
old module is gone) returns `Drifted(Dylib)` (§15), never a silent
re-pair with newer code. **Rotation is staged, and a rejected candidate
still publishes** (§3, §7's rule): the coordinator validates the
candidate epoch whole — identity checks, registration into its unpublished
token/arena, exact registry↔manifest-cursor equality, and configuration
validation — *before* publishing the input version that pins it. Success
publishes `Ready` only when every bootstrap-control row equals its §3 format
constant and every other registered TypeUuid has exactly one Active manifest
authority row at the same DSLH, with no missing/extra Active rows. Retired rows
preserve history but are excluded from this equality; only
the explicit retire/reactivate commands can change that classification, and
Ready additionally requires that no current entry or Migration endpoint names
a Retired type. Scan, import, CRUD, and manual reconciliation recheck that
negative invariant on every input publication. A violation publishes
`RetiredTypeReferenced` rather than Ready; reactivation admits the waiting
metadata only in the same transaction that restores Active authority.
A mismatch publishes stable `SchemaAcceptanceRequired`, never `Ready`; other
failure publishes the
version carrying a **pipeline poison** or the orthogonal snapshot-pinned
`ConfigurationState::Poisoned` (§18)
naming the error — the prior epoch is never silently retained as the new
version's code (obsolete processor code would serve new bytes), and the
version is never dropped (a retry-refreshed client that observed the new
dylib bytes would refresh into an unchanged version and spin at
quiescence). The poisoned version's snapshots carry
`PipelineState::Poisoned` or `SchemaAcceptanceRequired` (declared below) and
`epoch()` is fallible,
so the boundary is representable, not conventional: **pure-metadata
reads** — the path index, input versions, CAS reads, lease pinning —
remain valid under poison, while anything needing the pipeline map,
registry, defaults, or migration fns (load_current, terminal-type
queries, the derived-output namespace, builds) fails deterministically
with a stable `Failed` naming the registration error; the
old epoch remains resident for exactly the snapshots that pin it; the
next successful swap publishes normally over the poison. Pipeline poison
has **two named entry points**. `CandidateOpen` is the publication-time
failure just described and has no usable candidate epoch.
For a post-open `CandidateOpen` failure, the unpublished token is fenced and
the registration arena is destroyed in reverse installation order through
status thunks before `unload`; `dlclose` occurs only after every status
succeeds and no pin remains. Any failure deliberately leaks arena + library
and the typed poison records that cleanup disposition (§3).
`PublishedRuntime` is an already-published epoch whose status-returning
drop/free/update path reports callback panic/rejection, or whose retirement
cleanup fails. These are respectively DSPP codes `PublishedCallbackPanic`,
`PublishedCallbackRejected`, and `PublishedCleanup`, all with the exact
`PublishedEpochLeaked` disposition: the shared
`ModuleEpochToken` atomically changes every snapshot pinning that epoch
to `PipelineState::Poisoned`, fences new jobs and loads, makes drain
permanently incomplete, and prevents `dlclose` forever (a deliberate
library leak). A later successful candidate may serve new snapshots but
cannot heal or unload the poisoned old epoch. (§5's
schema/module source-identity mismatch is an ordinary unpublished-candidate
failure, not a second live-registry state.) **Pinning the epoch pins module residency**:
`PipelineEpoch` holds module-owned trait objects and function pointers,
so `dlclose` of the retired module is gated on the epoch `Arc`'s strong
count reaching zero — every snapshot clone, job context, and RPC
capability holding it counts. "Drained" means the epoch stopped
accepting new work and its jobs finished; **unload** additionally waits
for the last pinned reference to drop. A snapshot outliving a reload
keeps its code loaded and merely resolves `Drifted(Dylib)` — it can
never dangle into unloaded code (§15's `dlclose` rule is the engine-side
analog).

```rust
pub struct InputVersion(pub u64);   // advanced by watcher batches + authoring ops —
                                    // module/schema artifact swaps and config edits
                                    // arrive as watcher events, so epoch rotation is
                                    // an input event. Ordered only WITHIN one store
                                    // instance (below): never a universal identity
pub struct MemoSeq(pub u64);        // advanced by build-result commits

/// Minted randomly when `.distill/` is created — and re-minted whenever
/// daemon state is rebuilt from scratch. InputVersion counters restart
/// after state loss, so a bare u64 could alias two unrelated versions
/// across a client reconnect; every version that crosses the RPC
/// boundary is therefore instance-qualified: the pair below is the
/// **snapshot stamp**. The instance id travels once per connection
/// (§17's connect) with bare u64 versions per message — a client
/// observing a changed instance discards every held version comparison
/// and re-resolves from scratch, which is always safe (§17: no
/// client-related daemon state is authoritative). Loader-side adoption
/// identity is separate again — the loader-minted AdoptionId (§15),
/// since packs carry no InputVersion at all (§16). On the loader's IO
/// surface the stamp is the RPC-side IO-neutral basis token
/// (`IoBasis::Rpc`, §15). PackfileIO instead carries the mounted manifest
/// hash, and the stamp never appears in pack outcomes.
pub struct StoreInstanceId(pub [u8; 16]);
pub struct SnapshotStamp { pub instance: StoreInstanceId, pub version: InputVersion }

impl MetadataSnapshot {             // immutable; taking/holding one is an Arc clone
    pub fn version(&self) -> InputVersion;
    /// Some(err) ⇔ this snapshot is a poisoned version (§7): `current`
    /// advanced carrying an identity-validation failure. The poison is
    /// **version-global and uniform**: every namespace-facing operation
    /// below fails with this same error naming the collision — never one
    /// surviving duplicate, never last-good metadata from a projection
    /// that happens not to touch the colliding rows. Consumers that
    /// merely need the version number or the epoch still can.
    pub fn poisoned(&self) -> Option<&VersionPoison>;
    pub fn query(&self, q: &AssetQuery) -> Result<Vec<AssetUuid>, QueryError>;
                                    // tag poisoning surfaces here (§10);
                                    // unclosed local_id queries rejected (§10);
                                    // version poison is
                                    // QueryError::VersionPoisoned(the exact value)
    /// Err(Poisoned) on a poisoned version — an Option alone could not
    /// distinguish "absent" from "unanswerable", and answering from a
    /// poisoned index would let editor, codegen, and query consumers
    /// observe mutually incompatible projections of one version.
    pub fn entry(&self, uuid: AssetUuid) -> Result<Option<EntryMeta>, QueryError>;
    /// Ok(None) is a recordable miss; a path resolvable in more than one
    /// asset root is Err (§18), never a tiebreak; version poison is Err.
    pub fn resolve_path(&self, path: &str) -> Result<Option<AssetUuid>, QueryError>;
    /// Orthogonal to pipeline poison: a valid prior pipeline with an
    /// invalid configuration candidate is representable. Pure metadata
    /// operations do not call this; authoring and target/config-dependent
    /// operations must and return the typed poison.
    pub fn configuration(&self)
        -> Result<&Arc<ConfigurationEpoch>, &ConfigurationPoison>;
    /// The pipeline state current at this snapshot's version: snapshot
    /// and epoch pin together, and LoadContext takes both from here —
    /// never a snapshot from one basis and code from another. Fallible,
    /// because a pipeline-poisoned version has no PipelineEpoch to
    /// return (§3: a failed candidate never becomes one, and the prior
    /// epoch may not stand in): callers that need the pipeline map,
    /// registry, defaults, or migration fns — load_current,
    /// terminal-type queries, the derived-output namespace, builds —
    /// receive the unavailable error (pipeline poison, schema acceptance
    /// required, or RetiredTypeReferenced authority poison); pure-metadata reads (path index, input
    /// versions, CAS reads, lease pinning) never call this and remain
    /// valid under poison. A resolve whose
    /// pinned epoch has been drained returns Drifted(Dylib) (§15).
    pub fn epoch(&self) -> Result<&Arc<PipelineEpoch>, &PipelineUnavailable>;
    /// Crate-private coordinator entry; the authority token cannot be minted
    /// by RPC, loaders, pack builders' root-query path, or pipeline code.
    fn control(&self, authority: ControlAuthorityToken) -> ControlSnapshot<'_>;
}

/// What a snapshot carries: the validated epoch, or the named failure
/// that kept a candidate from becoming one. `last_good` is residency
/// bookkeeping only — the prior epoch stays loaded for the snapshots
/// that pin it, but is never served as this version's code (§3, §13):
/// obsolete processor code must not cook new bytes.
#[repr(u16)]
pub enum PipelinePoisonCode {
    CandidateOpen = 1,
    CandidateAttestation = 2,
    CandidateRegistration = 3,
    CandidateValidation = 4,
    CandidateCleanup = 5,
    PublishedCallbackPanic = 6,
    PublishedCallbackRejected = 7,
    PublishedCleanup = 8,
}
#[repr(u16)]
pub enum PipelinePoisonOrigin { CandidateOpen = 1, PublishedRuntime = 2 }
#[repr(u16)]
pub enum CleanupDisposition {
    None = 0,
    CleanedAndClosed = 1,
    RegistrationCleanupFailed = 2,
    ModuleUnloadFailed = 3,
    TokenPoisoned = 4,
    TokenPinned = 5,
    DlcloseFailed = 6,
    PublishedEpochLeaked = 7,
}
pub struct PipelinePoison {
    pub code: PipelinePoisonCode,
    pub origin: PipelinePoisonOrigin,
    pub cleanup: CleanupDisposition,
    pub identity: [u8; 32],
    pub message: String, // presentation only
}

/// DSPP v1 admits exactly this matrix:
/// - CandidateOpen origin + codes CandidateOpen..CandidateValidation +
///   cleanup None or CleanedAndClosed. `None` means failure preceded resource
///   acquisition; `CleanedAndClosed` means every acquired resource closed.
/// - CandidateOpen origin + CandidateCleanup code + one of
///   RegistrationCleanupFailed, ModuleUnloadFailed, TokenPoisoned,
///   TokenPinned, or DlcloseFailed. A cleanup failure replaces the initiating
///   candidate code; no successful/none disposition is admitted for code 5.
/// - PublishedRuntime origin + codes PublishedCallbackPanic,
///   PublishedCallbackRejected, or PublishedCleanup + PublishedEpochLeaked.
/// Every published-runtime poison fences the epoch and leaks it permanently.
/// No other origin/code/disposition tuple is canonical.
///
/// `identity = blake3("DSPP" || 0x01 || code:u16-LE || origin:u16-LE ||
/// cleanup:u16-LE)`. The message is excluded. Persistence and RPC decode
/// recompute identity and reject unknown versions/values, a matrix mismatch,
/// wrong-width identity, or a digest mismatch; evolution requires DSPP v2.

pub enum PipelineUnavailable {
    Poisoned(PipelinePoison),
    SchemaAcceptanceRequired(SchemaAcceptanceRequired),
    RetiredTypeReferenced(RetiredTypeReferenced),
}

pub struct RetiredTypeReferenced {
    pub manifest_hash: BundleFileHash,
    pub basis: SnapshotStamp,
    pub type_uuid: TypeUuid,
    /// Sorted/deduplicated by (tag, raw AssetUuid bytes).
    pub references: Vec<RetiredTypeReference>,
}
pub enum RetiredTypeReference {
    Entry(AssetUuid),
    MigrationEndpoint(AssetUuid),
}
#[repr(u8)]
pub enum RetiredTypeReferenceTag { Entry = 1, MigrationEndpoint = 2 }

pub struct SchemaAcceptanceRequired {
    pub manifest_hash: BundleFileHash,
    pub candidate: CandidateEpochIdentity,
    /// Sorted by TypeUuid over non-bootstrap candidate rows and Active
    /// authority rows; None on either side names missing/extra Active
    /// authority. Bootstrap mismatch is the separate hard gate, not lineage
    /// acceptance. Retired rows do not appear unless an explicit reactivation
    /// transition is requested.
    pub mismatches: Vec<(TypeUuid, Option<LogicalHash>, Option<LogicalHash>)>,
}
pub struct CandidateEpochIdentity {
    pub dylib_hash: [u8; 32],
    pub target_set: CanonicalTargetSet,
}

/// Fully opened but unpublished candidate retained while explicit lineage
/// acceptance is required. It owns the library, token, registration arena,
/// and validated map/target set, but cannot mint LoadContexts or serve work.
/// Accept/rollback promotes this exact object only after its identity and
/// every selected cursor are rechecked; replacement/cancellation tears it
/// down through §3's failed-candidate cleanup state machine.
pub struct StagedPipelineEpoch { /* identity, library, token, arena, map, targets */ }

pub enum PipelineState {
    /// Invariant: every §3 bootstrap-control row passed its separate logical
    /// format-authority gate; for every other candidate schema row, exactly
    /// one Active manifest type row exists and its DSLH equals the digest at
    /// the current cursor; the manifest has no extra Active row.
    /// Retired rows are retained authority/history but intentionally excluded.
    Ready(Arc<PipelineEpoch>),
    SchemaAcceptanceRequired {
        required: SchemaAcceptanceRequired,
        staged: Arc<StagedPipelineEpoch>,
        last_good: Option<Arc<PipelineEpoch>>,
    },
    RetiredTypeReferenced {
        error: RetiredTypeReferenced,
        /// The otherwise-valid code epoch is retained but cannot mint a
        /// LoadContext until explicit reactivation admits the waiting bytes.
        blocked_epoch: Arc<PipelineEpoch>,
        last_good: Option<Arc<PipelineEpoch>>,
    },
    /// For PublishedRuntime this is a shared state transition observed by
    /// all snapshots pinning `poisoned_epoch`, not a newly minted input
    /// version. That epoch remains resident forever.
    Poisoned {
        error: PipelinePoison,
        last_good: Option<Arc<PipelineEpoch>>,
        poisoned_epoch: Option<Arc<PipelineEpoch>>,
    },
}

pub enum ConfigurationState {
    Ready(Arc<ConfigurationEpoch>),
    /// Persists code plus the complete versioned canonical DscpV1 detail and
    /// recomputed reason_hash; message remains presentation-only.
    Poisoned { reason: ConfigurationPoison,
               last_good: Option<Arc<ConfigurationEpoch>> },
}
```

`ConfigurationState` is pinned by every snapshot alongside
`PipelineState`, and its operation classification is exact. Taking,
holding, and refreshing snapshots remains valid so clients can observe
healing. Pure metadata reads over the already-published index — version
and state inspection, UUID/bundle/path/tag metadata, `resolve_path`, CAS
reads, ContentHash fetch, and lease pinning — remain valid (a query that
asks for pipeline-derived terminal type is not pure). Authoring operations
all fail with `ConfigurationPoisoned(reason)`: they may depend on roots,
output paths, or publication policy and never silently use `last_good`.
The sole mutation exception is §6/§17's unbound `LineageRepair`, and only for
current MissingLineageManifest or DuplicateLineageManifest DSCP: it can create
the configured missing file or remove explicitly rejected duplicate claimants,
but exposes no ordinary authoring operation and still never uses `last_good`.
Build/resolve and every target/config-dependent RPC operation fail with
that same stable typed result; snapshot/refresh/subscription and immutable
ContentHash fetch remain available. The prior configuration serves only
snapshots that pinned its earlier Ready version, never the poisoned
version.

**A snapshot denotes inputs; outputs are memoized attributes of those
inputs.** The ContentHash for (asset, target) at version `N` is *determined* by
`N`'s inputs whether or not any build has run — purity is what makes that
statement well-defined. A lazy build completing after `N` therefore
attaches its result to every version whose inputs match, including `N`,
without changing any version: returning `(content_hash, N)` is reading a memo, not
inventing history. Derived outputs (§9) resolve the same way — a UUIDv5
child at snapshot `N` denotes "build the parent at `N`, read its output
table," so children need no snapshot metadata of their own. This is what
makes "immutable snapshot" and "lazily materialized artifacts" compatible:
the snapshot pins the question, not the answer.

### Concurrency model

One writer, many snapshot readers:

- **Coordinator** — a single task owns every SQLite write (WAL mode). It
  consumes watcher batches, RPC authoring operations, and completed build
  results, applying each as one transaction: input events advance the
  input version, build results advance only the memo sequence (see the
  consistency contract). Watched-import results commit under
  attempted-basis validation (§8): the committing transaction
  revalidates the complete outcome-bearing read-set — successful and
  failed reads/probes/listings, importer-capability hits/misses, and a
  failed attempt's terminal `FailureCause` — against the current raw-file
  index and pipeline epoch, and installs the write and dependencies only
  if every entry still matches. A mismatch discards and re-enqueues, so
  an event consumed mid-import can never strand a stale bundle.
  Serializing writes gives the contract for free
  and sidesteps SQLite write contention.
- **Metadata snapshots** — the hot metadata index is an immutable in-memory
  structure versioned by the coordinator; taking or holding a snapshot is an
  `Arc` clone. RPC snapshot capabilities (§17) pin one.
- **Compiled state** — what the daemon compiles from a configuration (the
  schema authority, build targets, pipeline projection and importers, the
  pipeline epoch or its failure, the scanner and its roots) is one immutable
  entry keyed by the input version that published it
  (`store_meta.compiled_version`, written in that version's input). Every
  owner of a store connection (the process loop, each RPC connection, each
  build worker) looks the entry up for the version its own transaction
  sees; a version with no entry is a typed retryable error, never another
  version's state. A publication stages its entry before it commits and
  confirms it after, so a failed publication changes nothing; the module
  host and the watcher's roots change only after the commit. Superseded
  entries live while held, and a bounded number stay for a while for older
  snapshots. State with no compiled form (the pending scan rejection with
  the subjects that heal it, the configuration source's error) is SQLite
  rows that only the scan and configuration publications write.
- **Build pool** — pure work (imports, processing, artifact encoding) runs on
  a work-stealing pool sized by `parallelism`. Jobs read a pinned snapshot,
  never the live store, and return results to the coordinator for commit.
  Pipeline-module code runs here. Results are never discarded: a completed
  job is a pure-function result keyed by its inputs, so it always commits to
  the cache; the *current* manifest entry updates only if the current
  version's inputs still match the job's — otherwise the entry stays
  invalidated and the next resolve builds against newer inputs. A pinned
  requester receives its result either way. A failed build never
  unpublishes — the last good artifact stays current, with the error
  surfaced — and deterministic failures commit **failure records** with
  their basis and partial trace (the CAS rules above), so an unchanged
  basis answers from the record rather than rebuilding per client.
  Requests for one node share one build: builds are scheduled as **build
  cells** keyed by their static inputs (below), and a worker needing a
  dependency some other cell builds waits on it or steals it by the rules
  below. Synchronous `ctx.read` executes descendants on the same worker,
  with request-local cycle detection and the mandatory depth bound from
  §9. The pool configures 8 MiB worker stacks, and
  callbacks must avoid large native-stack allocations. Results converge
  through the serialized CAS commit path. The queue has
  two priorities, FIFO within each: interactive resolves ahead of batch work
  (pack builds, `doctor`) — with **bounded starvation**. Staging requires
  `parallelism >= 1` and
  `1 <= batch_reserved_workers <= max(1, parallelism - 1)`. For
  `parallelism >= 2`, whenever batch work is pending the reserved slots
  serve the oldest batch job and at least one slot remains interactive;
  every other slot keeps strict interactive priority. At the degenerate
  `parallelism == 1`, the sole slot alternates the oldest pending batch
  job and interactive work, so neither class can starve. A live pool
  resize re-clamps the reservation to these bounds; already active excess
  slots drain naturally and are never cancelled or reassigned mid-job.
- **RPC threads** — one listener thread accepts; every client connection is
  served on an OS thread of its own (capnp's `RpcSystem` is single-threaded,
  so each runs on that thread's current-thread runtime), resolving reads
  against its own snapshots and forwarding writes to the coordinator. A
  stalled or large request delays only its own connection (§17 Server
  threading).
- **Watcher thread** — OS watcher events batch into coordinator messages
  (§14).

### Build cells

A build is a pure function of all of its input data. A snapshot is not
one of those inputs: two requesters at different snapshots whose inputs
agree want the same build, and the artifacts it produces are shared CAS
state. The daemon therefore keys, schedules and shares builds by node, not
by requester.

**Keys and traces.** A node is one asset's import plus its processor chain
for one target. Its key (`KeyKind::Node`, canonical `DSNK` bytes) holds
only its *static* inputs: the asset uuid, its entry metadata and its
bundle's content hash, the chain (each stage's processor identity and
types, the terminal and extra types, the pipeline dylib hash), the target
definition, the validated flag, every type the chain names with its
logical and layout hashes and build-only policy, and the migration-planner
and artifact format versions. Everything a build discovers while it runs
(reads of other assets' built artifacts, path resolutions, queries, tool and control
calls, reference and role checks) is *dynamic*: it is recorded in the
node's trace rather than its key, and a strong load dependency enters the
trace as a `Read` of the content its dependency built to. A cached node
answers a lookup only where its trace revalidates. The asset uuid stays in
the key: artifacts embed their asset uuid (§12), so two assets with equal
content are still two builds. `asset_resolutions` stays a projection of
the input version (§13): it records *that* an asset is drifted, never a
build's result, so a build's completion changes no input version.

**Resolve.** A resolve of a drifted asset at snapshot `S` computes the
node's key at `S` and looks it up: a cached result whose trace holds at
`S` answers `Built` at once. A miss submits the key's cell, or joins the
cell already in flight for it, and the requester awaits its ticket.

**Cells.** The scheduler holds at most one cell per key, `Queued`,
`Running` or `Done`. A requester's ticket is a `Send` future holding one
interest in its cell; dropping it withdraws that interest, and a queued
cell no ticket and no worker wants is removed without running. A running
cell always finishes, and its result is published whoever still waits.
A cell is queued at the highest work class among its waiters: an
interactive requester joining a queued batch cell promotes it. A cell's
worker builds at a snapshot it opens when the cell starts.

**Answering each waiter.** A finished cell's outcome is answered at each
waiter's own snapshot. `Built` carries the node's trace: a waiter at
whose snapshot it revalidates gets `Built`; any other gets `Drifted`
naming the first input that differs, and retries at a fresher snapshot.
A failure is answered `Failed` only to waiters at the snapshot the worker
built at (it is a fact about exactly those inputs); others get `Drifted`.
Failed nodes are not cached as nodes, and a node with an uncacheable stage
(§9) gets no cache entry: a later request rebuilds it, though concurrent
requests still share its one cell.

**Workers and dependencies.** A worker building a node that needs another
node first looks it up in the cache at its own snapshot, then claims its
cell: an absent or queued cell is run (stolen) inline by this worker; a
cell running on another worker is waited on, unless waiting would close a
cycle of waiting workers (this one included), in which case the node is
built privately, outside any cell. A waited-for result is used
only if its trace holds at the waiting worker's snapshot. Waiting never
needs a free worker, so a chain of any depth completes with one worker,
and the visiting set and the §9 depth bound keep every walk finite.

**Revalidation reads only what a trace asks.** A trace is revalidated,
and a build's own trace questions answered, by indexed point queries in
the snapshot's read transaction, never by loading the project: an entry,
its bundle's hash, a derived output, the assets at a path, the tool hash
at the build's tool version, and for a query the assets of its most
selective indexed selector (uuid, bundle and local id, bundle path, tag
value, authored type, path or glob literal prefix, terminal type through
the authored types that reach it, tag), each then checked against every
selector as before. Every question costs `O(log n)` plus its answer's
size, and a five-question trace is a dozen statements at any project
size. Nothing is kept between questions: each is asked of SQLite again,
the one copy of the snapshot. A `Read` is answered from the contents of
nodes built so far. A tool question is one `ToolEpoch` row at the build's
tool version. A store failure while answering (a poisoned bundle, a chain error) fails
only the build that asked a question reaching it. Two shapes still read
more than their answer: a query with no indexed selector (only a local
id, only `authoring_only`, or a glob with no literal prefix) reads every
entry, and a tagged query without one reads every tag-index poison.

**Publication.** A worker publishes each node in one write transaction:
its stage cache rows, wire trees, artifacts with their load edges, and the
node's cache row. Only then does it complete the node's cell, so a waiter
woken by a result always finds its artifacts in the CAS. Workers hold no
write transaction while a processor runs.

**The RPC side.** A connection thread only reads: it looks the node up and
submits or joins a cell, then awaits the ticket on its `LocalSet`; the
build runs on the build workers, never on a connection thread. Inline
builds (tag-index refinement) run on a writer inside an open input and
are reachable only through an `OpenInput` proof; nothing that answers a
resolve holds one. `doctor verify` builds at one read snapshot, without
any cache, and writes nothing. A reconciliation pass refines the tag
index in its one apply input (§14, one pass, one input version), never in
its rolled-back plan. A complete refinement (a full rescan or a configuration
publication) that fails poisons every asset's tags, reading the
asset-to-bundle map inside that input: a whole-namespace bulk operation,
like the publication it belongs to.

## 14. File Tracking & Consistency

Modeled on v1's `FileTracker`, whose behavior is carried over:

- **Watcher → incremental batched transactions.** Native OS events retain their
  affected physical paths and ordered rename pairs, wait for the filesystem
  to settle, and publish one transaction after path-local re-observation.
  Settling is a **trailing quiet window** (`watch.quiet_ms`, default 250 ms):
  every event restarts it, so a burst (a git checkout, a branch switch) is
  reconciled once, after its last event, never from a half-written
  intermediate state. No pass of the process loop starts while the window
  is open — a retry of a failed pass, a codegen retry, an outside
  publication's codegen, or the idle pass waits for it too; only a watcher
  failure stops the loop at once. There is no cap: a path that never stops
  changing holds reconciliation off, and once the window has been held open
  for 5 s the daemon warns (then every 30 s while it stays open), naming the
  paths that changed most so they can be moved out of the watched roots.
  `[[rebuild]]` jobs wait for their inputs to settle under the same window.
  Event
  metadata is never trusted as namespace authority: a file create/write/remove
  re-observes exactly that path, a directory create or rename-to enumerates only
  that subtree (closing the recursive-watch installation race), and a directory
  remove or rename-from deletes only that stored prefix. Unrelated roots,
  directories, files, and bundle bytes are neither enumerated nor rehashed.
  Coalescing may replace repeated final-state events for one path, but it must
  preserve the ordered rename chain needed for live identity and dependency
  updates.
- **One pass, one input version.** A settled batch is reconciled by one pass
  that publishes exactly one input version: the path-local scan, directory
  and watched imports for the affected bundles, the dirty-queue
  acknowledgement and the runtime pipeline-failure sync all land in it, so
  no snapshot ever pins a half-reconciled burst. The pass plans in a write
  that always rolls back (to learn which imports the new file state needs),
  runs the importers in parallel outside any write against an overlay of
  that file state, then applies everything in one coordinated input whose
  base must still be the version the plan read. An outside write (an RPC
  authoring edit) in between makes the pass `Stale`: nothing is applied and
  the whole pass reruns, so no import is applied twice. An import whose
  read-set moved between plan and apply is not applied; it and the pending
  paths stay queued for a retry pass. One bundle's importer failure does not
  hold back the rest: the failing bundle keeps its last good result (with
  the failure memoized) or, with nothing to keep, is reported as the pass's
  background error, which clears when a later pass succeeds. Outside the
  single input remain: configuration/schema/pipeline candidate
  publications (each its own version, before the pass, because they install
  the epoch and importers the pass uses), codegen (files only, no version),
  and the disk writes of import outputs (written before the input commits,
  as before). The pass observes those outputs in its own input, so the
  watcher's echo of them publishes nothing: an echo whose namespace
  observation is unchanged and whose renames start from a path never
  observed (an atomic write's temporary file) is no change.
- **Chained imports run in the same pass.** An import whose sources include
  another import's output runs after it, against its output: once a level
  of imports has run, the pass compares each output with the `files` row it
  will replace, discovers (in the rolled-back plan input, over the overlay
  plus those outputs) the imports that read a changed output, and runs them
  as the next level, upstream first; an import found again at a later level
  runs only there. The apply checks each level against the outputs before
  it, so a chain lands in the pass's one input version. A pass runs at most
  8 levels; the rest of a deeper chain runs in the next pass, which the
  pass reports as more work. An import that reads its own output through a
  chain (a cycle) is cut: the pass reports it as an import failure naming
  the chain and it reruns in the next pass.
- **Incremental-workload invariant.** After a successful startup scan, ordinary
  filesystem activity never invokes the complete-root scanner. Complete scans
  are restricted to process startup, an accepted configured-root replacement,
  explicit `doctor verify`, and recovery after the native backend explicitly
  reports overflow or incomplete observation. Overflow recovery is exceptional;
  treating every ordinary event as an overflow/full-rescan request is
  nonconforming.
- **Control-path coverage is incremental too.** A control file whose immediate
  parent does not yet exist is covered by watching its nearest existing
  ancestor. Any event at an ancestor or descendant of the eventual control
  path invalidates that exact control authority, then synchronously rebuilds
  coverage before the authority is reread. Introducing or atomically replacing
  a schema/module/configuration parent therefore never requires a complete
  asset-root scan and cannot leave later edits outside native watch coverage.
- **Startup reconciliation.** Watchers arm **before** the scan begins,
  never after. Events arriving during traversal remain in that startup
  generation and are replayed through the incremental path after the full-scan
  transaction commits; they cannot be consumed by the live loop first. Only a
  native overflow/incomplete-observation signal discards that partial event set
  and repeats a fully armed scan. This scan/event union means no change can fall
  between a path's scan visit and watcher activation. The full scan runs while the DB holds the
  previous session's state; at scan end, DB-known files absent from the scan
  become synthesized deletes, files outside watched roots are purged, and
  changed mtime/size/kind marks dirty. Metadata equality is trusted
  **conservatively, by watermark**: each session durably records a
  **clean watermark** — the newest mtime it observed under active watch —
  and reconciliation content-hashes any file whose stored metadata
  differs *or* whose mtime is not strictly older than the previous
  session's watermark. An offline replacement that preserves size
  necessarily carries an mtime at or after the daemon's last
  observation, so equal-metadata staleness cannot persist; a
  deliberately backdated mtime is outside the contract, and `doctor
  verify`'s full rehash is the net. The daemon may be off during arbitrary
  tree changes (git!) and converges. Enumeration is total: failure to open or
  enumerate a configured root or raw relative subtree publishes §7
  `UnreadableScanSubtree { Root|Subtree, PermissionDenied|NotFound|
  InvalidFileType|SymlinkIdentityChanged|IoDataLoss }`; no unreadable branch is
  silently treated as an empty directory or synthesized deletion set.
- **Dirty queue discipline.** Downstream consumers capture a dirty/rename
  prefix, publish their idempotent input results, then clear that prefix with
  a compare-and-delete; the process loop's acknowledgement joins the pass's
  input (above), other consumers use an unversioned maintenance transaction.
  The acknowledgement compares each path's stored observation generation with
  the generation the consumer used, path by path: a path whose generation
  moved is newer work, not an error, and stays queued for the next pass while
  settled paths clear; a crash or failed consumer clears nothing and the
  prefix retries, and rows appended after the captured sequence survive.
  Queue maintenance
  never fabricates a new externally visible input version. For watched
  imports the comparison is total: the entire outcome-bearing read-set —
  failed reads/probes/listings and importer-capability hit/miss included —
  plus a failure's terminal cause revalidates inside the committing
  transaction (§8), so a source event
  consumed mid-import can never strand a stale result.
- **Rename log.** Ordered rename events consumed transactionally; live renames
  update path state without losing identity.
- **Symlink handling.** Symlinked directories extend the watched set
  dynamically — under identity discipline, never blind traversal (the
  v2 rule below).

v2 improvements over v1's tracker:

- **Identity-based rename recovery.** Because bundles carry their own UUIDs,
  scan reconciliation pairs a disappeared path with an appeared path by bundle
  UUID — offline renames preserve identity and don't cascade into re-imports
  (content hash unchanged). Dependents whose reference queries used the old
  path are invalidated and surface resolution errors (§4) rather than
  silently pointing at stale targets.
- **Content hashes, not just mtime/size,** gate import work, so touch-without-
  change is cheap.
- **Edge cases are specified:** native watcher overflow or an incomplete
  observation discards the incomplete path batch and schedules one fully armed
  recovery scan; editor atomic-save rename chains coalesce to incremental final
  states while preserving their ordered rename pairs; symlinks
  escaping the asset roots are errors; startup reconciliation
  content-hashes files per the watermark rule above — equal metadata is
  trusted only strictly below the previous session's clean watermark.
- **Daemon-owned directories are excluded by canonical path.** Configuration
  validation already requires `state_path`, module artifact paths, and
  every daemon-owned output directory to be disjoint from every asset
  root (§18 — a staging-time error naming both paths, because a daemon
  writing inside a watched root would advance input versions with its
  own outputs: CAS writes and module rebuilds re-triggering watched
  imports indefinitely). The scanner enforces it again as defense in
  depth: it canonicalizes the daemon-owned paths during configuration staging
  and rejects any overlap or alias with a configured asset root. The scanner
  skips the configured canonical paths during traversal and surfaces an overlap
  as a named diagnostic, never scanning, querying, or publishing it. The per-root
  **quarantine directories** (below) are the one deliberate exception
  to the disjointness rule — they must live on their root's filesystem
  to receive displaced inodes by rename — and are excluded by exactly
  this canonical-path mechanism: never scanned, watched, queried, or
  published, so nothing in them can advance an input version.
- **Symlinks are followed by canonical target path.** The scanner resolves a
  directory before traversing it and requires the canonical target to remain
  inside a configured root. It carries a per-recursion canonical-path ancestry
  set: encountering an ancestor path is a named cycle diagnostic carrying the
  complete path chain and is never recursed, so cycles terminate without
  confusing them with aliases. Separately, a scan-global map records the first
  normalized physical path for every canonical directory. Encountering the same
  canonical directory at a non-ancestor path is an alias configuration error naming both
  paths: the configuration epoch is poisoned and the scan candidate publishes
  **no file, bundle, asset, query, or watcher rows**. The same-target case is
  checked eagerly across all configured asset roots (including roots reached
  through distinct spellings/symlinks) and poisons before the first scan is
  publishable. No path is silently chosen as canonical. Before opening an
  observed file or entering an observed directory, the scanner resolves it
  again, repeats containment and type checks, and after the read revalidates the
  relevant metadata/content observation. Drift retries the observation or
  produces the named scan error; a link resolving outside every configured
  root remains an error.
- **Trusted local-workspace boundary.** Configured asset roots and daemon-owned
  output directories are developer-controlled local workspaces. Distill must
  remain correct under ordinary concurrent editor, build, rename, and deletion
  activity by observing, reopening, revalidating, and retrying with
  `std::fs` APIs. It does not claim to confine a malicious local process that
  deliberately races namespace replacement between those operations. Daemon
  writes revalidate their canonical output directory and destination parent
  immediately before mutation and reject symlinked destination entries; drift
  fails or retries without publishing state. A Windows cook target does not by
  itself promise that the daemon is supported as a Windows host process.
- **Journaled rename-aside publication.** Every daemon rewrite of an
  authored file (adoption, watched imports, disk migration, rename
  fixups — and §20's generated source files) publishes by journaled
  rename-aside, verification, and no-clobber install. A multi-file operation first records one
  durable parent publication group and all child write intents in the
  `Unarmed` state. While unarmed, no child filesystem mutation is legal;
  startup may therefore abandon the complete group without executing even a
  deletion child. The daemon writes and fsyncs every non-empty proposal temp,
  then durably changes the parent to `Armed` before resuming any child. Each
  child records terminal success or failure with its retirement, and the
  parent retires only after every child is terminal. Operation-specific
  metadata may advance only from the complete child result set (for example,
  codegen installs the proposed ownership map only if every child succeeded).
  This group gate closes the crash window between recording a multi-path plan
  and producing all bytes needed to execute it.

  Each child **write-intent journal** entry —
  the target path, the temp path, the conflict path that would be used,
  the expected pre-image hash, and the proposed content hash — is
  written to daemon state and **fsynced before the first rename** of the
  protocol. The daemon writes and fsyncs the proposal temp, then atomically
  renames the user-visible target to the journal-reserved,
  store-instance-and-intent-ID-derived aside path. That destination is private
  to the journal under R37's trusted local-workspace boundary; an occupied
  reserved path is classified and replanned rather than overwritten. The
  daemon fsyncs the moved file and its directories, records the durable
  transition, hashes the displaced bytes, and compares them against the
  recorded pre-image. The aside is the
  **quarantine entry for its filesystem** — the daemon
  keeps one per watched root, and one beside each daemon-owned output
  directory (`rs_mod_path`, §20), each daemon-owned, on the *same
  filesystem* as the inodes it receives (a rename can carry a live
  inode only within its filesystem; a cross-filesystem copy would
  silently orphan an open writer's descriptor), and excluded from
  scanning by the daemon-owned identity rule above — under a name
  derived from the durable **store instance ID** and the journal entry's
  **intent ID**. Intent counters may restart after disposable daemon state is
  recreated, but the re-minted store instance prevents a retained quarantine
  name from aliasing a new intent.
  Content hashes are recorded as journal metadata, never used as
  quarantine names: two displaced inodes with equal bytes are still two
  inodes, each possibly held open by a different writer, and
  hash-naming would alias them. The journal entry records the physical
  quarantine location before the intent retires — `.distill/` keeps
  only the journal and metadata — and the inode is retained for the
  configured window
  (`displaced_retention_days`, §18 — default 7 days), destroyed only by
  journaled retention expiry or explicit `doctor clean`.

  If the aside hash matches, the proposal temp is installed at the absent
  target with an atomic no-clobber hard link; the daemon-owned temp name is
  then removed and the file and directories are fsynced. A crash between link
  and temp-name removal is recovered by recognizing the proposal at both names
  and completing daemon-temp cleanup. If the target reappears first, it is
  atomically renamed to a separately journal-reserved conflict path and never
  overwritten. The verified pre-image is restored by an atomic no-clobber hard
  link from its aside path to the absent target. The aside name remains in
  quarantine, is marked restored, and remains covered by verification and
  retention. A second reappearance wins and stops or retries restoration.
  Quarantine is what extends the no-silent-loss
  guarantee to **in-place writers holding open descriptors**: the
  post-rename hash proves only what the inode held at that instant —
  an editor writing through a descriptor it already holds can land bytes
  after verification, and those late bytes land in the quarantined inode
  and survive. Startup reconciliation re-hashes every quarantined file
  and surfaces any that no longer matches its journal entry as a
  **recovered-edit diagnostic** naming the origin path. Restoration re-verifies
  the retained pre-image, and any unknown or reappeared bytes are preserved at
  a named conflict path; nothing unverified is deleted. Target-to-aside and
  target-to-conflict moves are one atomic `std::fs::rename` from a user-visible
  name to a journal-reserved unique name. They are never implemented as
  `hard_link(source, destination)` followed by `remove_file(source)`, because an
  ordinary editor save could replace the source between those calls and be
  unlinked. Hard-link-plus-remove is used only for daemon-owned proposal temps.
  Stated precisely, the guarantee
  is: **every displacement is journaled under its intent ID and
  retained per policy** — nothing is ever destroyed silently or
  anonymously; destruction happens only by journaled retention expiry
  or explicit `doctor clean`, both of which name what they remove.
  Within the retention window every raced edit is recoverable; after
  it, the journal still records that — and what — was displaced. The durable
  states are `Prepared`, `AsideJournaled`, `TargetAsideDurable`,
  `PreimageVerifiedDurable`, `ConflictJournaled`, `ConflictPreservedDurable`,
  `PreimageRestoredDurable`, and `ProposedInstalledDurable`. Each state journals
  authority before its next filesystem mutation and records completion only
  after the affected file and directories are flushed. This is the sole daemon
  publication protocol on supported Unix hosts. Windows remains a cook target,
  not a daemon host.

  Startup recovery resumes solely from the durable state plus hashes of target,
  temp, aside, conflict, and quarantine. In `Prepared` it either begins the
  reserved aside or retires untouched; in the aside/verified states it
  classifies any reappeared target before install; in `Installed` it verifies
  target equals proposed bytes while retaining aside; and in every state an
  unknown byte sequence is moved only through a journaled reserved conflict
  transition. File flush precedes every publication that relies on file bytes;
  directory flush follows every rename/link and precedes the next durable-state
  advance. Thus crash recovery yields a named pre-image, proposal, and every
  raced target without any conforming overwrite primitive. **Deletion** (Asset CRUD's bundle delete, §17, and any
  daemon-initiated delete) is the same rename-aside-and-verify family,
  never a bare unlink: a check-then-unlink would destroy an external
  save landing between the operation's base-version check and the
  unlink — precisely the TOCTOU the atomic rename-aside closes for rewrites. The
  journal entry records the expected pre-image; the file is atomically
  **renamed into its root's quarantine under the intent's ID**, the
  displaced bytes are hashed and compared — match: the intent retires
  and the deletion publishes, the quarantined inode retained per the
  window above; mismatch: the displaced bytes are restored to the
  original path by a no-clobber retained-aside link and the operation fails as a conflict, exactly like a
  rewrite conflict. **Creation** (a new bundle from import, an editor, or §6's
  Missing-lineage repair) has no
  target to move aside: it publishes by atomic no-replace linkage
  (`O_CREAT | O_EXCL` semantics, temp linked into place) — if the
  destination appeared externally in the meantime, both objects are
  preserved under conflict naming and the operation stops or retries,
  under the same invariant: the daemon only ever deletes bytes it
  authored. Duplicate-lineage repair uses one durable parent intent naming
  every exact claimant preimage and child rewrite/quarantine intent; recovery
  completes or restores all children before the parent may publish its unique-
  survivor rescan, so an intermediate crash remains configuration-poisoned and
  can never expose an implicit winner. **Crash recovery reconciles every unfinished intent at
  startup**, before any other publication runs: each path the intent
  names (target, temp, conflict, quarantine) is classified by hashing
  what sits
  there against the journaled pre-image and proposed hashes — the
  daemon's own bytes may be deleted or reinstalled per the intent, the
  expected pre-image is restored to the target if displaced, and an
  **unclassified file is never deleted**: at worst it moves to a
  quarantine location and is named in diagnostics, with the intent
  retiring only once every named path is accounted for. Without the
  journal, a crash between rename-aside and verify would leave displaced
  user bytes at an anonymous temp path that any cleanup pass could
  destroy — so **temp cleanup is journal-driven**: the daemon deletes
  only temp files some retired intent names as its own, never "stray"
  files by pattern.

## 15. Loader & Runtime

A loader connection declares its **target once at connect**; every pull on
that connection is for that target.

### Client manifest

The client holds a local **manifest** — `asset UUID → artifact ContentHash` for
the assets it has loaded — where every entry carries the **adoption id**
it was adopted under (declared below). Consistency is **per load-dependency component** (see
Version-consistent swap): each component is single-version; the manifest as
a whole is a union of component cuts, not one global version. A packfile's
manifest *is* globally one frozen version — the degenerate case; its
identity is the manifest's own hash (§16 — a pack carries no
`InputVersion`), and RpcIO
maintains a live manifest via deltas. Loading is always manifest lookup →
content-addressed fetch.

A manifest entry is in exactly one state (declared below): `Current` —
built at this version; `Invalidated` — inputs changed, no rebuild pulled
yet; `StaleLastGood` — the rebuild failed and the prior artifact stays
served, carrying the error and its origin version; `Missing` — never built
(the normal lazy state); `Dead` — the asset was deleted (see Deletion). "One version" holds **per load-dependency
component**: a failed rebuild freezes its entire component at the previous
version (see Version-consistent swap) — failed assets marked
`StaleLastGood`, blocked-but-buildable members staying `Invalidated`; the
freeze is component-level adoption state, never a mixed edge.

```rust
pub struct ManifestEntry { pub state: ManifestState, pub adopted_at: AdoptionId }

/// Loader-minted, monotone within the loader session; a fresh id per
/// component adoption sweep. Adoption and storage key on it because no
/// daemon-side version is a universal adoption identity: `InputVersion`
/// is instance-local (§13 — the counter restarts after daemon-state
/// loss) and a pack has no `InputVersion` at all (its identity is the
/// mounted manifest's own hash, §16). One loader-owned counter can
/// never alias two adoptions, whatever IO produced them or how many
/// reconnects and remounts happened in between.
pub struct AdoptionId(pub u64);

pub enum ManifestState {
    Current       { content_hash: ContentHash },
    Invalidated   { last: ContentHash },
    StaleLastGood { content_hash: ContentHash, error: String, built_from: SnapshotStamp },
    Missing,
    Dead,
}
```

### Pull = resolve + fetch

- `resolve(uuid, at: snapshot)` — returns the artifact ContentHash for (asset,
  connection target) built against exactly that snapshot's inputs, building
  on demand; returns `Drifted` if they are no longer obtainable.
  `resolve(uuid)` is sugar: latest snapshot plus internal retry. The
  RpcIO wraps the terminal payload in exactly one `IoEvent` basis — the
  IO-neutral token (`IoBasis`, declared below): over RPC the
  instance-qualified snapshot stamp, and from a pack the mounted manifest hash
  (§16). The RPC
  basis is well-defined even when the build ran
  after the snapshot was taken, because a snapshot denotes inputs and
  acquires memoized outputs without a version change (§13). Derived UUIDv5
  outputs resolve against the same snapshot through their parent's build.
- `fetch(content_hash)` — immutable, idempotent, content-addressed; the same shape as
  a packfile index lookup.

```rust
/// The IO-neutral basis token: what a sweep's answers are answered
/// *under*. RpcIO realizes it as the adopted snapshot's stamp.
/// PackfileIO uses the mounted manifest's own "DSPM" hash (§16) — a pack
/// carries no InputVersion, which is exactly why the loader cannot key
/// consistency on SnapshotStamp. Obtained from LoaderIO::begin_sweep,
/// passed into every resolution, and carried exactly once by the outer
/// terminal IoEvent (never repeated inside ResolveResult or another payload):
/// component adoption requires all members' outcomes to carry ONE basis
/// (Version-consistent swap, below).
pub enum IoBasis {
    Rpc { snapshot: SnapshotStamp },
    Pack { manifest: ManifestHash },
}
/// A pack manifest's "DSPM" hash (§16): the mounted pack's identity.
pub struct ManifestHash(pub [u8; 32]);

pub enum ResolveResult {
    Built   { content_hash: ContentHash },
    Drifted { input: DriftedInput, current: SnapshotStamp },
    /// Build error or poison (§7): the asset exists but cannot build —
    /// manifest: StaleLastGood if a last-good artifact is held, else
    /// stays Missing. Deterministic failures are memoized at their
    /// basis (§13's failure records) — the record's trace and terminal
    /// FailureCause (§9) revalidate exactly — and repeated resolves at an
    /// unchanged snapshot answer from the record rather than re-running
    /// once per client. Never used for deletion or absence: recovery
    /// after a failed build and restoration after deletion have
    /// different swap semantics, so conflating them would run the
    /// wrong one (Deletion, below).
    Failed  { error: String },
    /// The UUID exists but is authoring/control metadata and therefore cannot
    /// be built, processed, loaded, used as a closure member, or packed.
    /// Internal strong/direct reference resolution records RoleCheck and the
    /// stable RoleIneligible fingerprint; it never collapses this to Missing.
    RoleIneligible { uuid: AssetUuid, role: EntryRole },
    /// No asset with this UUID exists at the resolved version — as far
    /// as this store instance knows, it never has. Distinct from
    /// Deleted: a reference to a Missing asset is a dangling reference
    /// (§4), not a tombstone. NOTE the loader-side mapping (Deletion,
    /// below): a handle that ever resolved and now sees Missing
    /// surfaces Deleted locally.
    Missing,
    /// The daemon's state remembers this UUID existing and observed its
    /// deletion; carries the deleting stamp. **Best-effort by design**:
    /// daemon state is disposable (§2), so after state loss a deleted
    /// UUID legitimately answers `Missing` — there are no daemon-side
    /// tombstones (they would make daemon state precious, §2), and
    /// authored tombstones are rejected (§22). The normative
    /// Deleted-vs-Missing distinction is CLIENT-relative (Deletion,
    /// below); this typed answer is the accelerator for the common
    /// live case, never the definition.
    Deleted { at: SnapshotStamp },
}
pub enum EntryRole { Runtime, AuthoringOnly }
pub enum DriftedInput {             // resolve names exactly what drifted
    File(String),
    Asset(AssetUuid),
    Query(AssetQuery),
    Dylib,
    /// The named package tool's staged directory for this basis was evicted —
    /// the snapshot's ToolEpoch mapping (§9, §13) can no longer be honored.
    /// Ambient path availability is a transient launch outcome instead.
    Tool(String),
}
```

**The pull identifier is the asset UUID.** It is the only name that exists
before a build: the ContentHash is known only after the build runs, and no
input hash can be computed client-side (dep traces are discovered during the
build; the dylib hash and resolution state live in the daemon). The protocol
is a two-step narrowing of names — `resolve` maps the stable authored name to
a content name, `fetch` maps the content name to bytes — and `resolve` is the
only place an asset ID appears; everything downstream is hash-addressed. A
packfile is `resolve` fully precomputed: its manifest is the frozen
`uuid → content_hash` table, so PackfileIO clients skip step one entirely. Dev and
ship differ only in when the first step binds.

**Laziness is a load-bearing invariant, not an optimization.** The space of
build configurations (target × API set × options) is combinatorially large,
so artifacts must not exist until pulled. Everything is addressed
accordingly: a pull names inputs (asset UUID, connection target), never
outputs; manifests and snapshots record output content hashes only after a build has
run; nothing in the protocol requires an output hash to be known before the
work that produces it.

### Watch and client-side staleness

Clients subscribe to the assets they hold. Subscription is
**cursor-bound** (§17): the client names the version its holdings were
resolved at (`since`), installation happens atomically at a returned
`installed ≥ since`, and the stream's first message is the ordered
delta covering `(since, installed]` for the named assets and paths — a
commit landing between a client's initial resolves and subscription
installation is delivered, never permanently missed. On every commit the daemon
re-hashes changed inputs and walks the reverse-dep indexes; affected manifest
entries flip `Current(content_hash) → Invalidated(last_content_hash)` and subscribers receive
a **delta**: `(stamp, affected entries)` — each entry carrying a
typed state (changed / deleted / restored, the `IoEvent::Delta` grammar
below), never a bare invalidated UUID: `ManifestState::Dead` and the
deletion policy are reachable only if the IO surface can say "deleted",
and a restored asset must be distinguishable from recovery after a
failed build — restoration revives a `Dead` handle, recovery resumes a
frozen component's swap; conflating them would run the wrong swap
semantics. Emitting the delta is the
daemon's entire response to staleness — no build is ever queued without a
pull. The client compares the delta against what it holds and decides when to
re-resolve: the loader batches once per frame, codegen-sync (§20) debounces,
a pack build never subscribes (a one-shot client: pull the closure, exit).
If a re-resolve returns the ContentHash the client already holds — the rebuild
produced identical bytes — it does nothing: early cutoff, client-side, free.

### Version-consistent swap

The loader adopts versions atomically **per load-dependency component**:
begin a sweep by obtaining its basis (`LoaderIO::begin_sweep`, below),
accumulate deltas (they compose), re-resolve all held-and-invalidated
assets under that basis, then expand — a rebuilt parent's new artifact
may carry load deps
the client has never held, so the sweep resolves newly appearing
dependencies against the same basis and repeats to a fixpoint — fetch,
and swap behind handles at one frame boundary. Component adoption
requires every member's terminal outcome to carry **one** basis: the
basis rides in every outcome (`IoEvent`, below), so a mixed-basis
component is detectable by inspection, and the rule for it is the
mid-sweep `Drifted` rule below — re-resolve every member at the newer
basis, never adopt a mix. The sweep also validates each authenticated
artifact's terminal TypeUuid and DSLH against the live descriptor, rejects a
locally build-only descriptor, authenticates its DSWL tree, and compiles the
fixup plan against that descriptor's native layout. A module epoch change
reconnects RpcIO before new work and causes held closures to re-resolve
normally. If **any** member of a
component's expanded closure fails to resolve (build error, `Missing`
child), the whole component aborts its swap and stays at its previous
consistent version — the failure freezes the *component*, never just the
single asset (only actually-failed members are labeled `StaleLastGood`;
the rest stay `Invalidated`), so a new parent is never adopted over a
stale child.
A component is computed over **weak connectivity of the union graph**:
currently adopted load-dep edges ∪ the candidate version's edges,
restricted to held assets plus newly required dependencies — direction
ignored, reverse dependents included. If held `P` and `Q` both reference
child `C`, swapping `C` under `P` drags `Q` into the same component even
with no directed path between them (otherwise `Q` would silently render
against a new child), and a newly added edge merges formerly independent
components before anything swaps. Components with no connection adopt
independently: one broken shader freezes its own closure, never unrelated
iteration. A failed merged-component swap rolls back to the pre-merge
picture: the candidate union graph is discarded, each pre-sweep component
stays at its own prior consistent cut, and only assets whose own resolves
actually failed are labeled `StaleLastGood` — components dragged in solely
by candidate edges revert to independence, never frozen as bystanders.
A commit landing mid-sweep surfaces as
`Drifted` on the affected resolves: refresh the basis (a fresh
`begin_sweep`) and re-resolve
**every member of each affected component** against it — never just the
drifted subset: members that already resolved did so at the old basis,
and a component adopts results of exactly one basis, so old-basis
successes are re-resolved rather than relabeled with the refreshed
version (a re-resolve returning the ContentHash already fetched needs no
new fetch — early cutoff makes unchanged members one round-trip, not one
rebuild). The component swaps only when every member has succeeded
against the single refreshed basis.

### One strategy: snapshot-pinned requests, client retry policy

Every resolve names a snapshot basis and succeeds only while that snapshot's
inputs are still obtainable — checked by *reading*, not pre-checking: the
build reads each input into owned bytes, hashes those exact bytes, compares
against the snapshot, and consumes those same bytes, so there is no
check-then-read race. Pipeline-code work requires an unchanged dylib hash.
**`Drifted` is a first-class result, not an
exception** — it names the drifted input and the current version. It is
never a silent rebind to latest and never served from a stashed copy of
source bytes. Pinned fetches of already-built artifacts always succeed for
the life of the lease, and drift is per-input: an edit to an unrelated asset
never drifts your resolve.

The only freedom left to a client is its **retry policy** on `Drifted`:

- **Retry-refreshed (the loader).** Refresh to the current version and
  re-resolve. Retries touch only genuinely affected *components* — every
  member of an affected component re-resolves at the refreshed snapshot
  (Version-consistent swap, above; early cutoff keeps unchanged members
  cheap), and unrelated components never drift; the loop
  terminates at quiescence; until a full sweep succeeds at one snapshot the
  client keeps its previous consistent set. Mixed versions are structurally
  impossible — every resolve in a batch names the same snapshot, so batches
  are consistent by construction, not by checking.
- **Surface (tooling).** No retry: report the drift. A pack build fails
  loudly rather than ship a mixed set; an editor refreshes when the user
  acts, not before.

The daemon's own build jobs follow the same discipline: each runs against
the snapshot current at job start (§13). The collapse rests on two facts:
drift cannot be prevented (the tree is externally mutable; the daemon only
observes it, and gating authored data would invert §2), and the past cannot
be served (code does not rewind; source stashing is rejected). What a client
does about `Drifted` — retry or report — is policy, not architecture. The
loaded set is therefore a union of per-component single-version cuts:
every load-dependency edge connects same-version members — a new parent
never renders against old children — and a component lags the newest
version only when frozen by a flagged failure — its failed members in
`StaleLastGood` with the error and origin version attached, the rest
`Invalidated` — rejoining the
sweep when a later version builds. Failure never unpublishes, and it never
mixes versions across a dependency edge.

### Deletion

A deleted asset's handle keeps its last data and enters a `Dead` state —
nothing is yanked mid-frame; the game observes the event and decides.
**Deletion is client-relative**: *deleted* means previously resolved in
this client's session or pack lineage and now absent. The daemon types
it when its disposable state observed the deletion
(`ResolveResult::Deleted`, `deleted` delta states) — a bare invalidation
could never justify entering `Dead` — but after daemon-state loss the
same UUID legitimately answers `Missing` (§2 forbids the tombstone that
could remember more), so the loader applies the mapping locally: **a
handle that was ever `Resolved` and now resolves `Missing` enters `Dead`
exactly as a typed `Deleted` would have.** A handle that never resolved
and sees `Missing` is a dangling reference (§4), never `Dead`. No
daemon-side tombstone may exist — daemon state is disposable (§2), and a
tombstone would make it precious — and authored tombstones are rejected
(§22): deleting an authored file must not require authoring another. Types
may register a **placeholder thunk** (the magenta texture, the unit
cube — `register_placeholder`, below): when one exists, the swap
replaces each dead handle's data with a freshly minted placeholder value
instead of pinning the corpse. A per-handle mint rather than one value,
because one non-`Clone` value cannot serve multiple handles — every
affected handle needs its own owned `ErasedValue` (§4) through
`AssetStorage::update` — so the deletion swap calls the thunk once per
handle. A restored file (branch switch back) arrives as a typed
`restored` delta and transitions the handle live again — distinguishable
from recovery after a failed build, which resumes a frozen component at
its next successful sweep rather than reviving a `Dead` handle.

Deletion and restoration are **component transitions**, not per-handle
side effects. A `deleted` delta invalidates the dead asset's component
like any change, and the next sweep resolves it under the ordinary
component rules. When no placeholder is registered for the type,
dependents **freeze by default**: the component keeps its prior
consistent cut, the dead member's handles `Dead` over their last data —
the failed-member freeze, because adopting a parent over a vanished
child would swap a live cut around a hole. When a placeholder thunk is
registered, adoption **proceeds**: the sweep resolves every other
member normally, mints one placeholder value per dead handle, and
commits the whole component through the normal atomic swap —
placeholder values are members of the adopted cut, never out-of-band
patches. Before any placeholder value is installed, the loader runs that
type's generated `AssetRuntimeDescriptor::encode` visitor over it with a
reference-collecting host sink. The sink ignores every non-reference event
for this check. Each emitted **strong** reference is resolved and terminal-
type-checked against the sweep's same `IoBasis`, and its edge joins the
candidate union graph. Newly discovered assets are visited the same way and
the closure/component partition is recomputed to a fixpoint before swap;
weak references do not gate this expansion. A missing, mistyped, drifted,
visitor-failed, or callback-failed reference aborts the component, so a
placeholder can never smuggle an unchecked strong dependency into an adopted
cut. Rollback follows the standard rule: a failed placeholder mint
(the thunk reports a contained panic) fails that member, the component
freezes at its prior cut, and only actually-failed members are
labeled — nothing half-adopts. Restoration is an ordinary
**new-version adoption**: the `restored` delta invalidates the
component, the next sweep resolves the revived asset like any changed
member, and the swap that commits it revives the `Dead` handles — the
typed delta states (Watch, above) are what keep it distinguishable
from recovery after a failed build, which resumes a frozen component
instead.

### Plumbing

RpcIO has **no thread of its own**. It owns a single-threaded tokio
runtime and a `LocalSet` on which capnp's `RpcSystem`, the subscription
stream, and every in-flight resolve/fetch run as tasks, and it steps them
on the engine thread whenever the loader polls it: each step runs the tasks
that are ready, polls the socket reactor with a zero timeout, and repeats
until nothing is ready or a small time budget (2 ms by default) is spent.
Calls (resolve, fetch, subscribe/unsubscribe driven by handle create/drop)
only record work; answers (content hashes, payload buffers, deltas,
errors) collect in a plain queue that `poll` hands over. Three invariants
are part of the contract:

- **The engine never waits on IO.** No `LoaderIO` call blocks on the
  daemon; a frame pays at most the step budget plus one bounded turn.
- **IO never blocks on the engine.** Answers queue for `poll`; the one
  thing gated on the loader draining them is reading a fetched payload
  that does not fit the memory budget, which waits until `poll` hands
  earlier payloads over. The engine waits on nothing the IO holds, so
  there is no cycle.
- **Limits are applied at admission.** Requests queue in RpcIO and start
  only while fewer than a configured number are in flight (fetches under
  a lower cap within it); requests beyond a limit are not started. A
  started fetch reserves its payload's bytes against the memory budget as
  soon as the daemon reports its size, before reading any of it; one that
  does not fit waits, in FIFO order, with its stream unread (the chunk
  stream is pulled, so the daemon sends nothing meanwhile). Payloads are
  held only in memory, never spilled to disk.

A sweep holds its basis, a snapshot capability, from `begin_sweep` until
`end_sweep`; `begin_sweep` returns the newest snapshot RpcIO already holds
and never waits. RpcIO keeps that snapshot current: an event that makes
the loader start a new round (a delta, a `Drifted` resolve, an expired
snapshot) is published only once a snapshot that answers it is held, and
an idle snapshot is renewed well inside the daemon's TTL. `end_sweep`
cancels by dropping: the sweep's queued and running requests are dropped
with their partial payloads, admission waits, and reservations, no answer
to them is delivered, and the snapshot is released unless it is still
current. A rebind does the same for everything of the old connection,
which fences it: nothing from the old connection follows a rebind. **Completions
correlate by request generation, never by name**: every command carries
a loader-minted, never-reused `ReqId` (declared below) that its
completion echoes; the loader keeps (handle, purpose, snapshot basis,
connection epoch) per outstanding id, and a completion whose id is no
longer the newest outstanding request for its handle — or whose connection
epoch has been superseded by a reconnect — is **explicitly discarded**.
Multiple retries, path rebindings, and reconnects may be in flight
concurrently. Target, store, protocol, and pipeline epoch checks are enforced
server-side on every target-bound capability;
without the generation, a delayed `Missing` or `Failed` from an earlier
basis could overwrite a later successful result, which uuid- or
path-keyed events alone could never prevent. The engine calls
`Loader::process()` once per frame on its own thread: drain completions,
advance handle state machines, run the adoption sweep, commit the swap
batch — so `AssetStorage` implementations need no thread safety, matching
newgameplus's single-threaded frame model. GPU-bound assets gate `Loaded`
on the engine's existing async transfer queue (two-phase: data-ready →
committed; v1's loader versioning already models this) — observable
through `AssetStorage::update`'s `UpdateResult` (below): a `Pending`
member defers its whole component's commit until every member polls
`Ready`, because a promise the loader cannot observe is a promise it
cannot enforce. PackfileIO has no
thread: mmap lookups and zstd block decompression run inline in
`process()` (blocks are ≤256 KiB, §16's canonical boundary; a decode
pool is a later optimization, not
a design change). Deserialization and fixup always run game-side, using
fixup plans registered by the asset-types crate. Fetched payload memory
is bounded by config: payload bytes held in memory (in flight or awaiting
`poll`) reserve against one aggregate budget until the loader takes them.
Admission for oversized payloads is pinned (§13's oversize records): a
payload larger than the whole budget is admitted alone, once nothing else
is held, and is read into memory like any other; admission is FIFO, so
later small payloads never overtake (and starve) a waiting large one. The
DSWL tree that completes a payload grows its reservation without waiting,
even past the budget, which then holds later admissions back until it is
released. Nothing spills to disk. Memory is freed only by the loader
taking payloads in `poll`, or by a fetch being dropped; the engine never
waits for admission, so a waiting fetch cannot stall a frame.

```rust
/// Loader-minted request generation: unique per command, never reused.
/// Every completion echoes the id of the command it answers (Plumbing,
/// above): the loader's outstanding-request table carries the basis and
/// connection epoch, and stale completions are discarded explicitly.
pub struct ReqId(pub u64);

pub trait LoaderIO {                     // RpcIO (dev) | PackfileIO (ship), no third path
    /// The basis every call of the coming sweep is issued under: RpcIO
    /// returns the stamp of the newest snapshot it holds (never waiting:
    /// an event that starts a new round is published only once such a
    /// snapshot answers it); PackfileIO the mounted manifest hash.
    /// Explicit, never ambient — the loader
    /// obtains one per sweep and threads it through every resolution,
    /// so an IO implementation can neither erase the basis nor answer
    /// two members of one component from two bases undetected.
    fn begin_sweep(&mut self) -> IoBasis;
    /// The sweep is complete or abandoned: requests issued under `basis`
    /// are cancelled (no answer is delivered for them) and what the IO
    /// held for them — snapshot, payloads, admission waits and reservations —
    /// is released. The loader retires their outstanding entries itself.
    fn end_sweep(&mut self, basis: &IoBasis);
    fn resolve(&mut self, req: ReqId, uuid: AssetUuid, basis: &IoBasis);
                                         // RpcIO: snapshot-pinned, retry-refreshed;
    fn fetch(&mut self, req: ReqId, content_hash: ContentHash, basis: &IoBasis);
                                         // PackfileIO: manifest lookup + mmap
    /// Indirect (path-late-bound) handles bind through this and nothing
    /// else — `add_ref_indirect` is implementable only through the
    /// declared IO boundary. RpcIO reaches the daemon's logical path
    /// index (§13); PackfileIO answers from the pack's path table when
    /// built with include_path_table (§16) and completes with a typed
    /// Unsupported otherwise. An `AssetPath` is a bundle path and an
    /// optional asset name (a local id): without one, the path's primary.
    fn resolve_path(&mut self, req: ReqId, path: &AssetPath, basis: &IoBasis);
    fn subscribe(&mut self, uuid: AssetUuid); // no-op for PackfileIO; RpcIO
                                              // rides Hub.subscribe's cursor
                                              // semantics (§17)
    fn unsubscribe(&mut self, uuid: AssetUuid);
    /// Path-mapping invalidation for bound indirect handles: a
    /// subscribed path whose mapping changes (create, delete, rename,
    /// primary change) arrives in `Delta.paths` and the handle
    /// re-resolves. Carried first-class by Hub.subscribe (§17).
    /// No-op for PackfileIO (a pack is frozen).
    fn subscribe_path(&mut self, path: &str);
    fn unsubscribe_path(&mut self, path: &str);
    fn poll(&mut self) -> Vec<IoEvent>;       // drained by Loader::process()
}

/// This envelope is the SOLE basis carrier. Every request-terminal event has
/// exactly one basis; nested payloads contain none. The loader compares that
/// basis with the active sweep before it even inspects/adopts the payload, so
/// a mixed-basis result cannot enter component state.
pub enum IoEvent {
    Resolved     { req: ReqId, uuid: AssetUuid, result: ResolveResult,
                   basis: IoBasis },
    PathResolved { req: ReqId, path: AssetPath, result: PathResolveResult,
                   basis: IoBasis },
    Fetched      { req: ReqId, content_hash: ContentHash, artifact: FetchedArtifact,
                   basis: IoBasis },
    /// Typed per-asset states, never bare invalidated UUIDs —
    /// `ManifestState::Dead`, deletion policy, and restoration are
    /// unreachable unless the surface can say "deleted" and "restored"
    /// (Watch, above). `paths` carries invalidated path subscriptions.
    Delta        { stamp: SnapshotStamp,
                   assets: Vec<(AssetUuid, AssetDeltaState)>,
                   paths: Vec<String> },
    /// Typed connection-level prompt corresponding exactly to the RPC
    /// envelope arm; RpcIO tears down the fenced capabilities and reconnects.
    ReconnectRequired { reason: ReconnectReason },
    RequestError { req: ReqId, message: String, basis: IoBasis },
    ConnectionError { message: String }, // not a request-terminal outcome
}

pub enum AssetDeltaState { Changed, Deleted, Restored }

pub enum PathResolveResult {
    Resolved(AssetUuid),
    Missing,                    // no mapping at this version — first-class, retryable
    /// PackfileIO without a path table: a pack built with
    /// include_path_table = false refuses indirect handles at request
    /// time, loudly — never by silently failing loads (§16).
    Unsupported,
    Failed { error: String },   // ambiguity (§18) or daemon error — never a tiebreak
}

/// §12's composite: structural bytes plus one backing per blob-table entry.
pub struct FetchedArtifact { pub structural: Arc<[u8]>, pub blobs: Vec<Blob> }
```

**Runtime compatibility is artifact-local.** An RPC connection binds one
canonical target name and DSTG. Target, store, protocol, or PipelineEpoch drift
returns the corresponding `ReconnectRequired`; RpcIO tears down the old Hub,
reconnects, obtains a fresh snapshot, resubscribes, and re-resolves held
closures. Every target-bound method is fenced server-side, so the stream event
is only a prompt and a client cannot keep using a stale capability.

PackfileIO verifies the mounted pack's target and DSTG. For both IO paths the
loader then authenticates each artifact and its DSWL tree, requires terminal
TypeUuid and DSLH to match the current local descriptor, rejects a local
`build_only` descriptor, and compiles a fresh plan against that descriptor's
native layout. Plans are owned by the resident game-module epoch and are
dropped during its drain. There is no global registry comparison, accepted
set, expansion protocol, or mutable compatibility handshake.

### Carried over from v1

`Loader` state machine per handle (metadata → dependencies → data → loaded),
ref-counted handles, indirect (path-late-bound) handles for tooling —
bound through `LoaderIO::resolve_path` (Plumbing, above), never a side
channel — the
`AssetStorage` trait (typed, deserialized data plus version for atomic swap),
per-frame polling. Every interaction flows through `LoaderIO` — RPC in dev,
packfile in release, no third path. Carried over as *shape*, not code: v1's
loader explicitly does not verify that dependencies adopted their new
versions, so the swap discipline above is a rework of its state machine,
not a port.

```rust
impl Loader {
    /// The game-side registration boundary: fetched artifacts identify
    /// their type dynamically by TypeUuid, and this table is how that
    /// UUID reaches the binary's generated tables (§4's
    /// AssetRuntimeDescriptor). A fetched artifact whose TypeUuid has no
    /// registered descriptor is a load error naming the type, never a
    /// guess; a duplicate registration for one TypeUuid is a registration
    /// error. Registrations are scoped to the game-module epoch that owns
    /// the descriptor statics: `'static` here means "for the module's
    /// resident lifetime", and the drain protocol below is what makes
    /// that sound across dlclose. Registration for a successor epoch
    /// also invalidates live RPC connections (Plumbing, above): loads block
    /// until each IO instance reconnects under the successor epoch.
    pub fn register_types(&mut self, epoch: GameModuleEpoch,
                          descriptors: &[&'static AssetRuntimeDescriptor])
        -> Result<(), RegistrationError>;
    /// The unload barrier. After this call the loader issues no new loads
    /// for the epoch's types; each `process()` sweep frees every storage
    /// adoption of those types through `AssetStorage::free`, which destroys
    /// any constructed value or transient staging state still retained
    /// (running module drop thunks while its code is still resident), frees
    /// every placeholder value the epoch's thunks minted the
    /// same way and forgets the thunks themselves,
    /// drops the epoch's compiled fixup plans, fails its in-flight
    /// fetches, and finally forgets its descriptors. Handles go `Dead`
    /// (Deletion semantics) unless the successor module re-registers the
    /// same TypeUuid, in which case pending reloads re-resolve and swap in
    /// values built through the new descriptors — asset reload composing
    /// with module reload is exactly drain + re-register + re-resolve.
    pub fn begin_module_drain(&mut self, epoch: GameModuleEpoch);
    /// True once no value (placeholder values included), plan, fetch,
    /// thunk, or descriptor of the epoch
    /// remains — and never true for a poisoned epoch (a failed drop
    /// thunk, including failure returned by `AssetStorage::free` or the
    /// consumed-on-Err `update` cleanup, §4's ErasedValue): new work is
    /// fenced immediately and the host then leaks the module rather
    /// than dlclosing corrupt state. The module host (§3) must observe this before dlclose;
    /// unloading earlier is a checked host error, since descriptors and
    /// drop glue point into the module's code.
    pub fn drain_complete(&self, epoch: GameModuleEpoch) -> bool;
    pub fn add_ref<T: AssetType>(&mut self, uuid: AssetUuid) -> Handle<T>;
    pub fn add_ref_indirect<T: AssetType>(&mut self, path: &str) -> Handle<T>;
                                         // late-bound via LoaderIO::resolve_path
    /// The asset named `name` among those imported at `path` (its local
    /// id, §10): one source can yield several assets of one type — a
    /// glTF's skeleton beside its clips "Walk", "Run" — each addressed
    /// by (path, type, name). Late-bound like `add_ref_indirect`.
    pub fn add_ref_named<T: AssetType>(&mut self, path: &str, name: &str) -> Handle<T>;
    /// Placeholder thunk for deletion swaps (Deletion, above). A
    /// generated constructor thunk, never a bare `fn() -> T`: the
    /// module-side `placeholder!` macro (the `migration_fn!` pattern,
    /// §11 — a bare fn is not registrable, so containment cannot be
    /// forgotten) wraps the body in catch_unwind and returns an owned
    /// `ErasedValue` (§4) status — a bare fn could neither catch a
    /// panic inside the module nor suppress automatic drop on the
    /// crossing value. Per-handle minting, never a single value: one
    /// non-Clone value cannot serve multiple handles, each of which
    /// needs its own owned `ErasedValue` through
    /// `AssetStorage::update` — deletion swaps mint one value per
    /// handle through `make`. A thunk that reports a contained panic
    /// fails that handle's swap member (the component freeze rule,
    /// Deletion above). Epoch-scoped like every
    /// registration: `begin_module_drain` frees every value the thunk
    /// produced through the module's still-resident drop thunks and
    /// forgets the thunk itself; `drain_complete` covers both — no
    /// placeholder vtable or drop glue can outlive dlclose.
    pub fn register_placeholder<T: AssetType>(&mut self, epoch: GameModuleEpoch,
                                              make: &'static PlaceholderThunk);
    pub fn status<T: AssetType>(&self, h: &Handle<T>) -> LoadStatus;
    /// Once per frame on the engine thread: drain IoEvents, advance handle
    /// state machines, run the adoption sweep, commit the swap batch.
    pub fn process(&mut self, storage: &mut dyn AssetStorage);
    /// Engine-local residency loss (for example GPU device loss), not a daemon
    /// or content-version event. Cancels/frees every staged and committed
    /// storage adoption while retaining handles, subscriptions, manifest
    /// content identities, and dependency graphs; cancellation of those old
    /// pending tokens is initiated and consumed by this reset path, so it does
    /// not freeze the component as an ordinary failed upload would. The loader
    /// then marks every live
    /// component for a forced fetch/construct/adopt sweep. The forced sweep
    /// bypasses the unchanged-content cutoff, because identical artifact bytes
    /// must be delivered again to repopulate empty storage.
    pub fn begin_storage_repopulation(&mut self,
                                      storage: &mut dyn AssetStorage);
}

/// Generated by the module-side `placeholder!` macro — not
/// constructible from a bare fn, so panic containment and automatic-
/// drop suppression cannot be forgotten (§3's thunk rule, §4's
/// ErasedValue contract).
pub struct PlaceholderThunk {
    pub type_uuid: TypeUuid,
    /// The host supplies the registered epoch's token explicitly. The thunk
    /// embeds that exact token in the returned ErasedValue; a missing or
    /// mismatched token is an error, never ambient epoch inference.
    pub make: fn(owner_epoch: ModuleEpochToken)
        -> Result<ErasedValue, CallbackPanic>,
}

pub struct Handle<T: AssetType> { /* ref-counted; clone = add ref, drop = release */ }
pub struct HandleId(pub u64);            // loader-internal slot behind every Handle<T>
pub struct GameModuleEpoch(pub u64);     // minted by the engine's module host (§3)
                                         // per game-module load; never reused

pub enum LoadStatus { Unloaded, Resolving, LoadingDeps, Fetching, Loaded, Dead }

pub trait AssetStorage {
    /// Two-phase (GPU gating, Plumbing above): update delivers the
    /// constructed value; commit makes `adoption` visible at the swap
    /// boundary; free retires that storage adoption. Adoption identity is the loader-minted
    /// `AdoptionId` end to end — never a daemon version, which is
    /// instance-local (§13) and absent for packs (§16), so distinct
    /// adoptions can never alias, even across daemon-state loss, a
    /// reconnect, or a remount.
    /// `Ready` means immediately committable; `Pending` returns an
    /// engine-minted token the loader polls. The two-phase promise
    /// (data-ready → committed) is enforceable only because readiness
    /// is observable here: the loader commits a component only after
    /// EVERY member reports Ready, so one Pending member defers the
    /// whole component's swap — the atomic-per-component rule,
    /// unchanged, extended through asynchronous uploads.
    /// Storage need not retain a CPU copy for the adoption's full lifetime.
    /// After it has extracted an engine-owned resource or its transfer queue
    /// owns the required staging bytes, it may destroy `value` immediately
    /// through the value's status thunk; a Pending update may do so before
    /// `poll` first returns Ready. `free` then destroys the engine-owned
    /// adoption and any transient value/staging state that still remains.
    /// No module pointer, callback, or automatic-drop value may be hidden in
    /// the engine-owned remainder after the status thunk succeeds.
    ///
    /// `value` is CONSUMED on entry on both Ok and Err. On Err storage
    /// must retain no module-backed state from it — including upload
    /// callbacks or a hidden pending value — and must destroy or
    /// deliberately leak it through its status thunk before returning.
    /// A destruction failure poisons `value.owner_epoch`; returning Err
    /// while retaining the value is a contract violation because the
    /// loader's drain accounting could never see it.
    fn update(&mut self, type_uuid: TypeUuid, handle: HandleId,
              value: ErasedValue, adoption: AdoptionId)
        -> Result<UpdateResult, StorageError>;
    /// Tokens are bound to the (handle, adoption, epoch) their update
    /// named, so a stale readiness can never gate the wrong swap.
    /// Failed fails the member exactly like a failed resolve: the
    /// component freezes at its prior consistent version (swap rule,
    /// above).
    fn poll(&mut self, token: PendingToken) -> PendingState;
    fn commit(&mut self, type_uuid: TypeUuid, handle: HandleId, adoption: AdoptionId);
    /// Freeing an adoption with an outstanding token cancels the token:
    /// the engine abandons the upload, and a later poll of a cancelled
    /// token is Failed, never a phantom Ready. Any still-retained value is
    /// destroyed through its status thunk (§4's
    /// ErasedValue — never automatic Rust drop): a drop failure reports,
    /// leaks the value,
    /// and poisons the owning module epoch, so `drain_complete` never
    /// reports true and the host leaks the module (§3's failed-unload
    /// rule) rather than dlclosing corrupt state.
    fn free(&mut self, type_uuid: TypeUuid, handle: HandleId, adoption: AdoptionId)
        -> Result<(), CallbackPanic>;
}

pub enum UpdateResult { Ready, Pending(PendingToken) }
/// Engine-minted; opaque to the loader beyond identity. Bound to the
/// (handle, AdoptionId, GameModuleEpoch) of the update that returned it.
pub struct PendingToken(pub u64);
pub enum PendingState { Ready, Pending, Failed(StorageError) }
/// Includes a contained CallbackPanic when update's consumed-value cleanup
/// fails; that variant has already poisoned the value's owner epoch.
pub enum StorageError { /* engine failure classes, CallbackPanic */ }
```

newgameplus integration: the engine's asset layer implements `AssetStorage`
over its slotmap resources; GPU upload continues through the existing transfer
queue. Asset hot-reload composes with module hot-reload — both are downstream
of the same schema system, so a schema change flows: source-walk → daemon
migrates bundles → rebuild → loader event → engine swaps data, while the
engine separately stages, migrates, and commits the gameplay module. In
authoring mode `distilld dev` may supervise compilation of both cdylibs, but
compilation ownership does not move gameplay-module loading into the daemon.

GPU device loss is a **storage-residency reset, not an asset-content change**.
New Game Plus first fences rendering and releases every reference to the dead
device while preserving its stable slotmap resource keys. It then recreates
the device and empty resource slots and calls
`Loader::begin_storage_repopulation`. That call abandons old-device pending
uploads, frees the dead committed storage adoptions, and forces every live
load-dependency component through fetch, construction, upload gating, and
atomic adoption even when its manifest `ContentHash` is unchanged. `HandleId`
and the engine resource key remain stable, while each newly materialized
storage instance receives a fresh `AdoptionId`; adoption identity names an
owned storage instance, not asset content.

Repopulation reads authoritative artifact bytes again from RpcIO/CAS in dev or
the mounted pack in release. Constructed texture/mesh CPU values and their
decompressed/staging bytes are transient: `AssetStorage::update` transfers what
the GPU upload needs and destroys the `ErasedValue` as soon as the transfer owns
or has consumed those bytes. The steady state therefore retains no second full
copy of bulk asset payloads solely for device recovery. A compact cooked
pipeline package is the deliberate exception: New Game Plus may retain its one
shared `Arc` representation after shader creation so pipelines rebuild
automatically on the replacement device without retaining a second copy or
recooking source. A transfer completion is privately fenced by the engine's
device generation, so a late completion from the dead device is discarded.
Rendering may remain behind a global recovery barrier or use the existing
dummy/placeholder resources until the forced component adoptions commit; it
must never treat a recreated-but-empty allocation as resident.
If RpcIO is temporarily unavailable, ordinary reconnect/backoff leaves the
handles nonresident until refetch succeeds; a mounted shipping pack can replay
locally. External/imported GPU objects that are not Distill assets remain their
owning subsystem's responsibility and are re-imported after device creation.

## 16. Packfile Format

Content-addressed, patch-friendly (CASC-inspired):

- **ContentHash** = raw blake3 of a built artifact — §5's deliberately
  domainless byte-identity digest, the same identity
  build results record in their output tables and `fetch` serves (§9), so
  packing is selection + encoding of the reachable set from
  root assets, never re-cooking.
- **EKey** = hash of the encoded form: a stream of independently
  decompressible zstd frames over fixed 256 KiB structural chunks (byte
  grammar, below) for **structural bytes**, plus
  zero or more **blob extents** — each `#[asset(blob)]` payload stored as
  one contiguous, uncompressed, 16-byte-aligned, unbounded extent, never
  block-split (daemon-side, a record exceeding the CAS segment cap
  lives in its own oversize segment, §13). `Blob` therefore borrows the pack mmap directly regardless
  of size (§4's promise, by construction: a range inside a compressed
  block stream is not borrowable, and a block-spanning blob would not be
  contiguous). The structural stream reassembles into an `Arc` buffer at
  load; `ConstructBlob` binds extents from the mmap. Over RpcIO, `Blob`
  borrows the fetched artifact buffer instead.
- **Per-target:** a pack is built for exactly one target platform; the
  manifest and the reachable artifact set are target-specific, so shipped
  builds carry only their own backend's artifacts.
- **Tables:** manifest (`AssetUuid → ContentHash + typed load deps`), encoding
  (`ContentHash → block EKeys + blob-extent EKeys`), index (`EKey → archive,
  offset, len`), a **wire-tree table** (`LayoutHash → canonical DSWL bytes`),
  and an optional normalized path table. The header carries the canonical
  target name, its DSTG, and authenticated archive references. Pack
  construction derives the exact closure from typed load dependencies under
  one snapshot and rejects authoring-only or build-only members using the
  pinned daemon schema. At mount, PackfileIO verifies the manifest and archive
  trailers, target/DSTG, canonical table ordering, closure edges, and DSWL
  hashes. Runtime TypeUuid/DSLH/build-only/native-layout compatibility is then
  checked per artifact against the current local descriptor (§15). A mounted
  pack's adoption identity is the manifest's own `"DSPM"` hash; a pack carries
  no `InputVersion`, and remounting a different manifest mints fresh
  loader-side `AdoptionId`s.
- **Byte grammar (normative):** every pack file — manifest,
  archive; there is no other file kind — begins
  `magic "DPK1"` ‖ format version `u32 LE`; all
  integers little-endian, and every file ends with a **per-file blake3
  trailer**: the raw, domainless byte-identity blake3 (§5) of *all
  preceding bytes of that file*, stored
  as the final 32 bytes and excluded from its own input; the reader
  verifies it before using any table (an unverified table would turn
  silent corruption into misdirected fetches rather than a named
  integrity error). The manifest file's layout is pinned whole, **to
  the byte** — every field width, ordering, and padding rule below is
  normative, so two conforming implementations produce mutually
  readable files (all integers LE, as everywhere in the format):

  ```
  magic "DPK1" (4 bytes) ‖ version u32 LE
  header, fields in this order, no padding between them:
    target            u32 byte length ‖ UTF-8 canonical target name
    target_def_hash   [u8; 32]                       ("DSTG", §18)
    archive_refs      u32 count ‖ (generation u32 ‖ file_hash [u8;32])*
                      sorted by generation ascending, generations unique
  table directory     u32 count ‖ (kind u8 ‖ offset u64 ‖ len u64)*
                      sorted by kind, each kind at most once; offsets
                      from file start; table regions contiguous in
                      directory order, no padding between them. Kinds:
                      0x01 manifest (required), 0x02 encoding
                      (required), 0x03 EKey→location index (required),
                      0x04 wire-tree (required), 0x05 path table
                      (present iff built with include_path_table —
                      optional-table presence IS directory membership,
                      never a flag byte). An unknown kind is a parse
                      error at this format version.
  tables              each: u32 entry count ‖ rows, keys sorted, no
                      padding between rows; row grammars:
    manifest   asset_uuid [16] ‖ content_hash [32] ‖ dep_count u32
               ‖ load_deps (asset_uuid [16] ‖ expected_terminal [16])
               ×dep_count, sorted/deduplicated by both fields. Expected
               terminals equal the producing result's verified
               `TraceOp::RefCheck` observations (§§9, 12).
    encoding   content_hash [32] ‖ block_count u32 ‖ block EKeys
               [32]×block_count (structural stream order) ‖
               blob_count u32 ‖ blob EKeys [32]×blob_count
               (blob-table order, §12)
    index      ekey [32] ‖ generation u32 ‖ offset u64 ‖ len u64
               (offset and len name the payload bytes exactly — the
               EKey's hash input — never record framing)
    wire-tree  layout_hash [32] ‖ len u32 ‖ canonical DSWL bytes (§12)
    path       len u32 ‖ normalized path bytes (§10) ‖ asset_uuid [16]
  trailer             blake3 of all preceding bytes of this file [32]
  ```

  **Index data is manifest tables — there are no separate index
  files**: the EKey→location index is a table of the manifest file,
  covered by its trailer and replaced atomically with it. Each table is
  length-framed with a `u32` entry count and sorted
  keys — manifest by AssetUuid bytes, encoding by ContentHash bytes,
  index by EKey bytes, wire-tree by LayoutHash bytes, path table by
  path bytes — so two writers
  cannot order one table two ways. **Archive files share the outer
  framing** — `magic "DPK1"` ‖ version `u32 LE` — followed by the
  archive header (`generation u32` ‖ encoder identity: `u32` length ‖
  UTF-8 library+version string ‖ `zstd_level i32` — diagnostics only,
  per the reproducibility rule below), then records, then the same
  per-file blake3 trailer. The **record grammar**: `kind u8` (0x01
  structural block, 0x02 blob extent) ‖ `ekey [u8; 32]` ‖ `raw_len u64`
  (decoded length — ≤ 256 KiB for blocks) ‖ `stored_len u64` ‖
  `crc32c u32` over the payload bytes ‖ zero pad to 16-byte alignment ‖
  payload `[u8; stored_len]` ‖ zero pad to 16-byte alignment. Blob
  extents store raw bytes (`stored_len = raw_len`, uncompressed,
  unbounded); block payloads are one zstd frame each. **The EKey's hash
  scope is the payload exactly as stored** — for a block, the zstd
  frame bytes; for a blob, the raw extent — and it **excludes record
  framing**: kind, lengths, CRC, and padding are transport, whose
  integrity is the CRC's job; content authentication is the EKey's.
  A future framing revision therefore cannot move any EKey, and the
  index's (offset, len) points at payload bytes, so a blob extent is
  mmap-served without touching framing. The **path table**
  (`include_path_table`, Roots below) is pinned to the byte: rows of
  `normalized logical path (§10's grammar, length-framed str), asset
  name (length-framed str: empty for the primary, else the entry's
  local id) → AssetUuid (16 bytes)`, sorted by (path, name): one
  primary row per bundle path whose primary entry is in the pack's
  manifest, and one named row per runtime entry in it — exactly the
  daemon's logical path index (§13) projected onto the pack closure at
  the build snapshot, the same mappings `resolvePath` and
  `resolveNamed` answer over RPC. Coverage and ambiguity are
  pinned with it: a path resolving ambiguously at the snapshot fails
  the pack build (§18's error — never a tiebreak row); a path whose
  primary is outside the closure is omitted, so `resolve_path` answers
  `Missing` (§15). The table rides in the manifest file — covered by
  its blake3 trailer, replaced atomically with it under patching: same
  authentication, same activation. **EKey** =
  `blake3("DSEK" ‖ version:u8 ‖ encoded bytes)` over the encoded block
  or extent exactly as stored — domain-separated like every hash in
  this document, so an EKey can never alias a ContentHash. Structural
  bytes chunk at **fixed 256 KiB boundaries** — the canonical rule;
  writers must not choose other boundaries, or identical structural
  bytes in two packs would chunk differently and dedup across versions
  breaks — and each chunk is one independent **zstd frame** (pinned:
  the zstd frame format, v0.8 / RFC 8878; any compliant encoder —
  decode compatibility is the contract, so encoder parameters may vary
  without a format break, but the frame format itself may not). Every
  structural frame is dictionary-free and self-contained: its frame header
  MUST carry no nonzero dictionary id, its declared decoded content size must
  be present and `<= 256 KiB`, and its required window size must be
  `<= 256 KiB`. Readers reject absent/oversized decoded sizes, nonzero
  dictionary ids, oversized windows, concatenated/skippable frames in one
  block payload, or any decode that does not end at exactly the declared size;
  they never consult an external/shared dictionary. Blob
  extents are **EKey-addressed objects exactly like structural
  blocks**: the encoding table names them by EKey only, and the
  physical location `(archive generation: u32, offset: u64, len: u64)`
  lives solely in the EKey→location index — extent starts 16-byte
  aligned, zero padding between extents. One addressing model, so a
  patch shipping a new manifest can reference blobs the client already
  stores by EKey without embedding placement inside an archive the
  client never downloaded. A manifest references archives by
  generation id, each paired with the archive file's hash — every file
  a manifest references is named by hash in the manifest body. An archive's
  physical filename is exactly `archive-<64hex>.dpk`, where `<64hex>` is the
  lowercase hexadecimal raw blake3 of the **complete archive file bytes,
  including its per-file trailer**. Any other spelling, abbreviated hash,
  generation-derived name, or uppercase digit is invalid; the manifest's
  `file_hash` is the same full-file hash and generation never participates in
  naming. **The
  bootstrap has no circularity**: the manifest is a plain file at a
  pinned name — `manifest-<hex>.dpk`, `<hex>` the lowercase hex of its
  **manifest hash** `blake3("DSPM" ‖ version:u8 ‖ manifest file
  bytes)` — and the root pointer file (path pinned: `pack.current`)
  contains exactly the hash as **64 lowercase ASCII hex bytes followed
  by one `\n` byte** — 65 bytes total, no other whitespace. A reader
  rejects any other length or spelling and opens the named manifest file
  directly — no index lookup to find the index, which lives *inside*
  the manifest's tables — and authenticates it by recomputing the hash
  over the bytes read, never by trust. **Activation is atomic and
  durable**, and touches exactly the two file kinds that exist: every
  newly referenced archive is built as a unique no-replace temporary in the
  destination directory, fsynced, and hashed over its complete bytes. It is
  published to the exact content-addressed name with a no-replace operation.
  If that name already exists, the publisher verifies the existing file's
  complete bytes and recomputed hash against the temporary byte-for-byte;
  only an exact match permits discarding the temporary, while any mismatch is
  a hard integrity error and neither file is overwritten. The published
  archive is fsynced and its containing directory fsynced before manifest
  publication begins. The hash-named manifest follows the **same immutable
  protocol**: write its complete bytes to a unique same-directory no-replace
  temporary, fsync it, compute the DSPM hash/name, and link/rename it
  no-replace to exactly `manifest-<64 lowercase hex>.dpk`. If that final name
  already exists, recompute its full DSPM hash and compare complete bytes with
  the temporary; only an exact match permits discarding the temporary. A
  mismatch is a hard integrity error, and the final manifest path is never
  truncated or overwritten. The published manifest and containing directory
  are fsynced. Finally activation
  writes the 65 pointer bytes to a uniquely named **no-replace** temp in
  the same directory as `pack.current`, fsyncs that temp file, renames it
  over `pack.current`, and fsyncs the directory — in exactly that order.
  Directory fsync alone makes the rename durable but does not guarantee
  the new file's data, which is why the pointer-file fsync is mandatory.
  A crash at any point leaves
  either the old or the new manifest active, never a mix and never a
  pointer naming undurable files, and activation is auditable from the
  pointer file alone: every file the manifest references is named by
  hash in the manifest body.
- **Patching:** a new manifest plus archives containing only EKeys the client
  lacks. Old archives remain valid; the index spans archive generations;
  explicit compaction repacks. Dedup across versions is automatic via
  content hashes — never across assets: the asset UUID rides inside the artifact
  bytes, so identical values under two identities are distinct content hashes by
  construction.
- **Integrity & determinism:** every block record carries raw and compressed
  lengths and CRCs. Pack construction verifies the manifest AssetUuid and
  typed load-dependency projection against each ContentHash-authenticated
  artifact header (any mismatch is a pack-build failure naming the asset). It
  also verifies every manifest load edge's
  `expected_terminal` against the producing result's persisted RefCheck trace
  and the referenced closure member, which is what lets construction and mount
  derive the identical exact closure. A loader that observes divergence between a fetched,
  ContentHash-verified header and its manifest row treats the pack as
  **corrupt** — a mount-refusing integrity failure, never a per-asset
  shrug and never a choice of which copy to trust. The decode contract pins only the zstd **frame
  format** (byte grammar, above) — any compliant reader decodes any
  writer. Reproducibility per definition is a separate promise, pinned
  at the right layer — the **decoded, logical level**: two builds from
  one `PackDefinition` at one
  snapshot reproduce identical **logical content** — the same asset
  set, decoded artifact bytes, ContentHash set, path table, chunk
  boundaries (format-pinned above), and logical manifest semantics —
  never identical encoded frame bytes, which
  `zstd_level` alone cannot pin (encoder library version, strategy, and
  internal parameters all move them). EKeys hash encoded bytes, so the
  encoding and location tables — and with them the physical manifest
  hash (`"DSPM"`) — identify a particular pack **build**, encoder
  included: the manifest hash is deliberately not promised byte-stable
  across encoder implementations. The archive header records the
  **encoder identity and settings** (library + version, level) for
  diagnostics only — never a decode contract and never a byte promise.
  Same encoder, same settings ⇒ byte-stable EKeys in practice, which
  patch dedup enjoys, but correctness never rests on it: patching diffs
  by EKey and re-fetches whatever differs. A patch activates by
  atomic manifest replacement (`pack.current`, above);
  interrupted downloads resume by missing-EKey diff.
- **Roots:** pack contents are declared by authored `PackDefinition` bundles —
  root `AssetQuery` selections (declared below) plus target, compression
  settings, and options such as `include_path_table` (default off:
  path-bound handles are dev/tooling unless a game opts in; a pack
  built without the table refuses `LoaderIO::resolve_path` with a typed
  `Unsupported` at request time — loudly, never by silently failing
  loads). A pack build
  is a one-shot client: read the definition, evaluate the root queries
  against one snapshot, pull the load-dep closure for the pack's target — a
  `build_only` type anywhere in the pinned schema closure fails the pack build —
  encode from the CAS. Root queries with `authoring_only = Some(true)` are
  rejected, authoring-only entries are removed from no implicit/default root
  set, and encountering one anywhere in closure validation is an unconditional
  pack-build error: tooling visibility can never make a control asset
  shippable.
- v1 ships a single archive, but the ContentHash/EKey split and block format are in
  the format from day one, so patching is additive tooling, not a format
  break.

```rust
#[asset(uuid = "…")]                  // built-in type
pub struct PackDefinition {
    /// Closure seeds. A root is a selection, not a reference: each query
    /// evaluates against the pack build's snapshot, the union of results
    /// seeds the load-dep closure, and a root whose result is empty is a
    /// pack-build error. AssetQuery is a plain schema-described struct, so
    /// no reference type erasure is needed; uuid-selector roots survive
    /// renames exactly as UUID references do.
    pub roots: Vec<AssetQuery>,
    pub target: String,               // named [targets] entry (§18)
    pub zstd_level: i32,              // recorded in the archive header with
                                      // the encoder identity — diagnostics
                                      // only: the per-definition promise is
                                      // logical-content reproducibility
                                      // (§16), never encoded bytes, never a
                                      // decode contract; block boundaries
                                      // are format-pinned (byte grammar
                                      // above), never per-definition
    pub include_path_table: bool,     // default false; without it the pack
                                      // refuses resolve_path with a typed
                                      // Unsupported at request time (§15)
}
```

## 17. RPC Interface

The protocol is Cap'n Proto RPC and the endpoint is **loopback-only**.
Configuration staging rejects a non-loopback daemon address. The surface
carries immutable reads, subscriptions, authoring writes, imports, migration,
and maintenance without transport authentication; remote access remains a
separate authenticated-transport design item (§22).

The checked-in `distill_rpc.capnp` schema is the normative ordinal and union
grammar. Fixed-size `Data` fields are length-checked before semantic decode:
UUID and StoreInstanceId are 16 bytes, while content, schema, target-definition,
and layout hashes are 32 bytes. Unknown union arms, bad widths, malformed
canonical records, and trailing data fail closed.

### Connection and epoch fencing

```capnp
interface Root {
  connect @0 (target :Text, targetDefHash :Data, protocol :UInt32)
          -> (result :ConnectCall);
  metadata @1 (protocol :UInt32) -> (result :MetadataConnectResult);
  lineageRepair @2 (protocol :UInt32) -> (result :LineageRepairConnectResult);
}

struct ConnectSuccess {
  hub @0 :Hub;
  instance @1 :Data;
}

enum ReconnectReason {
  targetDefinitionChanged @0;
  storeInstanceChanged @1;
  protocolEpochChanged @2;
  pipelineEpochChanged @3;
}
```

`Root.connect` canonicalizes the target name, recomputes its DSTG, validates
the protocol epoch, and requires a usable current PipelineEpoch. Success binds
the returned Hub and every child capability to that target, DSTG,
StoreInstanceId, protocol epoch, and PipelineEpoch. Drift in any bound value is
checked on every target-bound call and returns the exact
`ReconnectRequired` reason before data or a nested capability is exposed.
RpcIO then tears down the old Hub, reconnects, obtains a fresh snapshot,
resubscribes, and re-resolves held closures. Compatibility state is replaced
only by reconnecting.

`Root.metadata` is the poison-safe, target-independent bootstrap for pure
metadata inspection, diagnostics, immutable CAS fetches, and authoring
inspection. It binds only StoreInstanceId and protocol epoch and remains
available while configuration or pipeline state is poisoned. It does not run
migration, tag extraction, terminal projection, target-dependent work, or
pipeline callbacks. Its one mutation is the closed
`schemaTransition(base, payload) -> ProgressCall` recovery command. That
method can submit only the canonical `SchemaTransitionRequest` payload, never
an arbitrary `LongRunningOp`; it fences the StoreInstanceId, protocol epoch,
and exact input-version base both before preparation and at terminal
publication. This narrow exception is necessary because
`SchemaAcceptanceRequired` deliberately prevents acquiring a target-bound Hub.
It does not expose general authoring, import, disk migration, doctor, or
rename operations through the poison-safe capability.

`Root.lineageRepair` is a separate narrow local capability available only
while the current configuration poison is MissingLineageManifest or
DuplicateLineageManifest. It exposes only the exact inspection and
compare-and-swap repair operations from §6 and cannot be converted into a
metadata, target, query, build, or general authoring capability.

### Target-bound capabilities

```capnp
interface Hub {
  snapshot @0 () -> (result :SnapshotCall);
  subscribe @1 (since :UInt64, assets :List(Data), paths :List(Text))
            -> (result :SubscribeCall);
  write @2 (base :UInt64, ops :List(AuthoringOp), forceLossy :Bool)
         -> (result :DataCall);   # an encoded WriteReceipt
  import @3 (base :UInt64, request :ImportRequest) -> (result :UuidCall);
  reimport @4 (base :UInt64, bundle :Uuid) -> (result :UuidCall);
  operation @5 (base :UInt64, operation :LongRunningOp)
             -> (result :ProgressCall);
  wireTree @6 (layoutHash :Data) -> (result :DataCall);
  unsubscribe @7 (assets :List(Data), paths :List(Text))
              -> (result :VoidCall);
  authoringSnapshot @8 () -> (result :AuthoringSnapshotCall);
  importFailures @9 () -> (result :ImportFailuresCall);
}

interface Snapshot {
  version @0 () -> (result :UInt64Call);
  query @1 (query :AssetQuery) -> (result :UuidListCall);
  entry @2 (uuid :Data) -> (result :EntryMetaCall);
  resolve @3 (uuid :Data) -> (result :ResolveCall);
  refresh @4 () -> (result :SnapshotCall);
  resolvePath @5 (path :Text) -> (result :PathResolveCall);
  configuration @6 () -> (result :VoidCall);
  fetch @7 (hash :Data) -> (result :ChunkStreamCall);
  resolveNamed @9 (path :Text, name :Text) -> (result :PathResolveCall);
}

interface AuthoringSnapshot {
  version @0 () -> (result :UInt64Call);
  query @1 (query :AssetQuery) -> (result :UuidListCall);
  inspect @2 (uuid :Data) -> (result :AuthoringInspectCall);
  refresh @3 () -> (result :AuthoringSnapshotCall);
  file @4 (root :Text, path :Text) -> (result :DataCall);
}
```

A Snapshot pins one `SnapshotStamp { StoreInstanceId, InputVersion }` and an
ordinary bounded lease. Every terminal resolve, fetch, path, and delta outcome
carries that stamp exactly once as its RPC basis. Snapshot reads are mutually
consistent. Refresh returns a new capability; expiration yields a typed lease
failure, never an implicit latest read.

`resolve` maps AssetUuid to ContentHash under the pinned inputs and returns a
typed built, drifted, failed, missing, deleted, or role-ineligible result.
`fetch` is immutable and content-addressed. Artifact payloads stream through a
bounded `ChunkStream`; the adapter verifies the requested ContentHash and
serves the authenticated typed load edges with the payload. Wire trees are
looked up by LayoutHash, returned as canonical DSWL bytes, and rehashed by the
consumer.

`importFailures` (protocol 10) lists the current watched-import failures:
bundle, root, path, and message. They are memo state, not input: recording or
clearing one publishes no version, so runtime clients poll it (the RPC loader
does once a second). A bundle listed there keeps serving its last good
contents; its entry clears when a later import succeeds.

Subscriptions are cursor-bound and one connection owns one ordered delta
stream. Its first installation at `installed >= since` atomically returns the
stream's single initial delta covering `(since, installed]` for the newly
subscribed assets and paths. Later installations union more names into that
same stream and replay their retained history as ordinary ordered deltas;
they never enqueue a second stream-level initial event. A cursor older than
retained history yields `resyncRequired`; a watcher-coverage loss yields
`restartRequired`; a bound epoch change yields `reconnectRequired`. No
notification is the authority for a fence—the server checks the fence on every
call.

Authoring methods carry an InputVersion base precondition. A stale base returns
a typed conflict without mutation.

**Authoring writes are file writes.** `write` plans its batch against the
base, inside an input on the connection's writer that is then rolled back
(it holds the write lock while the plan is checked and the file written,
and commits nothing), and answers once the bundle file is atomically on
disk: a temp in the root's staging directory, fsynced, checked against the
pre-image the plan read, renamed over the target, the directory fsynced.
The store follows through the watcher and the next pass, like any other
edit. The reply is a `WriteReceipt`: each changed file as (root, path,
content hash, or none when removed). A client that needs the result waits
for the first version whose `AuthoringSnapshot.file(root, path)` answers
every receipt entry; a failure means no file changed. A second write
planned against the same base before the store has caught up finds the
file changed under it and fails without writing.

Long-running operations expose ordered, cancellable progress. A
rename-with-fixups (`operation`) is planned when it is asked for and
applied when the client consumes its Completed event, whose payload is the
receipt. Every temp is staged and every pre-image and the destination are
checked before the first rename; then each referencing bundle is rewritten
to the destination path, the moving bundle's own references are rewritten
in place, and the bundle moves by one rename. Every intermediate state is
ordinary authored input: a rewritten referencer's reference does not
resolve until the move (a build reports it), and the bundle is never at
both paths. A crash or I/O failure part-way leaves a prefix of the steps,
reported as Failed with what was applied; the same request, retried, plans
exactly what remains.

Subscription, snapshot, and authoring capabilities are bounded and leased so abandoned clients cannot retain
unbounded daemon state.

### Server threading

The daemon's listener runs on one thread and does nothing but accept. Each
accepted connection gets an OS thread of its own with a current-thread
tokio runtime and a `LocalSet`, on which that connection's `RpcSystem`, its
capabilities and its pending calls live. A request that stalls inside a
backend, or a fetch streaming a large artifact, therefore delays only its own
connection; other connections and new connects proceed. A resolve that
needs a build never runs it there: it submits or joins the node's build
cell (§13 Build cells) and awaits the ticket on the `LocalSet`, so the
connection keeps serving its other calls meanwhile, and a cancelled call
drops the ticket.

**Nothing mutable is shared between connections.** A connection owns its
front end outright: its own store reader, its own snapshots (each an open
read transaction on a reader of its own; calls on one connection at one
version share it), its own subscription set, delta queue and change-log
cursor. None of it is reachable from another thread, so none of it is
locked. What connections share is the server handle, which is `Send + Sync`
and holds no per-connection state: the store opener (configuration, identity
and CAS directory; immutable but for the operational configuration, which
every writer rereads as each transaction begins), the backends, the
publication `watch` signal, and the snapshot policy and two counters (open
snapshots and admitted connections), which are atomics. A connection opens a
store writer of its own on its first write and runs writes, import
publications and operation completions on its own thread, passing that
writer explicitly to the daemon backend; the process loop and each build
job own writers too, and nothing finds a writer through shared or
thread-local state. SQLite's write lock orders the writers. The build
backend's node cache and build cells share builds across connections, so
the RPC layer keeps no build cache of its own. The RPC layer takes no lock of its
own, so it has no lock order: the bounds are claimed by compare-and-swap,
and the only lock a connection waits on is the store's.

**Snapshots.** A connection opening a snapshot claims one slot of the
global `max_snapshots` bound by compare-and-swap. Past the bound it first
releases its own oldest snapshot; a connection holding none is refused with
a typed `ResourceLimit` rather than evicting another connection's. A
snapshot served over the wire expires `ttl` after it opens; the expiry runs
on the owning connection's thread, and releasing a snapshot (expiry, refresh
past the bound, the client dropping it, or the connection closing) returns
its slot.

**Delta fan-out.** A commit publishes by bumping the `watch` value; it
never touches a connection. Each connection waiting on its delta stream
wakes, reads the durable change log after its own cursor, filters it to its
subscriptions and fences, and advances the cursor. The writer is never
blocked by a slow reader and there is no polling. Undelivered events per
connection are bounded (1024): past that the queue collapses into one
`resyncRequired`, and a pending `reconnectRequired` is kept instead, so a
client that stops reading costs a bounded queue and a resync, never memory
or the writer's time.

**Limits and lifecycle.** `max_connections` bounds both the connection
threads (the listener closes a connection past the bound unserved) and the
hubs and metadata hubs bound through `Root.connect` and `Root.metadata`
(refused with the typed `ResourceLimit` in the existing error arm, so the
wire protocol is unchanged). A connection's thread ends when its client
disconnects; its front end, snapshots and slots are released with it. On
shutdown the listener stops accepting and closes every connection by
shutting its socket down, so capnp-rpc runs its own disconnect and releases
every capability; it then waits a bounded grace (1 s) for the threads. A
thread stuck inside a backend call is left to finish that call and exit on
its own, never joined indefinitely. A panic on a connection thread is caught
at the thread boundary, logged, and ends only that connection.

### Runtime boundary

RPC transports artifacts, wire trees, target identity, and snapshot identity;
it does not transport native layout tables or a global compiled registry. The
loader authenticates each artifact and DSWL tree locally, requires terminal
TypeUuid and DSLH to match the current descriptor, rejects a local
`build_only` descriptor, and compiles the fixup plan against the current
native layout (§§12, 15). A PipelineEpoch change reconnects the Hub. This keeps
module/schema freshness at the source-identity boundary and artifact
compatibility at the artifact actually being loaded.

## 18. Configuration

```toml
[daemon]
# Loopback only: configuration validation rejects a non-loopback address
# with a typed error at staging (§17 — the surface is unauthenticated by
# design; remote access is a named open item, §22).
address = "127.0.0.1:9999"
state_path = ".distill/"
# Displaced-inode quarantine retention (§14): every inode a rewrite,
# deletion, or conflict restoration displaces is retained in its filesystem's
# quarantine directory (per watched root / per daemon-owned output dir)
# under the write-intent journal's intent ID for this window;
# destruction is journaled retention expiry or explicit `doctor clean`.
displaced_retention_days = 7

[assets]
# Named roots: the normalized name IS the root's persistent identity —
# committed records and FILQ hashes carry it (§8); numeric root ids are
# process-local interning, never serialized. ImportRequest.root (§17)
# selects a physical destination by name — paths stay root-relative in
# one merged namespace for every read and query.
roots = { main = "assets/", engine = "engine-assets/" }
schema_path = "target/asset-schema.json"
# Source-controlled destination used only when the explicit local lineage
# repair command creates a missing manifest. Discovery still rejects any
# second SchemaLineageManifest anywhere in the configured roots.
lineage_manifest = { root = "main", path = "schema/schema-lineage.bundle" }

[modules]
# Rebuilt by `distilld dev`'s optional development supervisor; the serving
# daemon watches the completed artifact and swaps it by epoch (§3). Production
# `distilld serve` consumes an artifact produced by the project build/CI.
pipeline_dylib = "target/debug/libgame_pipeline.dylib"

[targets]
macos-dev = { os = "macos", arch = "aarch64", apis = ["metal"] }
# Cross-target cooking is gated on per-target layout emission (§22 open
# item); a non-host target like this is illustrative until that lands.
win-ship  = { os = "windows", arch = "x86_64", apis = ["vulkan", "dx12"] }

[codegen]
rs_mod_path = "../newgameplus-api/src/shaders/"
auto_codegen = true

[pipeline]
parallelism = 8
# Build-graph chain-depth cap (§9): exceeding it errors the request to
# its caller with the chain named — a scheduler outcome, never memoized,
# never a failure record; a later request with budget re-executes. The
# cap counts live executed frames only (a cache hit counts as one — the
# consult, not its recorded subtree). Valid range 1..=64; synchronous
# callback reads use native recursion on explicit 8 MiB build-worker stacks.
max_dependency_depth = 32
# Reserved batch capacity (§13): while batch-class work (pack builds,
# doctor) is pending, this many worker slots are dedicated to the oldest
# batch job — strict interactive priority elsewhere, bounded starvation.
batch_reserved_workers = 1

[cas]
segment_size = "256MiB"
cache_limit = "20GiB"

[watch]
# Optional. How long the filesystem must be quiet before the daemon
# reconciles it (§14): every change restarts the wait, so a burst is
# reconciled once, after it ends. 1..=5000 ms.
quiet_ms = 250
```

Multiple asset roots form one namespace. Paths are root-relative; a relative
path resolvable in more than one root is retained as the ordinary
`Ambiguous(roots)` index state and is an error only for path-dependent
operations (§4, §10); it never creates DSVP. By contrast, two distinct raw
physical names inside one root that collapse to one normalized path publish
§7's `SameRootNormalizedPathCollision` version poison with lossless raw-path
claimants before any file row is chosen.

A `[targets]` entry names cook parameters; the daemon resolves each entry
to a **bound `LayoutIdentity`** (§5) — for host-identity targets the
schema file's record, for cross targets the per-target layout table's own
record once emission lands (§22). The bound identity is part of the
target-definition hash (§9), so artifacts cooked under one toolchain
identity can never be served under another. The §12 gate checks the
binding against the layout table that encodes the target's artifacts; the
host check (§3, §5) separately ties module to schema, and the two sides
connect through the shared source fingerprint, never through triple
equality.

**Configuration changes are input events, never live mutation and never
unspecified restart behavior.** The daemon watches its own configuration
file; an edit to the `[assets]` roots or `[targets]` definitions lands
through the input-version mechanism (§13): the coordinator stages a
**candidate configuration epoch**, validates it whole — root names
normalized and unique, paths well-formed, the
`assets.lineage_manifest` rooted destination naming an existing configured
root, `daemon.address` **loopback**
(a non-loopback bind is a typed error at staging: the RPC surface is
unauthenticated by design, §17, and remote access is a named open item,
§22), `state_path`, module artifact paths, and every daemon-owned
output directory (`rs_mod_path` included) **disjoint from — never
nested inside — every asset root** (a violation is a staging-time
error naming both paths: a daemon writing inside a watched root would
advance input versions with its own outputs, an unbounded feedback
loop; §14's scanner excludes daemon-owned directories by identity as
defense in depth — the per-root quarantine directories, §14, are the
one deliberate exception to disjointness: they must share their root's
filesystem to receive live inodes by rename, and the same identity
exclusion keeps them out of scans and watches), target definitions complete
with non-empty API sets (§9's match rule), `parallelism >= 1`, and
`1 <= batch_reserved_workers <= max(1, parallelism - 1)` (§13's
progress rule) — and publishes the input
version pinned to it on success, exactly as pipeline epochs stage and
publish (§3, §13). Module, schemas, and target configuration validate
as **one candidate**: a target edit stages through the same candidate
mechanism as a module swap, and the candidate's pipeline map is
constructed and validated against the candidate's exact canonical
(normalized name, recomputed DSTG) rows (§3, §5) — never an opaque caller
hash —
adding or changing a target can force new chain selection and new
terminal/extras-invariance validation, so a snapshot never pairs a
target definition with a pipeline map built for a different target
set. Snapshots pin their configuration epoch alongside
their `PipelineEpoch`: existing snapshots keep resolving under the
configuration they were taken at; root changes reconcile through the
normal scan machinery (§14); and a target-bound Hub whose target
definition changed receives the connection-level
`ReconnectRequired { reason: TargetDefinitionChanged }` event on its
subscribe stream (§17): RpcIO tears down the Hub, reconnects —
`Root.connect` re-verifies target/DSTG and the current PipelineEpoch — and
resumes with ordinary `Drifted`
re-resolution. The change is connection-level, deliberately not a
`DriftedInput` variant: per-input drift would loop the retry-refreshed
loader against the same obsolete Hub, and `Failed` would freeze the
component instead of reconnecting. The event is advisory delivery; the
enforcement is §17's generation fence — every target-bound method on
the stale Hub's capabilities answers `ReconnectRequired` server-side
after the change, subscription or not. A module/schema policy change uses the
same reconnect flow under `PipelineEpochChanged` (§9, §17). A candidate that fails validation follows §7's rule —
rejection still publishes: the version carries a **configuration
poison** naming the error as snapshot-pinned
`ConfigurationState::Poisoned(reason)` alongside the independent
`PipelineState` (the prior pipeline may be valid; that fact no longer
erases the invalid configuration candidate). The prior configuration
serves only snapshots that pinned its earlier Ready version, and the next
valid edit heals. At the poisoned version, snapshot creation/refresh and
pure metadata/CAS reads remain valid; all authoring and target/config-
dependent operations return the stable typed
`ConfigurationPoisoned(reason)` carried by §17, never prior values under a
new version (§13's exact classification). The one closed exception is the
unbound local `LineageRepair` surface while the reason is exactly missing or
duplicate lineage authority; it performs only §6's absence/duplicate repair
and cannot serve or mutate ordinary authored state.

The configured source file itself remains a watched input. The native watcher
also covers the exact active `assets.schema_path` and
`modules.pipeline_dylib` paths; it watches their existing parent directories,
filters sibling and daemon-quarantine events before queue admission, and arms
replacement paths before reading or publishing a candidate that names them.
These files are re-read only after an admitted native event (or explicit
overflow/incomplete-observation recovery), never by periodic hash polling. If
the configuration source's exact raw
Unix path or Windows UTF-16 path is missing, permission-denied, not a permitted
regular file, or unreadable without I/O data loss, the candidate publishes
DSCP code 14 `ConfigurationSourceUnavailable`; deletion never means “reload
defaults.” Configuration staging enumerates every simultaneous defect and uses
§5's canonical `(code, detail bytes)` first row as the sole authority. In
particular, lineage repair is unavailable when missing/duplicate lineage is
only a later doctor diagnostic behind an earlier configuration defect.

**Every configuration key has a declared change class.** Three classes,
total over the surface — a future key must declare its class before it
ships; an unclassified key is a spec defect:

| Key | Class | Consequence of change |
|---|---|---|
| `assets.roots` | input-versioned epoch | staged candidate epoch (above); root changes reconcile through the scanner (§14); existing snapshots keep their configuration |
| `assets.schema_path` | input-versioned epoch | the schema artifact is watched at the new path; a swap lands as a schema input event (§3, §5) |
| `assets.lineage_manifest` | input-versioned epoch | normalized rooted destination for Missing-manifest repair; staging requires a named configured root and valid §10 path, and changing it cannot move or select an existing manifest |
| `[targets]` definitions | input-versioned epoch | joins the combined execution candidate (§3): the pipeline map re-validates against the new target set; bound Hubs receive `ReconnectRequired` (§17) |
| `modules.pipeline_dylib` | input-versioned epoch | module epoch rotation (§3) through the staged-candidate mechanism |
| tool registrations (§3, §9) | input-versioned epoch | ToolEpoch (§13): a complete staged package or explicit ambient toolchain identity plus DSCT hash publishes at an input version |
| `pipeline.parallelism` | operational-live | must remain ≥1; the pool resizes, re-clamps `batch_reserved_workers`, and lets already-active excess slots drain; no identity, key, or version implication |
| `pipeline.max_dependency_depth` | operational-live | must remain in `1..=64` (default 32); the next request runs under the new bound and depth exhaustion is never memoized (§9) |
| `pipeline.batch_reserved_workers` | operational-live | staging requires `1 <= value <= max(1, parallelism - 1)`; live changes re-clamp at the next scheduling decision while active slots drain (§13) |
| `cas.segment_size` | operational-live | applies to newly rolled segments only |
| `cas.cache_limit` | operational-live | eviction policy shifts; the observability rules (§13) are unaffected |
| `watch.quiet_ms` | operational-live | optional, default 250, must be in `1..=5000`; the next wait for the filesystem to settle (§14) — reconciliation and `[[rebuild]]` jobs — uses the new window |
| `daemon.displaced_retention_days` | operational-live | applies at the next retention sweep (§14) |
| `daemon.state_path` | restart-only | daemon state is disposable (§2): relocation is stop, move-or-rebuild, start |
| `daemon.address` | restart-only | rebinding requires a restart; loopback is validated at staging either way (above) |
| `[codegen]` (`rs_mod_path`, `auto_codegen`) | restart-only | the codegen service reconfigures at startup — retargeting `rs_mod_path` mid-flight would orphan generated files and their journal pre-images (§20) |

**Restart-only keys still stage and validate.** An edit to a
restart-only key follows the same staged-candidate validation as every
configuration edit — an invalid value poisons staging exactly as
above. A *valid* edit does not touch active values and does **not**
advance the input version: the coordinator records a **pending-restart
configuration generation** and the daemon enters a defined
**`RestartRequired`** state — surfaced by `doctor` and status, and as
the `RestartRequired` event on the subscription stream (§17), naming
the pending keys — while every active value keeps serving unchanged.
The input version advances only when the values actually take effect,
at restart: startup adopts the pending generation and publishes it as
an ordinary input event, so no snapshot ever observes a version whose
recorded configuration disagrees with the values that served it.

```rust
#[asset(uuid = "…")]              // schema-described (§22): editors get UI for free
pub struct Target {
    pub os: TargetOs,
    pub arch: TargetArch,
    pub apis: BTreeSet<GraphicsApi>,  // enabled API set — non-empty (§9's
                                      // match rule), canonically sorted
                                      // (§5's record encoding)
    pub optimize: bool,
    pub debug_info: bool,
}
pub enum TargetOs    { Macos, Windows, Linux }
pub enum TargetArch  { Aarch64, X86_64 }
pub enum GraphicsApi { Metal, Vulkan, Dx12, Gles2, Gles3 }
// target-definition hash (§9) = blake3("DSTG" ‖ version ‖ canonical Target
// ‖ bound LayoutIdentity) — §5's record encoding, never the config name.
// Candidate target sets retain exact name-sorted (NFC normalized name,
// recomputed DSTG) rows; normalized-name duplicates reject. Candidate,
// request, and commit compare those rows and never trust caller ordering.
```

## 19. Carried Over vs. Rebuilt

| From v1 distill | Disposition |
|---|---|
| Loader/handle state machine, indirect handles, `AssetStorage` | Ported |
| Packfile mmap loading | Ported into new pack format |
| FileTracker consistency model (dirty queue, rename log, scan reconciliation) | Reimplemented on SQLite, improved by bundle UUIDs |
| `reverse_path_refs` | Generalized into the dependency model (§10) |
| LMDB, Cap'n Proto data model, `.meta` sidecars, `SerdeObj`/serde | Replaced |
| Build pipeline (stubbed in v1) | Designed and first-class (§9) |

| From newgameplus | Used for |
|---|---|
| `source-walk` | Schema extraction — gains the attribute channel (`#[asset(uuid/rev/skip/blob/tag/build_only)]`, the full §5 `TypeAttrs`/`FieldAttrs` surface) and per-target layout emission with logical-projection equality verification (§5, §22) |
| `ngp-schema` schema model | **The one schema model, refactored in place** (§5): logical/physical record split, stable type UUIDs, `I128`, `TagEncoding` extensions; the logical-hash walk and snapshot codec land here as new modules, consumed by engine and daemon alike. `FieldShape` shrinks — container/framework classification is by type identity (§5) |
| `migrate.rs` | Matching/planning core and widening rules shared; `FieldOp` and its offset executor stay engine-owned (hot-reload) — distill's data-level `MigrationOp` executor is its own (§11) |
| Module host (`module_state.rs`: libloading, audited tables, cleanup) | Factored into a shared module-host crate consumed by engine and daemon alike (§3) — shared, never duplicated |

## 20. Worked Example: the Shader Pipeline

Replaces `compile_shaders.sh` and batch `rafx-shader-processor` runs, and
exercises every dependency kind in §10.

### Asset types

| Type | Imported from | Role |
|---|---|---|
| `ShaderStage` | `.vert` `.frag` `.comp` | stage source text, stage kind, entry point |
| `ShaderInclude` | `.glsl` | include text; `build_only` |
| `ShaderOverride` | `.hlsl` `.metal` `.gles2/3` | hand-written per-(stage, API) replacement |
| `ShaderPipeline` | synthesized at import | primary entry: stage refs, debug name |
| `CookedPipeline` | terminal | bincode `RafxPipelinePackage` in one blob field |

Shader import is a watched **directory import** (§8): an authored rules
bundle for the shader root owns the listing query, and applying it is a
batch of independent single-bundle folds — one bundle per basename stem
(`blit.vert` + `blit.frag` → a `blit` bundle whose primary entry is the
`ShaderPipeline`, with stages and overrides as sibling entries) and **one
bundle per `.glsl`, owning that `ShaderInclude` entry**. Ownership is never
shared: a stem fold reads its stage and override files only (probes
recorded, hits *and* misses); include text is imported exactly once, by
the include's own bundle; the cook reaches it through `ctx.read_path`
content deps. Editing an include three levels deep therefore re-imports
one include bundle and recooks its includers via cook-time deps; adding
`blit.frag.metal` flips a stem's recorded probe miss; dropping
`foo.vert`/`foo.frag` into the directory hits the rules bundle's listing
and spawns a new stem fold. Grouping is authoring-time state, not a
build-time convention. The `@[vertex_formats([...])]` variant
list stays where it is authored today: annotation text inside stage source
(content, not configuration). The bincode blob is the bridge form; going
schema-native later is a migration — the operation this system exists to
make cheap.

### Cooking

`ShaderCook: ShaderPipeline → CookedPipeline`, cooked strictly for the
pulling connection's target `(os, arch, enabled API set)`. The eager
`--package-vk --package-dx12 --package-metal` union is gone: a Metal dev
session cooks Metal only; a Windows pack build pulls vk+dx12.

Per (API × vertex-format variant) pass, as the current tool does: compile
with per-pass defines (`PLATFORM_*`, `VERTEX_FORMAT_*`), cross-compile via
spirv-cross, reflect per API, assemble the package with `vertex_channels`
computed in-cook. shaderc and spirv-cross link statically into the
pipeline module (§3's linkage rule), covered by the dylib hash in the
input hash.

- Includes resolve via `ctx.read_path::<ShaderInclude>` per pass —
  `#ifdef`'d includes can differ per pass; every probe records hits and
  misses. (Each `.glsl`'s own bundle carries the include text; the cook
  never touches the raw filesystem.)
- Override lookup is a **same-bundle query** (§10's `local_id` selector):
  the cook asks for the `ShaderOverride` sibling entry for (stage, API) — a
  (bundle uuid, local_id) resolution dep, negative when absent. The
  filesystem-level probe for `blit.frag.metal` belongs to the *import's*
  read-set, which creates the sibling entry; the cook sees bundles only.
- The rafx-shader-processor compiler core (cross-compile, reflection,
  codegen passes) is kept; its include resolution, override discovery,
  batch grouping, and output writing are filesystem-coupled today and get
  refactored in-repo to run behind `ProcessContext` — rafx is ours to
  change.
- SPIR-V and generated HLSL intermediates are cache-internal debug payloads
  stored under the cook's result's auxiliary-payload table (§13) —
  dumpable via `distill dump-intermediates`, never pullable artifacts,
  never in manifests; `processed_shaders/` and committed `cooked_shaders/`
  disappear.

### Rust binding codegen

An authoring-side service, like auto-migration — not a build-graph node. It
runs the `RUST_CODEGEN` reflection pass against source bundles under its own
recorded dep trace, generates `<pipeline>.rs`, diffs against the tree, and
writes only on change (gated by `auto_codegen`); the `mod.rs` chain is
maintained under a recorded query over all `ShaderPipeline` assets. A
body-only edit diffs to a no-op — no `.rs` mtime bump, no cargo rebuild
cascade. A new pipeline gets its `.rs` as soon as its import lands, with no
game demand — the bootstrap path for referencing a new shader from code.
Every source-entry hit or miss is recorded as DSTR v1
`TraceOp::AuthoringRead`, committing the owning bundle's exact raw byte
identity; query membership remains an ordinary `TraceOp::Query`. This keeps
codegen on the shared outcome-bearing trace/revalidation path without treating
authored bundle bytes as artifact ContentHashes.

Each codegen attempt returns a single `CodegenAttempt { basis, trace,
outcome }`: the pinned input/configuration basis, the complete outcome-bearing
dependency trace (query membership, hits, misses, and failures), and either
the proposed full file batch or a typed failure. The coordinator revalidates
that basis and every trace observation **immediately before** publishing the
trace/result and before beginning any §14 publication. If anything differs —
including newly discovered membership, a previously missed dependency, or an
event consumed while generation ran — it discards the whole proposed batch
and outcome and requeues against the newest basis. No stale trace is published,
and no filesystem mutation starts from bytes computed for a mismatched basis.
The trace of the last attempt whose outcome stands (published, or failed
deterministically) is kept with its pipeline epoch. At each new basis, the
service revalidates that trace first. While the epoch is the same and every
observation still holds, a run would generate what stands, so codegen does not
run. Revalidation costs the trace's reads, and a run costs the codegens plus a
publication over the whole namespace.

Generated files are **daemon-owned, declared so**: `rs_mod_path` names a
directory whose generated `<pipeline>.rs` files and `mod.rs` chain
belong to the codegen service — a hand edit there is a conflict to
surface, never content to merge. Every write publishes by §14's full
protocol, generated sources being authored-tree files like any other:
write-intent journal, then atomic rename-aside, then displaced-byte
verification against the recorded pre-image (the previously generated
bytes), the displaced inode quarantined in the output directory's
same-filesystem quarantine under its journal-assigned intent ID (§14)
for the retention window,
with §14's crash recovery reconciling unfinished intents. A
concurrent human or build-tool edit is therefore displaced, detected,
restored, and reported as a named conflict — never silently overwritten
by a check-then-write race the diff step alone could not close — and
even an IDE still writing through an open descriptor loses nothing: its
late bytes land in the quarantined inode and surface as a recovered-edit
diagnostic (§14).

Generated physical names have exactly one grammar and one authority:
`sp_` followed by the asset's full 32-character lowercase `AssetUuid` hex
(no hyphens). The Rust module is `sp_<32hex>` and its file is
`sp_<32hex>.rs`; neither contains `local_id`, title, path, or an escaped
human spelling. An optional display slug may be emitted in a comment or
side-table only if it is at most 32 ASCII bytes and matches
`[a-z0-9_]+`; it is non-authoritative, never a filename/module/key, and may
change without identity or path churn. Authored strings embedded in generated
content use ordinary Rust string escaping and never influence token or path
boundaries. Before writing anything, the generator materializes and
validates the **entire generated namespace** — filenames, module names,
and `mod.rs` entries — for collisions. Any collision is a build/codegen
failure naming every claimant, and the batch writes nothing; replacement
is never a collision policy. Before each batch the codegen service canonicalizes
and revalidates `rs_mod_path`, and it rejects symlinked destination entries;
filesystem drift fails or retries the batch without publishing state (§14).
Generated content is escaped as data: every
string literal is emitted through Rust string escaping — authored
text can never terminate a literal or smuggle tokens into generated source.

### Invalidation walk-through

Two layers fire in sequence: the watched import's read-set turns a raw-file
edit into a bundle rewrite; the bundle rewrite invalidates through the
ordinary §10 dependency kinds.

| Edit | Caught by | Work done |
|---|---|---|
| `blit.frag` body | read-set → re-import → stage content dep | recook blit on pulled targets; codegen diffs to no-op |
| `color_space.glsl` | its own include bundle's read-set → re-import → cook content deps | recook exactly its includers |
| add `blit.frag.metal` | probe miss in read-set flips → new sibling entry → same-bundle dep | recook blit on Metal targets only |
| add `foo.vert` + `foo.frag` | rules bundle's listing query → new stem fold → codegen query membership | `foo.rs` + `mod.rs` written; nothing cooks until pulled |
| edit `@[vertex_formats]` | read-set → re-import → stage content dep | recook with the new variant set |
| upgrade shaderc | pipeline dylib hash | global recook; unchanged outputs keep their content hashes, so clients and packs fetch nothing |
| toggle optimize / API set | target-definition hash in input hash | per-target recook |

## 21. Phasing

1. **Core formats + pure build**: bundle format (JSON + container), logical/
   layout hashes, SQLite + CAS, file tracking, declarative import, artifact
   encoding, loader with RpcIO. Content works end-to-end with no pipeline
   module.
2. **Pipeline module**: module hosting (§3), custom importers (authoring
   import) with read-sets and watched imports, typed processors with the
   full dependency model (content/resolution/query). Shader cooking as the
   proving case.
3. **Migrations**: schema snapshots, automatic plans (disk executor), custom
   migration bundles, `doctor`.
4. **Shipping + editor**: packfile build/patch tooling, PackfileIO, editor-
   facing RPC surface.

Phase completion is proven by one vertical game-content smoke suite, not only
by synthetic payload tests. The fixture imports a tiny image, a single-triangle
mesh, and shader source with an included file; its test pipeline module owns
the deliberately small format parsing and cooking logic. The resulting typed
artifacts pass through daemon build, loopback RPC resolve/fetch, loader
adoption, and pack build/publish/mount. Editing the shader include must
invalidate and reload only the dependent shader closure through the normal
incremental watcher path. This is an integration fixture, not a requirement to
put production image codecs, mesh optimization, or shader compilers in core.

## 22. Resolved Decisions, Risks & Deferrals

> **Historical, non-normative ledger.** Sections 1–21 and the latest
> refinement entry are authoritative. Earlier entries below preserve decision
> chronology and may name mechanisms later removed; R34 supersedes every older
> occurrence of compiled-registry attestation, accepted-set expansion,
> in-place reattestation, native-layout comparison, and target-set digests.

### Resolved

- **References are queries** (§4): uuid/path/local-id forms are query
  encodings, none canonical; broken or ambiguous resolution is a build error;
  the daemon never rewrites authored references. (Refined in R23: bare
  UUID-shaped strings always parse as UUID selectors; a UUID-shaped bundle
  path uses the explicit `{ path: ... }` form, so parsing never falls back.)
- **Processed output identity** (§7, §9): the primary output inherits the
  parent UUID; derived outputs exist only in the built namespace, reachable
  through artifact references — a consequence of the pull-based model.
- **Per-target shipping** (§9, §16, §17): artifacts cook per target and
  packs carry exactly one target — bound at both consumption boundaries:
  the pack manifest header carries the target-definition hash, bound
  identity, **and the per-type layout-registry table with its "DSLA"
  aggregate digest** (dedicated header fields — declared identity cannot
  carry a measurement), verified at mount per type against the game's
  registered set (§16's coverage rule: exact §3 `B(C)`,
  with bootstrap independently locally verified; missing or
  mismatched types error, game-side extras are ignored, equal aggregates
  short-circuit), and `Root.connect` verifies **both** sides the same
  way (the client sends its target-definition hash and per-type registry
  table; same-named targets with different definitions are rejected) —
  wrong-backend content fails at open, never at draw.
- **RPC transport** (§17): Cap'n Proto, with snapshot capabilities over the
  versioned metadata store.
- **Concurrency model** (§13): single-writer coordinator, immutable snapshot
  readers, pure build pool.
- **Loader ↔ engine contract** (§15): per-frame polling; all interaction via
  `LoaderIO` (RPC dev / packfile release).
- **Metadata consistency** (§13): the store is a deterministic function of
  the fs snapshot, updated atomically per version; tags and indexes included.
- **Migration ambiguity** (§11): always an error, never a heuristic.
- **Multiple asset roots** (§18): supported as **named** roots (the name
  is the stable id destination selection uses, §17); root-relative paths
  in one merged namespace; cross-root ambiguity errors.
- **Pull/watch protocol** (§15): resolve/fetch split, content-addressed
  fetch, client-side staleness; the daemon never builds without a pull and
  holds no authoritative client state.
- **Client snapshot consistency** (§15, §17): every resolve is
  snapshot-pinned and `Drifted` is a first-class result; the only client
  freedom is retry policy (loader: retry-refreshed; tooling: surface). Swaps
  are atomic per load-dependency component — a failure freezes its whole
  component at the previous version, so no dependency edge ever mixes
  versions; build results are never discarded (pure, input-keyed).
- **Lazy processing** (§15): pulls name inputs, never outputs; artifacts
  exist only under demand — the build-configuration space is too large for
  anything eager.
- **Pack roots** (§16): authored `PackDefinition` bundles; roots are
  `AssetQuery` selections, not references — the union of results seeds the
  closure, and an empty root result is a pack-build error.
- **Shader pipeline** (§20): worked example, including Rust binding codegen
  as an authoring-side service.
- **Migration scaffolding** (§17): `migration new` embeds endpoint schemas
  at creation.
- **Snapshot semantics** (§13, §15): a snapshot denotes inputs; outputs are
  memoized attributes acquired without a version change. Lazy builds and
  immutable snapshots are compatible by definition, not by mechanism.
  Derived UUIDv5 outputs resolve through their parent's build at the same
  snapshot.
- **Watched imports** (§8, §20): per-import `watch = true` re-runs an
  authoring import when its recorded read-set invalidates. The read-set is
  the §10 dependency vocabulary over the raw-file namespace, rediscovered
  each run — transitive includes, probe misses, and listings covered by
  construction; enumerations are `FileQuery` deps, domain-separated from
  `AssetQuery` (`"FILQ"` vs `"ASTQ"` result hashes, both versioned and
  length-framed so variable-length results can never alias by
  re-splitting). The default remains
  explicit re-import.
- **Processor cache lookup** (§9): the static-input key carries every static
  determinant of the full input hash; stored-trace revalidation (verifying
  traces) covers the rest — external tools included, as `tool(id) → hash`
  trace operations resolved through the snapshot's ToolEpoch state (§9,
  §13: the DSCT execution identity the snapshot published); in-flight
  coalescing keys on the static-input key, with
  per-waiter revalidation across snapshots. (Refined in R31: the observed
  hash is the aggregate DSCT tool identity, not the launcher byte hash. An
  unfingerprinted ambient call never commits a candidate and therefore cannot
  be a cache hit.)
- **Build results are output tables** (§9, §13): the cache unit is
  `output_key → ContentHash` plus the trace, committed atomically with a
  derived-output index (`child uuid → parent, key` — stage-free,
  **input-versioned**: derived per published version from its assets ×
  pinned pipeline map and the *only* authority for child resolution —
  historical CAS rows are memos, never namespace claims — so a
  remembered child UUID survives eviction and daemon-state loss and a
  retired one is never resurrected; commit rows verify
  against it; the producing stage derives from the snapshot's pipeline
  map) that makes
  UUIDv5 children resolvable; results pin and evict as one unit, and
  chains aggregate — pinning a terminal artifact pins the terminal primary
  plus every stage's extras. A result also owns an auxiliary-payload
  table (`debug key → ContentHash`) for cache-internal debug records; durably,
  the transaction that indexes the group is its commit — a partial output
  group publishes nothing, and an extent no row holds is released in the
  transaction that drops its last holder.
- **Static output declarations** (§9): processors declare a closed
  output-key set with types at registration; extras are terminal,
  target-invariant, and chain-unique — collision-free child UUIDs and
  static-input keys computable before work. `Outputs::extra` returns the
  child's typed `AssetRef` — the one way a processor can embed the
  reference that makes its extra reachable — `Outputs` itself is
  obtained only via `ctx.outputs()`, pre-bound to (parent uuid, declared
  set). Keys are 1–255 bytes (empty
  is the primary's reserved encoding, rejected for extras), ≤ 256
  outputs, ≤ 65,535 stages. `TargetSelector` matches by coverage
  (`target.apis ⊆ apis`, `target.os ∈ os`); overlap = all present field
  pairs intersect.
- **Component swaps, precisely** (§15): components are weak connectivity
  over adopted ∪ candidate edges including reverse dependents; manifest
  entries carry per-entry loader-minted adoption ids — "one version" is a
  per-component invariant, global only in packfiles. A failed merged swap
  discards the candidate graph and reverts pre-sweep components to their
  own prior cuts — only actually-failed assets go `StaleLastGood`, never
  bystanders.
- **Tag poisoning** (§10): a failed `load_current` during indexing fails
  every tag query that could match the entry, naming the poisoned
  bundles — queries never silently under-approximate.
- **Build-import keys are per entry** (§8): the `"DSBI"` pre-key carries the
  entry's asset UUID, bundle uuid, local id, and authored *and terminal*
  types — same-typed sibling entries can never share an artifact — and
  typed-reference resolutions live in the entry's discovered trace with
  the referenced asset's
  observed terminal type, so a pipeline-map retype invalidates exactly the
  affected entries (no whole-map hash term: that would over-invalidate).
  Publication plus every cache hit re-check the artifact header's
  `asset_uuid` and `authored_type`/`terminal_type` against the requested
  entry and the current pipeline map.
- **Extra-output key sets are target-invariant** (§9, §13): every target
  chain for a parent type declares the same extras (keys and types),
  checked at pipeline-map construction — the target-free derived index
  stays well-defined and remembered children resolve on every target.
- **Sets are first-class** (§5, §6, §11, §12): `HashSet`/`BTreeSet` get their
  own grammar node (0x0E) and `BTreeMap` shares the map node — identity
  is structural; JSON encodes sets sorted by element bytes with
  duplicates a parse error; fixup constructs by real insertion
  (`ConstructSet`), duplicate elements an integrity error surfaced
  through `PushError::Duplicate` (distinct from callback panic); the
  DSWL set node carries its element node exactly like vec's (an element
  relayout must change the layout hash); `VarRef.len` is the element
  count; `MigrateElements` covers set elements with post-migration
  duplicates an error, mirroring the map-key rule.
- **Result records are tagged by key kind** (§8, §13): processor results
  key on the `"DSSI"` digest with trace + output table; build-import
  results key on the `"DSBI"` pre-key digest with the entry's single
  artifact row plus the discovered trace `load_current` produced
  (reference resolutions, plan-selection queries) — three lookup keys
  total (§9's orientation table), neither grammar shoehorned into the
  other.
- **The disk migration planner is structural** (§11): it diffs snapshot
  ASTs (fields/variants by name, containers by position — the logical
  hash's own equivalence); the entry's type UUID only selects the root.
  Nominal `TypeDef` matching is the engine hot-reload planner's, where
  live `TypeDef`s exist on both sides.
- **Fixup rollback and tag reads are fully named** (§12): each plan
  carries `whole_drop: Option<DropId>` into a generated whole-value drop
  table (containers and skip slots have their own paired drops), and
  `SwitchVariant` reads its wire tag through `WireTagRead`
  (canonical-u32 or the wire's in-place discriminant) — a separate axis
  from `NativeTagWrite`.
- **Registration keys are single-owner** (§3): duplicate importer IDs,
  tool ids, migration-fn keys, default tables, schemas, or processors
  per (input, target) are epoch-opening errors — never last-write-wins;
  only validators are additive. Debug keys share the 1–255-byte output-key
  bound, checked at result binding (§9).
- **Schema code is shared, refactored in place** (§5, §19): ngp-schema
  splits logical records from per-`LayoutIdentity` layout tables
  (surfacing today's silent layout overwrite in `merge`), gains type
  UUIDs, the attribute channel, `I128`, and `TagEncoding` extensions; the
  logical-hash walk and the snapshot codec live in ngp-schema, hashing
  the model records directly — never their serde form. Serializability
  classifies by type identity (well-known paths + attributed UUIDs), so
  `AssetRef`/`WeakAssetRef`/blobs never enter the shared model, and
  `FieldShape` shrinks rather than grows. `FieldOp` stays engine-owned;
  distill's migration executor interprets the logical `MigrationOp` plan
  over `AuthoredValue` (§11).
- **Deterministic maps** (§5, §12): serializable maps require a
  fixed-seed `BuildHasher` (std `RandomState` is unserializable) — the
  framework injects no entropy the input hash cannot see, so decoded map
  iteration is reproducible.
- **Query self-containment** (§10): bare `local_id` selectors are
  bundle-relative and closed over their origin (`bundle_uuid`) before
  evaluation or trace recording; context-free surfaces (snapshot queries,
  pack roots) reject unclosed queries. Path APIs never erase distinctions:
  cross-root ambiguity and I/O failure are errors, distinct from
  first-class misses.
- **Identity uniqueness is checked** (§7): duplicate bundle or asset
  UUIDs are integrity errors naming both paths; duplicate type UUIDs are
  schema-load errors; the authored ∪ precomputed-derived UUID union is
  validated atomically at every input-version publication (before the
  version is queryable — resolution is never ordering-dependent), and
  derived-output commits verify against the precomputed index —
  collisions checked, never trusted to probability.
- **Module ABI & staging** (§3, §9): a C-ABI identity bootstrap
  (serialized bytes, status codes — one of the calls before the checks
  pass) fronts a Rust-ABI table gated by `ModuleAbiIdentity` (rustc, the
  shared host-interface build fingerprint, a measured digest over the
  boundary types themselves, panic strategy, system-allocator contract —
  the daemon's own record vs the module's, since asset-layout identity says
  nothing about the host interface, whose types cross by value with
  cross-boundary ownership). A second C-ABI probe carries the complete
  compiled rows/DSCA; its DSNL field makes the old separate
  `measured_layouts` probe redundant, so that probe does not exist.
  Panics are contained inside the
  module by generated wrappers — every generated fn-pointer table
  (defaults, skip-writers, ctors, `MigrationFn`) holds catch_unwind
  thunks whose drop entries leak-and-report rather than unwind — and a
  failed `unload` leaks the module
  rather than dlclosing corrupt state. The host itself is one shared
  crate factored out of newgameplus's `module_state.rs`, consumed by
  engine and daemon alike — shared, never duplicated. Dylibs are **staged,
  content-addressed** — copy into daemon state, hash the copy, and load exactly
  the copy the hash names. Package tools similarly stage their complete package
  directory; ambient tools are explicitly outside byte staging and are
  nonmemoizable unless a trusted fingerprint is declared. Tool registration
  publishes input-versioned ToolEpoch state (§9, §13), so snapshots pin the
  selected registration. (Refined in R23: the C-ABI
  bootstrap also exports the complete sorted per-type compiled attestation
  and `DSCA` aggregate — TypeUuid, DSLH, DSNL, build_only, and every remaining
  excluded semantic/policy bit — checked before registration and on reload;
  every reverse host callback is independently contained by a host-side
  status thunk.) (Refined in R31, then simplified in R32: dylibs retain byte
  identity; DSCT now identifies either a complete package-directory snapshot or
  an explicit ambient toolchain registration.)
- **Version domains** (§13): input versions (fs/code/authoring) and the
  memo sequence (build commits) are separate; builds never advance an
  input version. The store partitions by the same authority:
  input-versioned metadata, monotone basis-keyed memos (old snapshot
  reading newer memo *is* the memoization semantic), and unversioned
  ephemera — with memo rows never readable before their input basis.
  Epoch rotation is an input event: snapshots pin their `PipelineEpoch`,
  and a drained epoch surfaces as `Drifted(Dylib)`, never a silent
  re-pair with newer code.
- **LayoutIdentity + semantic attestation** (§5, §12): a layout table records
  only target triple, rustc identity, and source-walk algorithm version under
  `"DSLI"`; it selects/binds a physical table and is not a reconstructed Cargo
  dependency closure. Module acceptance instead compares the independently
  derived complete CompiledTypeRow/DSCA tables, whose rows directly cover
  TypeUuid, DSLH, DSNL, build-only policy, and RegistryExtras. Pipeline-only
  behavior remains covered by the dylib hash and host-interface safety by
  ModuleAbiIdentity. All identity records hash by the normative
  canonical record encoding (`"DSLI"`/`"DSMA"`/`"DSTG"`, §5), and the same
  encoding serializes every hashed composite — static-input keys (`"DSSI"`,
  the CAS record key is the digest, §13), traces (`"DSTR"`), queries,
  output tables — so `‖` formulas can never repartition
  variable-length fields.
- **Tag indexing under lazy migration** (§10): extraction at current schema
  through `load_current`; migration-defaulted fields index **at their
  loaded values** — query semantics equal loaded-value semantics, so disk
  migration can never change a query result (provenance is diagnostics
  only); each
  index entry records every load input (migration bundles, plan-selection
  deps, planner version, dylib hash, annotation epoch); tag markers stay
  out of schema snapshots so hash → content stays unique.
- **Migration graph** (§11): custom edges only and **mandatory** — a greedy
  walk follows them from the entry's schema (branching or revisits are
  ambiguity errors) with at most one trailing automatic segment, so a
  destructive automatic diff can never shadow a custom edge; plan selection
  records per-visited-node edge-set deps; `Migration` itself bootstraps via
  `format_version`, never through its own machinery. (Refined in R25:
  Migration entries are authoring-only, so edge discovery uses the
  coordinator-private, basis-traced `ControlSnapshot` query rather than
  ordinary `AssetQuery`; runtime role filtering can no longer erase the
  mandatory graph.)
- **Semantic revisions** (§4, §5): `#[asset(rev = N)]` participates in the
  logical hash, making same-shape meaning changes migratable.
- **Manifest states** (§15): `Current / Invalidated / StaleLastGood /
  Missing / Dead`; a failure freezes its whole load-dep component — failed
  assets `StaleLastGood`, blocked members `Invalidated`, the freeze being
  component-level adoption state; swap batches expand closures to a
  fixpoint before committing.
- **Weak references** (§4): `WeakAssetRef<T>` — build-resolved,
  artifact-encoded, never load-gating; the cycle escape hatch.
- **Load-DAG enforcement in closure walks** (§9): commit-time cycle
  checking is impossible under laziness, so every closure expansion
  (`ctx.read` recursion, loader sweeps, pack builds) carries a visit stack
  and errors on cycles with members named — replacing v1's deadlock.
- **Artifact wire format** (§12): a derived **wire layout** with normative
  recursion rules — native layout except `VarRef`/`BlobRef` slots and the
  three-case enum rule (single-variant untagged; fully-flat native;
  otherwise canonical `u32` tag = name-sorted variant index), offsets
  repacked where size *or alignment* diverges, all wire padding zero, flat
  runs scatter-copied; 8-byte `VarRef`, `SwitchVariant`,
  constructed-value-stack rollback, pinned DSTL byte grammar with a blob
  table and blob section, composite ContentHash verification with synthesized
  inter-blob zero padding, canonical ordering for unordered data (sorted
  deduped `load_deps`, key-byte-sorted map entries), ≤4 GiB variable
  section, no target field. The
  layout hash — `"DSWL"`, over the wire tree — carries all
  target-dependence and hashes **semantic identity**: field and variant
  names, native tag offsets/values, and pointee nodes under every
  indirection ride in the nodes, so same-geometry reorders, discriminant
  reassignments, and element-stride changes never alias. DSWL is
  wire-only by design: fixup plans key on (type, wire layout hash,
  **fixup-table identity** — the binary-local §5 identity covering
  `CtorId`/`DropId`/`SkipDefaultId` assignment, never the cross-binary
  measured digest, which is layout-only), so consumer relayouts
  invisible to wire bytes
  (skip-field shifts) drop plans without touching artifacts;
  `ValidateScalar` gates `bool`/`char` bit patterns before flat-copied
  memory counts as initialized. DSWL recursion terminates by §5-style
  back-references; fixup plans form an arena (recursive types close
  cycles through indirections), constructing and rolling back through
  the consuming binary's generated ctor/drop table (`CtorId`) — never
  from offsets alone — with a **framed** rollback stack: completing an
  aggregate disarms its children's entries and pushes one whole-value
  drop-glue entry, so nothing double-drops and no custom `Drop` is
  skipped. The header carries authored, terminal, **and encoded** type
  UUIDs — mid-chain artifacts encode intermediates that neither
  endpoint UUID identifies.
- **Watched-import workflow** (§8): bundles rewritten by watched imports
  stay committed — identity must survive checkout — so a raw-source edit is
  a two-file commit. Import is a fold, `import(sources, settings,
  Option<prior bundle>)`: content from sources, identity and settings from
  the prior bundle, deterministic up to fresh UUID mints. Sync is therefore
  a byte-identical fixpoint, verified by `doctor` and CI. The built-in
  `ImportRecord` entry is the provenance carrier — importer id, sources,
  watch, read-set — so re-import reconstructs from the file alone.
  Imports are requested by explicit `ImportRequest` (importer id, sources,
  dest, settings, watch — never sniffed from extensions); `reimport`
  re-runs a bundle's recorded `ImportRecord` (§17).
- **Validators** (§9): typed, single-asset, bound by the determinism
  contract; two severities; advisory at authoring time, publishability
  gate at build import; part of build identity via the dylib hash.
- **One bundle-load path** (§11): `load_current` is the single
  deserialize-and-migrate implementation, shared by build import, tag
  indexing, validators, and disk migration — no consumer-specific loaders.
  Its `LoadContext` pairs a pinned snapshot with the snapshot's own
  pinned `PipelineEpoch` (§3, §13) — schemas, module, pipeline map, and
  planner version validated together, structurally impossible to mix
  across epochs or against a foreign snapshot.
- **Determinism contract** (§9): pipeline code is trusted, not sandboxed —
  all inputs through the context; every reproducibility claim holds under
  the contract; double-run and `doctor verify` are mitigations.
- **Logical-hash grammar** (§5): pinned normatively in this document —
  versioned, domain-prefixed binary AST encoding.
- **Blob-in-pack** (§16): each blob ships as one contiguous, stored,
  unbounded extent — never block-split — so `Blob` borrows the pack mmap
  at any size; RpcIO blobs borrow the fetched buffer. The DSTL blob
  section is the split point: pack builds map blob-table entries to
  extents one-to-one, and fetch reassembly verifies against the raw ContentHash
  with inter-blob zero padding synthesized from the blob table (§12).
- **Directory imports** (§8, §20): an authored rules bundle owns the
  listing; application is a batch of independent single-bundle folds; each
  `.glsl` include is owned by its own bundle — stem imports never copy
  include text. Listing loss orphans a generated bundle (doctor-listed,
  user-deleted), never auto-deletes; output paths are deterministic and
  collisions are errors.
- **CAS authority** (§13): SQLite is the authority on what committed and
  which segments exist; recovery holds each segment to its index; the
  record grammar is pinned, with asset UUID and output key in every record.
- **Search tags** (§4, §10): `#[asset(tag)]` authored fields; bundle-level
  re-indexing on dirt.
- **Blob runtime type** (§4, §12): `Blob`, an `Arc`-backed byte range; pack
  loads borrow the mmap.
- **Target** (§18): schema-described struct; named instances in config.
- **Sparse authoring** (§6): adoption materializes omitted fields; the
  canonical on-disk form is always total.
- **Deletion** (§15): handles go `Dead` keeping last data; optional
  per-type placeholder factory (epoch-scoped, minting per-handle
  values); restored files revive.
- **References policy** (§4): tools prefer the UUID form; path-ref fixups
  run as long-running operations, never silently.
- **Lazy migration** (§11): migration executes inside build import; disk
  migration is an explicit command only.
- **Schema/module pairing** (§§3, 5): `source-walk` owns workspace observation;
  the daemon watches its emitted schema and module artifacts and accepts only
  a shared `ngp-source-hash` identity match. It does not crawl the workspace or
  maintain a separate source-staleness mode.
- **Concurrent writes** (§17, §14): version-preconditioned, rejected on
  mismatch; per-file publication is rename-aside, verify, and no-clobber
  install/restore under one
  invariant — the daemon only ever deletes bytes it authored; anything
  unverified is preserved as a named conflict file; creation publishes
  by atomic no-replace linkage with the same conflict discipline.
- **Dylib orchestration** (§§3, 18): `distilld dev`'s optional outer supervisor
  owns `source-walk` and Cargo child-process lifetimes and may build both the
  pipeline and gameplay cdylibs. The serving daemon swaps only the pipeline
  module by epoch; the engine stages/migrates/commits the gameplay module.
  `source-walk` and Cargo retain workspace invalidation ownership, so this adds
  no daemon Cargo crawl or second freshness model.
- **Pack path table** (§16): `include_path_table`, default off.
- **Declared surface** (§3–§18): every structure and API named in this
  document carries a normative declaration — the module table,
  `Registry`, `DefaultTable`, `ModuleAbiIdentity`, and `PipelineEpoch`
  (§3), identity newtypes,
  `AssetRef`/`WeakAssetRef`/`Blob`
  native forms, and `AssetRuntimeDescriptor` (§4), `LayoutIdentity` and the schema AST (§5), the
  bundle/`AuthoredValue` data model (§6), importer/processor/validator
  traits with their contexts (§8, §9), `ImportRecord`/`FileDep` (§8),
  `StaticInputs`/`TraceOp` (§9), `AssetQuery`/`FileQuery` (§10, §8),
  migration types and `load_current`/`LoadContext` (§11),
  `FixupPlanArena`/`CtorId` (§12), version newtypes and
  `MetadataSnapshot` (§13), manifest states and
  the `LoaderIO`/`Loader`/`AssetStorage` surface (§15), `PackDefinition`
  (§16), the RPC interface sketch and typed `AssetEvent` (§17), and
  `Target` (§18). Declarations pin shape; prose defines semantics.
- **Full attribute channel** (§4, §5, §10, §19): `TypeAttrs` carries
  `build_only`, `FieldAttrs` carries `tag` — the registry derives the
  type→tag-field map and load-closure validation from the schema, not
  from convention; neither bit enters the logical hash (indexing and
  policy, not interpretation).
- **Plans bind to generated-table identity** (§5, §12): the
  **fixup-table identity** (never the measured layout digest, which is
  layout-only and cross-binary) covers the binary's
  `CtorId`/`DropId`/`SkipDefaultId` assignment (each ID paired with the
  nominal monomorphized key and DSNL identity of what it constructs,
  drops, or writes), so a rebuild that reshuffles generated tables
  without moving layout changes the fixup-table identity and orphans
  cached fixup plans — never epoch bookkeeping, never a
  wrong-constructor execution.
- **Game-side descriptor registry** (§4, §12, §15):
  `AssetRuntimeDescriptor` (both §5 identities, ctor/drop/skip-writer
  tables), generated by `#[asset]`, exposed via `AssetType::descriptor()`
  and registered with `Loader::register_types` — the declared boundary a
  fetched artifact's `TypeUuid` crosses to reach its fixup machinery;
  unregistered type = load error, duplicate = registration error.
- **Asset shapes are target-invariant** (§5, §18, §22): per-target
  emission verifies every asset-reachable type projects to an identical
  logical schema under every configured target's cfg evaluation;
  divergence is an extraction error — logical identity is never
  per-target, only layout is.
- **Import output is a total replacement set** (§8): bundle content
  entries after the fold are exactly the returned `local_id`s — absent
  entries delete and retire their UUIDs; duplicate `local_id` and double
  `primary()` are `ImportOutputError`s; a vanished prior primary with no
  declared replacement fails the fold rather than guess what paths
  resolve to.
- **Physical file tracking is per root** (§8, §13, §14): `files` keys on
  (root id, normalized path); the logical namespace derives as a
  multimap with `Missing`/`Unique`/`Ambiguous` states, and file deps
  evaluate over it — a transition into ambiguity invalidates recorded
  `Unique` results the same as a content change.
- **DSSI buckets, not slots** (§9, §13): one static-input key maps to a
  bucket of result candidates keyed secondarily by trace digest; lookup
  revalidates most-recent-first, commits append, recovery rebuilds the
  whole bucket — memo monotonicity holds even when two snapshots share
  statics but resolve dynamics differently, no rebuild ping-pong.
- **Two native identities, disjoint jobs** (§5, §12, §16, §17): the
  measured layout digest is layout-only and cross-binary (registration,
  pack-mount, connect); the fixup-table identity extends it with the
  binary's generated-table assignment — every plan-interpreted index
  (`CtorId`, `DropId`, `SkipDefaultId`) paired with the nominal
  monomorphized key of what it constructs/drops/writes (nominal for
  injectivity over DSNL-identical types, DSNL identity riding along) —
  and keys exactly one thing: fixup-plan caches. Conflating them would
  make cross-binary checks unsatisfiable or plan caching unsound.
- **Descriptors are sufficient and epoch-drained** (§4, §12, §15): 
  `AssetRuntimeDescriptor` carries both digests, the expected logical
  hash, the generated `NativeLayoutNode` tree (table ids annotated in
  place — plan compilation's native-side input), size/align, the three
  tables, and a `finalize` type-erasure fn producing
  `AssetStorage::update`'s owned value. Registrations are
  `GameModuleEpoch`-scoped: `begin_module_drain` stops new loads, frees
  the epoch's values through the still-resident drop glue, drops its
  plans and descriptors; `drain_complete` gates dlclose (unloading
  earlier is a checked host error). Asset reload composes with module
  reload as drain + re-register + re-resolve.
- **Target invariance is registry-projection equality** (§5): per-target
  verification compares logical AST, type UUID, and the full attribute
  channel including unhashed policy bits — DSLH equality alone would
  admit cfg_attr-varied UUIDs and policies that the shared TypeDef
  cannot represent.
- **Extras' header triple is pinned** (§9, §12): for a derived extra,
  authored = terminal = the declared extra output type; provenance lives
  in the derived-output index, never the header. `build_only` judges
  each closure node by its own type — authored assets by authored type
  (processing never launders a build-only asset), extras by their
  declared output type. (Refined in R22: `encoded_type` is also the
  declared type unconditionally; extras encode directly and never enter
  that type's processor chain, §§9, 12.)
- **Rejection still publishes** (§7, §13, §15): identity-validation
  failure publishes a poisoned version — `current` advances, resolves
  against it fail deterministically naming the collision, events fire —
  so retry-refreshed clients terminate in a stable error instead of
  spinning on drift, and last-good artifacts stay fetchable.
- **Physical keys everywhere** (§13): `bundles` rows carry
  (root id, normalized path) like `files`; reserved `$`-prefixed
  local_ids (`$settings`, `$record`) fence daemon-owned entries from
  importer output; `ByStem` groups key on (root, directory, stem).
- **Snapshots pin module residency** (§3, §13, §15): `PipelineEpoch`
  holds module-owned code, so `dlclose` gates on the epoch `Arc`'s
  strong count — snapshots, job contexts, and RPC capabilities all
  count. "Drained" stops new work; **unload** waits for the last pinned
  reference. A stale snapshot resolves `Drifted(Dylib)`; it can never
  dangle into unloaded code.
- **Rollback drops are named, never guessed** (§4, §12): every
  aggregate `NativeLayoutNode` carries `whole_drop: Option<DropId>`;
  `ConstructString`/`ConstructBlob` roll back through the executor's
  built-in drop for values it itself constructed; the fixup-table
  identity covers `whole_drop` assignments like every other table id.
- **The native grammar is total** (§12): `ScalarKind` spans every
  primitive (Bool/Char stay the only validated kinds), arrays are a
  node with len/stride, every node carries measured size/align, and
  field order is pinned as (offset asc, declaration index asc) — no
  "native order" ambiguity under reordering or equal-offset ZSTs.
- **A hash is not the tree** (§12, §13, §16, §17): wire trees persist
  in daemon state under their DSWL hash, serve over `Hub.wireTree`, and
  ship in a pack wire-tree table; consumers recompute the DSWL hash and
  require equality with the artifact header before compiling a plan —
  authenticated by recomputation, never trust.
- **Aggregate layout checks have one grammar** (§5, §16, §17): sorted
  `(TypeUuid, measured digest)` pairs under
  `blake3("DSLA" ‖ version ‖ count ‖ entries)`; coverage pinned per
  boundary (pack mount: the pack closure union bootstrap vs the game's
  registered set; connect: the client's runtime set union bootstrap vs the
  daemon's target table, with bootstrap independently locally verified);
  missing/mismatched types error naming the type, counterparty extras
  are ignored, equal aggregates short-circuit. (Refined in R30: both sides now
  derive exact §3 `B(C)` including authored policy, artifact encoded/terminal,
  load-edge expected-terminal, and bootstrap types.) (Refined in R31: packs
  stay exact; RPC attests accepted superset `A` and derives actual
  `B(served)` only for the per-response subset gate.)
- **Multi-root identity reaches file deps** (§8, §13): `RootedPath`
  (root + normalized path; serialized and hashed as the normalized
  root name, never an ordinal) in `sources()`, `enumerate`, `FILQ`
  hashes (root-name-framed), and `FileDep::Content`; `Probe` records the
  observed root — identical bytes moving between roots invalidate,
  because destination and group identity moved.
- **One normalization boundary** (§7, §9, §10): local_ids, output keys,
  and tag names/values NFC-normalize at intake, uniqueness runs
  post-normalization, and UUIDv5 hashes exactly those normalized bytes —
  no raw/normalized seams between uniqueness, indexes, and identity.
- **Path grammar is total** (§10, §13, §14): normalized paths are
  non-empty NFC components, no `.`/`..`/NUL/`\`/absolute/doubled
  separators, containment lexical by construction; same-root physical
  names colliding post-normalization are a scan-time DSVP integrity error with
  exact raw platform-path claimant bytes before any row insert. Cross-root
  equality remains the representable `Ambiguous(roots)` logical-index state,
  never a version-global poison.
- **Version poison is API-representable and uniform** (§7, §13):
  `MetadataSnapshot::poisoned()` plus every namespace-facing operation
  failing with the same version-global error — `entry` returns
  `Result`, never an `Option` that conflates absent with unanswerable;
  no consumer can observe a partial projection of a poisoned version.
- **`finalize` is `Box<dyn Any>`** (§4, §15): the engine model is
  single-threaded and `AssetType` requires only `'static`; undeclared
  `Send + Sync` bounds would exclude thread-affine skipped/GPU fields.
  (Superseded in R21: `finalize` yields `ErasedValue` — the
  no-auto-trait-bounds rationale stands, but a bare box's destructor is
  raw module drop glue, so ownership now crosses only through status
  thunks with automatic drop suppressed, §4, §15.)
- **`$` reservation at every boundary** (§6, §8, §17): parser,
  adoption, editor CRUD, and RPC writes reject unrecognized
  `$`-prefixed local_ids; `$settings`/`$record` are accepted only with
  their exact built-in role and type — spoofed daemon-owned entries are
  integrity errors.
- **Discriminants are raw bits** (§5, §12): tag values everywhere
  (model `TagEncoding`, `NativeTagEncoding`, `WireTagRead`/`Write`,
  DSWL payloads) carry the discriminant truncated to tag width and
  zero-extended to `u128` — two's complement for signed reprs, raw-bit
  equality at tag width, so `repr(u128)`/`repr(i128)` are fully
  representable with no signedness convention to disagree on.
- **Placeholders are epoch-scoped factories** (§3, §15):
  `register_placeholder` takes a no-unwind `fn() -> T` — one non-Clone
  value cannot serve multiple handles each needing its own owned
  `Box<dyn Any>` through `AssetStorage::update`, so deletion swaps mint
  per-handle values; `begin_module_drain` frees every produced value
  through still-resident drop glue and forgets the factory,
  `drain_complete` covers both — no placeholder vtable or drop glue
  outlives dlclose. (Superseded in R21: the bare `fn() -> T` could not
  catch an inside-module panic — `register_placeholder` now takes a
  generated `PlaceholderThunk` yielding
  `Result<ErasedValue, CallbackPanic>`, §15; the epoch scoping stands.)
  (Refined in R23: `make` receives `ModuleEpochToken` explicitly, and every
  minted value is encode-visited for strong references, checked on the sweep
  basis, and incorporated into the union graph to fixpoint before swap.)
- **Workers never park on descendants** (§9, §13): a descendant build
  executes inline on the joining worker (cooperative execution,
  trampolined from an explicit continuation stack — never native
  recursion), so a bounded pool cannot starve on an acyclic graph; the
  in-flight table keeps a global wait-for graph — joins record the
  joiner's ancestry, and a cycle-closing join fails immediately naming
  the cycle's members (§9's load-DAG error) instead of deadlocking two
  concurrent builds of mutually recursive assets, which no job-local
  visit stack alone can see.
- **Indirect handles flow through LoaderIO** (§15, §16, §17):
  `resolve_path` and path subscriptions are typed `LoaderIO`
  operations — RpcIO reaches the daemon's logical path index,
  PackfileIO answers from the pack's path table and returns a typed
  `Unsupported` without one — so a pack lacking a path table refuses
  indirect handles at request time, loudly, never by silently failing
  loads.
- **Deletion is typed on the IO surface** (§15, §17): `ResolveResult`
  distinguishes `Missing` / `Deleted` (best-effort daemon memory
  carrying the deleting stamp — deletion itself is client-relative,
  §15) / `Failed`, and deltas carry typed
  changed/deleted/restored states — `ManifestState::Dead` and deletion
  policy are reachable only if the surface can say "deleted", and
  restoration must be distinguishable from recovery-after-failed-build
  because the two swap differently.
- **GPU gating is observable** (§15): `AssetStorage::update` returns
  `Ready` or an engine-minted `PendingToken` bound to (handle, adoption
  id, epoch), polled to `Ready`/`Pending`/`Failed` and cancelled on free —
  the loader commits a component only after every member reports Ready,
  making the data-ready → committed promise enforceable; a stale
  readiness can never gate the wrong swap.
- **Root identity is the normalized name** (§8, §10, §13, §18):
  serialized and hashed forms (`ImportRecord.sources`, `FileDep`, FILQ
  result hashes) carry the root's normalized name length-prefixed,
  never an ordinal — reordering or adding configured roots can never
  reinterpret committed records; `RootId` is process-local interning,
  never serialized.
- **Every indexing failure publishes a poison row** (§6, §7, §13):
  malformed containers, schema-closure failures, and unsupported format
  versions publish a bundle-scoped poison row — when the file's bundle
  identity is recoverable (§7's scope rule: a prior indexed version or
  a parseable outer envelope; otherwise conservatively version-global) —
  replacing the file's
  prior rows — never retained (a quiescent snapshot would spin on
  `Drifted`) and never dropped (query semantics would change
  invisibly); resolves return a stable `Failed` naming the error,
  matching queries fail naming the poisoned bundle, and fixing the file
  heals on the next version. (Scope rule superseded in R21: bundle
  scope now requires the complete namespace skeleton from the current
  bytes, §7, §13; the publish-always rule stands.)
- **IO attestation is epoch-bound** (§15, §16, §17): every live IO
  instance binds to the (GameModuleEpoch, target-definition hash, DSLA
  registry digest) triple it
  attested with; `register_types` for a successor epoch blocks loads
  until RpcIO re-runs the connect check (`reattest`, carrying the same
  triple) or PackfileIO
  re-verifies the pack's layout-registry table and target-definition
  hash — "fails at open, never
  at draw" holds across module reloads, not just at first mount.
- **Packfile bytes have one grammar** (§16): `"DPK1"` magic + LE
  version; length-framed, key-sorted tables with blake3 trailers
  verified before use; EKey = `blake3("DSEK" ‖ version ‖ encoded
  bytes)`; structural bytes chunk at fixed 256 KiB boundaries into
  independent zstd frames (frame format pinned, encoder parameters
  free — decode compatibility is the contract); blob extents
  EKey-addressed like structural blocks, their (generation, offset,
  len) held solely by the EKey→location index, extent starts 16-aligned
  with zero padding; the manifest
  lives at its pinned hash-named filename (`"DSPM"`) and `pack.current`
  names that hash — no circular index lookup — and activation fsyncs
  every newly referenced file and directory before the pointer's atomic
  rename and directory fsync: a crash leaves old or
  new manifest, never a mix, never a pointer naming undurable files.
- **Primary inference sees content entries only** (§6, §8):
  daemon-owned `$settings`/`$record` never count; a sole content entry
  with no prior primary is the primary; multiple content entries with
  no declaration and no prior fail the fold asking for an explicit
  `primary()` — otherwise every normal first import would have no
  inferable primary.
- **u32 bounds bind at the boundary** (§5, §12): every fixed-layout
  size, offset, stride, array length, and plan index must fit u32
  (fixed sections capped at 4 GiB like the variable section);
  source-walk and `#[asset]` reject an exceeding type with a typed
  error naming it — never truncate, never wrap; §5's model stays u64
  because the check lives where the u32 grammars begin.
- **Blob keys are structural paths** (§6, §12): blobs order by
  (local_id, canonical structural path) — tagged, length-framed
  field/variant/index/mapkey components from the entry root — in
  bundles and artifact blob tables alike; a leaf-name key would alias
  two blobs under one name in different nested shapes, letting two
  writers produce different ContentHashes for one value.
- **Authored-file replacement journals its intent** (§2, §14, §17, §20):
  a write-intent record — target, temp, and conflict paths, expected
  pre-image hash, proposed content hash — is fsynced before the first
  rename of the sole rename-aside protocol;
  startup reconciles every unfinished intent by hashing what sits at
  each named path, an unclassified file is never deleted — quarantined
  and named in diagnostics at worst — and temp cleanup is journal-driven,
  so no cleanup pass can destroy displaced user bytes. Install is atomic
  no-clobber from a daemon temp; restore links from and retains the verified
  aside; reappeared targets/proposals/preimages are journaled and
  conflict-preserved; every rename/link has directory-flush recovery; and no
  overwrite primitive conforms.
- **Bundle-visible root identity is a typed name** (§6, §8):
  `RootName(String)` — the normalized root name — is what
  `RootedPath.root`, `FileDep::Probe.observed`, and everything in
  `ImportRecord` declare and serialize; `RootId(u32)` is process-local
  interning confined to daemon metadata, converted explicitly at the
  persistence boundary — the generic schema codec never sees an integer
  whose meaning depends on configuration order.
- **Epoch rotation is staged, and rejection publishes** (§3, §5, §13,
  §15): a candidate pipeline epoch validates whole (identity checks +
  registration) before the input version pinning it publishes; a failed
  candidate publishes a pipeline-poisoned version — resolves needing
  pipeline code fail deterministically naming the registration error,
  and the old epoch serves only the snapshots that pin it, never
  standing in silently as the new version's code — so retry-refreshed
  clients terminate in a stable `Failed`, not a `Drifted(Dylib)` loop.
- **The migration walk tests current first** (§11): every step tests
  `node == target_current` before querying outgoing edges and terminates
  immediately on equality — an old edge can never transform data already
  at current, and a future edge can never overshoot current into a
  backward automatic diff; the terminating node records no edge dep
  because none is consulted.
- **Custom edges carry literals, never live defaults** (§3, §11):
  `WriteFieldDefault`/`WriteParentDefault` are automatic-segment-only;
  edge authoring materializes then-current defaults into literal
  `WriteValue` ops, so a custom edge replays byte-identically from its
  bundle forever — a later module's `DefaultTable` can neither strand
  nor silently reinterpret it — and a custom edge carrying a
  default-table op is rejected naming the edge.
- **Migration execution is normative** (§11): ops read the edge's
  immutable input value and write a fresh output; `CopyField` copies
  (moves are copy + explicit drop); plans are total and disjoint over
  the output — unwritten, doubly written, or unresolvable paths are
  typed plan-validation errors naming the path — and every edge's
  output, function edges included, validates against its `to_hash`
  endpoint schema before tag extraction, validators, or encoding see it.
- **Blobs are barred beneath map keys and set elements** (§5, §6, §12):
  extraction rejects `#[asset(blob)]` anywhere in a key or set-element
  subtree with a typed error naming the path — canonical ordering is by
  encoded bytes and a blob's encoding holds an order-assigned offset,
  so the circularity is broken by construction; map values keep blobs
  freely.
- **Emitted references are trace-verified** (§4, §9, §10, §12): result
  binding resolves every reference in output data as (uuid, expected
  terminal type) and records a `TraceOp::RefCheck` revalidated on every
  lookup — a strong ref absent or retyped at the snapshot fails binding
  naming the field path; weak-ref absence records as an observation —
  so deletion or retyping invalidates cached artifacts that
  `load_deps`' bare UUIDs could never catch.
- **Wire trees are pinned CAS records** (§12, §13, §15, §17): every
  encoded DSWL tree commits as a content-addressed record under its
  layout hash before any referencing result, pins and evicts with every
  referencing result, and rebuilds its index from the segment scan —
  any artifact fetchable by ContentHash has its tree servable by
  `Hub.wireTree` or a pack table, across eviction, restart, and epoch
  retirement.
- **Poison scope follows identity recoverability** (§7, §13): a failing
  file publishes a bundle-scoped poison row only when its bundle
  identity is recoverable — the prior indexed version of that (root,
  path) or a parseable outer envelope; a new unparseable file poisons
  the version globally, conservatively, since nothing can bound which
  exact-UUID, type, or tag queries it could match — an
  under-approximation is never served silently. (Superseded in R21:
  prior rows and identity alone are no longer scoping bases — bundle
  scope requires a fully validated, complete namespace skeleton from
  the current bytes, §7, §13.)
- **Deletion is client-relative** (§2, §13, §15, §22): `Deleted` means
  previously resolved in this client's session or pack lineage and now
  absent — the daemon's typed answer is a best-effort accelerator, and
  after state loss it legitimately answers `Missing`; the loader maps
  ever-Resolved-now-Missing to `Dead` locally. No daemon tombstones (§2
  forbids precious daemon state); authored tombstones rejected.
- **Adoption identity is loader-minted** (§13, §15, §16, §17):
  `InputVersion` is instance-local — RPC qualifies every version by the
  `StoreInstanceId` delivered at connect (versions from two instances
  never compare), a pack's identity is its manifest hash (it has no
  `InputVersion`), and `AssetStorage` keys on the loader's own
  `AdoptionId` — a reconnect or remount can never alias a new candidate
  with an old (handle, version) storage entry.
- **Subscriptions are cursor-bound and cover paths** (§15, §17):
  `Hub.subscribe` takes `since`, installs atomically at
  `installed ≥ since`, and opens with the ordered delta for
  (since, installed] over the named assets *and paths* — nothing
  between initial resolve and installation is lost, and
  `LoaderIO::subscribe_path` has a real RPC carrier; a `since` beyond
  retained history returns a resync marker.
- **Completions correlate by request generation** (§15): every
  `LoaderIO` command carries a never-reused `ReqId` echoed by its
  completion; the loader tracks (handle, purpose, basis, connection
  epoch) per outstanding id and explicitly discards stale completions —
  a delayed `Missing` or `Failed` from an earlier basis, retry, rebind,
  or pre-reconnect epoch can never overwrite a later result.
- **Reattestation repeats the whole open-time check** (§15, §16, §17):
  `Hub.reattest` carries the (GameModuleEpoch, target-definition hash,
  DSLA registry) triple `Root.connect` verifies — a successor module
  built from a different target definition fails even when measured
  layouts coincide; PackfileIO's epoch re-verification re-checks the
  target-definition hash alongside the layout table. (Refined in R25:
  every Hub also CAS-installs an exact base+1 attestation generation;
  concurrent stale calls cannot mutate, and success echoes the installed
  generation that alone unblocks loader traffic. Refined in R26: the repeated
  check is the complete CompiledTypeRow/DSCA and load-policy/DSLP attestation;
  installation CASes the full attestation/target/policy/store/protocol tuple,
  and a dedicated structured ReattestResult carries every typed outcome.)
- **Failures coalesce with basis and trace** (§9, §13): a failed job's
  outcome carries its basis and the discovered trace through the
  failing operation, and joined waiters revalidate it per snapshot
  exactly as successes — a waiter whose snapshot resolves the failing
  entry differently re-executes at its own basis, never adopting a
  false failure; traceless infrastructure failures propagate only
  within their originating snapshot.
- **Drift recovery re-resolves whole components** (§15): a mid-sweep
  `Drifted` refreshes the snapshot and re-resolves every member of the
  affected component — old-basis successes are never relabeled with the
  refreshed version, and early cutoff makes unchanged members one
  round-trip, not one rebuild; the swap commits only when every member
  succeeded at the single refreshed snapshot.
- **Watchers arm before the scan, live workloads stay incremental, and metadata
  trust has a watermark** (§13, §14): startup arms watchers first and retains
  every event observed during traversal for incremental replay after the scan
  commit; only native overflow/incomplete coverage repeats the scan. Ordinary
  post-startup events re-observe affected paths/subtrees and never scan unrelated
  tree state — no change falls between a path's scan visit and watcher activation;
  equal-metadata files are trusted only strictly below the previous
  session's durably recorded clean watermark, else rehashed — offline
  same-metadata replacement cannot stay stale indefinitely.
- **Descendant execution is trampolined and depth-capped** (§9, §13,
  §18): inline descendant builds run iteratively from an explicit
  continuation stack — native stack depth stays bounded regardless of
  chain depth — and a configured `max_dependency_depth` errors over-deep
  chains back to their callers with the chain named (a scheduler
  outcome, never a memoized failure record, §9); the cap is policy,
  the trampoline is safety, each independent of the other.
- **`pack.current` bootstraps without circularity** (§16): the pointer
  holds the manifest hash (`"DSPM"`), the manifest lives at its pinned
  hash-named filename and authenticates by recomputation — no index
  lookup to find the index, which is a manifest table, never a separate
  file — and activation fsyncs every newly
  referenced archive and containing directory before the
  pointer's atomic rename and directory fsync: a crash leaves old or
  new, never a mix, never a pointer naming undurable files. (Refined in
  R22: `pack.current` is exactly 64 lowercase hash hex bytes plus `\n`
  (65 bytes), written to a same-directory no-replace temp, file-fsynced,
  renamed over the pointer, then directory-fsynced; directory fsync alone
  does not guarantee pointer data, §16.)
- **Enum variants are self-contained records** (§12):
  `NativeLayoutNode::Enum` stores `NativeVariant { name,
  declaration_index, payload node, per-variant tag info }` — a raw
  discriminant associates with its variant through the record's own
  fields, never positionally against a separate declaration-order value
  list, and wire tables re-sort the records by name; `NativeTagEncoding`
  keeps only the tag's geometry.
- **The native digest has its own grammar** (§4, §5, §12, §16, §17):
  the measured layout digest is `blake3("DSNL" ‖ version ‖ root)` over
  a pinned native-layout grammar — every node and skip slot with
  size/align/stride/offset and tag encoding, binary-local table ids
  excluded by rule — so independently built binaries measuring the same
  layouts hash equal, and the fixup-table identity (which adds table
  assignment) extends it rather than leaking into it; `"DSNL"` joins
  the §5 domain table.
- **Symlinks follow canonical targets** (§10, §14): scans keep a canonical-path
  ancestry set for cycle diagnostics and a scan-global canonical-directory map
  for aliases; every non-ancestor alias — same-directory configured roots
  included — is configuration poison before rows publish. Opens repeat
  canonical containment and type checks, and reads revalidate their observations.
  This is the R37 simplification of the former R23/R30 retained-identity and
  descriptor-relative design.
- **Canonical JSON bytes are total** (§6): RFC 8785 is the reference
  serialization with named deviations — UTF-8 byte-order keys,
  parse-error duplicates, minimal escaping, no insignificant
  whitespace, exactly one trailing newline, minimal-decimal integers,
  8785 shortest-round-trip floats — every byte choice normative,
  because whole-file bytes enter hashes and watched-import fixpoints.
- **Failures leave a basis record** (§8, §13, §15): deterministic build
  failures commit failure records keyed like results (digest + partial
  trace + basis) — an unchanged basis answers from the record instead
  of rebuilding once per client, and an invalidated basis re-runs; a
  failed watched import records its rediscovered read-set in daemon
  state (never rewriting the bundle's `ImportRecord`) and re-runs on
  its invalidation; transient infrastructure errors are typed apart and
  never memoized. (Refined in R22: `FileDep` is outcome-bearing for
  read/probe/enumerate failures, importer capability hits/misses join the
  basis, and the terminal failure revalidates like a build trace, so a
  NotFound or listing failure can wake when it heals, §§8–9, 13–14.)
- **The pack path table has a byte grammar** (§16): length-framed rows
  of normalized-path str → primary AssetUuid, sorted by path bytes,
  covering exactly the bundle paths whose primaries are in the pack
  closure at the build snapshot — ambiguous paths fail the build,
  out-of-closure paths are omitted into `Missing` — riding in the
  manifest under its blake3 trailer and replaced atomically with it.
- **Pack reproducibility is logical** (§16): one
  `PackDefinition` at one snapshot reproduces identical logical
  content — asset set, decoded bytes, ContentHash set, path table,
  chunk boundaries, logical manifest semantics — never
  encoded frame bytes, which no `zstd_level` can pin; EKeys hash
  encoded bytes, so the physical manifest hash (`"DSPM"`) identifies a
  particular pack build, not a byte-stable cross-encoder identity; the
  archive
  header records encoder identity + settings for diagnostics only, the
  frame format stays the decode contract, and patch dedup enjoys
  byte-stable EKeys without correctness resting on them. (Refined in R23:
  structural frames are dictionary-free/self-contained, declare decoded size
  and window at most 256 KiB, and readers reject dictionary ids, oversized
  windows, and concatenated/skippable-frame payloads.)
- **Generated sources publish like bundles** (§14, §20): `rs_mod_path`
  is declared daemon-owned for generated files, and every
  `.rs`/`mod.rs` write runs §14's full protocol — intent journal,
  atomic rename-aside, displaced-byte verification against the last
  generated bytes — so a concurrent hand or build-tool edit is
  restored and reported as a conflict, never lost to check-then-write.
- **Configuration changes are input events** (§13, §18): the daemon
  watches its own config; root and target edits stage a validated
  configuration epoch and publish through the input-version mechanism —
  snapshots pin it, changed roots reconcile via the scanner, and a
  redefined target surfaces as a named definition-changed drift until
  reconnect — while a rejected config publishes a configuration poison
  (§7's rule), never silently keeping or half-adopting anything. (Refined
  in R22: snapshots now pin explicit `ConfigurationState = Ready |
  Poisoned(reason)` alongside pipeline state; pure metadata/snapshot
  operations remain valid, authoring and target/config-dependent RPC fail
  with a stable typed poison result, §§13, 17–18.)
- **Processor outputs cross by encode visitor** (§3, §4, §9, §12):
  `#[asset]` generates a no-unwind per-type encode visitor
  (`AssetRuntimeDescriptor::encode` + the daemon-side `EncodeSink`)
  that lowers a typed value into the neutral wire-builder vocabulary —
  flat bytes, container events in canonical §5 order (never native
  iteration order), blob handoffs, typed reference emissions — before
  anything crosses the module boundary. §9's result binding consumes
  the emitted references and §12's encoder consumes the events: raw
  layout alone could never iterate a HashMap deterministically or tell
  a strong reference from a weak one.
- **LoaderIO answers under an explicit basis** (§13, §15, §17):
  `IoBasis = Rpc(SnapshotStamp) | Pack(ManifestHash)` is the IO-neutral
  basis token — `begin_sweep` mints it, every resolve/fetch/
  resolve_path takes it, and the outer `IoEvent` is the sole basis carrier
  for every request-terminal outcome (nested payloads carry none).
  Component adoption requires
  one basis across all members; a mixed-basis component re-resolves at
  the newer basis under the mid-sweep Drifted rule. `SnapshotStamp`
  stays the RPC-side realization. (Refined in R22: each RPC basis also
  owns the `Root.connect`/`reattest`-verified load-policy rows, `"DSLP"`
  digest, and generation; Pack bases own their manifest projection, so
  sweeps validate basis-bound policy uniformly, §§15–17. Refined in R24:
  the duplicated basis inside `ResolveResult::Built` is removed; payload
  admission checks the one event-envelope basis first. Refined in R25:
  typed ConnectSuccess is the sole source of StoreInstanceId and the
  policy/target/attestation generations carried by an RPC basis. Refined in
  R26: that exclusivity applies to initial construction; installed
  ReattestSuccess is the only attestation-successor source and rotates basis
  plus connection epoch atomically. Refined in R28: the same success rotates
  the accepted-set policy projection/digest/generation when its runtime set
  changes.)
- **Displaced inodes are quarantined, never unlinked** (§2, §14, §18,
  §20): after rename-aside + verify, the displaced pre-image is retained at
  `.distill/displaced/<content-hash>` (journal-recorded) for
  `displaced_retention_days` (default 7), removed only by retention
  expiry or `doctor clean` — an in-place writer's post-verification
  bytes land in the quarantined inode and survive, and startup
  reconciliation re-hashes quarantined files, surfacing mismatches as
  recovered-edit diagnostics. Generated-source writes (§20) inherit
  the rule. (Refined in R21: the content-hash naming and `.distill/`
  location are superseded — quarantine entries are named by
  journal-assigned intent IDs in per-filesystem daemon-owned quarantine
  directories, and conflict-restored preimages retain their aside names,
  §14.)
- **Traces are outcome-bearing** (§9, §13, §15): every `TraceOp`
  records `Observed<T> = Ok(T) | Err(StableFailureFingerprint)` — a
  typed, content-derived failure encoding (sorted conflicting ids,
  poison-row identity, a missing strong ref's query + expected
  terminal, a descendant's fingerprint, tool identity + error class) —
  so a failure record's trace includes its failing terminal op and
  revalidates exactly like a success; transient infrastructure
  failures stay traceless and unmemoized. (Refined in R21: capability
  lookups record `TraceOp::Capability` hit and miss, and failure
  records terminate in a `FailureCause` — the failing op or a `Local`
  fingerprint for deterministic local failures — §9, §13.)
- **Tools are ToolEpoch snapshot inputs** (§3, §9, §13, §15): registering or
  replacing a tool publishes a complete package snapshot or an explicit ambient
  toolchain identity plus DSCT hash at an input version. Jobs resolve that
  registration through their snapshot. `DriftedInput::Tool` covers an evicted
  package; ambient availability is a transient launch property. An ambient
  registration without a trusted fingerprint may execute but never memoizes.
- **Pipeline-only native deps link statically** (§3, §20): the dylib
  hash is the module's code identity and covers only that one file, so
  pipeline-only libraries must be statically linked into the cdylib; a
  dependency must instead execute out of process as a registered §9 tool. The
  daemon does not parse the pipeline image or reproduce platform-loader
  resolution; static linkage is enforced by the pipeline build. The system
  runtime is acknowledged as part of LayoutIdentity's (target,
  rustc) and outside the dylib hash — the residual: system-runtime
  drift is not tracked. (Refined in R22: the staged-library alternative
  is withdrawn. Pipeline runtime `dlopen` is banned and no library-open
  API exists; dynamic tools run only as `run_tool` subprocesses, §§3,
  9, 13. Refined in R32: tool packages are closed by hashing their complete
  directory, not by parsing executables; ambient tools require an explicit
  toolchain identity and trusted fingerprint to memoize.)
- **`build_only` has a load-policy digest** (§5, §9, §13, §15, §16):
  blake3 over the sorted (type_uuid, build_only) pairs of the current
  registry, input-versioned; closure validation (loader sweep, pack
  builds) depends on it, and a staged module epoch that changes any
  type's bit publishes the new digest, invalidating affected loaded
  closures as ordinary component deltas — the deliberately unhashed
  policy bit gets change-tracking exactly as `tag`'s annotation epoch
  gives its. (Refined in R21: the digest gets the `"DSLP"` domain,
  descriptors expose the bit, and packs carry the closure-projected
  table, attested at mount — §4, §16. Refined in R22: RPC connect and
  reattest carry the same rows/digest, bind them to `IoBasis::Rpc`, and
  policy changes generation-fence stale capabilities, §§15–17.)
- **Rev mismatch is an automatic-plan hard stop** (§4, §5, §11): a
  name-matched field or variant whose `rev` differs refuses automatic
  planning, naming the path and both revs, and requires a custom
  edge — automatic copy or drop-plus-default across a rev change is
  forbidden: rev means same shape, new meaning, and both silent
  options are the defect the revision exists to prevent.
- **Automatic migration is direction-checked by schema lineage** (§5,
  §11, §13): each successfully staged module epoch appends
  (type_uuid, previous current hash → new current hash) to an
  input-versioned lineage; the trailing automatic diff is legal only
  when the source node is on the recorded forward chain to current (or
  the type has no lineage yet — first sight). A registry recorded
  behind the data refuses schema-dependent builds with a staleness
  error naming both hashes; a deliberate rollback needs an explicit
  reverse custom edge. (Refined in R21: the chain assigns per-type
  generations and a `"DSSL"` chain digest, staging rejects non-head
  re-entries as rollbacks, and every entry carries a `LineageStamp`
  (Refined in R31: that historical statement now means every non-bootstrap
  entry carries `EntryLineageV1::Manifest`; the exact five bootstrap types
  instead require the sealed `Bootstrap` arm and never enter user lineage.)
  beside its `schema_hash` — the "no lineage yet, trivially forward"
  case is withdrawn; first-sight diffs are legal only from a strictly
  lower-generation stamp, §6, §11. (Refined in R22: generation plus an
  opaque chain digest cannot prove prefix ancestry. `LineageStamp` now
  carries the full ordered digest list; automatic migration requires
  data-list-prefix-of-registry-list, rollback/divergence hard-stop, and
  reconstruction unions only prefix-consistent lists.) (Refined in R23:
  the source-controlled `SchemaLineageManifest` is the sole acceptance
  authority; append-only epoch records carry explicit parent links and an
  independent current cursor, and bundle stamps cannot reconstruct missing
  acceptance history.))
- **Pipeline poison is a representable state** (§3, §11, §13):
  `MetadataSnapshot` carries `PipelineState = Ready(Arc<PipelineEpoch>)
  | Poisoned { error, last_good }` and `epoch()` is fallible. The
  classification is exact: pure-metadata reads (path index, input
  versions, CAS reads, pinning) remain valid under poison; anything
  needing the pipeline map, registry, defaults, or migration fns
  (load_current, terminal-type queries, the derived-output namespace,
  builds) returns the poison error. (Refined in R22: poison has distinct
  `CandidateOpen` and `PublishedRuntime` origins. A status failure in an
  already-published epoch fences new work, makes drain permanently
  incomplete, and forbids `dlclose` forever, §§3–4, 12–13, 15.)
- **Module, schemas, and target configuration stage as one candidate**
  (§3, §13, §18): a candidate epoch's pipeline map is constructed and
  validated against the candidate's target set, and target-config
  edits go through the same staged-candidate mechanism as module
  swaps — a snapshot never pairs a target definition with a pipeline
  map built for a different target set.
- **The DSNL AST is grammar-complete** (§5, §12): `Unit { offset }`
  (size 0, align 1 by rule), `BackRef { distance, offset }` (size and
  align the referenced frame's, stated as inferred-by-rule), `Skip`
  gains measured `align`, and `NativeField` gains `declaration_index`
  — the DSWL tie-break for equal wire offsets after repacking, which
  native slice order cannot supply. Grammar–AST parity is itself a
  rule: every normative grammar field must exist in the declared AST.
- **Depth exhaustion is a scheduler outcome** (§9, §13, §18): never a
  build failure record — not memoized, not trace-bearing, never
  poisoning; the request errors to its caller with the chain named,
  and a later request with budget re-executes. The cap counts live
  executed dependency frames only — a memoized result consumed from
  cache counts as one frame (the consult), never its recorded subtree.
- **Blob extents are EKey-addressed** (§16): the ContentHash→encoding
  table references structural blocks and blob extents by EKey only;
  (archive generation, offset, len) lives solely in the EKey→location
  index — one addressing model, so a patch manifest can reference
  blobs the client already stores by EKey without embedding placement
  inside archives the client never downloaded.
- **The RPC bind is loopback by validated invariant** (§17, §18, §22):
  configuration staging rejects a non-loopback `daemon.address` with a
  typed error — auth/TLS are descoped on exactly the loopback basis,
  so the basis is enforced, never assumed; remote access is the named
  Open item (authenticated transport), not a config knob.
- **Target-definition drift is a connection-level event** (§15, §17,
  §18): `ReconnectRequired { reason: TargetDefinitionChanged }` on the
  subscribe stream — RpcIO tears down the Hub, reconnects, re-verifies
  the §15 attestation triple, and resumes with ordinary Drifted
  re-resolution. Deliberately not a `DriftedInput` variant: per-input
  drift would loop the retry-refreshed loader on the same obsolete
  Hub, and `Failed` would freeze the component instead of
  reconnecting. (Refined in R21: the event is delivery, not
  enforcement — every target-bound method is generation-fenced
  server-side and answers `ReconnectRequired` on a stale capability,
  §17.)
- **Daemon-owned paths are disjoint from asset roots** (§14, §18):
  `state_path`, module artifact paths, and every daemon-owned output
  directory must not be nested inside any asset root — a staging-time
  config error naming both paths, since a daemon writing inside a
  watched root would advance input versions with its own outputs
  indefinitely — and staging plus scanning reject canonical-path overlap or
  alias as defense in depth. R37 removes the former persisted platform-identity
  mechanism.
- **The DSBI key is a pre-key plus discovered trace** (§8, §9, §13):
  the static pre-key covers everything computable before work (entry
  identity, canonical bundle bytes, hashes, format version); the
  reference resolutions the *migrated* value produces — synthesizable
  by MigrationFns and default materializers, so discoverable only by
  running `load_current` — are acknowledged as execution, recorded as
  a trace, and memoized under the processor candidate-bucket
  revalidation rules. The former fully-static claim is withdrawn.
- **Pack reproducibility is defined at the logical level** (§16): one
  definition + snapshot reproduces identical logical content — asset
  set, decoded bytes, path table, logical manifest semantics; the
  physical manifest hash (`"DSPM"`) identifies a particular pack
  *build* (encoder-dependent EKeys included) and is not promised
  byte-stable across encoder implementations.
- **Canonical floats are schema-directed** (§5, §6): an `f32` leaf's
  value is rounded to its nearest binary32 value before
  shortest-round-trip decimal emission; adoption and validation
  enforce it, and a bundle claiming an `f32` leaf whose decimal is not
  binary32-exact fails canonicality — two conforming writers can never
  emit different canonical bytes for one value. (Refined in R22: the
  “binary32-exact” test is superseded. Parsing is nearest-ties-even to
  binary32 and canonicality is byte equality with shortest re-emission
  that reparses to the same bits; `0.1` is canonical, §§5–6.)
- **`MigrationOp::Custom` is removed** (§11): it carried no input
  path, no output path, and no endpoint schemas, so it could not
  participate in the total-and-disjoint output rule; whole-edge
  `MigrationKind::Function` is the single custom-logic carrier.
- **`resolvePath` is a declared RPC** (§15, §17): `Snapshot.resolvePath
  @10 (path) -> (result)` — the `LoaderIO::resolve_path` carrier over
  the §13 logical path index, basis-tagged like `resolve` (answered
  under the snapshot's stamp); `Missing` is first-class, ambiguity is
  an error, never a tiebreak.
- **Hash domains are total** (§5, §8, §9, §12): `ModuleAbiIdentity`
  gets its own `"DSMA"` domain (two meanings never share `"DSCI"`),
  the fixup-table identity (`"DSFT"`) and the full input hash
  (`"DSIH"`) join the domain table, and the table's rule is explicit —
  every hash construction in the document must appear in it. (Refined
  in R22: totality is scoped to semantic/composite hashes. The closed,
  named byte-identity exception class uses domainless raw blake3 over
  exactly the named bytes — ContentHash, raw files, dylib/tools, CAS
  payloads, and archive file/trailer digests, §§5, 9, 12–13, 16. Refined in
  R25: candidate target-set identity gets the registered `DSTS` domain and
  name-sorted `(normalized target name, DSTG)` v1 grammar.)
- **Every config key declares its change class** (§13, §18): a
  per-field policy table classifies the whole surface — input-versioned
  epoch (roots, schema_path, targets, pipeline_dylib, tool
  registrations), operational-live (parallelism, depth cap, CAS
  thresholds, retention windows), restart-only (state_path, address,
  codegen) — with consequences named per key; a future key must
  declare its class before it ships.
- **The pack byte grammar is closed** (§16): manifest file = header +
  tables (manifest, encoding, EKey→location index, wire-tree, path) +
  per-file blake3 trailer — blake3 of all preceding bytes of that
  file, excluded from itself; index data is manifest tables, so no
  separate index files exist and activation fsyncs archives and the
  manifest only; every file a manifest references is named by hash in
  the manifest body. (Refined in R21: the layout is pinned to the
  byte — every header field, table row, table-directory entry, and
  archive record grammar — and the EKey hash scope excludes record
  framing, §16.) (Refined in R23: archive filenames are exactly
  `archive-<lowercase-full-file-hash>.dpk`; no-replace publication verifies
  an existing same-name file byte-for-byte and fsyncs the archive and
  directory before any referencing manifest.)
- **Lineage direction survives daemon-state loss** (§2, §5, §6, §11,
  §13): `schema_lineage` alone was chronology no bundle could
  reconstruct — after `.distill/` loss, "no lineage yet" permitted an
  automatic diff from *any* embedded schema to current, and blind
  append could record a rollback as a forward edge or a cycle. Now
  direction rides in the data: every entry carries a `LineageStamp`
  (Refined in R31: non-bootstrap entries carry that stamp in
  `EntryLineageV1::Manifest`; the five bootstrap types require the sealed
  `Bootstrap` arm and are excluded from user lineage and migration.)
  (per-type generation + `"DSSL"` chain digest, assigned at adoption)
  beside its `schema_hash`; staging rejects a candidate schema whose
  digest is a non-head chain entry (a rollback — schema-writing
  services refuse for that type until an explicit reverse edge lands,
  the rev-rule hard stop); rebuilds re-establish the registry's
  position from the stamps on data carrying its current digest; and
  first-sight automatic diffs are legal only from a strictly
  lower-generation stamp — unknown, divergent, or higher-generation
  stamps hard-stop into an explicit edge. §2's disposability claim is
  qualified accordingly: deleting `.distill/` costs a rebuild and can
  never enable silent backward migration. (Refined in R22: the R21
  generation/digest stamp is superseded by the explicit ordered digest
  list and prefix test; migration endpoint records participate in
  prefix-consistency-checked reconstruction, §6, §11, §13.) (Refined in
  R23: direction authority moves to the required source-controlled manifest;
  bundle and endpoint stamps verify against it but are never unioned to invent
  acceptance after state loss.)
- **Poison scope is proved by the current bytes** (§7, §13): a prior
  indexed row identifies what the *old* bytes claimed — a malformed
  edit can introduce a new UUID, type, `local_id`, or tag before the
  syntax error, so prior-row-scoped poison let matching queries return
  `Missing` or shrunken results. Bundle-scoped poison now requires the
  current malformed bytes to yield a fully validated, complete
  namespace skeleton (every asset UUID, local id, type, and tag parsed
  and validated despite the whole-file failure); anything less is
  version-global regardless of prior metadata — a poison is scoped
  only by facts validated from the bytes being poisoned. (Refined in R27:
  version-global poison is a closed DSVP-tagged identity for UUID collisions,
  same-root normalized-path collisions, incomplete skeletons, and stable
  unreadable-path classes, maps
  identically through snapshot/query/RPC, and never memoizes as a bundle
  sentinel. Refined in R29: code 6 losslessly carries a physical path that
  cannot normalize, and simultaneous global defects select only after complete
  canonical-set enumeration rather than scan order.)
- **Import publication revalidates its read-set** (§8, §13, §14): a
  source event consumed between an importer's reads and its commit
  would never re-fire once the stale result installed its read-set — a
  lost wakeup leaving the watched bundle stale indefinitely. The
  committing coordinator transaction now revalidates the complete
  rediscovered read-set against the current raw-file index and
  installs the bundle write and its dependencies atomically only if
  every entry still matches; a mismatch discards the result and
  re-enqueues the import — the attempted-basis discipline build
  memoization already uses.
- **Capability lookups are observed, hit and miss** (§8, §9, §10, §11,
  §13): a missing `MigrationFn` or default-table entry fails before
  any code runs, so its memoized failure carried no module dependency
  and never healed. Every pipeline capability resolution — MigrationFn
  keys, default tables, importer/processor lookup — now records
  `TraceOp::Capability { key, observed }` in the §9 `Observed` grammar:
  Ok(dylib hash) on hit, `Err(MissingCapability)` carrying the
  requested `CapabilityKey` on miss — so the failure record invalidates
  on the first epoch that supplies the registration. (Refined in R29: tool is
  fixed `CapabilityKey` tag 5; a ToolEpoch miss terminates `TraceOp::Tool`,
  while post-hit launch failure is distinct. Refined in R30: it is a transient
  `{id, tool_hash, class}` that discards the attempt and never memoizes;
  historical DSTR tag 4 `ToolLaunch` is permanently reserved/rejected, and
  the successful/missing Tool observation is tag 10. The field is `tool_hash`,
  the aggregate DSCT identity; transience is unchanged.)
- **Deletion is rename-aside-and-verify too** (§2, §14, §17): whole-bundle
  deletion was a bare unlink behind a base-version check — exactly the
  TOCTOU the replacement protocol closes for rewrites. Every deletion
  (Asset CRUD and daemon-initiated) now publishes by journaled rename
  into quarantine under its intent ID, then verifies the displaced
  bytes against the expected pre-image — a mismatch restores the file
  to its original path and fails the operation as a conflict.
- **Quarantine is intent-ID-named and same-filesystem** (§2, §14, §18,
  §20): content-hash naming aliased distinct equal-content inodes (each
  possibly held open by a different writer), and a quarantine under
  `state_path` fails across filesystems, where copying orphans the
  live inode. Quarantine entries are now named by a journal-assigned,
  never-reused intent ID (content hash stays as recorded metadata) in
  per-filesystem daemon-owned quarantine directories — one per watched
  root, one per daemon-owned output directory, identity-excluded from
  scanning as the declared exception to §18's disjointness rule —
  `.distill/` keeps only the journal, which references the physical
  locations; restored preimages retain their aside names under the same
  protocol; and the
  no-silent-loss claim is restated honestly: every displacement is
  journaled and retained per policy, destroyed only by journaled
  retention expiry or explicit `doctor clean`.
- **Load policy is attested on every IO base** (§4, §5, §9, §13, §15,
  §16): `build_only` is deliberately absent from DSLH and DSNL, so
  layout attestation could never detect a same-identity, same-layout
  policy divergence. The pack manifest now carries the load-policy
  projection — sorted `(TypeUuid, build_only)` pairs over the pack
  closure with their `"DSLP"` digest — verified at mount and on every
  game-module epoch change against registered descriptors, which now
  expose `build_only`; divergence is a mount refusal. The exclusion
  from DSLH/DSNL stands: policy is carried and compared, never hashed
  into layout identity. (Refined in R22: “every IO base” now includes an
  actual RPC carrier — `Root.connect`/`reattest` rows + `DSLP`, verified
  and bound to each RPC basis; policy changes fence like target changes,
  §§13, 15–17.)
- **Target definitions fence every capability** (§15, §17, §18): the
  `ReconnectRequired` event alone was advisory — a Hub with no
  subscription, or one calling `Hub.snapshot`/`Snapshot.refresh`
  before consuming the event, had no defined behavior. Every
  target-bound method (`Hub.snapshot`, `Snapshot.refresh`, `resolve`,
  `fetch`, `resolvePath`, `subscribe`) is now generation-fenced
  server-side: capabilities bind the target-definition generation they
  were minted under, and a stale capability answers the defined
  `ReconnectRequired { reason }` result instead of data — subscription
  or not, event consumed or not. (Refined in R23: the fence covers literally
  every Hub/Snapshot method through one five-arm `FencedCall` envelope, and
  `IoEvent::ReconnectRequired` carries the typed reason.)
- **Ownership crosses the module boundary by status thunk** (§3, §4,
  §15): `AssetStorage`'s `Box<dyn Any>` and `register_placeholder`'s
  bare `fn() -> T` put raw module drop glue and uncontained
  constructors across an unloadable boundary — a panicking `Drop` or
  placeholder could unwind across it and abort. Game-side storage now
  holds `ErasedValue` — construction and destruction through generated
  no-unwind status thunks in §12's ctor/drop-table vocabulary,
  automatic Rust drop suppressed (ManuallyDrop semantics); a drop
  failure reports, deliberately leaks the value, and poisons the module
  epoch (`drain_complete` never true — the host leaks the module);
  `register_placeholder` takes a generated `PlaceholderThunk` with the
  same contract. (Refined in R22: `ErasedValue` carries its owner-epoch
  token; every DropTable/Ctor/Skip destruction and `AssetStorage::free`
  returns status; `update` consumes on Ok and Err and retains no module-
  backed state on Err, §§3–4, 12–13, 15.) (Refined in R23:
  `AssetRuntimeDescriptor::finalize` and `PlaceholderThunk::make` take the
  `ModuleEpochToken` explicitly; placeholder strong references are collected,
  checked on the same basis, and expanded to fixpoint before adoption.)
- **The pack grammar is byte-complete** (§16): "block records carry
  lengths and CRCs" was not implementable interoperably. Every header
  field, table row, table-directory entry, archive reference,
  optional-table presence rule, width, endianness, ordering, and
  padding is now pinned to the byte, along with the archive block/blob
  record grammar — and the EKey hash scope is pinned: blake3 over the
  stored encoded extent exactly as it appears in the archive payload
  (the zstd frame bytes), excluding record framing; framing integrity
  is the CRC's job, content authentication the EKey's. (Refined in R23:
  zstd structural frames ban dictionaries and oversized windows, and archive
  publication uses the exact full-file-hash filename plus no-replace
  byte-verification and pre-manifest fsync ordering.)
- **Deletion and restoration are component transitions** (§15): the
  component algorithm froze on failures while deletion independently
  marked children `Dead`, leaving dependents unspecified. Now:
  dependents' components freeze on a `Dead` child by default (the
  failed-member freeze); a registered placeholder lets adoption proceed
  through the normal atomic component swap, minting one placeholder
  value per dead handle as ordinary members of the adopted cut — a
  failed mint fails that member and freezes the component at its prior
  cut; restoration is an ordinary new-version adoption driven by the
  typed `restored` delta.
- **Directory-import ownership rides in the bundle** (§2, §8, §13):
  generated bundles persist a `DirectoryOrigin` record — rules-bundle
  UUID, rule identity, group key — in the daemon-owned `$record`
  entry's reserved metadata; reconstruction after daemon-state loss
  reclaims ownership from it (orphan tracking re-derives, never
  precious), and its absence means explicit import — the daemon can
  always distinguish generated output from an equivalent explicit
  import. (Refined in R22: `DirectoryOrigin.rule` is the authored,
  UUID-style stable `ImportRuleId`, never its vector index; reordering
  preserves ownership and deleting the id produces the orphan state,
  §§2, 8, 13.)
- **Local failures have a trace grammar** (§9, §13): validator
  diagnostics, migration-plan validation, and processor `BuildError`s
  arise from no context operation, yet failure records were required
  to end in an `Observed::Err` op. A failure record is now a
  dependency trace (possibly empty) plus a terminal `FailureCause` —
  the failing observed op, or `Local(StableFailureFingerprint)` for
  deterministic local failures — so nothing invents synthetic ops and
  local failures memoize under the same revalidation rules. (Refined in R23:
  canonical local detail and configuration-poison reason facts now have
  dedicated `DSLF` and `DSCP` domains/grammars; presentation prose is outside
  both fingerprints.)
- **Restart-only edits have a defined transition** (§13, §17, §18):
  whether a valid restart-only edit advanced the input version,
  was ignored, or poisoned was undefined. Staging validates it like
  every edit (invalid values poison staging); a valid edit records a
  pending-restart configuration generation, leaves active values
  untouched, and surfaces the defined `RestartRequired` state/event
  (doctor/status plus the subscription stream); the input version
  advances only at restart, when the values take effect.
- **Variant revisions have one declared carrier** (§4, §5, §11): the
  grammar hashed variant revs while the attribute table permitted
  `#[asset(rev)]` only on structs and fields and the model declared no
  variant channel — extractors could emit zero, inherit a neighboring
  rev, or accept an undeclared attribute. `#[asset(rev)]` is now legal
  on enum variants, and extraction reads only the referenced
  `EnumVariant` TypeDef's `attrs.rev`. The existing type-attribute channel
  is reused instead of introducing a separate `VariantAttrs` record.
- **Codegen identifiers map injectively** (§10, §14, §20): raw
  interpolation of authored identifiers into `<pipeline>.rs` permitted
  traversal and invalid or colliding module names, while bare path
  rejection would make legitimate identifiers uncodegenable. Generated
  filenames and module names now use a pinned injective encoding
  (`[a-z0-9]` pass-through, everything else `_hex_`-escaped with the
  introducer itself escaped, under a pinned keyword-avoiding prefix)
  over NFC-normalized identifiers; codegen revalidates its canonical output
  directory and rejects symlinked destinations before writes, and every generated
  string literal and identifier is escaped. (Refined in R22: local_id is
  bundle-local, so the global name also appends `_` plus full lowercase
  AssetUuid hex; the entire generated namespace is collision-checked
  before any write, §§7, 10, 20.) (Refined in R23: authored identifiers no
  longer participate in physical names; filenames/modules are exactly
  `sp_<full-lowercase-AssetUuid-hex>`, and an optional bounded ASCII slug is
  display-only.)
- **Oversize records and fetches are specified** (§13, §15, §16):
  blobs are unbounded while CAS segments cap and RpcIO bounds in-flight
  fetch memory — one implementation could reject or permanently
  backpressure an artifact another accepts. A record exceeding the
  segment cap now gets a dedicated oversize segment (one record per
  file, same record grammar, typed as oversize in the generation
  manifest), and RpcIO's admission rule is pinned: an oversized fetch
  is admitted alone against the bounded budget, and beyond a pinned
  threshold responses stream/spool to disk rather than buffer in
  memory. (Refined with the single-threaded RpcIO, §15 Plumbing: nothing
  spools. A fetch reserves its payload before reading it and waits,
  unread, in FIFO order while it does not fit; an oversized payload is
  admitted alone once nothing else is held. Only the loader taking
  payloads in `poll` frees memory, and the engine never waits on
  admission.)
- **Manifest fields cross-check headers, both ways** (§13, §16): the
  manifest duplicates authored/terminal types, logical hash, and load
  deps from artifact headers with no required comparison — a generator
  defect could ship a hash-valid pack whose closure omits a strong
  dependency. Pack construction MUST verify every duplicated field
  against the ContentHash-verified header (mismatch = build failure),
  and pack loading treats observed divergence as pack corruption — a
  mount-refusing integrity failure.
- **Batch work cannot starve** (§13, §18): strict two-class priority
  with FIFO let continuous interactive demand starve pack builds and
  doctor runs forever. Strict interactive priority is retained, but
  whenever batch-class work is pending a reserved minimum capacity
  (`batch_reserved_workers`, default one slot, configurable) is
  dedicated to the oldest batch job — bounded starvation, unchanged
  interactive latency on every other slot. (Refined in R22: staging
  enforces `parallelism >= 1` and reservation in
  `1..=max(1, parallelism-1)`; one-worker mode alternates classes and
  live resize re-clamps while active slots drain, §§13, 18.)
- **Lineage ancestry is proved by explicit ordered lists** (§2, §5, §6,
  §11, §13): every `LineageStamp` carries the full oldest-first schema-
  digest history and a checked `"DSSL"` commitment; generation is derived
  from list length. Data-prefix-of-registry authorizes automatic migration,
  registry-prefix-of-data is rollback, divergence hard-stops, and state-
  loss reconstruction unions only prefix-consistent lists, including
  migration endpoints. No explicit ancestry proof means no automatic diff.
  (Refined in R23: list-prefix ancestry is superseded by exact manifest-prefix
  verification plus `forward_parent` reachability from the current cursor;
  explicit rollback moves only the cursor after complete reverse-edge
  validation.)
- **Failed authoring imports retain the condition that wakes them** (§8,
  §9, §13, §14): read/probe/enumerate dependencies are outcome-bearing,
  stable failures such as NotFound and listing failure are recorded,
  importer capability hits and misses join the basis, and the terminal
  failure revalidates like a build trace so healing wakes exactly once.
- **Drop failure is status-bearing through published-epoch poison** (§3,
  §4, §12, §13, §15): `ErasedValue` carries its owner-epoch token;
  DropTable, Ctor abort/drop, Skip drop, and `AssetStorage::free` return
  status; update consumes on both statuses and retains nothing on Err. A
  published failure fences new work, makes drain permanently incomplete,
  and deliberately leaks the never-dlclosed module.
- **Configuration poison is snapshot state, not prose** (§13, §17, §18):
  `ConfigurationState = Ready | Poisoned(reason)` is orthogonal to
  pipeline state. Snapshots and pure metadata/CAS reads remain valid;
  authoring and target/config-dependent work return the stable typed RPC
  poison result, and prior configuration never serves under the new version.
  (Refined in R29: the sole mutation exception is the unbound, no-last-good
  local repair capability under missing/duplicate lineage DSCP, with no general
  authoring surface.)
- **RPC bases carry load-policy attestation** (§4, §9, §13, §15, §16,
  §17): `Root.connect` and `reattest` carry the pack-identical sorted
  policy rows plus `"DSLP"`; verified rows/digest/generation bind to
  `IoBasis::Rpc`, Pack bases bind their manifest projection, and policy
  changes generation-fence capabilities like target-definition changes.
  (Refined in R31: "pack-identical" means the row/DSLP grammar only; packs
  cover exact `B(C)`, while RPC covers canonical accepted superset `A`.)
- **Codegen names are globally unique and batch-validated** (§7, §10,
  §20): the escaped normalized bundle-local `local_id` is suffixed with
  `_` plus full lowercase AssetUuid hex. The generator validates the
  complete filename/module namespace before writing; collision fails the
  batch and never replaces another generated unit. (Refined in R23: the
  escaped-local-id portion is removed; fixed AssetUuid-only names are the
  physical namespace, while full-batch validation remains defense in depth.)
- **Directory origins name stable authored rule IDs** (§2, §8, §13):
  every `ImportRule` owns a unique UUID-style `ImportRuleId`, and
  `DirectoryOrigin.rule` stores it rather than a vector index. Reordering
  preserves ownership; deleting the id produces the defined orphan state.
- **`pack.current` has a durable 65-byte protocol** (§16): its bytes are
  64 lowercase manifest-hash hex digits plus newline; activation writes a
  same-directory no-replace temp, fsyncs the file, renames it over the
  pointer, then fsyncs the directory. Directory fsync is not a substitute
  for pointer-file data durability.
- **Pipeline runtime `dlopen` is banned** (§3, §9, §13): pipeline-only
  native dependencies link statically, and there is no staged-library
  resolution/open API. Anything dynamic executes only as a staged,
  hashed §9 subprocess through `run_tool`; daemon hosting of the pipeline
  cdylib remains the distinct module boundary.
- **Extras encode directly as their terminal declaration** (§9, §12):
  every extra has `authored_type = terminal_type = encoded_type =` its
  declared type, even when that type is a processor input. Only primaries
  traverse processor chains.
- **`f32` canonicality is ties-even shortest round-trip** (§5, §6): parse
  to binary32 using IEEE-754 round-to-nearest-ties-even, re-emit the
  shortest decimal that reparses to the same bits, and require byte
  equality. Ordinary `0.1` is canonical; mathematical binary32 exactness
  is not the test.
- **Hash-domain totality excludes named byte-identity digests** (§5, §9,
  §12, §13, §16): semantic/composite hashes remain registered and domain-
  prefixed; ContentHash, raw-file, staged dylib/tool, CAS-payload, and
  pack/archive file/trailer digests are the closed exception class, raw
  blake3 over exactly the named bytes.
- **Scheduler configuration preserves progress** (§13, §18): staging
  requires `parallelism >= 1` and reservation in
  `1..=max(1, parallelism-1)`; one-worker mode alternates oldest batch and
  interactive work, and live resizing re-clamps while active slots drain.
- **CAS CRC coverage is disjoint and exact** (§13): CRC-32C covers header
  bytes `kind` through `content_hash` inclusive plus static-input key,
  output key, and payload; magic, version, the CRC field, and padding are
  excluded, eliminating self-referential interpretations.
- **Editorial / implementation feedback: distill-wire semantics are
  pinned** (§12): DSWL enums include variant count; flat copies exclude
  padding; sequence and map-pair strides use the stated align-up formulas;
  Box/Arc lengths equal pointee wire size; non-single enums use one root
  SwitchVariant with enum-relative subplans; `ExecLimits` defaults to
  depth 128 / allocations 2^32; and the wire-tree reader, not the fixup
  executor, verifies unnamed padding while the executor validates every
  byte and status its ops name.

<!-- R23_LEDGER_BEGIN count=17 -->
- **Generic asset roots are rejected** (§4, §5): macro expansion and
  source-walk reject any generic/lifetime/const parameter on a root
  `#[asset]` item, so one TypeUuid never denotes a family; fully concrete
  nested monomorphizations remain legal and retain their nominal arguments.
- **Compiled type semantics have a complete pre-registration attestation**
  (§3, §5): the C-ABI `compiled_types` export carries sorted TypeUuid, DSLH,
  DSNL, `build_only`, and canonical bytes for every remaining
  DSLH/DSNL-excluded semantic/policy fact, plus a `DSCA` aggregate.
  Source-walk's expected table
  is compared before any Rust-ABI registration and again on every reload.
  (Refined in R24: those bytes are the finite `RegistryExtrasV1` row grammar,
  and the same complete rows/DSCA now cross descriptors, packs, connect, and
  reattestation.)
- **Module ABI panic containment is bidirectional** (§3, §4, §9): module-owned
  targets use module-side `catch_unwind` status thunks; every host-owned
  reverse callback uses a host-side status thunk. `Registry`, `EncodeSink`,
  `Outputs`, and `ProcessContext` expose explicit callback results, and the ABI
  audit enumerates both call directions. (Refined in R24: a failed unpublished
  candidate also owns a token and status-bearing registration arena with
  reverse cleanup, unload, and conditional dlclose/leak semantics.)
- **Schema acceptance has a durable source-controlled manifest** (§2, §6,
  §11, §13): exactly one authoring-only `SchemaLineageManifest`, mutated only
  by explicit schema acceptance/rollback, records every accepted type epoch
  even when no data bundle is written. Bundle stamps verify against it; after
  state loss, missing manifest authority is never forward proof. (Refined in
  R24: `Ready` additionally requires exact registry TypeUuid/current-digest
  equality, and a mismatch retains the named candidate as
  `SchemaAcceptanceRequired`.) (Refined in R29: current missing/duplicate
  authority alone enables a separate unbound local repair capability whose
  exact-basis operations can create the configured absent file or remove
  explicitly selected duplicates, never edit a unique valid manifest.)
- **Lineage separates append-only history from the current cursor** (§6, §11,
  §13): a forward acceptance appends `(digest, forward_parent=old_current)`
  and advances; deliberate rollback moves only the cursor and requires
  complete validated reverse custom paths from current and all live stamped
  schemas that are not forward ancestors. Automatic diffs follow parent links
  forward only. (Refined in R24: accept and rollback are stale-base- and
  candidate-identity-checked, may select only the candidate's exact digests,
  and automatic migration runs only through `Ready`.)
- **Codegen publication revalidates an outcome-bearing basis** (§20): every
  attempt returns its pinned basis, complete hit/miss/failure trace, and
  proposed batch or typed failure. The coordinator revalidates immediately
  before trace/result publication and before filesystem mutation; mismatch discards the
  whole attempt and requeues, closing discovered-dependency lost wakeups.
- **Placeholder dependencies are visited to a same-basis fixpoint** (§4,
  §15): each minted placeholder runs through the generated encode visitor and
  a reference-only host sink; non-reference events are ignored, strong refs
  resolve and type-check under the sweep basis, new edges expand the union
  graph to fixpoint, and any failure aborts the component swap.
- **Every target-bound RPC uses one fenced envelope** (§15, §17): all Hub and
  Snapshot methods return success or the uniform typed arms
  `ReconnectRequired(reason)`, configuration poison, lease failure, and RPC
  error. Stale generations return no data, and `IoEvent::ReconnectRequired`
  carries the same typed reason to the loader. (Refined in R24: each method
  has a schema-native typed-success union, and one fixed four-value reconnect
  reason mapping is shared by Cap'n Proto, Rust, and LoaderIO.)
  (Refined in R25: `AuthoringSnapshot` joins the fenced typed surface, while
  unbound `Root.connect` gets a dedicated typed ConnectResult with attestation,
  configuration, and protocol failure arms.) (Refined in R27: the reconnect
  vocabulary adds `CompiledAttestationChanged=4`; `Root.connect` is the
  target-bound bootstrap despite the older wording, while separate unbound
  `Root.metadata` remains usable under every poison with only
  store/protocol/lease-fenced metadata, inspection, diagnostics, and immutable
  CAS capabilities.)
- **Archives have exact content-addressed names and durable no-replace
  publication** (§16): `archive-<64 lowercase full-file-hash hex>.dpk` uses raw
  blake3 over complete bytes including the trailer. An existing same-name file
  must match hash and bytes; archives and their directory are fsynced before a
  referencing manifest, and no archive is ever overwritten. (Refined in R24:
  hash-named manifest files use the identical same-directory no-replace,
  existing-byte verification, and pre-pointer fsync protocol.)
- **Bare reference strings have UUID-first syntax** (§4): every UUID-shaped
  string is a UUID selector with no path fallback; a UUID-shaped bundle path
  must use the explicit `{ path: ... }` object form.
- **Directory aliases and cycles use different identity sets** (§14, §18): an
  ancestry set detects cycles; a scan-global identity map rejects every
  non-ancestor inode alias, including same-inode configured roots, as
  configuration poison before any candidate rows publish.
- **Bundle entry role is explicit and uniformly enforced** (§6, §10, §16):
  reserved metadata, rules, migrations, pack definitions, and the lineage
  manifest are `authoring_only`; they are never primary or runtime/shipping
  results. Only an explicit tooling query may select them, and pack roots can
  never opt them in. (Refined in R24: direct runtime UUID resolution and every
  dependency/closure carrier return typed `RoleIneligible`; value inspection
  is a separate non-building tooling RPC. Refined in R25: exact internal
  reference resolution records a role-index trace/fingerprint, and daemon
  control consumption uses a private `ControlSnapshot`, never the public
  tooling/runtime query mode.)
- **Default tables use finite schema-graph keys** (§3, §11): each writer is
  keyed by deterministic `SchemaNodeId` plus node-local typed path; recursive
  back-references reuse the first node id instead of unrolling an infinite
  root path.
- **Structural zstd frames are bounded and dictionary-free** (§16): one
  self-contained frame per block declares decoded size and window no larger
  than 256 KiB. Readers reject nonzero dictionary ids, missing/oversized sizes,
  oversized windows, concatenated/skippable payloads, and size mismatch.
- **Generated physical names are fixed AssetUuid identities** (§7, §10, §20):
  module `sp_<32 lowercase hex>` and file `sp_<32 lowercase hex>.rs` contain no
  authored identifier. An optional `[a-z0-9_]+` ASCII slug is capped at 32
  bytes, display-only, and never authoritative.
- **Local failure and configuration-poison fingerprints have dedicated
  domains** (§5, §9, §13, §17): `DSLF` canonically encodes typed local failure
  detail and `DSCP` canonically encodes stable poison reason facts; both exclude
  presentation messages and sort unordered facts. (Refined in R24: v1 now has
  exhaustive fixed discriminants and per-variant fields, framing, unknown-code
  rejection, and explicit version-evolution rules. Refined in R25: DSLF adds
  migration-function, output-binding, importer/intake, and artifact-encoding
  producers with stable codes and AssetUuid edge identities; DSCP uses entry
  identities and canonical symmetric-pair ordering.)
- **Erased-value constructors receive ownership tokens explicitly** (§4,
  §15): `AssetRuntimeDescriptor::finalize` and `PlaceholderThunk::make` take
  `ModuleEpochToken` as an argument and embed that exact token in the returned
  `ErasedValue`; ambient or tokenless epoch inference is forbidden.
<!-- R23_LEDGER_END -->

<!-- R24_LEDGER_BEGIN count=9 -->
- **Failed candidates have an owned cleanup state machine** (§3, §13): every
  opened candidate receives an unpublished `ModuleEpochToken` and registration
  arena; failure fences it, destroys installed objects in reverse order through
  status thunks, then calls `unload`, and permits `dlclose` only when every
  status succeeds with no poison or pins. Otherwise `CandidateOpen` records the
  disposition and the host deliberately leaks arena and library. (Refined in
  R25: generated erased capsules transfer ownership on host-callback entry,
  install an arena guard before any fallible action, and return an explicit
  Consumed disposition, closing the rejection/panic handoff seam.)
- **Registry publication is locked to the lineage-manifest cursor** (§2,
  §§5–6, §11, §13): `Ready` requires the exact manifest TypeUuid set and exact
  selected logical digest for every compiled row. A mismatch is stable
  `SchemaAcceptanceRequired`; accept/rollback recheck manifest hash, cursors,
  candidate identity, and candidate-equal selected digests, and only `Ready`
  can authorize an automatic migration. (Refined in R25: equality is against
  Active rows; Retired rows preserve history but are excluded, and explicit
  stale-base candidate-checked retire/reactivate transitions are the only way
  authority state changes.) (Refined in R27: the closed five-row bootstrap
  control source and DSCI-keyed measured output remain in DSCA but validate
  against bundle-format authority under a separate gate and are the only
  exclusion from Active-manifest equality.)
- **Registry extras have a finite canonical grammar** (§3, §5):
  `RegistryExtrasV1` uses deterministic schema-node IDs and back-references,
  fixed path/fact discriminants for references, blob, tag, skip, build/load
  policy, and control role, canonical row ordering/framing, duplicate and
  unknown rejection, and source extraction fails for an unrepresentable fact;
  DSRE and DSCA hash the canonical rows themselves. (Refined in R26:
  `RegistryExtraFact::Tag` projects into registered DSTA v1 with TypeUuid/path
  ordering, fixed facts, duplicate rejection, and independent staging,
  index-publication, and revalidation recomputation.)
- **Complete compiled-type attestation reaches every consumer** (§§3–5,
  §§15–17): runtime descriptors, pack headers, `Root.connect`, and `reattest`
  carry full `CompiledTypeRow`s plus DSCA. Packs compare their exact closure
  union the five bootstrap rows as a subset of the registered game set; RPC
  compares the registered client runtime set union those rows as a subset of
  the daemon set, with every overlapping row field-equal and bootstrap rows
  independently equal to each local consumer's embedded authority.
  (Refined in R25: concurrent reattestation installs by exact base+1 CAS and
  typed connect success returns its initial generation.) (Refined in R27:
  every Hub binds the daemon DSCA projected onto its accepted client runtime
  TypeUuid set union bootstrap; every call fences projection drift, while reattestation also CASes the
  underlying daemon compiled-table generation.)
- **Reconnect results are generated-binding-safe and identical end to end**
  (§15, §17): Cap'n Proto methods use method-specific typed-success unions with
  the same four failure arms, while Cap'n Proto, Rust, LoaderIO, and events use
  the single fixed mapping `TargetDefinitionChanged=0`,
  `LoadPolicyChanged=1`, `StoreInstanceChanged=2`, and
  `ProtocolEpochChanged=3`. (Refined in R27: the exhaustive mapping gains
  `CompiledAttestationChanged=4` for drift in the accepted daemon projection.)
  (Refined in R25: bootstrap connect is also generated-binding-safe through a
  dedicated typed ConnectResult, and authoring reads move to a pinned
  AuthoringSnapshot capability.)
- **Authoring-only UUIDs cannot enter runtime resolution** (§6, §10, §15,
  §17): direct runtime resolve and dependency/closure expansion reject them
  with typed `RoleIneligible` before build, process, load, or pack work; a
  separate tooling inspection RPC exposes metadata/value inspection without
  producing artifacts, dependency tokens, or shipping roots. (Refined in R25:
  role failure has a canonical fingerprint and RoleCheck trace, while query
  and inspection share one pinned AuthoringSnapshot stamp.)
- **Each loader terminal answer carries exactly one basis** (§15): the outer
  `IoEvent` is the sole `IoBasis` carrier, basis admission precedes payload
  decoding/adoption, and `ResolveResult::Built` and all other nested payloads
  carry no second basis that could disagree.
- **DSLF and DSCP v1 are exhaustive tagged records** (§5, §9, §13, §17):
  stable discriminants, exact per-variant fields, canonical framing/order, and
  message exclusion define local-failure and configuration-poison identity;
  unknown versions/codes fail and incompatible evolution requires a version
  bump, while processor error-code meaning is pinned by processor version.
  (Refined in R25: missing local producers and same-bundle edge/manifest
  identities are added, and DSCP symmetric operands are canonically ordered.)
- **Hash-named pack manifests are immutable before activation** (§16): a
  manifest is written and fsynced as a unique same-directory no-replace temp;
  an existing final name must match the complete DSPM hash and bytes; the
  published file and directory are fsynced before `pack.current` is replaced.
<!-- R24_LEDGER_END -->

<!-- R25_LEDGER_BEGIN count=10 -->
- **Authoring controls have a private, traced consumption plane** (§6,
  §§10–11, §13): coordinator-minted `ControlSnapshot`/`ControlQuery` authority
  discovers authoring-only Migration edges for planning and rollback, while
  distinct non-artifact readers consume pack definitions, import
  rules/settings, and the lineage manifest. Every hit, empty set, and stable
  failure is basis-stamped and canonically traced/revalidated; the private
  types cannot come from ProcessContext, references, runtime/tooling queries,
  or pack roots, and no control asset is ever built or shipped. (Refined in
  R26: readers enumerate autonomously into a closed `ControlValue`; every
  migration edge is a `ControlSubject::Migration` read recorded before typed
  decode, and successful or failed query/read prefixes remain in `LoadInputs`
  for same-basis revalidation.) (Refined in R27: `LoadInputs` has one ordered
  `Vec<TraceOp>` authority across control, reads, and capabilities; split views
  are derived only and a terminal failure is necessarily last.)
- **Schema authority supports explicit retirement and reactivation** (§2,
  §§5–6, §11, §13): each lineage preserves append-only epochs/current plus
  `Active | Retired`; Ready compares the candidate exactly with Active rows
  while Retired history remains authoritative. Retirement requires exact
  stale-base candidate omission plus proof of no live authored entry or
  migration endpoint; reactivation requires candidate inclusion and explicit
  selection of its exact digest under normal history rules. Neither transition
  is inferred from staging. (Refined in R26: the retirement CAS includes the
  exact metadata/control SnapshotStamp negative proof, every publication path
  maintains the no-live-entry/endpoint invariant, violations become typed
  `RetiredTypeReferenced`, and reactivation admits waiting bytes only
  atomically.)
- **Registration ownership transfers before failure is possible** (§3, §13):
  generated `ErasedRegistrationCapsule`s suppress automatic drop and carry the
  candidate owner token plus contained destroy thunk. Host callbacks consume
  on entry, install the guarded arena record before duplicate checking or any
  fallible action, catch host panic only after that guard exists, and return an
  explicit `Consumed` disposition; reverse arena cleanup is the sole
  destructor path. (Refined in R26: the module allocates an intrusive capsule
  node; the host thunk's first panic-bounded instruction pure-moves it into an
  allocation-free guard and pointer-links it, while link/rejection/panic
  failure is independently latched even if module code ignores the diagnostic
  status.)
- **Candidate target sets have the DSTS identity grammar** (§5, §6, §13,
  §18): DSTS v1 hashes a name-sorted, duplicate-rejecting sequence of
  `(NFC-normalized target name, recomputed DSTG)` rows. Candidate staging,
  request decode, and commit independently recompute it; supplied opaque hash
  bytes are never trusted.
- **Reattestation installs by generation CAS** (§15, §17): every Hub owns an
  `attestation_generation`; a request carries current base and exact
  never-reused `base+1` successor, with overflow a hard failure. Validation
  uses one daemon snapshot and CAS installs only if the base is still current;
  stale completions cannot mutate, success echoes the installed generation,
  and only that echo unblocks loader traffic. (Refined in R26: install CASes
  the full attestation/target/policy/store/protocol tuple; `ReattestResult`
  has dedicated typed success, attestation/stale/overflow/reconnect/poison/
  lease/RPC arms with pinned payloads, and RpcIO rotates basis/epoch and drops
  old outstanding work atomically before unblocking.)
- **Role ineligibility is an observed dependency outcome** (§6, §§9–10,
  §15): `StableFailureFingerprint::RoleIneligible` and `TraceOp::RoleCheck`
  preserve the target UUID and observed role. Strong/direct internal
  references report the typed role failure rather than Missing and revalidate
  the role index, so an explicit role edit wakes the memo while all public
  runtime carriers remain barred.
- **DSLF covers every named memoizable local producer** (§5, §§8–9, §11,
  §13): migration functions, output binding, importer returns, daemon intake,
  and artifact encoding join validators, migration planning, and processor
  errors with fixed classes/codes and exact fields. Conflicting migration
  edges use sorted AssetUuid identities, so siblings in one bundle remain
  distinguishable; presentation text stays outside the fingerprint and new
  producers require a grammar arm/version. (Refined in R26: option-filled
  code records are replaced by per-code payload variants whose required
  path/key/type/role/count and expected/observed facts are exact;
  noncanonical code/payload combinations reject.)
- **DSCP canonicalizes entry identity and symmetric operands** (§5, §6,
  §§13–14, §17): duplicate lineage manifests name sorted/deduplicated
  manifest AssetUuids rather than bundles. Owned-path overlaps order their two
  `(kind,path)` sides and directory aliases order their two normalized
  path/identity sides by canonical bytes; every future symmetric variant must
  declare the same normalization rule. (Refined in R26: invalid-path keys and
  owned-path kinds are fixed numeric enums, and DSTR v1 pins control query/read
  subject tags, `ControlFailureCode` u16 encoding, entry ordering/deduplication,
  and noncanonical-decode rejection. Refined in R29: persisted and RPC poison
  values carry the complete typed same-code `DscpV1` detail; decode recomputes
  DSCP and rejects unknown, noncanonical, mismatched, or digest-invalid data.)
- **Authoring reads are snapshot-consistent** (§10, §17): Hub mints a
  tooling-only `AuthoringSnapshot` pinned to one SnapshotStamp and lease;
  authoring query and value inspection both execute at that basis and refresh
  explicitly. Their result types still cannot build, process, trace a runtime
  dependency, or become a pack root/member. (Refined in R26: inspection
  repeats the exact SnapshotStamp and uses a closed branded metadata/
  canonical-value/blob schema with typed missing and role-ineligible arms and
  no ContentHash, artifact, runtime, dependency, or shipping carrier.)
- **Connect success is the sole RPC-basis generation carrier** (§15, §17):
  unbound `Root.connect` returns a method-specific typed `ConnectResult` whose
  success carries Hub, StoreInstanceId, and accepted policy, target, and
  attestation generations; typed failure arms cover attestation,
  configuration, protocol, and ordinary RPC errors. RpcIO constructs
  `IoBasis::Rpc` only from that success and never invents a generation.
  (Refined in R26: `ConnectSuccess` is the sole *initial* generation source;
  only installed `ReattestSuccess` may supply its attestation successor, and
  RpcIO performs that basis/connection-epoch rotation atomically.) (Refined in R27:
  `Root.connect` is target-bound despite the older wording, both success
  records also carry the accepted daemon compiled projection, and the unbound
  metadata bootstrap has its own store/protocol-only basis and cannot mint
  RpcIO.) (Refined in R28: `ReattestSuccess` may also supply the successor
  accepted-set policy projection/digest/generation, while target generation
  remains connect-only. Refined in R29: `ConnectSuccess` next ordinals carry
  the server-recomputed initial policy rows/digest, canonically validated by
  RpcIO instead of reusing request values.)
<!-- R25_LEDGER_END -->

<!-- R26_LEDGER_BEGIN count=11 -->
- **Migration control reads are closed, typed, and failure-complete** (§6,
  §§10–11): every queried edge is read as `ControlSubject::Migration(uuid)`
  into a fully decoded `MigrationControlValue` carrying endpoints, lineage,
  kind, and function/ops facts. Its `ControlRead` is installed before
  decode/validation, stable failure hard-stops, and `LoadInputs` plus failure
  records retain and revalidate the complete query/read prefix.
- **Retirement negative proof is continuously enforced** (§6, §11, §13): the
  retirement CAS rechecks the exact metadata/control SnapshotStamp proving no
  live entry or Migration endpoint. Scan, import, CRUD, and manual publication
  preserve that invariant; typed `RetiredTypeReferenced` requires explicit
  atomic reactivation before waiting bytes are admitted.
- **Registration handoff is allocation-free before any failure seam** (§3,
  §13): the module allocates an intrusive capsule node, and the host thunk's
  first instruction inside its panic boundary pure-moves it into a local guard
  and pointer-links it without allocation. Link failure latches cleanup/leak;
  the host independently latches first rejection even if status is ignored,
  while `must_use` remains diagnostic only.
- **Reattestation CAS covers the complete connection fence** (§15, §17):
  validation captures and installation CASes attestation, target, policy,
  StoreInstanceId, and protocol epoch together. Any non-attestation drift
  returns its exact reconnect reason and never mutates Hub state. (Refined in R27:
  the CAS also captures the underlying daemon compiled-table generation
  and atomically installs DSCA projected onto the successor accepted set.)
- **Reattestation has a dedicated structured wire result** (§17):
  `ReattestResult`/`ReattestSuccess` distinguish attestation failure, stale
  base, generation overflow, reconnect, configuration poison, lease failure,
  and RPC error. Attestation failures identify type/target/table/aggregate/
  policy subjects with fixed numeric codes and code-specific payloads.
  (Refined in R27: an exhaustive code/subject/payload matrix fixes every byte
  width and rejects every unlisted tuple in both connect and reattest.)
- **All authoring-control readers use one closed private value sum** (§6,
  §10): `ControlValue` contains only Migration, PackDefinition,
  DirectoryImportRules, ImportSettings, and Lineage; autonomous basis-stamped
  enumeration includes directory rules, every decode is recorded and typed,
  and no raw `AuthoredValue`, public/runtime value, artifact, or pack carrier
  escapes.
- **Tag annotations have the registered DSTA epoch grammar** (§5, §10): DSTA
  v1 hashes TypeUuid-sorted canonical tag paths with fixed facts, rejects
  duplicates/unknown/noncanonical rows, and is independently recomputed at
  candidate staging, index publication, and dependency revalidation.
- **RPC generation sources and rotation are singular** (§15, §17):
  `ConnectSuccess` alone supplies the initial generations and installed
  `ReattestSuccess` alone supplies an attestation successor. RpcIO atomically
  rotates basis and connection epoch, discards all old outstanding work, and
  only then unblocks traffic. (Refined in R27: those same two typed successes
  are also the sole carriers of the initial/successor daemon compiled
  projection, rotated in the same atomic basis update.)
  (Refined in R28: because reattestation may replace the accepted runtime set,
  its typed success also carries and atomically rotates that set's
  union-bootstrap policy rows/digest/generation; target generation remains
  connect-only. Refined in R29: connect success symmetrically carries the
  server-recomputed initial union-bootstrap policy rows/digest. Refined in
  R31: the replaceable set is canonical accepted `A`; an expansion challenge
  reattests `A union required` before retry.)
- **Authoring inspection is exact, branded, and non-shippable** (§10, §17):
  every response repeats its precise SnapshotStamp and returns only the closed
  metadata/canonical-value/blob schema, with typed missing and role failures;
  ContentHash, artifact/runtime capability, dependency token, and root/member
  carriers are absent. (Refined in R27: Blob leaves use only the schema-guided
  `{"$distill_blob":u32}` token, indices cover 0..N-1 exactly once, and the
  returned canonical logical schema must recompute to `schemaHash`.)
- **DSLF uses per-code payload variants** (§5, §§8–9, §11): migration-plan,
  output-binding, import-intake, and artifact-encoding failures carry only the
  exact required fields for their fixed code, including expected/observed
  identities where applicable; option-filled catch-all combinations and all
  other noncanonical encodings reject.
- **DSCP and control DSTR vocabularies are numeric and closed** (§5, §9,
  §§13, 17–18): `InvalidPath.key` and `OwnedPathSide.kind` are fixed enums;
  DSTR v1 pins control query/read subject tags, `ControlFailureCode` as u16-LE,
  per-code entry cardinality, sorting/deduplication, and unknown/noncanonical
  rejection. (Refined in R27: DSCP code 13 is exactly
  `DuplicateTargetName { normalized_name }` across staging, persistence, and
  wire decode.)
<!-- R26_LEDGER_END -->

<!-- R27_LEDGER_BEGIN count=9 -->
- **Live RPC attestation fences daemon projection drift** (§§3, 13, 15,
  17–18): each Hub stores its accepted client runtime TypeUuid set and DSCA
  recomputed from those daemon rows union the five bootstrap rows;
  `ConnectSuccess`, `ReattestSuccess`, and
  `IoBasis::Rpc` carry it. Every target-bound call compares the current
  projection and returns `CompiledAttestationChanged` on drift, while daemon
  additions outside the set do not fence. Reattestation also CASes the
  underlying compiled-table generation and installs the new-set projection
  atomically without mutation on drift. (Refined in R28: DSLP and its
  generation are Hub-sensitive over the same union; full-table generation is
  only a reattest race CAS, and outside-set runtime closure data is refused
  before service.) (Refined in R31: refusal is the no-data
  `AttestationExpansionRequired` challenge; expansion reattests `A union
  required`, while `CompiledAttestationChanged` remains projection drift.)
- **Poison-safe metadata has an unbound bootstrap** (§§13, 17–18):
  `Root.metadata` requires no target, compiled, policy, or PipelineEpoch
  attestation and succeeds under configuration/pipeline/version poison and
  `SchemaAcceptanceRequired`. Store/protocol/lease-fenced capabilities expose
  only pure metadata snapshot/refresh, authoring inspection, typed diagnostics,
  and immutable CAS fetch — never resolve/build, mutation, subscription,
  pipeline-dependent query, pack roots, or `last_good` substitution.
- **Authored publication is no-overwrite and crash-total** (§§2, 14, 17): the
  supported Unix daemon hosts use one durable rename-aside state machine that
  flushes journal/file/directories at every transition; install is atomic
  no-clobber from a daemon temp and restore links from the retained aside. Any
  target that reappears is journaled and conflict-preserved alongside the temp
  and verified aside before stop/retry, and no overwrite primitive conforms.
- **Bootstrap controls have one closed authority exception** (§§2–3, 5–6,
  13): the fixed five-row semantic source and DSCI-keyed, DSNL-measured output
  remain in every compiled table and DSCA, validate byte-for-byte under a
  separate bootstrap gate, and are exactly the set excluded from Active-
  manifest equality. All other compiled rows equal Active authority; the set
  cannot be configured or extended. (Refined in R28: every pack/RPC boundary
  projection is its requested runtime set union these five rows, with DSCA and
  DSLP recomputed over that union and each local consumer independently
  matching its embedded authority; the rows never enter runtime closure.)
- **Duplicate target names have a canonical DSCP arm** (§5, §13, §18):
  `DuplicateTargetName { normalized_name }` is fixed code 13 in target staging,
  persisted poison, RPC, and decode; DSTS duplicate rejection cannot be
  misreported as a root collision or generic parser error.
- **LoadInputs has one observation-order authority** (§§9, 11): one
  `Vec<TraceOp>` records interleaved control queries, reads, ordinary reads, and
  capability lookups exactly when observed. Typed views are derived only,
  legacy split vectors reject, and an observed terminal failure is last.
- **Version-global poison has closed stable identity** (§§7, 9, 13, 17): DSVP
  canonically tags duplicate asset/bundle UUIDs, same-root normalized-path
  collisions, incomplete skeleton, and stable unreadable global-path classes.
  Snapshot, query, persistence, invalidation, and RPC carry the same value; it
  never becomes a bundle sentinel or build memo. (Refined in R28: asset UUID
  poison carries tagged authored/derived claimants, same-root path poison
  carries lossless raw physical claimants, and cross-root same-path files stay
  the ordinary `Ambiguous(roots)` index state. Refined in R29: invalid physical
  names have code 6/raw platform bytes/reason, and the one authoritative poison
  is the canonical first of the completely enumerated defect set.)
- **Authoring blob inspection is schema-authenticated** (§§6, 10, 17): only a
  logical Blob node interprets the exact `{"$distill_blob":u32}` JSON token;
  indices cover the blob table 0..N-1 exactly once, all other JSON stays
  canonical, and inspection carries the canonical logical schema whose DSLH
  equals `schemaHash`, without artifact/ContentHash authority.
- **Attestation failures have one exhaustive tuple matrix** (§17): every fixed
  failure code admits only its listed subject and payload arms with exact byte
  widths and semantics. Connect and reattest share the matrix and reject
  unknown, malformed, wrong-width, or otherwise unlisted combinations.
  (Refined in R30: every fixed-width Data field has a typed `MalformedField`
  subject with expected width and arbitrary observed bytes; wrong width never
  impersonates semantic `ExpectedObserved` mismatch.)
<!-- R27_LEDGER_END -->

<!-- R28_LEDGER_BEGIN count=5 -->
- **DSVP distinguishes namespace poison from multi-root ambiguity** (§§7, 13,
  17–18): cross-root same-normalized-path files remain `Ambiguous(roots)` and
  affect only path-dependent operations. Code 3 is exactly
  `SameRootNormalizedPathCollision`, naming one root/path and at least two
  strictly ordered lossless raw-platform-path/file-hash claimants.
- **Duplicate AssetUuid poison names every claimant kind** (§§7, 9, 13, 17):
  its at-least-two strict tagged rows are `Authored { source, bundle,
  local_id }` or `Derived { parent, output_key }`, with fixed nonzero tags,
  normalization, canonical ordering, and duplicate rejection; duplicate bundle
  UUID poison continues to carry readable bundle sources.
- **Every boundary attestation includes bootstrap authority** (§§3, 5,
  15–17): pack closure and RPC client runtime sets are each unioned with the
  exact five bootstrap rows for DSCA/DSLP. Mount/connect independently compare
  those rows with the local consumer's embedded DSCI-keyed authority, while
  runtime artifact closure, roots, dependencies, and eligibility stay
  unchanged. (Refined in R30: the complete boundary is exact §3 `B(C)`—every
  manifest-node authored policy type, artifact encoded/terminal type,
  load-edge expected terminal, and bootstrap type—so processing cannot launder
  authored `build_only` policy.) (Refined in R31: packs remain exact `B(C)`;
  RPC connect/reattest bind a canonical accepted superset `A` including
  bootstrap and gate each actual `B(served)` by subset.)
- **RPC registry and policy fences are Hub projections** (§§9, 13, 15, 17):
  DSCA, DSLP, and policy generation cover exactly the accepted runtime set
  union bootstrap; daemon additions outside it neither fence nor become
  servable closure data. Reattest alone uses the full-table generation for its
  validate/install race CAS and atomically rotates successor projections.
  (Refined in R29: `ConnectSuccess` carries the server-recomputed initial DSLP
  rows/digest at ordinals 6/7; request values are never the initial basis.)
  (Refined in R31: that projection is accepted `A`; outside-`A` served
  closures return `AttestationExpansionRequired`, reattest expands `A`, and
  `CompiledAttestationChanged` is only drift of a row already in `A`.)
- **Pipeline poison has interoperable DSPP v1 identity** (§§3, 5, 13, 17):
  fixed codes, origins, cleanup dispositions, and their exhaustive allowed
  matrix hash under `DSPP`; unknown/mismatched tuples reject, presentation
  messages are excluded, and Cap'n Proto diagnostics carry the explicit typed
  record rather than opaque `Data`.
<!-- R28_LEDGER_END -->

<!-- R29_LEDGER_BEGIN count=7 -->
- **Connect success carries the complete initial policy basis** (§§15, 17):
  `ConnectSuccess` ordinals 6/7 contain the server-recomputed, strictly sorted
  load-policy rows and DSLP over the accepted runtime set union the exact five
  bootstrap rows. RpcIO canonically verifies those success fields and constructs
  its first `IoBasis::Rpc` from them, never from request values; the grammar and
  verification are identical to `ReattestSuccess`. (Refined in R30: both now
  carry §3's exact authored-policy/header/load-edge/bootstrap `B(C)`, and a
  fresh unavailable pipeline returns the typed connect arm without minting a
  Hub or spinning attestation.) (Refined in R31: for RPC those rows now cover
  canonical accepted superset `A`; actual served `B(served)` is checked as a
  subset and expands through typed reattestation. Pack rows remain exact.)
- **Missing/duplicate lineage authority has one narrow recovery path** (§§2, 6,
  13–14, 17–18): only current DSCP 10/11 enables unbound `Root.lineageRepair`
  for the explicit local repair command. Inspection returns instance, exact
  stamp, configured missing path or sorted physical claimants; absence/preimage
  CAS, canonical non-bootstrap manifest validation, explicit survivor choice,
  no-replace journal/quarantine/fsync, and crash recovery precede publication,
  with no MetadataHub, general authoring, `last_good`, or silent tiebreak.
  (Refined in R30: Missing includes absent/canonical-occupied/opaque-occupied
  destination basis and preserves/quarantines exact preimages; every claimant
  is the injective root/path/bundle/local-id/asset/file-hash row.)
- **DSVP represents physical names that cannot normalize** (§§7, 10, 13, 17):
  code 6 carries root, lossless Unix-byte/Windows-UTF-16 path, and fixed reason
  1..7 for invalid encoding, absolute/empty/dot/parent components, or forbidden
  characters. It needs no normalized String; scanner publication, DSVP hashing,
  wire decode, equality, and healing all use the exact raw arm and reason.
  (Refined in R30: Unix/Windows predicates and lowest-code precedence are
  byte/code-unit exact, and code 7 separately represents unreadable scan roots
  or subtrees.)
- **Simultaneous global defects have deterministic singular authority** (§§7,
  13): the scanner completely enumerates, canonical-encodes, deduplicates, and
  sorts all candidate defects by `(code, detail bytes)` before selecting the
  first as the one persisted/snapshot/RPC `VersionPoison`. The remaining sorted
  set is doctor-only diagnostics; scan or thread completion order never wins.
  (Refined in R30: unreadable root/subtree enumeration failures participate in
  this same complete set and authority ordering.)
- **ToolEpoch misses are closed, memoizable capability failures** (§§9, 13):
  `CapabilityKey::Tool(String)` is fixed tag 5 after tags 1..4. An absent tool
  terminates `TraceOp::Tool` with
  `Observed::Err(MissingCapability { Tool(id) })` and heals when revalidation
  resolves a tool identity. (Refined in R30: a hit records
  `Observed::Ok(tool_hash)`; post-hit launch failures are transient,
  trace-discarding, and never memoized, while DSTR v1 tag 4 `ToolLaunch` is
  permanently reserved and decode-rejected and `Tool` uses tag 10.) (Refined
  in R31/R32: both historical hash references now mean the aggregate DSCT tool
  hash, and the transient carrier field is `tool_hash`.)
- **Configuration poison carries its complete DSCP reason** (§§5, 13, 17–18):
  Rust state, persistence, and Cap'n Proto now carry the exact same-code typed
  `DscpV1` detail in addition to code/hash/message. Decode validates version,
  tags, payload widths/order/normalization, code/detail pairing, and recomputed
  reason hash; presentation text never supplies missing type facts. (Refined in
  R30: code 14 carries raw configuration-source failure, and the authoritative
  poison is the canonical first of the complete defect set.)
- **BundleFileHash is observed-byte identity, not canonicality proof** (§§5–7,
  9, 11, 13): it is raw blake3 of the exact file bytes for valid and malformed
  bundles alike, making incomplete-skeleton and repair preimages representable.
  Bundle parsing/canonical validation remains an independent mandatory gate;
  no hash equality authorizes malformed bytes as canonical.
<!-- R29_LEDGER_END -->

<!-- R30_LEDGER_BEGIN count=11 -->
- **Tool lookup and execution failures are distinct** (§§9, 13): an absent
  ToolEpoch key is the memoizable `MissingCapability { Tool(id) }`; a hit
  records `Observed::Ok(tool_hash)`, while NotExecutable, SpawnDenied,
  PackageUnavailable, and AmbientUnavailable return transient
  `{id, tool_hash, class}` and discard the attempted trace/result. DSTR v1
  tag 4 `ToolLaunch` is permanently reserved and decode-rejected; `Tool` is
  fixed tag 10. A hit records the aggregate DSCT `tool_hash`, and the transient
  carrier uses that field; the R30
  trace-discarding/nonmemoized outcome rule is unchanged.)
- **Missing-lineage repair has destination-aware exact preimages** (§§6,
  13–14, 17): inspection distinguishes `Absent`, occupied canonical bundle,
  and occupied opaque bytes by exact hash/kind. Creation is no-replace;
  canonical occupation preserves every non-manifest entry and adds exactly one
  manifest, while opaque occupation is quarantined raw and recoverably before
  canonical install; CAS, journal, fsync, restart recovery, and rescan precede
  publication.
- **Lineage claimants are injective entry rows** (§§5–6, 17): DSCP detail,
  repair inspection, stale checking, and survivor selection carry and strictly
  sort `(root, path, bundle, normalized local_id, asset, file_hash)`, so two
  manifests in one physical bundle remain distinct and repair names exact
  entries.
- **Fresh connect reports pipeline unavailability as a closed type** (§§13,
  17): `ConnectResult.pipelineUnavailable` distinguishes pipeline poison,
  schema acceptance required, and retired-type authority. No current usable
  epoch—especially no `last_good`—mints no Hub; retry waits for new published
  state/reconnect rather than looping successful attestation.
- **Filesystem identity is not a persisted protocol fact** (§§5, 14, 17–18):
  R37 replaces the former cross-platform physical-identity grammar with
  canonical configured paths plus observe/revalidate/retry. Directory-alias
  DSCP detail carries the two normalized paths; implementation-local metadata
  may be used transiently but is not hashed, persisted, or exposed on the wire.
- **Unreadable scan coverage is version poison** (§§7, 13–14, 17): DSVP code
  7 carries `Root {root_name}` or `Subtree {root_name, raw_relative_path}` and
  one of PermissionDenied, NotFound, InvalidFileType,
  SymlinkIdentityChanged, or IoDataLoss. Enumeration failures join the
  canonical defect set; an unreadable branch is never silently empty.
- **Configuration-source loss is typed and never defaults** (§§5, 13,
  17–18): DSCP code 14 carries the raw Unix/Windows configured path and exact
  Missing, PermissionDenied, InvalidFileType, or IoDataLoss class. Deletion or
  read failure publishes poison instead of reverting to implicit defaults.
- **Simultaneous configuration defects have singular canonical authority**
  (§§5, 13, 17–18): staging enumerates, canonical-encodes, deduplicates, and
  sorts the complete DSCP set by `(code, detail bytes)`; the first is the sole
  persisted/snapshot/RPC poison and the rest are doctor-only. Lineage repair is
  enabled only when its defect is that authoritative first row.
- **Raw physical-path reasons are byte-exact** (§§7, 10, 17): Unix and Windows
  encoding, absolute-form, separator/component, dot/parent, and forbidden-unit
  predicates are pinned with lowest-code precedence. Persistence and RPC
  decode recompute the reason from lossless units and reject an unproved or
  noncanonical arm/code pair.
- **Wrong-width attestation fields have a typed carrier** (§17):
  `MalformedField` names every fixed-width request field, carries
  `expectedWidth:u32` plus arbitrary length-framed observed bytes, and is in
  the exhaustive connect/reattest tuple matrix. A width error can never use
  `ExpectedObserved` or fall through to an untyped RPC error.
- **All boundaries derive the same complete type set** (§§3, 9, 15–17):
  `B(C)` is the union of every closure asset's authored policy type, every
  verified artifact encoded/terminal type, every load edge's expected terminal
  type, and bootstrap. Pack construction/mount and RPC connect, coverage,
  reattest, and RpcIO recompute that set for DSCA/DSLP, preventing authored
  `build_only` laundering through processing. (Refined in R31: pack boundaries
  still attest exact `B(C)`; RPC derives the same `B(served)` per response but
  connect/reattest attest canonical superset `A`, with a typed expansion
  challenge when the subset proof fails.)
<!-- R30_LEDGER_END -->

<!-- R31_LEDGER_BEGIN count=3 -->
- **Entry lineage has a closed bootstrap arm** (§§3, 6, 11, 13): bundle
  entries carry canonical `EntryLineageV1 = Manifest(LineageStamp) |
  Bootstrap { bundle_format_version }` with fixed record/JSON tags. The
  Bootstrap arm is legal and required for exactly the five sealed format-v1
  control TypeUuids, whose schema facts and authoring/control roles must equal
  `BootstrapControlTableV1`; every non-bootstrap type requires the exact
  manifest-prefix arm. Scanner, schema closure, adoption, repair, and control
  reads share that validator, so missing-manifest repair can install the first
  bootstrap manifest without putting any bootstrap entry in user migration.
- **Lazy RPC attestation binds an accepted superset** (§§3, 15, 17): connect
  and reattest send complete canonical rows and DSCA/DSLP for accepted `A`, and
  the Hub binds that set after daemon-table verification. Each actual served
  closure recomputes `B(served)` and proves it is a subset; otherwise the
  no-data `AttestationExpansionRequired` arm carries its pinned snapshot/DSAE
  identity and exact required TypeUuid set, RpcIO reattests `A union required`,
  and retries. `CompiledAttestationChanged` now means only drift inside `A`;
  pack boundaries remain exact `B(C)`.
- **R31 tool capsules are superseded by R32 packaging** (§§3, 5, 9, 13): R31
  introduced recursive interpreter/DSO/plugin/resource closure discovery. R32
  removes that executable-loader model: DSCT v2 covers a complete package
  directory or an explicit ambient toolchain identity. `TraceOp::Tool::Ok` and
  revalidation use the DSCT tool hash; post-lookup launch failures remain
  transient and nonmemoized.
<!-- R31_LEDGER_END -->

<!-- R32_LEDGER_BEGIN count=4 -->
- **Tool packaging replaces executable-loader emulation** (§§3, 5, 9, 13):
  DSCT v2 identifies either a recursively snapshotted package directory or an
  explicit ambient toolchain registration. Distill does not parse ELF, Mach-O,
  PE, shebang, interpreter, DSO, rpath, or platform-loader metadata. Package
  production is responsible for supplying every byte the tool needs.
- **Ambient tools are honest about cache authority** (§§9, 13): an ambient
  registration carries an absolute launcher and `toolchain_id`. A caller-supplied
  trusted fingerprint permits memoization; without one the tool may run, but the
  complete calling processor attempt is never committed as a success or failure
  candidate.
- **Package identity is directory identity** (§§5, 9, 13): package DSCT commits
  every canonical regular-file path, executable bit, length, and byte hash plus
  the launcher, sanitized environment, and cwd policy. Symlinks and special files
  are rejected; launch reopens and verifies the staged package.
- **Pipeline static linkage is a build contract** (§§3, 9, 20): pipeline-only
  native dependencies remain statically linked and runtime `dlopen` remains
  banned, but the daemon does not parse candidate binaries or implement an OS
  loader policy. The pipeline build/CI owns that linkage check.
<!-- R32_LEDGER_END -->

<!-- R33_LEDGER_BEGIN count=4 -->
- **Semantic attestation replaces reconstructed layout closures** (§§3, 5,
  12, 18): the exact Cargo feature/cfg/source/manifest/lock dependency closure
  is removed. Source-walk and the candidate module independently produce the
  complete CompiledTypeRow table, and exact row/DSCA equality checks the
  consequences that matter directly.
- **LayoutIdentity is a table selector, not module attestation** (§§3, 5, 18):
  it contains only target triple, rustc identity, and source-walk algorithm
  version under DSLI. The module does not export or compare it;
  ModuleAbiIdentity gates the Rust host interface, while target binding and
  per-output logical/DSWL hashes prevent layout reuse under the wrong table.
- **One compiled-type probe carries the complete asset attestation** (§§3, 5):
  an opened, unpublished candidate exports its independently generated rows and
  DSCA through C ABI before registration. Because every row already contains
  DSNL, the separate measured-layout probe and duplicate comparison are removed.
- **Source hashes pair emitted artifacts** (§5): `ngp-source-hash` continues to
  drive New Game Plus's schema/module production and pairs those artifacts at
  candidate acceptance, but the daemon does not independently watch workspace
  sources. It is not a Cargo graph identity or an ABI, target, or safety gate.
<!-- R33_LEDGER_END -->

<!-- R34_LEDGER_BEGIN count=6 -->
- **Shared source identity pairs pipeline module and schema** (§§3, 5): the
  daemon uses New Game Plus's shared source export/read implementation before
  Rust registration. This replaces R33's candidate compiled-type probe; source
  identity is a pairing check, while `ModuleAbiIdentity` remains the host-ABI
  check.
- **Runtime compatibility is artifact-local** (§§12, 15–17): authenticated
  terminal TypeUuid/DSLH plus successful DSWL-to-live-descriptor plan
  compilation are the compatibility boundary. Cross-binary DSNL equality is
  removed after local descriptors are made sound.
- **Compiled registry attestation is removed** (§§3, 5, 15–17):
  `CompiledTypeRow`, DSCA, RegistryExtras/DSRE, DSLP, accepted-set projections,
  DSAE expansion, and target-native bootstrap rows duplicate facts already
  owned by schemas, artifacts, descriptors, or the bundle decoder.
- **Module changes reconnect rather than reattest** (§§15, 17): a module epoch
  change invalidates Hubs; clients reconnect and resubscribe. The in-place
  reattestation CAS/generation protocol has no remaining state to mutate.
- **Canonical target rows replace DSTS** (§§5, 13, 18): exact normalized
  `(name, DSTG)` rows are stored and compared transactionally. DSTG remains the
  necessary cross-process identity for one selected target; DSTS is deleted.
- **Unsafe simplification has an ordering gate** (§§4, 12): generic
  `Option<T>` cannot fabricate a niche descriptor. A typed construction path
  or a proven restriction lands before removal of the compensating DSNL gate.
<!-- R34_LEDGER_END -->

<!-- R35_LEDGER_BEGIN count=1 -->
- **Native watcher batches preserve incremental workloads** (§§13–14, 19):
  R35 corrects the over-simplified sticky-full-scan model. Startup still arms
  the watcher before its complete reconciliation, but events retain affected
  paths and ordered rename pairs and are replayed by path-local
  path/subtree observation. Normal live activity never scans or rehashes
  unrelated tree state; a complete scan after startup is reserved for root-set
  replacement, explicit verification, or an admitted native overflow/
  incomplete-observation recovery.
<!-- R35_LEDGER_END -->

<!-- R36_LEDGER_BEGIN count=1 -->
- **Synchronous build reads use bounded native recursion** (§§9, 13, 18): the
  callback API cannot be honestly trampolined without becoming resumable.
  Build workers therefore use explicit 8 MiB stacks, dependency depth is
  constrained to `1..=64` (default 32), and callbacks avoid large stack
  buffers. Concurrent identical stages may execute twice and converge in the
  CAS; the DSSI flight table, cross-job wait graph, waiter threads, and
  cooperative parking machinery are removed.
<!-- R36_LEDGER_END -->

<!-- R37_LEDGER_BEGIN count=1 -->
- **Filesystem operations use a trusted-workspace model** (§§5, 10, 14, 18,
  20): configured roots and daemon-owned output directories are trusted local
  developer workspaces. Standard Rust path and file APIs, canonical containment,
  and observe/revalidate/retry cover ordinary concurrent edits and renames;
  adversarial local namespace races are outside the contract. Retained directory
  handles, descriptor-relative traversal, and persisted platform file identities
  are removed. Windows remains a supported cook target, not an implied daemon-host
  platform promise. Supported Unix daemon hosts use §14's single standard-API
  rename-aside protocol; there is no separate exchange/Windows publication
  implementation. R35's fully incremental post-startup workload invariant is unchanged.
<!-- R37_LEDGER_END -->

### Open — remaining

**Authenticated remote transport.** The RPC endpoint is loopback-only by
validated invariant (§17, §18); serving a daemon across machines needs
an authenticated, transport-secured channel designed as such — not a
loosened bind. Until that lands there is no supported remote access.

**Per-target layout table emission from source-walk.** It
**blocks cross-target artifact encoding**: until it lands, a target is
buildable only if its layout identity (triple, toolchain, algorithm)
equals the schema's (§12) — the daemon refuses others rather than assuming
layout equality — and the §18 non-host target example is illustrative.
source-walk today emits
one schema for one cargo target, with hardcoded 64-bit pointer and
container layouts; the work is real and scoped, and it gates shipping to a
second platform — not phase 1. The *representation* is settled (§5's
`SchemaLayouts` per `LayoutIdentity`); what remains is the emission
work itself.

### Accepted risks (validate early)

- source-walk layout fidelity vs. actual rustc `repr(Rust)` layout: validate
  the emitted DSWL tables early against compiler-derived fixtures for generics
  and exotic enums. Runtime consumers still require a successful plan against
  their compiler-generated live descriptor, but that does not independently
  attest the producer table used during encoding.
- The module host's same-rustc host-interface invariant (carried by the
  shared module-host crate factored out of newgameplus's
  `module_state.rs`, consumed by engine and daemon alike; there is no
  off-the-shelf host).
- `ngp-source-hash` pairs emitted schema and pipeline-module artifacts at
  candidate acceptance; it is not an ABI or safety gate. `ModuleAbiIdentity`
  separately gates the Rust host interface, and runtime compatibility remains
  artifact-local (§3's R34 model).

### Deferred by decision

Patch/compaction tooling (format is patch-ready from day one); the editor
application (RPC surface only); tuning constants (debounce, segment/block
sizes, cache limits).

### Descoped (with the cheap 80% named)

Process isolation for pipeline code — instead, the dylib content hash enters
every input hash, an optional double-run determinism check exists, and the
determinism contract (§9) names the trust boundary explicitly; external
tools are already subprocesses. RPC auth/TLS — the loopback bind is a
validated invariant (§17, §18: non-loopback addresses are rejected at
config staging), and remote access is an Open item above, never a
config knob. Pack signing —
CRCs detect corruption, not adversaries. Flow-control QoS — bounded queues,
disconnect slow clients. Multi-daemon / shared caches — one daemon per
checkout. Exotic filesystems — document supported setups.

### Rejected

Blanket watching of external raw sources — superseded by opt-in per-import
`watch` (§8); the default remains explicit import with weak provenance.
Arbitrary code predicates in queries (invalidation must stay indexable).
Base64 blob encoding (container format instead). Machine state in version
control (no `_distill`; daemon state fully derived). Named derivations —
independently *pullable* per-processor cache entries; §9's extra outputs
are instead reachable only through the parent's artifact, never pulled by
name, and codegen runs as an authoring service. Daemon-side standing-demand /
auto-rebuild state — staleness produces events, never builds. Snapshot
source stashing (copying every referenced source version into the CAS) —
pinned resolves fail on drift instead. Authored deletion tombstones —
deleting a file must not require authoring another; deletion is
client-relative (§15: ever-Resolved-now-Missing surfaces `Deleted`
locally), and daemon-side tombstones would violate §2's disposable-state
rule.
