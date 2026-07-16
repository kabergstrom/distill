# Distill v2 — implementation handoff

_Updated 2026-07-16 after the complete implementation and review pass. This document is
an operational handoff, not a second specification. `DESIGN.md` §§1–21 and its
latest §22 refinements are normative; git history is the authority for landed
milestones._

## 1. Current state

The Distill v2 implementation is present under `v2/` as a sixteen-package Rust
workspace, including its two real dynamic-module test fixtures. The current
implementation/review milestone is commit `6e8d93f` (`Close rotated audit race
conditions`). At that commit:

- `cargo test --workspace --offline` passes, including integration, UI, and doc
  tests.
- `cargo clippy --workspace --all-targets --offline -- -D warnings` passes.
- `git diff --check` passes.
- The complete texture/mesh/shader and schema-transition vertical suites pass.
- Every confirmed CRITICAL/HIGH/MEDIUM adversarial-review finding is fixed with
  a regression or rejected with concrete counterevidence.
- The filesystem overengineering audit is folded into R37 and the code: no
  retained directory handles, descriptor-relative traversal, persisted
  platform file identity, direct NT scanner, daemon `libc` dependency, or
  separate Windows publication backend remains.

Use the installed Rust toolchain directly:

```sh
cd /Users/karl/Projects/distill/v2
cargo test --workspace --offline
cargo clippy --workspace --all-targets --offline -- -D warnings
```

`cargo fmt --all` traverses external path dependencies and therefore attempts
to format repositories that Distill does not own. Format the sixteen Distill
packages explicitly, or format only changed packages.

Do not add `Co-Authored-By` lines. Commit coherent milestones as they become
green. Preserve unrelated New Game Plus untracked files.

## 2. Governing simplifications

The historical §22 ledger contains names from superseded designs. The current
model is the one in §§1–21 plus R34, R35, R36, and R37:

- Pipeline module and emitted schema are paired with New Game Plus's shared
  source identity before Rust registration. `ModuleAbiIdentity` separately
  checks the host ABI.
- Runtime compatibility is artifact-local: authenticate the artifact's terminal
  `TypeUuid` and logical hash, authenticate its DSWL wire tree, and successfully
  compile that tree against the consumer's live descriptor.
- DSCA, DSLP, cross-binary DSNL comparison, `CompiledTypeRow`,
  `RegistryExtras`/DSRE, accepted-set projections, DSAE, DSTS, and in-place
  reattestation are removed. Do not reintroduce them through implementation or
  review findings.
- Exact canonical `(target name, DSTG)` rows are the target-set authority.
- A module epoch change invalidates Hubs; clients reconnect and resubscribe.
- Synchronous build reads use bounded native recursion on explicit 8 MiB worker
  stacks. Dependency depth is `1..=64` with default 32. There is no DSSI flight
  table, cross-job wait graph, waiter-thread system, or cooperative parking
  layer.
- Native watcher events preserve incremental workloads. Normal edits never
  trigger a full tree scan or rehash of unrelated state. Missing control files
  are watched from their nearest existing ancestor; ancestor introduction or
  replacement invalidates the exact control and rebuilds watch coverage before
  reread, without an asset-root scan.
- Filesystem scanning and codegen use the trusted local-workspace model:
  canonical configured paths, standard Rust filesystem APIs, and
  observe/revalidate/retry for ordinary concurrent edits and renames. They do
  not claim confinement against a malicious local namespace-racing process.
  Directory-alias DSCP detail contains the two normalized paths, not a
  persisted platform file identity. Windows is a cook target; that alone is
  not a promise of Windows-host daemon support.

DSCT is still current. It is the canonical identity of a registered external
tool execution environment: either a complete staged package-directory
snapshot plus launcher/cwd/environment metadata, or an explicit ambient
launcher/toolchain identity. A **tool** here is an external subprocess invoked
through the pipeline capability surface; it is not a pipeline module, schema
compiler, binary parser, or arbitrary in-process plugin.

The five checked-in bootstrap control types are:

1. `SchemaLineageManifest`
2. `Migration`
3. `ImportRecord`
4. `DirectoryImportRules`
5. `PackDefinition`

They are fixed semantic schema roots used to bootstrap bundle control records,
not a consumer-runtime compatibility set.

## 3. Shared New Game Plus code

Schema, source identity, and dynamic module hosting are shared with New Game
Plus rather than forked in Distill:

- `/Users/karl/Projects/newgameplus/ngp-schema`
- `/Users/karl/Projects/newgameplus/source-walk`
- `/Users/karl/Projects/newgameplus/ngp-module-host`
- `/Users/karl/Projects/newgameplus/ngp-source-hash`

Relevant New Game Plus milestones, oldest to newest:

- `c03cc91 Share dynamic module hosting`
- `5e1ccf1 Share schema model and projections`
- `843ae03 Pin compact schema snapshot JSON`
- `257159e Open authenticated module descriptors`
- `dccb215 Decode shared schema artifacts safely`
- `ba491c3 Validate authenticated modules before loading`
- `0db47ed Share module source identity across hosts`
- `112255a Share logical schema decoding and narrow layout identity`
- `6dff0e3 Reject duplicate schema merge identities`

`ngp-schema` rejects duplicate canonical keys in either merge input. Its full
offline test suite and `--no-deps` Clippy pass. Full dependency Clippy currently
encounters an unrelated pre-existing lint in `ngp-source-hash`; that is outside
this Distill milestone.

## 4. Workspace map and implemented behavior

| Package | Implemented responsibility |
|---|---|
| `distill-core` | Typed UUID/hash identities, canonical record codec, target rows, lineage, five bootstrap schemas |
| `distill-json` | Canonical authored JSON parsing/writing and typed authored values |
| `distill-schema` | Distill-facing shared-schema integration |
| `distill-bundle` | Plain/container bundle parsing, canonical writing, blobs, lineage, exact schema repair |
| `distill-migrate` | Automatic/custom migration validation, planning, and disk execution |
| `distill-wire` | Artifact grammar, DSWL trees, canonical encoding, plan compilation, contained fixup execution |
| `distill-store` | SQLite authority, versions/poison, journal/quarantine, append-only CAS, recovery, pins, GC |
| `distill-asset` | Runtime descriptors, erased ownership, placeholders, deterministic containers, callback status surfaces |
| `distill-asset-macro` | `#[asset]` descriptors, defaults, reflection, canonical encoding, compile-time rejection tests |
| `distill-build` | Query/dependency traces, imports, processors, tools, cache keys, artifact encoding |
| `distill-loader` | Basis-consistent resolve/fetch, artifact admission, DSWL planning, component adoption and swaps |
| `distill-pack` | Canonical manifest/archive construction, activation, mount validation, `PackfileIO` |
| `distill-rpc` | Loopback Cap'n Proto protocol, snapshots, resolve/fetch/subscribe, leases, target fencing |
| `distill-daemon` | Watcher/scanner, config staging, module epochs, import/build orchestration, codegen, pack and doctor commands |
| `distill-pipeline-fixture` | Real dynamic pipeline module used by module-host integration tests |
| `distill-game-assets-pipeline-fixture` | Tiny PPM/OBJ/shader import and cook module used by the full game-asset vertical test |

The final implementation pass closed these cross-cutting gaps:

- Every module reverse callback is contained. Host callback panics become typed
  failures; only failures attributable to module code poison that exact module
  epoch. Owned erased values retain their epoch and cannot unwind across ABI
  boundaries.
- Disk loads share one migration path. Custom migrations and the automatic tail
  execute without holding store locks across callbacks.
- Build cache admission authenticates complete result records, artifact headers,
  output declarations, traces, and durable extents before reuse.
- Production pack construction rejects authoring/build-only leakage, requires
  exact selected asset/encoding coverage, validates artifact headers and edges,
  and rehashes every embedded DSWL tree.
- RPC serves artifact and DSWL payloads from the durable CAS. Combined spooling
  is bounded, split transport is authenticated, and large responses avoid an
  extra full copy.
- Snapshot/connection/stream queues and deadline bookkeeping are bounded.
  Snapshot leases have TTL and per-connection/global caps; expiry releases RPC
  views, streams, and CAS pins.
- Configuration staging aggregates all independent static defects canonically.
  Exact schema/module events reread only the named control source; config edits
  and recovery scans reread all control sources.
- Daemon-owned state/module/package/codegen/quarantine paths are canonicalized
  and excluded from authored scanning. Invalid aliases diagnose and skip only
  their subtree, so unrelated assets still publish.
- Complete malformed bundle poison is scoped only by the exact current
  namespace skeleton and heals incrementally. Scoped healing uses raw bundle
  asset identities and never requires interpreting the poisoned metadata it is
  replacing.
- Capability changes revalidate only importer read sets that recorded a
  capability lookup. Directory folds invalidate only their group; lineage-only
  config changes do not scan authored roots.

The closure milestone adds these end-to-end and durability guarantees:

- A real daemon/RPC/typed-loader vertical test imports a 1x1 binary PPM
  texture, one-triangle OBJ mesh, and include-bearing shader; adopts all three,
  observes a native watcher include edit as one shader-only reload, then builds,
  activates, mounts, and loads a pack containing all three assets.
- The schema-transition metadata command remains available while ordinary Hub
  connection is fenced by `SchemaAcceptanceRequired`. Accept, rollback, retire,
  and reactivate requests are canonical and version-fenced; accepting the final
  mismatch durably replaces the lineage manifest before promoting the retained
  module candidate. A real dynamic-module vertical test covers acceptance and
  promotion.
- Filesystem publication records every child in an unarmed group, durably stages
  all proposals, arms the group, and only then mutates user-visible paths.
  Recovery aborts unarmed groups and retires armed parents only after every child
  has a durable terminal outcome.
- Supported Unix daemon hosts use one standard-library rename-aside/no-replace
  protocol. Recovery never unlinks a new user target that raced an atomic save;
  displaced prior content remains retained for explicit cleanup.
- Disk migrations publish one independent journal group per bundle, so one
  bundle's staging failure does not prevent later bundles from committing.
- RPC subscriptions have one stream-level initial snapshot, later subscriptions
  union names and replay retained history as ordinary deltas, and loader sweeps
  remain scoped to the affected dependency component with an unchanged-content
  cutoff before fetch/adoption.

The adversarial closure review then fixed these failure paths:

- Rollback and divergent reactivation—and only those reverse/divergent
  cases—consume fully decoded, schema-closed,
  lineage-checked migration proofs. Custom operations must be total and
  disjoint, referenced functions must exist in the pending module, and asset
  multiplicity is retained so duplicate edges remain ambiguous rather than
  disappearing through endpoint deduplication.
- References to retired types, including migration endpoints, publish a durable
  `RetiredTypeReferenced` pipeline state and hold the affected bundle projection
  waiting. A moved waiting bundle retains both its old and new paths so SQLite
  and RPC projections cannot split. Migration diagnostics retain the offending
  control asset UUID, not merely its endpoint schema hash. Explicit reactivation
  flips authority and admits those waiting paths in the same stale-base-fenced
  transaction.
- Cleanup failure for an unpublished pipeline candidate is classified as
  `CandidateOpen`/`CandidateCleanup`, safely leaks the affected module when
  required, and remains durably visible instead of being discarded or
  mislabeled as published-runtime poison.
- Storage adoption failure rolls back the complete component, destroys every
  unconsumed module-owned value, preserves last-good state, and freezes the
  affected component. RPC transport loss enters bounded-backoff rebind,
  bounds the complete handshake, remains independently cancellable during a
  stalled peer, reinstalls subscriptions, and publishes `TargetBound` only after
  recovery.
- Native publication rejects symlinked entries and immediately revalidates its
  rooted parents. Missing targets, atomic editor replacement, and late writes to
  displaced open inodes all end in terminal no-clobber recovery rather than a
  permanently pending journal child. Retained names include the durable store
  instance, reserved-path collision accounting and replacement selection share
  one crash-atomic transaction, and journal-owned proposal cleanup is
  ownership/hash checked.
- Missing control parents and parent-directory replacement invalidate the exact
  descendant control and rebuild native coverage without scanning unrelated
  asset roots. Recovery-scan errors finalize the queue rather than leaving it
  armed. Dead Windows/portable daemon-host branches and the duplicate cleanup
  wrapper were removed.

The final review milestone additionally closes these concrete defects:

- `doctor verify` no longer holds the store lock across fresh build/import
  execution, fresh verification bypasses memo reuse, schema repair includes
  migration endpoints, and noncanonical but valid bundle inputs are accepted as
  build inputs before canonical rewrite.
- Loader dependency cycles are rejected before adoption, and DSWL payloads are
  durably installed before a result can name them. RPC admission happens before
  spawning request work; subscription/lease/session state is bounded and pack
  sessions pin their complete basis.
- Pack archives reject one EKey reused across structural and blob kinds.
- Scanner/watcher reconciliation preserves exact native spellings, keeps
  unreadable-scan poison scoped until its affected observation heals, aggregates
  independent defects, treats backend coverage failure as terminal, and never
  publishes an empty startup namespace before the armed scan completes.
- R37 removes the transitive platform traversal machinery while retaining the
  fully incremental R35 workload invariant and the same ordinary-race
  regressions.

## 5. CAS implementation

The CAS is a Bitcask-style append-only store for **derived data only**. Authored
source bytes remain authoritative in the filesystem and are not copied into the
CAS.

### Record and commit path

- Segment files contain framed records with a pinned 70-byte header: magic,
  version, record kind, static key, trace digest, payload length, content hash,
  and CRC-32C. The CRC covers the pinned header range plus variable sections;
  reads also recompute the content hash of the payload.
- A build commit appends all output/auxiliary payload records first and one
  result record last. The result record is the commit marker for the group.
- A regular segment rolls before the configured cap. A single record larger
  than that cap is stored alone in a manifest-typed oversize segment.
- Each touched segment is `fsync`ed once, in record order, after the complete
  group has been appended. Only after those durable writes does one SQLite
  transaction publish extent and candidate indexes. A crash before the index
  transaction is repaired by recovery; a crash before the result record leaves
  no committed group.

### Authority and recovery

- `cas/CURRENT` is the single authority for the active ordered segment
  generation. It is replaced with write-temp, file `fsync`, rename, and
  directory `fsync`.
- SQLite stores the disposable physical index `(content hash -> segment,
  offset, length)` and memo candidates. It is not allowed to overrule
  `CURRENT`.
- Startup scans the `CURRENT` segments, validates framing/CRC/hashes, truncates
  invalid tails, adopts durable unindexed committed groups, removes stray
  segments, and rebuilds the SQLite index whenever generation state disagrees.

### Retention and compaction

- Manifest, RPC lease, in-flight build, and pack-session pin classes protect
  complete result units from eviction. RPC resolve pins artifact and DSWL
  hashes before responding; lease expiry releases those pins.
- LRU eviction removes only unpinned complete result units.
- Compaction copies live records to a new durable generation, atomically flips
  `CURRENT`, flips the SQLite index in one transaction, and deletes old
  segments last. Current readers use bounded per-call reads, so no long-lived
  mmap generation survives that operation.

The CAS is therefore binary-framed internally because crash recovery requires
unambiguous record boundaries, integrity checks, and commit markers. It is not
a general binary asset parser and it does not inspect arbitrary tool or module
binaries.

## 6. Watcher behavior and Distill v1 comparison

Both implementations source filesystem events from notify's platform
`RecommendedWatcher`; v2 is not claiming a different native event source.

| Concern | Distill v1 | Distill v2 |
|---|---|---|
| Native input | notify `RecommendedWatcher` | notify `RecommendedWatcher` |
| Debounce | notify's old 300 ms `DebouncedEvent` layer, then a 40 ms listener-update delay | raw notify events retained in a bounded queue; the coordinator consumes a 40 ms union |
| Startup | watcher runs a recursive traversal and emits synthetic create events | watcher is armed first, one canonical-root full scan publishes, then retained native events replay incrementally |
| Rename handling | debouncer provides a rename pair; directory rename triggers a subtree scan | preserves ordered `Both` pairs and pairs split `From`/`To` events by native tracker; incomplete pairing escalates to recovery |
| Symlinks | follows symlink targets and dynamically adds watches outside the lexical root | native watcher does not follow symlinks; scanner canonicalizes targets and rejects escapes and daemon-owned aliases |
| Event authority | watcher follows paths and reads metadata while translating events | native paths are invalidation addresses only; the rooted scanner re-observes the exact path/subtree and revalidates containment and metadata |
| Overflow | notify `Rescan` triggers a full traversal | `need_rescan`, pathless mutation, queue overflow, or incomplete observation triggers one admitted recovery scan |
| Normal work | event-local, but directory/symlink machinery may scan subtrees | exact path/subtree only; never scans or hashes unrelated roots after startup |
| Durable work | v1 file-tracker transaction batches state | v2 persists generation-aware dirty paths and ordered rename rows, then observation-checks acknowledgements without minting synthetic input versions |
| Control files | separate older application behavior | config, schema, and pipeline module use the same native stream with exact-path admission and exact-source rereads |

“Lossless native event batching” means lossless **within the events actually
delivered by notify**: affected paths are unioned and rename order/pairing is
retained rather than collapsed into a generic dirty bit. It does not promise
that an operating-system watcher can never overflow. Overflow or incomplete
coverage is detected and repaired by an explicit full reconciliation.

Full scans after startup are limited to:

1. configured root-set replacement,
2. explicit verification (`doctor verify`), or
3. admitted native overflow/incomplete-observation recovery.

## 7. Doctor and repair

`doctor verify` is deliberately the explicit expensive verification path. It:

- performs a complete rooted scan and compares it with the published snapshot,
- carries scanner diagnostics for daemon-owned aliases and invalid subtrees,
- reruns watched imports to a fixed point in verification mode,
- enumerates runtime assets and targets, compares the published/normal build
  result with two fresh cache-bypassing rebuilds, and reports both stale output
  and nondeterminism,
- verifies every CAS extent through the ordinary hash-checking read path,
- verifies quarantine/journal state,
- finds bundle schema omissions only when an exact retained schema holder
  exists, and journals canonical plain/container repairs through normal
  publication machinery.

Schema repair never guesses. If no exact schema snapshot is retained for every
missing hash, doctor reports the defect and leaves the file unchanged. Container
repair validates framing and CRCs and preserves the blob chunk bytes.

`doctor clean` drives the journaled displaced-inode retention cleanup; it does
not silently bypass quarantine history.

## 8. Milestone history

The final implementation sequence on the Distill repository is:

- `6653d08 Harden build cache and output validation`
- `e307385 Simplify schema source freshness authority`
- `49bb5dc Bound runtime queues and lifecycle state`
- `8679b40 Serve RPC payloads from the durable CAS`
- `5189656 Wire production pack construction`
- `ae04fbb Add production pipeline module exports`
- `ef985f0 Batch CAS segment durability points`
- `91580af Simplify runtime compatibility design`
- `a0684ac Simplify synchronous build scheduling`
- `1f18b5f Close final implementation correctness gaps`
- `5a4c226 Address final review and simplify filesystem model`
- `b9aaa04 Refresh implementation handoff after final review`
- `321f260 Harden CAS recovery and compaction`
- `8c30754 Fix loader reload ownership and reconnection`
- `b766932 Close module lifecycle and schema closure gaps`
- `c7533e1 Fix incremental scan poison healing`
- `24555c7 Remove duplicate module publication host`
- `99654da Make authoring publication recovery total`
- `963c54c Complete durable schema and asset workflows`
- `236015b Close adversarial review failure paths`
- `d888c11 Finish final review recovery edges`
- `6e8d93f Close rotated audit race conditions`

Earlier watcher milestones include:

- `5e14d24 Restore watcher correctness invariants`
- `45db922 Make watcher publication fully incremental`
- `e57e481 Index incremental import work transactionally`
- `65fe0d4 Drive control inputs from native watcher events`

## 9. Review rules

Final implementation review must separate:

- a concrete defect in implemented required behavior,
- required behavior that is genuinely absent, and
- intentional Open/Deferred/Descoped work in §22.

An overengineering finding is valid only if the mechanism is not required by
current §§1–21/R34–R37 and removing it materially simplifies the system without
losing a named invariant. Internal format decoders for Distill's own bundle,
artifact, pack, protocol, journal, and CAS grammars are required integrity
boundaries; arbitrary executable/tool binary parsing is not.

Do not use the historical review reports or stale ledger entries as a checklist
without reconciling them against R34–R37. In particular, do not reintroduce
DSCA, DSLP, cross-binary DSNL gates, target-set summaries, consumer bootstrap
brands, exact Cargo layout-dependency closure, or in-place reattestation.

## 10. Explicit non-goals and open boundaries

The implementation is complete for the current supported phase. The remaining
boundaries retain their exact §22 categories:

**Open:**

- **Authenticated remote transport:** RPC is validated loopback-only. Remote
  use requires an authenticated secure transport design, not a relaxed bind.
- **Per-target source-walk layout emission:** current source-walk emits one
  host-target layout table. A target whose layout identity differs is rejected;
  cross-target cooking remains blocked until per-target emission exists.

**Deferred by decision:**

- Patch/compaction distribution tooling. CAS compaction itself is implemented;
  this item is the deferred pack/distribution product workflow.
- The editor application. The RPC surface is implemented; the editor UI is not
  part of this phase.
- Tuning constants such as debounce, segment/block sizes, and cache limits.

**Descoped:**

- Pipeline-code process isolation, RPC auth/TLS for the validated loopback-only
  phase, pack signing, flow-control QoS, shared multi-daemon caches, and exotic
  filesystem support.
- **Adversarial local namespace races and Windows-host daemon support:** R37
  defines trusted developer workspaces and ordinary race revalidation. A future
  hostile-filesystem confinement guarantee or Windows-host support is new scope,
  not a reason to restore the deleted retained-handle/NT traversal layer.

These are not hidden implementation gaps. Any decision to implement them is a
new scope decision and must first update `DESIGN.md`.

## 11. Resume checklist

1. Confirm the Distill worktree and submodules/path dependencies are at the
   intended commits.
2. Read current `DESIGN.md` §§1–21 and the R34–R37 ledger refinements; treat the
   rest of §22 as historical context and scope classification.
3. Run the workspace tests and Clippy commands from §1.
4. Triage any new review findings against §9. Fix only endorsed concrete defects,
   add regression tests, rerun workspace validation, and commit a coherent
   milestone.
5. Update this document whenever a later committed milestone changes the
   operational state or a governing scope refinement.
