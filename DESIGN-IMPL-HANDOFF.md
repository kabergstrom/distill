# Distill v2 — Implementation Handoff

_Written 2026-07-13 and updated through 2026-07-15. This is the cold-start
implementation checklist; the git history is the authority for completed
milestones and the working tree should normally be clean between them._

---

## 1. The standing directives (all still active)

**A. The `/goal` (user is away, will not answer questions):**
> "implement the new distill design spec fully, exhaustively, with nothing
> deferred, in a new rust workspace folder tree. Make as much progress as
> possible without asking me anything, because i will be away. Do it as TDD:
> lead with a RED test, and then turn it green. All specified behaviour should
> have tests verifying it, as well as edge cases, negatives, and happy paths."
> Plus: "Ask codex gpt 5.6 sol for reviews when ALL code is done, and iterate
> until happy."

**B. The standing design-review loop:** iterate `DESIGN.md` with Codex CLI
adversarial reviews until a round returns **no CRITICAL and no HIGH** findings.
Each round: run Codex, save the report as `DESIGN-REVIEW-codex-N.md`, triage
(endorse vs reject) every finding, fold endorsed findings into `DESIGN.md` +
the §22 ledger, relaunch the next round.

**C. The governing code-sharing interrupt (earlier user message):**
> "dont take shortcuts with sharing code. pull latest on newgameplus and
> actually do what the design calls for."

All schema code is shared in `ngp-schema` (in the newgameplus repo); the v2
crates path-depend on it rather than forking schema types.

**D. Accepted minimal compatibility model (2026-07-15):**
R34 supersedes the intermediate R33 design. Do not add Cargo-closure identity,
candidate compiled-type/DSCA probes, cross-binary DSNL gates, RegistryExtras,
DSLP, accepted runtime type sets, DSAE expansion, target-native bootstrap rows,
in-place reattestation, or DSTS. The pipeline candidate is paired with the
watched schema through New Game Plus's shared source identity before Rust-ABI
registration; `ModuleAbiIdentity` separately gates the host interface. Runtime
compatibility is checked per authenticated artifact by terminal TypeUuid/DSLH
and successful DSWL plan compilation against the live descriptor. Module epoch
changes reconnect/resubscribe. Exact canonical target rows replace DSTS while
per-target DSTG remains. DSFT is also removed: there is no cross-epoch plan
cache, and any future epoch-local cache uses `(TypeUuid, LayoutHash)`. Generic
`Option<T>` now uses typed contained construction, and the obsolete DSNL gates
and schema-derived measured-native mirror have been deleted.

**Memory constraints (from `MEMORY.md`):**
- Use `nix run nixpkgs#cargo -- <cmd>` for all v2 cargo commands (plain `cargo`
  is not on PATH).
- For newgameplus engine builds: `cd /Users/karl/Projects/newgameplus && nix
  develop --command cargo ...` (the flake provides cmake for joltc-sys).
- **Do NOT add `Co-Authored-By` lines to commits.**
- Commit coherent milestones as they pass their focused verification; the user
  explicitly requested incremental commits. Do not add `Co-Authored-By` lines.

---

## 2. How to resume (operational playbook)

### Toolchain
- Workspace root: `/Users/karl/Projects/distill/v2`. Run cargo as
  `nix run nixpkgs#cargo -- test` / `-- clippy --all-targets` / `-- test -p <crate>`.
- Shared schema crate: `/Users/karl/Projects/newgameplus/ngp-schema` (path dep).
- The design spec: `/Users/karl/Projects/distill/DESIGN.md` (the single source of
  truth; ~6784 lines at post-R21).

### The Codex review command (verbatim; only the round-count phrase changes)
```
nix shell nixpkgs#nodejs --command npx -y @openai/codex@latest exec \
  --model gpt-5.6-sol -c 'model_reasoning_effort="xhigh"' --sandbox read-only \
  "You are an adversarial design reviewer. Read /Users/karl/Projects/distill/DESIGN.md in full — it is a complete design spec for a rewrite of the distill asset pipeline (daemon, build system, loader, live game-module reload). Rounds 1-N of review already happened; their accepted findings are folded into the text and logged in section 22 (Design-review ledger). Do not re-report anything the ledger already covers unless the fold itself introduced a NEW defect. Find genuine defects: contradictions between sections, unsound protocols (races, TOCTOU, deadlock, livelock), underspecified behavior that two implementers would implement incompatibly, missing failure-path definitions, security holes, and irrecoverable states. For each finding give: SEVERITY (CRITICAL/HIGH/MEDIUM/LOW), section number(s), a one-line title, the defect in 2-4 sentences, and a concrete fix direction. Order findings by severity. Be exhaustive within CRITICAL and HIGH. If you find no CRITICAL or HIGH defects, say so explicitly." </dev/null
```
Run it **in the background** with a 600000ms timeout. `</dev/null` is required
(Codex hangs on stdin otherwise). Update "Rounds 1-N" to the last completed round.

### Extracting the report from Codex output
Codex interleaves reasoning/exec blocks. The final answer is the LAST `codex`
block before `tokens used`:
```
grep -n '^codex$\|^tokens used' <output-file>   # find the last 'codex' line L and 'tokens used' line T
sed -n 'L+1,T-1p' <output-file> > DESIGN-REVIEW-codex-N.md
```
(Historically the review answer is a few hundred lines; the block boundaries are
the last `codex`…`tokens used` pair.)

### The fold pattern
Folding is done by a background subagent with a **detailed per-finding brief**:
for each endorsed finding, give the concrete DESIGN DECISION (not just "fix it"),
name every section it touches, and require reconciliation of contradicted §22
ledger bullets with `(Refined in RN: …)` amendments — never deleting history.
The fold agent must self-verify and report: even fence count, exact new ledger
bullet count, targeted greps that must succeed and must fail, final line count.

### Verifying a fold (do this yourself after the agent returns)
```
wc -l DESIGN.md
grep -c '^[[:space:]]*```' DESIGN.md         # must be EVEN
awk '/^### Resolved/,/^### Open — remaining/' DESIGN.md | grep -c '^- '   # ledger total
grep -c '<new-marker-string>' DESIGN.md      # e.g. ConfigurationState after R22
```

### Gotchas (learned the hard way)
- **zsh `=`-prefix quirk:** `echo`/`sed` args starting with `=` fail with
  "== not found". Never use `===SPLIT===`-style separators. Write Python to a
  file and run it rather than heredoc `python3 -c` (heredocs also misbehave).
- **Transcript grep self-match:** grepping the session transcript for the Codex
  command matches your own grep. Filter on the full launch shape
  (`--sandbox read-only` + `model_reasoning_effort` + `</dev/null` + startswith
  `nix shell`).
- **You cannot message a running agent.** No SendMessage/message tool exists in
  this environment. A new `Agent` call spawns a fresh contextless agent. If a
  running agent needs a correction, either wait for it to land and correct
  after, or stop it and relaunch with a corrected brief.
- **Agents write files but their final summary is data, not user prose** — tell
  them so in the brief.
- **Session/account limits kill background agents mid-work** without warning.
  This bit us three times in one session. Agents left files in half-written
  states: a stub module referenced by lib.rs, tests referencing not-yet-created
  types, a Cargo.toml member with no `src/lib.rs`. Always run an assess-first
  ("inventory done/partial/missing, THEN finish") brief when resuming a killed
  agent, and unbreak the workspace manifest before launching anything.

---

## 3. Current exact state (verified 2026-07-13 12:40)

### DESIGN.md
- **6784 lines, 74 fences (even/paired), 173 resolved ledger bullets.**
- This is the **post-Round-21** state. **Round 22 is NOT folded** (verified:
  `ConfigurationState` / `byte-identity` count = 0). The R22 report exists at
  `DESIGN-REVIEW-codex-22.md` and is fully triaged (all 14 endorsed) — see §5.

### Review reports on disk
`DESIGN-REVIEW-codex-2.md` … `DESIGN-REVIEW-codex-22.md` (21 files; round 1 was
never saved separately). R22 is the newest.

### v2 workspace — crate status
Members (all registered in `v2/Cargo.toml`): distill-core, distill-json,
distill-schema, distill-bundle, distill-migrate, distill-wire, distill-store,
distill-asset, distill-asset-macro.

| Crate | State | Notes |
|---|---|---|
| distill-core | ✅ GREEN | ids, hashes (`ContentHash`, `AssetUuid`, `TypeUuid`, `LogicalHash`), canonical record encoding, `domain_digest`. |
| distill-json | ✅ GREEN | Canonical RFC-8785 JSON + `AuthoredValue`. Compact writer, ECMAScript-number floats. ~23 tests. |
| distill-schema | ✅ GREEN | Thin; the real schema model lives in `ngp-schema`. |
| distill-bundle | ✅ GREEN | §6 bundle format, both encodings, CRC-32C container, `PathComponent`/`encode_path` structural-path encoder. 83 tests. |
| distill-migrate | ✅ GREEN* | §11 planner + AuthoredValue executor. 99 tests. *Needs an H1-lineage audit (§11 gating) — likely daemon-side, probably no change, but unverified this round. |
| distill-wire | ✅ GREEN | §12 DSTL artifacts, DSNL/DSWL hashes, wire derivation, fixup plans + framed-rollback executor. **157 tests.** Made 8 normative decisions where §12 was silent — folded into the R22 editorial batch (§5). |
| distill-store | 🔴 RED (tests) | Lib compiles. **Test targets do NOT compile** — a killed agent wrote the R21 failure-grammar TESTS but not the src types: missing `FailureCause`, `CapabilityKey`, `LocalFailureClass` in `cas::record`; `FailureFingerprint::MissingCapability` and `::Local` variants; `cause` field on `ResultOutcome::Failure` / `CommitOutcome::Failure`. Had 139 green tests before the R21 pass began. |
| distill-asset | 🔴 BROKEN (lib) | `src/lib.rs` declares `pub mod build; pub mod defaults; pub mod reflect; pub mod thunks;` — **those four files do not exist** (killed macro agent). Present: `lib.rs`, `types.rs`, `hasher.rs`. This breaks the whole-workspace build. |
| distill-asset-macro | 🟡 STUB | Cargo.toml is complete (proc-macro=true; deps proc-macro2/quote/syn; dev-deps trybuild + distill-asset/core/wire/json). `src/lib.rs` is a placeholder doc-comment I wrote to keep the manifest loadable. No macro implemented. |

**`distill-asset/src/lib.rs` already contains** the intended public surface: it
re-exports the §12 vocabulary from distill-wire (`CtorEntry`, `DropTable`,
`SkipEntry`, `NativeLayoutNode`, `CallbackPanic`, etc.), declares
`AssetType`/`AssetRuntimeDescriptor`/`ErasedValue`/`EncodeSink`/`AssetRef`/
`WeakAssetRef`/`PlaceholderThunk` (in `types.rs`), the deterministic hasher
(`hasher.rs`), and a `placeholder!` macro. The four missing modules (`build`,
`defaults`, `reflect`, `thunks`) are the reflection/builder/default-table/thunk
machinery that `#[asset]`-generated code runs against — they must be created or
removed from the `mod` list.

---

## 4. Immediate next actions, in dependency order

1. **Unbreak the workspace build.** Either create the four missing
   `distill-asset` modules or (interim) trim `lib.rs`'s `mod` list to what
   exists. The whole workspace currently fails `cargo test --no-run` at
   `distill-asset` lib. Nothing else can be workspace-tested until this is fixed.
2. **Finish the R21 store deltas** (task #21, §6 below). distill-store tests are
   RED on the failure-grammar correction; six other R21 corrections may be
   done/partial/missing — assess first. Land green (target: ≥139 tests) +
   clippy clean. Also do the distill-migrate H1 audit.
3. **Fold Round 22 into DESIGN.md** (task #22, full spec in §5 below). 14
   findings + the editorial batch. Verify: 173 → **188** ledger bullets, even
   fences, R22 markers present.
4. **Run the R22 code-correction batch** (§7 below) against the shipped crates —
   several R22 findings change already-written code (store lineage shape,
   ConfigurationState, wire drop-status APIs, f32 canonicality).
5. **Finish distill-asset + distill-asset-macro** (task #10) against post-R22 §4
   (status-returning drop/free, epoch tokens on `ErasedValue`).
6. **Build distill-build** (task #9) and **distill-loader/pack/rpc/daemon**
   (task #11) — the last two implementation crates (§8 below).
7. **Relaunch the Codex loop** (round 23, then 24, …) until a round reports no
   CRITICAL/HIGH. Then do the final full-implementation Codex review (task #13).

The review loop (steps 3/7) and the code work (steps 1/2/4/5/6) are independent
and were being run concurrently via background agents. With a hard limit in
effect, they'll likely run one at a time on resume.

---

## 5. PENDING: fold Codex Round 22 into DESIGN.md

Report: `DESIGN-REVIEW-codex-22.md` (1 CRITICAL, 7 HIGH, 5 MEDIUM, 1 LOW). **All
14 endorsed.** DESIGN.md starts at 6784 lines / 173 bullets; end at **188
bullets** (14 findings + 1 editorial-batch bullet). Amend superseded R20/R21
ledger bullets with `(Refined in R22: …)` notes; never delete.

**C1 — lineage ancestry proof (§§2/6/11/13).** R21's `LineageStamp { generation,
chain-digest }` is insufficient: an opaque prefix-digest can't be tested for a
prefix relationship, and missing intermediate stamps break reconstruction.
Decision: the embedded schema snapshot's lineage record carries the type's
**full ordered schema-digest list** (32 bytes per historical schema; histories
are short since revs are rare) — every data file becomes a complete predecessor
record. The `"DSSL"` chain digest stays as the compact commitment; ancestry
decisions use the explicit list. Ancestry test: data's list is a prefix of the
registry's list → automatic migration authorized; registry's list is a prefix of
data's → rollback → HARD STOP; divergence → HARD STOP. Reconstruction after
state loss: union of observed lists, prefix-consistency-checked; inconsistency =
hard stop for that type. Generation = list length (derived, not independently
trusted). No automatic migration without this explicit ancestry proof. Update
§6's `LineageStamp` decl, §11 gating, §13 `schema_lineage` row, §2's claim.

**H2 — failed imports can't wake (§§8/9/13/14).** Import dependencies become
**outcome-bearing** like build traces (the R20/H4 `Observed<T>` pattern):
`FileDep` (or successor) records read/probe/enumerate outcomes including stable
failures (NotFound, listing failure); importer capability lookups (hit AND miss)
join the authoring-import basis; the terminal failure is revalidated like build
traces so the import wakes when the condition heals.

**H3 — drop-failure status path (§§3/4/12/13/15).** Make poison end-to-end:
`AssetStorage::free`, `AssetStorage::update` (value is CONSUMED on Ok AND Err; on
Err the storage must not retain module-backed state), `DropTable` entries,
`CtorEntry::abort`/`drop_in_place`, `SkipEntry::drop_in_place` all return a
status (`Result<(), CallbackPanic>` style). `ErasedValue` carries its owning
module-epoch token. Define the runtime transition for a **published** epoch
becoming poisoned (distinct from candidate-open failure): fence new work against
it, mark drain permanently incomplete, prevent `dlclose` forever (deliberate
leak — consistent with §3 containment). Name both poison entry points in
PipelineState/§13.

**H4 — configuration poison representable (§§13/17/18).** Add snapshot-pinned
`ConfigurationState = Ready | Poisoned(reason)` alongside pipeline state: "valid
prior pipeline, invalid configuration candidate" must be representable. Classify
which operations remain valid at a config-poisoned version (follow §13's
PipelineState classification style: decide for metadata reads, snapshots,
authoring, target-bound RPC — state each). RPC exposes a stable typed poison
result.

**H5 — RPC load-policy carrier (§§4/9/13/15/16/17).** `Root.connect`/`reattest`
carry the load-policy projection rows + `"DSLP"` digest exactly like the pack
manifest (R21/H7 parity); the verified policy binds to the `IoBasis` (Rpc side);
sweeps validate against basis-bound policy uniformly; policy changes are
generation-fenced like target definitions.

**H6 — codegen names globally unique (§§7/10/20).** Filenames/module names derive
from a globally unique identity: escaped `local_id` (R21/M16 scheme) + `_` +
full lowercase `AssetUuid` hex. The generator validates the entire generated
namespace for collisions before writing (collision = build failure, not
replacement).

**H7 — stable rule identity (§§2/8/13).** Every `ImportRule` carries a stable
unique rule ID (authored, UUID-style). `DirectoryOrigin.rule` stores that ID,
never a list index. Reordering never changes identity; deleting a rule ID puts
its generated bundles into the defined orphan state.

**H8 — pack.current durability (§16).** Pin the pointer grammar: lowercase
blake3 hex of the manifest + trailing `\n` (65 bytes). Activation = write a
no-replace temp in the same dir, fsync the file, rename over `pack.current`,
fsync the dir. State that directory fsync alone does not guarantee file data
durability (the reason for the file fsync).

**M9 — runtime dlopen banned (§§3/9/13).** BAN runtime `dlopen` in pipeline
code outright: anything dynamic is a §9 tool subprocess via `run_tool`. Extend
§3's static-linkage rule (R20/H6). No staged-library API exists by design. The
daemon does not parse candidate binaries to enforce this; the pipeline build/CI
owns the static-linkage check. Tool registration uses DSCT v2: either a complete
hashed package directory, or an explicit ambient launcher/toolchain identity.
Unfingerprinted ambient calls run without committing a memo candidate.
Reconcile the R20/H6 ledger bullet.

**M10 — extras terminal (§§9/12).** An extra output is encoded directly:
`encoded_type = terminal_type =` its declared type, regardless of whether that
type is a registered processor input; only primaries traverse processor chains.
State in both §9 and §12's header rule.

**M11 — f32 canonicality (§§5/6).** Parsing = IEEE-754 round-to-nearest-ties-even
to binary32; canonicality = byte equality with re-emission of the shortest
decimal that reparses (ties-even) to the same binary32 bits. `0.1` is canonical;
replace all "binary32-exact" language.

**M12 — hash-domain rule scope (§§5/9/12/13/16).** Scope §5's totality rule to
SEMANTIC/COMPOSITE hashes; add an explicit named exception class — **byte-identity
digests** (`ContentHash`, raw-file hashes, dylib/tool hashes, CAS payload hashes,
archive per-file trailers) are raw blake3 over exactly the named bytes,
deliberately domainless, enumerated as such next to the domain table.

**M13 — scheduler config bounds (§§13/18).** Staging-time validation:
`parallelism >= 1`; `1 <= batch_reserved_workers <= max(1, parallelism - 1)` (at
least one interactive slot always remains; when `parallelism == 1` the single
slot alternates oldest-batch/interactive — state the degenerate rule); live pool
resize re-clamps the reservation and lets active slots drain.

**L14 — CAS CRC coverage (§13).** Pin exact disjoint ranges: CRC-32C covers
header bytes from `kind` through `content_hash` inclusive plus the three
variable-length sections; the `crc` field, `magic`, and `version` are excluded.
(Matches shipped distill-store — the spec converges to the code.)

**EDITORIAL BATCH — pin distill-wire's normative decisions into §12** (one ledger
bullet, marked editorial/implementation-feedback; the code is shipped and tested,
so the spec adopts it):
1. DSWL hash input includes the enum VARIANT COUNT (grammar not injective without
   it; DSNL already has it).
2. Flat-copy runs cover initialized value bytes only — never padding.
3. Sequence element stride = `align_up(element wire size, element align)`.
4. Map pair layout: key at offset 0, value at `align_up(key size, value align)`,
   pair stride rounded up to `max(key align, value align)`.
5. Box/Arc `VarRef.len` must equal the pointee's wire size exactly (else
   integrity error).
6. Non-single-variant enums always compile enum-rooted with sole op
   `SwitchVariant`; variant sub-plans are enum-relative with `whole_drop: None`;
   the enum plan's completion pushes the whole drop after the tag write.
7. The allocation-count and recursion-depth caps have configurable defaults
   (128 / 2^32) via an `ExecLimits`-style parameter — numbers are deployment
   policy, the CHECK is normative.
8. Slot-padding/gap zero-verification is the obligation of a wire-tree-holding
   READER, not the fixup executor (whose ops carry no slot sizes); the executor
   validates everything its ops name (VarRef bounds/alignment, blob pad word,
   tags, bool/char patterns, UTF-8).

**Fold verification targets:** even fences; ledger = 188; greps must succeed:
`ConfigurationState`, ordered-digest-list language in `LineageStamp`, rule ID
(not index) in `DirectoryOrigin`, `pack.current` fsync sequence, byte-identity
digest exception class, variant-count in DSWL text, stride formulas in §12.
Greps must FAIL as operative text: "binary32-exact", index-as-identity for
DirectoryOrigin.

---

## 6. PENDING: R21 code deltas to distill-store (task #21, interrupted RED)

The store was implemented against pre-R21 text (139 green tests). A killed agent
was applying these 8 corrections TDD-style. **Assess done/partial/missing first**
(the failure-grammar one is known partial: tests exist, src types don't).

1. **H1 — schema lineage** (§11/§13/§6/§5): staging a schema whose digest is a
   NON-HEAD entry in a type's lineage chain = staging refusal (no "forward wins"
   append). `LineageStamp { generation, chain }` with the `"DSSL"` formula;
   position re-establishment from stamps after state loss; first-sight automatic
   diff only from a strictly lower-generation consistent stamp. **NOTE: R22/C1
   changes this again** — the stamp must carry the full ordered digest LIST, not
   just a chain digest. If doing R21 and R22 back-to-back, implement the R22/C1
   shape directly and skip the intermediate.
2. **H2 — poison scoping** (§7/§13): bundle-scoped poison requires a fully
   validated complete namespace skeleton from the CURRENT bytes (caller-supplied
   validated skeleton); anything less = version-global poison. No prior-row
   scoping. (Store impl note: `poison_bundle` currently requires an existing row
   = the old recoverable-identity rule; reshape it.)
3. **H4 + M13 — failure grammar** (§9/§13): failure record = trace + terminal
   `FailureCause` (Op | Local); `StableFailureFingerprint::Local(LocalFailureClass)`
   and `::MissingCapability`; `CapabilityKey` (MigrationFn/DefaultTable/Importer/
   Processor). Result-payload encode/decode must match. **← THIS is the one that
   left the crate RED.**
4. **M14 + M19 — config** (§18): restart-only keys stage-and-validate into a
   pending-restart configuration generation with a `RestartRequired` state
   (active values unchanged; input version advances at restart); new key
   `batch_reserved_workers = 1` + change-class row.
5. **M17 — oversize segments** (§13): a CAS record exceeding the segment cap gets
   a dedicated oversize segment (one record per file, same grammar, typed in the
   CURRENT manifest); store/recovery/GC handle it.
6. **DSLP — load-policy digest** (§5/§13): domain-prefixed under `"DSLP"` (the
   fold already added the `"DSLP"` domain row to §5; code must match).
7. **H5 + H6 — quarantine/journal** (§14/§2/§18): intent-ID naming (content hash
   = metadata only), per-filesystem quarantine dirs, swap-back inodes quarantined
   never unlinked, deletion = journaled rename-into-quarantine with pre-image
   verify. Check `write_intents`/`displaced` DDL + journal/retention APIs.
8. **M12 — directory origin** (§8/§13): `DirectoryOrigin` record (rules-bundle
   UUID, rule identity, group key) persisted with generated bundles.
   **NOTE: R22/H7** makes `DirectoryOrigin.rule` a stable rule ID, not an index.

**distill-migrate H1 audit:** check post-R21 §11 gating against migrate's
walk/planner. Lineage gating is likely daemon-side (store), so migrate probably
needs no change — but verify and state why, and fix any encoded "first sight is
trivially forward" assumption if present.

---

## 7. PENDING: R22 code-correction batch (task #22)

After the R22 fold lands, correct the already-shipped crates for the R22
findings that change code:
- **distill-store**: R22/C1 lineage becomes the explicit ordered-digest-LIST +
  prefix ancestry test (supersedes R21/H1's chain-digest shape — do this shape,
  not the R21 one); R22/H4 `ConfigurationState` Ready|Poisoned + operation
  classification; R22/H7 stable rule ID in `DirectoryOrigin`; R22/M13 config
  bounds validation; R22/L14 CRC rule now normative (verify shipped code matches
  — it should).
- **distill-wire**: R22/H3 status-returning `DropTable`/`CtorEntry::abort`/
  `drop_in_place`/`SkipEntry::drop_in_place` + `ErasedValue` epoch token; R22/M10
  extras `encoded_type` note. The 8 editorial pins already match shipped code
  (verify only).
- **distill-json / distill-bundle**: R22/M11 f32 nearest-ties-even canonicality —
  find which crate owns schema-directed f32 validation and align it.
- **distill-asset / distill-asset-macro**: build against the post-R22 §4 surface
  (status-returning free/update, `ErasedValue` epoch token) — the finisher brief
  already bakes this in.

---

## 8. REMAINING crates to build (tasks #9, #11) — BOTH ENTIRELY UNSTARTED

Neither crate directory exists yet. These are the two largest remaining pieces
and together they are the bulk of the outstanding implementation work. Build
`distill-build` (#9) first: it depends on the corrected `distill-store` API, and
`distill-loader/pack/rpc/daemon` (#11) in turn depends on the build engine's
output formats and the daemon state it produces. Task #11 is written as one task
but is naturally **four crates** — `distill-loader`, `distill-pack`,
`distill-rpc`, `distill-daemon` — and should be split when built. TDD throughout
(RED test first); the design sections named below are the source of truth — read
them in full before starting each crate.

### distill-build (§8 import + §9 processing + §10 deps) — task #9

The offline build engine. Consumes distill-store (metadata DB + log-structured
CAS), distill-migrate, distill-wire, distill-bundle, distill-json, ngp-schema.
Design subsections to implement:

- **§8 Import pipeline** (DESIGN.md 1314–1679):
  - _Authoring import (impure)_ — importers read source bytes, emit bundle
    contents. The `run_tool` package/ambient subprocess model (dlopen banned,
    R22/M9/R32). Directory
    import with STABLE rule IDs and the `DirectoryOrigin` record (R21/M12 +
    R22/H7).
  - _Read-sets and watched imports_ — the OUTCOME-BEARING read-set (R22/H2): read
    /probe/enumerate outcomes including stable failures join the basis; importer
    capability lookups (hit AND miss) recorded; publication revalidates the
    complete read-set inside the committing transaction (R21/H3 lost-wakeup fix);
    a stale result is discarded and re-enqueued.
  - _Build import (pure)_ — the DSBI static pre-key + discovered-trace bucket
    (R20/M18); `load_current` acknowledged as execution; memoized under processor
    candidate rules; trace-bearing, bucket-indexed result records.
- **§9 Processing pipeline** (DESIGN.md 1679–2313):
  - _Pipeline map_ — importer/processor registration, the `ToolEpoch`
    package-or-ambient model (package staging, explicit ambient toolchain
    identity, snapshot-resolved execution), static
    linkage rule (R20/H6 + R22/M9 dlopen ban).
  - _Input hashes_ — the DSIH full-input-hash composition; per-field config
    policy table.
  - _Validators_ — validator diagnostics as `FailureCause::Local` /
    `LocalFailureClass::Validator` (R22/M13); no synthetic trace ops.
  - _The determinism contract_ — `Observed<T>` trace outcomes (R20/H4), the
    combined execution epoch (R20/H11), depth-cap = non-memoized SCHEDULER
    OUTCOME (never a failure record, R20/H13), f32 binary32 rounding at adoption
    (R20/M20 + R22/M11 ties-even), the `EncodeSink` encode-visitor consumption
    (R20/C1), extras encoded DIRECTLY with `encoded_type = terminal_type` (R22/M10),
    reserved batch capacity in the scheduler (R21/M19 + R22/M13 bounds).
- **§10 Dependency & invalidation model** (DESIGN.md 2313–2470):
  - _AssetQuery, precisely_ — the query grammar and its resolution basis.
  - _Tag indexing under lazy migration_ — tag-index inputs include capability
    lookups (R22/H4); negative results are first-class invalidation triggers.

This is the largest single crate. Expect the DSBI/DSIH/DSSI key machinery, the
trampolined build scheduler (stack-safe, chain-depth cap counting live frames),
and the processor-candidate memoization to be the hard cores.

### distill-loader (§15 Loader & Runtime) — part of task #11

Runtime asset loading. Design subsections (DESIGN.md 3971–4594): _Client
manifest_, _Pull = resolve + fetch_, _Watch and client-side staleness_,
_Version-consistent swap_, _snapshot-pinned requests + client retry policy_,
_Deletion_, _Plumbing_, _Carried over from v1_. Implement:
- `LoaderIO` with `IoBasis = Rpc(SnapshotStamp) | Pack(ManifestHash)`; one basis
  per component; mixed-basis → re-resolve. `LoaderIO::begin_sweep()`.
- Component-graph adoption (`Ready | Poisoned`), the `AssetStorage` surface with
  R22/H3 STATUS-RETURNING `free`/`update` and the published-epoch-poison runtime
  transition; placeholder adoption via `PlaceholderThunk`.
- R22/H5 basis-bound load-policy validation (`"DSLP"` digest carried through the
  basis); `ResolveResult::Built { basis }`; deletion component transitions
  (R21/M11 — freeze by default, placeholder adoption if registered, restoration =
  new-version adoption); `ReconnectRequired { reason }` handling on the RPC basis.
- R22/M17 oversize-record fetch admission (admitted alone; spool beyond threshold).

### distill-pack (§16 Packfile Format) — part of task #11

The pack (cooked-shipping) format. Design section DESIGN.md 4594–4887. Implement:
- `PackfileIO`. The pinned manifest BYTE GRAMMAR: header + five tables + trailer
  (R20/M25), full byte-for-byte encodings incl. Target, archive refs, table rows,
  optional-table presence, widths/endianness/ordering/padding (R21/H10); the
  archive block/blob record grammar.
- EKey scope = blake3 over the stored encoded extent (zstd frame bytes) EXCLUDING
  record framing (R22/H10); framing integrity is the CRC's job.
- Load-policy table + `"DSLP"` digest in the manifest (R21/H7); manifest ↔
  artifact-header cross-check normative both ways (R21/M18 — construction verifies
  vs ContentHash-verified header = build failure; load divergence = mount refusal).
- `pack.current` activation (R22/H8): pointer = lowercase blake3 hex of the
  manifest + `\n` (65 bytes); write no-replace temp, fsync file, rename, fsync dir.
- Logical-level reproducibility (R20/M19 — DSPM identifies a pack BUILD, not
  byte-stable across encoders).

### distill-rpc (§17 RPC Interface) — part of task #11

The daemon↔client RPC surface. Design section DESIGN.md 4887–5144. Implement:
- The cap'n-proto interface: `Root.connect`/`reattest` (carrying the R22/H5
  load-policy projection + `"DSLP"` digest), `Hub`, `Snapshot` with `refresh`,
  `resolve`, `fetch`, `resolvePath @10` (R20/M22), `subscribe`.
- GENERATION-FENCED target-bound methods (R21/H8 + R22/H5): a stale capability
  returns `ReconnectRequired { reason: ReconnectReason }` server-side, independent
  of whether the client has a subscription or consumed the event;
  `ReconnectRequired::TargetDefinitionChanged` (R20).
- `SnapshotStamp` as the RPC realization of `IoBasis::Rpc`; every terminal
  `IoEvent` carries a basis. LOOPBACK-ONLY bind (R20/H15 — non-loopback address
  rejected at staging; authenticated remote transport is a named §22 open item).
- Configuration-poison typed result over RPC (R22/H4 `ConfigurationState`);
  `AssetEvent::RestartRequired { keys }` (R22/M14).

### distill-daemon (§3 Architecture / module host) — part of task #11

The daemon process + module host that ties everything together. Design section
DESIGN.md 88–360 (_Processes_, _Modules_, _Module surface_). Implement:
- The process model, the module boundary + no-unwind thunk rules, the
  `PipelineEpoch` (targets + candidate staging, R20/H11), `dlclose` gating.
- Both poison entry points (R22/H3): candidate-open failure AND a published epoch
  going runtime-poisoned on a drop failure (fences new work, drain permanently
  incomplete, `dlclose` prevented forever — deliberate leak).
- Static-linkage rule + runtime `dlopen` ban in pipeline/module code (R20/H6 +
  R22/M9). The scheduler (interactive vs batch classes, reserved batch capacity,
  chain-depth cap, trampolined stack-safety).
- Displaced-inode quarantine driver with intent-ID naming and per-filesystem
  quarantine dirs (R21/H6), deletion as journaled rename-into-quarantine with
  pre-image verify (R21/H5), retention sweep (`doctor clean`).

### Then task #13
Final Codex review of the FULL v2 IMPLEMENTATION (the code, not just the design);
iterate until clean. This is distinct from the design-review loop (§9 of this
handoff) — it reviews the shipped Rust, and only starts once every crate above is
green and the design loop has terminated.

---

## 9. The design-review loop — trajectory & termination

Severity counts by round (folded unless noted):
- R17: 2C / 7H / 3M
- R18: 1C / 8H / 3M
- R19: 1C / 22H / 6M
- R20: 3C / 14H / 8M (all 25 endorsed)
- R21: 0C / 10H / 9M (all 19 endorsed — first zero-CRITICAL round; findings were
  mostly SECOND-ORDER defects in R20's own remedies)
- R22: 1C / 7H / 5M / 1L (all 14 endorsed — the CRITICAL was a second-order
  defect in R21's lineage-stamp remedy)

**Convergence signal:** rounds are increasingly finding defects introduced by
previous rounds' fixes rather than original design gaps, and severity/count is
trending down. **Termination condition: a round returns no CRITICAL and no
HIGH.** Keep going until then. Ledger total: 154 (post-R20) → 173 (post-R21) →
188 (post-R22, once folded).

---

## 10. Reference: shipped-crate public surfaces (for downstream crates)

**distill-core**: `id::{AssetUuid, TypeUuid, LogicalHash, ContentHash}`,
`domain_digest(domain, version, bytes)` (blake3 with domain prefix).

**distill-json**: `AuthoredValue` (Null/Bool/Int/UInt/Float/Str/Array/Object/
Blob), `write(&AuthoredValue) -> Result<String, WriteError>` (compact canonical
RFC-8785; value form, no trailing newline — file writers own the §6 whole-file
`\n`), a parser, `WriteError` (NonFiniteFloat/Blob).

**distill-bundle**: `Bundle`, `AssetEntry`, `parse_bundle`, `write_bundle`,
`crc32c`, `PathComponent`, `encode_path` (structural-path encoder — REUSE for any
path-ordered table). Path tags 0x01–0x04, u64-BE length framing (length-first
ordering: "beta" < "alpha"). MapKey payload = `distill_json::write` bytes.

**distill-migrate**: `MigrationOp` (CopyField/WriteValue/WriteNone/
WriteFieldDefault/WriteParentDefault/Widen/MapVariant/MigrateInline/
MigrateElements/DropField — NO `Custom`, removed R20/M21), `validate_plan(ops,
from, to, EdgeKind)`, `EdgeKind::{Automatic, Custom}`, `PlanError`, the executor
(immutable input, fresh output), the greedy walk. Name-matched field/variant with
differing `rev` = HARD STOP.

**distill-wire**: `artifact` (DSTL: magic `89 44 53 54 4C 0D 1A 0A`, 160-byte
header, canonical load_deps, blob table in `encode_path` order); `native`
(`NativeLayoutNode` 14 kinds, `NativeField{name, declaration_index, node}`,
`NativeVariant`, `CtorId`/`CtorEntry`/`CtorCursor`/`PushError`/`CallbackPanic`,
`DropId`/`DropTable`, `SkipEntry`/`SkipDefaultId`); `dsnl`/`dswl` hashes; `derive`
(wire layout per Class, 3 enum forms); `plan` (`compile_plans(wire, native) ->
CompiledPlans{arena, metas}`, `PlanError::Mismatch`); `exec` (`execute_fixup`,
`Blob`, `ExecEnv`, `ExecLimits`, `ExecError`, framed-rollback stack). **R22/H3
will make the Ctor/Drop/Skip status-returning.**

**distill-store**: SQLite (rusqlite bundled) tables — `files, dirty_files,
rename_events, bundles, assets, path_index, deps, schemas, pipeline_state, tools,
schema_lineage, asset_tags, roots, result_candidates, derived_outputs,
derived_assertions, cas_extents, cas_segments, registrations, pins, store_meta,
write_intents, displaced`. Log-structured CAS: 70-byte record header, kind bytes
0–4, CRC-32C, `CURRENT` pointer (atomic replace), recovery/GC/compaction.
`PipelineState`/`PipelineEpoch`/`PipelinePoison`/`VersionPoison`,
`OperationKind`, `SnapshotStamp`, `load_policy_digest`, `ToolEpoch`,
`SchemaLineage`. Physical `(segment, offset, len)` lives ONLY in `cas_extents`
(the R20/H14 invariant). **Currently RED on the R21 failure-grammar delta.**

**distill-asset** (partial): intended §4 runtime-support crate —
`AssetType`, `AssetRuntimeDescriptor`, `ErasedValue`, `EncodeSink`,
`EncodeContainer`, `AssetRef`/`WeakAssetRef`, `PlaceholderThunk`, `placeholder!`
macro, deterministic `BuildHasher`, re-exports of distill-wire's §12 vocabulary.
`build`/`defaults`/`reflect`/`thunks` modules declared but NOT created.

---

## 11. Task list crosswalk

- #1–#8: done (core, json, schema refactor, bundle, migrate, wire, store — but
  store now RED on R21 delta).
- #9 distill-build: pending (build after store corrections).
- #10 distill-asset-macro: in_progress (stub + partial distill-asset; broken lib).
- #11 distill-loader/pack/rpc/daemon: pending.
- #13 final Codex code review: pending.
- #21: verify R21 fold (done) + apply store/migrate R21 corrections (RED,
  interrupted) + relaunch R22 (done, folded-pending).
- #22: verify R22 fold + R22 code batch + relaunch R23 (all pending).

---

_End of handoff. Resume at §4 step 1._
