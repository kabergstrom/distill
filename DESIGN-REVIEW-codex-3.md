codex
The revision is materially better, but it is still not implementation-ready. Three CRITICAL correctness gaps and eight HIGH issues remain.

## Round-2 disposition

| # | Round-2 finding | Disposition |
|---|---|---|
| 1 | Lazy snapshot resolution | **Partially papered over.** “Snapshots denote inputs” is a coherent semantic direction, and the exact-byte drift protocol is fixed. No snapshot memo/output-table representation exists, however, and derived UUID resolution remains non-invertible from the RPC input. See new finding 3. |
| 2 | Cache determinants and lookup | **Partially addressed.** Target definitions, tools, validators, migration planning, static action keys, and verifying traces were added. The action hit rule omits static output determinants, in-flight coalescing is contradictory and unsafe across snapshots, and native-code impurity remains. See findings 5 and 8. |
| 3 | Lazy migration versus tag indexing | **Partially addressed.** `load_current` and the tag-annotation epoch solve the dual-loader problem. Tag indexes still do not depend on migration-plan selection or data-driven migration bundles, and tag metadata aliases under the logical hash. See finding 7. |
| 4 | Structurally impossible memory artifact | **Partially addressed, CRITICAL survives.** `VarRef`, `SwitchVariant`, and rollback are real improvements. A `VarRef` still cannot simultaneously encode a Rust pointer niche and its wire reference. See finding 2. |
| 5 | Per-target layout | **Not closed; honestly gated.** The document now acknowledges the blocker, but “layout-identical to host” cannot be established using the one host schema currently emitted. See finding 11. |
| 6 | AssetQuery cannot express references | **Closed for the round-2 issue.** UUID, path-primary, path/local-id, and same-bundle local-id selectors and indexes are now present. Raw-file query result typing remains a lesser underspecification. |
| 7 | Semantic revisions and canonical hashing | **Partially addressed.** `#[asset(rev)]` closes same-shape semantic changes. The binary hash grammar is expressly deferred until phase 1, contradicting the claim that it is fully specified. See finding 9. |
| 8 | Logical migration IR and graph | **Partially addressed.** The logical op vocabulary and custom-edge graph are much better. The cost tuple selects the wrong path, and the expanded IR cannot lower into the existing `FieldOp` executor unchanged. See findings 1 and 6. |
| 9 | Graph-atomic hot reload | **Partially addressed.** Fixpoint closure expansion and explicit states were added. `StaleLastGood` still permits a new parent to run with an old child. See finding 4. |
| 10 | CAS rebuild, compaction, eviction | **Partially addressed.** Framing, embedded cache keys, generation manifests, hashing, and pins address most previous cases. SQLite/CURRENT crash authority and the exact record grammar remain unresolved. See finding 10. |
| 11 | DSB/canonical JSON underspecification | **Mostly closed.** CRC variant, offsets, padding, ordering, rename durability, map encoding, and compatibility are now specified. Resource caps and a few numeric canonicalization details remain lower-severity work. |
| 12 | Shader example contradicts import model | **Mostly addressed.** Watched import read-sets reconcile raw-source edits with build purity, and the rafx refactor is now explicitly admitted. Ownership of imported include entries remains unclear. See finding 13. |
| 13 | Rubicon/panic-isolation claims | **Closed.** The design correctly describes bespoke `libloading`, limits `catch_unwind`, and no longer claims an existing Rubicon host. Actual newgameplus code supports the precedent claim. |

## New and surviving findings

### 1. CRITICAL: the migration cost tuple prefers the destructive path (§§8, 11)

The formal graph contradicts its own stated policy.

Given current schema `C`, old schema `A`, and a custom edge `A → B`, the candidates are:

- direct automatic `A → C`: cost `(1, 0)`;
- custom `A → B`, then automatic `B → C`: cost `(1, 1)`.

Lexicographic minimization selects `(1, 0)`, even if the direct automatic diff implements a rename as drop-plus-default while the custom edge preserves the data. This directly contradicts “custom edges win the cost order.”

Only a custom chain reaching `C` exactly, with cost `(0, n)`, beats the automatic diff. Partial custom migrations—the principal reason to allow one automatic tail—are normally ignored.

The selection rule must specify the semantic preference explicitly. It cannot be “minimize custom edge count” after equal automatic counts. Migration-plan invalidation must also cover the entire reachable subgraph or a migration-graph epoch, not merely an underspecified query for edges “applicable to `(type, from)`.”

### 2. CRITICAL: DSTL cannot encode pointer-niche variants (§§5, 12)

`SwitchVariant` does not solve the conflict between wire placeholders and Rust niche encodings.

For `Option<Box<()>>`:

- Rust commonly identifies `None` using the null pointer niche.
- `Some(Box::new(()))` has a zero-byte flattened pointee.
- The proposed `VarRef` is therefore naturally `{ offset: 0, len: 0 }`.
- Its eight wire bytes are all zero—the same bytes the layout schema interprets as `None`.

`Option<Vec<T>>::Some(Vec::new())` has the same class of problem. More complex niche encodings can similarly interpret an offset/length bit pattern as a reserved discriminant. The document requires both the `VarRef` and the Rust niche to inhabit the same slot, so there is no independent tag for `SwitchVariant` to read.

The format needs an explicit wire discriminant independent of Rust object-layout niches. The constructor should decode that wire tag, select a generated variant plan, and then construct the Rust value. It must never use placeholder bytes as if they were a valid Rust niche value.

Related format holes remain: `var_len` is `u64` while `VarRef.offset` is `u32`, with no normative 4-GiB limit or large-artifact rule.

### 3. CRITICAL: snapshot memos and derived outputs have no representable data model (§§7, 9, 13, 15, 17)

“Snapshot denotes inputs” resolves the philosophical version-number conflict, but not the protocol or storage problem.

The specified structures remain singular:

- `artifacts` maps action key to one “result” and cache key to one CKey.
- DSTL contains one asset header and no output table.
- Manifests map one UUID to one CKey.
- The `assets` table contains authored entries only.

A processor can nevertheless return multiple typed outputs. The document refers to the parent’s “output table,” but never defines its encoding, cache identity, transaction, or lease ownership.

More fundamentally, `resolve(child_uuid, snapshot)` cannot discover the parent. UUIDv5 is deliberately one-way. A child is not in snapshot metadata, and the RPC carries no parent/build-session capability. “Build the parent and read its output table” is impossible unless some `(snapshot input basis, child UUID) → parent/output` index has already been populated—which is the snapshot-scoped overlay requested in round 2 under another name.

The immutable in-memory snapshot is also an `Arc`, while lazy results supposedly attach to it without creating a version. The mutable memo store, its atomicity relative to CAS publication, retention, pinning, and behavior after daemon restart are unspecified.

Define a build-result object containing the complete named output table, with a cache identity for the table and CKeys for each output. Publish it atomically into a memo index keyed by an input-basis fingerprint, parent UUID, target definition, and stage. Derived resolution then needs either a session/output-table capability or an explicit snapshot-scoped child index.

### 4. HIGH: `StaleLastGood` still breaks graph-atomic adoption (§§9, 15)

Consider version `N+1` where:

- parent `P1` builds successfully;
- `P1` now references child `C1`;
- `C1` fails and is represented as `StaleLastGood(C0, …)`.

The prescribed sweep can adopt `P1` and serve `C0`. That is exactly “a new parent renders against an old child,” which §15 simultaneously says is structurally impossible.

A visible state label makes degradation observable; it does not make the graph version-consistent. The loader needs a closure-level rule:

- either any `StaleLastGood`, `Missing`, or failed newly introduced dependency aborts the entire swap and retains the previous closed graph;
- or mixed-version graphs are explicitly permitted, with compatibility semantics and the single-version guarantee removed.

The current text chooses both.

The existing v1 loader limitation is accurately cited: it accepts loaded dependencies without ensuring their new versions were adopted ([loader.rs](/Users/karl/Projects/distill/loader/src/loader.rs:417)).

### 5. HIGH: action-key hits and in-flight joins remain unsound (§§9, 13, 15)

The full cache key includes output logical/layout hashes and artifact-format version. The action key omits all three. The hit algorithm says to accept the previous result when its dynamic trace revalidates; it does not require recomputing and comparing the full cache key with current static determinants.

Thus an unchanged input and dependency trace can reuse an artifact after:

- an output relayout;
- an output logical-schema revision;
- a DSTL format-version change.

There are two additional contradictions:

- §9 says the in-flight table is keyed by action key; §13 says cache key.
- Two snapshots can share an action key while resolving a path/query differently. If their requests join the same job, each waiter must validate the completed trace against its own snapshot before accepting it. No such post-join rule exists.

Either include every static determinant in the action key or mandate reconstruction of the full cache key from the stored trace before a hit. Coalesced waiters need per-snapshot result verification and a retry when the joined result does not validate.

### 6. HIGH: the expanded migration IR cannot use the existing executor unchanged (§§11, 19)

The current `FieldOp` supports basic copies, widening, defaults, and a limited set of container drops/empty constructions. It has no representation for:

- `MapVariant` and variant payload subplans;
- element-wise `Vec`/array/`Option` migration;
- map-value or map-key migration;
- collision detection;
- custom fallible conversions.

See the actual [FieldOp definition](/Users/karl/Projects/newgameplus/ngp-schema/src/migrate.rs:46). Current root matching is still crate plus type name ([migrate.rs](/Users/karl/Projects/newgameplus/ngp-schema/src/migrate.rs:499)), not type UUID.

The current unsafe executor also logs a missing default function and zero-fills instead of propagating failure ([ngp-reflect migrate executor](/Users/karl/Projects/newgameplus/ngp-reflect/src/migrate.rs:64)). That behavior is incompatible with the proposed integrity guarantees.

The logical IR is good new work, but lowering it requires extending or replacing `FieldOp` and its executor. “Consumes the lowered plan unchanged” and the reuse claim in §19 are false.

### 7. HIGH: tag schema identity and invalidation are still unsound (§§5, 6, 10, 11, 13)

Two separate problems interact.

First, tag markers are stored in schema snapshots but excluded from the logical hash. Yet schemas are keyed solely by logical hash, and §11 claims that the hash pins schema content. Two snapshots that differ only in `#[asset(tag)]` metadata therefore have the same key but different content. `schemas: logical_hash → schema JSON`, schema closure, and `doctor` repair no longer have unique values.

Hashed logical structure and unhashed indexing annotations need separate records, with a distinct annotation identity.

Second, tag extraction does not record all inputs to `load_current`. Suppose a data-driven migration bundle is added or edited and changes the current tag value:

- the asset bundle is not dirtied;
- the tag-annotation epoch is unchanged;
- the dylib hash is unchanged;
- the old tag index remains published.

Recording only the dylib hash is insufficient. Every tag entry must depend on migration-plan selection, applied migration bundle bytes, planner version, defaults/code where used, and the annotation epoch.

The text also says it reads tag fields from the current migrated value but indexes defaulted fields as absent. That requires explicit provenance from migration execution; it cannot be derived from the typed current value alone. Failure behavior for `load_current` during indexing is likewise missing: whether type/path metadata commits with absent tags, retains old tags, or rejects the store version must be specified.

### 8. HIGH: build purity is still an unenforced assumption (§§1, 3, 8, 9, 13, 22)

A dylib content hash establishes code identity, not purity. Native Rust processors, validators, default functions, and custom migrations can still read:

- arbitrary files;
- environment variables and locale;
- clocks and randomness;
- network state;
- process-global mutable state;
- CPU-specific behavior.

“No `ctx.read`” does not make a validator “pure by construction”; it can call `std::fs` directly.

The current rafx implementation demonstrates the practical risk: includes use direct filesystem reads ([include.rs](/Users/karl/Projects/rafx/rafx-shader-processor/src/include.rs:32)), overrides use `exists` and direct reads ([lib.rs](/Users/karl/Projects/rafx/rafx-shader-processor/src/lib.rs:606)), and directory processing enumerates and writes files ([lib.rs](/Users/karl/Projects/rafx/rafx-shader-processor/src/lib.rs:188)). Refactoring the intended paths behind `ProcessContext` does not prevent a future processor from bypassing it.

An optional double-run check cannot support the hard claims that CKeys are determined by snapshot inputs and arbitrary checkouts rebuild byte-identically. Either enforce capability-only execution—normally in a restricted worker—or state that extensions are trusted to obey a determinism contract and downgrade the cache/reproducibility guarantees accordingly.

### 9. HIGH: the logical-hash grammar is explicitly not pinned (§§5, 22)

Section 5 says the binary grammar will be “pinned normatively when phase 1 lands.” Section 22 says the logical hash is fully specified. Both cannot be true.

An implementer still needs the actual version byte, node tags, integer widths and endianness, field framing, revision-node placement, back-reference distance definition, and encoding of every enum/tuple/newtype shape. Without those, independent tools cannot reproduce bundle schema hashes.

This is also substantial new schema work, not reuse of the current model: current `TypeDef` has no stable asset type UUID or asset annotations ([ngp-schema TypeDef](/Users/karl/Projects/newgameplus/ngp-schema/src/lib.rs:212)).

The grammar must be part of this design before phase 1, because bundle validation, migration endpoints, CAS keys, and artifact headers all depend on it.

### 10. HIGH: the CAS generation protocol still has two authorities (§13)

`CURRENT` selects the active segment generation, while a SQLite transaction “flips the index.” Those cannot be changed atomically together.

Depending on ordering, a crash can leave:

- `CURRENT` naming the old generation while SQLite points into the new one; or
- `CURRENT` naming the new generation while SQLite still points into the old one.

Both generations may remain durable, but recovery still needs an authoritative generation ID and a deterministic rule: for example, `CURRENT` is authoritative and a SQLite generation mismatch forces a complete index rebuild before reads.

The CAS record grammar is also not pinned sufficiently for that rebuild. “Magic + version, kind, cache key, blake3, len, crc, payload” does not define field widths, byte order, CRC variant, bounds, alignment, or whether cache keys are fixed or length-framed.

Multiple processor outputs aggravate this: the durable model is `cache key → CKey`, while a build can produce an output table. That must become `cache key → named output table`, or use separately domain-separated per-output cache keys.

### 11. HIGH: cross-target layout remains a real gate and is not verifiable today (§§5, 12, 18, 22)

The revised text now acknowledges the issue, which prevents silent unsafe implementation, but the work remains blocking for the configured Windows target and per-target shipping.

Actual `source-walk`:

- selects one cargo target and one backend feature ([target setup](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:456));
- hardcodes `Vec`, pointer, `Arc`, and `HashMap` layouts ([container fallback](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:1251));
- hardcodes pointer size to eight bytes ([pointer layout](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:1515));
- accesses private rust-analyzer layout internals through unsafe reinterpretation ([layout internals](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:1489)).

`ngp-source-hash` now includes transitive workspace-local Rust sources and optional extras, but still uses `DefaultHasher` and does not hash manifest contents, the lockfile, or the target unless supplied separately ([ngp-source-hash](/Users/karl/Projects/newgameplus/ngp-source-hash/src/lib.rs:16)). The design describes that limitation accurately.

Until per-target emission exists, the enforceable rule should be “target exactly equals the schema-producing compilation identity,” not “believed layout-identical to host.”

### 12. Medium-high: mmap-backed `Blob` contradicts compressed pack loading (§§4, 12, 15, 16)

`Blob` is promised as an `Arc`-backed range borrowing the artifact mapping, including for packs. But v2 packs store artifacts in compressed zstd blocks. After decompression there is no raw artifact byte range in the pack mmap to borrow.

The blob can borrow an `Arc`-owned decompression buffer, or the pack format can store blob payloads in separately addressable uncompressed blocks. It cannot both borrow the compressed mmap and expose raw blob bytes without copying/decompression.

The current v1 PackfileIO genuinely mmaps its raw archive ([packfile_io.rs](/Users/karl/Projects/distill/loader/src/packfile_io.rs:29)); that precedent does not establish the v2 compressed claim.

### 13. Medium-high: the shader example does not define ownership of include assets (§§8, 20)

An importer folds one prior bundle into one resulting bundle. The directory importer is described as producing one bundle per shader stem with stages and overrides as siblings. The cook then resolves `ShaderInclude` by path, while the import allegedly copied every transitively included `.glsl` file into `ShaderInclude` entries.

It is unclear whether:

- each pipeline bundle duplicates every include as a sibling;
- each `.glsl` owns a separate include bundle;
- or a pipeline import is allowed to mutate other bundles.

The first makes path identity and duplicate ownership unclear; the second means the pipeline importer cannot itself update the include bundle; the third violates the stated one-bundle fold and complicates atomicity.

Define a separate include importer/owner and make pipeline imports record references/read-sets to those entries, or explicitly define a multi-bundle import transaction.

The rafx coupling claims themselves check out: grouping, reads, overrides, include expansion, and output writing are currently filesystem-based, so the stated refactor is genuinely required.

### Other repository claims checked

- The v1 FileTracker does have persisted dirty/rename tables, startup scan reconciliation, a roughly 40-ms debounce, dynamic symlink watches, and full rescans after watcher overflow ([file tracker tables](/Users/karl/Projects/distill/daemon/src/file_tracker.rs:39), [scan reconciliation](/Users/karl/Projects/distill/daemon/src/file_tracker.rs:365), [debounce](/Users/karl/Projects/distill/daemon/src/file_tracker.rs:770), [overflow recovery](/Users/karl/Projects/distill/daemon/src/watcher.rs:196)). The carry-over description is accurate.
- `reverse_path_refs` exists as stated ([file_asset_source.rs](/Users/karl/Projects/distill/daemon/src/file_asset_source.rs:55)).
- newgameplus uses bespoke `libloading`, source/ABI checks, function-pointer auditing, and an explicit old-library drop sequence ([module load](/Users/karl/Projects/newgameplus/newgameplus/src/module_state.rs:180), [reload commit](/Users/karl/Projects/newgameplus/newgameplus/src/module_state.rs:623)). The revised hosting claim is accurate; the proposed daemon epoch and `catch_unwind` boundaries remain new implementation.
- Generated default/drop tables exist, but the generic transactional DSTL constructor does not. The design now describes that distinction accurately.

## Prioritized top 10

1. Replace Rust-niche decoding from `VarRef` bytes with an explicit wire discriminant and generated typed construction plan.
2. Correct the migration-path preference so custom semantic edges are not bypassed by a destructive direct automatic diff.
3. Define the snapshot-scoped memo/build-result model for 1→N outputs, including child-to-parent discovery, atomic CAS publication, and leases.
4. Specify closure-level failure behavior: abort a swap containing stale/missing members or formally relinquish single-version graph consistency.
5. Make action-cache lookup and in-flight joining snapshot-safe, and constrain or explicitly weaken the native-code purity assumption.
6. Extend the migration IR, `FieldOp`, and executor together; remove the claim that the existing executor can consume the expanded plan unchanged.
7. Separate tag annotations from logical-schema identity and record complete migration-plan dependencies for every tag index entry.
8. Pin the logical-hash binary grammar before phase 1.
9. Make one CAS generation authority normative and define exact record framing and multi-output cache records.
10. Complete per-target schema/layout emission and key it by the full compilation identity before enabling non-host cooking.

**VERDICT: Remaining CRITICAL issues: migration path selection, DSTL niche encoding, and snapshot/derived-output representation. Remaining HIGH issues: last-good closure consistency, action-key/in-flight correctness, migration executor mismatch, tag identity/invalidation, unenforced build purity, unpinned logical-hash grammar, CAS generation recovery, and per-target layouts.**
