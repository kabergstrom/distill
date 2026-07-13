codex
The revisions close the three round-3 CRITICAL defects in principle. No CRITICAL issue remains, but nine HIGH issues and several implementation-blocking medium issues remain.

## Round-3 disposition

| # | Round-3 finding | Disposition |
|---|---|---|
| 1 | Migration cost tuple preferred destructive automatic diffs | **Closed.** The mandatory greedy custom-edge walk prevents an automatic diff from bypassing a custom edge. Edge dependency expressibility is a new issue below. |
| 2 | Pointer-niche variants were unencodable | **Closed in principle.** Explicit wire tags ensure niches are written but never decoded. The wire-layout grammar and constructor remain underdefined. |
| 3 | Multi-output results and derived resolution had no model | **Partially closed.** Named output tables and the child index make resolution conceptually possible. The durable CAS representation cannot reconstruct or atomically recover those tables. |
| 4 | `StaleLastGood` broke graph-atomic adoption | **Policy closed, mechanism incomplete.** Failure now freezes a component. The component definition and manifest version model remain contradictory. |
| 5 | Action hits and cross-snapshot joins were unsafe | **Partially closed.** Output hashes and per-waiter trace validation are fixed. External-tool determinants remain outside both the action key and its trace. |
| 6 | Existing `FieldOp` executor could not be reused unchanged | **Closed.** The document now calls this extension work and mandates fail-hard behavior. |
| 7 | Tag annotations aliased schema identity and had incomplete deps | **Mostly closed.** Markers moved out of snapshots and complete migration inputs are named. Treating failed extraction as an absent tag makes build queries unsound. |
| 8 | Native pipeline purity was an unenforced assumption | **Closed as a contract.** The trust boundary and conditional nature of reproducibility claims are explicit. |
| 9 | Logical-hash grammar was deferred | **Partially closed.** A normative grammar now exists, but it lacks a representation for unit and does not match the actual primitive model completely. |
| 10 | CAS generation had two authorities | **Authority closed.** `CURRENT` is unambiguous. Record framing still cannot recover the indexes and atomic result units claimed. |
| 11 | Cross-target layout was unverifiable | **Partially closed.** Refusal is now the right policy, but neither the schema nor target configuration carries the identity needed to enforce it. |
| 12 | Mmap-backed blobs contradicted compressed packs | **Partially closed.** Stored blocks help, but a blob spanning bounded blocks still has no contiguous mmap range. |
| 13 | Shader include ownership was undefined | **Closed.** One `.glsl` per independently owned bundle is coherent. Removal and naming-collision policy remain unspecified. |

## New and remaining findings

### 1. HIGH: CAS records cannot recover an atomic output table (§§9, 13)

The SQLite transaction at [DESIGN.md:649](/Users/karl/Projects/distill/DESIGN.md:649) makes the volatile indexes atomic, but the durable record at [DESIGN.md:1040](/Users/karl/Projects/distill/DESIGN.md:1040) contains only a cache key, output key, one UUID, and one payload. It does not contain:

- the action key;
- the dependency trace;
- a result/group ID;
- the expected output count or complete output-key set;
- a result commit marker.

Consequently, a segment scan cannot rebuild `action key → trace + output table`, despite the claim at [DESIGN.md:1058](/Users/karl/Projects/distill/DESIGN.md:1058). It also cannot distinguish a complete multi-output append from a crash after output 2 of 3. Adopting the tail can publish a partial result.

`asset_uuid` is also ambiguous. If it is the derived artifact’s UUID, the parent cannot be recovered. If it is the parent UUID, it differs from the UUID in the extra output’s DSTL header, and the empty primary key needs a special rule.

Add a durable result record or begin/output/commit framing containing the action key, trace, parent UUID, complete typed output table, and transaction ID. Recovery must ignore uncommitted groups.

### 2. HIGH: output declarations are not static enough for the action key or UUID identity (§§7, 9)

`Processor::process` returns an open-ended `Outputs`, while the pipeline map describes only a singular primary type chain. Nevertheless, the action key claims every output type/hash is known from that map at [DESIGN.md:628](/Users/karl/Projects/distill/DESIGN.md:628).

The design does not require registration to declare:

- the complete output-key set;
- each output’s authored and terminal type;
- whether extras themselves pass through processors;
- target invariance of extra output types;
- uniqueness of output keys across every stage in a parent’s chain.

Without those rules, output names or types can be data-dependent and therefore unavailable when computing the action key. Two stages can also both emit `"reflection"` and collide at `UUIDv5(parent_uuid, "reflection")`.

Require a closed, statically registered output schema per processor stage. Either namespace the UUID name by processor/stage plus output key, or enforce globally unique stable keys across the entire chain.

### 3. HIGH: the wire layout is not normative enough for memory-safe interoperability (§12)

The explicit `u32` tag fixes the niche collision, but “derive a wire layout” does not specify:

- tag placement and alignment;
- payload-union layout for niche enums;
- wire size/alignment of each indirection slot;
- nested enum/container transformation;
- the canonical bytes hashed into `layout_hash`;
- source and destination ranges used by the constructor.

There is also a direct execution contradiction: [DESIGN.md:976](/Users/karl/Projects/distill/DESIGN.md:976) says fixup begins after memcpy of the fixed section, while [DESIGN.md:1000](/Users/karl/Projects/distill/DESIGN.md:1000) limits memcpy to runs where wire and native offsets coincide. Once a wire tag changes an outer struct’s offsets, whole-section memcpy is invalid; flat fields must be scatter-copied to native offsets.

This needs a normative recursive wire-layout grammar and layout-hash grammar, plus an explicit `MaybeUninit` construction algorithm. The current source schema only stores native offsets and tag encoding, not this derived representation.

### 4. HIGH: component-atomic swaps still lack a sound component definition (§15)

The local manifest allegedly corresponds to exactly one store version at [DESIGN.md:1193](/Users/karl/Projects/distill/DESIGN.md:1193), but later becomes a union of independently versioned component cuts at [DESIGN.md:1305](/Users/karl/Projects/distill/DESIGN.md:1305). Both cannot be true.

The graph used to compute a component is also unspecified. It must be the weakly connected component over the union of currently adopted and candidate load-dependency edges, including held reverse dependents. Consider old parents `P` and `Q` both referencing `C`. Swapping only the forward closure `P + C` changes the child underneath old `Q`, even though there is no directed path from `P` to `Q`. Newly added edges can similarly merge formerly independent components.

Define per-entry or per-component snapshot provenance rather than one manifest version, and define component construction over old-plus-candidate weak connectivity before any handle is swapped.

The cited v1 limitation remains real: it checks only whether dependencies are loaded, with a TODO to verify their new versions ([loader.rs:417](/Users/karl/Projects/distill/loader/src/loader.rs:417)).

### 5. HIGH: external tools are still missing from the action key (§9)

The full cache key includes `(tool id, binary content hash)` at [DESIGN.md:605](/Users/karl/Projects/distill/DESIGN.md:605), but the exhaustive action-key tuple at [DESIGN.md:631](/Users/karl/Projects/distill/DESIGN.md:631) omits it. The trace vocabulary immediately above contains only `read`, `resolve`, and `query`.

An unchanged action key can therefore hit after `dxc` or another declared tool binary changes. Put declared tool identities in the action key, or make tool invocation a labeled, revalidated trace operation. The current text’s “every static determinant” claim is false.

### 6. HIGH: the strict compilation-identity gate is not representable (§§5, 12, 18, 22)

The required equality is triple, features, and toolchain at [DESIGN.md:1010](/Users/karl/Projects/distill/DESIGN.md:1010), but:

- target config carries only OS, architecture, APIs, and options ([DESIGN.md:1470](/Users/karl/Projects/distill/DESIGN.md:1470));
- current `Schema` carries only `source_hashes` and types ([ngp-schema/src/lib.rs:204](/Users/karl/Projects/newgameplus/ngp-schema/src/lib.rs:204));
- `source-walk` selects a target internally or from `TARGET` ([source-walk/src/main.rs:456](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:456));
- its recorded source-hash extras include the backend feature, not the target or toolchain ([source-walk/src/main.rs:844](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:844));
- `ngp-source-hash` still hashes Rust paths/content with `DefaultHasher`, not manifests, lockfile, target, or toolchain ([ngp-source-hash/src/lib.rs:32](/Users/karl/Projects/newgameplus/ngp-source-hash/src/lib.rs:32)).

The daemon thus has nothing authoritative to compare. Add a canonical `CompilationIdentity` record to schema output and the target definition: exact triple, rustc identity, enabled features/cfgs, relevant profile/layout options, manifests/lock identity, and source-walk/layout algorithm version.

The existing layout warnings remain accurate: containers are hardcoded for 64-bit layouts ([source-walk/src/main.rs:1251](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:1251)), pointers are fixed to eight bytes ([source-walk/src/main.rs:1515](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:1515)), and private RA layout internals are accessed through unsafe reinterpretation ([source-walk/src/main.rs:1489](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:1489)).

### 7. HIGH: failed tag extraction can publish incorrect aggregate builds (§10)

On `load_current` failure, the asset remains indexed with its tags absent at [DESIGN.md:790](/Users/karl/Projects/distill/DESIGN.md:790). A diagnostic does not restore correctness.

If a processor queries `tag=enemy`, a migration failure on an enemy asset makes the query return an under-approximation. The processor can then publish an incomplete atlas or index as a successful deterministic build. Nothing in its dependency trace says the result was poisoned.

A tag query whose potential membership includes an indexing failure must fail or return an explicit incomplete result that processors cannot silently accept. Retaining a last-good tag set is possible only if it is snapshot-scoped and visibly stale.

### 8. HIGH: stored blocks still do not provide a contiguous mmap-backed `Blob` (§§12, 16)

Large artifacts span 256 KiB–1 MiB blocks, while blob ranges are merely marked stored at [DESIGN.md:1367](/Users/karl/Projects/distill/DESIGN.md:1367). A blob larger than a block spans multiple framed extents. Block headers, padding, or archive placement mean those raw bytes are not one contiguous slice in the mmap.

This also conflicts with `ConstructBlob` taking a range over “the artifact mapping” at [DESIGN.md:976](/Users/karl/Projects/distill/DESIGN.md:976): a block-encoded artifact is not itself one contiguous mapping unless it is reconstructed into a buffer.

Use separately indexed contiguous blob extents, permit one unbounded stored extent per blob, change `Blob` to scatter/gather storage, or weaken the no-copy promise. The v1 precedent only proves mmap of a raw archive ([packfile_io.rs:26](/Users/karl/Projects/distill/loader/src/packfile_io.rs:26)); it does not solve encoded block assembly.

### 9. HIGH: the logical-hash grammar cannot encode unit (§5)

The normative grammar has no unit node at [DESIGN.md:253](/Users/karl/Projects/distill/DESIGN.md:253). This matters for `()`, `Option<()>`, unit fields, and empty tuples. Mapping unit to a zero-field struct would collide with an actual empty struct despite their different JSON interpretation.

The actual schema model has `PrimitiveType::Unit` ([ngp-schema/src/lib.rs:170](/Users/karl/Projects/newgameplus/ngp-schema/src/lib.rs:170)). It also lacks signed `I128`, while the grammar claims `i8…i128`. Either unsupported leaves must be rejected normatively or represented exactly.

Add distinct unit/tuple nodes or explicitly define a collision-free canonical lowering. Until then, hash-to-schema-content uniqueness is not true for the full stated asset type system.

### 10. MEDIUM-HIGH: input versions and memo commits use the same version authority (§13)

Build-result transactions advance the store version at [DESIGN.md:1097](/Users/karl/Projects/distill/DESIGN.md:1097) and [DESIGN.md:1122](/Users/karl/Projects/distill/DESIGN.md:1122), while the memo model says completing a build changes no version at [DESIGN.md:1106](/Users/karl/Projects/distill/DESIGN.md:1106).

An implementation needs two domains:

- input snapshot version, changed only by filesystem/code/index inputs;
- memo/CAS commit sequence, which may attach results to any compatible input snapshot.

Without that split, snapshot capabilities, `Built` event versions, current-manifest updates, and “attach to every matching historical version” have incompatible meanings.

### 11. MEDIUM-HIGH: per-node migration edge dependencies are not expressible by `AssetQuery` (§§10, 11)

A migration edge is selected by `(target_type_uuid, from_hash)`, but the complete `AssetQuery` selector list at [DESIGN.md:747](/Users/karl/Projects/distill/DESIGN.md:747) contains no selector for either field. Section 11 nevertheless says migration assets use normal query machinery and records an outgoing-edge-set query per visited node ([DESIGN.md:849](/Users/karl/Projects/distill/DESIGN.md:849)).

Querying all `Migration` assets and reading every one is correct but globally invalidating and contradicts the per-node claim. Add a dedicated indexed `migration_edge(type_uuid, from_hash)` selector/dependency, or normatively mark/index those fields as query tags.

### 12. MEDIUM: directory-import removal and output-path ownership remain undefined (§§8, 20)

The new ownership rule is coherent for creation, but deletion is not. A disappeared watched source ordinarily leaves the prior bundle intact with an error ([DESIGN.md:472](/Users/karl/Projects/distill/DESIGN.md:472)); a directory listing losing a matched group therefore has no specified effect on the generated stem/include bundle.

The design must decide whether the rules operation deletes it, marks it orphaned, or requires explicit user action. It also needs deterministic output paths and collision behavior when a stem bundle and `.glsl` include would choose the same name.

## Repository verification

The revised repository claims check out:

- The existing migration IR is indeed limited to copies, widening, defaults, and basic container handling ([ngp-schema migrate.rs:46](/Users/karl/Projects/newgameplus/ngp-schema/src/migrate.rs:46)); root matching remains crate plus type name ([migrate.rs:499](/Users/karl/Projects/newgameplus/ngp-schema/src/migrate.rs:499)); missing defaults still zero-fill ([ngp-reflect migrate.rs:64](/Users/karl/Projects/newgameplus/ngp-reflect/src/migrate.rs:64)). Calling the v2 executor extension work is accurate.
- newgameplus really uses bespoke `libloading`, ABI checks, function-pointer auditing, and ordered old-library teardown ([module_state.rs:180](/Users/karl/Projects/newgameplus/newgameplus/src/module_state.rs:180), [module_state.rs:723](/Users/karl/Projects/newgameplus/newgameplus/src/module_state.rs:723), [module_state.rs:912](/Users/karl/Projects/newgameplus/newgameplus/src/module_state.rs:912)).
- rafx include resolution, override probing, directory grouping, and output writing remain filesystem-coupled ([include.rs:46](/Users/karl/Projects/rafx/rafx-shader-processor/src/include.rs:46), [lib.rs:188](/Users/karl/Projects/rafx/rafx-shader-processor/src/lib.rs:188), [lib.rs:606](/Users/karl/Projects/rafx/rafx-shader-processor/src/lib.rs:606), [lib.rs:1056](/Users/karl/Projects/rafx/rafx-shader-processor/src/lib.rs:1056)). The claimed refactor is genuinely required.
- The v1 file tracker still supports persisted dirty/rename state, 40 ms notification debounce, and overflow rescans ([file_tracker.rs:37](/Users/karl/Projects/distill/daemon/src/file_tracker.rs:37), [file_tracker.rs:770](/Users/karl/Projects/distill/daemon/src/file_tracker.rs:770), [watcher.rs:196](/Users/karl/Projects/distill/daemon/src/watcher.rs:196)). `reverse_path_refs` also exists as described ([file_asset_source.rs:55](/Users/karl/Projects/distill/daemon/src/file_asset_source.rs:55)).

## Prioritized top 10

1. Define static processor output declarations and collision-free derived UUID namespaces.
2. Add durable result framing so CAS recovery preserves complete output tables, traces, and action keys.
3. Pin the recursive wire-layout and layout-hash grammar, including scatter construction.
4. Define component swaps over old-plus-candidate weak connectivity and replace the single-version manifest claim.
5. Put external tool identities in the action key or verifying trace.
6. Emit and compare a canonical compilation identity before claiming the strict target gate is enforceable.
7. Make tag-index failures poison affected tag queries instead of silently removing members.
8. Give large blobs contiguous pack extents or weaken/change the mmap-backed `Blob` contract.
9. Add unit and every supported leaf to the logical-hash grammar.
10. Separate input snapshot versions from memo/CAS commit sequencing.

**VERDICT: No CRITICAL issues remain. Remaining HIGH issues: durable multi-output CAS recovery, static output identity/declarations, normative wire layout and construction, component/manifest consistency, missing external-tool action determinants, unrepresentable compilation identity, unsound failed-tag query behavior, non-contiguous mmap blobs, and incomplete logical-hash grammar.**
