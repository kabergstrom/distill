codex
The spec is not implementation-ready yet. The revisions close several prior findings, but they also introduce new contradictions around lazy snapshots, migration-driven indexing, derived outputs, and cache identity.

## Findings

### 1. Critical: lazy resolution cannot produce a manifest that belongs to the pinned snapshot (§§13, 15, 17)

A snapshot is immutable metadata for version `N`, while §15 says output CKeys are recorded only after a lazy build runs. If `resolve(uuid, snapshot=N)` builds a previously unbuilt asset:

- The CKey is not in snapshot `N`.
- The build result commits in a later store transaction, version `M`.
- Returning `(ckey, N)` invents a manifest entry that version `N` never contained.
- Returning `(ckey, M)` breaks the claim that all batch resolves used one snapshot.
- If an unrelated commit creates `M`, §13 may still publish the result because its inputs match, making the response version even less well-defined.

Derived outputs make this fatal. A parent can return an artifact containing a UUIDv5 child that did not exist in snapshot metadata. The loader then cannot resolve that child against the same snapshot. No `assets`/derived-output table or snapshot-scoped result overlay is specified.

This is also contradicted by §22: §§7 and 9 explicitly support named extra outputs, while §22 rejects “auxiliary per-processor outputs.”

The protocol needs a snapshot-scoped build session/overlay that accumulates authored and derived `UUID → CKey` bindings against one immutable input basis, followed by either:

- a returned successor manifest capability, or
- an explicit definition that a snapshot denotes inputs and can acquire memoized outputs without changing its logical version.

The current “snapshot is an immutable manifest version” and “outputs appear lazily afterward” models cannot both hold.

`Drifted` also needs a read protocol, not a pre-check: read each file into owned bytes, hash those exact bytes, compare with the snapshot, and build from them. Checking the path’s hash before reading leaves a check/read race.

### 2. Critical: the revised cache keys are still incomplete, and processor cache hits are not computable (§§8–11, 13, 15)

New omissions include:

- **Build import omits target.** §12 places `target` in artifact bytes, so two targets with identical layouts produce different artifacts but the same §8 cache key.
- **Processor keys mention output schema hashes, but not explicitly output layout hashes or the artifact encoder/fixup-format version.**
- **A target name is insufficient.** The canonical target definition—triple, API set, features, options, and relevant toolchain identity—must enter the key.
- **Automatic migration defaults execute pipeline/generated code**, but §8 adds the dylib hash only when a function-backed custom edge runs.
- **Migration path selection depends on graph membership.** Adding a competing edge must turn a former cache hit into an ambiguity error. Hashing only the applied migration bundles cannot detect that; the migration query result, planner version, and path-selection policy must enter validation.
- **Validators affect whether an artifact is publishable.** A schema-only asset can currently reuse an artifact accepted by an older validator because §8 omits the pipeline hash when no processor/function migration ran.
- **External executable tools are not covered by the pipeline dylib hash.**
- Environment, locale, clock, filesystem and network reads remain available to arbitrary in-process Rust. The dylib hash addresses stale code, not purity.

There is also a lookup problem: a processor’s full cache key includes its dependency trace, but that trace is discovered only by executing it. The in-flight table likewise cannot be keyed by that full key before execution. The design needs a static action key such as `(asset, stage, input CKey, target-definition hash, code hash)`, a stored previous trace, trace revalidation against the snapshot, and then the full determinant key. Otherwise the processor cache is effectively write-only and concurrent requests cannot reliably join before doing the work.

This is a surviving prior-round finding, partially papered over by labeled traces and the dylib hash.

### 3. Critical: lazy migration contradicts both “no pipeline code” and query indexing (§§8, 10, 11)

§8 says no pipeline code runs for schema-described build import. §11 requires pipeline code for:

- function migration edges;
- `WriteDefault`;
- potentially parent-level custom defaults.

The cache and epoch rules need to treat build import as pipeline-code execution whenever any of these occur.

More seriously, lazy migration is incompatible with the tag index:

- §10 says tags are indexed directly from bundle content with no pipeline code.
- Old-schema bundles may lag indefinitely.
- A current `#[asset(tag)]` field may only exist after migration or may have been renamed.
- `#[asset(tag)]` is excluded from the logical hash, so merely adding/removing the attribute does not dirty a bundle or change its schema hash.
- §10 says reindexing happens when a bundle is dirtied, not when tag annotations or migration plans change.

Thus queries can silently use stale or nonexistent tag values. Either query indexing must run the logical migration pipeline—making it eager and possibly code-executing—or tags must be defined against each bundle’s embedded historical schema and stored in that schema snapshot. The current text chooses neither.

Migration discovery itself must record positive and negative query dependencies over all applicable migration edges. Otherwise a new conflicting migration does not invalidate cached builds.

### 4. Critical: the memory-layout artifact remains structurally impossible as specified (§§5, 12)

The concrete `VarRef` representation does not fit the promised Rust slots:

- A `{offset, len}` pair needs at least 16 bytes.
- `Box<T>`, `Arc<T>`, and `Rc<T>` are normally one pointer wide.
- A fixed section cannot store a `VarRef` “in” those fields without overwriting adjacent fields.
- `HashMap` construction additionally needs entry count, key/value encoding, hasher policy and duplicate-key handling.

The fixup op set is insufficient:

- No conditional/switch operation for enum variants or niche-encoded `Option`.
- No `ConstructRc`, despite §5 declaring `Rc` serializable.
- No precise operations for arrays, nested maps, map hashers, ZSTs or recursive allocations.
- “Recurse into element” does not define how the loader selects the active enum variant.
- A linear “constructed prefix” is not enough rollback state for nested allocations and partially inserted maps.
- Bounds checks also need overflow, alignment, UTF-8, discriminant, allocation-count and recursion-depth validation.

The claimed reusable machinery does not exist in the stated form. Current newgameplus has generated default/drop function tables and an unsafe migration executor, not a generic fallible constructor with transactional rollback. The executor operates directly on offsets and logs missing default/drop functions rather than providing the proposed initialization guarantee: [ngp-reflect migrate executor](/Users/karl/Projects/newgameplus/ngp-reflect/src/migrate.rs:12).

A workable design needs a separate wire layout whose slots are sized for their wire representation, plus a generated/compiled typed construction plan. The Rust object layout should only be the destination.

### 5. Critical: the prior target-layout finding is explicitly deferred, not resolved (§§5, 8, 12, 18, 21, 22)

§12 admits per-target layout emission is unavailable, while §21 puts cross-target artifact encoding into phase 1 and §18 already configures macOS and Windows targets. “Current targets are all 64-bit little-endian” does not imply equal layouts: target `cfg`, enabled features, dependency selection and enum variants can change logical shape and layout.

Actual `source-walk` currently:

- emits one schema file;
- chooses one cargo target and one active backend feature: [source-walk target setup](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:456);
- hardcodes 64-bit pointers in fallback/niche logic: [pointer-size fallback](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:1515);
- hardcodes container layouts such as `Vec=24` and `HashMap=48`: [known container layouts](/Users/karl/Projects/newgameplus/ngp-schema/src/lib.rs:471);
- reaches into private rust-analyzer layout internals using `transmute`: [layout_internals](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:1489).

This remains the prior review’s top issue. It should block cross-target artifact encoding, rather than remain an accepted future spike.

The staleness claim is also inaccurate: current schemas contain per-crate combined hashes, not per-file hashes. The hash uses `DefaultHasher`, hashes Rust files and selected extras, and omits Cargo manifests, lockfile/dependency versions, build scripts, most cfg inputs and the target triple: [ngp-source-hash](/Users/karl/Projects/newgameplus/ngp-source-hash/src/lib.rs:5).

### 6. High: the “precise” AssetQuery language cannot express the reference forms (§§4, 10)

§4 says every `AssetRef` is an `AssetQuery`, but §10 only defines selectors for:

- authored/terminal type;
- tags;
- path prefix/glob.

It has no selectors for:

- exact UUID;
- exact bundle path and primary;
- `(bundle path, local_id)`;
- local ID within the current bundle;
- existence/type validation of an exact UUID.

Those forms also require distinct invalidation indexes. For example, a same-bundle local reference depends on `(bundle_uuid, local_id)`, not a global path resolution.

Other missing invalidation cases include:

- pipeline-map changes affecting `terminal_type` membership and reference type validation;
- schema annotation changes affecting tags;
- selector-free “all assets” queries;
- old and new tag keys on a tag change;
- old and new prefixes on a move;
- path normalization, separator, Unicode and case-sensitivity rules;
- a pinned glob grammar.

The current query indexes handle ordinary asset events, but not the code/schema events introduced by the rewrite.

### 7. High: the logical-hash specification still cannot represent semantic migrations (§§5, 11)

A structural hash cannot detect meaning changes that preserve shape—for example, changing a `f32 distance` from metres to centimetres. Because migration endpoints are keyed only by hashes, a same-shape semantic revision never triggers lazy migration and cannot be expressed as a custom edge.

The schema needs an explicit stable semantic revision/salt attribute that participates in the logical hash.

The canonical grammar is also not yet precise enough to independently implement:

- exact node tags and length framing;
- tuples, unit/newtype structs and every enum variant shape;
- whether back-reference distance counts wrapper/container nodes or expanded type frames;
- string escaping and Unicode treatment;
- map wire representation and key ordering for non-string `HashMap<K,V>`;
- canonical ordering for integer-indexed fields;
- domain separation between different canonical node kinds.

“Canonical JSON” does not resolve these without specifying key comparison and escaping. A versioned canonical AST with a binary hash encoding would be safer than treating JSON prose as the hash algorithm.

Actual `ngp-schema::TypeDef` currently has no asset UUID or `skip`/`blob`/`tag` metadata: [schema model](/Users/karl/Projects/newgameplus/ngp-schema/src/lib.rs:205). Current `source-walk` also does not classify `Rc` as a traversal shape even though it has a size fallback.

### 8. High: migration reuse is directionally correct, but the proposed logical IR is materially larger than stated (§11)

The prior review’s core recommendation—a logical field-addressed IR lowered into `FieldOp`—is adopted. It is not existing reusable functionality.

Current `FieldOp` is purely offset/layout based: [FieldOp definition](/Users/karl/Projects/newgameplus/ngp-schema/src/migrate.rs:46). Root matching still uses `(crate, type name)`, not stable type UUID: [root matching](/Users/karl/Projects/newgameplus/ngp-schema/src/migrate.rs:482).

The proposed logical op list also omits semantics already required by the current migration system:

- parent-level `Default` materialization and `CopyFromDefault`;
- enum variant matching and payload migration;
- container element migration;
- map-key migration and collision handling;
- explicit custom conversion/failure operations.

Current code distinguishes parent `Default` from field-type `Default`, because they can produce different values: [default planning](/Users/karl/Projects/newgameplus/ngp-schema/src/migrate.rs:677). Collapsing both into `WriteDefault` changes behavior.

Migration-path cost is still undefined. Since an automatic diff can potentially connect any pair of known schemas, “fill gaps automatically,” “prefer custom edges,” “equal cost” and ambiguity need a formal graph construction and cost tuple. Adding an unrelated historical schema must not silently change the selected chain.

The bootstrap problem for migrating the `Migration` asset type itself also survives.

### 9. High: version-consistent swapping is weakened by last-good behavior and missing closure expansion (§§9, 13, 15)

The loader re-resolves “held-and-invalidated assets,” but a rebuilt parent can introduce a new load dependency that the client did not previously hold. The batch algorithm must repeatedly expand and resolve the new load-dependency closure against the same snapshot before any swap.

Last-good behavior needs a formal manifest state. Returning an old CKey after current inputs failed violates “built against exactly that snapshot’s inputs.” It should be represented as something like:

- `Current(ckey)`;
- `StaleLastGood(ckey, build_error, built_from_version)`;
- `Missing`;
- `Dead`.

Otherwise the claim that the loaded set equals one version’s manifest is false by exception, and a new parent may be paired with a stale child.

Strong load-dependency cycles are declared errors, but the weak-reference type is not defined anywhere in §4, §5’s serializable kinds, the artifact encoding, or query invalidation. The prior cycle finding is therefore only papered over.

The v1 loader cannot simply be ported for this guarantee; it explicitly does not verify that dependencies loaded their new version: [loader TODO](/Users/karl/Projects/distill/loader/src/loader.rs:417).

### 10. High: the CAS protocol handles the basic append crash, but not full rebuild, compaction or eviction correctness (§13)

The append → segment fsync → SQLite commit ordering correctly prevents a committed row from preceding durable payload under normal filesystem semantics. Remaining holes:

- Records need magic/version/framing, bounded lengths and verification that the payload hashes to the stored BLAKE3, not only CRC.
- Scanning segments can rebuild `CKey → location`, but not `cache key → CKey`, because cache keys are absent from records. The text should distinguish those indexes.
- New segment creation and compaction require directory fsync.
- Orphaned compacted segments need an active-generation manifest or deterministic recovery rule.
- Recovery must define duplicate-CKey selection and behavior after interior corruption.
- Eviction must atomically clear every current/last-good manifest reference or pin all referenced CKeys. Otherwise `resolve` can return a CKey that `fetch` no longer has.
- The lease must pin the returned CKey before `resolve` becomes observable; pinning afterward leaves an eviction race.
- Build jobs and snapshot-scoped derived outputs also require generation pins.

### 11. Medium-high: DSB and canonical JSON remain underspecified for independent implementations (§6)

The revision addresses most prior parser concerns, but still needs:

- a named CRC variant;
- maximum JSON/blob/file sizes before allocation;
- checked arithmetic for every offset and alignment operation;
- a definition of whether blob offsets are relative to the blob chunk;
- zero validation for outer and internal padding;
- ordering across assets, nested field paths and collections—not merely “sorted field order”;
- rules for zero-length and repeated blob ranges;
- header-version/JSON-version compatibility rules;
- directory fsync after atomic rename;
- canonical string escaping and key comparison;
- a representation for `HashMap<K,V>` when `K` is not a JSON object key.

The DSTL artifact container is much less precise than DSB: it lacks exact field sizes, section offsets/lengths, integrity fields and canonical target encoding.

### 12. Medium-high: the shader example contradicts the explicit-import model and cannot reuse rafx IO unchanged (§§1, 8, 20)

The worked invalidation table says editing `blit.frag`, adding `foo.vert`, or adding an override automatically changes the pipeline. But external raw files are explicitly weak provenance and re-import is explicit. Editing the external `.frag` cannot trigger a stage content dependency until re-import writes a new bundle.

Overrides are described as sibling entries inside a bundle, while cooking probes names such as `blit.frag.metal` through path lookup. A sibling local ID is not a bundle path; this needs a same-bundle lookup API or explicit stage references.

Current rafx shader processing is filesystem-coupled:

- Includes call `std::fs::read_to_string` directly: [include implementation](/Users/karl/Projects/rafx/rafx-shader-processor/src/include.rs:32).
- Overrides use `Path::exists` and direct file reads: [override probing](/Users/karl/Projects/rafx/rafx-shader-processor/src/lib.rs:606).
- Batch grouping and generated output writing are embedded in the processor: [directory processing](/Users/karl/Projects/rafx/rafx-shader-processor/src/lib.rs:188).

The compiler/reflection/codegen core is reusable, but include resolution, override discovery, grouping, output emission and dependency tracing require extraction behind new interfaces. “SPIR-V intermediates live in the CAS” also lacks an intermediate-output API, particularly since §22 rejects the same named derivation mechanism that could expose them.

### 13. Medium: Rubicon and panic-isolation claims do not match the checked code (§3)

No Rubicon/xgraph implementation or dependency exists in the checked repositories. newgameplus uses bespoke `libloading`, manual function-pointer auditing and resource cleanup: [module reload](/Users/karl/Projects/newgameplus/newgameplus/src/module_state.rs:623). That is useful precedent, but not an existing reusable Rubicon host.

“Panic fails the job, not the daemon” requires an explicit `catch_unwind` boundary, unwind-compatible panic configuration, and a strict ban on `abort`. It does not cover allocator corruption, native-library crashes, deadlocks or process aborts. The epoch drain addresses dangling code pointers only if every callback, allocator-owned value, thread-local and background thread is tracked.

## Prior-round disposition

| Prior finding | Disposition |
|---|---|
| Per-target schemas/layouts | Survives; explicitly deferred. |
| Safe artifact loading | Partially papered over; fixup representation remains incomplete. |
| Authored vs terminal type | Mostly addressed by the pipeline map; hot-reload and derived-output metadata remain. |
| Logical migration IR | Correct direction, but not existing reuse and op semantics are incomplete. |
| Lossy missing-schema degradation | Addressed by schema closure and hard failure. |
| Labeled cache dependencies | Addressed, but new determinant and lookup holes remain. |
| Arbitrary pipeline impurity | Partially addressed by dylib hashing; external/environmental inputs survive. |
| Stale-job CAS/last-good | Commit guard added; snapshot-successor and degraded-manifest semantics survive. |
| CAS crash protocol | Basic ordering addressed; compaction, rebuild and eviction holes remain. |
| Dylib reload safety | Epoch barrier added; hosting/isolation claims remain unimplemented. |
| UUID copy collisions | Addressed. |
| Load cycles | Only partially addressed; weak refs are undefined. |
| Graph-atomic hot reload | Partially addressed; closure expansion and last-good exception survive. |
| Canonical JSON/DSB | Substantially improved, still not independently implementable. |
| Migration graph stability | Survives. |
| Processor scheduling | Largely addressed. |

## Prioritized top 10

1. Define a snapshot-scoped lazy build session and successor/virtual manifest model, including derived-output resolution.
2. Replace in-slot `VarRef` encoding with a real wire layout and complete generated construction/rollback plan.
3. Specify static action keys, stored-trace validation and complete cache determinants.
4. Make per-target schema/layout generation a prerequisite, not a deferred shipping risk.
5. Formalize migration graph construction, logical IR semantics, default-code identity and migration query dependencies.
6. Add explicit semantic schema revisions and a versioned canonical logical-hash AST.
7. Unify reference queries with the precise AssetQuery grammar and cover code/schema-driven invalidation.
8. Define full-closure atomic swaps and formal `Current/StaleLastGood/Missing/Dead` manifest states.
9. Complete CAS generation, compaction, lease, eviction and rebuild protocols.
10. Pin DSB/DSTL byte grammars and prove the shader pipeline using context-backed IO rather than filesystem-coupled rafx code.
tokens used
