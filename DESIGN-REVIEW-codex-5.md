codex
No CRITICAL issue remains. Five HIGH issues remain: wire-format completeness, blob split/reconstruction, multi-stage derived-output resolution, enforceable compilation identity, and load-DAG validation.

## Round-4 disposition

| # | Round-4 finding | Disposition |
|---|---|---|
| 1 | CAS could not recover atomic output tables | **Closed for typed outputs.** A trailing result record now contains the action key, trace, and complete output table; payload and result records have separate rebuild roles, and incomplete groups are ignored ([§13](/Users/karl/Projects/distill/DESIGN.md:1127)). Debug payload recovery remains a new, narrower issue below. |
| 2 | Output declarations were not static | **Closed locally.** Registration declares the exact keys and types, extras are terminal and target-invariant, and keys are chain-unique ([§9](/Users/karl/Projects/distill/DESIGN.md:553)). Multi-stage ownership/resolution of those outputs is still undefined; see finding 3. |
| 3 | Wire layout was not normative | **Partially closed.** Explicit niche tags, recursive repacking, scatter-copy, rollback, and a DSWL domain are substantial fixes. Direct/single-variant enums, tag values, padding, and the actual DSWL byte grammar remain insufficient; see finding 1. |
| 4 | Component definition and manifest versions contradicted | **Closed in principle.** Entries now carry adoption versions, and components use weak connectivity over adopted ∪ candidate edges with reverse dependents ([§15](/Users/karl/Projects/distill/DESIGN.md:1275), [§15](/Users/karl/Projects/distill/DESIGN.md:1353)). Failed merges still need a precise rollback rule; see finding 8. |
| 5 | External tools were absent from cache identity | **Closed for invalidation correctness.** `tool(id) → binary hash` is now a revalidated trace operation ([§9](/Users/karl/Projects/distill/DESIGN.md:641)). Tool-ID resolution remains underspecified, but the original stale-hit defect is fixed. |
| 6 | Strict compilation gate was unrepresentable | **Not fully closed.** The schema-side record is specified, but the target/pipeline side still has no comparable compilation identity; see finding 4. |
| 7 | Failed tag extraction silently under-approximated queries | **Closed.** Potentially matching tag queries fail and name poisoned bundles ([§10](/Users/karl/Projects/distill/DESIGN.md:822)). |
| 8 | Blocked packs could not provide contiguous mmap blobs | **Physical-contiguity fix accepted, end-to-end representation incomplete.** Unbounded stored extents solve the block-splitting problem, but DSTL and `LoaderIO` do not describe how those extents replace ranges of the raw CKey artifact; see finding 2. |
| 9 | Logical grammar lacked unit | **Closed.** `0x0D unit` is distinct from an empty struct, and unsupported leaves fail extraction ([§5](/Users/karl/Projects/distill/DESIGN.md:259)). |
| 10 | Input versions and memo commits shared an authority | **Partially closed.** The semantic domains are now separate, but metadata/dependency ownership still contradicts immutable input snapshots; see finding 6. |
| 11 | Migration-edge queries were inexpressible | **Closed.** `migration_edge(type_uuid, from_hash)` is a dedicated indexed selector ([§10](/Users/karl/Projects/distill/DESIGN.md:793)). |
| 12 | Directory-import deletion and output ownership were undefined | **Closed.** Lost groups become doctor-listed orphans; paths are deterministic and collisions are errors ([§8](/Users/karl/Projects/distill/DESIGN.md:497)). |

## New and remaining findings

### 1. HIGH: the wire format still cannot be implemented uniquely and safely (§12)

The new recursive rules leave several observable cases undefined:

- Direct-tag enums retain native tag bytes, but there is no rule for recomputing their payload-union offset, alignment, or total size when a recursively transformed variant changes layout ([DESIGN.md:1056](/Users/karl/Projects/distill/DESIGN.md:1056)).
- Single-variant enums are not covered.
- Explicit niche tags have no canonical mapping from variant to `u32`.
- Repacking is triggered only by a “size-diverging” member; an alignment-only divergence can also change enclosing offsets ([DESIGN.md:1062](/Users/karl/Projects/distill/DESIGN.md:1062)).
- DSWL is called a canonical serialization, but no node tags, integer widths, traversal order, or string encoding are given ([DESIGN.md:1065](/Users/karl/Projects/distill/DESIGN.md:1065)).
- Internal struct/union padding is not required to be zero. “Wire = native bytes” and maximal flat-copy runs can therefore serialize uninitialized or nondeterministic padding ([DESIGN.md:994](/Users/karl/Projects/distill/DESIGN.md:994), [DESIGN.md:1051](/Users/karl/Projects/distill/DESIGN.md:1051)).

The current schema reinforces the problem: `TagEncoding::Direct` carries only `tag_size`; it contains neither direct discriminant values nor a tag offset, while `Single` and `Niche` are separate cases ([ngp-schema/src/lib.rs](/Users/karl/Projects/newgameplus/ngp-schema/src/lib.rs:98)). `Field` likewise has no variant discriminant value ([ngp-schema/src/lib.rs](/Users/karl/Projects/newgameplus/ngp-schema/src/lib.rs:227)).

Specify the complete wire AST and binary DSWL grammar, all enum categories and tag mappings, canonical zero padding, and encoder copy runs that contain initialized value bytes only.

### 2. HIGH: pack blob extents have no mapping back to the raw DSTL artifact (§§12, 15, 16)

The contiguous extent fixes physical mmap eligibility, but not the encoding contract:

- DSTL has no blob table or blob section.
- `Blob` is absent from the listed `VarRef` slot kinds, although `ConstructBlob` exists ([DESIGN.md:997](/Users/karl/Projects/distill/DESIGN.md:997), [DESIGN.md:1025](/Users/karl/Projects/distill/DESIGN.md:1025)).
- The pack encoding table says only `CKey → block EKeys + blob extents`; it does not record which raw DSTL ranges each extent replaces ([DESIGN.md:1474](/Users/karl/Projects/distill/DESIGN.md:1474)).
- CKey hashes the raw artifact, while pack loading reassembles only “structural bytes” and binds blobs separately ([DESIGN.md:1458](/Users/karl/Projects/distill/DESIGN.md:1458)). No scatter-hash/reconstruction algorithm is specified.
- `fetch(ckey)` is described as returning the same byte-shaped object for RPC and pack IO, but pack construction needs a structural buffer plus mmap extent handles ([DESIGN.md:1306](/Users/karl/Projects/distill/DESIGN.md:1306)).

Define a `BlobRef` wire slot, raw range/extent descriptors, canonical split and reconstruction rules, CKey verification over the composite representation, and a `fetch` result capable of carrying both structural storage and external extents.

### 3. HIGH: derived outputs from non-final processor stages are not resolvable or safely pinned (§§9, 13, 15)

Action keys and result tables are per chain stage ([DESIGN.md:659](/Users/karl/Projects/distill/DESIGN.md:659)), but child resolution stores only `(parent UUID, output_key)` and then resolves the parent and reads `outputs[output_key]` ([DESIGN.md:680](/Users/karl/Projects/distill/DESIGN.md:680)).

For `S1 → S2`, if `S1` emits extra `reflection` and `S2` emits the terminal primary:

- the final `S2` table does not contain `S1`’s extra;
- the child index does not identify `S1` or its action/result;
- moving the key between stages is claimed to preserve identity, but no chain-level table defines that continuity;
- pinning the final result does not pin the earlier-stage result. Eviction can remove the earlier derived-index row after the final parent artifact—containing the child UUID—has been fetched, contradicting the claim that child lookup “cannot miss.”

Either commit a chain-level aggregate result containing the terminal primary plus every extra from all stages, or make the durable child index identify the producing stage/result and keep that mapping reconstructible and pinned for as long as any referencing artifact is pinned.

### 4. HIGH: `CompilationIdentity` remains one-sided (§§5, 12, 18, 22)

The schema record now has the necessary fields ([DESIGN.md:193](/Users/karl/Projects/distill/DESIGN.md:193)), but the configured target still carries only OS, architecture, and APIs ([DESIGN.md:1566](/Users/karl/Projects/distill/DESIGN.md:1566)). Those values cannot determine:

- the exact Rust target triple (`gnu` versus `musl`, `msvc` versus `gnu`);
- rustc identity;
- asset-type features and cfgs;
- manifest/lock identity;
- source-walk algorithm version.

The design also does not specify a compilation-identity attestation in the pipeline module. Consequently, the equality test at [§12](/Users/karl/Projects/distill/DESIGN.md:1078) has only one operand.

Current code confirms this remains future work: `Schema` contains only `source_hashes` and `types` ([ngp-schema/src/lib.rs](/Users/karl/Projects/newgameplus/ngp-schema/src/lib.rs:204)); source-walk selects a target from `TARGET` or host cases ([source-walk/src/main.rs](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:456)); and source hashing still uses `DefaultHasher` over Rust paths/content plus extras ([ngp-source-hash/src/lib.rs](/Users/karl/Projects/newgameplus/ngp-source-hash/src/lib.rs:32)).

Add the same canonical `CompilationIdentity` to each target/layout set and pipeline-module registration, then state exactly which identities must compare equal.

### 5. HIGH: commit-time load-DAG enforcement conflicts with per-asset lazy resolution (§§9, 15)

The daemon promises to reject load-dependency cycles “when artifacts commit” ([DESIGN.md:581](/Users/karl/Projects/distill/DESIGN.md:581)). But processor-produced load edges are known only after building each artifact, and ordinary load dependencies do not trigger `ctx.read`; the loader learns and expands them after resolving/fetching the parent.

Thus, when committing `A → B`, the daemon may not yet have built `B` and cannot know that `B → A`. Committing `A` first admits the exact cycle the rule promises to prevent. Resolving the entire candidate load closure before any artifact publication would work, but that behavior and its transaction/cycle-detection boundary are not specified.

This matters because the cited v1 loader really only waits for dependencies and still has a TODO to check their new versions ([loader.rs](/Users/karl/Projects/distill/loader/src/loader.rs:417)); it provides no cycle-safe precedent.

Define whether daemon `resolve` preflights/builds the complete load closure and runs SCC detection before publication, or whether clients detect SCCs before entering their waiting states. The current commit-time claim is not implementable as written.

### 6. MEDIUM-HIGH: memo state and immutable input snapshots still share undefined metadata (§13)

The design correctly separates input versions from memo sequences, but then says dependency records and indexes are part of the same input-versioned store ([DESIGN.md:1175](/Users/karl/Projects/distill/DESIGN.md:1175)). Processor traces are memo outputs, committed without advancing the input version. Immutable snapshot indexes are `Arc` clones ([DESIGN.md:1211](/Users/karl/Projects/distill/DESIGN.md:1211)), so an old clone cannot acquire a newly committed memo unless resolution consults a separate mutable memo layer.

Partition:

- input-versioned metadata, tags, path/type/query membership;
- memo action/result records and their reverse trace indexes;
- ephemeral manifests and leases.

Also specify coordinator ordering when an input transaction and a memo commit race. The single writer makes a sound rule possible, but “same versioned store” does not provide it.

### 7. MEDIUM-HIGH: raw-file enumeration cannot use the stated `AssetQuery` result type (§§8, 10)

Importer listings reuse `AssetQuery` over the raw-file table and return a sorted matched set ([DESIGN.md:473](/Users/karl/Projects/distill/DESIGN.md:473)). The supposedly same query language otherwise returns canonically sorted asset UUIDs ([DESIGN.md:772](/Users/karl/Projects/distill/DESIGN.md:772), [DESIGN.md:795](/Users/karl/Projects/distill/DESIGN.md:795)). Raw files have no asset UUIDs, and the path selectors’ asset semantics—bundle entries beneath a prefix—differ from raw-file membership.

Introduce `FileQuery → Vec<NormalizedPath>` or parameterize the query namespace and result-key encoding. Pin the result hash grammar separately for paths and UUIDs.

### 8. MEDIUM-HIGH: failed candidate-component merges have no coherent “previous version” (§15)

A new edge can merge two adopted components that currently sit at different versions ([DESIGN.md:1353](/Users/karl/Projects/distill/DESIGN.md:1353)). If candidate resolution then fails, the merged candidate component has no single “previous consistent version” to remain at, contrary to [DESIGN.md:1348](/Users/karl/Projects/distill/DESIGN.md:1348).

Safety is recoverable: discard the candidate graph and restore each pre-sweep adopted component at its own prior cut. The document should state that explicitly and define which entries become `StaleLastGood`; an unrelated old component pulled into a failed provisional merge should not be relabeled as though its own build failed.

### 9. MEDIUM: CAS recovery does not cover cache-internal debug payloads (§§13, 20)

The CAS grammar includes `debug` payload records ([DESIGN.md:1113](/Users/karl/Projects/distill/DESIGN.md:1113)), while result commit markers contain only the typed output table ([DESIGN.md:1130](/Users/karl/Projects/distill/DESIGN.md:1130)). Shader intermediates are nevertheless supposed to live under the cook’s cache entry ([DESIGN.md:1666](/Users/karl/Projects/distill/DESIGN.md:1666)).

A segment scan cannot tell which debug records a committed result owns, rebuild their lookup keys, or distinguish them from orphaned payloads. Include a committed auxiliary-payload table in the result record or give debug data its own result/commit framing.

### 10. MEDIUM: `tool(id)` lacks executable-resolution semantics (§9)

Revalidating a binary hash fixes the round-4 stale-hit bug, but the design does not define what `id` identifies or how lookup handles:

- PATH changes and symlink retargeting;
- multiple tool versions;
- the resolved executable path;
- invocation-specific environment or auxiliary tool files.

Require subprocesses to be launched through `ProcessContext`, define `id → resolved executable` via a target/tool registry, and record the resolved identity and content hash. Inputs intentionally outside that interface should be explicitly covered by the trusted determinism contract.

## Code verification

The significant repository claims remain accurate:

- `ngp-schema` has `Unit` but no signed `I128`, exactly as the logical-grammar text says ([ngp-schema/src/lib.rs](/Users/karl/Projects/newgameplus/ngp-schema/src/lib.rs:166)).
- Source-walk still hardcodes 64-bit `Vec`/`String` and pointer layouts ([source-walk/src/main.rs](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:1251), [source-walk/src/main.rs](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:1515)) and accesses RA layout internals through unsafe reinterpretation ([source-walk/src/main.rs](/Users/karl/Projects/newgameplus/source-walk/src/main.rs:1489)).
- The existing migration executor still zero-fills when required default functions are missing, so the document is correct to require fail-hard extension work ([ngp-reflect/src/migrate.rs](/Users/karl/Projects/newgameplus/ngp-reflect/src/migrate.rs:51)).
- Rafx include reads, override probing, directory grouping, and output writes remain filesystem-coupled, so the stated refactor is real ([include.rs](/Users/karl/Projects/rafx/rafx-shader-processor/src/include.rs:46), [lib.rs](/Users/karl/Projects/rafx/rafx-shader-processor/src/lib.rs:188), [lib.rs](/Users/karl/Projects/rafx/rafx-shader-processor/src/lib.rs:606), [lib.rs](/Users/karl/Projects/rafx/rafx-shader-processor/src/lib.rs:1054)).
- Newgameplus continues to provide a valid bespoke module-host precedent: `libloading`, function-pointer auditing, and ordered old-library teardown are present ([module_state.rs](/Users/karl/Projects/newgameplus/newgameplus/src/module_state.rs:180), [module_state.rs](/Users/karl/Projects/newgameplus/newgameplus/src/module_state.rs:723), [module_state.rs](/Users/karl/Projects/newgameplus/newgameplus/src/module_state.rs:912)).

## Prioritized top 10

1. Complete the wire AST, enum encoding, deterministic padding rules, and exact DSWL grammar.
2. Define DSTL `BlobRef`s and the raw-range ↔ pack-extent reconstruction/CKey contract.
3. Add a chain-level aggregate result or durable producing-stage identity for derived outputs.
4. Put `CompilationIdentity` on targets/layout sets and pipeline-module registration.
5. Specify full-closure load-DAG validation before artifacts can become publishable.
6. Split input-versioned metadata from memo traces/results and reverse memo indexes.
7. Give raw-file listings their own result type and canonical hash encoding.
8. Define rollback and state labeling when a failed candidate edge provisionally merges old components.
9. Include debug/auxiliary payloads in durable CAS result framing.
10. Define tool-ID resolution and force traced tool execution through `ProcessContext`.

**VERDICT: Remaining CRITICAL issues: none. Remaining HIGH issues: incomplete wire-layout/DSWL encoding; undefined DSTL-to-pack blob reconstruction; unresolved multi-stage derived-output lookup and pinning; one-sided `CompilationIdentity` enforcement; and unspecified load-dependency DAG validation under lazy resolution.**
