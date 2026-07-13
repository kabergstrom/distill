I found 3 CRITICAL, 14 HIGH, and 8 MEDIUM defects not already covered by the ledger.

## CRITICAL

1. **CRITICAL — §§3, 4, 9, 12 — Typed processor outputs have no serialization surface**

   `Outputs::primary<T>` and `extra<T>` accept arbitrary typed Rust values, but `AssetRuntimeDescriptor` exposes only construction, dropping, defaults, and finalization—no encoder or visitor. Raw layout information cannot deterministically iterate `HashMap`/`HashSet`, extract `Blob` bytes, or discover strong versus weak references, so conforming code cannot actually produce DSTL artifacts from processor outputs.  
   **Fix direction:** Generate no-unwind encode/visit callbacks for every asset type, preferably converting module-side values into a neutral canonical wire builder or `AuthoredValue` plus reference metadata before crossing the module boundary.

2. **CRITICAL — §§13, 15, 17 — `LoaderIO` erases the snapshot basis required for atomic component swaps**

   Component adoption requires every asset and path resolution in a sweep to use one snapshot, but `LoaderIO::resolve` and `resolve_path` accept no snapshot or batch token; `Failed`, `Missing`, and every `PathResolveResult` also carry no basis. Conversely, `ResolveResult::Built` requires a `SnapshotStamp`, which `PackfileIO` cannot produce because packs explicitly have no `InputVersion`.  
   **Fix direction:** Introduce an IO-neutral `IoBasis` such as `Rpc(SnapshotStamp) | Pack(ManifestHash)`, pass a basis into every resolution, return it in every terminal outcome, and expose an explicit begin-sweep or batch-resolve operation.

3. **CRITICAL — §§2, 14, 20 — Exchange verification can still destroy a concurrent in-place edit**

   After an exchange succeeds, the displaced target inode sits at the temp path; the daemon hashes it and then deletes it when it matches the expected pre-image. An editor holding an open descriptor to that inode can write after verification but before or even after unlink, and those new bytes disappear when the final descriptor closes—violating the “no edit is ever silently lost” guarantee.  
   **Fix direction:** Preserve every displaced inode under a named backup until conservative retention or explicit user cleanup, or explicitly restrict the guarantee to atomic-replacement writers; a one-time post-exchange hash cannot justify immediate deletion.

## HIGH

4. **HIGH — §§9, 13, 15 — Failure traces cannot represent the failing operation**

   Failure records promise a trace “up to and including” the failing operation so other snapshots can revalidate it. `TraceOp`, however, represents only successful reads, resolutions, queries, and tool observations; it cannot encode ambiguity, poisoning, missing strong references, descendant build failures, or tool-launch errors.  
   **Fix direction:** Make each trace operation carry a stable observed outcome, for example `Observed<T> = Ok(T) | Err(StableFailureFingerprint)`, while continuing to exclude transient infrastructure failures from memoization.

5. **HIGH — §§3, 9, 13, 15 — Tool binaries are live inputs, not snapshot inputs**

   A tool’s hash is observed from its registered path per invocation and per cache revalidation, while a snapshot does not pin that path-to-hash mapping. The same snapshot can therefore select an old candidate before a tool replacement and a new candidate afterward, contradicting the rule that a snapshot denotes immutable inputs.  
   **Fix direction:** Publish tool hashes as input-versioned `ToolEpoch` state pinned by snapshots, execute the corresponding staged copy, and add `DriftedInput::Tool` for an old basis whose tool bytes are unavailable.

6. **HIGH — §§3, 5, 9, 13 — The dylib hash does not cover dynamically linked code**

   The spec claims linked libraries such as shaderc and spirv-cross are covered by the pipeline dylib hash, which is true only for static linkage. A dependent `.so`, `.dylib`, or DLL can change while the copied cdylib and every cache key remain unchanged, and `dlopen` will execute the live dependency.  
   **Fix direction:** Require static linkage for pipeline-only libraries, or stage and hash the full dynamic dependency closure, bind loading to those staged paths, and include the closure digest in the epoch and build keys.

7. **HIGH — §§5, 9, 10, 13, 15, 16 — `build_only` has no invalidation epoch**

   `build_only` is deliberately excluded from both logical and layout hashes, but unlike the similarly unhashed `tag` attribute it has no policy epoch or reverse-invalidation mechanism. Toggling it can leave an already loaded or cached closure active indefinitely even though that closure is now forbidden.  
   **Fix direction:** Add a per-type load-policy digest to input-versioned metadata, make closure validation depend on it, and emit component invalidations whenever it changes.

8. **HIGH — §§4, 5, 11 — Semantic revisions have no mandatory migration rule**

   `#[asset(rev = N)]` changes the logical hash, but §11 never says how the automatic planner treats a revision mismatch. A field-by-field implementation can legally copy an unchanged shape and silently reinterpret metres as centimetres—the exact defect revisions are meant to prevent.  
   **Fix direction:** Make every struct, field, or variant revision mismatch an automatic-plan hard stop requiring an explicit custom migration edge, unless a specific built-in semantic rule is declared.

9. **HIGH — §§5, 11 — Automatic migration has no direction and can run backward**

   Logical hashes contain no chronology or ancestry, yet any non-current node without an outgoing custom edge is automatically diffed into the current schema. During a code rollback—or while §5 deliberately serves an older registry during schema staleness—a newer bundle is therefore automatically projected into older semantics, including backward semantic revisions.  
   **Fix direction:** Record schema lineage/direction and permit automatic migration only along declared forward ancestry; otherwise require an explicit reverse edge or refuse schema-dependent builds while the registry is known stale.

10. **HIGH — §§3, 7, 13 — A pipeline-poisoned snapshot has no representable epoch**

    A failed candidate explicitly never becomes a `PipelineEpoch`, and the prior epoch may not stand in for it. Nevertheless, every `MetadataSnapshot` must return `&Arc<PipelineEpoch>`, while terminal-type queries, the derived-output namespace, and `load_current` all require a pipeline map or registry for that version.  
    **Fix direction:** Store `PipelineState::Ready(Arc<PipelineEpoch>) | Poisoned(error, schema_only_state)` in snapshots, make `epoch()` fallible, and precisely classify which metadata operations remain valid under poison.

11. **HIGH — §§3, 9, 13, 18 — Pipeline maps and configuration epochs can become inconsistent**

    `PipelineEpoch` owns the pipeline map, but target edits rotate only a separate configuration epoch. Adding or changing a target can require a new chain selection and new terminal/extras-invariance validation, leaving a snapshot pairing a fresh target definition with a pipeline map constructed for the previous target set.  
    **Fix direction:** Stage a combined execution epoch from module registrations, schemas, and target configuration, or define and validate one symbolic pipeline map over the complete supported OS/API domain rather than configured targets.

12. **HIGH — §§5, 12 — The native-layout AST cannot encode its normative grammar**

    DSNL says every node carries offset, size, and alignment, but `Unit` and `BackRef` carry none and `Skip` omits alignment; this also contradicts the promise that skipped-slot alignment is measured. `NativeField` omits `declaration_index`, even though DSWL requires it to break equal-wire-offset ties after repacking, where native slice order may no longer provide that information.  
    **Fix direction:** Add complete geometry to all applicable nodes and declaration indices to fields, or normatively define inferable values and a preserved-order rule that requires no missing data.

13. **HIGH — §§9, 13, 18 — Dependency-depth failures are context-dependent but cache keys are not**

    The depth cap is measured from the root request, while DSSI keys, candidate traces, and failure records contain no ancestry or remaining-depth budget. A child can fail when reached deeply and have that failure reused for a valid top-level request, or a cached child can bypass the cap unless every memo revalidation recursively recounts the entire chain.  
    **Fix direction:** Treat depth exhaustion as a non-memoized scheduler outcome and validate stored dependency graphs against the caller’s remaining budget, or include an explicit depth policy/budget in the relevant lookup semantics.

14. **HIGH — §16 — Blob extents have two incompatible addressing models**

    §16 first makes every structural block and blob extent an EKey-addressed object through the global index, but later defines blob references directly as `(archive generation, offset, len)`. A patch containing only EKeys the client lacks cannot safely reuse an existing blob if the new manifest embeds a physical location in an archive the client did not download.  
    **Fix direction:** Represent every blob extent by EKey in the ContentHash encoding table and keep generation/offset/length solely in the EKey-to-location index.

15. **HIGH — §§17, 18, 22 — The unauthenticated RPC endpoint is configurable beyond localhost**

    Authentication and TLS are descoped specifically on the basis of a localhost bind, but `daemon.address` is arbitrary and configuration validation does not require loopback. Binding to a non-loopback address exposes authoring writes, imports, deletion, migration, and maintenance operations without authentication.  
    **Fix direction:** Reject non-loopback TCP addresses and prefer a permissioned local socket, or require authentication and transport security whenever the endpoint is not local.

16. **HIGH — §§13, 15, 18 — Target-definition drift has no protocol representation**

    §18 requires a target-bound connection to receive a definition-changed condition that forces reconnect, but `DriftedInput` has no configuration or target-definition variant. Returning ordinary `Drifted` can make the retry-refreshed loader loop on the same obsolete Hub, while returning `Failed` freezes the component instead of reconnecting.  
    **Fix direction:** Add a typed `ReconnectRequired::TargetDefinitionChanged` result or connection event and make RpcIO replace the Hub before retrying.

17. **HIGH — §§8, 13, 14, 18 — Daemon-owned paths may fall inside watched roots and create feedback loops**

    Configuration does not require `state_path`, schema artifacts, module artifacts, or other daemon outputs to be disjoint from asset roots. Because raw `FileQuery` dependencies see tracked files, CAS writes or module rebuilds inside a root can advance input versions, invalidate watched imports, and trigger further writes indefinitely.  
    **Fix direction:** Validate daemon-owned paths as disjoint from asset roots or exclude them by retained directory identity before scanning, querying, and watcher publication.

## MEDIUM

18. **MEDIUM — §§8, 11, 13 — The “fully static” DSBI key may require executing arbitrary migration code**

    DSBI includes the resolution results of references in the current migrated value, but a `MigrationFn` or default materializer can synthesize those references. The daemon cannot know the referenced queries before running the very code the cache lookup is intended to avoid.  
    **Fix direction:** Define a mandatory canonical pre-key `load_current` phase and admit that it executes code, or give build imports the same static-key-plus-discovered-trace bucket model as processors.

19. **MEDIUM — §16 — Pack reproducibility contradicts EKey construction**

    The spec promises identical manifests and tables for one definition and snapshot while allowing zstd encoders to produce different frame bytes. Because EKeys hash those encoded bytes and the manifest’s encoding/index tables contain EKeys, different compliant encoders necessarily produce different tables and manifest hashes.  
    **Fix direction:** Either pin the complete encoder implementation and parameters or redefine reproducibility over a logical manifest that excludes physical EKeys and archive placement.

20. **MEDIUM — §§5, 6 — Canonical JSON does not define `f32` quantization**

    `AuthoredValue::Float(f64)` represents both `f32` and `f64` leaves, but canonical serialization only specifies ECMAScript double formatting. One writer can preserve the parsed double before validating/casting to `f32`, while another can round to `f32` first and widen back, producing different canonical bundle bytes.  
    **Fix direction:** Make canonicalization schema-directed and require `f32` values to be rounded to their exact binary32 value before decimal emission.

21. **MEDIUM — §11 — `MigrationOp::Custom` has no defined semantics**

    `MigrationKind::Function` already defines whole-edge custom execution, while `MigrationOp::Custom { fn_key }` supplies neither a path nor subtree endpoint schemas. It is therefore unclear what value it receives, what destination it writes, or how it participates in the total-and-disjoint output rule.  
    **Fix direction:** Remove `MigrationOp::Custom`, or give it an explicit input path, output path, endpoint schemas, and fresh-output semantics.

22. **MEDIUM — §§15, 17 — The declared RPC has no `resolvePath` method**

    §17 calls `resolvePath` one of the five loader operations and §15 says `LoaderIO::resolve_path` crosses only that boundary, but neither `Hub` nor `Snapshot` declares it. Implementing it through `Snapshot.query` would also leave the required path-specific result and basis semantics implicit.  
    **Fix direction:** Add `Snapshot.resolvePath` returning a basis-tagged `PathResolveResult`, with Hub sugar if needed.

23. **MEDIUM — §§5, 12 — Hash-domain rules are internally incomplete**

    `CompilationIdentity` and `ModuleAbiIdentity` share `"DSCI"` without a record-kind tag despite the rule that distinct meanings never share a domain. The fixup-table identity and full input hash are also specified as bare concatenation hashes but have no entries in the supposedly exhaustive domain table.  
    **Fix direction:** Assign distinct versioned domains or explicit record-kind tags to every hash construction and add the missing identities to the domain registry.

24. **MEDIUM — §§13, 18 — Most configuration fields have no reload or restart semantics**

    The design says configuration changes are never unspecified, but only roots and targets receive candidate-epoch behavior. Changes to `pipeline_dylib`, `state_path`, address, codegen settings, depth policy, parallelism, and CAS limits are not classified as live input, operational tuning, or restart-only.  
    **Fix direction:** Define a per-field policy table and include every semantic live field in the appropriate epoch or cache-policy identity.

25. **MEDIUM — §16 — The pack index and trailer grammar is contradictory**

    The index is described as living inside the manifest’s tables, but activation later requires separate “index files”; no relationship between those two forms is defined. The “per-file blake3 trailer” also lacks an exact byte layout and coverage rule, making it unclear whether the trailer hashes preceding bytes or participates in the manifest’s own hash.  
    **Fix direction:** Publish one complete byte grammar specifying file boundaries, table placement, trailer exclusion/coverage, and the exact files referenced by a manifest.
