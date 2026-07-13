codex
The seven round-14 repairs are internally coherent as written. I found these new issues:

1. **MEDIUM — §§4, 5, 9, 10, 19, 22 — the schema attribute channel cannot carry `tag` or `build_only`.**  
   `FieldAttrs` contains only `{ rev, skip, blob }`, while `TypeAttrs` contains only `rev`. Consequently, the live registry cannot derive the promised type-to-tag-field map, and nothing represents `#[asset(build_only)]`. A literal implementation could silently omit tagged entries from queries or fail to reject a build-only type in a load-dependency closure.  
   **Fix direction:** Add `tag: bool` to `FieldAttrs` and `build_only: bool` to `TypeAttrs`; explicitly exclude both from the logical hash while including them in the tag-annotation epoch and load-closure validation respectively. Update the §19 source-walk attribute list.

2. **HIGH — §§3, 5, 12, 13 — cached fixup plans are not bound to the generated constructor/drop-table identity or module epoch.**  
   Plans cache only on `(type, wire layout hash, native layout digest)`, but the native digest measures memory layout, not the assignment of `CtorId`/`DropId` slots. Suppose epoch E1 assigns `CtorId(7)` to `Vec<u32>`. A module rebuild adds an unrelated container and shifts generated table ordering while leaving the root type’s native layout unchanged. Epoch E2 can reuse the old plan and interpret ID 7 as another constructor, producing memory corruption or UB. The same issue arises if a macro/codegen revision changes ID ordering without changing layouts.  
   **Fix direction:** Include the pipeline/module epoch and a generated table-mapping digest in the plan-cache key, and clear epoch-owned plans before unloading. Alternatively, define per-root descriptor tables whose ID assignment is canonical and attest that assignment as part of the native identity.

3. **MEDIUM — §§4, 12, 15 — the game-side fixup tables have no declared registration or lookup boundary.**  
   `AssetType` exposes only `TYPE_UUID`, while fetched dependencies are dynamically identified by `TypeUuid`. No normative API maps that UUID to its native-layout descriptor, `CtorTable`, `DropTable`, or skip-writer table. `Registry::defaults` is the daemon/pipeline migration surface and cannot supply the game loader’s tables. Thus a fetched terminal artifact containing a vector, blob, or skipped field cannot reach the data required to compile and execute its fixup plan through any declared API.  
   **Fix direction:** Define a generated `AssetRuntimeDescriptor` and either expose it through `AssetType` or register it in a game-side `TypeUuid → descriptor` registry passed to `Loader`. Bind its identity to finding 2’s table digest.

4. **MEDIUM — §§5, 12, 18, 22 — one logical `TypeDef` tree cannot represent target-dependent logical shapes.**  
   `SchemaLayouts` supports multiple target layouts, but every table is positionally parallel to one shared `Schema.types`. A Windows-only `#[cfg]` field, variant, or type substitution changes the logical field set, so a Windows layout cannot be zipped safely against a macOS-derived `TypeDef`. Worse, the host pipeline module cannot construct a foreign-target field that does not exist in its host type. The current host-only gate prevents this today, but the declared “settled” representation remains insufficient once per-target emission lands.  
   **Fix direction:** Require and verify that every configured target produces the same logical projection and node correspondence, rejecting target-dependent asset shapes. Otherwise, logical schemas—not merely layouts—must become per-target.

5. **MEDIUM — §§6–8, 10 — re-import replacement and collision semantics are unspecified at the normative output boundary.**  
   `ImportOutput::entry` and `primary` return no error, and the fold does not say what happens to prior content entries absent from the new output. If a glTF stops containing `mesh/arm`, an implementation could preserve the old entry indefinitely; duplicate `entry("mesh/arm", …)` calls could also become last-write-wins. Either behavior silently leaves the authored/query namespace different from the importer’s result.  
   **Fix direction:** Define importer-produced content as a total replacement set, preserving daemon-owned settings/record entries and UUIDs only for matching returned local IDs. Make duplicate local IDs and conflicting primary declarations explicit `ImportOutput` errors, and specify how a disappeared prior primary is handled.

6. **MEDIUM — §§8, 10, 13, 18 — the physical file index cannot represent cross-root ambiguity as declared.**  
   Multiple roots form one logical namespace and duplicate root-relative paths must be ambiguity errors, but `files` is described as `path → …`, and bundle paths likewise omit the root identity. If both `main` and `engine` contain `shaders/common.bundle`, a singular row cannot retain both observations to report ambiguity; an overwrite would silently choose one root and corrupt reads, listings, or path resolution.  
   **Fix direction:** Key physical tracking by `(root_id, normalized_path)`, then derive the merged logical path index as a multimap with explicit `Missing / Unique / Ambiguous` states. `FileDep` may remain root-relative, but its evaluation must observe transitions into and out of ambiguity.

7. **LOW — §§9, 13 — singular DSSI indexing contradicts monotone, input-basis memo semantics.**  
   Two snapshots can share identical `StaticInputs` while having different query or path results, yielding distinct valid traces and output tables under one DSSI digest. The declared `digest → trace + output table` index and recovery’s last-winner mapping discard all but one, despite §13 calling memos monotone and stating completed results are never discarded. Trace revalidation preserves correctness, but old and new snapshots can repeatedly rebuild and overwrite one another, especially after recovery.  
   **Fix direction:** Store a bucket of result candidates per DSSI digest, secondarily identified by a trace/result digest, and revalidate candidates on lookup; or explicitly weaken the monotonicity claim and specify replacement/cache-thrashing semantics.

Remaining CRITICAL issues: none. Remaining HIGH issues: 2.
tokens used
