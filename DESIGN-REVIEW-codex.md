The design has several foundational gaps that should be resolved before implementation. The largest are not tuning risks: target-specific Rust layout, type-changing identity, migration representation, and cache purity currently make correctness impossible to establish.

## Critical findings

1. **One schema cannot serve multiple build targets (§5, §9, §12, §16, §18).**

   `ctx.target` makes processing and packs target-specific, but configuration provides one `schema_path`, and artifacts are encoded using that schema’s Rust offsets. Both logical shape and layout can change with target `cfg`, features, profile, pointer width, endianness, and backend selection.

   The current source-walk defaults to the host target and contains 64-bit hard-coded fallback layouts for `Vec`, pointers, `HashMap`, etc. ([source-walk/main.rs](</Users/karl/Projects/newgameplus/source-walk/src/main.rs:1204>)). A macOS daemon cannot safely encode a Windows, 32-bit, or differently-featured artifact from this schema.

   The format must either:

   - use a target-independent wire representation and generated target-side decoding, or
   - produce schemas keyed by the complete compilation identity: target triple, rustc, features, profile/cfgs, dependency versions, and asset-types source hash.

   `target` alone is not a sufficient cache-key component.

2. **The binary artifact design is incompatible with safe generic Rust loading (§5, §12, §15).**

   §5 says the layout hash enables a “zero-copy fast path,” while §12 says loading copies the fixed section and reconstructs containers. Those are different designs.

   A serialized `Vec`, `String`, `HashMap`, `Box`, or `Arc` cannot safely occupy its normal Rust field layout using a `VarRef`. A generic loader must construct valid owned values, account for allocator provenance, initialize padding, unwind safely after partial failure, and preserve or explicitly discard `Arc` aliasing. A `HashMap` is especially not reconstructible from a generic pointer-shaped placeholder.

   Missing pieces include:

   - a precise wire type for every serializable kind;
   - generated or compiled constructors/destructors;
   - bounds, overflow, alignment, allocation, and recursion checks;
   - behavior when layout hashes differ;
   - artifact format version, byte order, pointer-width rules, and integrity checks.

   If the loader constructs normal Rust values, it is not zero-copy. If it casts bytes to Rust values, it is unsound.

3. **Type-changing processing breaks the stated identity and typing model (§4, §7, §9, §13, §15, §16).**

   The same UUID begins as `ShaderSource` and ends as `CookedShaderPackage`, but the design does not define which type that UUID has in:

   - `AssetRef<T>` validation;
   - the `assets` table;
   - RPC metadata;
   - pack manifests;
   - the loader’s typed handle/storage;
   - processor dependencies such as `ctx.read::<T>`;
   - editor queries by type.

   This becomes worse if processor registration changes the terminal type during hot reload. “Processing is invisible to the runtime” cannot coexist with a runtime that needs the output type UUID and schema.

   The processor graph itself is also unspecified: selection among multiple processors for one input type, chaining, terminal-output selection, target-specific alternatives, optional processors, versioning, and registration conflicts.

   A build profile needs an explicit mapping such as `(source UUID, target, pipeline configuration) → terminal artifact type/schema`, distinct from authored type identity.

4. **§11 cannot reuse the existing migration plan for JSON migration.**

   Current `ngp-schema::FieldOp` consists of offsets, sizes, allocation/drop operations, and generated default calls; it contains no source or destination field paths ([migrate.rs](</Users/karl/Projects/newgameplus/ngp-schema/src/migrate.rs:46>)). The plan builder first matches fields by identifier but then discards those identifiers when producing the plan.

   Therefore a disk executor cannot apply “the same plan” by JSON field name. Moreover, the current root matcher uses `(crate, type name)`, not the stable asset type UUID ([migrate.rs](</Users/karl/Projects/newgameplus/ngp-schema/src/migrate.rs:482>)). A Rust type rename contradicts §4’s promise that the type UUID survives refactors.

   This needs a separate logical migration IR containing stable field/variant paths and value-level operations. The offset plan should be compiled from that logical plan for in-memory migration, not treated as its source representation.

5. **Migration degradation explicitly loses authored content (§2, §6, §11).**

   §11 says unknown fields are dropped, then claims deleting schema snapshots can “never” lose content that cannot be rebuilt. Unknown fields are authored values and may be precisely the renamed data for which the old schema was required. Once code has moved on, the historical schema is not regenerable from current source-walk output.

   Schema snapshots are therefore not disposable machine fields. They are historical source data necessary to interpret authored content. Missing snapshots should produce a blocking diagnostic or require an explicit lossy-recovery command with backup/diff, never automatic degradation.

   Related ambiguity: with `auto_migrate = false`, §8 and §12 do not say whether an old-schema bundle can build. Encoding it with the “current logical hash” would be false; retaining the old hash makes it unloadable by the current game.

6. **The cache keys do not uniquely describe processor inputs (§8–§10).**

   `sorted dep CKeys` loses the association between a dependency and the read that produced it. If processor inputs A and B exchange contents, the multiset can remain identical while `f(A, B)` changes.

   Each dependency trace needs to include at least operation identity, asset UUID, resolved type, CKey, and lookup/query identity. Query issues include:

   - `ctx.query` returns `Vec`, but the recorded result is sorted. A processor can observe nondeterministic enumeration order while receiving the same cache key.
   - “Query broad and filter after reading” is correct only if every member whose content can affect filtering is actually read; the API does not enforce this.
   - canonical encoding and ordering of resolution/query traces are unspecified.
   - whole-bundle bytes in the import key invalidate every entry when an unrelated entry changes.

   Queries should return a canonically ordered collection or an explicitly unordered set, and cache traces must preserve labeled dependency relationships.

7. **The build is not pure merely because impurity is discouraged (§1, §3, §8–§10, §13).**

   Pipeline code is arbitrary native Rust in-process. It can read files, environment variables, clocks, randomness, global mutable state, network state, tool versions, locale, CPU features, or thread scheduling without going through `ProcessContext`.

   `processor id + version` is only sound if versions are automatic hashes of all executable code and relevant tools/configuration. A manually unchanged version silently poisons the cache. The same applies to validators and custom migration functions.

   Practical options are:

   - run extensions in a restricted worker process with capability-only inputs;
   - make the dylib/build/toolchain hash part of every relevant key;
   - include declared environment/config/tool inputs;
   - provide a determinism-check mode that executes twice and compares outputs/dependency traces.

8. **Pinned-snapshot jobs have no stale-result commit protocol (§13, §14, §17).**

   A job can finish after its bundle, dependency mapping, processor registry, schema, or target configuration has changed. The coordinator must compare-and-swap against the exact input generation before making the result current. Merely committing its CKey atomically does not prevent an asset’s current-artifact pointer from regressing.

   A snapshot RPC has a related semantic conflict: requesting a build from snapshot version N produces a result committed at N+1, which the pinned capability cannot observe. The RPC needs an explicit “build against version N and return artifact directly” operation or a returned successor snapshot.

   The design also omits last-known-good behavior. On a failed rebuild, it must specify whether the daemon continues serving the old artifact, marks it stale, removes it, or blocks the entire dependency closure.

9. **SQLite and append-only files cannot be one atomic transaction (§13).**

   “Append + insert one transaction” is not an atomic operation across SQLite and a segment file. Crash cases include:

   - durable segment record with no SQLite row;
   - SQLite row pointing at an unflushed/truncated record;
   - partial tail record;
   - compaction index committed before replacement segments are durable;
   - old segment removed while snapshots or mmap readers still reference it.

   A correct protocol needs record checksums, tail recovery, fsync ordering, commit markers or an intent journal, idempotent index rebuilding, and generation/lease pinning for mapped segments.

   Snapshot consistency also has to cover CAS lifetime. An `Arc` metadata snapshot is not useful if GC evicts or compacts the artifact it references. Long-lived RPC capabilities otherwise pin unbounded metadata, WAL history, and CAS generations.

10. **Dylib reload is unsafe under the stated concurrency model (§3, §9, §13).**

    “Drop references, `dlclose`, reload” is underspecified when work-stealing jobs may be executing module code or retaining values, callbacks, trait objects, errors, allocators, thread-locals, or dependency library state. Cancellation does not prove that code has stopped executing.

    The daemon needs a module epoch with drain/barrier semantics and strict ABI-owned types. Panics, aborts, deadlocks, malformed-input crashes, and toolchain crashes currently take down the entire daemon. Production importers and shader compilers are strong candidates for subprocess isolation even if the registration dylib remains in-process.

## Additional correctness and production gaps

- **Stable logical hashing is undefined (§4–§6).** The design does not define canonical graph traversal, type-ID remapping, generic instantiations, recursive types, crate/path renames, enum ordering, or which attributes enter the hash. Current `Schema` also lacks asset UUID, `skip`, `blob`, and asset-reference metadata ([ngp-schema/lib.rs](</Users/karl/Projects/newgameplus/ngp-schema/src/lib.rs:205>)).

- **Defaults require executable semantics (§4, §5, §11, §12).** Source analysis records only whether a type implements `Default`; it does not know the value. `Default` may execute arbitrary code. “Default skipped fields on load” and disk-migrating new fields therefore require generated functions or explicit schema-level literal defaults.

- **Canonical JSON is not specified enough (§5, §6, §8).** Key ordering alone does not resolve non-finite floats, `-0.0`, Unicode normalization, number spelling, duplicate object keys, arbitrary `HashMap<K,V>` keys, or deterministic `HashMap` iteration. Cyclic/shared `Arc` graphs are also unaddressed.

- **The DSB parser contract is too weak (§6).** Exact total length detects truncation, not corruption. Blob references need overflow-safe bounds, non-overlap/alignment rules, duplicate-region policy, per-record or whole-file integrity, resource limits, and a defined relationship between header `version` and JSON `format_version`. Writes need temp-file, fsync, and atomic-replace semantics.

- **UUID collision and copy semantics are absent (§6, §7, §14).** Copying a bundle file duplicates its bundle and asset UUIDs. Re-import may also produce duplicate local IDs or remove old outputs. The system must distinguish move, copy, merge conflict, and malicious collision without silently choosing one. UUID adoption cannot safely mint an identity if existing UUID references may target the missing value.

- **Derived output creation is underspecified (§7, §9).** A processor needs a typed way to reference sibling outputs before they exist. Output-key normalization and stability are part of identity; renaming a key deletes one UUID and creates another. Registration must reject duplicate keys and UUID collisions.

- **Load cycles are not solved (§9, §12, §15).** Processing cycles may be errors, but ordinary asset references can form runtime cycles. The carried-over loader waits for every load dependency before loading the parent ([loader.rs](</Users/karl/Projects/distill/loader/src/loader.rs:402>)), so a strong A↔B cycle deadlocks. The model needs weak references, cycle prohibition, or SCC-aware allocation/fixup.

- **Hot reload is not graph-atomic (§15, §17).** Per-asset swaps can expose a new parent with old dependencies or vice versa. The existing loader even notes that it does not properly verify dependency versions ([loader.rs](</Users/karl/Projects/distill/loader/src/loader.rs:430>)). Build generations and atomic closure commits are needed.

- **File reconciliation still trusts metadata first (§14).** Startup marks only changed mtime/size/kind dirty, while the later claim says content hashes gate work. Same-size, preserved-mtime changes can be missed unless the startup scan hashes all candidates. Watcher overflow, atomic-save rename patterns, case folding, Unicode normalization, symlink loops, paths escaping roots, and network filesystem behavior are missing.

- **Filesystem authoring is not transactionally atomic (§4, §11, §17).** An RPC rename that rewrites many referencing bundles cannot be atomic through a SQLite transaction. Editor writes also race external editors and Git. Operations need optimistic version preconditions, a filesystem journal, atomic per-file replacement, crash recovery, and partial-commit diagnostics.

- **Importer opaque state contradicts disposability (§2, §8, §13).** If it affects output, deleting `.distill` requires an impure re-import from a weak external source that may no longer exist. If it does not affect output, it should not be persisted. All semantically necessary import state belongs in the managed bundle.

- **Migration graph semantics are unstable (§11).** Automatic diffs effectively create many implicit edges, so “equal-cost,” “filling gaps,” and preference for custom paths need a formal cost model. Function-backed edges can outlive their registered function even though endpoint schemas survive. Migration assets themselves create a bootstrap problem when the `Migration` asset type changes.

- **Automatic migration needs operational safeguards (§11, §14).** Watching transient schema outputs and eagerly rewriting a working tree is dangerous. It needs schema completeness/compilation validation, dry runs, backups or VCS-aware diffs, multi-file journaling, cancellation, and explicit behavior on custom-function failure halfway through a bundle set.

- **Packfile CKey/EKey semantics are incomplete (§16).** “EKey is the encoded stream hash” conflicts with `CKey → EKeys`, which implies per-block EKeys. The format needs block order, compressed and raw lengths/hashes, codec/version/dictionary identity, deterministic zstd settings, archive headers, manifest integrity/signing, atomic patch activation, rollback, interrupted-download recovery, and generation GC. Root-asset selection is not defined anywhere.

- **RPC needs lifecycle and flow-control semantics (§17).** Snapshot leases, maximum pin age, authentication/authorization, protocol negotiation, request limits, streaming artifacts, cancellation, event replay/resume, queue overflow, reconnect behavior, and slow-client backpressure are missing. A single Cap’n Proto task risks head-of-line blocking if artifact payloads pass through it.

- **Build operations need scheduling policy (§9, §13, §15).** Demand deduplication, cancellation, priority between game loads and bulk packs, memory/resource budgets, processor concurrency limits, timeouts, and shutdown recovery are absent.

## Prioritized top 10

1. Replace host `repr(Rust)` artifact encoding with a safe portable wire format, or make schemas rigorously target/build-specific.
2. Define authored type identity versus terminal processed type, including processor graph selection and typed-reference rules.
3. Introduce a logical, field-addressed migration IR; do not reuse offset-based `ngp-schema::FieldOp` for disk migration.
4. Make cache inputs complete and labeled, and address arbitrary dylib impurity with automatic code/tool/config hashes or isolation.
5. Specify stale-job compare-and-swap, last-known-good serving, and graph-generation semantics for builds and hot reload.
6. Design a crash-safe CAS/SQLite commit and compaction protocol with snapshot/reader pinning.
7. Remove lossy automatic schema degradation; treat historical schema snapshots as essential authored provenance.
8. Fully specify deterministic serialization, query ordering, dependency traces, and logical-schema canonical hashing.
9. Define dylib reload barriers and failure isolation before executing pipeline code concurrently.
10. Close production lifecycle gaps: load cycles, atomic authoring/migration, UUID collisions, watcher overflow, RPC leases/replay, and signed atomic pack patches.
