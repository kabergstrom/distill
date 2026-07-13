codex
I found 1 CRITICAL, 22 HIGH, and 6 MEDIUM defects not already resolved by the ledger. I excluded the explicitly accepted risks and deferrals.

## CRITICAL

1. **CRITICAL — §§2, 14, 17 — Atomic authored-file replacement has no crash-recovery protocol**

   After the exchange step, the new daemon bytes occupy the target while the displaced user bytes occupy a temporary path; a crash before verification leaves no durable record saying which file must be restored or preserved. Startup cleanup could delete that “temporary” file, while the Windows rename-aside fallback has an equivalent window where the canonical target is absent. Fix by fsyncing a write-intent journal before the first rename, recording target, temp/conflict paths, expected preimage, and proposed hash; startup must reconcile every unfinished intent and never delete an unclassified file.

## HIGH

1. **HIGH — §§6, 8 — Persisted root identity is represented by a process-local integer**

   `ImportRecord` is an ordinary schema-described asset, but `RootedPath.root` and `FileDep::Probe.observed` contain `RootId(u32)` while their normative serialized form is the normalized root name. The generic schema codec will see an integer and cannot reconstruct the promised name-based identity after restart or root reordering. Fix by using a persistent `RootName(String)` in bundle-visible types and converting explicitly to an interned `RootId` only inside daemon metadata.

2. **HIGH — §§3, 5, 13, 15 — A rejected pipeline epoch has no publishable state**

   Module/schema file events advance the input version and are supposed to rotate `PipelineEpoch`, but registration can fail due to identity mismatch, duplicate registration, or a bad module. The design never says whether the old epoch remains authoritative, whether the new version is poisoned, or whether the old epoch is drained; plausible implementations either silently keep obsolete processor code or make retry-refreshed clients loop on `Drifted(Dylib)`. Fix by staging a candidate epoch atomically, retaining the old epoch until validation completes, and publishing a stable pipeline-failure state when the candidate cannot open.

3. **HIGH — §11 — The mandatory-edge walk can migrate past the current schema**

   The algorithm follows outgoing custom edges before checking whether its current node already equals the requested current schema. Thus an old `A→B` edge can transform an entry already at current schema `A`, or a future `B→C` edge can make an `A→B` migration overshoot current `B` and then automatically migrate backward. Fix by testing `node == target_current` before every outgoing-edge query and terminating immediately when true.

4. **HIGH — §§3, 11 — Historical data-driven defaults are not replayable**

   A custom `MigrationKind::Ops` edge may contain `WriteFieldDefault` or `WriteParentDefault`, but execution consults the current module’s `DefaultTable<T>`. If a later schema removes that historical field or changes its default, an old `A→B` edge becomes unexecutable or silently changes meaning. Fix by materializing defaults into literal `AuthoredValue`s when a custom edge is authored; dynamic default-table operations should be restricted to the final automatic diff into the current schema.

5. **HIGH — §11 — Migration operations lack normative transactional semantics**

   The spec does not define whether paths read from the original or evolving value, whether `CopyField` copies or moves, how overlapping writes/drops are rejected, or when function-edge output is validated against `to_hash`. Two executors can therefore produce different authored values, and malformed function output can reach tag extraction before a later encoder notices it. Fix by defining an immutable-input/new-output execution model, path and overlap validation, exact missing-path errors, and mandatory endpoint-schema validation after every edge.

6. **HIGH — §§5, 6, 12 — Blobs make map/set canonicalization circular**

   Sets are ordered by encoded element bytes and map blob paths use encoded key bytes, while a blob’s encoding contains an offset or index assigned from that same final order. A legal set element or map key containing a blob therefore requires knowing the order to compute the bytes used to determine the order. Fix by rejecting blobs anywhere below map keys and set elements, or define a separate blob-independent semantic ordering encoding using blob content hashes.

7. **HIGH — §§4, 9, 10, 12 — References emitted by processors have no invalidation dependency**

   A processor can construct `AssetRef<T>` from any UUID, but result binding only discovers a UUID load-dependency list; no `TraceOp` records the expected terminal type or even requires the UUID to exist. Deletion or retyping can therefore leave a cached processor artifact valid while it contains a now-invalid typed reference, and the sorted load-dependency table has already discarded the expected edge type. Fix by resolving every emitted reference as `(uuid, expected_terminal_type)` during result binding and recording that resolution as a revalidated trace entry.

8. **HIGH — §§12, 13, 15, 17 — Wire trees are promised but have no storage or pinning protocol**

   Section 12 says every encoded wire tree is persisted, yet section 13 defines no SQLite table, CAS record kind, recovery rule, or lease/GC pin for it. A ContentHash can remain fetchable after its producing epoch or layout table disappears while `Hub.wireTree(layoutHash)` can no longer supply the tree needed to load it. Fix by making wire trees first-class content-addressed CAS records pinned and evicted with every referencing result, or embed the authenticated tree in the artifact.

9. **HIGH — §§7, 13 — Bundle-scoped poison is impossible for a newly malformed file**

   A new unreadable bundle exposes no bundle UUID, asset UUIDs, types, tags, or local IDs, so the daemon cannot determine which exact-UUID or type queries “could match” it. Answering those queries from the remaining index can silently under-approximate; failing only path-related queries is not the promised poison semantics. Fix by making such failures conservatively version-global, or introduce a separately parseable outer identity/index envelope that remains available when the body is malformed.

10. **HIGH — §§2, 13, 15 — `Deleted` cannot be reconstructed from disposable state**

    No authored tombstone exists, so after `.distill` loss the daemon cannot distinguish a UUID that was deleted from one that never existed. This makes the normative `ResolveResult::Deleted` versus `Missing` distinction and reconnect behavior unreconstructible. Fix by defining deletion as client-relative—previously resolved plus now absent—or by adding durable authored tombstones; daemon-only tombstones violate §2.

11. **HIGH — §§2, 13, 15, 16 — `InputVersion` is not a valid universal adoption identity**

    The counter is daemon-state-local and can reset or reuse values after state loss, while pack manifests do not carry an `InputVersion` even though `AssetStorage` requires one end to end. A reconnect or remount can consequently alias a new candidate with an old `(handle, version)` storage entry. Fix by using an instance-qualified snapshot stamp for RPC, an immutable manifest identity for packs, and a separate loader-generated `AdoptionId` for storage operations.

12. **HIGH — §§15, 17 — Subscription setup has a lost-update race and no path-subscription RPC**

    `Hub.subscribe` has neither a `since` version nor an atomic snapshot barrier, so a change between initial resolve and subscription installation can be missed permanently. It also accepts only UUIDs even though `LoaderIO::subscribe_path` is required for indirect-handle correctness. Fix with a snapshot/cursor-bound subscription API covering both assets and paths, returning the installation version and an ordered initial delta.

13. **HIGH — §15 — Asynchronous IO completions cannot be correlated safely**

    `Resolved` and `PathResolved` events contain only the UUID or path, while multiple retries, path rebindings, reconnects, and reattestations may be in flight concurrently. A delayed `Missing` or `Failed` response—neither even carries a basis version—can overwrite a later successful result. Fix by including a request generation, connection epoch, and snapshot basis in every command and completion, with stale completions discarded explicitly.

14. **HIGH — §§15–17 — Epoch reattestation omits the target-definition identity**

    `Root.connect` verifies both the target-definition hash and layout registry, but `Hub.reattest` accepts only the layout registry. A successor game-module epoch built from a different source identity or target definition can therefore pass reattestation whenever measured layouts happen to match, contradicting the claim that reattestation repeats the open-time check. Fix by resending and verifying the epoch’s target-definition hash at every RPC and pack reattestation.

15. **HIGH — §§9, 13 — Cross-snapshot coalescing specifies only successful outcomes**

    Successful DSSI jobs have their completed trace revalidated per waiter, but failures have no corresponding trace or basis rule. If snapshot N fails because a path is missing while snapshot M resolves it, an M waiter joined to N’s in-flight job may receive a false failure. Fix by making failed outcomes basis- and trace-bearing with the same per-waiter validation, or restrict failure propagation/coalescing to the originating snapshot.

16. **HIGH — §15 — Drift recovery contradicts component single-version adoption**

    The loader is told to refresh and re-resolve only the “affected subset,” while also requiring every accepted result to belong to one snapshot. Previously successful members remain results from the old basis and cannot simply be relabeled with the refreshed version. Fix by re-resolving or daemon-validating every member of the component against the refreshed snapshot before adoption.

17. **HIGH — §§13, 14 — Startup reconciliation can permanently miss a file state**

    No watcher-before-scan barrier is defined, so a change after a path is scanned but before watcher activation can be absent from both the scan and event stream. The phrase “metadata alone is inconclusive” also fails to state whether equal mtime/size files are rehashed, allowing offline same-metadata replacements to remain stale indefinitely. Fix by arming watchers before scanning, queueing and replaying events across an explicit scan generation, and pinning a conservative content-hash rule.

18. **HIGH — §§9, 13 — Cooperative descendant execution can overflow the native stack**

    Every unclaimed descendant runs inline on the caller’s stack, but no build-graph recursion cap exists; the fixup recursion cap in §12 is unrelated. A deeply acyclic authored dependency chain can therefore abort the daemon with stack overflow on every resolve. Fix with an iterative/trampolined scheduler or a configured dependency-depth limit that returns a named build error.

19. **HIGH — §16 — `pack.current` is not a complete crash-consistent bootstrap**

    It contains a “manifest EKey,” but EKey is defined only for encoded blocks/extents and no deterministic filename or pre-manifest index location is specified; locating the index through the manifest is circular. Activation also does not require every newly referenced archive/index and containing directory to be durable before the pointer swap. Fix by pinning the manifest hash grammar and filename, then fsyncing all referenced files and directories before atomically replacing and directory-fsyncing `pack.current`.

20. **HIGH — §12 — Enum discriminants are positionally detached from variant names**

    `NativeTagEncoding::Direct.values` is in declaration order, while `NativeLayoutNode::Enum.variants` has no declared order or declaration index and wire tables later require name-sorted order. An implementation cannot unambiguously associate a raw discriminant with its variant, risking wrong variant construction. Fix by storing each variant as one record containing name, declaration index, payload node, and direct/niche tag information.

21. **HIGH — §§4, 5, 12, 16, 17 — The measured native-layout digest has no normative grammar**

    The only fully specified DSWL grammar hashes the wire tree, while `layout_digest` is said to hash native offsets, skip slots, and tag encodings “in DSWL node form.” There is no separate domain, projection rule, or exact treatment of binary-local table IDs, so source-walk and generated descriptors can implement incompatible—or incomplete—memory-safety attestations. Fix by defining a versioned native-layout grammar, e.g. `DSNL`, covering every native node and skip slot while explicitly excluding table assignments.

22. **HIGH — §§10, 14 — Symlink support defeats lexical containment and can prevent scan termination**

    The scanner intentionally follows symlinked directories, but a scan-time “does not escape” check is vulnerable to retargeting before open, and no visited-directory identity rule prevents in-root symlink cycles. This can read outside the configured namespace or make startup reconciliation recurse indefinitely. Fix with descriptor-relative beneath-root opens, symlink identity revalidation, and a visited `(device, inode)` set with explicit alias/cycle diagnostics.

## MEDIUM

1. **MEDIUM — §6 — “Canonical JSON” leaves byte-significant choices unspecified**

   Whitespace, separator formatting, final newline, and complete integer/exponent formatting are not pinned or delegated to a named canonicalization standard. Because whole-file bytes enter hashes and watched-import fixpoints, independent writers can produce different bytes for the same bundle. Fix by adopting a named standard such as RFC 8785 with explicit deviations, or specify the remaining byte grammar directly.

2. **MEDIUM — §§8, 13, 15 — Failure quiescence has no attempted-basis record**

   A watched import that fails retains an invalid old read-set, while deterministic build failures have no memo record in the CAS. Implementations may retry continuously, suppress retries forever, or rebuild once per client at an unchanged basis. Fix by recording failed attempts and their observed dependency basis, distinguishing deterministic build failures from transient infrastructure errors.

3. **MEDIUM — §16 — The optional pack path table has no byte or ambiguity grammar**

   `include_path_table` materially changes `PackfileIO::resolve_path`, but the table is absent from the declared tables, sort rules, manifest coverage, and patch behavior. Fix by specifying its exact key/value encoding, closure coverage, ambiguity handling, hashing, and authentication.

4. **MEDIUM — §16 — Per-definition pack-byte reproducibility is not actually pinned**

   The design permits any compliant zstd encoder but promises identical archive bytes from one `PackDefinition`; `zstd_level` alone does not fix library version, strategy, checksum flags, or other encoder parameters. Fix by pinning the complete encoder identity/settings or narrowing the promise to decoded-content reproducibility.

5. **MEDIUM — §20 — Generated Rust files bypass the safe publication protocol**

   The codegen service diffs and writes `.rs` and `mod.rs` files, but §14’s exchange-and-verify protocol does not include these writes or define whether the directory is exclusively daemon-owned. A concurrent human or build-tool edit can be overwritten by a check-then-write implementation. Fix by declaring ownership explicitly and applying the same preimage-verified atomic publication and crash recovery to generated source files.

6. **MEDIUM — §§13, 18 — Configuration changes have no snapshot semantics**

   Asset roots and target definitions affect namespace resolution, target hashes, and pipeline maps, but the design does not say whether configuration is live-watched, restart-only, or an input-version event. Existing snapshots and target-bound Hubs can therefore observe incompatible behavior after a config edit. Fix by declaring configuration restart-only or publishing validated configuration epochs through the same input-version mechanism.
