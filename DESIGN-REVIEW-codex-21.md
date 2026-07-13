I found no CRITICAL defects. I found 10 HIGH defects; several are second-order failures introduced by remedies recorded in the ledger, rather than restatements of ledger findings.

## HIGH

1. **HIGH — §§2, 11, 13 — Disposable schema lineage re-enables backward migration**

   `schema_lineage` contains chronology that cannot be reconstructed from bundles and current code, contradicting §2’s claim that deleting `.distill/` costs only a rebuild. After state loss, “no lineage yet” permits an automatic diff from any embedded schema to current, so newer data opened under rolled-back code can be interpreted or explicitly disk-migrated backward; without state loss, blindly appending `previous → new` when `new` is an existing ancestor similarly turns a rollback into a forward edge or cycle. Fix direction: place directional ancestry in authored/reconstructible schema records, reject ancestor transitions during epoch staging, and require an explicit edge whenever direction is unknown.

2. **HIGH — §§7, 13 — Prior metadata is insufficient to scope malformed-file poison**

   A prior indexed row identifies what the old bytes claimed, not what the current malformed bytes might claim. A malformed edit can introduce a new UUID, type, local ID, or tag before the syntax error, so bundle-scoped poison based on the prior row lets corresponding queries return `Missing` or incomplete results instead of failing conservatively. Fix direction: permit bundle-scoped poison only when the current bytes expose a fully validated, complete namespace skeleton; otherwise use version-global poison regardless of prior metadata.

3. **HIGH — §§8, 13, 14 — Watched-import dependency installation has a lost-wakeup race**

   A watched import can read source bytes, then have the source-change event processed before its newly discovered read-set is committed. If the stale import result commits afterward, that event is already consumed and no future invalidation is guaranteed, leaving the watched bundle stale indefinitely. Fix direction: revalidate the complete read-set against the coordinator’s current raw-file index immediately before publication, atomically installing the dependencies only if they still match; otherwise retry or enqueue an invalidated attempted basis.

4. **HIGH — §§8, 10, 11, 13 — Failed pipeline capability lookups do not depend on the pipeline**

   `LoadInputs.dylib_hash` is present only when pipeline code ran, but a missing `MigrationFn` or default-table entry fails before code runs. Such a build failure or tag-poison result can therefore be memoized without any module observation; adding the missing registration leaves the DSBI key and trace valid, so the failure never heals until unrelated input changes or eviction. Fix direction: record the module/dylib and requested capability before every lookup, including misses, or conservatively key every schema-dependent load on the pipeline epoch.

5. **HIGH — §§2, 14, 17 — Whole-bundle deletion bypasses the concurrent-write protocol**

   §14 defines safe replacement but no equivalent protocol for Asset CRUD deletion of an entire bundle. An external edit landing after the RPC base-version check but before unlink can be destroyed silently, precisely the TOCTOU that exchange-and-verify prevents for rewrites. Fix direction: implement deletion as a journaled rename into a unique same-filesystem quarantine, verify the displaced bytes against the expected pre-image, and restore on mismatch.

6. **HIGH — §§2, 14, 18, 20 — The quarantine scheme cannot preserve every displaced inode**

   Naming quarantine solely as `.distill/displaced/<content-hash>` aliases distinct equal-content inodes, although each may have a different open writer; placing it under `state_path` also fails across filesystems, where copying loses the live inode. Conflict swap-back additionally treats the briefly exposed proposed inode as safely deletable “daemon bytes,” even though another process may already hold it open, and finite retention cannot support the unconditional “no edit is ever silently lost” claim. Fix direction: give every inode ever exposed at the target a unique intent ID and same-filesystem quarantine location, including swap-back objects, with any central journal referring to that physical location.

7. **HIGH — §§4, 5, 9, 15, 16 — Pack attestation cannot enforce `build_only` policy**

   Pack mount and reattestation compare target identity and DSNL layout data, while `build_only` is deliberately absent from both DSLH and DSNL; it is also absent from `AssetRuntimeDescriptor` and the pack header. Consequently PackfileIO cannot perform §15’s promised load-policy validation or detect a same-identity/same-layout policy divergence caused by an undeclared build input—the exact class measured attestation is meant to backstop. Fix direction: expose the full relevant registry projection from descriptors and carry a versioned `(TypeUuid, logical hash, build_only)` table/digest in packs and reattestation.

8. **HIGH — §§15, 17, 18 — Target-definition invalidation is advisory rather than a capability fence**

   An old Hub learns about a target change only through an asynchronous subscription event, but it may have no subscription or may call `Hub.snapshot`/`Snapshot.refresh` before consuming the event. Those methods have no defined reconnect result, leaving the server to either serve a snapshot under an unattested definition or fail through an unspecified RPC exception. Fix direction: generation-fence every target-bound capability operation server-side and return a defined `ReconnectRequired` outcome—or revoke the capability in a way RpcIO must map to reconnection—before any post-change answer is issued.

9. **HIGH — §§3, 4, 15 — Game-side ownership bypasses the no-unwind module boundary**

   `AssetStorage` owns `Box<dyn Any>`, whose destructor is raw module vtable drop glue, while `register_placeholder` accepts `fn() -> T` even though the text promises an inside-module panic wrapper that can report failure. Neither signature can catch a panic inside the module and return an error, so a panicking `Drop` or placeholder can unwind across the unloadable-module boundary or abort. Fix direction: replace raw boxes and factories with epoch-bound type-erased owners using explicit generated `Result`/status thunks for construction and destruction, with automatic Rust drop suppressed across the boundary.

10. **HIGH — §16 — The purported normative pack grammar is not implementable interoperably**

    The spec does not define exact encodings for the manifest header’s `Target`, archive references, table rows, optional tables, or archive block/blob records; it merely says block records carry lengths and CRCs. It also leaves unclear whether EKey covers a zstd frame/extent alone or its record framing, so two conforming implementations can produce mutually unreadable files and disagree on authentication. Fix direction: publish byte-for-byte layouts for every header and row, including tags, widths, framing, ordering, padding, hash scope, and archive record grammar.

## MEDIUM

11. **MEDIUM — §15 — Deletion has no component-level adoption semantics**

    The component algorithm freezes on build failure or missing children, while deletion independently marks a dependency `Dead` and may replace it with a placeholder. It never specifies whether reverse dependents remain frozen on the corpse, adopt with the placeholder, or transition atomically with the deleted node. Fix direction: define deletion and restoration as explicit component transitions, including placeholder behavior and rollback rules.

12. **MEDIUM — §§2, 8, 13 — Directory-import ownership is not reconstructible**

    Generated bundles carry importer, sources, settings, and read-set, but no rules-bundle UUID, rule identity, or group key. After daemon-state loss, the daemon cannot distinguish directory-generated output from an equivalent explicit import or reliably reconstruct orphan ownership. Fix direction: persist an optional directory-origin record in the bundle’s reserved metadata.

13. **MEDIUM — §§9, 13 — Deterministic local failures cannot satisfy the declared trace grammar**

    Validator diagnostics, migration-plan validation errors, and a processor returning `BuildError` need not arise from a context operation, yet failure records are required to end in a `TraceOp` containing `Observed::Err`. Implementers must either invent incompatible synthetic operations or decline to memoize failures the spec says are memoized. Fix direction: define a canonical terminal `FailureCause` outside the dependency trace, or add a specified local-failure trace variant.

14. **MEDIUM — §§13, 18 — Restart-only configuration edits have no runtime transition**

    The general rule says configuration edits stage and publish configuration epochs or poison, while the change table says `state_path`, address, and codegen settings take effect only at restart. It is undefined whether a valid restart-only edit advances the input version with unapplied values, is ignored, or poisons the running daemon. Fix direction: define a `RestartRequired` state/event and keep the active configuration unchanged until restart.

15. **MEDIUM — §§4, 5, 11 — Enum-variant revisions have no declared source-model carrier**

    The logical grammar and migration rules assign `rev` to enum variants, but the attribute table permits `#[asset(rev)]` only on structs or fields and the model declares no `VariantAttrs`. Different extractors can therefore emit zero, inherit another revision, or accept an undeclared variant attribute. Fix direction: explicitly add variant scope and a variant attribute record, or remove variant-level revision from the grammar.

16. **MEDIUM — §§10, 14, 20 — Shader-codegen filename and Rust-identifier mapping is unspecified**

    `<pipeline>.rs` is derived from an unspecified authored identifier, while local IDs may contain path separators and arbitrary Unicode. Raw interpolation permits traversal or invalid/colliding Rust module names; merely applying path rejection would make legitimate asset identifiers uncodegenable. Fix direction: define an injective filename/module-identifier encoding, use descriptor-relative no-follow writes, and escape all generated Rust strings and identifiers.

17. **MEDIUM — §§13, 15, 16 — Unbounded blobs conflict with bounded CAS and fetch resources**

    Blobs are explicitly unbounded, but CAS segments have a size cap and RpcIO says in-flight fetch memory is bounded without defining oversized-record or single-fetch admission. A valid artifact larger than either cap can therefore be rejected or permanently backpressured by one implementation and accepted by another. Fix direction: specify oversize segments plus streaming/spooling or a one-oversized-request admission rule.

18. **MEDIUM — §§13, 16 — Pack manifest metadata is not required to match artifact headers**

    The manifest duplicates authored type, terminal type, logical hash, and load dependencies from each artifact, but no normative cross-check is stated. A generator defect can therefore produce a fully hash-valid pack whose closure omits a strong dependency or whose manifest advertises the wrong type. Fix direction: require pack construction and loading to compare every duplicated field against the ContentHash-verified artifact header, or remove duplicated authoritative fields where possible.

19. **MEDIUM — §13 — Strict priority permits permanent batch starvation**

    Interactive jobs are always ahead of pack and doctor work, with FIFO only inside each class. Continuous interactive demand can therefore prevent a pack build or integrity check from ever progressing. Fix direction: use aging, weighted fairness, or reserved batch capacity while retaining low interactive latency.
