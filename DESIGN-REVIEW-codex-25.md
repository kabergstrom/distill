# Adversarial design review — Round 25

> Review provenance: the external GPT-5.6-sol CLI is unavailable under the
> workspace privacy guard. This report is the permitted in-app adversarial
> review of the complete folded design, including the §22 ledger through
> Round 24.

One CRITICAL, four HIGH, and five MEDIUM findings follow.

## CRITICAL

### §§6, 10–11, 13 — `authoring_only` makes every custom migration edge invisible

R24 requires every `Migration` entry to be `authoring_only = true`, while §10 makes an absent role selector mean `false` and permits `Some(true)` only on the explicit tooling RPC; §11 nevertheless discovers outgoing migration edges through an ordinary snapshot `migration_edge` query and records that query in the build trace. The defined query therefore returns an empty edge set, so the mandatory-custom-edge walk can take its trailing automatic diff and silently drop/default data that an authored custom edge was meant to preserve. Add a daemon-internal, capability-typed control-query surface (including a canonical trace form) that may select `Migration` controls only for migration planning/rollback validation, while remaining impossible to construct from `ProcessContext`, authored references, runtime RPC queries, or pack roots; define similarly explicit non-artifact control readers for `PackDefinition`, import rules/settings, and the lineage manifest.

## HIGH

### §§2, 5–6, 11, 13 — Exact manifest equality has no type-retirement transition

`Ready` now requires the manifest and compiled registry to have exactly the same TypeUuid set, but `AcceptedTypeLineage` can only append epochs or move `current`; it has no inactive/retired state and the acceptance commands cannot remove authority while preserving history. Removing an asset type from code consequently leaves an extra manifest row forever and permanently traps every later candidate in `SchemaAcceptanceRequired`. Preserve the lineage row but add an explicit stale-base- and candidate-checked `Active | Retired` state (with rules for live entries and migration endpoints), compare only active authority rows for `Ready`, and define reactivation as a history-preserving explicit transition.

### §§3, 13 — Candidate cleanup still lacks a defined ownership handoff on registration failure

The new arena says it owns installed objects, but the declared `Registry` methods still take module-owned values by value and return only `Result`; they do not say whether a rejected/duplicate/panicking call consumed the value or whether an arena cleanup record existed before the first fallible host action. A failure at that seam can therefore double-drop, drop through uncontained module glue, or leave an object outside the arena to outlive `dlclose`, so the R24 cleanup state machine is not closed at its entry boundary. Define a generated erased registration capsule whose ownership transfers to the host thunk on entry on every outcome, install a guarded cleanup record before duplicate checking or other fallible work, and make every status explicitly consumed so reverse arena cleanup is the only destruction path.

### §§5–6, 13 — `CandidateEpochIdentity.target_set_hash` has no hash grammar or domain

R24 makes `target_set_hash` part of the stale-candidate guard that authorizes manifest mutation and promotion, but no section defines its members, ordering, inclusion of target names/bound identities, version, or domain, and it is absent from §5's supposedly total semantic-hash table. Two candidates with the same dylib and DSCA but different target sets therefore have no interoperable or auditable identity rule, allowing an implementation-defined hash to defeat the exact-candidate check. Add a distinct registered domain (for example `DSTS`) over a versioned, name-sorted list of `(normalized target name, DSTG target-definition hash)` with duplicate rejection, and require both request decoding and commit to recompute it rather than trust caller bytes.

### §§15, 17 — Concurrent `reattest` calls can reinstall an obsolete game epoch

§15 explicitly permits multiple reattestations to be in flight, while `Hub.reattest` supplies a proposed epoch but no base generation and the server-side mutation has no monotonic/CAS rule. If epoch N+1 verifies before a delayed epoch N request completes, the latter may overwrite the Hub's bound compiled rows and make new responses pass under an epoch whose descriptors and thunks the game has already drained. Give each Hub an atomic reattestation generation: requests carry the expected base plus a never-reused successor generation, installation is compare-and-swap and strictly monotone, stale calls cannot mutate state, and success echoes the installed generation that alone may unblock loader traffic.

## MEDIUM

### §§6, 9–10, 15 — `RoleIneligible` has no trace/fingerprint representation

Direct resolve now has a typed `RoleIneligible` result, but `StableFailureFingerprint` has no corresponding arm and `RefCheck` can observe only a terminal type or another existing fingerprint; an exact UUID `AssetQuery` also filters the row to an ordinary miss under the default-false role rule. A build whose strong reference targets an authoring-only entry thus cannot satisfy both the typed-role contract and the rule that deterministic failures memoize and heal when their observed basis changes. Add a canonical role-ineligible fingerprint/outcome keyed by asset and observed role, use it for exact reference/dependency resolution rather than `MissingRef`, and revalidate it against the role index so a role edit wakes the memo.

### §§5, 9, 11, 13 — The claimed exhaustive DSLF grammar cannot encode named local failures

`MigrationFn` returns a local `MigrationError`, and result binding has local errors such as missing/duplicate declared outputs, yet `LocalFailureClass` has only validator, migration-*plan*, and processor-`BuildError` forms; no stable code carrier is declared for those other failures. In addition, `conflicting_edges: Vec<BundleUuid>` cannot identify two conflicting `Migration` entries in one bundle even though bundles may contain multiple assets and diagnostics promise to name the conflicting migration assets. Add stable classes/codes and exact fields for every memoizable local failure source, and identify migration edges by `AssetUuid` (or bundle plus local id), not only their containing bundle.

### §§5–6, 13, 17 — DSCP does not canonically identify all instances of its own variants

`DuplicateLineageManifest` carries bundle UUIDs, so two manifest entries in the same bundle either collapse or violate the general duplicate-rejection rule; `OwnedPathOverlap` and `DirectoryAlias` also leave the two symmetric sides unsorted, allowing the same poison to hash differently by discovery order. Those cases contradict the R24 claim that DSCP v1 is an exhaustive canonical reason grammar. Carry manifest entry identity (`AssetUuid`, or bundle plus local id), sort/deduplicate by that identity, and define a canonical lexicographic ordering for every symmetric pair before hashing.

### §17 — The authoring inspection APIs are live reads outside snapshot consistency

The RPC section says reads flow through snapshot capabilities, but R24 places `authoringQuery` and `inspectAuthoring` directly on `Hub` with neither a snapshot argument nor a returned stamp. A concurrent edit can therefore make a query result and its subsequent inspection refer to different input versions, with no way for tooling to detect or retry the torn read. Put both methods on a dedicated authoring snapshot capability (or require/return one `SnapshotStamp` and reject mismatches) while preserving their non-buildable, non-shippable result types.

### §§15, 17 — `Root.connect` cannot construct the declared RPC basis or return typed attestation failure

`IoBasis::Rpc` requires the server's verified `policy_generation`, but `Root.connect` returns only `(hub, instance)` and no later snapshot call exposes that generation; its raw return also sits outside the method-specific typed result-union discipline used for all bound calls. RpcIO must therefore invent a generation or rely on an unspecified RPC exception for connect-time DSCA/policy/configuration failures. Define a typed `ConnectCall` whose success carries the Hub, store instance, accepted policy generation (and the bound target/attestation generations), and whose failure arms name attestation/configuration/protocol errors; require RpcIO to build its basis only from that success record.
