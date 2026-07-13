# Adversarial design review — Round 24

> Review provenance: the external GPT-5.6-sol CLI is unavailable under the
> workspace privacy guard. This report is the permitted in-app adversarial
> review of the complete folded design.

No CRITICAL findings. Five HIGH and four MEDIUM findings follow.

## HIGH

### §§3, 13 — Failed candidate registration has no safe cleanup state machine

A candidate can fail after `register` has already installed module-owned processor, validator, importer, default, or migration objects—for example on a later duplicate or pipeline-map validation—but `CandidateOpen` explicitly has no usable epoch and cleanup is specified only at a published epoch's drain barrier. Dropping those partial registrations after `dlclose` is use-after-unload, while automatically dropping them before `dlclose` bypasses the design's status-thunk/leak-on-panic rule. Give every opened candidate an unpublished owner token and status-bearing registration arena; on failure, fence it, destroy all installed objects through contained thunks, call `unload`, and `dlclose` only if every cleanup succeeds, otherwise deliberately retain the library forever.

### §§2, 5–6, 11, 13 — Manifest acceptance is not coupled to the live registry cursor

The manifest is declared the sole authority for accepting a schema epoch, yet candidate epoch opening never requires each registry logical hash to equal the manifest's selected digest, and the trailing automatic-diff rule checks only whether the *source* stamp is on the chain from `L.current`. Consequently registry B can publish while the manifest still selects A, after which an A entry passes the ancestry test and is automatically interpreted as B even though B was never accepted; an arbitrary rollback command can likewise select A while the live registry remains B. Make `Ready(PipelineEpoch)` require exact per-type equality between the registry projection and the manifest cursor, represent a mismatch as a stable `SchemaAcceptanceRequired` state, and allow accept/rollback publication only against a stale-base-checked candidate whose registry digest is exactly the requested cursor.

### §§3, 5 — `registry_extras` has no normative encoding

Round 23 makes `registry_extras` part of the pre-registration security/correctness attestation, but defines it only as “canonical bytes” containing tag-marked structural paths and all future excluded facts. It gives no node/path grammar, back-reference rule for recursive schemas, attribute discriminants, ordering, or unknown-field evolution rule, so source-walk and macro-generated module code have no pinned bytes to compare and can either reject equivalent types or attest different projections under one `DSCA`. Replace the opaque byte convention with a versioned `RegistryExtra` grammar (finite schema-node IDs/backrefs, fixed discriminants and ordering), and require extraction to fail when an excluded semantic fact lacks a representable row.

### §§3–5, 15–17 — Consumer attestation drops the new semantic projection

The pipeline-module gate now compares TypeUuid, DSLH, DSNL, `build_only`, and `registry_extras`, and game descriptors expose the same facts, but pack mount, `Root.connect`, and `reattest` still carry only DSNL rows plus the separate `build_only` table. A game with layout-equivalent but logically stale `rev`, reference semantics, tag paths, or a future excluded policy bit therefore passes open/connect and fails only when an affected artifact happens to load (or is not checked at all for an extras-only difference), contradicting the complete-attestation and fail-at-open contracts. Carry the full compiled-type rows and a `DSCA` aggregate through pack headers and RPC attestation, with the existing closure/registered-set coverage rules.

### §§15, 17 — The uniform reconnect result is not uniform in the declared types

The Cap'n Proto `ReconnectReason` has four arms (`targetDefinitionChanged`, `loadPolicyChanged`, `storeInstanceChanged`, `protocolEpochChanged`), while the Rust `ReconnectReason` used by `IoEvent::ReconnectRequired` has only the first two, despite the fold promising the same typed reason end to end. In addition, `FencedCall.success` is an `AnyPointer`; ordinary generated Cap'n Proto bindings cannot provide the claimed method-specific typed success accessor without an additional generated wrapper contract. Define one shared four-arm reason mapping and either generate/require a typed wrapper layer explicitly or use method-specific result structs whose unions share the same four failure arms.

## MEDIUM

### §§6, 10, 15, 17 — Direct UUID resolve bypasses `authoring_only`

The role rule says authoring-only entries are never runtime inputs or references, and query-based paths default to `authoring_only = false`, but `Snapshot.resolve(uuid)` is a direct UUID method with no stated role rejection. A runtime that learns a control UUID through tooling metadata or source files can therefore request its artifact without passing through the guarded query/pack-root path. Require runtime `resolve` and derived closure expansion to return a typed role failure for authoring-only UUIDs; keep any authoring-time value inspection on an explicitly separate tooling API that cannot build, process, or ship the entry.

### §15 — A successful resolve carries two potentially different bases

`ResolveResult::Built` contains an `IoBasis`, and `IoEvent::Resolved` wraps the same result with another `basis: IoBasis`; no invariant requires equality or selects one as authoritative. An IO adapter can therefore emit an old inner basis and a refreshed outer basis (or the reverse), letting two conforming loaders make opposite component-adoption decisions at the exact boundary meant to prohibit mixed cuts. Carry the basis exactly once in the event envelope, or validate byte/content equality and reject a mismatch before the outcome enters a sweep.

### §§5, 9, 13, 17 — `DSLF` and `DSCP` name domains but not complete grammars

The fold assigns domains, but `DSLF` still says only “typed fields of that class” and `DSCP` refers to an undeclared `ConfigurationPoisonCode` registry and “reason facts”; fixed code values, per-code fields, option/enum encodings, and evolution behavior are absent. Persisted failure memo revalidation and RPC poison identity can therefore vary across implementations or daemon upgrades while both claim v1. Publish exhaustive discriminant tables and field grammars (including version/unknown-code behavior), or serialize a declared tagged record type through the already-pinned canonical record codec.

### §16 — Manifest files are the remaining mutable content-addressed object

Archive publication is now explicitly no-replace with existing-byte verification, and `pack.current` uses a no-replace temporary, but activation never gives the hash-named `manifest-<hex>.dpk` file the same rule—it merely says the manifest is fsynced before the pointer. Writing or truncating that final path in place can damage a manifest already named by an active pointer or race another publisher, defeating the otherwise immutable hash namespace. Publish manifests from same-directory no-replace temporaries, verify an existing same-name file hash-and-byte-for-byte, fsync it and the directory, and only then update `pack.current`.
