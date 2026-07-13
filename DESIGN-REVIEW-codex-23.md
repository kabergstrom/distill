# Adversarial design review — Round 23

> Review provenance: the external GPT-5.6-sol CLI invocation was denied by the
> workspace privacy guard even after user authorization. This report is the
> permitted in-app adversarial subagent review using the same Round 23 brief.

No CRITICAL findings. Nine HIGH, six MEDIUM, and one LOW finding follow.

## HIGH

### §§4–5, 7, 9 — Generic `#[asset]` roots have no representable identity

The design permits generic `TypeDef`s and defines logical hashing by monomorphization, but `AssetType` exposes one const `TYPE_UUID`, while `AssetEntry`, `AssetRef`, `OutputDecls`, and registry/measured-layout/default tables are single-valued by `TypeUuid`. `Foo<u32>` and `Foo<String>` therefore alias before schema/layout selection, and even one instance has no generic-argument carrier. Reject generic `#[asset]` roots at macro/extraction time while continuing to allow concrete nested generic field types, or carry a per-instantiation identity through every surface.

### §§3–5, 12 — Module attestation misses layout-equivalent semantic divergence

`CompilationIdentity` admits undeclared build-script/environment inputs, while `measured_layouts` exports only `(TypeUuid, DSNL)`. DSNL omits rev, tag/build-only attributes, and reference strength/target, all of which can change while native layout stays identical. Export and compare a compiled full registry projection per type—identity, logical hash, DSNL, and policy/attribute bits—before registration.

### §§3–4, 9 — Panic containment is only one-way across a bidirectional ABI

Module wrappers catch module-originated panics, but module code invokes daemon-owned vtables/callbacks, most explicitly `EncodeSink` and likewise registry/process-context plumbing. Those methods have no host-side catch/status boundary, so a host panic can unwind into the dylib before the module wrapper catches it. Make every reverse callback a host-contained no-unwind/status thunk and propagate failures explicitly; audit both directions of every function/vtable crossing.

### §§2, 6, 11, 13 — Ordered lineage loses an epoch that never wrote a bundle

If A is stamped, B is staged but writes no authored data, state is deleted, C is accepted and writes `[A,C]`, then checking out historical B finds B absent and misclassifies it as a successor. Bundle-carried lists cannot record registry epochs that never touched data, so the claimed state-loss-safe direction proof is false. Persist a source-controlled per-type lineage manifest or require an explicit durable acceptance record for every unseen current digest; absence after rebuild must never imply forward direction.

### §§11, 13 — Explicit rollback cannot make a non-head schema current

Staging rejects a digest already present as a non-head entry, while the text says a deliberate reversal is enabled by a reverse custom edge. The edge can migrate values, but no rule lets the candidate epoch containing its target schema open. Represent append-only history separately from a current cursor and allow a non-head cursor only after reverse-edge coverage validation, or require reversal to mint a new head digest.

### §§13, 20 — Rust binding codegen repeats the watched-import lost-wakeup race

Codegen runs asynchronously under a recorded trace and then installs output, but unlike watched imports it has no attempted-basis revalidation at publication. A source/query change consumed while the run is in flight—especially a first run or newly discovered dependency—can be missed before the stale trace installs. Return the complete outcome-bearing trace and basis, revalidate immediately before exchange/trace installation, and discard/re-enqueue on mismatch.

### §§4, 9, 15 — Placeholders can introduce untracked strong load edges

A `PlaceholderThunk` returns arbitrary `T` as `ErasedValue`, while component construction expands only artifact `load_deps`; the value is never visited by the descriptor encoder. `AssetRef` fields inside a placeholder are therefore neither resolved nor added to the candidate union graph. Either prohibit strong refs in placeholder-capable schemas or visit each minted value, resolve/type-check emitted refs under the sweep basis, and expand/recheck the component before update.

### §§15, 17 — `ReconnectRequired` is not uniformly encodable

Prose requires every stale target/policy-bound method to return typed `ReconnectRequired`, but the declared RPC/loader result surfaces do not uniformly carry that arm, and the loader completion grammar lacks a typed reconnect completion. Define a common result envelope with success, `ReconnectRequired(reason)`, configuration poison, and lease/error arms for every fenced method, plus a typed `LoaderIO` event that drives reconnect.

### §16 — Archive activation lacks an immutable physical-name rule

`archive_refs` carries only `(generation, file_hash)`; unlike the hash-named manifest, no archive filename grammar or no-replace publication rule is pinned. A generation-named implementation can overwrite an archive still referenced by the old manifest before `pack.current` flips, violating the old-or-new crash guarantee. Pin content-addressed archive filenames, publish them no-replace, verify existing bytes, and fsync before activating the new manifest.

## MEDIUM

### §§4, 10 — UUID and path reference strings are syntactically ambiguous

A string denotes either UUID selector or `bundle_path`, while a one-component path may itself be UUID-shaped. Define tagged/object syntax or a precedence/reservation rule that preserves intent.

### §§13–14, 18 — Global visited-inode aliases are first-visitor-wins

Two symlinks or roots reaching one directory cause the second path not to be traversed, but the alias diagnostic is not required to block publication. Namespace contents can depend on traversal order. Use ancestry for cycles and either index all non-ancestor aliases with deduplicated watches or reject aliases before publishing their rows.

### §§6, 9–10, 16 — Authoring metadata can be selected and shipped as runtime content

`$settings`/`$record`, migration entries, directory rules, and pack definitions lack a normative authoring-only role. Broad queries and pack roots can select them. Add a per-entry authoring-only role excluded from runtime queries and pack roots by default; reserved entries cannot be primary.

### §§3, 6, 11 — `DefaultTable` full-root paths are not finite for recursive assets

Backrefs allow infinitely many root paths such as `children[].children[]…new_field`, but a finite static writer table cannot contain them. Key default writers by finite schema-node identity plus node-local path and make backrefs reuse the target node entry.

### §16 — “Any compliant” zstd frame permits an undeclared dictionary

The pack carries no dictionary table, so a valid RFC 8878 frame with a nonzero dictionary ID need not be decodable. Require self-contained dictionary-free frames with bounded window parameters or carry authenticated dictionaries explicitly.

### §§10, 20 — Valid local IDs can exceed filesystem component limits

Physical generated filenames embed the complete escaped `local_id` plus UUID, so a long/non-ASCII ID can exceed `NAME_MAX` and block batch-wide namespace validation. Make the authoritative filename fixed-size (the global `AssetUuid` is sufficient) and keep any human slug bounded and non-authoritative.

## LOW

### §§5, 9, 17 — Diagnostic fingerprints violate the closed hash-domain rule

`StableFailureFingerprint::Local.detail` and `ConfigurationPoison.reasonHash` are semantic digests without registered domains and are not in the closed byte-identity exception list. Define canonical diagnostic encodings and dedicated domains.
