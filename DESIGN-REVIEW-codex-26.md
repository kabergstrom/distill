# Adversarial design review — Round 26

> Review provenance: the external GPT-5.6-sol CLI is unavailable under the
> workspace privacy guard. This report is the permitted in-app adversarial
> review of the complete folded `DESIGN.md`, including the §22 ledger through
> Round 25.

## Release verdict

**Release-blocked.** This review found **1 CRITICAL, 5 HIGH, 5 MEDIUM, and
0 LOW** issues. The most serious defect is that the new private migration
query can discover mandatory custom edges but the declared control plane
still cannot read or trace those `Migration` values. Several other R25
repairs are locally correct but remain incomplete at their commit, failure,
or wire-carrier boundaries.

The R25 re-audit did confirm three fixes without a new contradiction: DSTS
has a registered, duplicate-rejecting target-set grammar; role ineligibility
has a basis-revalidated role-index outcome; and query plus inspection on an
`AuthoringSnapshot` share one pinned stamp. DSCP's symmetric-pair ordering is
also now explicit, although one lower-level vocabulary ambiguity remains
below.

## CRITICAL

### §§10–11, 13 — Mandatory migration controls can be discovered but cannot be read through the declared private plane

`ControlQuery::MigrationEdges` returns only sorted `AssetUuid`s, but
`ControlSubject` has no `Migration(AssetUuid)` arm and
`ControlSnapshot::read` therefore has no authorized operation that yields the
edge's `to_hash`, endpoint schemas, or `MigrationKind`. The consumer side is
also absent: `LoadInputs` retains per-node `TraceOp::Control` queries and
hashes for **applied** migration bundles, but no collection of
`TraceOp::ControlRead` outcomes for every edge examined, including decode and
plan-validation failures.

There is no safe implementation choice. Reading the UUID through an ordinary
asset path reintroduces the `authoring_only` filter that R25 was meant to
escape. Reading directly from bundle storage bypasses the capability and its
stable read/decode trace. Treating an unreadable edge as absent permits the
trailing automatic diff to bypass a mandatory custom edge; failing closed can
still persist a deterministic plan/decode failure that never heals when the
edge bundle changes but its `(type_uuid, from_hash)` query membership does
not.

Add `ControlSubject::Migration(AssetUuid)` and a typed decoded migration
result. Every queried edge that is inspected must append a
`ControlRead`—success or stable failure—to the attempted basis **before**
decode/validation can fail, and `LoadInputs` plus failure records must retain
and revalidate those reads. A failed control read must hard-stop planning; it
must never be interpreted as an empty edge set.

## HIGH

### §§6, 11, 13 — Retirement is a point-in-time check, not a maintained authority invariant

Retirement checks that the candidate omits the type and that no live authored
entry or migration endpoint currently requires it, but the declared stale
preconditions cover the manifest/cursors/candidate, not the exact
`ControlSnapshot`/metadata basis used for that negative proof. More
fundamentally, no scan, import, CRUD, or manual-filesystem publication rule
rejects a later entry or migration endpoint that names an already `Retired`
type.

A checkout or authoring write can therefore introduce live data after the
retirement transaction. `Ready` still succeeds because it deliberately
ignores Retired rows, while the live entry has no compiled descriptor or
executable migration authority. This breaks the very condition retirement
proved and leaves a nominally Ready version containing unbuildable authority.

Make `Retired(type) => no live entry and no live migration endpoint for type`
a publication invariant, not merely a command precondition. CAS retirement
against the exact metadata/control `SnapshotStamp` used for the blocker scan,
and make every scanner/authoring boundary publish a typed
`RetiredTypeReferenced` poison or explicit reactivation-required state if
current bytes violate it. Reactivation may admit those bytes only in the same
stale-base checked transaction that restores Active authority.

### §3 — The registration ingress guard is itself fallible, and rejected registrations can be ignored

The host promises to install an arena cleanup record before any fallible
action, yet `CandidateRegistrationArena.entries` is a `Vec`. Moving the first
`CandidateRegistration` into that vector can allocate or fail before the
record exists, after ownership has transferred and while automatic payload
drop is suppressed. The prose also places the host panic boundary only after
guard installation, so an insertion failure occurs in precisely the
uncontained interval the capsule was intended to close.

Separately, `Registry` now returns `RegistrationStatus`, but the type is not
declared `must_use` and the candidate-open protocol does not say that the host
latches every rejected status. Module registration code can ignore a
duplicate/rejection and return `Ok(())`; without a host latch, the candidate
can publish with only the first registration even though the design says any
duplicate prevents opening.

Create an allocation-free callback-local ingress guard by a pure move at the
host thunk's first instruction, inside the host panic boundary. Transfer it
to pre-reserved/intrusive arena storage only after fallible capacity handling
has a defined leak/cleanup result, or declare an exact preflight registration
count and reserve before invoking module code. The host must also latch the
first non-successful registration result and fail candidate publication
regardless of what `register` returns; `RegistrationStatus` should be
`must_use` for diagnostics, not the sole enforcement mechanism.

### §§15, 17 — Reattestation CAS ignores the target/policy generations used during validation

R25 correctly added a Hub-local `attestation_generation`, but installation
CASes only that counter. A request can validate against a pinned daemon
snapshot at target/policy generation N, a configuration or load-policy commit
can advance to N+1, and the old validation can still win the
`attestation_generation` CAS and return success. `reattest` is itself a
target-bound Hub method, so that outcome directly contradicts the rule that
every such method returns `ReconnectRequired` after the generation-changing
commit. It can also unblock the client on a basis the server has already
fenced.

Capture the Hub's complete fence tuple with the validation snapshot and CAS
installation against `(attestation_generation, target_generation,
policy_generation, protocol/store epoch)`. A target or policy mismatch must
return the corresponding `ReconnectRequired` arm without mutating
attestation state; only an unchanged tuple may install and echo the successor.

### §17 — `Hub.reattest` cannot return the attestation failures its contract promises

`Root.connect` has a dedicated `ConnectResult` with an
`AttestationFailure` arm, while `Hub.reattest` returns generic
`UInt64Call`. The latter contains only success, reconnect, configuration
poison, lease failure, and ordinary `RpcError`, despite the method's explicit
promise to repeat connect's complete identity check with the same errors.
Generated bindings therefore cannot distinguish a row/DSCA/policy
attestation refusal from an infrastructure RPC error.

The existing `AttestationFailure` is incomplete as well: it always requires a
`typeUuid`, but target-definition mismatch, duplicate rows, aggregate DSCA
failure, policy-digest failure, and malformed table framing need not identify
one type. Empty UUID bytes would be an unspecified sentinel.

Define a dedicated `ReattestResult` and `ReattestSuccess` with structured
attestation, stale-base/overflow, reconnect, poison, lease, and RPC arms.
Replace the compulsory `typeUuid` with a typed subject union covering a
specific type, target definition, registry aggregate/table, and policy
projection; pin stable failure codes and required payloads for both connect
and reattest.

### §§6, 8, 10, 16 — The remaining control readers are neither closed nor capability-typed

`ControlSnapshot::read` returns the same unbranded `AuthoredValue` used by
migration functions and artifact encoders. The comment that no artifact,
processor input, or shipping carrier can be produced from this value is not
enforceable: the type system exposes exactly the generic value accepted by
those paths. A coordinator mistake can pass control bytes into a normal value
pipeline despite the claimed capability boundary.

The surface is also incomplete for autonomous consumers. A
`DirectoryImportRules(AssetUuid)` subject can read a rule only after its UUID
is known, but `ControlQuery` has no role-inclusive rule enumeration. Directory
imports must discover all authored rule assets after startup and edits; using
ordinary `AssetQuery { authored_type, authoring_only: None }` returns the
runtime-only set by definition, while `Some(true)` is tooling-only and is not
coordinator authority.

Return a private closed `ControlValue` sum with typed variants for Migration,
PackDefinition, DirectoryImportRules, ImportSettings, and lineage—never raw
`AuthoredValue`. Add coordinator-only, basis-stamped enumeration for every
autonomous control class (at minimum directory rules), and route all readers
through recorded typed decode outcomes. The scanner may populate the index,
but no public query mode or generic value may be its carrier.

## MEDIUM

### §§5, 10, 13 — The tag-annotation epoch is an unregistered semantic hash with no byte grammar

§10 says the live `(type -> tag-field set)` map's hash is the
tag-annotation epoch and stores it as an indexing input, yet §5's supposedly
total domain table contains no domain for it and no section defines version,
row members, field-path encoding, ordering, or duplicate rejection. Two
implementations—or one implementation across a refactor—can hash the same map
differently or different maps compatibly. Because this epoch is what forces
tag re-extraction, an unsound comparison can leave tag queries silently
under-indexed and processors can bake incomplete aggregate assets.

Register a domain such as `DSTA` and define v1 as a TypeUuid-sorted sequence
of rows with a canonical, sorted tag-field-path set and fixed annotation facts.
Recompute it from the same compiled registry projection at candidate staging,
index publication, and revalidation; never persist an opaque implementation
hash.

### §§15, 17, 22 — “Connect success is the sole generation carrier” contradicts successful reattestation

`IoBasis::Rpc` contains `attestation_generation`, and successful reattestation
must advance it from the echoed installed successor before traffic resumes.
The R25 ledger and §17 instead say ConnectSuccess is the sole source of all
RPC-basis generations and RpcIO constructs a basis only from it. Keeping the
connect value mislabels post-reattest outcomes; consuming the reattest echo
violates the stated sole-source rule.

Restrict ConnectSuccess's exclusivity to **initial** basis construction.
Declare `ReattestSuccess.installed_attestation_generation` as the only legal
successor source, atomically rotate RpcIO's active basis/connection epoch from
that record, and discard every outstanding event carrying the previous
attestation generation before unblocking.

### §17 — `AuthoringInspection` is referenced but has no declared success grammar

`AuthoringSnapshot.inspect` and `AuthoringInspectCall` name
`AuthoringInspection`, but neither Cap'n Proto nor Rust declares its fields,
missing/role behavior, value encoding, or relationship to the pinned stamp.
The design's claim that it cannot carry an artifact, dependency token, or pack
root is therefore only prose; independent implementations must invent the
boundary that is supposed to enforce that restriction.

Define the exact metadata/value-only success record and typed missing/role
results. Its value should use the closed authoring/control inspection grammar,
and the schema must explicitly contain no ContentHash, artifact capability,
runtime Snapshot/Hub, dependency token, or root/member carrier.

### §5 — DSLF's code-plus-options arms do not define per-code canonical presence rules

`DslfV1::OutputBinding` and `DslfV1::ImportIntake` combine a fixed code with
several optional fields but never say which fields are required, forbidden, or
what they mean for each code. For example, `ImportIntake::TypeMismatch` has
one ambiguous `type_uuid` rather than expected and observed identities;
`RoleViolation`, `SettingsInvalid`, and `NonCanonicalValue` have no pinned
payload shape. Output-binding type mismatch likewise leaves the
expected/observed/key presence policy implicit.

Two conforming producers can fingerprint the same failure differently, while
distinct failures can collapse to one record. Replace these arms with
per-code payload variants, or normatively enumerate required/absent fields and
expected/observed meanings for every code. Reject noncanonical field
combinations at persistence and RPC decode.

### §§5, 9–10, 13 — New canonical failure records still contain unspecified code/vocabulary bytes

DSCP now orders symmetric operands correctly, but `OwnedPathSide.kind` and
`InvalidPath.key` remain arbitrary strings with no fixed vocabulary. The same
overlap can therefore hash differently depending on whether an implementation
calls a side `asset-root`, `root`, or `assets`. Separately,
`ControlFailureCode` is declared `repr(u16)` while the generic §5 record codec
serializes enums as declaration-order `u8`; the `DSTR` encoding of
`StableFailureFingerprint::Control` never chooses one rule or pins the
numeric discriminants. That undermines R25's promise of canonical control
failure traces.

Replace DSCP's side/key strings with fixed tagged enums and exact per-variant
payloads. For control failures, pin the code width and explicit numeric values,
the `ControlFailureSubject` tags, and sorted/deduplicated `entries` grammar as
part of DSTR v1; Rust `repr` alone is not a serialization specification.

## LOW

No additional LOW findings. The release blockers above should be resolved
before another breadth pass; lower-severity polish would not change the
current verdict.
