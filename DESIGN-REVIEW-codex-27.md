# Adversarial design review — Round 27

> Review provenance: permitted in-app adversarial review of the complete
> folded `DESIGN.md`, including the accepted-finding ledger through Round 26.
> Ledgered issues are not repeated unless their fold introduced a distinct
> defect.

## Release verdict

**Release-blocked.** This review found **0 CRITICAL, 3 HIGH, 6 MEDIUM, and
0 LOW** issues. The most consequential remaining defect is that the RPC
connection fence tracks client reattestation, target, policy, store, and
protocol generations, but not the daemon compiled registry against which the
client was attested. A daemon pipeline/schema rotation can therefore invalidate
the accepted rows without fencing the Hub, and the new Round 26 reattestation
CAS can still install a validation that raced such a rotation.

There are **no CRITICAL findings** in this round.

## HIGH

### §§3, 13, 15, 17–18 — Daemon compiled-registry drift is absent from both the live-connection fence and reattestation CAS

`Root.connect` verifies the client's complete rows/DSCA against the daemon's
target table, but a Hub subsequently fences only target-definition and
load-policy generations; its `attestation_generation` is the client's mutable
reattest counter, not a generation of the daemon table. A daemon module/schema
rotation can change or remove an overlapping `CompiledTypeRow` while leaving
target, policy, store instance, and protocol unchanged, after which the old Hub
continues serving new snapshots; likewise, `reattest` can validate against the
old daemon table and still win its newly widened CAS after that table rotates.
This defeats the complete-attestation/fail-at-open contract and moves a logical,
layout, skip/default, reference, or registry-extras mismatch to artifact load.

Add a daemon-side compiled-attestation generation or, more precisely, a digest
of the daemon rows projected onto the Hub's accepted TypeUuid set. Bind it in
`ConnectSuccess`/`IoBasis`, generation-fence every target-bearing capability
when that projection changes, add an exact reconnect reason, and include it in
the full reattestation validation/install CAS; daemon-only row additions may
leave an existing subset-bound Hub valid if its accepted projection is
unchanged.

### §§13, 17–18 — Poison-safe metadata is unreachable through a fresh RPC bootstrap

The state model promises that snapshot creation/refresh, version inspection,
pure metadata, immutable CAS reads, and authoring inspection remain usable
under configuration or pipeline poison. The only bootstrap, however, is
target-bound `Root.connect`: its success requires current target/compiled/policy
attestation, while configuration poison has a failure-only union arm and a
pipeline-poisoned or `SchemaAcceptanceRequired` version may have no current
attestable `PipelineEpoch` at all. A fresh client—or every client after leases
expire or the daemon restarts into poison—therefore cannot obtain any Hub or
Snapshot with which to use the diagnostic surfaces the design says remain
available.

Declare an unbound metadata/diagnostic bootstrap capability that can mint
metadata and authoring-inspection snapshots without a target or compiled
registry, while keeping build/resolve/authoring mutations unavailable under the
existing state rules. Alternatively define an explicit metadata-only
`ConnectSuccess` mode and its method set; do not attest it against `last_good`,
which the design correctly forbids from standing in as the poisoned version's
code/configuration.

### §§2, 14, 17 — The Windows rename-aside fallback can overwrite a file created in its absence window

The non-Windows exchange protocol makes the displaced inode inspectable, but
the Windows fallback says only “rename target aside, verify, then install.” If
an editor creates a new target after the aside and before installation, a
replace-style install destroys or displaces bytes that were never captured by
the first rename and cannot be recovered after an ordinary overwrite; the
general journal invariant does not name the required primitive or transition
that prevents this second race. This is a concrete silent-data-loss path on a
supported daemon platform, distinct from Round 19's now-folded requirement to
journal before the first rename.

Pin the Windows state machine: installation after the aside must be an atomic
**no-replace** operation. If the target reappeared, preserve the proposed temp,
classify and retain the newly appeared target under a journaled conflict path,
restore the verified pre-image only by another no-replace transition, and stop
or retry; specify crash recovery and directory-fsync points for each state so
no overwrite primitive is a conforming implementation choice.

## MEDIUM

### §§2–3, 5–6, 13 — Bootstrap control types have no defined participation in exact registry authority

The complete compiled projection says it covers every compiled asset type and
even defines `RegistryExtraFact::ControlRole` for built-in controls, while
`Ready` requires exact equality between every compiled TypeUuid and every
Active manifest row. The manifest's own bootstrap TypeUuid is expressly
forbidden from `SchemaLineageManifest.types`, but the document never declares
whether that type (and the other format-version bootstrap controls) is outside
the compiled table/DSCA or is an exception to the Active-row equality gate. If
included, `Ready` is impossible; if silently excluded, two extractors can attest
different type universes while both claim completeness.

Define a closed bootstrap-control TypeUuid set and one projection rule. Either
exclude it normatively from module/client compiled tables and DSCA, or carry it
in DSCA and validate its constant rows through a separate bootstrap-authority
gate while excluding exactly those rows from manifest equality.

### §§5, 13, 18 — DSTS duplicate-target rejection has no DSCP reason variant

DSTS rejects duplicate NFC-normalized target names, and §18 says a target
configuration candidate failing validation publishes a typed configuration
poison. The exhaustive `ConfigurationPoisonCode`/`DscpV1` grammar has
`DuplicateRootName` but no duplicate-target-name arm, so two distinct TOML keys
that normalize to the same target name reach an error that cannot be encoded
without misclassifying it as an unrelated catch-all. Add
`DuplicateTargetName { normalized_name }` with a fixed code, or explicitly
define a general semantic-configuration arm and enumerate which validation
failures map to it.

### §§9, 11 — `LoadInputs` cannot preserve DSTR observation order

DSTR requires one operation sequence in observation order, but `LoadInputs`
splits migration queries, control reads, and capability lookups into three
independent vectors. A real run interleaves them (`query`, edge `read`, function
capability, next `query`, ...), and the split representation cannot reconstruct
that sequence or prove which `Observed::Err` was terminal, despite Round 26's
promise to retain the complete query/read failure prefix. Replace the split
authority with one ordered `Vec<TraceOp>` (derived views may remain), or attach
and validate a unique monotone sequence number to every stored operation.

### §§7, 9, 13 — Version-global poison references an undeclared type and has no exact stable identity

`MetadataSnapshot::poisoned()` returns `Option<&VersionPoison>`, but
`VersionPoison` is never declared, while bundle-scoped trace failures have only
`StableFailureFingerprint::Poisoned { bundle }`. The design therefore does not
pin the subjects/facts of a version-global collision or incomplete-skeleton
poison, how equality/revalidation works, or how RPC/query errors carry the same
stable identity; implementations must fall back to prose or an unrelated
bundle sentinel. Declare `VersionPoison` as a closed tagged record, including
canonical identities for cross-file collisions and global unreadable-path
failures, and define its typed mapping to snapshot/query/RPC failures (plus a
trace fingerprint if such a failure is ever memoized).

### §§10, 17 — Authoring inspection's blob-index value grammar is not pinned or self-describing

`AuthoringValue.canonicalValue` is described simultaneously as canonical §6
JSON and as JSON in which blob leaves become bare `u32` indices. §6's container
JSON uses `{offset,len}` blob references, and no replacement token, escaping
rule, schema walk, or integer-vs-blob disambiguation is defined here; the
inspection returns only `schemaHash`, not the schema tree needed to infer which
numeric leaves are indices. Define a tagged, collision-free blob-reference JSON
form with canonical path/order rules, or use a schema-native recursive value
union; state exact index coverage and deliver or reference the authenticated
schema needed to decode it.

### §17 — Attestation failure codes still admit noncanonical subject/payload combinations

Round 26 pins payload *shapes* for malformed/duplicate/missing/mismatch cases,
but not the complete `(code, subject arm, payload arm)` matrix. For example,
`duplicateType` can name either a specific type or the registry table,
`malformedTable` may concern the compiled or policy table, and
`buildOnlyMismatch` can be paired with either a compiled-row or policy subject;
the current prose rejects “other combinations” without specifying which of
these are canonical. Publish the exhaustive matrix for every
`AttestationFailureCode`, including byte widths for each
`expectedObserved` meaning, and require both connect and reattest decoders to
reject every unlisted tuple.

