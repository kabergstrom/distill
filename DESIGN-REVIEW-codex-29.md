# Adversarial design review — Round 29

> Review provenance: permitted in-app adversarial review of the complete
> folded `DESIGN.md`, including the accepted-finding ledger through Round 28.
> Ledgered issues are not repeated unless their fold introduced a distinct
> defect.

## Release verdict

**Release-blocked.** This review found **0 CRITICAL, 3 HIGH, 4 MEDIUM, and
0 LOW** issues. There are no Critical findings, but the RPC basis bootstrap,
lineage-manifest recovery path, and lossless physical-path poison grammar each
remain incomplete.

## HIGH

### §§15, 17 — `ConnectSuccess` cannot construct the required initial RPC basis

`IoBasis::Rpc` requires verified load-policy rows and a digest, and the text
explicitly requires RpcIO to obtain its initial basis only from
`ConnectSuccess`, never from values it sent. The actual `ConnectSuccess` wire
struct carries generations and the daemon projection but no `loadPolicy` or
`policyDigest`, making the required initial basis unconstructable. Add both
fields to `ConnectSuccess`, populated from the server-recomputed
accepted-set-union-bootstrap projection, and require the same canonical
verification already specified for `ReattestSuccess`.

### §§6, 13, 17 — A missing lineage manifest is irrecoverable through the declared control plane

The only permitted manifest writer requires a base `BundleFileHash`, but the
missing-manifest state has no base hash, prevents `Root.connect` from returning
a Hub, and exposes only a read-only metadata bootstrap. Consequently a fresh
project or deleted manifest cannot be initialized through any specified
capability without bypassing the “only explicit command may rewrite it” and
journaled-swap rules. Define a narrowly scoped bootstrap/repair operation
available under `MissingLineageManifest`, using an absence-aware snapshot CAS
and journaled atomic creation; separately define deterministic
duplicate-manifest repair.

### §§7, 13, 18 — Scan rejection of a non-normalizable physical bundle path has no DSVP representation

Every indexing failure must publish, and scan rejects physical paths that
cannot become valid normalized NFC UTF-8 paths, but every relevant DSVP arm
requires `normalized_path: String`. A Unix filename with invalid UTF-8 or a
Windows filename containing unpaired UTF-16 therefore cannot be represented
without lossy invention or failure outside the closed poison grammar. Add an
`InvalidPhysicalPath` DSVP arm carrying `root_name`, lossless
`PlatformPathBytes`, and a closed reason code, and use that raw identity for
equality and healing.

## MEDIUM

### §§7, 13 — Simultaneous version-global defects have no canonical aggregate or winner

A single filesystem state can contain multiple duplicate-UUID groups, path
collisions, and unreadable files, while each published header can store only
one `VersionPoisonV1`. No total ordering or aggregation rule selects which
defect becomes the canonical wire value, so conforming implementations can
publish different poison identities for identical inputs. Introduce a sorted
`VersionPoisonSetV1`, or specify a complete deterministic winner ordering while
retaining all remaining diagnostics separately.

### §9 — Missing tool registration has no closed stable failure fingerprint

`run_tool` resolves through a snapshot’s `ToolEpoch`, and tool misses must be
outcome-bearing and heal when a registration appears, but `ToolLaunch` permits
only launch-time failures and `CapabilityKey` has no tool arm. An absent
registration therefore cannot produce the required canonical `Observed::Err`
without misclassifying the miss or inventing an unversioned variant. Add
`MissingTool { id }` or `CapabilityKey::Tool(id)` with pinned DSTR encoding and
explicit revalidation behavior.

### §§5, 17 — Configuration-poison wire values omit their canonical DSCP facts

DSCP defines code-specific canonical payloads, including normalized names,
paths, colliding entries, and target identities, but `ConfigurationPoison`
carries only a numeric code, digest, and presentation-only message. Receivers
cannot recompute the digest, validate the code/payload pairing, or obtain the
typed facts without treating presentation text as the forbidden type carrier.
Carry a typed DSCP detail union—or versioned canonical detail bytes—and require
recomputation and rejection of mismatched code/detail/hash tuples.

### §§5, 7 — `IncompleteSkeleton` requires a hash type restricted to canonical bundles

`BundleFileHash` is defined as a hash of canonical bundle bytes, yet
`IncompleteSkeleton` necessarily describes malformed or incomplete bytes and
requires a `ReadableBundleSource` containing that type. Some required poison
records therefore cannot be constructed according to the hash type’s declared
domain. Redefine `BundleFileHash` as the raw digest of exact observed file
bytes regardless of validity, or introduce a separate `ObservedFileHash` for
malformed-file poison identities.
