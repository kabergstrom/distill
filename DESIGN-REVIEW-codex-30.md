# Adversarial design review — Round 30

> Review provenance: permitted in-app adversarial review of the complete
> folded `DESIGN.md`, including the accepted-finding ledger through Round 29.
> Ledgered issues are not repeated unless their fold introduced a distinct
> defect.

## Release verdict

**Release-blocked.** This review found **0 CRITICAL, 6 HIGH, 5 MEDIUM, and
0 LOW** issues. There are no Critical findings. The remaining High findings
are concrete unrepresentable recovery/input states or unsound memo/authority
boundaries; they are not repetitions of the Round 29 findings that introduced
the affected surfaces.

## HIGH

### §§9, 13 — A post-hit tool-launch failure is not bound to the staged executable or to stable inputs

`TraceOp::Tool` promises to record the staged binary hash, but its failure arm
stores `StableFailureFingerprint::ToolLaunch { id, class }`, which carries no
hash. After a lookup hit fails to launch, a later ToolEpoch can map the same id
to different bytes while revalidation still observes the same id/class and
accepts the old failure memo. The revalidator cannot recover the old hash from
the trace after restart. Moreover, `MissingInterpreter` and `SpawnDenied` (and
`NotExecutable` unless staged mode is immutable and included in ToolEpoch)
depend on interpreter/permission state that is neither content-addressed nor
input-versioned, so they are not the content-derived stable outcomes the memo
grammar requires. Add the exact staged hash to the fingerprint/DSTR grammar
and make every launch determinant an explicit ToolEpoch observation, or treat
the affected launch classes as transient and unmemoized. Round 29 correctly
separated an absent registration from `ToolLaunch`; it did not close this
post-hit identity gap.

### §§6, 13–14, 17–18 — Missing-lineage repair is still impossible when the configured destination already exists

`MissingLineageManifest` says only that no manifest entry exists; it does not
imply that `assets.lineage_manifest` is absent. The configured path can already
hold an ordinary bundle or unrelated file. The new `Missing` inspection carries
no destination occupancy/preimage, while `createMissing` requires the path to
be absent and uses no-replace creation. Ordinary authoring is deliberately
blocked by configuration poison, so this state has no declared mutation path
through the control plane. Extend inspection with `Absent | Occupied` and let
the occupied case perform an exact-preimage, journaled atomic rewrite that
preserves all existing entries while adding the validated manifest, or declare
and enforce a different invariant that makes an occupied non-manifest
destination impossible. Manual out-of-band editing does not satisfy the
Round 29 promise of a complete explicit repair operation.

### §§5–6, 13–14, 17 — Lineage claimant identity collapses distinct entries in one physical file

`LineageManifestClaimant` is `(asset, root_name, normalized_path, file_hash)`
and duplicate inspection strictly deduplicates that tuple. A bundle can contain
two different local ids that both claim `SchemaLineageManifest` and reuse the
same invalid `AssetUuid`; both claims then have the same path, file hash, and
asset UUID. Inspection collapses them to one row, violating the duplicate arm's
at-least-two wire cardinality, and `resolveDuplicate` can neither select one
entry nor identify which non-survivor to remove. DSCP's
`DuplicateLineageManifest.entries: Vec<AssetUuid>` has the same collision, so
its claim that entries in one bundle remain distinct is false for precisely
the duplicate-UUID state DSVP permits. Include the containing `BundleUuid` and
normalized `local_id` (or reuse the injective authored `AssetClaimant`) in the
claimant, DSCP detail, sorting, survivor CAS, and removal grammar.

### §§3, 13, 15, 17 — Fresh target connection has no typed result for a pipeline-unavailable version

`PipelineState` has ordinary stable states with no usable current epoch:
`CandidateOpen` poison can occur before a compiled table is available,
`SchemaAcceptanceRequired` is intentionally non-serving, and
`RetiredTypeReferenced` blocks the otherwise opened epoch. A fresh
`Root.connect` therefore cannot construct `ConnectSuccess` without illegally
attesting `last_good`, yet `ConnectResult` has only success, attestation,
configuration, protocol, and generic `RpcError` arms. `RpcError` has no closed
pipeline-unavailable payload/code grammar and cannot carry the canonical DSPP,
acceptance, or retired-reference facts; this contradicts the document's typed
failure rule and leaves RpcIO startup behavior undefined in a normal published
state. Add a typed `pipelineUnavailable` arm carrying the closed
`PipelineDiagnostic`/`PipelineUnavailable` union and specify retry/reconnect
behavior. `Root.metadata` makes diagnostics reachable, but does not make the
target connect result representable.

### §§5, 14, 17–18 — The declared filesystem identity cannot represent Windows directory identity

Alias detection, symlink revalidation, daemon-owned-directory exclusion, and
DSCP `DirectoryAliasSide` all use exactly `(device: u64, inode: u64)`, while the
design claims Windows support and platform-equivalent descriptor-relative
behavior. Modern Windows exposes a volume identity plus a 128-bit file ID;
there is no injective encoding of that authority into the declared two-u64
Unix-shaped record while retaining the volume identity. Truncation or an
unregistered hash permits false equality (skipping/revalidating the wrong
directory) or false alias poison. Define a closed tagged
`PlatformFileIdentity`, for example Unix `(dev, ino)` and Windows
`(volume_serial, file_id[16])`, and use its canonical bytes consistently in
scanner maps, retained handles, quarantine checks, persistence, and DSCP.

### §§7, 13–14 — Scan-wide enumeration failure has no version-global poison representation

The DSVP grammar covers a known bundle file that cannot be read and a raw
pathname that cannot normalize, but not failure to enumerate a configured root
or an intermediate directory. Permission or I/O failure while listing a
subtree leaves an unknown set of bundle UUIDs, asset UUIDs, tags, and paths, so
publishing deletes/remaining rows under it would violate the same conservative
namespace rule that motivated version-global poison. Retaining prior rows would
violate rejection-still-publishes. `UnreadableGlobalBundlePath` requires a
normalized bundle path and cannot name the configured root itself (whose
root-relative path is empty and invalid under §10) or state that the subject is
a directory subtree. Add a version-global scan-subtree/root failure arm with a
lossless subject and closed listing/open reason codes, and include it in Round
29's complete canonical defect enumeration.

## MEDIUM

### §§5, 13, 18 — An unavailable configuration file cannot produce any DSCP value

The daemon watches its own configuration and rejected candidates publish
snapshot-pinned configuration poison, but the only parser/source arm is
`MalformedConfiguration { file_hash }`. Deletion, permission denial, or I/O
failure before bytes can be hashed has no `DscpV1` representation. Specify
whether deletion means an intentional default configuration; otherwise add
closed missing/unreadable-source arms with exact stable subjects/reasons so
the rejection and later healing can publish deterministically.

### §§5, 13, 18 — Simultaneous configuration defects have no canonical winner

One candidate can independently violate several checks (for example
`parallelism = 0` plus an invalid reservation, or duplicate roots plus an owned
path overlap and a missing lineage manifest), while `ConfigurationState` stores
one `ConfigurationPoison`. No validation order or canonical aggregate selects
that reason. Implementations can therefore derive different DSCP identities
from identical config bytes, and `Root.lineageRepair` availability can depend
on which validator happened to run first. Enumerate all DSCP candidates and
select by a pinned `(code, canonical detail bytes)` order, or carry a canonical
set, exactly as Round 29 now requires for DSVP.

### §§7, 10, 17 — Raw physical-path reason predicates are not pinned well enough for mandatory decode proof

Persistence and RPC decoders must reject bytes that do not prove
`Absolute`, `EmptyComponent`, `DotComponent`, `ParentComponent`, or
`ForbiddenCharacter`, but the grammar only says slash/backslash/NUL "as
applicable." It does not define Windows drive-relative versus drive-absolute,
rooted, UNC, or device-prefix forms; which raw code units are separators on
each arm; or how a value described as root-relative can canonically carry the
`Absolute` case. Two decoders can disagree on the same DSVP bytes. Pin exact
byte/code-unit predicates and component tokenization per platform, including
how multiple simultaneous defects are enumerated or reduced.

### §17 — Wrong-width attestation fields have no member of the exhaustive failure matrix

The matrix defines `EO(32)` to require both expected and observed values to be
exactly 32 bytes, then says wrong-width digests are rejected. A 31-byte
`targetDefHash`, `dscaAggregate`, `policyDigest`, DSLH, DSNL, or DSRE therefore
cannot use its semantic mismatch tuple, while `malformedTable/TD(row)` is
defined only for the compiled/policy table subjects and assumes an offending
row. Falling through to `RpcError` would bypass the supposedly exhaustive
attestation grammar and has no fixed code/payload here. Add malformed-field
subjects with raw length-framed observed bytes, or let `ExpectedObserved`
carry a fixed-width expected value and arbitrary length-framed observed value
for width errors.

### §§9, 15–17 — The exact boundary type set does not say whether authored closure-node types are included

Load eligibility is explicitly judged by an authored asset's **authored** type
even when processing produces another terminal type, but the pack grammar calls
its boundary merely "every runtime type in the artifact closure," and RPC calls
it the client runtime descriptor set. If an `A -> B` asset is shipped and only
`B` is interpreted as runtime, omitting `A` from DSCA/DSLP hides
`A.build_only = true` and launders the forbidden closure through processing;
including it yields the intended failure. Pin the set formula over manifest
rows and closure nodes—at minimum every authored type used for policy plus
every encoded/terminal type needed for loading, union bootstrap—and require
pack construction, mount, connect coverage, server data-coverage checks, and
RpcIO validation to derive that identical set.

## LOW

None.
