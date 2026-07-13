I found 1 CRITICAL and 7 HIGH defects.

### CRITICAL

**CRITICAL — §§2, 6, 11, 13 — Lineage stamps cannot reconstruct or prove ancestry after state loss** ([stamp definition](/Users/karl/Projects/distill/DESIGN.md:1173), [reconstruction rule](/Users/karl/Projects/distill/DESIGN.md:2631))

`LineageStamp` contains only a generation and a hash of the complete ordered prefix; two such opaque hashes cannot be tested for a prefix relationship. If an intermediate generation—or even the current hash—is not represented by an asset, `.distill` reconstruction cannot recover the ordered chain, validate an older stamp, detect rollback, or safely authorize the trailing automatic segment after a custom edge. An implementation must either trust generation numbers and risk destructive backward/divergent migration, or reject legitimate automatic migrations, contradicting the state-disposability guarantee.

Fix direction: persist an authenticated ordered lineage or complete predecessor records in authored data, including migration endpoints, and prohibit automatic migration unless an explicit ancestry proof from the source node to current is available.

### HIGH

**HIGH — §§8, 9, 13, 14 — Failed watched imports cannot record the operation that should wake them** ([ImportContext](/Users/karl/Projects/distill/DESIGN.md:1368), [FileDep](/Users/karl/Projects/distill/DESIGN.md:1511), [failure basis](/Users/karl/Projects/distill/DESIGN.md:1531))

`read`, `probe`, and `enumerate` return errors without producing a `FileDep`, while `FileDep` can represent only successful observations. If a required source disappears, a listing fails, or importer lookup fails before any successful read, the attempted basis can be empty despite claiming to include the failing operation; the import then never wakes when the condition heals. Pipeline capability changes are likewise absent from the authoring-import basis.

Fix direction: make import dependencies outcome-bearing, including stable read/probe/listing failures and importer capability hit/miss observations, and revalidate the terminal failure exactly like build traces.

**HIGH — §§3, 4, 12, 13, 15 — Drop-failure poisoning has no end-to-end status path** ([ErasedValue](/Users/karl/Projects/distill/DESIGN.md:470), [drop-table APIs](/Users/karl/Projects/distill/DESIGN.md:2990), [AssetStorage](/Users/karl/Projects/distill/DESIGN.md:4544))

The design requires a drop failure to poison its owning module epoch, yet `AssetStorage::free`, `DropTable`, `CtorEntry::abort/drop_in_place`, and `SkipEntry::drop_in_place` return no status; `ErasedValue` also carries no owner-epoch token. `AssetStorage::update` consumes an `ErasedValue` but does not define ownership on `Err`, so a storage implementation may retain module-backed state that the drain protocol cannot see. `PipelineState::Poisoned` only describes candidate-open failure, not an already-published epoch becoming poisoned at runtime.

Fix direction: make destruction/update ownership explicit and status-returning, bind every erased value and callback to an epoch poison token, and define the runtime transition that fences new work and prevents `dlclose`.

**HIGH — §§13, 17, 18 — Configuration poison is asserted but not representable** ([snapshot poison APIs](/Users/karl/Projects/distill/DESIGN.md:3594), [configuration rejection](/Users/karl/Projects/distill/DESIGN.md:5259))

Invalid configuration is said to publish a configuration-poisoned input version, but the declared model has only identity `VersionPoison` and `PipelineState::Poisoned`. There is no state carrying “valid prior pipeline, invalid configuration candidate,” nor defined behavior for metadata queries, snapshots, authoring operations, or target-bound RPC calls at that version. Implementations can therefore either silently serve the prior configuration under a new version or fail inconsistent surfaces, recreating the quiescent retry and partial-projection problems poison was meant to prevent.

Fix direction: add a snapshot-pinned `ConfigurationState::Ready/Poisoned`, define exactly which operations remain valid, and expose a stable typed poison result through RPC.

**HIGH — §§4, 9, 13, 15, 16, 17 — RpcIO has no carrier for the required load-policy attestation** ([loader requirement](/Users/karl/Projects/distill/DESIGN.md:4155), [RPC connect](/Users/karl/Projects/distill/DESIGN.md:4929))

RpcIO sweeps must validate the snapshot’s `DSLP` policy against registered descriptors, but `Root.connect`, `reattest`, `Snapshot`, and resolve outcomes carry neither a policy table nor its digest. The attested `DSLA` registry cannot substitute because `build_only` is deliberately excluded from layout identity. A source-walk/macro disagreement can therefore pass RPC attestation and allow a build-only type into a runtime closure, even though PackfileIO correctly rejects the same mismatch.

Fix direction: carry policy rows plus `DSLP` through connect/reattest and bind the verified policy to each `IoBasis`; either generation-fence policy changes or provide the basis-specific policy table during sweeps.

**HIGH — §§7, 10, 20 — Codegen’s supposedly injective filename key is only bundle-local** ([codegen naming](/Users/karl/Projects/distill/DESIGN.md:5434))

The escaping is injective over one `local_id`, but `local_id` is unique only within its bundle. Two `ShaderPipeline` assets in different bundles may legally share the same ID and consequently target the same `.rs` filename and module name. Because both files are daemon-owned, the publication protocol may treat the second write as legitimate replacement rather than detecting that one pipeline’s bindings erased another’s.

Fix direction: derive names from a globally unique identity—such as full `AssetUuid`, or an injectively framed bundle identity plus local ID—and validate the entire generated namespace for collisions before writing.

**HIGH — §§2, 8, 13 — Directory-import ownership uses a mutable vector index as identity** ([DirectoryOrigin](/Users/karl/Projects/distill/DESIGN.md:1501))

`DirectoryOrigin.rule` is the rule’s list index at generation time, but inserting or reordering rules changes what that index denotes. After daemon-state loss, implementations may associate an existing generated bundle with the wrong importer/settings, orphan it, or rewrite it under another rule; the spec alternately calls this field an identity and a historical index. This breaks the promised reconstruction of directory ownership from committed bundles.

Fix direction: give every `ImportRule` a stable, unique rule ID stored in `DirectoryOrigin`; reordering must not alter identity, while deletion of that ID must produce a defined orphan state.

**HIGH — §16 — `pack.current` activation is not durably or byte-wise complete** ([activation protocol](/Users/karl/Projects/distill/DESIGN.md:4790))

The pointer’s encoding is not pinned—raw 32 bytes versus hexadecimal text and newline are all plausible—and the activation sequence never fsyncs the newly written pointer file before renaming it. Directory fsync makes the rename durable but does not portably guarantee the pointer’s data, so a crash can leave neither the old nor a valid new pointer despite the stated guarantee.

Fix direction: pin the exact pointer grammar, write it to a no-replace temporary file, fsync that file, rename it over `pack.current`, then fsync the containing directory.

### MEDIUM

**MEDIUM — §§3, 9, 13 — Runtime `dlopen` dependencies cannot obtain the staged path**

A runtime library must be registered as a tool and loaded from its snapshot-staged path, but `ProcessContext` exposes only `run_tool`, which launches a subprocess. Pipeline code has no API to resolve or open the staged library and will otherwise retain or reopen the live path.

Fix direction: either ban runtime `dlopen`, or add an epoch-scoped, trace-recorded staged-library API with explicit handle lifetime and unload semantics.

**MEDIUM — §§9, 12 — Extra outputs are both terminal and assigned a position in their own chain** ([terminal rule](/Users/karl/Projects/distill/DESIGN.md:1763), [header rule](/Users/karl/Projects/distill/DESIGN.md:2836))

Section 9 says extras are terminal and only primaries continue through processors, while §12 assigns an extra’s `encoded_type` according to its position in that type’s chain. Those rules produce different headers whenever the extra’s declared type is itself a registered processor input.

Fix direction: state that an extra is encoded directly with `encoded_type = terminal_type = declared type`, or redesign extras as independently processed children and update namespace, cache, and reachability rules accordingly.

**MEDIUM — §§5, 6 — The `f32` canonicality test contradicts shortest round-trip output** ([float rules](/Users/karl/Projects/distill/DESIGN.md:1103))

“Decimal is binary32-exact” can mean exact mathematical equality, which rejects ordinary canonical values such as `0.1`, while the preceding rule requires shortest text that merely round-trips to the same binary32 bits. Two conforming implementations can therefore accept different bundles.

Fix direction: define parsing as IEEE-754 nearest-ties-even to binary32 and canonicality as byte equality with re-emission of the shortest decimal that reparses to the same bits.

**MEDIUM — §§5, 9, 12, 13, 16 — The total hash-domain rule contradicts raw content hashes** ([domain rule](/Users/karl/Projects/distill/DESIGN.md:657))

The spec says every hash construction is domain-prefixed and listed, but `ContentHash`, raw-file hashes, dylib/tool hashes, CAS payload hashes, archive file hashes, and per-file trailers are explicitly raw BLAKE3 digests and absent from the table. Implementers cannot obey both the global invariant and the concrete format formulas.

Fix direction: scope the registry rule to semantic/composite hashes and enumerate domainless byte-identity digests as explicit exceptions, or assign domains and update every wire format consistently.

**MEDIUM — §§13, 18 — Scheduler configuration admits values that defeat progress guarantees**

No validation requires `parallelism ≥ 1` or `1 ≤ batch_reserved_workers ≤ parallelism`. Zero workers deadlocks all builds; zero reserved workers restores unbounded batch starvation; a reservation larger than the pool has undefined resize/admission behavior.

Fix direction: add staging-time bounds and define how live pool resizing treats active and reserved slots.

### LOW

**LOW — §13 — The CAS CRC coverage is self-referential or ambiguous** ([record grammar](/Users/karl/Projects/distill/DESIGN.md:3431))

The CRC field says it covers `kind…payload`, but the CRC itself lies physically inside that span. Implementations may exclude it, zero it, or attempt a fixed-point CRC, producing incompatible recovery scanners.

Fix direction: specify the exact disjoint byte ranges covered by CRC32C, explicitly excluding the CRC field.
