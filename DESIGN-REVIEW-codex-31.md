# Distill v2 Design Review — Round 31

CRITICAL/HIGH findings are present.

Severity counts: **CRITICAL 2, HIGH 1, MEDIUM 0, LOW 0**.

## Findings

### CRITICAL — §§3, 6, 11 — Bootstrap control entries have no valid lineage encoding

`AssetEntry.lineage` is mandatory for every entry and a `LineageStamp` is valid only when it is an exact prefix of the corresponding type row in `SchemaLineageManifest`. The five bootstrap control types must be omitted from that manifest and never participate in user lineage, yet those same types must exist as ordinary entries (`SchemaLineageManifest` itself, `Migration`, `ImportRecord`, `DirectoryImportRules`, and `PackDefinition`), so none can construct a valid mandatory stamp. In particular, missing-manifest repair cannot install the first manifest bundle under the declared validator, leaving startup permanently unable to reach `Ready`.

Concrete fix: replace the field with a canonically tagged `EntryLineage = Manifest(LineageStamp) | Bootstrap { format_version }` (or an equally explicit optional form), require the bootstrap arm for exactly the closed five TypeUuids, and validate its `schema_hash`/role directly against `BootstrapControlTableV1`; require the manifest arm for every other type. Pin the JSON and canonical-record grammar and update schema-closure, adoption, repair, and control-read validation to use the same rule.

### CRITICAL — §§3, 15, 17 — Exact `B(C)` attestation is circular and has no wire carrier for `C`

`Root.connect` and `Hub.reattest` are required to send the exact `B(C)` of a client asset closure, but neither request carries the root/asset closure `C`, so the server cannot derive or store the claimed exact closure or reject surplus type rows. A fresh lazy client also cannot derive `B(C)`: dependency UUIDs, verified artifact header types, and persisted edge expectations become known only after resolve/fetch, while the server refuses those payloads when they require a type outside the already accepted set and returns only `CompiledAttestationChanged`, with no required-set challenge. The first asset whose transitive closure is not already guessed therefore enters a reconnect loop; sending the entire registered type table makes progress but directly violates the repeatedly stated exact-`B(C)` rule.

Concrete fix: choose one implementable protocol. Either define the accepted set as a client-declared superset and enforce `B(served closure) ⊆ accepted_set` on every response, or add an attestation-discovery/challenge result that carries a canonical required TypeUuid set (and a pinned snapshot/closure identity) without exposing artifact data, then let connect/reattest install that exact set before retrying. If exactness remains, requests must carry enough asset-root/closure identity for the server to recompute `C`, and the challenge must distinguish projection drift from required-set expansion.

### HIGH — §§3, 9, 13 — ToolEpoch hashes a launcher file, not the executable code closure

Tool identity and trace revalidation cover only the staged bytes of the registered path, even though the design expressly permits interpreter failures and ordinary subprocess executables may load interpreters, shared libraries, plugins, locale data, or inherited environment state. Changing a shebang interpreter or a dynamically loaded library leaves `TraceOp::Tool::Ok(staged_hash)` unchanged, so an old successful artifact remains a cache hit even though rerunning the named tool would execute different code; moving dynamic dependencies out of the pipeline dylib has merely moved the untracked-code seam into the subprocess. This is routine toolchain drift, not the separately documented system-runtime residual for the pipeline module.

Concrete fix: make a ToolEpoch row name a hermetic, content-addressed execution capsule and trace its aggregate identity: staged launcher/script, resolved interpreter, non-system DSO/plugin closure, executable metadata, declared resources, and a canonical sanitized environment. A simpler conforming alternative is to reject scripts and dynamically linked/plugin-loading tools, require self-contained executables, clear the environment, and explicitly define any remaining system-runtime exception as an accepted cache input risk.
