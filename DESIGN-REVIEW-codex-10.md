codex
The round-9 hash framing, ABI/layout attestation path, swap-back verification, pack digest, typed paths, and named roots are generally coherent. I found one remaining high-severity collision path and several API-level gaps.

1. **HIGH — §7, §9, §13: precomputed derived identities are not collision-checked when published.**  
   The old rule rejects collisions only when a derived output commits. The new reverse index exists before any build, so that check is too late. For example, an authored asset can explicitly claim the UUID of a remembered derived child from an earlier epoch. After restart, both the authored index and precomputed child index claim that UUID; `resolve` may select the wrong object or become ordering-dependent without any derived commit occurring.  
   **Fix direction:** epoch/input-version publication must atomically validate the union of authored UUIDs and all precomputed `(parent, output_key)` UUIDs, including child-vs-child collisions, and reject the version before it becomes queryable.

2. **MEDIUM — §5, §6: `Unit` has no normative authored-value encoding, creating a nullable-value ambiguity.**  
   `SchemaNode::Unit` exists, but `AuthoredValue` has no `Unit` variant. `Null` is explicitly `Option::None`, while `Some(x)` is encoded as `x`. If `()` uses the conventional `null`, `Option<()>` silently aliases `None` and `Some(())`; if it uses another JSON value, that encoding is currently unspecified despite canonical bytes being cache inputs. Transparent wrappers extend the same problem to forms such as `Option<Box<()>>`.  
   **Fix direction:** pin a distinct canonical JSON/AuthoredValue representation for unit, or reject `Option<T>` whenever normalized `T` has a null encoding. Apply the existing nested-option rejection after erasing transparent wrappers.

3. **MEDIUM — §9, §13: action-key and dependency-trace encodings are not normative or injective.**  
   Identity records and query-result hashes are carefully length-framed, but the serialized `ActionKey`, `TraceOp`, `AssetQuery`, output table, and trace sequence have no corresponding grammar. A literal implementation of the `‖` formulas can repartition variable-length IDs, paths, output keys, and selector strings, allowing two logical actions to share cache-key input bytes.  
   **Fix direction:** define one versioned, domain-separated grammar with enum discriminants, option markers, vector counts, and lengths for every variable field. Store either that encoding or its explicitly defined digest.

4. **MEDIUM — §9, §13: the declared identifier limits do not fit the CAS record.**  
   The document claims that 256 outputs with 255-byte keys bound every serialized action key, but just the output-key and two-hash tuples can exceed 81 KiB. `key_len` is only `u16`. Thus a registration accepted by §9 can be impossible to record under §13. `ActionKey::stage: u16` likewise lacks a corresponding maximum chain length.  
   **Fix direction:** make `key_len` at least `u32`, store a fixed-size action-key digest, or impose an aggregate encoded-key limit. Explicitly cap processor-chain length to the range of `stage`.

5. **MEDIUM — §3, §12: the ctor and skip-writer callback ABIs remain undeclared.**  
   `CtorId` and `SkipDefaultId` are declared, but their tables, function signatures, ownership transitions, status returns, and drop-reporting channel are not. Consequently, the round-9 promise that constructors return errors and panicking drops leak-and-report is enforceable for `DefaultWriter` and `MigrationFn`, but not at the normative API boundary for `ConstructMap`, other constructors, or `WriteSkipDefault`.  
   **Fix direction:** declare the generated tables and each entry signature, including initialization state, failure ownership, rollback registration, and a non-unwinding drop status/report path.

6. **MEDIUM — §9, §13: the empty extra-output key is not explicitly rejected.**  
   CAS records encode the primary output with an empty `output_key`, while `OutputDecls::extras` and `Outputs::extra` accept arbitrary strings and the stated bounds allow zero bytes. Accepting `""` aliases an extra with the primary in the output table and durable record format. Calling the primary key “reserved” is insufficient validation semantics at the normative API level.  
   **Fix direction:** require non-empty extra keys and reject every reserved namespace/key during registration and result binding.

7. **MEDIUM — §14, §17: safe publication is defined only for replacing an existing file.**  
   Atomic exchange requires the target to exist, but editor creation and import to a new destination are normal authoring operations. If the destination is absent at the base version but an external editor creates it before publication, a conventional atomic rename can overwrite that unobserved file. Watcher lag means the RPC version precondition alone cannot close the race.  
   **Fix direction:** specify an atomic no-replace creation path. If the destination appears, preserve both objects under named conflict paths and fail/retry; apply the same fsync and authored-bytes deletion rules as exchange publication.

Remaining CRITICAL issues: none. Remaining HIGH issues: 1.
