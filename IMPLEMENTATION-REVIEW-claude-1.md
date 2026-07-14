# Claude implementation review 1 (raw, untriaged)

_Run 2026-07-14 with Claude Code `opus` and its maximum effort setting against
Distill `ea60909` and New Game Plus `843ae03` / `c03cc91`. This is the raw
external report. Several claims are stale or over-severe and require local
triage before implementation._

## CRITICAL findings

### C-1: Wire executor destination writes are unbounded

**Files:** `v2/crates/distill-wire/src/exec.rs:908–912` (switch-variant tag
write), `:449–464` (flat copy)

The reviewer reports that neither write checks its destination offset and size
against `PlanMeta::native_size`, and recommends validation at plan compilation
and execution.

### C-2: `DynamicLoadedPipelineModule::dlclose` leaves callable table pointers

**Files:** `v2/crates/distill-daemon/src/module_loader.rs:137–176`

The reviewer reports that `dlclose(&mut self)` unloads the image while the
object retains its copied function table and recommends making teardown consume
the boxed module.

### C-3: `check_f32` compares f64 canonical text with f32 canonical text

**Files:** `v2/crates/distill-bundle/src/walk.rs:489–528`,
`v2/crates/distill-json/src/write.rs:20–29`

The reviewer reports that a longer decimal spelling that rounds to the same
binary32 is rejected and asks that the design intent be clarified.

## HIGH findings

### H-1: Hash-then-`dlopen` TOCTOU in the shared module host

**File:** `/Users/karl/Projects/newgameplus/ngp-module-host/src/lib.rs:108–123`

The reviewer reports that a staged library can be replaced between hashing and
`libloading::Library::new`, allowing constructors from replacement bytes to run
before a later identity check. It recommends fd-based loading where available
or a tightened, documented filesystem trust boundary.

### H-2: Rust-ABI function pointers in a `repr(C)` module table

**File:** `v2/crates/distill-daemon/src/module_loader.rs:24–42`

The reviewer reports that the `register` and `unload` entries use Rust ABI and
recommends C ABI entries or a stronger identical-build identity.

### H-3: Source-walk pointer-kind variants differ from `ngp-schema`

**Files:** `/Users/karl/Projects/newgameplus/source-walk/src/schema.rs:12–17`,
`/Users/karl/Projects/newgameplus/ngp-schema/src/lib.rs:180–186`

The reviewer reports that source-walk lacks `ConstPointer` and `Arc` variants
and recommends completing the variants or rejecting unsupported pointer kinds.

### H-4: Published module poison intentionally leaks without a process bound

**File:** `v2/crates/distill-daemon/src/epoch.rs:802–826`, `:1049–1092`

The reviewer reports that repeated poisoned epochs can grow resident memory
without bound and recommends an operational cap and restart recommendation.

### H-5: `pack.current` activation can select an older pack

**File:** `v2/crates/distill-pack/src/activation.rs:138–156`

The reviewer reports that concurrent activation is last-writer-wins and
recommends generation comparison plus serialization.

### H-6: Module probes may allocate 64 MiB each

**File:** `v2/crates/distill-daemon/src/module_loader.rs:251–288`

The reviewer recommends reducing the 64 MiB per-probe cap.

### H-7: Latent double-ownership window in callback ingress

**File:** `v2/crates/distill-daemon/src/epoch.rs:321–336`

The reviewer notes that the current statement between linking the node and
clearing the ingress capsule cannot panic, but recommends removing the latent
double-ownership window before future edits can make it observable.

## MEDIUM findings

1. `host_module_identity` hashes source-file bytes, so formatting changes
   invalidate modules (`distill-daemon/src/module_loader.rs:51–57`).
2. Idempotent immutable publication reads the complete existing file
   (`distill-pack/src/activation.rs:128–134`).
3. The wire executor represents a ZST dangling pointer with `align as *mut u8`
   rather than `NonNull::dangling()` (`distill-wire/src/exec.rs:270`).
4. `CanonicalTargetSet::from_canonical` clones rows during revalidation
   (`distill-core/src/target_set.rs:76`).
5. `CompilationIdentity` construction does not enforce NFC
   (`ngp-schema/src/identity.rs:36–49`).
6. The store does not enable SQLite foreign keys
   (`distill-store/src/db.rs:328–344`).
7. f32 integer and float values take different narrowing paths
   (`distill-bundle/src/walk.rs:494–509`).

## Raw verdict

The reviewer reported 3 CRITICAL, 7 HIGH, and 7 MEDIUM findings and said the
implementation should not be considered safe until the destination-write,
module teardown, module-open TOCTOU, Rust ABI, and pointer-kind findings are
resolved.

## Inspection claimed by the reviewer

- Full: `distill-core` canonical/id/lineage/attestation/tool/target-set code,
  `distill-json`, `distill-wire/src/exec.rs`, daemon epoch/module-loader,
  `distill-pack`, `ngp-module-host`, `ngp-schema` lib/identity, source-walk
  schema, and the handoff.
- Deep: bundle walk and store db/pipeline/state/journal/CAS record.
- Selective: `DESIGN.md` sections 1–6, 12, and 16.

The report's separate “unimplemented” section is omitted here because it was
demonstrably stale: it claimed several present, tested crates were unstarted.

## Local triage and resolution

| Finding | Decision | Resolution / evidence |
|---|---|---|
| C-1 native write bounds | Endorsed as defense in depth; exploit claim overstated | Runtime bounds now cover flat copies, known built-ins, recurse frames, and enum tag writes, including enum-relative variant plans. Added fabricated-plan negatives. Distill `7c3d639`. |
| C-2 callable table after close | Endorsed as lifecycle hardening; no current UAF call path | The table becomes `None` before the image closes and every call returns a typed closed-state error. Distill `48e0d5b`. |
| C-3 f32 canonicality | Rejected | The design explicitly requires equality with the shortest authored binary32 token. `0.30000001192092896` must canonicalize to `0.3`; the existing negative test pins this. |
| H-1 hash/`dlopen` path race | Endorsed | The shared host hashes an open descriptor and loads that same descriptor on Unix; Windows holds a handle denying write/delete replacement across path loading. New Game Plus `257159e`. |
| H-2 Rust ABI entries | Rejected | `DESIGN.md` explicitly makes only the three pre-verification probes C ABI; `register`/`unload` are deliberately Rust ABI after exact `CompilationIdentity` and `ModuleAbiIdentity` equality. |
| H-3 pointer-kind mismatch | Rejected | Raw pointers of either mutability classify as the same opaque, rejected class; no wire layout is produced. `Arc` is recognized by well-known type identity, not source-walk's raw-pointer enum. |
| H-4 poisoned epoch leak growth | Rejected as a correctness change | Permanent leak is the specified fail-stop behavior after callback corruption. `retired_count` and per-epoch poison diagnostics expose it; process restart is the recovery boundary. |
| H-5 pack activation monotonicity | Rejected | There is no pack-level generation authority. Archive generations locate physical records; `pack.current` intentionally selects a content-addressed manifest and may be explicitly rolled back. |
| H-6 64 MiB probe cap | Rejected | Probe buffers are allocated and decoded sequentially, not three at once, and the in-process module already executes trusted native code. The cap prevents unbounded accidental reports without constraining large registries to an arbitrary 1 MiB. |
| H-7 ingress ownership window | Rejected | `CallbackIngressGuard::drop` deliberately leaks an armed no-Drop capsule; it never calls `cleanup_reverse` or frees the linked node, so the alleged double-free path does not exist. |
| M-1 source-byte ABI fingerprint | Rejected | Conservative invalidation is intentional at this safety boundary; it cannot accept an incompatible module. |
| M-2 immutable-file comparison | Rejected | Performance observation only; byte identity is required when a hash-named file already exists. |
| M-3 ZST dangling pointer | Rejected | The dynamic aligned non-null pointer is intentional for a runtime alignment; `NonNull::dangling()` cannot express that alignment. |
| M-4 target-set clone | Rejected | Performance-only validation path; no semantic defect. |
| M-5 identity NFC | Rejected | `CanonicalEncoder::str` NFC-normalizes every identity string before DSCI hashing, and module-boundary decoding independently requires NFC. |
| M-6 SQLite foreign keys | Rejected | The DDL declares no foreign-key constraints; enabling the pragma would change nothing. |
| M-7 integer/float f32 paths | Rejected | The separate integer path deliberately prevents double rounding beyond binary64's exact integer range, as required by the design. |

**Resolved review outcome:** three defensive hardenings accepted and implemented;
all remaining findings rejected with code/design evidence. No unaddressed
CRITICAL or HIGH finding remains from this report.
