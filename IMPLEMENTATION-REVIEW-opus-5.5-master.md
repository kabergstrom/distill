# Opus 5.5 review — current-master integration

Closed 2026-09-30. New Game Plus master: `cee84ff`, with upstream `eadbaa1`
as an ancestor. Distill integration/spec milestones: `a99a1f1`, `3898ba8`,
`f83d72a`. Rafx: `0f3d1a83`.

The reviewer was the Nix-store Claude CLI at
`/nix/store/92n7qqq1sn1snpm26v9ys4pgpy6kh8yw-claude-code-2.1.280/bin/claude`,
with verified model `claude-opus-5-5`, effort `xhigh`, and read-only access.
The initial complete review covered the integration delta against `5962a71`;
closure passes covered every subsequent fix and the newest master commits
`bc31e7d` and `eadbaa1`. The final delta contains 63 files.

## Dispositions

- Unique module staging and authenticated image identity are shared by both
  hosts. Real Rust cdylib tests exercise descriptor reuse, initialized TLS,
  and explicit image leakage. Linux/Android preserves descriptors when a
  NOLOAD probe finds a TLS-pinned image after dlclose.
- Released loader handles no longer strand GPU-repopulation slots.
- Multi-layout and mismatched schema candidates reject before mutation;
  initial tracer installation requires the resident source identity.
- The engine observes artifacts; daemon-owned supervision and the launch
  script own producer processes. Caller-relative paths and supervisor cleanup
  are preserved.
- Failed GPU submissions restore pending texture transitions and transfer
  state. Upstream queue/barrier and render changes remain incorporated.
- Option payload reads require both measured offsets. Enum discriminant
  metadata, variant revisions, and upstream layout regressions are preserved.
- Producer normalization coalesces only agreeing observations. Both sides of
  every recursive comparison are remapped; direct and nested later-observation
  regressions cover the final correction. Unknown/opaque records do not gain
  global identities. Real engine and external-module schemas merge.
- Native keys distinguish usize/isize and concrete const arguments. Unresolved
  consts remain non-dedupable. This is not a DSLH or module-acceptance gate.
- In-memory shader cooking rejects ambient includes in both shaderc and the
  reflection parser, including inactive preprocessor branches.
- Drop reachability uses one visited set, avoiding exponential cloning.
- Latest-master reload retry semantics and HashMap/HashSet RPC introspection
  are ported onto shared classification/layout views. The upstream RPC
  regression remains and passes.
- Hash argument parsing rejects non-ASCII without panicking. Documentation
  distinguishes refetched shaders from retained cooked pipeline packages.

The last reviewer verdict found no remaining code defect, conditional on
finishing the Vulkan/binary tests and rebuilding the real fixture. Those
conditions subsequently passed. The normal final master merge produced a
tree byte-for-byte identical to the reviewed and tested integration tree.

## Final evidence

- Distill workspace/all-targets: 1,123 tests, 102 passing suites.
- Separate Distill doc-tests: passed (one test in total).
- Shared schema/host/reflection/source-hash: 128 tests passed.
- Source-walk producer: six tests passed.
- New Game Plus Vulkan/reload library: 62 tests passed using the installed
  MoltenVK ICD. Texture, mesh, cooked pipeline, repopulation and native-module
  migration tests use real resources and a real separately compiled cdylib.
- Binary hash parsing: one test passed.
- Final generated engine schema self-merge: 9,716 + 9,716 observations,
  10,500 merged records; opaque records intentionally remain separate.
- Real external schema merge: 9,716 + 2,075 records, 9,783 merged records.
  A producer-emitted enum variant's revision is independently verified.
- Script syntax and integration-delta whitespace checks passed. Existing
  upstream EOF formatting was not altered.

## Limits

- Rafx's non-legacy shader suite passes 44 tests. Four legacy `shader_types`
  tests remain excluded because the old `spirv-reflect` dependency aborts on
  Rust 1.98 unsafe preconditions. This is not a full Rafx-suite pass.
- Linux/Android execution and the full interactive external-module launch
  script were not run on this Mac. Metal was skipped as requested.
- The prior Rust-toolchain Clippy result is not claimed for current Rust 1.98.
- No push was performed. User `.DS_Store` changes and unrelated untracked
  files were preserved.

Raw review and test logs are under `/tmp/ngp-opus55-*.jsonl`,
`/tmp/ngp-final-*.log`, `/tmp/distill-master-integration-tests.log`,
`/tmp/distill-final-doctests.log` and `/tmp/rafx-final-shader-tests.log`.
Reproduction commands are in `DESIGN-IMPL-HANDOFF.md`.
