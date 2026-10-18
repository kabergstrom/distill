# distill v2

Implementation of `../DESIGN.md` — the distill v2 asset-pipeline daemon.

Every crate maps to a section of the design document; the document is
normative and the tests here verify its specified behaviour (happy paths,
edge cases, and negatives), test-first.

| Crate | Design section |
|---|---|
| `distill-core` | §5 canonical record encoding; identity newtypes (§7) |
| `distill-json` | §6 canonical JSON, `AuthoredValue` |
| `distill-schema` | §5 schema model, DSLH logical hash, snapshot codec, serializability |
| `distill-bundle` | §6 bundle file format |
| `distill-migrate` | §11 migration planner + executor |
| `distill-wire` | §12 wire layout (DSWL), DSTL artifacts, fixup |
| `distill-store` | §13 metadata store + log-structured CAS |
| `distill-build` | §8 import, §9 processing, §10 dependencies |
| `distill-asset-macro` | §4 `#[asset]` |
| `distill-protocol` | §17 protocol data types (requests, answers, errors, `state`), shared by client, server and loader |
| `distill-loader` | §15 runtime loader; feature `rpc` (default) adds `RpcIo` and authoring over `distill-client` |
| `distill-pack` | §16 pack reader (`PackfileIO`) |
| `distill-pack-build` | §16 pack writer and publish |
| `distill-client` | §17 Cap'n Proto schema, bindings and client transport |
| `distill-rpc` | §17 server: capabilities, publish, server transport |
| `distill-daemon` | §3 |

A runtime reading packs only (the loader without `rpc`) links `distill-asset`,
`distill-core`, `distill-loader`, `distill-pack`, `distill-protocol` and
`distill-wire`.

Schema-model code here is destined to merge into `newgameplus`'s
`ngp-schema`/`source-walk` per §5 ("One model, split — not a second one");
it is developed in this workspace first so distill's test suite drives it.

Build/test with `nix run nixpkgs#cargo -- test` from this directory.
