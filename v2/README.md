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
| `distill-loader` / `distill-pack` / `distill-rpc` / `distill-daemon` | §15/§16/§17/§3 |

Schema-model code here is destined to merge into `newgameplus`'s
`ngp-schema`/`source-walk` per §5 ("One model, split — not a second one");
it is developed in this workspace first so distill's test suite drives it.

Build/test with `nix run nixpkgs#cargo -- test` from this directory.
