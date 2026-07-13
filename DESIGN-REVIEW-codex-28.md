# Round 28 adversarial design review

**CRITICAL: 0**  
**HIGH: 5**

## 1. HIGH — Cross-root ambiguity is both representable and version-global poison

Anchors: §8 lines 2310–2317; §13 lines 4632–4637; §18 lines 7370–7371; §7 lines 2085–2153.

The core multi-root model deliberately retains same-path files as `Ambiguous(roots)` and makes only path-dependent operations fail, but DSVP adds `CrossFileLogicalPathCollision` as a version-global poison, causing every namespace query to fail. If that arm instead means same-root post-NFC collision, its claimant grammar lacks raw physical names and cannot represent two equal-content colliding entries because identical normalized source rows are rejected as duplicates. Remove cross-root overlap from DSVP and preserve the existing ambiguity state, or rename/narrow the arm to a forbidden same-root collision and carry distinct raw physical claimant identities.

## 2. HIGH — DSVP cannot represent all duplicate AssetUuid collisions that publication must reject

Anchors: §7 lines 2028–2051 and 2088–2153.

Publication checks authored-vs-derived and derived-vs-derived UUID collisions, and also permits two entries in one bundle to claim one UUID, but `DuplicateAssetUuid` carries only distinct `ReadableBundleSource` rows. A derived child has no such source claim, and two colliding entries or derived children from one bundle collapse to the same source row, violating the required cardinality and duplicate rejection. Replace `sources` with a sorted tagged claimant grammar such as `Authored { source, bundle, local_id } | Derived { parent, output_key }`, requiring at least two distinct claimants.

## 3. HIGH — Bootstrap rows have contradictory pack/DSCA coverage

Anchors: §3 lines 508–518; §16 lines 6173–6214; R27 ledger lines 9485–9490.

The new bootstrap rule says the five rows remain in every compiled table and DSCA and that completeness never changes with the caller, while the pack grammar says its compiled registry and DSCA contain exactly the artifact closure, which cannot contain authoring-only bootstrap controls. Conforming implementations will therefore hash different row sets or require non-closure types at mount. Pin one rule across every boundary: either packs/RPC projections are `closure-or-client-set + the five bootstrap rows` with matching mount requirements, or bootstrap authority is validated locally out of band and excluded from projected boundary DSCAs.

## 4. HIGH — Outside-set daemon additions both must and must not fence an RPC Hub

Anchors: §9 lines 2893–2909; §13 line 4643; §15 lines 5905–5921; §17 lines 7202–7225.

R27 says a daemon-only type addition outside the Hub’s accepted set does not fence it, but DSLP is defined over the full current registry and a changed table advances the global load-policy generation, which fences every old capability with `LoadPolicyChanged`. The same publication therefore has incompatible required outcomes; choosing the no-fence behavior also lacks a rule preventing that new type from later entering a served load-dependency closure. Make policy generations projection-sensitive to each accepted set, and require resolve/closure expansion to reject or reconnect before serving any type outside that set, or withdraw the outside-set no-fence guarantee.

## 5. HIGH — The poison-safe metadata diagnostic has no interoperable PipelinePoison grammar

Anchors: §13 lines 4995–5000 and 5043–5071; §17 lines 6541–6553 and 6962–6980.

`PipelineDiagnostic.poisoned` is opaque `Data` claimed to contain a decoded canonical closed `PipelinePoison`, but `PipelinePoison` is never declared and no versioned byte grammar exists; only `PipelinePoisonOrigin` and references to the missing type are present. Independent clients cannot decode or validate the primary recovery diagnostic under pipeline poison, defeating the new unbound bootstrap’s typed contract. Define a closed versioned `PipelinePoison` record—including origin and candidate/runtime cleanup disposition—and expose it as explicit Cap’n Proto structs/unions, or fully pin its canonical byte grammar and rejection rules.
