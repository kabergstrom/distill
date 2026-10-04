//! The store trace source against the eager capture it replaced: the same
//! answer to every question, the same `revalidate` verdict on every trace,
//! and at scale a trace revalidates with a handful of indexed reads.

use super::*;
use std::cell::RefCell as Cell;
use std::collections::BTreeSet;

use distill_build::query::TagSelector;
use distill_core::id::BundleFileHash;
use distill_core::tool::ToolCwdPolicy;
use distill_store::bundles::{AssetRecord, BundleMeta, NamespaceSkeleton, SkeletonEntry};
use distill_store::files::RootId;
use distill_store::pipeline::{ResolvedToolPackageFile, ResolvedToolSourceV2, ToolRegistrationV2};
use distill_store::state::InputVersion;
use distill_store::{InputTxn, StoreConfig};

use super::eager_trace::EagerBasis;

const TA: TypeUuid = TypeUuid([0xa1; 16]);
const TERM_A: TypeUuid = TypeUuid([0xa2; 16]);
const EXTRA_A: TypeUuid = TypeUuid([0xa3; 16]);
const TB: TypeUuid = TypeUuid([0xb1; 16]);
const TC: TypeUuid = TypeUuid([0xc1; 16]);
/// Registered for Windows only: on the Linux target its chain is empty.
const TD: TypeUuid = TypeUuid([0xd1; 16]);

fn asset(n: u8) -> AssetUuid {
    AssetUuid([n; 16])
}

fn bundle(n: u8) -> BundleUuid {
    let mut uuid = [n; 16];
    uuid[0] = 0xb0;
    BundleUuid(uuid)
}

const MISSING: AssetUuid = AssetUuid([0xee; 16]);
const GHOST: AssetUuid = AssetUuid([0xef; 16]);

fn registry() -> PipelineRegistry {
    let any = || TargetSelector::new(None, None).unwrap();
    PipelineRegistry::new(vec![
        ProcessorRegistration::new(
            "a",
            1,
            TA,
            any(),
            OutputDecls::new(TERM_A, vec![("meta".to_owned(), EXTRA_A)]).unwrap(),
            [1; 32],
        )
        .unwrap(),
        ProcessorRegistration::new(
            "c",
            1,
            TC,
            any(),
            OutputDecls::new(TB, Vec::<(String, TypeUuid)>::new()).unwrap(),
            [1; 32],
        )
        .unwrap(),
        ProcessorRegistration::new(
            "d",
            1,
            TD,
            TargetSelector::new(Some(BTreeSet::from([TargetOs::Windows])), None).unwrap(),
            OutputDecls::new(TB, Vec::<(String, TypeUuid)>::new()).unwrap(),
            [1; 32],
        )
        .unwrap(),
    ])
    .unwrap()
}

fn target() -> Target {
    Target::new(
        TargetOs::Linux,
        TargetArch::X86_64,
        BTreeSet::from([GraphicsApi::new("vulkan").unwrap()]),
        false,
        true,
        test_layout_identity(),
    )
    .unwrap()
}

fn current_load() -> CurrentLoadSource {
    CurrentLoadSource {
        capabilities: vec![(CapabilityKey::DefaultTable(TA), [9; 32])],
    }
}

fn tool_package(launcher: &[u8]) -> ToolRegistrationV2 {
    ToolRegistrationV2 {
        source: ResolvedToolSourceV2::Package {
            launcher: "bin/tool".into(),
            files: vec![ResolvedToolPackageFile {
                path: "bin/tool".into(),
                executable: true,
                bytes: launcher.to_vec(),
            }],
        },
        environment: vec![],
        cwd_policy: ToolCwdPolicy::EmptyScratch,
    }
}

struct Project {
    _dir: tempfile::TempDir,
    config: StoreConfig,
    store: Store,
}

impl Project {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = StoreConfig::new(dir.path().join("state"));
        let store = Store::open(config.clone()).unwrap();
        Self {
            _dir: dir,
            config,
            store,
        }
    }

    fn snapshot(&self) -> distill_store::served::StoreSnapshot {
        StoreReader::open(self.config.clone())
            .unwrap()
            .begin_snapshot()
            .unwrap()
    }

    fn input(
        &mut self,
        f: impl FnOnce(&mut InputTxn<'_>) -> Result<(), StoreError>,
    ) -> InputVersion {
        self.store.input_transaction(f).unwrap().1
    }
}

fn put_bundle(
    txn: &mut InputTxn<'_>,
    uuid: BundleUuid,
    root: i64,
    path: &str,
    hash: u8,
) -> Result<(), StoreError> {
    txn.upsert_bundle(&BundleMeta {
        bundle: uuid,
        root: RootId(root),
        path: path.to_owned(),
        format_version: 1,
        content_hash: ContentHash([hash; 32]),
        origin: None,
        import_watched: false,
    })
}

fn put_asset(
    txn: &mut InputTxn<'_>,
    uuid: AssetUuid,
    owner: BundleUuid,
    local_id: &str,
    type_uuid: TypeUuid,
    authoring_only: bool,
    tags: &[(&str, Option<&str>)],
) -> Result<(), StoreError> {
    txn.upsert_asset(&AssetRecord {
        asset: uuid,
        bundle: owner,
        local_id: local_id.to_owned(),
        type_uuid,
        logical_hash: LogicalHash([7; 32]),
        authoring_only,
        tags: tags
            .iter()
            .map(|(tag, value)| ((*tag).to_owned(), value.map(str::to_owned)))
            .collect(),
        terminal_type: None,
    })
}

/// A project with every kind of row a trace reads: runtime and
/// authoring-only entries of registered, chained, unregistered and
/// off-target types; tags with and without values; tag-index poisons;
/// a path in two roots (ambiguous), a path two roots give the same asset,
/// a path to a missing asset; derived children; tools and a tombstone.
fn fixture() -> (Project, InputVersion) {
    let mut project = Project::new();
    let first_tools = project.input(|txn| {
        txn.register_tool("tool-a", tool_package(b"a v1"))?;
        txn.register_tool("tool-b", tool_package(b"b v1"))?;
        Ok(())
    });
    project.input(|txn| {
        put_bundle(txn, bundle(1), 1, "textures/a.bundle", 1)?;
        put_bundle(txn, bundle(2), 1, "textures/b.bundle", 2)?;
        put_bundle(txn, bundle(3), 1, "models/m.bundle", 3)?;
        put_bundle(txn, bundle(4), 1, "textures/sub/c.bundle", 4)?;
        put_bundle(txn, bundle(5), 1, "\u{f6}/x.bundle", 5)?;
        put_bundle(txn, bundle(6), 2, "textures/a.bundle", 6)?;
        let albedo = ("kind", Some("albedo"));
        put_asset(
            txn,
            asset(1),
            bundle(1),
            "one",
            TA,
            false,
            &[albedo, ("flag", None)],
        )?;
        put_asset(
            txn,
            asset(2),
            bundle(1),
            "two",
            TB,
            false,
            &[("kind", Some("normal"))],
        )?;
        put_asset(txn, asset(3), bundle(1), "three", TA, true, &[albedo])?;
        put_asset(txn, asset(4), bundle(2), "one", TC, false, &[albedo])?;
        put_asset(txn, asset(5), bundle(2), "two", TB, false, &[])?;
        put_asset(
            txn,
            asset(6),
            bundle(3),
            "one",
            TA,
            false,
            &[("flag", None)],
        )?;
        put_asset(txn, asset(7), bundle(3), "two", TD, false, &[albedo])?;
        put_asset(txn, asset(8), bundle(4), "one", TB, false, &[albedo])?;
        put_asset(
            txn,
            asset(9),
            bundle(5),
            "one",
            TC,
            false,
            &[("flag", Some("x"))],
        )?;
        put_asset(txn, asset(10), bundle(6), "one", TB, false, &[albedo])?;
        put_asset(
            txn,
            asset(12),
            bundle(3),
            "three",
            TB,
            true,
            &[("flag", None)],
        )?;
        for poisoned in [asset(2), asset(4), asset(3), asset(8)] {
            txn.set_tag_index_pending(poisoned, [3; 32])?;
        }
        txn.set_path_entry("textures/a.bundle", RootId(1), asset(1))?;
        txn.set_path_entry("textures/a.bundle", RootId(2), asset(10))?;
        txn.set_path_entry("textures/b.bundle", RootId(1), asset(4))?;
        txn.set_path_entry("textures/b.bundle", RootId(2), asset(4))?;
        txn.set_path_entry("models/m.bundle", RootId(1), asset(6))?;
        txn.set_path_entry("textures/sub/c.bundle", RootId(1), asset(8))?;
        txn.set_path_entry("ghost.bundle", RootId(1), GHOST)?;
        txn.set_derived_output(AssetUuid::v5(asset(1), "meta"), asset(1), "meta", EXTRA_A)?;
        txn.set_derived_output(AssetUuid::v5(asset(6), "meta"), asset(6), "meta", EXTRA_A)?;
        Ok(())
    });
    (project, first_tools)
}

/// The second snapshot: a moved path, a role change, a rehashed bundle, a
/// new and a cleared tag poison, a new asset, a retired child, a removed
/// bundle, and a tool epoch that drops one tool and adds another.
fn mutate(project: &mut Project) {
    project.input(|txn| {
        txn.set_path_entry("models/m.bundle", RootId(1), asset(1))?;
        put_asset(
            txn,
            asset(2),
            bundle(1),
            "two",
            TB,
            true,
            &[("kind", Some("normal"))],
        )?;
        put_bundle(txn, bundle(2), 1, "textures/b.bundle", 22)?;
        txn.set_tag_index_pending(asset(5), [3; 32])?;
        put_asset(
            txn,
            asset(11),
            bundle(1),
            "four",
            TA,
            false,
            &[("kind", Some("albedo"))],
        )?;
        txn.remove_derived_output(AssetUuid::v5(asset(6), "meta"))?;
        txn.remove_bundle(bundle(4))?;
        txn.publish_tool_epoch(&BTreeMap::from([
            ("tool-b".to_owned(), tool_package(b"b v2")),
            ("tool-c".to_owned(), tool_package(b"c v1")),
        ]))?;
        Ok(())
    });
}

fn memo() -> BTreeMap<AssetUuid, NodeResult> {
    BTreeMap::from([
        (
            asset(1),
            NodeResult {
                trace: Vec::new(),
                outputs: BTreeMap::from([
                    (String::new(), ContentHash([0x11; 32])),
                    ("meta".to_owned(), ContentHash([0x12; 32])),
                ]),
            },
        ),
        (
            asset(5),
            NodeResult {
                trace: Vec::new(),
                outputs: BTreeMap::from([(String::new(), ContentHash([0x15; 32]))]),
            },
        ),
    ])
}

fn eager(
    store: &StoreReader,
    registry: &PipelineRegistry,
    target: &Target,
    tools_at: InputVersion,
    memo: &BTreeMap<AssetUuid, NodeResult>,
) -> EagerTraceSource {
    let mut source = EagerTraceSource::capture(
        store,
        EagerBasis {
            registry,
            target,
            input_version: tools_at,
            current_load: current_load(),
        },
    )
    .unwrap();
    source.content_hashes = node_content_hashes(memo.iter().map(|(a, n)| (*a, n))).unwrap();
    source
}

fn lazy<'a>(
    store: &'a StoreReader,
    registry: &'a PipelineRegistry,
    target: &'a Target,
    tools_at: InputVersion,
    current_load: &'a CurrentLoadSource,
    memo: &'a BTreeMap<AssetUuid, NodeResult>,
) -> StoreTraceSource<'a> {
    StoreTraceSource::new(
        store,
        TraceBasis {
            registry,
            target,
            tool_version: tools_at,
        },
        current_load,
        BuiltNodes::Memo(memo),
    )
}

fn assets() -> Vec<AssetUuid> {
    let mut assets = (1..=12).map(asset).collect::<Vec<_>>();
    assets.extend([
        AssetUuid::v5(asset(1), "meta"),
        AssetUuid::v5(asset(6), "meta"),
        AssetUuid::v5(asset(2), "meta"),
        MISSING,
        GHOST,
    ]);
    assets
}

const PATHS: &[&str] = &[
    "textures/a.bundle",
    "textures/b.bundle",
    "models/m.bundle",
    "textures/sub/c.bundle",
    "\u{f6}/x.bundle",
    "ghost.bundle",
    "nope.bundle",
];

const TOOLS: &[&str] = &["tool-a", "tool-b", "tool-c", "tool-none"];

/// A broad, deterministic set of queries: every single selector, every
/// pair of selectors, and pseudo-random combinations of three to five.
fn queries() -> Vec<AssetQuery> {
    type Setter = Box<dyn Fn(&mut AssetQuery)>;
    let tag = |name: &'static str, value: Option<&'static str>| -> Setter {
        Box::new(move |query: &mut AssetQuery| {
            query.tag = Some(TagSelector {
                tag: name.to_owned(),
                value: value.map(str::to_owned),
            })
        })
    };
    let mut selectors: Vec<Vec<Setter>> = Vec::new();
    selectors.push(
        [asset(1), asset(3), asset(9), MISSING]
            .into_iter()
            .map(|uuid| -> Setter { Box::new(move |q: &mut AssetQuery| q.uuid = Some(uuid)) })
            .collect(),
    );
    selectors.push(
        ["textures/a.bundle", "models/m.bundle", "nope.bundle"]
            .into_iter()
            .map(|path| -> Setter {
                Box::new(move |q: &mut AssetQuery| q.bundle_path = Some(path.to_owned()))
            })
            .collect(),
    );
    selectors.push(
        ["one", "two", "three"]
            .into_iter()
            .map(|id| -> Setter {
                Box::new(move |q: &mut AssetQuery| q.local_id = Some(id.to_owned()))
            })
            .collect(),
    );
    selectors.push(
        [bundle(1), bundle(3), bundle(9)]
            .into_iter()
            .map(|b| -> Setter { Box::new(move |q: &mut AssetQuery| q.bundle_uuid = Some(b)) })
            .collect(),
    );
    selectors.push(
        [TA, TB, TC, TD]
            .into_iter()
            .map(|t| -> Setter { Box::new(move |q: &mut AssetQuery| q.authored_type = Some(t)) })
            .collect(),
    );
    selectors.push(
        [TERM_A, TB, EXTRA_A, TD, TC]
            .into_iter()
            .map(|t| -> Setter { Box::new(move |q: &mut AssetQuery| q.terminal_type = Some(t)) })
            .collect(),
    );
    selectors.push(vec![
        tag("kind", None),
        tag("kind", Some("albedo")),
        tag("flag", None),
        tag("flag", Some("x")),
        tag("none", None),
    ]);
    selectors.push(
        [
            "textures/",
            "textures/sub",
            "\u{f6}",
            "models/m.bundle",
            "z",
            "",
        ]
        .into_iter()
        .map(|p| -> Setter {
            Box::new(move |q: &mut AssetQuery| q.path_prefix = Some(p.to_owned()))
        })
        .collect(),
    );
    selectors.push(
        [
            "textures/*.bundle",
            "**/*.bundle",
            "textures/s?b/*",
            "[",
            "models/{m,n}.bundle",
            "*",
            "textures/[ab].bundle",
            "\u{f6}/x.bundle",
            "**/c.bundle",
            "*/a.bundle",
            "**/x.bundle",
            "*.bundle",
        ]
        .into_iter()
        .map(|g| -> Setter { Box::new(move |q: &mut AssetQuery| q.path_glob = Some(g.to_owned())) })
        .collect(),
    );
    selectors.push(
        [false, true]
            .into_iter()
            .map(|b| -> Setter { Box::new(move |q: &mut AssetQuery| q.authoring_only = Some(b)) })
            .collect(),
    );

    let mut queries = vec![AssetQuery::default()];
    for (s, options) in selectors.iter().enumerate() {
        for option in options {
            let mut query = AssetQuery::default();
            option(&mut query);
            queries.push(query.clone());
            for later in &selectors[s + 1..] {
                for other in later {
                    let mut pair = query.clone();
                    other(&mut pair);
                    queries.push(pair);
                }
            }
        }
    }
    let mut seed = 0x2545_f491_4f6c_dd1d_u64;
    let mut next = |bound: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % bound as u64) as usize
    };
    for _ in 0..1500 {
        let mut query = AssetQuery::default();
        for _ in 0..3 + next(3) {
            let options = &selectors[next(selectors.len())];
            options[next(options.len())](&mut query);
        }
        queries.push(query);
    }
    queries
}

/// Every question both sources answer, as trace operations holding the
/// eager answer.
fn questions(source: &EagerTraceSource, queries: &[AssetQuery]) -> Vec<TraceOp> {
    let mut ops = Vec::new();
    for asset in assets() {
        ops.push(TraceOp::AuthoringRead {
            asset,
            observed: source.authoring_read(asset),
        });
        ops.push(TraceOp::Read {
            asset,
            observed: source.read(asset),
        });
        ops.push(TraceOp::RefCheck {
            asset,
            expected_terminal: TERM_A,
            observed: source.ref_check(asset, TERM_A),
        });
        ops.push(TraceOp::RoleCheck {
            asset,
            observed: source.role_check(asset),
        });
    }
    for path in PATHS {
        ops.push(TraceOp::Resolve {
            path: (*path).to_owned(),
            observed: source.resolve(path),
        });
    }
    for query in queries {
        ops.push(TraceOp::Query {
            query: Box::new(query.clone()),
            observed: source.query(query),
        });
    }
    for id in TOOLS {
        ops.push(TraceOp::Tool {
            id: (*id).to_owned(),
            observed: source.tool(id),
        });
    }
    for key in [
        CapabilityKey::DefaultTable(TA),
        CapabilityKey::DefaultTable(TB),
        CapabilityKey::Tool("tool-a".to_owned()),
    ] {
        ops.push(TraceOp::Capability {
            observed: source.capability(&key),
            key,
        });
    }
    ops
}

/// Ask `lazy` every question in `ops` and compare with the eager answer
/// recorded there; also compare query results.
fn assert_same_answers(
    ops: &[TraceOp],
    eager: &EagerTraceSource,
    lazy: &StoreTraceSource<'_>,
) -> usize {
    let mut errors = 0;
    for op in ops {
        let same = match op {
            TraceOp::AuthoringRead { asset, observed } => &lazy.authoring_read(*asset) == observed,
            TraceOp::Read { asset, observed } => &lazy.read(*asset) == observed,
            TraceOp::RefCheck {
                asset,
                expected_terminal,
                observed,
            } => &lazy.ref_check(*asset, *expected_terminal) == observed,
            TraceOp::RoleCheck { asset, observed } => &lazy.role_check(*asset) == observed,
            TraceOp::Resolve { path, observed } => &lazy.resolve(path) == observed,
            TraceOp::Query { query, observed } => {
                // A failed query has no results.
                &lazy.query(query) == observed
                    && (matches!(observed, Observed::Err(_))
                        || lazy.query_results(query) == eager.query_results(query))
            }
            TraceOp::Tool { id, observed } => &lazy.tool(id) == observed,
            TraceOp::Capability { key, observed } => &lazy.capability(key) == observed,
            TraceOp::Control { .. } | TraceOp::ControlRead { .. } => true,
        };
        if !same {
            errors += 1;
            eprintln!("lazy answer differs: {op:?}");
        }
    }
    lazy.check().unwrap();
    errors
}

#[test]
fn store_source_answers_every_question_as_the_eager_capture_did() {
    let (mut project, first_tools) = fixture();
    let registry = registry();
    let target = target();
    let memo = memo();
    let load = current_load();
    let queries = queries();

    let before = project.snapshot();
    mutate(&mut project);
    let after = project.snapshot();

    let mut checked = 0;
    let mut recorded = Vec::new();
    for (view, tool_versions) in [
        (&before, vec![first_tools, before.stamp().version]),
        (&after, vec![first_tools, after.stamp().version]),
    ] {
        for tools_at in tool_versions {
            let eager = eager(view, &registry, &target, tools_at, &memo);
            let ops = questions(&eager, &queries);
            let lazy = lazy(view, &registry, &target, tools_at, &load, &memo);
            assert_eq!(assert_same_answers(&ops, &eager, &lazy), 0);
            checked += ops.len();
            if std::ptr::eq(view, &before) && tools_at == before.stamp().version {
                recorded = ops;
            }
        }
    }
    assert!(checked > 4 * 2000, "{checked} questions");

    // The questions cover every kind of answer.
    let observed = |pick: &dyn Fn(&TraceOp) -> bool| recorded.iter().any(pick);
    assert!(observed(&|op| matches!(
        op,
        TraceOp::Resolve {
            observed: Observed::Err(StableFailureFingerprint::Ambiguous { .. }),
            ..
        }
    )));
    assert!(observed(&|op| matches!(
        op,
        TraceOp::Resolve {
            observed: Observed::Ok(Some(GHOST)),
            ..
        }
    )));
    assert!(observed(&|op| matches!(
        op,
        TraceOp::Query {
            observed: Observed::Err(StableFailureFingerprint::Poisoned { .. }),
            ..
        }
    )));
    assert!(observed(
        &|op| matches!(op, TraceOp::Read { observed: Observed::Err(StableFailureFingerprint::MissingRef { expected_terminal, .. }), .. } if *expected_terminal == TERM_A)
    ));
    assert!(observed(&|op| matches!(
        op,
        TraceOp::Read {
            observed: Observed::Ok(_),
            ..
        }
    )));
    assert!(observed(&|op| matches!(
        op,
        TraceOp::RoleCheck {
            observed: Observed::Ok(Some(EntryRole::AuthoringOnly)),
            ..
        }
    )));
    assert!(observed(
        &|op| matches!(op, TraceOp::RefCheck { observed: Observed::Ok(Some(t)), .. } if *t == EXTRA_A)
    ));
    assert!(observed(&|op| matches!(
        op,
        TraceOp::Tool {
            observed: Observed::Err(_),
            ..
        }
    )));
    let nonempty_queries = recorded
        .iter()
        .filter(|op| match op {
            TraceOp::Query {
                query,
                observed: Observed::Ok(hash),
            } => *hash != asset_query_result_hash(&[]) && query.selector_count() > 1,
            _ => false,
        })
        .count();
    assert!(
        nonempty_queries > 100,
        "{nonempty_queries} non-empty multi-selector queries"
    );
}

#[test]
fn store_source_revalidates_every_trace_as_the_eager_capture_did() {
    let (mut project, _) = fixture();
    let registry = registry();
    let target = target();
    let memo = memo();
    let load = current_load();
    let queries = queries();

    let before = project.snapshot();
    let tools_before = before.stamp().version;
    let recorded = questions(
        &eager(&before, &registry, &target, tools_before, &memo),
        &queries,
    );
    mutate(&mut project);
    let after = project.snapshot();
    let tools_after = after.stamp().version;

    // Every single question, and traces of a few questions each.
    let mut traces = recorded
        .iter()
        .map(|op| vec![op.clone()])
        .collect::<Vec<_>>();
    for width in [3, 5, 8] {
        for (start, _) in recorded.iter().enumerate().step_by(width + 1) {
            traces.push(
                (0..width)
                    .map(|offset| recorded[(start * 7 + offset * 13) % recorded.len()].clone())
                    .collect(),
            );
        }
    }

    for (view, tools_at, all_hold) in [(&before, tools_before, true), (&after, tools_after, false)]
    {
        let eager = eager(view, &registry, &target, tools_at, &memo);
        let lazy = lazy(view, &registry, &target, tools_at, &load, &memo);
        let mut drifted = 0;
        for trace in &traces {
            let verdict = revalidate(trace, &eager);
            assert_eq!(revalidate(trace, &lazy), verdict, "{trace:?}");
            drifted += usize::from(!verdict);
        }
        lazy.check().unwrap();
        if all_hold {
            assert_eq!(drifted, 0);
        } else {
            assert!(drifted > 50, "{drifted} of {} traces drifted", traces.len());
            assert!(drifted < traces.len());
        }
    }
}

#[test]
fn a_poisoned_bundle_fails_only_the_questions_that_reach_it() {
    let (mut project, _) = fixture();
    project.input(|txn| {
        txn.poison_bundle(
            &NamespaceSkeleton {
                bundle: bundle(7),
                root: RootId(1),
                path: "broken.bundle".to_owned(),
                format_version: 1,
                content_hash: ContentHash([7; 32]),
                entries: vec![SkeletonEntry {
                    asset: asset(70),
                    local_id: "one".to_owned(),
                    type_uuid: TB,
                    authoring_only: false,
                    tags: BTreeMap::new(),
                }],
            },
            "malformed",
        )
    });
    let registry = registry();
    let target = target();
    let memo = BTreeMap::new();
    let load = current_load();
    let view = project.snapshot();
    let tools_at = view.stamp().version;
    // The eager capture read every entry, so one poisoned bundle failed
    // every question of every build.
    assert!(EagerTraceSource::capture(
        &view,
        EagerBasis {
            registry: &registry,
            target: &target,
            input_version: tools_at,
            current_load: current_load(),
        },
    )
    .is_err());

    let source = lazy(&view, &registry, &target, tools_at, &load, &memo);
    assert_eq!(
        source.authoring_read(asset(1)),
        Observed::Ok(Some(BundleFileHash([1; 32])))
    );
    assert_eq!(
        source.query(&AssetQuery {
            bundle_uuid: Some(bundle(1)),
            ..AssetQuery::default()
        }),
        Observed::Ok(asset_query_result_hash(&[asset(1), asset(2)]))
    );
    source.check().unwrap();

    source.role_check(asset(70));
    let failure = source.check();
    assert!(
        matches!(&failure, Err(BuildError::Failed(message)) if message.contains("BundlePoisoned")),
        "{failure:?}"
    );
    // A query whose selectors match the poisoned skeleton fails naming the
    // bundle, as an answer a trace records; one that cannot reach it
    // answers.
    assert_eq!(
        source.query(&AssetQuery {
            local_id: Some("one".to_owned()),
            ..AssetQuery::default()
        }),
        Observed::Err(StableFailureFingerprint::Poisoned { bundle: bundle(7) })
    );
    assert_eq!(
        source.query(&AssetQuery {
            local_id: Some("one".to_owned()),
            authored_type: Some(TA),
            ..AssetQuery::default()
        }),
        Observed::Ok(asset_query_result_hash(&[asset(1), asset(6)]))
    );
    source.check().unwrap();
    // A reference resolved through such a query fails the build.
    assert!(source
        .query_results(&AssetQuery {
            terminal_type: Some(TB),
            ..AssetQuery::default()
        })
        .is_empty());
    assert!(source.check().is_err());
}

thread_local! {
    static STATEMENTS: Cell<Vec<String>> = const { Cell::new(Vec::new()) };
}

fn record_statement(sql: &str) {
    STATEMENTS.with(|statements| statements.borrow_mut().push(sql.to_owned()));
}

fn take_statements() -> Vec<String> {
    STATEMENTS.with(|statements| std::mem::take(&mut *statements.borrow_mut()))
}

/// With 20 000 assets in the store, revalidating a seven-question trace
/// runs under twenty statements, each planned on an index; the eager capture
/// ran tens of thousands. Its queries name an exact path and local id, a
/// glob whose final segment is literal beside a type every other asset
/// shares, and a local id alone.
#[test]
fn revalidating_a_trace_reads_only_what_it_asks_about() {
    const BUNDLES: usize = 200;
    const PER_BUNDLE: usize = 100;
    let mut project = Project::new();
    let uuid = |b: usize, a: usize| {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&(b as u64).to_be_bytes());
        bytes[8..].copy_from_slice(&(a as u64).to_be_bytes());
        AssetUuid(bytes)
    };
    let bundle_uuid = |b: usize| {
        let mut bytes = [0xb0; 16];
        bytes[8..].copy_from_slice(&(b as u64).to_be_bytes());
        BundleUuid(bytes)
    };
    project.input(|txn| {
        txn.register_tool("tool-a", tool_package(b"a"))?;
        for b in 0..BUNDLES {
            let path = format!("level{}/bundle{b}.bundle", b % 10);
            put_bundle(txn, bundle_uuid(b), 1, &path, b as u8)?;
            for a in 0..PER_BUNDLE {
                let type_uuid = if a % 2 == 0 { TA } else { TB };
                let kind = if a % 3 == 0 { "albedo" } else { "normal" };
                put_asset(
                    txn,
                    uuid(b, a),
                    bundle_uuid(b),
                    &format!("a{a}"),
                    type_uuid,
                    false,
                    &[("kind", Some(kind))],
                )?;
            }
            txn.set_path_entry(&path, RootId(1), uuid(b, 0))?;
            txn.set_derived_output(
                AssetUuid::v5(uuid(b, 0), "meta"),
                uuid(b, 0),
                "meta",
                EXTRA_A,
            )?;
        }
        // One bundle whose name and local id no other shares.
        put_bundle(txn, bundle_uuid(BUNDLES), 1, "extra/solo.bundle", 0xee)?;
        put_asset(txn, uuid(BUNDLES, 0), bundle_uuid(BUNDLES), "solo", TA, false, &[])?;
        Ok(())
    });
    let registry = registry();
    let target = target();
    let memo = BTreeMap::new();
    let load = current_load();

    // Record a trace at one snapshot: a path, the asset it names (its
    // authoring bytes, role and terminal type) and a reference query.
    let path = "level7/bundle117.bundle".to_owned();
    let trace = {
        let view = project.snapshot();
        let tools_at = view.stamp().version;
        let source = lazy(&view, &registry, &target, tools_at, &load, &memo);
        let named = match source.resolve(&path) {
            Observed::Ok(Some(named)) => named,
            other => panic!("{other:?}"),
        };
        let query = AssetQuery {
            bundle_path: Some(path.clone()),
            local_id: Some("a5".to_owned()),
            ..AssetQuery::default()
        };
        let by_name = AssetQuery {
            path_glob: Some("**/solo.bundle".to_owned()),
            authored_type: Some(TA),
            ..AssetQuery::default()
        };
        let by_local_id = AssetQuery {
            local_id: Some("solo".to_owned()),
            ..AssetQuery::default()
        };
        let trace = vec![
            TraceOp::Resolve {
                path: path.clone(),
                observed: source.resolve(&path),
            },
            TraceOp::AuthoringRead {
                asset: named,
                observed: source.authoring_read(named),
            },
            TraceOp::RoleCheck {
                asset: named,
                observed: source.role_check(named),
            },
            TraceOp::RefCheck {
                asset: named,
                expected_terminal: TERM_A,
                observed: source.ref_check(named, TERM_A),
            },
            TraceOp::Query {
                observed: source.query(&query),
                query: Box::new(query),
            },
            TraceOp::Query {
                observed: source.query(&by_name),
                query: Box::new(by_name.clone()),
            },
            TraceOp::Query {
                observed: source.query(&by_local_id),
                query: Box::new(by_local_id.clone()),
            },
        ];
        source.check().unwrap();
        assert_eq!(
            source.query_results(match &trace[4] {
                TraceOp::Query { query, .. } => query,
                _ => unreachable!(),
            }),
            vec![uuid(117, 5)]
        );
        for query in [&by_name, &by_local_id] {
            assert_eq!(source.query_results(query), vec![uuid(BUNDLES, 0)]);
        }
        trace
    };

    // Revalidate it at a fresh snapshot, counting every statement.
    let mut reader = StoreReader::open(project.config.clone()).unwrap();
    reader.trace_statements(Some(record_statement));
    let view = reader.begin_snapshot().unwrap();
    let tools_at = view.stamp().version;
    take_statements();
    let source = lazy(&view, &registry, &target, tools_at, &load, &memo);
    let started = std::time::Instant::now();
    assert!(revalidate(&trace, &source));
    let lazy_time = started.elapsed();
    source.check().unwrap();
    let statements = take_statements();
    assert!(
        statements.len() <= 20,
        "{} statements: {statements:#?}",
        statements.len()
    );
    for statement in &statements {
        let plan = view.query_plan_details(statement).unwrap();
        assert!(
            plan.iter().all(|step| !step.starts_with("SCAN ")),
            "{statement}: {plan:?}"
        );
    }

    // The same trace through the eager capture.
    let started = std::time::Instant::now();
    let eager = eager(&view, &registry, &target, tools_at, &memo);
    assert!(revalidate(&trace, &eager));
    let eager_time = started.elapsed();
    let eager_statements = take_statements().len();
    assert!(
        eager_statements > BUNDLES * PER_BUNDLE,
        "{eager_statements}"
    );
    eprintln!(
        "revalidating a 7-question trace over {} assets: store source {} statements in {lazy_time:?}, eager capture {eager_statements} statements in {eager_time:?}",
        BUNDLES * PER_BUNDLE,
        statements.len(),
    );
}
