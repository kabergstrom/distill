//! Trace answers read from the store at one snapshot.
//!
//! A build's trace records what it observed of the project; a cached
//! result serves a snapshot where every recorded question gets the same
//! answer there. [`StoreTraceSource`] answers each question when it is
//! asked, by indexed reads in the snapshot's read transaction: an entry is
//! one primary-key read (`distill_store::trace_reads`), a path one
//! `bundles_by_path` search, a query one asset query driven by its most selective
//! selector (DESIGN.md §13, asset queries), a tool one ToolEpoch row.
//! Nothing reads the whole project unless a query names no indexed
//! selector at all (its answer may then be the whole project). Nothing is
//! kept between questions: SQLite is the one copy of the snapshot.
//!
//! The source answers infallibly, as [`TraceSource`] requires. A store
//! failure while answering is kept and reported by
//! [`StoreTraceSource::check`]; every use goes through
//! [`super::ask_trace`] or calls it, so no answer read past a failure
//! decides anything.

use std::cell::{OnceCell, RefCell};
use std::collections::{BTreeMap, BTreeSet};

use distill_build::pipeline::{PipelineRegistry, Target};
use distill_build::query::{asset_query_result_hash, AssetQuery};
use distill_build::trace::{
    CapabilityKey, ControlQuery, ControlSubject, ControlValueHash, EntryRole, Observed,
    StableFailureFingerprint, TraceSource,
};
use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, ContentHash, TypeUuid};
use distill_store::bundles::AssetFilter;
use distill_store::files::{GlobKeys, GLOBSET_META};
use distill_store::state::InputVersion;
use distill_store::trace_reads::TraceEntry;
use distill_store::StoreReader;

use super::{no_trace, node_content_hashes, BuildError, CurrentLoadSource, NodeResult};

/// Where a source finds the contents of the nodes built so far: a build's
/// memo, or a lookup's nodes.
#[derive(Clone, Copy)]
pub(super) enum BuiltNodes<'a> {
    Memo(&'a BTreeMap<AssetUuid, NodeResult>),
    Lookup(&'a BTreeMap<AssetUuid, Option<NodeResult>>),
}

/// The fixed facts a source answers against besides the store.
#[derive(Clone, Copy)]
pub(super) struct TraceBasis<'a> {
    pub(super) registry: &'a PipelineRegistry,
    pub(super) target: &'a Target,
    /// The input version tool questions are answered at.
    pub(super) tool_version: InputVersion,
}

pub(super) struct StoreTraceSource<'a> {
    store: &'a StoreReader,
    basis: TraceBasis<'a>,
    current_load: &'a CurrentLoadSource,
    built: BuiltNodes<'a>,
    /// The built nodes' contents by asset, gathered on the first `read`
    /// (from the nodes, not the store).
    contents: OnceCell<BTreeMap<AssetUuid, ContentHash>>,
    failure: RefCell<Option<BuildError>>,
}

/// Answers the eager trace capture and the store source both give, for
/// helpers that need a query's results as well as its hash.
pub(super) trait TraceQueries: TraceSource {
    /// The runtime assets `query` selects, in asset order.
    fn query_results(&self, query: &AssetQuery) -> Vec<AssetUuid>;
}

/// The asset filter of a runtime asset query (a build's or a codegen's):
/// every selector but the terminal type, which depends on the target, and
/// the glob, which contributes its keys (when it compiles) and is matched
/// on the rows. `authoring_only` is not a selector here: such a query
/// selects runtime rows only.
pub(crate) fn runtime_asset_filter(query: &AssetQuery) -> AssetFilter {
    let filter = AssetFilter {
        asset: query.uuid,
        bundle: query.bundle_uuid,
        bundle_path: query.bundle_path.clone(),
        local_id: query.local_id.clone(),
        authored_type: query.authored_type,
        tag: query
            .tag
            .as_ref()
            .map(|tag| (tag.tag.clone(), tag.value.clone())),
        path_prefixes: query.path_prefix.iter().cloned().collect(),
        authoring_only: Some(false),
        ..AssetFilter::default()
    };
    match query
        .path_glob
        .as_deref()
        .filter(|pattern| globset::Glob::new(pattern).is_ok())
    {
        Some(pattern) => filter.with_glob_keys(GlobKeys::of(pattern, GLOBSET_META)),
        None => filter,
    }
}

/// The matcher of a query's glob; an invalid glob selects every path.
pub(super) fn query_glob(query: &AssetQuery) -> Option<globset::GlobMatcher> {
    query
        .path_glob
        .as_ref()
        .and_then(|pattern| globset::Glob::new(pattern).ok())
        .map(|glob| glob.compile_matcher())
}

impl<'a> StoreTraceSource<'a> {
    pub(super) fn new(
        store: &'a StoreReader,
        basis: TraceBasis<'a>,
        current_load: &'a CurrentLoadSource,
        built: BuiltNodes<'a>,
    ) -> Self {
        Self {
            store,
            basis,
            current_load,
            built,
            contents: OnceCell::new(),
            failure: RefCell::new(None),
        }
    }

    /// The first store failure met while answering, if any. Answers given
    /// after one are placeholders and must not be used.
    pub(super) fn check(&self) -> Result<(), BuildError> {
        match self.failure.borrow_mut().take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn answer<T>(&self, answer: Result<Observed<T>, BuildError>) -> Observed<T> {
        answer.unwrap_or_else(|error| {
            self.failure.borrow_mut().get_or_insert(error);
            no_trace()
        })
    }

    /// The asset row of `asset`: one primary-key read of `assets` joined to
    /// its bundle's row.
    fn entry(&self, asset: AssetUuid) -> Result<Option<TraceEntry>, BuildError> {
        self.store.trace_entry(asset).map_err(BuildError::failed)
    }

    /// The terminal type of an asset of authored type `authored`.
    fn chain_terminal(&self, authored: TypeUuid) -> Result<TypeUuid, BuildError> {
        Ok(self
            .basis
            .registry
            .chain(authored, self.basis.target)
            .map_err(BuildError::failed)?
            .terminal)
    }

    /// The terminal type of `asset` as a derived child of its parent's
    /// chain: its derived-output claim and the errors that would withhold it.
    fn derived_terminal(&self, asset: AssetUuid) -> Result<Option<TypeUuid>, BuildError> {
        let Some((parent, output_key)) = self
            .store
            .resolve_child(asset)
            .map_err(BuildError::infrastructure)?
        else {
            return Ok(None);
        };
        let parent = self.entry(parent)?.ok_or_else(|| {
            BuildError::Infrastructure("derived parent is absent from trace index".to_owned())
        })?;
        self.basis
            .registry
            .chain(parent.type_uuid, self.basis.target)
            .map_err(BuildError::failed)?
            .extras
            .get(&output_key)
            .copied()
            .map(Some)
            .ok_or_else(|| {
                BuildError::Infrastructure(
                    "derived output is absent from the pinned pipeline map".to_owned(),
                )
            })
    }

    fn terminal_type(&self, asset: AssetUuid) -> Result<Option<TypeUuid>, BuildError> {
        match self.derived_terminal(asset)? {
            Some(terminal) => Ok(Some(terminal)),
            None => self
                .entry(asset)?
                .map(|entry| self.chain_terminal(entry.type_uuid))
                .transpose(),
        }
    }

    fn role(&self, asset: AssetUuid) -> Result<Option<EntryRole>, BuildError> {
        match self.derived_terminal(asset)? {
            Some(_) => Ok(Some(EntryRole::Runtime)),
            None => Ok(self.entry(asset)?.map(|entry| {
                if entry.authoring_only {
                    EntryRole::AuthoringOnly
                } else {
                    EntryRole::Runtime
                }
            })),
        }
    }

    fn contents(&self) -> Result<&BTreeMap<AssetUuid, ContentHash>, BuildError> {
        if self.contents.get().is_none() {
            let contents = match self.built {
                BuiltNodes::Memo(memo) => {
                    node_content_hashes(memo.iter().map(|(asset, node)| (*asset, node)))?
                }
                BuiltNodes::Lookup(nodes) => node_content_hashes(
                    nodes
                        .iter()
                        .filter_map(|(asset, node)| node.as_ref().map(|node| (*asset, node))),
                )?,
            };
            let _ = self.contents.set(contents);
        }
        Ok(self.contents.get().expect("set above"))
    }

    /// The authored types an asset of terminal type `terminal` can have:
    /// those whose chains end there, and those whose chains fail (an asset
    /// of one fails any query reaching it).
    fn terminal_sources(&self, terminal: TypeUuid) -> Vec<TypeUuid> {
        // A type no registration takes ends its own chain, so only the
        // terminal itself and registered input types can end at it.
        std::iter::once(terminal)
            .chain(self.basis.registry.input_types())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|authored| {
                self.basis
                    .registry
                    .chain(*authored, self.basis.target)
                    .map_or(true, |chain| chain.terminal == terminal)
            })
            .collect()
    }

    /// The runtime assets `query` selects, in asset order, by one asset
    /// query; or the least poisoned bundle it reaches (DESIGN.md §13,
    /// asset queries). A terminal type selects the authored types whose
    /// chains end at it on this target.
    fn try_query_results(
        &self,
        query: &AssetQuery,
    ) -> Result<Result<Vec<AssetUuid>, BundleUuid>, BuildError> {
        let mut filter = runtime_asset_filter(query);
        if let Some(terminal) = query.terminal_type {
            filter.authored_type_in = Some(self.terminal_sources(terminal));
        }
        let glob = query_glob(query);
        let answer = self
            .store
            .namespace_assets_matching(&filter, |path| {
                glob.as_ref().is_none_or(|glob| glob.is_match(path))
            })
            .map_err(BuildError::infrastructure)?;
        let matched = match answer {
            Ok(matched) => matched,
            Err(bundles) => return Ok(Err(*bundles.first().expect("a poison names its bundle"))),
        };
        let mut results = Vec::with_capacity(matched.len());
        for matched in matched {
            // A selected type whose chain fails fails the query.
            if let Some(terminal) = query.terminal_type {
                if self.chain_terminal(matched.type_uuid)? != terminal {
                    continue;
                }
            }
            results.push(matched.asset);
        }
        Ok(Ok(results))
    }
}

impl TraceQueries for StoreTraceSource<'_> {
    fn query_results(&self, query: &AssetQuery) -> Vec<AssetUuid> {
        let failure = match self.try_query_results(query) {
            Ok(Ok(results)) => return results,
            Ok(Err(bundle)) => BuildError::Failed(format!(
                "asset query {query:?} reaches poisoned bundle {bundle}"
            )),
            Err(error) => error,
        };
        self.failure.borrow_mut().get_or_insert(failure);
        Vec::new()
    }
}

impl TraceSource for StoreTraceSource<'_> {
    fn authoring_read(&self, asset: AssetUuid) -> Observed<Option<BundleFileHash>> {
        self.answer(
            self.entry(asset)
                .map(|entry| Observed::Ok(entry.map(|entry| BundleFileHash(entry.bundle_hash.0)))),
        )
    }

    fn read(&self, asset: AssetUuid) -> Observed<ContentHash> {
        self.answer((|| {
            if let Some(hash) = self.contents()?.get(&asset) {
                return Ok(Observed::Ok(*hash));
            }
            Ok(Observed::Err(StableFailureFingerprint::MissingRef {
                query: Box::new(AssetQuery {
                    uuid: Some(asset),
                    ..AssetQuery::default()
                }),
                expected_terminal: self.terminal_type(asset)?.unwrap_or(TypeUuid([0; 16])),
            }))
        })())
    }

    fn resolve(&self, path: &str) -> Observed<Option<AssetUuid>> {
        let assets = self
            .store
            .path_assets(path)
            .map(|assets| assets.into_iter().collect::<Vec<_>>())
            .map_err(BuildError::infrastructure);
        self.answer(assets.map(|assets| match assets.as_slice() {
            [] => Observed::Ok(None),
            [asset] => Observed::Ok(Some(*asset)),
            conflicting => Observed::Err(StableFailureFingerprint::Ambiguous {
                conflicting: conflicting.to_vec(),
            }),
        }))
    }

    fn query(&self, query: &AssetQuery) -> Observed<[u8; 32]> {
        self.answer(self.try_query_results(query).map(|answer| match answer {
            Ok(results) => Observed::Ok(asset_query_result_hash(&results)),
            Err(bundle) => Observed::Err(StableFailureFingerprint::Poisoned { bundle }),
        }))
    }

    fn tool(&self, id: &str) -> Observed<[u8; 32]> {
        self.answer(
            self.store
                .tool_hash_at(id, self.basis.tool_version)
                .map_err(BuildError::infrastructure)
                .map(|hash| {
                    hash.map_or_else(
                        || {
                            Observed::Err(StableFailureFingerprint::MissingCapability {
                                key: CapabilityKey::Tool(id.to_owned()),
                            })
                        },
                        Observed::Ok,
                    )
                }),
        )
    }

    fn capability(&self, key: &CapabilityKey) -> Observed<[u8; 32]> {
        self.current_load.capability(key)
    }

    fn ref_check(&self, asset: AssetUuid, _expected: TypeUuid) -> Observed<Option<TypeUuid>> {
        self.answer(self.terminal_type(asset).map(Observed::Ok))
    }

    fn role_check(&self, asset: AssetUuid) -> Observed<Option<EntryRole>> {
        self.answer(self.role(asset).map(Observed::Ok))
    }

    fn control(&self, _query: &ControlQuery) -> Observed<[u8; 32]> {
        no_trace()
    }

    fn control_read(&self, _subject: &ControlSubject) -> Observed<ControlValueHash> {
        no_trace()
    }
}
