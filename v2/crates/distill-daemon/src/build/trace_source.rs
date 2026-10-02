//! Trace answers read from the store at one snapshot.
//!
//! A build's trace records what it observed of the project; a cached
//! result serves a snapshot where every recorded question gets the same
//! answer there. [`StoreTraceSource`] answers each question when it is
//! asked, by indexed reads in the snapshot's read transaction
//! (`distill_store::trace_reads`): an entry is one primary-key read, a path
//! one `path_index` range, a query the index range of its most selective
//! selector, a tool one ToolEpoch row. Nothing reads the whole project
//! unless a query names no indexed selector at all (its answer may then be
//! the whole project). Answers are kept per snapshot in [`TraceAnswers`],
//! so a build that asks a question again does not read again.
//!
//! The source answers infallibly, as [`TraceSource`] requires. A store
//! failure while answering is kept and reported by
//! [`StoreTraceSource::check`]; every use goes through
//! [`super::ask_trace`] or calls it, so no answer read past a failure
//! decides anything.

use std::cell::{OnceCell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

use distill_build::pipeline::{PipelineRegistry, Target};
use distill_build::query::{asset_query_result_hash, AssetQuery};
use distill_build::trace::{
    CapabilityKey, ControlQuery, ControlSubject, ControlValueHash, EntryRole, Observed,
    StableFailureFingerprint, TraceSource,
};
use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, ContentHash, TypeUuid};
use distill_store::state::InputVersion;
use distill_store::StoreReader;

use super::{no_trace, node_content_hashes, BuildError, CurrentLoadSource, NodeResult};

/// One asset row as a trace observes it.
pub(super) struct TraceEntry {
    pub(super) asset: AssetUuid,
    pub(super) bundle: BundleUuid,
    pub(super) bundle_path: String,
    pub(super) bundle_hash: BundleFileHash,
    pub(super) local_id: String,
    pub(super) authored_type: TypeUuid,
    pub(super) terminal_type: TypeUuid,
    pub(super) role: EntryRole,
    pub(super) tags: BTreeMap<String, Option<String>>,
}

/// What the trace sources of one snapshot have read: the answers to the
/// questions asked so far, never a copy of a table. Shared by every source
/// at that snapshot (one build's view, one resolve's snapshot).
#[derive(Default)]
pub(super) struct TraceAnswers {
    entries: RefCell<HashMap<AssetUuid, Option<Rc<TraceEntry>>>>,
    /// A derived child's terminal type; `None` for an asset that is none.
    derived: RefCell<HashMap<AssetUuid, Option<TypeUuid>>>,
    paths: RefCell<HashMap<String, Rc<[AssetUuid]>>>,
    queries: RefCell<HashMap<AssetQuery, Rc<[AssetUuid]>>>,
    /// The least tag-poisoned bundle among a tagless query's candidates.
    query_poisons: RefCell<HashMap<AssetQuery, Option<BundleUuid>>>,
    tools: RefCell<HashMap<String, Option<[u8; 32]>>>,
    /// The authored types whose chains end at a terminal type, with those
    /// whose chains fail (their assets fail any query reaching them).
    terminal_sources: RefCell<HashMap<TypeUuid, Rc<[TypeUuid]>>>,
    current_load: OnceCell<CurrentLoadSource>,
}

impl TraceAnswers {
    /// The capabilities of the loaded pipeline, captured on first use.
    pub(super) fn current_load(
        &self,
        capture: impl FnOnce() -> Result<CurrentLoadSource, BuildError>,
    ) -> Result<&CurrentLoadSource, BuildError> {
        if self.current_load.get().is_none() {
            let _ = self.current_load.set(capture()?);
        }
        Ok(self.current_load.get().expect("set above"))
    }
}

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
    answers: &'a TraceAnswers,
    current_load: &'a CurrentLoadSource,
    built: BuiltNodes<'a>,
    /// The built nodes' contents by asset, gathered on the first `read`.
    contents: OnceCell<BTreeMap<AssetUuid, ContentHash>>,
    failure: RefCell<Option<BuildError>>,
}

/// Answers the eager trace capture and the store source both give, for
/// helpers that need a query's results as well as its hash.
pub(super) trait TraceQueries: TraceSource {
    /// The runtime assets `query` selects, in asset order.
    fn query_results(&self, query: &AssetQuery) -> Vec<AssetUuid>;
}

impl<'a> StoreTraceSource<'a> {
    pub(super) fn new(
        store: &'a StoreReader,
        basis: TraceBasis<'a>,
        answers: &'a TraceAnswers,
        current_load: &'a CurrentLoadSource,
        built: BuiltNodes<'a>,
    ) -> Self {
        Self {
            store,
            basis,
            answers,
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

    /// The asset row of `asset`: one primary-key read of `assets` (with its
    /// tags and its bundle's poison) and one of `bundles`.
    fn entry(&self, asset: AssetUuid) -> Result<Option<Rc<TraceEntry>>, BuildError> {
        if let Some(known) = self.answers.entries.borrow().get(&asset) {
            return Ok(known.clone());
        }
        let entry = match self.store.entry(asset).map_err(BuildError::failed)? {
            None => None,
            Some(entry) => {
                let bundle = self
                    .store
                    .bundle(entry.bundle)
                    .map_err(BuildError::infrastructure)?
                    .ok_or_else(|| {
                        BuildError::Infrastructure(
                            "trace entry owner bundle hash is missing".to_owned(),
                        )
                    })?;
                Some(Rc::new(TraceEntry {
                    asset,
                    bundle: entry.bundle,
                    bundle_path: bundle.path,
                    bundle_hash: BundleFileHash(bundle.content_hash.0),
                    local_id: entry.local_id,
                    authored_type: entry.type_uuid,
                    terminal_type: self
                        .basis
                        .registry
                        .chain(entry.type_uuid, self.basis.target)
                        .map_err(BuildError::failed)?
                        .terminal,
                    role: if entry.authoring_only {
                        EntryRole::AuthoringOnly
                    } else {
                        EntryRole::Runtime
                    },
                    tags: entry.tags,
                }))
            }
        };
        self.answers
            .entries
            .borrow_mut()
            .insert(asset, entry.clone());
        Ok(entry)
    }

    /// The terminal type of `asset` as a derived child of its parent's
    /// chain: one primary-key read of `derived_outputs`.
    fn derived_terminal(&self, asset: AssetUuid) -> Result<Option<TypeUuid>, BuildError> {
        if let Some(known) = self.answers.derived.borrow().get(&asset) {
            return Ok(*known);
        }
        let terminal = match self
            .store
            .resolve_child(asset)
            .map_err(BuildError::infrastructure)?
        {
            None => None,
            Some((parent, output_key)) => {
                let parent = self.entry(parent)?.ok_or_else(|| {
                    BuildError::Infrastructure(
                        "derived parent is absent from trace index".to_owned(),
                    )
                })?;
                Some(
                    self.basis
                        .registry
                        .chain(parent.authored_type, self.basis.target)
                        .map_err(BuildError::failed)?
                        .extras
                        .get(&output_key)
                        .copied()
                        .ok_or_else(|| {
                            BuildError::Infrastructure(
                                "derived output is absent from the pinned pipeline map".to_owned(),
                            )
                        })?,
                )
            }
        };
        self.answers.derived.borrow_mut().insert(asset, terminal);
        Ok(terminal)
    }

    fn terminal_type(&self, asset: AssetUuid) -> Result<Option<TypeUuid>, BuildError> {
        match self.derived_terminal(asset)? {
            Some(terminal) => Ok(Some(terminal)),
            None => Ok(self.entry(asset)?.map(|entry| entry.terminal_type)),
        }
    }

    fn role(&self, asset: AssetUuid) -> Result<Option<EntryRole>, BuildError> {
        match self.derived_terminal(asset)? {
            Some(_) => Ok(Some(EntryRole::Runtime)),
            None => Ok(self.entry(asset)?.map(|entry| entry.role)),
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

    fn resolved(&self, path: &str) -> Result<Rc<[AssetUuid]>, BuildError> {
        if let Some(known) = self.answers.paths.borrow().get(path) {
            return Ok(Rc::clone(known));
        }
        let assets: Rc<[AssetUuid]> = self
            .store
            .path_assets(path)
            .map_err(BuildError::infrastructure)?
            .into_iter()
            .collect();
        self.answers
            .paths
            .borrow_mut()
            .insert(path.to_owned(), Rc::clone(&assets));
        Ok(assets)
    }

    fn tool_hash(&self, id: &str) -> Result<Option<[u8; 32]>, BuildError> {
        if let Some(known) = self.answers.tools.borrow().get(id) {
            return Ok(*known);
        }
        let hash = self
            .store
            .tool_hash_at(id, self.basis.tool_version)
            .map_err(BuildError::infrastructure)?;
        self.answers.tools.borrow_mut().insert(id.to_owned(), hash);
        Ok(hash)
    }

    /// The authored types an asset of terminal type `terminal` can have.
    fn terminal_sources(&self, terminal: TypeUuid) -> Rc<[TypeUuid]> {
        if let Some(known) = self.answers.terminal_sources.borrow().get(&terminal) {
            return Rc::clone(known);
        }
        // A type no registration takes ends its own chain, so only the
        // terminal itself and registered input types can end at it.
        let sources: Rc<[TypeUuid]> = std::iter::once(terminal)
            .chain(self.basis.registry.input_types())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|authored| {
                self.basis
                    .registry
                    .chain(*authored, self.basis.target)
                    .map_or(true, |chain| chain.terminal == terminal)
            })
            .collect();
        self.answers
            .terminal_sources
            .borrow_mut()
            .insert(terminal, Rc::clone(&sources));
        sources
    }

    /// A superset of the assets `query` can select, from the index of its
    /// most selective selector; `None` when it names no indexed selector.
    fn candidates(&self, query: &AssetQuery) -> Result<Option<Vec<AssetUuid>>, BuildError> {
        let store = self.store;
        let found = if let Some(uuid) = query.uuid {
            Ok(vec![uuid])
        } else if let (Some(bundle), Some(local_id)) = (query.bundle_uuid, &query.local_id) {
            store.local_asset_ids(bundle, local_id)
        } else if let (Some(path), Some(local_id)) = (&query.bundle_path, &query.local_id) {
            store.local_asset_ids_at_bundle_path(path, local_id)
        } else if let Some(bundle) = query.bundle_uuid {
            store
                .asset_ids_in_bundle(bundle)
                .map(|assets| assets.into_iter().collect())
        } else if let Some(path) = &query.bundle_path {
            store.asset_ids_at_bundle_path(path)
        } else if let Some(tag) = query.tag.as_ref().filter(|tag| tag.value.is_some()) {
            store.asset_ids_with_tag(&tag.tag, tag.value.as_deref())
        } else if let Some(authored) = query.authored_type {
            store.asset_ids_of_type(authored)
        } else if let Some(prefix) = path_prefix(query) {
            store.asset_ids_under_bundle_path(&prefix)
        } else if let Some(terminal) = query.terminal_type {
            let mut assets = Vec::new();
            for authored in self.terminal_sources(terminal).iter() {
                assets.extend(
                    store
                        .asset_ids_of_type(*authored)
                        .map_err(BuildError::infrastructure)?,
                );
            }
            assets.sort_unstable();
            assets.dedup();
            Ok(assets)
        } else if let Some(tag) = &query.tag {
            store.asset_ids_with_tag(&tag.tag, None)
        } else {
            return Ok(None);
        };
        found.map(Some).map_err(BuildError::infrastructure)
    }

    fn try_query_results(&self, query: &AssetQuery) -> Result<Rc<[AssetUuid]>, BuildError> {
        if let Some(known) = self.answers.queries.borrow().get(query) {
            return Ok(Rc::clone(known));
        }
        let candidates = match self.candidates(query)? {
            Some(candidates) => candidates,
            None => self
                .store
                .all_asset_ids()
                .map_err(BuildError::infrastructure)?,
        };
        let glob = query_glob(query);
        let mut results = Vec::new();
        for asset in candidates {
            if let Some(entry) = self.entry(asset)? {
                if entry.role == EntryRole::Runtime && entry_matches(&entry, query, glob.as_ref()) {
                    results.push(asset);
                }
            }
        }
        let results: Rc<[AssetUuid]> = results.into();
        self.answers
            .queries
            .borrow_mut()
            .insert(query.clone(), Rc::clone(&results));
        Ok(results)
    }

    /// The least bundle among `query`'s candidates (a query without a tag)
    /// whose tag index is poisoned.
    fn query_poison(&self, query: &AssetQuery) -> Result<Option<BundleUuid>, BuildError> {
        if let Some(known) = self.answers.query_poisons.borrow().get(query) {
            return Ok(*known);
        }
        let glob = query_glob(query);
        let selected = |asset: AssetUuid| -> Result<Option<BundleUuid>, BuildError> {
            Ok(self.entry(asset)?.and_then(|entry| {
                (entry.role == EntryRole::Runtime && entry_matches(&entry, query, glob.as_ref()))
                    .then_some(entry.bundle)
            }))
        };
        let mut least: Option<BundleUuid> = None;
        match self.candidates(query)? {
            // Few candidates: ask each whether its tag index is poisoned.
            Some(candidates) => {
                for asset in candidates {
                    if let Some(bundle) = selected(asset)? {
                        if self
                            .store
                            .tag_index_poisoned(asset)
                            .map_err(BuildError::infrastructure)?
                        {
                            least = Some(least.map_or(bundle, |least| least.min(bundle)));
                        }
                    }
                }
            }
            // Every asset is a candidate: only the poisoned ones matter.
            None => {
                for asset in self
                    .store
                    .tag_poisoned_asset_ids()
                    .map_err(BuildError::infrastructure)?
                {
                    if let Some(bundle) = selected(asset)? {
                        least = Some(least.map_or(bundle, |least| least.min(bundle)));
                    }
                }
            }
        }
        self.answers
            .query_poisons
            .borrow_mut()
            .insert(query.clone(), least);
        Ok(least)
    }
}

/// The literal prefix every path a query selects starts with: its path
/// prefix, or the part of its glob before the first glob syntax, whichever
/// is longer. `None` when both are empty or absent.
fn path_prefix(query: &AssetQuery) -> Option<String> {
    let glob = query
        .path_glob
        .as_deref()
        .filter(|pattern| globset::Glob::new(pattern).is_ok())
        .map(|pattern| {
            let end = pattern
                .find(['*', '?', '[', ']', '{', '}', ',', '\\', '!'])
                .unwrap_or(pattern.len());
            pattern[..end].to_owned()
        });
    [query.path_prefix.clone(), glob]
        .into_iter()
        .flatten()
        .filter(|prefix| !prefix.is_empty())
        .max_by_key(String::len)
}

pub(super) fn query_glob(query: &AssetQuery) -> Option<globset::GlobMatcher> {
    query
        .path_glob
        .as_ref()
        .and_then(|pattern| globset::Glob::new(pattern).ok())
        .map(|glob| glob.compile_matcher())
}

/// Whether `entry` meets every selector of `query` but its role.
fn entry_matches(
    entry: &TraceEntry,
    query: &AssetQuery,
    glob: Option<&globset::GlobMatcher>,
) -> bool {
    query.uuid.is_none_or(|uuid| uuid == entry.asset)
        && query
            .bundle_path
            .as_ref()
            .is_none_or(|path| path == &entry.bundle_path)
        && query
            .local_id
            .as_ref()
            .is_none_or(|local_id| local_id == &entry.local_id)
        && query
            .bundle_uuid
            .is_none_or(|bundle| bundle == entry.bundle)
        && query
            .authored_type
            .is_none_or(|authored| authored == entry.authored_type)
        && query
            .terminal_type
            .is_none_or(|terminal| terminal == entry.terminal_type)
        && query.tag.as_ref().is_none_or(|tag| {
            entry.tags.get(&tag.tag).is_some_and(|actual| {
                tag.value
                    .as_ref()
                    .is_none_or(|wanted| actual.as_ref() == Some(wanted))
            })
        })
        && query
            .path_prefix
            .as_ref()
            .is_none_or(|prefix| entry.bundle_path.starts_with(prefix))
        && glob.is_none_or(|glob| glob.is_match(&entry.bundle_path))
}

impl TraceQueries for StoreTraceSource<'_> {
    fn query_results(&self, query: &AssetQuery) -> Vec<AssetUuid> {
        match self.try_query_results(query) {
            Ok(results) => results.to_vec(),
            Err(error) => {
                self.failure.borrow_mut().get_or_insert(error);
                Vec::new()
            }
        }
    }
}

impl TraceSource for StoreTraceSource<'_> {
    fn authoring_read(&self, asset: AssetUuid) -> Observed<Option<BundleFileHash>> {
        self.answer(
            self.entry(asset)
                .map(|entry| Observed::Ok(entry.map(|entry| entry.bundle_hash))),
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
        self.answer(self.resolved(path).map(|assets| match &*assets {
            [] => Observed::Ok(None),
            [asset] => Observed::Ok(Some(*asset)),
            conflicting => Observed::Err(StableFailureFingerprint::Ambiguous {
                conflicting: conflicting.to_vec(),
            }),
        }))
    }

    fn query(&self, query: &AssetQuery) -> Observed<[u8; 32]> {
        self.answer((|| {
            if query.tag.is_some() {
                let mut without_tag = query.clone();
                without_tag.tag = None;
                if let Some(bundle) = self.query_poison(&without_tag)? {
                    return Ok(Observed::Err(StableFailureFingerprint::Poisoned { bundle }));
                }
            }
            Ok(Observed::Ok(asset_query_result_hash(
                &self.try_query_results(query)?,
            )))
        })())
    }

    fn tool(&self, id: &str) -> Observed<[u8; 32]> {
        self.answer(self.tool_hash(id).map(|hash| {
            hash.map_or_else(
                || {
                    Observed::Err(StableFailureFingerprint::MissingCapability {
                        key: CapabilityKey::Tool(id.to_owned()),
                    })
                },
                Observed::Ok,
            )
        }))
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
