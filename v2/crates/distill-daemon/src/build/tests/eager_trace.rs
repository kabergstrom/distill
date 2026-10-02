//! A test-only copy of the eager trace capture the store source replaced:
//! it read the whole project into maps before answering anything. The
//! equivalence tests in `lazy_trace` hold the store source to its answers.

use std::collections::BTreeMap;

use distill_build::pipeline::{PipelineRegistry, Target};
use distill_build::query::{asset_query_result_hash, AssetQuery};
use distill_build::trace::{
    CapabilityKey, ControlQuery, ControlSubject, ControlValueHash, EntryRole, Observed,
    StableFailureFingerprint, TraceSource,
};
use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, ContentHash, TypeUuid};
use distill_store::StoreReader;

use super::super::trace_source::TraceQueries;
use super::super::{no_trace, BuildError, CurrentLoadSource};

#[derive(Clone)]
pub(super) struct EagerEntry {
    pub(super) asset: AssetUuid,
    pub(super) bundle: BundleUuid,
    pub(super) bundle_path: String,
    pub(super) local_id: String,
    pub(super) authored_type: TypeUuid,
    pub(super) terminal_type: TypeUuid,
    pub(super) role: EntryRole,
    pub(super) tags: BTreeMap<String, Option<String>>,
}

#[derive(Clone)]
pub(super) struct EagerTraceSource {
    pub(super) authoring_hashes: BTreeMap<AssetUuid, BundleFileHash>,
    pub(super) entries: BTreeMap<AssetUuid, EagerEntry>,
    pub(super) terminal_types: BTreeMap<AssetUuid, TypeUuid>,
    pub(super) roles: BTreeMap<AssetUuid, EntryRole>,
    pub(super) paths: BTreeMap<String, Vec<AssetUuid>>,
    pub(super) tools: BTreeMap<String, [u8; 32]>,
    pub(super) current_load: CurrentLoadSource,
    pub(super) content_hashes: BTreeMap<AssetUuid, ContentHash>,
    pub(super) tag_poisons: BTreeMap<AssetUuid, BundleUuid>,
}

pub(super) struct EagerBasis<'a> {
    pub(super) registry: &'a PipelineRegistry,
    pub(super) target: &'a Target,
    pub(super) input_version: distill_store::state::InputVersion,
    pub(super) current_load: CurrentLoadSource,
}

impl EagerTraceSource {
    pub(super) fn capture(store: &StoreReader, basis: EagerBasis<'_>) -> Result<Self, BuildError> {
        let bundle_rows = store.all_bundles().map_err(BuildError::infrastructure)?;
        let bundles = bundle_rows
            .iter()
            .map(|bundle| (bundle.bundle, bundle.path.clone()))
            .collect::<BTreeMap<_, _>>();
        let bundle_hashes = bundle_rows
            .into_iter()
            .map(|bundle| (bundle.bundle, BundleFileHash(bundle.content_hash.0)))
            .collect::<BTreeMap<_, _>>();
        let mut entries = BTreeMap::new();
        let mut authoring_hashes = BTreeMap::new();
        let mut tag_poisons = BTreeMap::new();
        for asset in store.all_asset_ids().map_err(BuildError::infrastructure)? {
            let Some(entry) = store.entry(asset).map_err(BuildError::failed)? else {
                continue;
            };
            let bundle = entry.bundle;
            let bundle_hash = bundle_hashes.get(&bundle).copied().ok_or_else(|| {
                BuildError::Infrastructure("trace entry owner bundle hash is missing".to_owned())
            })?;
            let bundle_path = bundles.get(&bundle).cloned().ok_or_else(|| {
                BuildError::Infrastructure("trace entry owner bundle is missing".to_owned())
            })?;
            entries.insert(
                asset,
                EagerEntry {
                    asset,
                    bundle,
                    bundle_path,
                    local_id: entry.local_id,
                    authored_type: entry.type_uuid,
                    terminal_type: basis
                        .registry
                        .chain(entry.type_uuid, basis.target)
                        .map_err(BuildError::failed)?
                        .terminal,
                    role: if entry.authoring_only {
                        EntryRole::AuthoringOnly
                    } else {
                        EntryRole::Runtime
                    },
                    tags: entry.tags,
                },
            );
            authoring_hashes.insert(asset, bundle_hash);
            if store
                .tag_index_state(asset)
                .map_err(BuildError::infrastructure)?
                .is_some_and(|state| state.poison.is_some())
            {
                tag_poisons.insert(asset, bundle);
            }
        }
        let mut paths = BTreeMap::<String, Vec<AssetUuid>>::new();
        for (path, _, asset) in store
            .all_path_entries()
            .map_err(BuildError::infrastructure)?
        {
            paths.entry(path).or_default().push(asset);
        }
        for assets in paths.values_mut() {
            assets.sort();
            assets.dedup();
        }
        let mut terminal_types = entries
            .iter()
            .map(|(asset, entry)| (*asset, entry.terminal_type))
            .collect::<BTreeMap<_, _>>();
        let mut roles = entries
            .iter()
            .map(|(asset, entry)| (*asset, entry.role))
            .collect::<BTreeMap<_, _>>();
        for (child, parent, output_key) in store
            .all_derived_outputs()
            .map_err(BuildError::infrastructure)?
        {
            let parent_type = entries.get(&parent).ok_or_else(|| {
                BuildError::Infrastructure("derived parent is absent from trace index".to_owned())
            })?;
            let terminal = basis
                .registry
                .chain(parent_type.authored_type, basis.target)
                .map_err(BuildError::failed)?
                .extras
                .get(&output_key)
                .copied()
                .ok_or_else(|| {
                    BuildError::Infrastructure(
                        "derived output is absent from the pinned pipeline map".to_owned(),
                    )
                })?;
            terminal_types.insert(child, terminal);
            roles.insert(child, EntryRole::Runtime);
        }
        let tools = store
            .tool_hashes_at(basis.input_version)
            .map_err(BuildError::infrastructure)?;
        let current_load = basis.current_load;
        Ok(Self {
            authoring_hashes,
            entries,
            terminal_types,
            roles,
            paths,
            tools,
            current_load,
            content_hashes: BTreeMap::new(),
            tag_poisons,
        })
    }

    pub(super) fn query_results(&self, query: &AssetQuery) -> Vec<AssetUuid> {
        let glob = query
            .path_glob
            .as_ref()
            .and_then(|pattern| globset::Glob::new(pattern).ok())
            .map(|glob| glob.compile_matcher());
        self.entries
            .values()
            .filter(|entry| entry.role == EntryRole::Runtime)
            .filter(|entry| query.uuid.is_none_or(|uuid| uuid == entry.asset))
            .filter(|entry| {
                query
                    .bundle_path
                    .as_ref()
                    .is_none_or(|path| path == &entry.bundle_path)
            })
            .filter(|entry| {
                query
                    .local_id
                    .as_ref()
                    .is_none_or(|local_id| local_id == &entry.local_id)
            })
            .filter(|entry| {
                query
                    .bundle_uuid
                    .is_none_or(|bundle| bundle == entry.bundle)
            })
            .filter(|entry| {
                query
                    .authored_type
                    .is_none_or(|authored| authored == entry.authored_type)
            })
            .filter(|entry| {
                query
                    .terminal_type
                    .is_none_or(|terminal| terminal == entry.terminal_type)
            })
            .filter(|entry| {
                query.tag.as_ref().is_none_or(|tag| {
                    entry.tags.get(&tag.tag).is_some_and(|actual| {
                        tag.value
                            .as_ref()
                            .is_none_or(|wanted| actual.as_ref() == Some(wanted))
                    })
                })
            })
            .filter(|entry| {
                query
                    .path_prefix
                    .as_ref()
                    .is_none_or(|prefix| entry.bundle_path.starts_with(prefix))
            })
            .filter(|entry| {
                glob.as_ref()
                    .is_none_or(|glob| glob.is_match(&entry.bundle_path))
            })
            .map(|entry| entry.asset)
            .collect()
    }
}

impl TraceSource for EagerTraceSource {
    fn authoring_read(&self, asset: AssetUuid) -> Observed<Option<BundleFileHash>> {
        Observed::Ok(self.authoring_hashes.get(&asset).copied())
    }

    fn read(&self, asset: AssetUuid) -> Observed<ContentHash> {
        self.content_hashes.get(&asset).copied().map_or_else(
            || {
                Observed::Err(StableFailureFingerprint::MissingRef {
                    query: Box::new(AssetQuery {
                        uuid: Some(asset),
                        ..AssetQuery::default()
                    }),
                    expected_terminal: self
                        .terminal_types
                        .get(&asset)
                        .copied()
                        .unwrap_or(TypeUuid([0; 16])),
                })
            },
            Observed::Ok,
        )
    }

    fn resolve(&self, path: &str) -> Observed<Option<AssetUuid>> {
        match self.paths.get(path).map(Vec::as_slice).unwrap_or_default() {
            [] => Observed::Ok(None),
            [asset] => Observed::Ok(Some(*asset)),
            conflicting => Observed::Err(StableFailureFingerprint::Ambiguous {
                conflicting: conflicting.to_vec(),
            }),
        }
    }

    fn query(&self, query: &AssetQuery) -> Observed<[u8; 32]> {
        if query.tag.is_some() {
            let mut without_tag = query.clone();
            without_tag.tag = None;
            let candidates = self.query_results(&without_tag);
            if let Some(bundle) = candidates
                .iter()
                .filter_map(|asset| self.tag_poisons.get(asset))
                .min()
            {
                return Observed::Err(StableFailureFingerprint::Poisoned { bundle: *bundle });
            }
        }
        Observed::Ok(asset_query_result_hash(&self.query_results(query)))
    }

    fn tool(&self, id: &str) -> Observed<[u8; 32]> {
        self.tools.get(id).copied().map_or_else(
            || {
                Observed::Err(StableFailureFingerprint::MissingCapability {
                    key: CapabilityKey::Tool(id.to_owned()),
                })
            },
            Observed::Ok,
        )
    }

    fn capability(&self, key: &CapabilityKey) -> Observed<[u8; 32]> {
        self.current_load.capability(key)
    }

    fn ref_check(&self, asset: AssetUuid, _expected: TypeUuid) -> Observed<Option<TypeUuid>> {
        Observed::Ok(self.terminal_types.get(&asset).copied())
    }

    fn role_check(&self, asset: AssetUuid) -> Observed<Option<EntryRole>> {
        Observed::Ok(self.roles.get(&asset).copied())
    }

    fn control(&self, _query: &ControlQuery) -> Observed<[u8; 32]> {
        no_trace()
    }

    fn control_read(&self, _subject: &ControlSubject) -> Observed<ControlValueHash> {
        no_trace()
    }
}

impl TraceQueries for EagerTraceSource {
    fn query_results(&self, query: &AssetQuery) -> Vec<AssetUuid> {
        EagerTraceSource::query_results(self, query)
    }
}
