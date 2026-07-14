//! Executable authoring-import integration.
//!
//! `distill-build` owns the deterministic fold and outcome-bearing read-set;
//! this module supplies the daemon's rooted filesystem authority, importer
//! registry, `$settings`/`$record` bundle controls, basis revalidation, and
//! journaled publication.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use distill_build::import::{
    fold_import, DirectoryOrigin, FileDep, FoldRequest, IdentitySource, ImportBackend,
    ImportContext, ImportError, ImportOutput, ImportRecord, ImportedBundle, ImportedEntry,
};
use distill_build::query::{normalize_identifier, normalize_path, FileQuery, RootName, RootedPath};
use distill_build::trace::{CapabilityKey, Observed, RawFileFailureClass};
use distill_bundle::{AssetEntry, Bundle, EntryLineageV1, BUNDLE_FORMAT_VERSION};
use distill_core::attestation::{
    is_bootstrap_control_type, BootstrapControlSpecV1, BootstrapControlSymbol,
    IMPORT_RECORD_TYPE_UUID,
};
use distill_core::canonical::CanonicalEncoder;
use distill_core::id::{AssetUuid, BundleUuid, ContentHash, LogicalHash, TypeUuid};
use distill_core::lineage::{lineage_chain_digest, AcceptedSchemaEpoch, LineageStamp};
use distill_json::AuthoredValue;
use distill_rpc::{
    decode_authoring_payload, ImportRequest, InputVersion, PreparedImportCommit, RpcFailure,
};
use distill_schema::ngp_schema::{node_hash, snapshot_to_json, verify_snapshot, LogicalSchema};
use distill_store::bundles::BundleMeta;
use distill_store::journal::PublicationGroupKind;
use distill_store::Store;
use globset::Glob;

use crate::authoring::{invalid, require_base, AuthoringService};
use crate::scanner::{RootedScanner, ScanError, ScannedFileKind};

pub trait AuthoringImporter: Send + Sync {
    fn id(&self) -> &str;
    fn version(&self) -> u32;
    fn settings_type_uuid(&self) -> TypeUuid;
    fn settings_schema(&self) -> &LogicalSchema;
    fn default_settings(&self) -> AuthoredValue;
    fn import(
        &self,
        context: &mut dyn AuthoringImportContext,
        settings: &AuthoredValue,
    ) -> Result<ImportOutput, String>;
}

pub trait AuthoringImportContext {
    fn sources(&self) -> &[RootedPath];
    fn read(&mut self, path: &str) -> Result<Vec<u8>, ImportError>;
    fn probe(&mut self, path: &str) -> Result<bool, ImportError>;
    fn enumerate(&mut self, query: &FileQuery) -> Result<Vec<RootedPath>, ImportError>;
    fn importer_capability(&mut self, id: &str) -> Result<[u8; 32], ImportError>;
}

#[derive(Clone)]
pub(crate) struct RegisteredImporter {
    pub id: String,
    version: u32,
    settings_type_uuid: TypeUuid,
    settings_schema: LogicalSchema,
    settings_hash: LogicalHash,
    default_settings: AuthoredValue,
    capability_hash: [u8; 32],
    executor: Arc<dyn AuthoringImporter>,
}

pub(crate) type RegisteredImporters = BTreeMap<String, RegisteredImporter>;

impl RegisteredImporter {
    pub(crate) fn validate(importer: Arc<dyn AuthoringImporter>) -> Result<Self, RpcFailure> {
        let id = normalize_identifier(importer.id()).map_err(invalid)?;
        let settings_type_uuid = importer.settings_type_uuid();
        if is_bootstrap_control_type(settings_type_uuid) {
            return Err(invalid(
                "an importer settings type cannot be a format bootstrap control",
            ));
        }
        let settings_schema = importer.settings_schema().clone();
        let settings_hash = node_hash(&settings_schema.root).map_err(invalid)?;
        let default_settings = importer.default_settings();
        validate_default_settings(
            settings_type_uuid,
            settings_hash,
            &settings_schema,
            &default_settings,
        )?;
        let version = importer.version();
        let capability_hash = distill_core::canonical::domain_digest(*b"DSIC", 1, |encoder| {
            encoder.str(&id);
            encoder.u32(version);
            encoder.raw(&settings_type_uuid.0);
            encoder.raw(&settings_hash.0);
        });
        Ok(Self {
            id,
            version,
            settings_type_uuid,
            settings_schema,
            settings_hash,
            default_settings,
            capability_hash,
            executor: importer,
        })
    }
}

impl AuthoringService {
    pub(crate) fn prepare_import_request(
        &self,
        base: InputVersion,
        request: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        let importer = self.registered_importer(&request.importer)?;
        let settings_snapshot = snapshot_to_json(&importer.settings_schema).map_err(invalid)?;
        let settings = decode_authoring_payload(
            importer.settings_hash,
            settings_snapshot.as_bytes(),
            &request.settings,
        )
        .map_err(|error| invalid(format!("invalid importer settings: {error:?}")))?;

        let dest = normalize_path(&request.dest).map_err(invalid)?;
        let store = self
            .store
            .lock()
            .map_err(|_| invalid("durable store coordinator mutex is poisoned"))?;
        require_base(&store, base)?;
        let destination = self.resolve_explicit_destination(&store, &dest, &request.root)?;
        let prior = destination
            .meta
            .as_ref()
            .map(|meta| self.read_prior_import(&store, meta))
            .transpose()?;
        drop(store);

        let capabilities = self.importer_capabilities()?;
        let mut backend = RootedImportBackend::new(&self.scanner, &capabilities);
        let sources = root_explicit_sources(&mut backend, &destination.root, &request.sources)?;
        self.execute_import(
            base,
            importer,
            ImportInvocation {
                destination,
                prior,
                sources,
                explicit_settings: Some(settings),
                watch: request.watch,
            },
        )
    }

    pub(crate) fn prepare_reimport_bundle(
        &self,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        let store = self
            .store
            .lock()
            .map_err(|_| invalid("durable store coordinator mutex is poisoned"))?;
        require_base(&store, base)?;
        let meta = store
            .bundle(bundle)
            .map_err(invalid)?
            .ok_or_else(|| invalid(format!("cannot reimport unknown bundle {bundle}")))?;
        let prior = self.read_prior_import(&store, &meta)?;
        let importer = self.registered_importer(&prior.model.record.importer)?;
        if prior.settings_type_uuid != importer.settings_type_uuid {
            return Err(invalid(
                "the recorded settings entry type no longer matches the importer registration",
            ));
        }
        let root = store
            .root_name(meta.root)
            .map_err(invalid)?
            .ok_or_else(|| invalid("bundle root identity is missing"))?;
        let target = self
            .scanner
            .physical_path(&root, &meta.path)
            .map_err(invalid)?;
        let destination = ImportDestination {
            root,
            path: meta.path.clone(),
            target,
            meta: Some(meta),
        };
        let sources = prior.model.record.sources.clone();
        let watch = prior.model.record.watch;
        drop(store);
        let prepared = self.execute_import(
            base,
            importer,
            ImportInvocation {
                destination,
                prior: Some(prior),
                sources,
                explicit_settings: None,
                watch,
            },
        )?;
        debug_assert_eq!(prepared.bundle, bundle);
        Ok(prepared)
    }

    fn execute_import(
        &self,
        base: InputVersion,
        importer: RegisteredImporter,
        invocation: ImportInvocation,
    ) -> Result<PreparedImportCommit, RpcFailure> {
        let ImportInvocation {
            destination,
            prior,
            sources,
            explicit_settings,
            watch,
        } = invocation;
        let capabilities = self.importer_capabilities()?;
        let mut backend = RootedImportBackend::new(&self.scanner, &capabilities);
        let mut context =
            ImportContext::new(&importer.id, sources.clone(), &mut backend).map_err(invalid)?;
        let observed_capability = context
            .importer_capability(&importer.id)
            .map_err(|error| invalid(format!("importer capability lookup failed: {error:?}")))?;
        if observed_capability != importer.capability_hash {
            return Err(invalid("importer capability changed before execution"));
        }
        let output = {
            let mut adapter = TracedImportContext {
                context: &mut context,
            };
            importer
                .executor
                .import(
                    &mut adapter,
                    explicit_settings.as_ref().unwrap_or_else(|| {
                        prior
                            .as_ref()
                            .map(|prior| &prior.model.settings)
                            .unwrap_or(&importer.default_settings)
                    }),
                )
                .map_err(|error| invalid(format!("importer {:?} failed: {error}", importer.id)))?
        };
        let read_set = context.into_read_set();
        if read_set.iter().any(dep_has_failure) {
            return Err(invalid(
                "an importer cannot publish after catching a failed context observation",
            ));
        }

        let seed = import_identity_seed(
            base,
            &destination.root,
            &destination.path,
            importer.capability_hash,
        );
        let mut ids = HashIdentitySource::new(seed);
        let folded = fold_import(
            prior.as_ref().map(|prior| &prior.model),
            FoldRequest {
                output,
                explicit_settings,
                default_settings: importer.default_settings.clone(),
                importer: importer.id.clone(),
                sources,
                watch,
                read_set: read_set.clone(),
                origin: prior
                    .as_ref()
                    .and_then(|prior| prior.model.record.origin.clone()),
            },
            &mut ids,
        )
        .map_err(|error| invalid(format!("import fold failed: {error:?}")))?;

        let store = self
            .store
            .lock()
            .map_err(|_| invalid("durable store coordinator mutex is poisoned"))?;
        require_base(&store, base)?;
        let capabilities = self.importer_capabilities()?;
        let mut recheck = RootedImportBackend::new(&self.scanner, &capabilities);
        if !revalidate_read_set(&read_set, &mut recheck) {
            return Err(invalid(
                "import read-set changed before publication; the result was discarded",
            ));
        }
        let bytes = build_import_bundle(
            &store,
            &importer,
            &folded,
            prior.as_ref().map(|prior| &prior.bundle),
            &mut ids,
        )?;
        let bundle = folded.bundle_uuid;
        let preimage = destination.meta.as_ref().map(|meta| meta.content_hash);
        let basis = encode_import_basis(
            base,
            &destination,
            importer.version,
            importer.capability_hash,
            &bytes,
        );
        drop(store);
        let commit = self.publish_file(
            base,
            PublicationGroupKind::Import,
            &basis,
            destination.target,
            preimage,
            Some(bytes),
        )?;
        Ok(PreparedImportCommit { bundle, commit })
    }

    fn registered_importer(&self, id: &str) -> Result<RegisteredImporter, RpcFailure> {
        let id = normalize_identifier(id).map_err(invalid)?;
        self.importers
            .read()
            .map_err(|_| invalid("importer registry lock is poisoned"))?
            .get(&id)
            .cloned()
            .ok_or_else(|| invalid(format!("importer {id:?} is not registered")))
    }

    fn importer_capabilities(&self) -> Result<BTreeMap<String, [u8; 32]>, RpcFailure> {
        Ok(self
            .importers
            .read()
            .map_err(|_| invalid("importer registry lock is poisoned"))?
            .iter()
            .map(|(id, importer)| (id.clone(), importer.capability_hash))
            .collect())
    }

    fn resolve_explicit_destination(
        &self,
        store: &Store,
        path: &str,
        requested_root: &str,
    ) -> Result<ImportDestination, RpcFailure> {
        let matches = store
            .all_bundles()
            .map_err(invalid)?
            .into_iter()
            .filter(|meta| meta.path == path)
            .collect::<Vec<_>>();
        let meta = match matches.as_slice() {
            [] => None,
            [meta] => Some(meta.clone()),
            _ => {
                return Err(invalid(format!(
                    "destination path {path:?} is ambiguous across asset roots"
                )))
            }
        };
        let root = if let Some(meta) = &meta {
            store
                .root_name(meta.root)
                .map_err(invalid)?
                .ok_or_else(|| invalid("bundle root identity is missing"))?
        } else if requested_root.is_empty() {
            match self.roots.as_slice() {
                [root] => root.name.clone(),
                _ => {
                    return Err(invalid(
                        "a new import destination requires an explicit root when multiple roots are configured",
                    ))
                }
            }
        } else {
            let root = normalize_identifier(requested_root).map_err(invalid)?;
            if !self.roots.iter().any(|candidate| candidate.name == root) {
                return Err(invalid(format!("unknown import destination root {root:?}")));
            }
            root
        };
        let target = self.scanner.physical_path(&root, path).map_err(invalid)?;
        Ok(ImportDestination {
            root,
            path: path.to_owned(),
            target,
            meta,
        })
    }

    fn read_prior_import(
        &self,
        store: &Store,
        meta: &BundleMeta,
    ) -> Result<PriorImport, RpcFailure> {
        let root = store
            .root_name(meta.root)
            .map_err(invalid)?
            .ok_or_else(|| invalid("bundle root identity is missing"))?;
        let target = self
            .scanner
            .physical_path(&root, &meta.path)
            .map_err(invalid)?;
        let bytes = self
            .scanner
            .read_identity_checked(&target)
            .map_err(invalid)?;
        if ContentHash(*blake3::hash(&bytes).as_bytes()) != meta.content_hash {
            return Err(invalid("import destination changed since the durable base"));
        }
        let bundle = distill_bundle::parse_bundle(&bytes).map_err(invalid)?;
        let settings = bundle
            .assets
            .get("$settings")
            .ok_or_else(|| invalid("existing import bundle has no $settings entry"))?;
        let record = bundle
            .assets
            .get("$record")
            .ok_or_else(|| invalid("existing destination is not an imported bundle"))?;
        if record.type_uuid != IMPORT_RECORD_TYPE_UUID || !record.authoring_only {
            return Err(invalid("existing $record entry has the wrong type or role"));
        }
        let record = decode_import_record(&record.data)?;
        let entries = bundle
            .assets
            .iter()
            .filter(|(local_id, _)| !local_id.starts_with('$'))
            .map(|(local_id, entry)| {
                (
                    local_id.clone(),
                    ImportedEntry {
                        uuid: entry.uuid,
                        type_uuid: entry.type_uuid,
                        value: entry.data.clone(),
                    },
                )
            })
            .collect();
        Ok(PriorImport {
            model: ImportedBundle {
                bundle_uuid: bundle.uuid,
                primary: bundle.primary.clone(),
                entries,
                settings: settings.data.clone(),
                record,
            },
            settings_type_uuid: settings.type_uuid,
            bundle,
        })
    }
}

struct TracedImportContext<'a, 'b> {
    context: &'a mut ImportContext<'b, RootedImportBackend<'b>>,
}

impl AuthoringImportContext for TracedImportContext<'_, '_> {
    fn sources(&self) -> &[RootedPath] {
        self.context.sources()
    }

    fn read(&mut self, path: &str) -> Result<Vec<u8>, ImportError> {
        self.context.read(path)
    }

    fn probe(&mut self, path: &str) -> Result<bool, ImportError> {
        self.context.probe(path)
    }

    fn enumerate(&mut self, query: &FileQuery) -> Result<Vec<RootedPath>, ImportError> {
        self.context.enumerate(query)
    }

    fn importer_capability(&mut self, id: &str) -> Result<[u8; 32], ImportError> {
        self.context.importer_capability(id)
    }
}

struct RootedImportBackend<'a> {
    scanner: &'a RootedScanner,
    capabilities: &'a BTreeMap<String, [u8; 32]>,
}

impl<'a> RootedImportBackend<'a> {
    fn new(scanner: &'a RootedScanner, capabilities: &'a BTreeMap<String, [u8; 32]>) -> Self {
        Self {
            scanner,
            capabilities,
        }
    }

    fn rows(&self) -> Result<crate::scanner::ScanSnapshot, RawFileFailureClass> {
        self.scanner.scan().map_err(scan_failure)
    }
}

impl ImportBackend for RootedImportBackend<'_> {
    fn read(&mut self, path: &str) -> Result<(RootedPath, Vec<u8>), RawFileFailureClass> {
        let rows = self.rows()?;
        let matches = rows
            .files
            .iter()
            .filter(|file| file.normalized_path == path && file.content_hash.is_some())
            .collect::<Vec<_>>();
        let [file] = matches.as_slice() else {
            return Err(if matches.is_empty() {
                RawFileFailureClass::NotFound
            } else {
                RawFileFailureClass::OtherStable
            });
        };
        let physical = self
            .scanner
            .physical_path(&file.root_name, path)
            .map_err(scan_failure)?;
        let bytes = self
            .scanner
            .read_identity_checked(&physical)
            .map_err(scan_failure)?;
        Ok((
            RootedPath::new(&file.root_name, path).map_err(|_| RawFileFailureClass::OtherStable)?,
            bytes,
        ))
    }

    fn probe(&mut self, path: &str) -> Result<Option<RootName>, RawFileFailureClass> {
        let rows = self.rows()?;
        let roots = rows
            .files
            .iter()
            .filter(|file| file.normalized_path == path)
            .map(|file| file.root_name.as_str())
            .collect::<BTreeSet<_>>();
        match roots.iter().copied().collect::<Vec<_>>().as_slice() {
            [] => Ok(None),
            [root] => RootName::new(root)
                .map(Some)
                .map_err(|_| RawFileFailureClass::OtherStable),
            _ => Err(RawFileFailureClass::OtherStable),
        }
    }

    fn enumerate(&mut self, query: &FileQuery) -> Result<Vec<RootedPath>, RawFileFailureClass> {
        let matcher = query
            .path_glob
            .as_ref()
            .map(|glob| Glob::new(glob).map(|glob| glob.compile_matcher()))
            .transpose()
            .map_err(|_| RawFileFailureClass::OtherStable)?;
        let rows = self.rows()?;
        let mut results = Vec::new();
        for file in rows.files {
            if !matches!(file.kind, ScannedFileKind::File | ScannedFileKind::Symlink) {
                continue;
            }
            let prefix_matches = query.path_prefix.as_ref().is_none_or(|prefix| {
                file.normalized_path == *prefix
                    || file
                        .normalized_path
                        .strip_prefix(prefix)
                        .is_some_and(|suffix| suffix.starts_with('/'))
            });
            if prefix_matches
                && matcher
                    .as_ref()
                    .is_none_or(|matcher| matcher.is_match(&file.normalized_path))
            {
                results.push(
                    RootedPath::new(&file.root_name, &file.normalized_path)
                        .map_err(|_| RawFileFailureClass::OtherStable)?,
                );
            }
        }
        Ok(results)
    }

    fn capability(&mut self, key: &CapabilityKey) -> Option<[u8; 32]> {
        match key {
            CapabilityKey::Importer(id) => self.capabilities.get(id).copied(),
            _ => None,
        }
    }
}

fn scan_failure(error: ScanError) -> RawFileFailureClass {
    match error {
        ScanError::Io { source, .. } if source.kind() == std::io::ErrorKind::PermissionDenied => {
            RawFileFailureClass::PermissionDenied
        }
        ScanError::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound => {
            RawFileFailureClass::NotFound
        }
        _ => RawFileFailureClass::OtherStable,
    }
}

fn root_explicit_sources(
    backend: &mut RootedImportBackend<'_>,
    fallback_root: &str,
    sources: &[String],
) -> Result<Vec<RootedPath>, RpcFailure> {
    sources
        .iter()
        .map(|source| {
            let path = normalize_path(source).map_err(invalid)?;
            let root = backend
                .probe(&path)
                .map_err(|error| invalid(format!("source {path:?} is ambiguous: {error:?}")))?
                .map_or_else(|| RootName::new(fallback_root), Ok)
                .map_err(invalid)?;
            RootedPath::new(&root.0, &path).map_err(invalid)
        })
        .collect()
}

fn revalidate_read_set(read_set: &[FileDep], backend: &mut impl ImportBackend) -> bool {
    read_set.iter().all(|expected| match expected {
        FileDep::Read { path, observed } => match backend.read(path) {
            Ok((path, bytes)) => {
                *observed
                    == Observed::Ok(distill_build::import::FileContentObservation {
                        path,
                        hash: *blake3::hash(&bytes).as_bytes(),
                    })
            }
            Err(_) => false,
        },
        FileDep::Probe { path, observed } => backend
            .probe(path)
            .is_ok_and(|value| *observed == Observed::Ok(value)),
        FileDep::Listing { query, observed } => backend.enumerate(query).is_ok_and(|mut paths| {
            paths.sort_unstable();
            paths.dedup();
            *observed == Observed::Ok(distill_build::query::file_query_result_hash(&paths))
        }),
        FileDep::Capability { key, observed } => {
            *observed
                == backend.capability(key).map_or_else(
                    || {
                        Observed::Err(
                            distill_build::trace::StableFailureFingerprint::MissingCapability {
                                key: key.clone(),
                            },
                        )
                    },
                    Observed::Ok,
                )
        }
    })
}

fn dep_has_failure(dep: &FileDep) -> bool {
    match dep {
        FileDep::Read { observed, .. } => matches!(observed, Observed::Err(_)),
        FileDep::Probe { observed, .. } => matches!(observed, Observed::Err(_)),
        FileDep::Listing { observed, .. } | FileDep::Capability { observed, .. } => {
            matches!(observed, Observed::Err(_))
        }
    }
}

fn build_import_bundle(
    store: &Store,
    importer: &RegisteredImporter,
    imported: &ImportedBundle,
    prior: Option<&Bundle>,
    ids: &mut HashIdentitySource,
) -> Result<Vec<u8>, RpcFailure> {
    let mut schemas = BTreeMap::new();
    let mut assets = BTreeMap::new();
    for (local_id, entry) in &imported.entries {
        let (hash, schema, lineage) = current_type_schema(store, entry.type_uuid)?;
        assets.insert(
            local_id.clone(),
            AssetEntry {
                uuid: entry.uuid,
                type_uuid: entry.type_uuid,
                schema_hash: hash,
                lineage: EntryLineageV1::Manifest(lineage),
                authoring_only: false,
                data: entry.value.clone(),
            },
        );
        schemas.insert(hash, schema);
    }

    let settings_lineage = store
        .current_lineage_stamp(importer.settings_type_uuid)
        .map_err(invalid)?
        .filter(|stamp| stamp.selected_digest() == Some(importer.settings_hash))
        .ok_or_else(|| invalid("importer settings schema is not the accepted current lineage"))?;
    schemas.insert(importer.settings_hash, importer.settings_schema.clone());
    assets.insert(
        "$settings".into(),
        AssetEntry {
            uuid: prior
                .and_then(|bundle| bundle.assets.get("$settings"))
                .map_or_else(|| ids.next_asset(), |entry| entry.uuid),
            type_uuid: importer.settings_type_uuid,
            schema_hash: importer.settings_hash,
            lineage: EntryLineageV1::Manifest(settings_lineage),
            authoring_only: true,
            data: imported.settings.clone(),
        },
    );

    let spec = BootstrapControlSpecV1::embedded().map_err(invalid)?;
    let record_row = spec
        .0
        .iter()
        .find(|row| row.symbol == BootstrapControlSymbol::ImportRecord)
        .expect("closed bootstrap table contains ImportRecord");
    let record_schema =
        crate::logical_node::decode_logical_schema_bytes(&record_row.logical_schema)
            .map_err(invalid)?;
    schemas.insert(record_row.logical_hash, record_schema);
    assets.insert(
        "$record".into(),
        AssetEntry {
            uuid: prior
                .and_then(|bundle| bundle.assets.get("$record"))
                .map_or_else(|| ids.next_asset(), |entry| entry.uuid),
            type_uuid: IMPORT_RECORD_TYPE_UUID,
            schema_hash: record_row.logical_hash,
            lineage: EntryLineageV1::Bootstrap {
                bundle_format_version: BUNDLE_FORMAT_VERSION,
            },
            authoring_only: true,
            data: encode_import_record(&imported.record)?,
        },
    );
    let bundle = Bundle {
        format_version: BUNDLE_FORMAT_VERSION,
        uuid: imported.bundle_uuid,
        primary: imported.primary.clone(),
        schemas,
        assets,
    };
    distill_bundle::write_bundle(&bundle).map_err(invalid)
}

fn current_type_schema(
    store: &Store,
    type_uuid: TypeUuid,
) -> Result<(LogicalHash, LogicalSchema, LineageStamp), RpcFailure> {
    if is_bootstrap_control_type(type_uuid) {
        return Err(invalid(
            "importer output cannot mint bootstrap control entries",
        ));
    }
    let hash = store
        .lineage_current(type_uuid)
        .map_err(invalid)?
        .ok_or_else(|| invalid(format!("output type {type_uuid} has no accepted schema")))?;
    let snapshot = store
        .schema(hash)
        .map_err(invalid)?
        .ok_or_else(|| invalid(format!("accepted schema {hash} is not cached")))?;
    let schema = verify_snapshot(&snapshot, hash).map_err(invalid)?;
    let stamp = store
        .current_lineage_stamp(type_uuid)
        .map_err(invalid)?
        .ok_or_else(|| invalid("accepted output lineage has no stamp"))?;
    Ok((hash, schema, stamp))
}

fn validate_default_settings(
    type_uuid: TypeUuid,
    hash: LogicalHash,
    schema: &LogicalSchema,
    value: &AuthoredValue,
) -> Result<(), RpcFailure> {
    let epochs = vec![AcceptedSchemaEpoch {
        digest: hash,
        forward_parent: None,
    }];
    let bundle = Bundle {
        format_version: BUNDLE_FORMAT_VERSION,
        uuid: BundleUuid([0x41; 16]),
        primary: Some("settings".into()),
        schemas: BTreeMap::from([(hash, schema.clone())]),
        assets: BTreeMap::from([(
            "settings".into(),
            AssetEntry {
                uuid: AssetUuid([0x42; 16]),
                type_uuid,
                schema_hash: hash,
                lineage: EntryLineageV1::Manifest(LineageStamp {
                    chain: lineage_chain_digest(type_uuid, &epochs, 0),
                    epochs,
                    cursor: 0,
                }),
                authoring_only: false,
                data: value.clone(),
            },
        )]),
    };
    distill_bundle::write_bundle(&bundle)
        .map(drop)
        .map_err(|error| invalid(format!("invalid importer default settings: {error}")))
}

fn encode_import_record(record: &ImportRecord) -> Result<AuthoredValue, RpcFailure> {
    Ok(object([
        ("importer", AuthoredValue::Str(record.importer.clone())),
        (
            "origin",
            record
                .origin
                .as_ref()
                .map(encode_origin)
                .unwrap_or(AuthoredValue::Null),
        ),
        (
            "read_set",
            AuthoredValue::Array(
                record
                    .read_set
                    .iter()
                    .map(encode_file_dep)
                    .collect::<Result<_, _>>()?,
            ),
        ),
        ("settings", AuthoredValue::Str("$settings".into())),
        (
            "sources",
            AuthoredValue::Array(record.sources.iter().map(encode_rooted_path).collect()),
        ),
        ("watch", AuthoredValue::Bool(record.watch)),
    ]))
}

fn decode_import_record(value: &AuthoredValue) -> Result<ImportRecord, RpcFailure> {
    let fields = as_object(value, "ImportRecord")?;
    let importer = as_string(field(fields, "importer")?, "ImportRecord.importer")?.to_owned();
    let sources = as_array(field(fields, "sources")?, "ImportRecord.sources")?
        .iter()
        .map(decode_rooted_path)
        .collect::<Result<_, _>>()?;
    let watch = as_bool(field(fields, "watch")?, "ImportRecord.watch")?;
    if as_string(field(fields, "settings")?, "ImportRecord.settings")? != "$settings" {
        return Err(invalid("ImportRecord.settings must name $settings"));
    }
    let origin = match field(fields, "origin")? {
        AuthoredValue::Null => None,
        value => Some(decode_origin(value)?),
    };
    Ok(ImportRecord {
        importer,
        sources,
        watch,
        read_set: Vec::new(),
        origin,
    })
}

fn encode_file_dep(dep: &FileDep) -> Result<AuthoredValue, RpcFailure> {
    let (variant, fields) = match dep {
        FileDep::Read { path, observed } => (
            "Read",
            object([
                ("observed", encode_observed_content(observed)?),
                ("path", AuthoredValue::Str(path.clone())),
            ]),
        ),
        FileDep::Probe { path, observed } => (
            "Probe",
            object([
                ("observed", encode_observed_root(observed)?),
                ("path", AuthoredValue::Str(path.clone())),
            ]),
        ),
        FileDep::Listing { query, observed } => (
            "Listing",
            object([
                ("observed", encode_observed_hash(observed)?),
                ("query", encode_file_query(query)),
            ]),
        ),
        FileDep::Capability { key, observed } => (
            "Capability",
            object([
                ("key", encode_capability_key(key)),
                ("observed", encode_observed_hash(observed)?),
            ]),
        ),
    };
    Ok(object([(variant, fields)]))
}

fn encode_observed_content(
    observed: &Observed<distill_build::import::FileContentObservation>,
) -> Result<AuthoredValue, RpcFailure> {
    match observed {
        Observed::Ok(value) => Ok(object([(
            "Ok",
            object([(
                "value",
                object([
                    ("hash", byte_array(&value.hash)),
                    ("path", encode_rooted_path(&value.path)),
                ]),
            )]),
        )])),
        Observed::Err(_) => Err(invalid("failed import observations cannot be committed")),
    }
}

fn encode_observed_root(
    observed: &Observed<Option<RootName>>,
) -> Result<AuthoredValue, RpcFailure> {
    match observed {
        Observed::Ok(value) => Ok(object([(
            "Ok",
            object([(
                "value",
                value
                    .as_ref()
                    .map(|root| AuthoredValue::Str(root.0.clone()))
                    .unwrap_or(AuthoredValue::Null),
            )]),
        )])),
        Observed::Err(_) => Err(invalid("failed import observations cannot be committed")),
    }
}

fn encode_observed_hash(observed: &Observed<[u8; 32]>) -> Result<AuthoredValue, RpcFailure> {
    match observed {
        Observed::Ok(value) => Ok(object([("Ok", object([("value", byte_array(value))]))])),
        Observed::Err(_) => Err(invalid("failed import observations cannot be committed")),
    }
}

fn encode_capability_key(key: &CapabilityKey) -> AuthoredValue {
    let (variant, value) = match key {
        CapabilityKey::MigrationFn(key) => (
            "MigrationFn",
            object([("key", AuthoredValue::Str(key.clone()))]),
        ),
        CapabilityKey::DefaultTable(type_uuid) => (
            "DefaultTable",
            object([("type_uuid", byte_array(&type_uuid.0))]),
        ),
        CapabilityKey::Importer(id) => {
            ("Importer", object([("id", AuthoredValue::Str(id.clone()))]))
        }
        CapabilityKey::Processor { input } => {
            ("Processor", object([("input", byte_array(&input.0))]))
        }
        CapabilityKey::Tool(id) => ("Tool", object([("id", AuthoredValue::Str(id.clone()))])),
    };
    object([(variant, value)])
}

fn encode_file_query(query: &FileQuery) -> AuthoredValue {
    object([
        (
            "path_glob",
            query
                .path_glob
                .as_ref()
                .map(|value| AuthoredValue::Str(value.clone()))
                .unwrap_or(AuthoredValue::Null),
        ),
        (
            "path_prefix",
            query
                .path_prefix
                .as_ref()
                .map(|value| AuthoredValue::Str(value.clone()))
                .unwrap_or(AuthoredValue::Null),
        ),
    ])
}

fn encode_origin(origin: &DirectoryOrigin) -> AuthoredValue {
    object([
        ("group", encode_rooted_path(&origin.group)),
        ("rule", byte_array(&origin.rule.0)),
        ("rules_bundle", byte_array(&origin.rules_bundle.0)),
    ])
}

fn decode_origin(value: &AuthoredValue) -> Result<DirectoryOrigin, RpcFailure> {
    let fields = as_object(value, "DirectoryOrigin")?;
    Ok(DirectoryOrigin {
        rules_bundle: BundleUuid(fixed_bytes(field(fields, "rules_bundle")?, "rules_bundle")?),
        rule: distill_build::import::ImportRuleId(fixed_bytes(field(fields, "rule")?, "rule")?),
        group: decode_rooted_path(field(fields, "group")?)?,
    })
}

fn encode_rooted_path(path: &RootedPath) -> AuthoredValue {
    object([
        ("path", AuthoredValue::Str(path.path.clone())),
        ("root", AuthoredValue::Str(path.root.0.clone())),
    ])
}

fn decode_rooted_path(value: &AuthoredValue) -> Result<RootedPath, RpcFailure> {
    let fields = as_object(value, "RootedPath")?;
    RootedPath::new(
        as_string(field(fields, "root")?, "RootedPath.root")?,
        as_string(field(fields, "path")?, "RootedPath.path")?,
    )
    .map_err(invalid)
}

fn object<const N: usize>(fields: [(&str, AuthoredValue); N]) -> AuthoredValue {
    AuthoredValue::Object(
        fields
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    )
}

fn byte_array(bytes: &[u8]) -> AuthoredValue {
    AuthoredValue::Array(
        bytes
            .iter()
            .map(|byte| AuthoredValue::UInt(u128::from(*byte)))
            .collect(),
    )
}

fn fixed_bytes<const N: usize>(
    value: &AuthoredValue,
    context: &str,
) -> Result<[u8; N], RpcFailure> {
    let values = as_array(value, context)?;
    if values.len() != N {
        return Err(invalid(format!("{context} must contain exactly {N} bytes")));
    }
    let mut bytes = [0; N];
    for (target, value) in bytes.iter_mut().zip(values) {
        let AuthoredValue::UInt(value) = value else {
            return Err(invalid(format!("{context} contains a non-byte value")));
        };
        *target = u8::try_from(*value)
            .map_err(|_| invalid(format!("{context} contains an out-of-range byte")))?;
    }
    Ok(bytes)
}

fn as_object<'a>(
    value: &'a AuthoredValue,
    context: &str,
) -> Result<&'a BTreeMap<String, AuthoredValue>, RpcFailure> {
    match value {
        AuthoredValue::Object(value) => Ok(value),
        _ => Err(invalid(format!("{context} must be an object"))),
    }
}

fn as_array<'a>(
    value: &'a AuthoredValue,
    context: &str,
) -> Result<&'a [AuthoredValue], RpcFailure> {
    match value {
        AuthoredValue::Array(value) => Ok(value),
        _ => Err(invalid(format!("{context} must be an array"))),
    }
}

fn as_string<'a>(value: &'a AuthoredValue, context: &str) -> Result<&'a str, RpcFailure> {
    match value {
        AuthoredValue::Str(value) => Ok(value),
        _ => Err(invalid(format!("{context} must be text"))),
    }
}

fn as_bool(value: &AuthoredValue, context: &str) -> Result<bool, RpcFailure> {
    match value {
        AuthoredValue::Bool(value) => Ok(*value),
        _ => Err(invalid(format!("{context} must be a boolean"))),
    }
}

fn field<'a>(
    fields: &'a BTreeMap<String, AuthoredValue>,
    name: &str,
) -> Result<&'a AuthoredValue, RpcFailure> {
    fields
        .get(name)
        .ok_or_else(|| invalid(format!("missing field {name:?}")))
}

struct ImportDestination {
    root: String,
    path: String,
    target: std::path::PathBuf,
    meta: Option<BundleMeta>,
}

struct PriorImport {
    model: ImportedBundle,
    settings_type_uuid: TypeUuid,
    bundle: Bundle,
}

struct ImportInvocation {
    destination: ImportDestination,
    prior: Option<PriorImport>,
    sources: Vec<RootedPath>,
    explicit_settings: Option<AuthoredValue>,
    watch: bool,
}

struct HashIdentitySource {
    seed: [u8; 32],
    next: u64,
}

impl HashIdentitySource {
    fn new(seed: [u8; 32]) -> Self {
        Self { seed, next: 0 }
    }

    fn mint(&mut self, domain: &[u8]) -> [u8; 16] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(domain);
        hasher.update(&self.seed);
        hasher.update(&self.next.to_le_bytes());
        self.next = self
            .next
            .checked_add(1)
            .expect("one import cannot mint u64::MAX identities");
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        bytes
    }
}

impl IdentitySource for HashIdentitySource {
    fn next_asset(&mut self) -> AssetUuid {
        AssetUuid(self.mint(b"DSIA"))
    }

    fn next_bundle(&mut self) -> BundleUuid {
        BundleUuid(self.mint(b"DSIB"))
    }
}

fn import_identity_seed(
    base: InputVersion,
    root: &str,
    path: &str,
    capability: [u8; 32],
) -> [u8; 32] {
    distill_core::canonical::domain_digest(*b"DSII", 1, |encoder| {
        encoder.u64(base.0);
        encoder.str(root);
        encoder.str(path);
        encoder.raw(&capability);
    })
}

fn encode_import_basis(
    base: InputVersion,
    destination: &ImportDestination,
    importer_version: u32,
    capability: [u8; 32],
    proposed: &[u8],
) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::new();
    encoder.u64(base.0);
    encoder.str(&destination.root);
    encoder.str(&destination.path);
    encoder.u32(importer_version);
    encoder.raw(&capability);
    encoder.raw(blake3::hash(proposed).as_bytes());
    encoder.into_bytes()
}
