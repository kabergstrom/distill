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
use distill_build::query::{
    file_query_result_hash, normalize_identifier, normalize_path, FileQuery, RootName, RootedPath,
};
use distill_build::trace::{
    CapabilityKey, DirectoryGrouping, LocalFailureClass, Observed, RawFileFailureClass, RawFileOp,
    RawFileSubject, StableFailureFingerprint,
};
use distill_bundle::{AssetEntry, Bundle, EntryLineageV1, BUNDLE_FORMAT_VERSION};
use distill_core::attestation::{
    is_bootstrap_control_type, BootstrapControlSpecV1, BootstrapControlSymbol,
    DIRECTORY_IMPORT_RULES_TYPE_UUID, IMPORT_RECORD_TYPE_UUID,
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
use distill_store::imports::{WatchedImportFailure, WatchedImportTerminal};
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
    ) -> Result<ImportOutput, AuthoringImporterError>;
}

/// Closed bridge failure: a context dependency keeps its exact observed
/// fingerprint, while importer-owned deterministic rejection carries the
/// stable non-zero code defined by that importer version. Messages are
/// presentation only and never decide wakeup or equality.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoringImporterError {
    Dependency(ImportError),
    Rejected {
        code: u32,
        message: String,
    },
    /// Epoch fencing, callback panic, or host infrastructure. This outcome is
    /// never stable importer data and therefore must not be memoized.
    PipelineUnavailable(String),
}

impl AuthoringImporterError {
    pub fn rejected(code: u32, message: impl Into<String>) -> Self {
        Self::Rejected {
            code,
            message: message.into(),
        }
    }

    fn message(&self) -> String {
        match self {
            Self::Dependency(error) => format!("{error:?}"),
            Self::Rejected { message, .. } => message.clone(),
            Self::PipelineUnavailable(message) => message.clone(),
        }
    }
}

impl From<ImportError> for AuthoringImporterError {
    fn from(error: ImportError) -> Self {
        Self::Dependency(error)
    }
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

#[derive(Debug, Clone)]
pub(crate) struct DirectoryImportTask {
    rules_bundle: BundleUuid,
    rule: distill_build::import::ImportRuleId,
    group: RootedPath,
    importer: String,
    sources: Vec<RootedPath>,
    settings: AuthoredValue,
    destination_root: String,
    destination_path: String,
    listing_dep: FileDep,
}

#[derive(Debug)]
struct DecodedDirectoryRules {
    listing: FileQuery,
    rules: Vec<DecodedDirectoryRule>,
}

#[derive(Debug)]
struct DecodedDirectoryRule {
    id: distill_build::import::ImportRuleId,
    matches: FileQuery,
    group: DirectoryGrouping,
    importer: String,
    settings: AuthoredValue,
    output: String,
}

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
    /// Return every watched bundle whose complete committed read-set no longer
    /// reproduces under the current rooted filesystem and importer-capability
    /// projection. The caller reruns these under the single-writer RPC CAS.
    pub fn watched_imports_needing_reimport(&self) -> Result<Vec<BundleUuid>, RpcFailure> {
        let store = self
            .store
            .lock()
            .map_err(|_| invalid("durable store coordinator mutex is poisoned"))?;
        let capabilities = self.importer_capabilities()?;
        let mut pending = Vec::new();
        for meta in store.all_bundles().map_err(invalid)? {
            let prior = match self.read_prior_import(&store, &meta) {
                Ok(prior) => prior,
                Err(_) => continue,
            };
            if !prior.model.record.watch {
                continue;
            }
            let mut backend = RootedImportBackend::new(&self.scanner, &capabilities);
            let basis = match store.watched_import_failure(meta.bundle).map_err(invalid)? {
                Some(failure) => decode_attempt_basis(&failure.basis)?,
                None => prior.model.record.read_set.clone(),
            };
            if !revalidate_read_set(&basis, &mut backend) {
                pending.push(meta.bundle);
            }
        }
        pending.sort_unstable();
        pending.dedup();
        Ok(pending)
    }

    pub(crate) fn directory_import_tasks(&self) -> Result<Vec<DirectoryImportTask>, RpcFailure> {
        let store = self
            .store
            .lock()
            .map_err(|_| invalid("durable store coordinator mutex is poisoned"))?;
        let capabilities = self.importer_capabilities()?;
        let mut backend = RootedImportBackend::new(&self.scanner, &capabilities);
        let mut all_rule_ids = BTreeMap::<[u8; 16], BundleUuid>::new();
        let mut tasks = Vec::new();

        for meta in store.all_bundles().map_err(invalid)? {
            let root = store
                .root_name(meta.root)
                .map_err(invalid)?
                .ok_or_else(|| invalid("bundle root identity is missing"))?;
            let path = self
                .scanner
                .physical_path(&root, &meta.path)
                .map_err(invalid)?;
            let bytes = self.scanner.read_identity_checked(&path).map_err(invalid)?;
            if ContentHash(*blake3::hash(&bytes).as_bytes()) != meta.content_hash {
                return Err(invalid(format!(
                    "directory-rules bundle {} changed since the durable version",
                    meta.path
                )));
            }
            let bundle = distill_bundle::parse_bundle(&bytes).map_err(invalid)?;
            for entry in bundle
                .assets
                .values()
                .filter(|entry| entry.type_uuid == DIRECTORY_IMPORT_RULES_TYPE_UUID)
            {
                let rules = decode_directory_rules(&entry.data)?;
                for rule in &rules.rules {
                    if let Some(prior) = all_rule_ids.insert(rule.id.0, bundle.uuid) {
                        return Err(invalid(format!(
                            "directory import rule {} is duplicated by bundles {prior} and {}",
                            uuid_text(rule.id.0),
                            bundle.uuid
                        )));
                    }
                }
                let mut listed = backend
                    .enumerate(&rules.listing)
                    .map_err(|error| invalid(format!("directory listing failed: {error:?}")))?;
                listed.sort_unstable();
                listed.dedup();
                let listing_dep = FileDep::Listing {
                    query: rules.listing.clone(),
                    observed: Observed::Ok(file_query_result_hash(&listed)),
                };
                let mut groups = BTreeMap::<(usize, RootedPath), Vec<RootedPath>>::new();
                for source in listed {
                    let Some((index, rule)) = rules
                        .rules
                        .iter()
                        .enumerate()
                        .find(|(_, rule)| query_matches(&rule.matches, &source.path))
                    else {
                        continue;
                    };
                    let group = directory_group(rule.group, &source)?;
                    groups.entry((index, group)).or_default().push(source);
                }
                for ((rule_index, group), mut sources) in groups {
                    let rule = &rules.rules[rule_index];
                    sources.sort_unstable();
                    sources.dedup();
                    let destination_path = render_directory_output(&rule.output, &group, &sources)?;
                    let task = DirectoryImportTask {
                        rules_bundle: bundle.uuid,
                        rule: rule.id.clone(),
                        group: group.clone(),
                        importer: rule.importer.clone(),
                        sources,
                        settings: rule.settings.clone(),
                        destination_root: group.root.0.clone(),
                        destination_path,
                        listing_dep: listing_dep.clone(),
                    };
                    if self.directory_task_needs_run(&store, &task, &capabilities)? {
                        tasks.push(task);
                    }
                }
            }
        }
        tasks.sort_by(|left, right| {
            left.destination_root
                .cmp(&right.destination_root)
                .then_with(|| left.destination_path.cmp(&right.destination_path))
                .then_with(|| left.rule.0.cmp(&right.rule.0))
        });
        for pair in tasks.windows(2) {
            if pair[0].destination_root == pair[1].destination_root
                && pair[0].destination_path == pair[1].destination_path
            {
                return Err(invalid(format!(
                    "directory import rules collide at {}:{}",
                    pair[0].destination_root, pair[0].destination_path
                )));
            }
        }
        Ok(tasks)
    }

    pub(crate) fn prepare_watched_directory_import(
        &self,
        base: InputVersion,
        task: &DirectoryImportTask,
    ) -> Result<Option<PreparedImportCommit>, RpcFailure> {
        let (importer, invocation) = self.directory_import_invocation(base, task)?;
        match self.execute_import(base, importer, invocation) {
            Ok(prepared) => Ok(Some(prepared)),
            Err(error) if error.memoized => Ok(None),
            Err(error) => Err(error.into_rpc()),
        }
    }

    fn directory_import_invocation(
        &self,
        base: InputVersion,
        task: &DirectoryImportTask,
    ) -> Result<(RegisteredImporter, ImportInvocation), RpcFailure> {
        let importer = self.registered_importer(&task.importer)?;
        validate_default_settings(
            importer.settings_type_uuid,
            importer.settings_hash,
            &importer.settings_schema,
            &task.settings,
        )?;
        let store = self
            .store
            .lock()
            .map_err(|_| invalid("durable store coordinator mutex is poisoned"))?;
        require_base(&store, base)?;
        let destination = self.resolve_directory_destination(
            &store,
            &task.destination_root,
            &task.destination_path,
        )?;
        let prior = destination
            .meta
            .as_ref()
            .map(|meta| self.read_prior_import(&store, meta))
            .transpose()?;
        let origin = DirectoryOrigin {
            rules_bundle: task.rules_bundle,
            rule: task.rule.clone(),
            group: task.group.clone(),
        };
        if prior
            .as_ref()
            .is_some_and(|prior| prior.model.record.origin.as_ref() != Some(&origin))
        {
            return Err(invalid(format!(
                "directory import destination {}:{} is owned by another origin",
                task.destination_root, task.destination_path
            )));
        }
        drop(store);
        Ok((
            importer,
            ImportInvocation {
                destination,
                prior,
                sources: task.sources.clone(),
                explicit_settings: Some(task.settings.clone()),
                watch: true,
                origin: Some(origin),
                basis_deps: vec![task.listing_dep.clone()],
            },
        ))
    }

    fn directory_task_needs_run(
        &self,
        store: &Store,
        task: &DirectoryImportTask,
        capabilities: &BTreeMap<String, [u8; 32]>,
    ) -> Result<bool, RpcFailure> {
        let destination = self.resolve_directory_destination(
            store,
            &task.destination_root,
            &task.destination_path,
        )?;
        let Some(meta) = destination.meta else {
            return Ok(true);
        };
        let prior = self.read_prior_import(store, &meta)?;
        let expected_origin = DirectoryOrigin {
            rules_bundle: task.rules_bundle,
            rule: task.rule.clone(),
            group: task.group.clone(),
        };
        if prior.model.record.origin.as_ref() != Some(&expected_origin) {
            return Err(invalid(format!(
                "directory import output collides with an unowned bundle at {}:{}",
                task.destination_root, task.destination_path
            )));
        }
        if prior.model.record.importer != task.importer
            || prior.model.record.sources != task.sources
            || prior.model.settings != task.settings
            || !prior.model.record.watch
        {
            return Ok(true);
        }
        let mut backend = RootedImportBackend::new(&self.scanner, capabilities);
        let basis = match store.watched_import_failure(meta.bundle).map_err(invalid)? {
            Some(failure) => decode_attempt_basis(&failure.basis)?,
            None => prior.model.record.read_set.clone(),
        };
        Ok(!revalidate_read_set(&basis, &mut backend))
    }

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
                origin: None,
                basis_deps: Vec::new(),
            },
        )
        .map_err(ImportExecutionError::into_rpc)
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
        let prepared = self
            .execute_import(
                base,
                importer,
                ImportInvocation {
                    destination,
                    prior: Some(prior),
                    sources,
                    explicit_settings: None,
                    watch,
                    origin: None,
                    basis_deps: Vec::new(),
                },
            )
            .map_err(ImportExecutionError::into_rpc)?;
        debug_assert_eq!(prepared.bundle, bundle);
        Ok(prepared)
    }

    /// Coordinator-only watched retry. A stable failed attempt that was
    /// durably memoized is a handled outcome, not a background-loop error.
    pub(crate) fn prepare_watched_reimport(
        &self,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<Option<PreparedImportCommit>, RpcFailure> {
        match self.prepare_reimport_for_watcher(base, bundle) {
            Ok(prepared) => Ok(Some(prepared)),
            Err(error) if error.memoized => Ok(None),
            Err(error) => Err(error.into_rpc()),
        }
    }

    fn prepare_reimport_for_watcher(
        &self,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<PreparedImportCommit, ImportExecutionError> {
        let store = self.store.lock().map_err(|_| {
            ImportExecutionError::unmemoized(invalid("durable store coordinator mutex is poisoned"))
        })?;
        require_base(&store, base).map_err(ImportExecutionError::unmemoized)?;
        let meta = store
            .bundle(bundle)
            .map_err(invalid)
            .map_err(ImportExecutionError::unmemoized)?
            .ok_or_else(|| {
                ImportExecutionError::unmemoized(invalid(format!(
                    "cannot reimport unknown bundle {bundle}"
                )))
            })?;
        let prior = self
            .read_prior_import(&store, &meta)
            .map_err(ImportExecutionError::unmemoized)?;
        let importer = self
            .registered_importer(&prior.model.record.importer)
            .map_err(ImportExecutionError::unmemoized)?;
        if prior.settings_type_uuid != importer.settings_type_uuid {
            return Err(ImportExecutionError::unmemoized(invalid(
                "the recorded settings entry type no longer matches the importer registration",
            )));
        }
        let root = store
            .root_name(meta.root)
            .map_err(invalid)
            .map_err(ImportExecutionError::unmemoized)?
            .ok_or_else(|| {
                ImportExecutionError::unmemoized(invalid("bundle root identity is missing"))
            })?;
        let target = self
            .scanner
            .physical_path(&root, &meta.path)
            .map_err(invalid)
            .map_err(ImportExecutionError::unmemoized)?;
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
                origin: None,
                basis_deps: Vec::new(),
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
    ) -> Result<PreparedImportCommit, ImportExecutionError> {
        let ImportInvocation {
            destination,
            prior,
            sources,
            explicit_settings,
            watch,
            origin,
            basis_deps,
        } = invocation;
        let capabilities = self
            .importer_capabilities()
            .map_err(ImportExecutionError::unmemoized)?;
        let mut backend = RootedImportBackend::new(&self.scanner, &capabilities);
        let mut context = ImportContext::new(&importer.id, sources.clone(), &mut backend)
            .map_err(invalid)
            .map_err(ImportExecutionError::unmemoized)?;
        let observed_capability = context
            .importer_capability(&importer.id)
            .map_err(|error| invalid(format!("importer capability lookup failed: {error:?}")))
            .map_err(ImportExecutionError::unmemoized)?;
        if observed_capability != importer.capability_hash {
            return Err(ImportExecutionError::unmemoized(invalid(
                "importer capability changed before execution",
            )));
        }
        let result = {
            let mut adapter = TracedImportContext {
                context: &mut context,
            };
            importer.executor.import(
                &mut adapter,
                explicit_settings.as_ref().unwrap_or_else(|| {
                    prior
                        .as_ref()
                        .map(|prior| &prior.model.settings)
                        .unwrap_or(&importer.default_settings)
                }),
            )
        };
        let mut read_set = context.into_read_set();
        read_set.extend(basis_deps);
        let output = match result {
            Ok(output) => output,
            Err(error) => {
                let terminal = match &error {
                    AuthoringImporterError::Dependency(dependency)
                        if read_set
                            .last()
                            .and_then(dep_failure)
                            .is_some_and(|failure| failure == &dependency.fingerprint) =>
                    {
                        WatchedImportTerminal::Dependency
                    }
                    AuthoringImporterError::Rejected { code, .. } if *code != 0 => {
                        WatchedImportTerminal::Importer { code: *code }
                    }
                    AuthoringImporterError::Dependency(_) => {
                        return Err(ImportExecutionError::unmemoized(invalid(
                            "importer returned a dependency failure that is not its terminal observation",
                        )));
                    }
                    AuthoringImporterError::Rejected { .. } => {
                        return Err(ImportExecutionError::unmemoized(invalid(
                            "importer failure code zero is reserved",
                        )));
                    }
                    AuthoringImporterError::PipelineUnavailable(message) => {
                        return Err(ImportExecutionError::unmemoized(invalid(format!(
                            "pipeline importer became unavailable: {message}"
                        ))));
                    }
                };
                let message = error.message();
                let memoized = self
                    .record_failed_attempt(
                        base,
                        destination.meta.as_ref(),
                        watch,
                        &read_set,
                        terminal,
                        &message,
                    )
                    .map_err(ImportExecutionError::unmemoized)?;
                return Err(ImportExecutionError {
                    rpc: invalid(format!("importer {:?} failed: {message}", importer.id)),
                    memoized,
                });
            }
        };
        if read_set.iter().any(dep_has_failure) {
            let message = "an importer cannot publish after catching a failed context observation";
            let memoized = self
                .record_failed_attempt(
                    base,
                    destination.meta.as_ref(),
                    watch,
                    &read_set,
                    WatchedImportTerminal::Dependency,
                    message,
                )
                .map_err(ImportExecutionError::unmemoized)?;
            return Err(ImportExecutionError {
                rpc: invalid(message),
                memoized,
            });
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
                origin: origin.or_else(|| {
                    prior
                        .as_ref()
                        .and_then(|prior| prior.model.record.origin.clone())
                }),
            },
            &mut ids,
        )
        .map_err(|error| invalid(format!("import fold failed: {error:?}")))
        .map_err(ImportExecutionError::unmemoized)?;

        let mut store = self
            .store
            .lock()
            .map_err(|_| invalid("durable store coordinator mutex is poisoned"))
            .map_err(ImportExecutionError::unmemoized)?;
        require_base(&store, base).map_err(ImportExecutionError::unmemoized)?;
        let capabilities = self
            .importer_capabilities()
            .map_err(ImportExecutionError::unmemoized)?;
        let mut recheck = RootedImportBackend::new(&self.scanner, &capabilities);
        if !revalidate_read_set(&read_set, &mut recheck) {
            return Err(ImportExecutionError::unmemoized(invalid(
                "import read-set changed before publication; the result was discarded",
            )));
        }
        let bytes = build_import_bundle(
            &store,
            &importer,
            &folded,
            prior.as_ref().map(|prior| &prior.bundle),
            &mut ids,
        )
        .map_err(ImportExecutionError::unmemoized)?;
        let bundle = folded.bundle_uuid;
        if store
            .bundle(bundle)
            .map_err(invalid)
            .map_err(ImportExecutionError::unmemoized)?
            .is_some_and(|existing| {
                destination.meta.as_ref().map(|meta| meta.bundle) != Some(existing.bundle)
            })
        {
            return Err(ImportExecutionError::unmemoized(invalid(format!(
                "new import bundle identity {bundle} collides with the existing namespace"
            ))));
        }
        let proposed_bundle = distill_bundle::parse_bundle(&bytes)
            .map_err(invalid)
            .map_err(ImportExecutionError::unmemoized)?;
        for entry in proposed_bundle.assets.values() {
            if store
                .entry(entry.uuid)
                .map_err(invalid)
                .map_err(ImportExecutionError::unmemoized)?
                .is_some_and(|existing| existing.bundle != bundle)
            {
                return Err(ImportExecutionError::unmemoized(invalid(format!(
                    "import-generated asset identity {} collides with another bundle",
                    entry.uuid
                ))));
            }
        }
        let preimage = destination.meta.as_ref().map(|meta| meta.content_hash);
        let basis = encode_import_basis(
            base,
            &destination,
            importer.version,
            importer.capability_hash,
            &bytes,
        );
        if destination.meta.is_some() {
            store
                .clear_watched_import_failure(bundle)
                .map_err(invalid)
                .map_err(ImportExecutionError::unmemoized)?;
        }
        drop(store);
        let commit = self
            .publish_file(
                base,
                PublicationGroupKind::Import,
                &basis,
                destination.target,
                preimage,
                Some(bytes),
            )
            .map_err(ImportExecutionError::unmemoized)?;
        Ok(PreparedImportCommit { bundle, commit })
    }

    fn record_failed_attempt(
        &self,
        base: InputVersion,
        destination: Option<&BundleMeta>,
        watch: bool,
        read_set: &[FileDep],
        terminal: WatchedImportTerminal,
        message: &str,
    ) -> Result<bool, RpcFailure> {
        let Some(destination) = destination.filter(|_| watch) else {
            return Ok(false);
        };
        let basis = encode_attempt_basis(read_set)?;
        let capabilities = self.importer_capabilities()?;
        let mut backend = RootedImportBackend::new(&self.scanner, &capabilities);
        if !revalidate_read_set(read_set, &mut backend) {
            return Ok(false);
        }
        let mut store = self
            .store
            .lock()
            .map_err(|_| invalid("durable store coordinator mutex is poisoned"))?;
        require_base(&store, base)?;
        let memo_seq = store.memo_seq();
        store
            .record_watched_import_failure(&WatchedImportFailure {
                bundle: destination.bundle,
                attempted_input_version: base,
                basis,
                terminal,
                message: message.to_owned(),
                memo_seq,
            })
            .map_err(invalid)?;
        Ok(true)
    }

    fn registered_importer(&self, id: &str) -> Result<RegisteredImporter, RpcFailure> {
        let id = normalize_identifier(id).map_err(invalid)?;
        if let Some(importer) = self
            .pipeline_importers
            .read()
            .map_err(|_| invalid("pipeline importer registry lock is poisoned"))?
            .get(&id)
            .cloned()
        {
            return Ok(importer);
        }
        self.builtin_importers
            .read()
            .map_err(|_| invalid("built-in importer registry lock is poisoned"))?
            .get(&id)
            .cloned()
            .ok_or_else(|| invalid(format!("importer {id:?} is not registered")))
    }

    fn importer_capabilities(&self) -> Result<BTreeMap<String, [u8; 32]>, RpcFailure> {
        let mut capabilities = self
            .builtin_importers
            .read()
            .map_err(|_| invalid("built-in importer registry lock is poisoned"))?
            .iter()
            .map(|(id, importer)| (id.clone(), importer.capability_hash))
            .collect::<BTreeMap<_, _>>();
        for (id, importer) in self
            .pipeline_importers
            .read()
            .map_err(|_| invalid("pipeline importer registry lock is poisoned"))?
            .iter()
        {
            if capabilities
                .insert(id.clone(), importer.capability_hash)
                .is_some()
            {
                return Err(invalid(format!(
                    "importer {id:?} is registered by both the built-in and pipeline registries"
                )));
            }
        }
        Ok(capabilities)
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
            match self.roots_snapshot().as_slice() {
                [root] => root.name.clone(),
                _ => {
                    return Err(invalid(
                        "a new import destination requires an explicit root when multiple roots are configured",
                    ))
                }
            }
        } else {
            let root = normalize_identifier(requested_root).map_err(invalid)?;
            if !self
                .roots_snapshot()
                .iter()
                .any(|candidate| candidate.name == root)
            {
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

    fn resolve_directory_destination(
        &self,
        store: &Store,
        root: &str,
        path: &str,
    ) -> Result<ImportDestination, RpcFailure> {
        let root = normalize_identifier(root).map_err(invalid)?;
        let path = normalize_path(path).map_err(invalid)?;
        if !self
            .roots_snapshot()
            .iter()
            .any(|candidate| candidate.name == root)
        {
            return Err(invalid(format!(
                "unknown directory import destination root {root:?}"
            )));
        }
        let mut meta = None;
        for candidate in store.all_bundles().map_err(invalid)? {
            if candidate.path != path {
                continue;
            }
            let candidate_root = store
                .root_name(candidate.root)
                .map_err(invalid)?
                .ok_or_else(|| invalid("bundle root identity is missing"))?;
            if candidate_root == root {
                meta = Some(candidate);
                break;
            }
        }
        let target = self.scanner.physical_path(&root, &path).map_err(invalid)?;
        if meta.is_none() && std::fs::symlink_metadata(&target).is_ok() {
            return Err(invalid(format!(
                "directory import destination {}:{} is occupied by a non-bundle file",
                root, path
            )));
        }
        Ok(ImportDestination {
            root,
            path,
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
            Err(class) => {
                *observed
                    == Observed::Err(StableFailureFingerprint::RawFile {
                        op: RawFileOp::Read,
                        subject: RawFileSubject::Path(path.clone()),
                        class,
                    })
            }
        },
        FileDep::Probe { path, observed } => match backend.probe(path) {
            Ok(value) => *observed == Observed::Ok(value),
            Err(class) => {
                *observed
                    == Observed::Err(StableFailureFingerprint::RawFile {
                        op: RawFileOp::Probe,
                        subject: RawFileSubject::Path(path.clone()),
                        class,
                    })
            }
        },
        FileDep::Listing { query, observed } => match backend.enumerate(query) {
            Ok(mut paths) => {
                paths.sort_unstable();
                paths.dedup();
                *observed == Observed::Ok(distill_build::query::file_query_result_hash(&paths))
            }
            Err(class) => {
                *observed
                    == Observed::Err(StableFailureFingerprint::RawFile {
                        op: RawFileOp::Enumerate,
                        subject: RawFileSubject::Query(query.clone()),
                        class,
                    })
            }
        },
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
    dep_failure(dep).is_some()
}

fn dep_failure(dep: &FileDep) -> Option<&distill_build::trace::StableFailureFingerprint> {
    match dep {
        FileDep::Read {
            observed: Observed::Err(failure),
            ..
        }
        | FileDep::Probe {
            observed: Observed::Err(failure),
            ..
        }
        | FileDep::Listing {
            observed: Observed::Err(failure),
            ..
        }
        | FileDep::Capability {
            observed: Observed::Err(failure),
            ..
        } => Some(failure),
        _ => None,
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
    let read_set = as_array(field(fields, "read_set")?, "ImportRecord.read_set")?
        .iter()
        .map(decode_file_dep)
        .collect::<Result<_, _>>()?;
    Ok(ImportRecord {
        importer,
        sources,
        watch,
        read_set,
        origin,
    })
}

pub(crate) fn decoded_import_record(bundle: &Bundle) -> Result<Option<ImportRecord>, RpcFailure> {
    let Some(record) = bundle.assets.get("$record") else {
        return Ok(None);
    };
    if record.type_uuid != IMPORT_RECORD_TYPE_UUID || !record.authoring_only {
        return Err(invalid("$record has the wrong built-in type or role"));
    }
    decode_import_record(&record.data).map(Some)
}

pub(crate) fn decoded_directory_origin(
    bundle: &Bundle,
) -> Result<Option<distill_store::bundles::DirectoryOrigin>, RpcFailure> {
    Ok(decoded_import_record(bundle)?
        .and_then(|record| record.origin)
        .map(|origin| distill_store::bundles::DirectoryOrigin {
            rules_bundle: origin.rules_bundle,
            rule: distill_store::bundles::DirectoryRuleId(origin.rule.0),
            group_root: origin.group.root.0,
            group_path: origin.group.path,
        }))
}

fn decode_file_dep(value: &AuthoredValue) -> Result<FileDep, RpcFailure> {
    let variants = as_object(value, "FileDep")?;
    if variants.len() != 1 {
        return Err(invalid("FileDep must contain exactly one variant"));
    }
    let (variant, payload) = variants.first_key_value().expect("one variant");
    let fields = as_object(payload, "FileDep payload")?;
    match variant.as_str() {
        "Read" => Ok(FileDep::Read {
            path: as_string(field(fields, "path")?, "FileDep.Read.path")?.to_owned(),
            observed: decode_observed_content(field(fields, "observed")?)?,
        }),
        "Probe" => Ok(FileDep::Probe {
            path: as_string(field(fields, "path")?, "FileDep.Probe.path")?.to_owned(),
            observed: decode_observed_root(field(fields, "observed")?)?,
        }),
        "Listing" => Ok(FileDep::Listing {
            query: decode_file_query(field(fields, "query")?)?,
            observed: decode_observed_hash(field(fields, "observed")?)?,
        }),
        "Capability" => Ok(FileDep::Capability {
            key: decode_capability_key(field(fields, "key")?)?,
            observed: decode_observed_hash(field(fields, "observed")?)?,
        }),
        _ => Err(invalid(format!("unknown FileDep variant {variant:?}"))),
    }
}

fn encode_attempt_basis(read_set: &[FileDep]) -> Result<Vec<u8>, RpcFailure> {
    let value = object([
        (
            "read_set",
            AuthoredValue::Array(
                read_set
                    .iter()
                    .map(encode_attempt_file_dep)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        ),
        ("version", AuthoredValue::UInt(1)),
    ]);
    distill_json::write(&value)
        .map(String::into_bytes)
        .map_err(invalid)
}

fn decode_attempt_basis(bytes: &[u8]) -> Result<Vec<FileDep>, RpcFailure> {
    let text = std::str::from_utf8(bytes).map_err(invalid)?;
    let value = distill_json::parse(text).map_err(invalid)?;
    if distill_json::write(&value).map_err(invalid)?.as_bytes() != bytes {
        return Err(invalid(
            "watched import failure basis is not canonical JSON",
        ));
    }
    let fields = as_object(&value, "watched import failure basis")?;
    if fields.len() != 2 || as_u128(field(fields, "version")?, "basis.version")? != 1 {
        return Err(invalid("unsupported watched import failure basis"));
    }
    as_array(field(fields, "read_set")?, "basis.read_set")?
        .iter()
        .map(decode_attempt_file_dep)
        .collect()
}

fn encode_attempt_file_dep(dep: &FileDep) -> Result<AuthoredValue, RpcFailure> {
    let (variant, fields) = match dep {
        FileDep::Read { path, observed } => (
            "Read",
            object([
                ("observed", encode_attempt_content(observed)?),
                ("path", AuthoredValue::Str(path.clone())),
            ]),
        ),
        FileDep::Probe { path, observed } => (
            "Probe",
            object([
                ("observed", encode_attempt_root(observed)?),
                ("path", AuthoredValue::Str(path.clone())),
            ]),
        ),
        FileDep::Listing { query, observed } => (
            "Listing",
            object([
                ("observed", encode_attempt_hash(observed)?),
                ("query", encode_file_query(query)),
            ]),
        ),
        FileDep::Capability { key, observed } => (
            "Capability",
            object([
                ("key", encode_capability_key(key)),
                ("observed", encode_attempt_hash(observed)?),
            ]),
        ),
    };
    Ok(object([(variant, fields)]))
}

fn decode_attempt_file_dep(value: &AuthoredValue) -> Result<FileDep, RpcFailure> {
    let variants = as_object(value, "attempted FileDep")?;
    if variants.len() != 1 {
        return Err(invalid("attempted FileDep must contain one variant"));
    }
    let (variant, payload) = variants.first_key_value().expect("one variant");
    let fields = as_object(payload, "attempted FileDep payload")?;
    match variant.as_str() {
        "Read" => Ok(FileDep::Read {
            path: as_string(field(fields, "path")?, "FileDep.Read.path")?.to_owned(),
            observed: decode_attempt_content(field(fields, "observed")?)?,
        }),
        "Probe" => Ok(FileDep::Probe {
            path: as_string(field(fields, "path")?, "FileDep.Probe.path")?.to_owned(),
            observed: decode_attempt_root(field(fields, "observed")?)?,
        }),
        "Listing" => Ok(FileDep::Listing {
            query: decode_file_query(field(fields, "query")?)?,
            observed: decode_attempt_hash(field(fields, "observed")?)?,
        }),
        "Capability" => Ok(FileDep::Capability {
            key: decode_capability_key(field(fields, "key")?)?,
            observed: decode_attempt_hash(field(fields, "observed")?)?,
        }),
        _ => Err(invalid(format!(
            "unknown attempted FileDep variant {variant:?}"
        ))),
    }
}

fn encode_attempt_content(
    observed: &Observed<distill_build::import::FileContentObservation>,
) -> Result<AuthoredValue, RpcFailure> {
    match observed {
        Observed::Ok(_) => encode_observed_content(observed),
        Observed::Err(failure) => encode_attempt_error(failure),
    }
}

fn encode_attempt_root(observed: &Observed<Option<RootName>>) -> Result<AuthoredValue, RpcFailure> {
    match observed {
        Observed::Ok(_) => encode_observed_root(observed),
        Observed::Err(failure) => encode_attempt_error(failure),
    }
}

fn encode_attempt_hash(observed: &Observed<[u8; 32]>) -> Result<AuthoredValue, RpcFailure> {
    match observed {
        Observed::Ok(_) => encode_observed_hash(observed),
        Observed::Err(failure) => encode_attempt_error(failure),
    }
}

fn encode_attempt_error(failure: &StableFailureFingerprint) -> Result<AuthoredValue, RpcFailure> {
    Ok(object([(
        "Err",
        object([("failure", encode_import_failure(failure)?)]),
    )]))
}

fn decode_attempt_content(
    value: &AuthoredValue,
) -> Result<Observed<distill_build::import::FileContentObservation>, RpcFailure> {
    if observed_is(value, "Ok")? {
        decode_observed_content(value)
    } else {
        decode_attempt_error(value).map(Observed::Err)
    }
}

fn decode_attempt_root(value: &AuthoredValue) -> Result<Observed<Option<RootName>>, RpcFailure> {
    if observed_is(value, "Ok")? {
        decode_observed_root(value)
    } else {
        decode_attempt_error(value).map(Observed::Err)
    }
}

fn decode_attempt_hash(value: &AuthoredValue) -> Result<Observed<[u8; 32]>, RpcFailure> {
    if observed_is(value, "Ok")? {
        decode_observed_hash(value)
    } else {
        decode_attempt_error(value).map(Observed::Err)
    }
}

fn observed_is(value: &AuthoredValue, variant: &str) -> Result<bool, RpcFailure> {
    let variants = as_object(value, "attempted observation")?;
    if variants.len() != 1 || (!variants.contains_key("Ok") && !variants.contains_key("Err")) {
        return Err(invalid(
            "attempted observation must contain one Ok or Err variant",
        ));
    }
    Ok(variants.contains_key(variant))
}

fn decode_attempt_error(value: &AuthoredValue) -> Result<StableFailureFingerprint, RpcFailure> {
    let variants = as_object(value, "attempted observation")?;
    let payload = variants
        .get("Err")
        .ok_or_else(|| invalid("attempted failure has no Err variant"))?;
    decode_import_failure(field(as_object(payload, "Err payload")?, "failure")?)
}

fn encode_import_failure(failure: &StableFailureFingerprint) -> Result<AuthoredValue, RpcFailure> {
    let (variant, payload) = match failure {
        StableFailureFingerprint::RawFile { op, subject, class } => (
            "RawFile",
            object([
                ("class", AuthoredValue::UInt(*class as u8 as u128)),
                ("op", AuthoredValue::UInt(*op as u8 as u128)),
                (
                    "subject",
                    match subject {
                        RawFileSubject::Path(path) => {
                            object([("Path", AuthoredValue::Str(path.clone()))])
                        }
                        RawFileSubject::Query(query) => {
                            object([("Query", encode_file_query(query))])
                        }
                    },
                ),
            ]),
        ),
        StableFailureFingerprint::MissingCapability { key } => (
            "MissingCapability",
            object([("key", encode_capability_key(key))]),
        ),
        StableFailureFingerprint::Local { class, detail } => (
            "Local",
            object([
                ("class", AuthoredValue::UInt(*class as u16 as u128)),
                ("detail", byte_array(detail)),
            ]),
        ),
        _ => {
            return Err(invalid(
                "failure fingerprint is not valid in an authoring import basis",
            ));
        }
    };
    Ok(object([(variant, payload)]))
}

fn decode_import_failure(value: &AuthoredValue) -> Result<StableFailureFingerprint, RpcFailure> {
    let variants = as_object(value, "import failure fingerprint")?;
    if variants.len() != 1 {
        return Err(invalid(
            "import failure fingerprint must contain one variant",
        ));
    }
    let (variant, payload) = variants.first_key_value().expect("one variant");
    let fields = as_object(payload, "import failure payload")?;
    match variant.as_str() {
        "RawFile" => {
            let op = match as_u128(field(fields, "op")?, "RawFile.op")? {
                0 => RawFileOp::Read,
                1 => RawFileOp::Probe,
                2 => RawFileOp::Enumerate,
                value => return Err(invalid(format!("unknown raw-file op {value}"))),
            };
            let class = match as_u128(field(fields, "class")?, "RawFile.class")? {
                0 => RawFileFailureClass::NotFound,
                1 => RawFileFailureClass::PermissionDenied,
                2 => RawFileFailureClass::ListingFailed,
                3 => RawFileFailureClass::OtherStable,
                value => return Err(invalid(format!("unknown raw-file failure class {value}"))),
            };
            let subject = as_object(field(fields, "subject")?, "RawFile.subject")?;
            if subject.len() != 1 {
                return Err(invalid("raw-file subject must contain one variant"));
            }
            let subject = if let Some(path) = subject.get("Path") {
                RawFileSubject::Path(as_string(path, "RawFile.Path")?.to_owned())
            } else if let Some(query) = subject.get("Query") {
                RawFileSubject::Query(decode_file_query(query)?)
            } else {
                return Err(invalid("unknown raw-file subject"));
            };
            Ok(StableFailureFingerprint::RawFile { op, subject, class })
        }
        "MissingCapability" => Ok(StableFailureFingerprint::MissingCapability {
            key: decode_capability_key(field(fields, "key")?)?,
        }),
        "Local" => {
            let class = match as_u128(field(fields, "class")?, "Local.class")? {
                1 => LocalFailureClass::Validator,
                2 => LocalFailureClass::MigrationPlan,
                3 => LocalFailureClass::Processor,
                4 => LocalFailureClass::MigrationFunction,
                5 => LocalFailureClass::OutputBinding,
                6 => LocalFailureClass::Importer,
                7 => LocalFailureClass::ImportIntake,
                8 => LocalFailureClass::ArtifactEncoding,
                value => return Err(invalid(format!("unknown local failure class {value}"))),
            };
            Ok(StableFailureFingerprint::Local {
                class,
                detail: fixed_bytes(field(fields, "detail")?, "Local.detail")?,
            })
        }
        _ => Err(invalid(format!(
            "unknown import failure fingerprint {variant:?}"
        ))),
    }
}

fn decode_observed_content(
    value: &AuthoredValue,
) -> Result<Observed<distill_build::import::FileContentObservation>, RpcFailure> {
    let value = decode_observed_ok(value, "file content")?;
    let fields = as_object(value, "FileContentObservation")?;
    Ok(Observed::Ok(
        distill_build::import::FileContentObservation {
            path: decode_rooted_path(field(fields, "path")?)?,
            hash: fixed_bytes(field(fields, "hash")?, "FileContentObservation.hash")?,
        },
    ))
}

fn decode_observed_root(value: &AuthoredValue) -> Result<Observed<Option<RootName>>, RpcFailure> {
    Ok(Observed::Ok(
        match decode_observed_ok(value, "root probe")? {
            AuthoredValue::Null => None,
            value => Some(RootName::new(as_string(value, "root probe value")?).map_err(invalid)?),
        },
    ))
}

fn decode_observed_hash(value: &AuthoredValue) -> Result<Observed<[u8; 32]>, RpcFailure> {
    Ok(Observed::Ok(fixed_bytes(
        decode_observed_ok(value, "hash observation")?,
        "hash observation value",
    )?))
}

fn decode_observed_ok<'a>(
    value: &'a AuthoredValue,
    context: &str,
) -> Result<&'a AuthoredValue, RpcFailure> {
    let variants = as_object(value, context)?;
    let Some(payload) = variants.get("Ok") else {
        return Err(invalid(format!(
            "{context} contains a failed observation; committed import records permit only successful observations"
        )));
    };
    if variants.len() != 1 {
        return Err(invalid(format!("{context} must contain one Ok variant")));
    }
    field(as_object(payload, context)?, "value")
}

fn decode_capability_key(value: &AuthoredValue) -> Result<CapabilityKey, RpcFailure> {
    let variants = as_object(value, "CapabilityKey")?;
    if variants.len() != 1 {
        return Err(invalid("CapabilityKey must contain exactly one variant"));
    }
    let (variant, payload) = variants.first_key_value().expect("one variant");
    let fields = as_object(payload, "CapabilityKey payload")?;
    match variant.as_str() {
        "MigrationFn" => Ok(CapabilityKey::MigrationFn(
            as_string(field(fields, "key")?, "MigrationFn.key")?.to_owned(),
        )),
        "DefaultTable" => Ok(CapabilityKey::DefaultTable(TypeUuid(fixed_bytes(
            field(fields, "type_uuid")?,
            "DefaultTable.type_uuid",
        )?))),
        "Importer" => Ok(CapabilityKey::Importer(
            as_string(field(fields, "id")?, "Importer.id")?.to_owned(),
        )),
        "Processor" => Ok(CapabilityKey::Processor {
            input: TypeUuid(fixed_bytes(field(fields, "input")?, "Processor.input")?),
        }),
        "Tool" => Ok(CapabilityKey::Tool(
            as_string(field(fields, "id")?, "Tool.id")?.to_owned(),
        )),
        _ => Err(invalid(format!(
            "unknown CapabilityKey variant {variant:?}"
        ))),
    }
}

fn decode_file_query(value: &AuthoredValue) -> Result<FileQuery, RpcFailure> {
    let fields = as_object(value, "FileQuery")?;
    let optional = |name: &str| -> Result<Option<String>, RpcFailure> {
        match field(fields, name)? {
            AuthoredValue::Null => Ok(None),
            value => Ok(Some(as_string(value, name)?.to_owned())),
        }
    };
    FileQuery::new(optional("path_prefix")?, optional("path_glob")?).map_err(invalid)
}

fn decode_directory_rules(value: &AuthoredValue) -> Result<DecodedDirectoryRules, RpcFailure> {
    let fields = as_object(value, "DirectoryImportRules")?;
    let listing = decode_file_query(field(fields, "listing")?)?;
    let rules = as_array(field(fields, "rules")?, "DirectoryImportRules.rules")?
        .iter()
        .map(|value| {
            let fields = as_object(value, "ImportRule")?;
            Ok(DecodedDirectoryRule {
                id: distill_build::import::ImportRuleId(fixed_bytes(
                    field(fields, "id")?,
                    "ImportRule.id",
                )?),
                matches: decode_file_query(field(fields, "matches")?)?,
                group: decode_directory_grouping(field(fields, "group")?)?,
                importer: normalize_identifier(as_string(
                    field(fields, "importer")?,
                    "ImportRule.importer",
                )?)
                .map_err(invalid)?,
                settings: decode_wrapped_authored_value(field(fields, "settings")?, 0)?,
                output: as_string(field(fields, "output")?, "ImportRule.output")?.to_owned(),
            })
        })
        .collect::<Result<Vec<_>, RpcFailure>>()?;
    if rules.is_empty() {
        return Err(invalid("DirectoryImportRules.rules cannot be empty"));
    }
    Ok(DecodedDirectoryRules { listing, rules })
}

fn decode_directory_grouping(value: &AuthoredValue) -> Result<DirectoryGrouping, RpcFailure> {
    let variants = as_object(value, "Grouping")?;
    if variants.len() != 1 {
        return Err(invalid("Grouping must contain exactly one variant"));
    }
    let (variant, payload) = variants.first_key_value().expect("one variant");
    if !matches!(payload, AuthoredValue::Object(fields) if fields.is_empty()) {
        return Err(invalid("Grouping unit payload must be an empty object"));
    }
    match variant.as_str() {
        "PerFile" => Ok(DirectoryGrouping::PerFile),
        "ByStem" => Ok(DirectoryGrouping::ByStem),
        _ => Err(invalid(format!("unknown Grouping variant {variant:?}"))),
    }
}

fn decode_wrapped_authored_value(
    value: &AuthoredValue,
    depth: usize,
) -> Result<AuthoredValue, RpcFailure> {
    if depth >= 1024 {
        return Err(invalid("AuthoredValueV1 exceeds the nesting limit"));
    }
    let variants = as_object(value, "AuthoredValueV1")?;
    if variants.len() != 1 {
        return Err(invalid("AuthoredValueV1 must contain exactly one variant"));
    }
    let (variant, payload) = variants.first_key_value().expect("one variant");
    let fields = as_object(payload, "AuthoredValueV1 payload")?;
    match variant.as_str() {
        "Null" if fields.is_empty() => Ok(AuthoredValue::Null),
        "Bool" => Ok(AuthoredValue::Bool(as_bool(
            field(fields, "value")?,
            "AuthoredValueV1.Bool.value",
        )?)),
        "Int" => Ok(AuthoredValue::Int(as_i128(
            field(fields, "value")?,
            "AuthoredValueV1.Int.value",
        )?)),
        "UInt" => Ok(AuthoredValue::UInt(as_u128(
            field(fields, "value")?,
            "AuthoredValueV1.UInt.value",
        )?)),
        "Float" => Ok(AuthoredValue::Float(as_f64(
            field(fields, "value")?,
            "AuthoredValueV1.Float.value",
        )?)),
        "Str" => Ok(AuthoredValue::Str(
            as_string(field(fields, "value")?, "AuthoredValueV1.Str.value")?.to_owned(),
        )),
        "Array" => Ok(AuthoredValue::Array(
            as_array(field(fields, "value")?, "AuthoredValueV1.Array.value")?
                .iter()
                .map(|value| decode_wrapped_authored_value(value, depth + 1))
                .collect::<Result<_, _>>()?,
        )),
        "Object" => Ok(AuthoredValue::Object(
            as_object(field(fields, "value")?, "AuthoredValueV1.Object.value")?
                .iter()
                .map(|(key, value)| {
                    Ok((
                        key.clone(),
                        decode_wrapped_authored_value(value, depth + 1)?,
                    ))
                })
                .collect::<Result<_, RpcFailure>>()?,
        )),
        "Blob" => match field(fields, "value")? {
            AuthoredValue::Blob(bytes) => Ok(AuthoredValue::Blob(bytes.clone())),
            _ => Err(invalid("AuthoredValueV1.Blob.value must be blob bytes")),
        },
        _ => Err(invalid(format!(
            "unknown or malformed AuthoredValueV1 variant {variant:?}"
        ))),
    }
}

fn query_matches(query: &FileQuery, path: &str) -> bool {
    let prefix_matches = query.path_prefix.as_ref().is_none_or(|prefix| {
        path == prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('/'))
    });
    let glob_matches = query.path_glob.as_ref().is_none_or(|pattern| {
        Glob::new(pattern).is_ok_and(|glob| glob.compile_matcher().is_match(path))
    });
    prefix_matches && glob_matches
}

fn directory_group(
    grouping: DirectoryGrouping,
    source: &RootedPath,
) -> Result<RootedPath, RpcFailure> {
    match grouping {
        DirectoryGrouping::PerFile => Ok(source.clone()),
        DirectoryGrouping::ByStem => {
            let (parent, name) = split_parent_name(&source.path);
            let stem = file_stem(name)?;
            let path = if parent.is_empty() {
                stem.to_owned()
            } else {
                format!("{parent}/{stem}")
            };
            RootedPath::new(&source.root.0, &path).map_err(invalid)
        }
    }
}

fn render_directory_output(
    template: &str,
    group: &RootedPath,
    sources: &[RootedPath],
) -> Result<String, RpcFailure> {
    let first = sources
        .first()
        .ok_or_else(|| invalid("directory import group has no sources"))?;
    if sources.iter().any(|source| source.root != group.root) {
        return Err(invalid("directory import group spans multiple roots"));
    }
    let (parent, group_name) = split_parent_name(&group.path);
    let (_, source_name) = split_parent_name(&first.path);
    let rendered = template
        .replace("{stem}", file_stem(group_name)?)
        .replace("{name}", source_name);
    if rendered.is_empty() || rendered.contains('/') || rendered.contains('\\') {
        return Err(invalid(
            "directory import output template must render one nonempty file name",
        ));
    }
    normalize_path(&if parent.is_empty() {
        rendered
    } else {
        format!("{parent}/{rendered}")
    })
    .map_err(invalid)
}

fn split_parent_name(path: &str) -> (&str, &str) {
    path.rsplit_once('/')
        .map_or(("", path), |(parent, name)| (parent, name))
}

fn file_stem(name: &str) -> Result<&str, RpcFailure> {
    let stem = name
        .rsplit_once('.')
        .map_or(name, |(stem, _)| if stem.is_empty() { name } else { stem });
    if stem.is_empty() {
        Err(invalid("directory import source has no file stem"))
    } else {
        Ok(stem)
    }
}

fn uuid_text(bytes: [u8; 16]) -> String {
    AssetUuid(bytes).to_string()
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

fn as_u128(value: &AuthoredValue, context: &str) -> Result<u128, RpcFailure> {
    match value {
        AuthoredValue::UInt(value) => Ok(*value),
        _ => Err(invalid(format!("{context} must be an unsigned integer"))),
    }
}

fn as_i128(value: &AuthoredValue, context: &str) -> Result<i128, RpcFailure> {
    match value {
        AuthoredValue::Int(value) => Ok(*value),
        AuthoredValue::UInt(value) => {
            i128::try_from(*value).map_err(|_| invalid(format!("{context} is outside i128 range")))
        }
        _ => Err(invalid(format!("{context} must be an integer"))),
    }
}

fn as_f64(value: &AuthoredValue, context: &str) -> Result<f64, RpcFailure> {
    match value {
        AuthoredValue::Float(value) => Ok(*value),
        _ => Err(invalid(format!("{context} must be a float"))),
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
    origin: Option<DirectoryOrigin>,
    basis_deps: Vec<FileDep>,
}

struct ImportExecutionError {
    rpc: RpcFailure,
    memoized: bool,
}

impl ImportExecutionError {
    fn unmemoized(rpc: RpcFailure) -> Self {
        Self {
            rpc,
            memoized: false,
        }
    }

    fn into_rpc(self) -> RpcFailure {
        self.rpc
    }
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
