//! Which layer claims a source file (§8 "Default imports" precedence:
//! explicit > rules bundle > default), for the authoring listing
//! (`AuthoringSnapshot.sourceFiles`, §17 "Browsing"), and the release of an
//! explicit import back to the rule claiming its sources (`Hub.release`).

use std::collections::BTreeSet;

use distill_build::import::NO_IMPORTER;
use distill_build::query::RootedPath;
use distill_core::id::BundleUuid;
use distill_rpc::{InputVersion, RpcFailure, SourceClaim, SourceFile};
use distill_store::files::FileKind;
use distill_store::StoreReader;
use globset::Glob;

use super::{
    complete_task_settings, default_rule_entries, directory_assignment, indexed_directory_task,
    query_matches, render_directory_output, DirectoryRuleEntry, FreshRules, ImportExecutionError,
    ImportInvocation, ImportRun, RegisteredImporter,
};
use crate::authoring::{invalid, require_base, AuthoringService};
use crate::default_imports::DefaultImports;

/// The rule claiming `source` among `entries` (authored entries first, then
/// the default layer's), with its rule index and the group `source` is in.
fn claiming_rule<'e>(
    entries: &'e [DirectoryRuleEntry],
    defaults: Option<&DefaultImports>,
    source: &RootedPath,
) -> Result<Option<(&'e DirectoryRuleEntry, usize, RootedPath)>, RpcFailure> {
    for entry in entries {
        if entry.default_rule.is_some() {
            let Some(root) = defaults.and_then(|defaults| defaults.root(&entry.root_name)) else {
                continue;
            };
            if entry.root_name != source.root.0 || root.excludes(&source.path) {
                continue;
            }
        }
        if !query_matches(&entry.rules.listing, &source.path) {
            continue;
        }
        if let Some((index, group)) = directory_assignment(&entry.rules, source)? {
            return Ok(Some((entry, index, group)));
        }
    }
    Ok(None)
}

impl AuthoringService {
    /// Every rule entry of `store`: the authored ones, then the default
    /// layer's.
    fn claim_entries(
        &self,
        store: &StoreReader,
        defaults: Option<&DefaultImports>,
    ) -> Result<Vec<DirectoryRuleEntry>, RpcFailure> {
        let sources = store.directory_rule_sources().map_err(invalid)?;
        let mut entries = self.directory_rule_entries(store, sources, &FreshRules::new())?;
        if let Some(defaults) = defaults {
            entries.extend(default_rule_entries(defaults));
        }
        Ok(entries)
    }

    /// The files under `root` (bundle files excepted) `glob` matches, with
    /// the layer claiming each at `store`.
    pub(crate) fn source_files(
        &self,
        store: &StoreReader,
        root: &str,
        glob: Option<&str>,
    ) -> Result<Vec<SourceFile>, RpcFailure> {
        let compiled = self.compiled(store)?;
        let glob = glob
            .map(|pattern| Glob::new(pattern).map(|glob| glob.compile_matcher()))
            .transpose()
            .map_err(invalid)?;
        let defaults = compiled.default_imports();
        let explicit = store.explicit_import_claims().map_err(invalid)?;
        let entries = self.claim_entries(store, defaults)?;
        let mut files = Vec::new();
        for row in store.observed_files_under(root, "").map_err(invalid)? {
            if !matches!(row.file.state.kind, FileKind::File | FileKind::Symlink)
                || row.path.ends_with(".bundle")
                || glob.as_ref().is_some_and(|glob| !glob.is_match(&row.path))
            {
                continue;
            }
            let claim = match explicit.get(&(row.root_name.clone(), row.path.clone())) {
                Some(bundle) => SourceClaim::Explicit { bundle: *bundle },
                None => {
                    let source = RootedPath::new(&row.root_name, &row.path).map_err(invalid)?;
                    match claiming_rule(&entries, defaults, &source)? {
                        Some((entry, index, group)) => {
                            let rule = &entry.rules.rules[index];
                            let output =
                                render_directory_output(&rule.output, &group, &[source.clone()])?;
                            let bundle = store
                                .bundle_at(&row.root_name, &output)
                                .map_err(invalid)?
                                .map(|meta| meta.bundle);
                            SourceClaim::Rule {
                                rules_bundle: entry.default_rule.is_none().then_some(entry.rules_bundle),
                                rule: rule.id.0,
                                importer: rule.importer.clone(),
                                output,
                                bundle,
                            }
                        }
                        None => SourceClaim::Unclaimed,
                    }
                }
            };
            files.push(SourceFile {
                root: row.root_name,
                path: row.path,
                claim,
            });
        }
        Ok(files)
    }

    /// Run a release of explicit import `bundle` at `base` without
    /// publishing it: the rule claiming its sources re-imports it under
    /// that rule's settings and origin, keeping its bundle and entry UUIDs.
    pub(crate) fn run_release_bundle(
        &self,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<ImportRun, RpcFailure> {
        let (importer, invocation) = {
            let snapshot = self.snapshot()?;
            self.release_invocation(&snapshot, base, bundle)?
        };
        self.run_import(base, importer, invocation, None)
            .map_err(ImportExecutionError::into_rpc)
    }

    fn release_invocation(
        &self,
        store: &StoreReader,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<(RegisteredImporter, ImportInvocation), RpcFailure> {
        require_base(store, base)?;
        let meta = store
            .bundle(bundle)
            .map_err(invalid)?
            .ok_or_else(|| invalid(format!("cannot release unknown bundle {bundle}")))?;
        if meta.origin.is_some() {
            return Err(invalid(format!(
                "bundle {} follows a rule already",
                meta.path
            )));
        }
        let root = store
            .root_name(meta.root)
            .map_err(invalid)?
            .ok_or_else(|| invalid("bundle root identity is missing"))?;
        let prior = self.read_prior_import(store, &meta)?;
        let sources = prior.model.record.sources.iter().cloned().collect::<BTreeSet<_>>();
        let first = sources
            .first()
            .ok_or_else(|| invalid(format!("import {} names no sources", meta.path)))?;
        let compiled = self.compiled(store)?;
        let entries = self.claim_entries(store, compiled.default_imports())?;
        let Some((entry, index, group)) =
            claiming_rule(&entries, compiled.default_imports(), first)?
        else {
            return Err(invalid(format!(
                "no rule claims {}:{}: {} has no rule to follow",
                first.root.0, first.path, meta.path
            )));
        };
        let mut task = indexed_directory_task(entry, index, &group, &sources)?;
        if task.importer == NO_IMPORTER {
            return Err(invalid(format!(
                "the rule claiming {}:{} imports nothing (importer {NO_IMPORTER})",
                first.root.0, first.path
            )));
        }
        if task.destination_root != root || task.destination_path != meta.path {
            return Err(invalid(format!(
                "the rule claiming {}:{} writes {}:{}, not {}:{}",
                first.root.0,
                first.path,
                task.destination_root,
                task.destination_path,
                root,
                meta.path
            )));
        }
        let registered = compiled
            .pipeline_importers()
            .get(&task.importer)
            .ok_or_else(|| invalid(format!("importer {} is not registered", task.importer)))?;
        complete_task_settings(&mut task, registered);
        // The rule takes the explicit import's bundle over, as a default
        // task adopts an orphan: identities kept, its origin recorded.
        task.adopts = Some(bundle);
        self.directory_import_invocation(store, base, &task)
    }
}
