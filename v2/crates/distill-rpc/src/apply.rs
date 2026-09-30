//! Publishing RPC state into the store (LOCKLESS.md §2.2). A typed
//! [`Commit`] is applied to the served tables inside the publishing input
//! transaction, so a version is never visible without its RPC projection.
//! Fence changes that publish no input version (runtime pipeline failure,
//! restart keys, target replacement) are [`ServedWrite`] helpers too.

use std::collections::{BTreeMap, BTreeSet};

use distill_store::bundles::{AssetRecord, BundleMeta, ServedAuthoring};
use distill_store::served::{
    encode_authored_value, Change, DerivedOutputRow, ResolutionRow, ServedWrite,
    SERVED_PIPELINE, SERVED_RESTART_KEYS,
};
use distill_store::{InputTxn, StoreError};

use crate::persist::{
    decode_served_pipeline, delta_state_code, encode_drifted_input, encode_served_pipeline,
    reconnect_code,
};
use crate::validate::validate_commit;
use crate::*;

/// Input versions of subscription history kept in the change log.
pub const RETAINED_HISTORY_VERSIONS: u64 = 4096;

/// The root name embedded stores file RPC-authored bundles under.
const EMBEDDED_ROOT: &str = "rpc";

/// What [`apply_commit`] writes besides the served-only state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyMode {
    /// The embedded server owns the namespace: authoring entries, paths,
    /// derived outputs, tags, configuration and namespace errors are written
    /// from the commit.
    Full,
    /// The daemon already wrote the namespace in the same transaction; only
    /// resolutions, diagnostics, fences and the change log are written.
    Delta,
}

/// Why a commit could not be applied. `Invalid` rejects the commit itself;
/// either way the caller's transaction must roll back.
#[derive(Debug)]
pub enum ApplyError {
    Invalid(AdminError),
    Store(StoreError),
}

impl From<StoreError> for ApplyError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<AdminError> for ApplyError {
    fn from(error: AdminError) -> Self {
        Self::Invalid(error)
    }
}

impl From<ApplyError> for StoreError {
    fn from(error: ApplyError) -> Self {
        match error {
            ApplyError::Invalid(error) => StoreError::Rejected {
                detail: format!("{error:?}"),
            },
            ApplyError::Store(error) => error,
        }
    }
}

fn corrupt(detail: impl std::fmt::Display) -> StoreError {
    StoreError::Rejected {
        detail: detail.to_string(),
    }
}

/// The served pipeline diagnostic and the input version that installed it.
/// No blob means the pipeline has been Ready since the store began.
pub(crate) fn read_served_pipeline(
    bytes: Option<Vec<u8>>,
) -> Result<(InputVersion, PipelineDiagnostic), StoreError> {
    match bytes {
        None => Ok((InputVersion(0), PipelineDiagnostic::Ready)),
        Some(bytes) => decode_served_pipeline(&bytes).map_err(corrupt),
    }
}

/// The RPC view of the store's configuration state.
pub(crate) fn configuration_status(
    state: &distill_store::state::ConfigurationState,
) -> ConfigurationStatus {
    match state {
        distill_store::state::ConfigurationState::Ready(_) => ConfigurationStatus::Ready,
        distill_store::state::ConfigurationState::Failed { reason, .. } => {
            ConfigurationStatus::Failed(reason.clone())
        }
    }
}

/// Apply `commit` as the version `txn` publishes.
pub fn apply_commit(
    txn: &mut InputTxn<'_>,
    commit: &Commit,
    mode: ApplyMode,
) -> Result<(), ApplyError> {
    apply(txn, commit, |txn| match mode {
        ApplyMode::Full => write_namespace(txn, commit),
        ApplyMode::Delta => Ok(()),
    })
}

/// Apply `commit`'s served projection ([`ApplyMode::Delta`]) on top of the
/// version the store already published, in a later transaction.
pub fn apply_commit_served<W: ServedWrite>(txn: &mut W, commit: &Commit) -> Result<(), ApplyError> {
    apply(txn, commit, |_| Ok(()))
}

fn apply<W: ServedWrite>(
    txn: &mut W,
    commit: &Commit,
    namespace: impl FnOnce(&mut W) -> Result<(), ApplyError>,
) -> Result<(), ApplyError> {
    validate_commit(commit)?;
    for error in commit.namespace_errors.iter().flatten() {
        error
            .validate()
            .map_err(|error| AdminError::InvalidNamespaceError { error })?;
    }
    let version = txn.change_version();
    let (_, current_pipeline) = read_served_pipeline(txn.txn_served_blob(SERVED_PIPELINE)?)?;
    let pipeline_epoch_changed = commit.pipeline_epoch_changed
        || commit
            .pipeline
            .as_ref()
            .is_some_and(|next| next != &current_pipeline);

    // The fence row comes first: a front end fences its connections on it
    // before it reaches this version's deltas.
    if pipeline_epoch_changed {
        txn.bump_rpc_pipeline_generation()?;
        txn.append_change(
            version,
            &Change::ReconnectAll {
                reason: reconnect_code(ReconnectReason::PipelineEpochChanged),
            },
        )?;
    }
    namespace(txn)?;

    let mut asset_deltas = Vec::with_capacity(commit.assets.len());
    for mutation in &commit.assets {
        match mutation {
            AssetMutation::Set {
                uuid,
                resolution,
                delta,
            } => {
                let row = match resolution {
                    StoredResolve::Built { content_hash } => ResolutionRow::Built(*content_hash),
                    StoredResolve::Drifted { input } => {
                        ResolutionRow::Drifted(encode_drifted_input(input))
                    }
                    StoredResolve::Failed { error } => ResolutionRow::Failed(error.clone()),
                    StoredResolve::Deleted => ResolutionRow::Deleted(version),
                };
                txn.set_asset_resolution(*uuid, Some(&row))?;
                asset_deltas.push((*uuid, *delta));
            }
            AssetMutation::Remove { uuid, delta } => {
                txn.set_asset_resolution(*uuid, None)?;
                asset_deltas.push((*uuid, *delta));
            }
        }
    }
    if let Some(pipeline) = &commit.pipeline {
        txn.set_served_blob(
            SERVED_PIPELINE,
            Some(&encode_served_pipeline(version, pipeline)),
        )?;
    }

    asset_deltas.sort_by_key(|(uuid, _)| *uuid);
    for (asset, state) in asset_deltas {
        txn.append_change(
            version,
            &Change::Asset {
                asset,
                state: delta_state_code(state),
            },
        )?;
    }
    let paths = commit
        .paths
        .iter()
        .map(|mutation| match mutation {
            PathMutation::Set { path, .. } | PathMutation::Remove { path } => path.clone(),
        })
        .collect::<BTreeSet<_>>();
    for path in paths {
        txn.append_change(version, &Change::Path { path })?;
    }
    txn.trim_change_log(version, RETAINED_HISTORY_VERSIONS)?;
    Ok(())
}

/// The embedded server's namespace: every authoring entry is its own
/// bundle row under one synthetic root.
fn write_namespace(txn: &mut InputTxn<'_>, commit: &Commit) -> Result<(), ApplyError> {
    let root = txn.intern_root(EMBEDDED_ROOT)?;
    for mutation in &commit.authoring {
        match mutation {
            AuthoringMutation::Set(entry) => {
                // `upsert_asset` resets the tag index; RPC tag poison is
                // separate state and survives a value change.
                let poison = txn.txn_tag_poison(entry.uuid)?;
                txn.upsert_bundle(&BundleMeta {
                    bundle: entry.bundle,
                    root,
                    path: entry.normalized_path.clone(),
                    format_version: 1,
                    content_hash: ContentHash([0; 32]),
                    origin: None,
                })?;
                let schema = std::str::from_utf8(&entry.logical_schema).map_err(|_| {
                    AdminError::InvalidAuthoringValue {
                        uuid: entry.uuid,
                        error: AuthoringValueError::LogicalSchemaUtf8,
                    }
                })?;
                txn.put_schema(entry.schema_hash, schema)?;
                let blobs = entry
                    .value
                    .blobs
                    .iter()
                    .map(|blob| &blob[..])
                    .collect::<Vec<_>>();
                txn.upsert_asset(&AssetRecord {
                    asset: entry.uuid,
                    bundle: entry.bundle,
                    local_id: entry.local_id.clone(),
                    type_uuid: entry.type_uuid,
                    logical_hash: entry.schema_hash,
                    authoring_only: entry.role == AuthoringEntryRole::AuthoringOnly,
                    tags: entry.tags.clone(),
                    served: Some(ServedAuthoring {
                        authored_value: encode_authored_value(&entry.value.canonical_value, &blobs),
                        terminal_type: entry.terminal_type,
                    }),
                })?;
                if let Some(poison) = poison {
                    txn.set_served_tag_poison(entry.uuid, Some(&poison))?;
                }
            }
            AuthoringMutation::Remove { uuid } => txn.remove_served_asset(*uuid)?,
        }
    }
    if let Some(projection) = &commit.tag_projection {
        let empty = BTreeMap::new();
        for asset in txn.txn_served_assets()? {
            txn.set_served_tags(asset, projection.get(&asset).unwrap_or(&empty))?;
        }
    }
    for mutation in &commit.paths {
        match mutation {
            PathMutation::Set { path, candidates } => txn.set_served_path(path, candidates)?,
            PathMutation::Remove { path } => txn.set_served_path(path, &BTreeSet::new())?,
        }
    }
    if let Some(outputs) = &commit.derived_outputs {
        txn.clear_served_derived_outputs()?;
        for (child, entry) in outputs {
            txn.set_served_derived_output(*child, Some(&derived_row(entry)))?;
        }
    }
    for mutation in &commit.derived_output_mutations {
        match mutation {
            DerivedOutputMutation::Set { child, entry } => {
                txn.set_served_derived_output(*child, Some(&derived_row(entry)))?
            }
            DerivedOutputMutation::Remove { child } => {
                txn.set_served_derived_output(*child, None)?
            }
        }
    }
    if let Some(poisons) = &commit.tag_poisons {
        txn.clear_served_tag_poisons()?;
        for asset in poisons.keys() {
            txn.set_served_tag_poison(*asset, Some(TAG_POISON))?;
        }
    }
    for mutation in &commit.tag_poison_mutations {
        match mutation {
            TagPoisonMutation::Set { asset, .. } => {
                txn.set_served_tag_poison(*asset, Some(TAG_POISON))?
            }
            TagPoisonMutation::Remove { asset } => txn.set_served_tag_poison(*asset, None)?,
        }
    }
    for mutation in &commit.tag_projection_mutations {
        let (asset, tags) = match mutation {
            TagProjectionMutation::Set { asset, tags } => (*asset, tags.clone()),
            TagProjectionMutation::Remove { asset } => (*asset, BTreeMap::new()),
        };
        if txn.txn_is_served_asset(asset)? {
            txn.set_served_tags(asset, &tags)?;
        }
    }
    match &commit.configuration {
        Some(ConfigurationStatus::Ready) => txn.publish_configuration_ready(0)?,
        Some(ConfigurationStatus::Failed(error)) => {
            txn.publish_configuration_error(&error.detail, &error.message)?
        }
        None => {}
    }
    if let Some(errors) = &commit.namespace_errors {
        txn.set_namespace_errors(errors.iter().cloned())?;
    }
    Ok(())
}

const TAG_POISON: &str = "rpc tag poison";

fn derived_row(entry: &DerivedOutputEntry) -> DerivedOutputRow {
    DerivedOutputRow {
        parent: entry.parent,
        output_key: entry.output_key.clone(),
        terminal_type: entry.terminal_type,
    }
}

/// Publish a runtime failure of the current pipeline epoch. The diagnostic
/// keeps its installing version, so every snapshot that pinned this epoch
/// sees the failure; every connection must reconnect. Returns `false` when
/// the same failure is already published.
pub fn publish_runtime_pipeline_failure<W: ServedWrite>(
    txn: &mut W,
    failure: PipelineFailure,
) -> Result<bool, StoreError> {
    let (installed_at, current) = read_served_pipeline(txn.txn_served_blob(SERVED_PIPELINE)?)?;
    match &current {
        PipelineDiagnostic::Ready => {}
        PipelineDiagnostic::Failed(existing) if existing == &failure => return Ok(false),
        other => {
            return Err(StoreError::Rejected {
                detail: format!("current RPC pipeline is not the observed ready epoch: {other:?}"),
            })
        }
    }
    txn.set_served_blob(
        SERVED_PIPELINE,
        Some(&encode_served_pipeline(
            installed_at,
            &PipelineDiagnostic::Failed(failure),
        )),
    )?;
    txn.bump_rpc_pipeline_generation()?;
    let version = txn.change_version();
    txn.append_change(
        version,
        &Change::ReconnectAll {
            reason: reconnect_code(ReconnectReason::PipelineEpochChanged),
        },
    )?;
    Ok(true)
}

/// Stage the restart-required key set. Returns whether it changed.
pub fn publish_restart_required<W: ServedWrite>(
    txn: &mut W,
    keys: &[String],
) -> Result<bool, StoreError> {
    let mut keys = keys.to_vec();
    keys.sort();
    keys.dedup();
    let current = txn
        .txn_served_blob(SERVED_RESTART_KEYS)?
        .map(|bytes| {
            distill_store::served::decode_keys(&bytes)
                .ok_or_else(|| corrupt("restart-required keys are not UTF-8"))
        })
        .transpose()?
        .unwrap_or_default();
    if current == keys {
        return Ok(false);
    }
    txn.set_served_blob(
        SERVED_RESTART_KEYS,
        (!keys.is_empty())
            .then(|| distill_store::served::encode_keys(&keys))
            .as_deref(),
    )?;
    if !keys.is_empty() {
        let version = txn.change_version();
        txn.append_change(version, &Change::RestartRequired { keys })?;
    }
    Ok(true)
}

/// Replace the complete RPC target set. A changed or removed target fences
/// its connections. Returns whether anything changed.
pub fn publish_target_set<W: ServedWrite>(
    txn: &mut W,
    targets: &BTreeMap<String, TargetDefinitionHash>,
) -> Result<bool, StoreError> {
    let version = txn.change_version();
    let mut changed = false;
    let mut reconnect = Vec::new();
    for row in txn.txn_rpc_targets()? {
        if !targets.contains_key(&row.name) {
            txn.remove_rpc_target(&row.name)?;
            reconnect.push(row.name);
            changed = true;
        }
    }
    let existing = txn
        .txn_rpc_targets()?
        .into_iter()
        .map(|row| row.name)
        .collect::<BTreeSet<_>>();
    for (name, hash) in targets {
        changed |= !existing.contains(name);
        if txn.set_rpc_target(name, hash.0)? {
            reconnect.push(name.clone());
            changed = true;
        }
    }
    reconnect.sort();
    for target in reconnect {
        txn.append_change(
            version,
            &Change::ReconnectTarget {
                target,
                reason: reconnect_code(ReconnectReason::TargetDefinitionChanged),
            },
        )?;
    }
    Ok(changed)
}

/// Replace one existing target's definition, fencing its connections.
/// Returns `None` for an unknown target, else whether it changed.
pub fn publish_target<W: ServedWrite>(
    txn: &mut W,
    name: &str,
    hash: TargetDefinitionHash,
) -> Result<Option<bool>, StoreError> {
    if !txn.txn_rpc_targets()?.iter().any(|row| row.name == name) {
        return Ok(None);
    }
    if !txn.set_rpc_target(name, hash.0)? {
        return Ok(Some(false));
    }
    let version = txn.change_version();
    txn.append_change(
        version,
        &Change::ReconnectTarget {
            target: name.to_owned(),
            reason: reconnect_code(ReconnectReason::TargetDefinitionChanged),
        },
    )?;
    Ok(Some(true))
}

/// Advance the protocol epoch, fencing every connection. Returns whether it
/// changed.
pub fn publish_protocol_epoch<W: ServedWrite>(
    txn: &mut W,
    epoch: u32,
) -> Result<bool, StoreError> {
    if txn.txn_rpc_fences()?.protocol_epoch.unwrap_or(PROTOCOL_VERSION) == epoch {
        return Ok(false);
    }
    txn.set_rpc_protocol_epoch(epoch)?;
    let version = txn.change_version();
    txn.append_change(
        version,
        &Change::ReconnectAll {
            reason: reconnect_code(ReconnectReason::ProtocolEpochChanged),
        },
    )?;
    Ok(true)
}
