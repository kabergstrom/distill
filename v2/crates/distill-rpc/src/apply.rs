//! Publishing RPC state into the store (LOCKLESS.md §2.2). A typed
//! [`Commit`] is applied to the served tables inside the publishing input
//! transaction, so a version is never visible without its RPC projection.
//! Fence changes that publish no input version (runtime pipeline failure,
//! restart keys, target replacement) are [`ServedWrite`] helpers too.

use std::collections::{BTreeMap, BTreeSet};

use distill_store::served::{
    Change, ResolutionRow, ServedWrite,
    SERVED_PIPELINE, SERVED_RESTART_KEYS,
};
use distill_store::StoreError;

use crate::persist::{
    decode_served_pipeline, delta_state_code, encode_drifted_input, encode_served_pipeline,
    reconnect_code,
};
use crate::validate::validate_commit;
use crate::*;

/// Input versions of subscription history kept in the change log.
pub const RETAINED_HISTORY_VERSIONS: u64 = 4096;

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

/// Apply `commit`'s served projection: the version the daemon publishes in
/// `txn` (or published just before it) already holds the namespace the
/// commit describes; this writes the resolutions, diagnostics, fences and
/// change log that go with it.
pub fn apply_commit<W: ServedWrite>(txn: &mut W, commit: &Commit) -> Result<(), ApplyError> {
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
    // A runtime entry the namespace has not announced yet can be the target
    // of a named reference (path and local id) still waiting for it: its
    // bundle's path changes too. Entries keep their identity per local id,
    // so a changed bundle with the same names notifies only its assets.
    let mut named_paths = BTreeSet::new();
    for mutation in &commit.authoring {
        if let AuthoringMutation::Set(entry) = mutation {
            if entry.role == AuthoringEntryRole::Runtime
                && !txn.txn_has_live_resolution(entry.uuid)?
            {
                named_paths.insert(entry.normalized_path.clone());
            }
        }
    }

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
        .chain(named_paths)
        .collect::<BTreeSet<_>>();
    for path in paths {
        txn.append_change(version, &Change::Path { path })?;
    }
    txn.trim_change_log(version, RETAINED_HISTORY_VERSIONS)?;
    Ok(())
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
