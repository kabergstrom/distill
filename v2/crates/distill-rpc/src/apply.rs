//! Publishing RPC state into the store (LOCKLESS.md §2.2). A typed
//! [`Commit`] is applied to the served tables inside the publishing input
//! transaction, so a version is never visible without its RPC projection.
//! Fence changes that publish no input version (runtime pipeline failure,
//! restart keys, target replacement) are [`ServedWrite`] helpers too.

use std::collections::{BTreeMap, BTreeSet};

use distill_store::served::{Change, ServedWrite};
use distill_store::StoreError;

use crate::persist::delta_state_code;
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
/// commit describes; this writes the fences and change log that go with
/// it.
pub fn apply_commit<W: ServedWrite>(txn: &mut W, commit: &Commit) -> Result<(), ApplyError> {
    validate_commit(commit)?;
    for error in commit.namespace_errors.iter().flatten() {
        error
            .validate()
            .map_err(|error| AdminError::InvalidNamespaceError { error })?;
    }
    let version = txn.change_version();
    // The fence row comes first: a front end fences its connections on it
    // before it reaches this version's deltas.
    if commit.pipeline_epoch_changed {
        publish_pipeline_fence(txn)?;
    }
    // The first publication on an empty store logs no deltas: no client can
    // hold the version before it (the daemon binds its listener only after
    // its startup publication), so every client reads current state.
    if version.0 <= 1 {
        return Ok(());
    }
    let mut asset_deltas = commit
        .assets
        .iter()
        .map(|mutation| (mutation.uuid, mutation.delta))
        .collect::<Vec<_>>();
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
        .chain(commit.new_entry_paths.iter().cloned())
        .collect::<BTreeSet<_>>();
    for path in paths {
        txn.append_change(version, &Change::Path { path })?;
    }
    txn.trim_change_log(version, RETAINED_HISTORY_VERSIONS)?;
    Ok(())
}

/// Fence every connection on a changed pipeline: an epoch installed,
/// retired or failed, at the version `txn` writes.
pub fn publish_pipeline_fence<W: ServedWrite>(txn: &mut W) -> Result<(), StoreError> {
    txn.bump_rpc_pipeline_generation()?;
    let version = txn.change_version();
    txn.append_change(version, &Change::ReconnectAll)
}

/// Announce a changed restart-required key set: the keys the store's
/// pending restart (its only source) holds now, after `before`. Returns
/// whether it changed.
pub(crate) fn publish_restart_required<W: ServedWrite>(
    txn: &mut W,
    before: &[String],
    keys: &[String],
) -> Result<bool, StoreError> {
    if before == keys {
        return Ok(false);
    }
    if !keys.is_empty() {
        let version = txn.change_version();
        txn.append_change(
            version,
            &Change::RestartRequired {
                keys: keys.to_vec(),
            },
        )?;
    }
    Ok(true)
}

/// Replace the complete RPC target set. It fences nothing itself: a target
/// changes only in a configuration publication, which fences every
/// connection with the pipeline fence. The one that does not, a pipeline
/// failing as the base's did, has no connection to fence, since a target
/// connects only while its pipeline is ready.
pub fn publish_target_set<W: ServedWrite>(
    txn: &mut W,
    targets: &BTreeMap<String, TargetDefinitionHash>,
) -> Result<(), StoreError> {
    for row in txn.txn_rpc_targets()? {
        if !targets.contains_key(&row.name) {
            txn.remove_rpc_target(&row.name)?;
        }
    }
    for (name, hash) in targets {
        txn.set_rpc_target(name, hash.0)?;
    }
    Ok(())
}
