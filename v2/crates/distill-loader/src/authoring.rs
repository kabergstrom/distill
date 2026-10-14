//! Authoring calls (§17) on an [`crate::RpcIo`]'s connection, for editors
//! running in the engine: read authored entries and type descriptions,
//! write entries, rename bundles. Each call runs as a task while the IO
//! steps and answers under a ticket ([`crate::RpcIo::authoring`]).
//!
//! A write or rename answers once the store reflects it: its receipt names
//! the files it changed, and the answer carries the first version whose
//! `AuthoringSnapshot.file` matches every one (§17 "Write receipts").

use std::time::{Duration, Instant};

use distill_core::id::{AssetUuid, BundleUuid};
use distill_rpc::capnp_loader::{RemoteAuthoringSnapshot, RemoteCall, RemoteHub};
use distill_rpc::{
    AssetQuery, Authorable, AuthoringInspectResult, AuthoringInspection, AuthoringOp, AuthoringProgressState, AuthoringRow,
    BundleReferences, ImportRequest, SourceFile,
    DescribableType, DescribableTypesResult, LongRunningOp, RenameWithFixupsRequest, RpcFailure,
    TypeDescription, WriteReceipt,
};
use distill_store::state::InputVersion;

use crate::capnp_io::{describe, remote, TypeName};

/// How long a write's files may take to reach a published version.
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);
const CONFIRM_POLL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone)]
pub enum AuthoringRequest {
    /// The entries `query` selects, each inspected, at one snapshot.
    Entries(AssetQuery),
    /// These entries, inspected at one snapshot.
    Inspect(Vec<AssetUuid>),
    DescribeType(TypeName),
    DescribableTypes,
    /// `Hub.write`, answered once a version reflects the receipt.
    Write {
        base: InputVersion,
        ops: Vec<AuthoringOp>,
        force_lossy: bool,
    },
    /// `renameWithFixups`, run to completion, answered once a version
    /// reflects its receipt.
    Rename {
        base: InputVersion,
        request: RenameWithFixupsRequest,
    },
    /// The entries `query` selects as rows (`AuthoringSnapshot.list`).
    List(AssetQuery),
    /// The files under `root` with the layer claiming each.
    SourceFiles { root: String, glob: Option<String> },
    References(BundleUuid),
    /// `Hub.import`, answered with the bundle and the version publishing
    /// it.
    Import { base: InputVersion, request: ImportRequest },
    Reimport { base: InputVersion, bundle: BundleUuid },
    /// `Hub.release`: the explicit import follows its rule again.
    Release { base: InputVersion, bundle: BundleUuid },
    /// The authored types the pipeline declares and its importers.
    Authorable,
}

#[derive(Debug, Clone)]
pub enum AuthoringAnswer {
    /// Inspected at `version`; `missing` names asked-for entries with no
    /// authored entry there (deleted, or never authored).
    Entries {
        version: InputVersion,
        entries: Vec<AuthoringInspection>,
        missing: Vec<AssetUuid>,
    },
    Description(Box<TypeDescription>),
    Types {
        version: InputVersion,
        types: Vec<DescribableType>,
    },
    /// The files are written and `version` is the first that reflects them.
    Written {
        receipt: WriteReceipt,
        version: InputVersion,
    },
    /// `base` was not the current version (nothing was written).
    Stale { current: InputVersion },
    Rows {
        version: InputVersion,
        rows: Vec<AuthoringRow>,
    },
    SourceFiles {
        version: InputVersion,
        files: Vec<SourceFile>,
    },
    References {
        version: InputVersion,
        references: BundleReferences,
    },
    /// An import, reimport or release published `bundle`; `version` is
    /// current after it.
    Imported {
        bundle: BundleUuid,
        version: InputVersion,
    },
    Authorable {
        version: InputVersion,
        authorable: Authorable,
    },
}

/// A watched entry changed at `version` ([`crate::RpcIo::watch_authoring`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthoringChange {
    pub uuid: AssetUuid,
    pub version: InputVersion,
    pub deleted: bool,
}

pub(crate) async fn run(hub: &RemoteHub, request: AuthoringRequest) -> Result<AuthoringAnswer, String> {
    match request {
        AuthoringRequest::Entries(query) => {
            let snapshot = remote(hub.authoring_snapshot().await)?;
            let uuids = remote(snapshot.query(&query).await)?;
            inspect(&snapshot, uuids).await
        }
        AuthoringRequest::Inspect(uuids) => {
            let snapshot = remote(hub.authoring_snapshot().await)?;
            inspect(&snapshot, uuids).await
        }
        AuthoringRequest::DescribeType(name) => {
            describe(hub, name).await.map(|description| AuthoringAnswer::Description(Box::new(description)))
        }
        AuthoringRequest::DescribableTypes => {
            let snapshot = remote(hub.authoring_snapshot().await)?;
            match remote(snapshot.describable_types().await)? {
                DescribableTypesResult::Types { stamp, types, .. } => Ok(AuthoringAnswer::Types {
                    version: stamp.version,
                    types,
                }),
                DescribableTypesResult::Drifted { input, .. } => {
                    Err(format!("{input:?} changed while it was read; ask again"))
                }
            }
        }
        AuthoringRequest::Write { base, ops, force_lossy } => {
            let receipt = match hub.write(base, &ops, force_lossy).await {
                Ok(RemoteCall::Error(error)) => match error.failure {
                    Some(RpcFailure::StaleInputVersion { expected, .. }) => {
                        return Ok(AuthoringAnswer::Stale { current: expected })
                    }
                    _ => return Err(error.message),
                },
                call => remote(call)?,
            };
            confirm(hub, receipt).await
        }
        AuthoringRequest::List(query) => {
            let snapshot = remote(hub.authoring_snapshot().await)?;
            let version = remote(snapshot.version().await)?;
            let rows = remote(snapshot.list(&query).await)?;
            Ok(AuthoringAnswer::Rows { version, rows })
        }
        AuthoringRequest::SourceFiles { root, glob } => {
            let snapshot = remote(hub.authoring_snapshot().await)?;
            let version = remote(snapshot.version().await)?;
            let files = remote(snapshot.source_files(&root, glob.as_deref()).await)?;
            Ok(AuthoringAnswer::SourceFiles { version, files })
        }
        AuthoringRequest::References(bundle) => {
            let snapshot = remote(hub.authoring_snapshot().await)?;
            let version = remote(snapshot.version().await)?;
            let references = remote(snapshot.references(bundle).await)?;
            Ok(AuthoringAnswer::References { version, references })
        }
        AuthoringRequest::Authorable => {
            let snapshot = remote(hub.authoring_snapshot().await)?;
            let version = remote(snapshot.version().await)?;
            let authorable = remote(snapshot.authorable().await)?;
            Ok(AuthoringAnswer::Authorable { version, authorable })
        }
        AuthoringRequest::Import { base, request } => imported(hub, hub.import(base, &request).await).await,
        AuthoringRequest::Reimport { base, bundle } => imported(hub, hub.reimport(base, bundle).await).await,
        AuthoringRequest::Release { base, bundle } => imported(hub, hub.release(base, bundle).await).await,
        AuthoringRequest::Rename { base, request } => {
            let operation = LongRunningOp::RenameWithFixups(request.encode());
            let mut progress = match hub.operation(base, &operation).await {
                Ok(RemoteCall::Error(error)) => match error.failure {
                    Some(RpcFailure::StaleInputVersion { expected, .. }) => {
                        return Ok(AuthoringAnswer::Stale { current: expected })
                    }
                    _ => return Err(error.message),
                },
                call => remote(call)?,
            };
            loop {
                let Some(event) = progress.next().await.map_err(|error| error.to_string())? else {
                    return Err("the rename ended without completing".to_owned());
                };
                match event.state {
                    AuthoringProgressState::Started | AuthoringProgressState::Running => {}
                    AuthoringProgressState::Completed => {
                        let receipt = WriteReceipt::decode(&event.payload).map_err(|error| error.to_string())?;
                        return confirm(hub, receipt).await;
                    }
                    AuthoringProgressState::Cancelled => return Err("the rename was cancelled".to_owned()),
                    AuthoringProgressState::Failed => {
                        return Err(format!("the rename failed: {}", String::from_utf8_lossy(&event.payload)))
                    }
                }
            }
        }
    }
}

/// An import call's answer: the bundle and the version current after it
/// (the call publishes before it answers), or [`AuthoringAnswer::Stale`].
async fn imported(
    hub: &RemoteHub,
    call: Result<RemoteCall<BundleUuid>, impl std::fmt::Display>,
) -> Result<AuthoringAnswer, String> {
    let bundle = match call {
        Ok(RemoteCall::Error(error)) => match error.failure {
            Some(RpcFailure::StaleInputVersion { expected, .. }) => {
                return Ok(AuthoringAnswer::Stale { current: expected })
            }
            _ => return Err(error.message),
        },
        call => remote(call)?,
    };
    let snapshot = remote(hub.authoring_snapshot().await)?;
    let version = remote(snapshot.version().await)?;
    Ok(AuthoringAnswer::Imported { bundle, version })
}

async fn inspect(snapshot: &RemoteAuthoringSnapshot, uuids: Vec<AssetUuid>) -> Result<AuthoringAnswer, String> {
    let version = remote(snapshot.version().await)?;
    let mut entries = Vec::with_capacity(uuids.len());
    let mut missing = Vec::new();
    for uuid in uuids {
        match remote(snapshot.inspect(uuid).await)? {
            AuthoringInspectResult::Inspection(inspection) => entries.push(inspection),
            AuthoringInspectResult::Missing | AuthoringInspectResult::RoleIneligible { .. } => missing.push(uuid),
            AuthoringInspectResult::Drifted { input, .. } => {
                return Err(format!("{input:?} changed while it was read; ask again"))
            }
        }
    }
    Ok(AuthoringAnswer::Entries { version, entries, missing })
}

/// Poll fresh snapshots until one reflects every file of `receipt`.
async fn confirm(hub: &RemoteHub, receipt: WriteReceipt) -> Result<AuthoringAnswer, String> {
    let started = Instant::now();
    loop {
        let snapshot = remote(hub.authoring_snapshot().await)?;
        let version = remote(snapshot.version().await)?;
        let mut reflected = true;
        for file in &receipt.files {
            if remote(snapshot.file(&file.root, &file.path).await)? != file.content_hash {
                reflected = false;
                break;
            }
        }
        if reflected {
            return Ok(AuthoringAnswer::Written { receipt, version });
        }
        if started.elapsed() >= CONFIRM_TIMEOUT {
            return Err(format!(
                "the write's files were not reflected within {} s: {:?}",
                CONFIRM_TIMEOUT.as_secs(),
                receipt.files
            ));
        }
        tokio::time::sleep(CONFIRM_POLL).await;
    }
}
