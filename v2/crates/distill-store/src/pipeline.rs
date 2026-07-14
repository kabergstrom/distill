//! Pipeline-side metadata (§13): the `pipeline_state` row and
//! `registrations`, the `tools` ToolEpoch table, and the
//! source-controlled schema-lineage manifest projection: append-only
//! accepted epochs, explicit parent links, an independent current cursor,
//! and `"DSSL"` commitments gating automatic migration diffs (§6, §11).

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::PathBuf;

use distill_core::attestation::{
    bootstrap_control_logical_registry_v1, is_bootstrap_control_type, BundleFormatVersion,
    CompiledAttestationDigest, CompiledTypeTable,
};
use distill_core::id::{ContentHash, LogicalHash, TypeUuid};
pub use distill_core::lineage::{lineage_chain_digest, AcceptedSchemaEpoch, LineageStamp};
use distill_core::target_set::{CanonicalTargetSet, TargetSetHash, TargetSetRow};
use distill_core::tool::{
    ToolCapsuleFile, ToolCapsuleFileRole, ToolCwdPolicy, ToolExecutionCapsuleV1,
    ToolLaunchMetadataV1, ToolPlatformBinding,
};
use distill_schema::bootstrap_gen_v1::ConsumerBootstrapAuthorityV1;
use rusqlite::OptionalExtension;
use unicode_normalization::is_nfc;

use crate::db::{InputTxn, Store};
use crate::error::{RetiredTypeReference, StoreError};
use crate::state::{
    InputVersion, PipelineCandidateIdentity, PipelineEpoch, PipelinePoison, PipelineState,
    Registration, RegistrationKind, SchemaAcceptanceRequired, SchemaManifestBasis,
    SchemaRegistryMismatch,
};

/// One already-resolved member supplied at the registration boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedToolCapsuleFile {
    pub path: String,
    pub role: ToolCapsuleFileRole,
    pub executable: bool,
    pub bytes: Vec<u8>,
}

/// Complete registration input after interpreter/DSO/plugin/resource
/// resolution. The store derives every byte hash and refuses partial or
/// noncanonical closure metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCapsuleRegistrationV1 {
    pub files: Vec<ResolvedToolCapsuleFile>,
    pub resolved_interpreter: Option<String>,
    pub launch: ToolLaunchMetadataV1,
    pub environment: Vec<(String, String)>,
    pub cwd_policy: ToolCwdPolicy,
    pub platform: ToolPlatformBinding,
}

/// One published ToolEpoch mapping. Jobs launch only inside `root` using the
/// verified capsule object and trace the aggregate DSCT `capsule_hash`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedTool {
    pub key: String,
    pub root: PathBuf,
    pub capsule: ToolExecutionCapsuleV1,
    pub capsule_hash: [u8; 32],
    pub input_version: InputVersion,
}

impl StagedTool {
    /// Reopen the full staged closure immediately before process creation.
    /// Failure is transient launch state and must not enter a memoized trace.
    pub fn revalidate(&self) -> Result<(), StoreError> {
        verify_capsule_root(&self.key, &self.root, &self.capsule)
    }

    /// Recheck the configured launch platform against the sealed binding.
    /// Explicit residual bindings still pin the platform while deliberately
    /// naming the accepted ambient runtime class in the DSCT object.
    pub fn validate_launch_platform(
        &self,
        platform_id: &str,
        system_runtime_id: &str,
    ) -> Result<(), StoreError> {
        let matches = match &self.capsule.platform {
            ToolPlatformBinding::Pinned {
                platform_id: expected_platform,
                system_runtime_id: expected_runtime,
            } => expected_platform == platform_id && expected_runtime == system_runtime_id,
            ToolPlatformBinding::ExplicitResidual {
                platform_id: expected_platform,
                ..
            } => expected_platform == platform_id,
        };
        if !matches {
            return Err(StoreError::ToolCapsuleUnavailable {
                key: self.key.clone(),
                path: self.root.clone(),
                detail: "configured platform/runtime differs from the sealed binding",
            });
        }
        Ok(())
    }
}

impl ToolCapsuleRegistrationV1 {
    fn seal(self) -> Result<(ToolExecutionCapsuleV1, Vec<ResolvedToolCapsuleFile>), StoreError> {
        let files = self
            .files
            .iter()
            .map(|file| ToolCapsuleFile {
                path: file.path.clone(),
                role: file.role,
                executable: file.executable,
                len: file.bytes.len() as u64,
                bytes_hash: *blake3::hash(&file.bytes).as_bytes(),
            })
            .collect();
        let capsule = ToolExecutionCapsuleV1 {
            files,
            resolved_interpreter: self.resolved_interpreter,
            launch: self.launch,
            environment: self.environment,
            cwd_policy: self.cwd_policy,
            platform: self.platform,
        };
        capsule.validate().map_err(StoreError::InvalidToolCapsule)?;
        Ok((capsule, self.files))
    }
}

fn create_dir_all(path: &std::path::Path) -> Result<(), StoreError> {
    std::fs::create_dir_all(path).map_err(|source| StoreError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn hex_hash(hash: &[u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn stage_immutable_file(
    key: &str,
    path: &std::path::Path,
    bytes: &[u8],
    metadata: &ToolCapsuleFile,
) -> Result<(), StoreError> {
    if !path.exists() {
        let parent = path
            .parent()
            .ok_or_else(|| StoreError::ToolCapsuleUnavailable {
                key: key.to_owned(),
                path: path.to_path_buf(),
                detail: "staged object has no parent directory",
            })?;
        let mut nonce = [0u8; 8];
        getrandom::getrandom(&mut nonce).map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source: std::io::Error::other(source.to_string()),
        })?;
        let tmp = parent.join(format!(".stage-{}", u64::from_le_bytes(nonce)));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .map_err(|source| StoreError::Io {
                path: tmp.clone(),
                source,
            })?;
        file.write_all(bytes).map_err(|source| StoreError::Io {
            path: tmp.clone(),
            source,
        })?;
        set_staged_permissions(&file, metadata.executable, &tmp)?;
        file.sync_all().map_err(|source| StoreError::Io {
            path: tmp.clone(),
            source,
        })?;
        match std::fs::hard_link(&tmp, path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(source) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(StoreError::Io {
                    path: path.to_path_buf(),
                    source,
                });
            }
        }
        std::fs::remove_file(&tmp).map_err(|source| StoreError::Io { path: tmp, source })?;
        crate::cas::manifest::fsync_dir(parent)?;
    }
    verify_capsule_file(key, path, metadata)
}

#[cfg(unix)]
fn set_staged_permissions(
    file: &std::fs::File,
    executable: bool,
    path: &std::path::Path,
) -> Result<(), StoreError> {
    use std::os::unix::fs::PermissionsExt;
    let mode = if executable { 0o555 } else { 0o444 };
    file.set_permissions(std::fs::Permissions::from_mode(mode))
        .map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(not(unix))]
fn set_staged_permissions(
    file: &std::fs::File,
    _executable: bool,
    path: &std::path::Path,
) -> Result<(), StoreError> {
    let mut permissions = file
        .metadata()
        .map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    permissions.set_readonly(true);
    file.set_permissions(permissions)
        .map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })
}

fn verify_capsule_root(
    key: &str,
    root: &std::path::Path,
    capsule: &ToolExecutionCapsuleV1,
) -> Result<(), StoreError> {
    let root_metadata =
        std::fs::symlink_metadata(root).map_err(|_| StoreError::ToolCapsuleUnavailable {
            key: key.to_owned(),
            path: root.to_path_buf(),
            detail: "staged capsule root is unavailable",
        })?;
    if !root_metadata.is_dir() || root_metadata.file_type().is_symlink() {
        return Err(StoreError::ToolCapsuleUnavailable {
            key: key.to_owned(),
            path: root.to_path_buf(),
            detail: "staged capsule root is not a real directory",
        });
    }
    let expected = capsule
        .files
        .iter()
        .map(|file| file.path.clone())
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    collect_capsule_files(key, root, root, &mut actual)?;
    if actual != expected {
        return Err(StoreError::ToolCapsuleUnavailable {
            key: key.to_owned(),
            path: root.to_path_buf(),
            detail: "staged capsule file set differs from the sealed object",
        });
    }
    for file in &capsule.files {
        verify_capsule_file(key, &root.join(&file.path), file)?;
    }
    Ok(())
}

fn collect_capsule_files(
    key: &str,
    root: &std::path::Path,
    directory: &std::path::Path,
    files: &mut BTreeSet<String>,
) -> Result<(), StoreError> {
    let entries = std::fs::read_dir(directory).map_err(|_| StoreError::ToolCapsuleUnavailable {
        key: key.to_owned(),
        path: directory.to_path_buf(),
        detail: "staged capsule directory is unreadable",
    })?;
    for entry in entries {
        let entry = entry.map_err(|_| StoreError::ToolCapsuleUnavailable {
            key: key.to_owned(),
            path: directory.to_path_buf(),
            detail: "staged capsule directory entry is unreadable",
        })?;
        let path = entry.path();
        let metadata =
            std::fs::symlink_metadata(&path).map_err(|_| StoreError::ToolCapsuleUnavailable {
                key: key.to_owned(),
                path: path.clone(),
                detail: "staged capsule member metadata is unavailable",
            })?;
        if metadata.file_type().is_symlink() {
            return Err(StoreError::ToolCapsuleUnavailable {
                key: key.to_owned(),
                path,
                detail: "staged capsule contains a symlink",
            });
        }
        if metadata.is_dir() {
            collect_capsule_files(key, root, &path, files)?;
        } else if metadata.is_file() {
            let relative =
                path.strip_prefix(root)
                    .map_err(|_| StoreError::ToolCapsuleUnavailable {
                        key: key.to_owned(),
                        path: path.clone(),
                        detail: "staged capsule member escaped its root",
                    })?;
            let relative = relative
                .components()
                .map(|component| component.as_os_str().to_str())
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| StoreError::ToolCapsuleUnavailable {
                    key: key.to_owned(),
                    path: path.clone(),
                    detail: "staged capsule member path is not UTF-8",
                })?
                .join("/");
            files.insert(relative);
        } else {
            return Err(StoreError::ToolCapsuleUnavailable {
                key: key.to_owned(),
                path,
                detail: "staged capsule contains a non-file member",
            });
        }
    }
    Ok(())
}

fn verify_capsule_file(
    key: &str,
    path: &std::path::Path,
    expected: &ToolCapsuleFile,
) -> Result<(), StoreError> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|_| StoreError::ToolCapsuleUnavailable {
            key: key.to_owned(),
            path: path.to_path_buf(),
            detail: "staged capsule member is unavailable",
        })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(StoreError::ToolCapsuleUnavailable {
            key: key.to_owned(),
            path: path.to_path_buf(),
            detail: "staged capsule member is not a regular file",
        });
    }
    if metadata.len() != expected.len {
        return Err(StoreError::ToolCapsuleUnavailable {
            key: key.to_owned(),
            path: path.to_path_buf(),
            detail: "staged capsule member length changed",
        });
    }
    let bytes = std::fs::read(path).map_err(|_| StoreError::ToolCapsuleUnavailable {
        key: key.to_owned(),
        path: path.to_path_buf(),
        detail: "staged capsule member is unreadable",
    })?;
    if blake3::hash(&bytes).as_bytes() != &expected.bytes_hash {
        return Err(StoreError::ToolCapsuleUnavailable {
            key: key.to_owned(),
            path: path.to_path_buf(),
            detail: "staged capsule member hash changed",
        });
    }
    verify_executable_mode(key, path, &metadata, expected.executable)
}

#[cfg(unix)]
fn verify_executable_mode(
    key: &str,
    path: &std::path::Path,
    metadata: &std::fs::Metadata,
    expected: bool,
) -> Result<(), StoreError> {
    use std::os::unix::fs::PermissionsExt;
    if (metadata.permissions().mode() & 0o111 != 0) != expected {
        return Err(StoreError::ToolCapsuleUnavailable {
            key: key.to_owned(),
            path: path.to_path_buf(),
            detail: "staged capsule executable mode changed",
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_executable_mode(
    _key: &str,
    _path: &std::path::Path,
    _metadata: &std::fs::Metadata,
    _expected: bool,
) -> Result<(), StoreError> {
    Ok(())
}

/// Sealed publication input tying the store summary to the independently
/// validated full compiled table and the current consumer's unforgeable
/// bootstrap authority. There is no raw publication path.
#[derive(Debug, Clone)]
pub struct ValidatedPipelineEpoch {
    epoch: PipelineEpoch,
}

impl ValidatedPipelineEpoch {
    pub fn validate(
        epoch: PipelineEpoch,
        compiled_types: &CompiledTypeTable,
        bootstrap_authority: &ConsumerBootstrapAuthorityV1,
    ) -> Result<Self, StoreError> {
        compiled_types
            .validate()
            .map_err(StoreError::InvalidCompiledAttestation)?;
        bootstrap_authority
            .validate_boundary_rows(&compiled_types.rows, BundleFormatVersion::V1)
            .map_err(StoreError::InvalidBootstrapAuthority)?;
        validate_target_set(&epoch.target_set)?;
        if epoch.compiled_types != compiled_types.digest {
            return Err(StoreError::InvalidPipelineEpoch {
                detail: "DSCA summary does not match the full compiled table",
            });
        }
        let schema_registry = compiled_types
            .rows
            .iter()
            .map(|row| (row.type_uuid, row.logical_hash))
            .collect::<BTreeMap<_, _>>();
        if epoch.schema_registry != schema_registry {
            return Err(StoreError::InvalidPipelineEpoch {
                detail: "schema registry is not the full compiled-table projection",
            });
        }
        let policy = compiled_types
            .rows
            .iter()
            .map(|row| (row.type_uuid, row.build_only))
            .collect::<Vec<_>>();
        if epoch.load_policy_digest != crate::state::load_policy_digest(&policy) {
            return Err(StoreError::InvalidPipelineEpoch {
                detail: "load-policy digest is not derived from the full compiled table",
            });
        }
        Ok(Self { epoch })
    }

    pub fn epoch(&self) -> &PipelineEpoch {
        &self.epoch
    }
}

impl std::ops::Deref for ValidatedPipelineEpoch {
    type Target = PipelineEpoch;

    fn deref(&self) -> &Self::Target {
        &self.epoch
    }
}

/// A type's append-only accepted history and independently movable current
/// cursor. A rollback changes `current`, never `epochs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedTypeLineage {
    pub epochs: Vec<AcceptedSchemaEpoch>,
    pub current: u32,
    pub authority: TypeAuthorityState,
}

/// Whether one accepted lineage participates in exact Ready registry
/// equality. Retirement retains history and cursor authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeAuthorityState {
    Active,
    Retired { retired_from: u32 },
}

/// The already parsed, unique source-controlled lineage authority. The
/// metadata store holds only a disposable projection of this value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaLineageManifest {
    pub types: BTreeMap<TypeUuid, AcceptedTypeLineage>,
}

/// Explicit trust-boundary handoff for the unique source-controlled lineage
/// manifest. The coordinator constructs this only after parsing the complete
/// file and verifying that `manifest_hash` is its byte-identity hash; the
/// store persists that hash beside the disposable SQLite projection and
/// never synthesizes a successor manifest itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSchemaLineageManifest {
    manifest_hash: ContentHash,
    manifest: SchemaLineageManifest,
}

impl VerifiedSchemaLineageManifest {
    pub fn from_verified_source(
        manifest_hash: ContentHash,
        manifest: SchemaLineageManifest,
    ) -> Self {
        Self {
            manifest_hash,
            manifest,
        }
    }

    pub fn manifest_hash(&self) -> ContentHash {
        self.manifest_hash
    }

    pub fn manifest(&self) -> &SchemaLineageManifest {
        &self.manifest
    }
}

/// One authored custom migration edge supplied to explicit rollback
/// validation. Automatic diffs are deliberately absent from this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReverseMigrationEdge {
    pub from: LogicalHash,
    pub to: LogicalHash,
}

/// The rollback-specific proof inputs, grouped so the candidate, verified
/// manifest base, and verified proposed manifest remain visually distinct at
/// the trust boundary.
#[derive(Debug, Clone, Copy)]
pub struct SchemaRollbackRequest<'a> {
    pub type_uuid: TypeUuid,
    pub target: LogicalHash,
    pub live_schema_hashes: &'a [LogicalHash],
    pub reverse_edges: &'a [ReverseMigrationEdge],
}

/// Proof inputs for explicit reactivation when the candidate selects an
/// already accepted non-current digest. Forward ancestry is sufficient;
/// reverse/divergent selection requires these complete custom edges.
#[derive(Debug, Clone, Copy)]
pub struct SchemaReactivationRequest<'a> {
    pub type_uuid: TypeUuid,
    pub live_schema_hashes: &'a [LogicalHash],
    pub reverse_edges: &'a [ReverseMigrationEdge],
}

/// One recorded append-only `schema_lineage` row (§13). The independently
/// movable current cursor and full-vector DSSL commitment live in
/// `schema_lineage_current`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineageEntry {
    pub generation: u64,
    pub schema_hash: LogicalHash,
    pub forward_parent: Option<u32>,
}

/// Where the data's selected accepted epoch sits relative to the registry's
/// current by explicit parent reachability (§6, §11, §13).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineageClass {
    /// The data is at the registry's current — the walk terminates, no
    /// diff runs.
    AtCurrent,
    /// The data's hash sits strictly earlier on the recorded chain than
    /// the registry's current: the single automatic diff is legal.
    ForwardOnChain,
    /// The data is recorded — by chain position or by stamp — *ahead*
    /// of the registry's current: the registry is behind the data
    /// (§5's staleness window) — schema-dependent builds refuse with a
    /// staleness error naming both hashes; a deliberate rollback needs
    /// an explicit reverse custom edge.
    RegistryBehindData,
    /// No legal direction judgment exists: an explicit edge is required
    /// (the rev-rule hard stop, §11).
    HardStop(HardStopReason),
}

/// Why a placement hard-stopped (§11): missing authority, unknown,
/// divergent, or positionless — never a heuristic tiebreak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardStopReason {
    /// The unique source-controlled manifest has not been projected. Bundle
    /// stamps cannot substitute for it, including after state loss.
    MissingManifest,
    /// The hash is on no recorded chain entry and the data carries no
    /// stamp: an unknown or unstamped schema.
    Unstamped,
    /// A stamp whose explicit ordered list is not prefix-comparable with
    /// the registry list, or whose `"DSSL"` commitment is invalid — a
    /// foreign branch, never a forward ancestor.
    Divergent,
    /// The registry's own current is on no recorded chain entry: no
    /// position to judge direction from (re-establish first, §11).
    UnknownPosition,
}

impl LineageClass {
    /// Whether §11's single trailing automatic diff is legal from this
    /// placement. `AtCurrent` is excluded not as a refusal but because
    /// the walk already terminated — no diff runs at all.
    pub fn permits_automatic_diff(self) -> bool {
        matches!(self, LineageClass::ForwardOnChain)
    }
}

impl InputTxn<'_> {
    /// Publish a staged pipeline candidate (§3, §13). `Ready` is possible
    /// only when the candidate's complete registry projection exactly equals
    /// the authoritative manifest cursors. Every missing, extra, or unequal
    /// row instead publishes a stable `SchemaAcceptanceRequired` state while
    /// retaining the prior epoch only as `last_good` residency bookkeeping.
    pub fn publish_pipeline_epoch(
        &mut self,
        epoch: &ValidatedPipelineEpoch,
    ) -> Result<bool, StoreError> {
        validate_target_set(&epoch.target_set)?;
        validate_bootstrap_schema_registry(&epoch.schema_registry)?;
        let basis = manifest_basis(&self.txn)?.ok_or(StoreError::LineageManifestUnavailable)?;
        let mismatches = schema_registry_mismatches(&epoch.schema_registry, &basis.current_cursors);
        if !mismatches.is_empty() {
            self.publish_schema_acceptance_required(epoch, &basis, &mismatches)?;
            return Ok(false);
        }
        self.publish_ready_pipeline_epoch(epoch)?;
        Ok(true)
    }

    fn publish_ready_pipeline_epoch(&mut self, epoch: &PipelineEpoch) -> Result<(), StoreError> {
        self.txn.execute(
            "INSERT INTO pipeline_state(
                 id, dylib_hash, load_policy_digest, compiled_types,
                 target_set_hash, input_version,
                 poison_code, poison_origin, poison_cleanup, poison_identity, poison_message,
                 acceptance_candidate_dylib_hash,
                 acceptance_candidate_compiled_types,
                 acceptance_candidate_target_set_hash,
                 acceptance_manifest_hash
             ) VALUES (0, ?1, ?2, ?3, ?4, ?5, NULL, NULL, NULL, NULL, NULL,
                       NULL, NULL, NULL, NULL)
             ON CONFLICT(id) DO UPDATE SET
               dylib_hash = excluded.dylib_hash,
               load_policy_digest = excluded.load_policy_digest,
               compiled_types = excluded.compiled_types,
               target_set_hash = excluded.target_set_hash,
               input_version = excluded.input_version,
               poison_code = NULL,
               poison_origin = NULL,
               poison_cleanup = NULL,
               poison_identity = NULL,
               poison_message = NULL,
               acceptance_candidate_dylib_hash = NULL,
               acceptance_candidate_compiled_types = NULL,
               acceptance_candidate_target_set_hash = NULL,
               acceptance_manifest_hash = NULL",
            rusqlite::params![
                epoch.dylib_hash.as_slice(),
                epoch.load_policy_digest.as_slice(),
                epoch.compiled_types.0.as_slice(),
                epoch.target_set.digest.0.as_slice(),
                self.version().0 as i64,
            ],
        )?;
        self.txn.execute("DELETE FROM registrations", [])?;
        for reg in &epoch.registrations {
            let kind = match reg.kind {
                RegistrationKind::Importer => 0i64,
                RegistrationKind::Processor => 1i64,
            };
            self.txn.execute(
                "INSERT INTO registrations(kind, reg_id, version) VALUES (?1, ?2, ?3)",
                rusqlite::params![kind, reg.id, reg.version],
            )?;
        }
        replace_schema_registry(
            &self.txn,
            "pipeline_schema_registry",
            &epoch.schema_registry,
        )?;
        self.txn
            .execute("DELETE FROM pipeline_candidate_schema_registry", [])?;
        replace_target_set(&self.txn, false, &epoch.target_set)?;
        self.txn
            .execute("DELETE FROM pipeline_candidate_target_set", [])?;
        Ok(())
    }

    fn publish_schema_acceptance_required(
        &mut self,
        epoch: &PipelineEpoch,
        manifest: &SchemaManifestBasis,
        mismatches: &[SchemaRegistryMismatch],
    ) -> Result<(), StoreError> {
        self.txn.execute(
            "INSERT INTO pipeline_state(
                 id, dylib_hash, load_policy_digest, compiled_types,
                 target_set_hash, input_version,
                 poison_code, poison_origin, poison_cleanup, poison_identity, poison_message,
                 acceptance_candidate_dylib_hash,
                 acceptance_candidate_compiled_types,
                 acceptance_candidate_target_set_hash,
                 acceptance_manifest_hash
             ) VALUES (0, NULL, NULL, NULL, NULL, ?1, NULL, NULL, NULL, NULL, NULL,
                       ?2, ?3, ?4, ?5)
             ON CONFLICT(id) DO UPDATE SET
               input_version = excluded.input_version,
               poison_code = NULL,
               poison_origin = NULL,
               poison_cleanup = NULL,
               poison_identity = NULL,
               poison_message = NULL,
               acceptance_candidate_dylib_hash = excluded.acceptance_candidate_dylib_hash,
               acceptance_candidate_compiled_types =
                   excluded.acceptance_candidate_compiled_types,
               acceptance_candidate_target_set_hash =
                   excluded.acceptance_candidate_target_set_hash,
               acceptance_manifest_hash = excluded.acceptance_manifest_hash",
            rusqlite::params![
                self.version().0 as i64,
                epoch.dylib_hash.as_slice(),
                epoch.compiled_types.0.as_slice(),
                epoch.target_set.digest.0.as_slice(),
                manifest.manifest_hash.0.as_slice(),
            ],
        )?;
        replace_schema_registry(
            &self.txn,
            "pipeline_candidate_schema_registry",
            &epoch.schema_registry,
        )?;
        replace_target_set(&self.txn, true, &epoch.target_set)?;
        debug_assert_eq!(
            mismatches,
            schema_registry_mismatches(
                &epoch.schema_registry,
                &manifest_basis(&self.txn)?
                    .expect("manifest checked")
                    .current_cursors
            )
        );
        Ok(())
    }

    /// A rejected candidate still publishes (§13): the version carries a
    /// pipeline poison naming the error. The prior epoch's identity
    /// columns are retained as `last_good` residency bookkeeping — never
    /// served as this version's code.
    pub fn publish_pipeline_poison(&mut self, poison: &PipelinePoison) -> Result<(), StoreError> {
        poison
            .validate()
            .map_err(StoreError::InvalidPipelinePoison)?;
        self.txn.execute(
            "INSERT INTO pipeline_state(
                 id, dylib_hash, load_policy_digest, compiled_types,
                 target_set_hash, input_version,
                 poison_code, poison_origin, poison_cleanup, poison_identity, poison_message,
                 acceptance_candidate_dylib_hash,
                 acceptance_candidate_compiled_types,
                 acceptance_candidate_target_set_hash,
                 acceptance_manifest_hash
             ) VALUES (0, NULL, NULL, NULL, NULL, ?1, ?2, ?3, ?4, ?5, ?6,
                       NULL, NULL, NULL, NULL)
             ON CONFLICT(id) DO UPDATE SET
               input_version = excluded.input_version,
               poison_code = excluded.poison_code,
               poison_origin = excluded.poison_origin,
               poison_cleanup = excluded.poison_cleanup,
               poison_identity = excluded.poison_identity,
               poison_message = excluded.poison_message,
               acceptance_candidate_dylib_hash = NULL,
               acceptance_candidate_compiled_types = NULL,
               acceptance_candidate_target_set_hash = NULL,
               acceptance_manifest_hash = NULL",
            rusqlite::params![
                self.version().0 as i64,
                poison.code as u16,
                poison.origin as u16,
                poison.cleanup as u16,
                poison.identity.as_slice(),
                poison.message,
            ],
        )?;
        self.txn
            .execute("DELETE FROM pipeline_candidate_schema_registry", [])?;
        self.txn
            .execute("DELETE FROM pipeline_candidate_target_set", [])?;
        Ok(())
    }

    /// Stage and publish a complete hermetic tool capsule. Every member is
    /// reopened and verified before the ToolEpoch row becomes visible.
    pub fn stage_tool(
        &mut self,
        key: &str,
        registration: ToolCapsuleRegistrationV1,
    ) -> Result<StagedTool, StoreError> {
        if key.is_empty() || !is_nfc(key) || key.contains('\0') {
            return Err(StoreError::InvalidToolKey);
        }
        let (capsule, sources) = registration.seal()?;
        let capsule_hash = capsule.digest().map_err(StoreError::InvalidToolCapsule)?;
        let capsule_object = capsule
            .encode_record()
            .map_err(StoreError::InvalidToolCapsule)?;
        let tools_dir = self.state_path.join("tools");
        let objects_dir = tools_dir.join("objects");
        let root = tools_dir.join("capsules").join(hex_hash(&capsule_hash));
        create_dir_all(&objects_dir)?;
        create_dir_all(&root)?;
        for (metadata, source) in capsule.files.iter().zip(&sources) {
            let mode = if metadata.executable { "x" } else { "n" };
            let object = objects_dir.join(format!("{}-{mode}", hex_hash(&metadata.bytes_hash)));
            stage_immutable_file(key, &object, &source.bytes, metadata)?;
            let member = root.join(&metadata.path);
            if let Some(parent) = member.parent() {
                create_dir_all(parent)?;
            }
            match std::fs::hard_link(&object, &member) {
                Ok(()) => {
                    if let Some(parent) = member.parent() {
                        crate::cas::manifest::fsync_dir(parent)?;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(source) => {
                    return Err(StoreError::Io {
                        path: member,
                        source,
                    })
                }
            }
        }
        verify_capsule_root(key, &root, &capsule)?;
        self.txn.execute(
            "INSERT INTO tools(tool_key, present, capsule_object, capsule_hash, input_version)
             VALUES (?1, 1, ?2, ?3, ?4)
             ON CONFLICT(tool_key, input_version) DO UPDATE SET
               present = 1,
               capsule_object = excluded.capsule_object,
               capsule_hash = excluded.capsule_hash",
            rusqlite::params![
                key,
                capsule_object,
                capsule_hash.as_slice(),
                self.version().0 as i64,
            ],
        )?;
        Ok(StagedTool {
            key: key.to_owned(),
            root,
            capsule,
            capsule_hash,
            input_version: self.version(),
        })
    }

    /// Publish one complete ToolEpoch projection. Keys absent from `tools`
    /// receive input-versioned tombstones so a removed registration cannot
    /// fall through to an older live capsule at a newer snapshot.
    pub fn publish_tool_epoch(
        &mut self,
        tools: &BTreeMap<String, ToolCapsuleRegistrationV1>,
    ) -> Result<Vec<StagedTool>, StoreError> {
        let mut previous = BTreeSet::new();
        {
            let mut statement = self.txn.prepare(
                "SELECT candidate.tool_key
                   FROM tools AS candidate
                  WHERE candidate.input_version = (
                        SELECT MAX(prior.input_version)
                          FROM tools AS prior
                         WHERE prior.tool_key = candidate.tool_key
                           AND prior.input_version <= ?1)
                    AND candidate.present = 1",
            )?;
            let rows = statement.query_map(
                [i64::try_from(self.base_stamp().version.0).unwrap_or(i64::MAX)],
                |row| row.get::<_, String>(0),
            )?;
            for row in rows {
                previous.insert(row?);
            }
        }

        let mut staged = Vec::with_capacity(tools.len());
        for (key, registration) in tools {
            staged.push(self.stage_tool(key, registration.clone())?);
        }
        let current = tools.keys().cloned().collect::<BTreeSet<_>>();
        for removed in previous.difference(&current) {
            self.txn.execute(
                "INSERT INTO tools(tool_key, present, capsule_object, capsule_hash, input_version)
                 VALUES (?1, 0, X'', ?2, ?3)
                 ON CONFLICT(tool_key, input_version) DO UPDATE SET
                   present = 0,
                   capsule_object = X'',
                   capsule_hash = excluded.capsule_hash",
                rusqlite::params![removed, [0_u8; 32].as_slice(), self.version().0 as i64,],
            )?;
        }
        Ok(staged)
    }

    /// Initialize the disposable lineage projection from the already parsed,
    /// unique source-controlled manifest. Once initialized this API is
    /// idempotent only: any changed history or cursor must use the
    /// candidate-bound acceptance/rollback methods below. Bundle and
    /// migration-endpoint stamps are never unioned into authority.
    pub fn project_verified_lineage_manifest(
        &mut self,
        source: &VerifiedSchemaLineageManifest,
    ) -> Result<(), StoreError> {
        reject_bootstrap_manifest_rows(&source.manifest)?;
        for (type_uuid, lineage) in &source.manifest.types {
            validate_type_lineage(*type_uuid, lineage)?;
        }
        if manifest_available(&self.txn)? {
            let basis = manifest_basis(&self.txn)?.expect("availability checked");
            if basis.manifest_hash == source.manifest_hash
                && projected_manifest(&self.txn)? == source.manifest
            {
                return Ok(());
            }
            return Err(StoreError::LineageMutationRequiresCandidate);
        }
        replace_lineage_projection(&self.txn, self.version(), source)
    }

    /// Explicitly accept one genuinely new schema digest compiled into the
    /// pending candidate. The manifest base and candidate identity are
    /// checked before any row changes. The accepted history appends exactly
    /// one epoch whose parent is the prior cursor; if other candidate rows
    /// still disagree, the state remains `SchemaAcceptanceRequired` at the
    /// new manifest base.
    pub fn accept_schema_candidate(
        &mut self,
        candidate: &ValidatedPipelineEpoch,
        expected_manifest: &SchemaManifestBasis,
        proposed: &VerifiedSchemaLineageManifest,
        type_uuid: TypeUuid,
        requested: LogicalHash,
    ) -> Result<LineageStamp, StoreError> {
        self.validate_schema_command(candidate, expected_manifest, type_uuid, requested)?;
        let mut next_manifest = projected_manifest(&self.txn)?;
        let next = match next_manifest.types.get_mut(&type_uuid) {
            Some(lineage) => {
                if !matches!(lineage.authority, TypeAuthorityState::Active) {
                    return Err(StoreError::InvalidAuthorityTransition {
                        type_uuid,
                        detail: "ordinary acceptance cannot implicitly reactivate a retired type"
                            .to_owned(),
                    });
                }
                if let Some(position) = lineage
                    .epochs
                    .iter()
                    .position(|epoch| epoch.digest == requested)
                {
                    let current = lineage.epochs[lineage.current as usize].digest;
                    return Err(StoreError::LineageRollback {
                        type_uuid,
                        candidate: lineage.epochs[position].digest,
                        current,
                    });
                }
                let parent = lineage.current;
                lineage.epochs.push(AcceptedSchemaEpoch {
                    digest: requested,
                    forward_parent: Some(parent),
                });
                lineage.current = u32::try_from(lineage.epochs.len() - 1).map_err(|_| {
                    invalid_manifest(
                        Some(type_uuid),
                        "accepted epoch count exceeds the DSSL u32 sequence bound",
                    )
                })?;
                lineage.clone()
            }
            None => {
                let lineage = AcceptedTypeLineage {
                    epochs: vec![AcceptedSchemaEpoch {
                        digest: requested,
                        forward_parent: None,
                    }],
                    current: 0,
                    authority: TypeAuthorityState::Active,
                };
                next_manifest.types.insert(type_uuid, lineage.clone());
                lineage
            }
        };
        validate_type_lineage(type_uuid, &next)?;
        validate_verified_transition(expected_manifest, proposed, &next_manifest)?;
        replace_lineage_projection(&self.txn, self.version(), proposed)?;
        let stamp = stamp_for(type_uuid, &next)?;
        self.publish_pipeline_epoch(candidate)?;
        Ok(stamp)
    }

    /// Move an accepted type's current cursor to an existing non-current
    /// digest after validating total custom reverse paths from the old
    /// current and every supplied live data/migration-endpoint schema that
    /// is not already a forward ancestor of the requested cursor.
    ///
    /// The caller obtains `live_schema_hashes` (including migration
    /// endpoints) from one pinned source-tree snapshot; indexed live asset
    /// schemas are added automatically. `proposed` is the coordinator's
    /// already byte-verified result from the journaled source-manifest
    /// authoring protocol. This method requires it to differ from the stale
    /// base only by the requested cursor move before replacing the disposable
    /// projection; accepted history remains append-only.
    pub fn rollback_schema_candidate(
        &mut self,
        candidate: &ValidatedPipelineEpoch,
        expected_manifest: &SchemaManifestBasis,
        proposed: &VerifiedSchemaLineageManifest,
        request: SchemaRollbackRequest<'_>,
    ) -> Result<LineageStamp, StoreError> {
        let SchemaRollbackRequest {
            type_uuid,
            target,
            live_schema_hashes,
            reverse_edges,
        } = request;
        self.validate_schema_command(candidate, expected_manifest, type_uuid, target)?;
        let lineage = type_lineage(&self.txn, type_uuid)?.ok_or_else(|| {
            StoreError::IncompleteRollbackCoverage {
                type_uuid,
                target,
                source: target,
                detail: "type is absent from the accepted lineage manifest".to_owned(),
            }
        })?;
        if !matches!(lineage.authority, TypeAuthorityState::Active) {
            return Err(StoreError::InvalidAuthorityTransition {
                type_uuid,
                detail: "rollback cannot implicitly reactivate a retired type".to_owned(),
            });
        }
        let target_index = lineage
            .epochs
            .iter()
            .position(|epoch| epoch.digest == target)
            .ok_or_else(|| StoreError::IncompleteRollbackCoverage {
                type_uuid,
                target,
                source: target,
                detail: "target digest is not an accepted epoch".to_owned(),
            })? as u32;
        let old_current = lineage.current;
        if old_current == target_index {
            return Err(invalid_manifest(
                Some(type_uuid),
                "rollback target already is the current cursor",
            ));
        }

        let mut required_live = live_schema_hashes.to_vec();
        required_live.extend(live_asset_schema_hashes(&self.txn, type_uuid)?);

        validate_rollback_coverage(
            type_uuid,
            target,
            &lineage,
            target_index,
            &required_live,
            reverse_edges,
        )?;

        let moved = AcceptedTypeLineage {
            epochs: lineage.epochs,
            current: target_index,
            authority: TypeAuthorityState::Active,
        };
        let mut next_manifest = projected_manifest(&self.txn)?;
        next_manifest.types.insert(type_uuid, moved.clone());
        validate_verified_transition(expected_manifest, proposed, &next_manifest)?;
        replace_lineage_projection(&self.txn, self.version(), proposed)?;
        let stamp = stamp_for(type_uuid, &moved)?;
        self.publish_pipeline_epoch(candidate)?;
        Ok(stamp)
    }

    /// Explicitly remove one accepted type from active registry authority.
    /// The pending candidate must omit it, and the coordinator must prove
    /// that neither authored entries nor migration endpoints still require
    /// it. History, cursor, and DSSL are preserved byte-for-byte.
    pub fn retire_schema_candidate(
        &mut self,
        candidate: &ValidatedPipelineEpoch,
        expected_manifest: &SchemaManifestBasis,
        control_basis: crate::state::SnapshotStamp,
        proposed: &VerifiedSchemaLineageManifest,
        type_uuid: TypeUuid,
        live_migration_endpoints: &[LogicalHash],
    ) -> Result<(), StoreError> {
        if control_basis != self.base_stamp() {
            return Err(StoreError::StaleControlSnapshotBasis {
                provided: control_basis,
                current: self.base_stamp(),
            });
        }
        self.validate_schema_command_basis(candidate, expected_manifest)?;
        let candidate_digest = candidate.schema_registry.get(&type_uuid).copied();
        if candidate_digest.is_some() {
            return Err(StoreError::SchemaCandidateRetirementMismatch {
                type_uuid,
                candidate: candidate_digest,
            });
        }

        let live_assets = live_asset_count(&self.txn, type_uuid)?;
        if live_assets != 0 || !live_migration_endpoints.is_empty() {
            return Err(StoreError::SchemaRetirementBlocked {
                type_uuid,
                live_assets,
                live_migration_endpoints: live_migration_endpoints.len(),
            });
        }

        let mut next_manifest = projected_manifest(&self.txn)?;
        let lineage = next_manifest.types.get_mut(&type_uuid).ok_or_else(|| {
            StoreError::InvalidAuthorityTransition {
                type_uuid,
                detail: "retirement requires an accepted lineage row".to_owned(),
            }
        })?;
        if !matches!(lineage.authority, TypeAuthorityState::Active) {
            return Err(StoreError::InvalidAuthorityTransition {
                type_uuid,
                detail: "type is already retired".to_owned(),
            });
        }
        lineage.authority = TypeAuthorityState::Retired {
            retired_from: lineage.current,
        };
        let retired = lineage.clone();
        validate_type_lineage(type_uuid, &retired)?;
        validate_verified_transition(expected_manifest, proposed, &next_manifest)?;
        replace_lineage_projection(&self.txn, self.version(), proposed)?;
        self.publish_pipeline_epoch(candidate)?;
        Ok(())
    }

    /// Reject a decoded migration endpoint that names retired schema
    /// authority. Coordinators call this for every endpoint before publishing
    /// scanner/import results in this same input transaction.
    pub fn ensure_migration_endpoint_type_active(
        &self,
        type_uuid: TypeUuid,
        endpoint: LogicalHash,
    ) -> Result<(), StoreError> {
        self.ensure_type_reference_active(
            type_uuid,
            RetiredTypeReference::MigrationEndpoint(endpoint),
        )
    }

    pub(crate) fn ensure_type_reference_active(
        &self,
        type_uuid: TypeUuid,
        reference: RetiredTypeReference,
    ) -> Result<(), StoreError> {
        let authority = self
            .txn
            .query_row(
                "SELECT authority FROM schema_lineage_current WHERE type_uuid = ?1",
                [type_uuid.0.as_slice()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        if authority == Some(1) {
            return Err(StoreError::RetiredTypeReferenced {
                type_uuid,
                reference,
            });
        }
        Ok(())
    }

    /// Explicitly return a retired type to active authority. The exact
    /// candidate digest is selected: current is a pure authority flip, a new
    /// digest appends, a known forward descendant advances, and all other
    /// existing selections require rollback coverage.
    pub fn reactivate_schema_candidate(
        &mut self,
        candidate: &ValidatedPipelineEpoch,
        expected_manifest: &SchemaManifestBasis,
        proposed: &VerifiedSchemaLineageManifest,
        request: SchemaReactivationRequest<'_>,
    ) -> Result<LineageStamp, StoreError> {
        self.validate_schema_command_basis(candidate, expected_manifest)?;
        let type_uuid = request.type_uuid;
        let requested = candidate
            .schema_registry
            .get(&type_uuid)
            .copied()
            .ok_or(StoreError::SchemaCandidateReactivationMismatch { type_uuid })?;
        let mut next_manifest = projected_manifest(&self.txn)?;
        let lineage = next_manifest.types.get_mut(&type_uuid).ok_or_else(|| {
            StoreError::InvalidAuthorityTransition {
                type_uuid,
                detail: "reactivation requires retained accepted history".to_owned(),
            }
        })?;
        if !matches!(lineage.authority, TypeAuthorityState::Retired { .. }) {
            return Err(StoreError::InvalidAuthorityTransition {
                type_uuid,
                detail: "type is already active".to_owned(),
            });
        }

        match lineage
            .epochs
            .iter()
            .position(|epoch| epoch.digest == requested)
        {
            None => {
                let parent = lineage.current;
                lineage.epochs.push(AcceptedSchemaEpoch {
                    digest: requested,
                    forward_parent: Some(parent),
                });
                lineage.current = u32::try_from(lineage.epochs.len() - 1).map_err(|_| {
                    invalid_manifest(
                        Some(type_uuid),
                        "accepted epoch count exceeds the DSSL u32 sequence bound",
                    )
                })?;
            }
            Some(position) => {
                let target_index = u32::try_from(position).map_err(|_| {
                    invalid_manifest(
                        Some(type_uuid),
                        "accepted epoch index exceeds the DSSL u32 cursor bound",
                    )
                })?;
                if target_index != lineage.current
                    && !is_ancestor(lineage, lineage.current, target_index)
                {
                    let mut required_live = request.live_schema_hashes.to_vec();
                    required_live.extend(live_asset_schema_hashes(&self.txn, type_uuid)?);
                    validate_rollback_coverage(
                        type_uuid,
                        requested,
                        lineage,
                        target_index,
                        &required_live,
                        request.reverse_edges,
                    )?;
                }
                lineage.current = target_index;
            }
        }
        lineage.authority = TypeAuthorityState::Active;
        let activated = lineage.clone();
        validate_type_lineage(type_uuid, &activated)?;
        validate_verified_transition(expected_manifest, proposed, &next_manifest)?;
        replace_lineage_projection(&self.txn, self.version(), proposed)?;
        let stamp = stamp_for(type_uuid, &activated)?;
        self.publish_pipeline_epoch(candidate)?;
        Ok(stamp)
    }

    fn validate_schema_command(
        &self,
        candidate: &PipelineEpoch,
        expected_manifest: &SchemaManifestBasis,
        type_uuid: TypeUuid,
        requested: LogicalHash,
    ) -> Result<(), StoreError> {
        self.validate_schema_command_basis(candidate, expected_manifest)?;
        let candidate_digest = candidate.schema_registry.get(&type_uuid).copied();
        if candidate_digest != Some(requested) {
            return Err(StoreError::SchemaCandidateCursorMismatch {
                type_uuid,
                requested,
                candidate: candidate_digest,
            });
        }
        Ok(())
    }

    fn validate_schema_command_basis(
        &self,
        candidate: &PipelineEpoch,
        expected_manifest: &SchemaManifestBasis,
    ) -> Result<(), StoreError> {
        validate_target_set(&candidate.target_set)?;
        validate_bootstrap_schema_registry(&candidate.schema_registry)?;
        let actual_manifest = manifest_basis(&self.txn)?;
        if actual_manifest.as_ref() != Some(expected_manifest) {
            return Err(StoreError::StaleSchemaManifestBase {
                expected: Box::new(expected_manifest.clone()),
                actual: actual_manifest.map(Box::new),
            });
        }
        let actual_candidate =
            PipelineCandidateIdentity::try_from(candidate).map_err(StoreError::InvalidTargetSet)?;
        let expected_candidate = pending_candidate_identity(&self.txn)?
            .ok_or(StoreError::LineageMutationRequiresCandidate)?;
        if actual_candidate != expected_candidate
            || load_schema_registry(&self.txn, true)? != candidate.schema_registry
            || load_target_set(&self.txn, true, expected_candidate.target_set_hash)?
                != candidate.target_set
        {
            return Err(StoreError::StaleSchemaCandidate {
                expected: Box::new(expected_candidate),
                actual: Box::new(actual_candidate),
            });
        }
        Ok(())
    }
}

fn lineage_rows(
    conn: &rusqlite::Connection,
    type_uuid: TypeUuid,
) -> Result<Vec<LineageEntry>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT generation, schema_hash, forward_parent FROM schema_lineage
         WHERE type_uuid = ?1 ORDER BY generation",
    )?;
    let rows = stmt.query_map([type_uuid.0.as_slice()], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, Vec<u8>>(1)?,
            r.get::<_, Option<i64>>(2)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (generation, hash, forward_parent) = row?;
        let expected_generation = out.len() as u64 + 1;
        let generation = u64::try_from(generation)
            .map_err(|_| invalid_manifest(Some(type_uuid), "a projected generation is negative"))?;
        if generation != expected_generation {
            return Err(invalid_manifest(
                Some(type_uuid),
                "projected accepted generations are not contiguous",
            ));
        }
        let schema_hash = LogicalHash(hash.try_into().map_err(|_| {
            invalid_manifest(
                Some(type_uuid),
                "a projected schema digest is not exactly 32 bytes",
            )
        })?);
        let forward_parent = forward_parent
            .map(|parent| {
                u32::try_from(parent).map_err(|_| {
                    invalid_manifest(Some(type_uuid), "a projected parent index is invalid")
                })
            })
            .transpose()?;
        out.push(LineageEntry {
            generation,
            schema_hash,
            forward_parent,
        });
    }
    Ok(out)
}

fn validate_type_lineage(
    type_uuid: TypeUuid,
    lineage: &AcceptedTypeLineage,
) -> Result<(), StoreError> {
    if lineage.epochs.is_empty() {
        return Err(invalid_manifest(
            Some(type_uuid),
            "a manifest type must contain at least one accepted epoch",
        ));
    }
    if lineage.epochs.len() > u32::MAX as usize {
        return Err(invalid_manifest(
            Some(type_uuid),
            "accepted epoch count exceeds the DSSL u32 sequence bound",
        ));
    }
    if usize::try_from(lineage.current)
        .ok()
        .filter(|current| *current < lineage.epochs.len())
        .is_none()
    {
        return Err(invalid_manifest(
            Some(type_uuid),
            "current cursor is outside the accepted epoch vector",
        ));
    }
    if let TypeAuthorityState::Retired { retired_from } = lineage.authority {
        if retired_from != lineage.current {
            return Err(invalid_manifest(
                Some(type_uuid),
                "a retired lineage must record its preserved current cursor as retired_from",
            ));
        }
    }
    let mut digests = BTreeSet::new();
    for (index, epoch) in lineage.epochs.iter().enumerate() {
        if !digests.insert(epoch.digest) {
            return Err(invalid_manifest(
                Some(type_uuid),
                "one digest appears in more than one accepted epoch",
            ));
        }
        match (index, epoch.forward_parent) {
            (0, None) => {}
            (0, Some(_)) => {
                return Err(invalid_manifest(
                    Some(type_uuid),
                    "the first accepted epoch must not have a forward parent",
                ));
            }
            (_, Some(parent)) if (parent as usize) < index => {}
            (_, Some(_)) => {
                return Err(invalid_manifest(
                    Some(type_uuid),
                    "a forward parent must name an earlier accepted epoch",
                ));
            }
            (_, None) => {
                return Err(invalid_manifest(
                    Some(type_uuid),
                    "only the first accepted epoch may omit its forward parent",
                ));
            }
        }
    }
    Ok(())
}

fn invalid_manifest(type_uuid: Option<TypeUuid>, detail: &str) -> StoreError {
    StoreError::InvalidLineageManifest {
        type_uuid,
        detail: detail.to_owned(),
    }
}

fn manifest_available(conn: &rusqlite::Connection) -> Result<bool, StoreError> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM schema_lineage_state WHERE id = 0",
            [],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn manifest_basis(conn: &rusqlite::Connection) -> Result<Option<SchemaManifestBasis>, StoreError> {
    let manifest_hash = conn
        .query_row(
            "SELECT manifest_hash FROM schema_lineage_state WHERE id = 0",
            [],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    let Some(manifest_hash) = manifest_hash else {
        return Ok(None);
    };
    let manifest = projected_manifest(conn)?;
    let current_cursors = manifest
        .types
        .into_iter()
        .filter_map(|(type_uuid, lineage)| {
            matches!(lineage.authority, TypeAuthorityState::Active)
                .then_some((type_uuid, lineage.epochs[lineage.current as usize].digest))
        })
        .collect();
    Ok(Some(SchemaManifestBasis {
        manifest_hash: ContentHash(exact_blob32(manifest_hash, "source manifest hash")?),
        current_cursors,
    }))
}

fn validate_verified_transition(
    expected: &SchemaManifestBasis,
    proposed: &VerifiedSchemaLineageManifest,
    exact_manifest: &SchemaLineageManifest,
) -> Result<(), StoreError> {
    for (type_uuid, lineage) in &proposed.manifest.types {
        validate_type_lineage(*type_uuid, lineage)?;
    }
    if proposed.manifest_hash == expected.manifest_hash {
        return Err(invalid_manifest(
            None,
            "a changed source manifest must carry its new verified byte hash",
        ));
    }
    if proposed.manifest != *exact_manifest {
        return Err(invalid_manifest(
            None,
            "verified source manifest is not the exact candidate-bound one-step transition",
        ));
    }
    Ok(())
}

fn replace_lineage_projection(
    conn: &rusqlite::Connection,
    version: InputVersion,
    source: &VerifiedSchemaLineageManifest,
) -> Result<(), StoreError> {
    reject_bootstrap_manifest_rows(&source.manifest)?;
    for (type_uuid, lineage) in &source.manifest.types {
        validate_type_lineage(*type_uuid, lineage)?;
    }
    conn.execute("DELETE FROM schema_lineage_current", [])?;
    conn.execute("DELETE FROM schema_lineage", [])?;
    conn.execute("DELETE FROM schema_lineage_state", [])?;
    for (type_uuid, lineage) in &source.manifest.types {
        for (index, epoch) in lineage.epochs.iter().enumerate() {
            conn.execute(
                "INSERT INTO schema_lineage(
                     type_uuid, generation, schema_hash, forward_parent, input_version
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    type_uuid.0.as_slice(),
                    (index + 1) as i64,
                    epoch.digest.0.as_slice(),
                    epoch.forward_parent.map(i64::from),
                    version.0 as i64,
                ],
            )?;
        }
        write_lineage_current(conn, version, *type_uuid, lineage)?;
    }
    conn.execute(
        "INSERT INTO schema_lineage_state(id, input_version, manifest_hash)
         VALUES (0, ?1, ?2)",
        rusqlite::params![version.0 as i64, source.manifest_hash.0.as_slice()],
    )?;
    Ok(())
}

fn schema_registry_mismatches(
    candidate: &BTreeMap<TypeUuid, LogicalHash>,
    manifest: &BTreeMap<TypeUuid, LogicalHash>,
) -> Vec<SchemaRegistryMismatch> {
    candidate
        .keys()
        .chain(manifest.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|type_uuid| !is_bootstrap_control_type(*type_uuid))
        .filter_map(|type_uuid| {
            let candidate = candidate.get(&type_uuid).copied();
            let manifest = manifest.get(&type_uuid).copied();
            (candidate != manifest).then_some(SchemaRegistryMismatch {
                type_uuid,
                candidate,
                manifest,
            })
        })
        .collect()
}

fn validate_bootstrap_schema_registry(
    candidate: &BTreeMap<TypeUuid, LogicalHash>,
) -> Result<(), StoreError> {
    let expected =
        bootstrap_control_logical_registry_v1().map_err(StoreError::InvalidBootstrapSpec)?;
    for (type_uuid, expected_hash) in expected {
        let observed = candidate.get(&type_uuid).copied();
        if observed != Some(expected_hash) {
            return Err(StoreError::InvalidBootstrapRegistry {
                type_uuid,
                expected: expected_hash,
                observed,
            });
        }
    }
    Ok(())
}

fn reject_bootstrap_manifest_rows(manifest: &SchemaLineageManifest) -> Result<(), StoreError> {
    if let Some(type_uuid) = manifest
        .types
        .keys()
        .copied()
        .find(|type_uuid| is_bootstrap_control_type(*type_uuid))
    {
        return Err(invalid_manifest(
            Some(type_uuid),
            "bootstrap-control types are format authority and must be omitted from the lineage manifest",
        ));
    }
    Ok(())
}

fn replace_schema_registry(
    conn: &rusqlite::Connection,
    table: &'static str,
    registry: &BTreeMap<TypeUuid, LogicalHash>,
) -> Result<(), StoreError> {
    let (delete, insert) = match table {
        "pipeline_schema_registry" => (
            "DELETE FROM pipeline_schema_registry",
            "INSERT INTO pipeline_schema_registry(type_uuid, logical_hash) VALUES (?1, ?2)",
        ),
        "pipeline_candidate_schema_registry" => (
            "DELETE FROM pipeline_candidate_schema_registry",
            "INSERT INTO pipeline_candidate_schema_registry(type_uuid, logical_hash) VALUES (?1, ?2)",
        ),
        _ => unreachable!("registry table is an internal closed choice"),
    };
    conn.execute(delete, [])?;
    for (type_uuid, logical_hash) in registry {
        conn.execute(
            insert,
            rusqlite::params![type_uuid.0.as_slice(), logical_hash.0.as_slice()],
        )?;
    }
    Ok(())
}

fn load_schema_registry(
    conn: &rusqlite::Connection,
    candidate: bool,
) -> Result<BTreeMap<TypeUuid, LogicalHash>, StoreError> {
    let sql = if candidate {
        "SELECT type_uuid, logical_hash FROM pipeline_candidate_schema_registry ORDER BY type_uuid"
    } else {
        "SELECT type_uuid, logical_hash FROM pipeline_schema_registry ORDER BY type_uuid"
    };
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    let mut registry = BTreeMap::new();
    for row in rows {
        let (type_uuid, logical_hash) = row?;
        let type_uuid = TypeUuid(type_uuid.try_into().map_err(|_| {
            invalid_manifest(None, "a pipeline registry UUID is not exactly 16 bytes")
        })?);
        let logical_hash = LogicalHash(logical_hash.try_into().map_err(|_| {
            invalid_manifest(
                Some(type_uuid),
                "a pipeline registry logical hash is not exactly 32 bytes",
            )
        })?);
        registry.insert(type_uuid, logical_hash);
    }
    Ok(registry)
}

fn validate_target_set(target_set: &CanonicalTargetSet) -> Result<(), StoreError> {
    CanonicalTargetSet::from_canonical(target_set.rows.clone(), target_set.digest)
        .map(|_| ())
        .map_err(StoreError::InvalidTargetSet)
}

fn replace_target_set(
    conn: &rusqlite::Connection,
    candidate: bool,
    target_set: &CanonicalTargetSet,
) -> Result<(), StoreError> {
    validate_target_set(target_set)?;
    let (delete, insert) = if candidate {
        (
            "DELETE FROM pipeline_candidate_target_set",
            "INSERT INTO pipeline_candidate_target_set(name, target_definition_hash) VALUES (?1, ?2)",
        )
    } else {
        (
            "DELETE FROM pipeline_target_set",
            "INSERT INTO pipeline_target_set(name, target_definition_hash) VALUES (?1, ?2)",
        )
    };
    conn.execute(delete, [])?;
    for row in &target_set.rows {
        conn.execute(
            insert,
            rusqlite::params![row.name, row.target_definition_hash.as_slice()],
        )?;
    }
    Ok(())
}

fn load_target_set(
    conn: &rusqlite::Connection,
    candidate: bool,
    digest: TargetSetHash,
) -> Result<CanonicalTargetSet, StoreError> {
    let table = if candidate {
        "pipeline_candidate_target_set"
    } else {
        "pipeline_target_set"
    };
    let mut stmt = conn.prepare(&format!(
        "SELECT name, target_definition_hash FROM {table} ORDER BY CAST(name AS BLOB)"
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    let mut target_rows = Vec::new();
    for row in rows {
        let (name, target_definition_hash) = row?;
        target_rows.push(TargetSetRow {
            name,
            target_definition_hash: exact_blob32(target_definition_hash, "target-definition hash")?,
        });
    }
    CanonicalTargetSet::from_canonical(target_rows, digest).map_err(StoreError::InvalidTargetSet)
}

fn pending_candidate_identity(
    conn: &rusqlite::Connection,
) -> Result<Option<PipelineCandidateIdentity>, StoreError> {
    type CandidateRow = (Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>);
    let row: Option<CandidateRow> = conn
        .query_row(
            "SELECT acceptance_candidate_dylib_hash,
                    acceptance_candidate_compiled_types,
                    acceptance_candidate_target_set_hash
             FROM pipeline_state WHERE id = 0",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((dylib, compiled_types, target_set_hash)) = row else {
        return Ok(None);
    };
    match (dylib, compiled_types, target_set_hash) {
        (None, None, None) => Ok(None),
        (Some(dylib), Some(compiled_types), Some(target_set_hash)) => {
            Ok(Some(PipelineCandidateIdentity {
                dylib_hash: exact_blob32(dylib, "candidate dylib hash")?,
                compiled_types: CompiledAttestationDigest(exact_blob32(
                    compiled_types,
                    "candidate compiled-type attestation",
                )?),
                target_set_hash: {
                    let digest =
                        TargetSetHash(exact_blob32(target_set_hash, "candidate target-set hash")?);
                    load_target_set(conn, true, digest)?.digest
                },
            }))
        }
        _ => Err(invalid_manifest(
            None,
            "pipeline candidate identity columns are incomplete",
        )),
    }
}

fn exact_blob32(bytes: Vec<u8>, name: &str) -> Result<[u8; 32], StoreError> {
    bytes
        .try_into()
        .map_err(|_| invalid_manifest(None, &format!("{name} is not exactly 32 bytes")))
}

fn type_lineage(
    conn: &rusqlite::Connection,
    type_uuid: TypeUuid,
) -> Result<Option<AcceptedTypeLineage>, StoreError> {
    let rows = lineage_rows(conn, type_uuid)?;
    let current_row: Option<(i64, Vec<u8>, i64, Option<i64>)> = conn
        .query_row(
            "SELECT current_cursor, chain_digest, authority, retired_from
             FROM schema_lineage_current
             WHERE type_uuid = ?1",
            [type_uuid.0.as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((current, chain, authority, retired_from)) = current_row else {
        if rows.is_empty() {
            return Ok(None);
        }
        return Err(invalid_manifest(
            Some(type_uuid),
            "accepted history has no current cursor",
        ));
    };
    let current = u32::try_from(current).map_err(|_| {
        invalid_manifest(Some(type_uuid), "the projected current cursor is invalid")
    })?;
    let authority = match (authority, retired_from) {
        (0, None) => TypeAuthorityState::Active,
        (1, Some(retired_from)) => TypeAuthorityState::Retired {
            retired_from: u32::try_from(retired_from).map_err(|_| {
                invalid_manifest(
                    Some(type_uuid),
                    "the projected retired_from cursor is invalid",
                )
            })?,
        },
        _ => {
            return Err(invalid_manifest(
                Some(type_uuid),
                "the projected authority state is invalid",
            ));
        }
    };
    let lineage = AcceptedTypeLineage {
        epochs: rows
            .into_iter()
            .map(|entry| AcceptedSchemaEpoch {
                digest: entry.schema_hash,
                forward_parent: entry.forward_parent,
            })
            .collect(),
        current,
        authority,
    };
    validate_type_lineage(type_uuid, &lineage)?;
    let chain: [u8; 32] = chain.try_into().map_err(|_| {
        invalid_manifest(
            Some(type_uuid),
            "the projected DSSL commitment is not exactly 32 bytes",
        )
    })?;
    if chain != lineage_chain_digest(type_uuid, &lineage.epochs, lineage.current) {
        return Err(invalid_manifest(
            Some(type_uuid),
            "the disposable projection's DSSL commitment does not verify",
        ));
    }
    Ok(Some(lineage))
}

fn projected_manifest(conn: &rusqlite::Connection) -> Result<SchemaLineageManifest, StoreError> {
    let mut stmt =
        conn.prepare("SELECT type_uuid FROM schema_lineage_current ORDER BY type_uuid")?;
    let rows = stmt.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
    let mut types = BTreeMap::new();
    for row in rows {
        let bytes = row?;
        let type_uuid = TypeUuid(bytes.try_into().map_err(|_| {
            invalid_manifest(
                None,
                "a projected lineage type UUID is not exactly 16 bytes",
            )
        })?);
        let lineage = type_lineage(conn, type_uuid)?.ok_or_else(|| {
            invalid_manifest(
                Some(type_uuid),
                "a projected current cursor has no accepted history",
            )
        })?;
        types.insert(type_uuid, lineage);
    }
    Ok(SchemaLineageManifest { types })
}

fn write_lineage_current(
    conn: &rusqlite::Connection,
    version: InputVersion,
    type_uuid: TypeUuid,
    lineage: &AcceptedTypeLineage,
) -> Result<(), StoreError> {
    let chain = lineage_chain_digest(type_uuid, &lineage.epochs, lineage.current);
    let (authority, retired_from) = match lineage.authority {
        TypeAuthorityState::Active => (0i64, None),
        TypeAuthorityState::Retired { retired_from } => (1i64, Some(i64::from(retired_from))),
    };
    conn.execute(
        "INSERT INTO schema_lineage_current(
             type_uuid, current_cursor, chain_digest, authority, retired_from, input_version
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(type_uuid) DO UPDATE SET
             current_cursor = excluded.current_cursor,
             chain_digest = excluded.chain_digest,
             authority = excluded.authority,
             retired_from = excluded.retired_from,
             input_version = excluded.input_version",
        rusqlite::params![
            type_uuid.0.as_slice(),
            i64::from(lineage.current),
            chain.as_slice(),
            authority,
            retired_from,
            version.0 as i64,
        ],
    )?;
    Ok(())
}

fn stamp_for(
    type_uuid: TypeUuid,
    lineage: &AcceptedTypeLineage,
) -> Result<LineageStamp, StoreError> {
    validate_type_lineage(type_uuid, lineage)?;
    Ok(LineageStamp {
        chain: lineage_chain_digest(type_uuid, &lineage.epochs, lineage.current),
        epochs: lineage.epochs.clone(),
        cursor: lineage.current,
    })
}

fn is_ancestor(lineage: &AcceptedTypeLineage, ancestor: u32, descendant: u32) -> bool {
    let mut cursor = Some(descendant);
    while let Some(index) = cursor {
        if index == ancestor {
            return true;
        }
        cursor = lineage.epochs[index as usize].forward_parent;
    }
    false
}

fn validate_rollback_coverage(
    type_uuid: TypeUuid,
    target: LogicalHash,
    lineage: &AcceptedTypeLineage,
    target_index: u32,
    live_schema_hashes: &[LogicalHash],
    reverse_edges: &[ReverseMigrationEdge],
) -> Result<(), StoreError> {
    let positions: BTreeMap<_, _> = lineage
        .epochs
        .iter()
        .enumerate()
        .map(|(index, epoch)| (epoch.digest, index as u32))
        .collect();
    let mut outgoing: BTreeMap<LogicalHash, Vec<LogicalHash>> = BTreeMap::new();
    for edge in reverse_edges {
        if !positions.contains_key(&edge.from) || !positions.contains_key(&edge.to) {
            return Err(coverage_error(
                type_uuid,
                target,
                edge.from,
                "a supplied reverse edge endpoint is not an accepted epoch",
            ));
        }
        outgoing.entry(edge.from).or_default().push(edge.to);
    }

    let old_current = lineage.epochs[lineage.current as usize].digest;
    let mut required = BTreeSet::from([old_current]);
    for source in live_schema_hashes {
        let Some(&position) = positions.get(source) else {
            return Err(coverage_error(
                type_uuid,
                target,
                *source,
                "a live data or migration-endpoint schema is not accepted",
            ));
        };
        if !is_ancestor(lineage, position, target_index) {
            required.insert(*source);
        }
    }

    for source in required {
        let mut cursor = source;
        let mut visited = BTreeSet::new();
        while cursor != target {
            if !visited.insert(cursor) {
                return Err(coverage_error(
                    type_uuid,
                    target,
                    source,
                    "custom reverse path is cyclic",
                ));
            }
            let Some(next) = outgoing.get(&cursor) else {
                return Err(coverage_error(
                    type_uuid,
                    target,
                    source,
                    "custom reverse path is incomplete",
                ));
            };
            if next.len() != 1 {
                return Err(coverage_error(
                    type_uuid,
                    target,
                    source,
                    "custom reverse path is ambiguous",
                ));
            }
            cursor = next[0];
        }
    }
    Ok(())
}

fn live_asset_schema_hashes(
    conn: &rusqlite::Connection,
    type_uuid: TypeUuid,
) -> Result<Vec<LogicalHash>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT logical_hash FROM assets
         WHERE type_uuid = ?1 AND logical_hash IS NOT NULL",
    )?;
    let rows = stmt.query_map([type_uuid.0.as_slice()], |row| row.get::<_, Vec<u8>>(0))?;
    rows.map(|row| {
        let bytes = row?;
        Ok(LogicalHash(bytes.try_into().map_err(|_| {
            invalid_manifest(
                Some(type_uuid),
                "a live asset's schema digest is not exactly 32 bytes",
            )
        })?))
    })
    .collect()
}

fn live_asset_count(conn: &rusqlite::Connection, type_uuid: TypeUuid) -> Result<u64, StoreError> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM assets WHERE type_uuid = ?1",
        [type_uuid.0.as_slice()],
        |row| row.get(0),
    )?;
    u64::try_from(count)
        .map_err(|_| invalid_manifest(Some(type_uuid), "the live authored-entry count is negative"))
}

fn coverage_error(
    type_uuid: TypeUuid,
    target: LogicalHash,
    source: LogicalHash,
    detail: &str,
) -> StoreError {
    StoreError::IncompleteRollbackCoverage {
        type_uuid,
        target,
        source,
        detail: detail.to_owned(),
    }
}

impl Store {
    /// Persist the first poison discovered in an already-published module
    /// epoch without minting a new input version. This is a narrow monotonic
    /// runtime-lifecycle transition, guarded by the exact dylib identity.
    pub fn poison_published_pipeline_epoch(
        &mut self,
        expected_dylib_hash: [u8; 32],
        poison: &PipelinePoison,
    ) -> Result<(), StoreError> {
        poison
            .validate()
            .map_err(StoreError::InvalidPipelinePoison)?;
        if poison.origin != crate::state::PipelinePoisonOrigin::PublishedRuntime {
            return Err(StoreError::InvalidPipelinePoison(
                crate::state::PipelinePoisonError::InvalidMatrix,
            ));
        }

        let transaction = self.conn.transaction()?;
        let row: Option<(Option<Vec<u8>>, Option<i64>)> = transaction
            .query_row(
                "SELECT dylib_hash, poison_code FROM pipeline_state WHERE id = 0",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let (actual, already_unavailable) = match row {
            Some((actual, poison_code)) => (
                actual
                    .map(|bytes| exact_blob32(bytes, "published pipeline dylib hash"))
                    .transpose()?,
                poison_code.is_some(),
            ),
            None => (None, false),
        };
        if actual != Some(expected_dylib_hash) || already_unavailable {
            return Err(StoreError::StalePublishedPipeline {
                expected: expected_dylib_hash,
                actual,
                already_unavailable,
            });
        }
        let changed = transaction.execute(
            "UPDATE pipeline_state SET
                 poison_code = ?1,
                 poison_origin = ?2,
                 poison_cleanup = ?3,
                 poison_identity = ?4,
                 poison_message = ?5
             WHERE id = 0 AND dylib_hash = ?6 AND poison_code IS NULL",
            rusqlite::params![
                poison.code as u16,
                poison.origin as u16,
                poison.cleanup as u16,
                poison.identity.as_slice(),
                poison.message,
                expected_dylib_hash.as_slice(),
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::StalePublishedPipeline {
                expected: expected_dylib_hash,
                actual,
                already_unavailable: true,
            });
        }
        transaction.commit()?;
        Ok(())
    }

    /// The published pipeline state, or `None` before any publication.
    pub fn pipeline_state(&self) -> Result<Option<PipelineState>, StoreError> {
        type StateRow = (
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<Vec<u8>>,
            Option<String>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
        );
        let row: Option<StateRow> = self
            .conn
            .query_row(
                "SELECT dylib_hash, load_policy_digest, compiled_types,
                        target_set_hash, poison_code, poison_origin, poison_cleanup,
                        poison_identity, poison_message,
                        acceptance_candidate_dylib_hash,
                        acceptance_candidate_compiled_types,
                        acceptance_candidate_target_set_hash,
                        acceptance_manifest_hash
                 FROM pipeline_state WHERE id = 0",
                [],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                        r.get(8)?,
                        r.get(9)?,
                        r.get(10)?,
                        r.get(11)?,
                        r.get(12)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            dylib,
            lpd,
            compiled_types,
            target_set_hash,
            poison_code,
            poison_origin,
            poison_cleanup,
            poison_identity,
            poison_message,
            candidate_dylib,
            candidate_compiled_types,
            candidate_target_set,
            stored_manifest_hash,
        )) = row
        else {
            return Ok(None);
        };

        let epoch = match (dylib, lpd, compiled_types, target_set_hash) {
            (Some(dylib), Some(lpd), Some(compiled_types), Some(target_set_hash)) => {
                let mut stmt = self
                    .conn
                    .prepare("SELECT kind, reg_id, version FROM registrations")?;
                let registrations: Vec<Registration> = stmt
                    .query_map([], |r| {
                        Ok(Registration {
                            kind: if r.get::<_, i64>(0)? == 0 {
                                RegistrationKind::Importer
                            } else {
                                RegistrationKind::Processor
                            },
                            id: r.get(1)?,
                            version: r.get(2)?,
                        })
                    })?
                    .collect::<Result<_, _>>()?;
                Some(std::sync::Arc::new(PipelineEpoch {
                    dylib_hash: exact_blob32(dylib, "published pipeline dylib hash")?,
                    load_policy_digest: exact_blob32(lpd, "published pipeline load-policy digest")?,
                    compiled_types: CompiledAttestationDigest(exact_blob32(
                        compiled_types,
                        "published compiled-type attestation",
                    )?),
                    target_set: {
                        let digest = TargetSetHash(exact_blob32(
                            target_set_hash,
                            "published target-set hash",
                        )?);
                        load_target_set(&self.conn, false, digest)?
                    },
                    schema_registry: load_schema_registry(&self.conn, false)?,
                    registrations,
                }))
            }
            (None, None, None, None) => None,
            _ => {
                return Err(invalid_manifest(
                    None,
                    "published pipeline identity columns are incomplete",
                ));
            }
        };

        let has_candidate = candidate_dylib.is_some()
            || candidate_compiled_types.is_some()
            || candidate_target_set.is_some()
            || stored_manifest_hash.is_some();
        let poison = match (
            poison_code,
            poison_origin,
            poison_cleanup,
            poison_identity,
            poison_message,
        ) {
            (None, None, None, None, None) => None,
            (Some(code), Some(origin), Some(cleanup), Some(identity), Some(message)) => Some(
                PipelinePoison::from_wire(
                    u16::try_from(code).map_err(|_| {
                        StoreError::InvalidPipelinePoison(
                            crate::state::PipelinePoisonError::UnknownCode(code as u16),
                        )
                    })?,
                    u16::try_from(origin).map_err(|_| {
                        StoreError::InvalidPipelinePoison(
                            crate::state::PipelinePoisonError::UnknownOrigin(origin as u16),
                        )
                    })?,
                    u16::try_from(cleanup).map_err(|_| {
                        StoreError::InvalidPipelinePoison(
                            crate::state::PipelinePoisonError::UnknownCleanup(cleanup as u16),
                        )
                    })?,
                    exact_blob32(identity, "pipeline-poison identity")?,
                    message,
                )
                .map_err(StoreError::InvalidPipelinePoison)?,
            ),
            _ => {
                return Err(invalid_manifest(
                    None,
                    "pipeline poison columns are incomplete",
                ));
            }
        };

        if poison.is_some() && has_candidate {
            return Err(invalid_manifest(
                None,
                "pipeline state is both poisoned and schema-acceptance-required",
            ));
        }
        if has_candidate {
            let candidate = pending_candidate_identity(&self.conn)?.ok_or_else(|| {
                invalid_manifest(None, "pipeline candidate identity columns are incomplete")
            })?;
            let actual_manifest = manifest_basis(&self.conn)?.ok_or_else(|| {
                invalid_manifest(None, "schema candidate has no projected source manifest")
            })?;
            let stored_manifest_hash = ContentHash(exact_blob32(
                stored_manifest_hash.ok_or_else(|| {
                    invalid_manifest(None, "schema candidate has no source manifest hash")
                })?,
                "stored schema candidate manifest hash",
            )?);
            if stored_manifest_hash != actual_manifest.manifest_hash {
                return Err(invalid_manifest(
                    None,
                    "schema candidate base differs from the projected source manifest",
                ));
            }
            let candidate_registry = load_schema_registry(&self.conn, true)?;
            let mismatches =
                schema_registry_mismatches(&candidate_registry, &actual_manifest.current_cursors);
            return Ok(Some(PipelineState::SchemaAcceptanceRequired {
                required: SchemaAcceptanceRequired {
                    manifest: actual_manifest,
                    candidate,
                    mismatches,
                },
                last_good: epoch,
            }));
        }

        Ok(Some(match poison {
            None => match epoch {
                Some(epoch) => PipelineState::Ready(epoch),
                // A row with no identity and no typed unavailable state
                // cannot be published through this API.
                None => return Ok(None),
            },
            Some(error) => PipelineState::Poisoned {
                error,
                last_good: epoch,
            },
        }))
    }

    /// Resolve a tool key through the ToolEpoch table (§13).
    pub fn tool(&self, key: &str) -> Result<Option<StagedTool>, StoreError> {
        self.tool_at(key, self.input_version())
    }

    /// Resolve the last ToolEpoch mapping visible at an exact pinned input
    /// version. Historical rows remain addressable while their immutable
    /// capsule roots coexist, so an older job never launches a replacement.
    pub fn tool_at(
        &self,
        key: &str,
        basis: InputVersion,
    ) -> Result<Option<StagedTool>, StoreError> {
        let row = self
            .conn
            .query_row(
                "SELECT present, capsule_object, capsule_hash, input_version
                   FROM tools
                  WHERE tool_key = ?1 AND input_version <= ?2
                  ORDER BY input_version DESC
                  LIMIT 1",
                rusqlite::params![key, i64::try_from(basis.0).unwrap_or(i64::MAX)],
                |r| {
                    Ok((
                        r.get::<_, bool>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, Vec<u8>>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((present, record, hash, input_version)) = row else {
            return Ok(None);
        };
        if !present {
            return Ok(None);
        }
        let capsule = ToolExecutionCapsuleV1::decode_record(&record)
            .map_err(StoreError::InvalidToolCapsule)?;
        let capsule_hash: [u8; 32] = hash.try_into().map_err(|_| {
            StoreError::InvalidToolCapsule(distill_core::tool::ToolCapsuleError::Truncated)
        })?;
        if capsule.digest().map_err(StoreError::InvalidToolCapsule)? != capsule_hash {
            return Err(StoreError::ToolCapsuleUnavailable {
                key: key.to_owned(),
                path: self.config.state_path.join("tools"),
                detail: "persisted capsule hash does not match its object",
            });
        }
        let root = self
            .config
            .state_path
            .join("tools/capsules")
            .join(hex_hash(&capsule_hash));
        let input_version =
            u64::try_from(input_version).map_err(|_| StoreError::ToolCapsuleUnavailable {
                key: key.to_owned(),
                path: root.clone(),
                detail: "persisted ToolEpoch input version is negative",
            })?;
        Ok(Some(StagedTool {
            key: key.to_owned(),
            root,
            capsule,
            capsule_hash,
            input_version: InputVersion(input_version),
        }))
    }

    /// Whether the unique source-controlled lineage manifest has been
    /// validated and projected for this store instance.
    pub fn lineage_manifest_available(&self) -> Result<bool, StoreError> {
        manifest_available(&self.conn)
    }

    /// A type's append-only accepted epoch history, in manifest order.
    pub fn lineage(&self, type_uuid: TypeUuid) -> Result<Vec<LineageEntry>, StoreError> {
        lineage_rows(&self.conn, type_uuid)
    }

    /// The digest selected by the type's independent current cursor.
    pub fn lineage_current(&self, type_uuid: TypeUuid) -> Result<Option<LogicalHash>, StoreError> {
        Ok(type_lineage(&self.conn, type_uuid)?
            .map(|lineage| lineage.epochs[lineage.current as usize].digest))
    }

    /// The stamp for a value written at the accepted current cursor. It
    /// carries the full accepted vector, so a rollback cursor may select a
    /// non-final entry without erasing later history.
    pub fn current_lineage_stamp(
        &self,
        type_uuid: TypeUuid,
    ) -> Result<Option<LineageStamp>, StoreError> {
        type_lineage(&self.conn, type_uuid)?
            .map(|lineage| stamp_for(type_uuid, &lineage))
            .transpose()
    }

    /// Classify `data`'s placement against `registry_current` (§11's
    /// direction gate). The chain rules first; where the chain does not
    /// cover the hash (state loss), the entry's own `data_stamp` — the
    /// §6 lineage stamp riding beside its `schema_hash` — decides: a
    /// first-sight automatic diff is legal **only** when the stamp's
    /// explicit digest list is a strict prefix of the registry list; an
    /// unknown or unstamped schema, a divergent stamp, or a stamp whose
    /// list strictly extends the registry's never is.
    pub fn classify_lineage(
        &self,
        type_uuid: TypeUuid,
        data: LogicalHash,
        data_stamp: Option<LineageStamp>,
        registry_current: LogicalHash,
    ) -> Result<LineageClass, StoreError> {
        if !self.lineage_manifest_available()? {
            return Ok(LineageClass::HardStop(HardStopReason::MissingManifest));
        }
        let Some(lineage) = type_lineage(&self.conn, type_uuid)? else {
            return Ok(LineageClass::HardStop(HardStopReason::UnknownPosition));
        };
        let accepted_current = lineage.epochs[lineage.current as usize].digest;
        if registry_current != accepted_current {
            return Ok(LineageClass::HardStop(HardStopReason::UnknownPosition));
        }
        if data == registry_current {
            return Ok(LineageClass::AtCurrent);
        }
        let Some(stamp) = data_stamp else {
            return Ok(LineageClass::HardStop(HardStopReason::Unstamped));
        };
        let Some(data_position) =
            validate_stamp_against_manifest(type_uuid, data, &stamp, &lineage)
        else {
            return Ok(LineageClass::HardStop(HardStopReason::Divergent));
        };
        if is_ancestor(&lineage, data_position, lineage.current) {
            return Ok(LineageClass::ForwardOnChain);
        }
        if is_ancestor(&lineage, lineage.current, data_position) {
            return Ok(LineageClass::RegistryBehindData);
        }
        Ok(LineageClass::HardStop(HardStopReason::Divergent))
    }
}

fn validate_stamp_against_manifest(
    type_uuid: TypeUuid,
    data: LogicalHash,
    stamp: &LineageStamp,
    lineage: &AcceptedTypeLineage,
) -> Option<u32> {
    let cursor = usize::try_from(stamp.cursor).ok()?;
    if stamp.epochs.is_empty()
        || stamp.epochs.len() > lineage.epochs.len()
        || cursor >= stamp.epochs.len()
        || stamp.epochs != lineage.epochs[..stamp.epochs.len()]
        || stamp.selected_digest()? != data
        || stamp.chain != lineage_chain_digest(type_uuid, &stamp.epochs, stamp.cursor)
    {
        return None;
    }
    Some(stamp.cursor)
}
