//! Pipeline-side metadata (§13): the `pipeline_state` row and
//! `registrations`, the compiled schema registry, and the `tools`
//! ToolEpoch table.

use std::collections::{BTreeMap, BTreeSet};
use crate::atomic_file;
use std::path::PathBuf;

use distill_core::bootstrap::bootstrap_control_logical_registry_v1;
use distill_core::id::{LogicalHash, TypeUuid};
use distill_core::target_set::{CanonicalTargetSet, TargetSetRow};
use distill_core::tool::{
    ToolCwdPolicy, ToolExecutionIdentityV2, ToolPackageFile, ToolSourceIdentityV2,
};
use rusqlite::OptionalExtension;
use unicode_normalization::is_nfc;

use crate::db::{InputTxn, Store, StoreReader};
use crate::error::StoreError;
use crate::state::{
    InputVersion, PipelineEpoch, PipelineFailure, PipelineState, Registration, RegistrationKind,
};

/// One package member supplied at the registration boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedToolPackageFile {
    pub path: String,
    pub executable: bool,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedToolSourceV2 {
    Package {
        launcher: String,
        files: Vec<ResolvedToolPackageFile>,
    },
    Ambient {
        launcher: String,
        toolchain_id: String,
        trusted_fingerprint: Option<[u8; 32]>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolRegistrationV2 {
    pub source: ResolvedToolSourceV2,
    pub environment: Vec<(String, String)>,
    pub cwd_policy: ToolCwdPolicy,
}

/// One published ToolEpoch mapping. Package roots are immutable staged trees;
/// ambient registrations retain only their explicit executable path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredTool {
    pub key: String,
    pub root: Option<PathBuf>,
    pub identity: ToolExecutionIdentityV2,
    pub tool_hash: [u8; 32],
    pub input_version: InputVersion,
}

impl RegisteredTool {
    pub fn is_cacheable(&self) -> bool {
        self.identity.is_cacheable()
    }

    /// Reopen the staged package or ambient executable immediately before
    /// process creation. Failure is transient and never memoized.
    pub fn revalidate(&self) -> Result<(), StoreError> {
        match (&self.identity.source, &self.root) {
            (ToolSourceIdentityV2::Package { files, .. }, Some(root)) => {
                verify_package_root(&self.key, root, files)
            }
            (ToolSourceIdentityV2::Ambient { launcher, .. }, None) => {
                verify_ambient_launcher(&self.key, std::path::Path::new(launcher))
            }
            _ => Err(StoreError::ToolUnavailable {
                key: self.key.clone(),
                path: self
                    .root
                    .clone()
                    .unwrap_or_else(|| PathBuf::from("<ambient>")),
                detail: "persisted tool source and staged root disagree",
            }),
        }
    }
}

impl ToolRegistrationV2 {
    fn seal(self) -> Result<(ToolExecutionIdentityV2, Vec<ResolvedToolPackageFile>), StoreError> {
        let (source, files) = match self.source {
            ResolvedToolSourceV2::Package { launcher, files } => {
                let rows = files
                    .iter()
                    .map(|file| ToolPackageFile {
                        path: file.path.clone(),
                        executable: file.executable,
                        len: file.bytes.len() as u64,
                        bytes_hash: *blake3::hash(&file.bytes).as_bytes(),
                    })
                    .collect();
                (
                    ToolSourceIdentityV2::Package {
                        launcher,
                        files: rows,
                    },
                    files,
                )
            }
            ResolvedToolSourceV2::Ambient {
                launcher,
                toolchain_id,
                trusted_fingerprint,
            } => (
                ToolSourceIdentityV2::Ambient {
                    launcher,
                    toolchain_id,
                    trusted_fingerprint,
                },
                Vec::new(),
            ),
        };
        let identity = ToolExecutionIdentityV2 {
            source,
            environment: self.environment,
            cwd_policy: self.cwd_policy,
        };
        identity
            .validate()
            .map_err(StoreError::InvalidToolIdentity)?;
        Ok((identity, files))
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

/// Empty the tool object store's staging directory: the temps an earlier
/// process left there never committed. Called under the state lock.
pub(crate) fn open_tool_staging(state_path: &std::path::Path) -> Result<(), StoreError> {
    let objects = state_path.join("tools/objects");
    atomic_file::open_staging(&objects).map_err(|source| StoreError::Io {
        path: objects,
        source,
    })
}

fn stage_immutable_file(
    key: &str,
    objects: &std::path::Path,
    path: &std::path::Path,
    bytes: &[u8],
    metadata: &ToolPackageFile,
) -> Result<(), StoreError> {
    if !path.exists() {
        atomic_file::stage_with(objects, path, bytes, |file| {
            set_staged_permissions(file, metadata.executable)
        })
        .and_then(atomic_file::Staged::commit_new)
        .map_err(|error| match error {
            atomic_file::AtomicWriteError::Io { path, source } => StoreError::Io { path, source },
            conflict => StoreError::Io {
                path: path.to_path_buf(),
                source: std::io::Error::other(conflict.to_string()),
            },
        })?;
    }
    verify_package_file(key, path, metadata)
}

#[cfg(unix)]
fn set_staged_permissions(file: &std::fs::File, executable: bool) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = if executable { 0o555 } else { 0o444 };
    file.set_permissions(std::fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
fn set_staged_permissions(file: &std::fs::File, _executable: bool) -> std::io::Result<()> {
    let mut permissions = file.metadata()?.permissions();
    permissions.set_readonly(true);
    file.set_permissions(permissions)
}

fn verify_package_root(
    key: &str,
    root: &std::path::Path,
    expected_files: &[ToolPackageFile],
) -> Result<(), StoreError> {
    let root_metadata =
        std::fs::symlink_metadata(root).map_err(|_| StoreError::ToolUnavailable {
            key: key.to_owned(),
            path: root.to_path_buf(),
            detail: "staged package root is unavailable",
        })?;
    if !root_metadata.is_dir() || root_metadata.file_type().is_symlink() {
        return Err(StoreError::ToolUnavailable {
            key: key.to_owned(),
            path: root.to_path_buf(),
            detail: "staged package root is not a real directory",
        });
    }
    let expected = expected_files
        .iter()
        .map(|file| file.path.clone())
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    collect_package_files(key, root, root, &mut actual)?;
    if actual != expected {
        return Err(StoreError::ToolUnavailable {
            key: key.to_owned(),
            path: root.to_path_buf(),
            detail: "staged package file set differs from its identity",
        });
    }
    for file in expected_files {
        verify_package_file(key, &root.join(&file.path), file)?;
    }
    Ok(())
}

fn collect_package_files(
    key: &str,
    root: &std::path::Path,
    directory: &std::path::Path,
    files: &mut BTreeSet<String>,
) -> Result<(), StoreError> {
    let entries = std::fs::read_dir(directory).map_err(|_| StoreError::ToolUnavailable {
        key: key.to_owned(),
        path: directory.to_path_buf(),
        detail: "staged package directory is unreadable",
    })?;
    for entry in entries {
        let entry = entry.map_err(|_| StoreError::ToolUnavailable {
            key: key.to_owned(),
            path: directory.to_path_buf(),
            detail: "staged package directory entry is unreadable",
        })?;
        let path = entry.path();
        let metadata =
            std::fs::symlink_metadata(&path).map_err(|_| StoreError::ToolUnavailable {
                key: key.to_owned(),
                path: path.clone(),
                detail: "staged package member metadata is unavailable",
            })?;
        if metadata.file_type().is_symlink() {
            return Err(StoreError::ToolUnavailable {
                key: key.to_owned(),
                path,
                detail: "staged package contains a symlink",
            });
        }
        if metadata.is_dir() {
            collect_package_files(key, root, &path, files)?;
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| StoreError::ToolUnavailable {
                    key: key.to_owned(),
                    path: path.clone(),
                    detail: "staged package member escaped its root",
                })?;
            let relative = relative
                .components()
                .map(|component| component.as_os_str().to_str())
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| StoreError::ToolUnavailable {
                    key: key.to_owned(),
                    path: path.clone(),
                    detail: "staged package member path is not UTF-8",
                })?
                .join("/");
            files.insert(relative);
        } else {
            return Err(StoreError::ToolUnavailable {
                key: key.to_owned(),
                path,
                detail: "staged package contains a non-file member",
            });
        }
    }
    Ok(())
}

fn verify_package_file(
    key: &str,
    path: &std::path::Path,
    expected: &ToolPackageFile,
) -> Result<(), StoreError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| StoreError::ToolUnavailable {
        key: key.to_owned(),
        path: path.to_path_buf(),
        detail: "staged package member is unavailable",
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(StoreError::ToolUnavailable {
            key: key.to_owned(),
            path: path.to_path_buf(),
            detail: "staged package member is not a regular file",
        });
    }
    if metadata.len() != expected.len {
        return Err(StoreError::ToolUnavailable {
            key: key.to_owned(),
            path: path.to_path_buf(),
            detail: "staged package member length changed",
        });
    }
    let bytes = std::fs::read(path).map_err(|_| StoreError::ToolUnavailable {
        key: key.to_owned(),
        path: path.to_path_buf(),
        detail: "staged package member is unreadable",
    })?;
    if blake3::hash(&bytes).as_bytes() != &expected.bytes_hash {
        return Err(StoreError::ToolUnavailable {
            key: key.to_owned(),
            path: path.to_path_buf(),
            detail: "staged package member hash changed",
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
        return Err(StoreError::ToolUnavailable {
            key: key.to_owned(),
            path: path.to_path_buf(),
            detail: "staged package executable mode changed",
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

fn verify_ambient_launcher(key: &str, path: &std::path::Path) -> Result<(), StoreError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| StoreError::ToolUnavailable {
        key: key.to_owned(),
        path: path.to_path_buf(),
        detail: "ambient executable is unavailable",
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(StoreError::ToolUnavailable {
            key: key.to_owned(),
            path: path.to_path_buf(),
            detail: "ambient executable is not a regular file",
        });
    }
    verify_executable_mode(key, path, &metadata, true)
}

/// Sealed publication input whose logical registry and exact target rows have
/// been validated before publication. There is no raw publication path.
#[derive(Debug, Clone)]
pub struct ValidatedPipelineEpoch {
    epoch: PipelineEpoch,
}

impl ValidatedPipelineEpoch {
    pub fn validate(epoch: PipelineEpoch) -> Result<Self, StoreError> {
        validate_target_set(&epoch.target_set)?;
        validate_bootstrap_schema_registry(&epoch.schema_registry)?;
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

impl InputTxn<'_> {
    /// Publish a staged pipeline candidate (§3, §13) as the Ready epoch.
    pub fn publish_pipeline_epoch(
        &mut self,
        epoch: &ValidatedPipelineEpoch,
    ) -> Result<(), StoreError> {
        validate_target_set(&epoch.target_set)?;
        validate_bootstrap_schema_registry(&epoch.schema_registry)?;
        self.txn.execute(
            "INSERT INTO pipeline_state(
                 id, dylib_hash, input_version,
                 poison_code, poison_origin, poison_cleanup, poison_identity, poison_message
             ) VALUES (0, ?1, ?2, NULL, NULL, NULL, NULL, NULL)
             ON CONFLICT(id) DO UPDATE SET
               dylib_hash = excluded.dylib_hash,
               input_version = excluded.input_version,
               poison_code = NULL,
               poison_origin = NULL,
               poison_cleanup = NULL,
               poison_identity = NULL,
               poison_message = NULL",
            rusqlite::params![epoch.dylib_hash.as_slice(), self.version().0 as i64,],
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
        replace_schema_registry(&self.txn, &epoch.schema_registry)?;
        replace_target_set(&self.txn, &epoch.target_set)?;
        Ok(())
    }

    /// A rejected candidate still publishes (§13): the version carries a
    /// pipeline failure naming the error. The prior epoch's identity
    /// columns are retained as `last_good` residency bookkeeping — never
    /// served as this version's code.
    pub fn publish_pipeline_failure(
        &mut self,
        failure: &PipelineFailure,
    ) -> Result<(), StoreError> {
        failure
            .validate()
            .map_err(StoreError::InvalidPipelineFailure)?;
        self.txn.execute(
            "INSERT INTO pipeline_state(
                 id, dylib_hash, input_version,
                 poison_code, poison_origin, poison_cleanup, poison_identity, poison_message
             ) VALUES (0, NULL, ?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(id) DO UPDATE SET
               input_version = excluded.input_version,
               poison_code = excluded.poison_code,
               poison_origin = excluded.poison_origin,
               poison_cleanup = excluded.poison_cleanup,
               poison_identity = excluded.poison_identity,
               poison_message = excluded.poison_message",
            rusqlite::params![
                self.version().0 as i64,
                failure.code as u16,
                failure.origin as u16,
                failure.cleanup as u16,
                failure.identity.as_slice(),
                failure.message,
            ],
        )?;
        Ok(())
    }

    /// Publish a package snapshot or explicit ambient toolchain identity.
    pub fn register_tool(
        &mut self,
        key: &str,
        registration: ToolRegistrationV2,
    ) -> Result<RegisteredTool, StoreError> {
        if key.is_empty() || !is_nfc(key) || key.contains('\0') {
            return Err(StoreError::InvalidToolKey);
        }
        let (identity, sources) = registration.seal()?;
        let tool_hash = identity.digest().map_err(StoreError::InvalidToolIdentity)?;
        let identity_object = identity
            .encode_record()
            .map_err(StoreError::InvalidToolIdentity)?;
        let root = match &identity.source {
            ToolSourceIdentityV2::Package { files, .. } => {
                let tools_dir = self.state_path.join("tools");
                let objects_dir = tools_dir.join("objects");
                let root = tools_dir.join("packages").join(hex_hash(&tool_hash));
                create_dir_all(&objects_dir)?;
                create_dir_all(&root)?;
                for (metadata, source) in files.iter().zip(&sources) {
                    let mode = if metadata.executable { "x" } else { "n" };
                    let object =
                        objects_dir.join(format!("{}-{mode}", hex_hash(&metadata.bytes_hash)));
                    stage_immutable_file(key, &objects_dir, &object, &source.bytes, metadata)?;
                    let member = root.join(&metadata.path);
                    if let Some(parent) = member.parent() {
                        create_dir_all(parent)?;
                    }
                    match std::fs::hard_link(&object, &member) {
                        Ok(()) => {
                            if let Some(parent) = member.parent() {
                                crate::cas::store::fsync_dir(parent)?;
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
                verify_package_root(key, &root, files)?;
                Some(root)
            }
            ToolSourceIdentityV2::Ambient { launcher, .. } => {
                verify_ambient_launcher(key, std::path::Path::new(launcher))?;
                None
            }
        };
        self.txn.execute(
            "INSERT INTO tools(tool_key, present, identity_object, tool_hash, input_version)
             VALUES (?1, 1, ?2, ?3, ?4)
             ON CONFLICT(tool_key, input_version) DO UPDATE SET
               present = 1,
               identity_object = excluded.identity_object,
               tool_hash = excluded.tool_hash",
            rusqlite::params![
                key,
                identity_object,
                tool_hash.as_slice(),
                self.version().0 as i64,
            ],
        )?;
        Ok(RegisteredTool {
            key: key.to_owned(),
            root,
            identity,
            tool_hash,
            input_version: self.version(),
        })
    }

    /// Publish one complete ToolEpoch projection. Keys absent from `tools`
    /// receive input-versioned tombstones so a removed registration cannot
    /// fall through to an older live registration at a newer snapshot.
    pub fn publish_tool_epoch(
        &mut self,
        tools: &BTreeMap<String, ToolRegistrationV2>,
    ) -> Result<Vec<RegisteredTool>, StoreError> {
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
            staged.push(self.register_tool(key, registration.clone())?);
        }
        let current = tools.keys().cloned().collect::<BTreeSet<_>>();
        for removed in previous.difference(&current) {
            self.txn.execute(
                "INSERT INTO tools(tool_key, present, identity_object, tool_hash, input_version)
                 VALUES (?1, 0, X'', ?2, ?3)
                 ON CONFLICT(tool_key, input_version) DO UPDATE SET
                   present = 0,
                   identity_object = X'',
                   tool_hash = excluded.tool_hash",
                rusqlite::params![removed, [0_u8; 32].as_slice(), self.version().0 as i64,],
            )?;
        }
        Ok(staged)
    }
}

fn invalid_state(detail: &str) -> StoreError {
    StoreError::InvalidPipelineState {
        detail: detail.to_owned(),
    }
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

fn replace_schema_registry(
    conn: &rusqlite::Connection,
    registry: &BTreeMap<TypeUuid, LogicalHash>,
) -> Result<(), StoreError> {
    conn.execute("DELETE FROM pipeline_schema_registry", [])?;
    for (type_uuid, logical_hash) in registry {
        conn.execute(
            "INSERT INTO pipeline_schema_registry(type_uuid, logical_hash) VALUES (?1, ?2)",
            rusqlite::params![type_uuid.0.as_slice(), logical_hash.0.as_slice()],
        )?;
    }
    Ok(())
}

fn load_schema_registry(
    conn: &rusqlite::Connection,
) -> Result<BTreeMap<TypeUuid, LogicalHash>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT type_uuid, logical_hash FROM pipeline_schema_registry ORDER BY type_uuid",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    let mut registry = BTreeMap::new();
    for row in rows {
        let (type_uuid, logical_hash) = row?;
        let type_uuid = TypeUuid(
            type_uuid
                .try_into()
                .map_err(|_| invalid_state("a pipeline registry UUID is not exactly 16 bytes"))?,
        );
        let logical_hash = LogicalHash(logical_hash.try_into().map_err(|_| {
            invalid_state("a pipeline registry logical hash is not exactly 32 bytes")
        })?);
        registry.insert(type_uuid, logical_hash);
    }
    Ok(registry)
}

fn validate_target_set(target_set: &CanonicalTargetSet) -> Result<(), StoreError> {
    CanonicalTargetSet::from_canonical(target_set.rows.clone())
        .map(|_| ())
        .map_err(StoreError::InvalidTargetSet)
}

fn replace_target_set(
    conn: &rusqlite::Connection,
    target_set: &CanonicalTargetSet,
) -> Result<(), StoreError> {
    validate_target_set(target_set)?;
    conn.execute("DELETE FROM pipeline_target_set", [])?;
    for row in &target_set.rows {
        conn.execute(
            "INSERT INTO pipeline_target_set(name, target_definition_hash) VALUES (?1, ?2)",
            rusqlite::params![row.name, row.target_definition_hash.as_slice()],
        )?;
    }
    Ok(())
}

fn load_target_set(conn: &rusqlite::Connection) -> Result<CanonicalTargetSet, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT name, target_definition_hash FROM pipeline_target_set ORDER BY CAST(name AS BLOB)",
    )?;
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
    CanonicalTargetSet::from_canonical(target_rows).map_err(StoreError::InvalidTargetSet)
}

fn exact_blob32(bytes: Vec<u8>, name: &str) -> Result<[u8; 32], StoreError> {
    bytes
        .try_into()
        .map_err(|_| invalid_state(&format!("{name} is not exactly 32 bytes")))
}

impl Store {
    /// Persist the first failure discovered in an already-published module
    /// epoch without minting a new input version. This is a narrow monotonic
    /// runtime-lifecycle transition, guarded by the exact dylib identity.
    pub fn fail_published_pipeline_epoch(
        &mut self,
        expected_dylib_hash: [u8; 32],
        failure: &PipelineFailure,
    ) -> Result<(), StoreError> {
        self.write_txn(|store| {
            failure
                .validate()
                .map_err(StoreError::InvalidPipelineFailure)?;
            if failure.origin != crate::state::PipelineFailureOrigin::PublishedRuntime {
                return Err(StoreError::InvalidPipelineFailure(
                    crate::state::PipelineFailureDecodeError::InvalidMatrix,
                ));
            }

            let transaction = store.read.conn.savepoint()?;
            let row: Option<(Option<Vec<u8>>, Option<i64>)> = transaction
                .query_row(
                    "SELECT dylib_hash, poison_code FROM pipeline_state WHERE id = 0",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let (actual, already_unavailable) = match row {
                Some((actual, failure_code)) => (
                    actual
                        .map(|bytes| exact_blob32(bytes, "published pipeline dylib hash"))
                        .transpose()?,
                    failure_code.is_some(),
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
                    failure.code as u16,
                    failure.origin as u16,
                    failure.cleanup as u16,
                    failure.identity.as_slice(),
                    failure.message,
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
        })
    }
}

/// The last ToolEpoch row of one key at a pinned version (primary key).
pub(crate) const TOOL_AT: &str = "SELECT present, identity_object, tool_hash, input_version
     FROM tools WHERE tool_key = ?1 AND input_version <= ?2
     ORDER BY input_version DESC LIMIT 1";

impl StoreReader {
    /// The published pipeline state, or `None` before any publication.
    pub fn pipeline_state(&self) -> Result<Option<PipelineState>, StoreError> {
        type StateRow = (
            Option<Vec<u8>>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<Vec<u8>>,
            Option<String>,
        );
        let row: Option<StateRow> = self
            .conn
            .query_row(
                "SELECT dylib_hash, poison_code, poison_origin, poison_cleanup,
                        poison_identity, poison_message
                 FROM pipeline_state WHERE id = 0",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
            )
            .optional()?;
        let Some((
            dylib,
            failure_code,
            failure_origin,
            failure_cleanup,
            failure_identity,
            failure_message,
        )) = row
        else {
            return Ok(None);
        };

        let epoch = match dylib {
            Some(dylib) => {
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
                    target_set: load_target_set(&self.conn)?,
                    schema_registry: load_schema_registry(&self.conn)?,
                    registrations,
                }))
            }
            None => None,
        };

        let failure = match (
            failure_code,
            failure_origin,
            failure_cleanup,
            failure_identity,
            failure_message,
        ) {
            (None, None, None, None, None) => None,
            (Some(code), Some(origin), Some(cleanup), Some(identity), Some(message)) => Some(
                PipelineFailure::from_wire(
                    u16::try_from(code).map_err(|_| {
                        StoreError::InvalidPipelineFailure(
                            crate::state::PipelineFailureDecodeError::UnknownCode(code as u16),
                        )
                    })?,
                    u16::try_from(origin).map_err(|_| {
                        StoreError::InvalidPipelineFailure(
                            crate::state::PipelineFailureDecodeError::UnknownOrigin(origin as u16),
                        )
                    })?,
                    u16::try_from(cleanup).map_err(|_| {
                        StoreError::InvalidPipelineFailure(
                            crate::state::PipelineFailureDecodeError::UnknownCleanup(
                                cleanup as u16,
                            ),
                        )
                    })?,
                    exact_blob32(identity, "pipeline-failure identity")?,
                    message,
                )
                .map_err(StoreError::InvalidPipelineFailure)?,
            ),
            _ => {
                return Err(invalid_state("pipeline failure columns are incomplete"));
            }
        };

        Ok(Some(match failure {
            None => match epoch {
                Some(epoch) => PipelineState::Ready(epoch),
                // A row with no identity and no failure cannot be published
                // through this API.
                None => return Ok(None),
            },
            Some(error) => PipelineState::Failed {
                error,
                last_good: epoch,
            },
        }))
    }

    pub fn tool_hashes_at(
        &self,
        basis: InputVersion,
    ) -> Result<BTreeMap<String, [u8; 32]>, StoreError> {
        let mut statement = self.conn.prepare(
            "SELECT candidate.tool_key, candidate.tool_hash
               FROM tools AS candidate
              WHERE candidate.input_version = (
                    SELECT MAX(prior.input_version)
                      FROM tools AS prior
                     WHERE prior.tool_key = candidate.tool_key
                       AND prior.input_version <= ?1)
                AND candidate.present = 1
              ORDER BY candidate.tool_key",
        )?;
        let rows = statement.query_map([i64::try_from(basis.0).unwrap_or(i64::MAX)], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;
        rows.map(|row| {
            let (key, hash) = row?;
            let hash = hash.try_into().map_err(|_| {
                StoreError::InvalidToolIdentity(distill_core::tool::ToolIdentityError::Truncated)
            })?;
            Ok((key, hash))
        })
        .collect()
    }

    /// Resolve a tool key through the ToolEpoch table (§13).
    pub fn tool(&self, key: &str) -> Result<Option<RegisteredTool>, StoreError> {
        self.tool_at(key, self.input_version())
    }

    /// Resolve the last ToolEpoch mapping visible at an exact pinned input
    /// version. Historical package roots coexist, so an older job never
    /// launches a replacement registration. One primary-key read
    /// ([`TOOL_AT`]); a build reads it per tool use.
    pub fn tool_at(
        &self,
        key: &str,
        basis: InputVersion,
    ) -> Result<Option<RegisteredTool>, StoreError> {
        let row = self
            .conn
            .prepare_cached(TOOL_AT)?
            .query_row(
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
        let identity = ToolExecutionIdentityV2::decode_record(&record)
            .map_err(StoreError::InvalidToolIdentity)?;
        let tool_hash: [u8; 32] = hash.try_into().map_err(|_| {
            StoreError::InvalidToolIdentity(distill_core::tool::ToolIdentityError::Truncated)
        })?;
        if identity.digest().map_err(StoreError::InvalidToolIdentity)? != tool_hash {
            return Err(StoreError::ToolUnavailable {
                key: key.to_owned(),
                path: self.config.state_path.join("tools"),
                detail: "persisted tool hash does not match its identity object",
            });
        }
        let root = matches!(&identity.source, ToolSourceIdentityV2::Package { .. }).then(|| {
            self.config
                .state_path
                .join("tools/packages")
                .join(hex_hash(&tool_hash))
        });
        let input_version =
            u64::try_from(input_version).map_err(|_| StoreError::ToolUnavailable {
                key: key.to_owned(),
                path: root
                    .clone()
                    .unwrap_or_else(|| self.config.state_path.join("tools")),
                detail: "persisted ToolEpoch input version is negative",
            })?;
        Ok(Some(RegisteredTool {
            key: key.to_owned(),
            root,
            identity,
            tool_hash,
            input_version: InputVersion(input_version),
        }))
    }
}
