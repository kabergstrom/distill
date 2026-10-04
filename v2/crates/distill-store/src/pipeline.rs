//! Pipeline-side metadata (§13): the version's pipeline failure (an `errors`
//! row) and the `tools` ToolEpoch table. Everything else about an epoch, its
//! module's content hash included, is the loaded module's.

use crate::atomic_file;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use distill_core::bootstrap::bootstrap_control_logical_registry_v1;
use distill_core::id::{LogicalHash, TypeUuid};
use distill_core::target_set::CanonicalTargetSet;
use distill_core::tool::{
    ToolCwdPolicy, ToolExecutionIdentityV2, ToolPackageFile, ToolSourceIdentityV2,
};
use rusqlite::OptionalExtension;
use unicode_normalization::is_nfc;

use crate::db::{InputTxn, StoreReader};
use crate::error::StoreError;
use crate::state::{InputVersion, PipelineEpoch, PipelineFailure};

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

fn hex_hash(hash: &[u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Empty the tool store's staging directory: the package trees an earlier
/// process staged for inputs that never committed. Called under the state
/// lock, before anything registers.
pub(crate) fn open_tool_staging(state_path: &std::path::Path) -> Result<(), StoreError> {
    let tools = state_path.join("tools");
    atomic_file::open_staging(&tools).map_err(|source| StoreError::Io {
        path: tools,
        source,
    })
}

fn atomic_error(error: atomic_file::AtomicWriteError) -> StoreError {
    match error {
        atomic_file::AtomicWriteError::Io { path, source } => StoreError::Io { path, source },
        atomic_file::AtomicWriteError::Conflict { path } => StoreError::Io {
            path,
            source: std::io::Error::other("conflict"),
        },
    }
}

/// A package tree a registration staged in the tool store's staging
/// directory. It is uncommitted until its input commits: the input
/// publishes it under `packages/<tool hash>` by one rename right before
/// its `COMMIT` ([`StagedPackage::publish`]), and an input that rolls back
/// drops it, which deletes the tree. A rolled-back registration therefore
/// leaves no file outside the staging directory, and nothing collects
/// packages.
#[derive(Debug)]
pub(crate) struct StagedPackage {
    key: String,
    files: Vec<ToolPackageFile>,
    dir: atomic_file::StagedDir,
}

impl StagedPackage {
    fn stage(
        key: &str,
        tools: &std::path::Path,
        root: &std::path::Path,
        files: &[ToolPackageFile],
        sources: &[ResolvedToolPackageFile],
    ) -> Result<Self, StoreError> {
        let dir = atomic_file::stage_dir(tools, root).map_err(atomic_error)?;
        for (metadata, source) in files.iter().zip(sources) {
            dir.write_member(
                std::path::Path::new(&metadata.path),
                &source.bytes,
                |file| set_staged_permissions(file, metadata.executable),
            )
            .map_err(atomic_error)?;
        }
        verify_package_root(key, dir.path(), files)?;
        Ok(Self {
            key: key.to_owned(),
            files: files.to_vec(),
            dir,
        })
    }

    pub(crate) fn target(&self) -> &std::path::Path {
        self.dir.target()
    }

    /// Publish the tree under its content-addressed root. Returns the root
    /// when this call created it (the caller removes it again if its
    /// commit fails); an existing root is verified instead.
    pub(crate) fn publish(self) -> Result<Option<PathBuf>, StoreError> {
        let root = self.dir.target().to_path_buf();
        if self.dir.publish_new().map_err(atomic_error)? {
            Ok(Some(root))
        } else {
            verify_package_root(&self.key, &root, &self.files)?;
            Ok(None)
        }
    }
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
    /// Publish a staged pipeline candidate (§3, §13) as the Ready epoch:
    /// the version carries no pipeline failure.
    pub fn publish_pipeline_epoch(
        &mut self,
        epoch: &ValidatedPipelineEpoch,
    ) -> Result<(), StoreError> {
        validate_target_set(&epoch.target_set)?;
        validate_bootstrap_schema_registry(&epoch.schema_registry)?;
        self.set_pipeline_failure(None)
    }

    /// A rejected candidate still publishes (§13): the version carries a
    /// pipeline failure naming the error.
    pub fn publish_pipeline_failure(
        &mut self,
        failure: &PipelineFailure,
    ) -> Result<(), StoreError> {
        failure
            .validate()
            .map_err(StoreError::InvalidPipelineFailure)?;
        self.set_pipeline_failure(Some(failure))
    }

    /// Publish a package snapshot or explicit ambient toolchain identity.
    pub fn register_tool(
        &mut self,
        key: &str,
        registration: ToolRegistrationV2,
    ) -> Result<RegisteredTool, StoreError> {
        self.stage_tool(key, registration, None)
    }

    /// Stage `key`'s tool and write its row at this version, unless
    /// `published` (the hash of the row it would replace) already names
    /// the same tool.
    fn stage_tool(
        &mut self,
        key: &str,
        registration: ToolRegistrationV2,
        published: Option<&[u8]>,
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
                let root = tools_dir.join("packages").join(hex_hash(&tool_hash));
                // A package is content-addressed: one already published, or
                // already staged by this input, is the same tree.
                if root.exists() {
                    verify_package_root(key, &root, files)?;
                } else if !self
                    .staged_packages
                    .iter()
                    .any(|staged| staged.target() == root)
                {
                    let staged = StagedPackage::stage(key, &tools_dir, &root, files, &sources)?;
                    self.staged_packages.push(staged);
                }
                Some(root)
            }
            ToolSourceIdentityV2::Ambient { launcher, .. } => {
                verify_ambient_launcher(key, std::path::Path::new(launcher))?;
                None
            }
        };
        if published == Some(tool_hash.as_slice()) {
            return Ok(RegisteredTool {
                key: key.to_owned(),
                root,
                identity,
                tool_hash,
                input_version: self.version(),
            });
        }
        self.txn
            .prepare_cached(
                "INSERT INTO tools(tool_key, present, identity_object, tool_hash, input_version)
             VALUES (?1, 1, ?2, ?3, ?4)
             ON CONFLICT(tool_key, input_version) DO UPDATE SET
               present = 1,
               identity_object = excluded.identity_object,
               tool_hash = excluded.tool_hash",
            )?
            .execute(rusqlite::params![
                key,
                identity_object,
                tool_hash.as_slice(),
                self.version().0 as i64,
            ])?;
        Ok(RegisteredTool {
            key: key.to_owned(),
            root,
            identity,
            tool_hash,
            input_version: self.version(),
        })
    }

    /// Publish one complete ToolEpoch projection: a row for each key whose
    /// tool changed or that `tools` drops. Keys absent from `tools`
    /// receive input-versioned tombstones so a removed registration cannot
    /// fall through to an older live registration at a newer snapshot.
    pub fn publish_tool_epoch(
        &mut self,
        tools: &BTreeMap<String, ToolRegistrationV2>,
    ) -> Result<Vec<RegisteredTool>, StoreError> {
        let mut previous = BTreeMap::<String, Vec<u8>>::new();
        {
            let mut statement = self.txn.prepare_cached(PUBLISHED_TOOLS)?;
            let rows = statement.query_map(
                [i64::try_from(self.base_stamp().version.0).unwrap_or(i64::MAX)],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, bool>(1)?,
                        row.get(2)?,
                    ))
                },
            )?;
            for row in rows {
                let (key, present, hash) = row?;
                if present {
                    previous.insert(key, hash);
                }
            }
        }

        let mut staged = Vec::with_capacity(tools.len());
        for (key, registration) in tools {
            let published = previous.get(key).map(Vec::as_slice);
            staged.push(self.stage_tool(key, registration.clone(), published)?);
        }
        for removed in previous.keys().filter(|key| !tools.contains_key(*key)) {
            self.txn
                .prepare_cached(
                    "INSERT INTO tools(tool_key, present, identity_object, tool_hash, input_version)
                 VALUES (?1, 0, X'', ?2, ?3)
                 ON CONFLICT(tool_key, input_version) DO UPDATE SET
                   present = 0,
                   identity_object = X'',
                   tool_hash = excluded.tool_hash",
                )?
                .execute(rusqlite::params![removed, [0_u8; 32].as_slice(), self.version().0 as i64,])?;
        }
        Ok(staged)
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

fn validate_target_set(target_set: &CanonicalTargetSet) -> Result<(), StoreError> {
    CanonicalTargetSet::from_canonical(target_set.rows.clone())
        .map(|_| ())
        .map_err(StoreError::InvalidTargetSet)
}

/// Each key's last ToolEpoch row at a version, `(key, present, hash)`:
/// one pass over the primary key (the row of a group's `MAX` supplies the
/// bare columns).
pub(crate) const PUBLISHED_TOOLS: &str = "SELECT tool_key, present, tool_hash, MAX(input_version)
     FROM tools WHERE input_version <= ?1 GROUP BY tool_key";

/// The last ToolEpoch row of one key at a pinned version (primary key).
pub(crate) const TOOL_AT: &str = "SELECT present, identity_object, tool_hash, input_version
     FROM tools WHERE tool_key = ?1 AND input_version <= ?2
     ORDER BY input_version DESC LIMIT 1";

impl StoreReader {
    /// Test hook: every tool's hash at `basis`, for a reference trace
    /// index that checks the builds' own reads.
    #[cfg(any(test, feature = "test-hooks"))]
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
        self.tool_at(key, self.input_version()?)
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
