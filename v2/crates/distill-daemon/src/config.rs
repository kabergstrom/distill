//! Complete §18 daemon configuration parsing and staging validation.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};

use distill_build::keys::target_definition_hash;
use distill_build::pipeline::{GraphicsApi, Target, TargetArch, TargetOs};
use distill_core::attestation::CompiledTypeTable;
use distill_rpc::{LoadPolicyEntry, TargetDefinition, TargetDefinitionHash};
use distill_schema::ngp_schema::CompilationIdentity;
use distill_store::config::{parse_byte_size, ConfigValidationError};
use distill_store::state::{ConfigurationPathKey, DscpV1, OwnedPathKind, OwnedPathSide};
use distill_store::StoreConfig;
use serde::Deserialize;
use unicode_normalization::{is_nfc, UnicodeNormalization};

use crate::coordinator::LineageDestination;
use crate::epoch::{CandidateRequirements, MeasuredLayout, TargetDefinition as PipelineTarget};
use crate::module_loader::host_module_identity;
use crate::scanner::AssetRoot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfig {
    pub source_path: PathBuf,
    pub daemon: DaemonSection,
    pub assets: AssetsSection,
    pub modules: ModulesSection,
    pub targets: BTreeMap<String, TargetSection>,
    pub codegen: CodegenSection,
    pub pipeline: PipelineSection,
    pub cas: CasSection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonSection {
    pub address: SocketAddr,
    pub state_path: PathBuf,
    pub displaced_retention_days: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetsSection {
    pub roots: BTreeMap<String, PathBuf>,
    pub schema_path: PathBuf,
    pub lineage_manifest: LineageDestination,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModulesSection {
    pub pipeline_dylib: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetSection {
    pub os: TargetOs,
    pub arch: TargetArch,
    pub apis: BTreeSet<GraphicsApi>,
    pub optimize: bool,
    pub debug_info: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodegenSection {
    pub rs_mod_path: PathBuf,
    pub auto_codegen: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineSection {
    pub parallelism: usize,
    pub max_dependency_depth: usize,
    pub batch_reserved_workers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasSection {
    pub segment_size: u64,
    pub cache_limit: u64,
}

#[derive(Debug)]
pub enum DaemonConfigError {
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    Toml(String),
    InvalidAddress(String),
    NonLoopbackAddress(SocketAddr),
    InvalidName {
        kind: &'static str,
        name: String,
    },
    DuplicateNormalizedName {
        kind: &'static str,
        name: String,
    },
    RootUnavailable(PathBuf),
    UnknownLineageRoot(String),
    InvalidLineagePath(String),
    EmptyTargets,
    EmptyTargetApis(String),
    InvalidTargetApi {
        target: String,
        api: String,
    },
    Scheduler(ConfigValidationError),
    InvalidByteSize(String),
    PathOverlap {
        asset_root: PathBuf,
        controlled_path: PathBuf,
        role: &'static str,
    },
    Target(String),
}

impl std::fmt::Display for DaemonConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "daemon configuration: {self:?}")
    }
}

impl std::error::Error for DaemonConfigError {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    daemon: RawDaemon,
    assets: RawAssets,
    modules: RawModules,
    targets: BTreeMap<String, RawTarget>,
    codegen: RawCodegen,
    pipeline: RawPipeline,
    cas: RawCas,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDaemon {
    address: String,
    state_path: PathBuf,
    #[serde(default = "default_retention")]
    displaced_retention_days: u32,
}

const fn default_retention() -> u32 {
    7
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAssets {
    roots: BTreeMap<String, PathBuf>,
    schema_path: PathBuf,
    lineage_manifest: RawLineageDestination,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLineageDestination {
    root: String,
    path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawModules {
    pipeline_dylib: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTarget {
    os: RawTargetOs,
    arch: RawTargetArch,
    apis: BTreeSet<String>,
    #[serde(default)]
    optimize: bool,
    #[serde(default)]
    debug_info: bool,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RawTargetOs {
    Linux,
    Macos,
    Windows,
}

#[derive(Debug, Clone, Copy, Deserialize)]
enum RawTargetArch {
    #[serde(rename = "aarch64")]
    Aarch64,
    #[serde(rename = "x86_64")]
    X86_64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCodegen {
    rs_mod_path: PathBuf,
    auto_codegen: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPipeline {
    parallelism: usize,
    max_dependency_depth: usize,
    batch_reserved_workers: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCas {
    segment_size: String,
    cache_limit: String,
}

impl DaemonConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, DaemonConfigError> {
        let path = path.as_ref();
        let source = std::fs::read_to_string(path).map_err(|source| DaemonConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(path, &source)
    }

    pub fn parse(source_path: impl AsRef<Path>, source: &str) -> Result<Self, DaemonConfigError> {
        let source_path = absolutize(source_path.as_ref())?;
        let base = source_path.parent().unwrap_or_else(|| Path::new("/"));
        let raw: RawConfig =
            toml::from_str(source).map_err(|error| DaemonConfigError::Toml(error.to_string()))?;
        let address = raw
            .daemon
            .address
            .parse::<SocketAddr>()
            .map_err(|_| DaemonConfigError::InvalidAddress(raw.daemon.address.clone()))?;
        if !address.ip().is_loopback() {
            return Err(DaemonConfigError::NonLoopbackAddress(address));
        }

        let roots = normalize_paths("asset root", raw.assets.roots, base, true)?;
        if !roots.contains_key(&raw.assets.lineage_manifest.root) {
            return Err(DaemonConfigError::UnknownLineageRoot(
                raw.assets.lineage_manifest.root,
            ));
        }
        let lineage_path = distill_build::query::normalize_path(&raw.assets.lineage_manifest.path)
            .map_err(|_| {
                DaemonConfigError::InvalidLineagePath(raw.assets.lineage_manifest.path.clone())
            })?;
        let state_path = resolve(base, &raw.daemon.state_path)?;
        let schema_path = resolve(base, &raw.assets.schema_path)?;
        let pipeline_dylib = resolve(base, &raw.modules.pipeline_dylib)?;
        let rs_mod_path = resolve(base, &raw.codegen.rs_mod_path)?;

        for root in roots.values() {
            for (role, controlled) in [
                ("daemon.state_path", &state_path),
                ("assets.schema_path", &schema_path),
                ("modules.pipeline_dylib", &pipeline_dylib),
                ("codegen.rs_mod_path", &rs_mod_path),
            ] {
                if nested_either_way(root, controlled) {
                    return Err(DaemonConfigError::PathOverlap {
                        asset_root: root.clone(),
                        controlled_path: controlled.clone(),
                        role,
                    });
                }
            }
        }

        let scheduler = StoreConfig {
            state_path: state_path.clone(),
            displaced_retention_days: raw.daemon.displaced_retention_days,
            segment_size: 1,
            cache_limit: 1,
            parallelism: raw.pipeline.parallelism,
            batch_reserved_workers: raw.pipeline.batch_reserved_workers,
        };
        scheduler
            .validate_scheduler()
            .map_err(DaemonConfigError::Scheduler)?;
        if raw.pipeline.max_dependency_depth == 0 {
            return Err(DaemonConfigError::Target(
                "pipeline.max_dependency_depth must be at least 1".to_owned(),
            ));
        }

        let targets = normalize_targets(raw.targets)?;
        let segment_size = parse_byte_size(&raw.cas.segment_size)
            .map_err(|error| DaemonConfigError::InvalidByteSize(error.to_string()))?;
        let cache_limit = parse_byte_size(&raw.cas.cache_limit)
            .map_err(|error| DaemonConfigError::InvalidByteSize(error.to_string()))?;

        Ok(Self {
            source_path,
            daemon: DaemonSection {
                address,
                state_path,
                displaced_retention_days: raw.daemon.displaced_retention_days,
            },
            assets: AssetsSection {
                roots,
                schema_path,
                lineage_manifest: LineageDestination {
                    root: raw.assets.lineage_manifest.root,
                    path: lineage_path,
                },
            },
            modules: ModulesSection { pipeline_dylib },
            targets,
            codegen: CodegenSection {
                rs_mod_path,
                auto_codegen: raw.codegen.auto_codegen,
            },
            pipeline: PipelineSection {
                parallelism: raw.pipeline.parallelism,
                max_dependency_depth: raw.pipeline.max_dependency_depth,
                batch_reserved_workers: raw.pipeline.batch_reserved_workers,
            },
            cas: CasSection {
                segment_size,
                cache_limit,
            },
        })
    }

    pub fn store_config(&self) -> StoreConfig {
        StoreConfig {
            state_path: self.daemon.state_path.clone(),
            displaced_retention_days: self.daemon.displaced_retention_days,
            segment_size: self.cas.segment_size,
            cache_limit: self.cas.cache_limit,
            parallelism: self.pipeline.parallelism,
            batch_reserved_workers: self.pipeline.batch_reserved_workers,
        }
    }

    pub fn asset_roots(&self) -> Vec<AssetRoot> {
        self.assets
            .roots
            .iter()
            .map(|(name, path)| AssetRoot::new(name, path, path.join(".distill-displaced")))
            .collect()
    }

    /// Host-identity target binding. Cross-target layout emission remains the
    /// explicitly named §22 gate; it cannot substitute a caller hash here.
    pub fn build_targets(
        &self,
        identity: &CompilationIdentity,
    ) -> Result<BTreeMap<String, Target>, DaemonConfigError> {
        self.targets
            .iter()
            .map(|(name, target)| {
                if !target_matches_compilation_identity(target, identity) {
                    return Err(DaemonConfigError::Target(format!(
                        "{name}: configured {:?}/{:?} does not match schema compilation target `{}`; cross-target layouts are not emitted yet",
                        target.os, target.arch, identity.target_triple
                    )));
                }
                Target::new(
                    target.os,
                    target.arch,
                    target.apis.clone(),
                    target.optimize,
                    target.debug_info,
                    identity.clone(),
                )
                .map(|target| (name.clone(), target))
                .map_err(|error| DaemonConfigError::Target(format!("{name}: {error:?}")))
            })
            .collect()
    }

    /// Host-identity target binding. Cross-target layout emission remains the
    /// explicitly named §22 gate; it cannot substitute a caller hash here.
    pub fn target_definitions(
        &self,
        compiled: &CompiledTypeTable,
        identity: &CompilationIdentity,
    ) -> Result<Vec<TargetDefinition>, DaemonConfigError> {
        self.build_targets(identity)?
            .iter()
            .map(|(name, target)| {
                let definition_hash = TargetDefinitionHash(target_definition_hash(target, &[]));
                let policy = compiled
                    .rows
                    .iter()
                    .map(|row| LoadPolicyEntry {
                        type_uuid: row.type_uuid,
                        build_only: row.build_only,
                    })
                    .collect();
                TargetDefinition::canonical(name, definition_hash, compiled.rows.clone(), policy)
                    .map_err(|error| DaemonConfigError::Target(format!("{name}: {error}")))
            })
            .collect()
    }

    pub fn candidate_requirements(
        &self,
        compiled: &CompiledTypeTable,
        identity: &CompilationIdentity,
    ) -> Result<CandidateRequirements, DaemonConfigError> {
        let rpc_targets = self.target_definitions(compiled, identity)?;
        let targets = rpc_targets
            .iter()
            .map(|target| PipelineTarget {
                name: target.name().to_owned(),
                fingerprint: target.definition_hash().0,
            })
            .collect();
        let measured_layouts = compiled
            .rows
            .iter()
            .map(|row| MeasuredLayout {
                type_id: type_uuid_hex(row.type_uuid.0),
                digest: row.native_layout_digest,
            })
            .collect();
        Ok(CandidateRequirements {
            identity: host_module_identity(identity.clone()),
            measured_layouts,
            compiled_types: compiled.clone(),
            targets,
        })
    }
}

fn target_matches_compilation_identity(
    target: &TargetSection,
    identity: &CompilationIdentity,
) -> bool {
    let mut components = identity.target_triple.split('-');
    let Some(arch) = components.next() else {
        return false;
    };
    let components = components.collect::<Vec<_>>();
    let arch_matches = matches!(
        (target.arch, arch),
        (TargetArch::Aarch64, "aarch64") | (TargetArch::X86_64, "x86_64")
    );
    let os_matches = match target.os {
        TargetOs::Linux => components.contains(&"linux"),
        TargetOs::MacOs => components.last() == Some(&"darwin"),
        TargetOs::Windows => components.contains(&"windows"),
    };
    arch_matches && os_matches
}

/// Convert a rejected source candidate into the stable DSCP fact carried by
/// the poisoned version. Errors whose grammar has no more specific DSCP row
/// bind to the exact source bytes through `MalformedConfiguration`.
pub(crate) fn config_error_reason(error: &DaemonConfigError, source: &[u8]) -> DscpV1 {
    let malformed = || DscpV1::MalformedConfiguration {
        file_hash: *blake3::hash(source).as_bytes(),
    };
    let path_text = |path: &Path| path.to_str().filter(|text| is_nfc(text)).map(str::to_owned);
    match error {
        DaemonConfigError::NonLoopbackAddress(address) => DscpV1::NonLoopbackAddress {
            address: address.to_string(),
        },
        DaemonConfigError::DuplicateNormalizedName { kind, name } if *kind == "asset root" => {
            DscpV1::DuplicateRootName {
                normalized_name: name.clone(),
            }
        }
        DaemonConfigError::DuplicateNormalizedName { kind, name } if *kind == "target" => {
            DscpV1::DuplicateTargetName {
                normalized_name: name.clone(),
            }
        }
        DaemonConfigError::RootUnavailable(path) => {
            path_text(path).map_or_else(malformed, |path| DscpV1::InvalidPath {
                key: ConfigurationPathKey::AssetRoot,
                normalized_or_raw_path: path,
            })
        }
        DaemonConfigError::InvalidLineagePath(path) => {
            let normalized = path.nfc().collect::<String>();
            DscpV1::InvalidPath {
                key: ConfigurationPathKey::ImportDestination,
                normalized_or_raw_path: normalized,
            }
        }
        DaemonConfigError::UnknownLineageRoot(root) => DscpV1::InvalidPath {
            key: ConfigurationPathKey::ImportDestination,
            normalized_or_raw_path: root.clone(),
        },
        DaemonConfigError::EmptyTargetApis(target) => DscpV1::EmptyTargetApis {
            target: target.clone(),
        },
        DaemonConfigError::Scheduler(ConfigValidationError::ParallelismZero) => {
            DscpV1::InvalidParallelism { value: 0 }
        }
        DaemonConfigError::Scheduler(ConfigValidationError::BatchReservationOutOfBounds {
            got,
            ..
        }) => match (u32::try_from(*got), scheduler_values(source)) {
            (Ok(reservation), Some((parallelism, _))) => DscpV1::InvalidBatchReservation {
                parallelism,
                reservation,
            },
            _ => malformed(),
        },
        DaemonConfigError::PathOverlap {
            asset_root,
            controlled_path,
            role,
        } => match (path_text(asset_root), path_text(controlled_path)) {
            (Some(asset_root), Some(controlled_path)) => DscpV1::OwnedPathOverlap {
                first: OwnedPathSide {
                    kind: OwnedPathKind::AssetRoot,
                    path: asset_root,
                },
                second: OwnedPathSide {
                    kind: owned_path_kind(role),
                    path: controlled_path,
                },
            },
            _ => malformed(),
        },
        _ => malformed(),
    }
}

fn scheduler_values(source: &[u8]) -> Option<(u32, u32)> {
    let source = std::str::from_utf8(source).ok()?;
    let value: toml::Value = toml::from_str(source).ok()?;
    let pipeline = value.get("pipeline")?;
    Some((
        u32::try_from(pipeline.get("parallelism")?.as_integer()?).ok()?,
        u32::try_from(pipeline.get("batch_reserved_workers")?.as_integer()?).ok()?,
    ))
}

fn owned_path_kind(role: &str) -> OwnedPathKind {
    match role {
        "daemon.state_path" => OwnedPathKind::DaemonState,
        "assets.schema_path" => OwnedPathKind::SchemaArtifact,
        "modules.pipeline_dylib" => OwnedPathKind::PipelineModule,
        "codegen.rs_mod_path" => OwnedPathKind::CodegenOutput,
        _ => OwnedPathKind::ImportDestination,
    }
}

fn type_uuid_hex(bytes: [u8; 16]) -> String {
    let mut result = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut result, "{byte:02x}").expect("writing to String is infallible");
    }
    result
}

fn normalize_targets(
    raw: BTreeMap<String, RawTarget>,
) -> Result<BTreeMap<String, TargetSection>, DaemonConfigError> {
    if raw.is_empty() {
        return Err(DaemonConfigError::EmptyTargets);
    }
    let mut targets = BTreeMap::new();
    for (raw_name, raw) in raw {
        validate_name("target", &raw_name)?;
        let name = raw_name.nfc().collect::<String>();
        if raw.apis.is_empty() {
            return Err(DaemonConfigError::EmptyTargetApis(name));
        }
        let mut apis = BTreeSet::new();
        for api in raw.apis {
            let parsed =
                GraphicsApi::new(&api).map_err(|_| DaemonConfigError::InvalidTargetApi {
                    target: name.clone(),
                    api,
                })?;
            apis.insert(parsed);
        }
        if targets
            .insert(
                name.clone(),
                TargetSection {
                    os: match raw.os {
                        RawTargetOs::Linux => TargetOs::Linux,
                        RawTargetOs::Macos => TargetOs::MacOs,
                        RawTargetOs::Windows => TargetOs::Windows,
                    },
                    arch: match raw.arch {
                        RawTargetArch::Aarch64 => TargetArch::Aarch64,
                        RawTargetArch::X86_64 => TargetArch::X86_64,
                    },
                    apis,
                    optimize: raw.optimize,
                    debug_info: raw.debug_info,
                },
            )
            .is_some()
        {
            return Err(DaemonConfigError::DuplicateNormalizedName {
                kind: "target",
                name,
            });
        }
    }
    Ok(targets)
}

fn normalize_paths(
    kind: &'static str,
    raw: BTreeMap<String, PathBuf>,
    base: &Path,
    require_directory: bool,
) -> Result<BTreeMap<String, PathBuf>, DaemonConfigError> {
    let mut paths = BTreeMap::new();
    for (raw_name, path) in raw {
        validate_name(kind, &raw_name)?;
        let name = raw_name.nfc().collect::<String>();
        let path = resolve(base, &path)?;
        if require_directory && !path.is_dir() {
            return Err(DaemonConfigError::RootUnavailable(path));
        }
        if paths.insert(name.clone(), path).is_some() {
            return Err(DaemonConfigError::DuplicateNormalizedName { kind, name });
        }
    }
    Ok(paths)
}

fn validate_name(kind: &'static str, name: &str) -> Result<(), DaemonConfigError> {
    if name.is_empty()
        || !is_nfc(name)
        || name.contains(['/', '\\', '\0'])
        || matches!(name, "." | "..")
    {
        return Err(DaemonConfigError::InvalidName {
            kind,
            name: name.to_owned(),
        });
    }
    Ok(())
}

fn resolve(base: &Path, path: &Path) -> Result<PathBuf, DaemonConfigError> {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    };
    let mut ancestor = joined.as_path();
    let mut suffix = Vec::new();
    while !ancestor.exists() {
        let Some(name) = ancestor.file_name() else {
            return normalize_absolute(&joined);
        };
        suffix.push(name.to_owned());
        let Some(parent) = ancestor.parent() else {
            return normalize_absolute(&joined);
        };
        ancestor = parent;
    }
    let mut resolved =
        std::fs::canonicalize(ancestor).map_err(|source| DaemonConfigError::Read {
            path: ancestor.to_path_buf(),
            source,
        })?;
    for component in suffix.into_iter().rev() {
        resolved.push(component);
    }
    normalize_absolute(&resolved)
}

fn absolutize(path: &Path) -> Result<PathBuf, DaemonConfigError> {
    if path.is_absolute() {
        normalize_absolute(path)
    } else {
        let cwd = std::env::current_dir().map_err(|source| DaemonConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        normalize_absolute(&cwd.join(path))
    }
}

fn normalize_absolute(path: &Path) -> Result<PathBuf, DaemonConfigError> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(component) => normalized.push(component),
        }
    }
    if normalized.is_absolute() {
        Ok(normalized)
    } else {
        Err(DaemonConfigError::Target(format!(
            "path {} did not resolve absolutely",
            path.display()
        )))
    }
}

fn nested_either_way(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}
