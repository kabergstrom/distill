//! Host-owned resolution of declarative pipeline tool registrations.
//!
//! Modules name source paths and policy. Only the daemon may turn those names
//! into a byte-complete `ToolExecutionCapsuleV1` registration.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Component, Path, PathBuf};

use distill_core::tool::{
    ToolCapsuleFile, ToolCapsuleFileRole, ToolExecutionCapsuleV1, ToolLaunchMetadataV1,
};
use distill_store::pipeline::{ResolvedToolCapsuleFile, ToolCapsuleRegistrationV1};
use object::read::elf::Dyn as _;
use object::read::macho::{FatArch, LoadCommandVariant, MachHeader};
use object::Object;

use crate::callbacks::{ToolDescriptor, ToolRegistration, ToolResourceDeclaration};
use crate::policy::is_system_runtime_library;

const LAUNCHER_PATH: &str = "bin/launcher";
const INTERPRETER_PATH: &str = "runtime/interpreter";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolResolutionError {
    detail: String,
}

impl ToolResolutionError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for ToolResolutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for ToolResolutionError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeFormat {
    Elf,
    MachO,
    Pe,
}

#[derive(Debug)]
struct NativeInfo {
    format: NativeFormat,
    libraries: Vec<String>,
    search_paths: Vec<String>,
}

#[derive(Debug, Clone)]
struct Member {
    source: PathBuf,
    path: String,
    role: ToolCapsuleFileRole,
    executable: bool,
    bytes: Vec<u8>,
}

pub(crate) fn resolve_tool_epoch(
    descriptors: Vec<ToolDescriptor>,
) -> Result<BTreeMap<String, ToolCapsuleRegistrationV1>, ToolResolutionError> {
    descriptors
        .into_iter()
        .map(|descriptor| {
            let key = descriptor.id;
            let registration = resolve_registration(descriptor.registration)
                .map_err(|error| ToolResolutionError::new(format!("tool `{key}`: {error}")))?;
            Ok((key, registration))
        })
        .collect()
}

fn resolve_registration(
    registration: ToolRegistration,
) -> Result<ToolCapsuleRegistrationV1, ToolResolutionError> {
    let mut members = BTreeMap::new();
    let launcher = read_member(
        &registration.launcher,
        LAUNCHER_PATH.to_owned(),
        ToolCapsuleFileRole::Launcher,
    )?;
    let launcher_source = launcher.source.clone();
    let shebang = parse_shebang(&launcher.bytes)?;
    if shebang.is_none() && !launcher.executable {
        return Err(ToolResolutionError::new(
            "native launcher is not executable",
        ));
    }
    insert_member(&mut members, launcher.clone())?;

    for declaration in &registration.declared_resources {
        insert_declaration(
            &mut members,
            declaration,
            ToolCapsuleFileRole::DeclaredResource,
        )?;
    }

    let mut native_queue = VecDeque::new();
    for declaration in &registration.plugins {
        let plugin = read_declaration(declaration, ToolCapsuleFileRole::Plugin)?;
        insert_member(&mut members, plugin.clone())?;
        native_queue.push_back(plugin);
    }

    let (resolved_interpreter, interpreter_args) = if let Some((interpreter, arguments)) = shebang {
        if interpreter.file_name().and_then(|name| name.to_str()) == Some("env") {
            return Err(ToolResolutionError::new(
                "shebang through `env` would reintroduce ambient PATH resolution",
            ));
        }
        if !interpreter.is_absolute() {
            return Err(ToolResolutionError::new(
                "shebang interpreter path is not absolute",
            ));
        }
        let interpreter = read_member(
            &interpreter,
            INTERPRETER_PATH.to_owned(),
            ToolCapsuleFileRole::Interpreter,
        )?;
        if !interpreter.executable {
            return Err(ToolResolutionError::new(
                "resolved shebang interpreter is not executable",
            ));
        }
        insert_member(&mut members, interpreter.clone())?;
        native_queue.push_back(interpreter);
        (Some(INTERPRETER_PATH.to_owned()), arguments)
    } else {
        native_queue.push_back(launcher);
        (None, Vec::new())
    };

    while let Some(owner) = native_queue.pop_front() {
        let info = inspect_native_image(&owner.bytes).map_err(|detail| {
            ToolResolutionError::new(format!(
                "native capsule member `{}` is invalid: {detail}",
                owner.path
            ))
        })?;
        for library in info.libraries {
            let system_runtime = is_system_runtime_library(&library);
            if system_runtime && Path::new(&library).is_absolute() {
                continue;
            }
            let Some((source, capsule_path)) = resolve_native_dependency(
                &library,
                &info.search_paths,
                info.format,
                &owner,
                &launcher_source,
                !system_runtime,
            )?
            else {
                // With a cleared environment and no capsule-relative shadow,
                // a recognized name is supplied by the sealed platform runtime.
                continue;
            };
            let dependency = read_member(&source, capsule_path, ToolCapsuleFileRole::NonSystemDso)?;
            let inserted = insert_member(&mut members, dependency.clone())?;
            if inserted {
                native_queue.push_back(dependency);
            }
        }
    }

    let files = members
        .into_values()
        .map(|member| ResolvedToolCapsuleFile {
            path: member.path,
            role: member.role,
            executable: member.executable,
            bytes: member.bytes,
        })
        .collect::<Vec<_>>();
    let launch = ToolLaunchMetadataV1 {
        argv0: LAUNCHER_PATH.to_owned(),
        interpreter_args,
    };
    let capsule = ToolExecutionCapsuleV1 {
        files: files
            .iter()
            .map(|file| ToolCapsuleFile {
                path: file.path.clone(),
                role: file.role,
                executable: file.executable,
                len: file.bytes.len() as u64,
                bytes_hash: *blake3::hash(&file.bytes).as_bytes(),
            })
            .collect(),
        resolved_interpreter: resolved_interpreter.clone(),
        launch: launch.clone(),
        environment: registration.environment.clone(),
        cwd_policy: registration.cwd_policy.clone(),
        platform: registration.platform.clone(),
    };
    capsule
        .validate()
        .map_err(|error| ToolResolutionError::new(format!("invalid sealed capsule: {error}")))?;
    Ok(ToolCapsuleRegistrationV1 {
        files,
        resolved_interpreter,
        launch,
        environment: registration.environment,
        cwd_policy: registration.cwd_policy,
        platform: registration.platform,
    })
}

fn insert_declaration(
    members: &mut BTreeMap<String, Member>,
    declaration: &ToolResourceDeclaration,
    role: ToolCapsuleFileRole,
) -> Result<(), ToolResolutionError> {
    insert_member(members, read_declaration(declaration, role)?)?;
    Ok(())
}

fn read_declaration(
    declaration: &ToolResourceDeclaration,
    role: ToolCapsuleFileRole,
) -> Result<Member, ToolResolutionError> {
    let path = normalize_capsule_path(Path::new(&declaration.capsule_path))?;
    read_member(&declaration.source, path, role)
}

fn read_member(
    source: &Path,
    path: String,
    role: ToolCapsuleFileRole,
) -> Result<Member, ToolResolutionError> {
    let source = std::fs::canonicalize(source).map_err(|error| {
        ToolResolutionError::new(format!("cannot resolve `{}`: {error}", source.display()))
    })?;
    let metadata = std::fs::symlink_metadata(&source).map_err(|error| {
        ToolResolutionError::new(format!("cannot inspect `{}`: {error}", source.display()))
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(ToolResolutionError::new(format!(
            "`{}` does not resolve to a regular file",
            source.display()
        )));
    }
    let bytes = std::fs::read(&source).map_err(|error| {
        ToolResolutionError::new(format!("cannot read `{}`: {error}", source.display()))
    })?;
    Ok(Member {
        source,
        path,
        role,
        executable: executable_mode(&metadata),
        bytes,
    })
}

fn insert_member(
    members: &mut BTreeMap<String, Member>,
    member: Member,
) -> Result<bool, ToolResolutionError> {
    if let Some(existing) = members.get(&member.path) {
        if existing.source == member.source
            && existing.role == member.role
            && existing.executable == member.executable
            && existing.bytes == member.bytes
        {
            return Ok(false);
        }
        return Err(ToolResolutionError::new(format!(
            "capsule path `{}` is declared by more than one closure member",
            member.path
        )));
    }
    members.insert(member.path.clone(), member);
    Ok(true)
}

fn parse_shebang(bytes: &[u8]) -> Result<Option<(PathBuf, Vec<String>)>, ToolResolutionError> {
    if !bytes.starts_with(b"#!") {
        return Ok(None);
    }
    let line = bytes[2..]
        .split(|byte| *byte == b'\n' || *byte == b'\r')
        .next()
        .unwrap_or_default();
    let line =
        std::str::from_utf8(line).map_err(|_| ToolResolutionError::new("shebang is not UTF-8"))?;
    let mut words = line.split_ascii_whitespace();
    let interpreter = words
        .next()
        .ok_or_else(|| ToolResolutionError::new("shebang has no interpreter"))?;
    if interpreter.contains('\0') {
        return Err(ToolResolutionError::new("shebang contains NUL"));
    }
    let arguments = words.map(str::to_owned).collect::<Vec<_>>();
    if arguments.iter().any(|argument| argument.contains('\0')) {
        return Err(ToolResolutionError::new("shebang argument contains NUL"));
    }
    Ok(Some((PathBuf::from(interpreter), arguments)))
}

fn inspect_native_image(bytes: &[u8]) -> Result<NativeInfo, String> {
    match object::FileKind::parse(bytes).map_err(|error| error.to_string())? {
        object::FileKind::MachOFat32 => {
            return inspect_fat_macho::<object::macho::FatArch32>(bytes)
        }
        object::FileKind::MachOFat64 => {
            return inspect_fat_macho::<object::macho::FatArch64>(bytes)
        }
        _ => {}
    }
    let image = object::File::parse(bytes).map_err(|error| error.to_string())?;
    match image {
        object::File::Elf32(image) => elf_native_info(&image),
        object::File::Elf64(image) => elf_native_info(&image),
        object::File::MachO32(image) => macho_native_info(&image),
        object::File::MachO64(image) => macho_native_info(&image),
        object::File::Pe32(image) => pe_native_info(&image),
        object::File::Pe64(image) => pe_native_info(&image),
        _ => Err("unsupported native executable format".to_owned()),
    }
}

fn inspect_fat_macho<Fat>(bytes: &[u8]) -> Result<NativeInfo, String>
where
    Fat: FatArch,
{
    let file = object::read::macho::MachOFatFile::<Fat>::parse(bytes)
        .map_err(|error| error.to_string())?;
    let architecture = host_architecture();
    let arch = file
        .arches()
        .iter()
        .find(|arch| arch.architecture() == architecture)
        .ok_or_else(|| format!("universal Mach-O has no {architecture:?} slice"))?;
    let slice = arch.data(bytes).map_err(|error| error.to_string())?;
    inspect_native_image(slice)
}

fn host_architecture() -> object::Architecture {
    match std::env::consts::ARCH {
        "aarch64" => object::Architecture::Aarch64,
        "arm" => object::Architecture::Arm,
        "x86" => object::Architecture::I386,
        "x86_64" => object::Architecture::X86_64,
        "powerpc" => object::Architecture::PowerPc,
        "powerpc64" => object::Architecture::PowerPc64,
        _ => object::Architecture::Unknown,
    }
}

fn elf_native_info<'data, Elf>(
    image: &object::read::elf::ElfFile<'data, Elf>,
) -> Result<NativeInfo, String>
where
    Elf: object::read::elf::FileHeader,
{
    let sections = image.elf_section_table();
    let Some((dynamic, strings_index)) = sections
        .dynamic(image.endian(), image.data())
        .map_err(|error| error.to_string())?
    else {
        return Ok(NativeInfo {
            format: NativeFormat::Elf,
            libraries: Vec::new(),
            search_paths: Vec::new(),
        });
    };
    let strings = sections
        .strings(image.endian(), image.data(), strings_index)
        .map_err(|error| error.to_string())?;
    let mut libraries = Vec::new();
    let mut search_paths = Vec::new();
    for entry in dynamic {
        match entry.tag32(image.endian()) {
            Some(object::elf::DT_NEEDED) => libraries.push(dynamic_string(entry, image, strings)?),
            Some(object::elf::DT_RPATH | object::elf::DT_RUNPATH) => {
                search_paths.extend(
                    dynamic_string(entry, image, strings)?
                        .split(':')
                        .map(str::to_owned),
                );
            }
            _ => {}
        }
    }
    canonicalize_strings(&mut libraries);
    canonicalize_strings(&mut search_paths);
    Ok(NativeInfo {
        format: NativeFormat::Elf,
        libraries,
        search_paths,
    })
}

fn dynamic_string<'data, Elf>(
    entry: &'data Elf::Dyn,
    image: &object::read::elf::ElfFile<'data, Elf>,
    strings: object::read::StringTable<'data>,
) -> Result<String, String>
where
    Elf: object::read::elf::FileHeader,
{
    let bytes = entry
        .string(image.endian(), strings)
        .map_err(|error| error.to_string())?;
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| "ELF dynamic path is not UTF-8".to_owned())
}

fn macho_native_info<'data, Mach>(
    image: &object::read::macho::MachOFile<'data, Mach>,
) -> Result<NativeInfo, String>
where
    Mach: MachHeader,
{
    let mut libraries = Vec::new();
    let mut search_paths = Vec::new();
    let mut commands = image
        .macho_load_commands()
        .map_err(|error| error.to_string())?;
    while let Some(command) = commands.next().map_err(|error| error.to_string())? {
        match command.variant().map_err(|error| error.to_string())? {
            LoadCommandVariant::Dylib(dylib) => libraries.push(
                std::str::from_utf8(
                    command
                        .string(image.endian(), dylib.dylib.name)
                        .map_err(|error| error.to_string())?,
                )
                .map_err(|_| "Mach-O dependency path is not UTF-8".to_owned())?
                .to_owned(),
            ),
            LoadCommandVariant::Rpath(rpath) => search_paths.push(
                std::str::from_utf8(
                    command
                        .string(image.endian(), rpath.path)
                        .map_err(|error| error.to_string())?,
                )
                .map_err(|_| "Mach-O rpath is not UTF-8".to_owned())?
                .to_owned(),
            ),
            _ => {}
        }
    }
    canonicalize_strings(&mut libraries);
    canonicalize_strings(&mut search_paths);
    Ok(NativeInfo {
        format: NativeFormat::MachO,
        libraries,
        search_paths,
    })
}

fn pe_native_info<'data>(image: &impl Object<'data>) -> Result<NativeInfo, String> {
    let mut libraries = image
        .imports()
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|import| {
            std::str::from_utf8(import.library())
                .map(str::to_owned)
                .map_err(|_| "PE dependency name is not UTF-8".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    canonicalize_strings(&mut libraries);
    Ok(NativeInfo {
        format: NativeFormat::Pe,
        libraries,
        search_paths: Vec::new(),
    })
}

fn canonicalize_strings(values: &mut Vec<String>) {
    values.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    values.dedup();
}

fn resolve_native_dependency(
    library: &str,
    search_paths: &[String],
    format: NativeFormat,
    owner: &Member,
    launcher_source: &Path,
    required: bool,
) -> Result<Option<(PathBuf, String)>, ToolResolutionError> {
    if library.is_empty() || library.contains('\0') {
        return Err(ToolResolutionError::new(
            "native dependency name is empty or contains NUL",
        ));
    }
    let owner_source_dir = owner
        .source
        .parent()
        .ok_or_else(|| ToolResolutionError::new("native member has no source directory"))?;
    let owner_capsule_dir = Path::new(&owner.path)
        .parent()
        .unwrap_or_else(|| Path::new(""));
    let launcher_source_dir = launcher_source
        .parent()
        .ok_or_else(|| ToolResolutionError::new("launcher has no source directory"))?;
    let launcher_capsule_dir = Path::new(LAUNCHER_PATH)
        .parent()
        .expect("launcher capsule path has a parent");

    let mut candidates = Vec::new();
    if let Some(relative) = library.strip_prefix("@loader_path/") {
        candidates.push((
            owner_source_dir.join(relative),
            owner_capsule_dir.join(relative),
        ));
    } else if let Some(relative) = library.strip_prefix("@executable_path/") {
        candidates.push((
            launcher_source_dir.join(relative),
            launcher_capsule_dir.join(relative),
        ));
    } else if let Some(relative) = library.strip_prefix("@rpath/") {
        for search in search_paths {
            if let Some((source_base, capsule_base)) = expand_search_path(
                search,
                owner_source_dir,
                owner_capsule_dir,
                launcher_source_dir,
                launcher_capsule_dir,
            )? {
                candidates.push((source_base.join(relative), capsule_base.join(relative)));
            }
        }
    } else if Path::new(library).is_absolute() {
        return Err(ToolResolutionError::new(format!(
            "non-system dependency `{library}` uses an absolute loader path"
        )));
    } else if format == NativeFormat::Elf {
        for search in search_paths {
            if let Some((source_base, capsule_base)) = expand_search_path(
                search,
                owner_source_dir,
                owner_capsule_dir,
                launcher_source_dir,
                launcher_capsule_dir,
            )? {
                candidates.push((source_base.join(library), capsule_base.join(library)));
            }
        }
    } else if format == NativeFormat::Pe {
        candidates.push((
            owner_source_dir.join(library),
            owner_capsule_dir.join(library),
        ));
    } else {
        return Err(ToolResolutionError::new(format!(
            "non-system Mach-O dependency `{library}` has no closed loader-relative path"
        )));
    }

    let mut resolved = BTreeSet::new();
    for (source, capsule) in candidates {
        let Ok(source) = std::fs::canonicalize(&source) else {
            continue;
        };
        if !source.is_file() {
            continue;
        }
        let capsule = normalize_capsule_path(&capsule)?;
        resolved.insert((source, capsule));
    }
    if resolved.len() > 1 || (required && resolved.is_empty()) {
        return Err(ToolResolutionError::new(format!(
            "non-system dependency `{library}` resolved to {} files (expected exactly one)",
            resolved.len()
        )));
    }
    Ok(resolved.into_iter().next())
}

fn expand_search_path(
    search: &str,
    owner_source_dir: &Path,
    owner_capsule_dir: &Path,
    launcher_source_dir: &Path,
    launcher_capsule_dir: &Path,
) -> Result<Option<(PathBuf, PathBuf)>, ToolResolutionError> {
    if search.is_empty() {
        return Err(ToolResolutionError::new(
            "native search path contains an ambient empty entry",
        ));
    }
    for prefix in ["$ORIGIN", "${ORIGIN}", "@loader_path"] {
        if search == prefix {
            return Ok(Some((
                owner_source_dir.to_path_buf(),
                owner_capsule_dir.to_path_buf(),
            )));
        }
        if let Some(relative) = search.strip_prefix(&format!("{prefix}/")) {
            return Ok(Some((
                owner_source_dir.join(relative),
                owner_capsule_dir.join(relative),
            )));
        }
    }
    if search == "@executable_path" {
        return Ok(Some((
            launcher_source_dir.to_path_buf(),
            launcher_capsule_dir.to_path_buf(),
        )));
    }
    if let Some(relative) = search.strip_prefix("@executable_path/") {
        return Ok(Some((
            launcher_source_dir.join(relative),
            launcher_capsule_dir.join(relative),
        )));
    }
    if Path::new(search).is_absolute() {
        if is_system_runtime_directory(search) {
            return Ok(None);
        }
        return Err(ToolResolutionError::new(format!(
            "native search path `{search}` is ambient rather than capsule-relative"
        )));
    }
    Err(ToolResolutionError::new(format!(
        "native search path `{search}` is not rooted at the loader or executable"
    )))
}

fn is_system_runtime_directory(path: &str) -> bool {
    [
        "/lib",
        "/lib32",
        "/lib64",
        "/usr/lib",
        "/usr/lib32",
        "/usr/lib64",
        "/System/Library/Frameworks",
        "/System/Library/PrivateFrameworks",
    ]
    .iter()
    .any(|root| path == *root || path.starts_with(&format!("{root}/")))
}

fn normalize_capsule_path(path: &Path) -> Result<String, ToolResolutionError> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| {
                    ToolResolutionError::new("capsule path component is not UTF-8")
                })?;
                if part.is_empty() || part.contains('\0') || part.contains('\\') {
                    return Err(ToolResolutionError::new(
                        "capsule path contains an invalid component",
                    ));
                }
                parts.push(part.to_owned());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if parts.pop().is_none() {
                    return Err(ToolResolutionError::new("capsule path escapes its root"));
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(ToolResolutionError::new("capsule path is absolute"));
            }
        }
    }
    if parts.is_empty() {
        return Err(ToolResolutionError::new("capsule path is empty"));
    }
    Ok(parts.join("/"))
}

#[cfg(unix)]
fn executable_mode(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn executable_mode(_metadata: &std::fs::Metadata) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use distill_core::tool::{ToolCwdPolicy, ToolPlatformBinding};
    use std::io::Write;

    fn registration(launcher: PathBuf) -> ToolRegistration {
        ToolRegistration {
            launcher,
            declared_resources: Vec::new(),
            plugins: Vec::new(),
            environment: Vec::new(),
            cwd_policy: ToolCwdPolicy::EmptyScratch,
            platform: ToolPlatformBinding::Pinned {
                platform_id: "test-platform".to_owned(),
                system_runtime_id: "test-runtime".to_owned(),
            },
        }
    }

    #[test]
    fn daemon_resolves_shebang_and_declared_resource_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let launcher = directory.path().join("tool");
        std::fs::write(&launcher, b"#!/bin/sh -e\nprintf ok\n").unwrap();
        let resource = directory.path().join("rules.txt");
        std::fs::write(&resource, b"rules-v1").unwrap();
        let mut input = registration(launcher);
        input.declared_resources.push(ToolResourceDeclaration {
            source: resource,
            capsule_path: "share/rules.txt".to_owned(),
        });

        let resolved = resolve_registration(input).unwrap();

        assert_eq!(
            resolved.resolved_interpreter.as_deref(),
            Some(INTERPRETER_PATH)
        );
        assert_eq!(resolved.launch.interpreter_args, ["-e"]);
        assert!(resolved.files.iter().any(|file| {
            file.path == "share/rules.txt"
                && file.role == ToolCapsuleFileRole::DeclaredResource
                && file.bytes == b"rules-v1"
        }));
    }

    #[test]
    fn ambient_env_shebang_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let launcher = directory.path().join("tool");
        let mut file = std::fs::File::create(&launcher).unwrap();
        file.write_all(b"#!/usr/bin/env sh\n").unwrap();

        let error = resolve_registration(registration(launcher)).unwrap_err();

        assert!(error.to_string().contains("ambient PATH"));
    }

    #[test]
    fn capsule_path_collisions_are_rejected_before_publication() {
        let directory = tempfile::tempdir().unwrap();
        let launcher = directory.path().join("tool");
        std::fs::write(&launcher, b"#!/bin/sh\n").unwrap();
        let first = directory.path().join("first");
        let second = directory.path().join("second");
        std::fs::write(&first, b"one").unwrap();
        std::fs::write(&second, b"two").unwrap();
        let mut input = registration(launcher);
        input.declared_resources = vec![
            ToolResourceDeclaration {
                source: first,
                capsule_path: "share/data".to_owned(),
            },
            ToolResourceDeclaration {
                source: second,
                capsule_path: "share/data".to_owned(),
            },
        ];

        assert!(resolve_registration(input).is_err());
    }

    #[test]
    fn system_soname_is_staged_when_origin_search_can_shadow_it() {
        let directory = tempfile::tempdir().unwrap();
        let launcher = directory.path().join("tool");
        let shadow = directory.path().join("libc.so.6");
        std::fs::write(&launcher, b"native-placeholder").unwrap();
        std::fs::write(&shadow, b"shadow-bytes").unwrap();
        let owner = read_member(
            &launcher,
            LAUNCHER_PATH.to_owned(),
            ToolCapsuleFileRole::Launcher,
        )
        .unwrap();

        let resolved = resolve_native_dependency(
            "libc.so.6",
            &["$ORIGIN".to_owned()],
            NativeFormat::Elf,
            &owner,
            &launcher,
            false,
        )
        .unwrap()
        .unwrap();

        assert_eq!(resolved.0, std::fs::canonicalize(shadow).unwrap());
        assert_eq!(resolved.1, "bin/libc.so.6");
    }
}
