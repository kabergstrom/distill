//! Canonical, hermetic tool-execution capsule identity (§9, §13).

use std::fmt;

use unicode_normalization::is_nfc;

use crate::canonical::{domain_digest, CanonicalEncoder, DSCT};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum ToolCapsuleFileRole {
    Launcher = 1,
    Interpreter = 2,
    NonSystemDso = 3,
    Plugin = 4,
    DeclaredResource = 5,
}

impl ToolCapsuleFileRole {
    fn decode(value: u8) -> Result<Self, ToolCapsuleError> {
        match value {
            1 => Ok(Self::Launcher),
            2 => Ok(Self::Interpreter),
            3 => Ok(Self::NonSystemDso),
            4 => Ok(Self::Plugin),
            5 => Ok(Self::DeclaredResource),
            value => Err(ToolCapsuleError::UnknownFileRole(value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCapsuleFile {
    pub path: String,
    pub role: ToolCapsuleFileRole,
    pub executable: bool,
    pub len: u64,
    pub bytes_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolCwdPolicy {
    EmptyScratch,
    ReadOnlyCapsuleRoot,
    ReadOnlyDeclaredSubdir(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolPlatformBinding {
    Pinned {
        platform_id: String,
        system_runtime_id: String,
    },
    ExplicitResidual {
        platform_id: String,
        system_runtime_class: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolLaunchMetadataV1 {
    pub argv0: String,
    pub interpreter_args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolExecutionCapsuleV1 {
    pub files: Vec<ToolCapsuleFile>,
    pub resolved_interpreter: Option<String>,
    pub launch: ToolLaunchMetadataV1,
    pub environment: Vec<(String, String)>,
    pub cwd_policy: ToolCwdPolicy,
    pub platform: ToolPlatformBinding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCapsuleError {
    UnsupportedVersion(u8),
    UnknownFileRole(u8),
    UnknownCwdPolicy(u8),
    UnknownPlatformBinding(u8),
    InvalidBool(u8),
    InvalidUtf8,
    NonCanonicalText,
    InvalidCapsulePath,
    FilesNotCanonical,
    LauncherMismatch,
    InterpreterMismatch,
    EnvironmentNotCanonical,
    InvalidEnvironment,
    InvalidCwdPolicy,
    InvalidPlatform,
    CountOverflow,
    Truncated,
    TrailingBytes,
}

impl fmt::Display for ToolCapsuleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ToolCapsuleError {}

impl ToolExecutionCapsuleV1 {
    const RECORD_VERSION: u8 = 1;

    pub fn validate(&self) -> Result<(), ToolCapsuleError> {
        if u32::try_from(self.files.len()).is_err()
            || u32::try_from(self.launch.interpreter_args.len()).is_err()
            || u32::try_from(self.environment.len()).is_err()
            || self
                .files
                .iter()
                .any(|file| u32::try_from(file.path.len()).is_err())
            || self
                .launch
                .interpreter_args
                .iter()
                .any(|value| u32::try_from(value.len()).is_err())
            || self.environment.iter().any(|(key, value)| {
                u32::try_from(key.len()).is_err() || u32::try_from(value.len()).is_err()
            })
        {
            return Err(ToolCapsuleError::CountOverflow);
        }
        if self.files.is_empty()
            || self
                .files
                .iter()
                .any(|file| !valid_capsule_path(&file.path))
        {
            return Err(ToolCapsuleError::InvalidCapsulePath);
        }
        if self.files.windows(2).any(|files| {
            (files[0].path.as_bytes(), files[0].role) >= (files[1].path.as_bytes(), files[1].role)
                || files[0].path == files[1].path
        }) {
            return Err(ToolCapsuleError::FilesNotCanonical);
        }

        let launchers = self
            .files
            .iter()
            .filter(|file| file.role == ToolCapsuleFileRole::Launcher)
            .collect::<Vec<_>>();
        if launchers.len() != 1
            || !valid_capsule_path(&self.launch.argv0)
            || launchers[0].path != self.launch.argv0
        {
            return Err(ToolCapsuleError::LauncherMismatch);
        }

        let interpreters = self
            .files
            .iter()
            .filter(|file| file.role == ToolCapsuleFileRole::Interpreter)
            .collect::<Vec<_>>();
        match &self.resolved_interpreter {
            None if interpreters.is_empty() => {}
            Some(path)
                if valid_capsule_path(path)
                    && interpreters.len() == 1
                    && interpreters[0].path == *path => {}
            _ => return Err(ToolCapsuleError::InterpreterMismatch),
        }
        if self
            .resolved_interpreter
            .as_ref()
            .is_some_and(|value| u32::try_from(value.len()).is_err())
            || u32::try_from(self.launch.argv0.len()).is_err()
        {
            return Err(ToolCapsuleError::CountOverflow);
        }
        if self
            .launch
            .interpreter_args
            .iter()
            .any(|value| !valid_text(value, true))
        {
            return Err(ToolCapsuleError::NonCanonicalText);
        }

        if self
            .environment
            .windows(2)
            .any(|rows| rows[0].0 >= rows[1].0)
        {
            return Err(ToolCapsuleError::EnvironmentNotCanonical);
        }
        if self.environment.iter().any(|(key, value)| {
            key.is_empty()
                || key.contains('=')
                || !valid_text(key, false)
                || !valid_text(value, true)
        }) {
            return Err(ToolCapsuleError::InvalidEnvironment);
        }

        if let ToolCwdPolicy::ReadOnlyDeclaredSubdir(path) = &self.cwd_policy {
            if u32::try_from(path.len()).is_err() {
                return Err(ToolCapsuleError::CountOverflow);
            }
            if !valid_capsule_path(path)
                || !self.files.iter().any(|file| {
                    file.path
                        .strip_prefix(path)
                        .is_some_and(|suffix| suffix.starts_with('/'))
                })
            {
                return Err(ToolCapsuleError::InvalidCwdPolicy);
            }
        }

        let platform_fields_fit = match &self.platform {
            ToolPlatformBinding::Pinned {
                platform_id,
                system_runtime_id,
            } => string_fits(platform_id) && string_fits(system_runtime_id),
            ToolPlatformBinding::ExplicitResidual {
                platform_id,
                system_runtime_class,
            } => string_fits(platform_id) && string_fits(system_runtime_class),
        };
        if !platform_fields_fit {
            return Err(ToolCapsuleError::CountOverflow);
        }
        match &self.platform {
            ToolPlatformBinding::Pinned {
                platform_id,
                system_runtime_id,
            } if valid_nonempty_text(platform_id) && valid_nonempty_text(system_runtime_id) => {}
            ToolPlatformBinding::ExplicitResidual {
                platform_id,
                system_runtime_class,
            } if valid_nonempty_text(platform_id)
                && valid_nonempty_text(system_runtime_class)
                && !matches!(system_runtime_class.as_str(), "current" | "generic") => {}
            _ => return Err(ToolCapsuleError::InvalidPlatform),
        }
        Ok(())
    }

    /// DSCT v1 over the canonical capsule object (the record version is the
    /// digest version byte and is not duplicated inside the object).
    pub fn digest(&self) -> Result<[u8; 32], ToolCapsuleError> {
        self.validate()?;
        Ok(domain_digest(DSCT, Self::RECORD_VERSION, |encoder| {
            self.encode_fields(encoder)
        }))
    }

    /// Persisted/interchange record: `version:u8 || canonical object`.
    pub fn encode_record(&self) -> Result<Vec<u8>, ToolCapsuleError> {
        self.validate()?;
        let mut bytes = vec![Self::RECORD_VERSION];
        let mut encoder = CanonicalEncoder::new();
        self.encode_fields(&mut encoder);
        bytes.extend_from_slice(&encoder.into_bytes());
        Ok(bytes)
    }

    pub fn decode_record(bytes: &[u8]) -> Result<Self, ToolCapsuleError> {
        let mut reader = RecordReader::new(bytes);
        let version = reader.u8()?;
        if version != Self::RECORD_VERSION {
            return Err(ToolCapsuleError::UnsupportedVersion(version));
        }
        let files = reader.seq(|reader| {
            Ok(ToolCapsuleFile {
                path: reader.string()?,
                role: ToolCapsuleFileRole::decode(reader.u8()?)?,
                executable: reader.boolean()?,
                len: reader.u64()?,
                bytes_hash: reader.array()?,
            })
        })?;
        let resolved_interpreter = reader.option(|reader| reader.string())?;
        let launch = ToolLaunchMetadataV1 {
            argv0: reader.string()?,
            interpreter_args: reader.seq(|reader| reader.string())?,
        };
        let environment = reader.seq(|reader| Ok((reader.string()?, reader.string()?)))?;
        let cwd_policy = match reader.u8()? {
            1 => ToolCwdPolicy::EmptyScratch,
            2 => ToolCwdPolicy::ReadOnlyCapsuleRoot,
            3 => ToolCwdPolicy::ReadOnlyDeclaredSubdir(reader.string()?),
            value => return Err(ToolCapsuleError::UnknownCwdPolicy(value)),
        };
        let platform = match reader.u8()? {
            1 => ToolPlatformBinding::Pinned {
                platform_id: reader.string()?,
                system_runtime_id: reader.string()?,
            },
            2 => ToolPlatformBinding::ExplicitResidual {
                platform_id: reader.string()?,
                system_runtime_class: reader.string()?,
            },
            value => return Err(ToolCapsuleError::UnknownPlatformBinding(value)),
        };
        if reader.remaining() != 0 {
            return Err(ToolCapsuleError::TrailingBytes);
        }
        let value = Self {
            files,
            resolved_interpreter,
            launch,
            environment,
            cwd_policy,
            platform,
        };
        value.validate()?;
        Ok(value)
    }

    fn encode_fields(&self, encoder: &mut CanonicalEncoder) {
        encoder.seq(&self.files, |encoder, file| {
            encoder.str(&file.path);
            encoder.enum_variant(file.role as u8);
            encoder.bool(file.executable);
            encoder.u64(file.len);
            encoder.raw(&file.bytes_hash);
        });
        encoder.option(self.resolved_interpreter.as_ref(), |encoder, path| {
            encoder.str(path)
        });
        encoder.str(&self.launch.argv0);
        encoder.seq(&self.launch.interpreter_args, |encoder, arg| {
            encoder.str(arg)
        });
        encoder.seq(&self.environment, |encoder, row| {
            encoder.str(&row.0);
            encoder.str(&row.1);
        });
        match &self.cwd_policy {
            ToolCwdPolicy::EmptyScratch => encoder.enum_variant(1),
            ToolCwdPolicy::ReadOnlyCapsuleRoot => encoder.enum_variant(2),
            ToolCwdPolicy::ReadOnlyDeclaredSubdir(path) => {
                encoder.enum_variant(3);
                encoder.str(path);
            }
        }
        match &self.platform {
            ToolPlatformBinding::Pinned {
                platform_id,
                system_runtime_id,
            } => {
                encoder.enum_variant(1);
                encoder.str(platform_id);
                encoder.str(system_runtime_id);
            }
            ToolPlatformBinding::ExplicitResidual {
                platform_id,
                system_runtime_class,
            } => {
                encoder.enum_variant(2);
                encoder.str(platform_id);
                encoder.str(system_runtime_class);
            }
        }
    }
}

fn valid_capsule_path(value: &str) -> bool {
    !value.is_empty()
        && valid_text(value, false)
        && !value.contains('\\')
        && value
            .split('/')
            .all(|component| !component.is_empty() && !matches!(component, "." | ".."))
}

fn valid_nonempty_text(value: &str) -> bool {
    !value.is_empty() && valid_text(value, false)
}

fn string_fits(value: &str) -> bool {
    u32::try_from(value.len()).is_ok()
}

fn valid_text(value: &str, allow_empty: bool) -> bool {
    (allow_empty || !value.is_empty()) && is_nfc(value) && !value.contains('\0')
}

struct RecordReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> RecordReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ToolCapsuleError> {
        let end = self
            .position
            .checked_add(N)
            .ok_or(ToolCapsuleError::Truncated)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(ToolCapsuleError::Truncated)?;
        self.position = end;
        Ok(bytes.try_into().expect("slice has requested length"))
    }

    fn u8(&mut self) -> Result<u8, ToolCapsuleError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, ToolCapsuleError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, ToolCapsuleError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn boolean(&mut self) -> Result<bool, ToolCapsuleError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(ToolCapsuleError::InvalidBool(value)),
        }
    }

    fn string(&mut self) -> Result<String, ToolCapsuleError> {
        let len = usize::try_from(self.u32()?).map_err(|_| ToolCapsuleError::CountOverflow)?;
        let end = self
            .position
            .checked_add(len)
            .ok_or(ToolCapsuleError::CountOverflow)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(ToolCapsuleError::Truncated)?;
        self.position = end;
        let value = std::str::from_utf8(bytes).map_err(|_| ToolCapsuleError::InvalidUtf8)?;
        if !is_nfc(value) {
            return Err(ToolCapsuleError::NonCanonicalText);
        }
        Ok(value.to_owned())
    }

    fn option<T>(
        &mut self,
        decode: impl FnOnce(&mut Self) -> Result<T, ToolCapsuleError>,
    ) -> Result<Option<T>, ToolCapsuleError> {
        match self.u8()? {
            0 => Ok(None),
            1 => decode(self).map(Some),
            value => Err(ToolCapsuleError::InvalidBool(value)),
        }
    }

    fn seq<T>(
        &mut self,
        mut decode: impl FnMut(&mut Self) -> Result<T, ToolCapsuleError>,
    ) -> Result<Vec<T>, ToolCapsuleError> {
        let count = usize::try_from(self.u32()?).map_err(|_| ToolCapsuleError::CountOverflow)?;
        if count > self.remaining() {
            return Err(ToolCapsuleError::Truncated);
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(decode(self)?);
        }
        Ok(values)
    }
}
