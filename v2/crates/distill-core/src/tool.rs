//! Canonical tool-execution identity (§9, §13).

use std::fmt;
use std::path::Path;

use unicode_normalization::is_nfc;

use crate::canonical::{domain_digest, CanonicalEncoder, DSCT};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolPackageFile {
    pub path: String,
    pub executable: bool,
    pub len: u64,
    pub bytes_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolSourceIdentityV2 {
    Package {
        launcher: String,
        files: Vec<ToolPackageFile>,
    },
    Ambient {
        launcher: String,
        toolchain_id: String,
        trusted_fingerprint: Option<[u8; 32]>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolCwdPolicy {
    EmptyScratch,
    ReadOnlyPackageRoot,
    ReadOnlyPackageSubdir(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolExecutionIdentityV2 {
    pub source: ToolSourceIdentityV2,
    pub environment: Vec<(String, String)>,
    pub cwd_policy: ToolCwdPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolIdentityError {
    UnsupportedVersion(u8),
    UnknownSource(u8),
    UnknownCwdPolicy(u8),
    InvalidBool(u8),
    InvalidUtf8,
    NonCanonicalText,
    InvalidPackagePath,
    FilesNotCanonical,
    LauncherMismatch,
    InvalidAmbientLauncher,
    InvalidToolchainId,
    EnvironmentNotCanonical,
    InvalidEnvironment,
    InvalidCwdPolicy,
    CountOverflow,
    Truncated,
    TrailingBytes,
}

impl fmt::Display for ToolIdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ToolIdentityError {}

impl ToolExecutionIdentityV2 {
    pub const RECORD_VERSION: u8 = 2;

    pub fn validate(&self) -> Result<(), ToolIdentityError> {
        if u32::try_from(self.environment.len()).is_err()
            || self.environment.iter().any(|(key, value)| {
                u32::try_from(key.len()).is_err() || u32::try_from(value.len()).is_err()
            })
        {
            return Err(ToolIdentityError::CountOverflow);
        }
        if self
            .environment
            .windows(2)
            .any(|rows| rows[0].0 >= rows[1].0)
        {
            return Err(ToolIdentityError::EnvironmentNotCanonical);
        }
        if self.environment.iter().any(|(key, value)| {
            key.is_empty()
                || key.contains('=')
                || !valid_text(key, false)
                || !valid_text(value, true)
        }) {
            return Err(ToolIdentityError::InvalidEnvironment);
        }

        match &self.source {
            ToolSourceIdentityV2::Package { launcher, files } => {
                if files.is_empty()
                    || u32::try_from(files.len()).is_err()
                    || u32::try_from(launcher.len()).is_err()
                    || files.iter().any(|file| {
                        u32::try_from(file.path.len()).is_err() || !valid_package_path(&file.path)
                    })
                {
                    return Err(ToolIdentityError::InvalidPackagePath);
                }
                if files
                    .windows(2)
                    .any(|rows| rows[0].path.as_bytes() >= rows[1].path.as_bytes())
                {
                    return Err(ToolIdentityError::FilesNotCanonical);
                }
                if !valid_package_path(launcher)
                    || !files
                        .iter()
                        .any(|file| file.path == *launcher && file.executable)
                {
                    return Err(ToolIdentityError::LauncherMismatch);
                }
                match &self.cwd_policy {
                    ToolCwdPolicy::EmptyScratch | ToolCwdPolicy::ReadOnlyPackageRoot => {}
                    ToolCwdPolicy::ReadOnlyPackageSubdir(path) => {
                        if u32::try_from(path.len()).is_err() {
                            return Err(ToolIdentityError::CountOverflow);
                        }
                        if !valid_package_path(path)
                            || !files.iter().any(|file| {
                                file.path
                                    .strip_prefix(path)
                                    .is_some_and(|suffix| suffix.starts_with('/'))
                            })
                        {
                            return Err(ToolIdentityError::InvalidCwdPolicy);
                        }
                    }
                }
            }
            ToolSourceIdentityV2::Ambient {
                launcher,
                toolchain_id,
                ..
            } => {
                if u32::try_from(launcher.len()).is_err()
                    || !valid_text(launcher, false)
                    || !Path::new(launcher).is_absolute()
                {
                    return Err(ToolIdentityError::InvalidAmbientLauncher);
                }
                if u32::try_from(toolchain_id.len()).is_err() {
                    return Err(ToolIdentityError::CountOverflow);
                }
                if !valid_text(toolchain_id, false) {
                    return Err(ToolIdentityError::InvalidToolchainId);
                }
                if self.cwd_policy != ToolCwdPolicy::EmptyScratch {
                    return Err(ToolIdentityError::InvalidCwdPolicy);
                }
            }
        }
        Ok(())
    }

    pub fn is_cacheable(&self) -> bool {
        match &self.source {
            ToolSourceIdentityV2::Package { .. } => true,
            ToolSourceIdentityV2::Ambient {
                trusted_fingerprint,
                ..
            } => trusted_fingerprint.is_some(),
        }
    }

    pub fn package_files(&self) -> Option<&[ToolPackageFile]> {
        match &self.source {
            ToolSourceIdentityV2::Package { files, .. } => Some(files),
            ToolSourceIdentityV2::Ambient { .. } => None,
        }
    }

    pub fn digest(&self) -> Result<[u8; 32], ToolIdentityError> {
        self.validate()?;
        Ok(domain_digest(DSCT, Self::RECORD_VERSION, |encoder| {
            self.encode_fields(encoder)
        }))
    }

    pub fn encode_record(&self) -> Result<Vec<u8>, ToolIdentityError> {
        self.validate()?;
        let mut bytes = vec![Self::RECORD_VERSION];
        let mut encoder = CanonicalEncoder::new();
        self.encode_fields(&mut encoder);
        bytes.extend_from_slice(&encoder.into_bytes());
        Ok(bytes)
    }

    pub fn decode_record(bytes: &[u8]) -> Result<Self, ToolIdentityError> {
        let mut reader = RecordReader::new(bytes);
        let version = reader.u8()?;
        if version != Self::RECORD_VERSION {
            return Err(ToolIdentityError::UnsupportedVersion(version));
        }
        let source = match reader.u8()? {
            1 => ToolSourceIdentityV2::Package {
                launcher: reader.string()?,
                files: reader.seq(|reader| {
                    Ok(ToolPackageFile {
                        path: reader.string()?,
                        executable: reader.boolean()?,
                        len: reader.u64()?,
                        bytes_hash: reader.array()?,
                    })
                })?,
            },
            2 => ToolSourceIdentityV2::Ambient {
                launcher: reader.string()?,
                toolchain_id: reader.string()?,
                trusted_fingerprint: reader.option(|reader| reader.array())?,
            },
            value => return Err(ToolIdentityError::UnknownSource(value)),
        };
        let environment = reader.seq(|reader| Ok((reader.string()?, reader.string()?)))?;
        let cwd_policy = match reader.u8()? {
            1 => ToolCwdPolicy::EmptyScratch,
            2 => ToolCwdPolicy::ReadOnlyPackageRoot,
            3 => ToolCwdPolicy::ReadOnlyPackageSubdir(reader.string()?),
            value => return Err(ToolIdentityError::UnknownCwdPolicy(value)),
        };
        if reader.remaining() != 0 {
            return Err(ToolIdentityError::TrailingBytes);
        }
        let identity = Self {
            source,
            environment,
            cwd_policy,
        };
        identity.validate()?;
        Ok(identity)
    }

    fn encode_fields(&self, encoder: &mut CanonicalEncoder) {
        match &self.source {
            ToolSourceIdentityV2::Package { launcher, files } => {
                encoder.enum_variant(1);
                encoder.str(launcher);
                encoder.seq(files, |encoder, file| {
                    encoder.str(&file.path);
                    encoder.bool(file.executable);
                    encoder.u64(file.len);
                    encoder.raw(&file.bytes_hash);
                });
            }
            ToolSourceIdentityV2::Ambient {
                launcher,
                toolchain_id,
                trusted_fingerprint,
            } => {
                encoder.enum_variant(2);
                encoder.str(launcher);
                encoder.str(toolchain_id);
                encoder.option(trusted_fingerprint.as_ref(), |encoder, fingerprint| {
                    encoder.raw(*fingerprint)
                });
            }
        }
        encoder.seq(&self.environment, |encoder, row| {
            encoder.str(&row.0);
            encoder.str(&row.1);
        });
        match &self.cwd_policy {
            ToolCwdPolicy::EmptyScratch => encoder.enum_variant(1),
            ToolCwdPolicy::ReadOnlyPackageRoot => encoder.enum_variant(2),
            ToolCwdPolicy::ReadOnlyPackageSubdir(path) => {
                encoder.enum_variant(3);
                encoder.str(path);
            }
        }
    }
}

fn valid_package_path(value: &str) -> bool {
    !value.is_empty()
        && valid_text(value, false)
        && !value.contains('\\')
        && value
            .split('/')
            .all(|component| !component.is_empty() && !matches!(component, "." | ".."))
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

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ToolIdentityError> {
        let end = self
            .position
            .checked_add(N)
            .ok_or(ToolIdentityError::Truncated)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(ToolIdentityError::Truncated)?;
        self.position = end;
        Ok(bytes.try_into().expect("slice has requested length"))
    }

    fn u8(&mut self) -> Result<u8, ToolIdentityError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, ToolIdentityError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, ToolIdentityError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn boolean(&mut self) -> Result<bool, ToolIdentityError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(ToolIdentityError::InvalidBool(value)),
        }
    }

    fn string(&mut self) -> Result<String, ToolIdentityError> {
        let len = usize::try_from(self.u32()?).map_err(|_| ToolIdentityError::CountOverflow)?;
        let end = self
            .position
            .checked_add(len)
            .ok_or(ToolIdentityError::CountOverflow)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(ToolIdentityError::Truncated)?;
        self.position = end;
        let value = std::str::from_utf8(bytes).map_err(|_| ToolIdentityError::InvalidUtf8)?;
        if !is_nfc(value) || value.contains('\0') {
            return Err(ToolIdentityError::NonCanonicalText);
        }
        Ok(value.to_owned())
    }

    fn option<T>(
        &mut self,
        decode: impl FnOnce(&mut Self) -> Result<T, ToolIdentityError>,
    ) -> Result<Option<T>, ToolIdentityError> {
        match self.u8()? {
            0 => Ok(None),
            1 => decode(self).map(Some),
            value => Err(ToolIdentityError::InvalidBool(value)),
        }
    }

    fn seq<T>(
        &mut self,
        mut decode: impl FnMut(&mut Self) -> Result<T, ToolIdentityError>,
    ) -> Result<Vec<T>, ToolIdentityError> {
        let count = usize::try_from(self.u32()?).map_err(|_| ToolIdentityError::CountOverflow)?;
        if count > self.remaining() {
            return Err(ToolIdentityError::Truncated);
        }
        (0..count).map(|_| decode(self)).collect()
    }
}
