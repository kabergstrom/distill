//! §§8/10 query grammar, intake normalization, and result digests.

use std::fmt;

use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};
use globset::Glob;
use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntakeError {
    EmptyIdentifier,
    IdentifierTooLong,
    Nul,
    InvalidPath,
    EmptyQuery,
    AuthoringOnlyRestricted,
    BundleRelativeWithoutOrigin,
    InvalidGlob(String),
}

impl fmt::Display for IntakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyIdentifier => f.write_str("identifier must not be empty"),
            Self::IdentifierTooLong => f.write_str("identifier exceeds 255 UTF-8 bytes"),
            Self::Nul => f.write_str("NUL is not allowed"),
            Self::InvalidPath => {
                f.write_str("path is not a normalized, lexical root-relative path")
            }
            Self::EmptyQuery => f.write_str("a query must contain at least one selector"),
            Self::AuthoringOnlyRestricted => {
                f.write_str("authoring_only=true is restricted to tooling queries")
            }
            Self::BundleRelativeWithoutOrigin => {
                f.write_str("a bare local_id query requires an origin bundle")
            }
            Self::InvalidGlob(e) => write!(f, "invalid glob: {e}"),
        }
    }
}

impl std::error::Error for IntakeError {}

pub fn normalize_identifier(value: &str) -> Result<String, IntakeError> {
    if value.contains('\0') {
        return Err(IntakeError::Nul);
    }
    let value: String = value.nfc().collect();
    if value.is_empty() {
        return Err(IntakeError::EmptyIdentifier);
    }
    if value.len() > 255 {
        return Err(IntakeError::IdentifierTooLong);
    }
    Ok(value)
}

pub fn normalize_path(value: &str) -> Result<String, IntakeError> {
    if value.is_empty()
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains(['\\', '\0'])
    {
        return Err(IntakeError::InvalidPath);
    }
    let mut normalized = Vec::new();
    for component in value.split('/') {
        if component.is_empty() || matches!(component, "." | "..") {
            return Err(IntakeError::InvalidPath);
        }
        let component: String = component.nfc().collect();
        if component.is_empty() || matches!(component.as_str(), "." | "..") {
            return Err(IntakeError::InvalidPath);
        }
        normalized.push(component);
    }
    Ok(normalized.join("/"))
}

fn normalize_glob(value: &str) -> Result<String, IntakeError> {
    if value.is_empty()
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains(['\\', '\0'])
        || value
            .split('/')
            .any(|c| c.is_empty() || matches!(c, "." | ".."))
    {
        return Err(IntakeError::InvalidPath);
    }
    let value: String = value.nfc().collect();
    Glob::new(&value).map_err(|e| IntakeError::InvalidGlob(e.to_string()))?;
    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RootName(pub String);

impl RootName {
    pub fn new(value: impl AsRef<str>) -> Result<Self, IntakeError> {
        normalize_identifier(value.as_ref()).map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RootedPath {
    pub root: RootName,
    pub path: String,
}

impl RootedPath {
    pub fn new(root: impl AsRef<str>, path: impl AsRef<str>) -> Result<Self, IntakeError> {
        Ok(Self {
            root: RootName::new(root)?,
            path: normalize_path(path.as_ref())?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileQuery {
    pub path_prefix: Option<String>,
    pub path_glob: Option<String>,
}

impl FileQuery {
    pub fn new(
        path_prefix: Option<String>,
        path_glob: Option<String>,
    ) -> Result<Self, IntakeError> {
        if path_prefix.is_none() && path_glob.is_none() {
            return Err(IntakeError::EmptyQuery);
        }
        Ok(Self {
            path_prefix: path_prefix.map(|p| normalize_path(&p)).transpose()?,
            path_glob: path_glob.map(|p| normalize_glob(&p)).transpose()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TagSelector {
    pub tag: String,
    pub value: Option<String>,
}

/// Ordinary asset-query grammar. Migration control lookup is intentionally
/// not a selector here; it exists only as `trace::ControlQuery`.
///
/// ```compile_fail
/// use distill_pipeline_api::query::AssetQuery;
/// use distill_core::id::{LogicalHash, TypeUuid};
///
/// let _ = AssetQuery {
///     migration_edge: Some((TypeUuid([1; 16]), LogicalHash([2; 32]))),
///     ..AssetQuery::default()
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct AssetQuery {
    pub uuid: Option<AssetUuid>,
    pub bundle_path: Option<String>,
    pub local_id: Option<String>,
    pub bundle_uuid: Option<BundleUuid>,
    pub authored_type: Option<TypeUuid>,
    pub terminal_type: Option<TypeUuid>,
    pub tag: Option<TagSelector>,
    pub path_prefix: Option<String>,
    pub path_glob: Option<String>,
    pub authoring_only: Option<bool>,
}

impl AssetQuery {
    pub fn close(mut self, origin: Option<BundleUuid>) -> Result<Self, IntakeError> {
        if self.selector_count() == 0 {
            return Err(IntakeError::EmptyQuery);
        }
        if self.authoring_only == Some(true) {
            return Err(IntakeError::AuthoringOnlyRestricted);
        }
        self.bundle_path = self.bundle_path.map(|p| normalize_path(&p)).transpose()?;
        self.path_prefix = self.path_prefix.map(|p| normalize_path(&p)).transpose()?;
        self.path_glob = self.path_glob.map(|p| normalize_glob(&p)).transpose()?;
        self.local_id = self
            .local_id
            .map(|p| normalize_identifier(&p))
            .transpose()?;
        if let Some(tag) = &mut self.tag {
            tag.tag = normalize_identifier(&tag.tag)?;
            tag.value = tag
                .value
                .take()
                .map(|v| normalize_identifier(&v))
                .transpose()?;
        }
        if self.local_id.is_some() && self.bundle_path.is_none() && self.bundle_uuid.is_none() {
            self.bundle_uuid = Some(origin.ok_or(IntakeError::BundleRelativeWithoutOrigin)?);
        }
        Ok(self)
    }

    pub fn selector_count(&self) -> usize {
        [
            self.uuid.is_some(),
            self.bundle_path.is_some(),
            self.local_id.is_some(),
            self.bundle_uuid.is_some(),
            self.authored_type.is_some(),
            self.terminal_type.is_some(),
            self.tag.is_some(),
            self.path_prefix.is_some(),
            self.path_glob.is_some(),
            self.authoring_only.is_some(),
        ]
        .into_iter()
        .filter(|v| *v)
        .count()
    }
}

pub fn asset_query_result_hash(results: &[AssetUuid]) -> [u8; 32] {
    let mut results = results.to_vec();
    results.sort_unstable();
    results.dedup();
    let mut h = blake3::Hasher::new();
    h.update(b"ASTQ");
    h.update(&[1]);
    h.update(&(results.len() as u32).to_le_bytes());
    for id in results {
        h.update(&id.0);
    }
    *h.finalize().as_bytes()
}

pub fn file_query_result_hash(results: &[RootedPath]) -> [u8; 32] {
    let mut results = results.to_vec();
    results.sort_unstable();
    results.dedup();
    let mut h = blake3::Hasher::new();
    h.update(b"FILQ");
    h.update(&[1]);
    h.update(&(results.len() as u32).to_le_bytes());
    for item in results {
        for text in [&item.root.0, &item.path] {
            h.update(&(text.len() as u32).to_le_bytes());
            h.update(text.as_bytes());
        }
    }
    *h.finalize().as_bytes()
}
