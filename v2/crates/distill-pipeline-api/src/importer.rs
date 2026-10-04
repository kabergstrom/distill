//! The authoring importer interface (§8).

use distill_core::id::TypeUuid;
use distill_json::AuthoredValue;
use ngp_schema::LogicalSchema;

use crate::import::{ImportError, ImportOutput};
use crate::query::{FileQuery, RootedPath};

pub trait AuthoringImporter: Send + Sync {
    fn id(&self) -> &str;
    fn version(&self) -> u32;
    fn settings_type_uuid(&self) -> TypeUuid;
    fn settings_schema(&self) -> &LogicalSchema;
    fn default_settings(&self) -> AuthoredValue;
    fn import(
        &self,
        context: &mut dyn AuthoringImportContext,
        settings: &AuthoredValue,
    ) -> Result<ImportOutput, AuthoringImporterError>;
}

/// Closed bridge failure: a context dependency keeps its exact observed
/// fingerprint, while importer-owned deterministic rejection carries the
/// stable non-zero code defined by that importer version. Messages are
/// presentation only and never decide wakeup or equality.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoringImporterError {
    Dependency(ImportError),
    Rejected {
        code: u32,
        message: String,
    },
    /// Epoch fencing, callback panic, or host infrastructure. This outcome is
    /// never stable importer data and therefore must not be memoized.
    PipelineUnavailable(String),
}

impl AuthoringImporterError {
    pub fn rejected(code: u32, message: impl Into<String>) -> Self {
        Self::Rejected {
            code,
            message: message.into(),
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::Dependency(error) => format!("{error:?}"),
            Self::Rejected { message, .. } => message.clone(),
            Self::PipelineUnavailable(message) => message.clone(),
        }
    }
}

impl From<ImportError> for AuthoringImporterError {
    fn from(error: ImportError) -> Self {
        Self::Dependency(error)
    }
}

pub trait AuthoringImportContext {
    fn sources(&self) -> &[RootedPath];
    fn read(&mut self, path: &str) -> Result<Vec<u8>, ImportError>;
    fn probe(&mut self, path: &str) -> Result<bool, ImportError>;
    fn enumerate(&mut self, query: &FileQuery) -> Result<Vec<RootedPath>, ImportError>;
    fn importer_capability(&mut self, id: &str) -> Result<[u8; 32], ImportError>;

    /// The `$settings` of the import of `path`: the bundle at
    /// `{path}.bundle` whose `$record` names `path` among its sources.
    /// `None` when that file is missing, is not a bundle, or is not an
    /// import of `path`.
    ///
    /// Reads the bundle through [`read`](Self::read), so the read set holds
    /// the whole bundle file: any re-publish of it (a settings edit, or a
    /// re-import after its source changed) re-runs the reading import.
    fn read_settings(&mut self, path: &str) -> Result<Option<AuthoredValue>, ImportError> {
        let path = crate::query::normalize_path(path).unwrap_or_else(|_| path.to_owned());
        let bundle_path = format!("{path}.bundle");
        if !self.probe(&bundle_path)? {
            return Ok(None);
        }
        let bytes = self.read(&bundle_path)?;
        Ok(import_settings(&bytes, &path))
    }
}

/// `$settings` of the bundle `bytes` when its `$record` lists `path` as a
/// source.
fn import_settings(bytes: &[u8], path: &str) -> Option<AuthoredValue> {
    let mut bundle = distill_bundle::parse_bundle(bytes).ok()?;
    let record = bundle.assets.get("$record")?;
    if record.type_uuid != distill_core::bootstrap::IMPORT_RECORD_TYPE_UUID
        || !record.authoring_only
    {
        return None;
    }
    let AuthoredValue::Object(fields) = &record.data else {
        return None;
    };
    let Some(AuthoredValue::Array(sources)) = fields.get("sources") else {
        return None;
    };
    let names_path = sources.iter().any(|source| match source {
        AuthoredValue::Object(source) => {
            matches!(source.get("path"), Some(AuthoredValue::Str(source)) if source == path)
        }
        _ => false,
    });
    if !names_path {
        return None;
    }
    bundle.assets.remove("$settings").map(|entry| entry.data)
}
