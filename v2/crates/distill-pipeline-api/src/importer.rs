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
}
