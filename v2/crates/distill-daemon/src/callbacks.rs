//! Executable, epoch-owned pipeline registration surfaces.
//!
//! The public traits are the low-level neutral boundary used by generated
//! module adapters. Values crossing this boundary are host-owned data. The
//! generic erasure thunks are monomorphized into the registering module and
//! contain unwind before returning to the host.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::mem::ManuallyDrop;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::Arc;

use distill_asset::CallbackPanic;
use distill_build::codegen::{CodegenFailure, GeneratedFile};
use distill_build::dslf::OutputBindingFailureV1;
use distill_build::outputs::OutputDecls;
use distill_build::pipeline::{Target, TargetSelector};
use distill_build::query::{AssetQuery, IntakeError};
use distill_build::tool::{ProcessContext, ToolEpochSnapshot, ToolOutput, ToolRunError};
use distill_build::trace::{CapabilityKey, StableFailureFingerprint};
use distill_core::id::{AssetUuid, ContentHash, TypeUuid};
use distill_core::tool::ToolCwdPolicy;
use distill_json::AuthoredValue;
use distill_migrate::FieldPath;
use distill_schema::ngp_schema::{LogicalSchema, SchemaNode};

use crate::epoch::ModuleCallError;
use crate::epoch::PipelineEpoch;
use crate::importer::{AuthoringImportContext, AuthoringImporter, AuthoringImporterError};

#[derive(Debug, Clone, PartialEq)]
pub struct ImporterDescriptor {
    pub id: String,
    pub version: u32,
    pub settings_type_uuid: TypeUuid,
    pub settings_schema: LogicalSchema,
    pub default_settings: AuthoredValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorDescriptor {
    pub id: String,
    pub version: u32,
    pub input: TypeUuid,
    pub selector: TargetSelector,
    pub outputs: OutputDecls,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatorDescriptor {
    /// Stable diagnostic producer identity. Validators remain additive even
    /// when several rows target the same asset type.
    pub id: String,
    pub asset_type: TypeUuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultsDescriptor {
    pub type_uuid: TypeUuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodegenDescriptor {
    pub id: String,
    pub version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolDescriptor {
    pub id: String,
    pub registration: ToolRegistration,
}

/// Declarative module-facing tool registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolRegistration {
    pub source: ToolSource,
    pub environment: Vec<(String, String)>,
    pub cwd_policy: ToolCwdPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolSource {
    Package {
        root: PathBuf,
        launcher: String,
    },
    Ambient {
        launcher: PathBuf,
        toolchain_id: String,
        trusted_fingerprint: Option<[u8; 32]>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorError {
    pub code: u32,
    pub message: String,
}

impl ProcessorError {
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl From<ProcessContextError> for ProcessorError {
    fn from(error: ProcessContextError) -> Self {
        Self {
            // Zero is reserved by the host boundary for infrastructure/context
            // rejection and is never committed as a deterministic DSLF row.
            code: 0,
            message: error.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationFunctionError {
    pub code: u32,
    pub message: String,
}

impl MigrationFunctionError {
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub severity: DiagnosticSeverity,
    pub path: FieldPath,
    pub message: String,
}

#[derive(Debug, Default)]
pub struct Diagnostics {
    rows: Vec<Diagnostic>,
}

impl Diagnostics {
    pub fn error(&mut self, path: FieldPath, message: impl Into<String>) {
        self.rows.push(Diagnostic {
            severity: DiagnosticSeverity::Error,
            path,
            message: message.into(),
        });
    }

    pub fn warn(&mut self, path: FieldPath, message: impl Into<String>) {
        self.rows.push(Diagnostic {
            severity: DiagnosticSeverity::Warning,
            path,
            message: message.into(),
        });
    }

    pub fn rows(&self) -> &[Diagnostic] {
        &self.rows
    }

    pub fn into_rows(self) -> Vec<Diagnostic> {
        self.rows
    }
}

/// Neutral processor result. Generated typed adapters lower their values to
/// `AuthoredValue`; the daemon validates the closed declaration and performs
/// the canonical wire encoding before publication.
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessorProduct {
    pub type_uuid: TypeUuid,
    pub value: AuthoredValue,
}

impl ProcessorProduct {
    pub fn new(type_uuid: TypeUuid, value: AuthoredValue) -> Self {
        Self { type_uuid, value }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ProcessorProducts {
    pub primary: Option<ProcessorProduct>,
    pub extras: BTreeMap<String, ProcessorProduct>,
    pub debug: BTreeMap<String, Vec<u8>>,
}

/// One snapshot-pinned terminal artifact returned by a processor dependency
/// read. Generated typed adapters fix up `structural`/`blobs` into `T`; the
/// neutral boundary retains the complete authenticated payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessArtifact {
    pub asset: AssetUuid,
    pub content_hash: ContentHash,
    pub encoded_type: TypeUuid,
    pub terminal_type: TypeUuid,
    pub structural: Arc<[u8]>,
    pub blobs: Vec<Arc<[u8]>>,
}

/// One exact authored source entry exposed to an authoring-side generator.
#[derive(Debug, Clone, PartialEq)]
pub struct CodegenAsset {
    pub asset: AssetUuid,
    pub type_uuid: TypeUuid,
    pub value: AuthoredValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodegenContextError {
    Unavailable(&'static str),
    AttemptStopped,
    InvalidQuery(IntakeError),
    Failed(String),
}

impl std::fmt::Display for CodegenContextError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for CodegenContextError {}

/// Job-bound output namespace. A processor can only mint child identities
/// from the parent and declaration set installed for its own stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessOutputs {
    parent: AssetUuid,
    declarations: OutputDecls,
}

impl ProcessOutputs {
    pub(crate) fn new(parent: AssetUuid, declarations: OutputDecls) -> Self {
        Self {
            parent,
            declarations,
        }
    }

    pub fn parent(&self) -> AssetUuid {
        self.parent
    }

    pub fn declarations(&self) -> &OutputDecls {
        &self.declarations
    }

    pub fn child(&self, key: &str) -> Result<AssetUuid, ProcessContextError> {
        if !self.declarations.extras.contains_key(key) {
            return Err(ProcessContextError::UndeclaredOutput(key.to_owned()));
        }
        Ok(AssetUuid::v5(self.parent, key))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessContextError {
    Unavailable(&'static str),
    AttemptStopped,
    InvalidQuery(IntakeError),
    Observed(StableFailureFingerprint),
    WrongTerminal {
        asset: AssetUuid,
        expected: TypeUuid,
        observed: TypeUuid,
    },
    UndeclaredOutput(String),
    Failed(String),
}

impl std::fmt::Display for ProcessContextError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ProcessContextError {}

pub trait PipelineImporter: Send + Sync + 'static {
    fn import(
        &self,
        context: &mut dyn AuthoringImportContext,
        settings: &AuthoredValue,
    ) -> Result<distill_build::import::ImportOutput, AuthoringImporterError>;
}

impl<T: AuthoringImporter + 'static> PipelineImporter for T {
    fn import(
        &self,
        context: &mut dyn AuthoringImportContext,
        settings: &AuthoredValue,
    ) -> Result<distill_build::import::ImportOutput, AuthoringImporterError> {
        AuthoringImporter::import(self, context, settings)
    }
}

pub trait PipelineProcessContext {
    fn read(
        &mut self,
        _asset: AssetUuid,
        _expected_terminal: TypeUuid,
    ) -> Result<ProcessArtifact, ProcessContextError> {
        Err(ProcessContextError::Unavailable("read"))
    }

    fn read_path(
        &mut self,
        _path: &str,
        _expected_terminal: TypeUuid,
    ) -> Result<ProcessArtifact, ProcessContextError> {
        Err(ProcessContextError::Unavailable("read_path"))
    }

    fn query(&mut self, _query: &AssetQuery) -> Result<Vec<AssetUuid>, ProcessContextError> {
        Err(ProcessContextError::Unavailable("query"))
    }

    fn target(&self) -> Result<&Target, ProcessContextError> {
        Err(ProcessContextError::Unavailable("target"))
    }

    fn outputs(&self) -> Result<ProcessOutputs, ProcessContextError> {
        Err(ProcessContextError::Unavailable("outputs"))
    }

    fn run_tool(
        &mut self,
        id: &str,
        args: &[String],
        stdin: &[u8],
    ) -> Result<ToolOutput, ToolRunError>;
}

/// Snapshot-pinned authored reads available only to codegen callbacks.
pub trait PipelineCodegenContext {
    fn query(&mut self, _query: &AssetQuery) -> Result<Vec<AssetUuid>, CodegenContextError> {
        Err(CodegenContextError::Unavailable("query"))
    }

    fn read(&mut self, _asset: AssetUuid) -> Result<Option<CodegenAsset>, CodegenContextError> {
        Err(CodegenContextError::Unavailable("read"))
    }
}

/// Daemon-owned reverse-ABI adapter. Every vtable entry contains a host panic
/// before control returns through module frames; the latched failure overrides
/// any result the module subsequently tries to return.
pub(crate) struct ContainedImportContext<'a> {
    inner: &'a mut dyn AuthoringImportContext,
    panicked: Cell<bool>,
}

impl<'a> ContainedImportContext<'a> {
    pub(crate) fn new(inner: &'a mut dyn AuthoringImportContext) -> Self {
        Self {
            inner,
            panicked: Cell::new(false),
        }
    }

    pub(crate) fn panicked(&self) -> bool {
        self.panicked.get()
    }

    fn failure(&self) -> distill_build::import::ImportError {
        self.panicked.set(true);
        distill_build::import::ImportError {
            fingerprint: StableFailureFingerprint::MissingCapability {
                key: CapabilityKey::Importer("host-callback-panic".to_owned()),
            },
        }
    }
}

impl AuthoringImportContext for ContainedImportContext<'_> {
    fn sources(&self) -> &[distill_build::query::RootedPath] {
        if self.panicked.get() {
            return &[];
        }
        match catch_unwind(AssertUnwindSafe(|| self.inner.sources())) {
            Ok(sources) => sources,
            Err(_) => {
                self.panicked.set(true);
                &[]
            }
        }
    }

    fn read(&mut self, path: &str) -> Result<Vec<u8>, distill_build::import::ImportError> {
        if self.panicked.get() {
            return Err(self.failure());
        }
        catch_unwind(AssertUnwindSafe(|| self.inner.read(path)))
            .unwrap_or_else(|_| Err(self.failure()))
    }

    fn probe(&mut self, path: &str) -> Result<bool, distill_build::import::ImportError> {
        if self.panicked.get() {
            return Err(self.failure());
        }
        catch_unwind(AssertUnwindSafe(|| self.inner.probe(path)))
            .unwrap_or_else(|_| Err(self.failure()))
    }

    fn enumerate(
        &mut self,
        query: &distill_build::query::FileQuery,
    ) -> Result<Vec<distill_build::query::RootedPath>, distill_build::import::ImportError> {
        if self.panicked.get() {
            return Err(self.failure());
        }
        catch_unwind(AssertUnwindSafe(|| self.inner.enumerate(query)))
            .unwrap_or_else(|_| Err(self.failure()))
    }

    fn importer_capability(
        &mut self,
        id: &str,
    ) -> Result<[u8; 32], distill_build::import::ImportError> {
        if self.panicked.get() {
            return Err(self.failure());
        }
        catch_unwind(AssertUnwindSafe(|| self.inner.importer_capability(id)))
            .unwrap_or_else(|_| Err(self.failure()))
    }
}

pub(crate) struct ContainedProcessContext<'a> {
    inner: &'a mut dyn PipelineProcessContext,
    panicked: Cell<bool>,
}

impl<'a> ContainedProcessContext<'a> {
    pub(crate) fn new(inner: &'a mut dyn PipelineProcessContext) -> Self {
        Self {
            inner,
            panicked: Cell::new(false),
        }
    }

    pub(crate) fn panicked(&self) -> bool {
        self.panicked.get()
    }

    fn failed<T>(&self) -> Result<T, ProcessContextError> {
        self.panicked.set(true);
        Err(ProcessContextError::Failed(
            "daemon process-context callback panicked".to_owned(),
        ))
    }
}

impl PipelineProcessContext for ContainedProcessContext<'_> {
    fn read(
        &mut self,
        asset: AssetUuid,
        expected_terminal: TypeUuid,
    ) -> Result<ProcessArtifact, ProcessContextError> {
        if self.panicked.get() {
            return self.failed();
        }
        catch_unwind(AssertUnwindSafe(|| {
            self.inner.read(asset, expected_terminal)
        }))
        .unwrap_or_else(|_| self.failed())
    }

    fn read_path(
        &mut self,
        path: &str,
        expected_terminal: TypeUuid,
    ) -> Result<ProcessArtifact, ProcessContextError> {
        if self.panicked.get() {
            return self.failed();
        }
        catch_unwind(AssertUnwindSafe(|| {
            self.inner.read_path(path, expected_terminal)
        }))
        .unwrap_or_else(|_| self.failed())
    }

    fn query(&mut self, query: &AssetQuery) -> Result<Vec<AssetUuid>, ProcessContextError> {
        if self.panicked.get() {
            return self.failed();
        }
        catch_unwind(AssertUnwindSafe(|| self.inner.query(query))).unwrap_or_else(|_| self.failed())
    }

    fn target(&self) -> Result<&Target, ProcessContextError> {
        if self.panicked.get() {
            return self.failed();
        }
        match catch_unwind(AssertUnwindSafe(|| self.inner.target())) {
            Ok(target) => target,
            Err(_) => self.failed(),
        }
    }

    fn outputs(&self) -> Result<ProcessOutputs, ProcessContextError> {
        if self.panicked.get() {
            return self.failed();
        }
        catch_unwind(AssertUnwindSafe(|| self.inner.outputs())).unwrap_or_else(|_| self.failed())
    }

    fn run_tool(
        &mut self,
        id: &str,
        args: &[String],
        stdin: &[u8],
    ) -> Result<ToolOutput, ToolRunError> {
        if self.panicked.get() {
            return Err(ToolRunError::Infrastructure {
                id: id.to_owned(),
                detail: "daemon process-context callback panicked".to_owned(),
            });
        }
        catch_unwind(AssertUnwindSafe(|| self.inner.run_tool(id, args, stdin))).unwrap_or_else(
            |_| {
                self.panicked.set(true);
                Err(ToolRunError::Infrastructure {
                    id: id.to_owned(),
                    detail: "daemon process-context callback panicked".to_owned(),
                })
            },
        )
    }
}

pub(crate) struct ContainedCodegenContext<'a> {
    inner: &'a mut dyn PipelineCodegenContext,
    panicked: Cell<bool>,
}

impl<'a> ContainedCodegenContext<'a> {
    pub(crate) fn new(inner: &'a mut dyn PipelineCodegenContext) -> Self {
        Self {
            inner,
            panicked: Cell::new(false),
        }
    }

    pub(crate) fn panicked(&self) -> bool {
        self.panicked.get()
    }

    fn failed<T>(&self) -> Result<T, CodegenContextError> {
        self.panicked.set(true);
        Err(CodegenContextError::Failed(
            "daemon codegen-context callback panicked".to_owned(),
        ))
    }
}

impl PipelineCodegenContext for ContainedCodegenContext<'_> {
    fn query(&mut self, query: &AssetQuery) -> Result<Vec<AssetUuid>, CodegenContextError> {
        if self.panicked.get() {
            return self.failed();
        }
        catch_unwind(AssertUnwindSafe(|| self.inner.query(query))).unwrap_or_else(|_| self.failed())
    }

    fn read(&mut self, asset: AssetUuid) -> Result<Option<CodegenAsset>, CodegenContextError> {
        if self.panicked.get() {
            return self.failed();
        }
        catch_unwind(AssertUnwindSafe(|| self.inner.read(asset))).unwrap_or_else(|_| self.failed())
    }
}

impl<S: ToolEpochSnapshot + ?Sized> PipelineProcessContext for ProcessContext<'_, S> {
    fn run_tool(
        &mut self,
        id: &str,
        args: &[String],
        stdin: &[u8],
    ) -> Result<ToolOutput, ToolRunError> {
        let args = args.iter().map(String::as_str).collect::<Vec<_>>();
        ProcessContext::run_tool(self, id, &args, stdin)
    }
}

pub trait PipelineProcessor: Send + Sync + 'static {
    fn process(
        &self,
        input: AuthoredValue,
        context: &mut dyn PipelineProcessContext,
    ) -> Result<ProcessorProducts, ProcessorError>;
}

pub trait PipelineCodegen: Send + Sync + 'static {
    fn generate(
        &self,
        context: &mut dyn PipelineCodegenContext,
    ) -> Result<Vec<GeneratedFile>, CodegenFailure>;
}

pub trait PipelineValidator: Send + Sync + 'static {
    fn validate(
        &self,
        asset: &AuthoredValue,
        diagnostics: &mut Diagnostics,
    ) -> Result<(), CallbackPanic>;
}

pub trait PipelineMigration: Send + Sync + 'static {
    fn migrate(&self, value: AuthoredValue) -> Result<AuthoredValue, MigrationFunctionError>;
}

impl<F> PipelineMigration for F
where
    F: Fn(AuthoredValue) -> Result<AuthoredValue, MigrationFunctionError> + Send + Sync + 'static,
{
    fn migrate(&self, value: AuthoredValue) -> Result<AuthoredValue, MigrationFunctionError> {
        self(value)
    }
}

pub trait PipelineDefaults: Send + Sync + 'static {
    fn field_default(&self, to_schema: &SchemaNode, at: &FieldPath) -> Option<AuthoredValue>;
    fn parent_default(&self, to_schema: &SchemaNode, at: &FieldPath) -> Option<AuthoredValue>;
}

#[derive(Debug)]
pub enum CallbackInvokeError<E> {
    Missing,
    Unavailable(String),
    HostRejected(String),
    OutputBinding(OutputBindingFailureV1),
    Panicked,
    Rejected(E),
}

impl<E: std::fmt::Display> std::fmt::Display for CallbackInvokeError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing => formatter.write_str("callback is not registered"),
            Self::Unavailable(detail) => formatter.write_str(detail),
            Self::HostRejected(detail) => formatter.write_str(detail),
            Self::OutputBinding(failure) => {
                write!(formatter, "invalid processor output binding: {failure:?}")
            }
            Self::Panicked => formatter.write_str("callback panicked"),
            Self::Rejected(error) => write!(formatter, "callback rejected: {error}"),
        }
    }
}

pub type InfallibleCallbackError = CallbackInvokeError<Infallible>;

pub(crate) struct EpochAuthoringImporter {
    epoch: PipelineEpoch,
    descriptor: ImporterDescriptor,
}

impl EpochAuthoringImporter {
    pub(crate) fn all(epoch: &PipelineEpoch) -> Vec<Arc<dyn AuthoringImporter>> {
        epoch
            .importer_descriptors()
            .into_iter()
            .map(|descriptor| {
                Arc::new(Self {
                    epoch: epoch.clone(),
                    descriptor,
                }) as Arc<dyn AuthoringImporter>
            })
            .collect()
    }

    pub(crate) fn metadata_only(
        descriptors: Vec<ImporterDescriptor>,
    ) -> Vec<Arc<dyn AuthoringImporter>> {
        descriptors
            .into_iter()
            .map(|descriptor| {
                Arc::new(DescriptorAuthoringImporter { descriptor }) as Arc<dyn AuthoringImporter>
            })
            .collect()
    }
}

struct DescriptorAuthoringImporter {
    descriptor: ImporterDescriptor,
}

impl AuthoringImporter for DescriptorAuthoringImporter {
    fn id(&self) -> &str {
        &self.descriptor.id
    }

    fn version(&self) -> u32 {
        self.descriptor.version
    }

    fn settings_type_uuid(&self) -> TypeUuid {
        self.descriptor.settings_type_uuid
    }

    fn settings_schema(&self) -> &LogicalSchema {
        &self.descriptor.settings_schema
    }

    fn default_settings(&self) -> AuthoredValue {
        self.descriptor.default_settings.clone()
    }

    fn import(
        &self,
        _context: &mut dyn AuthoringImportContext,
        _settings: &AuthoredValue,
    ) -> Result<distill_build::import::ImportOutput, AuthoringImporterError> {
        Err(AuthoringImporterError::PipelineUnavailable(
            "metadata-only importer cannot execute".to_owned(),
        ))
    }
}

impl AuthoringImporter for EpochAuthoringImporter {
    fn id(&self) -> &str {
        &self.descriptor.id
    }

    fn version(&self) -> u32 {
        self.descriptor.version
    }

    fn settings_type_uuid(&self) -> TypeUuid {
        self.descriptor.settings_type_uuid
    }

    fn settings_schema(&self) -> &LogicalSchema {
        &self.descriptor.settings_schema
    }

    fn default_settings(&self) -> AuthoredValue {
        self.descriptor.default_settings.clone()
    }

    fn import(
        &self,
        context: &mut dyn AuthoringImportContext,
        settings: &AuthoredValue,
    ) -> Result<distill_build::import::ImportOutput, AuthoringImporterError> {
        match self
            .epoch
            .invoke_importer(&self.descriptor.id, context, settings)
        {
            Ok(output) => Ok(output),
            Err(CallbackInvokeError::Rejected(error)) => Err(error),
            Err(CallbackInvokeError::Missing) => Err(AuthoringImporterError::PipelineUnavailable(
                "the importer disappeared from its pinned pipeline epoch".to_owned(),
            )),
            Err(CallbackInvokeError::Unavailable(detail)) => {
                Err(AuthoringImporterError::PipelineUnavailable(detail))
            }
            Err(CallbackInvokeError::HostRejected(detail)) => {
                Err(AuthoringImporterError::PipelineUnavailable(detail))
            }
            Err(CallbackInvokeError::OutputBinding(failure)) => {
                Err(AuthoringImporterError::PipelineUnavailable(format!(
                    "unexpected importer output-binding failure: {failure:?}"
                )))
            }
            Err(CallbackInvokeError::Panicked) => Err(AuthoringImporterError::PipelineUnavailable(
                "the importer callback panicked and poisoned its pipeline epoch".to_owned(),
            )),
        }
    }
}

type ImporterCall = unsafe fn(
    *const u8,
    &mut dyn AuthoringImportContext,
    &AuthoredValue,
) -> Result<
    Result<distill_build::import::ImportOutput, AuthoringImporterError>,
    CallbackPanic,
>;
type ProcessorCall = unsafe fn(
    *const u8,
    AuthoredValue,
    &mut dyn PipelineProcessContext,
) -> Result<Result<ProcessorProducts, ProcessorError>, CallbackPanic>;
type CodegenCall = unsafe fn(
    *const u8,
    &mut dyn PipelineCodegenContext,
) -> Result<Result<Vec<GeneratedFile>, CodegenFailure>, CallbackPanic>;
type ValidatorCall = unsafe fn(
    *const u8,
    &AuthoredValue,
    &mut Diagnostics,
) -> Result<Result<(), CallbackPanic>, CallbackPanic>;
type MigrationCall =
    unsafe fn(
        *const u8,
        AuthoredValue,
    ) -> Result<Result<AuthoredValue, MigrationFunctionError>, CallbackPanic>;
type DefaultCall =
    unsafe fn(*const u8, &SchemaNode, &FieldPath) -> Result<Option<AuthoredValue>, CallbackPanic>;

#[derive(Clone)]
pub(crate) enum CallbackHandle {
    None,
    Importer {
        descriptor: ImporterDescriptor,
        call: ImporterCall,
    },
    Processor {
        descriptor: ProcessorDescriptor,
        call: ProcessorCall,
    },
    Codegen {
        descriptor: CodegenDescriptor,
        call: CodegenCall,
    },
    Validator {
        descriptor: ValidatorDescriptor,
        call: ValidatorCall,
    },
    Migration {
        key: String,
        call: MigrationCall,
    },
    Defaults {
        descriptor: DefaultsDescriptor,
        field: DefaultCall,
        parent: DefaultCall,
    },
    Tool(ToolDescriptor),
}

impl std::fmt::Debug for CallbackHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::None => "CallbackHandle::None",
            Self::Importer { .. } => "CallbackHandle::Importer",
            Self::Processor { .. } => "CallbackHandle::Processor",
            Self::Codegen { .. } => "CallbackHandle::Codegen",
            Self::Validator { .. } => "CallbackHandle::Validator",
            Self::Migration { .. } => "CallbackHandle::Migration",
            Self::Defaults { .. } => "CallbackHandle::Defaults",
            Self::Tool(_) => "CallbackHandle::Tool",
        })
    }
}

impl CallbackHandle {
    pub(crate) fn importer<T: PipelineImporter>(descriptor: ImporterDescriptor) -> Self {
        Self::Importer {
            descriptor,
            call: call_importer::<T>,
        }
    }

    pub(crate) fn processor<T: PipelineProcessor>(descriptor: ProcessorDescriptor) -> Self {
        Self::Processor {
            descriptor,
            call: call_processor::<T>,
        }
    }

    pub(crate) fn codegen<T: PipelineCodegen>(descriptor: CodegenDescriptor) -> Self {
        Self::Codegen {
            descriptor,
            call: call_codegen::<T>,
        }
    }

    pub(crate) fn validator<T: PipelineValidator>(descriptor: ValidatorDescriptor) -> Self {
        Self::Validator {
            descriptor,
            call: call_validator::<T>,
        }
    }

    pub(crate) fn migration<T: PipelineMigration>(key: String) -> Self {
        Self::Migration {
            key,
            call: call_migration::<T>,
        }
    }

    pub(crate) fn defaults<T: PipelineDefaults>(descriptor: DefaultsDescriptor) -> Self {
        Self::Defaults {
            descriptor,
            field: call_field_default::<T>,
            parent: call_parent_default::<T>,
        }
    }
}

pub(crate) fn erase_callback<T>(callback: T) -> *mut u8 {
    Box::into_raw(Box::new(ManuallyDrop::new(callback))).cast()
}

/// Drop the callback under containment and deallocate only after successful
/// destruction. A panicking callback destructor leaves the allocation intact.
pub(crate) unsafe fn cleanup_callback<T>(pointer: *mut u8) -> Result<(), ModuleCallError> {
    let typed = pointer.cast::<T>();
    catch_unwind(AssertUnwindSafe(|| unsafe {
        std::ptr::drop_in_place(typed)
    }))
    .map_err(|_| ModuleCallError::new("pipeline callback destructor panicked"))?;
    drop(unsafe { Box::from_raw(pointer.cast::<ManuallyDrop<T>>()) });
    Ok(())
}

unsafe fn call_importer<T: PipelineImporter>(
    pointer: *const u8,
    context: &mut dyn AuthoringImportContext,
    settings: &AuthoredValue,
) -> Result<Result<distill_build::import::ImportOutput, AuthoringImporterError>, CallbackPanic> {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        (&*pointer.cast::<T>()).import(context, settings)
    }))
    .map_err(|_| CallbackPanic)
}

unsafe fn call_processor<T: PipelineProcessor>(
    pointer: *const u8,
    input: AuthoredValue,
    context: &mut dyn PipelineProcessContext,
) -> Result<Result<ProcessorProducts, ProcessorError>, CallbackPanic> {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        (&*pointer.cast::<T>()).process(input, context)
    }))
    .map_err(|_| CallbackPanic)
}

unsafe fn call_codegen<T: PipelineCodegen>(
    pointer: *const u8,
    context: &mut dyn PipelineCodegenContext,
) -> Result<Result<Vec<GeneratedFile>, CodegenFailure>, CallbackPanic> {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        (&*pointer.cast::<T>()).generate(context)
    }))
    .map_err(|_| CallbackPanic)
}

unsafe fn call_validator<T: PipelineValidator>(
    pointer: *const u8,
    asset: &AuthoredValue,
    diagnostics: &mut Diagnostics,
) -> Result<Result<(), CallbackPanic>, CallbackPanic> {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        (&*pointer.cast::<T>()).validate(asset, diagnostics)
    }))
    .map_err(|_| CallbackPanic)
}

unsafe fn call_migration<T: PipelineMigration>(
    pointer: *const u8,
    value: AuthoredValue,
) -> Result<Result<AuthoredValue, MigrationFunctionError>, CallbackPanic> {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        (&*pointer.cast::<T>()).migrate(value)
    }))
    .map_err(|_| CallbackPanic)
}

unsafe fn call_field_default<T: PipelineDefaults>(
    pointer: *const u8,
    schema: &SchemaNode,
    at: &FieldPath,
) -> Result<Option<AuthoredValue>, CallbackPanic> {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        (&*pointer.cast::<T>()).field_default(schema, at)
    }))
    .map_err(|_| CallbackPanic)
}

unsafe fn call_parent_default<T: PipelineDefaults>(
    pointer: *const u8,
    schema: &SchemaNode,
    at: &FieldPath,
) -> Result<Option<AuthoredValue>, CallbackPanic> {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        (&*pointer.cast::<T>()).parent_default(schema, at)
    }))
    .map_err(|_| CallbackPanic)
}
