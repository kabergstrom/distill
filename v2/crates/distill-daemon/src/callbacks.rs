//! Host side of the pipeline callback boundary.
//!
//! The callback traits, descriptors and erasure thunks live in
//! `distill-pipeline-api` and are re-exported here. This module holds the
//! daemon's containment adapters for the reverse (host-to-module) contexts,
//! the invoke error, and the epoch-backed importer.

use std::cell::Cell;
use std::convert::Infallible;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use distill_build::dslf::OutputBindingFailureV1;
use distill_build::pipeline::Target;
use distill_build::query::AssetQuery;
use distill_build::tool::{ToolOutput, ToolRunError};
use distill_build::trace::{CapabilityKey, StableFailureFingerprint};
use distill_core::id::{AssetUuid, TypeUuid};
use distill_json::AuthoredValue;
use distill_schema::ngp_schema::LogicalSchema;

pub use distill_pipeline_api::callbacks::*;

use crate::epoch::PipelineEpoch;
use crate::importer::{AuthoringImportContext, AuthoringImporter, AuthoringImporterError};

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
