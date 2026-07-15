//! Concrete pipeline cdylib loader over the shared New Game Plus host layer.

use std::collections::BTreeSet;

use distill_asset::{
    AssetRuntimeDescriptor, CallbackPanic, EncodeContainer, EncodeSink, EpochToken, ErasedValue,
    ModuleEpochToken, PlaceholderThunk,
};
use distill_build::import::ImportOutput;
use distill_build::outputs::{OutputDecls, OutputError};
use distill_build::tool::{ToolOutput, ToolRunError};
use distill_core::canonical::{domain_digest, DSMA};
use distill_json::AuthoredValue;
use unicode_normalization::is_nfc;

use crate::callbacks::{
    CallbackInvokeError, CodegenAsset, CodegenContextError, CodegenDescriptor, DefaultsDescriptor,
    Diagnostic, Diagnostics, ImporterDescriptor, MigrationFunctionError, PipelineCodegenContext,
    PipelineProcessContext, ProcessArtifact, ProcessContextError, ProcessOutputs,
    ProcessorDescriptor, ProcessorError, ProcessorProducts, ToolDescriptor, ToolRegistration,
    ToolSource, ValidatorDescriptor,
};
use crate::epoch::{
    CandidateRegistrationArena, ErasedRegistrationCapsule, HostCallbackBoundary,
    LoadedPipelineModule, ModuleAbiIdentity, ModuleCallError, PipelineModuleLoader, Registration,
    RegistrationSet, RegistrationStatus, StagedModule, TargetDefinition,
};
use crate::importer::{AuthoringImportContext, AuthoringImporterError};

mod host_interface_closure {
    include!(concat!(env!("OUT_DIR"), "/host_interface_closure.rs"));
}

pub const PIPELINE_MODULE_ABI_VERSION_V2: u32 = 2;
pub const PIPELINE_MODULE_SYMBOL_V2: &[u8] = b"distill_pipeline_module_v2\0";
const IDENTITY_ENCODING_VERSION: u8 = 1;
const MAX_PROBE_BYTES: usize = 64 * 1024;

pub type PipelineProbeFnV2 =
    unsafe extern "C" fn(buffer: *mut u8, capacity: u32, length: *mut u32) -> i32;
pub type PipelineRegisterFnV2 = unsafe fn(
    targets: &[TargetDefinition],
    arena: &mut CandidateRegistrationArena,
) -> Result<BTreeSet<String>, ModuleCallError>;
pub type PipelineUnloadFnV2 = unsafe fn() -> Result<(), ModuleCallError>;

/// The audited pipeline module table. The C-ABI prefix is called before Rust
/// ABI compatibility is established; the lower entries are called only after
/// the returned identity equals the host's complete expected identity.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PipelineModuleTableV2 {
    pub abi_version: u32,
    pub module_abi: PipelineProbeFnV2,
    pub register: PipelineRegisterFnV2,
    pub unload: PipelineUnloadFnV2,
}

pub type PipelineModuleEntryV2 = unsafe extern "C" fn() -> *const PipelineModuleTableV2;

pub fn host_interface_closure_manifest() -> &'static [(&'static str, &'static [u8])] {
    host_interface_closure::HOST_INTERFACE_CLOSURE
}

/// Exact `rustc -vV` of the compiler that built this resident host image.
/// This is intentionally independent of the watched project's DSCI.
pub fn host_rustc_identity() -> &'static str {
    host_interface_closure::HOST_RUSTC_IDENTITY
}

/// The daemon-side ABI identity embedded into the expected candidate. A
/// pipeline module built against this exact interface crate calls the same
/// helper when exporting its C-prefix identity bytes.
pub fn host_module_abi_identity() -> ModuleAbiIdentity {
    let interface_fingerprint = domain_digest(DSMA, 1, |encoder| {
        encoder.seq(
            host_interface_closure::HOST_INTERFACE_CLOSURE,
            |encoder, row| {
                encoder.str(row.0);
                encoder.u64(row.1.len() as u64);
                encoder.raw(row.1);
            },
        );
        encoder.seq(
            host_interface_closure::HOST_BUILD_CONFIGURATION,
            |encoder, row| {
                encoder.str(row.0);
                encoder.str(row.1);
            },
        );
    });
    let measured_interface = domain_digest(DSMA, 2, |encoder| {
        encode_measurement::<PipelineModuleTableV2>(encoder);
        encode_measurement::<PipelineProbeFnV2>(encoder);
        encode_measurement::<PipelineRegisterFnV2>(encoder);
        encode_measurement::<PipelineUnloadFnV2>(encoder);
        encode_measurement::<TargetDefinition>(encoder);
        encode_measurement::<CandidateRegistrationArena>(encoder);
        encode_measurement::<ErasedRegistrationCapsule>(encoder);
        encode_measurement::<Registration>(encoder);
        encode_measurement::<RegistrationSet>(encoder);
        encode_measurement::<RegistrationStatus>(encoder);
        encode_measurement::<HostCallbackBoundary>(encoder);
        encode_measurement::<ModuleCallError>(encoder);
        encode_measurement::<ModuleAbiIdentity>(encoder);
        encode_measurement::<ModuleEpochToken>(encoder);
        encode_measurement::<EpochToken>(encoder);
        encode_measurement::<ErasedValue>(encoder);
        encode_measurement::<AssetRuntimeDescriptor>(encoder);
        encode_measurement::<EncodeContainer>(encoder);
        encode_measurement::<PlaceholderThunk>(encoder);
        encode_measurement::<CallbackPanic>(encoder);
        encode_measurement::<*mut dyn EncodeSink>(encoder);
        encode_measurement::<*mut dyn AuthoringImportContext>(encoder);
        encode_measurement::<*mut dyn PipelineProcessContext>(encoder);
        encode_measurement::<*mut dyn PipelineCodegenContext>(encoder);
        encode_measurement::<AuthoredValue>(encoder);
        encode_measurement::<ImporterDescriptor>(encoder);
        encode_measurement::<ProcessorDescriptor>(encoder);
        encode_measurement::<CodegenDescriptor>(encoder);
        encode_measurement::<CodegenAsset>(encoder);
        encode_measurement::<CodegenContextError>(encoder);
        encode_measurement::<ValidatorDescriptor>(encoder);
        encode_measurement::<DefaultsDescriptor>(encoder);
        encode_measurement::<ToolDescriptor>(encoder);
        encode_measurement::<ToolRegistration>(encoder);
        encode_measurement::<ToolSource>(encoder);
        encode_measurement::<ProcessorProducts>(encoder);
        encode_measurement::<ProcessArtifact>(encoder);
        encode_measurement::<ProcessOutputs>(encoder);
        encode_measurement::<ProcessContextError>(encoder);
        encode_measurement::<ProcessorError>(encoder);
        encode_measurement::<MigrationFunctionError>(encoder);
        encode_measurement::<Diagnostic>(encoder);
        encode_measurement::<Diagnostics>(encoder);
        encode_measurement::<CallbackInvokeError<ProcessorError>>(encoder);
        encode_measurement::<AuthoringImporterError>(encoder);
        encode_measurement::<ImportOutput>(encoder);
        encode_measurement::<OutputDecls>(encoder);
        encode_measurement::<OutputError>(encoder);
        encode_measurement::<ToolOutput>(encoder);
        encode_measurement::<ToolRunError>(encoder);
    });
    ModuleAbiIdentity {
        rustc: host_rustc_identity().to_owned(),
        interface_fingerprint,
        measured_interface,
        panic_strategy: if cfg!(panic = "unwind") {
            "unwind".to_owned()
        } else {
            "abort".to_owned()
        },
        allocator: "system".to_owned(),
    }
}

fn encode_measurement<T>(encoder: &mut distill_core::canonical::CanonicalEncoder) {
    encoder.str(std::any::type_name::<T>());
    encoder.u64(std::mem::size_of::<T>() as u64);
    encoder.u64(std::mem::align_of::<T>() as u64);
}

#[derive(Debug, Default)]
pub struct DynamicPipelineModuleLoader;

impl PipelineModuleLoader for DynamicPipelineModuleLoader {
    fn open_staged(
        &mut self,
        staged: &StagedModule,
    ) -> Result<Box<dyn LoadedPipelineModule>, ModuleCallError> {
        // SAFETY: opening native code remains the explicitly audited module
        // boundary. The shared host verifies the staged bytes immediately
        // before opening exactly this host-owned path.
        let image = unsafe {
            ngp_module_host::HostedLibrary::open_verified(&staged.path, staged.content_hash)
        }
        .map_err(|error| ModuleCallError::new(error.to_string()))?;
        // SAFETY: `open_verified` authenticated and pinned the exact staged
        // image. The shared reader bounds and copies the four NGP data symbols
        // while that image remains resident.
        let source_identity = unsafe { ngp_module_host::read_source_identity(&image) }
            .map_err(|error| ModuleCallError::new(error.to_string()))?;
        let table = {
            // SAFETY: the symbol has a fixed C ABI and is not called through a
            // Rust ABI until the table's prefix version has been checked.
            let entry = unsafe { image.get::<PipelineModuleEntryV2>(PIPELINE_MODULE_SYMBOL_V2) }
                .map_err(|error| {
                    ModuleCallError::new(format!(
                        "pipeline module is missing distill_pipeline_module_v2: {error}"
                    ))
                })?;
            // SAFETY: the exported entry contract returns a resident static
            // table. It is copied while the image is pinned by `image`.
            let pointer = unsafe { entry() };
            if pointer.is_null() {
                return Err(ModuleCallError::new(
                    "pipeline module returned a null function table",
                ));
            }
            // SAFETY: non-null pointer is promised by the fixed entry ABI to
            // identify a properly aligned PipelineModuleTableV2.
            unsafe { *pointer }
        };
        if table.abi_version != PIPELINE_MODULE_ABI_VERSION_V2 {
            return Err(ModuleCallError::new(format!(
                "pipeline module ABI version {} is unsupported (expected {})",
                table.abi_version, PIPELINE_MODULE_ABI_VERSION_V2
            )));
        }
        Ok(Box::new(DynamicLoadedPipelineModule {
            image: Some(image),
            table: Some(table),
            source_identity,
        }))
    }
}

struct DynamicLoadedPipelineModule {
    image: Option<ngp_module_host::HostedLibrary>,
    table: Option<PipelineModuleTableV2>,
    source_identity: ngp_module_host::ModuleSourceIdentity,
}

impl DynamicLoadedPipelineModule {
    fn table(&self) -> Result<PipelineModuleTableV2, ModuleCallError> {
        self.table
            .ok_or_else(|| ModuleCallError::new("pipeline module is already closed"))
    }
}

impl LoadedPipelineModule for DynamicLoadedPipelineModule {
    fn source_identity(
        &mut self,
    ) -> Result<ngp_module_host::ModuleSourceIdentity, ModuleCallError> {
        self.table()?;
        Ok(self.source_identity.clone())
    }

    fn module_abi(&mut self) -> Result<ModuleAbiIdentity, ModuleCallError> {
        decode_module_abi_identity(&read_probe(self.table()?.module_abi)?)
    }

    fn register(
        &mut self,
        targets: &[TargetDefinition],
        arena: &mut CandidateRegistrationArena,
    ) -> Result<BTreeSet<String>, ModuleCallError> {
        // SAFETY: this Rust-ABI entry is reached only after the host compared
        // the shared source identity and ModuleAbiIdentity exactly.
        unsafe { (self.table()?.register)(targets, arena) }
    }

    fn unload(&mut self) -> Result<(), ModuleCallError> {
        // SAFETY: same checked Rust-ABI contract as `register`.
        unsafe { (self.table()?.unload)() }
    }

    fn dlclose(&mut self) {
        // Invalidate every copied call target before releasing the image. Even
        // accidental trait-object reuse can now return only a closed-state
        // error rather than jumping through a resident-table pointer.
        self.table = None;
        if let Some(image) = self.image.take() {
            image.close();
        }
    }
}

pub fn encode_module_abi_identity(
    identity: &ModuleAbiIdentity,
) -> Result<Vec<u8>, ModuleCallError> {
    let mut writer = Writer::default();
    writer.byte(IDENTITY_ENCODING_VERSION);
    writer.string(&identity.rustc)?;
    writer.raw(&identity.interface_fingerprint);
    writer.raw(&identity.measured_interface);
    writer.string(&identity.panic_strategy)?;
    writer.string(&identity.allocator)?;
    Ok(writer.bytes)
}

pub fn decode_module_abi_identity(bytes: &[u8]) -> Result<ModuleAbiIdentity, ModuleCallError> {
    let mut reader = Reader::new(bytes);
    let version = reader.byte()?;
    if version != IDENTITY_ENCODING_VERSION {
        return Err(ModuleCallError::new(format!(
            "unsupported module identity encoding {version}"
        )));
    }
    let module_abi = ModuleAbiIdentity {
        rustc: reader.string()?,
        interface_fingerprint: reader.array()?,
        measured_interface: reader.array()?,
        panic_strategy: reader.string()?,
        allocator: reader.string()?,
    };
    reader.finish()?;
    Ok(module_abi)
}

fn read_probe(probe: PipelineProbeFnV2) -> Result<Vec<u8>, ModuleCallError> {
    let mut required = 0_u32;
    // SAFETY: the C prefix explicitly permits a null buffer with zero
    // capacity and writes only the required length through `required`.
    let status = unsafe { probe(std::ptr::null_mut(), 0, &mut required) };
    if status < 0 {
        return Err(ModuleCallError::new(format!(
            "pipeline probe rejected its size query with status {status}"
        )));
    }
    let required = usize::try_from(required)
        .map_err(|_| ModuleCallError::new("pipeline probe size does not fit usize"))?;
    if required > MAX_PROBE_BYTES {
        return Err(ModuleCallError::new(format!(
            "pipeline probe requested {required} bytes (limit {MAX_PROBE_BYTES})"
        )));
    }
    let mut bytes = vec![0_u8; required];
    let mut written = u32::try_from(required)
        .map_err(|_| ModuleCallError::new("pipeline probe size does not fit u32"))?;
    // SAFETY: `bytes` provides exactly `written` writable bytes. The C prefix
    // contract reports failure as status and never unwinds.
    let status = unsafe { probe(bytes.as_mut_ptr(), written, &mut written) };
    if status < 0 {
        return Err(ModuleCallError::new(format!(
            "pipeline probe rejected its write with status {status}"
        )));
    }
    let written = usize::try_from(written)
        .map_err(|_| ModuleCallError::new("pipeline probe result does not fit usize"))?;
    if written > bytes.len() {
        return Err(ModuleCallError::new(
            "pipeline probe grew between size and write calls",
        ));
    }
    bytes.truncate(written);
    Ok(bytes)
}

#[derive(Default)]
struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    fn byte(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn raw(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    fn count(&mut self, value: usize) -> Result<(), ModuleCallError> {
        let value = u32::try_from(value)
            .map_err(|_| ModuleCallError::new("module ABI count exceeds u32"))?;
        self.raw(&value.to_le_bytes());
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<(), ModuleCallError> {
        if !is_nfc(value) {
            return Err(ModuleCallError::new("module ABI string is not NFC"));
        }
        self.count(value.len())?;
        self.raw(value.as_bytes());
        Ok(())
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ModuleCallError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| ModuleCallError::new("module ABI length overflow"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| ModuleCallError::new("truncated module ABI payload"))?;
        self.position = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ModuleCallError> {
        self.take(N)?
            .try_into()
            .map_err(|_| ModuleCallError::new("truncated module ABI array"))
    }

    fn byte(&mut self) -> Result<u8, ModuleCallError> {
        Ok(self.array::<1>()?[0])
    }

    fn count(&mut self) -> Result<usize, ModuleCallError> {
        usize::try_from(u32::from_le_bytes(self.array()?))
            .map_err(|_| ModuleCallError::new("module ABI count does not fit usize"))
    }

    fn string(&mut self) -> Result<String, ModuleCallError> {
        let length = self.count()?;
        let value = std::str::from_utf8(self.take(length)?)
            .map_err(|_| ModuleCallError::new("module ABI string is not UTF-8"))?;
        if !is_nfc(value) {
            return Err(ModuleCallError::new("module ABI string is not NFC"));
        }
        Ok(value.to_owned())
    }

    fn finish(&self) -> Result<(), ModuleCallError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(ModuleCallError::new("trailing bytes in module ABI payload"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe extern "C" fn rejected_probe(
        _buffer: *mut u8,
        _capacity: u32,
        _length: *mut u32,
    ) -> i32 {
        -1
    }

    unsafe fn empty_register(
        _targets: &[TargetDefinition],
        _arena: &mut CandidateRegistrationArena,
    ) -> Result<BTreeSet<String>, ModuleCallError> {
        Ok(BTreeSet::new())
    }

    unsafe fn empty_unload() -> Result<(), ModuleCallError> {
        Ok(())
    }

    #[test]
    fn closed_module_invalidates_every_copied_call_target() {
        let mut module = DynamicLoadedPipelineModule {
            image: None,
            table: Some(PipelineModuleTableV2 {
                abi_version: PIPELINE_MODULE_ABI_VERSION_V2,
                module_abi: rejected_probe,
                register: empty_register,
                unload: empty_unload,
            }),
            source_identity: ngp_module_host::ModuleSourceIdentity {
                crate_name: "pipeline".to_owned(),
                source_hash: "0123456789abcdef".to_owned(),
            },
        };

        module.dlclose();
        assert!(module
            .module_abi()
            .unwrap_err()
            .detail()
            .contains("already closed"));
        assert!(module
            .unload()
            .unwrap_err()
            .detail()
            .contains("already closed"));
    }
}
