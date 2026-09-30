//! The pipeline module table, its ABI identity, and the export macro.
//!
//! A pipeline cdylib supplies safe `register` and `unload` functions, derives
//! New Game Plus's shared `NgpSourceIdentity`, and invokes
//! [`export_pipeline_module_v2!`]. The generated unsafe surface is deliberately
//! small: one C ABI table entry, one C ABI identity probe, and two Rust ABI
//! calls that contain unwinding before it can cross the dynamic-library
//! boundary.
//!
//! The identity covers this crate and its dependency crates only (see
//! `build.rs`), so host edits outside that closure leave built modules valid.

use std::collections::BTreeSet;
use std::panic::{catch_unwind, AssertUnwindSafe};

use distill_core::callback::CallbackPanic;
use distill_core::canonical::{domain_digest, DSMA};
use distill_json::AuthoredValue;
use distill_migrate::FieldPath;
use ngp_schema::SchemaNode;
use unicode_normalization::is_nfc;

use crate::callbacks::{
    CallbackHandle, CodegenAsset, CodegenContextError, CodegenDescriptor, DefaultsDescriptor,
    Diagnostic, Diagnostics, ImporterDescriptor, MigrationFunctionError, PipelineCodegenContext,
    PipelineProcessContext, ProcessArtifact, ProcessContextError, ProcessOutputs,
    ProcessorDescriptor, ProcessorError, ProcessorProducts, ToolDescriptor, ToolRegistration,
    ToolSource, ValidatorDescriptor,
};
use crate::codegen::{CodegenFailure, GeneratedFile};
use crate::failure::StableFailureFingerprint;
use crate::import::ImportOutput;
use crate::importer::{AuthoringImportContext, AuthoringImporterError};
use crate::outputs::{OutputDecls, OutputError};
use crate::registration::{
    ErasedCallback, ModuleCallError, Registration, RegistrationArena, RegistrationHost,
    RegistrationStatus, TargetDefinition,
};
use crate::target::Target;
use crate::tool::{ToolOutput, ToolRunError};

mod host_interface_closure {
    include!(concat!(env!("OUT_DIR"), "/host_interface_closure.rs"));
}

pub const PIPELINE_MODULE_ABI_VERSION_V2: u32 = 2;
pub const PIPELINE_MODULE_SYMBOL_V2: &[u8] = b"distill_pipeline_module_v2\0";
const IDENTITY_ENCODING_VERSION: u8 = 1;

pub type PipelineProbeFnV2 =
    unsafe extern "C" fn(buffer: *mut u8, capacity: u32, length: *mut u32) -> i32;
pub type PipelineRegisterFnV2 = unsafe fn(
    targets: &[TargetDefinition],
    arena: &mut RegistrationArena<'_>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleAbiIdentity {
    pub rustc: String,
    pub interface_fingerprint: [u8; 32],
    pub measured_interface: [u8; 32],
    pub panic_strategy: String,
    pub allocator: String,
}

/// The files the interface fingerprint hashes, by label.
pub fn host_interface_closure_manifest() -> &'static [(&'static str, &'static [u8])] {
    host_interface_closure::HOST_INTERFACE_CLOSURE
}

/// Exact `rustc -vV` of the compiler that built this image.
/// This is intentionally independent of the watched project's DSLI.
pub fn host_rustc_identity() -> &'static str {
    host_interface_closure::HOST_RUSTC_IDENTITY
}

/// The ABI identity of this interface crate as compiled into this image. The
/// host puts it in the expected candidate; a module returns its own from the
/// C-prefix probe, and the two must be equal.
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
        encode_measurement::<RegistrationArena<'static>>(encoder);
        encode_measurement::<*mut dyn RegistrationHost>(encoder);
        encode_measurement::<ErasedCallback>(encoder);
        encode_measurement::<CallbackHandle>(encoder);
        encode_measurement::<Registration>(encoder);
        encode_measurement::<RegistrationStatus>(encoder);
        encode_measurement::<ModuleCallError>(encoder);
        encode_measurement::<ModuleAbiIdentity>(encoder);
        encode_measurement::<CallbackPanic>(encoder);
        encode_measurement::<*mut dyn AuthoringImportContext>(encoder);
        encode_measurement::<*mut dyn PipelineProcessContext>(encoder);
        encode_measurement::<*mut dyn PipelineCodegenContext>(encoder);
        encode_measurement::<AuthoredValue>(encoder);
        encode_measurement::<SchemaNode>(encoder);
        encode_measurement::<FieldPath>(encoder);
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
        encode_measurement::<AuthoringImporterError>(encoder);
        encode_measurement::<ImportOutput>(encoder);
        encode_measurement::<OutputDecls>(encoder);
        encode_measurement::<OutputError>(encoder);
        encode_measurement::<Target>(encoder);
        encode_measurement::<StableFailureFingerprint>(encoder);
        encode_measurement::<GeneratedFile>(encoder);
        encode_measurement::<CodegenFailure>(encoder);
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

const PROBE_OK: i32 = 0;
const PROBE_INSUFFICIENT_CAPACITY: i32 = 1;
const PROBE_ERROR: i32 = -1;

/// The shared module-side implementation of the pre-Rust-ABI identity probe.
///
/// A null buffer with zero capacity is the required size-query form. On every
/// non-error return, `length` receives the complete required byte count. A
/// positive status means that the supplied buffer was too small.
///
/// # Safety
///
/// `length` must be null or point to writable `u32` storage. When `capacity`
/// is nonzero, `buffer` must be null or point to `capacity` writable bytes.
pub unsafe extern "C" fn module_abi_probe_v2(
    buffer: *mut u8,
    capacity: u32,
    length: *mut u32,
) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if length.is_null() {
            return Err(ModuleCallError::new(
                "pipeline ABI probe received a null length pointer",
            ));
        }
        let bytes = encode_module_abi_identity(&host_module_abi_identity())?;
        let required = u32::try_from(bytes.len())
            .map_err(|_| ModuleCallError::new("pipeline ABI identity exceeds u32"))?;

        // SAFETY: required by this function's caller contract and checked for
        // null immediately above.
        unsafe { length.write(required) };
        if capacity < required {
            return Ok(PROBE_INSUFFICIENT_CAPACITY);
        }
        if required != 0 && buffer.is_null() {
            return Err(ModuleCallError::new(
                "pipeline ABI probe received a null output buffer",
            ));
        }
        if required != 0 {
            // SAFETY: the caller promises `capacity` writable bytes and the
            // capacity check proves the complete payload fits.
            unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer, bytes.len()) };
        }
        Ok(PROBE_OK)
    }));

    match result {
        Ok(Ok(status)) => status,
        Ok(Err(_)) | Err(_) => PROBE_ERROR,
    }
}

#[doc(hidden)]
pub fn contain_module_call<T>(
    operation: &str,
    call: impl FnOnce() -> Result<T, ModuleCallError>,
) -> Result<T, ModuleCallError> {
    catch_unwind(AssertUnwindSafe(call)).unwrap_or_else(|_| {
        Err(ModuleCallError::new(format!(
            "pipeline module {operation} panicked"
        )))
    })
}

/// Export a Distill v2 pipeline module table from safe author functions.
///
/// The register function must have the signature
/// `fn(&[TargetDefinition], &mut RegistrationArena)
/// -> Result<BTreeSet<String>, ModuleCallError>`. The unload function must be
/// `fn() -> Result<(), ModuleCallError>`. A module must also export the shared
/// source identity, normally with `#[derive(NgpSourceIdentity)]` and a
/// `build.rs` that calls `ngp_source_hash::build_script_main()`.
///
/// The macro may be invoked only once in a cdylib.
#[macro_export]
macro_rules! export_pipeline_module_v2 {
    (register = $register:path, unload = $unload:path $(,)?) => {
        // The module boundary moves allocations across; see the crate docs.
        #[global_allocator]
        static __DISTILL_SYSTEM_ALLOCATOR: ::std::alloc::System = ::std::alloc::System;

        unsafe fn __distill_pipeline_register_v2(
            targets: &[$crate::registration::TargetDefinition],
            arena: &mut $crate::registration::RegistrationArena<'_>,
        ) -> ::std::result::Result<
            ::std::collections::BTreeSet<::std::string::String>,
            $crate::registration::ModuleCallError,
        > {
            $crate::module::contain_module_call("register", || $register(targets, arena))
        }

        unsafe fn __distill_pipeline_unload_v2(
        ) -> ::std::result::Result<(), $crate::registration::ModuleCallError> {
            $crate::module::contain_module_call("unload", $unload)
        }

        static __DISTILL_PIPELINE_MODULE_TABLE_V2: $crate::module::PipelineModuleTableV2 =
            $crate::module::PipelineModuleTableV2 {
                abi_version: $crate::module::PIPELINE_MODULE_ABI_VERSION_V2,
                module_abi: $crate::module::module_abi_probe_v2,
                register: __distill_pipeline_register_v2,
                unload: __distill_pipeline_unload_v2,
            };

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn distill_pipeline_module_v2(
        ) -> *const $crate::module::PipelineModuleTableV2 {
            &__DISTILL_PIPELINE_MODULE_TABLE_V2
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_supports_size_query_and_exact_write() {
        let mut length = 0_u32;
        // SAFETY: null/zero is the documented size-query form and `length` is
        // valid writable storage.
        let status = unsafe { module_abi_probe_v2(std::ptr::null_mut(), 0, &mut length) };
        assert_eq!(status, PROBE_INSUFFICIENT_CAPACITY);
        assert_ne!(length, 0);

        let mut bytes = vec![0_u8; length as usize];
        // SAFETY: `bytes` supplies the exact reported capacity.
        let status = unsafe { module_abi_probe_v2(bytes.as_mut_ptr(), length, &mut length) };
        assert_eq!(status, PROBE_OK);
        assert_eq!(
            decode_module_abi_identity(&bytes).unwrap(),
            host_module_abi_identity()
        );
    }

    #[test]
    fn probe_and_module_calls_contain_panics() {
        // SAFETY: a null length pointer is accepted as a reported probe error.
        assert_eq!(
            unsafe { module_abi_probe_v2(std::ptr::null_mut(), 0, std::ptr::null_mut()) },
            PROBE_ERROR
        );
        let error = contain_module_call::<()>("register", || panic!("fixture panic")).unwrap_err();
        assert_eq!(error.detail(), "pipeline module register panicked");
    }

    #[test]
    fn interface_closure_is_this_crate_and_its_dependencies() {
        let manifest = host_interface_closure_manifest();
        let crates = manifest
            .iter()
            .map(|(label, _)| label.split('/').next().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            crates,
            BTreeSet::from([
                "distill-core",
                "distill-json",
                "distill-migrate",
                "distill-pipeline-api",
                "ngp-schema",
                "ngp-source-hash",
            ])
        );
        assert!(manifest
            .iter()
            .any(|(label, _)| *label == "distill-pipeline-api/src/callbacks.rs"));
        assert!(manifest
            .iter()
            .any(|(label, _)| *label == "ngp-schema/src/identity.rs"));
    }
}
